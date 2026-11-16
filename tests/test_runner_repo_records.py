"""Records committed to the repo beside the code a module publishes.

Run: python -m unittest tests.test_runner_repo_records
"""

from __future__ import annotations

import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.paths import (
    chunk_artifact_base,
    chunk_plan_path,
    reviews_record_path,
    selection_record_path,
    session_record_path,
)
from sbx_omnigent.runner.records.judging import JudgePick
from sbx_omnigent.runner.records.results import NodeResult
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.repo_records import RepoRecords
from sbx_omnigent.swarm_session import SwarmSessionError

_PIPELINE = """\
name: mods
repo: ./proj
publish: local
task: |
  the shared project brief
{subtasks}agents:
  plan: {{template: planner, model: claude-sonnet-5}}
  build: {{template: coder, model: claude-sonnet-5}}
stages:
{plan}  - {{id: build, run: build, write: true}}
"""

_SUBTASKS = (
    'subtasks:\n'
    '  - {id: m0, title: contracts and core}\n'
    '  - {id: m1, title: storage and schema}\n'
)
_PLAN_STAGE = '  - {id: plan, run: plan}\n'

_M1 = pipeline.Subtask(id='m1', title='storage and schema')


class _FakeWorktrees:
    """The worktree-manager calls the records make."""

    def __init__(self) -> None:
        self.written: dict[str, str] = {}
        self.commits: list[tuple[str, str | None]] = []
        self.refuse = False
        self.commit_ok = True

    def node_worktree_path(self, run_id: str, node_id: str) -> str:
        return f'/w/{node_id}'

    def write_tracked_file(self, worktree: str, path: str, text: str):
        if self.refuse:
            raise click.ClickException('git refused the write')
        self.written[path] = text

    def commit_node(self, run_id, node_id, *, message, author=None):
        self.commits.append((message, author))
        return 'a' * 40 if self.commit_ok else None


class _RecordsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        echo = mock.patch.object(click, 'echo')
        echo.start()
        self.addCleanup(echo.stop)
        self.wt = _FakeWorktrees()
        self.sessions = mock.Mock()
        self.nodes: dict[str, NodeResult] = {}
        self.reviews: list[ReviewRecord] = []
        self.picks: list[JudgePick] = []
        self.turns: dict[str, tuple[tuple[str, str], ...]] = {}

    def config(
        self, *, subtasks: bool = True, planner: bool = True
    ) -> pipeline.PipelineConfig:
        path = self.root / 'pipeline.yaml'
        path.write_text(_PIPELINE.format(
            subtasks=_SUBTASKS if subtasks else '',
            plan=_PLAN_STAGE if planner else '',
        ), encoding='utf-8')
        return pipeline.load_pipeline(path)

    def records(self, **kw: bool) -> RepoRecords:
        cfg = self.config(**kw)
        ctx = RunContext(
            config=cfg, worktrees=self.wt, sessions=self.sessions,
            run_id='r1', nodes=self.nodes, stage_by_id={},
            lock=threading.RLock(), max_rounds=3,
        )
        return RepoRecords(
            ctx,
            chunk_reviews=lambda: self.reviews,
            chunk_picks=lambda: self.picks,
            reader_turns=lambda: self.turns,
        )


class TestWhereRecordsLive(_RecordsTest):
    def test_a_modules_plan_has_its_own_path(self) -> None:
        cfg = self.config()
        self.assertEqual(chunk_plan_path(cfg, _M1), 'docs/plans/mods-m1.md')

    def test_outside_a_module_the_runs_plan_path_is_used(self) -> None:
        cfg = self.config()
        self.assertEqual(chunk_plan_path(cfg, None), 'docs/plans/mods.md')

    def test_a_flat_campaign_folds_the_module_into_record_names(self) -> None:
        # Flat mode shares one plan path across modules, so a shared
        # record path would be rewritten by every module.
        cfg = self.config(subtasks=False)
        base = chunk_artifact_base(cfg, _M1, 'docs/plans/mods.md')
        self.assertEqual(base, 'docs/plans/mods-m1.md')

    def test_a_per_module_plan_path_is_not_folded_twice(self) -> None:
        cfg = self.config()
        plan = 'docs/plans/mods-m1.md'
        self.assertEqual(chunk_artifact_base(cfg, _M1, plan), plan)

    def test_each_record_sits_beside_the_plan(self) -> None:
        cfg = self.config()
        plan = 'docs/plans/mods-m1.md'
        self.assertEqual(
            (reviews_record_path(cfg, _M1, plan),
             selection_record_path(cfg, _M1, plan),
             session_record_path(plan)),
            ('docs/plans/mods-m1-reviews.md',
             'docs/plans/mods-m1-selection.md',
             'docs/plans/mods-m1-session.md'),
        )


class TestThePlanOfRecord(_RecordsTest):
    def test_the_plan_is_committed_at_its_path(self) -> None:
        self.records().commit_plan('m1-build', 'docs/plans/mods-m1.md', '# P')
        self.assertEqual(self.wt.written, {'docs/plans/mods-m1.md': '# P'})
        self.assertEqual(self.wt.commits, [
            ('docs: add plan of record', 'planner <planner@pipeline.local>'),
        ])

    def test_a_refused_plan_commit_is_not_swallowed(self) -> None:
        # Unlike the other records, the plan is what the PR is for.
        records = self.records()
        self.wt.refuse = True
        with self.assertRaises(click.ClickException):
            records.commit_plan('m1-build', 'docs/plans/mods-m1.md', '# P')


class TestTheReviewsRecord(_RecordsTest):
    def review(self) -> ReviewRecord:
        rec = ReviewRecord(
            chunk='m1', stage='m1-review', reviewer='sec', round_no=1,
            verdict='APPROVED', turns=(('assistant', 'It builds.'),),
        )
        self.reviews.append(rec)
        return rec

    def test_the_modules_reviews_are_committed(self) -> None:
        records = self.records()
        self.review()
        path = records.commit_reviews(
            'm1-build', 'docs/plans/mods-m1.md', module=_M1,
        )
        self.assertEqual(path, 'docs/plans/mods-m1-reviews.md')
        self.assertIn('Reviews — mods [m1] storage and schema',
                      self.wt.written[path])

    def test_no_reviews_commits_nothing(self) -> None:
        self.assertIsNone(self.records().commit_reviews(
            'm1-build', 'docs/plans/mods-m1.md', module=_M1,
        ))

    def test_a_refused_write_is_reported_not_raised(self) -> None:
        records = self.records()
        self.review()
        self.wt.refuse = True
        self.assertIsNone(records.commit_reviews(
            'm1-build', 'docs/plans/mods-m1.md', module=_M1,
        ))

    def test_nothing_to_commit_returns_no_path(self) -> None:
        records = self.records()
        self.review()
        self.wt.commit_ok = False
        self.assertIsNone(records.commit_reviews(
            'm1-build', 'docs/plans/mods-m1.md', module=_M1,
        ))


class TestTheSelectionRecord(_RecordsTest):
    def test_the_judges_picks_are_committed_as_the_judge(self) -> None:
        records = self.records()
        self.picks.append(JudgePick(
            chunk='m1', stage='m1-judge', candidates=('a', 'b'),
            selected='a', stated='a',
        ))
        path = records.commit_selection(
            'm1-build', 'docs/plans/mods-m1.md', module=_M1,
        )
        self.assertEqual(path, 'docs/plans/mods-m1-selection.md')
        # A role, never the judge agent's name.
        self.assertEqual(
            self.wt.commits[-1][1], 'judge <judge@pipeline.local>',
        )

    def test_a_pipeline_with_no_judge_commits_nothing(self) -> None:
        self.assertIsNone(self.records().commit_selection(
            'm1-build', 'docs/plans/mods-m1.md', module=_M1,
        ))


class TestThePlanningSession(_RecordsTest):
    _PLAN = 'docs/plans/mods-m1.md'
    _PATH = 'docs/plans/mods-m1-session.md'

    def test_the_buffered_conversation_is_committed(self) -> None:
        # The buffer first: a resumed run has no session to read.
        records = self.records()
        self.nodes['m1-plan'] = NodeResult('m1-plan', 'reader')
        self.turns['m1-plan'] = (('user', 'Use ULIDs.'), ('assistant', 'OK'))
        records.commit_planning_session('m1-build', self._PLAN, module=_M1)
        self.assertIn('Use ULIDs.', self.wt.written[self._PATH])
        self.sessions.read_transcript.assert_not_called()

    def test_without_a_buffer_the_live_session_is_read(self) -> None:
        records = self.records()
        self.nodes['m1-plan'] = NodeResult('m1-plan', 'reader', session='s1')
        self.sessions.read_transcript.return_value = [('user', 'From VM')]
        records.commit_planning_session('m1-build', self._PLAN, module=_M1)
        self.assertIn('From VM', self.wt.written[self._PATH])

    def test_an_unreadable_session_records_nothing(self) -> None:
        records = self.records()
        self.nodes['m1-plan'] = NodeResult('m1-plan', 'reader', session='s1')
        self.sessions.read_transcript.side_effect = SwarmSessionError('gone')
        records.commit_planning_session('m1-build', self._PLAN, module=_M1)
        self.assertEqual(self.wt.written, {})

    def test_a_planner_that_never_ran_records_nothing(self) -> None:
        records = self.records()
        records.commit_planning_session('m1-build', self._PLAN, module=_M1)
        self.assertEqual(self.wt.written, {})

    def test_a_pipeline_with_no_planner_records_nothing(self) -> None:
        records = self.records(planner=False)
        records.commit_planning_session('m1-build', self._PLAN, module=_M1)
        self.assertEqual(self.wt.written, {})

    def test_outside_a_module_the_runs_planner_is_used(self) -> None:
        records = self.records(subtasks=False)
        self.nodes['plan'] = NodeResult('plan', 'reader')
        self.turns['plan'] = (('assistant', 'The one plan'),)
        records.commit_planning_session(
            'build', 'docs/plans/mods.md', module=None,
        )
        self.assertIn('The one plan',
                      self.wt.written['docs/plans/mods-session.md'])


if __name__ == '__main__':
    unittest.main()
