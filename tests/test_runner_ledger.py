"""The decisions ledger: what each module settled for the ones after it.

Run: python -m unittest tests.test_runner_ledger
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
from sbx_omnigent.runner.ledger import DecisionLedger
from sbx_omnigent.runner.parsing.decisions import parse_decisions_doc
from sbx_omnigent.runner.paths import decisions_ledger_path
from sbx_omnigent.runner.prompts.review import _REVIEW_SETTLED
from sbx_omnigent.runner.records.reviews import ReviewRecord

_CAMPAIGN = """\
name: mods
repo: ./proj
publish: local
task: |
  the shared project brief
subtasks:
  - {id: m0, title: contracts and core}
  - {id: m1, title: storage and schema}
agents:
  build: {template: coder, model: claude-sonnet-5}
stages:
  - {id: build, run: build, write: true}
"""

_PLAN = """\
# m0 design

DECISIONS FOR LATER MODULES:
- tuples are upserted, never inserted
- ids are ULIDs
"""


class _FakeWorktrees:
    """The worktree-manager calls the ledger makes."""

    def __init__(self) -> None:
        self.files: dict[str, str] = {}
        self.written: list[tuple[str, str, str]] = []
        self.commits: list[tuple[str, str, str, str]] = []

    def read_tracked_file(self, worktree: str, path: str) -> str:
        if path not in self.files:
            raise click.ClickException(f'{path}: not tracked')
        return self.files[path]

    def node_worktree_path(self, run_id: str, node_id: str) -> str:
        return f'/w/{run_id}/{node_id}'

    def write_tracked_file(self, worktree: str, path: str, text: str) -> None:
        self.written.append((worktree, path, text))

    def commit_node(
        self, run_id: str, node_id: str, *, message: str, author: str,
    ) -> None:
        self.commits.append((run_id, node_id, message, author))


def _review(*later: str) -> ReviewRecord:
    return ReviewRecord(
        chunk='m0', stage='m0-review', reviewer='sec', round_no=1,
        verdict='APPROVED', later_increment=later,
    )


class _LedgerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        echo = mock.patch.object(click, 'echo')
        echo.start()
        self.addCleanup(echo.stop)
        self.wt = _FakeWorktrees()

    def ledger(self, text: str = _CAMPAIGN) -> DecisionLedger:
        path = self.root / 'pipeline.yaml'
        path.write_text(text, encoding='utf-8')
        cfg = pipeline.load_pipeline(path)
        return DecisionLedger(RunContext(
            config=cfg,
            worktrees=self.wt,
            sessions=mock.Mock(),
            run_id='r1',
            nodes={},
            stage_by_id={},
            lock=threading.RLock(),
            max_rounds=3,
        ))

    @property
    def subtasks(self) -> tuple[pipeline.Subtask, ...]:
        return (
            pipeline.Subtask(id='m0', title='contracts and core'),
            pipeline.Subtask(id='m1', title='storage and schema'),
        )


class TestRecordingDecisions(_LedgerTest):
    def test_a_plans_decisions_are_recorded_for_its_module(self) -> None:
        ledger = self.ledger()
        ledger.record_decisions('m0', _PLAN)
        self.assertEqual(ledger.decisions, [
            ('m0', 'tuples are upserted, never inserted'),
            ('m0', 'ids are ULIDs'),
        ])

    def test_outside_a_module_nothing_is_recorded(self) -> None:
        ledger = self.ledger()
        ledger.record_decisions(None, _PLAN)
        self.assertEqual(ledger.decisions, [])

    def test_a_decision_already_held_is_not_doubled(self) -> None:
        ledger = self.ledger()
        ledger.record_decisions('m0', _PLAN)
        ledger.record_decisions('m1', _PLAN)
        self.assertEqual(len(ledger.decisions), 2)


class TestRecordingReferrals(_LedgerTest):
    def test_each_observation_is_recorded_once(self) -> None:
        # Both reviewers noticing it, and a re-raise each round, are
        # the normal case; the later planner needs it once.
        ledger = self.ledger()
        ledger.record_referrals('m0', [
            _review('pagination belongs to [m1]'),
            _review('pagination belongs to [m1]', 'retention too'),
        ])
        self.assertEqual(ledger.referrals, [
            ('m0', 'pagination belongs to [m1]'), ('m0', 'retention too'),
        ])

    def test_outside_a_module_nothing_is_recorded(self) -> None:
        ledger = self.ledger()
        ledger.record_referrals(None, [_review('x')])
        self.assertEqual(ledger.referrals, [])


class TestWhatEachReaderIsShown(_LedgerTest):
    def filled(self) -> DecisionLedger:
        ledger = self.ledger()
        ledger.decisions = [('m0', 'ids are ULIDs')]
        ledger.referrals = [('m0', 'pagination belongs to [m1]')]
        return ledger

    def test_the_planner_sees_decisions_and_referrals(self) -> None:
        block = self.filled().planner_block(self.subtasks)
        self.assertIn('[m0] contracts and core\n  - ids are ULIDs', block)
        self.assertIn('NOT decisions', block)
        self.assertIn('raised in [m0]\n  - pagination belongs to [m1]', block)

    def test_a_reviewer_sees_only_settled_decisions(self) -> None:
        # A referral is not binding; beside real rulings it would read
        # as a closed question.
        block = self.filled().rulings_block(self.subtasks)
        self.assertTrue(block.startswith(_REVIEW_SETTLED))
        self.assertIn('ids are ULIDs', block)
        self.assertNotIn('pagination', block)

    def test_with_nothing_settled_a_reviewer_is_told_nothing(self) -> None:
        self.assertEqual(self.ledger().rulings_block(self.subtasks), '')

    def test_the_document_reads_back_losslessly(self) -> None:
        # The document IS the channel, so writing and reading it back
        # has to be lossless — otherwise a decision degrades a little
        # on every module that passes it along.
        ledger = self.ledger()
        ledger.decisions = [
            ('m0', 'first decision'),
            ('m0', 'second decision — with an em dash and `code`'),
            ('m1', 'third decision'),
        ]
        doc = ledger.document(self.subtasks)
        self.assertEqual(parse_decisions_doc(doc), ledger.decisions)
        self.assertIn('# Decisions carried forward — mods', doc)

    def test_a_referral_is_not_read_back_as_a_decision(self) -> None:
        # Referrals are recorded as explicitly NOT binding. Reading one
        # back as a decision would promote a reviewer's passing note
        # into a constraint no later module may alter.
        ledger = self.filled()
        doc = ledger.document(self.subtasks)
        self.assertEqual(parse_decisions_doc(doc), [('m0', 'ids are ULIDs')])


class TestTheLedgerSurvivesAcrossRuns(_LedgerTest):
    def test_an_earlier_runs_decisions_are_carried_forward(self) -> None:
        earlier = self.ledger()
        earlier.decisions = [('m0', 'ids are ULIDs')]
        self.wt.files[earlier.path] = earlier.document(self.subtasks)
        later = self.ledger()
        later.seed_from('/w/plan')
        self.assertEqual(later.decisions, [('m0', 'ids are ULIDs')])
        self.assertIn('ids are ULIDs', later.planner_block(self.subtasks))

    def test_what_is_already_held_is_not_doubled(self) -> None:
        # A resumed run already holds them in state.
        ledger = self.ledger()
        ledger.decisions = [('m0', 'ids are ULIDs')]
        self.wt.files[ledger.path] = ledger.document(self.subtasks)
        ledger.seed_from('/w/plan')
        self.assertEqual(ledger.decisions, [('m0', 'ids are ULIDs')])

    def test_no_ledger_yet_seeds_nothing(self) -> None:
        # The first module of a campaign, which is the normal case.
        ledger = self.ledger()
        ledger.seed_from('/w/plan')
        self.assertEqual(ledger.decisions, [])

    def test_outside_a_campaign_nothing_is_read(self) -> None:
        ledger = self.ledger(_CAMPAIGN.replace(
            'subtasks:\n  - {id: m0, title: contracts and core}\n'
            '  - {id: m1, title: storage and schema}\n', '',
        ))
        self.wt.files[ledger.path] = '## [m0]\n\n- ids are ULIDs\n'
        ledger.seed_from('/w/plan')
        self.assertEqual(ledger.decisions, [])


class TestCommittingTheLedger(_LedgerTest):
    def test_it_is_committed_onto_the_published_branch(self) -> None:
        ledger = self.ledger()
        ledger.decisions = [('m0', 'ids are ULIDs')]
        ledger.commit('m0-build', self.subtasks)
        self.assertEqual(self.wt.written, [(
            '/w/r1/m0-build', 'docs/plans/mods-decisions.md',
            ledger.document(self.subtasks),
        )])
        self.assertEqual(self.wt.commits, [(
            'r1', 'm0-build', 'docs: record decisions binding later modules',
            'planner <planner@pipeline.local>',
        )])

    def test_a_referral_alone_is_committed(self) -> None:
        ledger = self.ledger()
        ledger.referrals = [('m0', 'pagination belongs to [m1]')]
        ledger.commit('m0-build', self.subtasks)
        self.assertEqual(len(self.wt.commits), 1)

    def test_an_empty_ledger_commits_nothing(self) -> None:
        self.ledger().commit('m0-build', self.subtasks)
        self.assertEqual((self.wt.written, self.wt.commits), ([], []))


class TestWhereTheLedgerLives(_LedgerTest):
    def test_beside_the_default_plan_path(self) -> None:
        self.assertEqual(self.ledger().path, 'docs/plans/mods-decisions.md')

    def test_beside_a_configured_plan_path(self) -> None:
        cfg = self.ledger(
            _CAMPAIGN.replace('publish: local', 'publish: local\n'
                              'plan_artifact: design/plan.md'),
        ).ctx.config
        self.assertEqual(
            decisions_ledger_path(cfg), 'design/plan-decisions.md',
        )


if __name__ == '__main__':
    unittest.main()
