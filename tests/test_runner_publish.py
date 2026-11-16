"""Publishing: a module's records, its branch, and its pull request.

Run: python -m unittest tests.test_runner_publish
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
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.publish import Publisher, series_note
from sbx_omnigent.runner.records.results import ChunkSpan
from sbx_omnigent.runner.records.reviews import ReviewRecord

_PIPELINE = """\
name: mods
repo: {repo}
base_branch: main
publish: {mode}
task: |
  the shared project brief
{subtasks}agents:
  build: {{template: coder, model: claude-sonnet-5}}
stages:
  - {{id: build, run: build, write: true}}
"""

_SUBTASKS = (
    'subtasks:\n'
    '  - {id: m0, title: contracts and core}\n'
    '  - {id: m1, title: storage and schema — the tables and their indexes}\n'
)

_GITHUB = 'https://github.com/org/proj'
_START = 'a' * 40
_END = 'b' * 40
_M0 = pipeline.Subtask(id='m0', title='contracts and core')
_M1 = pipeline.Subtask(
    id='m1', title='storage and schema — the tables and their indexes',
)


class _FakeWorktrees:
    """The worktree-manager calls publishing makes."""

    def __init__(self) -> None:
        self.tip: str | None = _END
        self.published: list[dict] = []
        self.base_has: bool | None = False
        self.edits: list[tuple[str, str]] = []
        self.refuse_edit = False

    def hub_branch_tip(self, run_id: str, node_id: str) -> str | None:
        return self.tip

    def publish_node(self, run_id, node_id, repo_url, **kw) -> str:
        self.published.append({'node': node_id, 'repo': repo_url, **kw})
        return f'{_GITHUB}/pull/{40 + len(self.published)}\n'

    def base_contains(self, repo_url, *, branch, commit) -> bool | None:
        return self.base_has

    def edit_pr_body(self, repo_url: str, pr_url: str, body: str) -> None:
        if self.refuse_edit:
            raise click.ClickException('the token lacks pull_requests')
        self.edits.append((pr_url, body))


class _PublishTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        echo = mock.patch.object(click, 'echo')
        echo.start()
        self.addCleanup(echo.stop)
        self.wt = _FakeWorktrees()
        self.records = mock.Mock()
        self.records.commit_reviews.return_value = 'docs/r.md'
        self.records.commit_selection.return_value = None
        self.ledger = mock.Mock()
        self.security = mock.Mock()
        self.security.commit_summary.return_value = None
        self.filing = mock.Mock(filed=[])
        self.reviews: list[ReviewRecord] = []
        self.saves = 0

    def publisher(
        self, *, mode: str = 'pr', repo: str = _GITHUB,
        stage_by_id: dict | None = None, per_module: bool = True,
    ) -> Publisher:
        path = self.root / 'pipeline.yaml'
        path.write_text(_PIPELINE.format(
            mode=mode, repo=repo, subtasks=_SUBTASKS if per_module else '',
        ), encoding='utf-8')
        cfg = pipeline.load_pipeline(path)
        ctx = RunContext(
            config=cfg, worktrees=self.wt, sessions=mock.Mock(),
            run_id='r1', nodes={}, stage_by_id=stage_by_id or {},
            lock=threading.RLock(), max_rounds=3,
        )

        def save() -> None:
            self.saves += 1

        return Publisher(
            ctx, publish_repo=repo, records=self.records, ledger=self.ledger,
            security=self.security, filing=self.filing,
            chunk_reviews=lambda: self.reviews, chunk_picks=lambda: [],
            save_state=save,
        )

    def publish_m1(self, publisher: Publisher, **kw: object) -> None:
        args: dict[str, object] = {
            'first': False, 'plan': '# m1 plan',
            'plan_path': 'docs/plans/mods-m1.md', 'prior': None,
            'subtasks': [_M0, _M1],
        }
        args.update(kw)
        publisher.publish_module(_M1, 'm1-build', **args)


class TestThePreflight(_PublishTest):
    def test_a_pr_needs_a_github_target(self) -> None:
        with self.assertRaises(PipelineRunError) as caught:
            self.publisher(repo='./proj').preflight()
        self.assertIn('--publish-repo', str(caught.exception))

    def test_a_local_publish_needs_no_github_target(self) -> None:
        self.publisher(mode='local', repo='./proj').preflight()


class TestPublishingAModule(_PublishTest):
    def test_nothing_happens_when_publishing_is_off(self) -> None:
        self.publish_m1(self.publisher(mode='none'))
        self.assertEqual((self.wt.published, self.saves), ([], 0))
        self.records.commit_plan.assert_not_called()

    def test_every_record_is_committed_before_the_push(self) -> None:
        self.publish_m1(self.publisher())
        self.records.commit_plan.assert_called_once_with(
            'm1-build', 'docs/plans/mods-m1.md', '# m1 plan',
        )
        self.ledger.record_referrals.assert_called_once_with('m1', [])
        self.ledger.commit.assert_called_once_with('m1-build', [_M0, _M1])
        self.records.commit_planning_session.assert_called_once_with(
            'm1-build', 'docs/plans/mods-m1.md', module=_M1,
        )
        self.filing.publish.assert_called_once_with('m1-build', 'docs/r.md')
        self.security.commit_reports.assert_called_once()
        self.security.commit_summary.assert_called_once()
        self.records.commit_selection.assert_called_once()

    def test_no_plan_commits_no_plan(self) -> None:
        self.publish_m1(self.publisher(), plan=None)
        self.records.commit_plan.assert_not_called()

    def test_it_publishes_to_the_modules_own_branch(self) -> None:
        self.publish_m1(self.publisher())
        (call,) = self.wt.published
        self.assertEqual(call['remote_branch'], 'pipeline/r1-m1')
        self.assertEqual(call['base_branch'], 'main')
        self.assertTrue(call['open_pr'])
        self.assertEqual(
            call['title'], '[pipeline] mods — m1: storage and schema',
        )

    def test_its_seed_and_pr_are_remembered_for_the_next_module(self) -> None:
        publisher = self.publisher()
        self.publish_m1(publisher)
        self.assertEqual(
            publisher.chunk_publishes['m1'],
            {'seed': _END, 'pr': f'{_GITHUB}/pull/41'},
        )
        self.assertEqual(publisher.published, [f'{_GITHUB}/pull/41\n'])
        self.assertEqual(self.saves, 1)

    def test_a_seed_that_is_not_a_commit_is_not_remembered(self) -> None:
        publisher = self.publisher()
        self.wt.tip = 'main; rm -rf'
        self.publish_m1(publisher)
        self.assertEqual(publisher.chunk_publishes['m1']['seed'], '')

    def test_a_local_publish_opens_no_pr(self) -> None:
        publisher = self.publisher(mode='local')
        self.publish_m1(publisher, prior='m0')
        self.assertFalse(self.wt.published[0]['open_pr'])
        self.assertEqual(publisher.chunk_publishes['m1']['pr'], '')


class TestThePlanningSessionRecord(_PublishTest):
    """Flat mode has ONE planner for the whole campaign, so its record
    is committed once and carried on; per-module mode records each."""

    def test_a_flat_campaign_records_it_on_its_first_module(self) -> None:
        self.publish_m1(self.publisher(per_module=False), first=True)
        self.records.commit_planning_session.assert_called_once()

    def test_a_flat_campaign_does_not_record_it_again(self) -> None:
        self.publish_m1(self.publisher(per_module=False), first=False)
        self.records.commit_planning_session.assert_not_called()

    def test_each_module_records_its_own_planner(self) -> None:
        self.publish_m1(self.publisher(), first=False)
        self.records.commit_planning_session.assert_called_once()


class TestASeriesOfModules(_PublishTest):
    def chained(self, publisher: Publisher) -> str:
        publisher.chunk_publishes['m0'] = {
            'seed': _START, 'pr': f'{_GITHUB}/pull/7',
        }
        self.publish_m1(publisher, prior='m0')
        return self.wt.published[0]['body']

    def test_the_first_module_has_no_series_note(self) -> None:
        self.publish_m1(self.publisher())
        self.assertNotIn('Part of a series', self.wt.published[0]['body'])

    def test_a_later_module_says_what_to_merge_first(self) -> None:
        body = self.chained(self.publisher())
        self.assertIn('**Part of a series.**', body)
        self.assertIn('Merge #7 first.', body)
        self.assertIn(f'compare/{_START}...{_END}', body)

    def test_the_note_then_links_this_prs_own_commits(self) -> None:
        self.chained(self.publisher())
        (url, body), = self.wt.edits
        self.assertEqual(url, f'{_GITHUB}/pull/41\n')
        self.assertIn(f'/pull/41/files/{_START}..{_END}', body)

    def test_a_refused_edit_keeps_the_pr(self) -> None:
        publisher = self.publisher()
        self.wt.refuse_edit = True
        self.chained(publisher)
        self.assertEqual(publisher.published, [f'{_GITHUB}/pull/41\n'])

    def test_no_note_once_the_base_holds_the_earlier_module(self) -> None:
        self.wt.base_has = True
        body = self.chained(self.publisher())
        self.assertNotIn('Part of a series', body)


class TestTheSeriesNote(unittest.TestCase):
    def span(self, **kw: str) -> ChunkSpan:
        fields = {
            'chunk': 'm1', 'prior': 'm0', 'prior_pr': '', 'start': _START,
            'end': _END, 'slug': 'org/proj', 'base': 'main',
        }
        fields.update(kw)
        return ChunkSpan(**fields)

    def test_without_the_earlier_pr_it_names_the_module(self) -> None:
        note = series_note(self.span(), None)
        self.assertIn("Merge [m0]'s pull request first.", note)

    def test_without_both_commits_there_is_no_link(self) -> None:
        note = series_note(self.span(start=''), None)
        self.assertNotIn('changes only', note)


class TestASinglePass(_PublishTest):
    def test_it_publishes_the_branch_with_the_task(self) -> None:
        publisher = self.publisher()
        result = publisher.publish_run(
            'build', plan='# the plan', module=None,
        )
        (call,) = self.wt.published
        self.assertEqual(call['title'], '[pipeline] mods')
        self.assertIn('the shared project brief', call['body'])
        self.assertNotIn('remote_branch', call)
        self.assertEqual(publisher.published, [result])
        self.assertEqual(self.saves, 1)

    def test_with_a_plan_the_plan_and_session_are_committed(self) -> None:
        self.publisher().publish_run(
            'build', plan='# the plan', module=None,
        )
        self.records.commit_plan.assert_called_once_with(
            'build', 'docs/plans/mods.md', '# the plan',
        )
        self.records.commit_planning_session.assert_called_once()

    def test_without_a_plan_the_reviews_still_are(self) -> None:
        self.publisher().publish_run('build', plan=None, module=None)
        self.records.commit_plan.assert_not_called()
        self.records.commit_reviews.assert_called_once()


class TestThePullRequestBody(_PublishTest):
    def test_a_contract_check_is_not_in_the_roster(self) -> None:
        # It read the contract, not the branch.
        checks = pipeline.PipelineStage(
            'm1-check', run=('build',), checks='m1-plan',
        )
        publisher = self.publisher(stage_by_id={'m1-check': checks})
        vote = ReviewRecord(chunk='m1', stage='m1-review', reviewer='sec',
                            round_no=1, verdict='APPROVED')
        check = ReviewRecord(chunk='m1', stage='m1-check', reviewer='c',
                             round_no=1, verdict='APPROVED')
        self.reviews += [vote, check]
        with mock.patch(
            'sbx_omnigent.runner.publish.render_pr_body', return_value='b',
        ) as render:
            self.publish_m1(publisher)
        self.assertEqual(render.call_args.kwargs['reviews'], [vote])

    def test_the_gates_evidence_is_shown(self) -> None:
        publisher = self.publisher()
        publisher.evidence['m1-build'] = mock.sentinel.outcome
        with mock.patch(
            'sbx_omnigent.runner.publish.render_pr_body', return_value='b',
        ) as render:
            self.publish_m1(publisher)
        self.assertIs(render.call_args.kwargs['outcome'],
                      mock.sentinel.outcome)


if __name__ == '__main__':
    unittest.main()
