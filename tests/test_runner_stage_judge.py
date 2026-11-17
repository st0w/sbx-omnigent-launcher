"""The judge stage: choosing one candidate implementation to ship.

Run: python -m unittest tests.test_runner_stage_judge
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

import click

from sbx_omnigent.pipeline import PipelineStage, Subtask
from sbx_omnigent.runner.briefing import Briefing
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.forfeits import Forfeits
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.records.results import NodeResult
from sbx_omnigent.runner.stages.judge import JudgeStage, retry_instruction

_TESTS = PipelineStage(id='tests', run=('tw',), write=True)
_IMPL_A = PipelineStage(id='impl-a', run=('a',), write=True,
                        from_branch='tests')
_IMPL_B = PipelineStage(id='impl-b', run=('b',), write=True,
                        from_branch='tests')
_PICK = PipelineStage(id='pick', run=('judge',),
                      needs=('impl-a', 'impl-b'), selects='impl')


class _JudgeTest(unittest.TestCase):
    def setUp(self) -> None:
        echo = mock.patch.object(click, 'echo')
        self.echo = echo.start()
        self.addCleanup(echo.stop)
        self.config = mock.Mock()
        self.config.task = 'Parse port ranges.'
        self.config.acceptance = None
        self.config.setup = None
        self.config.base_branch = 'main'
        self.wt = mock.Mock()
        self.wt.create_judge_worktree.return_value = '/w/pick'
        self.wt.branch_contains.return_value = True
        self.wt.retain_node_bundle.side_effect = (
            lambda run, node, *, against: f'/c/_retained/{run}/{node}.bundle'
        )
        self.wt.node_branch.side_effect = lambda run, node: f'pl/{run}/{node}'
        self.nodes: dict[str, NodeResult] = {
            node: NodeResult(node, 'writer', branch=f'pl/r1/{node}')
            for node in ('tests', 'impl-a', 'impl-b')
        }
        self.driver = mock.Mock()
        self.driver.create.return_value = 'sess-pick'
        #: The judge's successive replies.
        self.replies: list[str] = []
        self.driver.drive.side_effect = lambda *a, **k: self.replies.pop(0)
        self.book = mock.Mock()
        self.book.reviewed_ok = set()
        self.capture = mock.Mock()
        self.progress = RunProgress()

    def judge(self, *, resume: bool = False) -> JudgeStage:
        ctx = RunContext(
            config=self.config, worktrees=self.wt, sessions=mock.Mock(),
            run_id='r1', nodes=self.nodes,
            stage_by_id={s.id: s for s in (_TESTS, _IMPL_A, _IMPL_B, _PICK)},
            lock=threading.RLock(), max_rounds=3,
        )
        self.forfeits = Forfeits(ctx)
        return JudgeStage(
            ctx,
            progress=self.progress,
            book=self.book,
            driver=self.driver,
            capture=self.capture,
            briefing=Briefing(ctx, progress=self.progress),
            forfeits=self.forfeits,
            resume=resume,
        )

    def pick(self) -> tuple:
        return self.book.record_pick.call_args


class TestAJudgeThatChooses(_JudgeTest):
    def test_its_pick_publishes_as_the_judges_own_branch(self) -> None:
        self.replies = ['B is simpler.\n\nSELECT: impl-b']
        self.judge().run(_PICK)
        node = self.nodes['pick']
        self.assertEqual((node.kind, node.selected, node.branch),
                         ('judge', 'impl-b', 'pl/r1/impl-b'))
        self.wt.alias_node_branch.assert_called_once_with(
            'r1', 'pick', 'impl-b',
        )
        self.assertEqual(self.progress.last_branch_node, 'pick')

    def test_it_reads_both_candidates_read_only(self) -> None:
        self.replies = ['SELECT: impl-a']
        self.judge().run(_PICK)
        self.wt.create_judge_worktree.assert_called_once_with(
            'r1', 'pick', ['impl-a', 'impl-b'], replace=False,
        )
        self.driver.create.assert_called_once_with(
            'judge', '/w/pick', 'ro', 'pick',
        )

    def test_the_loser_is_kept_and_the_pick_recorded(self) -> None:
        self.replies = ['SELECT: impl-a']
        self.judge().run(_PICK)
        args, _kw = self.pick()
        self.assertEqual(args[:4], ('pick', ['impl-a', 'impl-b'], 'impl-a',
                                    'impl-a'))
        self.assertEqual(args[5],
                         (('impl-b', '/c/_retained/r1/impl-b.bundle'),))

    def test_a_loser_that_cannot_be_kept_does_not_stop_it(self) -> None:
        self.wt.retain_node_bundle.side_effect = OSError('disk full')
        self.replies = ['SELECT: impl-a']
        self.judge().run(_PICK)
        self.assertEqual(self.pick().args[5], ())
        said = ' '.join(str(c.args[0]) for c in self.echo.call_args_list)
        self.assertIn('could NOT be preserved', said)

    def test_a_judge_that_did_not_decide_is_asked_once(self) -> None:
        self.replies = ['I have started the tests in the background.',
                        'SELECT: impl-b']
        self.judge().run(_PICK)
        self.assertEqual(self.nodes['pick'].selected, 'impl-b')
        retry = self.driver.drive.call_args_list[1].args[1]
        self.assertEqual(retry, retry_instruction(['impl-a', 'impl-b']))


class TestAJudgeThatNeverChooses(_JudgeTest):
    def test_the_run_halts_rather_than_take_the_first(self) -> None:
        self.replies = ['Thinking.', 'Still thinking.']
        with self.assertRaises(PipelineRunError) as caught:
            self.judge().run(_PICK)
        self.assertIn('refusing to pick one by position',
                      str(caught.exception))
        self.assertNotIn('pick', self.nodes)
        self.book.record_pick.assert_not_called()
        self.capture.turn.assert_called_once()

    def test_a_candidate_it_was_not_given_is_no_choice(self) -> None:
        self.replies = ['SELECT: impl-z', 'SELECT: impl-z']
        with self.assertRaises(PipelineRunError):
            self.judge().run(_PICK)


class TestTheField(_JudgeTest):
    def test_no_candidate_at_all_stops_the_run(self) -> None:
        self.nodes.pop('impl-a')
        self.nodes.pop('impl-b')
        with self.assertRaises(PipelineRunError) as caught:
            self.judge().run(_PICK)
        self.assertIn('no writer candidates', str(caught.exception))

    def test_one_left_after_a_forfeit_is_uncontested(self) -> None:
        judge = self.judge()
        self.forfeits.forfeited.update({'review-b', 'impl-b'})
        self.forfeits.forfeits['impl-b'] = ('review-b', 4)
        judge.run(_PICK)
        self.driver.create.assert_not_called()
        self.assertEqual(self.pick().kwargs,
                         {'forfeited': (('impl-b', 'review-b', 4),),
                          'uncontested': True})
        self.assertEqual(self.nodes['pick'].selected, 'impl-a')

    def test_a_reduced_field_is_recorded_with_a_real_pick(self) -> None:
        stage = PipelineStage(id='pick', run=('judge',), selects='impl',
                              needs=('impl-a', 'impl-b', 'impl-c'))
        self.nodes['impl-c'] = NodeResult('impl-c', 'writer', branch='c')
        judge = self.judge()
        judge.ctx.stage_by_id['pick'] = stage
        self.forfeits.forfeited.add('impl-c')
        self.forfeits.forfeits['impl-c'] = ('review-c', 2)
        self.replies = ['SELECT: impl-a']
        judge.run(stage)
        self.assertEqual(self.pick().kwargs['forfeited'],
                         (('impl-c', 'review-c', 2),))

    def test_a_candidate_built_without_the_contract_stops_it(self) -> None:
        self.wt.branch_contains.side_effect = (
            lambda run, node, *, ancestor: node != 'impl-b'
        )
        with self.assertRaises(PipelineRunError) as caught:
            self.judge().run(_PICK)
        self.assertIn("cannot compare 'impl-b'", str(caught.exception))
        # Refused BEFORE building the comparison tree.
        self.wt.create_judge_worktree.assert_not_called()

    def test_candidates_cut_from_the_base_are_not_questioned(self) -> None:
        judge = self.judge()
        judge.ctx.stage_by_id['impl-a'] = PipelineStage(
            id='impl-a', run=('a',), write=True,
        )
        judge.ctx.stage_by_id['impl-b'] = PipelineStage(
            id='impl-b', run=('b',), write=True,
        )
        self.replies = ['SELECT: impl-a']
        judge.run(_PICK)
        self.wt.branch_contains.assert_not_called()


class TestTheTurn(_JudgeTest):
    def turn(self) -> str:
        return self.judge().instruction(_PICK, ['impl-a', 'impl-b'])

    def test_it_names_the_candidates_and_the_select_line(self) -> None:
        turn = self.turn()
        self.assertIn('./impl-a, ./impl-b', turn)
        self.assertIn('SELECT: <id>', turn)
        self.assertIn('Task:\nParse port ranges.', turn)

    def test_it_is_told_this_is_its_only_turn(self) -> None:
        self.assertIn('This is your ONLY turn', self.turn())
        self.assertIn('the run HALTS', self.turn())

    def test_reviewed_candidates_are_not_rebuilt(self) -> None:
        self.book.reviewed_ok = {'impl-a', 'impl-b'}
        self.assertIn('Do NOT build or re-run the suite', self.turn())

    def test_the_claim_is_made_only_when_all_were_reviewed(self) -> None:
        self.book.reviewed_ok = {'impl-a'}
        self.assertNotIn('already passed its own review', self.turn())

    def test_in_a_module_staying_inside_it_counts(self) -> None:
        self.book.reviewed_ok = {'impl-a', 'impl-b'}
        self.progress.start_module(Subtask(id='m1', title='t'), first=False)
        self.assertIn('STAYED INSIDE THE MARKED INCREMENT', self.turn())

    def test_outside_a_module_it_does_not(self) -> None:
        self.book.reviewed_ok = {'impl-a', 'impl-b'}
        self.assertNotIn('STAYED INSIDE', self.turn())

    def test_the_retry_shows_the_exact_line(self) -> None:
        retry = retry_instruction(['impl-a', 'impl-b'])
        self.assertIn('SELECT: <id>', retry)
        self.assertIn('one of: impl-a, impl-b', retry)


if __name__ == '__main__':
    unittest.main()
