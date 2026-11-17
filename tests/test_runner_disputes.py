"""Disputes: a stage saying its contract cannot be met stops the run.

Run: python -m unittest tests.test_runner_disputes
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

import click

from sbx_omnigent.pipeline import PipelineStage
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.disputes import Disputes
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.records.results import NodeResult
from tests.support.disputes import NEAR_MISS_REPLY

_DISPUTE = 'DISPUTED: the frozen test at x.py:50 forbids it'

_WRITER = PipelineStage(id='build', run=('b',), write=True)
_READER = PipelineStage(id='spec', run=('r',))
_CHECK = PipelineStage(id='check', run=('c',), checks='tests')
_REVIEW = PipelineStage(id='review', run=('sec',), needs=('build',),
                        gate='consensus', on_block='build')


class _DisputeTest(unittest.TestCase):
    def setUp(self) -> None:
        echo = mock.patch.object(click, 'echo')
        self.echo = echo.start()
        self.addCleanup(echo.stop)
        self.nodes: dict[str, NodeResult] = {}
        self.capture = mock.Mock()
        ctx = RunContext(
            config=mock.Mock(), worktrees=mock.Mock(), sessions=mock.Mock(),
            run_id='r1', nodes=self.nodes, stage_by_id={},
            lock=threading.RLock(), max_rounds=3,
        )
        self.disputes = Disputes(ctx, capture=self.capture)

    def replied(self, stage: PipelineStage, text: str,
                session: str | None = 's1') -> None:
        self.nodes[stage.id] = NodeResult(stage.id, 'writer', output=text,
                                          session=session)

    def said(self) -> str:
        return ' '.join(str(c.args[0]) for c in self.echo.call_args_list)


class TestANearMiss(_DisputeTest):
    def test_it_is_reported_with_the_form_that_works(self) -> None:
        self.disputes.warn_near_miss('build', NEAR_MISS_REPLY)
        self.assertIn('build', self.said())
        self.assertIn('start of a line', self.said().lower())

    def test_a_clean_reply_says_nothing(self) -> None:
        self.disputes.warn_near_miss('build', 'all green')
        self.assertEqual(self.said(), '')

    def test_no_reply_says_nothing(self) -> None:
        self.disputes.warn_near_miss('build', None)
        self.assertEqual(self.said(), '')


class TestAWritersDispute(_DisputeTest):
    def test_it_halts_and_keeps_the_reasoning(self) -> None:
        self.replied(_WRITER, _DISPUTE)
        with self.assertRaises(PipelineRunError) as caught:
            self.disputes.halt_on_writer(_WRITER)
        self.assertIn('the writer raised 1 dispute(s)', str(caught.exception))
        self.assertIn('x.py:50', str(caught.exception))
        self.capture.turn.assert_called_once_with(
            's1', 'disputed its own contract',
        )

    def test_a_near_miss_warns_and_goes_on(self) -> None:
        self.replied(_WRITER, NEAR_MISS_REPLY)
        self.disputes.halt_on_writer(_WRITER)
        self.assertIn('DISPUTED:', self.said())
        self.capture.turn.assert_not_called()

    def test_a_dispute_with_no_session_still_halts(self) -> None:
        self.replied(_WRITER, _DISPUTE, session=None)
        with self.assertRaises(PipelineRunError):
            self.disputes.halt_on_writer(_WRITER)
        self.capture.turn.assert_not_called()

    def test_a_stage_that_has_not_run_passes(self) -> None:
        self.disputes.halt_on_writer(_WRITER)


class TestAReadersDispute(_DisputeTest):
    def test_a_reader_halts_by_name(self) -> None:
        self.replied(_READER, _DISPUTE)
        with self.assertRaises(PipelineRunError) as caught:
            self.disputes.halt_on_reader(_READER)
        self.assertIn('the reader raised', str(caught.exception))

    def test_a_contract_check_halts_by_name(self) -> None:
        self.replied(_CHECK, _DISPUTE)
        with self.assertRaises(PipelineRunError) as caught:
            self.disputes.halt_on_reader(_CHECK)
        self.assertIn('the contract check raised', str(caught.exception))

    def test_a_clean_reader_passes(self) -> None:
        self.replied(_READER, 'the spec is sound')
        self.disputes.halt_on_reader(_READER)


class TestAPlansDisputes(_DisputeTest):
    def test_they_halt_naming_each_check(self) -> None:
        with self.assertRaises(PipelineRunError) as caught:
            self.disputes.halt_on_plan(
                PipelineStage(id='plan', run=('p',)),
                [('plan-check', 'the repo has no src/')],
            )
        self.assertIn('[plan-check] the repo has no src/',
                      str(caught.exception))

    def test_none_pass(self) -> None:
        self.disputes.halt_on_plan(PipelineStage(id='plan', run=('p',)), [])


class TestAReviewersDispute(_DisputeTest):
    def test_a_blocking_reviewers_dispute_halts(self) -> None:
        with self.assertRaises(PipelineRunError) as caught:
            self.disputes.halt_on_review(_REVIEW, {'sec': _DISPUTE}, ['sec'])
        self.assertIn('[sec] the frozen test', str(caught.exception))
        self.assertEqual(self.nodes['review'].verdict, 'BLOCKING')

    def test_an_approving_reviewers_dispute_does_not(self) -> None:
        # One raised beside an approval is an observation.
        self.disputes.halt_on_review(_REVIEW, {'sec': _DISPUTE}, [])
        self.assertNotIn('review', self.nodes)

    def test_a_blocking_reviewers_near_miss_is_reported(self) -> None:
        self.disputes.halt_on_review(
            _REVIEW, {'sec': NEAR_MISS_REPLY}, ['sec'],
        )
        self.assertIn('sec', self.said())
        self.assertIn('DISPUTED:', self.said())

    def test_an_approving_reviewers_near_miss_is_not(self) -> None:
        self.disputes.halt_on_review(_REVIEW, {'sec': NEAR_MISS_REPLY}, [])
        self.assertEqual(self.said(), '')


if __name__ == '__main__':
    unittest.main()
