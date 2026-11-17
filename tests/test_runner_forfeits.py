"""Forfeits: a candidate whose review never agreed is withdrawn.

Run: python -m unittest tests.test_runner_forfeits
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

import click

from sbx_omnigent.pipeline import PipelineStage
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import _Blocked
from sbx_omnigent.runner.forfeits import Forfeits
from sbx_omnigent.runner.records.results import NodeResult

_REVIEW_A = PipelineStage(id='review-a', run=('sec',), needs=('impl-a',),
                          on_block='impl-a')
_REVIEW_B = PipelineStage(id='review-b', run=('sec',), needs=('impl-b',),
                          on_block='impl-b')
_PARALLEL = PipelineStage(id='review', parallel=(_REVIEW_A, _REVIEW_B))
_IMPL_A = PipelineStage(id='impl-a', run=('a',), write=True)
_IMPL_B = PipelineStage(id='impl-b', run=('b',), write=True)
_IMPL_C = PipelineStage(id='impl-c', run=('c',), write=True)
_PICK = PipelineStage(id='pick', run=('judge',),
                      needs=('impl-a', 'impl-b'), selects='impl')


class _ForfeitTest(unittest.TestCase):
    def setUp(self) -> None:
        echo = mock.patch.object(click, 'echo')
        self.echo = echo.start()
        self.addCleanup(echo.stop)
        self.nodes: dict[str, NodeResult] = {}
        self.stages = {s.id: s for s in (_REVIEW_A, _REVIEW_B, _IMPL_A,
                                         _IMPL_B, _IMPL_C, _PICK)}
        ctx = RunContext(
            config=mock.Mock(), worktrees=mock.Mock(), sessions=mock.Mock(),
            run_id='r1', nodes=self.nodes, stage_by_id=self.stages,
            lock=threading.RLock(), max_rounds=3,
        )
        self.forfeits = Forfeits(ctx)

    def reviewed(self, review: str, verdict: str | None = None) -> None:
        self.nodes[review] = NodeResult(review, 'review', verdict=verdict)


class TestABlockedReviewInAParallelGroup(_ForfeitTest):
    """
    A review that never reaches consensus withdraws ITS candidate.

    Two writers are run so a judge can choose between them on evidence.
    A blocked review used to raise out of the whole run, so a campaign
    could end with one candidate approved by every reviewer,
    gate-verified and unpublished, because the other could not converge.
    Seen twice in live campaigns.
    """

    def test_a_cleared_sibling_lets_the_run_continue(self) -> None:
        self.reviewed('review-a')
        self.reviewed('review-b', 'BLOCKING')
        proceed = self.forfeits.forfeit_blocked_reviews(
            _PARALLEL, [_Blocked('review-b', 4)],
        )
        self.assertTrue(proceed)
        # The blocked review AND the candidate it was vetting withdraw.
        self.assertEqual(self.forfeits.forfeited, {'review-b', 'impl-b'})
        # And the selection can say so, with the rounds it took (#47).
        self.assertEqual(self.forfeits.forfeits,
                         {'impl-b': ('review-b', 4)})

    def test_no_cleared_sibling_still_stops_the_run(self) -> None:
        # Nothing was vetted, so there is nothing to judge.
        self.reviewed('review-a', 'BLOCKING')
        self.reviewed('review-b', 'BLOCKING')
        proceed = self.forfeits.forfeit_blocked_reviews(
            _PARALLEL, [_Blocked('review-a', 4), _Blocked('review-b', 4)],
        )
        self.assertFalse(proceed)
        self.assertEqual(self.forfeits.forfeited, set())

    def test_a_sibling_that_never_reviewed_has_not_cleared(self) -> None:
        proceed = self.forfeits.forfeit_blocked_reviews(
            _PARALLEL, [_Blocked('review-b', 4)],
        )
        self.assertFalse(proceed)


class TestABlockedTopLevelReview(_ForfeitTest):
    """Both shipped race examples declare their reviews top-level, so a
    candidate that could not converge ended the whole run (#47)."""

    def test_a_judge_with_a_rival_keeps_the_run_going(self) -> None:
        proceed = self.forfeits.forfeit_to_a_judge(
            _REVIEW_B, _Blocked('review-b', 3),
        )
        self.assertTrue(proceed)
        self.assertEqual(self.forfeits.forfeited, {'review-b', 'impl-b'})
        self.assertEqual(self.forfeits.forfeits,
                         {'impl-b': ('review-b', 3)})
        said = str(self.echo.call_args.args[0])
        self.assertIn('The judge still has impl-a', said)

    def test_the_last_candidate_is_never_withdrawn(self) -> None:
        # The review that would withdraw it finds no rival and blocks.
        self.forfeits.forfeit_to_a_judge(_REVIEW_B, _Blocked('review-b', 3))
        proceed = self.forfeits.forfeit_to_a_judge(
            _REVIEW_A, _Blocked('review-a', 3),
        )
        self.assertFalse(proceed)
        self.assertNotIn('impl-a', self.forfeits.forfeited)

    def test_a_review_with_no_writer_to_withdraw_blocks(self) -> None:
        review = PipelineStage(id='review', run=('sec',), gate='consensus')
        self.assertFalse(
            self.forfeits.forfeit_to_a_judge(review, _Blocked('review', 3)),
        )

    def test_a_writer_no_judge_compares_has_no_rival(self) -> None:
        review = PipelineStage(id='review-c', run=('sec',),
                               needs=('impl-c',), on_block='impl-c')
        self.assertFalse(
            self.forfeits.forfeit_to_a_judge(review, _Blocked('review-c', 3)),
        )

    def test_a_rival_whose_review_has_not_run_still_counts(self) -> None:
        # Read from the declared pipeline, not from what has run.
        self.assertEqual(self.forfeits.rival_candidates('impl-b'), ['impl-a'])


class TestWhatAJudgeCompares(_ForfeitTest):
    def built(self, *writers: str) -> None:
        for writer in writers:
            self.nodes[writer] = NodeResult(writer, 'writer',
                                            branch=f'pl/{writer}')

    def test_every_built_candidate_is_compared(self) -> None:
        self.built('impl-a', 'impl-b')
        self.assertEqual(self.forfeits.judge_candidates(_PICK),
                         ['impl-a', 'impl-b'])

    def test_a_forfeited_candidate_is_not_judged(self) -> None:
        # Judging a forfeited branch would compare a vetted candidate
        # against an unvetted one and call the result a choice.
        self.built('impl-a', 'impl-b')
        self.forfeits.forfeited.add('impl-b')
        self.assertEqual(self.forfeits.judge_candidates(_PICK), ['impl-a'])

    def test_a_need_with_no_branch_is_not_a_candidate(self) -> None:
        self.built('impl-a')
        self.nodes['impl-b'] = NodeResult('impl-b', 'writer')
        self.assertEqual(self.forfeits.judge_candidates(_PICK), ['impl-a'])

    def test_a_need_that_is_not_a_writer_is_not_a_candidate(self) -> None:
        self.built('impl-a')
        self.nodes['impl-b'] = NodeResult('impl-b', 'reader', branch='x')
        self.assertEqual(self.forfeits.judge_candidates(_PICK), ['impl-a'])


if __name__ == '__main__':
    unittest.main()
