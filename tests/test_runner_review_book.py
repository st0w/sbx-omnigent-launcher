"""The review book: every vote and judge pick, and each stage's budget.

Run: python -m unittest tests.test_runner_review_book
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

import click

from sbx_omnigent.pipeline import PipelineStage, Subtask
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.records.results import NodeResult
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.review_book import ReviewBook
from sbx_omnigent.swarm_session import SwarmSessionError

_REPORT = (
    'The parser accepts a negative port in src/ports.rs:40 and the '
    'range check is skipped entirely.\n\nVERDICT: BLOCKING'
)


class _BookTest(unittest.TestCase):
    def setUp(self) -> None:
        echo = mock.patch.object(click, 'echo')
        self.echo = echo.start()
        self.addCleanup(echo.stop)
        self.wt = mock.Mock()
        self.sessions = mock.Mock()
        self.sessions.read_transcript.return_value = [
            ('user', 'Review it.'), ('assistant', 'It builds.'),
        ]
        self.progress = RunProgress()
        self.saves = 0
        self.nodes = {
            'build': NodeResult('build', 'writer', branch='pl/r1/build'),
        }
        review = PipelineStage(id='review', run=('sec',), needs=('build',),
                               gate='consensus', on_block='build')
        ctx = RunContext(
            config=mock.Mock(), worktrees=self.wt, sessions=self.sessions,
            run_id='r1', nodes=self.nodes, stage_by_id={'review': review},
            lock=threading.RLock(), max_rounds=3,
        )

        def save() -> None:
            self.saves += 1

        self.book = ReviewBook(ctx, progress=self.progress, save_state=save)

    def vote(self, stage: str = 'review', round_no: int = 1,
             chunk: str | None = None) -> ReviewRecord:
        return ReviewRecord(chunk=chunk, stage=stage, reviewer='sec',
                            round_no=round_no, verdict='APPROVED')


class TestRounds(_BookTest):
    def test_the_first_round_is_one(self) -> None:
        self.assertEqual(self.book.next_round('review'), 1)

    def test_numbering_continues_from_what_was_recorded(self) -> None:
        # Derived from the records, so a resumed run continues it.
        self.book.reviews = [self.vote(round_no=1), self.vote(round_no=2),
                             self.vote(stage='other', round_no=7)]
        self.assertEqual(self.book.next_round('review'), 3)
        self.assertEqual(self.book.next_round('other'), 8)

    def test_a_stage_with_no_votes_starts_at_one(self) -> None:
        self.book.reviews = [self.vote(round_no=2)]
        self.assertEqual(self.book.next_round('other'), 1)

    def test_the_budget_is_charged_per_stage(self) -> None:
        self.book.spend_round('review-a')
        self.assertEqual(self.book.spend_round('review-a'), 2)
        self.assertEqual(self.book.spend_round('review-b'), 1)
        self.assertEqual(self.book.rounds_spent('review-a'), 2)

    def test_a_stage_that_never_spent_has_spent_none(self) -> None:
        self.assertEqual(self.book.rounds_spent('review'), 0)

    def test_the_budget_is_not_the_recorded_number(self) -> None:
        # A resume buys a fresh budget but keeps the numbering.
        self.book.reviews = [self.vote(round_no=4)]
        self.assertEqual(self.book.rounds_spent('review'), 0)
        self.assertEqual(self.book.next_round('review'), 5)


class TestAddingAVote(_BookTest):
    def test_it_is_kept_written_out_and_saved(self) -> None:
        record = self.vote()
        self.book.add(record)
        self.assertEqual(self.book.reviews, [record])
        (args, _kw), = self.wt.write_run_artifact.call_args_list
        self.assertEqual(args[:2], ('r1', f'reviews/{record.slug}.md'))
        self.assertEqual(self.saves, 1)


class TestRecordingAReview(_BookTest):
    def record(self, verdict: str | None, reply: str) -> ReviewRecord:
        self.book.record_review('review', 'sec', 2, verdict, 's1', reply)
        (record,) = self.book.reviews
        return record

    def test_it_keeps_the_whole_conversation(self) -> None:
        record = self.record('APPROVED', 'It builds.\n\nVERDICT: APPROVED')
        self.assertEqual(record.turns, (('user', 'Review it.'),
                                        ('assistant', 'It builds.')))
        self.assertEqual((record.round_no, record.target), (2, 'build'))

    def test_an_unreadable_session_keeps_the_reply(self) -> None:
        self.sessions.read_transcript.side_effect = SwarmSessionError('gone')
        record = self.record('APPROVED', 'It builds.')
        self.assertEqual(record.turns, (('assistant', 'It builds.'),))

    def test_a_blocking_report_is_kept_whole(self) -> None:
        record = self.record('BLOCKING', _REPORT)
        self.assertEqual(record.blocking_report, _REPORT)

    def test_an_approval_keeps_no_blocking_report(self) -> None:
        self.assertEqual(self.record('APPROVED', _REPORT).blocking_report,
                         '')

    def test_a_block_that_names_nothing_is_said(self) -> None:
        self.record('BLOCKING', 'VERDICT: BLOCKING')
        printed = ' '.join(str(c.args[0]) for c in self.echo.call_args_list)
        self.assertIn('the report named nothing', printed)

    def test_it_belongs_to_the_active_module(self) -> None:
        self.progress.start_module(Subtask(id='m1', title='t'), first=False)
        self.assertEqual(self.record('APPROVED', 'ok').chunk, 'm1')

    def test_a_stage_with_no_writer_still_records_the_vote(self) -> None:
        del self.nodes['build']
        self.assertIsNone(self.record('APPROVED', 'ok').target)


class TestRecordingAPick(_BookTest):
    def pick(self, stated: str | None, **kw: object) -> None:
        self.book.record_pick('judge', ['impl-a', 'impl-b'], 'impl-a',
                              stated, 'A is cleaner.', **kw)

    def printed(self) -> str:
        return ' '.join(str(c.args[0]) for c in self.echo.call_args_list)

    def test_it_is_kept_and_written_out(self) -> None:
        self.pick('impl-a')
        (pick,) = self.book.picks
        self.assertEqual((pick.selected, pick.stated), ('impl-a', 'impl-a'))
        (args, _kw), = self.wt.write_run_artifact.call_args_list
        self.assertEqual(args[:2], ('r1', 'judging/judge.md'))

    def test_a_judge_that_named_its_pick_is_reported(self) -> None:
        self.pick('impl-a')
        self.assertIn('selected impl-a from impl-a, impl-b', self.printed())

    def test_a_missing_select_is_called_an_absent_decision(self) -> None:
        self.pick(None)
        self.assertIn('ABSENT decision', self.printed())

    def test_an_uncontested_pick_says_nothing_more(self) -> None:
        self.pick(None, uncontested=True)
        self.assertEqual(self.printed(), '')


class TestTheActiveModulesRecords(_BookTest):
    def test_only_the_active_modules_votes_and_picks(self) -> None:
        self.book.reviews = [self.vote(chunk='m0'), self.vote(chunk='m1')]
        self.progress.start_module(Subtask(id='m1', title='t'), first=False)
        self.book.record_pick('m1-judge', ['a', 'b'], 'a', 'a', '')
        self.assertEqual([r.chunk for r in self.book.module_reviews()],
                         ['m1'])
        self.assertEqual([p.chunk for p in self.book.module_picks()], ['m1'])

    def test_outside_a_campaign_every_record_belongs(self) -> None:
        self.book.reviews = [self.vote()]
        self.assertEqual(len(self.book.module_reviews()), 1)


if __name__ == '__main__':
    unittest.main()
