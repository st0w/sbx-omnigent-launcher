"""What a run records about judgments and reviews.

Run: python -m unittest tests.test_runner_records
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.records.judging import JudgePick
from sbx_omnigent.runner.records.reviews import ReviewRecord


class TestJudgePickRecordsTheField(unittest.TestCase):
    def _pick(self, **kw):
        base = {
            'chunk': None, 'stage': 'pick', 'candidates': ('impl-b',),
            'selected': 'impl-b', 'stated': None,
        }
        return JudgePick(**{**base, **kw})

    def test_forfeits_and_uncontested_survive_a_round_trip(self) -> None:
        pick = self._pick(
            forfeited=(('impl-a', 'review-a', 3),), uncontested=True
        )
        self.assertEqual(JudgePick.from_dict(pick.as_dict()), pick)

    def test_a_pick_from_older_state_is_contested_with_no_forfeits(
        self,
    ) -> None:
        raw = self._pick(stated='impl-b').as_dict()
        del raw['forfeited'], raw['uncontested']
        pick = JudgePick.from_dict(raw)
        self.assertEqual(pick.forfeited, ())
        self.assertFalse(pick.uncontested)

    def test_a_malformed_forfeit_is_dropped(self) -> None:
        raw = self._pick().as_dict()
        raw['forfeited'] = [['impl-a', 'review-a', 'three'], 'junk',
                            ['impl-c', 'review-c', 2]]
        pick = JudgePick.from_dict(raw)
        self.assertEqual(pick.forfeited, (('impl-c', 'review-c', 2),))


class TestReviewRecordCarriesTheSections(unittest.TestCase):
    """
    The sections survive a resume, and their absence is not a failure.

    State written before the split has no section keys. It must restore
    as "uncategorized" rather than refusing to load — that is the whole
    reason RUN_STATE_VERSION does not move for this change, and a run
    was in flight when it landed.
    """

    def test_the_sections_round_trip_through_state(self) -> None:
        rec = ReviewRecord(
            chunk='m1', stage='review-a', reviewer='sec', round_no=1,
            verdict='APPROVED', findings=('d', 'l', 'p'),
            defects=('d',), later_increment=('l',), premises=('p',),
        )
        back = ReviewRecord.from_dict(rec.as_dict())
        self.assertEqual(back.defects, ('d',))
        self.assertEqual(back.later_increment, ('l',))
        self.assertEqual(back.premises, ('p',))
        self.assertEqual(back.findings, ('d', 'l', 'p'))

    def test_state_without_the_section_keys_still_loads(self) -> None:
        raw = {
            'chunk': 'm0b', 'stage': 'review-a', 'reviewer': 'sec',
            'round_no': 1, 'verdict': 'APPROVED',
            'findings': ['raised before the split'], 'turns': [],
        }
        back = ReviewRecord.from_dict(raw)
        self.assertEqual(back.findings, ('raised before the split',))
        self.assertEqual(back.defects, ())
        self.assertEqual(back.later_increment, ())
        self.assertEqual(back.premises, ())

    def test_a_malformed_section_entry_is_dropped_not_fatal(self) -> None:
        raw = {
            'stage': 'review-a', 'reviewer': 'sec', 'round_no': 1,
            'verdict': 'APPROVED', 'turns': [],
            'defects': ['keep', 7, None, 'also keep'],
        }
        self.assertEqual(
            ReviewRecord.from_dict(raw).defects, ('keep', 'also keep')
        )


if __name__ == '__main__':
    unittest.main()
