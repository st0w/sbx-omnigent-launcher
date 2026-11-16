"""The record of every reviewer's vote and report.

Run: python -m unittest tests.test_runner_render_reviews
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.render.reviews import render_review_records


class TestRenderReviewRecords(unittest.TestCase):
    def _rec(self, **kw):
        base = {
            'chunk': None, 'stage': 'review', 'reviewer': 'sec',
            'round_no': 1, 'verdict': 'APPROVED',
            'turns': (('assistant', 'It builds.'),),
        }
        return ReviewRecord(**{**base, **kw})

    def test_no_records_renders_nothing(self) -> None:
        self.assertEqual(render_review_records([], title='T'), '')

    def test_the_roster_lists_every_vote(self) -> None:
        doc = render_review_records(
            [self._rec(), self._rec(reviewer='bugs', verdict='BLOCKING')],
            title='T',
        )
        self.assertIn('| `review-sec` | 1 | APPROVED |', doc)
        self.assertIn('| `review-bugs` | 1 | BLOCKING |', doc)

    def test_a_silent_reviewer_is_recorded_as_such(self) -> None:
        # Not blank: "none stated" is itself the finding.
        doc = render_review_records([self._rec(verdict=None)], title='T')
        self.assertIn('none stated', doc)
        self.assertIn('no verdict stated', doc)

    def test_an_unreadable_session_says_so(self) -> None:
        doc = render_review_records([self._rec(turns=())], title='T')
        self.assertIn('could not be read', doc)

    def test_the_slug_keeps_rounds_apart(self) -> None:
        # The stage id already carries the module in a campaign, so the
        # record must not prefix it again ("m1-m1-review-sec").
        self.assertEqual(
            self._rec(chunk='m1', stage='m1-review', round_no=2).slug,
            'm1-review-sec-r2',
        )


if __name__ == '__main__':
    unittest.main()
