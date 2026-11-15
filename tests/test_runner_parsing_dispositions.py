"""The verifier's conclusion on each finding it checked.

Run: python -m unittest tests.test_runner_parsing_dispositions
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.parsing.dispositions import (
    DISPOSITION_ABSENT,
    DISPOSITION_DUPLICATE,
    DISPOSITION_FILED,
    DISPOSITION_RECORDED,
    parse_blocking_dispositions,
    parse_dispositions,
)


class TestParseDispositions(unittest.TestCase):
    """
    A verifier's conclusions, and the rule that it FAILS OPEN.

    Everything here protects one property: an index this parser does not
    return a conclusion for is filed exactly as it is today. A verifier
    that dies, answers nothing, or writes something unreadable costs a
    wasted triage — never a lost finding.
    """

    def test_each_conclusion_is_read_with_its_reason(self) -> None:
        got = parse_dispositions(
            'DISPOSITIONS:\n'
            '- 1: reproduces — still leaked at pool.py:88\n'
            '- 2: absent — removed in 4622247\n'
        )
        self.assertEqual(got[1].verdict, DISPOSITION_FILED)
        self.assertEqual(got[1].reason, 'still leaked at pool.py:88')
        self.assertEqual(got[2].verdict, DISPOSITION_ABSENT)
        self.assertFalse(got[2].files)

    def test_only_reproduces_still_files(self) -> None:
        got = parse_dispositions(
            'DISPOSITIONS:\n- 1: reproduces\n- 2: absent\n'
            '- 3: recorded\n- 4: decided\n- 5: duplicate\n'
        )
        self.assertEqual(
            [i for i in sorted(got) if got[i].files], [1]
        )

    def test_the_verdict_may_sit_inside_a_sentence(self) -> None:
        # A verifier writes a conclusion, not a token. "already
        # documented in models.py" carries its verdict in word two.
        got = parse_dispositions(
            'DISPOSITIONS:\n'
            '- 1: already documented in models.py\n'
            '- 2: duplicate of 1\n'
        )
        self.assertEqual(got[1].verdict, DISPOSITION_RECORDED)
        self.assertEqual(got[1].reason, 'in models.py')
        self.assertEqual(got[2].verdict, DISPOSITION_DUPLICATE)

    def test_an_unreadable_line_does_not_discard_the_ones_below_it(
        self,
    ) -> None:
        # The expensive failure: one line a verifier phrased oddly
        # silently swallowing every conclusion after it.
        got = parse_dispositions(
            'DISPOSITIONS:\n'
            '- 1: reproduces\n'
            '- 2: I could not tell\n'
            '- 3: absent — checked, it is gone\n'
        )
        self.assertNotIn(2, got)
        self.assertEqual(got[3].verdict, DISPOSITION_ABSENT)

    def test_no_block_at_all_yields_no_conclusions(self) -> None:
        # Which files everything — the pre-gate behaviour exactly.
        for text in ('', None, 'I ran out of turns.'):
            with self.subTest(text=text):
                self.assertEqual(parse_dispositions(text), {})

    def test_the_last_block_wins_like_every_other_marker(self) -> None:
        got = parse_dispositions(
            'DISPOSITIONS:\n- 1: absent\n\n'
            'On reflection.\n\nDISPOSITIONS:\n- 1: reproduces\n'
        )
        self.assertTrue(got[1].files)


class TestParseBlockingDispositions(unittest.TestCase):
    """The verifier answers per claim, under its own header."""

    def test_claims_are_read_by_report_and_number(self) -> None:
        got = parse_blocking_dispositions(
            'BLOCKING-DISPOSITIONS:\n'
            '- B1.1: reproduces — repr prints the key in the winner\n'
            '- B1.2: absent — the winner coerces the timestamp\n'
        )
        self.assertEqual(got[(1, 1)].verdict, DISPOSITION_FILED)
        self.assertIn('repr prints the key', got[(1, 1)].reason)
        self.assertEqual(got[(1, 2)].verdict, DISPOSITION_ABSENT)

    def test_the_B_prefix_is_optional(self) -> None:
        got = parse_blocking_dispositions(
            'BLOCKING-DISPOSITIONS:\n- 2.1: reproduces — still there\n'
        )
        self.assertEqual(got[(2, 1)].verdict, DISPOSITION_FILED)

    def test_no_header_yields_nothing(self) -> None:
        self.assertEqual(parse_blocking_dispositions('nothing here'), {})

    def test_an_unreadable_verdict_is_skipped_not_guessed(self) -> None:
        got = parse_blocking_dispositions(
            'BLOCKING-DISPOSITIONS:\n'
            '- B1.1: mumble — no idea\n'
            '- B1.2: reproduces — still there\n'
        )
        self.assertNotIn((1, 1), got)
        self.assertEqual(got[(1, 2)].verdict, DISPOSITION_FILED)


if __name__ == '__main__':
    unittest.main()
