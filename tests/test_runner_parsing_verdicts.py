"""Verdicts, selections, finish markers and disputes in replies.

Run: python -m unittest tests.test_runner_parsing_verdicts
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.parsing.verdicts import (
    parse_disputes,
    parse_select,
    parse_verdict,
    work_finished,
)

_PS_CANDIDATES = ['m0-impl-a', 'm0-impl-b']


class TestParseDisputes(unittest.TestCase):
    """
    Telling "my contract is impossible" from the prompt asking for it.

    Every role prompt ends "label it DISPUTED, and stop", so the bare
    word is in nearly every report. Treating that as a dispute would
    halt almost every run; measured on four campaigns, both that
    SHIPPED contained the echo and no real claim.
    """

    def test_the_prompt_echo_is_not_a_dispute(self) -> None:
        # The exact phrase every role prompt ends with. If this reads as
        # a dispute, the runner halts on every review it ever sees.
        echo = (
            'If it truly cannot be resolved that way, say so in your '
            'reply, label it DISPUTED, and stop. Your reply is the only '
            'channel that reaches a human.'
        )
        self.assertEqual(parse_disputes(echo), ())

        # The same phrase WRAPPED so it begins a line. Only the required
        # `:`/dash separator rejects this one — the start-of-line anchor
        # does not — and reports do quote the prompt back at any width.
        wrapped = (
            'If it truly cannot be resolved that way, say so and\n'
            'DISPUTED, and stop. Your reply is the only channel.\n'
        )
        self.assertEqual(parse_disputes(wrapped), ())

    def test_a_claim_after_the_marker_is_a_dispute(self) -> None:
        got = parse_disputes(
            'DISPUTED: the frozen mapping test conflicts with the '
            'vocabulary, so this cannot be approved.'
        )
        self.assertEqual(len(got), 1)
        self.assertTrue(got[0].startswith('the frozen mapping test'))

    def test_the_marker_may_be_decorated_like_a_finding(self) -> None:
        for text in (
            '**DISPUTED — the frozen test is internally inconsistent.**',
            '1. **DISPUTED — the suite is unsatisfiable.** Details follow.',
            '- DISPUTED: stages disagree on a contract.',
            'DISPUTED - no catalog content can satisfy it.',
        ):
            with self.subTest(text=text[:34]):
                self.assertEqual(len(parse_disputes(text)), 1)

    def test_a_report_with_no_marker_yields_nothing(self) -> None:
        for text in ('', None, 'VERDICT: BLOCKING', 'nothing disputed'):
            with self.subTest(text=text):
                self.assertEqual(parse_disputes(text), ())

    def test_every_dispute_in_a_multi_finding_report_is_returned(
        self,
    ) -> None:
        got = parse_disputes(
            'DISPUTED: the first assertion cannot hold.\n'
            'Some prose in between.\n'
            'DISPUTED — the second one cannot either.\n'
        )
        self.assertEqual(len(got), 2)


class TestParseSelect(unittest.TestCase):
    """Unit coverage for the two-tier parser."""

    C = _PS_CANDIDATES

    def test_tier_one_exact_form(self) -> None:
        self.assertEqual(
            parse_select('SELECT: m0-impl-b', self.C), 'm0-impl-b'
        )

    def test_tier_two_needs_the_marker_AND_a_known_id(self) -> None:
        self.assertEqual(
            parse_select('SELECT m0-impl-b', self.C), 'm0-impl-b'
        )
        self.assertIsNone(parse_select('m0-impl-b is better', self.C))
        self.assertIsNone(parse_select('SELECT the better one', self.C))

    def test_a_tier_one_capture_that_is_not_a_candidate_falls_through(
        self,
    ) -> None:
        # The regex eats the next word of a sentence; a real choice
        # further down must still win.
        self.assertEqual(
            parse_select(
                'SELECT: whichever is safer\n\nSELECT m0-impl-b', self.C
            ),
            'm0-impl-b',
        )

    def test_a_substring_id_never_shadows_the_longer_one(self) -> None:
        self.assertEqual(
            parse_select('SELECT m0-impl-a', ['impl-a', 'm0-impl-a']),
            'm0-impl-a',
        )

    def test_the_last_select_line_wins(self) -> None:
        self.assertEqual(
            parse_select(
                'SELECT m0-impl-a\nno wait\nSELECT m0-impl-b', self.C
            ),
            'm0-impl-b',
        )

    def test_without_candidates_the_old_behaviour_is_exact(self) -> None:
        # Callers that pass no candidate list get tier 1, unvalidated.
        self.assertEqual(parse_select('SELECT: anything'), 'anything')
        self.assertIsNone(parse_select('SELECT anything'))


class TestTheFinishLineIsRead(unittest.TestCase):
    """Only a line that is exactly the marker counts."""

    def test_the_line_counts(self) -> None:
        for text in ('done\n\nWORK COMPLETE', 'done\n**WORK COMPLETE**',
                     'done\n`WORK COMPLETE`\n'):
            with self.subTest(text=text):
                self.assertTrue(work_finished(text))

    def test_a_promise_does_not(self) -> None:
        for text in (
            'I will end with WORK COMPLETE once the build is done.',
            'WORK COMPLETE is what I will say later',
            '', None,
        ):
            with self.subTest(text=text):
                self.assertFalse(work_finished(text))


class TestParseVerdictAndSelect(unittest.TestCase):
    def test_parse_verdict_last_wins(self) -> None:
        self.assertEqual(
            parse_verdict('VERDICT: BLOCKING\n...\nVERDICT: APPROVED'),
            'APPROVED',
        )
        self.assertIsNone(parse_verdict('no verdict here'))

    def test_parse_verdict_approve_synonyms(self) -> None:
        # Models paraphrase; a real reviewer ended with "VERDICT: PASS".
        for text in (
            'VERDICT: PASS',
            'VERDICT: passed',
            'VERDICT: Approve',
            'VERDICT: LGTM',
            'VERDICT: ok',
        ):
            self.assertEqual(parse_verdict(text), 'APPROVED', text)

    def test_parse_verdict_block_synonyms(self) -> None:
        for text in (
            'VERDICT: FAIL',
            'VERDICT: blocked',
            'VERDICT: Reject',
        ):
            self.assertEqual(parse_verdict(text), 'BLOCKING', text)

    def test_parse_verdict_unknown_token_is_none(self) -> None:
        self.assertIsNone(parse_verdict('VERDICT: maybe'))

    def test_parse_verdict_unknown_does_not_clobber(self) -> None:
        # A stray later 'verdict:' mention must not override a real one.
        self.assertEqual(
            parse_verdict('VERDICT: PASS\nmy earlier verdict: unsure'),
            'APPROVED',
        )

    def test_parse_select(self) -> None:
        self.assertEqual(parse_select('reasons\nSELECT: impl-b'), 'impl-b')
        self.assertIsNone(parse_select('undecided'))


if __name__ == '__main__':
    unittest.main()
