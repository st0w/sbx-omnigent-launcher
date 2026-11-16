"""Triage: what the verifier is asked about, and what it concluded.

Run: python -m unittest tests.test_runner_triage
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.parsing.dispositions import (
    DISPOSITION_ABSENT,
    DISPOSITION_FILED,
    Disposition,
)
from sbx_omnigent.runner.prompts.triage import blocking_block, findings_block
from sbx_omnigent.runner.records.findings import (
    blocking_finding_id,
    finding_id,
)
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.triage import FindingTriage

_REPORT = (
    'The parser accepts a negative port in src/ports.rs:40 and the '
    'range check is skipped entirely.\n\nVERDICT: BLOCKING'
)


def _vote(reviewer: str = 'sec', **kw: object) -> ReviewRecord:
    fields: dict[str, object] = {
        'chunk': 'm0', 'stage': 'm0-review', 'reviewer': reviewer,
        'round_no': 1, 'verdict': 'APPROVED', 'target': 'm0-impl-a',
    }
    fields.update(kw)
    return ReviewRecord(**fields)


class _TriageTest(unittest.TestCase):
    def triage(self, *reviews: ReviewRecord) -> FindingTriage:
        return FindingTriage(chunk_reviews=lambda: list(reviews))


class TestWhatTheVerifierIsAsked(_TriageTest):
    def test_each_filed_finding_is_pending_in_review_order(self) -> None:
        first = _vote('sec', findings=('a', 'b'), defects=('a', 'b'))
        second = _vote('bugs', findings=('c',), defects=('c',))
        pending = self.triage(first, second).pending_findings()
        self.assertEqual(
            [(rec.reviewer, i, text) for rec, i, text in pending],
            [('sec', 1, 'a'), ('sec', 2, 'b'), ('bugs', 1, 'c')],
        )

    def test_a_referral_is_not_asked_about(self) -> None:
        # Already routed to the decisions ledger: it is about another
        # module's code.
        rec = _vote(findings=('d', 'later'), defects=('d',),
                    later_increment=('later',))
        pending = self.triage(rec).pending_findings()
        self.assertEqual([text for _r, _i, text in pending], ['d'])

    def test_every_blocking_report_is_pending(self) -> None:
        loser = _vote('sec', verdict='BLOCKING', blocking_report=_REPORT)
        winner = _vote('bugs', verdict='BLOCKING', blocking_report=_REPORT,
                       target='m0-impl-b')
        reports = self.triage(loser, winner).pending_blocking_reports()
        self.assertEqual([rec.reviewer for rec, _t in reports],
                         ['sec', 'bugs'])

    def test_a_report_that_names_nothing_is_left_out(self) -> None:
        empty = _vote(verdict='BLOCKING', blocking_report='VERDICT: BLOCKING')
        self.assertEqual(self.triage(empty).pending_blocking_reports(), [])


class TestRecordingConclusions(_TriageTest):
    def test_a_conclusion_is_kept_by_the_findings_id(self) -> None:
        rec = _vote(findings=('a', 'b'), defects=('a', 'b'))
        triage = self.triage(rec)
        pending = triage.pending_findings()
        gone = Disposition(DISPOSITION_ABSENT, 'checked ports.rs')
        triage.record_dispositions(pending, {2: gone})
        self.assertEqual(triage.dispositions, {finding_id(rec, 2): gone})

    def test_a_finding_without_a_conclusion_still_files(self) -> None:
        rec = _vote(findings=('a', 'b', 'c'), defects=('a', 'b', 'c'))
        triage = self.triage(rec)
        kept = triage.record_dispositions(triage.pending_findings(), {
            1: Disposition(DISPOSITION_FILED, 'real'),
            2: Disposition(DISPOSITION_ABSENT, 'gone'),
        })
        self.assertEqual(kept, 2)

    def test_a_blocking_claim_is_kept_with_its_restatement(self) -> None:
        rec = _vote(verdict='BLOCKING', blocking_report=_REPORT)
        triage = self.triage(rec)
        real = Disposition(DISPOSITION_FILED, 'negative ports pass')
        kept = triage.record_blocking_dispositions(
            triage.pending_blocking_reports(), {(1, 1): real},
        )
        ident = blocking_finding_id(rec, 1)
        self.assertEqual(kept, 1)
        self.assertEqual(triage.dispositions, {ident: real})
        self.assertEqual(triage.blocking_claims,
                         {ident: 'negative ports pass'})

    def test_a_claim_on_a_report_that_was_never_shown_is_dropped(self) -> None:
        rec = _vote(verdict='BLOCKING', blocking_report=_REPORT)
        triage = self.triage(rec)
        kept = triage.record_blocking_dispositions(
            triage.pending_blocking_reports(),
            {(7, 1): Disposition(DISPOSITION_FILED, 'invented')},
        )
        self.assertEqual((kept, triage.dispositions), (0, {}))

    def test_a_votes_claims_come_back_in_claim_order(self) -> None:
        rec = _vote(verdict='BLOCKING', blocking_report=_REPORT)
        other = _vote('bugs', verdict='BLOCKING', blocking_report=_REPORT)
        triage = self.triage(rec, other)
        triage.record_blocking_dispositions(
            triage.pending_blocking_reports(),
            {(1, 10): Disposition(DISPOSITION_FILED, 'tenth'),
             (1, 2): Disposition(DISPOSITION_FILED, 'second'),
             (2, 1): Disposition(DISPOSITION_FILED, 'not this vote')},
        )
        self.assertEqual(triage.blocking_claims_for(rec), [
            (blocking_finding_id(rec, 2), 'second'),
            (blocking_finding_id(rec, 10), 'tenth'),
        ])


class TestTheVerifiersTurn(unittest.TestCase):
    def test_findings_are_numbered_with_where_they_came_from(self) -> None:
        rec = _vote(round_no=2, findings=('a',), defects=('a',))
        block = findings_block([(rec, 1, 'ports are unchecked')])
        self.assertIn(
            '1. [raised by sec in round 2 against m0-impl-a] '
            'ports are unchecked', block,
        )
        self.assertIn('`DISPOSITIONS:`', block)

    def test_reports_are_numbered_for_per_claim_answers(self) -> None:
        rec = _vote(verdict='BLOCKING')
        block = blocking_block([(rec, _REPORT)])
        self.assertIn('Report 1 — `sec` blocked `m0-impl-a` in round 1',
                      block)
        self.assertIn('`BLOCKING-DISPOSITIONS:`', block)


if __name__ == '__main__':
    unittest.main()
