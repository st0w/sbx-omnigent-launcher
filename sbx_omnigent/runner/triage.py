"""Triage: the verifier's check of what reviewers raised.

Before anything reaches the tracker, a verifier checks each finding and
each blocking claim against the code that is about to ship. This holds
what it was asked and what it concluded; filing reads the conclusions.
"""

from __future__ import annotations

from collections.abc import Callable

from sbx_omnigent.runner.parsing.dispositions import Disposition
from sbx_omnigent.runner.parsing.findings import report_names_nothing
from sbx_omnigent.runner.records.findings import (
    _blocking_id_prefix,
    blocking_finding_id,
    finding_id,
)
from sbx_omnigent.runner.records.reviews import ReviewRecord


class FindingTriage:
    """
    What the verifier is asked about a module, and what it concluded.

    :param chunk_reviews: The current module's review records, in
        review order.

    :ivar dispositions: A verifier's conclusion per finding, by
        positional id. Absent means "no conclusion", which files the
        finding — the gate's fail-open rule lives in that absence rather
        than in a flag.
    :ivar blocking_claims: Blocking-claim id -> the claim as the
        verifier restated it. Held beside the dispositions because a
        blocking claim has no text of its own anywhere else: the
        reviewer wrote a report, and the verifier is what turns it into
        one sentence.
    """

    def __init__(
        self, *, chunk_reviews: Callable[[], list[ReviewRecord]]
    ) -> None:
        self._chunk_reviews = chunk_reviews
        self.dispositions: dict[str, Disposition] = {}
        self.blocking_claims: dict[str, str] = {}

    def pending_findings(self) -> list[tuple[ReviewRecord, int, str]]:
        """
        What this chunk's reviewers raised that would be filed.

        Referrals are already routed to the decisions ledger, so the
        verifier is not asked about them: it would be checking an
        observation against the wrong module's code.

        :returns: ``(record, index, text)``, in review order.
        """
        return [
            (rec, index, text)
            for rec in self._chunk_reviews()
            for index, text in rec.filed_findings()
        ]

    def pending_blocking_reports(
        self,
    ) -> list[tuple[ReviewRecord, str]]:
        """
        The reports behind this chunk's BLOCKING votes.

        EVERY candidate, not only the one that lost. A blocking finding
        is assumed closed by the loop-back fix turn, which is true of
        the branch that was blocked and says nothing about its sibling —
        and the winner's own earlier blocks are worth re-checking too,
        because a fix turn that made the symptom go away rather than
        the cause looks identical from here.

        A report that named nothing is left out: there is no claim in
        it for a verifier to check, and it was already reported when
        the vote was recorded.

        :returns: ``(record, report)`` in review order.
        """
        return [
            (rec, rec.blocking_report)
            for rec in self._chunk_reviews()
            if rec.blocking_report
            and not report_names_nothing(rec.blocking_report)
        ]

    def record_dispositions(
        self,
        pending: list[tuple[ReviewRecord, int, str]],
        verdicts: dict[int, Disposition],
    ) -> int:
        """
        Store one conclusion per finding, keyed by its positional id.

        :param pending: What the verifier was shown, in the order shown.
        :param verdicts: Its conclusions, by presented position.
        :returns: How many findings still file.
        """
        kept = 0
        for position, (rec, index, _text) in enumerate(pending, start=1):
            verdict = verdicts.get(position)
            if verdict is None or verdict.files:
                kept += 1
            if verdict is not None:
                self.dispositions[finding_id(rec, index)] = verdict
        return kept

    def record_blocking_dispositions(
        self,
        reports: list[tuple[ReviewRecord, str]],
        verdicts: dict[tuple[int, int], Disposition],
    ) -> int:
        """
        Store one conclusion per blocking claim the verifier named.

        Unlike :meth:`record_dispositions`, a claim with no conclusion
        is NOT recorded and never files: the claims exist only because
        the verifier enumerated them, so "no conclusion" means it never
        found one to enumerate. The reports it skipped are surfaced as
        a single issue at publish instead.

        :param reports: What the verifier was shown, in the order shown.
        :param verdicts: Its conclusions, by ``(report, claim)``.
        :returns: How many claims still reproduce against this code.
        """
        kept = 0
        by_report: dict[int, ReviewRecord] = {
            position: rec
            for position, (rec, _text) in enumerate(reports, start=1)
        }
        for (report_no, claim_no), verdict in sorted(verdicts.items()):
            rec = by_report.get(report_no)
            if rec is None:
                # A number the verifier invented. Dropping it is right:
                # it names no report anybody can attribute it to.
                continue
            ident = blocking_finding_id(rec, claim_no)
            self.dispositions[ident] = verdict
            self.blocking_claims[ident] = verdict.reason
            if verdict.files:
                kept += 1
        return kept

    def blocking_claims_for(
        self, rec: ReviewRecord
    ) -> list[tuple[str, str]]:
        """
        The verified claims from one blocking vote, in claim order.

        :param rec: The vote to collect for.
        :returns: ``(id, claim)`` pairs, empty when the verifier
            concluded nothing about this report.
        """
        prefix = _blocking_id_prefix(rec)
        found: list[tuple[int, str, str]] = []
        for ident, claim in self.blocking_claims.items():
            if not ident.startswith(prefix):
                continue
            tail = ident[len(prefix):]
            found.append((int(tail) if tail.isdigit() else 0, ident, claim))
        return [(ident, claim) for _n, ident, claim in sorted(found)]
