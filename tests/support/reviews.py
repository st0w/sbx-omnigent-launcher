"""Reviewer votes and environment limits, for findings and PR tests."""

from __future__ import annotations

from collections.abc import Sequence

from sbx_omnigent.runner.records.reviews import ReviewRecord

#: Checks a sandbox stopped, as a reviewer reports them.
AUDIT_LIMIT = 'cargo audit: api.osv.dev was refused by the sandbox'
SCAN_LIMIT = 'trivy: its database download from ghcr.io was refused'


def review_record(
    reviewer: str = 'bugs',
    target: str = 'impl-a',
    round_no: int = 1,
    findings: Sequence[str] = (),
    chunk: str = 'topology',
    verdict: str | None = 'APPROVED',
) -> ReviewRecord:
    """One reviewer's vote, for tests of what is filed and published."""
    return ReviewRecord(
        chunk=chunk, stage=f'{chunk}-review-a', reviewer=reviewer,
        round_no=round_no, verdict=verdict, findings=tuple(findings),
        target=target,
    )
