"""A finding's identity, its tracker marker, and its outcome text."""

from __future__ import annotations

from sbx_omnigent.runner.parsing.dispositions import Disposition
from sbx_omnigent.runner.records.reviews import ReviewRecord


def finding_id(rec: ReviewRecord, index: int) -> str:
    """
    A stable, positional id for one finding.

    Positional rather than content-derived, deliberately. The same issue
    is often raised by both reviewers and re-raised each round, and
    keying on text would silently merge those — but a re-raise after a
    round of work is a DIFFERENT event, and dropping it is worse than
    carrying the duplicate. Duplicates here are provenance; a human
    merges them, the runner never does.

    :param rec: The vote the finding came from.
    :param index: 1-based position within that reviewer's list.
    :returns: e.g. ``topology/topology-impl-a/bugs/r4#3``.
    """
    return (
        f'{rec.chunk or "-"}/{rec.target or "-"}/'
        f'{rec.reviewer}/r{rec.round_no}#{index}'
    )


def _blocking_id_prefix(rec: ReviewRecord) -> str:
    """The id every blocking claim from one vote shares."""
    return (
        f'{rec.chunk or "-"}/{rec.target or "-"}/'
        f'{rec.reviewer}/r{rec.round_no}#B'
    )


def blocking_finding_id(rec: ReviewRecord, claim: int) -> str:
    """
    A stable, positional id for one BLOCKING claim.

    Shaped like :func:`finding_id` and deliberately distinct from it:
    the ``B`` says this came from a verdict that stopped a branch, so
    the two can never collide in the tracker even though both are
    positional within the same vote.

    :param rec: The vote the claim was blocked on.
    :param claim: 1-based position within that report, as the verifier
        enumerated it.
    :returns: e.g. ``m3b/m3b-impl-a/bugs/r1#B2``.
    """
    return f'{_blocking_id_prefix(rec)}{claim}'


#: Label applied to every filed finding, so triage state can live on
#: the issue rather than in a `Status:` line somebody has to parse. A
#: repo without it makes `gh` refuse the whole call, so
#: :meth:`WorktreeManager.create_issue` retries once without it.
_FINDING_LABEL = 'finding'

#: A finding's outcome when the tracker already had it.
_ALREADY_TRACKED = 'Already tracked: an earlier run filed it'


def _filed(url: str | None) -> str:
    """A finding's outcome once filing was tried."""
    if url:
        return f'Filed: {url}'
    return 'Could not be filed; it is in the reviews record'


def _withheld(verdict: Disposition) -> str:
    """A finding's outcome when triage kept it out of the tracker."""
    reason = verdict.reason or 'no reason given'
    return f'Withheld by triage (`{verdict.verdict}`): {reason}'


def finding_marker(ident: str) -> str:
    """
    The machine-readable identity carried by a filed finding.

    An HTML comment: exact to match on, and invisible in the rendered
    issue, so a human editing the body cannot accidentally break dedup
    by reformatting around it.

    :param ident: The positional id from :func:`finding_id`.
    :returns: The marker line.
    """
    return f'<!-- finding-id: {ident} -->'
