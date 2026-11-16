"""Tracker issues for findings, and the ledger of what was filed."""

from __future__ import annotations

from sbx_omnigent.runner.records.findings import finding_id, finding_marker
from sbx_omnigent.runner.records.reviews import ReviewRecord


def _issue_title_text(finding: str, limit: int = 88) -> str:
    """
    A finding's first line, fit to be an issue title.

    Reviewers write markdown, and a title opening with a literal ``**``
    reads as noise in a tracker list. Emphasis is stripped, the leading
    SENTENCE is preferred over a hard character cut — reviewers tend to
    lead with the claim and follow with the detail — and a cut that has
    to happen lands on a word boundary rather than mid-token.

    :param finding: The finding text.
    :param limit: Longest title to produce.
    :returns: The title text.
    """
    text = ' '.join(finding.split()).replace('**', '').replace('`', '')
    head, sep, _rest = text.partition('. ')
    if sep and len(head) <= limit:
        return head
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(' ', 1)[0].rstrip(' ,;:')
    return f'{cut}…'


def render_finding_issue(
    rec: ReviewRecord,
    finding: str,
    index: int,
    *,
    run_id: str,
    report_doc: str | None = None,
) -> tuple[str, str]:
    """
    One non-blocking finding as a GitHub issue.

    :param rec: The vote it came from.
    :param finding: The finding text.
    :param index: 1-based position in that reviewer's list.
    :param run_id: The campaign that raised it.
    :param report_doc: Repo path of the full reviewer report.
    :returns: ``(title, body)``.
    """
    ident = finding_id(rec, index)
    head = _issue_title_text(finding)
    where = f'[{rec.chunk}] ' if rec.chunk else ''
    raised = f'`{rec.target}`' if rec.target else 'an unknown candidate'
    body = [
        finding.strip(),
        '',
        '---',
        '',
        f'**Raised against:** {raised} by `{rec.reviewer}`, review round '
        f'{rec.round_no}, alongside a verdict of '
        f'{rec.verdict or "none stated"}.',
    ]
    if report_doc:
        body.append(f'**Full report:** `{report_doc}`')
    body += [
        f'**Campaign:** `{run_id}`',
        '',
        'Raised by a reviewer as NON-BLOCKING and filed automatically. '
        'Nothing has been acted on — a blocking finding is handled '
        'inside its review round, and this is the set that used to be '
        'archived and never read again. Triage it as you would any '
        'other issue; closing it is how the pipeline learns not to '
        'raise it again.',
        '',
        finding_marker(ident),
    ]
    return f'{where}{head}', '\n'.join(body)


def render_blocking_issue(
    rec: ReviewRecord,
    claim: str,
    ident: str,
    *,
    run_id: str,
    report_doc: str | None = None,
) -> tuple[str, str]:
    """
    One surviving BLOCKING claim as a GitHub issue.

    The claim is the VERIFIER's sentence, not the reviewer's paragraph:
    a blocking report is prose, and the verifier is what turned it into
    one checkable statement about the code that shipped.

    :param rec: The vote it was blocked on.
    :param claim: The claim, as the verifier restated it.
    :param ident: Its positional id, from :func:`blocking_finding_id`.
    :param run_id: The campaign that raised it.
    :param report_doc: Repo path of the full reviewer report.
    :returns: ``(title, body)``.
    """
    head = _issue_title_text(claim)
    where = f'[{rec.chunk}] ' if rec.chunk else ''
    raised = f'`{rec.target}`' if rec.target else 'an unknown candidate'
    body = [
        claim.strip(),
        '',
        '---',
        '',
        f'**Raised as BLOCKING** against {raised} by `{rec.reviewer}`, '
        f'review round {rec.round_no}.',
        '**Still present:** a verification pass read that report '
        'against the code that shipped and concluded this claim still '
        'reproduces.',
    ]
    if report_doc:
        body.append(f'**Full report:** `{report_doc}`')
    body += [
        f'**Campaign:** `{run_id}`',
        '',
        'A blocking finding is normally closed by the writer it '
        'blocked. This one is filed because the shipping code still '
        'has it — which is what happens when the defect was fixed on '
        'the candidate that was blocked and the judge selected its '
        'competitor.',
        '',
        finding_marker(ident),
    ]
    return f'{where}{head}', '\n'.join(body)


def render_findings_ledger(
    records: list[ReviewRecord],
    *,
    title: str,
    existing: str | None,
    report_doc: str | None = None,
) -> str | None:
    """
    Append new findings to the ledger, never rewriting what is there.

    APPEND-ONLY is the whole contract. This is a HUMAN-EDITED artifact —
    a person annotates status and reasoning on it, across modules and
    across runs — so *existing* is carried through byte for byte and new
    entries are added after it. A renderer that rebuilt the document
    each run would clobber exactly the reasoning it exists to hold.

    Entries are sections rather than table rows, which is deliberate. A
    table only stays valid if every future append lands immediately
    after the last row, so the first person to write a paragraph under
    it breaks every later append. Sections append correctly after
    anything, which is what "human-editable" has to mean if the
    constraint above is real.

    :param records: Every vote for this chunk, in order cast.
    :param title: Heading, used only when creating the document.
    :param existing: The ledger already on the branch, or ``None``.
    :param report_doc: Repo path of the full reports, cross-referenced.
    :returns: The complete new document, or ``None`` when there is
        nothing new to add — the caller then writes nothing at all.
    """
    body = existing or ''
    fresh: list[str] = []
    for rec in records:
        for index, finding in enumerate(rec.findings, start=1):
            ident = finding_id(rec, index)
            # Substring match is sufficient and is the point: an id that
            # survives anywhere in the human's prose counts as present,
            # so moving a row or annotating it never resurrects it.
            if f'`[{ident}]`' in body:
                continue
            raised = (
                f'`{rec.target}`' if rec.target else '_unknown candidate_'
            )
            fresh += [
                '',
                '---',
                '',
                f'## `[{ident}]`',
                '',
                '**Status:** open',
                '',
                f'**Raised against:** {raised} by `{rec.reviewer}`, '
                f'round {rec.round_no}, verdict '
                f'{rec.verdict or "none stated"}',
            ]
            if report_doc:
                fresh += ['', f'**Full report:** `{report_doc}`']
            fresh += ['', finding.strip()]
    if not fresh:
        return None
    if body:
        return body.rstrip('\n') + '\n' + '\n'.join(fresh) + '\n'
    header = [
        f'# {title}',
        '',
        'Non-blocking findings raised by reviewers, across every module '
        'and every run. A blocking finding is acted on inside its review '
        'round; one raised beside an APPROVED verdict used to be '
        'archived and never read again, which is how a real defect '
        'noticed by a careful reviewer evaporated.',
        '',
        'Nothing here has been acted on. These are TRACKED, not fixed — '
        'an agent unilaterally changing code for everything a reviewer '
        'mentioned is worse than the findings being missed.',
        '',
        '**This file is yours to edit.** The runner only ever APPENDS; '
        'it never rewrites, reorders or removes an entry. Change a '
        '`Status:` line, add notes, group things — none of it will be '
        'clobbered. Each entry carries an id, and an entry whose id is '
        'still somewhere in this file is never added again, so an id is '
        'the one thing worth keeping.',
        '',
        'Ids are positional (`chunk/candidate/reviewer/round#n`), so the '
        'same issue raised by two reviewers, or re-raised after a round '
        'of work, appears more than once. That is provenance: merging '
        'them is a judgement call, and it is yours.',
    ]
    return '\n'.join(header + fresh) + '\n'
