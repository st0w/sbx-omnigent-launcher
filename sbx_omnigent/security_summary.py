"""A module's security summary: each scan round and finding outcome.

Committed as ``README.md`` in the module's ``-security`` folder, beside
each round's own report folder, after the module's findings are filed,
so it can link the issues they became. GitHub shows it when the folder
is opened.

Pure rendering: the runner gathers the rounds from the saved reports and
the outcomes from filing and triage, and this turns them into Markdown.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sbx_omnigent.security_scan import SEVERITIES, ScanFinding


@dataclass(frozen=True)
class ScanRound:
    """
    One scan round.

    :param number: The round number.
    :param verdict: ``BLOCKING`` or ``APPROVED``.
    :param folder: Its report folder's name, beside the summary, or
        ``None`` if none was saved.
    :param findings: Everything it reported, or ``None`` when its
        results could not be read back.
    :param triage: For a blocking round, what triage concluded when it
        re-checked the round's report against the shipping code.
    """

    number: int
    verdict: str
    folder: str | None
    findings: tuple[ScanFinding, ...] | None
    triage: str = ''


@dataclass(frozen=True)
class ScanStage:
    """
    One ``scans:`` stage's rounds in a module.

    :param stage: The stage id.
    :param rounds: Its rounds, oldest first.
    :param outcomes: What became of each Medium and Low finding of the
        final round, in the order it reported them.
    """

    stage: str
    rounds: tuple[ScanRound, ...]
    outcomes: tuple[str, ...]


def render_summary(*, title: str, stages: Sequence[ScanStage]) -> str:
    """
    The summary document.

    :param title: What was built, e.g. ``"discover [m1] collectors"``.
    :param stages: Each scan stage that ran in the module.
    :returns: Markdown.
    """
    parts = [
        f'# Security scan — {title}',
        '',
        'Every claude-security scan of the changes in this module, round '
        'by round, and what became of each finding. The report of each '
        'round (Markdown, JSONL, SARIF and the revision stamp) is in the '
        'folder named in its row.',
    ]
    for stage in stages:
        parts += _stage_lines(stage)
    return '\n'.join(parts).rstrip() + '\n'


def _stage_lines(stage: ScanStage) -> list[str]:
    lines = ['', f'## `{stage.stage}`', '']
    lines += _round_table(stage.rounds)
    lines += _blocking_section(stage.rounds)
    lines += _minor_section(stage)
    return lines


def _round_table(rounds: Sequence[ScanRound]) -> list[str]:
    lines = [
        '| Round | Result | Critical | High | Medium | Low | Report |',
        '| --- | --- | --- | --- | --- | --- | --- |',
    ]
    unreadable = []
    for rnd in rounds:
        result = 'blocked' if rnd.verdict == 'BLOCKING' else 'passed'
        if rnd.findings is None:
            counts = ['?'] * len(SEVERITIES)
            unreadable.append(rnd.number)
        else:
            counts = [
                str(sum(1 for f in rnd.findings if f.severity == sev))
                for sev in SEVERITIES
            ]
        report = f'[{rnd.folder}]({rnd.folder}/)' if rnd.folder else '—'
        lines.append(
            f'| {rnd.number} | {result} | {" | ".join(counts)} | '
            f'{report} |'
        )
    if unreadable:
        rounds_text = ', '.join(str(n) for n in unreadable)
        lines += [
            '',
            f'The results of round(s) {rounds_text} could not be read '
            f'back, so their findings are missing below. They are in the '
            f'reviews record.',
        ]
    return lines


def _blocking_section(rounds: Sequence[ScanRound]) -> list[str]:
    lines = ['', '### Critical and High', '']
    seen = _track(rounds, blocking=True)
    if not seen:
        return [*lines, 'No Critical or High finding was reported.']
    final = rounds[-1].number
    lines += [
        '| Finding | Severity | Where | Reported | Outcome |',
        '| --- | --- | --- | --- | --- |',
    ]
    for finding, first, last in seen:
        outcome = (
            f'Not reported from round {last + 1}'
            if last < final
            else 'Still reported by the final round'
        )
        lines.append(
            f'| {_name(finding)} | {finding.severity} | '
            f'`{_cell(finding.file)}:{finding.line}` | '
            f'{_span(first, last)} | {outcome} |'
        )
    for rnd in rounds:
        if rnd.verdict == 'BLOCKING' and rnd.triage:
            lines += [
                '',
                f"Triage's re-check of round {rnd.number}, against the "
                f'code that shipped: {rnd.triage}',
            ]
    return lines


def _minor_section(stage: ScanStage) -> list[str]:
    lines = ['', '### Medium and Low', '']
    rounds = stage.rounds
    final = rounds[-1]
    final_minor = [
        f for f in final.findings or () if not f.blocks
    ]
    tracked = _track(rounds, blocking=False)
    first_seen = {_key(f): first for f, first, _last in tracked}
    earlier = [
        (finding, first, last)
        for finding, first, last in tracked
        if last < final.number
    ]
    if not final_minor and not earlier:
        return [*lines, 'No Medium or Low finding was reported.']
    lines += [
        '| Finding | Severity | Where | Reported | Outcome |',
        '| --- | --- | --- | --- | --- |',
    ]
    for index, finding in enumerate(final_minor):
        outcome = (
            stage.outcomes[index]
            if index < len(stage.outcomes)
            else 'unknown: not recorded'
        )
        first = first_seen.get(_key(finding), final.number)
        lines.append(
            f'| {_name(finding)} | {finding.severity} | '
            f'`{_cell(finding.file)}:{finding.line}` | '
            f'{_span(first, final.number)} | {_cell(outcome)} |'
        )
    for finding, first, last in earlier:
        lines.append(
            f'| {_name(finding)} | {finding.severity} | '
            f'`{_cell(finding.file)}:{finding.line}` | '
            f'{_span(first, last)} | Not reported by the final round, so '
            f'not filed |'
        )
    return lines


def _track(
    rounds: Sequence[ScanRound], *, blocking: bool
) -> list[tuple[ScanFinding, int, int]]:
    """
    Each distinct finding, with the first and last round reporting it.

    Matched by the plugin's id, which it keeps from scan to scan, or by
    file and title when there is none. The finding shown is the latest
    report of it.

    :returns: ``(finding, first round, last round)``, in first-seen
        order.
    """
    found: dict[str, tuple[ScanFinding, int, int]] = {}
    for rnd in rounds:
        for finding in rnd.findings or ():
            if finding.blocks != blocking:
                continue
            key = _key(finding)
            first = found[key][1] if key in found else rnd.number
            found[key] = (finding, first, rnd.number)
    return list(found.values())


def _key(finding: ScanFinding) -> str:
    if finding.plugin_id:
        return f'id:{finding.plugin_id}'
    return f'at:{finding.file}\0{finding.title}'


def _name(finding: ScanFinding) -> str:
    return _cell(f'{finding.id} {finding.category}: {finding.title}')


def _span(first: int, last: int) -> str:
    return f'round {first}' if first == last else f'rounds {first} to {last}'


def _cell(text: str) -> str:
    """*text* safe in a Markdown table cell: one line, no bare pipe."""
    return ' '.join(text.split()).replace('|', '\\|')
