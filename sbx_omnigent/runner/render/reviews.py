"""The record of every reviewer's vote and report."""

from __future__ import annotations

from sbx_omnigent.runner.prompts.checks import _CHECK_DISPUTED, _CHECK_VERDICT
from sbx_omnigent.runner.records.reviews import ReviewRecord


def render_review_records(
    records: list[ReviewRecord], *, title: str
) -> str:
    """
    Render reviewer reports as a committable record.

    :param records: The votes to document, in the order they were cast.
    :param title: Heading for the document.
    :returns: The markdown document, or ``''`` for no records.
    """
    if not records:
        return ''
    parts = [
        f'# {title}',
        '',
        'Every reviewer that voted on this branch, and the report behind '
        'each vote. Reviewers ran read-only against the branch itself '
        'and were required to EXECUTE what they reviewed — a verdict '
        'reached by inspection alone is not one this pipeline accepts. '
        'All of them had to approve before the branch could publish.',
        '',
        'Kept here because the reviewers do not outlive their own votes: '
        'each microVM is disposed as soon as the round is decided, and '
        'this is the only surviving record of what was checked.',
        '',
        'Messages only — no tool calls and no tool output.',
        '',
    ]
    if any(
        rec.verdict in (_CHECK_VERDICT, _CHECK_DISPUTED) for rec in records
    ):
        parts += [
            f'A row marked {_CHECK_VERDICT} or {_CHECK_DISPUTED} is a '
            f'contract check or a plan check, not a vote on this branch. '
            f'A contract check read the frozen tests beside the brief and '
            f'the plan before any writer started. A plan check ran the '
            f'claims a plan makes about existing code before it was '
            f'final; a {_CHECK_DISPUTED} round went back to the human, '
            f'who settled it before the plan was accepted. Their findings '
            f'are kept and filed like any reviewer finding.',
            '',
        ]
    parts += [
        '| Reviewer | Round | Verdict |',
        '| --- | --- | --- |',
    ]
    for rec in records:
        parts.append(
            f'| `{rec.label}` | {rec.round_no} | '
            f'{rec.verdict or "none stated"} |'
        )
    for rec in records:
        parts += [
            '',
            '---',
            '',
            f'## `{rec.label}` — round {rec.round_no} — '
            f'{rec.verdict or "no verdict stated"}',
        ]
        if not rec.turns:
            parts += [
                '',
                '_The session could not be read; only the verdict was '
                'captured._',
            ]
        for role, text in rec.turns:
            if role == 'assistant':
                parts += ['', text.strip()]
            else:
                # The runner's own turn: identical for every reviewer in
                # a stage, so it is folded away rather than repeated at
                # full length above each report.
                parts += [
                    '',
                    '<details><summary>What the reviewer was asked'
                    '</summary>',
                    '',
                    text.strip(),
                    '',
                    '</details>',
                ]
    return '\n'.join(parts) + '\n'
