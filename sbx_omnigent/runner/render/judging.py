"""The record of a judge's decision."""

from __future__ import annotations

from sbx_omnigent.runner.records.judging import (
    JudgePick,
    _withdrawn,
    retained_bundle_ref,
)


def render_judge_decision(
    picks: list[JudgePick], *, title: str
) -> str:
    """
    Render the judge's choices as a durable record.

    Deliberately leads with the machine-readable summary line: the value
    of this artifact is the SERIES, not any single entry, so the first
    thing a reader (or a grep across modules) meets is one line each.

    :param picks: The picks to record, in stage order.
    :param title: Heading for the document.
    :returns: The markdown document, or ``''`` when there is nothing.
    """
    if not picks:
        return ''
    parts = [
        f'# {title}',
        '',
        'Which implementation won, and why. Two writers build the same '
        'frozen contract on isolated branches and a judge picks one; '
        'the loser is not published, so without this the choice leaves '
        'no trace at all.',
        '',
        '| stage | candidates | selected | judge said |',
        '| --- | --- | --- | --- |',
    ]
    for pick in picks:
        stated = f'`{pick.stated}`' if pick.stated else '_none stated_'
        if pick.uncontested:
            stated = '_uncontested, no judge ran_'
        cands = ', '.join(f'`{c}`' for c in pick.candidates)
        parts.append(
            f'| `{pick.stage}` | {cands} '
            f'| `{pick.selected}` | {stated} |'
        )
    for pick in picks:
        parts += [
            '', '---', '',
            f'## `{pick.stage}` — selected `{pick.selected}`',
        ]
        if pick.uncontested:
            parts += [
                '',
                f'> **Uncontested.** `{pick.selected}` was the only '
                f'candidate left, so no judge ran. Withdrawn: '
                f'{_withdrawn(pick)}.',
            ]
        elif pick.forfeited:
            parts += [
                '',
                f'> **A reduced field.** Withdrawn before judging: '
                f'{_withdrawn(pick)}.',
            ]
        if not pick.uncontested and not pick.honored:
            # Only picks recorded before the runner halted on a missing
            # SELECT reach here: it used to substitute the first
            # candidate. That was a fallback, not a decision, and a
            # series that silently counted it as one would misreport
            # the race.
            parts += [
                '',
                "> **Not the judge's stated choice.** It named "
                f'`{pick.stated or "nothing"}`, which is not one of the '
                f'candidates, so the runner fell back to the first one. '
                f'Read this as an ABSENT decision, not a preference.',
            ]
        if not pick.uncontested:
            parts += [
                '', pick.reasoning.strip() or '_The judge said nothing._'
            ]
        if pick.retained:
            parts += [
                '',
                '### The implementation(s) not chosen',
                '',
                'Kept as git bundles — they are complete, reviewed and '
                'test-passing, and this is the only copy. Each is under '
                "the runner's `--canonical-root`, on the machine that ran "
                'it. Restore one with `git fetch <bundle> '
                'refs/heads/<branch>:refs/heads/<name>`.',
                '',
            ]
            parts += [
                f'- `{node}` — `{retained_bundle_ref(path)}`'
                for node, path in pick.retained
            ]
    return '\n'.join(parts) + '\n'
