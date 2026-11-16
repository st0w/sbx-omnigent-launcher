"""Session transcripts kept as committable records."""

from __future__ import annotations

from pathlib import PurePosixPath


def render_turn_capture(
    turns: list[tuple[str, str]], *, title: str, reason: str
) -> str:
    """
    Render a session's conversation as a diagnostic record.

    Kept for the turns nobody can otherwise inspect. Disposing a
    session DELETES it, and teardown disposes everything moments after
    a run fails — so a writer that hung or failed leaves nothing behind
    but the timeout line. That has now happened twice: a refactor turn
    the human wanted to read and could not, and a coder turn that
    burned a full hour whose cause is still unknown because its
    transcript was gone before anyone could look.

    Reviewers already get this treatment (see
    :func:`render_review_records`); this is the same guarantee for
    every other turn.

    :param turns: ``(role, text)`` pairs oldest-first — messages only,
        never tool calls or their output.
    :param title: Heading for the document.
    :param reason: Why the turn was captured.
    :returns: The markdown document.
    """
    # Neutral on purpose. A `user` message is the RUNNER's instruction
    # in a writer's turn but the HUMAN's own words in a planning
    # session, and the item store cannot tell them apart — so naming
    # either one would credit a human's design decision to the
    # orchestrator, or vice versa, in the record kept precisely to
    # settle who decided what.
    label = {'user': 'User', 'assistant': 'Agent'}
    parts = [
        f'# {title}',
        '',
        f'**Captured because:** {reason}',
        '',
        'The session behind this turn is deleted when its microVM is '
        'disposed, which happens moments after a run ends. This is the '
        'only surviving record of what the agent was doing.',
        '',
        'Messages only — no tool calls and no tool output.',
    ]
    if not turns:
        parts += ['', '_The session could not be read._']
    for turn, (role, text) in enumerate(turns, start=1):
        parts += [
            '',
            '---',
            '',
            f'## {turn}. {label.get(role, role)}',
            '',
            text.strip(),
        ]
    return '\n'.join(parts) + '\n'


def render_planning_session(
    turns: list[tuple[str, str]], *, title: str, plan_doc: str
) -> str:
    """
    Render a planner session as a committable design record.

    The plan of record is a SUMMARY, and each rewrite drops what the
    last one made redundant — diagrams, rejected alternatives, and the
    human's reasoning go with it. Observed live: a module's design draft
    carried a mermaid diagram and nine SQL blocks, the revision that
    superseded it carried four and no diagram, and the revision is what
    would have reached the repo. Keeping the conversation beside the
    summary means the reasoning survives the summarizing.

    :param turns: ``(role, text)`` pairs oldest-first (see
        :meth:`sbx_omnigent.swarm_session.SwarmSessionClient
        .read_transcript`).
    :param title: Heading for the document.
    :param plan_doc: Repo path of the plan of record, cross-referenced.
    :returns: The markdown document, or ``''`` for no turns.
    """
    if not turns:
        return ''
    label = {'user': 'Human', 'assistant': 'Planner'}
    plan_name = PurePosixPath(plan_doc).name
    parts = [
        f'# {title}',
        '',
        f'The planning conversation behind [`{plan_name}`]({plan_name}) '
        '— every draft, the questions asked and answered, and the '
        'artifacts produced along the way.',
        '',
        'The plan of record is the approved SUMMARY of this discussion. '
        'This is the reasoning behind it, kept because consolidating a '
        'plan drops what it makes redundant, and a diagram or a rejected '
        'alternative is exactly what a later reader wants back.',
        '',
        'Messages only — no tool calls and no tool output.',
    ]
    for turn, (role, text) in enumerate(turns, start=1):
        parts += [
            '',
            '---',
            '',
            f'## {turn}. {label.get(role, role)}',
            '',
            text.strip(),
        ]
    return '\n'.join(parts) + '\n'
