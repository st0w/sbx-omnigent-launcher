"""Plan-shaped text for tests of plan parsing and selection."""

from __future__ import annotations


def plan_text(size: int, marker: str = '') -> str:
    """
    Plan-SHAPED text of roughly *size* chars.

    The selection rules are about relative size and recency, but the
    guard is now structural — so filler like ``'x' * 4000`` is correctly
    not a plan, and these fixtures have to look like one.
    """
    head = (
        '## Files\n'
        '- src/core.rs — the parser\n'
        '- src/lib.rs — exports it\n'
        '- tests/parse_test.rs — the cases\n\n'
        '## Interfaces\n'
        '`parse` takes a spec and returns whole seconds; its single '
        'responsibility is parsing.\n\n'
        '## Algorithm\n'
        '1. Read the spec\n2. Split the units\n3. Sum them\n\n'
        '## Edge cases\n'
        'An invalid suffix must fail; a malformed spec raises.\n\n'
        '## Test strategy\n'
        'The test author must verify each unit test case.\n\n'
        f'{marker}\n'
    )
    pad = max(0, size - len(head))
    return head + ('detail. ' * (pad // 8 + 1))[:pad]


def plan_with_decisions(*decisions: str, header: str | None = None) -> str:
    head = header or 'DECISIONS FOR LATER MODULES:'
    body = '\n'.join(f'- {d}' for d in decisions)
    return f'# Module design\n\nprose here\n\n{head}\n{body}\n'
