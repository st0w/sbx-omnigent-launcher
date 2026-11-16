"""The decisions ledger: what earlier modules settled for later ones."""

from __future__ import annotations

import re

from sbx_omnigent.runner.parsing.text import (
    _DECISION_ITEM_RE,
    _lift_marked_items,
)

#: A per-module planner's cross-module decisions (see
#: :func:`parse_decisions`): a line reading ``DECISIONS FOR LATER
#: MODULES:`` followed by ``- <decision>`` items. Heading/emphasis
#: marks around it are tolerated — models routinely wrap a requested
#: header in ``##`` or ``**``, and losing the whole block to a stray
#: asterisk would silently drop the decisions it names.
_DECISIONS_HEADER_RE = re.compile(
    r'^[ \t]*#{0,6}[ \t]*\*{0,2}[ \t]*'
    r'DECISIONS FOR LATER MODULES[ \t]*:?[ \t]*\*{0,2}[ \t]*$',
    re.MULTILINE | re.IGNORECASE,
)

#: A decisions-ledger module heading: ``## [m1] Ingestion core``.
#:
#: Anchored on ``## [`` so it cannot match the referrals section's
#: ``## Raised in [m1] ...`` subheads. Those are recorded as explicitly
#: NOT binding, and reading them back as decisions would promote a
#: reviewer's passing note into a constraint no later module may alter.
_LEDGER_MODULE_RE = re.compile(r'^##[ \t]+\[([^\]]+)\]', re.M)


def parse_decisions_doc(text: str | None) -> list[tuple[str, str]]:
    """
    Read a committed decisions ledger back into ``(module, text)``.

    The inverse of :meth:`DecisionLedger.document`, and the reason the
    ledger can survive a run at all: its decisions are otherwise
    populated only from run state, so a campaign that builds ONE module
    per run starts empty every time and carries nothing forward.

    Stops at the second top-level heading. Everything after it is the
    referrals section, which the writer marks as not binding — see
    :data:`_LEDGER_MODULE_RE`.

    An item is a bullet or numbered line, the same forms
    :func:`parse_decisions` accepts. Continuation lines are NOT joined
    on, and that is deliberate rather than incidental: the ledger is
    human-edited, and an indented note beneath an item — the annotation
    the file exists to hold — would otherwise be swallowed into the
    decision above it.

    :param text: The committed ledger, or ``None`` when none exists.
    :returns: ``(module id, decision)`` pairs in document order. Empty
        for a missing or heading-less document, which is the normal
        first-module case rather than a failure.
    """
    if not text:
        return []
    body = text
    headings = [
        m.start() for m in re.finditer(r'^#[ \t]', body, re.M)
    ]
    if len(headings) > 1:
        body = body[: headings[1]]
    out: list[tuple[str, str]] = []
    module: str | None = None
    for line in body.splitlines():
        heading = _LEDGER_MODULE_RE.match(line)
        if heading is not None:
            module = heading.group(1).strip()
            continue
        if module is None:
            continue
        item = _DECISION_ITEM_RE.match(line)
        if item is not None:
            value = item.group('text').strip()
            if value:
                out.append((module, value))
    return out


def parse_decisions(text: str | None) -> list[str]:
    """
    Parse a planner's ``DECISIONS FOR LATER MODULES:`` block.

    Each module is planned in its own session, so a decision reached
    with the human while designing module N is invisible to module N+1's
    planner unless it is carried forward deliberately. The per-module
    consolidation turn asks for the decisions that BIND later modules
    under that header; this lifts them out so the runner can thread them
    into every later planner and commit them as one reviewable ledger.

    The LAST such header wins; blank lines inside the list are
    tolerated; the first non-blank line that is not an item ends the
    block. A missing/empty block yields ``[]`` — a module that settled
    nothing cross-cutting is the normal case, not a failure.

    :param text: The module's consolidated plan of record.
    :returns: The decisions, in order, or ``[]`` when none are declared.
    """
    return list(_lift_marked_items(text, _DECISIONS_HEADER_RE))
