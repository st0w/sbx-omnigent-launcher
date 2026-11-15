"""Text helpers shared by the reply parsers."""

from __future__ import annotations

import re

#: A markdown thematic break closing a section. It has to be checked
#: BEFORE the item pattern, because `---` matches "bullet `-`, text
#: `--`" and was lifted into the committed ledger as a decision reading
#: `--`.
_THEMATIC_BREAK_RE = re.compile(r'^[ \t]*(?:-{3,}|\*{3,}|_{3,})[ \t]*$')
_DECISION_ITEM_RE = re.compile(
    r'^[ \t]*(?:[-*\u2022]|\d+[.)])[ \t]*(?P<text>.+?)[ \t]*$'
)


def _single_line(text: str) -> str:
    """
    Collapse *text* to a single line (whitespace runs → one space).

    An agy turn is typed into its TUI and the bridge waits a few seconds
    for the paste to render before submitting; a multi-line paste does
    not redraw in time under load and the turn is dropped unsubmitted.
    Flattening a turn to one line makes it render instantly. Harmless
    for an LLM prompt — newlines carry no meaning the model needs here.

    :param text: The turn message.
    :returns: The message with every whitespace run replaced by a single
        space and the ends stripped.
    """
    return ' '.join(text.split())


def _excerpt(text: str, limit: int = 600) -> str:
    """
    One line of *text*, its whitespace collapsed, cut to *limit*.

    :param text: A report.
    :param limit: Most characters kept.
    :returns: The excerpt, ``…``-terminated when cut.
    """
    flat = ' '.join(text.split())
    return flat if len(flat) <= limit else f'{flat[: limit - 1]}…'


def _has_marker_line(text: str | None, marker: str) -> bool:
    """
    Whether some line of *text* is exactly *marker*.

    The words must be the whole line, give or take markdown emphasis or
    backticks, so a reply that only promises to end with them ("I will
    end with CHECK COMPLETE once the tests finish") does not count.

    :param text: A reply.
    :param marker: The line it must contain.
    :returns: ``True`` when some line is exactly the marker.
    """
    if not text:
        return False
    return any(
        line.strip().strip('*_`').strip() == marker
        for line in text.splitlines()
    )


def _printable(text: str) -> str:
    """*text* without unprintable characters, for the terminal."""
    return ''.join(ch for ch in text if ch.isprintable())


def _strings(value: object) -> tuple[str, ...]:
    """
    The string items of a persisted list, dropping anything else.

    :param value: A value read from run state, of any shape.
    :returns: Its string items, in order, or ``()``.
    """
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _lift_marked_items(
    text: str | None, header: re.Pattern[str]
) -> tuple[str, ...]:
    """
    Items under the LAST occurrence of *header*.

    The one protocol every marker in this module shares: the last header
    wins, blank lines inside the list are tolerated, and the first
    non-blank line that is not an item ends the block. Extracted so a
    new marker cannot accidentally get a different one — three of them
    were about to be written by hand.

    :param text: The reply to read.
    :param header: The marker's compiled header pattern.
    :returns: The items, in order, or ``()`` when the marker is absent.
    """
    if not text:
        return ()
    matches = list(header.finditer(text))
    if not matches:
        return ()
    out: list[str] = []
    for line in text[matches[-1].end():].splitlines():
        if not line.strip():
            continue  # tolerate blank lines within the list
        if _THEMATIC_BREAK_RE.match(line):
            break  # a rule closes the section, it is not an item
        item = _DECISION_ITEM_RE.match(line)
        if item is None:
            break  # prose after the list ends the block
        value = item.group('text').strip()
        if value:
            out.append(value)
    return tuple(out)
