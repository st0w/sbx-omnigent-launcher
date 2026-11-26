"""Parse a server-sent event (SSE) stream into frames.

The Omnigent session client reads every turn's events from an SSE
stream. Parsing that stream is separate from driving a session: it is
pure, needs nothing from the client, and is tested on its own.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass


@dataclass(frozen=True)
class SseFrame:
    """One parsed SSE frame: an ``event:`` type (or None) + its data."""

    event: str | None
    data: str


def parse_sse(lines: Iterator[str]) -> Iterator[SseFrame]:
    """
    Parse a line stream into SSE frames.

    Minimal SSE: ``event:`` sets the frame type, ``data:`` lines
    accumulate (newline-joined), a blank line dispatches the frame, and
    ``:`` comment lines are ignored. Trailing newlines on each input
    line are tolerated.

    :param lines: Iterator of decoded stream lines (with or without a
        trailing newline).
    :returns: Iterator of :class:`SseFrame`, one per dispatched event.
    """
    event: str | None = None
    data: list[str] = []
    for raw in lines:
        line = raw.rstrip('\n').rstrip('\r')
        if line == '':
            if data or event is not None:
                yield SseFrame(event=event, data='\n'.join(data))
            event = None
            data = []
            continue
        if line.startswith(':'):
            continue
        field, _, value = line.partition(':')
        if value.startswith(' '):
            value = value[1:]
        if field == 'event':
            event = value
        elif field == 'data':
            data.append(value)
    if data or event is not None:
        yield SseFrame(event=event, data='\n'.join(data))
