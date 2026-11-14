"""Wait out the background work a Claude agent ended its turn on.

Claude Code is built to start a long command in the background (or
watch one with its Monitor tool), end the turn, and be woken when it
finishes: its own tool output says "You will be notified when it
completes — do not poll that file or sleep while waiting". Omnigent
records the wake like any turn. The ``<task-notification>`` arrives
as a user message, the session goes ``running``, the agent replies,
and it goes ``idle`` again.

The runner used to read the reply the moment the first turn ended. An
agent installing its toolchain in the background was then asked to
finish, ended its turn again, and was stopped as unfinished. Forbidding
background work instead would serialise every install, build and test
run. So the runner waits for the wake, reading only what the session
already records:

* a launch is a tool output reading ``Command running in background with
  ID: <id>`` or ``Monitor started (task <id>, ...)``;
* an end is a ``<task-notification>`` naming that ``<task-id>`` with a
  ``<status>``, or a Monitor's ``[Monitor stopped]`` /
  ``[Monitor expired ...]``.

The text is Claude Code's, not an interface. If it changes, nothing
reads as a launch and the runner simply stops waiting, which is the
behaviour it had before this module.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Sequence
from typing import Protocol

import click

#: How often a waiting runner reads the session.
POLL_S = 15.0

_LAUNCH_RES = (
    re.compile(r'Command running in background with ID: ([A-Za-z0-9_-]+)'),
    re.compile(r'Monitor started \(task ([A-Za-z0-9_-]+)'),
)
_TASK_ID_RE = re.compile(r'<task-id>([A-Za-z0-9_-]+)</task-id>')
_ENDED_RE = re.compile(
    r'<status>[a-z_]+</status>|\[Monitor stopped|\[Monitor expired'
)
_NOTIFICATION = '<task-notification>'

#: Session statuses after which nothing more will be recorded.
_LIVE_STATUSES = frozenset({'running', 'idle'})

Item = dict[str, object]


class SessionReader(Protocol):
    """The two session reads the wait needs."""

    def read_items(
        self, session_id: str, *, tail: int | None = None
    ) -> list[Item]:
        """Items oldest-first."""

    def get_status(self, session_id: str) -> dict[str, object]:
        """The session's snapshot, carrying ``status``."""


def _text(value: object) -> str:
    """The text of a string, or of a list of ``{'text': ...}`` parts."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return ''.join(
            part.get('text') or ''
            for part in value
            if isinstance(part, dict) and isinstance(part.get('text'), str)
        )
    return ''


def _is_message(item: Item, role: str) -> bool:
    return item.get('type') == 'message' and item.get('role') == role


def _id(item: Item) -> str | None:
    value = item.get('id')
    return value if isinstance(value, str) else None


def outstanding(items: Sequence[Item]) -> set[str]:
    """
    Background tasks launched in *items* that have not reported an end.

    :param items: Session items, oldest first.
    :returns: The ids still running.
    """
    launched: set[str] = set()
    ended: set[str] = set()
    for item in items:
        if item.get('type') == 'function_call_output':
            text = _text(item.get('output'))
            for pattern in _LAUNCH_RES:
                launched.update(pattern.findall(text))
        elif _is_message(item, 'user'):
            text = _text(item.get('content'))
            if text.lstrip().startswith(_NOTIFICATION) and _ENDED_RE.search(
                text
            ):
                ended.update(_TASK_ID_RE.findall(text))
    return launched - ended


def items_after(
    items: Sequence[Item], marker: str | None
) -> list[Item]:
    """
    The items after the one with id *marker*.

    :param items: Session items, oldest first.
    :param marker: An item id, or ``None`` for all of them.
    :returns: The later items; all of them when *marker* is not listed.
    """
    ids = [item.get('id') for item in items]
    if marker is None or marker not in ids:
        return list(items)
    return list(items[ids.index(marker) + 1:])


def assistant_text(items: Sequence[Item]) -> str:
    """The assistant messages in *items*, joined by blank lines."""
    return '\n\n'.join(
        text
        for item in items
        if _is_message(item, 'assistant')
        and (text := _text(item.get('content')).strip())
    )


def await_background(
    client: SessionReader,
    session: str,
    *,
    since: str | None,
    reply: str,
    done: Callable[[str], bool],
    deadline: float,
    label: str,
    stop: Callable[[], None],
    poll_s: float = POLL_S,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """
    Follow a turn that ended on background work until it really ends.

    Returns at once, after one read, when *reply* is already finished
    or the turn started nothing in the background, so a finished agent
    never waits on a server it left running. Otherwise it waits for
    each wake: new items whose newest is an assistant message, with the
    session idle and unchanged across one poll (Claude reads idle
    between tool rounds too). Each woken reply is appended. It stops
    when the reply is finished, nothing is outstanding, the session
    fails, or *deadline* passes.

    :param client: The session reader.
    :param session: The session.
    :param since: The newest item before this turn, so only this turn's
        launches count; ``None`` for all of them.
    :param reply: The turn's reply so far.
    :param done: Whether a reply is finished.
    :param deadline: :func:`time.monotonic` value the turn must end by.
    :param label: The node, for the console.
    :param stop: Called before every poll; raises to abandon the wait
        (Ctrl-C).
    :param poll_s: Seconds between reads.
    :param clock: Monotonic clock (injected in tests).
    :param sleep: Sleep (injected in tests).
    :returns: *reply*, with every woken reply appended.
    """
    turn = items_after(client.read_items(session), since)
    pending = outstanding(turn)
    if done(reply) or not pending:
        return reply
    click.echo(
        f'[background] {label}: ended its turn with {len(pending)} '
        f'background task(s) still running; waiting for it to be woken '
        f'and reply.'
    )
    seen = _id(turn[-1]) if turn else since
    candidate: str | None = None
    while pending and not done(reply):
        stop()
        if clock() >= deadline:
            click.echo(
                f'[background] {label}: the turn timed out with '
                f'{len(pending)} background task(s) still running.'
            )
            return reply
        sleep(poll_s)
        turn = items_after(client.read_items(session), since)
        status = client.get_status(session).get('status')
        newest: Item = turn[-1] if turn else {}
        newest_id = _id(newest)
        if newest_id == seen:
            continue
        failed = status not in _LIVE_STATUSES
        settled = failed or (
            status == 'idle'
            and newest_id == candidate
            and _is_message(newest, 'assistant')
        )
        if not settled:
            candidate = newest_id
            continue
        woken = assistant_text(items_after(turn, seen))
        if woken:
            reply = f'{reply}\n\n{woken}'
        if failed:
            return reply
        seen, candidate = newest_id, None
        pending = outstanding(turn)
    return reply
