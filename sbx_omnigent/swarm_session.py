"""Drive managed swarm sessions over the Omnigent HTTP API.

The trusted-plane primitive a per-swarm coordinator uses (via
``sys_os_shell``) to run coder/reviewer agents in their own microVMs and
know when each turn finishes. Native sub-agent tools cannot create
managed (microVM) sessions — they co-locate on the caller's runner and
expose no ``host_type``/``workspace`` — so the coordinator creates each
agent as a top-level ``host_type=managed`` session through
``POST /v1/sessions`` and drives it here (see
``docs/COLLABORATIVE_SWARM_DESIGN.md`` §7.1).

Turn completion is **push-based**, not polled: :meth:`SwarmSessionClient
.send_and_wait` subscribes to the session's SSE stream
(``GET /v1/sessions/{id}/stream``), waits for the ready heartbeat (which
the server emits the instant the subscriber slot registers), THEN posts
the turn — so the ``running`` → ``idle`` edge can never be missed — and
returns when the session reports a terminal ``session.status``. All of
the async plumbing lives in this tested helper, never in coordinator
prose, so the LLM can't mis-sequence a subscribe/post/await.
"""

from __future__ import annotations

import json
import queue
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import click

if TYPE_CHECKING:
    from collections.abc import Iterator

#: Message body shape the ``/events`` endpoint expects for a user turn
#: (mirrors what the runner posts for a sub-agent send).
_MESSAGE_TYPE = 'message'

#: A delivered tool RESULT in the item store. Its presence as the last
#: item is the signature of an abandoned turn (see
#: :data:`_ABANDON_CONFIRM_S`) — distinct from a PENDING
#: ``function_call``, which is what a turn mid-tool-round sits on.
_FUNCTION_OUTPUT_TYPE = 'function_call_output'

#: ``session.status`` values that mean a turn is still in flight — an
#: Terminal ``session.status`` values.
_STATUS_IDLE = 'idle'
_STATUS_FAILED = 'failed'

#: Grace (seconds) to keep reading after a terminal ``idle`` that had no
#: ``response_id`` and no reply yet: agy goes ``idle`` a beat BEFORE it
#: mirrors the turn's reply, so wait briefly for that reply rather than
#: return empty. Also bounds the wait when a turn truly has no reply.
_IDLE_REPLY_GRACE_S = 30.0

#: Stream silence (seconds) after which a started turn gets one
#: confirmation poll (see :meth:`_confirm_finished`). A Claude turn ends
#: on an idle carrying a ``response_id``; when that edge never arrives,
#: nothing else in the stream says the turn is over and the wait ran to
#: the full turn budget — an hour, twice: once on a review turn that had
#: finished and stated its verdict, and once on a coder turn whose
#: outcome could not be reconstructed afterwards at all, because the
#: session was disposed before anyone could read it.
#:
#: Armed by SILENCE, not by an idle event. An earlier version armed only
#: after a skipped id-less idle, which leaves the case where the stream
#: simply stops delivering — no idle, no close, nothing — and then
#: nothing ever arms. What makes the poll safe is the discriminator in
#: :meth:`_confirm_finished`, not whatever triggered it, so a broader
#: trigger costs nothing and covers strictly more.
#:
#: Long enough that the normal gap between an assistant message and its
#: next tool call does not trip it, short enough that a lost terminal
#: edge costs minutes instead of the whole budget.
_IDLE_CONFIRM_S = 120.0

#: Silence (seconds) before an idle session whose last item is a
#: delivered tool RESULT is declared abandoned rather than working.
#:
#: This is the fourth state, and the only one that used to have no
#: handling: the tool returned, the model was handed the result, and it
#: never continued. Observed live — a reviewer installed a toolchain,
#: built the workspace and launched the suite, then went to 0% CPU with
#: its tool output sitting unanswered. Nothing recognized that, so the
#: wait ran the full turn budget: 68 minutes gone and 45 more to come,
#: on a session that was never going to speak again.
#:
#: Deliberately several times :data:`_IDLE_CONFIRM_S`, and it is the
#: only verdict here that is a guess rather than a deduction. A model
#: thinking after a tool result should report ``running``, not ``idle``
#: — but if a harness ever reports idle mid-thought, this is what would
#: misfire, so it buys minutes of margin rather than seconds. A dead
#: turn costs this instead of the turn budget; a slow one is unharmed.
_ABANDON_CONFIRM_S = 360.0

#: Session-snapshot key holding interactive prompts the agent has
#: opened and is waiting on a human to answer.
_ELICITATIONS_KEY = 'pending_elicitations'

#: Reported when a turn is blocked on one. Says what to DO, because the
#: question itself is only visible in the web UI and nothing else in
#: the run surfaces it.
_ASKING_TURN_ERROR = (
    'the agent opened an interactive prompt and is waiting for a human '
    'to answer it, but this pipeline runs unattended — so the turn '
    'would sit until its timeout with nothing said on the console. '
    'Failing it here instead, with the question below. Answer it in '
    'the Omnigent UI and resume if you want this exact turn continued; '
    'otherwise just resume — agents are instructed to decide and '
    'report in their reply rather than ask. It asked'
)


def _elicitation_text(pending: object) -> str:
    """
    Best-effort human-readable text of a pending interactive prompt.

    The payload shape is the server's, not ours, so this reads the
    likely fields and falls back to the raw repr rather than losing the
    question — an unparsed prompt is still far more use to a human than
    a bare timeout.

    :param pending: The snapshot's pending-elicitations value.
    :returns: A one-line summary, or ``''`` when there is nothing.
    """
    if not isinstance(pending, list) or not pending:
        return ''
    out: list[str] = []
    for item in pending[:3]:
        if isinstance(item, dict):
            for key in ('message', 'question', 'prompt', 'text', 'title'):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    out.append(value.strip())
                    break
            else:
                out.append(repr(item))
        else:
            out.append(str(item))
    return ' | '.join(out)[:600]


#: Most prompts one turn will answer before giving up. This is a
#: runaway guard, not a budget: each approval costs a full silence
#: confirmation, so a session genuinely working through several
#: prompts stays well under it, while one re-asking the same thing
#: forever would otherwise spin to the turn deadline in silence.
_MAX_AUTO_APPROVALS_PER_TURN = 25

#: Reported when that cap is hit.
_ASKING_LOOP_ERROR = (
    'the agent kept opening interactive prompts and this turn answered '
    'the most it will'
)

#: Verdicts the resolve endpoint accepts. Enumerated server-side, so a
#: typo here is a 422 mid-turn; checked before the call instead.
_ELICITATION_ACTIONS = ('accept', 'decline', 'cancel')


def _elicitation_ids(pending: object) -> tuple[str, ...]:
    """
    The correlation ids of the prompts a session is blocked on.

    The ids are what makes a prompt ANSWERABLE, where
    :func:`_elicitation_text` only makes it reportable. Each pending
    entry is the original ``response.elicitation_request`` event dict,
    which carries ``elicitation_id`` at the top level.

    Read defensively, and per entry: the payload shape is the server's,
    and dropping a whole batch because one entry is malformed would
    strand a turn that could have continued on the rest.

    :param pending: The snapshot's pending-elicitations value.
    :returns: Ids in payload order, de-duplicated, empty when there is
        nothing answerable.
    """
    if not isinstance(pending, list):
        return ()
    ids: list[str] = []
    for item in pending:
        if not isinstance(item, dict):
            continue
        value = item.get('elicitation_id')
        if isinstance(value, str) and value and value not in ids:
            ids.append(value)
    return tuple(ids)

#: Reported when a turn is abandoned. Names the evidence, so the human
#: is not left guessing at a bare timeout the way they were before.
_ABANDONED_TURN_ERROR = (
    'the model stopped after a tool result and never continued — the '
    'session is idle and its newest item is a delivered tool output '
    'with no reply after it. Failing the turn rather than waiting out '
    'the turn budget on a session that will not speak again.'
)

#: Items to inspect when confirming a turn actually ended. Only the
#: last one decides; the rest are slack for trailing non-message items.
_CONFIRM_TAIL_ITEMS = 6

#: Grace (seconds) to wait for a turn-start signal before treating a
#: post as a lost submit and re-posting (a "nudge"). A successful submit
#: emits a ``running`` edge within a few seconds; only a submit dropped
#: by a not-yet-ready native TUI stays silent. Set safely past the agy
#: bridge's own submit-retry budget so a nudge never races its in-flight
#: paste — the nudge fires only once the bridge has fully given up.
_DEFAULT_STALL_GRACE_S = 25.0

#: Plan-consolidation settle (seconds). After the human approves, the
#: conversational agy planner both replies to "APPROVED" and (when
#: driven) emits the consolidated final plan — and its reply LAGS the
#: turn, so trusting the consolidation turn's streamed reply can capture
#: the PRIOR message. Instead the runner waits for the session to hold
#: ``idle`` for _PLAN_IDLE_STABLE_S (bounded by _PLAN_IDLE_TIMEOUT_S,
#: polled every _PLAN_IDLE_POLL_S) and reads the SETTLED final message.
_PLAN_IDLE_TIMEOUT_S = 180.0
_PLAN_IDLE_STABLE_S = 4.0
_PLAN_IDLE_POLL_S = 2.0

#: SSE stream sentinel the server sends on stream close.
_SSE_DONE = '[DONE]'

#: Default caps (seconds). The stream read timeout must exceed the
#: server's ``session.heartbeat`` cadence (~15s) so a healthy idle
#: stream isn't mistaken for a dead socket.
_DEFAULT_TURN_TIMEOUT_S = 1800.0
_DEFAULT_CONNECT_TIMEOUT_S = 60.0

#: Native-TUI (agy) terminal warm-up (see
#: :meth:`SwarmSessionClient.wait_for_terminal_ready`). The terminal
#: auto-creates on session create; wait for it to report running (the VM
#: must boot first, so allow a generous window), then settle for the
#: composer to become paste-ready before the first turn is typed in.
#:
#: The settle must outlast agy's CASCADE ROTATION, not just its render.
#: A cold agy mints its own cascade a few seconds after the terminal
#: reports running and ABANDONS the one the bridge cold-started — so a
#: turn pasted before that lands in a conversation agy immediately
#: leaves, and no reply can ever come. Measured live: terminal running
#: at T, paste at T+3s (the old settle), rotation at T+7s — the turn was
#: orphaned and the node sat silent until the turn timeout.
_DEFAULT_TERMINAL_READY_TIMEOUT_S = 90.0
_DEFAULT_TERMINAL_SETTLE_S = 20.0
_DEFAULT_TERMINAL_POLL_S = 1.0
_DEFAULT_STREAM_READ_TIMEOUT_S = 60.0
_DEFAULT_REQUEST_TIMEOUT_S = 30.0

#: Lowercase substrings that mark an LLM usage/rate limit rather than a
#: task failure. Matched against a turn's error text, its
#: ``last_task_error``, and (as a weak signal) the agent's own reply, so
#: a turn that ended because the model was throttled is flagged instead
#: of read as a bug. Deliberately broad — a false positive only adds a
#: "might be a rate limit" hint, it never fails a good turn.
_RATE_LIMIT_MARKERS: frozenset[str] = frozenset(
    {
        'rate limit',
        'rate-limit',
        'rate_limit',
        'usage limit',
        'usage-limit',
        'usage cap',
        'quota',
        'too many requests',
        'http 429',
        ' 429',
        'overloaded',
        'insufficient_quota',
        'resets at',
        'try again later',
        'capacity',
    }
)


def _assistant_reply_items(
    items: list[dict[str, object]],
) -> list[tuple[str, str]]:
    """
    Extract ``(item_id, text)`` for assistant messages with text.

    Keyed on the item **id**, not just text, so a caller can tell one
    turn's reply from the next even when two turns produce identical
    text (e.g. the same ``VERDICT:`` line on consecutive review rounds).

    :param items: Conversation items oldest-first (from
        :meth:`SwarmSessionClient.read_items`).
    :returns: ``(id, text)`` pairs for assistant messages carrying
        non-empty text, in chronological order.
    """
    out: list[tuple[str, str]] = []
    for item in items:
        if item.get('type') != 'message' or item.get('role') != 'assistant':
            continue
        parts: list[str] = []
        for block in item.get('content') or []:
            if isinstance(block, dict):
                text = block.get('text') or block.get('output_text')
                if isinstance(text, str) and text:
                    parts.append(text)
        item_id = item.get('id')
        if parts and isinstance(item_id, str):
            out.append((item_id, '\n'.join(parts)))
    return out


def _item_message_text(item: dict[str, object]) -> str:
    """
    Extract the concatenated text of an assistant message item.

    Handles the ``content`` block shapes seen on the SSE stream and in
    the item store (``text`` / ``output_text``).

    :param item: A ``message`` item dict (from a
        ``response.output_item.done`` frame or ``read_items``).
    :returns: The joined text, or ``""`` when the item carries none.
    """
    parts: list[str] = []
    for block in item.get('content') or []:
        if isinstance(block, dict):
            text = block.get('text') or block.get('output_text')
            if isinstance(text, str) and text:
                parts.append(text)
    return '\n'.join(parts)


#: The word a human posts to release an interactive plan to the
#: builders. A turn counts as approval only when, after normalisation,
#: it is EXACTLY this and nothing else — see :func:`_is_bare_approval`.
#:
#: ONE word, deliberately. `PLAN COMPLETE` and `PLAN APPROVED` were also
#: accepted, and the choice cost more than it gave: a reviewer has to be
#: TOLD what releases the gate, and three spellings make that sentence
#: longer without making the gate easier to open. A tuple rather than a
#: bare string so the matcher and its injectable parameter keep their
#: shape.
_PLAN_APPROVAL_PHRASES = ('APPROVED',)

#: Characters stripped from both ends of a candidate approval before it
#: is compared. Covers the ways a human types the word in a chat UI --
#: ``**APPROVED**``, ``"approved"``, ``APPROVED!`` -- without letting
#: any message containing OTHER words through.
_APPROVAL_TRIM = ' \t\r\n*_`"\'.!'

#: How many of the newest items the approval poll reads. Large enough
#: that the plan the human is approving is still in the window for the
#: look-back, since a long question-and-answer can be dozens of turns.
_PLAN_APPROVAL_TAIL = 200


def _is_bare_approval(text: str, phrases: tuple[str, ...]) -> bool:
    """
    Whether *text* is an approval turn and NOTHING else.

    A substring test cannot be used here. The runner posts the planner's
    instruction -- which embeds the whole task brief -- into the session
    as a ``role: user`` message, indistinguishable from a human turn, so
    any brief that merely contains the word "approved" (in the ordinary
    sense: "derives a suite from the approved plan") would release the
    gate before the human ever saw the questions. That happened: a run
    self-approved on its first poll and went straight to the test stage.

    So the whole message, once trimmed of chat decoration and
    whitespace-collapsed, must equal an approval phrase. Prose cannot
    survive that: "looks great, approved!" keeps "LOOKS GREAT," and does
    not match.

    :param text: The message text.
    :param phrases: Uppercase approval phrases to match exactly.
    :returns: ``True`` if this turn is bare approval.
    """
    return ' '.join(text.strip(_APPROVAL_TRIM).split()).upper() in phrases

#: Default cap on how long a planning session may sit SILENT before
#: the gate gives up. NOT a cap on the review itself: every turn in
#: the session restarts it, so an iterating human is never cut off.
_DEFAULT_PLAN_APPROVAL_IDLE_S = 3600.0


def _approved_plan_text(
    items: list[dict[str, object]],
    phrases: tuple[str, ...],
    *,
    seen_ids: frozenset[str] | set[str] = frozenset(),
) -> str | None:
    """
    The plan a human approved in a planner session, or ``None``.

    Three conditions must hold, because this gate is the ONLY thing
    standing between a generated plan and an unattended build, and it
    previously failed OPEN — silently, and nondeterministically.

    1. The turn must not already have been in the session when the wait
       began (*seen_ids*). The runner's own instruction is a
       ``role: user`` message like any other, so without this the gate
       can match the brief the runner just posted.
    2. The turn must be BARE approval — the whole message, nothing else
       (:func:`_is_bare_approval`).
    3. An approval with no assistant turn before it anywhere is not an
       approval. A human approves something the planner said, so that
       shape is structurally impossible and is refused rather than
       treated as "approved, plan unknown".

    The look-back for the plan itself deliberately scans the FULL item
    list, not just the post-baseline slice: the plan being approved was
    written before the wait began.

    :param items: Session items oldest-first (from ``read_items``).
    :param phrases: Uppercase approval phrases to match exactly.
    :param seen_ids: Ids of items already present when the wait began;
        these can never constitute the approval.
    :returns: The approved plan text, or ``None`` if not yet approved.
    """
    approval_idx: int | None = None
    for i, item in enumerate(items):
        if item.get('type') != 'message' or item.get('role') != 'user':
            continue
        item_id = item.get('id')
        if isinstance(item_id, str) and item_id in seen_ids:
            continue  # pre-existing turn (the runner's own instruction)
        if _is_bare_approval(_item_message_text(item), phrases):
            approval_idx = i
    if approval_idx is None:
        return None
    for item in reversed(items[:approval_idx]):
        if item.get('type') == 'message' and item.get('role') == 'assistant':
            text = _item_message_text(item)
            if text:
                return text
    # Fail closed: nothing was said before this "approval".
    return None


def _stream_event_from_payload(  # noqa: C901
    payload: dict[str, object], delta_buf: list[str]
) -> _StreamEvent | None:
    """
    Turn one decoded SSE payload into a ``_StreamEvent`` (or ``None``).

    ``session.status`` → a ``status`` event (carrying ``response_id``);
    the assistant message (streamed ``response.output_text.delta``
    chunks and the final ``response.output_item.done`` item) → a
    ``reply`` event. *delta_buf* accumulates delta chunks so a final
    message item with no content can still fall back to streamed text.

    :param payload: The decoded ``data:`` object of one SSE frame.
    :param delta_buf: Mutable accumulator for streamed text deltas.
    :returns: The event to emit, or ``None`` for frames we ignore.
    """
    kind = payload.get('type')
    if kind == 'response.completed':
        # agy fires this a beat before its id-less terminal idle, so it
        # marks that idle as the real completion (not a premature
        # settle). Claude fires it early and mid-turn, so it is NOT a
        # reliable Claude end-marker — the Claude path keys off the
        # response_id on the terminal idle instead.
        return _StreamEvent('completed')
    if kind == 'session.status':
        status = payload.get('status')
        error = payload.get('error')
        rid = payload.get('response_id')
        return _StreamEvent(
            'status',
            status=status if isinstance(status, str) else None,
            error=error if isinstance(error, str) else None,
            response_id=rid if isinstance(rid, str) else None,
        )
    if kind == 'response.output_text.delta':
        delta = payload.get('delta')
        if isinstance(delta, str):
            delta_buf.append(delta)
        if payload.get('final'):
            text = ''.join(delta_buf)
            delta_buf.clear()
            if text:
                return _StreamEvent('reply', reply=text)
        return None
    if kind == 'response.output_item.done':
        item = payload.get('item')
        if not isinstance(item, dict):
            return None
        is_msg = item.get('type') == 'message'
        is_assistant = item.get('role') == 'assistant'
        if is_msg and is_assistant:
            text = _item_message_text(item)
            if text:
                return _StreamEvent('reply', reply=text)
    return None


def looks_like_rate_limit(*texts: str | None) -> bool:
    """
    Whether any of *texts* reads like an LLM usage/rate-limit signal.

    A best-effort heuristic (see :data:`_RATE_LIMIT_MARKERS`) so callers
    can warn "this may be a rate limit, not a bug" when a turn ends
    early, empty, or failed. Case-insensitive; ``None`` entries ignored.

    :param texts: Candidate strings (error, ``last_task_error``, reply).
    :returns: ``True`` if any contains a known rate-limit marker.
    """
    for text in texts:
        if not text:
            continue
        low = text.lower()
        if any(marker in low for marker in _RATE_LIMIT_MARKERS):
            return True
    return False


class SwarmSessionError(click.ClickException):
    """A swarm-session operation failed (HTTP, timeout, or protocol)."""


@dataclass(frozen=True)
class SwarmTurnResult:
    """
    Outcome of a :meth:`SwarmSessionClient.send_and_wait` turn.

    :param status: Terminal ``session.status`` — ``"idle"`` (the turn
        completed) or ``"failed"``.
    :param error: Failure detail when *status* is ``"failed"``, else
        ``None``.
    :param reply: The assistant's reply text from THIS turn, captured
        live off the SSE stream as the turn ran (see
        :meth:`SwarmSessionClient.send_and_wait`). ``""`` when the turn
        produced no assistant text.
    """

    status: str
    error: str | None
    reply: str = ''

    @property
    def ok(self) -> bool:
        """:returns: ``True`` when the turn completed cleanly."""
        return self.status == _STATUS_IDLE


@dataclass(frozen=True)
class _SseFrame:
    """One parsed SSE frame: an ``event:`` type (or None) + its data."""

    event: str | None
    data: str


def parse_sse(lines: Iterator[str]) -> Iterator[_SseFrame]:
    """
    Parse a line stream into SSE frames.

    Minimal SSE: ``event:`` sets the frame type, ``data:`` lines
    accumulate (newline-joined), a blank line dispatches the frame, and
    ``:`` comment lines are ignored. Trailing newlines on each input
    line are tolerated.

    :param lines: Iterator of decoded stream lines (with or without a
        trailing newline).
    :returns: Iterator of :class:`_SseFrame`, one per dispatched event.
    """
    event: str | None = None
    data: list[str] = []
    for raw in lines:
        line = raw.rstrip('\n').rstrip('\r')
        if line == '':
            if data or event is not None:
                yield _SseFrame(event=event, data='\n'.join(data))
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
        yield _SseFrame(event=event, data='\n'.join(data))


class Transport(Protocol):
    """Injectable HTTP transport for :class:`SwarmSessionClient`."""

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, bytes]:
        """Perform a unary request; return ``(status_code, body)``."""
        ...

    def iter_lines(
        self, url: str, *, headers: dict[str, str], read_timeout: float
    ) -> Iterator[str]:
        """Open a streaming GET and yield decoded lines until close."""
        ...


class UrllibTransport:
    """Default :class:`Transport` over the standard library."""

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, bytes]:
        """
        Perform a unary HTTP request.

        :param method: HTTP verb, e.g. ``"POST"``.
        :param url: Absolute URL.
        :param headers: Request headers.
        :param body: Raw request body, or ``None``.
        :param timeout: Socket timeout in seconds.
        :returns: ``(status_code, response_body)``; HTTP error
            responses are returned (not raised) so callers can inspect
            the status.
        :raises SwarmSessionError: On a transport-level failure (no
            HTTP response at all).
        """
        req = urllib.request.Request(
            url, data=body, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SwarmSessionError(f'{method} {url} failed: {exc}') from exc

    def iter_lines(
        self, url: str, *, headers: dict[str, str], read_timeout: float
    ) -> Iterator[str]:
        """
        Open a streaming GET and yield decoded lines.

        :param url: Absolute stream URL.
        :param headers: Request headers.
        :param read_timeout: Per-read socket timeout; must exceed the
            server heartbeat cadence.
        :returns: Iterator of decoded (utf-8, replace) lines.
        :raises SwarmSessionError: If the stream cannot be opened.
        """
        req = urllib.request.Request(url, headers=headers, method='GET')
        try:
            resp = urllib.request.urlopen(req, timeout=read_timeout)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SwarmSessionError(
                f'could not open stream {url}: {exc}'
            ) from exc
        with resp:
            for raw in resp:
                yield raw.decode('utf-8', errors='replace')


@dataclass(frozen=True)
class _StreamEvent:
    """Internal reader-thread → waiter message.

    :param kind: ``"status"`` (a session.status edge), ``"reply"`` (an
        assistant message captured from the stream), ``"done"``
        (``[DONE]``), ``"closed"`` (stream ended), or ``"error"``
        (reader failure).
    :param status: The ``session.status`` value for a ``status`` event.
    :param error: Failure detail (``failed`` status, or reader error).
    :param response_id: The ``session.status`` event's ``response_id``.
        A native-terminal turn emits a PREMATURE settle-``idle`` with
        ``response_id=None`` before the real work; the true completion
        ``idle`` carries the turn's actual response id. Gating terminal
        idle on a non-null ``response_id`` ignores the settle edge.
    :param reply: For a ``reply`` event, the assistant message text.
    """

    kind: str
    status: str | None = None
    error: str | None = None
    response_id: str | None = None
    reply: str | None = None


class SwarmSessionClient:
    """
    Create and drive managed swarm sessions over the Omnigent API.

    :param server_url: Base URL of the Omnigent server, e.g.
        ``"http://localhost:6767"``.
    :param token: Optional bearer token for an authenticated server.
    :param transport: Injectable HTTP transport (defaults to
        :class:`UrllibTransport`); tests pass a fake.
    :param request_timeout: Socket timeout for unary requests.
    :param stream_read_timeout: Per-read timeout on the SSE stream.
    """

    def __init__(
        self,
        server_url: str,
        *,
        token: str | None = None,
        transport: Transport | None = None,
        request_timeout: float = _DEFAULT_REQUEST_TIMEOUT_S,
        stream_read_timeout: float = _DEFAULT_STREAM_READ_TIMEOUT_S,
        auto_approve: bool = True,
    ) -> None:
        #: Answer interactive prompts instead of failing the turn.
        #: On by default: the microVM is the containment boundary, so
        #: a prompt inside one carries no safety information — it is
        #: pure latency. Off (``--no-auto-approve``) to watch what
        #: agents actually ask for.
        self._auto_approve = auto_approve
        self._base = server_url.rstrip('/')
        self._token = token
        self._transport: Transport = transport or UrllibTransport()
        self._request_timeout = request_timeout
        self._stream_read_timeout = stream_read_timeout

    # ── HTTP helpers ──────────────────────────────────────────────

    def _headers(self, *, json_body: bool) -> dict[str, str]:
        headers: dict[str, str] = {'Accept': 'application/json'}
        if json_body:
            headers['Content-Type'] = 'application/json'
        if self._token:
            headers['Authorization'] = f'Bearer {self._token}'
        return headers

    def _json(
        self,
        method: str,
        path: str,
        body: dict[str, object] | None,
        *,
        ok: tuple[int, ...] = (200, 201, 202),
        timeout: float | None = None,
    ) -> dict[str, object]:
        """
        Perform a JSON request and decode the response body.

        :param method: HTTP verb.
        :param path: Path under the base, e.g. ``"/v1/sessions"``.
        :param body: JSON-serializable body, or ``None``.
        :param ok: Status codes accepted as success.
        :param timeout: Socket timeout override, or ``None`` for the
            client default. A turn post needs a long override because
            the ``/events`` endpoint long-polls while a managed session
            provisions — it returns only once the runner binds.
        :returns: The decoded JSON object (``{}`` for an empty body).
        :raises SwarmSessionError: On a non-``ok`` status.
        """
        raw = json.dumps(body).encode('utf-8') if body is not None else None
        status, data = self._transport.request(
            method,
            f'{self._base}{path}',
            headers=self._headers(json_body=body is not None),
            body=raw,
            timeout=timeout if timeout is not None else self._request_timeout,
        )
        if status not in ok:
            detail = data.decode('utf-8', errors='replace')[:500]
            raise SwarmSessionError(
                f'{method} {path} returned {status}: {detail}'
            )
        if not data:
            return {}
        decoded = json.loads(data)
        if not isinstance(decoded, dict):
            raise SwarmSessionError(
                f'{method} {path} returned non-object JSON'
            )
        return decoded

    # ── Session lifecycle ─────────────────────────────────────────

    def create(
        self,
        *,
        agent_id: str,
        workspace: str | None = None,
        parent_session_id: str | None = None,
        title: str | None = None,
        terminal_launch_args: list[str] | None = None,
        model_override: str | None = None,
        reasoning_effort: str | None = None,
    ) -> str:
        """
        Create a top-level managed (microVM) session.

        :param agent_id: The registered agent to bind, e.g.
            ``"ag_abc123"``.
        :param workspace: Optional workspace — for a swarm mount, the
            ``git@sbxmount:<path>#<rw|ro>`` sentinel the launcher reads.
        :param parent_session_id: Optional parent for tree linkage.
            NOTE: setting this makes the session inherit the parent's
            runner (co-location) and skips microVM provisioning — omit
            it for an isolated per-agent VM.
        :param title: Optional session label.
        :param terminal_launch_args: Optional pass-through CLI args for
            a native terminal harness (claude/codex), e.g.
            ``["--permission-mode", "auto"]`` so a headless agent
            auto-approves every tool instead of blocking on a prompt.
        :param model_override: Optional per-session model to pin, e.g.
            ``"claude-sonnet-5"`` or an agy ``"gemini-3.5-*"`` label.
            The server applies it the way Polly applies a per-dispatch
            model: native harnesses receive it as ``--model`` at launch,
            SDK harnesses via ``HARNESS_<H>_MODEL``. ``None`` = use the
            agent spec's own model.
        :param reasoning_effort: Optional per-session reasoning-effort
            hint — one of ``low``, ``medium``, ``high``, ``xhigh``, or
            ``max``. Honored by the Claude harnesses; agy has no effort
            knob and ignores it (informational). ``None`` = the default.
        :returns: The new session (conversation) id.
        :raises SwarmSessionError: On a create failure.
        """
        body: dict[str, object] = {
            'agent_id': agent_id,
            'host_type': 'managed',
        }
        if workspace is not None:
            body['workspace'] = workspace
        if parent_session_id is not None:
            body['parent_session_id'] = parent_session_id
        if title is not None:
            body['title'] = title
        if terminal_launch_args is not None:
            body['terminal_launch_args'] = terminal_launch_args
        if model_override is not None:
            body['model_override'] = model_override
        if reasoning_effort is not None:
            body['reasoning_effort'] = reasoning_effort
        data = self._json('POST', '/v1/sessions', body)
        session_id = data.get('id')
        if not isinstance(session_id, str) or not session_id:
            raise SwarmSessionError('create response had no session id')
        return session_id

    def get_status(self, session_id: str) -> dict[str, object]:
        """
        Fetch a session's snapshot (status, host, workspace, …).

        :param session_id: The session id.
        :returns: The decoded session snapshot.
        :raises SwarmSessionError: On a lookup failure.
        """
        return self._json('GET', f'/v1/sessions/{session_id}', None)

    def wait_for_terminal_ready(
        self,
        session_id: str,
        *,
        terminal_name: str = 'antigravity',
        timeout: float = _DEFAULT_TERMINAL_READY_TIMEOUT_S,
        settle: float = _DEFAULT_TERMINAL_SETTLE_S,
        poll: float = _DEFAULT_TERMINAL_POLL_S,
    ) -> bool:
        """
        Block until a native-TUI terminal is launched, then settle.

        A native-TUI harness (agy) auto-creates its terminal when the
        session is created and needs it fully mounted before the first
        turn is typed in: driving now races the TUI's cold start,
        the pasted message misses the bridge's short render window, and
        the turn is dropped before it submits. The swarm avoids this by
        creating every agent up front and driving minutes later (warm by
        then); a DAG node creates-and-drives back to back, so warm the
        terminal here first.

        Polls ``GET …/resources/terminals`` until a terminal whose
        ``metadata.terminal_name`` matches *terminal_name* reports
        ``running``, then pauses *settle* for the composer to become
        paste-ready. Best-effort: returns ``False`` after *timeout* even
        if never seen (the turn still runs — the terminal may just have
        no queryable resource) rather than blocking a whole run.

        :param session_id: The session whose terminal to await.
        :param terminal_name: The native terminal to match.
        :param timeout: Max seconds to await the running terminal.
        :param settle: Pause once running, for paste-readiness.
        :param poll: Seconds between polls.
        :returns: ``True`` if the terminal was seen running (then
            settled), ``False`` on timeout.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._terminal_running(session_id, terminal_name):
                time.sleep(settle)
                return True
            time.sleep(poll)
        return False

    def _terminal_running(
        self, session_id: str, terminal_name: str
    ) -> bool:
        """
        Whether a named native terminal exists and reports running.

        :param session_id: The session to query.
        :param terminal_name: The ``metadata.terminal_name`` to match.
        :returns: ``True`` when a matching, running terminal is present;
            ``False`` on any lookup failure (treated as not-ready).
        """
        try:
            data = self._json(
                'GET',
                f'/v1/sessions/{session_id}/resources/terminals',
                None,
            )
        except SwarmSessionError:
            return False
        entries = data.get('data')
        if not isinstance(entries, list):
            return False
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            meta = entry.get('metadata')
            if not isinstance(meta, dict):
                continue
            if (
                meta.get('terminal_name') == terminal_name
                and meta.get('running')
            ):
                return True
        return False

    def list_builtin_agents(self) -> list[dict[str, object]]:
        """
        List the server's built-in agents (``GET /v1/agents``).

        Used by the swarm's fail-loud agy gate to resolve a bound
        agent's harness. Returns the catalog page's ``data`` items that
        are JSON objects (each carries ``id``, ``name``, ``harness``).

        :returns: The built-in agent objects (empty if the page has no
            usable data array).
        :raises SwarmSessionError: On a lookup failure.
        """
        data = self._json('GET', '/v1/agents?limit=1000', None)
        items = data.get('data', [])
        if not isinstance(items, list):
            return []
        return [a for a in items if isinstance(a, dict)]

    def read_items(
        self, session_id: str, *, tail: int | None = None
    ) -> list[dict[str, object]]:
        """
        Read a session's conversation items in chronological order.

        :param session_id: The session id.
        :param tail: Optional cap on the most-recent items to return.
        :returns: Items oldest-first (the API returns newest-first).
        :raises SwarmSessionError: On a read failure.
        """
        path = f'/v1/sessions/{session_id}/items?order=desc'
        if tail is not None:
            path += f'&limit={int(tail)}'
        data = self._json('GET', path, None)
        items = data.get('data', [])
        if not isinstance(items, list):
            raise SwarmSessionError('items response had no data array')
        return list(reversed(items))

    def wait_for_plan_approval(
        self,
        session_id: str,
        *,
        approval_phrases: tuple[str, ...] = _PLAN_APPROVAL_PHRASES,
        poll: float = 5.0,
        idle_timeout: float = _DEFAULT_PLAN_APPROVAL_IDLE_S,
    ) -> str:
        """
        Block until a human approves the plan in the session, then
        return the approved plan text.

        Interactive planning: the planner asks clarifying questions a
        human answers directly in the Omnigent UI, iterating until the
        plan is complete, and then posts an approval message (an
        ``APPROVED`` phrase). This polls the item store for that
        approval and returns the plan the human approved — the last
        assistant message before the approval turn. ONLY the planning
        phase blocks on a human this way; nothing else does.

        Only a turn posted AFTER this call begins, whose whole text is
        an approval phrase and nothing else, releases the gate. Both
        conditions are load-bearing: the runner's own instruction is a
        ``role: user`` message carrying the entire brief, so a brief
        that merely uses the word "approved" in passing would otherwise
        self-approve on the first poll — observed live, and silent.

        The cap is on SILENCE, not on the review. Every new turn in
        the session restarts it, because this is the one stage paced by
        a human and the plan reaches the run state only when the stage
        COMPLETES — so a cap on total elapsed time ends a live review
        mid-sentence and takes every question, answer and draft with it.
        A cap still exists so an abandoned run cannot hold a sandbox
        forever, but only genuine silence spends it.

        :param session_id: The planner session to watch.
        :param approval_phrases: Uppercase phrases counting as
            approval, matched against the WHOLE turn.
        :param poll: Seconds between item-store polls.
        :param idle_timeout: Cap on how long the session may sit SILENT.
            Every new turn restarts it.
        :returns: The approved plan text.
        :raises SwarmSessionError: If the session sits silent for
            *idle_timeout* with no approval.
        """
        # Everything already in the session belongs to the runner or to
        # the planner's own turn, so none of it can be the approval.
        try:
            baseline = {
                item_id
                for item in self.read_items(
                    session_id, tail=_PLAN_APPROVAL_TAIL
                )
                if isinstance(item_id := item.get('id'), str)
            }
        except SwarmSessionError:
            baseline = set()
        # Activity is tracked separately from *baseline*, which is a
        # different question: baseline is the frozen set that can never
        # BE the approval. Folding into it happens to behave the same
        # today only because the approval check runs before the fold on
        # every pass — one set serving two rules, kept correct by
        # statement order. Two sets, so neither rule can drift.
        seen = set(baseline)
        deadline = time.monotonic() + idle_timeout
        while time.monotonic() < deadline:
            try:
                items = self.read_items(
                    session_id, tail=_PLAN_APPROVAL_TAIL
                )
            except SwarmSessionError:
                items = []
            plan = _approved_plan_text(
                items, approval_phrases, seen_ids=baseline
            )
            if plan is not None:
                return plan
            ids = {
                item_id
                for item in items
                if isinstance(item_id := item.get('id'), str)
            }
            if not ids <= seen:
                # The conversation moved: a question, an answer, a
                # redraft. The human is working — start the cap over.
                seen |= ids
                deadline = time.monotonic() + idle_timeout
            time.sleep(poll)
        raise SwarmSessionError(
            f'plan approval for {session_id} never arrived: the session '
            f'has been silent for {idle_timeout:.0f}s'
        )

    def wait_for_session_idle(
        self,
        session_id: str,
        *,
        timeout: float = _PLAN_IDLE_TIMEOUT_S,
        stable_window: float = _PLAN_IDLE_STABLE_S,
        poll: float = _PLAN_IDLE_POLL_S,
    ) -> bool:
        """
        Block until a session has held ``idle`` for *stable_window*.

        Lets a conversational native-TUI planner (agy) fully settle — it
        fires premature/transient ``idle`` beats and its reply lags — so
        a following :meth:`read_latest_reply` reads the true final
        message, not a mid-flight one. Bounded by *timeout*.

        :param session_id: The session to watch.
        :param timeout: Hard cap on the total wait.
        :param stable_window: Seconds ``idle`` must persist to count as
            settled.
        :param poll: Seconds between status polls.
        :returns: ``True`` if it settled, ``False`` on *timeout*.
        """
        deadline = time.monotonic() + timeout
        idle_since: float | None = None
        while time.monotonic() < deadline:
            try:
                status = self.get_status(session_id).get('status')
            except SwarmSessionError:
                status = None
            now = time.monotonic()
            if status == _STATUS_IDLE:
                if idle_since is None:
                    idle_since = now
                elif now - idle_since >= stable_window:
                    return True
            else:
                idle_since = None
            time.sleep(poll)
        return False

    def read_latest_reply(self, session_id: str) -> str:
        """
        The session's most recent assistant reply text (or ``""``).

        Reads the SETTLED item store, not a live turn stream, so it is
        robust to a native-TUI reply that lags its turn. Pair with
        :meth:`wait_for_session_idle` to capture a planner's final
        message.

        :param session_id: The session to read.
        :returns: The latest assistant text, or ``""`` if none.
        """
        return self._read_latest_reply(session_id)

    def read_recent_reply_text(
        self, session_id: str, *, count: int = 6
    ) -> str:
        """
        Concatenated text of the most recent assistant replies.

        Joins the last *count* settled assistant messages in
        chronological order, so a marker parse (e.g. ``VERDICT:``) over
        the result still finds the token when a short epilogue message
        trails the one that carried it (the marker in the earlier
        message wins as the last recognized token). Reads the settled
        item store, so it is robust to a native-TUI reply that lags its
        turn — used to capture an agy reviewer's verdict.

        :param session_id: The session to read.
        :param count: Number of trailing assistant messages to join.
        :returns: The joined recent reply text, or ``""`` if none.
        """
        try:
            items = self.read_items(session_id, tail=max(count * 2, 12))
        except SwarmSessionError:
            return ''
        msgs = _assistant_reply_items(items)
        return '\n'.join(text for _, text in msgs[-count:])

    def session_host_id(self, session_id: str) -> str | None:
        """
        The host id backing *session_id*, or ``None``.

        :param session_id: The session to describe.
        :returns: The host id, or ``None`` when the session reports no
            host (an unprovisioned or already-torn-down session).
        :raises SwarmSessionError: On a read failure.
        """
        body = self._json('GET', f'/v1/sessions/{session_id}', None)
        host = body.get('host_id') if isinstance(body, dict) else None
        return host if isinstance(host, str) and host else None

    def host_name(self, host_id: str) -> str | None:
        """
        The display name of *host_id*, which names its sandbox.

        The launcher names a managed sandbox after the host name the
        server chose, so this is how a session resolves to the
        microVM behind it. Read through the API rather than derived
        from the id: the naming rule is the server's to change.

        :param host_id: The host to look up.
        :returns: The host's name, e.g. ``"managed-cb683c32"``, or
            ``None`` when the host is not listed.
        :raises SwarmSessionError: On a read failure.
        """
        body = self._json('GET', '/v1/hosts', None)
        hosts = body.get('hosts') if isinstance(body, dict) else None
        if not isinstance(hosts, list):
            return None
        for host in hosts:
            if not isinstance(host, dict) or host.get('host_id') != host_id:
                continue
            name = host.get('name')
            return name if isinstance(name, str) and name else None
        return None

    def dispose(self, session_id: str) -> None:
        """
        Delete a session, tearing down its microVM.

        Idempotent: a ``404`` (already gone) is treated as success.

        :param session_id: The session id.
        :raises SwarmSessionError: On a non-404 delete failure.
        """
        self._json(
            'DELETE',
            f'/v1/sessions/{session_id}',
            None,
            ok=(200, 202, 204, 404),
        )

    def resolve_elicitation(
        self,
        session_id: str,
        elicitation_id: str,
        *,
        action: str = 'accept',
        content: dict[str, object] | None = None,
    ) -> None:
        """
        Answer one interactive prompt a session is blocked on.

        The launcher has always been able to SEE a prompt — it reads
        ``pending_elicitations`` off the snapshot and reports the turn
        as asking — but never to answer one, so every prompt cost a
        turn. This is the missing half.

        The endpoint is absent from the server's OpenAPI but real:
        probed live, an undefined route 404s while this one 422s for a
        missing ``action``. It shares its implementation with the
        ``approval`` event, so a verdict delivered here resolves a
        server-parked harness Future exactly as one from the UI does —
        which is what makes it work for native-TUI harnesses, whose
        permission hooks park precisely such a Future.

        A 404 is SUCCESS. It means nothing is waiting on an answer any
        more, which is the state this call exists to reach; raising
        would fail a turn that is free to continue.

        :param session_id: Session holding the prompt.
        :param elicitation_id: Correlation id from
            :func:`_elicitation_ids`.
        :param action: ``'accept'``, ``'decline'`` or ``'cancel'``.
        :param content: Optional MCP result payload for prompts that
            ask for a value rather than a yes/no.
        :raises ValueError: On an action the server does not accept.
        :raises SwarmSessionError: If the verdict is refused.
        """
        if action not in _ELICITATION_ACTIONS:
            raise ValueError(
                f'elicitation action must be one of '
                f'{", ".join(_ELICITATION_ACTIONS)}, got {action!r}'
            )
        body: dict[str, object] = {'action': action}
        if content is not None:
            body['content'] = content
        self._json(
            'POST',
            f'/v1/sessions/{session_id}/elicitations/'
            f'{elicitation_id}/resolve',
            body,
            ok=(200, 201, 202, 204, 404),
        )

    def _approve_pending(self, session_id: str) -> int:
        """
        Answer every prompt a session is currently blocked on.

        Re-reads the snapshot rather than trusting the classification
        that got here, so the ids answered are the ones outstanding NOW.

        Every approval is announced. An agent asking is worth seeing
        even when the answer is always yes — that is how the swapfile
        prompt was found at all, and it is the only way a genuinely new
        class of prompt will surface once they stop failing turns.

        :param session_id: The session holding the prompts.
        :returns: How many were answered. Zero means nothing here could
            be — an unreadable session, or prompts carrying no id — and
            the caller should fail the turn as it always did.
        """
        try:
            snapshot = self.get_status(session_id)
        except SwarmSessionError:
            return 0
        pending = snapshot.get(_ELICITATIONS_KEY)
        ids = _elicitation_ids(pending)
        if not ids:
            return 0
        question = _elicitation_text(pending)
        answered = 0
        for elicitation_id in ids:
            try:
                self.resolve_elicitation(session_id, elicitation_id)
            except SwarmSessionError as exc:
                # Loud, and not fatal: the others may still go through,
                # and failing to answer one lands on the old path.
                click.echo(
                    f'[elicit] {session_id}: could NOT auto-approve '
                    f'{elicitation_id} — {exc}'
                )
                continue
            answered += 1
        if answered:
            click.echo(
                f'[elicit] {session_id}: auto-approved {answered} '
                f'prompt(s) — {question}'
            )
        return answered

    # ── The push-based turn primitive ─────────────────────────────

    def send_and_wait(
        self,
        session_id: str,
        message: str,
        *,
        timeout: float = _DEFAULT_TURN_TIMEOUT_S,
        connect_timeout: float = _DEFAULT_CONNECT_TIMEOUT_S,
        idle_reply_grace: float = _IDLE_REPLY_GRACE_S,
        max_resubmits: int = 0,
        stall_grace_s: float = _DEFAULT_STALL_GRACE_S,
        redeliver_delay_s: float = 0.0,
    ) -> SwarmTurnResult:
        """
        Post a user turn and block until it reaches a terminal status.

        One delivery (:meth:`_deliver_once`) subscribes to the SSE
        stream, posts the turn, and returns its REAL completion —
        ``failed`` or a terminal ``idle`` (see :meth:`_await_terminal`).
        Claude native's real terminal idle carries a ``response_id``;
        its id-less idles (the pre-work settle and mid-turn quiescence
        lulls) are skipped, so an intermediate message is never mistaken
        for the reply. agy's terminal idle has no ``response_id`` and
        its reply lags, so a prior ``response.completed`` marks its idle
        terminal and a short grace awaits the lagging reply. The reply
        text is captured live off the SAME stream.

        When *max_resubmits* > 0, a delivery that fails or stalls BEFORE
        the turn ever starts is re-delivered from scratch (fresh
        subscribe + post). That is the dropped-submit signature: a
        native TUI (agy) still cold-starting swallowed the pasted
        message before it rendered, so the turn never began. By the next
        attempt the TUI is warm and the re-paste lands. Safe because the
        agy bridge clears the composer before each paste, so a
        re-delivery never doubles the message. A failure AFTER the turn
        started is a real failure and is returned as-is.

        :param session_id: The target session.
        :param message: The user message text.
        :param timeout: Max seconds to await turn completion.
        :param connect_timeout: Max seconds to await the subscription
            acknowledgment.
        :param max_resubmits: Max extra re-deliveries when the turn
            fails or stalls before starting. Zero keeps fail-fast.
        :param stall_grace_s: Pre-start silence that ends a non-final
            delivery early so it can be re-delivered rather than block.
        :param redeliver_delay_s: Seconds to pause before each
            re-delivery. A cold native TUI (agy) needs tens of seconds
            to finish its cascade/index startup before it can render a
            paste; back-to-back re-deliveries all lose that race, so
            this spaces them to let the TUI warm between attempts.
        :returns: The terminal :class:`SwarmTurnResult`, ``reply`` set
            to this turn's assistant text.
        :raises SwarmSessionError: On subscribe/post failure, timeout,
            or a stream that closes before the turn completes.
        """
        attempts = max(1, max_resubmits + 1)
        result = SwarmTurnResult(_STATUS_FAILED, None)
        for index in range(attempts):
            final = index == attempts - 1
            result, started = self._deliver_once(
                session_id,
                message,
                timeout=timeout,
                connect_timeout=connect_timeout,
                idle_reply_grace=idle_reply_grace,
                # Non-final attempts cut a pre-start stall short so the
                # turn can be re-delivered; the last keeps the full
                # timeout / raise contract.
                stall_grace_s=None if final else stall_grace_s,
            )
            # Re-deliver whenever the attempt yielded NOTHING — not
            # only when it errored. An orphaned agy turn (its cascade
            # rotated away mid-flight) can also come back nominally OK
            # with an EMPTY reply, which is indistinguishable from a
            # dropped paste and equally worth one more delivery. A
            # genuine failure AFTER real output still surfaces at once.
            got_output = bool(result.reply and result.reply.strip())
            if final or got_output or (started and not result.ok):
                return result
            # Failed/stalled BEFORE the turn started: the paste was
            # dropped into a still-cold native TUI. Give it time to warm
            # (finish cascade/index startup) before re-delivering the
            # whole turn (fresh subscribe + post). The agy bridge clears
            # the composer before each paste, so this never doubles the
            # message.
            if redeliver_delay_s > 0:
                time.sleep(redeliver_delay_s)
        return result

    def _deliver_once(
        self,
        session_id: str,
        message: str,
        *,
        timeout: float,
        connect_timeout: float,
        idle_reply_grace: float,
        stall_grace_s: float | None,
    ) -> tuple[SwarmTurnResult, bool]:
        """
        One subscribe → post → await cycle for a single turn delivery.

        Subscribes to the SSE stream and waits for the ready heartbeat
        BEFORE posting (so no turn edge is missed), posts the turn, then
        awaits its terminal status. Reports whether the turn ever
        started so :meth:`send_and_wait` re-delivers a dropped submit.

        :returns: ``(result, started)``.
        :raises SwarmSessionError: On subscribe/post failure, timeout,
            or a stream that closes before the turn completes.
        """
        events: queue.Queue[_StreamEvent] = queue.Queue()
        subscribed = threading.Event()
        stop = threading.Event()
        url = f'{self._base}/v1/sessions/{session_id}/stream'
        reader = threading.Thread(
            target=self._run_stream_reader,
            args=(url, events, subscribed, stop),
            name=f'swarm-sse-{session_id}',
            daemon=True,
        )
        reader.start()
        try:
            if not subscribed.wait(timeout=connect_timeout):
                raise SwarmSessionError(
                    f'stream for {session_id} did not acknowledge '
                    f'subscription within {connect_timeout}s'
                )
            # The post long-polls while a managed session provisions
            # (returns only when the runner binds), so give it the full
            # turn budget rather than the short unary default.
            self._post_message(session_id, message, timeout=timeout)
            result, started = self._await_terminal(
                session_id,
                events,
                timeout,
                idle_reply_grace,
                stall_grace_s=stall_grace_s,
            )
        finally:
            stop.set()
        if result.ok and not result.reply:
            # Stream carried no reply text (rare): fall back to a single
            # item-store read now that the turn has completed.
            fallback = self._read_latest_reply(session_id)
            if fallback:
                return (
                    SwarmTurnResult(result.status, result.error, fallback),
                    started,
                )
        return result, started

    def read_assistant_replies(
        self, session_id: str, *, tail: int = 60
    ) -> list[str]:
        """
        Every assistant message in a session, oldest first.

        Lets a caller judge a reply against the rest of the
        conversation rather than trusting the last one blind — see
        :func:`sbx_omnigent.runner.select_plan_of_record`.

        :param session_id: The session to read.
        :param tail: How many trailing items to scan.
        :returns: The assistant message texts, oldest first (``[]`` when
            the session cannot be read).
        """
        try:
            items = self.read_items(session_id, tail=tail)
        except SwarmSessionError:
            return []
        return [text for _id, text in _assistant_reply_items(items)]

    def read_transcript(
        self, session_id: str, *, tail: int = 400
    ) -> list[tuple[str, str]]:
        """
        The session's human/agent MESSAGES, oldest first.

        Only ``message`` items are returned — never function calls or
        their output. That is deliberate: a tool result can carry file
        contents, environment, or command output from inside the VM,
        and a caller may commit this to a repository. What the human and
        the agent SAID to each other is the record worth keeping; the
        rest is machinery.

        :param session_id: The session to read.
        :param tail: How many trailing items to scan.
        :returns: ``(role, text)`` pairs oldest-first (``[]`` when the
            session cannot be read).
        """
        try:
            items = self.read_items(session_id, tail=tail)
        except SwarmSessionError:
            return []
        out: list[tuple[str, str]] = []
        for item in items:
            if item.get('type') != 'message':
                continue
            role = item.get('role')
            text = _item_message_text(item)
            if isinstance(role, str) and text.strip():
                out.append((role, text))
        return out

    def _read_latest_reply(self, session_id: str) -> str:
        """
        Read the latest assistant reply from the item store (fallback).

        :param session_id: The session to read.
        :returns: The most recent assistant text, or ``""``.
        """
        try:
            items = self.read_items(session_id, tail=12)
        except SwarmSessionError:
            return ''
        msgs = _assistant_reply_items(items)
        return msgs[-1][1] if msgs else ''

    def _post_message(
        self, session_id: str, message: str, *, timeout: float | None = None
    ) -> None:
        """Post a single user message to the session's event queue."""
        self._json(
            'POST',
            f'/v1/sessions/{session_id}/events',
            {
                'type': _MESSAGE_TYPE,
                'data': {
                    'role': 'user',
                    'content': [{'type': 'input_text', 'text': message}],
                },
            },
            timeout=timeout,
        )

    def _run_stream_reader(
        self,
        url: str,
        events: queue.Queue[_StreamEvent],
        subscribed: threading.Event,
        stop: threading.Event,
    ) -> None:
        """
        Reader thread: parse the SSE stream into ``_StreamEvent``s.

        The first frame of any kind (the ready heartbeat) sets
        *subscribed*. ``session.status`` frames become ``status`` events
        (carrying ``response_id``); the assistant's message — delivered
        on the stream as ``response.output_text.delta`` chunks and a
        final ``response.output_item.done`` message item — becomes
        ``reply`` events, so the turn's reply is captured live rather
        than polled out of the lagging item store. ``[DONE]`` / stream
        end / an exception become ``done`` / ``closed`` / ``error``.
        *subscribed* is always set on exit so the waiter can never hang
        on a reader that failed before its first frame.
        """
        delta_buf: list[str] = []
        try:
            lines = self._transport.iter_lines(
                url,
                headers=self._headers(json_body=False),
                read_timeout=self._stream_read_timeout,
            )
            for frame in parse_sse(lines):
                if stop.is_set():
                    return
                subscribed.set()
                if frame.data == _SSE_DONE:
                    events.put(_StreamEvent('done'))
                    return
                self._dispatch_frame(frame, events, delta_buf)
        except SwarmSessionError as exc:
            events.put(_StreamEvent('error', error=str(exc)))
        except Exception as exc:  # reader must not crash silently
            events.put(_StreamEvent('error', error=str(exc)))
        finally:
            subscribed.set()
            events.put(_StreamEvent('closed'))

    @staticmethod
    def _dispatch_frame(
        frame: _SseFrame,
        events: queue.Queue[_StreamEvent],
        delta_buf: list[str],
    ) -> None:
        """
        Classify one SSE frame into ``status`` / ``reply`` events.

        :param frame: The parsed SSE frame.
        :param events: Queue the reader emits ``_StreamEvent``s to.
        :param delta_buf: Mutable accumulator for streamed text deltas,
            used as a reply fallback when the final message item carries
            no content.
        """
        try:
            payload = json.loads(frame.data) if frame.data else {}
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        event = _stream_event_from_payload(payload, delta_buf)
        if event is not None:
            events.put(event)

    def _await_terminal(  # noqa: C901
        self,
        session_id: str,
        events: queue.Queue[_StreamEvent],
        timeout: float,
        idle_reply_grace: float = _IDLE_REPLY_GRACE_S,
        *,
        stall_grace_s: float | None = None,
    ) -> tuple[SwarmTurnResult, bool]:
        """
        Consume events until the turn's terminal edge (or timeout).

        Completion is a genuinely-terminal ``idle`` — never a premature
        settle-idle nor a mid-turn quiescence lull. Claude and agy
        signal completion differently, told apart by whether the turn's
        status edges carry a ``response_id`` (Claude tags them; agy
        never does):

        - Claude: the REAL terminal idle carries a ``response_id``.
          Every id-less idle on a Claude turn is NON-terminal and
          skipped — both the pre-work settle-idle and the mid-turn
          quiescence idles the server infers whenever the item stream
          briefly goes quiet during a tool round (Claude exposes no
          explicit "done"). Skipping them stops an intermediate
          assistant message from being captured as the reply.
        - agy: no status edge carries a ``response_id``; its terminal
          idle is id-less. It is recognized by a reply already captured
          (reply-before-idle) or a prior ``response.completed``
          (idle-before-reply) — then a short *idle_reply_grace* awaits
          the reply agy mirrors a beat later. A ``running`` edge during
          that grace means the turn resumed, cancelling it.

        Also reports whether the turn ever STARTED (produced a
        running / reply / completed / response_id signal). A caller
        re-delivering a dropped submit keys off this: a failure BEFORE
        any start is a lost paste (the message never left a cold native
        TUI's composer) worth a fresh delivery; a failure AFTER start is
        a real turn failure. When *stall_grace_s* is set, silence that
        long before any start is likewise surfaced as an un-started
        failure (rather than blocking to *timeout*) so the caller can
        re-deliver.

        :param idle_reply_grace: Max seconds to await a lagging reply
            after a terminal idle before returning without it.
        :param stall_grace_s: When set, seconds of pre-start silence
            that end the wait early as an un-started failure (for
            re-delivery); ``None`` keeps waiting to *timeout*.
        :returns: ``(result, started)``.
        :raises SwarmSessionError: On timeout, or a stream that closes
            before the turn reaches a terminal status.
        """
        deadline = time.monotonic() + timeout
        reply = ''
        saw_completed = False
        saw_response_id = False
        grace_deadline: float | None = None
        turn_started = False
        #: When the last real frame arrived. Reset ONLY by a frame,
        #: never by a confirmation poll, so it measures true silence —
        #: which is what the abandoned verdict is timed against.
        last_frame_at = time.monotonic()
        #: When to next poll the item store. Paced separately so
        #: repeated polls cannot keep pushing the silence clock forward.
        next_confirm_at = last_frame_at + _IDLE_CONFIRM_S
        stall_deadline = (
            last_frame_at + stall_grace_s
            if stall_grace_s is not None
            else None
        )
        approvals = 0
        while True:
            now = time.monotonic()
            if now >= deadline:
                raise SwarmSessionError(
                    f'turn on {session_id} did not complete within '
                    f'{timeout:.0f}s'
                )
            watch_stall = stall_deadline is not None and not turn_started
            # Suspended while a grace wait is pending: that path is
            # short and self-bounding, and letting this shorten its wait
            # would return before the grace had actually elapsed.
            watch_silence = turn_started and grace_deadline is None
            wait = deadline - now
            if grace_deadline is not None:
                wait = min(wait, max(0.0, grace_deadline - now))
            if watch_silence:
                wait = min(wait, max(0.0, next_confirm_at - now))
            if watch_stall:
                wait = min(wait, max(0.0, stall_deadline - now))
            try:
                event = events.get(timeout=wait)
                last_frame_at = time.monotonic()
                next_confirm_at = last_frame_at + _IDLE_CONFIRM_S
                if stall_grace_s is not None and not turn_started:
                    # Any frame is activity: the watchdog measures
                    # silence since the LAST one, not elapsed time since
                    # the post. Otherwise a cold TUI's early chatter
                    # burns the whole grace before the turn could start.
                    stall_deadline = time.monotonic() + stall_grace_s
            except queue.Empty:
                if grace_deadline is not None:
                    # Terminal idle already seen; the reply never caught
                    # up within the grace — return with what we have.
                    return SwarmTurnResult(_STATUS_IDLE, None, reply), True
                if watch_silence and time.monotonic() >= next_confirm_at:
                    silent = time.monotonic() - last_frame_at
                    state, settled = self._classify_settled(session_id)
                    if state == 'finished' and settled:
                        return (
                            SwarmTurnResult(
                                _STATUS_IDLE, None, settled or reply
                            ),
                            True,
                        )
                    if state == 'asking':
                        # No margin needed: an open prompt is not a slow
                        # turn, it is a stopped one. What CAN move it is
                        # a verdict, which this sends before treating it
                        # as fatal — the microVM is the containment
                        # boundary, so the prompt is pure latency.
                        if (
                            self._auto_approve
                            and approvals < _MAX_AUTO_APPROVALS_PER_TURN
                            and (answered := self._approve_pending(
                                session_id
                            ))
                        ):
                            approvals += answered
                            # The turn is moving again, so restart the
                            # silence clock: leaving it stale would have
                            # the very next poll re-classify a session
                            # that has only just been unblocked.
                            last_frame_at = time.monotonic()
                            next_confirm_at = (
                                last_frame_at + _IDLE_CONFIRM_S
                            )
                            continue
                        capped = (
                            self._auto_approve
                            and approvals >= _MAX_AUTO_APPROVALS_PER_TURN
                        )
                        error = (
                            f'{_ASKING_LOOP_ERROR} '
                            f'({_MAX_AUTO_APPROVALS_PER_TURN}). It asked'
                            if capped
                            else _ASKING_TURN_ERROR
                        )
                        return (
                            SwarmTurnResult(
                                _STATUS_FAILED,
                                f'{error}: {settled}',
                                reply,
                            ),
                            True,
                        )
                    if (
                        state == 'abandoned'
                        and silent >= _ABANDON_CONFIRM_S
                    ):
                        # The tool result was delivered and the model
                        # never continued. Waiting out the turn budget
                        # discovers exactly this, hours later.
                        return (
                            SwarmTurnResult(
                                _STATUS_FAILED,
                                _ABANDONED_TURN_ERROR,
                                reply,
                            ),
                            True,
                        )
                    # Still working (or not silent long enough to
                    # call it dead): pace the next poll WITHOUT
                    # touching last_frame_at, or the silence clock
                    # never advances.
                    next_confirm_at = time.monotonic() + _IDLE_CONFIRM_S
                    continue
                if watch_stall and time.monotonic() >= stall_deadline:
                    # Silent past the stall grace before any turn-start:
                    # the paste was dropped into a cold TUI. Surface an
                    # un-started failure so the caller can re-deliver.
                    return (
                        SwarmTurnResult(_STATUS_FAILED, None, reply),
                        False,
                    )
                raise SwarmSessionError(
                    f'turn on {session_id} did not complete within '
                    f'{timeout:.0f}s'
                ) from None
            if event.kind == 'completed':
                saw_completed = True
                turn_started = True
            elif event.kind == 'reply':
                if event.reply:
                    # Only real TEXT counts as a start. An EMPTY delta
                    # does NOT: agy emits one as its cold-start cascade
                    # rotates away, and treating that as a start disarms
                    # the stall watchdog below — the turn then blocks to
                    # the full timeout on a conversation the agent has
                    # already abandoned.
                    turn_started = True
                    reply = event.reply
                if grace_deadline is not None:
                    # The lagging reply after a terminal idle arrived.
                    return SwarmTurnResult(_STATUS_IDLE, None, reply), True
            elif event.kind == 'status':
                if event.status == _STATUS_FAILED:
                    return (
                        SwarmTurnResult(_STATUS_FAILED, event.error, reply),
                        turn_started,
                    )
                if event.response_id:
                    # Claude tags its status edges with a response_id;
                    # agy never does — so one marks a Claude turn.
                    saw_response_id = True
                # NB: a bare ``running`` edge does NOT count as started.
                # The server marks a turn ``running`` the moment it is
                # ACCEPTED — before a native TUI has actually submitted
                # the paste — so a turn that fails with only a running
                # edge (no reply/completed, no clean idle) is a dropped
                # submit worth re-delivering. Only real output
                # (reply/completed) or a terminal idle is a real start.
                if event.status == _STATUS_IDLE:
                    if event.response_id:
                        # Claude's REAL terminal idle carries an id.
                        return SwarmTurnResult(_STATUS_IDLE, None, reply), True
                    if saw_response_id:
                        # A Claude turn's id-less idle is a premature
                        # settle or a mid-turn quiescence lull (the item
                        # stream went briefly quiet in a tool round) —
                        # NOT terminal. Skip, or an intermediate message
                        # gets captured as this turn's reply. The
                        # id-bearing idle that ends the turn CAN go
                        # missing, but that is covered by the silence
                        # watchdog above rather than armed here: this
                        # idle is itself a frame, so the watchdog is
                        # already counting from it.
                        pass
                    elif reply:
                        # No response_id this turn (agy): the reply is
                        # in and this id-less idle is terminal.
                        return SwarmTurnResult(_STATUS_IDLE, None, reply), True
                    elif saw_completed:
                        # agy's terminal idle before its lagging reply —
                        # wait briefly for the mirrored reply.
                        grace_deadline = now + idle_reply_grace
                    # else: premature pre-work idle → skip.
                elif grace_deadline is not None:
                    # A running edge after a provisionally-graced
                    # id-less idle: the turn resumed, so that idle
                    # wasn't terminal. Reopen and keep waiting.
                    grace_deadline = None
            else:
                # done / closed / error: the stream ended. If we already
                # saw this turn's terminal idle (grace pending), the end
                # just means no lagging reply is coming — return it.
                # Otherwise disambiguate with one snapshot poll.
                if grace_deadline is not None:
                    return SwarmTurnResult(_STATUS_IDLE, None, reply), True
                return (
                    self._resolve_on_stream_end(session_id, event, reply),
                    turn_started,
                )

    def _classify_settled(self, session_id: str) -> tuple[str, str]:
        """
        Read the item store and say what a silent turn is doing.

        Status alone cannot answer this: the server infers ``idle``
        whenever the item stream goes quiet, so a ten-minute build looks
        exactly like a finished turn. The LAST ITEM is what separates
        them, and it separates four states, not two:

        =====================  ==================  ==================
        session                last item           meaning
        =====================  ==================  ==================
        pending elicitation    anything            **asking**
        ``running``            anything            working
        ``idle``               pending call        tool running
        ``idle``               assistant message   **finished**
        ``idle``               tool result         **abandoned**
        =====================  ==================  ==================

        The elicitation check comes FIRST, and deliberately does not
        look at status: a session reads ``running`` for as long as its
        modal is open, so every other signal here says "working" while
        nothing whatsoever is happening. Observed live on a coder that
        asked permission for a change it had already made and verified;
        without this the turn burns its entire budget in silence.

        The last row is a turn the model walked away from: its tool
        returned, the result was delivered, and nothing came back. It
        reads as "quiet" exactly like a working turn, which is why it
        used to cost the entire turn budget to discover.

        :param session_id: The session being waited on.
        :returns: ``(state, text)`` where *state* is ``'working'``,
            ``'finished'``, ``'abandoned'`` or ``'asking'``. *text* is
            the closing reply when finished, or the question when
            asking. An unreadable session is ``'working'``: failing to
            read a session is not evidence about its turn.
        """
        try:
            snapshot = self.get_status(session_id)
            asking = _elicitation_text(snapshot.get(_ELICITATIONS_KEY))
            if asking:
                return 'asking', asking
            if snapshot.get('status') != _STATUS_IDLE:
                return 'working', ''
            items = self.read_items(session_id, tail=_CONFIRM_TAIL_ITEMS)
        except SwarmSessionError:
            return 'working', ''
        if not items:
            return 'working', ''
        last = items[-1]
        kind = last.get('type')
        if kind == _MESSAGE_TYPE and last.get('role') == 'assistant':
            return 'finished', _item_message_text(last) or ''
        if kind == _FUNCTION_OUTPUT_TYPE:
            # The tool result was delivered and the model never spoke.
            return 'abandoned', ''
        # A pending function_call, or a user message: still in flight.
        return 'working', ''

    def _confirm_finished(self, session_id: str) -> str | None:
        """
        The final assistant text if a silent turn has ended, else
        ``None``.

        Thin view over :meth:`_classify_settled` for callers that only
        care whether the turn produced its reply.

        :param session_id: The session being waited on.
        :returns: The closing reply, or ``None``.
        """
        state, text = self._classify_settled(session_id)
        return text or None if state == 'finished' else None

    def _resolve_on_stream_end(
        self,
        session_id: str,
        event: _StreamEvent,
        reply: str,
    ) -> SwarmTurnResult:
        """Fall back to a snapshot when the stream ends mid-wait."""
        polled = self.get_status(session_id).get('status')
        if polled == _STATUS_IDLE:
            return SwarmTurnResult(_STATUS_IDLE, None, reply)
        if polled == _STATUS_FAILED:
            return SwarmTurnResult(_STATUS_FAILED, event.error, reply)
        raise SwarmSessionError(
            f'stream for {session_id} closed before the turn completed '
            f'(last snapshot status={polled!r})'
        )


# ── CLI (the coordinator invokes this via sys_os_shell) ───────────


def _resolve_message(message: str | None, message_file: str | None) -> str:
    """Read the turn text from ``--message`` or ``--message-file``."""
    if message is not None and message_file is not None:
        raise click.UsageError('pass only one of --message / --message-file')
    if message is not None:
        return message
    if message_file is not None:
        if message_file == '-':
            return sys.stdin.read()
        with open(message_file, encoding='utf-8') as fh:
            return fh.read()
    raise click.UsageError('one of --message / --message-file is required')


@click.group()
@click.option(
    '--server',
    envvar='OMNI_SERVER',
    default='http://localhost:6767',
    show_default=True,
    help='Omnigent server base URL.',
)
@click.option(
    '--token',
    envvar='OMNI_TOKEN',
    default=None,
    help='Bearer token for an authenticated server.',
)
@click.pass_context
def cli(ctx: click.Context, server: str, token: str | None) -> None:
    """Create and drive managed swarm sessions (trusted plane)."""
    ctx.obj = SwarmSessionClient(server, token=token)


@cli.command('create')
@click.option('--agent-id', required=True)
@click.option('--workspace', default=None, help='Mount sentinel or repo URL.')
@click.option('--parent', 'parent_session_id', default=None)
@click.option('--title', default=None)
@click.pass_obj
def _create(
    client: SwarmSessionClient,
    agent_id: str,
    workspace: str | None,
    parent_session_id: str | None,
    title: str | None,
) -> None:
    """Create a managed session; print its id."""
    click.echo(
        client.create(
            agent_id=agent_id,
            workspace=workspace,
            parent_session_id=parent_session_id,
            title=title,
        )
    )


@cli.command('send-and-wait')
@click.option('--session', 'session_id', required=True)
@click.option('--message', default=None, help='Turn text (inline).')
@click.option(
    '--message-file', default=None, help="Turn text file, or '-' for stdin."
)
@click.option('--timeout', type=float, default=_DEFAULT_TURN_TIMEOUT_S)
@click.pass_obj
def _send_and_wait(
    client: SwarmSessionClient,
    session_id: str,
    message: str | None,
    message_file: str | None,
    timeout: float,
) -> None:
    """Post a turn, wait for completion; print {status, error} JSON."""
    text = _resolve_message(message, message_file)
    result = client.send_and_wait(session_id, text, timeout=timeout)
    click.echo(json.dumps({'status': result.status, 'error': result.error}))
    if not result.ok:
        raise SystemExit(1)


@cli.command('read')
@click.option('--session', 'session_id', required=True)
@click.option('--tail', type=int, default=None)
@click.pass_obj
def _read(
    client: SwarmSessionClient, session_id: str, tail: int | None
) -> None:
    """Print a session's recent conversation items as JSON."""
    click.echo(json.dumps(client.read_items(session_id, tail=tail)))


@cli.command('status')
@click.option('--session', 'session_id', required=True)
@click.pass_obj
def _status(client: SwarmSessionClient, session_id: str) -> None:
    """Print a session's snapshot as JSON."""
    click.echo(json.dumps(client.get_status(session_id)))


@cli.command('dispose')
@click.option('--session', 'session_id', required=True)
@click.pass_obj
def _dispose(client: SwarmSessionClient, session_id: str) -> None:
    """Delete a session and tear down its microVM."""
    client.dispose(session_id)


def main() -> None:
    """Console entry point (``python -m sbx_omnigent.swarm_session``).

    Runs the swarm-session CLI group.
    """
    cli()


if __name__ == '__main__':
    main()
