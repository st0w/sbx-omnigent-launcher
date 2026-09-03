"""Unit tests for :mod:`sbx_omnigent.swarm_session`.

Hermetic — no live server. A fake :class:`Transport` feeds canned HTTP
responses and canned SSE lines through the REAL parser and the REAL
reader-thread / await state machine, including the load-bearing
subscribe-before-post ordering. Run:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import contextlib
import json
import threading
import time
import unittest
from collections.abc import Iterator
from unittest import mock

from sbx_omnigent import pipeline
from sbx_omnigent.runner import main as runner_main
from sbx_omnigent.swarm_session import (
    _DEFAULT_TERMINAL_SETTLE_S,
    _PLAN_APPROVAL_PHRASES,
    SwarmSessionClient,
    SwarmSessionError,
    _approved_plan_text,
    _assistant_reply_items,
    _elicitation_ids,
    _elicitation_text,
    _is_bare_approval,
    _item_message_text,
    parse_sse,
)

# What production accepts, and the only thing it accepts — pinned
# against the shipped constant by the test at the end of this file.
_APPROVE = ('APPROVED',)


def _msg(role: str, text: str) -> dict[str, object]:
    return {
        'type': 'message',
        'role': role,
        'content': [{'type': 'text', 'text': text}],
    }


class TestApprovedPlanText(unittest.TestCase):
    """Interactive-planning approval detection."""

    def test_none_until_human_approves(self) -> None:
        items = [
            _msg('user', 'Produce a design plan...'),
            _msg('assistant', 'PLAN v1\nQUESTIONS: is A-A valid?'),
            _msg('user', 'yes, A-A is valid'),
            _msg('assistant', 'PLAN v2 (final)'),
        ]
        # no approval yet
        self.assertIsNone(_approved_plan_text(items, _APPROVE))

    def test_returns_plan_before_approval(self) -> None:
        items = [
            _msg('user', 'Produce a design plan...'),
            _msg('assistant', 'PLAN v1\nQUESTIONS: is A-A valid?'),
            _msg('user', 'yes, A-A is valid'),
            _msg('assistant', 'PLAN v2 (final)'),
            _msg('user', 'APPROVED'),
        ]
        # approved plan = last assistant message before APPROVED
        got = _approved_plan_text(items, _APPROVE)
        self.assertEqual(got, 'PLAN v2 (final)')

    def test_prose_containing_the_word_is_not_approval(self) -> None:
        """Approval is the WHOLE turn; commentary never releases it."""
        items = [
            _msg('assistant', 'PLAN'),
            _msg('user', 'looks great, approved!'),
        ]
        self.assertIsNone(_approved_plan_text(items, _APPROVE))

    def test_runner_instruction_carrying_the_brief_is_not_approval(
        self,
    ) -> None:
        """
        The regression. The runner posts the planner's instruction --
        the whole task brief -- as a ``role: user`` message, so a brief
        that merely uses the word "approved" in passing used to release
        the gate on the first poll, before the human saw the questions.
        Observed live: a run self-approved and went straight to tests.
        """
        brief = (
            'Task:\nStage directives. plan (interactive; read-only). '
            'Do not seek APPROVED while open questions remain. '
            'tests: derives a failing suite from the approved plan. '
            'pick: both branches arrive consensus-approved.'
        )
        items = [
            _msg('user', brief),
            _msg('assistant', 'PLAN v1\nQUESTIONS: 1. ... 2. ...'),
        ]
        self.assertIsNone(_approved_plan_text(items, _APPROVE))

    def test_preexisting_approval_is_ignored(self) -> None:
        """A turn already present when the wait began cannot approve."""
        items = [
            {**_msg('user', 'APPROVED'), 'id': 'old'},
            {**_msg('assistant', 'PLAN v2'), 'id': 'plan'},
        ]
        self.assertIsNone(
            _approved_plan_text(items, _APPROVE, seen_ids={'old'})
        )
        # ...but the same turn approves when it arrives after the wait.
        self.assertIsNone(_approved_plan_text(items, _APPROVE))

    def test_approval_with_nothing_before_it_fails_closed(self) -> None:
        """An approval preceding any planner turn is not an approval."""
        items = [_msg('user', 'APPROVED')]
        self.assertIsNone(_approved_plan_text(items, _APPROVE))

    def test_bare_approval_still_releases_the_gate(self) -> None:
        """The gate must still open for a human who types the word."""
        for text in ('APPROVED', 'approved', '**APPROVED**', 'APPROVED.',
                     '  approved  ', '"APPROVED"'):
            with self.subTest(text=text):
                items = [
                    _msg('assistant', 'PLAN v2 (final)'),
                    _msg('user', text),
                ]
                self.assertEqual(
                    _approved_plan_text(items, _APPROVE), 'PLAN v2 (final)'
                )

    def test_is_bare_approval(self) -> None:
        for text in ('APPROVED', 'approved', '**APPROVED**', '"approved"',
                     'APPROVED!', '`approved`', ' APPROVED '):
            with self.subTest(ok=text):
                self.assertTrue(_is_bare_approval(text, _APPROVE))
        for text in ('looks great, approved!', 'consensus-approved',
                     'APPROVED - but change the schema',
                     'Do not seek APPROVED while questions remain',
                     'not approved', '',
                     # Retired spellings: one word, deliberately.
                     'PLAN COMPLETE', 'PLAN APPROVED'):
            with self.subTest(rejected=text):
                self.assertFalse(_is_bare_approval(text, _APPROVE))

    def test_the_shipped_gate_accepts_exactly_one_word(self) -> None:
        """What production accepts, pinned against the fixture above.

        Every other test here INJECTS its phrase tuple, so the matcher
        can be exercised generally — which means nothing would otherwise
        notice if the shipped default drifted. `PLAN COMPLETE` and
        `PLAN APPROVED` were once accepted and were retired: a reviewer
        has to be told what releases the gate, and extra spellings make
        that sentence longer without making the gate easier to open.
        """
        self.assertEqual(_PLAN_APPROVAL_PHRASES, ('APPROVED',))
        self.assertEqual(_APPROVE, _PLAN_APPROVAL_PHRASES)

    def test_the_planner_template_names_what_the_gate_accepts(self) -> None:
        """The guidance and the matcher must not drift apart.

        The reviewer reads the planner's message in the UI, not the
        runner's terminal, so the template is where the rule actually
        reaches a human. A template naming a phrase the gate refuses
        would strand the pipeline on a message the human was told to
        send.
        """
        prompt = pipeline.template_prompt('planner')

        self.assertIn('APPROVED', prompt)
        for retired in ('PLAN COMPLETE', 'PLAN APPROVED'):
            with self.subTest(retired=retired):
                self.assertNotIn(retired, prompt)

    def test_answers_are_not_mistaken_for_approval(self) -> None:
        items = [
            _msg('assistant', 'PLAN v1\nQUESTIONS: ...'),
            _msg('user', '1. yes 2. accept 3. enforce string'),
        ]
        self.assertIsNone(_approved_plan_text(items, _APPROVE))


def _assistant_item(item_id: str, text: str) -> dict[str, object]:
    """Build an assistant message item (as read_items yields them)."""
    return {
        'id': item_id,
        'type': 'message',
        'role': 'assistant',
        'content': [{'type': 'output_text', 'text': text}],
    }


def _sse(event: str, payload: dict[str, object]) -> list[str]:
    """Render one SSE frame as wire lines (event + data + blank)."""
    return [f'event: {event}', f'data: {json.dumps(payload)}', '']


def _status_frame(
    status: str,
    error: str | None = None,
    *,
    response_id: str | None = 'resp_1',
) -> list[str]:
    """A ``session.status`` frame.

    *response_id* defaults to a non-null id so an ``idle`` reads as a
    real turn completion; pass ``None`` to model the native-terminal
    premature settle-``idle``.
    """
    payload: dict[str, object] = {
        'type': 'session.status',
        'conversation_id': 'conv_x',
        'status': status,
        'response_id': response_id,
    }
    if error is not None:
        payload['error'] = error
    return _sse('session.status', payload)


def _reply_frame(text: str) -> list[str]:
    """A ``response.output_item.done`` assistant-message frame."""
    return _sse(
        'response.output_item.done',
        {
            'type': 'response.output_item.done',
            'item': {
                'id': 'msg_1',
                'response_id': 'resp_1',
                'type': 'message',
                'role': 'assistant',
                'content': [{'type': 'output_text', 'text': text}],
            },
        },
    )


def _completed_frame() -> list[str]:
    """A ``response.completed`` frame (agy's work-finished marker)."""
    return _sse('response.completed', {'type': 'response.completed'})


def _delta_frame(text: str, *, final: bool = True) -> list[str]:
    """A streamed ``response.output_text.delta`` frame."""
    return _sse(
        'response.output_text.delta',
        {
            'type': 'response.output_text.delta',
            'delta': text,
            'final': final,
        },
    )


class FakeTransport:
    """
    Scriptable transport recording requests and replaying SSE lines.

    :param responses: Maps ``"METHOD /path"`` (path without query) to a
        ``(status_code, json_obj)`` reply.
    :param stream_lines: Lines the SSE stream yields. If
        *gate_stream_on_post* is set, the reader blocks after the first
        line until a message POST arrives — this deterministically
        exercises subscribe → post → terminal ordering.
    """

    def __init__(
        self,
        responses: dict[str, tuple[int, dict[str, object]]],
        stream_lines: list[str] | None = None,
        *,
        gate_stream_on_post: bool = False,
        stream_sequence: list[list[str]] | None = None,
        hold_open_s: float = 0.0,
    ) -> None:
        #: After the scripted lines, keep the stream OPEN and silent for
        #: this long. Distinct from letting it end: a stream that closes
        #: resolves via a snapshot poll already, while one that simply
        #: stops delivering is what stranded a finished turn.
        self._hold_open_s = hold_open_s
        self._responses = responses
        self._stream_lines = stream_lines or []
        #: One stream per subscribe (call N uses element N, clamped to
        #: the last), so a re-delivery sees a fresh stream. Overrides
        #: *stream_lines* when set.
        self._stream_sequence = stream_sequence
        self._subscribes = 0
        self._gate = gate_stream_on_post
        self.calls: list[tuple[str, str]] = []
        #: Decoded JSON body seen per ``"METHOD /path"`` (last wins).
        self.bodies: dict[str, object] = {}
        #: Socket timeout seen per ``"METHOD /path"`` (last wins).
        self.timeouts: dict[str, float] = {}
        self.posted = threading.Event()
        #: Whether a message POST had already happened when the stream
        #: yielded its FIRST frame — must be False (post follows sub).
        self.post_seen_before_first_frame: bool | None = None

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, bytes]:
        path = url.split('://', 1)[-1].split('/', 1)[-1]
        path = '/' + path.split('?', 1)[0]
        self.calls.append((method, path))
        if body is not None:
            try:
                self.bodies[f'{method} {path}'] = json.loads(body)
            except (ValueError, TypeError):
                self.bodies[f'{method} {path}'] = None
        self.timeouts[f'{method} {path}'] = timeout
        if method == 'POST' and path.endswith('/events'):
            self.posted.set()
        key = f'{method} {path}'
        if key not in self._responses:
            return 404, b'{}'
        status, payload = self._responses[key]
        return status, json.dumps(payload).encode('utf-8')

    def iter_lines(
        self, url: str, *, headers: dict[str, str], read_timeout: float
    ) -> Iterator[str]:
        if self._stream_sequence is not None:
            idx = min(self._subscribes, len(self._stream_sequence) - 1)
            lines = self._stream_sequence[idx]
        else:
            lines = self._stream_lines
        self._subscribes += 1
        first = True
        for line in lines:
            if first and line.startswith('event:'):
                self.post_seen_before_first_frame = self.posted.is_set()
            if first and line == '':
                # blank ends the first (heartbeat) frame; then gate.
                first = False
                yield line
                if self._gate and not self.posted.wait(timeout=3):
                    return
                continue
            yield line
        if self._hold_open_s:
            time.sleep(self._hold_open_s)


_HEARTBEAT = [
    'event: session.heartbeat',
    'data: {"type":"session.heartbeat"}',
    '',
]


class TestParseSse(unittest.TestCase):
    def test_event_and_data(self) -> None:
        frames = list(parse_sse(iter(['event: x', 'data: {"a":1}', ''])))
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].event, 'x')
        self.assertEqual(frames[0].data, '{"a":1}')

    def test_done_sentinel(self) -> None:
        frames = list(parse_sse(iter(['data: [DONE]', ''])))
        self.assertEqual(frames[0].data, '[DONE]')

    def test_comment_and_multiline_data(self) -> None:
        frames = list(
            parse_sse(iter([': keepalive', 'data: a', 'data: b', '']))
        )
        self.assertEqual(frames[0].data, 'a\nb')

    def test_trailing_frame_without_blank(self) -> None:
        frames = list(parse_sse(iter(['event: e', 'data: d'])))
        self.assertEqual(len(frames), 1)


class TestLifecycle(unittest.TestCase):
    def test_create_sends_managed_and_returns_id(self) -> None:
        t = FakeTransport({'POST /v1/sessions': (201, {'id': 'conv_1'})})
        client = SwarmSessionClient('http://x:6767', transport=t)
        sid = client.create(agent_id='ag_1', workspace='git@sbxmount:/w#rw')
        self.assertEqual(sid, 'conv_1')
        self.assertIn(('POST', '/v1/sessions'), t.calls)

    def test_create_forwards_model_and_effort(self) -> None:
        t = FakeTransport({'POST /v1/sessions': (201, {'id': 'conv_1'})})
        client = SwarmSessionClient('http://x:6767', transport=t)
        client.create(
            agent_id='ag_1',
            model_override='claude-sonnet-5',
            reasoning_effort='low',
        )
        body = t.bodies['POST /v1/sessions']
        assert isinstance(body, dict)
        self.assertEqual(body['model_override'], 'claude-sonnet-5')
        self.assertEqual(body['reasoning_effort'], 'low')

    def test_create_omits_model_and_effort_when_unset(self) -> None:
        t = FakeTransport({'POST /v1/sessions': (201, {'id': 'conv_1'})})
        client = SwarmSessionClient('http://x:6767', transport=t)
        client.create(agent_id='ag_1')
        body = t.bodies['POST /v1/sessions']
        assert isinstance(body, dict)
        self.assertNotIn('model_override', body)
        self.assertNotIn('reasoning_effort', body)

    def test_create_missing_id_errors(self) -> None:
        t = FakeTransport({'POST /v1/sessions': (201, {})})
        client = SwarmSessionClient('http://x:6767', transport=t)
        with self.assertRaises(SwarmSessionError):
            client.create(agent_id='ag_1')

    def test_create_bad_status_errors(self) -> None:
        t = FakeTransport({'POST /v1/sessions': (422, {'detail': 'no'})})
        client = SwarmSessionClient('http://x:6767', transport=t)
        with self.assertRaises(SwarmSessionError):
            client.create(agent_id='ag_1')

    def test_read_items_reversed_to_chronological(self) -> None:
        t = FakeTransport(
            {'GET /v1/sessions/conv_1/items': (200, {'data': [3, 2, 1]})}
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        self.assertEqual(client.read_items('conv_1'), [1, 2, 3])

    def test_dispose_tolerates_404(self) -> None:
        t = FakeTransport({})  # DELETE falls through to 404
        client = SwarmSessionClient('http://x:6767', transport=t)
        client.dispose('conv_gone')  # must not raise
        self.assertIn(('DELETE', '/v1/sessions/conv_gone'), t.calls)


class TestPlanApprovalWait(unittest.TestCase):
    """wait_for_plan_approval — the cap is on SILENCE, not on time.

    This gate is the only stage in the pipeline paced by a human, and
    the plan reaches the run state only when the stage COMPLETES. A cap
    on total elapsed time therefore kills an actively-iterating review
    mid-sentence and takes every question, answer and draft with it.
    """

    def _client(self) -> SwarmSessionClient:
        return SwarmSessionClient('http://x:6767', transport=FakeTransport({}))

    @staticmethod
    def _item(item_id: str, role: str, text: str) -> dict[str, object]:
        return {'id': item_id, **_msg(role, text)}

    def _reader(
        self,
        clock: dict[str, float],
        *,
        quiet_after: float,
        approve_at: float | None,
    ):
        """Items for a session gaining a turn every 10s."""
        def read_items(session_id, *, tail=None):
            items = [self._item('a1', 'assistant', 'PLAN v1')]
            active = min(clock['t'], quiet_after)
            items += [
                self._item(f'u{i}', 'user', f'answer {i}')
                for i in range(1, int(active // 10) + 1)
            ]
            if approve_at is not None and clock['t'] >= approve_at:
                items.append(self._item('ok', 'user', 'APPROVED'))
            return items
        return read_items

    @contextlib.contextmanager
    def _driving(self, client, reader, clock, *, horizon=10_000.0):
        # A wait that never ends would hang the suite rather than fail
        # it, and "the cap still exists" is exactly what one of these
        # tests is for. Give the fake clock a horizon so losing the cap
        # surfaces as a failure with a name on it.
        def advance(seconds: float) -> None:
            clock['t'] += seconds
            if clock['t'] > horizon:
                raise AssertionError(
                    f'wait_for_plan_approval ran past {horizon:.0f}s of '
                    f'fake time without returning or raising — the cap '
                    f'on silence is gone'
                )

        with mock.patch.object(
            client, 'read_items', side_effect=reader
        ), mock.patch(
            'sbx_omnigent.swarm_session.time.monotonic', lambda: clock['t']
        ), mock.patch(
            'sbx_omnigent.swarm_session.time.sleep', advance
        ):
            yield

    def test_an_actively_iterating_human_is_never_timed_out(self) -> None:
        # 500s of real back-and-forth under a 100s cap. Every one of
        # those turns is the human working, which is the whole point of
        # the stage — none of it may count against them.
        client = self._client()
        clock = {'t': 0.0}
        reader = self._reader(clock, quiet_after=500.0, approve_at=500.0)
        with self._driving(client, reader, clock):
            plan = client.wait_for_plan_approval(
                'conv_1', poll=10.0, idle_timeout=100.0
            )
        self.assertEqual(plan, 'PLAN v1')

    def test_the_cap_restarts_at_the_last_turn_not_the_first(self) -> None:
        # Active to t=200, then silent. The wait must end ~100s after
        # the LAST turn (t=300) — not 100s after the first (t=100).
        client = self._client()
        clock = {'t': 0.0}
        reader = self._reader(clock, quiet_after=200.0, approve_at=None)
        with self._driving(client, reader, clock):
            with self.assertRaises(SwarmSessionError):
                client.wait_for_plan_approval(
                    'conv_1', poll=10.0, idle_timeout=100.0
                )
        self.assertGreaterEqual(clock['t'], 300.0)
        self.assertLess(clock['t'], 320.0)

    def test_a_session_silent_from_the_start_still_times_out(self) -> None:
        # The cap must still exist: an abandoned run cannot block a
        # sandbox forever waiting for a human who went home.
        client = self._client()
        clock = {'t': 0.0}
        reader = self._reader(clock, quiet_after=0.0, approve_at=None)
        with self._driving(client, reader, clock):
            with self.assertRaises(SwarmSessionError):
                client.wait_for_plan_approval(
                    'conv_1', poll=10.0, idle_timeout=100.0
                )
        self.assertLess(clock['t'], 120.0)

    def test_the_timeout_says_the_session_went_silent(self) -> None:
        # "did not arrive within 3600s" reads as a hard deadline on the
        # review. Name what actually expired so nobody rushes a plan.
        client = self._client()
        clock = {'t': 0.0}
        reader = self._reader(clock, quiet_after=0.0, approve_at=None)
        with self._driving(client, reader, clock):
            with self.assertRaises(SwarmSessionError) as caught:
                client.wait_for_plan_approval(
                    'conv_1', poll=10.0, idle_timeout=100.0
                )
        self.assertIn('silent', str(caught.exception))

    def test_a_preexisting_approval_still_cannot_release_the_gate(self):
        # The baseline is what makes the runner's own brief unable to
        # self-approve; activity tracking must not disturb it.
        client = self._client()
        clock = {'t': 0.0}

        def read_items(session_id, *, tail=None):
            return [
                self._item('a1', 'assistant', 'PLAN v1'),
                self._item('old', 'user', 'APPROVED'),
            ]

        with self._driving(client, read_items, clock):
            with self.assertRaises(SwarmSessionError):
                client.wait_for_plan_approval(
                    'conv_1', poll=10.0, idle_timeout=100.0
                )


class TestSessionSettle(unittest.TestCase):
    """wait_for_session_idle / read_latest_reply (plan settle)."""

    def _client(self) -> SwarmSessionClient:
        return SwarmSessionClient('http://x:6767', transport=FakeTransport({}))

    def test_wait_for_session_idle_returns_when_stable(self) -> None:
        client = self._client()
        statuses = iter(['running', 'idle', 'idle', 'idle', 'idle'])
        clock = {'t': 0.0}
        with mock.patch.object(
            client, 'get_status',
            side_effect=lambda sid: {'status': next(statuses, 'idle')},
        ), mock.patch(
            'sbx_omnigent.swarm_session.time.monotonic',
            lambda: clock['t'],
        ), mock.patch(
            'sbx_omnigent.swarm_session.time.sleep',
            lambda s: clock.__setitem__('t', clock['t'] + s),
        ):
            ok = client.wait_for_session_idle(
                'conv_1', stable_window=4.0, poll=2.0, timeout=60.0
            )
        self.assertTrue(ok)

    def test_wait_for_session_idle_times_out_if_never_idle(self) -> None:
        client = self._client()
        clock = {'t': 0.0}
        with mock.patch.object(
            client, 'get_status', return_value={'status': 'running'}
        ), mock.patch(
            'sbx_omnigent.swarm_session.time.monotonic',
            lambda: clock['t'],
        ), mock.patch(
            'sbx_omnigent.swarm_session.time.sleep',
            lambda s: clock.__setitem__('t', clock['t'] + s),
        ):
            ok = client.wait_for_session_idle(
                'conv_1', stable_window=4.0, poll=2.0, timeout=10.0
            )
        self.assertFalse(ok)

    def test_read_latest_reply_returns_last_assistant(self) -> None:
        # read_items reverses to chronological; the last assistant is
        # the settled final message (reply items need an id).
        desc = [{**_msg('assistant', 'FINAL'), 'id': 'a2'},
                {**_msg('assistant', 'earlier'), 'id': 'a1'},
                {**_msg('user', 'q'), 'id': 'u1'}]
        t = FakeTransport(
            {'GET /v1/sessions/conv_1/items': (200, {'data': desc})}
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        self.assertEqual(client.read_latest_reply('conv_1'), 'FINAL')

    def test_read_latest_reply_empty_when_no_assistant(self) -> None:
        t = FakeTransport(
            {'GET /v1/sessions/conv_1/items': (200, {'data': []})}
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        self.assertEqual(client.read_latest_reply('conv_1'), '')

    def test_read_recent_reply_text_survives_trailing_epilogue(self) -> None:
        # A verdict in an earlier message survives a trailing epilogue:
        # the joined recent text (chronological) still holds the token.
        desc = [{**_msg('assistant', 'All done, thanks!'), 'id': 'a3'},
                {**_msg('assistant', 'Ok.\nVERDICT: APPROVED'), 'id': 'a2'},
                {**_msg('user', 'review it'), 'id': 'u1'}]
        t = FakeTransport(
            {'GET /v1/sessions/conv_1/items': (200, {'data': desc})}
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        text = client.read_recent_reply_text('conv_1')
        self.assertIn('VERDICT: APPROVED', text)
        self.assertIn('All done', text)
        # chronological order: the verdict precedes the epilogue.
        self.assertLess(text.index('VERDICT'), text.index('All done'))

    def test_read_recent_reply_text_empty_when_no_assistant(self) -> None:
        t = FakeTransport(
            {'GET /v1/sessions/conv_1/items': (200, {'data': []})}
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        self.assertEqual(client.read_recent_reply_text('conv_1'), '')


class TestSendAndWait(unittest.TestCase):
    def _client(
        self, lines: list[str], *, gate: bool = True
    ) -> tuple[SwarmSessionClient, FakeTransport]:
        t = FakeTransport(
            {'POST /v1/sessions/conv_1/events': (202, {'queued': True})},
            stream_lines=lines,
            gate_stream_on_post=gate,
        )
        return SwarmSessionClient('http://x:6767', transport=t), t

    def test_running_then_idle_completes(self) -> None:
        lines = (
            _HEARTBEAT
            + _status_frame('running')
            + _reply_frame('VERDICT: APPROVED')
            + _status_frame('idle')
        )
        client, t = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual(result.status, 'idle')
        # The reply is captured live off the stream — no store read.
        self.assertEqual(result.reply, 'VERDICT: APPROVED')
        # The turn was posted, and only AFTER the subscription frame.
        self.assertTrue(t.posted.is_set())
        self.assertIs(t.post_seen_before_first_frame, False)
        # The post carries the full turn budget (the /events endpoint
        # long-polls while a managed session provisions).
        self.assertEqual(t.timeouts['POST /v1/sessions/conv_1/events'], 5)

    def test_premature_settle_idle_is_ignored(self) -> None:
        # THE regression: a native-terminal turn emits a settle-idle
        # (response_id=None) BEFORE the real work — even after a running
        # edge. The wait must skip it and return the REAL completion's
        # reply, not an empty result.
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id=None)
            + _status_frame('idle', response_id=None)  # premature settle
            + _status_frame('running', response_id='resp_1')
            + _reply_frame('VERDICT: APPROVED')
            + _status_frame('idle', response_id='resp_1')  # real done
        )
        client, _ = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual(result.reply, 'VERDICT: APPROVED')

    def test_reply_falls_back_to_deltas_when_item_empty(self) -> None:
        # If the completed message item carries no text, the accumulated
        # streamed deltas supply the reply.
        lines = (
            _HEARTBEAT
            + _status_frame('running')
            + _delta_frame('VERDICT: ', final=False)
            + _delta_frame('BLOCKING', final=True)
            + _status_frame('idle')
        )
        client, _ = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertEqual(result.reply, 'VERDICT: BLOCKING')

    def test_failed_status_returns_failure(self) -> None:
        lines = (
            _HEARTBEAT
            + _status_frame('running')
            + _status_frame('failed', 'boom')
        )
        client, _ = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertFalse(result.ok)
        self.assertEqual(result.status, 'failed')
        self.assertEqual(result.error, 'boom')

    def test_null_response_id_idle_is_ignored(self) -> None:
        lines = (
            _HEARTBEAT
            + _status_frame('idle', response_id=None)  # settle, no id
            + _status_frame('running')
            + _status_frame('idle', response_id='resp_1')  # real done
        )
        client, _ = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)

    def test_agy_idle_without_response_id_completes_after_reply(self) -> None:
        # agy's REAL completion-idle carries NO response_id, and agy
        # emits no premature settle-idle: once this turn's reply has
        # arrived, a response_id-less idle must complete (not time out).
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id=None)
            + _reply_frame('I added safe_divide to calc.py')
            + _status_frame('idle', response_id=None)  # agy real done, no id
        )
        client, _ = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual(result.status, 'idle')
        self.assertEqual(result.reply, 'I added safe_divide to calc.py')

    def test_agy_tool_idle_before_reply_completes(self) -> None:
        # agy TOOL turn: response.completed, then the terminal id-less
        # idle, then the mirrored reply a beat later. Must complete with
        # the reply (this is THE tool-use case that used to hang).
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id=None)
            + _completed_frame()
            + _status_frame('idle', response_id=None)  # terminal, no id
            + _reply_frame('subtract added')
        )
        client, _ = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual(result.reply, 'subtract added')

    def test_agy_completed_idle_no_reply_returns_after_grace(self) -> None:
        # response.completed + id-less idle but NO reply ever: return
        # after the grace rather than hang to the full timeout.
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id=None)
            + _completed_frame()
            + _status_frame('idle', response_id=None)
        )
        client, _ = self._client(lines)
        result = client.send_and_wait(
            'conv_1', 'go', timeout=5, idle_reply_grace=0.2
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.status, 'idle')

    def test_premature_idle_before_completed_still_skipped(self) -> None:
        # A bare idle BEFORE any response.completed is Claude's
        # premature settle-idle — still skipped (saw_completed False).
        lines = (
            _HEARTBEAT
            + _status_frame('idle', response_id=None)  # premature
            + _status_frame('running', response_id=None)
            + _completed_frame()
            + _reply_frame('VERDICT: APPROVED')
            + _status_frame('idle', response_id='resp_1')  # real
        )
        client, _ = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual(result.reply, 'VERDICT: APPROVED')

    def test_claude_midturn_quiescence_idle_not_terminal(self) -> None:
        # THE reply-capture regression. A Claude reviewer narrates
        # ("Let me run the tests"), then the item stream briefly goes
        # quiet before the next tool round, so the server infers a
        # mid-turn idle (response_id=None). That id-less idle must NOT
        # be treated as terminal — the turn resumes and produces the
        # FINAL verdict. Old logic returned the intermediate message
        # because a reply was already captured.
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id=None)
            + _completed_frame()  # Claude fires this EARLY, mid-turn
            + _status_frame('running', response_id='resp_1')
            + _reply_frame('Let me run the tests to confirm behavior.')
            + _status_frame('idle', response_id=None)  # quiescence lull
            + _status_frame('running', response_id='resp_1')  # resumes
            + _reply_frame('VERDICT: APPROVED')
            + _status_frame('idle', response_id='resp_1')  # real done
        )
        client, _ = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)
        # The FINAL message, not the intermediate narration.
        self.assertEqual(result.reply, 'VERDICT: APPROVED')

    def test_running_reopens_graced_idless_idle(self) -> None:
        # Robustness for the narrow window where a Claude quiescence
        # idle arrives AFTER response.completed but BEFORE the turn's
        # first response_id: it provisionally grace-waits (looks agy),
        # then a running edge proves the turn resumed and reopens it, so
        # the FINAL reply is captured — not an intermediate one.
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id=None)
            + _completed_frame()
            + _status_frame('idle', response_id=None)  # provisional grace
            + _status_frame('running', response_id='resp_1')  # reopens
            + _reply_frame('VERDICT: BLOCKING')
            + _status_frame('idle', response_id='resp_1')  # real done
        )
        client, _ = self._client(lines)
        result = client.send_and_wait(
            'conv_1', 'go', timeout=5, idle_reply_grace=2.0
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.reply, 'VERDICT: BLOCKING')

    @staticmethod
    def _post_count(t: FakeTransport) -> int:
        return sum(
            1
            for method, path in t.calls
            if method == 'POST' and path.endswith('/events')
        )

    def test_before_start_failure_redelivers_then_completes(self) -> None:
        # A delivery that fails BEFORE any turn-start (the dropped
        # submit: the paste never rendered in a cold native TUI) is
        # re-delivered from scratch — a fresh subscribe + post. The TUI
        # is warm by the second try, so it lands and completes.
        t = FakeTransport(
            {'POST /v1/sessions/conv_1/events': (202, {'queued': True})},
            stream_sequence=[
                _HEARTBEAT + _status_frame('failed', 'boom'),
                (
                    _HEARTBEAT
                    + _status_frame('running')
                    + _reply_frame('VERDICT: APPROVED')
                    + _status_frame('idle')
                ),
            ],
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        result = client.send_and_wait(
            'conv_1', 'go', timeout=5, max_resubmits=1
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.reply, 'VERDICT: APPROVED')
        # Original delivery + exactly one re-delivery.
        self.assertEqual(self._post_count(t), 2)

    def test_redeliver_delay_spaces_attempts(self) -> None:
        # A cold agy needs time to warm; the re-deliveries are spaced by
        # redeliver_delay_s so a warm TUI takes the retry, not another
        # cold miss.
        t = FakeTransport(
            {'POST /v1/sessions/conv_1/events': (202, {'queued': True})},
            stream_sequence=[
                _HEARTBEAT + _status_frame('failed', 'boom'),
                (
                    _HEARTBEAT
                    + _status_frame('running')
                    + _reply_frame('VERDICT: APPROVED')
                    + _status_frame('idle')
                ),
            ],
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        with mock.patch('sbx_omnigent.swarm_session.time.sleep') as sleep:
            result = client.send_and_wait(
                'conv_1', 'go', timeout=5, max_resubmits=1,
                redeliver_delay_s=0.5,
            )
        self.assertTrue(result.ok)
        sleep.assert_any_call(0.5)  # waited before the re-delivery

    def test_before_start_stream_close_redelivers(self) -> None:
        # The real agy failure path: the bridge gives up and the stream
        # CLOSES before any turn-start (the snapshot then reports
        # failed). This must still re-deliver — the earlier in-stream
        # resubmit never fired here (only 1 post was seen live).
        t = FakeTransport(
            {
                'POST /v1/sessions/conv_1/events': (202, {'queued': True}),
                'GET /v1/sessions/conv_1': (200, {'status': 'failed'}),
            },
            stream_sequence=[
                [*_HEARTBEAT, 'data: [DONE]', ''],  # close, no start
                (
                    _HEARTBEAT
                    + _status_frame('running')
                    + _reply_frame('VERDICT: APPROVED')
                    + _status_frame('idle')
                ),
            ],
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        result = client.send_and_wait(
            'conv_1', 'go', timeout=5, max_resubmits=1
        )
        self.assertTrue(result.ok)
        self.assertEqual(self._post_count(t), 2)

    def test_failure_after_real_output_is_not_redelivered(self) -> None:
        # A failure AFTER the turn produced real output (a reply) is a
        # genuine turn failure — it must surface, never be re-delivered.
        lines = (
            _HEARTBEAT
            + _status_frame('running')
            + _reply_frame('working on it')  # real output → started
            + _status_frame('failed', 'boom')
        )
        client, t = self._client(lines)
        result = client.send_and_wait(
            'conv_1', 'go', timeout=5, max_resubmits=1
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.error, 'boom')
        self.assertEqual(self._post_count(t), 1)  # no re-delivery

    def test_running_then_failure_before_output_redelivers(self) -> None:
        # THE live regression: the server marks the turn ``running`` the
        # instant it is ACCEPTED, before a cold agy TUI submits the
        # paste. A ``running`` edge alone is NOT a real start, so a
        # failure with no reply/completed is still a dropped submit and
        # must re-deliver (the earlier build wrongly treated running as
        # started and never re-delivered).
        t = FakeTransport(
            {'POST /v1/sessions/conv_1/events': (202, {'queued': True})},
            stream_sequence=[
                _HEARTBEAT + _status_frame('running')
                + _status_frame('failed', 'boom'),
                (
                    _HEARTBEAT
                    + _status_frame('running')
                    + _reply_frame('VERDICT: APPROVED')
                    + _status_frame('idle')
                ),
            ],
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        result = client.send_and_wait(
            'conv_1', 'go', timeout=5, max_resubmits=1
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.reply, 'VERDICT: APPROVED')
        self.assertEqual(self._post_count(t), 2)

    def test_empty_reply_frame_is_not_a_start(self) -> None:
        # THE live regression (the agy coder that sat silent for an
        # hour): a cold agy mints its OWN cascade seconds after the
        # bridge cold-started one, abandoning the conversation the paste
        # went to and emitting an EMPTY delta as it goes. Counting that
        # as a real start disarmed the stall watchdog, so the node then
        # blocked to the full turn timeout on a conversation the agent
        # had already left. Empty text is not output.
        t = FakeTransport(
            {'POST /v1/sessions/conv_1/events': (202, {'queued': True})},
            stream_sequence=[
                _HEARTBEAT + _status_frame('running')
                + _reply_frame('') + _status_frame('failed', 'boom'),
                (
                    _HEARTBEAT
                    + _status_frame('running')
                    + _reply_frame('VERDICT: APPROVED')
                    + _status_frame('idle')
                ),
            ],
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        result = client.send_and_wait(
            'conv_1', 'go', timeout=5, max_resubmits=1
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.reply, 'VERDICT: APPROVED')
        self.assertEqual(self._post_count(t), 2)

    def test_ok_but_empty_reply_redelivers(self) -> None:
        # An orphaned turn can also come back nominally OK carrying no
        # text at all (a terminal idle, nothing ever said). That is not
        # a result — re-deliver rather than hand the caller silence.
        t = FakeTransport(
            {'POST /v1/sessions/conv_1/events': (202, {'queued': True})},
            stream_sequence=[
                _HEARTBEAT + _status_frame('running')
                + _status_frame('idle'),
                (
                    _HEARTBEAT
                    + _status_frame('running')
                    + _reply_frame('done')
                    + _status_frame('idle')
                ),
            ],
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        result = client.send_and_wait(
            'conv_1', 'go', timeout=5, max_resubmits=1
        )
        self.assertEqual(result.reply, 'done')
        self.assertEqual(self._post_count(t), 2)

    def test_ok_but_empty_reply_returns_without_budget(self) -> None:
        # No resubmit budget: the empty result still returns rather than
        # looping — the default contract is unchanged.
        lines = (
            _HEARTBEAT + _status_frame('running') + _status_frame('idle')
        )
        client, t = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual(self._post_count(t), 1)

    def test_stall_not_resubmitted_without_budget(self) -> None:
        # Default (max_resubmits=0) preserves the old contract: a
        # failed-before-start returns failure with no re-post.
        lines = _HEARTBEAT + _status_frame('failed', 'boom')
        client, t = self._client(lines)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertFalse(result.ok)
        self.assertEqual(self._post_count(t), 1)

    def test_stream_close_polls_snapshot(self) -> None:
        # running seen, then DONE with no idle edge → poll says idle.
        lines = _HEARTBEAT + _status_frame('running') + ['data: [DONE]', '']
        t = FakeTransport(
            {
                'POST /v1/sessions/conv_1/events': (202, {'queued': True}),
                'GET /v1/sessions/conv_1': (200, {'status': 'idle'}),
            },
            stream_lines=lines,
            gate_stream_on_post=True,
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)

    def test_connect_timeout_when_stream_hangs(self) -> None:
        # A stream that opens but never yields a frame within the
        # connect window → subscription is never acknowledged.
        stop = threading.Event()

        class _HangingTransport(FakeTransport):
            def iter_lines(self, url, *, headers, read_timeout):  # type: ignore[no-untyped-def]
                stop.wait(timeout=2)
                return
                yield  # unreachable; makes this a generator

        t = _HangingTransport(
            {'POST /v1/sessions/conv_1/events': (202, {'queued': True})}
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        try:
            with self.assertRaises(SwarmSessionError):
                client.send_and_wait('conv_1', 'go', connect_timeout=0.2)
        finally:
            stop.set()


_TERMINALS = 'GET /v1/sessions/conv_1/resources/terminals'


def _terminals_body(
    *, name: str = 'antigravity', running: bool = True
) -> dict[str, object]:
    return {
        'object': 'list',
        'data': [
            {
                'id': f'terminal_{name}_main',
                'metadata': {'terminal_name': name, 'running': running},
            }
        ],
    }


class TestMissedTerminalIdle(unittest.TestCase):
    """A Claude turn ends on an idle carrying a ``response_id``. When
    that edge never arrives the stream says nothing more, and the wait
    used to run to the full turn budget — an hour, on a review turn that
    had already finished and stated its verdict. Confirming from the
    item store caps that, WITHOUT mistaking a long tool round for the
    end: a paused round sits on a function_call, a finished turn on the
    assistant's closing message."""

    def _client(self, lines, *, items, status='idle', hold=1.5):
        t = FakeTransport(
            {
                'POST /v1/sessions/conv_1/events': (202, {'queued': True}),
                'GET /v1/sessions/conv_1': (200, {'status': status}),
                'GET /v1/sessions/conv_1/items': (200, {'data': items}),
            },
            stream_lines=lines,
            gate_stream_on_post=True,
            hold_open_s=hold,
        )
        return SwarmSessionClient('http://x:6767', transport=t), t

    def _stranded(self) -> list[str]:
        # A Claude turn that produced real output, then an id-less idle,
        # then nothing — the id-bearing idle never comes.
        return (
            _HEARTBEAT
            + _status_frame('running', response_id='resp_1')
            + _reply_frame('mid-turn narration')
            + _status_frame('idle', response_id=None)
        )

    @staticmethod
    def _msg(text: str) -> dict:
        return {
            'type': 'message',
            'role': 'assistant',
            'content': [{'text': text}],
        }

    def test_a_finished_turn_is_confirmed_and_returns_its_reply(self) -> None:
        # newest-first, as the API returns them.
        items = [self._msg('VERDICT: APPROVED'), self._msg('narration')]
        client, _t = self._client(self._stranded(), items=items)
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ):
            result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)
        # The store's closing message, not the streamed narration —
        # recovering from a missed edge means the stream is not trusted.
        self.assertEqual(result.reply, 'VERDICT: APPROVED')

    def test_a_pending_tool_call_is_not_the_end(self) -> None:
        # THE false-positive to avoid: the server infers idle whenever
        # the item stream goes quiet, so a 10-minute build looks exactly
        # like a finished turn on status alone. The last item does not.
        items = [
            {'type': 'function_call', 'name': 'Bash'},
            self._msg('running the suite'),
        ]
        client, _t = self._client(self._stranded(), items=items, hold=0.5)
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ):
            with self.assertRaises(SwarmSessionError):
                client.send_and_wait('conv_1', 'go', timeout=0.4)

    def test_a_running_session_is_not_the_end(self) -> None:
        client, _t = self._client(
            self._stranded(),
            items=[self._msg('VERDICT: APPROVED')],
            status='running',
            hold=0.5,
        )
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ):
            with self.assertRaises(SwarmSessionError):
                client.send_and_wait('conv_1', 'go', timeout=0.4)

    def test_a_pre_work_settle_idle_never_confirms(self) -> None:
        # Before a turn has produced anything, an id-less idle is the
        # native-TUI settle edge and the store still holds the PREVIOUS
        # turn's reply. Confirming there would return a stale verdict as
        # if this turn had stated it.
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id=None)
            + _status_frame('idle', response_id=None)
        )
        client, _t = self._client(
            lines, items=[self._msg('VERDICT: APPROVED')], hold=0.5
        )
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ):
            with self.assertRaises(SwarmSessionError):
                client.send_and_wait('conv_1', 'go', timeout=0.4)

    def test_silence_with_no_idle_at_all_is_confirmed(self) -> None:
        # THE case the idle-armed version missed completely: the stream
        # goes quiet with no idle, no close, nothing — so nothing ever
        # armed and the wait ran the whole turn budget. That cost an
        # hour of a live run, on a turn nobody could reconstruct
        # afterwards because its session had been disposed.
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id='resp_1')
            + _reply_frame('working on it')
        )
        client, _t = self._client(
            lines, items=[self._msg('VERDICT: APPROVED')]
        )
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ):
            result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertEqual(result.reply, 'VERDICT: APPROVED')

    def test_silence_over_a_pending_tool_call_keeps_waiting(self) -> None:
        # Broadening the trigger must not broaden what counts as done:
        # a long build is silent too, and the discriminator is the only
        # thing standing between the two.
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id='resp_1')
            + _reply_frame('building the workspace')
        )
        client, _t = self._client(
            lines,
            items=[
                {'type': 'function_call', 'name': 'Bash'},
                self._msg('building the workspace'),
            ],
            hold=0.5,
        )
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ):
            with self.assertRaises(SwarmSessionError):
                client.send_and_wait('conv_1', 'go', timeout=0.4)

    def test_the_agy_reply_grace_is_not_preempted(self) -> None:
        # agy goes idle a beat BEFORE mirroring its reply. The silence
        # watchdog stands down while that grace is pending — otherwise
        # it shortens the wait and the turn returns empty just before
        # the reply lands.
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id=None)
            + _sse('response.completed', {'type': 'response.completed'})
            + _status_frame('idle', response_id=None)
            + _reply_frame('the lagging agy reply')
        )
        client, _t = self._client(lines, items=[], hold=0.0)
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ):
            result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertEqual(result.reply, 'the lagging agy reply')

    def test_a_real_terminal_idle_still_wins_immediately(self) -> None:
        # The confirmation is a fallback, never the primary path: an
        # id-bearing idle must still end the turn at once, with the
        # reply captured live off the stream.
        lines = (
            _HEARTBEAT
            + _status_frame('running', response_id='resp_1')
            + _status_frame('idle', response_id=None)  # a lull
            + _reply_frame('VERDICT: BLOCKING')
            + _status_frame('idle', response_id='resp_1')
        )
        client, _t = self._client(
            lines, items=[self._msg('should not be read')], hold=0.0
        )
        result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertEqual(result.reply, 'VERDICT: BLOCKING')


class TestAbandonedTurn(unittest.TestCase):
    """The fourth state, and the only one that had no handling: the
    tool returned, the model was handed the result, and it never
    continued. Observed live — a reviewer built the workspace, launched
    the suite, then sat at 0% CPU with its tool output unanswered while
    the runner waited out a two-hour turn budget."""

    def _client(self, *, items, status='idle', hold=2.5):
        t = FakeTransport(
            {
                'POST /v1/sessions/conv_1/events': (202, {'queued': True}),
                'GET /v1/sessions/conv_1': (200, {'status': status}),
                'GET /v1/sessions/conv_1/items': (200, {'data': items}),
            },
            stream_lines=(
                _HEARTBEAT
                + _status_frame('running', response_id='resp_1')
                + _reply_frame('Now run the AWS provider tests.')
            ),
            gate_stream_on_post=True,
            hold_open_s=hold,
        )
        return SwarmSessionClient('http://x:6767', transport=t), t

    @staticmethod
    def _out(text='test result: ok. 67 passed'):
        return {'type': 'function_call_output', 'output': text}

    @staticmethod
    def _msg(text):
        return {
            'type': 'message', 'role': 'assistant',
            'content': [{'text': text}],
        }

    def test_a_delivered_tool_result_with_no_reply_fails_fast(self):
        # newest-first, as the API returns them.
        client, _t = self._client(
            items=[self._out(), self._msg('running it')]
        )
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ), mock.patch(
            'sbx_omnigent.swarm_session._ABANDON_CONFIRM_S', 0.4
        ):
            result = client.send_and_wait('conv_1', 'go', timeout=3)
        self.assertFalse(result.ok)
        self.assertIn('never continued', result.error)
        self.assertIn('tool output', result.error)

    def test_polling_does_not_keep_resetting_the_silence_clock(self):
        # The subtle one. Confirmation polls run every _IDLE_CONFIRM_S;
        # if each poll also reset the silence timer, the abandon window
        # could never elapse and this would hang to the timeout.
        client, _t = self._client(items=[self._out()], hold=2.5)
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ), mock.patch(
            'sbx_omnigent.swarm_session._ABANDON_CONFIRM_S', 0.4
        ):
            result = client.send_and_wait('conv_1', 'go', timeout=3)
        self.assertFalse(result.ok)  # not a timeout — it concluded

    def test_it_waits_out_the_margin_before_calling_a_turn_dead(self):
        # The abandon window is margin against a harness that reports
        # idle mid-thought. Inside it, the turn is still working.
        client, _t = self._client(items=[self._out()], hold=0.6)
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ), mock.patch(
            'sbx_omnigent.swarm_session._ABANDON_CONFIRM_S', 30.0
        ):
            with self.assertRaises(SwarmSessionError):
                client.send_and_wait('conv_1', 'go', timeout=0.5)

    def test_a_pending_tool_call_is_never_abandoned(self) -> None:
        # A long build looks identical from status alone; only the item
        # TYPE separates "tool running" from "model walked away".
        client, _t = self._client(
            items=[{'type': 'function_call', 'name': 'Bash'}], hold=0.6
        )
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ), mock.patch(
            'sbx_omnigent.swarm_session._ABANDON_CONFIRM_S', 0.1
        ):
            with self.assertRaises(SwarmSessionError):
                client.send_and_wait('conv_1', 'go', timeout=0.5)

    def test_a_finished_turn_still_wins_over_the_abandon_check(self):
        client, _t = self._client(
            items=[self._msg('VERDICT: APPROVED'), self._out()]
        )
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ), mock.patch(
            'sbx_omnigent.swarm_session._ABANDON_CONFIRM_S', 0.1
        ):
            result = client.send_and_wait('conv_1', 'go', timeout=3)
        self.assertTrue(result.ok)
        self.assertEqual(result.reply, 'VERDICT: APPROVED')


class TestAgentAskingAHuman(unittest.TestCase):
    """An agent that opens an interactive prompt is waiting on a human
    who is not there. Every other signal reads "working" — the session
    stays `running` for as long as the modal is open — so without this
    the turn burns its entire budget in silence. Observed live at a
    7200s budget, on a coder asking permission for a change it had
    already made and verified."""

    @staticmethod
    def _ask(text='Question 1 of 1: authorize the change?'):
        return [{'message': text, 'id': 'el_1'}]

    def _client(self, *, status='running', pending=None, items=None):
        return SwarmSessionClient(
            'http://x:6767',
            transport=FakeTransport({
                'GET /v1/sessions/c': (200, {
                    'status': status,
                    'pending_elicitations': pending or [],
                }),
                'GET /v1/sessions/c/items': (200, {'data': items or []}),
            }),
        )

    def test_a_pending_prompt_is_asking(self) -> None:
        state, text = self._client(pending=self._ask())._classify_settled('c')
        self.assertEqual(state, 'asking')
        self.assertIn('authorize the change', text)

    def test_it_beats_a_running_status(self) -> None:
        # THE point: the session reads `running` the whole time the
        # modal is open, so a status-first check calls this "working"
        # forever and nothing ever fires.
        c = self._client(status='running', pending=self._ask())
        self.assertEqual(c._classify_settled('c')[0], 'asking')

    def test_it_beats_a_finished_looking_store(self) -> None:
        # An unanswered prompt outranks a closing message: the turn is
        # stopped, not done.
        c = self._client(
            status='idle',
            pending=self._ask(),
            items=[{'type': 'message', 'role': 'assistant',
                    'content': [{'text': 'all done'}]}],
        )
        self.assertEqual(c._classify_settled('c')[0], 'asking')

    def test_no_prompt_classifies_normally(self) -> None:
        c = self._client(
            status='idle', pending=[],
            items=[{'type': 'message', 'role': 'assistant',
                    'content': [{'text': 'done'}]}],
        )
        self.assertEqual(c._classify_settled('c'), ('finished', 'done'))

    def test_confirm_finished_never_returns_an_asking_turn(self) -> None:
        c = self._client(pending=self._ask())
        self.assertIsNone(c._confirm_finished('c'))

    def test_the_turn_fails_fast_and_carries_the_question(self) -> None:
        # The question exists only in the web UI; the runner's error is
        # the one place a human watching the console will see it.
        t = FakeTransport(
            {
                'POST /v1/sessions/conv_1/events': (202, {'queued': True}),
                'GET /v1/sessions/conv_1': (200, {
                    'status': 'running',
                    'pending_elicitations': self._ask('SBOM format?'),
                }),
                'GET /v1/sessions/conv_1/items': (200, {'data': []}),
            },
            stream_lines=(
                _HEARTBEAT
                + _status_frame('running', response_id='resp_1')
                + _reply_frame('thinking about it')
            ),
            gate_stream_on_post=True,
            hold_open_s=2.5,
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ):
            result = client.send_and_wait('conv_1', 'go', timeout=3)
        self.assertFalse(result.ok)
        self.assertIn('SBOM format?', result.error)
        self.assertIn('unattended', result.error)

    def test_it_does_not_wait_out_the_abandon_margin(self) -> None:
        # An open prompt is not a slow turn, it is a stopped one — no
        # amount of extra waiting changes it, and only a human can.
        t = FakeTransport(
            {
                'POST /v1/sessions/conv_1/events': (202, {'queued': True}),
                'GET /v1/sessions/conv_1': (200, {
                    'status': 'running',
                    'pending_elicitations': self._ask(),
                }),
                'GET /v1/sessions/conv_1/items': (200, {'data': []}),
            },
            stream_lines=(
                _HEARTBEAT
                + _status_frame('running', response_id='resp_1')
                + _reply_frame('hm')
            ),
            gate_stream_on_post=True,
            hold_open_s=2.5,
        )
        client = SwarmSessionClient('http://x:6767', transport=t)
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ), mock.patch(
            'sbx_omnigent.swarm_session._ABANDON_CONFIRM_S', 999.0
        ):
            result = client.send_and_wait('conv_1', 'go', timeout=3)
        self.assertFalse(result.ok)   # not blocked by the 999s margin


class TestElicitationText(unittest.TestCase):
    """The payload shape is the server's, not ours — losing the
    question to a parse miss would leave a human with a bare timeout."""

    def test_common_shapes(self) -> None:
        for key in ('message', 'question', 'prompt', 'text', 'title'):
            with self.subTest(key=key):
                self.assertEqual(
                    _elicitation_text([{key: 'pick one'}]), 'pick one'
                )

    def test_an_unknown_shape_still_surfaces_something(self) -> None:
        out = _elicitation_text([{'weird': 'shape'}])
        self.assertIn('weird', out)

    def test_a_bare_string_survives(self) -> None:
        self.assertEqual(_elicitation_text(['just text']), 'just text')

    def test_nothing_pending_is_empty(self) -> None:
        for empty in ([], None, 'nonsense', 0):
            with self.subTest(empty=empty):
                self.assertEqual(_elicitation_text(empty), '')

    def test_it_is_bounded(self) -> None:
        self.assertLessEqual(
            len(_elicitation_text([{'message': 'x' * 5000}])), 600
        )


class TestClassifySettled(unittest.TestCase):
    """The four-way split, in isolation."""

    def _client(self, *, status='idle', items=None, fail=False):
        responses = {} if fail else {
            'GET /v1/sessions/c': (200, {'status': status}),
            'GET /v1/sessions/c/items': (200, {'data': items or []}),
        }
        return SwarmSessionClient(
            'http://x:6767', transport=FakeTransport(responses)
        )

    def test_running_is_working(self) -> None:
        c = self._client(status='running', items=[
            {'type': 'function_call_output', 'output': 'x'},
        ])
        self.assertEqual(c._classify_settled('c'), ('working', ''))

    def test_a_pending_call_is_working(self) -> None:
        c = self._client(items=[{'type': 'function_call', 'name': 'Bash'}])
        self.assertEqual(c._classify_settled('c')[0], 'working')

    def test_a_closing_message_is_finished(self) -> None:
        c = self._client(items=[
            {'type': 'message', 'role': 'assistant',
             'content': [{'text': 'done'}]},
        ])
        self.assertEqual(c._classify_settled('c'), ('finished', 'done'))

    def test_a_delivered_tool_result_is_abandoned(self) -> None:
        c = self._client(items=[
            {'type': 'function_call_output', 'output': 'ok'},
        ])
        self.assertEqual(c._classify_settled('c'), ('abandoned', ''))

    def test_an_unreadable_session_is_working_not_abandoned(self) -> None:
        # Failing to read a session says nothing about its turn, and
        # guessing "dead" would kill live runs on a blip.
        self.assertEqual(
            self._client(fail=True)._classify_settled('c')[0], 'working'
        )

    def test_an_empty_store_is_working(self) -> None:
        self.assertEqual(self._client(items=[])._classify_settled('c')[0],
                         'working')


class TestConfirmFinished(unittest.TestCase):
    """The discriminator itself, in isolation."""

    def _client(self, *, status='idle', items=None, fail=False):
        responses = {}
        if not fail:
            responses = {
                'GET /v1/sessions/c': (200, {'status': status}),
                'GET /v1/sessions/c/items': (200, {'data': items or []}),
            }
        return SwarmSessionClient(
            'http://x:6767', transport=FakeTransport(responses)
        )

    def test_idle_with_a_closing_message_is_finished(self) -> None:
        client = self._client(items=[
            {'type': 'message', 'role': 'assistant',
             'content': [{'text': 'done'}]},
        ])
        self.assertEqual(client._confirm_finished('c'), 'done')

    def test_a_user_message_last_is_not_finished(self) -> None:
        # The turn we just posted, with nothing back yet.
        client = self._client(items=[
            {'type': 'message', 'role': 'user',
             'content': [{'text': 'go'}]},
        ])
        self.assertIsNone(client._confirm_finished('c'))

    def test_an_empty_store_is_not_finished(self) -> None:
        self.assertIsNone(self._client(items=[])._confirm_finished('c'))

    def test_an_unreadable_session_is_not_finished(self) -> None:
        # A session that cannot be read is not evidence its turn ended.
        self.assertIsNone(self._client(fail=True)._confirm_finished('c'))


class TestWaitForTerminalReady(unittest.TestCase):
    """Warm-up: block until the native TUI is running, then settle."""

    def _client(
        self, response: tuple[int, dict[str, object]]
    ) -> SwarmSessionClient:
        t = FakeTransport({_TERMINALS: response})
        return SwarmSessionClient('http://x:6767', transport=t)

    def test_settle_outlasts_agy_cascade_rotation(self) -> None:
        # Measured live: the terminal reported running at T, the old 3s
        # settle pasted the turn at T+3s, and agy rotated its cascade
        # away at T+7s — orphaning the turn. The default settle must
        # keep a real margin over that window.
        self.assertGreaterEqual(_DEFAULT_TERMINAL_SETTLE_S, 15.0)

    def test_returns_true_and_settles_when_running(self) -> None:
        client = self._client((200, _terminals_body()))
        with mock.patch('sbx_omnigent.swarm_session.time.sleep') as sleep:
            ok = client.wait_for_terminal_ready('conv_1', settle=3.0)
        self.assertTrue(ok)
        sleep.assert_any_call(3.0)  # settled for paste-readiness

    def test_times_out_when_terminal_never_running(self) -> None:
        client = self._client((200, _terminals_body(running=False)))
        with mock.patch('sbx_omnigent.swarm_session.time.sleep'):
            ok = client.wait_for_terminal_ready(
                'conv_1', timeout=0.05, poll=0.01
            )
        self.assertFalse(ok)

    def test_ignores_other_named_terminals(self) -> None:
        client = self._client((200, _terminals_body(name='claude')))
        with mock.patch('sbx_omnigent.swarm_session.time.sleep'):
            ok = client.wait_for_terminal_ready(
                'conv_1', timeout=0.05, poll=0.01
            )
        self.assertFalse(ok)

    def test_lookup_failure_is_tolerated_as_not_ready(self) -> None:
        # An unknown route → 404 → treated as not-ready (not an error),
        # so a missing terminal resource never hard-fails a run.
        client = self._client((404, {}))
        with mock.patch('sbx_omnigent.swarm_session.time.sleep'):
            ok = client.wait_for_terminal_ready(
                'conv_1', timeout=0.05, poll=0.01
            )
        self.assertFalse(ok)


class TestAssistantReplyItems(unittest.TestCase):
    def test_extracts_id_and_text_skips_non_assistant(self) -> None:
        items = [
            {
                'type': 'message',
                'role': 'user',
                'content': [{'type': 'input_text', 'text': 'go'}],
            },
            _assistant_item('msg_a', 'first'),
            {'type': 'function_call', 'role': 'assistant', 'content': []},
            {
                'id': 'msg_empty',
                'type': 'message',
                'role': 'assistant',
                'content': [],
            },
            _assistant_item('msg_b', 'final'),
        ]
        self.assertEqual(
            _assistant_reply_items(items),
            [('msg_a', 'first'), ('msg_b', 'final')],
        )

    def test_ignores_assistant_message_without_id(self) -> None:
        items = [
            {
                'type': 'message',
                'role': 'assistant',
                'content': [{'type': 'output_text', 'text': 'x'}],
            }
        ]
        self.assertEqual(_assistant_reply_items(items), [])


class TestItemMessageText(unittest.TestCase):
    def test_joins_text_and_output_text_blocks(self) -> None:
        item = {
            'content': [
                {'type': 'text', 'text': 'a'},
                {'type': 'output_text', 'output_text': 'b'},
            ]
        }
        self.assertEqual(_item_message_text(item), 'a\nb')

    def test_empty_when_no_text(self) -> None:
        self.assertEqual(_item_message_text({'content': []}), '')
        self.assertEqual(_item_message_text({}), '')


class TestReplyFallback(unittest.TestCase):
    """When the stream carried no reply text, one store read runs."""

    def _client(
        self, lines: list[str]
    ) -> tuple[SwarmSessionClient, FakeTransport]:
        t = FakeTransport(
            {'POST /v1/sessions/conv_1/events': (202, {'queued': True})},
            stream_lines=lines,
            gate_stream_on_post=True,
        )
        return SwarmSessionClient('http://x:6767', transport=t), t

    def test_falls_back_to_store_when_stream_reply_empty(self) -> None:
        # A completing turn with NO reply frame on the stream → the
        # single post-turn store read supplies the text.
        lines = _HEARTBEAT + _status_frame('running') + _status_frame('idle')
        client, _ = self._client(lines)
        with mock.patch.object(
            client,
            'read_items',
            return_value=[_assistant_item('m1', 'FROM STORE')],
        ) as ri:
            result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertEqual(result.reply, 'FROM STORE')
        self.assertEqual(ri.call_count, 1)  # exactly one fallback read

    def test_no_store_read_when_stream_reply_present(self) -> None:
        # A reply on the stream means the store is never touched.
        lines = (
            _HEARTBEAT
            + _status_frame('running')
            + _reply_frame('FROM STREAM')
            + _status_frame('idle')
        )
        client, _ = self._client(lines)
        with mock.patch.object(client, 'read_items') as ri:
            result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertEqual(result.reply, 'FROM STREAM')
        ri.assert_not_called()

    def test_store_error_leaves_reply_empty(self) -> None:
        lines = _HEARTBEAT + _status_frame('running') + _status_frame('idle')
        client, _ = self._client(lines)
        with mock.patch.object(
            client, 'read_items', side_effect=SwarmSessionError('down')
        ):
            result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertTrue(result.ok)
        self.assertEqual(result.reply, '')


#: A pending prompt exactly as the server hands it over: the original
#: ``response.elicitation_request`` event dict, id at the top level and
#: the question under ``params.message``.
_PENDING = [
    {
        'type': 'response.elicitation_request',
        'elicitation_id': 'elicit_a',
        'params': {'message': 'Run `rm -rf /work`?'},
    },
    {
        'type': 'response.elicitation_request',
        'elicitation_id': 'elicit_b',
        'params': {'message': 'Delete /swapfile?'},
    },
]


class TestReadingPendingElicitationIds(unittest.TestCase):
    """
    The ids are what makes a prompt answerable; the text only makes it
    reportable. Read defensively — the payload shape is the server's.
    """

    def test_ids_come_from_the_event_dicts(self) -> None:
        self.assertEqual(
            _elicitation_ids(_PENDING), ('elicit_a', 'elicit_b')
        )

    def test_entries_without_a_usable_id_are_skipped(self) -> None:
        # One good entry among junk must still be answerable: dropping
        # the whole batch would strand a turn that could have continued.
        pending = [
            'not a dict',
            {'params': {'message': 'no id here'}},
            {'elicitation_id': ''},
            {'elicitation_id': 42},
            {'elicitation_id': 'elicit_ok'},
        ]
        self.assertEqual(_elicitation_ids(pending), ('elicit_ok',))

    def test_a_non_list_payload_yields_nothing(self) -> None:
        for value in (None, {}, '', 0, {'elicitation_id': 'x'}):
            with self.subTest(value=value):
                self.assertEqual(_elicitation_ids(value), ())

    def test_the_same_id_is_never_answered_twice(self) -> None:
        pending = [
            {'elicitation_id': 'dup'},
            {'elicitation_id': 'dup'},
        ]
        self.assertEqual(_elicitation_ids(pending), ('dup',))


class TestResolvingAnElicitation(unittest.TestCase):
    """
    Answering a prompt over the URL endpoint. It is absent from the
    server's OpenAPI but real — verified live against a calibration
    route that 404s while this one 422s for a missing ``action``.
    """

    _URL = '/v1/sessions/s1/elicitations/elicit_a/resolve'

    def test_a_verdict_reaches_the_elicitation_url(self) -> None:
        t = FakeTransport({f'POST {self._URL}': (200, {})})
        client = SwarmSessionClient('http://x:6767', transport=t)
        client.resolve_elicitation('s1', 'elicit_a')
        self.assertIn(('POST', self._URL), t.calls)
        self.assertEqual(
            t.bodies[f'POST {self._URL}'], {'action': 'accept'}
        )

    def test_a_declined_prompt_says_so(self) -> None:
        t = FakeTransport({f'POST {self._URL}': (200, {})})
        client = SwarmSessionClient('http://x:6767', transport=t)
        client.resolve_elicitation('s1', 'elicit_a', action='decline')
        self.assertEqual(
            t.bodies[f'POST {self._URL}'], {'action': 'decline'}
        )

    def test_content_is_sent_only_when_given(self) -> None:
        t = FakeTransport({f'POST {self._URL}': (200, {})})
        client = SwarmSessionClient('http://x:6767', transport=t)
        client.resolve_elicitation('s1', 'elicit_a', content={'k': 'v'})
        self.assertEqual(
            t.bodies[f'POST {self._URL}'],
            {'action': 'accept', 'content': {'k': 'v'}},
        )

    def test_a_prompt_already_gone_is_not_a_failure(self) -> None:
        # 404 means nothing is waiting on an answer any more, which is
        # the state this call exists to reach. Raising would fail a turn
        # that is in fact free to continue.
        t = FakeTransport({})          # unmapped -> 404
        client = SwarmSessionClient('http://x:6767', transport=t)
        client.resolve_elicitation('s1', 'elicit_a')

    def test_a_refused_verdict_is_reported(self) -> None:
        t = FakeTransport({f'POST {self._URL}': (500, {'error': 'boom'})})
        client = SwarmSessionClient('http://x:6767', transport=t)
        with self.assertRaises(SwarmSessionError):
            client.resolve_elicitation('s1', 'elicit_a')

    def test_an_unknown_action_never_reaches_the_server(self) -> None:
        # The server enumerates accept/decline/cancel; catching it here
        # turns a 422 mid-turn into an obvious programming error.
        t = FakeTransport({f'POST {self._URL}': (200, {})})
        client = SwarmSessionClient('http://x:6767', transport=t)
        with self.assertRaises(ValueError):
            client.resolve_elicitation('s1', 'elicit_a', action='yes')
        self.assertEqual(t.calls, [])


class _AnsweringTransport(FakeTransport):
    """
    A transport whose session stops asking once its prompt is answered.

    The real sequence is what matters here: a session reports a pending
    prompt, a verdict is POSTed, and the NEXT snapshot no longer reports
    it. A fixed-response fake cannot express that, and without it a test
    cannot tell "the turn continued" from "the turn never noticed".
    """

    def __init__(self, responses, **kw) -> None:
        super().__init__(responses, **kw)
        self.resolved: list[str] = []

    def request(self, method, url, *, headers, body, timeout):
        path = url.split('://', 1)[-1].split('/', 1)[-1]
        path = '/' + path.split('?', 1)[0]
        if method == 'POST' and path.endswith('/resolve'):
            self.calls.append((method, path))
            self.bodies[f'{method} {path}'] = json.loads(body or b'{}')
            self.resolved.append(path.split('/elicitations/')[1]
                                 .removesuffix('/resolve'))
            return 200, b'{}'
        if method == 'GET' and self.resolved and '/items' not in path:
            # Answered: the session is free again and its turn ended.
            return 200, json.dumps({
                'status': 'idle', 'pending_elicitations': [],
            }).encode()
        if method == 'GET' and self.resolved and path.endswith('/items'):
            return 200, json.dumps({
                'data': [_msg('assistant', 'done after approval')],
            }).encode()
        return super().request(
            method, url, headers=headers, body=body, timeout=timeout
        )


#: One answerable prompt, shaped as the server sends it.
_ASKING = [{
    'elicitation_id': 'el_1',
    'params': {'message': 'Run `rm -rf /work`?'},
}]


class TestAutoApprovingAPrompt(unittest.TestCase):
    """
    A prompt inside a microVM carries no safety information — the VM is
    the containment boundary — so stopping the turn over one converts a
    non-event into lost work. Twice mid-campaign it cost a whole turn.
    """

    def _transport(self, cls=_AnsweringTransport, pending=None):
        return cls(
            {
                'POST /v1/sessions/conv_1/events': (202, {'queued': True}),
                'GET /v1/sessions/conv_1': (200, {
                    'status': 'running',
                    'pending_elicitations':
                        _ASKING if pending is None else pending,
                }),
                'GET /v1/sessions/conv_1/items': (200, {'data': []}),
            },
            stream_lines=(
                _HEARTBEAT
                + _status_frame('running', response_id='resp_1')
                + _reply_frame('working')
            ),
            gate_stream_on_post=True,
            hold_open_s=2.5,
        )

    def test_the_prompt_is_answered_and_the_turn_finishes(self) -> None:
        t = self._transport()
        client = SwarmSessionClient('http://x:6767', transport=t)
        with mock.patch('sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05):
            result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertEqual(t.resolved, ['el_1'])
        self.assertTrue(result.ok, result.error)

    def test_the_verdict_is_accept(self) -> None:
        t = self._transport()
        client = SwarmSessionClient('http://x:6767', transport=t)
        with mock.patch('sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05):
            client.send_and_wait('conv_1', 'go', timeout=5)
        body = t.bodies['POST /v1/sessions/conv_1/elicitations/el_1/resolve']
        self.assertEqual(body, {'action': 'accept'})

    def test_every_approval_is_announced(self) -> None:
        # An agent asking is worth seeing even when the answer is always
        # yes: that is how the swapfile prompt was found at all.
        t = self._transport()
        client = SwarmSessionClient('http://x:6767', transport=t)
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05
        ), mock.patch('sbx_omnigent.swarm_session.click.echo') as echo:
            client.send_and_wait('conv_1', 'go', timeout=5)
        said = ' '.join(str(c.args[0]) for c in echo.call_args_list)
        self.assertIn('[elicit]', said)
        self.assertIn('rm -rf /work', said)

    def test_switching_it_off_restores_the_old_failure(self) -> None:
        t = self._transport()
        client = SwarmSessionClient(
            'http://x:6767', transport=t, auto_approve=False
        )
        with mock.patch('sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05):
            result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertEqual(t.resolved, [])
        self.assertFalse(result.ok)
        self.assertIn('rm -rf /work', result.error)

    def test_a_prompt_with_no_id_still_fails_the_turn(self) -> None:
        # Nothing to answer: report it the old way rather than spin.
        t = self._transport(pending=[{'message': 'unanswerable?'}])
        client = SwarmSessionClient('http://x:6767', transport=t)
        with mock.patch('sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.05):
            result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertEqual(t.resolved, [])
        self.assertFalse(result.ok)
        self.assertIn('unanswerable?', result.error)

    def test_a_runaway_ask_loop_is_capped(self) -> None:
        # A session that re-asks forever must not spin to the turn
        # budget in silence; it fails naming the cap.
        class _NeverSatisfied(_AnsweringTransport):
            def request(self, method, url, *, headers, body, timeout):
                path = '/' + url.split('://', 1)[-1].split('/', 1)[-1]
                if method == 'POST' and path.endswith('/resolve'):
                    self.resolved.append('again')
                    return 200, b'{}'
                return FakeTransport.request(
                    self, method, url,
                    headers=headers, body=body, timeout=timeout,
                )

        t = self._transport(cls=_NeverSatisfied)
        client = SwarmSessionClient('http://x:6767', transport=t)
        with mock.patch(
            'sbx_omnigent.swarm_session._IDLE_CONFIRM_S', 0.01
        ), mock.patch(
            'sbx_omnigent.swarm_session._MAX_AUTO_APPROVALS_PER_TURN', 3
        ):
            result = client.send_and_wait('conv_1', 'go', timeout=5)
        self.assertEqual(len(t.resolved), 3)
        self.assertFalse(result.ok)
        self.assertIn('3', result.error)


class TestAutoApproveIsOnByDefault(unittest.TestCase):
    """
    The default decides whether a whole campaign survives a prompt, and
    the CLI expresses it as a NEGATIVE flag — so an inverted wire-up
    would silently restore the behaviour this exists to remove.
    """

    def test_the_client_answers_unless_told_otherwise(self) -> None:
        client = SwarmSessionClient('http://x:6767')
        self.assertTrue(client._auto_approve)

    def test_the_pipeline_command_offers_the_opt_out(self) -> None:
        flag = next(
            (p for p in runner_main.params
             if '--no-auto-approve' in getattr(p, 'opts', [])),
            None,
        )
        self.assertIsNotNone(flag, '--no-auto-approve is not offered')
        # Default OFF: not passing it must leave answering ON.
        self.assertFalse(flag.default)
        self.assertTrue(flag.is_flag)


if __name__ == '__main__':
    unittest.main()
