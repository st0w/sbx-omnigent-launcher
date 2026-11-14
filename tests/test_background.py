"""The runner waits out a Claude agent's background work.

Claude Code starts long commands in the background, ends the turn,
and is woken by a ``<task-notification>`` when they finish. The text
below is what a live session recorded.

Run: .venv/bin/python -m unittest tests.test_background
"""

from __future__ import annotations

import unittest

from sbx_omnigent import background as bg

_LAUNCH = (
    'Command running in background with ID: bonptkorp. Output is being '
    'written to: /tmp/claude-0/x/tasks/bonptkorp.output. You will be '
    'notified when it completes — do not poll that file or sleep while '
    'waiting.'
)
_DONE = (
    '<task-notification>\n<task-id>bonptkorp</task-id>\n'
    '<tool-use-id>toolu_01</tool-use-id>\n'
    '<output-file>/tmp/claude-0/x/tasks/bonptkorp.output</output-file>\n'
    '<status>completed</status>\n'
    '<summary>Background command "Sleep" completed (exit code 0)'
    '</summary>\n</task-notification>'
)
_MONITOR = (
    'Monitor started (task b1mhekpmy, timeout 30m). You will be notified '
    'on each event. Keep working — do not poll or sleep.'
)
_MONITOR_EVENT = (
    '<task-notification>\n<task-id>b1mhekpmy</task-id>\n'
    '<summary>Monitor event: "install"</summary>\n'
    '<event>apt: 83 of 129 packages</event>\n</task-notification>'
)


def _out(n: int, text: str) -> dict[str, object]:
    return {'id': f'i{n}', 'type': 'function_call_output', 'output': text}


def _say(n: int, text: str) -> dict[str, object]:
    return {
        'id': f'i{n}', 'type': 'message', 'role': 'assistant',
        'content': [{'type': 'output_text', 'text': text}],
    }


def _note(n: int, text: str) -> dict[str, object]:
    return {
        'id': f'i{n}', 'type': 'message', 'role': 'user', 'is_meta': True,
        'content': [{'type': 'input_text', 'text': text}],
    }


def _call(n: int) -> dict[str, object]:
    return {'id': f'i{n}', 'type': 'function_call', 'name': 'Bash'}


class TestOutstanding(unittest.TestCase):
    def test_a_background_command_is_outstanding(self) -> None:
        self.assertEqual(bg.outstanding([_out(1, _LAUNCH)]), {'bonptkorp'})

    def test_its_notification_ends_it(self) -> None:
        items = [_out(1, _LAUNCH), _note(2, _DONE)]
        self.assertEqual(bg.outstanding(items), set())

    def test_a_monitor_is_outstanding_through_its_events(self) -> None:
        items = [_out(1, _MONITOR), _note(2, _MONITOR_EVENT)]
        self.assertEqual(bg.outstanding(items), {'b1mhekpmy'})

    def test_a_stopped_or_expired_monitor_is_not(self) -> None:
        for end in ('[Monitor stopped]',
                    '[Monitor expired after 30m with 2 events delivered.]'):
            with self.subTest(end=end):
                note = _MONITOR_EVENT.replace(
                    '<event>apt: 83 of 129 packages</event>',
                    f'<event>{end}</event>',
                )
                items = [_out(1, _MONITOR), _note(2, note)]
                self.assertEqual(bg.outstanding(items), set())

    def test_a_list_shaped_output_is_read(self) -> None:
        item = _out(1, '')
        item['output'] = [{'type': 'output_text', 'text': _LAUNCH}]
        self.assertEqual(bg.outstanding([item]), {'bonptkorp'})

    def test_a_notification_for_another_task_ends_nothing(self) -> None:
        other = _DONE.replace('bonptkorp', 'zzzzzzzzz')
        items = [_out(1, _LAUNCH), _note(2, other)]
        self.assertEqual(bg.outstanding(items), {'bonptkorp'})

    def test_malformed_items_are_ignored(self) -> None:
        items = [{}, {'type': 'function_call_output', 'output': None},
                 {'type': 'message', 'role': 'user', 'content': 'x'}]
        self.assertEqual(bg.outstanding(items), set())


class TestItemsAfter(unittest.TestCase):
    def test_after_a_marker(self) -> None:
        items = [_call(1), _call(2), _call(3)]
        self.assertEqual(
            [i['id'] for i in bg.items_after(items, 'i1')], ['i2', 'i3']
        )

    def test_no_marker_is_everything(self) -> None:
        self.assertEqual(len(bg.items_after([_call(1)], None)), 1)

    def test_a_marker_no_longer_listed_is_everything(self) -> None:
        self.assertEqual(len(bg.items_after([_call(1)], 'gone')), 1)


class _Session:
    """A session read once per poll: items and status, scripted."""

    def __init__(self, reads: list[tuple[list[dict], str]]) -> None:
        self._reads = list(reads)
        self._status = 'idle'
        self.item_reads = 0

    def read_items(self, session_id, *, tail=None):
        # Each read is one poll; the last one repeats. The status read
        # that follows belongs to the same poll.
        self.item_reads += 1
        items, self._status = self._reads[0]
        if len(self._reads) > 1:
            self._reads.pop(0)
        return list(items)

    def get_status(self, session_id):
        return {'status': self._status}


class TestAwaitBackground(unittest.TestCase):
    def setUp(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def _clock(self) -> float:
        return self.now

    def _sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.now += s

    def _wait(self, session, reply='STARTED', *, done=None, deadline=3600.0,
              stop=None):
        return bg.await_background(
            session, 's1', since=None, reply=reply,
            done=done or (lambda r: 'CHECK COMPLETE' in r),
            deadline=deadline, label='check',
            stop=stop or (lambda: None),
            poll_s=15.0, clock=self._clock, sleep=self._sleep,
        )

    def test_a_finished_reply_does_not_wait(self) -> None:
        session = _Session([([_out(1, _LAUNCH)], 'idle')])
        self.assertEqual(
            self._wait(session, 'done\nCHECK COMPLETE'),
            'done\nCHECK COMPLETE',
        )
        self.assertEqual(self.sleeps, [])

    def test_no_background_work_does_not_wait(self) -> None:
        session = _Session([([_say(1, 'STARTED')], 'idle')])
        self.assertEqual(self._wait(session), 'STARTED')
        self.assertEqual((self.sleeps, session.item_reads), ([], 1))

    def test_the_woken_reply_is_appended(self) -> None:
        first = [_out(1, _LAUNCH), _say(2, 'STARTED')]
        woke = [*first, _note(3, _DONE), _say(4, 'WOKE\nCHECK COMPLETE')]
        session = _Session([
            (first, 'idle'),
            ([*first, _note(3, _DONE)], 'running'),
            (woke, 'idle'),
            (woke, 'idle'),
        ])
        self.assertEqual(
            self._wait(session), 'STARTED\n\nWOKE\nCHECK COMPLETE'
        )

    def test_a_second_launch_keeps_it_waiting(self) -> None:
        first = [_out(1, _LAUNCH), _say(2, 'STARTED')]
        again = [
            *first,
            _note(3, _DONE), _out(4, _LAUNCH.replace('bonptkorp', 'b2')),
            _say(5, 'building'),
        ]
        done = [
            *again,
            _note(6, _DONE.replace('bonptkorp', 'b2')),
            _say(7, 'CHECK COMPLETE'),
        ]
        session = _Session([
            (first, 'idle'), (again, 'idle'), (again, 'idle'),
            (done, 'idle'), (done, 'idle'),
        ])
        self.assertEqual(
            self._wait(session),
            'STARTED\n\nbuilding\n\nCHECK COMPLETE',
        )

    def test_a_mid_turn_idle_is_not_the_end(self) -> None:
        # Claude reads idle between tool rounds; the newest item is
        # then a tool call, not its reply.
        first = [_out(1, _LAUNCH), _say(2, 'STARTED')]
        mid = [*first, _note(3, _DONE), _call(4)]
        end = [*mid, _say(5, 'CHECK COMPLETE')]
        session = _Session([
            (first, 'idle'), (mid, 'idle'), (mid, 'idle'),
            (end, 'idle'), (end, 'idle'),
        ])
        self.assertEqual(
            self._wait(session), 'STARTED\n\nCHECK COMPLETE'
        )

    def test_a_failed_session_ends_the_wait(self) -> None:
        first = [_out(1, _LAUNCH), _say(2, 'STARTED')]
        session = _Session([
            (first, 'idle'), ([*first, _note(3, _DONE)], 'failed'),
        ])
        self.assertEqual(self._wait(session), 'STARTED')

    def test_the_deadline_returns_the_reply_as_is(self) -> None:
        session = _Session([([_out(1, _LAUNCH), _say(2, 'STARTED')], 'idle')])
        self.assertEqual(self._wait(session, deadline=100.0), 'STARTED')
        self.assertGreaterEqual(self.now, 100.0)

    def test_an_interrupt_stops_it(self) -> None:
        session = _Session([([_out(1, _LAUNCH)], 'idle')])

        def stop() -> None:
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self._wait(session, stop=stop)

    def test_only_this_turns_launches_count(self) -> None:
        # A task started in an earlier turn is not this turn's wait.
        session = _Session([([_out(1, _LAUNCH), _say(2, 'STARTED')], 'idle')])
        reply = bg.await_background(
            session, 's1', since='i2', reply='STARTED',
            done=lambda r: False, deadline=3600.0, label='check',
            stop=lambda: None, poll_s=15.0, clock=self._clock,
            sleep=self._sleep,
        )
        self.assertEqual((reply, self.sleeps), ('STARTED', []))


if __name__ == '__main__':
    unittest.main()
