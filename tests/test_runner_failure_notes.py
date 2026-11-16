"""What a failed turn's message says about why it failed.

Run: python -m unittest tests.test_runner_failure_notes
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

from sbx_omnigent import guest_log
from sbx_omnigent.pipeline import PipelineAgent
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import LostTurn
from sbx_omnigent.runner.failure_notes import (
    FailureNotes,
    lost_not_failed,
    session_failure_note,
    turn_was_lost,
)
from sbx_omnigent.runner.sandboxes import sandbox_for
from sbx_omnigent.runner.session_registry import SessionRegistry
from sbx_omnigent.swarm_session import SwarmSessionError, SwarmTurnTimeout


class TestTheSandbox(unittest.TestCase):
    def test_it_is_the_host_the_session_runs_on(self) -> None:
        client = mock.Mock()
        client.session_host_id.return_value = 'host-1'
        client.host_name.return_value = 'managed-1'
        self.assertEqual(sandbox_for(client, 's1'), 'managed-1')

    def test_a_session_with_no_host_has_none(self) -> None:
        client = mock.Mock()
        client.session_host_id.return_value = None
        self.assertIsNone(sandbox_for(client, 's1'))

    def test_any_failure_to_look_it_up_is_none(self) -> None:
        # It runs only after a turn has failed; it must not replace
        # that failure with another.
        client = mock.Mock()
        client.session_host_id.side_effect = AttributeError('old server')
        self.assertIsNone(sandbox_for(client, 's1'))


class TestTheSessionsOwnAccount(unittest.TestCase):
    def test_an_unreadable_session_says_so(self) -> None:
        self.assertIn('could not be read', session_failure_note(None))

    def test_its_last_error_and_sandbox_are_named(self) -> None:
        note = session_failure_note(
            {'last_task_error': 'boom', 'sandbox_status': 'running'},
        )
        self.assertEqual(note, ' (last_task_error=boom; sandbox=running)')

    def test_an_offline_runner_is_called_out(self) -> None:
        self.assertIn('RUNNER IS OFFLINE',
                      session_failure_note({'runner_online': False}))

    def test_nothing_to_say_is_nothing(self) -> None:
        self.assertEqual(session_failure_note({}), '')


class TestALostTurn(unittest.TestCase):
    def test_no_error_anywhere_is_lost(self) -> None:
        self.assertTrue(turn_was_lost(None, {}))

    def test_an_offline_runner_is_lost(self) -> None:
        self.assertTrue(turn_was_lost('boom', {'runner_online': False}))

    def test_an_error_from_the_agent_is_a_failure(self) -> None:
        self.assertFalse(turn_was_lost('boom', {}))
        self.assertFalse(turn_was_lost(None, {'last_task_error': 'boom'}))

    def test_an_unreadable_session_with_no_error_is_lost(self) -> None:
        self.assertTrue(turn_was_lost(None, None))


class TestWorthASecondAttempt(unittest.TestCase):
    def test_a_lost_turn_is(self) -> None:
        self.assertTrue(lost_not_failed(LostTurn('lost')))

    def test_a_session_error_is(self) -> None:
        self.assertTrue(lost_not_failed(SwarmSessionError('dropped')))

    def test_a_timeout_is_not(self) -> None:
        self.assertFalse(lost_not_failed(SwarmTurnTimeout('slow')))

    def test_anything_else_is_not(self) -> None:
        self.assertFalse(lost_not_failed(RuntimeError('agent error')))


class _NotesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.sessions = mock.Mock()
        self.sessions.session_host_id.return_value = 'host-1'
        self.sessions.host_name.return_value = 'managed-1'
        self.registry = SessionRegistry()
        agent = mock.Mock(spec=PipelineAgent)
        agent.harness = 'codex-native'
        self.registry.register('s1', label='build', agent=agent,
                               worktree='/w/build')

    def notes(self, *, keep: bool = False) -> FailureNotes:
        ctx = RunContext(
            config=mock.Mock(), worktrees=mock.Mock(), sessions=self.sessions,
            run_id='r1', nodes={}, stage_by_id={}, lock=threading.RLock(),
            max_rounds=3,
        )
        return FailureNotes(ctx, registry=self.registry, keep=keep)


class TestARejectedCredential(_NotesTest):
    def test_the_turns_own_error_is_read_first(self) -> None:
        with mock.patch.object(guest_log, 'read_runner_log') as log:
            note = self.notes().auth('s1', 'Failed to authenticate.', None)
        self.assertIn("the turn's error shows", note)
        log.assert_not_called()

    def test_then_the_sessions_last_error(self) -> None:
        note = self.notes().auth(
            's1', None, {'last_task_error': '401 Unauthorized'},
        )
        self.assertIn("the session's last error", note)

    def test_then_the_runner_log_in_the_vm(self) -> None:
        with mock.patch.object(guest_log, 'read_runner_log',
                               return_value='invalid_grant'):
            note = self.notes().auth('s1', None, None)
        self.assertIn('the runner log in managed-1', note)

    def test_the_fix_is_the_harnesss_own(self) -> None:
        note = self.notes().auth('s1', 'invalid_grant', None)
        self.assertIn(guest_log.relogin_hint('codex-native'), note)

    def test_no_sign_of_one_says_nothing(self) -> None:
        with mock.patch.object(guest_log, 'read_runner_log',
                               return_value='all fine'):
            self.assertEqual(self.notes().auth('s1', 'boom', None), '')

    def test_a_failing_log_read_says_nothing(self) -> None:
        with mock.patch.object(guest_log, 'read_runner_log',
                               side_effect=RuntimeError('sbx')):
            self.assertEqual(self.notes().auth('s1', None, None), '')


class TestARateLimit(_NotesTest):
    def test_a_limit_is_named_with_the_fix(self) -> None:
        note = self.notes().rate_limit('s1', 'rate limit exceeded', None)
        self.assertIn('usage or rate limit', note)
        self.assertIn('--resume', note)

    def test_an_ordinary_failure_is_not_one(self) -> None:
        self.assertEqual(self.notes().rate_limit('s1', 'boom', None), '')


class TestNoPane(_NotesTest):
    def test_it_points_at_the_runner_log(self) -> None:
        note = self.notes().no_pane('s1')
        self.assertIn('sbx exec managed-1', note)
        self.assertIn('re-run with --keep', note)

    def test_under_keep_the_vm_is_still_there(self) -> None:
        self.assertIn('still there', self.notes(keep=True).no_pane('s1'))

    def test_a_vm_that_cannot_be_found_says_so(self) -> None:
        self.sessions.session_host_id.return_value = None
        self.assertIn('could not be located', self.notes().no_pane('s1'))


class TestTheSnapshot(_NotesTest):
    def test_it_is_the_sessions_status(self) -> None:
        self.sessions.get_status.return_value = {'status': 'failed'}
        self.assertEqual(self.notes().snapshot('s1'), {'status': 'failed'})

    def test_an_unreadable_session_has_none(self) -> None:
        self.sessions.get_status.side_effect = SwarmSessionError('gone')
        self.assertIsNone(self.notes().snapshot('s1'))


if __name__ == '__main__':
    unittest.main()
