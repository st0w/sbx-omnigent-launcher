"""Captures: a session's transcript and screen, kept before teardown.

Run: python -m unittest tests.test_runner_captures
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

import click

from sbx_omnigent import harness_versions, pane
from sbx_omnigent.pipeline import PipelineAgent
from sbx_omnigent.runner.captures import TurnCapture
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.harness_record import HarnessRecord
from sbx_omnigent.runner.session_registry import SessionRegistry
from sbx_omnigent.swarm_session import SwarmSessionError

_PICKER = """    Detected a custom API key in your environment

      1. Yes
    > 2. No (recommended)

    Enter to confirm · Esc to cancel"""


class _CaptureTest(unittest.TestCase):
    def setUp(self) -> None:
        echo = mock.patch.object(click, 'echo')
        self.echo = echo.start()
        self.addCleanup(echo.stop)
        self.sessions = mock.Mock()
        self.sessions.read_transcript.return_value = [
            ('user', 'Build it.'), ('assistant', 'Working on it.'),
        ]
        self.sessions.session_host_id.return_value = 'host-1'
        self.sessions.host_name.return_value = 'managed-1'
        self.written: dict[str, str] = {}
        self.wt = mock.Mock()
        self.wt.write_run_artifact.side_effect = (
            lambda run, path, text: self.written.__setitem__(path, text)
            or True
        )
        self.registry = SessionRegistry()
        agent = mock.Mock(spec=PipelineAgent)
        agent.harness = 'claude-native'
        self.registry.register('s1', label='build', agent=agent,
                               worktree='/w/build')
        ctx = RunContext(
            config=mock.Mock(), worktrees=self.wt, sessions=self.sessions,
            run_id='r1', nodes={}, stage_by_id={}, lock=threading.RLock(),
            max_rounds=3,
        )
        self.harness = HarnessRecord(ctx, registry=self.registry,
                                     save_state=lambda: None)
        self.capture = TurnCapture(ctx, registry=self.registry,
                                   harness=self.harness)
        versions = mock.patch.object(
            harness_versions, 'read_versions',
            return_value=dict(harness_versions.KNOWN_GOOD),
        )
        versions.start()
        self.addCleanup(versions.stop)

    def said(self) -> str:
        return '\n'.join(str(c.args[0]) for c in self.echo.call_args_list)


class TestTheTranscript(_CaptureTest):
    def test_it_is_written_under_the_nodes_name(self) -> None:
        self.capture.turn('s1', 'did not finish its turn')
        doc = self.written['turns/build.md']
        self.assertIn('Working on it.', doc)
        self.assertIn('did not finish its turn', doc)

    def test_a_second_capture_does_not_overwrite_the_first(self) -> None:
        self.capture.turn('s1', 'first')
        self.capture.turn('s1', 'second')
        self.assertEqual(sorted(self.written),
                         ['turns/build-2.md', 'turns/build.md'])

    def test_an_unreadable_session_is_still_recorded(self) -> None:
        self.sessions.read_transcript.side_effect = SwarmSessionError('x')
        self.capture.turn('s1', 'gone')
        self.assertIn('turns/build.md', self.written)

    def test_a_routine_capture_reads_no_pane(self) -> None:
        with mock.patch.object(pane, 'capture_pane') as screen:
            self.assertIsNone(self.capture.turn('s1', 'fix turn'))
        screen.assert_not_called()


class TestThePane(_CaptureTest):
    def test_it_is_written_beside_the_transcript(self) -> None:
        with mock.patch.object(pane, 'capture_pane',
                               return_value='$ cargo test'):
            path = self.capture.turn('s1', 'timed out', with_pane=True)
        self.assertEqual(path, 'turns/build.pane.txt')
        self.assertIn('$ cargo test', self.written[path])
        self.assertIn('TUI pane — managed-1', self.written[path])

    def test_the_vms_cli_versions_are_recorded_with_it(self) -> None:
        with mock.patch.object(pane, 'capture_pane', return_value='x'):
            self.capture.turn('s1', 'timed out', with_pane=True)
        self.assertIn('build', self.harness.recorded)

    def test_a_modal_prompt_is_said_where_the_human_looks(self) -> None:
        with mock.patch.object(pane, 'capture_pane', return_value=_PICKER):
            path = self.capture.turn('s1', 'timed out', with_pane=True)
        self.assertIn('THIS IS A MODAL PROMPT', self.written[path])
        self.assertIn('[blocked]', self.said())

    def test_an_empty_screen_writes_no_pane(self) -> None:
        with mock.patch.object(pane, 'capture_pane', return_value=''):
            self.assertIsNone(
                self.capture.turn('s1', 'timed out', with_pane=True),
            )

    def test_a_vm_that_cannot_be_found_writes_no_pane(self) -> None:
        self.sessions.session_host_id.return_value = None
        self.assertIsNone(
            self.capture.turn('s1', 'timed out', with_pane=True),
        )

    def test_a_failing_pane_read_writes_no_pane(self) -> None:
        with mock.patch.object(pane, 'capture_pane',
                               side_effect=RuntimeError('sbx')):
            self.assertIsNone(
                self.capture.turn('s1', 'timed out', with_pane=True),
            )


class TestAnInterrupt(_CaptureTest):
    def test_every_turn_still_running_is_kept_with_its_pane(self) -> None:
        self.registry.in_turn.update({'s1'})
        with mock.patch.object(pane, 'capture_pane', return_value='x'):
            self.capture.interrupted()
        self.assertIn('turns/build.md', self.written)
        self.assertIn('turns/build.pane.txt', self.written)

    def test_nothing_running_captures_nothing(self) -> None:
        self.capture.interrupted()
        self.assertEqual(self.written, {})


if __name__ == '__main__':
    unittest.main()
