"""The harness-version record: which CLI versions each session ran.

Run: python -m unittest tests.test_runner_harness_record
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

import click

from sbx_omnigent import harness_versions
from sbx_omnigent.pipeline import PipelineAgent
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.harness_record import HarnessRecord
from sbx_omnigent.runner.session_registry import SessionRegistry
from sbx_omnigent.runner.state import (
    RunState,
    _restore_harness_versions,
    encode,
)

_KNOWN = dict(harness_versions.KNOWN_GOOD)


def _agent(harness: str = 'claude-native') -> PipelineAgent:
    agent = mock.Mock(spec=PipelineAgent)
    agent.harness = harness
    return agent


class _RecordTest(unittest.TestCase):
    def setUp(self) -> None:
        echo = mock.patch.object(click, 'echo')
        self.echo = echo.start()
        self.addCleanup(echo.stop)
        self.sessions = mock.Mock()
        self.sessions.session_host_id.return_value = 'host-1'
        self.sessions.host_name.return_value = 'managed-1'
        self.registry = SessionRegistry()
        self.saves = 0
        ctx = RunContext(
            config=mock.Mock(), worktrees=mock.Mock(), sessions=self.sessions,
            run_id='r1', nodes={}, stage_by_id={}, lock=threading.RLock(),
            max_rounds=3,
        )

        def save() -> None:
            self.saves += 1

        self.record = HarnessRecord(
            ctx, registry=self.registry, save_state=save,
        )

    def session(self, sid: str, label: str = 'build',
                harness: str = 'claude-native') -> str:
        self.registry.register(sid, label=label, agent=_agent(harness),
                               worktree=f'/w/{label}')
        return sid

    def said(self) -> str:
        return '\n'.join(str(c.args[0]) for c in self.echo.call_args_list)


class TestTheFirstReading(_RecordTest):
    def test_it_is_kept_per_node_and_saved(self) -> None:
        self.record.note(self.session('s1'), dict(_KNOWN))
        self.assertEqual(self.record.recorded['build']['runs'], 'claude')
        self.assertEqual(self.record.recorded['build']['versions'], _KNOWN)
        self.assertEqual(self.saves, 1)

    def test_a_known_good_version_says_nothing(self) -> None:
        self.record.note(self.session('s1'), dict(_KNOWN))
        self.assertEqual(self.said(), '')

    def test_a_session_of_no_known_agent_is_ignored(self) -> None:
        self.record.note('stranger', dict(_KNOWN))
        self.assertEqual((self.record.recorded, self.saves), ({}, 0))

    def test_nothing_read_records_nothing(self) -> None:
        self.record.note(self.session('s1'), {})
        self.assertEqual(self.record.recorded, {})

    def test_codex_is_compared_as_codex(self) -> None:
        self.record.note(self.session('s1', harness='codex-native'),
                         dict(_KNOWN))
        self.assertEqual(self.record.recorded['build']['runs'], 'codex')


class TestWhatMovedIsSaid(_RecordTest):
    def test_a_mid_session_self_update(self) -> None:
        session = self.session('s1')
        self.record.note(session, dict(_KNOWN))
        self.record.note(session, {**_KNOWN, 'claude': '9.9.9'})
        self.assertIn('It updated itself mid-session', self.said())
        self.assertEqual(self.saves, 1)

    def test_a_later_reading_of_the_same_session_is_not_kept(self) -> None:
        session = self.session('s1')
        self.record.note(session, dict(_KNOWN))
        self.record.note(session, {**_KNOWN, 'claude': '9.9.9'})
        self.assertEqual(
            self.record.recorded['build']['versions']['claude'],
            _KNOWN['claude'],
        )

    def test_another_node_that_ran_an_older_version(self) -> None:
        self.record.note(self.session('s1', label='plan'), dict(_KNOWN))
        self.record.note(self.session('s2', label='build'),
                         {**_KNOWN, 'claude': '9.9.9'})
        self.assertIn('but plan ran', self.said())

    def test_an_earlier_session_of_the_same_node(self) -> None:
        # A retry's dead VM, or what a resume restored.
        self.record.note(self.session('s1'), dict(_KNOWN))
        self.record.note(self.session('s2'), {**_KNOWN, 'claude': '9.9.9'})
        self.assertIn('an earlier session of build', self.said())
        self.assertNotIn('mid-session', self.said())

    def test_off_the_known_good_set_is_said_once_per_cli(self) -> None:
        self.record.note(self.session('s1', label='a'),
                         {**_KNOWN, 'claude': '9.9.9'})
        self.record.note(self.session('s2', label='b'),
                         {**_KNOWN, 'claude': '9.9.9'})
        self.assertEqual(self.said().count('is not the known-good'), 1)

    def test_a_restored_reading_is_not_this_sessions_first(self) -> None:
        # A resume restores readings that belong to no live session.
        self.record.recorded['build'] = {
            'runs': 'claude', 'versions': {'claude': '2.1.266'},
        }
        self.record.note(self.session('s9'), dict(_KNOWN))
        self.assertNotIn('mid-session', self.said())
        self.assertIn('an earlier session of build', self.said())



class TestAResumedRun(_RecordTest):
    """A resume restores the previous attempt's readings, keyed by the
    node. A new VM for that node was compared against them as if it
    were the same session: "claude 2.1.280 is installed now, but it ran
    2.1.266 at its first turn. It updated itself mid-session." It was a
    different VM, pinned to 2.1.280 before it started."""

    def resume(self, claude: str) -> None:
        self.record.recorded = _restore_harness_versions({
            'build': {
                'runs': 'claude',
                'versions': {**_KNOWN, 'claude': '2.1.266'},
            },
        })
        self.record.note(
            self.session('sess-new'), {**_KNOWN, 'claude': claude},
        )

    def test_it_is_reported_as_an_earlier_session(self) -> None:
        self.resume('2.1.280')
        self.assertNotIn('mid-session', self.said())
        self.assertIn('an earlier session of build ran 2.1.266', self.said())

    def test_the_new_session_replaces_the_record(self) -> None:
        self.resume('2.1.280')
        self.assertEqual(
            self.record.recorded['build']['versions']['claude'], '2.1.280',
        )

    def test_the_same_version_says_nothing(self) -> None:
        self.resume('2.1.266')
        self.assertNotIn('earlier session', self.said())
        self.assertNotIn('mid-session', self.said())

    def test_the_session_is_not_written_to_the_state_file(self) -> None:
        # Sessions never survive a resume; the state format stays put.
        self.resume('2.1.280')
        saved = encode(RunState(
            run_id='r1', pipeline='demo',
            harness_versions=self.record.recorded,
        ))['harness_versions']['build']
        self.assertEqual(set(saved), {'runs', 'versions'})

class TestReadingAVm(_RecordTest):
    def test_the_first_turns_versions_are_read_from_its_vm(self) -> None:
        session = self.session('s1')
        with mock.patch.object(harness_versions, 'read_versions',
                               return_value=dict(_KNOWN)) as read:
            self.record.record_first_turn(session)
        read.assert_called_once_with('managed-1')
        self.assertIn('build', self.record.recorded)

    def test_a_vm_that_cannot_be_found_is_not_read(self) -> None:
        self.sessions.session_host_id.return_value = None
        with mock.patch.object(harness_versions, 'read_versions') as read:
            self.record.record_first_turn(self.session('s1'))
        read.assert_not_called()

    def test_a_failing_read_records_nothing(self) -> None:
        with mock.patch.object(harness_versions, 'read_versions',
                               side_effect=RuntimeError('sbx')):
            self.record.record_first_turn(self.session('s1'))
        self.assertEqual(self.record.recorded, {})


if __name__ == '__main__':
    unittest.main()
