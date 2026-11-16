"""The session driver: creating agent sessions and driving their turns.

Run: python -m unittest tests.test_runner_session_driver
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

import click

from sbx_omnigent import agy
from sbx_omnigent.pipeline import PipelineAgent
from sbx_omnigent.runner.captures import TurnCapture
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import (
    LostTurn,
    PipelineRunError,
    RunInterrupted,
)
from sbx_omnigent.runner.failure_notes import FailureNotes
from sbx_omnigent.runner.harness_record import HarnessRecord
from sbx_omnigent.runner.limits import _FIRST_TURN_MAX_RESUBMITS
from sbx_omnigent.runner.session_driver import _AGY_TASK_FILE, SessionDriver
from sbx_omnigent.runner.session_registry import SessionRegistry
from sbx_omnigent.swarm_session import SwarmSessionError, SwarmTurnResult


def _agent(harness: str) -> PipelineAgent:
    agent = mock.Mock(spec=PipelineAgent)
    agent.harness = harness
    agent.model = 'claude-sonnet-5'
    agent.effort = 'medium'
    return agent


class _DriverTest(unittest.TestCase):
    def setUp(self) -> None:
        echo = mock.patch.object(click, 'echo')
        self.echo = echo.start()
        self.addCleanup(echo.stop)
        self.sessions = mock.Mock()
        self.sessions.create.return_value = 'sess-1'
        self.sessions.read_items.return_value = []
        self.sessions.read_transcript.return_value = []
        self.sessions.get_status.return_value = {}
        # No VM to locate: nothing reads a pane or runs sbx exec.
        self.sessions.session_host_id.return_value = None
        self.sessions.send_and_wait.return_value = SwarmTurnResult(
            'idle', None, 'Done.',
        )
        self.wt = mock.Mock()
        self.registry = SessionRegistry()
        self.interrupted = threading.Event()
        self.swap_age: float | None = 60.0
        self.saves = 0
        self.config = mock.Mock()
        self.config.agents = {
            'build': _agent('claude-native'),
            'plan': _agent(next(iter(agy.AGY_HARNESSES))),
        }

    def driver(self) -> SessionDriver:
        ctx = RunContext(
            config=self.config, worktrees=self.wt, sessions=self.sessions,
            run_id='r1', nodes={}, stage_by_id={}, lock=threading.RLock(),
            max_rounds=3,
        )

        def save() -> None:
            self.saves += 1

        harness = HarnessRecord(ctx, registry=self.registry,
                                save_state=save)
        return SessionDriver(
            ctx,
            registry=self.registry,
            harness=harness,
            capture=TurnCapture(ctx, registry=self.registry,
                                harness=harness),
            notes=FailureNotes(ctx, registry=self.registry, keep=False),
            agent_ids={'build': 'ag-build', 'plan': 'ag-plan'},
            turn_timeout=600.0,
            swap_age_s=lambda: self.swap_age,
            interrupted=self.interrupted,
            save_state=save,
        )

    def sent(self) -> str:
        return self.sessions.send_and_wait.call_args.args[1]


class TestCreating(_DriverTest):
    def test_it_is_created_for_the_agent_and_registered(self) -> None:
        session = self.driver().create('build', '/w/build', 'rw', 'build')
        self.assertEqual(session, 'sess-1')
        kwargs = self.sessions.create.call_args.kwargs
        self.assertEqual(kwargs['agent_id'], 'ag-build')
        self.assertEqual(kwargs['title'], 'r1/build')
        self.assertEqual(kwargs['model_override'], 'claude-sonnet-5')
        self.assertEqual(self.registry.label_of('sess-1'), 'build')
        self.assertEqual(self.registry.worktrees['sess-1'], '/w/build')

    def test_it_is_saved_at_once_so_a_resume_can_reclaim_it(self) -> None:
        self.driver().create('build', '/w/build', 'rw', 'build')
        self.assertEqual(self.saves, 1)

    def test_an_agy_terminal_is_warmed_before_its_first_turn(self) -> None:
        self.driver().create('plan', '/w/plan', 'ro', 'plan')
        self.assertIn('sess-1', self.registry.agy)
        self.sessions.wait_for_terminal_ready.assert_called_once_with(
            'sess-1',
        )

    def test_nothing_is_created_after_an_interrupt(self) -> None:
        self.interrupted.set()
        with self.assertRaises(RunInterrupted):
            self.driver().create('build', '/w/build', 'rw', 'build')
        self.sessions.create.assert_not_called()


class TestDriving(_DriverTest):
    def created(self, agent: str = 'build') -> tuple[SessionDriver, str]:
        driver = self.driver()
        return driver, driver.create(agent, f'/w/{agent}', 'rw', agent)

    def test_the_reply_comes_back(self) -> None:
        driver, session = self.created()
        self.assertEqual(driver.drive(session, 'Build it.'), 'Done.')
        self.assertEqual(self.sent(), 'Build it.')

    def test_only_the_first_turn_may_be_redelivered(self) -> None:
        driver, session = self.created()
        driver.drive(session, 'one')
        first = self.sessions.send_and_wait.call_args.kwargs
        driver.drive(session, 'two')
        second = self.sessions.send_and_wait.call_args.kwargs
        self.assertEqual(first['max_resubmits'], _FIRST_TURN_MAX_RESUBMITS)
        self.assertEqual(second['max_resubmits'], 0)

    def test_the_session_is_in_turn_only_while_it_runs(self) -> None:
        driver, session = self.created()
        seen: list[bool] = []
        self.sessions.send_and_wait.side_effect = lambda *a, **k: (
            seen.append(session in self.registry.in_turn)
            or SwarmTurnResult('idle', None, 'Done.')
        )
        driver.drive(session, 'Build it.')
        self.assertEqual(seen, [True])
        self.assertNotIn(session, self.registry.in_turn)

    def test_a_failed_turn_says_why(self) -> None:
        driver, session = self.created()
        self.sessions.send_and_wait.return_value = SwarmTurnResult(
            'failed', 'the build broke',
        )
        with self.assertRaises(PipelineRunError) as caught:
            driver.drive(session, 'Build it.')
        self.assertNotIsInstance(caught.exception, LostTurn)
        self.assertIn('the build broke', str(caught.exception))

    def test_a_turn_with_no_reason_is_lost_not_failed(self) -> None:
        driver, session = self.created()
        self.sessions.send_and_wait.return_value = SwarmTurnResult(
            'failed', None,
        )
        with self.assertRaises(LostTurn):
            driver.drive(session, 'Build it.')

    def test_a_turn_that_never_returns_is_kept_and_raised(self) -> None:
        driver, session = self.created()
        self.sessions.send_and_wait.side_effect = SwarmSessionError('lost')
        with self.assertRaises(SwarmSessionError):
            driver.drive(session, 'Build it.')
        path = self.wt.write_run_artifact.call_args.args[1]
        self.assertEqual(path, 'turns/build.md')
        self.assertNotIn(session, self.registry.in_turn)

    def test_no_turn_starts_after_an_interrupt(self) -> None:
        driver, session = self.created()
        self.interrupted.set()
        with self.assertRaises(RunInterrupted):
            driver.drive(session, 'Build it.')
        self.sessions.send_and_wait.assert_not_called()


class TestAnAgyTurn(_DriverTest):
    def created(self) -> tuple[SessionDriver, str]:
        driver = self.driver()
        return driver, driver.create('plan', '/w/plan', 'ro', 'plan')

    def test_it_is_handed_over_as_a_file(self) -> None:
        # A multi-line or long paste never renders in agy's composer.
        driver, session = self.created()
        driver.drive(session, 'Plan it.\n\nIn detail.')
        self.wt.write_ignored_file.assert_called_once_with(
            '/w/plan', _AGY_TASK_FILE, 'Plan it.\n\nIn detail.',
        )
        self.assertIn(_AGY_TASK_FILE, self.sent())
        self.assertNotIn('\n', self.sent())

    def test_an_inline_turn_is_flattened_not_staged(self) -> None:
        driver, session = self.created()
        driver.drive(session, 'Say\nVERDICT: now', inline=True)
        self.wt.write_ignored_file.assert_not_called()
        self.assertNotIn('\n', self.sent())

    def test_a_stale_swap_secret_is_refused_before_the_turn(self) -> None:
        # It would hang to the timeout and report nothing.
        driver, session = self.created()
        self.swap_age = agy.MAX_SWAP_AGE_S + 1
        with self.assertRaises(PipelineRunError) as caught:
            driver.drive(session, 'Plan it.')
        self.assertIn('swap secret', str(caught.exception))
        self.sessions.send_and_wait.assert_not_called()

    def test_a_swap_secret_never_recorded_is_refused(self) -> None:
        driver, session = self.created()
        self.swap_age = None
        with self.assertRaises(PipelineRunError) as caught:
            driver.drive(session, 'Plan it.')
        self.assertIn('has never been recorded', str(caught.exception))


class TestTheNewestItem(_DriverTest):
    def test_it_is_the_last_items_id(self) -> None:
        self.sessions.read_items.return_value = [{'id': 'i-9'}]
        self.assertEqual(self.driver().newest_item_id('s1'), 'i-9')

    def test_an_empty_session_has_none(self) -> None:
        self.assertIsNone(self.driver().newest_item_id('s1'))

    def test_an_item_without_a_string_id_has_none(self) -> None:
        self.sessions.read_items.return_value = [{'id': 7}]
        self.assertIsNone(self.driver().newest_item_id('s1'))

    def test_an_unreadable_session_has_none(self) -> None:
        self.sessions.read_items.side_effect = SwarmSessionError('gone')
        self.assertIsNone(self.driver().newest_item_id('s1'))


if __name__ == '__main__':
    unittest.main()
