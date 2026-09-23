"""`swarm_session create` pins a model and an effort (#55).

The command could not pin a model, an effort or launch args, so a
session it created ran at the agent spec's defaults and without the
no-prompt launch args a headless agent needs. It now takes `--model`
and `--effort`, and derives the launch args from the agent's harness in
the server's catalog, with the same rules as `omni-sbx-swarm start`.

Run: .venv/bin/python -m unittest tests.test_swarm_session_create
"""

from __future__ import annotations

import unittest
from unittest import mock

from click.testing import CliRunner, Result

from sbx_omnigent import swarm_session
from sbx_omnigent.launch_args import (
    AGY_LAUNCH_ARGS,
    CODEX_LAUNCH_ARGS,
    YOLO_LAUNCH_ARGS,
)
from sbx_omnigent.swarm_session import SwarmSessionError

_CATALOG: list[dict[str, object]] = [
    {'id': 'ag_claude', 'name': 'coder', 'harness': 'claude-native'},
    {'id': 'ag_codex', 'name': 'codex-coder', 'harness': 'codex-native'},
    {'id': 'ag_agy', 'name': 'agy-coder', 'harness': 'antigravity-native'},
]


class _Client:
    """Stands in for SwarmSessionClient and records the create call."""

    def __init__(self, *, catalog_error: bool = False) -> None:
        self._catalog_error = catalog_error
        self.created: list[dict[str, object]] = []

    def list_builtin_agents(self) -> list[dict[str, object]]:
        if self._catalog_error:
            raise SwarmSessionError('GET /v1/agents failed')
        return list(_CATALOG)

    def create(self, **kwargs: object) -> str:
        self.created.append(kwargs)
        return 'conv_new'


def _create(
    *args: str, client: _Client | None = None
) -> tuple[Result, _Client]:
    fake = client if client is not None else _Client()
    with mock.patch.object(
        swarm_session, 'SwarmSessionClient', return_value=fake
    ):
        result = CliRunner().invoke(swarm_session.cli, ['create', *args])
    return result, fake


class TestTheLaunchArgsComeFromTheHarness(unittest.TestCase):
    def test_a_claude_agent_gets_the_no_prompt_args(self) -> None:
        result, fake = _create('--agent-id', 'ag_claude')
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(
            fake.created[0]['terminal_launch_args'], list(YOLO_LAUNCH_ARGS)
        )

    def test_an_agy_agent_gets_agys_args(self) -> None:
        _result, fake = _create('--agent-id', 'ag_agy')
        self.assertEqual(
            fake.created[0]['terminal_launch_args'], list(AGY_LAUNCH_ARGS)
        )

    def test_the_agent_is_found_by_name_too(self) -> None:
        _result, fake = _create('--agent-id', 'codex-coder')
        self.assertEqual(
            fake.created[0]['terminal_launch_args'], list(CODEX_LAUNCH_ARGS)
        )

    def test_it_prints_the_session_id(self) -> None:
        result, _fake = _create('--agent-id', 'ag_claude')
        self.assertEqual(result.stdout.strip(), 'conv_new')


class TestModelAndEffortArePinned(unittest.TestCase):
    def test_model_and_effort_reach_the_create_body(self) -> None:
        _result, fake = _create(
            '--agent-id', 'ag_claude',
            '--model', 'claude-opus-5-5', '--effort', 'high',
        )
        created = fake.created[0]
        self.assertEqual(created['model_override'], 'claude-opus-5-5')
        self.assertEqual(created['reasoning_effort'], 'high')

    def test_neither_is_sent_when_not_given(self) -> None:
        _result, fake = _create('--agent-id', 'ag_claude')
        self.assertIsNone(fake.created[0]['model_override'])
        self.assertIsNone(fake.created[0]['reasoning_effort'])

    def test_a_codex_effort_rides_the_launch_args(self) -> None:
        # codex-native ignores the create body's effort; `-c` is the
        # only channel that reaches it.
        _result, fake = _create(
            '--agent-id', 'ag_codex', '--effort', 'xhigh'
        )
        self.assertEqual(
            fake.created[0]['terminal_launch_args'],
            [*CODEX_LAUNCH_ARGS, '-c', 'model_reasoning_effort="xhigh"'],
        )

    def test_the_other_options_still_pass_through(self) -> None:
        _result, fake = _create(
            '--agent-id', 'ag_claude', '--workspace', 'git@sbxmount:/w#rw',
            '--parent', 'conv_p', '--title', 'run/build',
        )
        created = fake.created[0]
        self.assertEqual(created['workspace'], 'git@sbxmount:/w#rw')
        self.assertEqual(created['parent_session_id'], 'conv_p')
        self.assertEqual(created['title'], 'run/build')


class TestAnEffortItCannotApplyIsRefused(unittest.TestCase):
    """Refused before any session exists, as `swarm start` and the
    pipeline loader refuse them (#53, #6)."""

    def _refused(self, *args: str, client: _Client | None = None) -> str:
        result, fake = _create(*args, client=client)
        self.assertEqual(result.exit_code, 2, result.output)
        self.assertEqual(fake.created, [])
        return result.output

    def test_a_codex_effort_off_the_ladder(self) -> None:
        output = self._refused('--agent-id', 'ag_codex', '--effort', 'max')
        self.assertIn("'max'", output)
        self.assertIn('xhigh', output)

    def test_an_agy_effort(self) -> None:
        output = self._refused('--agent-id', 'ag_agy', '--effort', 'high')
        self.assertIn('model id', output)

    def test_an_effort_for_an_agent_the_catalog_does_not_list(self) -> None:
        output = self._refused('--agent-id', 'nope', '--effort', 'high')
        self.assertIn('harness', output)

    def test_an_effort_when_the_catalog_cannot_be_read(self) -> None:
        self._refused(
            '--agent-id', 'ag_claude', '--effort', 'high',
            client=_Client(catalog_error=True),
        )


class TestAnUnresolvedHarnessKeepsTodaysBehaviour(unittest.TestCase):
    """No effort to apply: the session is created as before, without
    launch args, and stderr says why."""

    def test_an_unlisted_agent_gets_no_launch_args(self) -> None:
        result, fake = _create('--agent-id', 'nope')
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIsNone(fake.created[0]['terminal_launch_args'])
        self.assertIn('no launch args', result.stderr)

    def test_an_unreadable_catalog_gets_no_launch_args(self) -> None:
        result, fake = _create(
            '--agent-id', 'ag_claude', client=_Client(catalog_error=True)
        )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIsNone(fake.created[0]['terminal_launch_args'])
        self.assertIn('no launch args', result.stderr)

    def test_stdout_is_still_only_the_session_id(self) -> None:
        # A coordinator reads the id from stdout.
        result, _fake = _create('--agent-id', 'nope')
        self.assertEqual(result.stdout.strip(), 'conv_new')


if __name__ == '__main__':
    unittest.main()
