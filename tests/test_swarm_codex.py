"""Tests for the codex credential check `omni-sbx-swarm start` runs.

The pipeline runner already refused to start on a dead Codex access
token, but `omni-sbx-swarm start` did not check at all, so a swarm with
a Codex agent spent its microVMs finding out on the first turn (#26).
No real server and no real credential. Run:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import base64
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

import click
from click.testing import CliRunner

from sbx_omnigent import codex, swarm

_AGENTS: list[dict[str, object]] = [
    {'id': 'ag_codex', 'name': 'swarm-codex-coder', 'harness': 'codex-native'},
    {'id': 'ag_claude', 'name': 'swarm-coder', 'harness': 'claude-native'},
]


def _credential(exp: datetime) -> Path:
    """A host `auth.json` whose access token expires at *exp*."""
    head = base64.urlsafe_b64encode(b'{"alg":"RS256"}').decode().rstrip('=')
    body = base64.urlsafe_b64encode(
        json.dumps({'exp': int(exp.timestamp()), 'iat': 0}).encode()
    ).decode().rstrip('=')
    doc = {
        'auth_mode': 'chatgpt',
        'OPENAI_API_KEY': None,
        'tokens': {
            'id_token': 'id-token-value',
            'access_token': f'{head}.{body}.{"s" * 32}',
            'refresh_token': 'r' * 196,
            'account_id': 'acct-1',
        },
        'last_refresh': '2026-08-19T00:53:27.420590Z',
    }
    path = Path(tempfile.mkdtemp()) / 'auth.json'
    path.write_text(json.dumps(doc))
    return path


_MISSING = Path('/nonexistent/auth.json')


class TestDetectCodexBindings(unittest.TestCase):
    def test_matches_by_name_and_by_id(self) -> None:
        self.assertEqual(
            swarm._detect_codex_bindings(
                _AGENTS, ['swarm-codex-coder', 'ag_codex']
            ),
            ['swarm-codex-coder', 'ag_codex'],
        )

    def test_a_claude_agent_is_not_codex(self) -> None:
        self.assertEqual(
            swarm._detect_codex_bindings(_AGENTS, ['swarm-coder']), []
        )


class TestPreflightCodexBindings(unittest.TestCase):
    def test_an_expired_token_is_refused_naming_agent_and_remedy(
        self,
    ) -> None:
        path = _credential(datetime.now(tz=UTC) - timedelta(minutes=1))
        with self.assertRaises(click.ClickException) as caught:
            swarm._preflight_codex_bindings(
                _AGENTS, ['swarm-codex-coder'], path=path
            )
        msg = caught.exception.message
        self.assertIn('swarm-codex-coder', msg)
        self.assertIn(codex.RELOGIN_HINT, msg)

    def test_a_missing_credential_is_refused(self) -> None:
        with self.assertRaises(click.ClickException):
            swarm._preflight_codex_bindings(
                _AGENTS, ['ag_codex'], path=_MISSING
            )

    def test_a_token_about_to_expire_warns(self) -> None:
        path = _credential(datetime.now(tz=UTC) + timedelta(hours=1))
        warning = swarm._preflight_codex_bindings(
            _AGENTS, ['ag_codex'], path=path
        )
        self.assertIsNotNone(warning)
        self.assertIn('expires', warning or '')

    def test_a_healthy_token_says_nothing(self) -> None:
        path = _credential(datetime.now(tz=UTC) + timedelta(days=9))
        self.assertIsNone(
            swarm._preflight_codex_bindings(_AGENTS, ['ag_codex'], path=path)
        )

    def test_a_swarm_without_codex_never_reads_the_credential(self) -> None:
        # A Claude-only swarm must not be refused over a Codex login it
        # does not use.
        self.assertIsNone(
            swarm._preflight_codex_bindings(
                _AGENTS, ['swarm-coder'], path=_MISSING
            )
        )

    def test_an_empty_catalog_checks_nothing(self) -> None:
        # Same rule as the agy guard: an unreachable catalog detects no
        # bindings, and the first turn remains the backstop.
        self.assertIsNone(
            swarm._preflight_codex_bindings([], ['ag_codex'], path=_MISSING)
        )


class TestStartRunsTheCodexPreflight(unittest.TestCase):
    """The wiring: `start` checks before it builds anything."""

    _ARGS = (
        'start', '--swarm-id', 's1', '--repo-url', 'https://example/r.git',
        '--canonical-root', '/srv/c', '--worktree-root', '/srv/w',
        '--coder-agent', 'swarm-codex-coder',
        '--reviewer-agent', 'swarm-coder',
    )

    def _invoke(
        self, preflight: mock.Mock
    ) -> tuple[click.testing.Result, mock.Mock]:
        client = mock.Mock()
        client.list_builtin_agents.return_value = _AGENTS
        orch = mock.Mock()
        orch.return_value.start_swarm.side_effect = RuntimeError('stop here')
        with (
            mock.patch.object(
                swarm, 'SwarmSessionClient', return_value=client
            ),
            mock.patch.object(swarm, 'SwarmOrchestrator', orch),
            mock.patch.object(swarm, 'WorktreeManager'),
            mock.patch.object(swarm.codex, 'preflight', preflight),
            # Never run the real `codex doctor` on this host.
            mock.patch.object(swarm.codex, 'probe_login', return_value=None),
        ):
            registry = tempfile.mkdtemp()
            result = CliRunner().invoke(
                swarm.cli, ['--registry', registry, *self._ARGS]
            )
        return result, orch

    def test_a_dead_credential_stops_start_before_any_vm(self) -> None:
        result, orch = self._invoke(
            mock.Mock(side_effect=codex.CodexAuthError('token expired'))
        )
        self.assertEqual(result.exit_code, 1, result.output)
        self.assertIn('swarm-codex-coder', result.output)
        orch.assert_not_called()

    def test_a_warning_goes_to_stderr_so_stdout_stays_json(self) -> None:
        # `start` prints the registry entry as JSON on stdout for a
        # coordinator to parse; a warning there would break it.
        result, orch = self._invoke(mock.Mock(return_value='codex: soon'))
        self.assertIn('[preflight] codex: soon', result.stderr)
        self.assertNotIn('codex: soon', result.stdout)
        orch.assert_called_once()


class TestStartChecksTheLoginAgainstTheServer(unittest.TestCase):
    """The expiry check passed a login the server had rejected (#26), so
    `start` also runs `codex doctor`'s authenticated handshake."""

    _ARGS = TestStartRunsTheCodexPreflight._ARGS

    def _invoke(
        self, probe: mock.Mock, *extra: str
    ) -> tuple[click.testing.Result, mock.Mock]:
        client = mock.Mock()
        client.list_builtin_agents.return_value = _AGENTS
        orch = mock.Mock()
        orch.return_value.start_swarm.side_effect = RuntimeError('stop here')
        with (
            mock.patch.object(
                swarm, 'SwarmSessionClient', return_value=client
            ),
            mock.patch.object(swarm, 'SwarmOrchestrator', orch),
            mock.patch.object(swarm, 'WorktreeManager'),
            mock.patch.object(swarm.codex, 'preflight', return_value=None),
            mock.patch.object(swarm.codex, 'probe_login', probe),
        ):
            registry = tempfile.mkdtemp()
            result = CliRunner().invoke(
                swarm.cli, ['--registry', registry, *self._ARGS, *extra]
            )
        return result, orch

    def test_a_rejected_login_stops_start_before_any_vm(self) -> None:
        result, orch = self._invoke(mock.Mock(
            side_effect=codex.CodexLoginRejected('handshake refused')
        ))
        self.assertEqual(result.exit_code, 1, result.output)
        self.assertIn('swarm-codex-coder', result.output)
        self.assertIn('handshake refused', result.output)
        orch.assert_not_called()

    def test_the_skip_flag_skips_the_probe(self) -> None:
        probe = mock.Mock(
            side_effect=codex.CodexLoginRejected('handshake refused')
        )
        result, orch = self._invoke(probe, codex.SKIP_LOGIN_CHECK_FLAG)
        probe.assert_not_called()
        orch.assert_called_once()
        self.assertIn('not checked', result.stderr)

    def test_the_flag_never_skips_the_expiry_check(self) -> None:
        client = mock.Mock()
        client.list_builtin_agents.return_value = _AGENTS
        orch = mock.Mock()
        with (
            mock.patch.object(
                swarm, 'SwarmSessionClient', return_value=client
            ),
            mock.patch.object(swarm, 'SwarmOrchestrator', orch),
            mock.patch.object(swarm, 'WorktreeManager'),
            mock.patch.object(
                swarm.codex, 'preflight',
                side_effect=codex.CodexAuthError('the token expired'),
            ),
            mock.patch.object(swarm.codex, 'probe_login') as probe,
        ):
            registry = tempfile.mkdtemp()
            result = CliRunner().invoke(swarm.cli, [
                '--registry', registry, *self._ARGS,
                codex.SKIP_LOGIN_CHECK_FLAG,
            ])
        self.assertEqual(result.exit_code, 1, result.output)
        self.assertIn('the token expired', result.output)
        probe.assert_not_called()
        orch.assert_not_called()

    def test_a_probe_warning_goes_to_stderr(self) -> None:
        result, _orch = self._invoke(
            mock.Mock(return_value='codex: doctor unreadable')
        )
        self.assertIn('[preflight] codex: doctor unreadable', result.stderr)
        self.assertNotIn('doctor unreadable', result.stdout)

    def test_a_swarm_without_codex_is_never_probed(self) -> None:
        probe = mock.Mock(return_value=None)
        self.assertIsNone(
            swarm._probe_codex_login(_AGENTS, ['ag_claude'], skip=False,
                                     probe=probe)
        )
        probe.assert_not_called()


class TestStartRefusesACodexEffortItWouldDrop(unittest.TestCase):
    """A bundle pinning `effort: max` on a Codex agent ran at codex's
    default, with nothing said (#53). `start` now refuses it before any
    VM, as the pipeline loader already does (#71)."""

    def test_an_off_ladder_effort_is_refused(self) -> None:
        with self.assertRaises(click.ClickException) as caught:
            swarm._refuse_dropped_codex_efforts(
                _AGENTS, ['ag_codex'], {'ag_codex': 'max'}
            )
        message = caught.exception.format_message()
        self.assertIn('ag_codex', message)
        self.assertIn("'max'", message)
        self.assertIn('xhigh', message)

    def test_a_ladder_effort_passes(self) -> None:
        swarm._refuse_dropped_codex_efforts(
            _AGENTS, ['ag_codex'], {'ag_codex': 'xhigh'}
        )

    def test_no_effort_passes(self) -> None:
        swarm._refuse_dropped_codex_efforts(_AGENTS, ['ag_codex'], {})

    def test_a_claude_agent_is_not_held_to_codex_ladder(self) -> None:
        # Claude takes `max`; its effort reaches it another way.
        swarm._refuse_dropped_codex_efforts(
            _AGENTS, ['ag_claude'], {'ag_claude': 'max'}
        )

    def test_it_matches_a_ref_by_name_too(self) -> None:
        with self.assertRaises(click.ClickException):
            swarm._refuse_dropped_codex_efforts(
                _AGENTS, ['swarm-codex-coder'], {'swarm-codex-coder': 'ultra'}
            )

    def test_start_refuses_before_any_vm(self) -> None:
        client = mock.Mock()
        client.list_builtin_agents.return_value = _AGENTS
        orch = mock.Mock()
        with (
            mock.patch.object(
                swarm, 'SwarmSessionClient', return_value=client
            ),
            mock.patch.object(swarm, 'SwarmOrchestrator', orch),
            mock.patch.object(swarm, 'WorktreeManager'),
            mock.patch.object(swarm.codex, 'preflight', return_value=None),
            mock.patch.object(swarm.codex, 'probe_login', return_value=None),
            mock.patch.object(
                swarm, '_model_effort_by_ref',
                return_value=({}, {'swarm-codex-coder': 'max'}),
            ),
        ):
            registry = tempfile.mkdtemp()
            result = CliRunner().invoke(
                swarm.cli,
                ['--registry', registry,
                 *TestStartRunsTheCodexPreflight._ARGS],
            )
        self.assertEqual(result.exit_code, 1, result.output)
        self.assertIn('swarm-codex-coder', result.output)
        self.assertIn("'max'", result.output)
        orch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
