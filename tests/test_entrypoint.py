"""Unit tests for the bundled-agent auto-registration in entrypoint.

Exercises the real packaged ``agents/`` bundle (coordinator + coder +
reviewer template). ``os.environ`` is patched so the process env is not
mutated. Run with:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import types
import unittest
from typing import ClassVar
from unittest import mock

from sbx_omnigent.defaults import DEFAULT_EGRESS_ALLOW
from sbx_omnigent.entrypoint import (
    _BUILTIN_AGENT_DIRS_ENV,
    _NO_SWARM_AGENTS_ENV,
    _as_bool,
    _as_egress_allow,
    _as_stagger,
    _build_sbx_config,
    _bundled_agent_dirs,
    install_agy_enterprise_onboarding_patch,
    register_bundled_agents,
    warn_on_global_allow_rules,
)
from sbx_omnigent.launcher import DEFAULT_PROVISION_STAGGER_S


class TestBundledAgentDirs(unittest.TestCase):
    def test_discovers_the_packaged_agents(self) -> None:
        dirs = _bundled_agent_dirs()
        names = {os.path.basename(d) for d in dirs}
        # The bundle ships at least these three; clones add more.
        self.assertIn('swarm-coordinator', names)
        self.assertIn('swarm-coder', names)
        self.assertIn('swarm-reviewer-security', names)
        # Every entry is an agent directory (holds a config.yaml).
        for d in dirs:
            self.assertTrue(os.path.isfile(os.path.join(d, 'config.yaml')))


class TestRegisterBundledAgents(unittest.TestCase):
    def test_sets_env_when_unset(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(_BUILTIN_AGENT_DIRS_ENV, None)
            os.environ.pop(_NO_SWARM_AGENTS_ENV, None)
            register_bundled_agents()
            value = os.environ[_BUILTIN_AGENT_DIRS_ENV]
        parts = value.split(os.pathsep)
        self.assertEqual(sorted(parts), sorted(_bundled_agent_dirs()))

    def test_opt_out_leaves_env_untouched(self) -> None:
        with mock.patch.dict(
            os.environ, {_NO_SWARM_AGENTS_ENV: '1'}, clear=False
        ):
            os.environ.pop(_BUILTIN_AGENT_DIRS_ENV, None)
            register_bundled_agents()
            self.assertNotIn(_BUILTIN_AGENT_DIRS_ENV, os.environ)

    def test_appends_preserving_existing_entries(self) -> None:
        with mock.patch.dict(
            os.environ,
            {_BUILTIN_AGENT_DIRS_ENV: '/my/own/agent'},
            clear=False,
        ):
            os.environ.pop(_NO_SWARM_AGENTS_ENV, None)
            register_bundled_agents()
            parts = os.environ[_BUILTIN_AGENT_DIRS_ENV].split(os.pathsep)
        # The user's own entry stays first; ours are appended.
        self.assertEqual(parts[0], '/my/own/agent')
        for d in _bundled_agent_dirs():
            self.assertIn(d, parts)

    def test_idempotent_no_duplicates(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(_BUILTIN_AGENT_DIRS_ENV, None)
            os.environ.pop(_NO_SWARM_AGENTS_ENV, None)
            register_bundled_agents()
            register_bundled_agents()
            parts = os.environ[_BUILTIN_AGENT_DIRS_ENV].split(os.pathsep)
        self.assertEqual(len(parts), len(set(parts)))


class TestAsStagger(unittest.TestCase):
    """``_as_stagger`` config coercion for ``provision_stagger_s``."""

    def test_none_returns_default(self) -> None:
        self.assertEqual(_as_stagger(None), DEFAULT_PROVISION_STAGGER_S)

    def test_zero_disables_gap(self) -> None:
        self.assertEqual(_as_stagger(0), 0.0)

    def test_int_and_float_accepted(self) -> None:
        self.assertEqual(_as_stagger(3), 3.0)
        self.assertEqual(_as_stagger(1.5), 1.5)

    def test_negative_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _as_stagger(-1)

    def test_bool_rejected(self) -> None:
        # bool is an int subclass — must not slip through as 0/1.
        with self.assertRaises(ValueError):
            _as_stagger(True)

    def test_non_number_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _as_stagger('2')


class TestAsEgressAllow(unittest.TestCase):
    """``_as_egress_allow`` resolution for ``egress_allow``."""

    def test_unset_returns_curated_default(self) -> None:
        self.assertEqual(_as_egress_allow(None), DEFAULT_EGRESS_ALLOW)

    def test_empty_list_is_dialback_only(self) -> None:
        # [] = lock down (dial-back only), NOT the curated default.
        self.assertEqual(_as_egress_allow([]), ())

    def test_default_carries_an_llm_endpoint(self) -> None:
        # Agents must work out of the box.
        self.assertIn('api.anthropic.com', DEFAULT_EGRESS_ALLOW)
        self.assertTrue(DEFAULT_EGRESS_ALLOW)

    def test_non_empty_list_replaces_default(self) -> None:
        self.assertEqual(
            _as_egress_allow(['api.anthropic.com']),
            ('api.anthropic.com',),
        )

    def test_non_list_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _as_egress_allow('api.anthropic.com')

    def test_non_string_entry_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _as_egress_allow(['ok', 123])

    def test_empty_string_entry_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _as_egress_allow(['ok', ''])


def _rule(
    name: str,
    resources: object,
    *,
    scope: str = 'global',
    decision: str = 'allow',
    origin: str = 'local',
    status: str = 'active',
) -> dict[str, object]:
    """One rule as ``sbx policy ls --type network --json`` lists it."""
    return {
        'name': name, 'scope': scope, 'decision': decision,
        'origin': origin, 'status': status, 'resources': resources,
    }


class TestWarnOnGlobalAllowRules(unittest.TestCase):
    """Startup advisory for broad global sbx allow rules.

    It counted lines of ``sbx policy ls`` text, which now prints one
    summary row per policy, so sbx's six default bundles (194 host
    patterns) were reported as one rule. It reads ``--json`` instead.
    """

    _GLOBALS: ClassVar[list[dict[str, object]]] = [
        _rule('default-ai-services', ['api.example.com:443', '**.a.example']),
        _rule(
            'default-package-managers', ['pypi.org:443', 'crates.io', 'x.io']
        ),
        _rule('scoped', ['one.example'], scope='sandbox:box-1'),
        _rule('a-deny', ['bad.example'], decision='deny'),
        _rule('off', ['off.example'], status='inactive'),
        _rule('from-org', ['org.example'], origin='org'),
    ]

    def _run(self, stdout: str, rc: int = 0) -> tuple[str, mock.Mock]:
        proc = mock.Mock(returncode=rc, stdout=stdout)
        with (
            mock.patch(
                'sbx_omnigent.entrypoint.subprocess.run', return_value=proc
            ) as run,
            mock.patch('sys.stderr', io.StringIO()) as err,
        ):
            warn_on_global_allow_rules()
        return err.getvalue(), run

    def _note(self, rules: object) -> str:
        return self._run(json.dumps({'rules': rules}))[0]

    def test_it_reads_the_json_listing(self) -> None:
        _out, run = self._run(json.dumps({'rules': []}))
        self.assertEqual(
            run.call_args.args[0],
            ['sbx', 'policy', 'ls', '--type', 'network', '--json'],
        )

    def test_it_counts_rules_and_the_hosts_they_allow(self) -> None:
        out = self._note(self._GLOBALS)
        self.assertIn('2 global sbx network allow rule(s)', out)
        self.assertIn('5 host pattern(s)', out)

    def test_it_names_each_counted_rule(self) -> None:
        out = self._note(self._GLOBALS)
        self.assertIn('default-ai-services (2)', out)
        self.assertIn('default-package-managers (3)', out)
        for left_out in ('scoped', 'a-deny', 'off', 'from-org'):
            with self.subTest(rule=left_out):
                self.assertNotIn(left_out, out)

    def test_it_still_offers_the_deny_all_baseline(self) -> None:
        self.assertIn(
            'sbx policy reset && sbx policy init deny-all',
            self._note(self._GLOBALS),
        )

    def test_silent_under_a_deny_all_baseline(self) -> None:
        self.assertEqual(self._note([]), '')

    def test_silent_when_only_other_rules_exist(self) -> None:
        self.assertEqual(self._note(self._GLOBALS[2:]), '')

    def test_a_rule_name_cannot_write_to_the_terminal(self) -> None:
        out = self._note([_rule('evil\x1b[2Jname\n', ['a.example'])])
        self.assertIn('evil[2Jname (1)', out)
        self.assertNotIn('\x1b', out)

    def test_unreadable_output_says_so_instead_of_a_count(self) -> None:
        for stdout in (
            'POLICY SOURCE APPLIES TO SUMMARY', '[]', '{}',
            '{"rules": {}}', '{"rules": ["x"]}',
            json.dumps({'rules': [_rule('r', 'a.example')]}),
        ):
            with self.subTest(stdout=stdout):
                out, _run = self._run(stdout)
                self.assertIn('could not read', out)
                self.assertIn('sbx policy ls --type network', out)
                self.assertNotIn('rule(s)', out)

    def test_silent_on_nonzero_exit(self) -> None:
        self.assertEqual(self._run('', rc=1)[0], '')

    def test_never_raises_when_sbx_missing_or_hung(self) -> None:
        for failure in (
            FileNotFoundError(),
            subprocess.TimeoutExpired(['sbx'], 15),
        ):
            with self.subTest(failure=type(failure).__name__):
                with (
                    mock.patch(
                        'sbx_omnigent.entrypoint.subprocess.run',
                        side_effect=failure,
                    ),
                    mock.patch('sys.stderr', io.StringIO()) as err,
                ):
                    warn_on_global_allow_rules()  # must not raise
                self.assertEqual(err.getvalue(), '')


class TestAgyEnterprisePatch(unittest.TestCase):
    """The enterprise-onboarding monkeypatch + its bool config gate."""

    def test_bool_coercion(self) -> None:
        self.assertIs(_as_bool(None, 'x'), False)
        self.assertIs(_as_bool(True, 'x'), True)
        self.assertIs(_as_bool(False, 'x'), False)

    def test_bool_rejects_non_bool(self) -> None:
        for bad in ('true', 1, 0, []):
            with self.assertRaises(ValueError):
                _as_bool(bad, 'x')

    @staticmethod
    def _bridge(state: object) -> types.SimpleNamespace:
        """A stand-in agy bridge carrying *state* as its seed."""
        return types.SimpleNamespace(_AGY_ONBOARDING_COMPLETE_STATE=state)

    def _patch_with(self, bridge: types.SimpleNamespace | None) -> None:
        with mock.patch(
            'sbx_omnigent.entrypoint.load_agy_bridge', return_value=bridge
        ):
            install_agy_enterprise_onboarding_patch()

    def test_patch_forces_enterprise_true(self) -> None:
        # A stand-in bridge, so this runs whether or not the installed
        # Omnigent ships one; the shim that finds the real one is
        # tested in tests/test_compat.py.
        state: dict[str, object] = {'enterpriseOnboardingComplete': False}
        self._patch_with(self._bridge(state))
        self.assertIs(state['enterpriseOnboardingComplete'], True)

    def test_patch_leaves_other_seed_keys_alone(self) -> None:
        state: dict[str, object] = {
            'enterpriseOnboardingComplete': False, 'other': 'kept',
        }
        self._patch_with(self._bridge(state))
        self.assertEqual(state['other'], 'kept')

    def test_patch_no_op_when_the_seed_is_not_a_mapping(self) -> None:
        # A bridge whose constant changed shape must not break startup.
        bridge = self._bridge(('not', 'a', 'dict'))
        self._patch_with(bridge)
        self.assertEqual(
            bridge._AGY_ONBOARDING_COMPLETE_STATE, ('not', 'a', 'dict')
        )

    def test_patch_no_op_when_bridge_missing(self) -> None:
        # An Omnigent with no agy bridge under ANY known path ->
        # best-effort no-op rather than a failed startup.
        with mock.patch(
            'sbx_omnigent.entrypoint.load_agy_bridge', return_value=None
        ):
            install_agy_enterprise_onboarding_patch()  # must not raise

    def test_patch_resolves_the_bridge_through_the_shim(self) -> None:
        # Guard against a future edit hard-coding a module path again.
        with mock.patch(
            'sbx_omnigent.entrypoint.load_agy_bridge', return_value=None
        ) as loader:
            install_agy_enterprise_onboarding_patch()
        loader.assert_called_once_with()


class TestHostConfigReachesTheSandbox(unittest.TestCase):
    """``sandbox.host_config`` is the verbatim in-sandbox
    ``~/.omnigent/config.yaml`` the server injects before the host
    starts — and it is where the ``providers:`` block lives, which
    the in-VM harness reads to decide how a model is actually
    reached.

    This config is built by hand rather than by upstream's parser,
    and it simply never read the key: every VM booted with NO
    ``~/.omnigent/config.yaml`` at all. Live, a codex-native agent
    could not see the ChatGPT subscription the launcher had just
    seeded into it, so Omnigent fell back to a gateway shape with no
    provider definition behind it and the Codex app-server never
    started a thread."""

    _RAW: ClassVar[dict[str, object]] = {
        'server_url': 'http://host.docker.internal:6767',
        'worktree_root': '/srv/worktrees',
    }

    def test_a_providers_block_is_threaded_through_verbatim(self) -> None:
        host_config = {
            'providers': {
                # `default:` is what makes launch routing use an entry
                # (#34); without it both would be written and ignored.
                'claude': {
                    'kind': 'subscription', 'cli': 'claude', 'default': True
                },
                'codex': {
                    'kind': 'subscription', 'cli': 'codex', 'default': True
                },
            }
        }
        cfg = _build_sbx_config({**self._RAW, 'host_config': host_config})
        self.assertEqual(cfg.host_config, host_config)

    def test_absent_stays_none_so_nothing_is_written(self) -> None:
        self.assertIsNone(_build_sbx_config(dict(self._RAW)).host_config)

    def test_a_malformed_providers_block_fails_at_startup(self) -> None:
        # Server startup is the only place a typo in this block can
        # fail loud; otherwise it surfaces as an agent that cannot
        # reach a model.
        for bad in ('nope', {'codex': {'kind': 'subscription'}}):
            with self.assertRaises(ValueError):
                _build_sbx_config(
                    {**self._RAW, 'host_config': {'providers': bad}}
                )

    def test_an_inline_api_key_is_refused(self) -> None:
        # It would ride to every sandbox in plaintext on every launch.
        with self.assertRaises(ValueError):
            _build_sbx_config({
                **self._RAW,
                'host_config': {
                    'providers': {
                        'openai': {
                            'kind': 'key',
                            'openai': {'api_key': 'sk-inline-secret'},
                        }
                    }
                },
            })


if __name__ == '__main__':
    unittest.main()
