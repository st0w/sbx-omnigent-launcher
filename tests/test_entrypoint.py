"""Unit tests for the bundled-agent auto-registration in entrypoint.

Exercises the real packaged ``agents/`` bundle (coordinator + coder +
reviewer template). ``os.environ`` is patched so the process env is not
mutated. Run with:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import io
import os
import unittest
from typing import ClassVar
from unittest import mock

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
from sbx_omnigent.launcher import (
    DEFAULT_EGRESS_ALLOW,
    DEFAULT_PROVISION_STAGGER_S,
)


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


class TestWarnOnGlobalAllowRules(unittest.TestCase):
    """Startup advisory for broad global sbx allow rules."""

    _GLOBALS = (
        'PROVENANCE APPLIES_TO POLICY TYPE DECISION RESOURCES\n'
        'local all default-ai-services network allow api.anthropic.com\n'
        '                                            openai.com\n'
        'local all default-package-managers network allow pypi.org\n'
        'local sandbox:box-1 rule-x network allow oauth2.googleapis.com\n'
    )
    _NO_GLOBALS = (
        'PROVENANCE APPLIES_TO POLICY TYPE DECISION RESOURCES\n'
        'local sandbox:box-1 rule-x network allow oauth2.googleapis.com\n'
    )

    def _run(self, stdout: str, rc: int = 0) -> str:
        proc = mock.Mock(returncode=rc, stdout=stdout)
        with (
            mock.patch(
                'sbx_omnigent.entrypoint.subprocess.run', return_value=proc
            ),
            mock.patch('sys.stderr', io.StringIO()) as err,
        ):
            warn_on_global_allow_rules()
        return err.getvalue()

    def test_warns_and_counts_only_global_allows(self) -> None:
        out = self._run(self._GLOBALS)
        self.assertIn('sbx policy reset && sbx policy init deny-all', out)
        # 2 global 'all' allows; the scoped rule is excluded.
        self.assertIn('2 global', out)

    def test_silent_when_no_global_allows(self) -> None:
        self.assertEqual(self._run(self._NO_GLOBALS), '')

    def test_silent_on_nonzero_exit(self) -> None:
        self.assertEqual(self._run(self._GLOBALS, rc=1), '')

    def test_never_raises_when_sbx_missing(self) -> None:
        with (
            mock.patch(
                'sbx_omnigent.entrypoint.subprocess.run',
                side_effect=FileNotFoundError,
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

    def test_patch_forces_enterprise_true(self) -> None:
        from omnigent import (  # noqa: PLC0415
            antigravity_native_bridge as bridge,
        )

        state = bridge._AGY_ONBOARDING_COMPLETE_STATE
        original = state.get('enterpriseOnboardingComplete')
        try:
            state['enterpriseOnboardingComplete'] = False
            install_agy_enterprise_onboarding_patch()
            self.assertIs(state['enterpriseOnboardingComplete'], True)
        finally:
            state['enterpriseOnboardingComplete'] = original

    def test_patch_no_op_when_bridge_missing(self) -> None:
        # Simulate older Omnigent: import raises -> best-effort no-op.
        with mock.patch.dict(
            'sys.modules', {'omnigent.antigravity_native_bridge': None}
        ):
            install_agy_enterprise_onboarding_patch()  # must not raise


if __name__ == '__main__':
    unittest.main()


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
                'claude': {'kind': 'subscription', 'cli': 'claude'},
                'codex': {'kind': 'subscription', 'cli': 'codex'},
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

