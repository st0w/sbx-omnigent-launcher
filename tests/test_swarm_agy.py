"""Stage-3 tests: swarm fail-loud agy gate.

Pure helpers (harness detection, ack resolution) plus the guard that
raises on an agy binding without acknowledgment. No real server. Run:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import os
import unittest
from unittest import mock

import click

from sbx_omnigent import swarm

_AGENTS: list[dict[str, object]] = [
    {
        'id': 'ag_coder',
        'name': 'swarm-agy-coder',
        'harness': 'antigravity-native',
    },
    {
        'id': 'ag_rev',
        'name': 'swarm-agy-reviewer-bug',
        'harness': 'native-antigravity',
    },
    {'id': 'ag_claude', 'name': 'swarm-coder', 'harness': 'claude-native'},
]


class TestDetectAgyBindings(unittest.TestCase):
    """`_detect_agy_bindings` resolves refs to the agy harness."""

    def test_matches_by_name(self) -> None:
        self.assertEqual(
            swarm._detect_agy_bindings(_AGENTS, ['swarm-agy-coder']),
            ['swarm-agy-coder'],
        )

    def test_matches_by_id(self) -> None:
        self.assertEqual(
            swarm._detect_agy_bindings(_AGENTS, ['ag_coder']), ['ag_coder']
        )

    def test_alias_harness_matches(self) -> None:
        self.assertEqual(
            swarm._detect_agy_bindings(_AGENTS, ['ag_rev']), ['ag_rev']
        )

    def test_claude_agent_not_flagged(self) -> None:
        self.assertEqual(
            swarm._detect_agy_bindings(_AGENTS, ['swarm-coder', 'ag_claude']),
            [],
        )

    def test_unknown_ref_ignored(self) -> None:
        self.assertEqual(swarm._detect_agy_bindings(_AGENTS, ['nope']), [])

    def test_dedupes_preserving_order(self) -> None:
        self.assertEqual(
            swarm._detect_agy_bindings(
                _AGENTS, ['ag_coder', 'ag_coder', 'ag_rev']
            ),
            ['ag_coder', 'ag_rev'],
        )

    def test_non_string_harness_ignored(self) -> None:
        agents: list[dict[str, object]] = [
            {'id': 'x', 'name': 'x', 'harness': None}
        ]
        self.assertEqual(swarm._detect_agy_bindings(agents, ['x']), [])


class TestAgyAckEnabled(unittest.TestCase):
    """`_agy_ack_enabled`: explicit flag wins, else env."""

    def test_explicit_true_wins(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertTrue(swarm._agy_ack_enabled(True))

    def test_explicit_false_wins_over_env(self) -> None:
        with mock.patch.dict(os.environ, {'OMNI_SBX_AGY_ENABLED': '1'}):
            self.assertFalse(swarm._agy_ack_enabled(False))

    def test_env_truthy_values(self) -> None:
        for val in ('1', 'true', 'YES', 'on'):
            with mock.patch.dict(os.environ, {'OMNI_SBX_AGY_ENABLED': val}):
                self.assertTrue(swarm._agy_ack_enabled(None))

    def test_env_unset_is_false(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(swarm._agy_ack_enabled(None))

    def test_env_falsy_string_is_false(self) -> None:
        with mock.patch.dict(os.environ, {'OMNI_SBX_AGY_ENABLED': '0'}):
            self.assertFalse(swarm._agy_ack_enabled(None))


class TestGuardAgyBindings(unittest.TestCase):
    """`_guard_agy_bindings`: raise on agy binding, silent otherwise."""

    def test_raises_when_agy_bound(self) -> None:
        with self.assertRaises(click.UsageError) as ctx:
            swarm._guard_agy_bindings(_AGENTS, ['swarm-agy-coder'])
        self.assertIn('agy_enabled', str(ctx.exception))

    def test_silent_when_no_agy_bound(self) -> None:
        swarm._guard_agy_bindings(_AGENTS, ['swarm-coder'])

    def test_empty_catalog_is_silent(self) -> None:
        swarm._guard_agy_bindings([], ['swarm-agy-coder'])


class TestLaunchArgsFor(unittest.TestCase):
    """`_launch_args_for` / `_harness_by_ref` pick per-harness args."""

    def test_agy_harness_gets_skip_permissions(self) -> None:
        self.assertEqual(
            swarm._launch_args_for('antigravity-native'),
            ('--dangerously-skip-permissions',),
        )

    def test_agy_alias_matches(self) -> None:
        self.assertEqual(
            swarm._launch_args_for('native-antigravity'),
            swarm._AGY_LAUNCH_ARGS,
        )

    def test_claude_gets_permission_mode(self) -> None:
        self.assertEqual(
            swarm._launch_args_for('claude-native'),
            ('--permission-mode', 'bypassPermissions'),
        )

    def test_unresolved_defaults_to_claude(self) -> None:
        self.assertEqual(
            swarm._launch_args_for(None), swarm._YOLO_LAUNCH_ARGS
        )

    def test_harness_by_ref_keys_id_and_name(self) -> None:
        refs = swarm._harness_by_ref(_AGENTS)
        self.assertEqual(refs['ag_coder'], 'antigravity-native')
        self.assertEqual(refs['swarm-agy-coder'], 'antigravity-native')
        self.assertEqual(refs['swarm-coder'], 'claude-native')


class TestOrchestratorLaunchArgs(unittest.TestCase):
    """The orchestrator applies harness-appropriate launch args."""

    def _orch(self, harnesses) -> swarm.SwarmOrchestrator:
        return swarm.SwarmOrchestrator(
            session_client=mock.Mock(),
            worktree_manager=mock.Mock(),
            coder_agent_id='ag_coder',
            agent_harnesses=harnesses,
        )

    def test_agy_agent_gets_skip(self) -> None:
        orch = self._orch({'ag_coder': 'antigravity-native'})
        self.assertEqual(
            orch._launch_args_for_agent('ag_coder'),
            ['--dangerously-skip-permissions'],
        )

    def test_claude_agent_gets_permission_mode(self) -> None:
        orch = self._orch({'ag_rev': 'claude-native'})
        self.assertEqual(
            orch._launch_args_for_agent('ag_rev'),
            ['--permission-mode', 'bypassPermissions'],
        )

    def test_no_map_uses_default(self) -> None:
        orch = self._orch(None)
        self.assertEqual(
            orch._launch_args_for_agent('anything'),
            list(swarm._YOLO_LAUNCH_ARGS),
        )


class TestOrchestratorAgyTag(unittest.TestCase):
    """start_swarm tags an agy agent's mount with -agy, others plain."""

    def _workspaces(self, harnesses) -> dict:
        sc = mock.Mock()
        sc.create.side_effect = lambda **kw: kw['agent_id']  # sid = id
        wt = mock.Mock()
        wt.create_swarm_worktree.return_value = '/wt/swarm-a'
        orch = swarm.SwarmOrchestrator(
            session_client=sc,
            worktree_manager=wt,
            coder_agent_id='ag_coder',
            reviewer_agent_id='ag_rev',
            agent_harnesses=harnesses,
        )
        orch.start_swarm('s', 'repo', reviewer_roles=('security',))
        return {
            c.kwargs['agent_id']: c.kwargs['workspace']
            for c in sc.create.call_args_list
        }

    def test_agy_coder_tagged(self) -> None:
        ws = self._workspaces({'ag_coder': 'antigravity-native'})
        self.assertTrue(ws['ag_coder'].endswith('#rw-agy'))

    def test_claude_reviewer_not_tagged(self) -> None:
        ws = self._workspaces(
            {'ag_coder': 'antigravity-native', 'ag_rev': 'claude-native'}
        )
        self.assertTrue(ws['ag_rev'].endswith('#ro'))
        self.assertFalse(ws['ag_rev'].endswith('-agy'))

    def test_no_harness_map_never_tags(self) -> None:
        ws = self._workspaces(None)
        self.assertTrue(ws['ag_coder'].endswith('#rw'))
        self.assertFalse(ws['ag_coder'].endswith('-agy'))


if __name__ == '__main__':
    unittest.main()
