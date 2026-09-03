"""Unit tests for :class:`sbx_omnigent.launcher.SbxLauncher`.

Pure-logic coverage — no live ``sbx``: the mount-sentinel parser, the
``worktree_root`` allowlist, the ``sbx create`` argv builder, and the
``start_host`` dispatch (mounting vs. delegating). Run with:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
import time
import unittest
from unittest import mock

import click

from sbx_omnigent import launcher as launcher_mod
from sbx_omnigent.launcher import _MOUNT_SENTINEL_PREFIX, SbxLauncher


def _sentinel(path: str, mode: str | None = None) -> str:
    """Build a ``git@sbxmount:`` sentinel workspace URL."""
    url = f'{_MOUNT_SENTINEL_PREFIX}{path}'
    return url if mode is None else f'{url}#{mode}'


class TestParseMountSentinel(unittest.TestCase):
    """The ``git@sbxmount:<path>#<mode>[-agy]`` splitter."""

    def test_rw_mode(self) -> None:
        path, mode, agy = SbxLauncher._parse_mount_sentinel(
            f'{_MOUNT_SENTINEL_PREFIX}/srv/worktrees/a', 'rw'
        )
        self.assertEqual(path, '/srv/worktrees/a')
        self.assertEqual(mode, 'rw')
        self.assertFalse(agy)

    def test_ro_mode(self) -> None:
        _, mode, agy = SbxLauncher._parse_mount_sentinel(
            f'{_MOUNT_SENTINEL_PREFIX}/srv/worktrees/a', 'ro'
        )
        self.assertEqual(mode, 'ro')
        self.assertFalse(agy)

    def test_missing_fragment_defaults_to_rw(self) -> None:
        _, mode, agy = SbxLauncher._parse_mount_sentinel(
            f'{_MOUNT_SENTINEL_PREFIX}/srv/worktrees/a', None
        )
        self.assertEqual(mode, 'rw')
        self.assertFalse(agy)

    def test_agy_suffix_rw(self) -> None:
        path, mode, agy = SbxLauncher._parse_mount_sentinel(
            f'{_MOUNT_SENTINEL_PREFIX}/srv/worktrees/a', 'rw-agy'
        )
        self.assertEqual((path, mode), ('/srv/worktrees/a', 'rw'))
        self.assertTrue(agy)

    def test_agy_suffix_ro(self) -> None:
        _, mode, agy = SbxLauncher._parse_mount_sentinel(
            f'{_MOUNT_SENTINEL_PREFIX}/srv/worktrees/a', 'ro-agy'
        )
        self.assertEqual(mode, 'ro')
        self.assertTrue(agy)

    def test_empty_path_rejected(self) -> None:
        with self.assertRaises(click.ClickException):
            SbxLauncher._parse_mount_sentinel(_MOUNT_SENTINEL_PREFIX, 'rw')

    def test_bad_mode_rejected(self) -> None:
        with self.assertRaises(click.ClickException):
            SbxLauncher._parse_mount_sentinel(
                f'{_MOUNT_SENTINEL_PREFIX}/srv/worktrees/a', 'zz'
            )


class TestResolveWorktreePath(unittest.TestCase):
    """The ``worktree_root`` allowlist (the mount choke point)."""

    def setUp(self) -> None:
        self.root = tempfile.mkdtemp(prefix='wt-root-')
        self.outside = tempfile.mkdtemp(prefix='wt-out-')
        self.swarm = os.path.join(self.root, 'swarm-a')
        os.mkdir(self.swarm)
        self.launcher = SbxLauncher(worktree_root=self.root)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)
        shutil.rmtree(self.outside, ignore_errors=True)

    def test_valid_subdir_returns_realpath(self) -> None:
        resolved = self.launcher._resolve_worktree_path(self.swarm)
        self.assertEqual(resolved, os.path.realpath(self.swarm))

    def test_root_itself_rejected(self) -> None:
        with self.assertRaises(click.ClickException):
            self.launcher._resolve_worktree_path(self.root)

    def test_path_outside_root_rejected(self) -> None:
        with self.assertRaises(click.ClickException):
            self.launcher._resolve_worktree_path(self.outside)

    def test_dotdot_escape_rejected(self) -> None:
        escape = os.path.join(self.swarm, '..', '..')
        with self.assertRaises(click.ClickException):
            self.launcher._resolve_worktree_path(escape)

    def test_symlink_out_rejected(self) -> None:
        link = os.path.join(self.root, 'evil')
        os.symlink(self.outside, link)
        with self.assertRaises(click.ClickException):
            self.launcher._resolve_worktree_path(link)

    def test_nonexistent_rejected(self) -> None:
        with self.assertRaises(click.ClickException):
            self.launcher._resolve_worktree_path(
                os.path.join(self.root, 'nope')
            )

    def test_unset_root_rejected(self) -> None:
        with self.assertRaises(click.ClickException):
            SbxLauncher(worktree_root=None)._resolve_worktree_path(self.swarm)


class TestCreateSandboxCommand(unittest.TestCase):
    """The ``sbx create shell`` argv builder."""

    def test_coder_single_rw_workspace(self) -> None:
        cmd = SbxLauncher(image='img:1')._create_sandbox_command(
            'box', ['/srv/worktrees/a']
        )
        self.assertEqual(
            cmd,
            [
                'sbx',
                'create',
                'shell',
                '/srv/worktrees/a',
                '--template',
                'img:1',
                '--name',
                'box',
                '--quiet',
            ],
        )

    def test_reviewer_ro_suffix_is_literal_arg(self) -> None:
        cmd = SbxLauncher(image='img:1')._create_sandbox_command(
            'box', ['/scratch', '/srv/worktrees/a:ro']
        )
        # The ``:ro`` rides as its own literal argv entry — never a
        # shell token that word-splitting or a zsh modifier could eat.
        self.assertIn('/srv/worktrees/a:ro', cmd)
        self.assertEqual(cmd[3:5], ['/scratch', '/srv/worktrees/a:ro'])

    def test_optional_flags_appended(self) -> None:
        cmd = SbxLauncher(
            image='img:1', profile='p', cpus=4, memory='8g'
        )._create_sandbox_command('box', ['/w'])
        self.assertEqual(
            cmd[-6:],
            ['--profile', 'p', '--cpus', '4', '--memory', '8g'],
        )


class TestProxyForwarding(unittest.TestCase):
    """Forwarding sbx's proxy env to the runner/harness."""

    def test_server_host_extracted_from_command(self) -> None:
        cmd = (
            'OMNIGENT_HOST_TOKEN=t omnigent host --server '
            "'http://host.docker.internal:6767'"
        )
        self.assertEqual(
            SbxLauncher._server_host_from_command(cmd),
            'host.docker.internal',
        )

    def test_server_host_none_when_absent(self) -> None:
        self.assertIsNone(
            SbxLauncher._server_host_from_command('omnigent host')
        )

    def test_assignments_pass_through_proxy_names(self) -> None:
        got = SbxLauncher()._proxy_launch_assignments(
            'omnigent host --server http://remote.example:8443'
        )
        joined = ' '.join(got)
        self.assertIn('OMNIGENT_RUNNER_ENV_PASSTHROUGH=', joined)
        for name in ('HTTPS_PROXY', 'NO_PROXY', 'NODE_USE_ENV_PROXY'):
            self.assertIn(name, joined)

    def test_no_proxy_keeps_server_and_locals_direct(self) -> None:
        got = SbxLauncher()._proxy_launch_assignments(
            'omnigent host --server http://remote.example:8443'
        )
        no_proxy = next(a for a in got if a.startswith('NO_PROXY='))
        for host in (
            'localhost',
            'host.docker.internal',
            'harness.local',
            'remote.example',
        ):
            self.assertIn(host, no_proxy)

    def test_no_proxy_excludes_credential_hosts(self) -> None:
        # api.anthropic.com must NOT be bypassed — it needs the proxy
        # for credential injection.
        got = SbxLauncher()._proxy_launch_assignments(
            'omnigent host --server http://host.docker.internal:6767'
        )
        no_proxy = next(a for a in got if a.startswith('NO_PROXY='))
        self.assertNotIn('anthropic', no_proxy)


class TestStartHostDispatch(unittest.TestCase):
    """``start_host`` routes sentinel vs. non-sentinel correctly."""

    def setUp(self) -> None:
        self.root = tempfile.mkdtemp(prefix='wt-root-')
        self.swarm = os.path.join(self.root, 'swarm-a')
        os.mkdir(self.swarm)
        self.real = os.path.realpath(self.swarm)
        self.launcher = SbxLauncher(worktree_root=self.root)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _run(self, **kwargs: object) -> str:
        defaults: dict[str, object] = {
            'token': 't',
            'host_id': 'h',
            'host_name': 'n',
            'server_url': 'http://x:6767',
        }
        defaults.update(kwargs)
        return self.launcher.start_host('box', **defaults)  # type: ignore[arg-type]

    def test_sentinel_rw_mounts_worktree(self) -> None:
        with (
            mock.patch.object(self.launcher, '_create_sandbox') as cs,
            mock.patch.object(self.launcher, '_launch_host') as lh,
            mock.patch.object(self.launcher, '_seed_claude_settings'),
        ):
            ws = self._run(repo_url=_sentinel(self.swarm), repo_branch='rw')
        cs.assert_called_once_with('box', [self.real])
        lh.assert_called_once()
        self.assertEqual(ws, self.real)

    def test_sentinel_ro_adds_scratch_and_ro_suffix(self) -> None:
        with (
            mock.patch.object(self.launcher, '_create_sandbox') as cs,
            mock.patch.object(self.launcher, '_launch_host'),
            mock.patch.object(
                self.launcher, '_make_scratch', return_value='/scratch'
            ),
            mock.patch.object(self.launcher, '_seed_claude_settings'),
        ):
            ws = self._run(repo_url=_sentinel(self.swarm), repo_branch='ro')
        cs.assert_called_once_with('box', ['/scratch', f'{self.real}:ro'])
        self.assertEqual(ws, self.real)

    def test_non_sentinel_repo_url_delegates(self) -> None:
        with (
            mock.patch.object(self.launcher, '_create_sandbox') as cs,
            mock.patch.object(
                self.launcher, '_make_scratch', return_value='/scratch'
            ),
            mock.patch(
                'sbx_omnigent.launcher.ExecModelHostLauncher.start_host',
                return_value='/root/workspace/repo',
            ) as base,
        ):
            ws = self._run(
                repo_url='git@github.com:org/repo.git',
                repo_branch='main',
                repo_name='repo',
            )
        cs.assert_called_once_with('box', ['/scratch'])
        base.assert_called_once()
        self.assertEqual(ws, '/root/workspace/repo')

    def test_none_workspace_delegates(self) -> None:
        with (
            mock.patch.object(self.launcher, '_create_sandbox') as cs,
            mock.patch.object(
                self.launcher, '_make_scratch', return_value='/scratch'
            ),
            mock.patch(
                'sbx_omnigent.launcher.ExecModelHostLauncher.start_host',
                return_value='/root/workspace',
            ),
        ):
            ws = self._run(repo_url=None)
        cs.assert_called_once_with('box', ['/scratch'])
        self.assertEqual(ws, '/root/workspace')


class TestCreateStagger(unittest.TestCase):
    """``_create_sandbox`` serialization + inter-create settle gap."""

    def setUp(self) -> None:
        # Module-global holder; reset so each test starts fresh.
        self._saved = launcher_mod._LAST_CREATE_DONE[0]
        launcher_mod._LAST_CREATE_DONE[0] = 0.0

    def tearDown(self) -> None:
        launcher_mod._LAST_CREATE_DONE[0] = self._saved

    def _launcher(self, stagger: float) -> SbxLauncher:
        return SbxLauncher(image='img:1', provision_stagger_s=stagger)

    def test_negative_stagger_clamped_to_zero(self) -> None:
        self.assertEqual(self._launcher(-5.0)._provision_stagger_s, 0.0)

    def test_first_create_waits_nothing(self) -> None:
        launcher = self._launcher(2.0)
        with mock.patch('sbx_omnigent.launcher.time.sleep') as sl:
            launcher._await_create_stagger()
        sl.assert_not_called()

    def test_gap_enforced_between_back_to_back(self) -> None:
        launcher = self._launcher(2.0)
        launcher_mod._LAST_CREATE_DONE[0] = 100.0
        with (
            mock.patch(
                'sbx_omnigent.launcher.time.monotonic', return_value=100.5
            ),
            mock.patch('sbx_omnigent.launcher.time.sleep') as sl,
        ):
            launcher._await_create_stagger()
        sl.assert_called_once()
        self.assertAlmostEqual(sl.call_args.args[0], 1.5, places=6)

    def test_no_sleep_when_gap_already_elapsed(self) -> None:
        launcher = self._launcher(2.0)
        launcher_mod._LAST_CREATE_DONE[0] = 100.0
        with (
            mock.patch(
                'sbx_omnigent.launcher.time.monotonic', return_value=105.0
            ),
            mock.patch('sbx_omnigent.launcher.time.sleep') as sl,
        ):
            launcher._await_create_stagger()
        sl.assert_not_called()

    def test_zero_stagger_never_sleeps(self) -> None:
        launcher = self._launcher(0.0)
        launcher_mod._LAST_CREATE_DONE[0] = 100.0
        with (
            mock.patch(
                'sbx_omnigent.launcher.time.monotonic', return_value=100.1
            ),
            mock.patch('sbx_omnigent.launcher.time.sleep') as sl,
        ):
            launcher._await_create_stagger()
        sl.assert_not_called()

    def test_create_stamps_last_done(self) -> None:
        launcher = self._launcher(0.0)
        with (
            mock.patch.object(launcher, '_run_local'),
            mock.patch(
                'sbx_omnigent.launcher.time.monotonic', return_value=42.0
            ),
        ):
            launcher._create_sandbox('box', ['/w'])
        self.assertEqual(launcher_mod._LAST_CREATE_DONE[0], 42.0)

    def test_create_serialized_across_threads(self) -> None:
        # Four concurrent creates must not overlap inside _run_local —
        # the process-wide _CREATE_LOCK is what stops the sbx daemon's
        # proxy injections from racing.
        launcher = self._launcher(0.0)
        active: list[int] = []
        overlap: list[str] = []
        guard = threading.Lock()

        def fake_run_local(command: list[str], *, action: str) -> None:
            with guard:
                active.append(1)
                if len(active) > 1:
                    overlap.append(action)
            time.sleep(0.03)  # widen the window a real race would hit
            with guard:
                active.pop()

        with mock.patch.object(
            launcher, '_run_local', side_effect=fake_run_local
        ):
            threads = [
                threading.Thread(
                    target=launcher._create_sandbox, args=(f'b{i}', ['/w'])
                )
                for i in range(4)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        self.assertEqual(overlap, [])


class TestEgressScoping(unittest.TestCase):
    """Per-sandbox egress allowlist derivation, gating, and wiring."""

    def test_dialback_docker_gateway_adds_localhost(self) -> None:
        self.assertEqual(
            SbxLauncher._dialback_hosts('http://host.docker.internal:6767'),
            ['host.docker.internal:6767', 'localhost:6767'],
        )

    def test_dialback_remote_defaults_https_port(self) -> None:
        self.assertEqual(
            SbxLauncher._dialback_hosts('https://omni.example.com'),
            ['omni.example.com:443'],
        )

    def test_dialback_explicit_port_no_localhost(self) -> None:
        self.assertEqual(
            SbxLauncher._dialback_hosts('http://10.0.0.5:8080'),
            ['10.0.0.5:8080'],
        )

    def test_dialback_no_hostname(self) -> None:
        self.assertEqual(SbxLauncher._dialback_hosts('garbage'), [])

    def test_apply_egress_noop_when_scoping_disabled(self) -> None:
        # Default scope_egress=False (direct/test use) → no scoped rule,
        # even with hosts configured.
        launcher = SbxLauncher(egress_allow=('api.anthropic.com',))
        with mock.patch.object(launcher, '_run_local') as rl:
            launcher._apply_egress('box', 'http://host.docker.internal:6767')
        rl.assert_not_called()

    def test_apply_egress_scopes_allow_with_dialback(self) -> None:
        launcher = SbxLauncher(
            egress_allow=('api.anthropic.com', 'oauth2.googleapis.com'),
            scope_egress=True,
        )
        with mock.patch.object(launcher, '_run_local') as rl:
            launcher._apply_egress('box', 'http://host.docker.internal:6767')
        rl.assert_called_once()
        argv = rl.call_args.args[0]
        self.assertEqual(
            argv[:6],
            ['sbx', 'policy', 'allow', 'network', '--sandbox', 'box'],
        )
        # dial-back first (both host.docker.internal + localhost), then
        # the configured hosts — the exact scoped allowlist.
        self.assertEqual(
            argv[6].split(','),
            [
                'host.docker.internal:6767',
                'localhost:6767',
                'api.anthropic.com',
                'oauth2.googleapis.com',
            ],
        )

    def test_apply_egress_empty_is_dialback_only(self) -> None:
        # scope_egress on + empty egress_allow = lock down to just the
        # mandatory server dial-back.
        launcher = SbxLauncher(egress_allow=(), scope_egress=True)
        with mock.patch.object(launcher, '_run_local') as rl:
            launcher._apply_egress('box', 'http://host.docker.internal:6767')
        rl.assert_called_once()
        self.assertEqual(
            rl.call_args.args[0][6].split(','),
            ['host.docker.internal:6767', 'localhost:6767'],
        )

    def test_start_host_scopes_egress_sentinel(self) -> None:
        root = tempfile.mkdtemp(prefix='wt-root-')
        try:
            swarm = os.path.join(root, 'swarm-a')
            os.mkdir(swarm)
            launcher = SbxLauncher(
                worktree_root=root, egress_allow=('api.anthropic.com',)
            )
            with (
                mock.patch.object(launcher, '_create_sandbox'),
                mock.patch.object(launcher, '_launch_host'),
                mock.patch.object(launcher, '_apply_egress') as ae,
                mock.patch.object(launcher, '_seed_claude_settings'),
            ):
                launcher.start_host(
                    'box',
                    token='t',
                    host_id='h',
                    host_name='n',
                    server_url='http://host.docker.internal:6767',
                    repo_url=_sentinel(swarm),
                    repo_branch='rw',
                )
            # `extra=()` is the point, not noise: a non-Codex VM must
            # gain no harness-specific reachability.
            ae.assert_called_once_with(
                'box', 'http://host.docker.internal:6767', extra=()
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_start_host_scopes_egress_non_sentinel(self) -> None:
        launcher = SbxLauncher(egress_allow=('api.anthropic.com',))
        with (
            mock.patch.object(launcher, '_create_sandbox'),
            mock.patch.object(launcher, '_make_scratch', return_value='/s'),
            mock.patch.object(launcher, '_apply_egress') as ae,
            mock.patch(
                'sbx_omnigent.launcher.ExecModelHostLauncher.start_host',
                return_value='/ws',
            ),
        ):
            launcher.start_host(
                'box',
                token='t',
                host_id='h',
                host_name='n',
                server_url='http://host.docker.internal:6767',
                repo_url=None,
            )
        ae.assert_called_once_with('box', 'http://host.docker.internal:6767')


if __name__ == '__main__':
    unittest.main()
