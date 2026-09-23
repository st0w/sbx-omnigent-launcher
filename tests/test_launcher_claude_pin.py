"""The launcher moves each Claude VM to a pinned Claude Code.

A pipeline writer pinned to Opus 5.5 failed every turn with "Claude
Code 2.1.266 does not support this model; version 2.1.280 or newer is
required": the host image carries 2.1.266. With ``claude_version`` set,
every Claude VM installs that version after its egress is applied and
before its host starts. Every process call is mocked; no real ``sbx``.

Run: .venv/bin/python -m unittest tests.test_launcher_claude_pin
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

import click

from sbx_omnigent import claude, sbx_cli
from sbx_omnigent.launcher import _MOUNT_SENTINEL_PREFIX, SbxLauncher

_RUN = 'sbx_omnigent.launcher.sbx_cli.run'


def _proc(rc: int = 0, stdout: str = '') -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(['sbx'], rc, stdout, '')


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.mkdtemp(prefix='wt-root-')
        self.swarm = os.path.join(self.root, 'swarm-a')
        os.mkdir(self.swarm)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _launcher(self, version: str | None = '2.1.280') -> SbxLauncher:
        return SbxLauncher(worktree_root=self.root, claude_version=version)

    def _start(self, launcher: SbxLauncher, mode: str = 'rw') -> list[str]:
        """Run start_host on a sentinel; return its steps in order."""
        steps: list[str] = []

        def note(name: str) -> mock._patch[mock.MagicMock]:
            return mock.patch.object(
                launcher, name, side_effect=lambda *a, **k: steps.append(name)
            )

        with (
            mock.patch.object(launcher, '_create_sandbox'),
            note('_apply_egress'),
            note('_pin_claude_version'),
            note('_seed_claude_settings'),
            note('_inject_codex_credentials'),
            note('_inject_agy_credentials'),
            note('_launch_host'),
        ):
            launcher.start_host(
                'box', token='t', host_id='h', host_name='n',
                server_url='http://host.docker.internal:6767',
                repo_url=f'{_MOUNT_SENTINEL_PREFIX}{self.swarm}',
                repo_branch=mode,
            )
        return steps


class TestOnlyClaudeVmsArePinned(_Base):
    def test_a_claude_vm_is_pinned_after_egress_before_the_host(self) -> None:
        steps = self._start(self._launcher())
        self.assertEqual(steps, [
            '_apply_egress', '_pin_claude_version',
            '_seed_claude_settings', '_launch_host',
        ])

    def test_a_codex_vm_is_not(self) -> None:
        steps = self._start(self._launcher(), mode='rw-codex')
        self.assertNotIn('_pin_claude_version', steps)

    def test_an_agy_vm_is_not(self) -> None:
        steps = self._start(self._launcher(), mode='rw-agy')
        self.assertNotIn('_pin_claude_version', steps)

    def test_no_pin_configured_means_no_install(self) -> None:
        steps = self._start(self._launcher(version=None))
        self.assertNotIn('_pin_claude_version', steps)


class TestThePinStep(_Base):
    def _ok(self) -> subprocess.CompletedProcess[str]:
        return _proc(stdout=f'{claude.PIN_OK_MARKER} 2.1.280\n')

    def test_it_runs_the_pin_script_in_the_vm(self) -> None:
        with mock.patch(_RUN, return_value=self._ok()) as run:
            self._launcher()._pin_claude_version('box')
        argv = run.call_args.args[0]
        self.assertEqual(argv[:6], ['sbx', 'exec', 'box', '--', 'sh', '-c'])
        self.assertEqual(
            argv[6], claude.build_version_pin_script('2.1.280')
        )

    def test_it_has_the_install_budget(self) -> None:
        with mock.patch(_RUN, return_value=self._ok()) as run:
            self._launcher()._pin_claude_version('box')
        self.assertEqual(
            run.call_args.kwargs['timeout_s'], sbx_cli.INSTALL_TIMEOUT_S
        )

    def test_a_failed_install_fails_the_vm(self) -> None:
        failed = _proc(3, 'npm install of Claude Code 2.1.280 failed:\n'
                          'npm error code E404\n')
        with mock.patch(_RUN, return_value=failed):
            with self.assertRaises(click.ClickException) as caught:
                self._launcher()._pin_claude_version('box')
        message = caught.exception.format_message()
        self.assertIn('2.1.280', message)
        self.assertIn("'box'", message)
        self.assertIn('E404', message)

    def test_success_without_the_marker_fails(self) -> None:
        with mock.patch(_RUN, return_value=_proc(0, 'something else\n')):
            with self.assertRaises(click.ClickException):
                self._launcher()._pin_claude_version('box')

    def test_sbx_that_cannot_run_fails(self) -> None:
        with mock.patch(_RUN, side_effect=OSError('no sbx')):
            with self.assertRaises(click.ClickException):
                self._launcher()._pin_claude_version('box')

    def test_a_hang_names_the_daemon(self) -> None:
        with mock.patch.object(
            sbx_cli.subprocess, 'run',
            side_effect=subprocess.TimeoutExpired(['sbx'], 1.0),
        ):
            with self.assertRaises(sbx_cli.SbxNotResponding):
                self._launcher()._pin_claude_version('box')


class TestAPinnedVmStaysPinned(_Base):
    """Claude's auto-updater moved a VM mid-run before; a pinned VM
    has it turned off."""

    def _seed_script(self, launcher: SbxLauncher) -> str:
        ok = _proc(stdout=f'{claude.SEED_OK_MARKER}\n')
        with mock.patch(_RUN, return_value=ok) as run:
            launcher._seed_claude_settings('box')
        return str(run.call_args.args[0][-1])

    def test_pinned_turns_the_autoupdater_off(self) -> None:
        self.assertEqual(
            self._seed_script(self._launcher()),
            claude.build_settings_seed_script(disable_autoupdater=True),
        )

    def test_unpinned_leaves_it_alone(self) -> None:
        self.assertEqual(
            self._seed_script(self._launcher(version=None)),
            claude.build_settings_seed_script(),
        )


class TestTheConstructorValidates(unittest.TestCase):
    def test_a_bad_version_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            SbxLauncher(claude_version='latest')


if __name__ == '__main__':
    unittest.main()
