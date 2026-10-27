"""The launcher updates agy in each agy VM when ``agy_update`` is on.

The host image pins agy, and an agy too old for a model serves a
different one without error: ``gemini-4-argon-high`` ran as ``Gemini
3.8 Flash (High)`` on the image's 1.1.16. With ``agy_update`` on, every
agy VM gets the updater's hosts per VM and runs ``agy update`` after
its egress is applied and before its host starts. Off by default,
because Omnigent pins agy for a reason: its bridge drives agy's
screen. Every process call is mocked; no real ``sbx``.

Run: .venv/bin/python -m unittest tests.test_launcher_agy_update
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

import click

from sbx_omnigent import agy, codex, sbx_cli
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

    def _launcher(self, update: bool = True) -> SbxLauncher:
        return SbxLauncher(
            worktree_root=self.root, agy_enabled=True, agy_update=update,
            agy_gcp_project='proj-1',
        )

    def _start(
        self, launcher: SbxLauncher, mode: str = 'rw-agy'
    ) -> tuple[list[str], list[tuple[str, ...]]]:
        """Run start_host on a sentinel; return its steps and egress."""
        steps: list[str] = []
        extras: list[tuple[str, ...]] = []

        def note(name: str) -> mock._patch[mock.MagicMock]:
            return mock.patch.object(
                launcher, name, side_effect=lambda *a, **k: steps.append(name)
            )

        def egress(
            *_a: object, extra: tuple[str, ...] = (), **_k: object
        ) -> None:
            steps.append('_apply_egress')
            extras.append(tuple(extra))

        def launch(*_a: object, adc: bool = False, **_k: object) -> None:
            steps.append('_launch_host' + (' (adc)' if adc else ''))

        with (
            mock.patch.object(launcher, '_create_sandbox'),
            mock.patch.object(launcher, '_apply_egress', side_effect=egress),
            note('_update_agy'),
            note('_inject_agy_credentials'),
            note('_inject_codex_credentials'),
            note('_pin_claude_version'),
            note('_seed_claude_settings'),
            mock.patch.object(launcher, '_launch_host', side_effect=launch),
        ):
            launcher.start_host(
                'box', token='t', host_id='h', host_name='n',
                server_url='http://host.docker.internal:6767',
                repo_url=f'{_MOUNT_SENTINEL_PREFIX}{self.swarm}',
                repo_branch=mode,
            )
        return steps, extras


class TestOnlyAgyVmsAreUpdated(_Base):
    def test_an_agy_vm_is_updated_after_egress_before_the_host(
        self,
    ) -> None:
        steps, _extras = self._start(self._launcher())
        self.assertLess(
            steps.index('_apply_egress'), steps.index('_update_agy')
        )
        self.assertLess(
            steps.index('_update_agy'), steps.index('_launch_host (adc)')
        )

    def test_an_agy_vm_gets_the_updater_hosts_per_vm(self) -> None:
        _steps, extras = self._start(self._launcher())
        self.assertEqual(extras, [agy.UPDATE_EGRESS])

    def test_off_by_default(self) -> None:
        launcher = SbxLauncher(worktree_root=self.root, agy_enabled=True)
        steps, extras = self._start(launcher)
        self.assertNotIn('_update_agy', steps)
        self.assertEqual(extras, [()])

    def test_off_adds_no_hosts(self) -> None:
        steps, extras = self._start(self._launcher(update=False))
        self.assertNotIn('_update_agy', steps)
        self.assertEqual(extras, [()])

    def test_a_claude_vm_is_never_updated(self) -> None:
        steps, extras = self._start(self._launcher(), mode='rw')
        self.assertNotIn('_update_agy', steps)
        self.assertEqual(extras, [()])

    def test_a_codex_vm_keeps_only_its_own_hosts(self) -> None:
        steps, extras = self._start(self._launcher(), mode='rw-codex')
        self.assertNotIn('_update_agy', steps)
        self.assertEqual(extras, [codex.CODEX_EGRESS])

    def test_a_reviewer_agy_vm_is_updated_too(self) -> None:
        steps, _extras = self._start(self._launcher(), mode='ro-agy')
        self.assertIn('_update_agy', steps)


class TestTheUpdateStep(_Base):
    def _ok(self) -> subprocess.CompletedProcess[str]:
        return _proc(stdout=f'{agy.UPDATE_OK_MARKER} 1.1.16 1.4.7\n')

    def test_it_runs_the_update_script_in_the_vm(self) -> None:
        with mock.patch(_RUN, return_value=self._ok()) as run:
            self._launcher()._update_agy('box')
        argv = run.call_args.args[0]
        self.assertEqual(
            argv,
            ['sbx', 'exec', 'box', '--', 'sh', '-c',
             agy.build_update_script()],
        )

    def test_it_has_the_install_budget(self) -> None:
        with mock.patch(_RUN, return_value=self._ok()) as run:
            self._launcher()._update_agy('box')
        self.assertEqual(
            run.call_args.kwargs['timeout_s'], sbx_cli.INSTALL_TIMEOUT_S
        )

    def test_it_logs_the_versions(self) -> None:
        with mock.patch(_RUN, return_value=self._ok()):
            with self.assertLogs('sbx_omnigent.launcher', 'INFO') as logs:
                self._launcher()._update_agy('box')
        self.assertIn('1.1.16 -> 1.4.7', '\n'.join(logs.output))

    def test_a_failed_update_fails_the_vm(self) -> None:
        failed = _proc(3, 'agy update failed (agy 1.1.16):\n'
                          'x could not reach the update server\n')
        with mock.patch(_RUN, return_value=failed):
            with self.assertRaises(click.ClickException) as caught:
                self._launcher()._update_agy('box')
        message = caught.exception.format_message()
        self.assertIn("'box'", message)
        self.assertIn('could not reach the update server', message)
        self.assertIn('sandbox.sbx.agy_update', message)

    def test_success_without_the_marker_fails(self) -> None:
        with mock.patch(_RUN, return_value=_proc(0, 'something else\n')):
            with self.assertRaises(click.ClickException):
                self._launcher()._update_agy('box')

    def test_sbx_that_cannot_run_fails(self) -> None:
        with mock.patch(_RUN, side_effect=OSError('no sbx')):
            with self.assertRaises(click.ClickException):
                self._launcher()._update_agy('box')

    def test_a_hang_names_the_daemon(self) -> None:
        with mock.patch.object(
            sbx_cli.subprocess, 'run',
            side_effect=subprocess.TimeoutExpired(['sbx'], 1.0),
        ):
            with self.assertRaises(sbx_cli.SbxNotResponding):
                self._launcher()._update_agy('box')


class TestTheHostSignsAgyInThroughAdc(_Base):
    """agy 1.4.x reads no token file, so an updated agy VM's host starts
    the metadata stand-in and passes the ADC env through to agy."""

    def test_only_an_updated_agy_vm_launches_with_adc(self) -> None:
        for mode, update, adc in (
            ('rw-agy', True, True), ('ro-agy', True, True),
            ('rw-agy', False, False), ('rw', True, False),
            ('rw-codex', True, False),
        ):
            with self.subTest(mode=mode, update=update):
                steps, _extras = self._start(self._launcher(update), mode)
                self.assertIn(
                    '_launch_host (adc)' if adc else '_launch_host', steps
                )

    def _remote(self, *, adc: bool) -> str:
        launcher = self._launcher()
        with mock.patch(
            'sbx_omnigent.launcher.subprocess.Popen'
        ) as popen, mock.patch(
            'sbx_omnigent.launcher._retain_host_process'
        ):
            launcher._launch_host(
                'box', token='t', host_id='h', host_name='n',
                server_url='http://host.docker.internal:6767', adc=adc,
            )
        argv = popen.call_args.args[0]
        self.assertEqual(argv[:5], ['sbx', 'exec', 'box', '--', 'sh'])
        return argv[-1]

    def test_the_stand_in_starts_in_the_hosts_own_session(self) -> None:
        # A separate `sbx exec` would be reaped when it returns, so the
        # stand-in shares the long-lived exec that keeps the host alive.
        remote = self._remote(adc=True)
        stub = remote.index('python3 -c')
        self.assertLess(stub, remote.index('omnigent host'))
        self.assertIn(str(agy.ADC_METADATA_PORT), remote)
        self.assertIn('&', remote[stub:remote.index('omnigent host')])

    def test_the_adc_env_reaches_the_harness(self) -> None:
        remote = self._remote(adc=True)
        for name, value in agy.adc_launch_env('proj-1'):
            with self.subTest(name=name):
                self.assertIn(f'{name}={value}', remote)
        passthrough = next(
            part for part in remote.split()
            if part.startswith('OMNIGENT_RUNNER_ENV_PASSTHROUGH=')
        )
        for name, _value in agy.adc_launch_env('proj-1'):
            self.assertIn(name, passthrough)
        self.assertIn('HTTPS_PROXY', passthrough)

    def test_a_plain_launch_has_neither(self) -> None:
        remote = self._remote(adc=False)
        self.assertNotIn('python3 -c', remote)
        self.assertNotIn('AGY_ADC_AUTH', remote)

    def test_adc_without_a_project_fails_loudly(self) -> None:
        launcher = SbxLauncher(
            worktree_root=self.root, agy_enabled=True, agy_update=True
        )
        with self.assertRaises(click.ClickException):
            launcher._launch_host(
                'box', token='t', host_id='h', host_name='n',
                server_url='http://host.docker.internal:6767', adc=True,
            )


if __name__ == '__main__':
    unittest.main()
