"""Stage-3 tests: launcher agy credential injection.

Injection is gated by ``agy_enabled`` and runs in-VM ``python3 -c``
setup scripts via ``sbx exec`` — the credential seed always, plus a
bridge patch when a Business account and/or a GCP project is configured.
Every process call is mocked (no real ``sbx``). Run with:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from unittest import mock

import click

from sbx_omnigent import agy
from sbx_omnigent.launcher import SbxLauncher

_RUN = 'sbx_omnigent.launcher.subprocess.run'


def _sentinel(path: str) -> str:
    return f'git@sbxmount:{path}'


class _Proc:
    """Minimal CompletedProcess stand-in."""

    def __init__(
        self, rc: int = 0, stdout: str = 'AGY_SEED_OK\n', stderr: str = ''
    ) -> None:
        self.returncode = rc
        self.stdout = stdout
        self.stderr = stderr


_SEED = _Proc(0, 'AGY_SEED_OK\n')
_PATCH = _Proc(0, 'AGY_BRIDGE_PATCHED 2\n')


class TestInjectAgyCredentials(unittest.TestCase):
    """``_inject_agy_credentials`` gating, argv, and fail-loud."""

    def test_noop_when_disabled(self) -> None:
        launcher = SbxLauncher()  # agy_enabled defaults False
        with mock.patch(_RUN) as run:
            launcher._inject_agy_credentials('box')
        run.assert_not_called()

    def test_consumer_still_gets_the_paste_bridge_patch(self) -> None:
        # EVERY agy VM is patched, not just enterprise/GCP ones: without
        # it a long or multi-line message (a human's reply to an
        # interactive planner) is refused before submit because agy
        # collapsed the paste into its own placeholder.
        launcher = SbxLauncher(agy_enabled=True, agy_enterprise=False)
        with mock.patch(_RUN, side_effect=[_SEED, _PATCH]) as run:
            launcher._inject_agy_credentials('box')
        self.assertEqual(run.call_count, 2)
        seed_argv = run.call_args_list[0].args[0]
        self.assertEqual(
            seed_argv[:5], ['sbx', 'exec', 'box', 'python3', '-c']
        )
        self.assertIn(agy.SEED_OK_MARKER, seed_argv[5])
        patch_script = run.call_args_list[1].args[0][5]
        self.assertIn('_do_paste = True', patch_script)
        # ...but a consumer VM gets none of the enterprise/GCP edits.
        self.assertIn('_do_ent = False', patch_script)
        self.assertIn('_do_settings = False', patch_script)

    def test_gcp_triggers_bridge_patch(self) -> None:
        launcher = SbxLauncher(
            agy_enabled=True, agy_enterprise=False, agy_gcp_project='p-1'
        )
        with mock.patch(_RUN, side_effect=[_SEED, _PATCH]) as run:
            launcher._inject_agy_credentials('box')
        self.assertEqual(run.call_count, 2)
        seed_script = run.call_args_list[0].args[0][5]
        patch_script = run.call_args_list[1].args[0][5]
        self.assertIn('p-1', seed_script)  # gcp seeded into settings
        self.assertIn('_AGY_SEED_FILES', patch_script)
        self.assertIn('_do_settings = True', patch_script)

    def test_enterprise_triggers_bridge_patch(self) -> None:
        launcher = SbxLauncher(agy_enabled=True, agy_enterprise=True)
        with mock.patch(_RUN, side_effect=[_SEED, _PATCH]) as run:
            launcher._inject_agy_credentials('box')
        self.assertEqual(run.call_count, 2)
        seed_script = run.call_args_list[0].args[0][5]
        patch_script = run.call_args_list[1].args[0][5]
        self.assertIn('"enterpriseOnboardingComplete": true', seed_script)
        self.assertIn('_do_ent = True', patch_script)

    def test_fail_loud_on_nonzero(self) -> None:
        launcher = SbxLauncher(agy_enabled=True, agy_enterprise=False)
        with mock.patch(_RUN, return_value=_Proc(rc=1, stdout='', stderr='x')):
            with self.assertRaises(click.ClickException):
                launcher._inject_agy_credentials('box')

    def test_fail_loud_on_missing_marker(self) -> None:
        launcher = SbxLauncher(agy_enabled=True, agy_enterprise=False)
        with mock.patch(_RUN, return_value=_Proc(rc=0, stdout='nope')):
            with self.assertRaises(click.ClickException):
                launcher._inject_agy_credentials('box')

    def test_fail_loud_on_oserror(self) -> None:
        launcher = SbxLauncher(agy_enabled=True, agy_enterprise=False)
        with mock.patch(_RUN, side_effect=OSError('no sbx')):
            with self.assertRaises(click.ClickException):
                launcher._inject_agy_credentials('box')


class TestInjectionCallSite(unittest.TestCase):
    """Injection fires only for an agy-tagged mount, not other VMs."""

    def setUp(self) -> None:
        self.root = tempfile.mkdtemp(prefix='wt-root-')
        self.swarm = os.path.join(self.root, 'swarm-a')
        os.mkdir(self.swarm)
        self.launcher = SbxLauncher(worktree_root=self.root, agy_enabled=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _run(self, **kwargs: object):
        defaults = {
            'token': 't',
            'host_id': 'h',
            'host_name': 'n',
            'server_url': 'http://x:6767',
        }
        with (
            mock.patch.object(self.launcher, '_create_sandbox'),
            mock.patch.object(self.launcher, '_launch_host'),
            mock.patch.object(
                self.launcher, '_make_scratch', return_value='/scratch'
            ),
            mock.patch(
                'sbx_omnigent.launcher.ExecModelHostLauncher.start_host',
                return_value='/w',
            ),
            mock.patch.object(
                self.launcher, '_inject_agy_credentials'
            ) as inj,
            mock.patch.object(
                self.launcher, '_seed_claude_settings'
            ) as seed,
        ):
            self.launcher.start_host('box', **{**defaults, **kwargs})
        return inj, seed

    def test_injects_on_agy_tagged_sentinel(self) -> None:
        inj, seed = self._run(
            repo_url=_sentinel(self.swarm), repo_branch='rw-agy'
        )
        inj.assert_called_once_with('box')
        # An agy VM never launches Claude, so it must not be given
        # Claude's bypass-permissions pre-acceptance either.
        seed.assert_not_called()

    def test_no_inject_on_plain_sentinel(self) -> None:
        inj, seed = self._run(
            repo_url=_sentinel(self.swarm), repo_branch='rw'
        )
        inj.assert_not_called()
        # A plain sentinel IS the Claude VM: it gets no credential (the
        # sbx proxy carries that) but it does need the launch-gate
        # pre-acceptance, or `bypassPermissions` stops on a dialog no
        # one can answer.
        seed.assert_called_once_with('box')

    def test_no_inject_on_non_sentinel(self) -> None:
        inj, seed = self._run(
            repo_url='git@github.com:o/r.git',
            repo_branch='main',
            repo_name='r',
        )
        inj.assert_not_called()
        seed.assert_not_called()


if __name__ == '__main__':
    unittest.main()
