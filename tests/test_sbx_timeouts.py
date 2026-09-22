"""Every ``sbx`` call the launcher, agy and the verify gate make is
bounded, and a hang says the daemon is why (#28).

Every process call is faked; no real ``sbx`` runs.
"""

from __future__ import annotations

import contextlib
import io
import subprocess
import unittest
from unittest import mock

import click
from click.testing import CliRunner, Result

from sbx_omnigent import agy, claude, codex, sbx_cli, verify
from sbx_omnigent import launcher as launcher_mod
from sbx_omnigent.launcher import SbxLauncher
from sbx_omnigent.verify import _SETUP_MARKER

_LAUNCHER_RUN = 'sbx_omnigent.launcher.subprocess.run'


class _Proc(subprocess.CompletedProcess[str]):
    """A finished process with the given output."""

    def __init__(
        self, stdout: str = '', returncode: int = 0, stderr: str = ''
    ) -> None:
        super().__init__(
            args=[], returncode=returncode, stdout=stdout, stderr=stderr
        )


class _Runner:
    """Records argv and kwargs; replays one result, or hangs on sbx."""

    def __init__(
        self, proc: _Proc | None = None, *, hang: str | None = None
    ) -> None:
        self.calls: list[tuple[list[str], dict[str, object]]] = []
        self._proc = proc if proc is not None else _Proc()
        self._hang = hang

    def __call__(self, argv: list[str], **kwargs: object) -> _Proc:
        self.calls.append((list(argv), kwargs))
        if self._hang is not None and argv[:2] == ['sbx', self._hang]:
            timeout = kwargs.get('timeout')
            raise subprocess.TimeoutExpired(
                argv, timeout if isinstance(timeout, float) else 0.0
            )
        return self._proc

    def timeout_of(self, verb: str) -> object:
        """The ``timeout`` the first ``sbx <verb>`` call was given."""
        for argv, kwargs in self.calls:
            if argv[:2] == ['sbx', verb]:
                return kwargs.get('timeout')
        raise AssertionError(f'no `sbx {verb}` call in {self.calls!r}')


def _daemon_named(message: str) -> bool:
    return 'sbx daemon is not responding' in message


class TestLauncherPrepare(unittest.TestCase):
    def _prepare(self, runner: _Runner) -> None:
        with mock.patch('sbx_omnigent.launcher.shutil.which',
                        return_value='/usr/bin/sbx'), \
                mock.patch(_LAUNCHER_RUN, runner):
            SbxLauncher().prepare()

    def test_the_probe_is_bounded(self) -> None:
        runner = _Runner()
        self._prepare(runner)
        self.assertEqual(runner.timeout_of('ls'), sbx_cli.PROBE_TIMEOUT_S)

    def test_a_hung_probe_names_the_daemon(self) -> None:
        with self.assertRaises(sbx_cli.SbxNotResponding):
            self._prepare(_Runner(hang='ls'))

    def test_a_failed_probe_still_says_to_log_in(self) -> None:
        with self.assertRaises(click.ClickException) as caught:
            self._prepare(_Runner(_Proc(returncode=1, stderr='nope')))
        self.assertIn('sbx login', caught.exception.format_message())


class TestLauncherManagement(unittest.TestCase):
    def test_create_gets_the_create_tier(self) -> None:
        runner = _Runner()
        with mock.patch(_LAUNCHER_RUN, runner):
            SbxLauncher(provision_stagger_s=0.0)._create_sandbox(
                'box', ['/w']
            )
        self.assertEqual(
            runner.timeout_of('create'), sbx_cli.CREATE_TIMEOUT_S
        )

    def test_a_hung_create_names_the_daemon(self) -> None:
        with mock.patch(_LAUNCHER_RUN, _Runner(hang='create')), \
                self.assertRaises(sbx_cli.SbxNotResponding):
            SbxLauncher(provision_stagger_s=0.0)._create_sandbox(
                'box', ['/w']
            )

    def test_egress_gets_the_management_tier(self) -> None:
        runner = _Runner()
        launcher = SbxLauncher(scope_egress=True, egress_allow=('a.b',))
        with mock.patch(_LAUNCHER_RUN, runner):
            launcher._apply_egress('box', 'http://h:1')
        self.assertEqual(
            runner.timeout_of('policy'), sbx_cli.MANAGE_TIMEOUT_S
        )

    def test_remove_gets_the_management_tier(self) -> None:
        runner = _Runner()
        with mock.patch(_LAUNCHER_RUN, runner), \
                mock.patch.dict('os.environ', clear=False) as env:
            env.pop(launcher_mod._KEEP_SANDBOXES_ENV, None)
            SbxLauncher().terminate('box')
        self.assertEqual(runner.timeout_of('rm'), sbx_cli.MANAGE_TIMEOUT_S)

    def test_a_hung_remove_warns_and_returns(self) -> None:
        # Teardown stays best-effort: a stuck daemon is reported, not
        # raised into a run that has already finished its work.
        err = io.StringIO()
        with mock.patch(_LAUNCHER_RUN, _Runner(hang='rm')), \
                mock.patch.dict('os.environ', clear=False) as env, \
                contextlib.redirect_stderr(err):
            env.pop(launcher_mod._KEEP_SANDBOXES_ENV, None)
            SbxLauncher().terminate('box')
        self.assertTrue(_daemon_named(err.getvalue()))
        self.assertIn("'box'", err.getvalue())


class TestLauncherSeeds(unittest.TestCase):
    def test_the_codex_seed_is_bounded_and_keeps_its_input(self) -> None:
        runner = _Runner(_Proc(codex.SEED_OK_MARKER))
        with mock.patch(_LAUNCHER_RUN, runner), \
                mock.patch.object(codex, 'read_host_auth',
                                  return_value={}), \
                mock.patch.object(codex, 'build_agent_payload',
                                  return_value='PAYLOAD'):
            SbxLauncher()._inject_codex_credentials('box')
        self.assertEqual(
            runner.timeout_of('exec'), sbx_cli.MANAGE_TIMEOUT_S
        )
        self.assertEqual(runner.calls[0][1].get('input'), 'PAYLOAD')

    def test_the_claude_seed_is_bounded(self) -> None:
        runner = _Runner(_Proc(claude.SEED_OK_MARKER))
        with mock.patch(_LAUNCHER_RUN, runner):
            SbxLauncher()._seed_claude_settings('box')
        self.assertEqual(
            runner.timeout_of('exec'), sbx_cli.MANAGE_TIMEOUT_S
        )

    def test_the_agy_inject_is_bounded(self) -> None:
        runner = _Runner(_Proc('DONE'))
        with mock.patch(_LAUNCHER_RUN, runner):
            SbxLauncher()._run_agy_inject('box', 'print(1)', 'DONE', 'seed')
        self.assertEqual(
            runner.timeout_of('exec'), sbx_cli.MANAGE_TIMEOUT_S
        )

    def test_a_hung_seed_names_the_daemon(self) -> None:
        with mock.patch(_LAUNCHER_RUN, _Runner(hang='exec')), \
                self.assertRaises(sbx_cli.SbxNotResponding):
            SbxLauncher()._seed_claude_settings('box')


class TestLauncherRemoteCommand(unittest.TestCase):
    def test_a_remote_command_gets_the_command_tier(self) -> None:
        # Omnigent's `git clone` runs through here, so its budget is
        # the longest one.
        runner = _Runner(_Proc('ok'))
        with mock.patch(_LAUNCHER_RUN, runner):
            SbxLauncher().run('box', 'git clone x y')
        self.assertEqual(
            runner.timeout_of('exec'), sbx_cli.COMMAND_TIMEOUT_S
        )

    def test_a_hung_remote_command_names_the_daemon(self) -> None:
        with mock.patch(_LAUNCHER_RUN, _Runner(hang='exec')), \
                self.assertRaises(sbx_cli.SbxNotResponding):
            SbxLauncher().run('box', 'true', check=False)


class TestAgyHarvester(unittest.TestCase):
    def test_the_secret_update_is_bounded(self) -> None:
        runner = _Runner()
        agy.Harvester(box='b', run=runner).update_secret('tok')
        self.assertEqual(
            runner.timeout_of('secret'), sbx_cli.MANAGE_TIMEOUT_S
        )

    def test_a_hung_secret_update_is_a_harvest_error(self) -> None:
        # The refresh loop retries on AgyHarvestError; anything else
        # would kill the always-on loop.
        with self.assertRaises(agy.AgyHarvestError) as caught:
            agy.Harvester(box='b', run=_Runner(hang='secret')).update_secret(
                'tok'
            )
        self.assertTrue(_daemon_named(str(caught.exception)))


class TestAgyBootstrap(unittest.TestCase):
    def _bootstrap(self, runner: _Runner) -> Result:
        with mock.patch.object(agy.subprocess, 'run', runner):
            return CliRunner().invoke(
                agy.cli, ['bootstrap', '--box', 'probe']
            )

    def test_create_and_policy_get_their_tiers(self) -> None:
        runner = _Runner()
        self._bootstrap(runner)
        self.assertEqual(
            runner.timeout_of('create'), sbx_cli.CREATE_TIMEOUT_S
        )
        self.assertEqual(
            runner.timeout_of('policy'), sbx_cli.MANAGE_TIMEOUT_S
        )

    def test_a_hung_create_names_the_daemon(self) -> None:
        result = self._bootstrap(_Runner(hang='create'))
        self.assertNotEqual(result.exit_code, 0)
        self.assertTrue(_daemon_named(result.output))


#: What the gate's exec prints once its prologue has run.
_GATE_OK = _Proc(f'{_SETUP_MARKER}\n')


class TestVerifyGate(unittest.TestCase):
    def _verify(self, runner: _Runner) -> verify.VerifyOutcome:
        return verify.run_verification(
            name='v', workspace='/w', script='true', image='img',
            egress=('a.b',), run=runner,
        )

    def test_create_policy_and_remove_get_their_tiers(self) -> None:
        runner = _Runner(_GATE_OK)
        self._verify(runner)
        self.assertEqual(
            runner.timeout_of('create'), sbx_cli.CREATE_TIMEOUT_S
        )
        self.assertEqual(
            runner.timeout_of('policy'), sbx_cli.MANAGE_TIMEOUT_S
        )
        self.assertEqual(runner.timeout_of('rm'), sbx_cli.MANAGE_TIMEOUT_S)

    def test_a_hung_create_is_a_verify_error_naming_the_daemon(self) -> None:
        with self.assertRaises(verify.VerifyError) as caught:
            self._verify(_Runner(hang='create'))
        self.assertTrue(_daemon_named(str(caught.exception)))

    def test_a_hung_policy_is_a_verify_error_naming_the_daemon(self) -> None:
        with self.assertRaises(verify.VerifyError) as caught:
            self._verify(_Runner(hang='policy'))
        self.assertTrue(_daemon_named(str(caught.exception)))

    def test_a_hung_remove_does_not_fail_the_gate(self) -> None:
        # Cleanup was already best-effort; it now also cannot hang.
        self._verify(_Runner(_GATE_OK, hang='rm'))


if __name__ == '__main__':
    unittest.main()
