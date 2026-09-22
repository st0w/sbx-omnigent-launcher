"""Tests for bounded sbx calls and the stuck-daemon error (#28)."""

from __future__ import annotations

import subprocess
import unittest
from unittest import mock

import click

from sbx_omnigent import sbx_cli


class _Proc(subprocess.CompletedProcess[str]):
    """A finished process with the given output."""

    def __init__(
        self, stdout: str = '', returncode: int = 0, stderr: str = ''
    ) -> None:
        super().__init__(
            args=[], returncode=returncode, stdout=stdout, stderr=stderr
        )


class _Runner:
    """A subprocess.run stand-in that records argv and kwargs."""

    def __init__(
        self,
        proc: _Proc | None = None,
        raises: BaseException | None = None,
    ) -> None:
        self.calls: list[tuple[list[str], dict[str, object]]] = []
        self._proc = proc if proc is not None else _Proc()
        self._raises = raises

    def __call__(self, argv: list[str], **kwargs: object) -> _Proc:
        self.calls.append((argv, kwargs))
        if self._raises is not None:
            raise self._raises
        return self._proc


def _hung(argv: list[str], timeout: float) -> _Runner:
    """A runner whose command never finishes inside *timeout*."""
    return _Runner(raises=subprocess.TimeoutExpired(argv, timeout))


class TestTheTiers(unittest.TestCase):
    def test_each_tier_is_longer_than_the_one_before(self) -> None:
        # A probe must answer faster than a management call, which must
        # answer faster than a create, which is faster than a clone.
        self.assertLess(sbx_cli.PROBE_TIMEOUT_S, sbx_cli.MANAGE_TIMEOUT_S)
        self.assertLess(sbx_cli.MANAGE_TIMEOUT_S, sbx_cli.CREATE_TIMEOUT_S)
        self.assertLess(
            sbx_cli.CREATE_TIMEOUT_S, sbx_cli.COMMAND_TIMEOUT_S
        )


class TestNotResponding(unittest.TestCase):
    def _error(self) -> sbx_cli.SbxNotResponding:
        return sbx_cli.SbxNotResponding(['sbx', 'create', 'shell'], 300.0)

    def test_it_names_the_command_and_the_budget(self) -> None:
        message = self._error().format_message()
        self.assertIn('`sbx create`', message)
        self.assertIn('300 s', message)

    def test_it_blames_the_daemon(self) -> None:
        self.assertIn(
            'sbx daemon is not responding', self._error().format_message()
        )

    def test_it_names_the_restart(self) -> None:
        self.assertIn(
            'sbx daemon stop && sbx daemon start',
            self._error().format_message(),
        )

    def test_it_says_how_to_check_the_shim_is_idle(self) -> None:
        message = self._error().format_message()
        self.assertIn('containerd-shim-nerdbox-v1', message)
        self.assertIn('pgrep -P', message)

    def test_it_says_nothing_was_restarted(self) -> None:
        # The launcher notices and refuses; it never touches the daemon.
        self.assertIn(
            'Nothing was stopped or restarted',
            self._error().format_message(),
        )

    def test_it_is_a_click_exception(self) -> None:
        # So every caller that already reports ClickException reports
        # this one too, with no new handling.
        self.assertIsInstance(self._error(), click.ClickException)

    def test_a_sandbox_name_is_never_in_the_subcommand(self) -> None:
        # Only the subcommand is named; a sandbox name is caller data.
        message = sbx_cli.SbxNotResponding(
            ['sbx', 'rm', '--force', 'managed-abc'], 120.0
        ).format_message()
        self.assertIn('`sbx rm`', message)


class TestNotRespondingMessage(unittest.TestCase):
    def test_it_matches_the_exception_text(self) -> None:
        argv = ['sbx', 'secret', 'set-custom', '-g']
        self.assertEqual(
            sbx_cli.not_responding_message(argv, 120.0),
            sbx_cli.SbxNotResponding(argv, 120.0).format_message(),
        )


class TestTheDefaultRunner(unittest.TestCase):
    def test_it_is_looked_up_at_call_time(self) -> None:
        # A caller's test patches subprocess.run; a default bound at
        # import would miss the patch and run the real sbx.
        fake = _Runner(_Proc('patched'))
        with mock.patch.object(sbx_cli.subprocess, 'run', fake):
            proc = sbx_cli.run(['sbx', 'ls'], timeout_s=5.0)
            sbx_cli.require_responsive()
        self.assertEqual(proc.stdout, 'patched')
        self.assertEqual(len(fake.calls), 2)


class TestRun(unittest.TestCase):
    def test_it_returns_the_finished_process(self) -> None:
        runner = _Runner(_Proc('ok\n'))
        proc = sbx_cli.run(['sbx', 'ls'], timeout_s=5.0, runner=runner)
        self.assertEqual(proc.stdout, 'ok\n')

    def test_it_passes_the_budget_and_captures_text(self) -> None:
        runner = _Runner()
        sbx_cli.run(['sbx', 'ls'], timeout_s=7.0, runner=runner)
        kwargs = runner.calls[0][1]
        self.assertEqual(kwargs.get('timeout'), 7.0)
        self.assertIs(kwargs.get('capture_output'), True)
        self.assertIs(kwargs.get('text'), True)

    def test_it_forwards_input(self) -> None:
        runner = _Runner()
        sbx_cli.run(
            ['sbx', 'exec', 'b'], timeout_s=5.0, input_text='secret',
            runner=runner,
        )
        self.assertEqual(runner.calls[0][1].get('input'), 'secret')

    def test_no_input_is_not_forwarded(self) -> None:
        runner = _Runner()
        sbx_cli.run(['sbx', 'ls'], timeout_s=5.0, runner=runner)
        self.assertNotIn('input', runner.calls[0][1])

    def test_a_timeout_raises_the_named_error(self) -> None:
        argv = ['sbx', 'create', 'shell', '/w']
        with self.assertRaises(sbx_cli.SbxNotResponding) as caught:
            sbx_cli.run(argv, timeout_s=300.0, runner=_hung(argv, 300.0))
        self.assertIn('`sbx create`', caught.exception.format_message())

    def test_a_non_zero_exit_is_returned_not_raised(self) -> None:
        # Callers already word their own failures; only a hang is new.
        proc = sbx_cli.run(
            ['sbx', 'rm', 'x'], timeout_s=5.0,
            runner=_Runner(_Proc(returncode=1, stderr='gone')),
        )
        self.assertEqual(proc.returncode, 1)

    def test_a_missing_binary_still_raises_oserror(self) -> None:
        # Callers already turn OSError into their own message.
        with self.assertRaises(OSError):
            sbx_cli.run(
                ['sbx', 'ls'], timeout_s=5.0,
                runner=_Runner(raises=OSError('no sbx')),
            )

    def test_a_non_sbx_command_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            sbx_cli.run(['rm', '-rf', '/'], timeout_s=5.0, runner=_Runner())

    def test_an_empty_command_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            sbx_cli.run([], timeout_s=5.0, runner=_Runner())

    def test_a_budget_that_is_not_positive_is_refused(self) -> None:
        for bad in (0.0, -1.0):
            with self.subTest(timeout_s=bad):
                with self.assertRaises(ValueError):
                    sbx_cli.run(['sbx', 'ls'], timeout_s=bad,
                                runner=_Runner())


class TestRequireResponsive(unittest.TestCase):
    def test_a_healthy_daemon_passes(self) -> None:
        runner = _Runner(_Proc('SANDBOX  STATUS\n'))
        sbx_cli.require_responsive(runner=runner)
        self.assertEqual(runner.calls[0][0], ['sbx', 'ls'])

    def test_it_is_bounded_by_the_probe_tier(self) -> None:
        runner = _Runner()
        sbx_cli.require_responsive(runner=runner)
        self.assertEqual(
            runner.calls[0][1].get('timeout'), sbx_cli.PROBE_TIMEOUT_S
        )

    def test_it_never_reads_stdin(self) -> None:
        runner = _Runner()
        sbx_cli.require_responsive(runner=runner)
        self.assertEqual(
            runner.calls[0][1].get('stdin'), subprocess.DEVNULL
        )

    def test_a_hung_daemon_is_named(self) -> None:
        with self.assertRaises(sbx_cli.SbxNotResponding):
            sbx_cli.require_responsive(
                runner=_hung(['sbx', 'ls'], sbx_cli.PROBE_TIMEOUT_S)
            )

    def test_a_missing_binary_says_so(self) -> None:
        with self.assertRaises(click.ClickException) as caught:
            sbx_cli.require_responsive(
                runner=_Runner(raises=FileNotFoundError('sbx'))
            )
        self.assertIn('cannot run `sbx`', caught.exception.format_message())

    def test_a_failed_listing_carries_sbx_stderr(self) -> None:
        with self.assertRaises(click.ClickException) as caught:
            sbx_cli.require_responsive(
                runner=_Runner(_Proc(returncode=1, stderr='not logged in'))
            )
        message = caught.exception.format_message()
        self.assertIn('not logged in', message)
        self.assertIn('sbx login', message)


if __name__ == '__main__':
    unittest.main()
