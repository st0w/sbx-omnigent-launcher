"""Reading a guest's runner log for the reason a turn failed (#26)."""

from __future__ import annotations

import subprocess
import unittest

from sbx_omnigent import agy, codex, guest_log

#: Lines seen live on 2026-09-14, when a dead Codex login surfaced only
#: as a startup timeout.
_CODEX_REFRESH = (
    'ERROR: Your access token could not be refreshed. Please log out '
    'and sign in again.'
)
_CODEX_401 = 'failed to connect to websocket: HTTP error: 401 Unauthorized'
_CODEX_EXPIRED = (
    'ERROR: Your access token could not be refreshed because your '
    'refresh token has expired.'
)
#: agy's wording for a dead login since 1.1 (#70).
_AGY_SIGN_IN = 'Please sign in to view available models'
#: Claude Code 2.1.280 on a made-up OAuth token and a made-up API key:
#: its StopFailure hook's `last_assistant_message`, which Omnigent makes
#: the failed turn's error.
_CLAUDE_OAUTH = (
    'Failed to authenticate. API Error: 401 OAuth access token is invalid.'
)
_CLAUDE_KEY = 'Failed to authenticate. API Error: 401 API key is invalid.'
#: The routing line printed beside that failure. It says "login" and
#: "logged in" and is NOT an auth failure.
_ROUTING = (
    "Launch routing: Codex CLI login (subscription provider 'codex'; "
    'Codex is logged in).'
)


class _Run:
    """Records the call and replays one outcome."""

    def __init__(
        self,
        stdout: str = '',
        *,
        rc: int = 0,
        raises: BaseException | None = None,
    ) -> None:
        self._stdout = stdout
        self._rc = rc
        self._raises = raises
        self.calls: list[tuple[list[str], dict[str, object]]] = []

    def __call__(
        self, argv: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((list(argv), kwargs))
        if self._raises is not None:
            raise self._raises
        return subprocess.CompletedProcess(argv, self._rc, self._stdout, '')


class TestTheTailCommand(unittest.TestCase):
    def test_it_runs_inside_the_named_sandbox(self) -> None:
        argv = guest_log.tail_command('managed-1234abcd', lines=50)
        self.assertEqual(
            argv[:6], ['sbx', 'exec', 'managed-1234abcd', '--', 'sh', '-lc']
        )

    def test_it_reads_the_newest_runner_log(self) -> None:
        script = guest_log.tail_command('b', lines=50)[-1]
        self.assertIn(guest_log.RUNNER_LOG_GLOB, script)
        self.assertIn('ls -t', script)
        self.assertIn('tail -n 50', script)

    def test_it_never_fails_for_a_missing_log(self) -> None:
        self.assertTrue(
            guest_log.tail_command('b', lines=5)[-1].endswith('exit 0')
        )


class TestReadingTheLog(unittest.TestCase):
    def test_it_returns_the_tail(self) -> None:
        run = _Run(f'boot\n{_CODEX_401}\n')
        self.assertEqual(
            guest_log.read_runner_log('b', run=run), f'boot\n{_CODEX_401}'
        )

    def test_it_is_bounded(self) -> None:
        run = _Run('x')
        guest_log.read_runner_log('b', run=run, timeout_s=7.0)
        self.assertEqual(run.calls[0][1]['timeout'], 7.0)

    def test_a_hang_is_none(self) -> None:
        run = _Run(raises=subprocess.TimeoutExpired(['sbx'], 7.0))
        self.assertIsNone(guest_log.read_runner_log('b', run=run))

    def test_no_sbx_is_none(self) -> None:
        self.assertIsNone(
            guest_log.read_runner_log('b', run=_Run(raises=OSError('no')))
        )

    def test_a_failed_exec_is_none(self) -> None:
        self.assertIsNone(guest_log.read_runner_log('b', run=_Run('x', rc=1)))

    def test_an_empty_log_is_none(self) -> None:
        self.assertIsNone(guest_log.read_runner_log('b', run=_Run(' \n')))


class TestFindingAnAuthFailure(unittest.TestCase):
    def test_the_codex_lines_seen_live_match(self) -> None:
        for line in (_CODEX_REFRESH, _CODEX_401, _CODEX_EXPIRED):
            with self.subTest(line=line):
                self.assertIsNotNone(guest_log.auth_failure(line))

    def test_agys_sign_in_wording_matches(self) -> None:
        self.assertIsNotNone(guest_log.auth_failure(_AGY_SIGN_IN))

    def test_claudes_wording_matches(self) -> None:
        for line in (_CLAUDE_OAUTH, _CLAUDE_KEY):
            with self.subTest(line=line):
                self.assertIsNotNone(guest_log.auth_failure(line))

    def test_the_routing_line_is_not_an_auth_failure(self) -> None:
        # Seen beside the real failure on 9/14, saying the login was
        # fine: "login" alone must never count.
        self.assertIsNone(guest_log.auth_failure(_ROUTING))

    def test_it_ignores_case(self) -> None:
        self.assertIsNotNone(guest_log.auth_failure(_CODEX_401.upper()))

    def test_it_names_a_fixed_phrase_never_the_line(self) -> None:
        # A log line can carry a token; the phrase comes from the table.
        found = guest_log.auth_failure(f'token=eyJsecret {_CODEX_401}')
        self.assertIn(found, guest_log.AUTH_SIGNALS)

    def test_nothing_is_none(self) -> None:
        self.assertIsNone(guest_log.auth_failure(None))
        self.assertIsNone(guest_log.auth_failure(''))
        self.assertIsNone(guest_log.auth_failure('runner started\nidle'))


class TestTheRemedy(unittest.TestCase):
    def test_codex_names_the_host_login(self) -> None:
        self.assertIn(
            codex.RELOGIN_HINT, guest_log.relogin_hint('codex-native')
        )

    def test_agy_names_the_trusted_box(self) -> None:
        self.assertIn(
            agy.TRUSTED_BOX_DEFAULT,
            guest_log.relogin_hint('antigravity-native'),
        )

    def test_claude_names_the_token_it_runs_on(self) -> None:
        self.assertIn(
            'claude setup-token', guest_log.relogin_hint('claude-native')
        )

    def test_an_unknown_harness_still_gets_advice(self) -> None:
        self.assertTrue(guest_log.relogin_hint('pi-native'))


if __name__ == '__main__':
    unittest.main()
