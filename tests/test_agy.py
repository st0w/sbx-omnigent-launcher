"""Unit tests for the agy harvester (``sbx_omnigent.agy``).

Every process call goes through an injected fake runner, so the whole
harvest loop — token parse, redaction, argv/stdin construction, failure
classification, cadence + backoff — is exercised with no real ``sbx``.
Run with:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from click.testing import CliRunner

from sbx_omnigent import agy
from sbx_omnigent.agy import (
    BACKOFF_CEIL_S,
    BACKOFF_FLOOR_S,
    EXPIRED_MARKER,
    PLACEHOLDER_TOKEN,
    SWAP_ENV,
    SWAP_HOSTS,
    AgyHarvestError,
    AgyReloginNeeded,
    Harvester,
    acquire_harvest_lock,
    build_poke_script,
    detail,
    harvest_age_s,
    harvester_running,
    parse_access_token,
    poke_command,
    record_harvest,
    redact,
    restore_missing_workspace,
    set_custom_argv,
    spawn_harvester,
    wait_for_fresh_swap,
)

_REAL_TOKEN = 'ya29.' + 'A' * 255


def _token_body(
    access: str = _REAL_TOKEN, expiry: str = '2030-01-01T00:00:00Z'
) -> str:
    """Build a well-formed agy token-file JSON body."""
    return json.dumps(
        {
            'auth_method': 'gcp',
            'token': {
                'access_token': access,
                'refresh_token': 'r' * 103,
                'token_type': 'Bearer',
                'expiry': expiry,
            },
        }
    )


def _ok_poke() -> FakeProc:
    """A successful poke result carrying a fresh token body."""
    return FakeProc(0, 'AGY_POKE_OK\n' + _token_body())


class FakeProc:
    """Stand-in for :class:`subprocess.CompletedProcess`."""

    def __init__(
        self, returncode: int = 0, stdout: str = '', stderr: str = ''
    ):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class RecordingRunner:
    """Injectable ``subprocess.run`` fake.

    Returns queued :class:`FakeProc` results (or a default OK),
    recording every ``(argv, stdin, stdin_fd, timeout)`` call and
    raising. Accepts ``**kwargs`` so it matches ``subprocess.run``'s
    keyword surface (incl. ``input=``) without shadowing the builtin.
    """

    def __init__(self, results=None, raises=None):
        self._results = list(results or [])
        self._raises = list(raises or [])
        self.calls: list[dict] = []

    def __call__(self, argv, **kwargs):
        self.calls.append(
            {
                'argv': argv,
                'stdin': kwargs.get('input'),
                'stdin_fd': kwargs.get('stdin'),
                'timeout': kwargs.get('timeout'),
            }
        )
        if self._raises:
            exc = self._raises.pop(0)
            if exc is not None:
                raise exc
        if self._results:
            return self._results.pop(0)
        return FakeProc(0, 'AGY_POKE_OK\n' + _token_body())


class _Sleeps:
    """Records sleep durations; never actually sleeps."""

    def __init__(self) -> None:
        self.durations: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.durations.append(seconds)


#: Throwaway harvest stamp for tests. A cycle records its refresh time,
#: so every Harvester here MUST be pointed away from the real
#: ``~/.sbx-swarm`` stamp — a test must never touch the user's home.
_STAMP_TMP = Path(tempfile.gettempdir()) / 'sbx-omni-test-agy-harvest.json'

#: Module-wide redirect of the DEFAULT stamp path. Injecting via
#: ``_quiet`` is not enough: the CLI tests invoke the real ``harvest``
#: command, which builds its own Harvester with the default path — so
#: without this a test run writes the user's real ~/.sbx-swarm stamp
#: with a FAKE token's fingerprint. That is not just untidy: the
#: pipeline preflight trusts that stamp, so a test run would forge
#: "the swap secret is fresh" and let a real agy pipeline start against
#: expired credentials (observed — it fired a live run).
_STAMP_PATCH = None
_STAMP_DIR = None


def setUpModule() -> None:
    """Point the default harvest stamp at a temp dir for every test."""
    global _STAMP_PATCH, _STAMP_DIR
    _STAMP_DIR = tempfile.mkdtemp(prefix='agy-stamp-mod-')
    _STAMP_PATCH = mock.patch.object(
        agy, 'HARVEST_STAMP', Path(_STAMP_DIR) / 'agy-harvest.json'
    )
    _STAMP_PATCH.start()


def tearDownModule() -> None:
    """Restore the real stamp path and drop the temp dir."""
    if _STAMP_PATCH is not None:
        _STAMP_PATCH.stop()
    if _STAMP_DIR is not None:
        shutil.rmtree(_STAMP_DIR, ignore_errors=True)


def _quiet(**kwargs) -> Harvester:
    """Build a Harvester: no-op sleep, swallowed echo, tmp stamp."""
    kwargs.setdefault('sleep', lambda _s: None)
    kwargs.setdefault('echo', lambda _m: None)
    kwargs.setdefault('stamp_path', _STAMP_TMP)
    return Harvester(**kwargs)


class TestRedact(unittest.TestCase):
    """redact() never discloses the token."""

    def test_fingerprint_hides_token(self) -> None:
        out = redact(_REAL_TOKEN)
        self.assertNotIn(_REAL_TOKEN, out)
        self.assertIn('sha256:', out)
        self.assertIn(f'len={len(_REAL_TOKEN)}', out)

    def test_stable_for_same_token(self) -> None:
        self.assertEqual(redact(_REAL_TOKEN), redact(_REAL_TOKEN))

    def test_changes_when_token_changes(self) -> None:
        self.assertNotEqual(redact('ya29.aaa'), redact('ya29.bbb'))

    def test_empty(self) -> None:
        self.assertEqual(redact(''), '<empty>')


class TestParseAccessToken(unittest.TestCase):
    """parse_access_token() validates the nested token structure."""

    def test_valid(self) -> None:
        access, expiry = parse_access_token(_token_body())
        self.assertEqual(access, _REAL_TOKEN)
        self.assertEqual(expiry, '2030-01-01T00:00:00Z')

    def test_missing_expiry_ok(self) -> None:
        body = json.dumps({'token': {'access_token': 'ya29.x'}})
        access, expiry = parse_access_token(body)
        self.assertEqual(access, 'ya29.x')
        self.assertEqual(expiry, '')

    def test_not_json(self) -> None:
        with self.assertRaises(AgyHarvestError):
            parse_access_token('not json {{{')

    def test_not_object(self) -> None:
        with self.assertRaises(AgyHarvestError):
            parse_access_token('[1, 2, 3]')

    def test_no_token_object(self) -> None:
        with self.assertRaises(AgyHarvestError):
            parse_access_token(json.dumps({'auth_method': 'gcp'}))

    def test_empty_access_token(self) -> None:
        with self.assertRaises(AgyHarvestError):
            parse_access_token(json.dumps({'token': {'access_token': ''}}))

    def test_non_string_access_token(self) -> None:
        with self.assertRaises(AgyHarvestError):
            parse_access_token(json.dumps({'token': {'access_token': 42}}))


class TestHarvestLock(unittest.TestCase):
    """Only one harvester may poke the box: concurrent pokes race on
    the trusted box's token file."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='agy-lock-'))
        self.lock = self.root / 'h.lock'

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_first_caller_takes_it(self) -> None:
        handle = acquire_harvest_lock(self.lock)
        self.addCleanup(handle.close)
        self.assertIsNotNone(handle)

    def test_second_caller_is_refused_while_held(self) -> None:
        handle = acquire_harvest_lock(self.lock)
        self.addCleanup(handle.close)
        self.assertIsNone(acquire_harvest_lock(self.lock))
        self.assertTrue(harvester_running(path=self.lock))

    def test_releasing_frees_it(self) -> None:
        acquire_harvest_lock(self.lock).close()
        self.assertFalse(harvester_running(path=self.lock))
        second = acquire_harvest_lock(self.lock)
        self.addCleanup(second.close)
        self.assertIsNotNone(second)

    def test_probe_does_not_keep_the_lock(self) -> None:
        # harvester_running must not leave the lock held, or the
        # harvester it was probing for could never start.
        self.assertFalse(harvester_running(path=self.lock))
        handle = acquire_harvest_lock(self.lock)
        self.addCleanup(handle.close)
        self.assertIsNotNone(handle)


class TestSpawnHarvester(unittest.TestCase):
    """An auto-started harvester is detached, logged, and stdin-safe."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='agy-spawn-'))

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_launch_shape(self) -> None:
        seen = {}

        def fake_popen(argv, **kwargs):
            seen['argv'], seen['kwargs'] = argv, kwargs
            return 'proc'

        log = self.root / 'h.log'
        self.assertEqual(
            spawn_harvester(log_path=log, popen=fake_popen), 'proc'
        )
        # Same interpreter, not whatever omni-sbx-agy PATH turns up.
        self.assertEqual(seen['argv'][0], sys.executable)
        self.assertEqual(
            seen['argv'][1:], ['-m', 'sbx_omnigent.agy', 'harvest']
        )
        # stdin on /dev/null: an inherited stdin is what hangs the poke.
        self.assertEqual(seen['kwargs']['stdin'], subprocess.DEVNULL)
        self.assertTrue(seen['kwargs']['start_new_session'])
        self.assertTrue(log.exists())  # log created up front


class TestWaitForFreshSwap(unittest.TestCase):
    """The runner waits for the first refresh before provisioning."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='agy-wait-'))
        self.stamp = self.root / 'stamp.json'

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_already_fresh_returns_at_once(self) -> None:
        record_harvest('f', path=self.stamp, now=990.0)
        slept = []
        self.assertTrue(
            wait_for_fresh_swap(
                deadline_s=60, stamp_path=self.stamp,
                sleep=slept.append, now=1000.0,
            )
        )
        self.assertEqual(slept, [])

    def test_gives_up_at_the_deadline(self) -> None:
        slept = []
        self.assertFalse(
            wait_for_fresh_swap(
                deadline_s=6, stamp_path=self.stamp, poll_s=2.0,
                sleep=slept.append, now=1000.0,
            )
        )
        self.assertEqual(slept, [2.0, 2.0, 2.0])

    def test_sees_the_stamp_appear_mid_wait(self) -> None:
        def sleep(_s):
            record_harvest('f', path=self.stamp, now=1000.0)
        self.assertTrue(
            wait_for_fresh_swap(
                deadline_s=60, stamp_path=self.stamp,
                sleep=sleep, now=1000.0,
            )
        )


class TestPokeScript(unittest.TestCase):
    """The in-box poke program is well-formed and self-contained."""

    def test_compiles(self) -> None:
        # Must be valid Python for `python3 -c`.
        compile(build_poke_script(), '<poke>', 'exec')

    def test_script_gives_agy_no_inheritable_stdin(self) -> None:
        # agy reads stdin; an inherited pipe with no writer never EOFs
        # and it blocks in anon_pipe_read until the deadline.
        self.assertIn('stdin=subprocess.DEVNULL', build_poke_script())

    def test_script_catches_the_agy_hang(self) -> None:
        # An uncaught TimeoutExpired would crash the script, discarding
        # agy's partial output — the only evidence of why it hung.
        script = build_poke_script()
        compile(script, '<poke>', 'exec')
        self.assertIn('TimeoutExpired', script)
        self.assertIn('AGY_POKE_TIMEOUT', script)

    def test_embeds_expired_marker_and_timeout(self) -> None:
        script = build_poke_script(timeout_s=90)
        self.assertIn(EXPIRED_MARKER, script)
        self.assertIn('timeout=90', script)

    def test_poke_command_shape(self) -> None:
        argv = poke_command('mybox')
        self.assertEqual(argv[:4], ['sbx', 'exec', 'mybox', 'python3'])
        self.assertEqual(argv[4], '-c')


class TestSetCustomArgv(unittest.TestCase):
    """set_custom_argv() never carries the value; updates in place."""

    def test_omits_value_flag(self) -> None:
        argv = set_custom_argv()
        self.assertNotIn('--value', argv)
        self.assertNotIn('--token', argv)

    def test_fixed_placeholder_and_env(self) -> None:
        argv = set_custom_argv()
        idx = argv.index('--placeholder')
        self.assertEqual(argv[idx + 1], PLACEHOLDER_TOKEN)
        self.assertEqual(argv[argv.index('--env') + 1], SWAP_ENV)

    def test_all_hosts_present(self) -> None:
        argv = set_custom_argv()
        hosts = [argv[i + 1] for i, a in enumerate(argv) if a == '--host']
        self.assertEqual(hosts, list(SWAP_HOSTS))

    def test_multilabel_wildcard_included(self) -> None:
        # Enterprise Vertex host has 3 labels: needs the ** wildcard.
        self.assertIn('**.googleapis.com', set_custom_argv())

    def test_global_scope(self) -> None:
        self.assertIn('-g', set_custom_argv())

    def test_empty_hosts_rejected(self) -> None:
        with self.assertRaises(ValueError):
            set_custom_argv(hosts=[])


class TestHarvesterPoke(unittest.TestCase):
    """Harvester.poke() classifies every poke outcome."""

    def test_success_returns_token(self) -> None:
        h = _quiet(run=RecordingRunner([_ok_poke()]))
        self.assertEqual(h.poke(), _REAL_TOKEN)

    def test_nofile_is_relogin(self) -> None:
        proc = FakeProc(0, 'AGY_POKE_NOFILE FileNotFoundError missing')
        h = _quiet(run=RecordingRunner([proc]))
        with self.assertRaises(AgyReloginNeeded):
            h.poke()

    def test_auth_failure_is_relogin(self) -> None:
        proc = FakeProc(0, 'AGY_POKE_FAIL 1 request UNAUTHENTICATED')
        h = _quiet(run=RecordingRunner([proc]))
        with self.assertRaises(AgyReloginNeeded):
            h.poke()

    def test_transient_failure_is_retryable(self) -> None:
        proc = FakeProc(0, 'AGY_POKE_FAIL 1 connection reset by peer')
        h = _quiet(run=RecordingRunner([proc]))
        with self.assertRaises(AgyHarvestError) as ctx:
            h.poke()
        self.assertNotIsInstance(ctx.exception, AgyReloginNeeded)

    def test_in_box_hang_surfaces_agy_own_output(self) -> None:
        # The whole point: a hang must report what agy said, not an
        # opaque exec failure carrying sbx's startup chatter.
        proc = FakeProc(0, 'AGY_POKE_TIMEOUT 120 waiting for consent')
        h = _quiet(run=RecordingRunner([proc]))
        with self.assertRaises(AgyHarvestError) as ctx:
            h.poke()
        msg = str(ctx.exception)
        self.assertIn('hung', msg)
        self.assertIn('waiting for consent', msg)
        self.assertNotIsInstance(ctx.exception, AgyReloginNeeded)

    def test_in_box_hang_with_auth_signal_is_relogin(self) -> None:
        proc = FakeProc(0, 'AGY_POKE_TIMEOUT 120 login required')
        h = _quiet(run=RecordingRunner([proc]))
        with self.assertRaises(AgyReloginNeeded):
            h.poke()

    def test_poke_exec_runs_with_devnull_stdin(self) -> None:
        runner = RecordingRunner([_ok_poke()])
        _quiet(run=runner).poke()
        self.assertEqual(runner.calls[0]['stdin_fd'], subprocess.DEVNULL)

    def test_exec_nonzero_raises(self) -> None:
        h = _quiet(run=RecordingRunner([FakeProc(1, '', 'no such sandbox')]))
        with self.assertRaises(AgyHarvestError):
            h.poke()

    def test_exec_timeout_raises(self) -> None:
        runner = RecordingRunner(
            raises=[subprocess.TimeoutExpired(['sbx'], 1)]
        )
        with self.assertRaises(AgyHarvestError):
            _quiet(run=runner).poke()

    def test_malformed_ok_body_raises(self) -> None:
        h = _quiet(run=RecordingRunner([FakeProc(0, 'AGY_POKE_OK\nnot json')]))
        with self.assertRaises(AgyHarvestError):
            h.poke()

    def test_unknown_marker_raises(self) -> None:
        h = _quiet(run=RecordingRunner([FakeProc(0, 'WAT something')]))
        with self.assertRaises(AgyHarvestError):
            h.poke()


def _missing_ws_err(path: str) -> str:
    """sbx's real refusal when a workspace directory has vanished."""
    return (
        'ERROR: failed to start sandbox: start runtime: request failed: '
        f'422 Unprocessable Entity: workspace directory "{path}" no '
        'longer exists on the host; restore the directory, or remove '
        'the sandbox'
    )


class TestDetail(unittest.TestCase):
    """Third-party output reaching a log: bounded, scrubbed, and honest
    about being cut. The old caps clipped sbx's own remediation hint
    mid-word ("restore the directory, or remove t"), so the line told
    you what to do and then stopped before saying it."""

    def test_short_output_is_untouched(self) -> None:
        self.assertEqual(detail('ERROR: image not found'),
                         'ERROR: image not found')

    def test_the_remediation_hint_now_survives(self) -> None:
        # The motivating case, verbatim from the live harvest log.
        msg = _missing_ws_err('/tmp/agy-auth-trusted-ws')
        self.assertIn('or remove the sandbox', detail(msg))

    def test_a_cut_says_it_was_cut(self) -> None:
        out = detail('x' * 5000)
        self.assertLess(len(out), 5000)
        self.assertIn('more characters', out)

    def test_the_placeholder_token_is_masked(self) -> None:
        # The real shape: 'ya29.' + 255 chars.
        out = detail(f'agy: refusing token {PLACEHOLDER_TOKEN}')
        self.assertNotIn(PLACEHOLDER_TOKEN, out)
        self.assertIn('ya29.***', out)

    def test_other_credential_shapes_are_masked(self) -> None:
        for raw, gone, kept in (
            ('refresh 1//0gLongRefreshTokenValue here', '1//0gLong', '1//***'),
            ('authorization: Bearer abc123def456ghi', 'abc123def456ghi',
             'Bearer ***'),
            ('{"access_token": "ya29.secretvalue"}', 'secretvalue',
             '"access_token": "***"'),
            ('{"client_secret": "hunter2hunter2"}', 'hunter2hunter2',
             '"client_secret": "***"'),
        ):
            with self.subTest(raw=raw):
                out = detail(raw)
                self.assertNotIn(gone, out)
                self.assertIn(kept, out)

    def test_a_secret_inside_kept_text_is_still_masked(self) -> None:
        # Scrub happens BEFORE the cap, so raising the cap cannot widen
        # the leak window — which is the whole reason the scrub exists.
        out = detail(f'{PLACEHOLDER_TOKEN} then ' + 'y' * 5000)
        self.assertNotIn(PLACEHOLDER_TOKEN, out)
        self.assertIn('ya29.***', out)

    def test_empty_input_is_empty(self) -> None:
        self.assertEqual(detail(''), '')
        self.assertEqual(detail(None), '')


class TestRestoreMissingWorkspace(unittest.TestCase):
    """The trusted box's workspace is an empty placeholder living in
    /tmp by default — which macOS clears on boot. sbx then refuses to
    start the box at all, the swap secret goes stale, and the whole
    pipeline is refused at startup over a directory we own and can
    recreate."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix='agy-ws-'))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_the_placeholder_is_recreated(self) -> None:
        target = self.tmp / 'agy-auth-trusted-ws'
        self.assertEqual(
            restore_missing_workspace(_missing_ws_err(str(target))),
            str(target),
        )
        self.assertTrue(target.is_dir())

    def test_a_missing_parent_is_left_alone(self) -> None:
        # THE guard: a missing PARENT is a different failure — an
        # unmounted volume, a moved home — and creating a tree onto the
        # mountpoint would hide it and run the box on the wrong disk.
        target = self.tmp / 'not-mounted' / 'ws'
        self.assertIsNone(
            restore_missing_workspace(_missing_ws_err(str(target)))
        )
        self.assertFalse(target.parent.exists())

    def test_a_relative_path_is_refused(self) -> None:
        self.assertIsNone(restore_missing_workspace(_missing_ws_err('ws')))

    def test_an_existing_path_needs_no_restore(self) -> None:
        self.assertIsNone(
            restore_missing_workspace(_missing_ws_err(str(self.tmp)))
        )

    def test_an_unrelated_failure_is_not_matched(self) -> None:
        for other in (
            '', 'ERROR: sandbox is already running',
            'ERROR: 422 Unprocessable Entity: image not found',
        ):
            with self.subTest(other=other):
                self.assertIsNone(restore_missing_workspace(other))


class TestPokeRestoresTheWorkspace(unittest.TestCase):
    """Recovering costs one mkdir and one retry; not recovering costs a
    human noticing, and every agy agent hanging to its turn timeout."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix='agy-poke-'))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.ws = self.tmp / 'agy-auth-trusted-ws'

    def _gone(self) -> FakeProc:
        return FakeProc(1, '', _missing_ws_err(str(self.ws)))

    def test_it_recovers_and_returns_the_token(self) -> None:
        runner = RecordingRunner([self._gone(), _ok_poke()])
        self.assertEqual(_quiet(run=runner).poke(), _REAL_TOKEN)
        self.assertTrue(self.ws.is_dir())
        self.assertEqual(len(runner.calls), 2)

    def test_it_says_so_rather_than_recovering_silently(self) -> None:
        # A box that keeps losing its workspace is worth seeing in the
        # log, even though the harvester handles it.
        said: list[str] = []
        _quiet(
            run=RecordingRunner([self._gone(), _ok_poke()]), echo=said.append
        ).poke()
        self.assertTrue(
            any('recreated it' in m for m in said), said
        )

    def test_it_retries_only_once(self) -> None:
        # The second attempt failing means something else is wrong; a
        # loop would bury that instead of reporting it.
        runner = RecordingRunner([self._gone(), self._gone()])
        with self.assertRaises(AgyHarvestError) as ctx:
            _quiet(run=runner).poke()
        self.assertEqual(len(runner.calls), 2)
        # sbx's own diagnosis still reaches the human (the harvester
        # caps it at 200 chars, so assert on its head, not its tail).
        self.assertIn('workspace directory', str(ctx.exception))

    def test_an_unrelated_exec_failure_is_not_retried(self) -> None:
        runner = RecordingRunner([FakeProc(1, '', 'ERROR: image not found')])
        with self.assertRaises(AgyHarvestError):
            _quiet(run=runner).poke()
        self.assertEqual(len(runner.calls), 1)


class TestHarvesterUpdateSecret(unittest.TestCase):
    """update_secret() feeds the token on stdin, never argv."""

    def test_token_on_stdin_not_argv(self) -> None:
        runner = RecordingRunner([FakeProc(0)])
        _quiet(run=runner).update_secret(_REAL_TOKEN)
        call = runner.calls[0]
        self.assertEqual(call['stdin'], _REAL_TOKEN)
        self.assertNotIn(_REAL_TOKEN, call['argv'])

    def test_uses_set_custom(self) -> None:
        runner = RecordingRunner([FakeProc(0)])
        _quiet(run=runner).update_secret(_REAL_TOKEN)
        self.assertEqual(
            runner.calls[0]['argv'][:3], ['sbx', 'secret', 'set-custom']
        )

    def test_nonzero_raises(self) -> None:
        runner = RecordingRunner([FakeProc(1, '', 'denied')])
        with self.assertRaises(AgyHarvestError):
            _quiet(run=runner).update_secret('t')


class TestHarvesterCycle(unittest.TestCase):
    """cycle_once() chains poke -> update and returns a redacted id."""

    def test_returns_redacted_fingerprint(self) -> None:
        runner = RecordingRunner([_ok_poke(), FakeProc(0)])
        out = _quiet(run=runner).cycle_once()
        self.assertNotIn(_REAL_TOKEN, out)
        self.assertEqual(out, redact(_REAL_TOKEN))

    def test_secret_gets_the_poked_token(self) -> None:
        runner = RecordingRunner([_ok_poke(), FakeProc(0)])
        _quiet(run=runner).cycle_once()
        # Second call is the secret update; its stdin is the real token.
        self.assertEqual(runner.calls[1]['stdin'], _REAL_TOKEN)


class TestHarvesterLoop(unittest.TestCase):
    """run_forever() cadence + backoff, driven by max_cycles."""

    def test_success_sleeps_interval_resets_backoff(self) -> None:
        runner = RecordingRunner(
            [_ok_poke(), FakeProc(0), _ok_poke(), FakeProc(0)]
        )
        sleeps = _Sleeps()
        _quiet(run=runner, sleep=sleeps, interval_s=1800).run_forever(
            max_cycles=2
        )
        self.assertEqual(sleeps.durations, [1800, 1800])

    def test_backoff_grows_geometrically_on_failure(self) -> None:
        # Every poke fails transiently -> geometric backoff from floor.
        fails = [FakeProc(0, 'AGY_POKE_FAIL 1 network down') for _ in range(4)]
        sleeps = _Sleeps()
        _quiet(run=RecordingRunner(fails), sleep=sleeps).run_forever(
            max_cycles=3
        )
        self.assertEqual(
            sleeps.durations,
            [BACKOFF_FLOOR_S, BACKOFF_FLOOR_S * 2, BACKOFF_FLOOR_S * 4],
        )

    def test_backoff_capped_at_ceiling(self) -> None:
        fails = [
            FakeProc(0, 'AGY_POKE_FAIL 1 network down') for _ in range(30)
        ]
        sleeps = _Sleeps()
        _quiet(run=RecordingRunner(fails), sleep=sleeps).run_forever(
            max_cycles=30
        )
        self.assertTrue(all(d <= BACKOFF_CEIL_S for d in sleeps.durations))
        self.assertEqual(sleeps.durations[-1], BACKOFF_CEIL_S)

    def test_backoff_resets_after_recovery(self) -> None:
        # fail, fail, succeed, succeed
        runner = RecordingRunner(
            [
                FakeProc(0, 'AGY_POKE_FAIL 1 network down'),
                FakeProc(0, 'AGY_POKE_FAIL 1 network down'),
                _ok_poke(), FakeProc(0),
                _ok_poke(), FakeProc(0),
            ]
        )
        sleeps = _Sleeps()
        _quiet(run=runner, sleep=sleeps, interval_s=1800).run_forever(
            max_cycles=4
        )
        self.assertEqual(
            sleeps.durations,
            [BACKOFF_FLOOR_S, BACKOFF_FLOOR_S * 2, 1800, 1800],
        )

    def test_relogin_logged_loudly_and_backs_off(self) -> None:
        proc = FakeProc(0, 'AGY_POKE_NOFILE FileNotFoundError x')
        sleeps = _Sleeps()
        msgs: list[str] = []
        Harvester(
            run=RecordingRunner([proc]), sleep=sleeps, echo=msgs.append
        ).run_forever(max_cycles=1)
        self.assertTrue(any('RE-LOGIN REQUIRED' in m for m in msgs))
        self.assertEqual(sleeps.durations, [BACKOFF_FLOOR_S])

    def test_no_token_ever_logged(self) -> None:
        runner = RecordingRunner([_ok_poke(), FakeProc(0)])
        msgs: list[str] = []
        Harvester(
            run=runner, sleep=_Sleeps(), echo=msgs.append, interval_s=1
        ).run_forever(max_cycles=1)
        for m in msgs:
            self.assertNotIn(_REAL_TOKEN, m)


def _invoke_cli(args, runner_stub):
    """Invoke the agy CLI with ``subprocess.run`` patched."""
    with mock.patch.object(agy.subprocess, 'run', runner_stub):
        return CliRunner().invoke(agy.cli, args)


_SET_CUSTOM = ['sbx', 'secret', 'set-custom']


class TestHarvestCli(unittest.TestCase):
    """The `harvest` click command (via CliRunner + patched runner)."""

    def setUp(self) -> None:
        # Never contend for the REAL lock: a harvester running on this
        # host would otherwise fail the suite.
        self.root = Path(tempfile.mkdtemp(prefix='agy-cli-'))
        patcher = mock.patch.object(
            agy, 'HARVEST_LOCK', self.root / 'h.lock'
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(shutil.rmtree, self.root, True)

    def test_refuses_when_another_harvester_holds_the_lock(self) -> None:
        held = acquire_harvest_lock(self.root / 'h.lock')
        self.addCleanup(held.close)
        result = _invoke_cli(['harvest', '--once'], RecordingRunner())
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('another agy harvester', result.output)

    def test_lock_is_released_when_the_command_returns(self) -> None:
        _invoke_cli(['harvest', '--once'], RecordingRunner([_ok_poke()]))
        self.assertFalse(harvester_running(path=self.root / 'h.lock'))

    def test_once_success_prints_fingerprint_not_token(self) -> None:
        runner = RecordingRunner([_ok_poke(), FakeProc(0)])
        result = _invoke_cli(['harvest', '--once'], runner)
        self.assertEqual(result.exit_code, 0)
        self.assertIn('sha256:', result.output)
        self.assertNotIn(_REAL_TOKEN, result.output)

    def test_once_installs_poked_token_on_stdin(self) -> None:
        runner = RecordingRunner([_ok_poke(), FakeProc(0)])
        _invoke_cli(['harvest', '--once'], runner)
        # calls: [0]=poke exec, [1]=secret set-custom w/ token on stdin.
        self.assertEqual(runner.calls[1]['stdin'], _REAL_TOKEN)
        self.assertNotIn(_REAL_TOKEN, runner.calls[1]['argv'])

    def test_once_failure_nonzero_exit(self) -> None:
        runner = RecordingRunner([FakeProc(0, 'AGY_POKE_FAIL 1 net down')])
        result = _invoke_cli(['harvest', '--once'], runner)
        self.assertNotEqual(result.exit_code, 0)


class TestBootstrapCli(unittest.TestCase):
    """The `bootstrap` command orchestrates the right sbx calls."""

    def test_create_path_orders_calls(self) -> None:
        runner = RecordingRunner()  # every call returns default OK
        result = _invoke_cli(
            ['bootstrap', '--box', 'probe', '--image', 'img:tag'], runner
        )
        self.assertEqual(result.exit_code, 0, result.output)
        argvs = [c['argv'] for c in runner.calls]
        self.assertEqual(argvs[0][:2], ['mkdir', '-p'])
        self.assertEqual(
            argvs[1][:4], ['sbx', 'create', 'shell', '/tmp/probe-ws']
        )
        self.assertIn('img:tag', argvs[1])
        self.assertEqual(
            argvs[2][:5],
            ['sbx', 'policy', 'allow', 'network', '--sandbox'],
        )
        self.assertEqual(argvs[3][:3], _SET_CUSTOM)

    def test_seeds_placeholder_value_on_stdin(self) -> None:
        runner = RecordingRunner()
        _invoke_cli(['bootstrap', '--box', 'probe'], runner)
        seed = next(
            c for c in runner.calls if c['argv'][:3] == _SET_CUSTOM
        )
        self.assertEqual(seed['stdin'], PLACEHOLDER_TOKEN)
        self.assertNotIn('--value', seed['argv'])

    def test_skip_create_omits_create(self) -> None:
        runner = RecordingRunner()
        _invoke_cli(['bootstrap', '--box', 'probe', '--skip-create'], runner)
        argvs = [c['argv'] for c in runner.calls]
        self.assertFalse(any(a[:2] == ['mkdir', '-p'] for a in argvs))
        self.assertFalse(
            any(a[:3] == ['sbx', 'create', 'shell'] for a in argvs)
        )
        # Still applies egress + seeds the secret.
        self.assertTrue(
            any(a[:4] == ['sbx', 'policy', 'allow', 'network'] for a in argvs)
        )

    def test_create_failure_aborts_before_secret(self) -> None:
        # mkdir OK, then `sbx create` fails -> no policy/secret calls.
        runner = RecordingRunner([FakeProc(0), FakeProc(1, '', 'boom')])
        result = _invoke_cli(['bootstrap', '--box', 'probe'], runner)
        self.assertNotEqual(result.exit_code, 0)
        argvs = [c['argv'] for c in runner.calls]
        self.assertFalse(any(a[:3] == _SET_CUSTOM for a in argvs))


class TestHarvestStamp(unittest.TestCase):
    """The freshness stamp: the only host-side signal that the
    write-only swap secret still holds a live token."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix='agy-stamp-'))
        self.stamp = self.tmp / 'nested' / 'agy-harvest.json'

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_records_time_and_fingerprint_no_token(self) -> None:
        record_harvest(redact(_REAL_TOKEN), path=self.stamp, now=1000.0)
        raw = json.loads(self.stamp.read_text(encoding='utf-8'))
        self.assertEqual(raw['refreshed_at'], 1000.0)
        self.assertEqual(raw['fingerprint'], redact(_REAL_TOKEN))
        # A stamp must never carry secret material.
        self.assertNotIn(_REAL_TOKEN, self.stamp.read_text(encoding='utf-8'))

    def test_age_measured_from_recorded_time(self) -> None:
        record_harvest('sha256:x len=1', path=self.stamp, now=1000.0)
        self.assertEqual(
            harvest_age_s(path=self.stamp, now=1600.0), 600.0
        )

    def test_age_none_when_never_harvested(self) -> None:
        # No stamp = freshness unknown; the caller must treat it as
        # stale, not as fresh.
        self.assertIsNone(harvest_age_s(path=self.stamp, now=1.0))

    def test_age_none_when_malformed(self) -> None:
        self.stamp.parent.mkdir(parents=True, exist_ok=True)
        for bad in ('not json {{{', '[1,2,3]', '{"refreshed_at": "soon"}'):
            self.stamp.write_text(bad, encoding='utf-8')
            self.assertIsNone(harvest_age_s(path=self.stamp, now=1.0))

    def test_age_never_negative_on_clock_skew(self) -> None:
        record_harvest('f', path=self.stamp, now=2000.0)
        self.assertEqual(harvest_age_s(path=self.stamp, now=1000.0), 0.0)

    def test_write_failure_never_breaks_the_harvest(self) -> None:
        # The refresh already succeeded; a stamp we cannot write costs a
        # preflight warning later, never a failed cycle.
        unwritable = self.tmp / 'a-file'
        unwritable.write_text('x', encoding='utf-8')
        record_harvest('f', path=unwritable / 'stamp.json', now=1.0)

    def test_cycle_once_records_the_refresh(self) -> None:
        runner = RecordingRunner([_ok_poke(), FakeProc(0)])
        out = _quiet(run=runner, stamp_path=self.stamp).cycle_once()
        age = harvest_age_s(path=self.stamp)
        self.assertIsNotNone(age)
        self.assertLess(age, 60.0)  # just stamped
        raw = json.loads(self.stamp.read_text(encoding='utf-8'))
        self.assertEqual(raw['fingerprint'], out)

    def test_failed_cycle_records_nothing(self) -> None:
        # A cycle that never installed a token must not claim freshness.
        runner = RecordingRunner([FakeProc(1, '', 'boom')])
        with self.assertRaises(AgyHarvestError):
            _quiet(run=runner, stamp_path=self.stamp).cycle_once()
        self.assertIsNone(harvest_age_s(path=self.stamp))

    def test_tests_never_write_the_users_real_stamp(self) -> None:
        # Regression guard. A test run once wrote ~/.sbx-swarm with a
        # FAKE token fingerprint, which the pipeline preflight then read
        # as "the swap secret is fresh" and let a real agy run start on
        # expired credentials. The default path must stay redirected for
        # the whole module — including the CLI-driven Harvesters.
        default = agy.HARVEST_STAMP
        self.assertNotEqual(default, Path.home() / '.sbx-swarm'
                            / 'agy-harvest.json')
        self.assertIn('agy-stamp-mod-', str(default))


if __name__ == '__main__':
    unittest.main()
