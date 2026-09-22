"""Codex credential seeding + preflight.

Run: .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import base64
import json
import subprocess
import tempfile
import unittest
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sbx_omnigent import codex


def _jwt(exp: datetime) -> str:
    """A token shaped like the real one, with a decodable `exp`."""
    head = base64.urlsafe_b64encode(b'{"alg":"RS256"}').decode().rstrip('=')
    body = base64.urlsafe_b64encode(
        json.dumps({'exp': int(exp.timestamp()), 'iat': 0}).encode()
    ).decode().rstrip('=')
    return f'{head}.{body}.{"s" * 32}'


def _auth(exp: datetime | None = None, **over: object) -> dict:
    """A credential document in the real 0.147.0 shape."""
    doc = {
        'auth_mode': 'chatgpt',
        'OPENAI_API_KEY': None,
        'tokens': {
            'id_token': 'id-token-value',
            'access_token': _jwt(exp) if exp else 'not-a-jwt',
            'refresh_token': 'r' * 196,
            'account_id': 'acct-1',
        },
        'last_refresh': '2026-08-19T00:53:27.420590Z',
    }
    doc.update(over)
    return doc


class _Base(unittest.TestCase):
    def _write(self, doc: object) -> Path:
        d = Path(tempfile.mkdtemp())
        f = d / 'auth.json'
        f.write_text(json.dumps(doc) if not isinstance(doc, str) else doc)
        return f


class TestTheRefreshTokenNeverLeavesTheHost(_Base):
    """The whole security argument for Codex-in-a-microVM. An agent VM
    gets a short-lived, scoped ACCESS token and nothing that can mint a
    new one. Dropping `refresh_token` outright is not an option — the
    field is schema-required and codex refuses the file with `missing
    field refresh_token` — so it is replaced with an inert placeholder,
    exactly as agy does."""

    def test_the_access_token_is_passed_through_verbatim(self) -> None:
        auth = _auth(datetime.now(tz=UTC) + timedelta(days=10))
        out = json.loads(codex.build_agent_payload(auth))
        self.assertEqual(
            out['tokens']['access_token'], auth['tokens']['access_token']
        )

    def test_the_real_refresh_token_is_not_in_the_payload(self) -> None:
        auth = _auth()
        out = json.loads(codex.build_agent_payload(auth))
        self.assertNotEqual(
            out['tokens']['refresh_token'], auth['tokens']['refresh_token']
        )
        self.assertNotIn('r' * 196, codex.build_agent_payload(auth))

    def test_the_placeholder_says_what_it_is(self) -> None:
        # Anyone who finds this in a VM or a log should see instantly
        # that it is not a credential.
        out = json.loads(codex.build_agent_payload(_auth()))
        self.assertTrue(
            out['tokens']['refresh_token'].startswith('PLACEHOLDER')
        )

    def test_the_placeholder_keeps_the_real_length(self) -> None:
        # So any shape/length check on the field still passes.
        auth = _auth()
        out = json.loads(codex.build_agent_payload(auth))
        self.assertEqual(
            len(out['tokens']['refresh_token']),
            len(auth['tokens']['refresh_token']),
        )

    def test_the_schema_required_field_is_still_present(self) -> None:
        # Stripping it makes codex refuse the file outright.
        out = json.loads(codex.build_agent_payload(_auth()))
        self.assertIn('refresh_token', out['tokens'])


class TestPreflightRefusesBeforeAnyVmIsProvisioned(_Base):
    """Codex tokens last ~240 hours, so there is deliberately no refresh
    daemon. Expiry is handled by refusing up front instead — before a
    campaign burns microVMs discovering it mid-run."""

    def test_a_healthy_token_produces_no_warning(self) -> None:
        f = self._write(_auth(datetime.now(tz=UTC) + timedelta(days=9)))
        self.assertIsNone(codex.preflight(path=f))

    def test_an_expired_token_is_refused(self) -> None:
        exp = datetime.now(tz=UTC) - timedelta(minutes=1)
        f = self._write(_auth(exp))
        with self.assertRaises(codex.CodexAuthError) as caught:
            codex.preflight(path=f)
        self.assertIn('expired', str(caught.exception))

    def test_a_missing_credential_is_refused(self) -> None:
        with self.assertRaises(codex.CodexAuthError):
            codex.preflight(path=Path('/nonexistent/auth.json'))

    def test_every_refusal_names_the_exact_remedy(self) -> None:
        # An operator should never have to go looking for the command.
        # `codex login` alone opens a browser on localhost:1455, which a
        # server host does not have — the device flow is the right one.
        cases = [
            Path('/nonexistent/auth.json'),
            self._write(_auth(datetime.now(tz=UTC) - timedelta(days=1))),
            self._write({'auth_mode': 'chatgpt'}),          # no tokens
            self._write('not json at all'),
        ]
        for path in cases:
            with self.assertRaises(codex.CodexAuthError) as caught:
                codex.preflight(path=path)
            self.assertIn('codex login --device-auth', str(caught.exception))

    def test_expiring_within_six_hours_warns_but_does_not_refuse(self) -> None:
        # The case that motivated the window: a long campaign started on
        # a token that dies partway through fails every turn after it.
        exp = datetime.now(tz=UTC) + timedelta(hours=5)
        f = self._write(_auth(exp))
        warning = codex.preflight(path=f)
        self.assertIsNotNone(warning)
        self.assertIn('expires in', warning)
        self.assertIn('codex login --device-auth', warning)

    def test_just_outside_the_window_is_silent(self) -> None:
        exp = datetime.now(tz=UTC) + timedelta(hours=7)
        self.assertIsNone(codex.preflight(path=self._write(_auth(exp))))

    def test_an_unreadable_expiry_warns_rather_than_refusing(self) -> None:
        # Never invent an expiry, and never refuse a credential that may
        # well work — say the check could not be made and continue.
        f = self._write(_auth(None))  # access token is not a JWT
        warning = codex.preflight(path=f)
        self.assertIn('could not read an expiry', warning)


class TestSecretsNeverReachALog(_Base):
    def test_a_bare_jwt_in_a_traceback_is_masked(self) -> None:
        token = _jwt(datetime.now(tz=UTC) + timedelta(days=1))
        self.assertNotIn(token, codex.redact(f'ValueError: bad {token}'))

    def test_the_json_payload_is_masked(self) -> None:
        # A guest traceback can echo its own stdin, which IS the
        # payload.
        auth = _auth(datetime.now(tz=UTC) + timedelta(days=1))
        payload = codex.build_agent_payload(auth)
        out = codex.redact(payload)
        self.assertNotIn(auth['tokens']['access_token'], out)

    def test_the_seed_script_reads_the_token_from_stdin(self) -> None:
        # NOT a script literal, unlike agy's inert placeholder:
        # this is a real secret and argv is visible in `ps`.
        script = codex.build_seed_script()
        self.assertIn('sys.stdin.read()', script)
        self.assertIn(codex.SEED_OK_MARKER, script)

    def test_the_seed_script_writes_the_file_mode_0600(self) -> None:
        self.assertIn('0o600', codex.build_seed_script())


def _doctor(
    handshake: str | None = 'ok', *, schema: object = 1
) -> str:
    """A `codex doctor --json` report in the 0.148.0 shape.

    Synthetic: real reports carry host paths, so none is copied here.
    """
    checks: dict[str, object] = {
        'auth.credentials': {
            'id': 'auth.credentials', 'status': 'ok',
            'summary': 'auth is configured',
        },
    }
    if handshake is not None:
        checks['network.websocket_reachability'] = {
            'id': 'network.websocket_reachability',
            'status': handshake,
            'summary': 'Responses WebSocket failed',
            'details': {'handshake transport error': '<redacted>'},
        }
    return json.dumps({
        'schemaVersion': schema, 'overallStatus': 'warning',
        'codexVersion': '0.148.0', 'checks': checks,
    })


class _Doctor:
    """Records each call and replays one outcome."""

    def __init__(
        self, stdout: str = '', *, raises: BaseException | None = None
    ) -> None:
        self._stdout = stdout
        self._raises = raises
        self.calls: list[tuple[list[str], dict[str, object]]] = []

    def __call__(
        self, argv: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((list(argv), kwargs))
        if self._raises is not None:
            raise self._raises
        return subprocess.CompletedProcess(argv, 0, self._stdout, '')


def _on_path(_name: str) -> str:
    return '/usr/local/bin/codex'


class TestTheLoginIsCheckedAgainstTheServer(unittest.TestCase):
    """`codex login status` and the token's `exp` both said a dead login
    was fine (#26). `codex doctor`'s WebSocket handshake is the one
    check that authenticates against the server."""

    def _probe(self, doctor: _Doctor) -> str | None:
        return codex.probe_login(run=doctor, which=_on_path)

    def test_a_live_login_passes(self) -> None:
        self.assertIsNone(self._probe(_Doctor(_doctor('ok'))))

    def test_a_failed_handshake_is_refused(self) -> None:
        with self.assertRaises(codex.CodexLoginRejected):
            self._probe(_Doctor(_doctor('warning')))

    def test_a_refusal_is_a_codex_auth_error(self) -> None:
        # Every caller already catches CodexAuthError.
        self.assertTrue(
            issubclass(codex.CodexLoginRejected, codex.CodexAuthError)
        )

    def test_a_refusal_names_the_relogin_command(self) -> None:
        with self.assertRaises(codex.CodexLoginRejected) as caught:
            self._probe(_Doctor(_doctor('warning')))
        self.assertIn(codex.RELOGIN_HINT, str(caught.exception))

    def test_a_refusal_names_the_skip_flag(self) -> None:
        # The reason is redacted, so a network that blocks WebSockets
        # looks the same as a rejected login.
        with self.assertRaises(codex.CodexLoginRejected) as caught:
            self._probe(_Doctor(_doctor('warning')))
        self.assertIn(codex.SKIP_LOGIN_CHECK_FLAG, str(caught.exception))

    def test_an_error_status_is_refused_too(self) -> None:
        with self.assertRaises(codex.CodexLoginRejected):
            self._probe(_Doctor(_doctor('error')))

    def test_it_runs_doctor_as_json_with_a_timeout(self) -> None:
        doctor = _Doctor(_doctor('ok'))
        self._probe(doctor)
        argv, kwargs = doctor.calls[0]
        self.assertEqual(argv, ['codex', 'doctor', '--json'])
        self.assertEqual(kwargs['timeout'], codex.DOCTOR_TIMEOUT_S)

    def test_the_report_never_reaches_a_message(self) -> None:
        # Redacted by codex, and still not ours to print.
        report = _doctor('warning').replace(
            '<redacted>', 'secret-looking-detail'
        )
        with self.assertRaises(codex.CodexLoginRejected) as caught:
            self._probe(_Doctor(report))
        self.assertNotIn('secret-looking-detail', str(caught.exception))


class TestAProbeThatCannotAnswerNeverBlocks(unittest.TestCase):
    """The probe is a check on the login, not a dependency: when it
    cannot be run or read, the run goes ahead with a warning."""

    def _warns(
        self,
        doctor: _Doctor,
        *,
        which: Callable[[str], str | None] = _on_path,
    ) -> str:
        warning = codex.probe_login(run=doctor, which=which)
        assert warning is not None
        return warning

    def test_no_codex_on_path(self) -> None:
        doctor = _Doctor(_doctor('ok'))
        warning = self._warns(doctor, which=lambda _n: None)
        self.assertIn('not found', warning)
        self.assertEqual(doctor.calls, [])

    def test_a_doctor_that_hangs(self) -> None:
        warning = self._warns(_Doctor(
            raises=subprocess.TimeoutExpired(['codex'], 60.0)
        ))
        self.assertIn('did not finish', warning)

    def test_a_doctor_that_cannot_run(self) -> None:
        self._warns(_Doctor(raises=OSError('exec format error')))

    def test_output_that_is_not_json(self) -> None:
        self._warns(_Doctor('Usage: codex doctor'))

    def test_a_report_that_is_not_an_object(self) -> None:
        self._warns(_Doctor('[]'))

    def test_an_unknown_report_schema(self) -> None:
        warning = self._warns(_Doctor(_doctor('warning', schema=2)))
        self.assertIn('schema', warning)

    def test_a_report_without_the_handshake_check(self) -> None:
        self._warns(_Doctor(_doctor(None)))

    def test_an_unknown_handshake_status(self) -> None:
        self._warns(_Doctor(_doctor('skipped')))

    def test_each_warning_says_the_login_was_not_checked(self) -> None:
        for doctor in (_Doctor('nope'), _Doctor(_doctor(None))):
            self.assertIn('not checked', self._warns(doctor))


if __name__ == '__main__':
    unittest.main()
