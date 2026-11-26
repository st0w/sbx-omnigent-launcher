"""Signing the runner in to a server with accounts, as a machine client.

Hermetic: the token endpoint and the API are a fake transport, the
secret is a temporary file, and the clock is injected, so no test opens
a socket or waits.
"""

from __future__ import annotations

import base64
import json
import os
import tempfile
import threading
import unittest
from collections.abc import Iterator
from pathlib import Path

from sbx_omnigent.server_auth import (
    BearerTransport,
    MachineClient,
    MachineSignInFailed,
    MachineToken,
    ServerRefusedToken,
)
from sbx_omnigent.swarm_session import SwarmSessionError

_SERVER = 'http://127.0.0.1:6767'


def _minted(token: str, expires_in: object = 3600) -> tuple[int, bytes]:
    body = {'access_token': token, 'token_type': 'Bearer',
            'expires_in': expires_in, 'scope': 'sessions'}
    return 200, json.dumps(body).encode()


def _refused(status: int, error: str) -> tuple[int, bytes]:
    return status, json.dumps({'error': error}).encode()


class _Transport:
    """Answers the token endpoint and the API from separate scripts."""

    def __init__(
        self,
        mints: list[tuple[int, bytes]],
        api: list[int] | None = None,
    ) -> None:
        self.mints = mints
        self.api = api or []
        self.mint_requests: list[tuple[dict[str, str], bytes | None]] = []
        self.api_headers: list[dict[str, str]] = []
        self.stream_headers: list[dict[str, str]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, bytes]:
        if url == f'{_SERVER}/oauth/token':
            self.mint_requests.append((dict(headers), body))
            return self.mints.pop(0)
        self.api_headers.append(dict(headers))
        return self.api.pop(0), b'{}'

    def iter_lines(
        self, url: str, *, headers: dict[str, str], read_timeout: float
    ) -> Iterator[str]:
        self.stream_headers.append(dict(headers))
        return iter([])


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class _TokenTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.secret_file = Path(tmp.name) / 'machine-secret'
        self.secret_file.write_text('s3cr3t-value\n')
        self.secret_file.chmod(0o600)
        self.clock = _Clock()

    def token(self, transport: _Transport) -> MachineToken:
        return MachineToken(
            _SERVER, MachineClient('pipeline', self.secret_file),
            transport, clock=self.clock,
        )


class TestMinting(_TokenTest):
    def test_it_presents_the_client_over_basic_auth(self) -> None:
        t = _Transport([_minted('jwt-1')])
        self.assertEqual(self.token(t).current(), 'jwt-1')
        headers, body = t.mint_requests[0]
        self.assertEqual(body, b'grant_type=client_credentials')
        scheme, _, encoded = headers['Authorization'].partition(' ')
        self.assertEqual(scheme, 'Basic')
        self.assertEqual(
            base64.b64decode(encoded).decode(), 'pipeline:s3cr3t-value'
        )

    def test_the_halves_are_form_encoded_before_base64(self) -> None:
        self.secret_file.write_text('a:b%c+d')
        t = _Transport([_minted('jwt-1')])
        self.token(t).current()
        encoded = t.mint_requests[0][0]['Authorization'].split(' ')[1]
        self.assertEqual(
            base64.b64decode(encoded).decode(), 'pipeline:a%3Ab%25c%2Bd'
        )

    def test_a_live_token_is_reused_not_reminted(self) -> None:
        t = _Transport([_minted('jwt-1')])
        token = self.token(t)
        token.current()
        self.clock.now += 3000
        self.assertEqual(token.current(), 'jwt-1')
        self.assertEqual(len(t.mint_requests), 1)

    def test_one_near_expiry_is_replaced(self) -> None:
        t = _Transport([_minted('jwt-1'), _minted('jwt-2')])
        token = self.token(t)
        token.current()
        self.clock.now += 3550
        self.assertEqual(token.current(), 'jwt-2')

    def test_an_unusable_lifetime_is_treated_as_short(self) -> None:
        for lifetime in ('soon', 0, -5, True, None):
            with self.subTest(lifetime=lifetime):
                t = _Transport(
                    [_minted('jwt-1', lifetime), _minted('jwt-2')]
                )
                token = self.token(t)
                token.current()
                self.clock.now += 200
                self.assertEqual(token.current(), 'jwt-2')

    def test_workers_asking_at_once_share_one_mint(self) -> None:
        t = _Transport([_minted('jwt-1')])
        token = self.token(t)
        seen: list[str] = []
        workers = [
            threading.Thread(target=lambda: seen.append(token.current()))
            for _ in range(8)
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=5)
        self.assertEqual(seen, ['jwt-1'] * 8)
        self.assertEqual(len(t.mint_requests), 1)

    def test_a_rotated_secret_is_read_at_the_next_mint(self) -> None:
        t = _Transport([_minted('jwt-1'), _minted('jwt-2')])
        token = self.token(t)
        token.current()
        self.secret_file.write_text('rotated')
        self.clock.now += 3550
        token.current()
        encoded = t.mint_requests[1][0]['Authorization'].split(' ')[1]
        self.assertTrue(base64.b64decode(encoded).endswith(b':rotated'))


class TestRefusals(_TokenTest):
    def assert_refused(self, answer: tuple[int, bytes], says: str) -> None:
        with self.assertRaises(MachineSignInFailed) as caught:
            self.token(_Transport([answer])).current()
        self.assertIn(says, str(caught.exception))

    def test_a_server_without_machine_sign_in(self) -> None:
        self.assert_refused(
            _refused(400, 'unsupported_grant_type'), '0.23.0'
        )

    def test_a_wrong_client_id_or_secret(self) -> None:
        self.assert_refused(
            _refused(401, 'invalid_client'), 'OMNIGENT_MACHINE_CLIENT_ID'
        )

    def test_an_admin_machine_account(self) -> None:
        self.assert_refused(
            _refused(400, 'unauthorized_client'), 'non-admin'
        )

    def test_throttled_sign_ins(self) -> None:
        self.assert_refused(_refused(429, 'slow_down'), 'throttling')

    def test_a_server_without_accounts(self) -> None:
        self.assert_refused((404, b'not found'), 'no accounts')

    def test_an_answer_without_a_token(self) -> None:
        self.assert_refused((200, b'{"token_type": "Bearer"}'), 'no token')

    def test_an_answer_that_is_not_json(self) -> None:
        self.assert_refused((200, b'<html>'), 'no token')

    def test_a_sign_in_failure_is_a_session_error(self) -> None:
        self.assertTrue(issubclass(MachineSignInFailed, SwarmSessionError))


class TestTheSecretFile(_TokenTest):
    def assert_secret_refused(self, says: str) -> None:
        t = _Transport([_minted('jwt-1')])
        with self.assertRaises(MachineSignInFailed) as caught:
            self.token(t).current()
        self.assertIn(says, str(caught.exception))
        self.assertEqual(t.mint_requests, [])

    def test_one_other_users_can_read_is_refused(self) -> None:
        self.secret_file.chmod(0o604)
        self.assert_secret_refused('other users')

    def test_an_empty_one_is_refused(self) -> None:
        self.secret_file.write_text('\n')
        self.assert_secret_refused('empty')

    def test_one_holding_more_than_one_line_is_refused(self) -> None:
        self.secret_file.write_text('one\ntwo\n')
        self.assert_secret_refused('one line')

    def test_a_missing_one_is_refused(self) -> None:
        os.remove(self.secret_file)
        self.assert_secret_refused('machine-secret')


class TestClientDetails(unittest.TestCase):
    def test_a_blank_client_id_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            MachineClient(' ', Path('/etc/secret'))

    def test_a_relative_secret_file_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            MachineClient('pipeline', Path('secret'))


class TestBearerTransport(_TokenTest):
    def call(self, transport: BearerTransport) -> int:
        status, _body = transport.request(
            'GET', f'{_SERVER}/v1/agents', headers={'Accept': 'x'},
            body=None, timeout=5.0,
        )
        return status

    def test_the_token_rides_every_request(self) -> None:
        t = _Transport([_minted('jwt-1')], api=[200])
        self.call(BearerTransport(t, self.token(t)))
        self.assertEqual(t.api_headers[0]['Authorization'], 'Bearer jwt-1')
        self.assertEqual(t.api_headers[0]['Accept'], 'x')

    def test_the_callers_headers_are_not_changed(self) -> None:
        t = _Transport([_minted('jwt-1')], api=[200])
        headers = {'Accept': 'x'}
        BearerTransport(t, self.token(t)).request(
            'GET', f'{_SERVER}/v1/agents', headers=headers, body=None,
            timeout=5.0,
        )
        self.assertEqual(headers, {'Accept': 'x'})

    def test_a_refused_token_is_reminted_and_retried_once(self) -> None:
        t = _Transport([_minted('jwt-1'), _minted('jwt-2')], api=[401, 200])
        self.assertEqual(self.call(BearerTransport(t, self.token(t))), 200)
        self.assertEqual(t.api_headers[1]['Authorization'], 'Bearer jwt-2')

    def test_a_fresh_token_also_refused_stops(self) -> None:
        t = _Transport([_minted('jwt-1'), _minted('jwt-2')], api=[401, 401])
        with self.assertRaises(ServerRefusedToken):
            self.call(BearerTransport(t, self.token(t)))
        self.assertEqual(len(t.api_headers), 2)

    def test_a_token_another_worker_renewed_is_not_reminted(self) -> None:
        t = _Transport([_minted('jwt-1'), _minted('jwt-2')])
        token = self.token(t)
        token.current()
        self.assertEqual(token.refresh('jwt-1'), 'jwt-2')
        self.assertEqual(token.refresh('jwt-1'), 'jwt-2')
        self.assertEqual(len(t.mint_requests), 2)

    def test_other_statuses_pass_through_unretried(self) -> None:
        t = _Transport([_minted('jwt-1')], api=[500])
        self.assertEqual(self.call(BearerTransport(t, self.token(t))), 500)
        self.assertEqual(len(t.mint_requests), 1)

    def test_the_stream_carries_the_token(self) -> None:
        t = _Transport([_minted('jwt-1')])
        list(BearerTransport(t, self.token(t)).iter_lines(
            f'{_SERVER}/v1/sessions/s/stream', headers={},
            read_timeout=5.0,
        ))
        self.assertEqual(
            t.stream_headers[0]['Authorization'], 'Bearer jwt-1'
        )


if __name__ == '__main__':
    unittest.main()
