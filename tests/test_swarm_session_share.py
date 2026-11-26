"""Sharing each session with the person who runs the pipeline.

On a server with accounts, the runner's account owns every session it
creates, and the web UI lists only the sessions a person owns or was
given. Hermetic: a fake transport records each request.
"""

from __future__ import annotations

import json
import unittest
from collections.abc import Iterator

from sbx_omnigent.swarm_session import (
    EDIT_ACCESS,
    READ_ACCESS,
    SwarmSessionClient,
    SwarmSessionError,
)


class _Transport:
    """Answers every request with one status; records what was sent."""

    def __init__(self, status: int = 200) -> None:
        self._status = status
        self.requests: list[tuple[str, str, object]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, bytes]:
        decoded = json.loads(body) if body is not None else None
        self.requests.append((method, url, decoded))
        return self._status, b'{"error": {"message": "sharing is off"}}'

    def iter_lines(
        self, url: str, *, headers: dict[str, str], read_timeout: float
    ) -> Iterator[str]:
        return iter([])


def _client(
    transport: _Transport, share_with: str | None = 'alice'
) -> SwarmSessionClient:
    return SwarmSessionClient(
        'http://x:6767', transport=transport, share_with=share_with
    )


class TestShare(unittest.TestCase):
    def test_it_grants_the_person_the_level_asked_for(self) -> None:
        t = _Transport()
        _client(t).share('conv_1', level=READ_ACCESS)
        self.assertEqual(
            t.requests,
            [(
                'PUT', 'http://x:6767/v1/sessions/conv_1/permissions',
                {'user_id': 'alice', 'level': 1},
            )],
        )

    def test_edit_is_level_two_which_lets_them_post(self) -> None:
        t = _Transport()
        _client(t).share('conv_1', level=EDIT_ACCESS)
        self.assertEqual(t.requests[0][2], {'user_id': 'alice', 'level': 2})

    def test_nobody_to_share_with_sends_nothing(self) -> None:
        t = _Transport()
        _client(t, share_with=None).share('conv_1', level=READ_ACCESS)
        self.assertEqual(t.requests, [])

    def test_a_refused_grant_fails_loud(self) -> None:
        with self.assertRaises(SwarmSessionError) as caught:
            _client(_Transport(status=403)).share('c', level=READ_ACCESS)
        self.assertIn('sharing is off', str(caught.exception))

    def test_manage_or_higher_is_never_granted(self) -> None:
        t = _Transport()
        for level in (0, 3, 4):
            with self.subTest(level=level), self.assertRaises(ValueError):
                _client(t).share('conv_1', level=level)
        self.assertEqual(t.requests, [])

    def test_a_name_omnigent_would_not_accept_is_refused(self) -> None:
        # The server creates any user it is told to share with, so a
        # typo would share every session with an account that does
        # not exist, and the person running the pipeline sees none.
        for name in (' ', 'two words', '--admin', 'Alice', 'a/b'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                _client(_Transport(), share_with=name)

    def test_a_username_or_an_email_is_accepted(self) -> None:
        for name in ('alice', 'st.0w_x-1', 'alice@example.com'):
            with self.subTest(name=name):
                _client(_Transport(), share_with=name)


if __name__ == '__main__':
    unittest.main()
