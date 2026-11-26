"""Sign the runner in to an Omnigent server that has accounts.

A server started with ``OMNIGENT_AUTH_ENABLED=1`` refuses every request
that carries no token, so a runner that sends none fails on its first
call with a bare 401. Omnigent 0.23.0 added machine sign-in for exactly
this: the OAuth 2.0 client-credentials grant (RFC 6749 section 4.4).
The server is configured with one confidential client
(``OMNIGENT_MACHINE_CLIENT_ID``, the hash of its secret, and
``OMNIGENT_MACHINE_SUB``, the account its tokens act as). The runner
presents that id and secret at ``POST /oauth/token`` and gets a token
of at most an hour, which the server confines to its session, agent
and host APIs. There is no refresh token: one near expiry is replaced
by signing in again.

Chosen over reusing an ``omni login``, which Omnigent's own long-running
runner renews the same way: a machine client needs no password login as
the service user, keeps no 30-day refresh grant on disk, can't reach an
account or admin route, and the server refuses to sign it in at all as
an admin. That last one matters most, because every VM agent acts as
the account that owns its session.

The endpoint allows about ten requests a minute from one address, so
the token is minted once and reused until near its expiry, by one
worker at a time.
"""

from __future__ import annotations

import base64
import json
import os
import stat
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import quote_plus

from sbx_omnigent.swarm_session import SwarmSessionError, Transport

#: The status the server answers a missing or dead token with.
_UNAUTHORIZED = 401

#: Sign in again this long before a token's expiry, so no request
#: leaves with a token that lapses on its way.
_RENEW_BEFORE_S = 120.0

#: Lifetime assumed when the server states none usable: short, so a
#: wrong guess costs one early sign-in rather than a refused request.
_ASSUMED_LIFETIME_S = 300.0

#: What to do about each refusal the token endpoint names (RFC 6749
#: section 5.2 error codes, and RFC 8628's ``slow_down``).
_REFUSALS = {
    'unsupported_grant_type': (
        'the server does not offer machine sign-in. It needs Omnigent '
        '0.23.0 or later, started with OMNIGENT_MACHINE_CLIENT_ID, '
        'OMNIGENT_MACHINE_CLIENT_SECRET_HASH and OMNIGENT_MACHINE_SUB set'
    ),
    'invalid_client': (
        'the server refused the client id or secret. Check them against '
        "the server's OMNIGENT_MACHINE_CLIENT_ID and "
        'OMNIGENT_MACHINE_CLIENT_SECRET_HASH'
    ),
    'unauthorized_client': (
        "the server won't sign in its machine account, which it refuses "
        'when OMNIGENT_MACHINE_SUB is an admin. Give it a non-admin account'
    ),
    'slow_down': (
        'the server is throttling sign-ins from this address. Wait a '
        'minute and start again'
    ),
}


class MachineSignInFailed(SwarmSessionError):
    """The runner could not get a token from the server."""


class ServerRefusedToken(SwarmSessionError):
    """The server refused even a freshly minted token."""


@dataclass(frozen=True)
class MachineClient:
    """
    The runner's machine-client credentials.

    :param client_id: The server's ``OMNIGENT_MACHINE_CLIENT_ID``.
    :param secret_file: File holding only the client secret. Read at
        every sign-in, so a secret rotated mid-run is picked up.
    :raises ValueError: For a blank id or a relative path.
    """

    client_id: str
    secret_file: Path

    def __post_init__(self) -> None:
        if not self.client_id.strip():
            raise ValueError('the machine client id is blank')
        if not self.secret_file.is_absolute():
            raise ValueError(
                f'the machine secret file must be an absolute path: '
                f'{self.secret_file}'
            )


class TokenSource(Protocol):
    """What :class:`BearerTransport` needs from a token."""

    def current(self) -> str:
        """:returns: A token to send now."""
        ...

    def refresh(self, refused: str) -> str:
        """:returns: A token to replace *refused*."""
        ...


class MachineToken:
    """
    A machine-client token for one server, minted and renewed on demand.

    :param server_url: The server, e.g. ``"http://127.0.0.1:6767"``.
    :param client: The machine-client credentials.
    :param transport: Transport the sign-in request goes over.
    :param clock: Monotonic clock (injected in tests).
    :param timeout: Socket timeout for a sign-in, in seconds.
    """

    def __init__(
        self,
        server_url: str,
        client: MachineClient,
        transport: Transport,
        *,
        clock: Callable[[], float] = time.monotonic,
        timeout: float = 30.0,
    ) -> None:
        self._url = f'{server_url.rstrip("/")}/oauth/token'
        self._client = client
        self._transport = transport
        self._clock = clock
        self._timeout = timeout
        self._lock = threading.Lock()
        self._token: str | None = None
        self._expires_at = 0.0

    def current(self) -> str:
        """
        The token to send now, minted first if there is none usable.

        :returns: The token.
        :raises MachineSignInFailed: When the server refuses to issue
            one, with what to fix.
        """
        with self._lock:
            if (
                self._token is not None
                and self._clock() < self._expires_at - _RENEW_BEFORE_S
            ):
                return self._token
            return self._mint()

    def refresh(self, refused: str) -> str:
        """
        Replace a token the server refused.

        :param refused: The token that was refused. When another worker
            has already replaced it, that replacement is returned and
            nothing is minted.
        :returns: A token other than *refused*, minted if needed.
        :raises MachineSignInFailed: When the server refuses to issue
            one.
        """
        with self._lock:
            if self._token is not None and self._token != refused:
                return self._token
            return self._mint()

    def _mint(self) -> str:
        """
        Sign in and keep the token; the caller holds the lock.

        :returns: The new token.
        :raises MachineSignInFailed: On any refusal or unusable answer.
        """
        secret = _read_secret(self._client.secret_file)
        # RFC 6749 section 2.3.1: each half is form-encoded before the
        # base64, and the server decodes them that way.
        pair = (
            f'{quote_plus(self._client.client_id)}:{quote_plus(secret)}'
        )
        basic = base64.b64encode(pair.encode()).decode()
        status, data = self._transport.request(
            'POST', self._url,
            headers={
                'Accept': 'application/json',
                'Authorization': f'Basic {basic}',
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            body=b'grant_type=client_credentials',
            timeout=self._timeout,
        )
        token, lifetime = _parse_grant(status, data, self._url)
        self._token = token
        self._expires_at = self._clock() + lifetime
        return token


def _read_secret(path: Path) -> str:
    """
    Read the client secret, refusing a file other users could read.

    :param path: The secret file.
    :returns: The secret.
    :raises MachineSignInFailed: When the file is missing, open to
        other users, empty, or more than one line.
    """
    try:
        mode = os.stat(path).st_mode
        text = path.read_text(encoding='utf-8')
    except OSError as exc:
        raise MachineSignInFailed(
            f'cannot read the machine secret file {path}: {exc}'
        ) from exc
    if mode & stat.S_IRWXO:
        raise MachineSignInFailed(
            f'the machine secret file {path} is open to other users '
            f'(mode {stat.filemode(mode)}); allow only its owner and group'
        )
    lines = text.splitlines()
    secret = lines[0].strip() if lines else ''
    if not secret:
        raise MachineSignInFailed(f'the machine secret file {path} is empty')
    if len([line for line in lines if line.strip()]) > 1:
        raise MachineSignInFailed(
            f'the machine secret file {path} must hold the secret on one '
            f'line and nothing else'
        )
    return secret


def _parse_grant(status: int, data: bytes, url: str) -> tuple[str, float]:
    """
    The token and its lifetime from a sign-in answer.

    :param status: The HTTP status.
    :param data: The response body.
    :param url: The token endpoint, for messages.
    :returns: ``(token, lifetime_seconds)``.
    :raises MachineSignInFailed: On a refusal or an unusable answer.
    """
    try:
        doc = json.loads(data)
    except ValueError:
        doc = None
    body = doc if isinstance(doc, dict) else {}
    if status == 404:
        raise MachineSignInFailed(
            f'{url} does not exist: the server has no accounts, so it '
            f'needs no sign-in. Leave the machine client out'
        )
    if status != 200:
        error = body.get('error')
        advice = _REFUSALS.get(error) if isinstance(error, str) else None
        raise MachineSignInFailed(
            f'machine sign-in at {url} was refused (HTTP {status}'
            f'{f", {error}" if isinstance(error, str) else ""}): '
            f'{advice or "see the server log"}'
        )
    token = body.get('access_token')
    if not isinstance(token, str) or not token:
        raise MachineSignInFailed(
            f'machine sign-in at {url} answered with no token'
        )
    lifetime = body.get('expires_in')
    if (
        isinstance(lifetime, bool)
        or not isinstance(lifetime, (int, float))
        or lifetime <= 0
    ):
        lifetime = _ASSUMED_LIFETIME_S
    return token, float(lifetime)


class BearerTransport:
    """
    A :class:`~sbx_omnigent.swarm_session.Transport` that signs in.

    Sends the token on every request. A request refused with a 401 is
    retried once with a replacement: the server checks the token before
    it acts on a request, so a refused POST did nothing and is safe to
    send again.

    :param inner: The transport that does the HTTP.
    :param token: The token to send.
    """

    def __init__(self, inner: Transport, token: TokenSource) -> None:
        self._inner = inner
        self._token = token

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, bytes]:
        """
        Perform a request with the token.

        :param method: HTTP verb.
        :param url: Absolute URL.
        :param headers: Request headers; not modified.
        :param body: Raw request body, or ``None``.
        :param timeout: Socket timeout in seconds.
        :returns: ``(status_code, response_body)``.
        :raises MachineSignInFailed: When no token can be minted.
        :raises ServerRefusedToken: When a replacement is refused too.
        """
        sent = self._token.current()
        status, data = self._inner.request(
            method, url, headers=_with(headers, sent), body=body,
            timeout=timeout,
        )
        if status != _UNAUTHORIZED:
            return status, data
        status, data = self._inner.request(
            method, url, headers=_with(headers, self._token.refresh(sent)),
            body=body, timeout=timeout,
        )
        if status == _UNAUTHORIZED:
            raise ServerRefusedToken(
                f'{method} {url} was refused (HTTP 401) even with a newly '
                f'minted machine token; see the server log'
            )
        return status, data

    def iter_lines(
        self, url: str, *, headers: dict[str, str], read_timeout: float
    ) -> Iterator[str]:
        """
        Open a streaming GET with the token.

        :param url: Absolute stream URL.
        :param headers: Request headers; not modified.
        :param read_timeout: Per-read socket timeout.
        :returns: Iterator of decoded lines.
        """
        return self._inner.iter_lines(
            url, headers=_with(headers, self._token.current()),
            read_timeout=read_timeout,
        )


def _with(headers: dict[str, str], token: str) -> dict[str, str]:
    """
    A copy of *headers* carrying *token*.

    :param headers: The caller's headers.
    :param token: The token.
    :returns: The headers to send.
    """
    return {**headers, 'Authorization': f'Bearer {token}'}
