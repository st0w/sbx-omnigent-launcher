"""
Codex (OpenAI) credential seeding for sbx microVMs.

The third supported harness, beside Claude and agy. Additive: nothing
here touches an agy or Claude code path.

WHY THIS IS SO MUCH SMALLER THAN :mod:`sbx_omnigent.agy`
--------------------------------------------------------
agy needed a placeholder token, an sbx proxy header-swap on
``**.googleapis.com``, a trusted auth box, and a refresh loop every ~30
minutes. Codex needs none of it: it reads its credentials from a plain
``auth.json``, so the file is injected directly and there is no wire
interception at all.

WHAT AN AGENT VM ACTUALLY HOLDS
-------------------------------
Only a short-lived, scoped ACCESS token. The refresh token — the thing
that can mint new credentials indefinitely — never leaves the host.

Dropping ``refresh_token`` outright is not an option: the field is
schema-required and Codex refuses the file with ``missing field
refresh_token``. So the same trick agy uses applies — a placeholder of
matching shape that is unmistakably inert. Verified against codex-cli
0.147.0: ``codex login status`` reports ``Logged in using ChatGPT`` and
a real turn completes, while the guest cannot refresh anything.

Measured on 2026-08-19, the access token's JWT lifetime is **240 hours**
(agy's is ~30 minutes), so there is deliberately NO refresh daemon here.
One injection covers a multi-day campaign, and expiry is handled by
:func:`preflight` refusing to start rather than by a background loop.

``codex login --with-access-token`` is NOT the injection path: piping a
ChatGPT OAuth access token into it fails with ``agent identity JWT
payload is not valid JSON``. It wants a different credential type.

SECRET HANDLING
---------------
The access token is a real secret, unlike agy's inert placeholder, so it
is never an argv element, never echoed, and never written to a host
temp file. It moves host -> guest over STDIN only (``sbx exec`` forwards
stdin), and lands at mode 0600 inside the VM.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

#: Harness ids that mean "this agent runs the Codex CLI". Mirrors
#: :data:`sbx_omnigent.agy.AGY_HARNESSES`; the launcher and swarm use it
#: to decide which credential a VM gets.
CODEX_HARNESSES: frozenset[str] = frozenset(
    {'codex-native', 'native-codex', 'codex'}
)

#: In-guest path to the credential file, relative to ``$HOME``. Codex
#: honours ``$CODEX_HOME``; the seed script resolves it the same way so
#: an image that sets it keeps working.
GUEST_AUTH_RELPATH = '.codex/auth.json'

#: The INERT refresh token every agent VM receives in place of the real
#: one. It is deliberately self-describing rather than random: anyone
#: who finds it in a VM (or a log) should see immediately that it is not
#: a credential. Length is padded to the real token's so any shape check
#: passes.
PLACEHOLDER_REFRESH_TOKEN_PREFIX = 'PLACEHOLDER-refresh-token-not-valid-'

#: Hosts a Codex VM needs outbound. Scoped to codex VMs, never opened
#: globally. ``chatgpt.com`` is deliberately absent: the policy engine
#: rejects it and a live turn proved it unnecessary.
CODEX_EGRESS: tuple[str, ...] = ('auth.openai.com', '*.openai.com')

#: Printed by the in-VM seed script on success.
SEED_OK_MARKER = '__omni_codex_seed_ok__'

#: Warn when the access token dies within this window. A campaign can
#: run for hours, and a token that expires mid-run fails every agent
#: turn after it — far more expensive than re-authenticating first.
EXPIRY_WARN_S = 6 * 60 * 60

#: What to tell a human whose credential is missing or expired. Codex's
#: default login wants a browser on localhost:1455; this is the headless
#: flow, which is what a server host actually has.
RELOGIN_HINT = 'codex login --device-auth'


class CodexAuthError(Exception):
    """The host has no usable Codex credential."""


def host_auth_path() -> Path:
    """
    Where the HOST keeps its Codex credentials.

    :returns: ``$CODEX_HOME/auth.json`` when set, else
        ``~/.codex/auth.json``.
    """
    home = os.environ.get('CODEX_HOME')
    root = Path(home) if home else Path.home() / '.codex'
    return root / 'auth.json'


def read_host_auth(path: Path | None = None) -> dict[str, object]:
    """
    Read and validate the host's ``auth.json``.

    :param path: Override for the credential file (tests).
    :returns: The parsed document.
    :raises CodexAuthError: If it is missing, unreadable, not JSON, or
        carries no access token.
    """
    fpath = path or host_auth_path()
    try:
        raw = fpath.read_text(encoding='utf-8')
    except OSError as exc:
        raise CodexAuthError(
            f'no Codex credential at {fpath}: {exc}'
        ) from exc
    try:
        doc = json.loads(raw)
    except ValueError as exc:
        raise CodexAuthError(f'{fpath} is not valid JSON: {exc}') from exc
    if not isinstance(doc, dict):
        raise CodexAuthError(f'{fpath} is not a JSON object')
    tokens = doc.get('tokens')
    if not isinstance(tokens, dict) or not tokens.get('access_token'):
        raise CodexAuthError(f'{fpath} carries no access token')
    return doc


def access_token_expiry(auth: dict[str, object]) -> datetime | None:
    """
    When the access token expires, from its own JWT ``exp`` claim.

    Decodes only the claims segment — the signature is never inspected
    and the token is never logged.

    :param auth: A parsed ``auth.json``.
    :returns: The expiry as an aware UTC datetime, or ``None`` when the
        token is not a JWT or carries no usable ``exp``.
    """
    tokens = auth.get('tokens')
    token = tokens.get('access_token') if isinstance(tokens, dict) else None
    if not isinstance(token, str) or token.count('.') != 2:
        return None
    claims_b64 = token.split('.')[1]
    claims_b64 += '=' * (-len(claims_b64) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(claims_b64))
        exp = claims['exp']
    except (ValueError, KeyError, binascii.Error, TypeError):
        return None
    if not isinstance(exp, (int, float)):
        return None
    return datetime.fromtimestamp(exp, tz=UTC)


def build_agent_payload(auth: dict[str, object]) -> str:
    """
    The ``auth.json`` an AGENT VM gets: real access token, inert
    refresh.

    :param auth: The host's parsed credential document.
    :returns: JSON to pipe into the guest over stdin.
    """
    tokens = dict(auth.get('tokens') or {})
    real = tokens.get('refresh_token')
    pad = (
        max(0, len(real) - len(PLACEHOLDER_REFRESH_TOKEN_PREFIX))
        if isinstance(real, str)
        else 0
    )
    tokens['refresh_token'] = PLACEHOLDER_REFRESH_TOKEN_PREFIX + ('x' * pad)
    return json.dumps({**auth, 'tokens': tokens})


def build_seed_script() -> str:
    """
    The in-VM program that installs the credential, reading it on STDIN.

    Deliberately not a script literal the way agy's placeholder seed is:
    this payload is a REAL access token, so it must never appear in
    argv, where ``ps`` would show it.

    :returns: A ``python3 -c`` program.
    """
    return (
        'import json, os, sys, pathlib\n'
        'payload = sys.stdin.read()\n'
        'json.loads(payload)  # fail before writing a malformed file\n'
        "root = os.environ.get('CODEX_HOME') or "
        "os.path.join(os.path.expanduser('~'), '.codex')\n"
        'p = pathlib.Path(root)\n'
        'p.mkdir(parents=True, exist_ok=True)\n'
        "f = p / 'auth.json'\n"
        "f.write_text(payload, encoding='utf-8')\n"
        'os.chmod(f, 0o600)\n'
        f"print({SEED_OK_MARKER!r})\n"
    )


def preflight(
    *,
    path: Path | None = None,
    now: datetime | None = None,
    warn_within_s: float = EXPIRY_WARN_S,
) -> str | None:
    """
    Check the host credential BEFORE any microVM is provisioned.

    There is no refresh daemon by design (240-hour tokens), so expiry is
    handled here instead — and it is handled before provisioning
    specifically so a campaign never burns VMs discovering the problem
    mid-run. Mirrors the agy preflight's contract.

    Two outcomes, agreed 2026-08-19:

    * missing or expired -> REFUSE, naming the exact remedy
    * expiring within *warn_within_s* -> WARN and continue, so a long
      run is not started on a credential that will die partway through

    :param path: Override for the credential file (tests).
    :param now: Override for the clock (tests).
    :param warn_within_s: Warn when less than this remains.
    :returns: A warning line, or ``None`` when the credential is
        comfortably valid.
    :raises CodexAuthError: If there is no usable credential. The
        message always names :data:`RELOGIN_HINT`.
    """
    try:
        auth = read_host_auth(path)
    except CodexAuthError as exc:
        raise CodexAuthError(
            f'{exc}\nRe-authenticate on this host with:\n  {RELOGIN_HINT}'
        ) from exc

    expires_at = access_token_expiry(auth)
    if expires_at is None:
        # Not a JWT, or no `exp`. Do not invent an expiry and do not
        # refuse a credential that may well work — say so and continue.
        return (
            'codex: could not read an expiry from the access token; '
            'proceeding without an expiry check'
        )

    moment = now or datetime.now(tz=UTC)
    remaining = (expires_at - moment).total_seconds()
    if remaining <= 0:
        raise CodexAuthError(
            f'the Codex access token expired at '
            f'{expires_at:%Y-%m-%d %H:%M:%S} UTC.\n'
            f'Re-authenticate on this host with:\n  {RELOGIN_HINT}'
        )
    if remaining <= warn_within_s:
        return (
            f'codex: the access token expires in '
            f'{remaining / 3600:.1f}h ({expires_at:%Y-%m-%d %H:%M:%S} '
            f'UTC) — a long run will fail partway through. Re-'
            f'authenticate first with:  {RELOGIN_HINT}'
        )
    return None


#: A bare JWT (three base64url segments). The seeded payload is JSON, so
#: :func:`sbx_omnigent.agy.detail` already masks its ``"access_token":
#: "..."`` form — but a guest traceback can echo the token on its own,
#: unquoted, and that shape needs its own rule.
_JWT_RE = re.compile(
    r'\beyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}'
)


def redact(text: str) -> str:
    """
    Mask Codex secrets in captured output before it reaches a log.

    Composes with :func:`sbx_omnigent.agy.detail`, which already handles
    the JSON ``"access_token"``/``"refresh_token"`` forms and bounds the
    length; this adds the bare-JWT shape a guest traceback can print.

    :param text: Raw captured output.
    :returns: Text safe to surface in an error message.
    """
    # Local by necessity: a module-level import here is a cycle.
    from sbx_omnigent import agy  # noqa: PLC0415

    return agy.detail(_JWT_RE.sub('<jwt-redacted>', text))
