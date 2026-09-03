"""Antigravity (``agy``) harvester — keep the swap secret's token fresh.

The ``agy`` swarm harness authenticates by **harvest / proxy-swap**
(see memory ``agy-harvest-proxy-swap`` and ``docs/ANTIGRAVITY.md``).
Every agy *agent* microVM carries only a **placeholder** OAuth access
token; an sbx custom secret swaps that placeholder for a **real** access
token in the outbound ``Authorization: Bearer`` header on the wire, so a
YOLO agent box never holds a durable credential.

The real access token is short-lived (~1h). One **trusted auth-agy** box
holds the *only* real refresh token and self-refreshes; this module is
the host-side helper that:

- **pokes** the trusted box on a cadence to force a token refresh
  (``agy models`` after expiring the on-disk token — proven in Stage 0),
- **reads** the freshly minted access token off the box, and
- **updates** the sbx custom secret's *value* with it — **on stdin**, so
  the token never appears in a process argument or a log line.

The trusted box is a plain ``sbx`` sandbox (NOT an Omnigent host): it is
driven purely via ``sbx exec`` and never dials back. It goes ``stopped``
when idle; ``sbx exec`` auto-starts it, so the harvester's own cadence
keeps it warm.

Run via the ``omni-sbx-agy`` console script.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import re
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

import click

#: Default name of the trusted auth-agy sandbox (Stage 0).
TRUSTED_BOX_DEFAULT = 'agy-auth-trusted'

#: In-box path to agy's nested OAuth token file (Linux microVM). The
#: value is ``{"auth_method":..,"token":{access_token,refresh_token,
#: token_type,expiry}}``; ``expiry`` is RFC3339 nanosecond UTC.
TOKEN_FILE = '~/.gemini/antigravity-cli/antigravity-oauth-token'

#: The **placeholder** access token that every agy *agent* VM emits and
#: that the swap secret matches on the wire. It must look like a real
#: access token (``ya29.`` prefix) so any format check passes, yet be
#: unmistakably inert. Stage 3 injects this SAME value into agent VM
#: token files — keep the two in sync by importing this constant.
PLACEHOLDER_TOKEN = (
    'ya29.a0-AGY-HARVEST-PROXY-SWAP-PLACEHOLDER-DO-NOT-USE-'
    '0000000000000000000000000000'
)

#: Inert refresh token seeded into an agent VM's token file. It is never
#: used: the far-future :data:`PLACEHOLDER_EXPIRY` stops agy from ever
#: attempting a refresh (the wire swap supplies a live token), and
#: the durable refresh credential lives only on the trusted box.
PLACEHOLDER_REFRESH_TOKEN = 'AGY-HARVEST-PROXY-SWAP-NO-REFRESH-DO-NOT-USE'

#: Far-future RFC3339 expiry for the placeholder token file so agy never
#: tries to refresh it (which would fail — the VM has no real refresh
#: credential). The proxy swaps a live token onto the wire regardless of
#: this local value.
PLACEHOLDER_EXPIRY = '2099-01-01T00:00:00Z'

#: The ``antigravity-native`` harness id + its canonical aliases (from
#: Omnigent ``harness_plugins``). Used by the swarm's fail-loud gate to
#: recognize an agy-bound agent from ``GET /v1/agents``.
AGY_HARNESSES: frozenset[str] = frozenset(
    {'antigravity-native', 'native-antigravity', 'antigravity'}
)

#: sbx env var the custom secret pins the placeholder to. The swap is
#: content-based (matches the placeholder in the header regardless of
#: this var), so the name is cosmetic — but ``set-custom`` wants one.
SWAP_ENV = 'AGY_OAUTH_ACCESS_TOKEN'

#: Hosts the swap secret is scoped to. ``**.googleapis.com`` (any number
#: of labels) is REQUIRED because enterprise/GCP accounts route the
#: call to the 3-label Vertex regional host — a single-``*`` wildcard
#: would miss it and yield ``UNAUTHENTICATED (401)``. The explicit hosts
#: are belt-and-suspenders for the single- and no-label backends.
SWAP_HOSTS: tuple[str, ...] = (
    '**.googleapis.com',
    '*.googleapis.com',
    'aiplatform.us.rep.googleapis.com',
    'cloudcode-pa.googleapis.com',
)

#: Minimal STEADY-STATE egress for the trusted box (Stage 0): refresh +
#: the ``agy models`` backend. Everything else is deniable post-login.
TRUSTED_BOX_EGRESS: tuple[str, ...] = (
    'oauth2.googleapis.com',
    'cloudcode-pa.googleapis.com',
)

#: Superset the trusted box needs the ONE time a human runs ``/login``
#: (eligibility / userinfo / OAuth consent / assets). Applied by
#: ``bootstrap``; may be tightened to :data:`TRUSTED_BOX_EGRESS` after.
TRUSTED_BOX_LOGIN_EGRESS: tuple[str, ...] = (
    *TRUSTED_BOX_EGRESS,
    'www.googleapis.com',
    'accounts.google.com',
    '*.googleusercontent.com',
    'antigravity.google',
    'antigravity-unleash.goog',
)

# NOTE on stdin: the outer ``sbx exec`` AND the in-box ``agy models``
# both run with stdin on /dev/null. ``capture_output`` pipes only
# stdout+stderr and leaves stdin INHERITED, so agy receives whatever
# the harvester was launched with. When that is a pipe with no writer,
# agy's read never sees EOF: it blocks in ``anon_pipe_read`` — no
# socket opened, no output, no error — until the poke deadline.
# Observed live: the loop failed EVERY cycle from a terminal while the
# identical argv succeeded in ~1s elsewhere.

#: A far-PAST RFC3339 timestamp written into the token file to force agy
#: to treat the token as expired and re-mint on the next authed action.
EXPIRED_MARKER = '2000-01-01T00:00:00Z'

#: Default cadence: token lifetime is ~1h, so a ~30-min force-refresh
#: keeps the secret holding a comfortably-fresh (<30-min-old) token.
DEFAULT_INTERVAL_S = 30 * 60

#: How long a single poke (``agy models`` round-trip) may take.
DEFAULT_POKE_TIMEOUT_S = 120.0

#: Backoff floor/ceiling (seconds) after a failed cycle, before the next
#: retry. Grows geometrically from the floor, capped at the ceiling.
BACKOFF_FLOOR_S = 30.0
BACKOFF_CEIL_S = 10 * 60.0

#: Where a successful harvest records WHEN it last refreshed the swap
#: secret. The secret itself is write-only (``sbx secret set-custom``
#: takes a value, never returns one), so this stamp is the only
#: host-side signal of whether the secret still holds a live token —
#: which is what a pipeline preflight needs to know before it spends
#: real time on agy VMs. It holds no secret material, only a redacted
#: fingerprint and a timestamp.
HARVEST_STAMP = Path.home() / '.sbx-swarm' / 'agy-harvest.json'

#: Exclusive lock a running harvester holds for its whole lifetime.
#: Only ONE may poke the trusted box: each poke rewrites the token file
#: with an expired marker before forcing a re-mint, so two overlapping
#: pokes race on that file. Held via ``flock``, so the kernel releases
#: it even if the harvester is killed outright.
HARVEST_LOCK = Path.home() / '.sbx-swarm' / 'agy-harvest.lock'

#: Where a harvester started BY THE RUNNER writes its output. An
#: auto-started loop has no terminal of its own, and its lines must not
#: interleave with the pipeline's own progress.
HARVEST_LOG = Path.home() / '.sbx-swarm' / 'agy-harvest.log'

#: How stale the last harvest may be before an agy pipeline is refused.
#: A real access token lives ~1h and a healthy harvester refreshes every
#: :data:`DEFAULT_INTERVAL_S` (~30 min), so 1.5x the cadence clears any
#: healthy loop while still catching the case that actually bites: no
#: harvester running at all, so the secret's token has expired and every
#: agy turn dies unauthenticated.
MAX_SWAP_AGE_S = DEFAULT_INTERVAL_S * 1.5


class AgyHarvestError(RuntimeError):
    """A harvest cycle failed for an operational (usually retryable)
    reason — box unreachable, malformed token file, secret update
    rejected."""


class AgyReloginNeeded(AgyHarvestError):
    """The trusted box can no longer refresh (its refresh token is
    revoked/expired) — a human must re-run ``agy /login``. Distinct
    so the loop can shout this specific, human-actionable state."""


#: Cap on third-party output (sbx's, agy's) embedded in an error or a
#: log line. The old 160-300 char caps cut mid-sentence — sbx's own
#: remediation hint reached the log as "restore the directory, or
#: remove t", stopping before it said what to do — which is the exact
#: opposite of what a diagnostic is for. Wide enough for a real
#: traceback or a multi-line refusal, still bounded so a runaway
#: process cannot flood a log file.
_DETAIL_CHARS = 2000

#: Token-shaped material masked before third-party output is logged.
#: agy's failure output is AGY's, not ours: on the way down it can
#: print a request header or a token-file dump, and this text lands in
#: a log that outlives the run. Raising the caps widens that window, so
#: it is closed here rather than left to the old truncation to hide by
#: accident. A fingerprint (see :func:`redact`) is the only form of a
#: token this module ever writes.
_SECRET_RES: tuple[tuple[re.Pattern[str], str], ...] = (
    # Google OAuth access ('ya29.') and refresh ('1//') tokens.
    (re.compile(r'ya29\.[A-Za-z0-9._\-]{8,}'), 'ya29.***'),
    (re.compile(r'1//[A-Za-z0-9._\-]{8,}'), '1//***'),
    (re.compile(r'(?i)\b(bearer)\s+[A-Za-z0-9._\-]{8,}'), r'\1 ***'),
    (
        re.compile(
            r'(?i)"(access_token|refresh_token|id_token|client_secret)"'
            r'(\s*:\s*)"[^"]*"'
        ),
        r'"\1"\2"***"',
    ),
)


def detail(text: str, *, limit: int = _DETAIL_CHARS) -> str:
    """
    Make third-party output safe and bounded for a log or an error.

    Masks token-shaped material, then caps the length — SAYING so when
    it cuts, instead of trailing off mid-word and leaving the reader
    unsure whether the sentence ended or the process did.

    :param text: Raw captured output from sbx or agy.
    :param limit: Max characters to keep.
    :returns: The scrubbed, bounded text.
    """
    out = (text or '').strip()
    for pattern, repl in _SECRET_RES:
        out = pattern.sub(repl, out)
    if len(out) > limit:
        out = f'{out[:limit]}… [+{len(out) - limit} more characters]'
    return out


def redact(token: str) -> str:
    """
    Render *token* as a non-reversible fingerprint safe to log.

    Never emits the token itself: only a short SHA-256 prefix plus the
    length, enough to tell "did the token change" across cycles without
    disclosing any secret material.

    :param token: The (real or placeholder) access token.
    :returns: e.g. ``"sha256:7cfb9337 len=260"``; ``<empty>`` if blank.
    """
    if not token:
        return '<empty>'
    digest = hashlib.sha256(token.encode()).hexdigest()[:8]
    return f'sha256:{digest} len={len(token)}'


def record_harvest(
    fingerprint: str,
    *,
    path: Path | None = None,
    now: float | None = None,
) -> None:
    """
    Record that the swap secret was just refreshed.

    Written after every successful cycle so a later pipeline run can
    tell whether the secret still holds a live token (see
    :func:`harvest_age_s`). Best-effort: the refresh itself already
    succeeded, so a stamp write failure must never fail the harvest —
    a stale/missing stamp only costs a preflight warning later.

    :param fingerprint: Redacted token fingerprint (:func:`redact`) —
        NEVER the token itself.
    :param path: Stamp file; ``None`` uses :data:`HARVEST_STAMP`.
    :param now: Epoch seconds to record; ``None`` uses the clock.
    """
    stamp = path or HARVEST_STAMP
    payload = {
        'refreshed_at': time.time() if now is None else now,
        'fingerprint': fingerprint,
    }
    try:
        stamp.parent.mkdir(parents=True, exist_ok=True)
        stamp.write_text(json.dumps(payload), encoding='utf-8')
    except OSError:
        return


def harvest_age_s(
    *, path: Path | None = None, now: float | None = None
) -> float | None:
    """
    Seconds since the swap secret was last refreshed, if knowable.

    :param path: Stamp file; ``None`` uses :data:`HARVEST_STAMP`.
    :param now: Epoch seconds to measure from; ``None`` uses the clock.
    :returns: The age in seconds, or ``None`` when no usable stamp
        exists (never harvested, unreadable, or malformed) — which a
        caller must treat exactly like "far too old", since it equally
        means the secret's freshness is unknown.
    """
    stamp = path or HARVEST_STAMP
    try:
        raw = json.loads(stamp.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    at = raw.get('refreshed_at') if isinstance(raw, dict) else None
    if not isinstance(at, (int, float)) or isinstance(at, bool):
        return None
    return max(0.0, (time.time() if now is None else now) - float(at))


def acquire_harvest_lock(path: Path | None = None):
    """
    Take the exclusive harvester lock, or report it already held.

    :param path: Lock file; ``None`` uses :data:`HARVEST_LOCK`.
    :returns: The open handle (KEEP it — closing releases the lock), or
        ``None`` when another live process holds it.
    """
    target = path or HARVEST_LOCK
    target.parent.mkdir(parents=True, exist_ok=True)
    handle = target.open('w')
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return None
    return handle


def harvester_running(*, path: Path | None = None) -> bool:
    """
    Whether another process currently holds the harvester lock.

    :param path: Lock file; ``None`` uses :data:`HARVEST_LOCK`.
    :returns: ``True`` when a harvester owns the lock.
    """
    handle = acquire_harvest_lock(path)
    if handle is None:
        return True
    fcntl.flock(handle, fcntl.LOCK_UN)
    handle.close()
    return False


def spawn_harvester(
    *,
    log_path: Path | None = None,
    popen: Callable[..., subprocess.Popen[bytes]] = subprocess.Popen,
) -> subprocess.Popen[bytes]:
    """
    Start a detached always-on harvester, logging to a file.

    Launched via ``sys.executable -m`` rather than the console script,
    so it resolves from the SAME interpreter as the caller instead of
    whatever ``omni-sbx-agy`` a stray PATH turns up.
    ``start_new_session`` detaches it from the caller's process group,
    and stdin is /dev/null
    — a harvester that inherits a live stdin is precisely what hangs the
    in-box poke (see the stdin note above).

    :param log_path: Output file; ``None`` uses :data:`HARVEST_LOG`.
    :param popen: Process launcher (injected in tests).
    :returns: The running child.
    """
    log = log_path or HARVEST_LOG
    log.parent.mkdir(parents=True, exist_ok=True)
    handle = log.open('a', encoding='utf-8')
    return popen(
        [sys.executable, '-m', 'sbx_omnigent.agy', 'harvest'],
        stdout=handle,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )


def wait_for_fresh_swap(
    *,
    deadline_s: float,
    stamp_path: Path | None = None,
    poll_s: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
    now: float | None = None,
) -> bool:
    """
    Poll until the swap secret is freshly harvested, or give up.

    :param deadline_s: How long to wait before reporting failure.
    :param stamp_path: Harvest stamp; ``None`` uses the default.
    :param poll_s: Seconds between checks.
    :param sleep: Sleeper (injected in tests).
    :param now: Epoch seconds to measure age from; ``None`` = clock.
    :returns: ``True`` once the stamp is within :data:`MAX_SWAP_AGE_S`.
    """
    waited = 0.0
    while True:
        age = harvest_age_s(path=stamp_path, now=now)
        if age is not None and age <= MAX_SWAP_AGE_S:
            return True
        if waited >= deadline_s:
            return False
        sleep(poll_s)
        waited += poll_s


def parse_access_token(raw: str) -> tuple[str, str]:
    """
    Extract ``(access_token, expiry)`` from an agy token-file body.

    :param raw: The raw JSON text of the token file.
    :returns: The non-empty access token and its RFC3339 expiry string.
    :raises AgyHarvestError: If the body is not JSON, not the expected
        nested ``token`` object, or carries an empty/absent access token
        (a structural failure — never retried as a re-login).
    """
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        raise AgyHarvestError(
            f'trusted-box token file is not valid JSON: {exc}'
        ) from exc
    if not isinstance(data, dict):
        raise AgyHarvestError('trusted-box token file is not a JSON object')
    token = data.get('token')
    if not isinstance(token, dict):
        raise AgyHarvestError(
            "trusted-box token file lacks a 'token' object"
        )
    access = token.get('access_token')
    if not isinstance(access, str) or not access:
        raise AgyHarvestError(
            "trusted-box token file has no non-empty 'access_token'"
        )
    expiry = token.get('expiry')
    if not isinstance(expiry, str):
        expiry = ''
    return access, expiry


# ── Agent-VM credential seeding (Stage 3) ─────────────────────────────
#
# Every agy *agent* microVM must carry a token file BEFORE its Omnigent
# runner launches, or Omnigent's ``harness_is_configured`` gate refuses
# to start the ``antigravity-native`` frame (it reads the file for a
# non-empty access/refresh token). The launcher seeds only a placeholder
# — never a real credential — so the gate passes while the agent holds
# nothing usable; the sbx proxy swaps in a live token on the wire. It
# also pre-accepts agy's first-run onboarding wizard so a headless,
# TTY-less launch never stalls on the theme/EULA prompts.


def auth_method_for(*, enterprise: bool) -> str:
    """
    The agy ``auth_method`` string for the account type.

    agy branches on this at runtime to pick its credential path: an
    **enterprise / Business (GCP)** account uses ``"gcp"``, a consumer
    account uses ``"oauth"``. NOT cosmetic — a mismatch makes agy
    reject the seeded token *locally* ("Please sign in") and never reach
    the wire swap, even though the token file is otherwise valid.
    Confirmed live: the trusted box's real token carries ``"gcp"`` for
    this Business account, and only ``"gcp"`` let the placeholder
    authenticate through the swap.

    :param enterprise: Whether the account is enterprise/Business.
    :returns: ``"gcp"`` for enterprise, else ``"oauth"``.
    """
    return 'gcp' if enterprise else 'oauth'


def placeholder_token_file(*, enterprise: bool) -> dict[str, object]:
    """
    Build the Linux nested token-file object seeded into an agent VM.

    Mirrors what agy writes to
    ``~/.gemini/antigravity-cli/antigravity-oauth-token`` on Linux: the
    OAuth object nested under ``token``. The access token is the inert
    :data:`PLACEHOLDER_TOKEN`; the far-future :data:`PLACEHOLDER_EXPIRY`
    stops agy from ever refreshing it. ``auth_method`` MUST match the
    account type (see :func:`auth_method_for`).

    :param enterprise: Whether the account is enterprise/Business.
    :returns: The token-file object (serialize with :func:`json.dumps`).
    """
    return {
        'auth_method': auth_method_for(enterprise=enterprise),
        'token': {
            'access_token': PLACEHOLDER_TOKEN,
            'refresh_token': PLACEHOLDER_REFRESH_TOKEN,
            'token_type': 'Bearer',
            'expiry': PLACEHOLDER_EXPIRY,
        },
    }


def placeholder_oauth_creds(*, enterprise: bool) -> dict[str, object]:
    """
    Build the flat ``oauth_creds.json`` seeded alongside the token.

    Omnigent's gate checks BOTH the nested Linux token file and the flat
    ``~/.gemini/oauth_creds.json`` (macOS shape); seeding both makes the
    agent read as "logged in" regardless of which path agy or the
    bridge consults. Same inert placeholder values + matching
    ``auth_method``.

    :param enterprise: Whether the account is enterprise/Business.
    :returns: The flat creds object (serialize with :func:`json.dumps`).
    """
    return {
        'auth_method': auth_method_for(enterprise=enterprise),
        'access_token': PLACEHOLDER_TOKEN,
        'refresh_token': PLACEHOLDER_REFRESH_TOKEN,
        'token_type': 'Bearer',
        'expiry': PLACEHOLDER_EXPIRY,
    }


def onboarding_json(*, enterprise: bool) -> dict[str, object]:
    """
    Build agy's onboarding-complete marker for an agent VM.

    Seeded at ``~/.gemini/antigravity-cli/cache/onboarding.json`` so a
    fresh, headless agy never launches its first-run TUI wizard.
    ``enterpriseOnboardingComplete`` tracks the account type: a
    Business/enterprise Google account must set it ``True`` (else agy
    re-runs enterprise onboarding — theme picker + EULA — which a
    TTY-less VM cannot answer), while a consumer account leaves it
    ``False``.

    :param enterprise: Whether the account is enterprise/Business.
    :returns: The onboarding marker object.
    """
    return {
        'consumerOnboardingComplete': True,
        'enterpriseOnboardingComplete': enterprise,
        'onboardingComplete': True,
    }


def settings_json(
    *, gcp_project: str | None, gcp_location: str = 'us'
) -> dict[str, object]:
    """
    Build the agy ``settings.json`` seed (the GCP project block).

    An enterprise / Vertex account needs a GCP project to run a cascade
    (else ``agent executor error: invalid project ID: ""``); ``agy
    models`` alone does not. ``settings.json`` IS in Omnigent's
    ``_AGY_SEED_FILES`` (copied HOME → the bridge dir), so seeding it
    here reaches the agy the executor drives.

    :param gcp_project: GCP project id, e.g. ``"p-p-gement-001"``. When
        falsy, returns ``{}`` — a consumer account needs no project.
    :param gcp_location: GCP location, default ``"us"``.
    :returns: ``{"gcp": {"project": .., "location": ..}}`` or ``{}``.
    """
    if not gcp_project:
        return {}
    return {
        'gcp': {'project': gcp_project, 'location': gcp_location or 'us'}
    }


#: In-VM seeding program. Writes the placeholder token file, the flat
#: creds decoy, and the onboarding marker into the agent's ``~/.gemini``
#: with tight perms (0700 dirs, 0600 files), then prints the OK marker.
#: Idempotent (overwrites). The three JSON payloads are injected as
#: literals — all inert placeholders, so no secret ever enters argv.
_SEED_SCRIPT = r"""
import json, os
home = os.path.expanduser('~')
gem = os.path.join(home, '.gemini')
acli = os.path.join(gem, 'antigravity-cli')
cache = os.path.join(acli, 'cache')
os.makedirs(cache, mode=0o700, exist_ok=True)
os.chmod(gem, 0o700)
os.chmod(acli, 0o700)


def _w(path, obj):
    with open(path, 'w') as fh:
        json.dump(obj, fh, sort_keys=True)
        fh.write('\n')
    os.chmod(path, 0o600)


_w(os.path.join(acli, 'antigravity-oauth-token'), json.loads({token!r}))
_w(os.path.join(gem, 'oauth_creds.json'), json.loads({creds!r}))
_w(os.path.join(cache, 'onboarding.json'), json.loads({onboarding!r}))
_settings = json.loads({settings!r})
if _settings:
    _w(os.path.join(acli, 'settings.json'), _settings)
print('AGY_SEED_OK')
"""

#: Sentinel the launcher greps for in the seed script's stdout.
SEED_OK_MARKER = 'AGY_SEED_OK'

#: Sentinel printed by the in-VM enterprise-onboarding bridge patch.
BRIDGE_PATCH_OK_MARKER = 'AGY_BRIDGE_PATCHED'

#: In-VM patch of the installed ``antigravity_native_bridge`` module,
#: run BEFORE the runner imports it. Two independent best-effort edits
#: (see :func:`build_bridge_patch_script`): flip the hardcoded
#: ``enterpriseOnboardingComplete: False`` (so a Business account skips
#: agy's first-run wizard), and add ``settings.json`` to
#: ``_AGY_SEED_FILES`` on older builds that omit it (so the seeded GCP
#: project reaches the per-session bridge dir). Best-effort — a no-op
#: when omnigent is absent or a literal changed; always prints the
#: marker.
_BRIDGE_PATCH_SCRIPT = r"""
import os, sys
_target = os.environ.get('AGY_BRIDGE_PATCH_TARGET')
if _target:
    # Test/probe path: edit exactly this file, importing NOTHING — so a
    # host editable-installed omnigent can never be resolved and written
    # (the editable import finder outranks PYTHONPATH). Never set in the
    # VM: the launcher runs this via `sbx exec` with no such env.
    _path = _target
    _seed_files = ()
else:
    try:
        import omnigent.antigravity_native_bridge as _b
    except Exception as exc:
        print('AGY_BRIDGE_PATCHED', 'skip', type(exc).__name__)
        sys.exit(0)
    _path = _b.__file__
    _seed_files = getattr(_b, '_AGY_SEED_FILES', ())
_do_ent = {enterprise}
_do_settings = {seed_settings}
_do_paste = {paste_placeholder}
try:
    _src = open(_path).read()
    _changed = 0
    if _do_ent:
        _needle = '"enterpriseOnboardingComplete": False'
        if _needle in _src:
            _src = _src.replace(
                _needle, '"enterpriseOnboardingComplete": True'
            )
            _changed += 1
    if _do_settings:
        _have = any(
            str(_x).endswith('settings.json') for _x in _seed_files
        ) or 'antigravity-cli") / "settings.json"' in _src
        _anchor = '_AGY_SEED_FILES = ('
        if not _have and _anchor in _src:
            _ins = _anchor + '\n    Path("antigravity-cli") / "settings.json",'
            _src = _src.replace(_anchor, _ins, 1)
            _changed += 1
    if _do_paste:
        # Accept three renders the composer-window check cannot see: a
        # MULTI-LINE draft (it grows agy's box past the last separator
        # pair the check scopes to, so that window reads empty), a long
        # paste COLLAPSED to "[Pasted text #N]", and a draft SCROLLED
        # past its own first line. The last one is why the needle alone
        # is not enough: the bridge derives it from the FIRST line
        # (_submit_needle), but a long reply scrolls that line out of
        # the pane entirely, so no needle match can succeed anywhere —
        # while the composer plainly shows the message's TAIL, cursor at
        # the end. So also accept a tail slice of the content. Match
        # whitespace-collapsed against the whole pane so a wrapped line
        # still matches. A pane with none of the three still fails.
        # Keyed on a unique sentinel so an incidental "[Pasted text"
        # elsewhere in the module can neither block nor falsely satisfy
        # this patch.
        _mark = 'agy_ml_paste_patch'
        _anchor = 'if not draft_seen and not mid_turn:'
        if _anchor in _src and _mark not in _src:
            _fix = (
                'if not draft_seen:  # ' + _mark + '\n'
                '        _flat = " ".join(last_commit_pane.split())\n'
                '        _n = " ".join((needle or "").split())\n'
                '        _t = " ".join((content or "").split())[-24:]\n'
                '        if ("[Pasted text" in last_commit_pane\n'
                '                or (_n and _n in _flat)\n'
                '                or (len(_t) >= 8 and _t in _flat)):\n'
                '            draft_seen = True\n'
                '    '
            )
            _src = _src.replace(_anchor, _fix + _anchor, 1)
            _changed += 1
    if _changed:
        open(_path, 'w').write(_src)
    print('AGY_BRIDGE_PATCHED', _changed)
except OSError as exc:
    print('AGY_BRIDGE_PATCHED', 'err', type(exc).__name__)
"""


def build_agent_seed_script(
    *,
    enterprise: bool,
    gcp_project: str | None = None,
    gcp_location: str = 'us',
) -> str:
    """
    Render the in-VM ``python3 -c`` program that seeds agy credentials.

    :param enterprise: Set for a Business/enterprise Google account
        (drives :func:`onboarding_json` + :func:`auth_method_for`).
    :param gcp_project: GCP project id seeded into ``settings.json``
        (required for an enterprise/Vertex cascade; ``None`` seeds no
        settings). See :func:`settings_json`.
    :param gcp_location: GCP location for the settings block.
    :returns: A self-contained Python program for ``sbx exec … python3
        -c``; prints :data:`SEED_OK_MARKER` on success.
    """
    return _SEED_SCRIPT.format(
        token=json.dumps(placeholder_token_file(enterprise=enterprise)),
        creds=json.dumps(placeholder_oauth_creds(enterprise=enterprise)),
        onboarding=json.dumps(onboarding_json(enterprise=enterprise)),
        settings=json.dumps(
            settings_json(
                gcp_project=gcp_project, gcp_location=gcp_location
            )
        ),
    )


def build_bridge_patch_script(
    *,
    enterprise: bool,
    seed_settings: bool,
    paste_placeholder: bool = True,
) -> str:
    """
    Render the in-VM bridge patch (``python3 -c``), run before the
    runner imports the module.

    :param enterprise: Flip ``enterpriseOnboardingComplete`` → ``True``
        so a Business account skips agy's first-run onboarding wizard.
    :param seed_settings: Add ``antigravity-cli/settings.json`` to
        ``_AGY_SEED_FILES`` on older Omnigent builds that omit it, so
        the seeded GCP project block is copied to the bridge dir
        (else an enterprise cascade fails ``invalid project ID``).
    :param paste_placeholder: Accept a render the bridge's composer
        check cannot see. Before pressing Enter it requires a substring
        of the message inside agy's composer, scoped to the lines
        BETWEEN the last two separator rules — but a MULTI-LINE draft
        grows the box past that pair (so the scoped window reads empty)
        and a long paste collapses to a ``[Pasted text #N … chars]``
        placeholder. Either way the turn dies with "agy did not render
        the pasted message", which is what a HUMAN's reply to an
        interactive planner hits (the runner's own turns dodge it by
        going through :data:`_AGY_TASK_FILE`). The patch re-checks the
        WHOLE pane, whitespace-collapsed so a wrapped line still
        matches, and accepts the placeholder — both mean agy really did
        take the paste. A pane showing neither still fails.
    :returns: The program; prints :data:`BRIDGE_PATCH_OK_MARKER`.
    """
    return _BRIDGE_PATCH_SCRIPT.format(
        enterprise=bool(enterprise),
        seed_settings=bool(seed_settings),
        paste_placeholder=bool(paste_placeholder),
    )


#: In-box poke script. Force-expires the on-disk token, runs the
#: lightest authenticated action (``agy models`` — no inference) to make
#: agy re-mint via ``oauth2``, then prints a machine-readable result:
#: ``AGY_POKE_OK`` followed by the fresh token-file JSON on the next
#: line, ``AGY_POKE_FAIL <rc> <stderr-head>`` on a non-zero ``agy``, or
#: ``AGY_POKE_TIMEOUT <secs> <output-head>`` when ``agy models`` hangs —
#: carrying whatever agy printed BEFORE it stalled, which is the only
#: evidence of why. Every branch exits 0: a non-zero exit would surface
#: as an opaque "sbx exec failed" carrying sbx's own startup chatter
#: instead of agy's diagnosis.
#: The host reads only stdout — the real token stays inside the trusted
#: plane (box -> host over ``sbx exec``, never a log).
_POKE_SCRIPT = r"""
import json, os, subprocess, sys
p = os.path.expanduser({token_file!r})
try:
    d = json.load(open(p))
except Exception as e:
    print('AGY_POKE_NOFILE', type(e).__name__, str(e)[:160]); sys.exit(0)
try:
    d['token']['expiry'] = {expired!r}
    json.dump(d, open(p, 'w'))
except Exception as e:
    print('AGY_POKE_NOFILE', type(e).__name__, str(e)[:160]); sys.exit(0)
try:
    r = subprocess.run(['agy', 'models'], capture_output=True, text=True,
                       stdin=subprocess.DEVNULL, timeout={timeout:d})
except subprocess.TimeoutExpired as e:
    so, se = e.stdout or '', e.stderr or ''
    if isinstance(so, bytes): so = so.decode('utf-8', 'replace')
    if isinstance(se, bytes): se = se.decode('utf-8', 'replace')
    head = (se + ' || ' + so).replace('\n', ' ').strip()[:1200]
    print('AGY_POKE_TIMEOUT', {timeout:d}, head or '<agy printed nothing>')
    sys.exit(0)
except Exception as e:
    print('AGY_POKE_FAIL', -1, type(e).__name__ + ' ' + str(e)[:800])
    sys.exit(0)
if r.returncode != 0:
    head = (r.stderr or r.stdout).replace('\n', ' ')[:1200]
    print('AGY_POKE_FAIL', r.returncode, head); sys.exit(0)
print('AGY_POKE_OK')
print(open(p).read())
"""


def build_poke_script(
    *, token_file: str = TOKEN_FILE, timeout_s: float = DEFAULT_POKE_TIMEOUT_S
) -> str:
    """
    Render the in-box poke script for the given token path/timeout.

    :param token_file: The ``~``-relative token path inside the box.
    :param timeout_s: Ceiling for the in-box ``agy models`` call; passed
        as an int (``subprocess.run`` timeout).
    :returns: A self-contained ``python3 -c`` program.
    """
    return _POKE_SCRIPT.format(
        token_file=token_file,
        expired=EXPIRED_MARKER,
        timeout=max(1, int(timeout_s)),
    )


#: sbx names the path in its refusal, so the fix does not have to guess
#: it (and still works if the box was bootstrapped elsewhere).
_MISSING_WORKSPACE_RE = re.compile(
    r'workspace directory "([^"]+)" no longer exists'
)


def restore_missing_workspace(stderr: str) -> str | None:
    """
    Recreate a workspace directory sbx reports as gone, when it is safe.

    sbx will not start a sandbox whose workspace has vanished — it fails
    the whole exec with a 422 naming the path. For the trusted box that
    directory is an empty PLACEHOLDER: :func:`bootstrap` creates it with
    one ``mkdir -p`` and nothing is ever written there (agy's token and
    settings live inside the box's own filesystem). Its default home is
    ``/tmp/<box>-ws``, which macOS clears on every boot, so a single
    reboot strands the harvester behind an opaque 422 — and a stale swap
    secret then refuses the whole pipeline at startup. Observed live,
    where the fix was one ``mkdir`` a human had to know to run.

    Only the LEAF is created, never parents. A missing PARENT is a
    different failure — an unmounted volume, a moved home — and building
    a tree onto a mountpoint would hide it and leave the box running
    against the wrong filesystem.

    :param stderr: Captured stderr from the failed ``sbx`` call.
    :returns: The restored path, or ``None`` when the error was not a
        missing workspace, the path is unusable, or creation failed.
    """
    match = _MISSING_WORKSPACE_RE.search(stderr or '')
    if match is None:
        return None
    path = Path(match.group(1))
    if not path.is_absolute() or path.exists() or not path.parent.is_dir():
        return None
    try:
        path.mkdir(exist_ok=True)
    except OSError:
        return None
    return str(path)


def poke_command(
    box: str, *, token_file: str = TOKEN_FILE,
    timeout_s: float = DEFAULT_POKE_TIMEOUT_S,
) -> list[str]:
    """
    Build the ``sbx exec`` argv that runs the poke script in *box*.

    :param box: Trusted-box sandbox name.
    :param token_file: In-box token path.
    :param timeout_s: In-box ``agy models`` timeout.
    :returns: Full argv for :func:`subprocess.run`.
    """
    return [
        'sbx', 'exec', box, 'python3', '-c',
        build_poke_script(token_file=token_file, timeout_s=timeout_s),
    ]


def set_custom_argv(
    *,
    placeholder: str = PLACEHOLDER_TOKEN,
    env: str = SWAP_ENV,
    hosts: Sequence[str] = SWAP_HOSTS,
) -> list[str]:
    """
    Build the ``sbx secret set-custom`` argv for the swap secret.

    Deliberately **omits** ``--value``: the token comes on **stdin**
    (``set-custom`` reads it from its hidden prompt when the flag is
    absent), so the secret never lands in argv/``ps``/shell history. A
    **fixed** ``--placeholder`` makes re-runs *update in place* (same
    entry, new value) rather than pile up duplicate placeholders.

    :param placeholder: The fixed placeholder token (must equal what agy
        agent VMs emit).
    :param env: sbx env var to pin the placeholder to.
    :param hosts: Hosts the swap applies to (repeated ``--host``).
    :returns: The argv (feed the token via ``input=`` on
        :func:`subprocess.run`).
    :raises ValueError: If *hosts* is empty (a secret with no target
        would silently swap nothing).
    """
    if not hosts:
        raise ValueError('set_custom_argv requires at least one host')
    argv = ['sbx', 'secret', 'set-custom', '-g']
    for host in hosts:
        argv += ['--host', host]
    argv += ['--env', env, '--placeholder', placeholder]
    return argv


def _classify_poke(stdout: str) -> str:
    """
    Return the first whitespace token of the poke's stdout (its status
    marker), or ``''`` if there is none.

    :param stdout: Captured stdout from the poke command.
    :returns: ``AGY_POKE_OK`` / ``AGY_POKE_FAIL`` / ``AGY_POKE_NOFILE``
        / ``AGY_POKE_TIMEOUT`` / ``''``.
    """
    stripped = stdout.strip()
    if not stripped:
        return ''
    return stripped.split(None, 1)[0]


def _looks_like_relogin(text: str) -> bool:
    """
    Heuristically decide whether an ``agy models`` failure means the
    trusted box's refresh token is dead (human re-login needed) vs. a
    transient/network error.

    :param text: The failure text (poke stdout tail / stderr).
    :returns: ``True`` when auth-failure signals dominate.
    """
    low = text.lower()
    signals = (
        'unauthenticated', 'invalid_grant', 'invalid_token', '401',
        'login', 'reauth', 'expired or revoked', 'permission_denied',
    )
    return any(s in low for s in signals)


def _run_subprocess(
    argv: list[str], **kwargs: object
) -> subprocess.CompletedProcess[str]:
    """
    Default process runner — resolves ``subprocess.run`` at *call* time.

    Used as the :class:`Harvester.run` default (and by
    :func:`_run_local`) so a test can patch ``sbx_omnigent.agy.
    subprocess.run`` and intercept EVERY process call. Binding
    ``subprocess.run`` directly as the field default would capture the
    reference at class-definition time and leave it un-patchable.

    :param argv: Full command argv.
    :param kwargs: Forwarded to :func:`subprocess.run`.
    :returns: The completed process.
    """
    return subprocess.run(argv, **kwargs)  # type: ignore[call-overload]


@dataclass
class Harvester:
    """
    Drives the trusted box's refresh loop and the secret update.

    All process I/O goes through injectable callables so the whole loop
    — cadence, backoff, failure classification, argv/stdin construction
    — is unit-testable without a real ``sbx``.

    :param box: Trusted-box sandbox name.
    :param placeholder: Fixed swap placeholder token.
    :param env: sbx env var for the swap secret.
    :param hosts: Swap secret host scope.
    :param interval_s: Seconds between successful cycles.
    :param poke_timeout_s: Ceiling for one poke round-trip.
    :param run: ``subprocess.run``-like runner (injected in tests).
    :param sleep: ``time.sleep``-compatible sleeper (injected in tests).
    :param echo: Line logger; defaults to :func:`click.echo`. Receives
        only redacted, secret-free strings.
    """

    box: str = TRUSTED_BOX_DEFAULT
    placeholder: str = PLACEHOLDER_TOKEN
    env: str = SWAP_ENV
    hosts: tuple[str, ...] = SWAP_HOSTS
    interval_s: float = DEFAULT_INTERVAL_S
    poke_timeout_s: float = DEFAULT_POKE_TIMEOUT_S
    run: Callable[..., subprocess.CompletedProcess[str]] = _run_subprocess
    sleep: Callable[[float], None] = time.sleep
    echo: Callable[[str], None] = field(default=click.echo)
    #: Freshness stamp written after each successful cycle; ``None``
    #: uses :data:`HARVEST_STAMP` (injected in tests).
    stamp_path: Path | None = None

    #: Extra wall-clock margin over the in-box timeout for the outer
    #: ``sbx exec`` (box auto-start + transport), so the host-side call
    #: doesn't time out before the in-box one reports.
    _EXEC_MARGIN_S: ClassVar[float] = 60.0

    def _exec_poke(
        self, argv: list[str]
    ) -> subprocess.CompletedProcess[str]:
        """
        Run one poke round-trip against the trusted box.

        :param argv: The ``sbx exec`` argv to run.
        :returns: The completed process (any return code).
        :raises AgyHarvestError: On timeout, or if sbx cannot be run.
        """
        try:
            return self.run(
                argv,
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
                timeout=self.poke_timeout_s + self._EXEC_MARGIN_S,
            )
        except subprocess.TimeoutExpired as exc:
            raise AgyHarvestError(
                f'poke timed out after {self.poke_timeout_s:.0f}s '
                f'(box {self.box!r} unresponsive)'
            ) from exc
        except OSError as exc:
            raise AgyHarvestError(f'could not exec sbx: {exc}') from exc

    def poke(self) -> str:
        """
        Force a refresh on the trusted box and return the fresh token.

        Runs the poke script via ``sbx exec``, then parses the emitted
        token-file JSON.

        :returns: The freshly minted real access token (never logged
            raw).
        :raises AgyReloginNeeded: If ``agy`` failed with auth signals,
            or the token file is missing — box needs a human ``/login``.
        :raises AgyHarvestError: For any other poke/parse failure.
        """
        argv = poke_command(self.box, timeout_s=self.poke_timeout_s)
        proc = self._exec_poke(argv)
        if proc.returncode != 0:
            # A workspace that vanished under the box is not an agy
            # problem and needs no human: restore the placeholder and
            # retry ONCE. Once only — a second identical failure means
            # something else is wrong, and a loop would just bury it.
            restored = restore_missing_workspace(proc.stderr or '')
            if restored is not None:
                self.echo(
                    f'[agy-harvest] {self.box!r} lost its workspace '
                    f'placeholder ({restored}) — recreated it; retrying.'
                )
                proc = self._exec_poke(argv)
        if proc.returncode != 0:
            raise AgyHarvestError(
                f'sbx exec on {self.box!r} failed (rc={proc.returncode}): '
                f'{detail(proc.stderr or "")}'
            )
        marker = _classify_poke(proc.stdout)
        if marker == 'AGY_POKE_OK':
            body = proc.stdout.split('\n', 1)[1] if '\n' in proc.stdout else ''
            access, _expiry = parse_access_token(body)
            return access
        tail = proc.stdout.strip()
        if marker == 'AGY_POKE_NOFILE':
            raise AgyReloginNeeded(
                f'trusted box {self.box!r} has no token file — run '
                f"'agy /login' on it (details: {detail(tail)})"
            )
        if marker == 'AGY_POKE_TIMEOUT':
            if _looks_like_relogin(tail):
                raise AgyReloginNeeded(
                    f'trusted box {self.box!r} stalled while refreshing — '
                    f"run 'agy /login' on it (details: {detail(tail)})"
                )
            raise AgyHarvestError(
                f"'agy models' hung on {self.box!r} and hit the in-box "
                f'{self.poke_timeout_s:.0f}s timeout. What agy printed '
                f'before stalling: {detail(tail)}'
            )
        if marker == 'AGY_POKE_FAIL':
            if _looks_like_relogin(tail):
                raise AgyReloginNeeded(
                    f'trusted box {self.box!r} can no longer refresh — its '
                    f"refresh token is revoked/expired; run 'agy /login' on "
                    f'it (details: {detail(tail)})'
                )
            raise AgyHarvestError(
                f'agy models failed on {self.box!r}: {detail(tail)}'
            )
        raise AgyHarvestError(
            f'unrecognized poke output from {self.box!r}: {detail(tail)}'
        )

    def update_secret(self, token: str) -> None:
        """
        Write *token* into the swap secret's value via ``set-custom``.

        The token is passed on **stdin** (never argv). The fixed
        placeholder makes this update the existing secret in place.

        :param token: Real access token to install as the swap value.
        :raises AgyHarvestError: If ``set-custom`` exits non-zero.
        """
        argv = set_custom_argv(
            placeholder=self.placeholder, env=self.env, hosts=self.hosts
        )
        try:
            proc = self.run(
                argv, input=token, capture_output=True, text=True,
            )
        except OSError as exc:
            raise AgyHarvestError(
                f'could not exec sbx secret set-custom: {exc}'
            ) from exc
        if proc.returncode != 0:
            raise AgyHarvestError(
                f'sbx secret set-custom failed (rc={proc.returncode}): '
                f'{detail(proc.stderr or "")}'
            )

    def cycle_once(self) -> str:
        """
        Run one full harvest cycle: poke → update secret → stamp.

        The stamp (:func:`record_harvest`) records WHEN the secret last
        held a fresh token, so a pipeline run can preflight it — the
        secret is write-only, so this is the only host-side signal.

        :returns: The redacted fingerprint of the token just installed
            (for the caller to log).
        :raises AgyHarvestError: Propagated from :meth:`poke` /
            :meth:`update_secret` (including :class:`AgyReloginNeeded`).
        """
        token = self.poke()
        self.update_secret(token)
        fingerprint = redact(token)
        record_harvest(fingerprint, path=self.stamp_path)
        return fingerprint

    def run_forever(self, *, max_cycles: int | None = None) -> None:
        """
        Loop the harvest cycle on cadence, with backoff on failure.

        On success: log the redacted fingerprint and sleep
        ``interval_s``. On :class:`AgyReloginNeeded`: log a loud,
        human-actionable message and back off (a human must intervene;
        the loop keeps probing so it recovers automatically once they
        re-login). On any other :class:`AgyHarvestError`: log and back
        off geometrically (floor→ceiling).

        :param max_cycles: Stop after this many loop iterations (any
            outcome). ``None`` runs until interrupted — the production
            mode. Bounded runs are for tests/one-shots.
        """
        backoff = BACKOFF_FLOOR_S
        cycles = 0
        while max_cycles is None or cycles < max_cycles:
            cycles += 1
            try:
                fingerprint = self.cycle_once()
            except AgyReloginNeeded as exc:
                self.echo(f'[agy-harvest] RE-LOGIN REQUIRED: {exc}')
                self.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_CEIL_S)
                continue
            except AgyHarvestError as exc:
                self.echo(f'[agy-harvest] cycle failed: {exc}')
                self.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_CEIL_S)
                continue
            backoff = BACKOFF_FLOOR_S
            self.echo(
                f'[agy-harvest] refreshed swap secret -> {fingerprint}; '
                f'next in {self.interval_s:.0f}s'
            )
            self.sleep(self.interval_s)


def _run_local(argv: list[str], *, action: str) -> None:
    """
    Run a local ``sbx`` management command, raising on failure.

    :param argv: Full argv.
    :param action: Human phrase for the error message.
    :raises click.ClickException: On a non-zero exit or exec failure.
    """
    try:
        proc = subprocess.run(argv, capture_output=True, text=True)
    except OSError as exc:
        raise click.ClickException(f'failed to {action}: {exc}') from exc
    if proc.returncode != 0:
        raise click.ClickException(
            f'failed to {action} (rc={proc.returncode}): '
            f'{proc.stderr.strip()}'
        )


@click.group()
def cli() -> None:
    """Manage the trusted auth-agy box and its token harvester."""


@cli.command('harvest')
@click.option(
    '--box', default=TRUSTED_BOX_DEFAULT, show_default=True,
    help='Trusted auth-agy sandbox name.',
)
@click.option(
    '--placeholder', default=PLACEHOLDER_TOKEN,
    help='Fixed swap placeholder (must match agy agent VM token files).',
)
@click.option('--env', default=SWAP_ENV, show_default=True)
@click.option(
    '--host', 'hosts', multiple=True,
    help=(
        'Swap secret host scope (repeatable); default: '
        f'{", ".join(SWAP_HOSTS)}.'
    ),
)
@click.option(
    '--interval', 'interval_s', type=float, default=DEFAULT_INTERVAL_S,
    show_default=True, help='Seconds between successful refreshes.',
)
@click.option(
    '--poke-timeout', 'poke_timeout_s', type=float,
    default=DEFAULT_POKE_TIMEOUT_S, show_default=True,
)
@click.option(
    '--once', is_flag=True,
    help='Run a single cycle then exit (verification/cron mode).',
)
def harvest(
    box: str, placeholder: str, env: str, hosts: tuple[str, ...],
    interval_s: float, poke_timeout_s: float, once: bool,
) -> None:
    """
    Keep the swap secret's token fresh (always-on loop, or ``--once``).

    Never prints the token — only a redacted fingerprint.
    """
    lock = acquire_harvest_lock()
    if lock is None:
        raise click.ClickException(
            f'another agy harvester already holds {HARVEST_LOCK}. Only '
            'one may poke the trusted box: concurrent pokes race on its '
            'token file. Stop that one first, or leave it running — it '
            'is already keeping the swap secret fresh.'
        )
    harvester = Harvester(
        box=box,
        placeholder=placeholder,
        env=env,
        hosts=tuple(hosts) or SWAP_HOSTS,
        interval_s=interval_s,
        poke_timeout_s=poke_timeout_s,
    )
    try:
        if once:
            try:
                fingerprint = harvester.cycle_once()
            except AgyHarvestError as exc:
                raise click.ClickException(str(exc)) from exc
            click.echo(
                f'[agy-harvest] refreshed swap secret -> {fingerprint}'
            )
            return
        harvester.run_forever()
    finally:
        # Explicit: leaving this to refcounting would hold the lock for
        # as long as any traceback keeps this frame alive.
        lock.close()


@cli.command('bootstrap')
@click.option(
    '--box', default=TRUSTED_BOX_DEFAULT, show_default=True,
    help='Trusted auth-agy sandbox name to create/scope.',
)
@click.option(
    '--image', default=None,
    help='Template image carrying the agy CLI (default: the host image).',
)
@click.option(
    '--workspace', default=None,
    help='Empty throwaway workspace dir (default: /tmp/<box>-ws).',
)
@click.option(
    '--skip-create', is_flag=True,
    help='Assume the box exists; only (re)apply egress + swap secret.',
)
def bootstrap(
    box: str, image: str | None, workspace: str | None, skip_create: bool,
) -> None:
    """
    Stand up the locked-down trusted box and guide the one-time login.

    Creates an isolated ``sbx`` sandbox (empty workspace, not an
    Omnigent host), applies its login-capable egress allowlist, seeds
    the swap secret with the inert placeholder, and prints the one-time
    ``agy /login`` steps. Browser login is interactive — this cannot
    complete it, only prepare for it.
    """
    if image is None:
        from sbx_omnigent.launcher import DEFAULT_HOST_IMAGE  # noqa: PLC0415

        image = DEFAULT_HOST_IMAGE
    workspace = workspace or f'/tmp/{box}-ws'

    if not skip_create:
        _run_local(['mkdir', '-p', workspace], action=f'create {workspace!r}')
        _run_local(
            [
                'sbx', 'create', 'shell', workspace,
                '--template', image, '--name', box, '--quiet',
            ],
            action=f'create trusted box {box!r}',
        )
    _run_local(
        [
            'sbx', 'policy', 'allow', 'network', '--sandbox', box,
            ','.join(TRUSTED_BOX_LOGIN_EGRESS),
        ],
        action=f'apply login egress allowlist to {box!r}',
    )
    # Seed the swap secret with the inert placeholder value so the entry
    # exists (harvest then updates its value). Placeholder-as-value is
    # harmless: no agent can authenticate until harvest installs a real
    # token, which is the intended gate.
    Harvester(box=box).update_secret(PLACEHOLDER_TOKEN)

    click.echo(
        '\n'.join(
            [
                f'[agy-bootstrap] trusted box {box!r} ready (image {image}).',
                f'  egress: {", ".join(TRUSTED_BOX_LOGIN_EGRESS)}',
                '  ONE-TIME LOGIN (interactive, opens a browser flow):',
                f'      sbx exec -it {box} agy',
                "      then run '/login' in agy and complete the browser "
                'consent.',
                '  After login succeeds, start the harvester:',
                f'      omni-sbx-agy harvest --box {box}',
                '  (Optional) tighten egress to the steady-state set:',
                f'      sbx policy allow network --sandbox {box} '
                f'{",".join(TRUSTED_BOX_EGRESS)}',
            ]
        )
    )


def main() -> None:
    """Console-script entry point for ``omni-sbx-agy``."""
    cli()


if __name__ == '__main__':
    main()
