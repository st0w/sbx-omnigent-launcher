"""Read a guest's runner log for the reason a turn failed.

A harness whose credential is dead does not fail as a credential
problem. On 2026-09-14 a Codex agent's first turn failed as "Codex
app-server never started a thread (startup timed out)", with no pane to
read, while the runner log inside the VM already held the answer: a
``401 Unauthorized`` and "could not be refreshed" (#26). This reads the
tail of that log once a turn has failed, and names an authentication
failure if it finds one.

Like :mod:`sbx_omnigent.pane`, it runs on a path where a turn has
already failed, so it never raises: every failure to read collapses to
``None``.

Only a fixed phrase from :data:`AUTH_SIGNALS` is ever reported, never a
log line: a line can carry a token.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable

from sbx_omnigent import agy, claude, codex

#: Where Omnigent's runner writes its log inside a guest. A relaunched
#: runner starts a new file, so the newest is read.
RUNNER_LOG_GLOB = '~/.omnigent/logs/runner/runner-*.log'

#: How much of the newest runner log to read.
DEFAULT_LINES = 400

#: Budget for the ``sbx exec`` round trip.
DEFAULT_TIMEOUT_S = 30.0

#: Lower-case phrases that mean a harness's credential was rejected.
#: The first four were seen live (#26); the last two are the standard
#: OAuth and Anthropic API error codes for the same thing. "login" and
#: "logged in" alone are deliberately absent: the routing line printed
#: beside the 9/14 failure used both to say the login was fine.
AUTH_SIGNALS: tuple[str, ...] = (
    'could not be refreshed',
    'refresh token has expired',
    '401 unauthorized',
    'please sign in',
    'invalid_grant',
    'authentication_error',
)


def tail_command(sandbox: str, *, lines: int = DEFAULT_LINES) -> list[str]:
    """
    The ``sbx exec`` argv that prints the newest runner log's tail.

    Exits 0 when there is no log, so a missing log reads as empty
    output rather than as a failed command.

    :param sandbox: The microVM name, e.g. ``"managed-cb683c32"``.
    :param lines: How many lines to print.
    :returns: The argv.
    """
    script = (
        f'F=$(ls -t {RUNNER_LOG_GLOB} 2>/dev/null | head -1); '
        f'[ -n "$F" ] && tail -n {int(lines)} "$F"; exit 0'
    )
    return ['sbx', 'exec', sandbox, '--', 'sh', '-lc', script]


def read_runner_log(
    sandbox: str,
    *,
    lines: int = DEFAULT_LINES,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> str | None:
    """
    The tail of *sandbox*'s newest runner log, or ``None``.

    NEVER raises: a hang, a missing ``sbx``, a failed exec and an empty
    log all collapse to ``None``.

    :param sandbox: The microVM name.
    :param lines: How many lines to read.
    :param timeout_s: Whole round-trip budget.
    :param run: Subprocess runner; ``None`` means ``subprocess.run``,
        looked up at call time so tests can patch it.
    :returns: The log tail, or ``None``.
    """
    call = run if run is not None else subprocess.run
    try:
        proc = call(
            tail_command(sandbox, lines=lines),
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout_s,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    out = (proc.stdout or '').strip('\n')
    return out if out.strip() else None


def auth_failure(text: str | None) -> str | None:
    """
    The authentication-failure phrase *text* contains, if any.

    :param text: A runner log tail, or ``None``.
    :returns: The matching phrase from :data:`AUTH_SIGNALS`, never text
        taken from the log, or ``None``.
    """
    if not text:
        return None
    low = text.lower()
    return next((s for s in AUTH_SIGNALS if s in low), None)


def relogin_hint(harness: str) -> str:
    """
    How to renew the credential a harness runs on.

    :param harness: The agent's harness id, e.g. ``"codex-native"``.
    :returns: The remedy, naming the command to run.
    """
    if harness in codex.CODEX_HARNESSES:
        return f'on this host, run `{codex.RELOGIN_HINT}`'
    if harness in agy.AGY_HARNESSES:
        return (
            f'run `sbx exec -it {agy.TRUSTED_BOX_DEFAULT} agy` and '
            f'`/login`; the harvester picks it up on its next cycle'
        )
    if harness in claude.CLAUDE_NATIVE_HARNESSES:
        return (
            'mint a new token with `claude setup-token` and replace the '
            'sbx secret that carries it (README, Credentials)'
        )
    return 'renew the credential this harness uses, then retry'
