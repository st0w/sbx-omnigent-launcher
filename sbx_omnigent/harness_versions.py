"""Read which harness CLI versions an agent VM is actually running.

``claude``, ``codex`` and ``agy`` are unpinned in the host image and
update themselves inside the VM, including mid-session. Each silent
change in one has cost this project a day (docs/HARNESS-VERSIONS.md),
and nothing recorded which versions a run had used, so a wedged run
could not be compared against the known-good set: the versions were
gone with the VM (#17).

This module reads the set with one ``sbx exec`` and never raises. The
runner records what it reads in the run state, and warns when a
version differs from :data:`KNOWN_GOOD` or from an earlier session in
the same run.

``--version`` reports the INSTALLED CLI. After an in-VM update that is
newer than the process still running in the pane, which is why
:func:`pending_self_update` exists.
"""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable, Mapping

from sbx_omnigent import agy, codex

#: The CLIs read, in the order they are reported.
CLIS: tuple[str, ...] = ('claude', 'codex', 'agy')

#: The known-good set from docs/HARNESS-VERSIONS.md. A test fails when
#: the two disagree, so update both together when a new image is
#: checked.
KNOWN_GOOD: Mapping[str, str] = {
    'claude': '2.1.266',
    'codex': '0.153.4',
    'agy': '1.1.16',
}

#: The date :data:`KNOWN_GOOD` was recorded.
KNOWN_GOOD_RECORDED = '2026-09-14'

#: Wall clock for the whole round-trip. ``sbx exec`` may start a stopped
#: box, and three CLIs each start a runtime to print one line.
DEFAULT_TIMEOUT_S = 30.0

#: A version token: ``2.1.266``, ``0.153.4``, ``2.2.0-beta.1``.
_VERSION_RE = re.compile(r'\b\d+\.\d+\.\d+(?:-[0-9A-Za-z.]+)?')

#: Claude Code's notice that it installed an update the running process
#: has not picked up: ``✔ Update installed · Restart to apply``.
_UPDATE_PENDING_RE = re.compile(
    r'update installed\W{0,8}restart to apply', re.I
)


def version_script() -> str:
    """
    The shell program that prints one ``<cli>=<version line>`` per CLI.

    stderr goes to ``/dev/null``: codex prints a Node warning there that
    would otherwise land where its version line should be. A CLI that
    is missing prints an empty value.

    :returns: The shell source.
    """
    names = ' '.join(CLIS)
    return (
        f'for c in {names}; do '
        'printf \'%s=%s\\n\' "$c" '
        '"$("$c" --version 2>/dev/null | head -n 1)"; '
        'done'
    )


def version_command(sandbox: str) -> list[str]:
    """
    The ``sbx exec`` argv that reads *sandbox*'s CLI versions.

    A login shell, because the CLIs are on the profile's PATH.

    :param sandbox: The microVM name, e.g. ``"managed-cb683c32"``.
    :returns: The argv. *sandbox* is its own element and never reaches
        a shell.
    """
    return ['sbx', 'exec', sandbox, '--', 'sh', '-lc', version_script()]


def parse_versions(out: str) -> dict[str, str]:
    """
    Parse :func:`version_script` output into ``{cli: version}``.

    :param out: The script's stdout.
    :returns: The CLIs whose line carried a version. A missing CLI, or a
        line with no version token, is left out rather than recorded as
        something a later comparison would report as a change.
    """
    versions: dict[str, str] = {}
    for line in out.splitlines():
        name, sep, rest = line.partition('=')
        if not sep or name.strip() not in CLIS:
            continue
        match = _VERSION_RE.search(rest)
        if match:
            versions[name.strip()] = match.group(0)
    return versions


def read_versions(
    sandbox: str,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    run: Callable[..., object] = subprocess.run,
) -> dict[str, str]:
    """
    Read *sandbox*'s harness CLI versions; never raises.

    A failed exec, a timeout or a missing ``sbx`` all come back as no
    versions. This is a record for later comparison, and it must never
    be what fails a run.

    :param sandbox: The microVM name.
    :param timeout_s: Whole round-trip budget.
    :param run: Subprocess runner (injected in tests).
    :returns: ``{cli: version}`` for each CLI that reported one.
    """
    try:
        proc = run(
            version_command(sandbox),
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout_s,
        )
    except (subprocess.TimeoutExpired, OSError):
        return {}
    if getattr(proc, 'returncode', 1) != 0:
        return {}
    return parse_versions(getattr(proc, 'stdout', '') or '')


def cli_for_harness(harness: str) -> str:
    """
    The CLI a harness drives.

    Anything that is not agy or codex is Claude, matching
    :func:`sbx_omnigent.readback.launch_mismatches`.

    :param harness: The agent's harness id.
    :returns: One of :data:`CLIS`.
    """
    if harness in agy.AGY_HARNESSES:
        return 'agy'
    if harness in codex.CODEX_HARNESSES:
        return 'codex'
    return 'claude'


def format_versions(versions: Mapping[str, str]) -> str:
    """
    One line naming each version, in :data:`CLIS` order.

    :param versions: ``{cli: version}``.
    :returns: e.g. ``"claude 2.1.266, agy 1.1.16"``.
    """
    parts = [f'{cli} {versions[cli]}' for cli in CLIS if cli in versions]
    return ', '.join(parts) or 'none could be read'


def pending_self_update(pane: str | None) -> bool:
    """
    Whether a pane shows an installed update waiting on a restart.

    When it does, the version read from the VM is newer than the process
    running in the pane.

    :param pane: Captured pane text.
    :returns: ``True`` when the update notice is on screen.
    """
    return bool(pane) and bool(_UPDATE_PENDING_RE.search(pane or ''))
