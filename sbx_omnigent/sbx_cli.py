"""Bounded ``sbx`` CLI calls, and the error a stuck daemon raises.

When the sbx daemon stops answering, an unbounded ``sbx`` call waits
forever. The run did not fail with anything naming sbx: ``sbx create``
never returned, and a pipeline turn failed against the Omnigent server
with ``runner_unavailable`` instead (#28). Every ``sbx`` call now runs
under a budget from one of the tiers below, and a call that exceeds it
raises :class:`SbxNotResponding`. That error says the daemon is the
problem and how to recover it.

This module only notices and names. It never stops, kills or restarts
anything: a daemon restart on a live host is the operator's decision.

No ``sbx create`` duration has been measured, so the tiers are sized
against Omnigent's other sandbox providers, which allow 5-15 minutes
for provisioning and for commands.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence

import click

#: ``sbx ls``: answers at once from a healthy daemon.
PROBE_TIMEOUT_S = 20.0

#: ``policy``, ``rm``, ``secret`` and the in-VM seed programs.
MANAGE_TIMEOUT_S = 120.0

#: ``sbx create``, which may pull its template the first time.
CREATE_TIMEOUT_S = 300.0

#: A remote command Omnigent hands the launcher, e.g. a ``git clone``.
COMMAND_TIMEOUT_S = 900.0

#: Checked against ``sbx daemon --help``, ``pgrep`` and ``ps`` on macOS
#: and Linux. ``sbx daemon stop`` has been seen hanging on a lingering
#: shim, and a restart also runs containerd's GC, which reclaims guest
#: disks that VMs leaked when they died abnormally.
REMEDY = (
    'Nothing was stopped or restarted. To recover the daemon:\n'
    '  1. Check it:  sbx daemon status\n'
    '  2. Look for a lingering shim:  '
    'pgrep -lf containerd-shim-nerdbox-v1\n'
    '     Before stopping the daemon, confirm each one is idle: '
    'zero CPU in\n'
    '     `ps -o pid,pcpu,etime -p <pid>`, and no children from '
    '`pgrep -P <pid>`.\n'
    '  3. Restart it:  sbx daemon stop && sbx daemon start\n'
    '     If the stop hangs on an idle shim, stop that shim '
    '(`kill <pid>`) and retry.'
)

#: A ``subprocess.run``-shaped callable.
Runner = Callable[..., subprocess.CompletedProcess[str]]


def not_responding_message(argv: Sequence[str], timeout_s: float) -> str:
    """
    Say that *argv* hung, that the daemon is why, and how to recover.

    For callers that report through their own error type.

    :param argv: The command that timed out.
    :param timeout_s: The budget it exceeded.
    :returns: The message, remedy included.
    """
    return (
        f'`{_subcommand(argv)}` did not finish within '
        f'{timeout_s:.0f} s: the sbx daemon is not responding.\n'
        f'{REMEDY}'
    )


class SbxNotResponding(click.ClickException):
    """An ``sbx`` call did not finish inside its budget."""

    def __init__(self, argv: Sequence[str], timeout_s: float) -> None:
        """
        :param argv: The command that timed out.
        :param timeout_s: The budget it exceeded.
        """
        super().__init__(not_responding_message(argv, timeout_s))


def _subcommand(argv: Sequence[str]) -> str:
    """
    The ``sbx <verb>`` part of *argv*, e.g. ``"sbx create"``.

    Only the verb is named. The rest is caller data (a sandbox name, a
    workspace path, an in-VM program) that has no place in an error.

    :param argv: An ``sbx`` argv.
    :returns: ``"sbx <verb>"``, or ``"sbx"`` for a bare call.
    """
    return ' '.join(argv[:2])


def run(
    argv: Sequence[str],
    *,
    timeout_s: float,
    input_text: str | None = None,
    runner: Runner | None = None,
) -> subprocess.CompletedProcess[str]:
    """
    Run an ``sbx`` command under a budget.

    A non-zero exit is returned, not raised: every caller already words
    its own failure. Only a hang is new, and it raises.

    :param argv: The full argv; ``argv[0]`` must be ``"sbx"``.
    :param timeout_s: Budget in seconds, from one of this module's
        tiers.
    :param input_text: Text piped to the command's stdin, if any.
    :param runner: Subprocess runner; ``None`` means ``subprocess.run``,
        looked up at call time so a caller's test can patch it.
    :returns: The finished process.
    :raises ValueError: If *argv* is not an ``sbx`` command, or the
        budget is not positive.
    :raises SbxNotResponding: If the command does not finish in time.
    :raises OSError: If ``sbx`` cannot be executed.
    """
    command = list(argv)
    if not command or command[0] != 'sbx':
        raise ValueError(f'not an sbx command: {command!r}')
    if timeout_s <= 0:
        raise ValueError(f'timeout_s must be positive, got {timeout_s!r}')
    call = runner if runner is not None else subprocess.run
    try:
        if input_text is None:
            return call(
                command, capture_output=True, text=True, timeout=timeout_s
            )
        return call(
            command,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        raise SbxNotResponding(command, timeout_s) from exc


def require_responsive(
    *,
    runner: Runner | None = None,
    timeout_s: float = PROBE_TIMEOUT_S,
) -> None:
    """
    Refuse unless ``sbx ls`` answers inside the probe budget.

    :param runner: Subprocess runner; ``None`` means ``subprocess.run``,
        looked up at call time.
    :param timeout_s: Budget for the listing.
    :raises SbxNotResponding: If the daemon does not answer.
    :raises click.ClickException: If ``sbx`` cannot be run, or the
        listing fails (e.g. not signed in).
    """
    argv = ['sbx', 'ls']
    call = runner if runner is not None else subprocess.run
    try:
        proc = call(
            argv,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        raise SbxNotResponding(argv, timeout_s) from exc
    except OSError as exc:
        raise click.ClickException(f'cannot run `sbx`: {exc}') from exc
    if proc.returncode != 0:
        raise click.ClickException(
            f'`sbx ls` failed (rc={proc.returncode}): '
            f'{(proc.stderr or "").strip()}\n'
            'If sbx is not signed in, run `sbx login` as this user.'
        )
