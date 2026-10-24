"""
The server's resolved egress allowlist, published for the runner.

The pipeline runner is a separate process from the server and never saw
its config, so the verify gate used the built-in default allowlist
whatever the operator had set, including ``[]`` (#41). The server now
writes its resolved ``sandbox.sbx.egress_allow`` to :data:`POLICY_FILE`
at the top of ``sbx.worktree_root``, which the runner's
``--worktree-root`` must already equal. The root itself is never
mounted into a VM; only directories under it are.

A pipeline's declared ``egress:`` hosts are checked against that list
before any turn is spent (:func:`check_declared_hosts`). sbx's own
authorizer decides, in a short-lived sandbox scoped like the gate,
because global sbx rules also grant reachability and its pattern
language is not one to re-implement here.
"""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass

import click

from sbx_omnigent import sbx_cli
from sbx_omnigent.pipeline import _sanitize

#: The file the server publishes its allowlist to, in ``worktree_root``.
POLICY_FILE = '.sbx-omnigent-egress.json'

#: Bumped when the file's shape changes, so an older reader refuses it.
_POLICY_VERSION = 1

#: Longest check-sandbox name, the same cap as the gate's.
_NAME_MAX = 56


class PolicyError(Exception):
    """The published allowlist is missing or unreadable."""


class EgressCheckError(Exception):
    """The declared hosts could not be checked at all."""


@dataclass(frozen=True)
class HostVerdict:
    """
    What sbx's authorizer says about one declared host.

    :param host: The declared host.
    :param allowed: Whether a VM scoped like the gate may reach it.
    :param reason: sbx's reason for a deny; empty when allowed.
    """

    host: str
    allowed: bool
    reason: str


def _check_listable(entries: object) -> tuple[str, ...]:
    """
    Check an allowlist can be joined with commas for ``sbx``.

    :param entries: The list to check.
    :returns: It, as a tuple.
    :raises ValueError: If it is not a list of non-empty strings free
        of commas and whitespace.
    """
    if not isinstance(entries, (list, tuple)):
        raise ValueError('the allowlist must be a list')
    for entry in entries:
        if (
            not isinstance(entry, str)
            or not entry
            or ',' in entry
            or any(ch.isspace() for ch in entry)
        ):
            raise ValueError(f'unusable allowlist entry {entry!r}')
    return tuple(entries)


def publish_policy(worktree_root: str, allowlist: tuple[str, ...]) -> None:
    """
    Write the server's resolved allowlist for the runner to read.

    Written to a temporary file and renamed into place, so a runner
    never reads it half-written. Readable by the owner only.

    :param worktree_root: The server's ``sbx.worktree_root``; created if
        it does not exist yet.
    :param allowlist: The resolved per-VM allowlist, dial-back excluded.
    :raises ValueError: If an entry would split when joined.
    :raises OSError: If the file cannot be written.
    """
    entries = list(_check_listable(allowlist))
    os.makedirs(worktree_root, exist_ok=True)
    fd, staged = tempfile.mkstemp(dir=worktree_root, prefix=f'{POLICY_FILE}.')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            json.dump(
                {'version': _POLICY_VERSION, 'egress_allow': entries}, fh
            )
        os.replace(staged, os.path.join(worktree_root, POLICY_FILE))
    except BaseException:
        if os.path.exists(staged):
            os.unlink(staged)
        raise


def read_policy(worktree_root: str) -> tuple[str, ...]:
    """
    Read the allowlist the server published at startup.

    :param worktree_root: The runner's ``--worktree-root``.
    :returns: The operator's resolved allowlist; empty for ``[]``.
    :raises PolicyError: If the file is missing or malformed.
    """
    path = os.path.join(worktree_root, POLICY_FILE)
    try:
        with open(path, encoding='utf-8') as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        raise PolicyError(
            f'the server has not published its egress allowlist to '
            f'{path}. Restart the server (omni-sbx server) so it writes '
            f"one, and check that 'sandbox.sbx.worktree_root' in its "
            f'config is the same directory as --worktree-root.'
        ) from None
    except (OSError, ValueError) as exc:
        raise PolicyError(f'cannot read {path}: {exc}') from exc
    if not isinstance(raw, dict) or raw.get('version') != _POLICY_VERSION:
        raise PolicyError(
            f'{path} is not an allowlist this runner can read; restart '
            f'the server so it writes a current one.'
        )
    try:
        return _check_listable(raw.get('egress_allow'))
    except ValueError as exc:
        raise PolicyError(f'{path}: {exc}') from exc


def check_box_name(run_id: str) -> str:
    """
    A unique sbx name for one run's egress check sandbox.

    Random-suffixed, so two runs of one pipeline never collide and a
    box a killed run left behind never blocks the next.

    :param run_id: The run id.
    :returns: e.g. ``"egress-check-my-pipeline-3fa2c1"``.
    """
    suffix = secrets.token_hex(3)
    stem = f'egress-check-{_sanitize(run_id)}'
    return f'{stem[: _NAME_MAX - len(suffix) - 1].rstrip("-")}-{suffix}'


def _sbx(
    argv: list[str],
    *,
    timeout_s: float,
    failure: str,
    runner: sbx_cli.Runner | None,
    require_ok: bool = True,
) -> subprocess.CompletedProcess[str]:
    """
    Run one ``sbx`` call for the check, or raise.

    :param argv: The ``sbx`` argv.
    :param timeout_s: Budget, from one of :mod:`sbx_cli`'s tiers.
    :param failure: What could not be done; the error starts with it.
    :param runner: Subprocess runner (injected in tests).
    :param require_ok: Raise on a non-zero exit too.
    :returns: The finished process.
    :raises EgressCheckError: If it hangs, cannot run, or (with
        *require_ok*) exits non-zero.
    """
    try:
        proc = sbx_cli.run(argv, timeout_s=timeout_s, runner=runner)
    except (sbx_cli.SbxNotResponding, OSError) as exc:
        raise EgressCheckError(f'{failure}: {exc}') from exc
    if require_ok and proc.returncode != 0:
        raise EgressCheckError(
            f'{failure} (rc={proc.returncode}): '
            f'{(proc.stderr or "").strip()}'
        )
    return proc


def _verdict(
    host: str, proc: subprocess.CompletedProcess[str]
) -> HostVerdict:
    """
    Read ``sbx policy check network --json`` output for *host*.

    :param host: The declared host.
    :param proc: The finished check; exit 1 is a deny, not a failure.
    :returns: The verdict.
    :raises EgressCheckError: If the output carries no verdict, so a
        404 or a changed format never reads as a pass.
    """
    try:
        raw: object = json.loads(proc.stdout or '')
    except ValueError:
        raw = None
    allowed = raw.get('allowed') if isinstance(raw, dict) else None
    if not isinstance(raw, dict) or not isinstance(allowed, bool):
        raise EgressCheckError(
            f'sbx gave no verdict for {host!r} (rc={proc.returncode}): '
            f'{(proc.stderr or proc.stdout or "").strip()}'
        )
    reason = '' if allowed else str(raw.get('reason') or 'denied')
    return HostVerdict(host, allowed, reason)


def _remove(name: str, runner: sbx_cli.Runner | None) -> None:
    """
    Remove the check sandbox, saying so if it cannot be removed.

    :param name: The sandbox.
    :param runner: Subprocess runner (injected in tests).
    """
    argv = ['sbx', 'rm', '--force', name]
    try:
        proc = sbx_cli.run(
            argv, timeout_s=sbx_cli.MANAGE_TIMEOUT_S, runner=runner
        )
        problem = (
            f'rc={proc.returncode}: {(proc.stderr or "").strip()}'
            if proc.returncode != 0 else None
        )
    except (sbx_cli.SbxNotResponding, OSError) as exc:
        problem = str(exc)
    if problem is not None:
        click.echo(
            f'[egress] WARNING: could not remove the check sandbox '
            f'{name!r} ({problem}). Remove it with: sbx rm --force {name}',
            err=True,
        )


def check_declared_hosts(
    *,
    name: str,
    hosts: Sequence[str],
    allowlist: Sequence[str],
    image: str,
    runner: sbx_cli.Runner | None = None,
) -> tuple[HostVerdict, ...]:
    """
    Ask sbx whether a VM scoped like the gate may reach each host.

    Creates a sandbox with no workspace mounted, gives it the same
    per-VM rules the gate gets, asks ``sbx policy check network`` about
    each host in that sandbox's context, then removes it. sbx needs the
    sandbox to exist to evaluate its rules. Agent VMs get the same list
    plus their dial-back and harness hosts, so a host allowed here is
    reachable from every VM in the run.

    :param name: The sandbox name, from :func:`check_box_name`.
    :param hosts: The pipeline's declared hosts.
    :param allowlist: The operator's resolved list; empty for ``[]``.
    :param image: Template image to create the sandbox from.
    :param runner: Subprocess runner (injected in tests).
    :returns: One verdict per host, in order.
    :raises EgressCheckError: If the sandbox cannot be created or
        scoped, or sbx gives no verdict for a host.
    """
    _sbx(
        [
            'sbx', 'create', 'shell', '--template', image,
            '--name', name, '--quiet', '--cpus', '1',
        ],
        timeout_s=sbx_cli.CREATE_TIMEOUT_S,
        failure=f'could not create the egress check sandbox {name!r}',
        runner=runner,
    )
    try:
        if allowlist:
            _sbx(
                [
                    'sbx', 'policy', 'allow', 'network',
                    '--sandbox', name, ','.join(allowlist),
                ],
                timeout_s=sbx_cli.MANAGE_TIMEOUT_S,
                failure=f'could not scope egress for {name!r}',
                runner=runner,
            )
        return tuple(
            _verdict(host, _sbx(
                [
                    'sbx', 'policy', 'check', 'network', '--json',
                    '--sandbox', name, host,
                ],
                timeout_s=sbx_cli.MANAGE_TIMEOUT_S,
                failure=f'could not check {host!r}',
                runner=runner,
                require_ok=False,
            ))
            for host in hosts
        )
    finally:
        _remove(name, runner)
