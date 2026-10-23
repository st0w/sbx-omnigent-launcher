"""
The server's resolved egress allowlist, published for the runner.

The pipeline runner is a separate process from the server and never saw
its config, so the verify gate used the built-in default allowlist
whatever the operator had set, including ``[]`` (#41). The server now
writes its resolved ``sandbox.sbx.egress_allow`` to :data:`POLICY_FILE`
at the top of ``sbx.worktree_root``, which the runner's
``--worktree-root`` must already equal. The root itself is never
mounted into a VM; only directories under it are.
"""

from __future__ import annotations

import json
import os
import tempfile

#: The file the server publishes its allowlist to, in ``worktree_root``.
POLICY_FILE = '.sbx-omnigent-egress.json'

#: Bumped when the file's shape changes, so an older reader refuses it.
_POLICY_VERSION = 1


class PolicyError(Exception):
    """The published allowlist is missing or unreadable."""


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
