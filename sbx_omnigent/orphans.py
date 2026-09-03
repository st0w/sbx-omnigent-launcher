"""Find guest disk images sbx has leaked, so a refusal can name them.

sbx leaks a microVM's read-write layer when the VM's tunnel drops
ABNORMALLY; a VM disposed cleanly does not leak. Nothing in `sbx ls`,
`sbx rm` or any session record shows the leftover — it is a file in
containerd's snapshotter store and only a daemon GC reclaims it. One
was measured holding **11 GB for twelve days** while `sbx ls` reported
two sandboxes and no managed VMs (TASKS.md #7).

That matters here because it is invisible to every reclaim the launcher
can do: the sessions are gone, the worktrees are gone, and the disk is
still full. A preflight that refuses a run without mentioning it sends
someone hunting through the wrong things — it took a filesystem
archaeology session to find the first one.

So this module only DETECTS and NAMES. It never deletes: hand-removing a
snapshot directory leaves a dangling row in the snapshotter's
``metadata.db``, which is corruption rather than cleanup. The supported
reclaim is a daemon restart (`sbx daemon` is a hidden subcommand), which
runs containerd's GC — and doing that automatically belongs with the
wedged-daemon work in TASKS.md #24, not in a disk preflight.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

#: Override for the snapshotter store, for a layout neither candidate
#: below matches. Set it rather than patching a path in.
ROOT_ENV_VAR = 'SBX_SNAPSHOT_ROOT'

#: Where sbx's containerd keeps per-sandbox snapshots. macOS first
#: (where this was found), then the XDG layout Linux would use — this
#: is heading for Linux (see docs/CLOUD.md), so the path must not be
#: hard-coded to one OS.
_SNAPSHOT_SUFFIX = Path(
    'sandboxes/sandboxd/containerd/root'
    '/io.containerd.snapshotter.v1.erofs/snapshots'
)
_ROOT_CANDIDATES = (
    Path('Library/Application Support/com.docker.sandboxes'),
    Path('.local/share/com.docker.sandboxes'),
    Path('.docker/sandboxes'),
)

#: The per-sandbox writable disk image inside a snapshot directory.
_LAYER_NAME = 'rwlayer.img'


def snapshot_root(
    home: Path | None = None, env: dict[str, str] | None = None
) -> Path | None:
    """
    The snapshotter's snapshots directory, or ``None`` if not found.

    :param home: Home directory (injected in tests).
    :param env: Environment (injected in tests).
    :returns: The directory, or ``None`` when no candidate exists.
    """
    environ = os.environ if env is None else env
    override = environ.get(ROOT_ENV_VAR)
    if override:
        path = Path(override)
        return path if path.is_dir() else None
    base = home or Path.home()
    for candidate in _ROOT_CANDIDATES:
        path = base / candidate / _SNAPSHOT_SUFFIX
        if path.is_dir():
            return path
    return None


def layer_bytes(root: Path | None = None) -> tuple[int, int]:
    """
    How many guest disk images exist, and how much they ALLOCATE.

    Allocated, not apparent: these files are sparse — one measured
    21,474,836,480 bytes apparent against 11 GB actually on disk — and
    the apparent figure would wildly overstate what a GC could return.

    :param root: Snapshots directory; ``None`` resolves it.
    :returns: ``(count, allocated_bytes)``; ``(0, 0)`` when unavailable.
    """
    base = root or snapshot_root()
    if base is None:
        return 0, 0
    count = 0
    total = 0
    try:
        for child in base.iterdir():
            layer = child / _LAYER_NAME
            try:
                stat = layer.stat()
            except OSError:
                continue
            count += 1
            total += getattr(stat, 'st_blocks', 0) * 512
    except OSError:
        return 0, 0
    return count, total


def live_sandboxes(
    run: object = subprocess.run, timeout_s: float = 20.0
) -> int | None:
    """
    How many sandboxes sbx currently lists, or ``None``.

    :param run: Subprocess runner (injected in tests).
    :param timeout_s: Budget for the call.
    :returns: The count, or ``None`` when sbx could not be asked — which
        must read as "unknown", never as "zero", or every layer would
        look orphaned.
    """
    try:
        proc = run(  # type: ignore[operator]
            ['sbx', 'ls'],
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout_s,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if getattr(proc, 'returncode', 1) != 0:
        return None
    lines = [
        ln for ln in (getattr(proc, 'stdout', '') or '').splitlines()
        if ln.strip()
    ]
    return max(0, len(lines) - 1)  # drop the header row


def orphan_advice(
    root: Path | None = None, run: object = subprocess.run
) -> str | None:
    """
    One sentence naming leaked guest disks, or ``None``.

    Deliberately reports the COUNT of excess layers and the total the
    store holds, and does not claim to know how many bytes the orphans
    are: a layer cannot be mapped back to its sandbox from the outside,
    so any per-orphan figure would be invented.

    :param root: Snapshots directory; ``None`` resolves it.
    :param run: Subprocess runner (injected in tests).
    :returns: The advice, or ``None`` when there is nothing to say.
    """
    count, total = layer_bytes(root)
    if not count:
        return None
    live = live_sandboxes(run)
    if live is None or count <= live:
        return None
    gb = 1_000_000_000
    excess = count - live
    return (
        f'Note: {excess} leaked guest disk image(s) — {count} exist for '
        f'{live} live sandbox(es), and the store holds '
        f'{total / gb:.1f} GB. '
        "sbx leaks a VM's read-write layer when "
        f'its tunnel drops abnormally, and nothing in `sbx ls` or '
        f'`sbx rm` shows them; one held 11 GB for twelve days. A daemon '
        'restart runs containerd GC and reclaims them: '
        f'`sbx daemon stop && sbx daemon start`, with no sandboxes '
        f'running. Never delete a snapshot directory by hand — it leaves '
        f'a dangling metadata.db row.'
    )
