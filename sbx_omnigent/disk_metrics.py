"""Record what a run ACTUALLY costs on disk, instead of estimating it.

Every disk figure this project has is either a one-off measurement
someone took by hand during an incident, or an inference from one. That
has now produced three open questions it cannot answer:

* ``per_worktree_gb`` was 2.0 against writers measured at 2.2-26 GB
  (TASKS.md #6). The 26 GB observation does not say WHICH node produced
  it, so the spread cannot be attributed.
* whether reclaiming a superseded node's build output mid-module is
  worth building (#7 item 2) depends entirely on that attribution: if
  the big trees are the two implementers, they die the instant the judge
  picks and reclaim is a large win; if they are the refactor and verify
  nodes, those two coexist at the end and it saves almost nothing.
* ``per_vm_gb`` models 3.5 GB per guest while the sbx store was observed
  at 29-35 GB for a 6-VM cadre — not directly comparable, because the
  store also holds base image layers shared across VMs, so the term
  needs its own measurement rather than an inference from that total.

One instrumented run answers all three. This samples every node's
worktree, the sbx snapshot store, and host free space at each stage
boundary, so the result is a TIME SERIES rather than a final tally —
which is the only shape that shows a concurrent PEAK, and the peak is
what the preflight is actually trying to predict.

OPT-IN, via :data:`ENABLE_ENV_VAR`. Measuring a 26 GB tree means walking
its inodes, and a full cadre has five of them across roughly eight stage
boundaries; that is minutes of wall clock bought for data nobody needs
on a routine run. Default-off keeps normal runs byte-identical.

Written OUTSIDE the run directory for the reason #30 exists: a completed
run deletes its own run dir, so a record kept there would survive only
the failures. Same home as the retained loser bundles (#32) —
``canonical_root``, which nothing in the launcher ever removes.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

#: Set to any non-empty value to record. See the module docstring for
#: why this is not on by default.
ENABLE_ENV_VAR = 'OMNI_SBX_DISK_METRICS'

#: Budget for one directory measurement. A 26 GB build tree is a lot of
#: inodes; a hung one must not hold the run.
DEFAULT_TIMEOUT_S = 120.0


def enabled(env: dict[str, str] | None = None) -> bool:
    """
    Whether disk recording is switched on.

    :param env: Environment (injected in tests).
    :returns: Whether to record.
    """
    return bool((os.environ if env is None else env).get(ENABLE_ENV_VAR))


def dir_bytes(
    path: str | Path,
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    run: object = subprocess.run,
) -> int | None:
    """
    Disk a directory occupies, in bytes, or ``None``.

    Uses ``du -skx``: ALLOCATED blocks (not apparent size, which a
    sparse file wildly overstates) and one filesystem only, so a
    bind-mounted guest disk cannot be counted into a host worktree.

    :param path: The directory to measure.
    :param timeout_s: Budget for the walk.
    :param run: Subprocess runner (injected in tests).
    :returns: Bytes, or ``None`` when it could not be measured.
    """
    try:
        proc = run(  # type: ignore[operator]
            ['du', '-skx', str(path)],
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout_s,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if getattr(proc, 'returncode', 1) != 0:
        return None
    first = (getattr(proc, 'stdout', '') or '').split('\t', 1)[0].strip()
    try:
        return int(first) * 1024
    except ValueError:
        return None


def node_dirs(run_dir: str | Path) -> list[Path]:
    """
    Every node worktree a run currently has on disk.

    From the FILESYSTEM, not from run state: the point of this module is
    to record what is actually there, and a crashed run can have the two
    disagree in either direction.

    :param run_dir: The run's directory.
    :returns: Node directories, sorted; empty when there are none.
    """
    try:
        return sorted(
            child for child in (Path(run_dir) / 'nodes').iterdir()
            if child.is_dir()
        )
    except OSError:
        return []


def sample(
    *,
    run_id: str,
    event: str,
    run_dir: str | Path,
    chunk: str | None = None,
    kinds: dict[str, str] | None = None,
    store_layers: tuple[int, int] | None = None,
    free_path: str | Path | None = None,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    now: float | None = None,
    measure: object = dir_bytes,
    usage: object = shutil.disk_usage,
) -> list[dict]:
    """
    One measurement of everything a run is holding right now.

    :param run_id: The run being measured.
    :param event: What triggered it, e.g. ``"stage-complete:impl-a"``.
        The series is only interpretable if this names the boundary.
    :param run_dir: The run's directory, holding ``nodes/``.
    :param chunk: Module id, when inside a campaign.
    :param kinds: ``{node: kind}`` so a reader can tell a writer's tree
        from a judge's without re-deriving the DAG.
    :param store_layers: ``(count, bytes)`` from
        :func:`sbx_omnigent.orphans.layer_bytes`, the guest-disk side of
        the model that ``per_vm_gb`` estimates.
    :param free_path: Filesystem to read free space from; ``None`` skips
        it.
    :param timeout_s: Per-directory budget.
    :param now: Clock override (injected in tests).
    :param measure: Directory measurer (injected in tests).
    :param usage: ``shutil.disk_usage``-alike (injected in tests).
    :returns: The records, oldest-relevant first. Never raises.
    """
    stamp = time.time() if now is None else now
    base = {'t': round(stamp, 3), 'run': run_id, 'event': event}
    if chunk is not None:
        base['chunk'] = chunk
    out: list[dict] = []
    for child in node_dirs(run_dir):
        size = measure(child, timeout_s=timeout_s)  # type: ignore[operator]
        if size is None:
            continue
        rec = {**base, 'what': 'worktree', 'node': child.name, 'bytes': size}
        if kinds and child.name in kinds:
            rec['kind'] = kinds[child.name]
        out.append(rec)
    if store_layers is not None:
        count, total = store_layers
        out.append(
            {**base, 'what': 'sbx-store', 'layers': count, 'bytes': total}
        )
    if free_path is not None:
        try:
            out.append(
                {**base, 'what': 'host-free',
                 'bytes': usage(free_path).free}  # type: ignore[operator]
            )
        except OSError:
            pass
    return out


def append(path: str | Path, records: list[dict]) -> bool:
    """
    Append records to the run's JSONL file, best-effort.

    Never raises: instrumentation that can fail a run is worse than no
    instrumentation. JSONL rather than one document so a killed run
    keeps every sample it already took — which is exactly the run whose
    disk behaviour is most worth reading.

    :param path: The metrics file.
    :param records: Records to append.
    :returns: Whether anything was written.
    """
    if not records:
        return False
    try:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('a', encoding='utf-8') as handle:
            for record in records:
                handle.write(json.dumps(record, sort_keys=True) + '\n')
    except OSError:
        return False
    return True
