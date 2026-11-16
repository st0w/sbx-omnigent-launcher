"""Whether the host has the disk a run will need at its peak."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path

import click

from sbx_omnigent import orphans, pipeline
from sbx_omnigent.runner.stage_kinds import stage_kind, writer_is_terminal


def max_concurrent_vms(
    config: pipeline.PipelineConfig, *, reclaim: bool = True
) -> int:
    """
    How many microVMs one pass through the DAG stands up at once.

    One VM per node, and one per reviewer agent in a review stage.
    Campaign runs loop this same set per chunk and dispose each chunk's
    VMs as it publishes (see
    :meth:`PipelineRunner._dispose_chunk_sessions`), so a pass is also
    the campaign's peak — unless ``--keep`` suppresses that reclaim,
    which :func:`preflight_disk` accounts for.

Only WRITERS now sum. Everything else is freed before the peak:

    - A review stage's VMs go as soon as its votes are in, so two
      review stages are never up together and only the LARGEST one
      counts. ``parallel:`` does not change that — it means isolated
      BRANCHES, not concurrent execution; :meth:`_exec_stage` walks a
      group's children one at a time.
    - A reader, a judge or a contract check is freed the moment its
      stage completes (see
      :meth:`PipelineRunner._release_completed_session`): none is ever
      driven again, so holding one to publish was pure waste.
    - A writer nothing can loop back to is freed the same way (see
      :func:`writer_is_terminal`). Only writers a review gate or the
      verification gate can re-drive keep their VMs to publish.

    :param config: The parsed pipeline.
    :param reclaim: Whether finished VMs are freed (false under
        ``--keep``, where every node of every round accumulates).
    :returns: The peak concurrent VM count.
    """
    total = 0
    reviewers: list[int] = []
    for stage in pipeline._iter_stages(config.stages):
        if stage.parallel or not stage.run:
            continue  # a parallel group's children are yielded too
        kind = stage_kind(stage)
        if kind == 'review':
            if reclaim:
                reviewers.append(len(stage.run))
            else:
                total += len(stage.run)
        elif reclaim and (
            kind in ('reader', 'judge', 'check', 'scan')
            or (kind == 'writer' and writer_is_terminal(config, stage.id))
        ):
            continue  # freed at stage completion, never re-driven
        else:
            total += 1
    total += max(reviewers, default=0)
    if config.verify is not None:
        # The pre-publish gate stands up one more sandbox, alongside the
        # chunk's nodes (they are disposed at publish, which is AFTER).
        total += 1
    return total


def writer_worktrees(config: pipeline.PipelineConfig) -> int:
    """
    How many BUILD worktrees one pass leaves on the host.

    Only writers are counted. Every node that cuts a clone pays for its
    checked-out tree, but a node clone is a LOCAL clone of the run's hub
    (hardlinked objects), so the repo itself is nearly free and the real
    cost is what the agent builds in it — which only a writer does.
    Measured: writer worktrees at 0.7 to 2.5 GB against reader and
    judge worktrees at 140 to 520 KB, four orders apart. A review
    round cuts a source-only snapshot of the branch and removes it
    when the round ends, which is the reader/judge class of cost —
    four orders below a writer's, and transient.

    The verification gate's throwaway clone counts as a writer: it runs
    the project's build and test command from clean.

    :param config: The parsed pipeline.
    :returns: The build-worktree count for one pass.
    """
    total = sum(
        1
        for stage in pipeline._iter_stages(config.stages)
        if stage.run and stage_kind(stage) == 'writer'
    )
    if config.verify is not None:
        total += 1
    return total


def preflight_disk(
    config: pipeline.PipelineConfig,
    *,
    keep: bool = False,
    path: Path | None = None,
    usage: Callable[..., object] = shutil.disk_usage,
    per_vm_bytes: int | None = None,
    per_worktree_bytes: int | None = None,
    floor_bytes: int | None = None,
    worktrees_on_disk: int = 0,
) -> None:
    """
    Refuse to start a run the host has no disk to finish.

    A microVM's disk is thin-provisioned: the guest sees a roomy
    filesystem while the host grows the backing file on demand. When the
    host runs out, the guest's writes fail as I/O errors and ext4
    protects itself by REMOUNTING READ-ONLY mid-run. Nothing reports
    this as a disk problem — the agent simply starts failing to write,
    and the harness surfaces an opaque ``[Errno 30] Read-only file
    system`` on a temp file. Observed live: five of six VMs went
    read-only and the run died without ever naming the cause.

    So check up front, where the message can name it.

    The estimate has TWO terms, because a run occupies the host twice
    over: the microVMs, and the host worktrees they mount. Counting only
    the VMs is what let a run start on ~23 GB and then exhaust the disk
    two modules in — the worktrees were the larger half.

    :param config: The parsed pipeline (sizes both counts).
    :param keep: Whether ``--keep`` is set. It suppresses EVERY
        reclaim — each module's VMs and worktrees, and each review
        round's reviewers — so the estimate both stops collapsing
        reviewers to one stage and multiplies by the module count,
        rather than letting that be discovered at module four. (A
        planner-proposed campaign's chunk count is unknown until it
        runs, so that case is still counted as one pass.)
    :param path: Filesystem to measure; ``None`` uses the home
        directory, where sbx keeps its per-sandbox images.
    :param usage: ``shutil.disk_usage``-alike (injected in tests).
    :param per_vm_bytes: Disk per microVM; ``None`` takes the
        pipeline's ``disk:`` value.
    :param per_worktree_bytes: Disk per build worktree; ``None`` takes
        the pipeline's ``disk:`` value.
    :param floor_bytes: Headroom to leave the host; ``None`` takes the
        pipeline's ``disk:`` value.
    :param worktrees_on_disk: Writer worktrees a RESUME already has,
        subtracted from the estimate. A resumed run does not re-cut the
        trees it already carries, and demanding their space a second
        time is what refused a resume for roughly twice its honest need.
    :raises click.ClickException: If free space is below the estimate.
    """
    gb = 1_000_000_000
    spec = config.disk
    per_vm = (
        int(spec.per_vm_gb * gb) if per_vm_bytes is None else per_vm_bytes
    )
    per_tree = (
        int(spec.per_worktree_gb * gb)
        if per_worktree_bytes is None
        else per_worktree_bytes
    )
    floor = (
        int(spec.headroom_gb * gb) if floor_bytes is None else floor_bytes
    )
    # --keep holds every module's VMs AND worktrees to the end of the
    # run; without it each module's are reclaimed as it publishes.
    passes = len(config.subtasks) if keep and config.subtasks else 1
    vms = max_concurrent_vms(config, reclaim=not keep) * passes
    trees = max(
        0, writer_worktrees(config) * passes - worktrees_on_disk
    )
    needed = floor + vms * per_vm + trees * per_tree
    free = usage(path or Path.home()).free
    if free >= needed:
        return
    held = (
        f' — {passes} modules held at once, because --keep never '
        f'reclaims them'
        if passes > 1
        else ''
    )
    advice = (
        "Drop --keep so each module's VMs and worktrees are reclaimed "
        'as it publishes, free some space, '
        if passes > 1
        else 'Free some space, '
    )
    # Name the leaked guest disks if there are any. They are invisible
    # to every reclaim the launcher can do and to `sbx ls` itself, so a
    # refusal that omits them sends someone hunting the wrong things.
    leaked = orphans.orphan_advice()
    raise click.ClickException(
        f'not enough free disk to run this pipeline: {free / gb:.1f} GB '
        f'free, about {needed / gb:.1f} GB needed{held}: {vms} '
        f'microVM(s) at ~{per_vm / gb:.1f} GB, {trees} host worktree(s) '
        f'at ~{per_tree / gb:.1f} GB (a writer builds into its clone), '
        f'plus {floor / gb:.0f} GB headroom. A microVM disk is thin-'
        f'provisioned, so running the host out mid-run does not fail '
        f'cleanly: the guest filesystem remounts READ-ONLY and the '
        f'agents start failing with opaque I/O errors. {advice}tune '
        f'`disk:` in the pipeline if these per-unit estimates do not fit '
        f'your project, or pass --skip-disk-check to override.'
        + (f'\n\n{leaked}' if leaked else '')
    )
