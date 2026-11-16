"""What a resumed run can reclaim from the one before it."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.stage_kinds import writer_is_terminal
from sbx_omnigent.swarm_session import SwarmSessionClient, SwarmSessionError
from sbx_omnigent.worktrees import WorktreeManager


def _resume_worktree_count(
    worktree_root: str, run_id: str, state: dict | None
) -> int:
    """
    How many BUILD worktrees a resume already has on disk.

    Counted from the filesystem rather than the state file: what matters
    to the disk estimate is what is actually THERE, and a run that
    crashed can have state and reality disagree in either direction.

    Only the kind :func:`writer_worktrees` estimates is counted: writer
    clones and the gate's ``-verify`` clones. Reader, judge and review
    directories are source-only, and counting them as writers' trees
    subtracted space the run still has to build (#29). A directory the
    state does not name as a writer is not counted, which errs toward
    asking for more space, never less.

    :param worktree_root: Host dir holding the run directories.
    :param run_id: The run being resumed.
    :param state: The run state, or ``None`` when unreadable.
    :returns: The count, or ``0`` when the run directory is absent.
    """
    recorded = (state or {}).get('nodes') or {}
    writers = {
        node for node, rec in recorded.items()
        if isinstance(rec, dict) and rec.get('kind') == 'writer'
    }
    nodes = Path(worktree_root) / run_id / 'nodes'
    try:
        return sum(
            1 for child in nodes.iterdir()
            if child.is_dir()
            and (child.name in writers or child.name.endswith('-verify'))
        )
    except OSError:
        return 0


def _node_stage(
    node_id: str, stage_ids: set[str], chunk_ids: list[str]
) -> tuple[str, str] | None:
    """
    Split a node id into its chunk prefix and pipeline stage id.

    :param node_id: e.g. ``impl`` or, in a campaign, ``m1-impl``.
    :param stage_ids: Every stage id in the pipeline.
    :param chunk_ids: The run's chunk ids.
    :returns: ``(prefix, stage id)`` with prefix ``''`` or
        ``'<chunk>-'``, or ``None`` when no reading, or more than one,
        fits (chunk ids ``core`` and ``core-extra`` share a prefix).
    """
    readings = [('', node_id)] if node_id in stage_ids else []
    for chunk in chunk_ids:
        prefix = f'{chunk}-'
        if node_id.startswith(prefix) and node_id[len(prefix):] in stage_ids:
            readings.append((prefix, node_id[len(prefix):]))
    return readings[0] if len(readings) == 1 else None


def _reclaim_finished_writers(
    wt: WorktreeManager,
    run_id: str,
    state: dict,
    config: pipeline.PipelineConfig,
    echo: Callable[[str], None],
) -> int:
    """
    Remove clones of finished writers that nothing can drive again.

    A published chunk's clones were the only ones a resume reclaimed,
    but a writer inside an unpublished chunk can be just as finished:
    a TDD writer no review loops back to, or a candidate that lost a
    recorded judge pick. Its build output is usually the largest thing
    a resume carries (#29).

    Everything downstream reads the hub, never these clones, so one is
    removed when ALL of these hold:

    * its stage completed, and the state records it as a writer
    * :func:`writer_is_terminal`, or it lost a recorded judge pick and
      every review that could re-drive it has completed
    * :meth:`WorktreeManager.node_matches_hub`: no uncommitted work
      and no commit the hub never saw

    :param wt: The worktree manager.
    :param run_id: The run being resumed.
    :param state: Its run state.
    :param config: The parsed pipeline.
    :param echo: Output sink.
    :returns: How many clones were removed.
    """
    stages = list(pipeline._iter_stages(config.stages))
    stage_ids = {s.id for s in stages}
    chunk_ids = [
        s['id'] for s in state.get('subtasks') or []
        if isinstance(s, dict) and isinstance(s.get('id'), str)
    ]
    completed = {c for c in state.get('completed') or [] if isinstance(c, str)}
    recorded = state.get('nodes') or {}
    losers = {
        cand
        for pick in state.get('judge_picks') or []
        if isinstance(pick, dict)
        for cand in pick.get('candidates') or []
        if isinstance(cand, str) and cand != pick.get('selected')
    }
    finished: list[str] = []
    kept: list[str] = []
    for node in sorted(completed):
        rec = recorded.get(node)
        if not isinstance(rec, dict) or rec.get('kind') != 'writer':
            continue
        parts = _node_stage(node, stage_ids, chunk_ids)
        if parts is None:
            continue
        prefix, stage_id = parts
        reviews_done = all(
            f'{prefix}{s.id}' in completed
            for s in stages
            if s.on_block == stage_id
        )
        if not (
            writer_is_terminal(config, stage_id)
            or (node in losers and reviews_done)
        ):
            continue
        if wt.node_matches_hub(run_id, node):
            finished.append(node)
        else:
            kept.append(node)
    if kept:
        echo(
            f'[resume] kept {", ".join(kept)}: finished, but the clone '
            f'holds work the hub does not have.'
        )
    if not finished:
        return 0
    try:
        freed = wt.dispose_node_worktrees(run_id, finished)
    except click.ClickException:
        return 0
    if freed:
        echo(
            f'[resume] reclaimed {freed} worktree(s) of finished '
            f'writers nothing can drive again ({", ".join(finished)}) '
            f'before measuring disk.'
        )
    return freed


def reclaim_for_resume(
    *,
    run_id: str,
    canonical_root: str,
    worktree_root: str,
    server: str,
    keep: bool,
    default_branch: str,
    client: SwarmSessionClient | None = None,
    manager: WorktreeManager | None = None,
    echo: Callable[[str], None] = click.echo,
    config: pipeline.PipelineConfig | None = None,
) -> int:
    """
    Free what the previous attempt provably no longer needs.

    Runs BEFORE :func:`preflight_disk`, which is the whole point. The
    reclaim used to live inside ``runner.run()`` — downstream of the
    gate — so a resume was refused by space the resume itself would have
    freed seconds later. Observed live: a machine crashed mid-module
    leaving six orphaned microVMs holding ~26 GB, the host measured 15.6
    GB free against a 46.5 GB demand, and the reclaim that would have
    returned the 26 GB sat behind the refusal.

    Two things are provably dead on a resume:

    * the microVMs the previous attempt left running — their ids are in
      the run state, so this needs no discovery
    * the worktrees of chunks already recorded complete — their branches
      are on the hub and their pull requests are open
    * with *config*, the clones of finished writers nothing can drive
      again (see :func:`_reclaim_finished_writers`)

    Best-effort throughout: a resume must not fail because something it
    was cleaning is already gone. The disposed sessions are removed from
    the run state, so the runner does not dispose them again.

    :param run_id: The run being resumed.
    :param canonical_root: Host dir holding the bare mirrors.
    :param worktree_root: Host dir holding the run directories.
    :param server: Omnigent server URL.
    :param keep: Whether ``--keep`` is set — it suppresses every
        reclaim, because the human asked for the VMs to stay.
    :param default_branch: The manager's base branch.
    :param client: Session client (injected in tests).
    :param manager: Worktree manager (injected in tests).
    :param echo: Output sink (injected in tests).
    :param config: The parsed pipeline; without it, finished writers
        are not reclaimed, since whether one can be re-driven is read
        from the DAG.
    :returns: How many node worktrees were removed.
    """
    wt = manager or WorktreeManager(
        canonical_root=canonical_root,
        worktree_root=worktree_root,
        default_branch=default_branch,
    )
    state = wt.read_run_state(run_id)
    if not state:
        return 0
    if keep:
        echo(
            '[resume] --keep: reclaiming nothing before the disk check, '
            'since the previous attempt was asked to hold its VMs and '
            'worktrees.'
        )
        return 0
    sessions = [s for s in state.get('sessions') or [] if isinstance(s, str)]
    if sessions:
        sc = client or SwarmSessionClient(server)
        left: list[str] = []
        for session in sessions:
            try:
                sc.dispose(session)
            except SwarmSessionError:
                # Already torn down, or the server lost it. It may
                # still be up, so it stays tracked.
                left.append(session)
        echo(
            f'[resume] disposed {len(sessions) - len(left)}/'
            f'{len(sessions)} microVM(s) the previous attempt left '
            f'running, before measuring disk.'
        )
        # The runner disposes whatever the state still lists (see
        # PipelineRunner._dispose_stale_sessions). Left in, it disposed
        # every one of these a second time and logged that as success,
        # since a 404 counts as disposed (#32).
        wt.write_run_state(run_id, {**state, 'sessions': left})
    freed = _reclaim_published_chunks(wt, run_id, state, echo)
    if config is not None:
        freed += _reclaim_finished_writers(wt, run_id, state, config, echo)
    return freed


def _reclaim_published_chunks(
    wt: WorktreeManager,
    run_id: str,
    state: dict,
    echo: Callable[[str], None],
) -> int:
    """
    Remove the clones of chunks the run state records as published.

    :param wt: The worktree manager.
    :param run_id: The run being resumed.
    :param state: Its run state.
    :param echo: Output sink.
    :returns: How many clones were removed.
    """
    freed = 0
    done = [
        c for c in state.get('completed_chunks') or []
        if isinstance(c, str)
    ]
    nodes = [n for n in state.get('nodes') or {} if isinstance(n, str)]
    for chunk in done:
        # A published chunk's branches are on the hub and its pull
        # request is open; its worktrees are the largest thing a resume
        # is still carrying and it can rebuild none of them.
        stale = [n for n in nodes if n == chunk or n.startswith(f'{chunk}-')]
        if not stale:
            continue
        try:
            freed += wt.dispose_node_worktrees(run_id, stale)
        except click.ClickException:
            continue
    if freed:
        echo(
            f'[resume] reclaimed {freed} worktree(s) from '
            f'{len(done)} published module(s) before measuring disk.'
        )
    return freed
