"""What every part of a run shares, fixed for the run's life."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from sbx_omnigent import pipeline
from sbx_omnigent.runner.records.results import NodeResult
from sbx_omnigent.swarm_session import SwarmSessionClient
from sbx_omnigent.worktrees import WorktreeManager


@dataclass(frozen=True)
class RunContext:
    """
    The run's shared, read-mostly state, handed to each collaborator.

    Every field is fixed when the run starts. The two mappings are
    shared, not copied: the runner fills them in place as the run
    advances, and a collaborator reading one sees the current state.

    :param config: The parsed pipeline.
    :param worktrees: Host-side git: clones, branches, run artifacts.
    :param sessions: The client that drives agent sessions.
    :param run_id: The run.
    :param nodes: Each node that has run, by id.
    :param stage_by_id: Every stage that runs, by id, a campaign
        module's stages included once it starts.
    :param lock: Guards what parallel nodes write.
    :param max_rounds: How many times a gate or review may loop back.
    """

    config: pipeline.PipelineConfig
    worktrees: WorktreeManager
    sessions: SwarmSessionClient
    run_id: str
    nodes: dict[str, NodeResult]
    stage_by_id: dict[str, pipeline.PipelineStage]
    lock: threading.RLock
    max_rounds: int
