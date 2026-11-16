"""Which branch a stage builds on, and which a review inspects."""

from __future__ import annotations

from collections.abc import Mapping

from sbx_omnigent import pipeline
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.records.results import NodeResult


def seed_from(
    stage: pipeline.PipelineStage, nodes: Mapping[str, NodeResult]
) -> str | None:
    """
    The upstream WRITER branch a node's worktree is cut from.

    An explicit ``from`` wins; else the first need that produced a
    branch. ``None`` means the stage is cut from the base branch.

    :param stage: The stage.
    :param nodes: The nodes that have run, by id.
    :returns: The upstream node, or ``None``.
    """
    if stage.from_branch:
        return stage.from_branch
    for need in stage.needs:
        node = nodes.get(need)
        if node and node.branch:
            return need
    return None


def seed_ref(ctx: RunContext, stage: pipeline.PipelineStage) -> str:
    """
    The hub ref a stage's branch was cut from.

    :param ctx: The run.
    :param stage: The stage.
    :returns: Its upstream writer's hub branch, else the base branch.
    """
    seed = seed_from(stage, ctx.nodes)
    if seed is not None:
        return ctx.worktrees.node_branch(ctx.run_id, seed)
    return ctx.config.base_branch or 'main'


def review_target(
    stage: pipeline.PipelineStage, nodes: Mapping[str, NodeResult]
) -> str:
    """
    The writer node a review stage inspects (first in needs).

    :param stage: The review stage.
    :param nodes: The nodes that have run, by id.
    :returns: The writer node it inspects.
    :raises PipelineRunError: If no need produced a branch.
    """
    for need in stage.needs:
        node = nodes.get(need)
        if node and node.branch:
            return need
    raise PipelineRunError(
        f'review {stage.id!r} has no writer in needs'
    )
