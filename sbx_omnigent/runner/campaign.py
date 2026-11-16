"""The campaign: building a pipeline once per module, threaded together.

Each module runs its own namespaced copy of the stages, is gated and
published on its own, and becomes the base the next module builds on.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace

import click

from sbx_omnigent import pipeline
from sbx_omnigent.pipeline import Subtask
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.filing import FindingFiler
from sbx_omnigent.runner.paths import chunk_plan_path
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.publish import Publisher
from sbx_omnigent.runner.stage_kinds import is_planner, stage_kind


@dataclass(frozen=True)
class CampaignHooks:
    """
    What the campaign asks of the runner, one call each.

    :param run_stage: Runs one top-level stage to completion.
    :param gate: Runs the verification gate on a module's winner, with
        the stages that ran; raises if it does not pass.
    :param save_state: Persists the run's state.
    :param dispose_prewarmed: Frees writers pre-warmed during planning.
    :param session_count: How many sessions the run has created.
    :param dispose_sessions_since: Disposes every session created after
        the given count.
    :param sample_disk: Records a disk sample, named by the event.
    """

    run_stage: Callable[[pipeline.PipelineStage], None]
    gate: Callable[[str, Sequence[pipeline.PipelineStage]], None]
    save_state: Callable[[], None]
    dispose_prewarmed: Callable[[], None]
    session_count: Callable[[], int]
    dispose_sessions_since: Callable[[int], None]
    sample_disk: Callable[[str], None]


class Campaign:
    """
    Run the pipeline once per module, publishing each as it finishes.

    :param ctx: The run.
    :param progress: The run's progress, which the campaign advances.
    :param publisher: Publishes each module.
    :param filing: Its records start empty for each module.
    :param keep: ``--keep``: leave every VM and clone in place.
    :param hooks: What the campaign asks of the runner.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        progress: RunProgress,
        publisher: Publisher,
        filing: FindingFiler,
        keep: bool,
        hooks: CampaignHooks,
    ) -> None:
        self.ctx = ctx
        self.progress = progress
        self._publisher = publisher
        self._filing = filing
        self._keep = keep
        self._hooks = hooks

    def applies(
        self, build_stages: Sequence[pipeline.PipelineStage]
    ) -> bool:
        """
        Whether to run the build cycle once per proposed chunk.

        Engages only when the planner proposed ≥2 chunks and there are
        build stages to loop; 0/1 chunks run the single pass unchanged
        (so existing single-increment pipelines are untouched).

        :param build_stages: The stages a module would loop.
        :returns: Whether a campaign runs.
        """
        return len(self.progress.subtasks) >= 2 and bool(build_stages)

    def run(
        self,
        loop_stages: list[pipeline.PipelineStage],
        *,
        per_module: bool,
    ) -> None:
        """
        Run *loop_stages* once per chunk, threaded and published.

        Each chunk runs an id-namespaced copy of *loop_stages*
        (``<chunk>-<node>``) so their branches never collide. The chunks
        thread through one accumulating hub branch ``campaign``: chunk
        0's entry node seeds from base, each later chunk's entry node
        seeds ``from`` the campaign tip (the prior chunk's winner), and
        after every chunk the winner is aliased onto ``campaign``. Each
        chunk publishes separately. A chunk that blocks stops the
        campaign there (earlier chunks stay published).

        Two modes share this loop:

        * flat campaign (``per_module=False``): a single up-front
          planner proposed the chunk list; *loop_stages* are the
          non-planner build stages, and refs to that shared planner
          stay un-namespaced.
        * per-module (``per_module=True``): the human supplied the
          module list; *loop_stages* are the FULL pipeline including
          its planner, so every ref is namespaced and each module runs
          its own planner (in-loop, against the frozen prior modules).

        :param loop_stages: The top-level stages to run per chunk.
        :param per_module: Whether the planner is inside the loop.
        """
        # The pre-warmed (un-namespaced) writers are replaced by
        # per-chunk ones — free their VMs so they don't idle here.
        self._hooks.dispose_prewarmed()
        # In per-module mode the planner is looped, so its refs must be
        # namespaced too (each module has its own); in flat mode the
        # shared up-front planner's refs stay un-prefixed.
        planner_ids = (
            set()
            if per_module
            else {
                s.id for s in self.ctx.config.stages
                if is_planner(s, self.ctx.config)
            }
        )
        writer_ids = {
            s.id
            for s in pipeline._iter_stages(tuple(loop_stages))
            if s.write
        }
        for index, sub in enumerate(self.progress.subtasks):
            if sub.id in self.progress.completed_chunks:
                click.echo(
                    f'[resume] skipping chunk [{sub.id}] (already '
                    f'published)'
                )
                continue
            self._build_module(
                index, sub, loop_stages, planner_ids, writer_ids
            )

    def _build_module(
        self,
        index: int,
        sub: Subtask,
        loop_stages: list[pipeline.PipelineStage],
        planner_ids: set[str],
        writer_ids: set[str],
    ) -> None:
        """
        Build, gate, publish and thread one module, then reclaim it.

        :param index: Its position in the module list.
        :param sub: The module.
        :param loop_stages: The stages each module runs.
        :param planner_ids: Stages shared by every module.
        :param writer_ids: Every writer stage in *loop_stages*.
        """
        self.progress.start_module(sub, first=index == 0)
        self._filing.start_module()
        mark = self._hooks.session_count()
        staged = namespace_stages(
            sub.id, loop_stages, planner_ids, writer_ids,
            seed_campaign=index > 0,
        )
        # Register the namespaced stages so a review loop-back can
        # look its writer up by the namespaced id.
        for st in pipeline._iter_stages(tuple(staged)):
            if st.run or st.parallel:
                self.ctx.stage_by_id[st.id] = st
        for st in staged:
            self._hooks.run_stage(st)
        winner = self.winner(sub.id)
        # Gate BEFORE the thread advances: a chunk that cannot pass
        # verification must not become the base every later module
        # is built on.
        self._hooks.gate(winner, staged)
        self._publish(sub, winner, first=index == 0)
        # Advance the campaign thread to this chunk's winner so the
        # next chunk builds on it. AFTER the publish, which commits
        # the chunk's plan, decisions and reviews onto the winner:
        # the next planner is told they are in its worktree under
        # docs/plans/, and before this they were only there if the
        # chunk's PR had already been merged into the base.
        self.ctx.worktrees.alias_node_branch(
            self.ctx.run_id, 'campaign', winner
        )
        self.progress.completed_chunks.add(sub.id)
        # Persist the completion NOW, not at the next stage's end.
        # The next stage is the following module's PLANNER, which
        # blocks on the human — so deferring this leaves a window
        # (potentially an hour of approval time) where the on-disk
        # state says a published chunk never finished. A resume in
        # that window rebuilds the whole module and publishes a
        # DUPLICATE PR for work that already shipped.
        self._hooks.save_state()
        # Published: this chunk's VMs AND clones are dead weight.
        self._release_sessions(mark)
        # The module's PEAK: every tree this chunk built is still
        # on disk, one line before the reclaim takes them.
        self._hooks.sample_disk('chunk-peak')
        self._reclaim_worktrees(staged, winner)
        self.progress.end_module()

    def winner(self, chunk_id: str) -> str:
        """
        The node whose branch a module publishes and threads.

        :param chunk_id: The module.
        :returns: The pipeline's ``publish.branch`` for the module, else
            the last node that produced a branch; a judge's selected
            candidate in place of the judge.
        :raises PipelineRunError: If the module produced no branch, or
            its judge selected no candidate.
        """
        want = self.ctx.config.publish.branch
        node_id = (
            f'{chunk_id}-{want}' if want
            else self.progress.last_branch_node
        )
        node = self.ctx.nodes.get(node_id) if node_id else None
        if node is None or node_id is None:
            raise PipelineRunError(
                f'chunk {chunk_id!r} produced no branch to publish'
            )
        if node.kind != 'judge':
            return node_id
        if node.selected is None:
            # A judge halts the run rather than finish without a pick,
            # so this is not expected; publishing "None" would be worse.
            raise PipelineRunError(
                f'chunk {chunk_id!r}: judge {node_id!r} selected no '
                f'candidate to publish'
            )
        return node.selected

    def _publish(
        self, sub: Subtask, winner: str, *, first: bool
    ) -> None:
        """Publish one module's winner; see :class:`Publisher`."""
        plan, plan_path = self._plan_for(sub, first=first)
        self._publisher.publish_module(
            sub, winner,
            first=first,
            plan=plan,
            plan_path=plan_path,
            prior=self.progress.prior_module(),
            subtasks=self.progress.subtasks,
        )

    def _plan_for(
        self, sub: pipeline.Subtask, *, first: bool
    ) -> tuple[str | None, str]:
        """
        The plan text + repo path to commit for a chunk, if any.

        Per-module mode commits each module's OWN planner design to a
        per-module path (``…-<module>.md``), so modules don't clobber
        one another's plan doc as the campaign thread accumulates. Flat
        mode commits the single shared plan of record once, on chunk 0's
        branch (later chunks inherit it through the thread).

        :param sub: The module.
        :param first: Whether it is the campaign's first module.
        :returns: ``(plan, path)``; the plan is ``None`` when there is
            none to commit.
        """
        if self.ctx.config.subtasks:
            node = self.ctx.nodes.get(f'{sub.id}-plan')
            plan = node.output if node else None
            return plan, chunk_plan_path(self.ctx.config, sub)
        plan = self.progress.plan_of_record if first else None
        return plan, chunk_plan_path(self.ctx.config, sub)

    def _release_sessions(self, mark: int) -> None:
        """
        Tear down the VMs a finished chunk created.

        A published chunk's VMs are dead weight: its work is committed
        to a branch and pushed, and the next chunk seeds from the hub,
        never from a node's clone. Holding them cost a VM per node per
        chunk for the WHOLE run — a 6-module full cadre peaked at ~72
        live microVMs and exhausted the host's disk, which surfaces as
        guest filesystems remounting read-only rather than as any
        legible error. ``--keep`` still keeps everything.

        :param mark: How many sessions the run had when the chunk
            started; every one since belongs to it.
        """
        if self._keep:
            return
        self._hooks.dispose_sessions_since(mark)

    def _reclaim_worktrees(
        self, staged: list[pipeline.PipelineStage], winner: str
    ) -> None:
        """
        Reclaim a published chunk's node clones.

        Its work is committed to hub branches and pushed, the next chunk
        seeds from the hub rather than from any clone, and a resume
        skips a completed chunk wholesale — so the clones are dead
        weight the moment it publishes. Keeping them makes disk cost
        CUMULATIVE across a campaign: 2.2-26 GB per writer node for a
        compiled language, which ran a host out of space two modules
        into a six-module run. Reclaiming here makes the cost
        per-module instead.

        Called only AFTER the chunk's completion is persisted, so a
        crash in between leaves a resume that skips the chunk anyway.
        Branches are never touched; ``--keep`` keeps everything.

        :param staged: The chunk's namespaced stages.
        :param winner: The node whose branch published (its
            verification clone, if any, is named after it).
        """
        if self._keep:
            return
        node_ids = [
            st.id
            for st in pipeline._iter_stages(tuple(staged))
            if st.run or st.parallel
        ]
        # The gate cuts its own throwaway clone; sweep it up too.
        node_ids.append(f'{winner}-verify')
        try:
            freed = self.ctx.worktrees.dispose_node_worktrees(
                self.ctx.run_id, node_ids
            )
        except click.ClickException:
            return  # reclaiming disk must never fail a healthy run
        if freed:
            click.echo(
                f'[cleanup] reclaimed {freed} worktree(s) from the '
                f'published chunk (their branches are on the hub).'
            )


def namespace_stages(
    chunk_id: str,
    stages: list[pipeline.PipelineStage],
    planner_ids: set[str],
    writer_ids: set[str],
    *,
    seed_campaign: bool,
) -> list[pipeline.PipelineStage]:
    """
    *stages* with their ids and references namespaced for a module.

    :param chunk_id: The module.
    :param stages: The top-level stages to copy.
    :param planner_ids: Stages shared by every module (a flat
        campaign's up-front planner), whose names are kept.
    :param writer_ids: Every writer stage in *stages*.
    :param seed_campaign: Whether entry nodes build on the campaign
        thread (every module after the first).
    :returns: The namespaced copies, in order.
    """
    out: list[pipeline.PipelineStage] = []
    for stage in stages:
        if stage.parallel:
            subs = tuple(
                _namespace_one(
                    chunk_id, sub, planner_ids, writer_ids,
                    seed_campaign=seed_campaign,
                )
                for sub in stage.parallel
            )
            out.append(
                replace(stage, id=f'{chunk_id}-{stage.id}', parallel=subs)
            )
        else:
            out.append(
                _namespace_one(
                    chunk_id, stage, planner_ids, writer_ids,
                    seed_campaign=seed_campaign,
                )
            )
    return out


def _namespace_one(
    chunk_id: str,
    stage: pipeline.PipelineStage,
    planner_ids: set[str],
    writer_ids: set[str],
    *,
    seed_campaign: bool,
) -> pipeline.PipelineStage:
    """Namespace one stage's id + references (planner refs stay)."""
    def ns(ref: str) -> str:
        return ref if ref in planner_ids else f'{chunk_id}-{ref}'

    new_from = ns(stage.from_branch) if stage.from_branch else None
    # A chunk-entry node that cuts its own worktree (a reader like
    # the per-module planner, or the entry writer) with no in-chunk
    # writer upstream and no explicit from seeds from the campaign
    # tip on chunks after the first — so it designs/builds against
    # the frozen prior chunks. Reviews/judges don't cut from a seed,
    # so leaving their from unset is a no-op for them.
    if (
        seed_campaign
        and stage_kind(stage) in ('reader', 'writer')
        and stage.from_branch is None
        and not any(n in writer_ids for n in stage.needs)
    ):
        new_from = 'campaign'
    return replace(
        stage,
        id=f'{chunk_id}-{stage.id}',
        needs=tuple(ns(n) for n in stage.needs),
        from_branch=new_from,
        on_block=ns(stage.on_block) if stage.on_block else None,
        checks=ns(stage.checks) if stage.checks else None,
        scans=ns(stage.scans) if stage.scans else None,
    )
