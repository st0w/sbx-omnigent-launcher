"""Records committed to the repo beside the code a module publishes.

The approved plan of record travels with the code it describes, and
beside it the planner's whole conversation, every reviewer's report,
and the judge's choice between candidates. The plan is the point of
the pull request, so a failure to commit it fails the publish. The
other three are best-effort: worth having, never worth failing a
publish over.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.paths import (
    reviews_record_path,
    selection_record_path,
    session_record_path,
)
from sbx_omnigent.runner.records.judging import JudgePick
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.render.judging import render_judge_decision
from sbx_omnigent.runner.render.reviews import render_review_records
from sbx_omnigent.runner.render.sessions import render_planning_session
from sbx_omnigent.runner.stage_kinds import is_planner
from sbx_omnigent.swarm_session import SwarmSessionError

#: The author of the plan and planning-session commits: a role, never
#: an agent's name, which would put the model into git log.
_PLANNER = 'planner <planner@pipeline.local>'


class RepoRecords:
    """
    Commit a module's plan and the records of how it was built.

    :param ctx: The run.
    :param chunk_reviews: The current module's review records.
    :param chunk_picks: The current module's judge picks.
    :param reader_turns: Each reader's buffered conversation, by node.
        A callable because a resume replaces the mapping.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        chunk_reviews: Callable[[], list[ReviewRecord]],
        chunk_picks: Callable[[], list[JudgePick]],
        reader_turns: Callable[[], Mapping[str, tuple[tuple[str, str], ...]]],
    ) -> None:
        self.ctx = ctx
        self._chunk_reviews = chunk_reviews
        self._chunk_picks = chunk_picks
        self._reader_turns = reader_turns

    def commit_plan(self, winner: str, plan_path: str, plan: str) -> None:
        """
        Commit the plan of record onto the branch about to ship.

        The approved design travels with the code in the PR. Not
        best-effort: a refused write raises.

        :param winner: The node whose branch publishes.
        :param plan_path: Where the plan goes.
        :param plan: The plan of record.
        :raises click.ClickException: If git refuses the write.
        """
        wt = self.ctx.worktrees
        wt.write_tracked_file(
            wt.node_worktree_path(self.ctx.run_id, winner), plan_path, plan,
        )
        wt.commit_node(
            self.ctx.run_id, winner,
            message='docs: add plan of record',
            author=_PLANNER,
        )

    def commit_reviews(
        self,
        winner: str,
        plan_path: str,
        *,
        module: pipeline.Subtask | None,
    ) -> str | None:
        """
        Commit the reviewer reports onto the branch about to publish.

        :param winner: The node whose branch publishes.
        :param plan_path: The plan artifact path, whose name this record
            is derived from (it needs no plan to exist).
        :param module: The module, or ``None`` outside a campaign.
        :returns: The committed repo path, or ``None``.
        """
        records = self._chunk_reviews()
        if not records:
            return None
        title = f'Reviews — {self.ctx.config.name}' + _module_suffix(module)
        path = reviews_record_path(self.ctx.config, module, plan_path)
        if not self._commit(
            winner, path, render_review_records(records, title=title),
            message='docs: add reviewer reports',
            author='reviewer <reviewer@pipeline.local>',
        ):
            return None
        click.echo(
            f'[review] committed {len(records)} reviewer report(s) '
            f'to {path}.'
        )
        return path

    def commit_selection(
        self,
        winner: str,
        plan_path: str,
        *,
        module: pipeline.Subtask | None,
    ) -> str | None:
        """
        Commit the judge's choice onto the branch about to publish.

        A pipeline with no judge writes nothing.

        :param winner: The node whose branch publishes.
        :param plan_path: The plan artifact path, whose name this record
            is derived from (it needs no plan to exist).
        :param module: The module, or ``None`` outside a campaign.
        :returns: The committed repo path, or ``None``.
        """
        picks = self._chunk_picks()
        if not picks:
            return None
        title = f'Selection — {self.ctx.config.name}' + _module_suffix(module)
        path = selection_record_path(self.ctx.config, module, plan_path)
        if not self._commit(
            winner, path, render_judge_decision(picks, title=title),
            message='docs: add the judge selection',
            # A role, like the planner and reviewer records — never the
            # judge AGENT's name, which would put the deciding model
            # back into git log.
            author='judge <judge@pipeline.local>',
        ):
            return None
        click.echo(
            f'[judge] committed {len(picks)} selection(s) to {path}.'
        )
        return path

    def commit_planning_session(
        self,
        winner: str,
        plan_path: str,
        *,
        module: pipeline.Subtask | None,
    ) -> None:
        """
        Commit the planner's whole conversation beside its plan.

        An unreadable session or a git error simply skips it.

        :param winner: The node whose branch publishes.
        :param plan_path: Repo path of this plan of record.
        :param module: The module, or ``None`` outside a campaign.
        """
        node_id = self._planner_node_id(module)
        if node_id is None:
            return  # no planner in this pipeline: nothing to record
        turns = self._planning_turns(node_id)
        if turns is None:
            return
        title = (
            f'Planning session — {self.ctx.config.name}'
            + _module_suffix(module)
        )
        doc = render_planning_session(turns, title=title, plan_doc=plan_path)
        if not doc:
            click.echo(
                f'[plan-record] no session record: {node_id!r} produced '
                f'{len(turns)} message(s), which rendered to nothing.'
            )
            return
        path = session_record_path(plan_path)
        if self._commit(
            winner, path, doc,
            message='docs: add planning session record', author=_PLANNER,
        ):
            click.echo(
                f'[plan] committed the planning session '
                f'({len(turns)} turns) to {path}.'
            )

    def _planner_node_id(self, module: pipeline.Subtask | None) -> str | None:
        """The planner node for *module*, else the run's."""
        config = self.ctx.config
        for stage in config.stages:
            if not is_planner(stage, config):
                continue
            if module is not None and config.subtasks:
                return f'{module.id}-{stage.id}'
            return stage.id
        return None

    def _planning_turns(
        self, node_id: str
    ) -> list[tuple[str, str]] | None:
        """
        The planner's conversation, or ``None`` with the reason logged.

        The BUFFER first, and a live session only as the fallback. The
        two used to be the other way round, which is why no planning
        session record ever reached the repo: a resumed run restores its
        nodes WITHOUT a session (deliberately — see
        ``test_state_never_records_a_session``: a session handle names
        a VM the new run cannot reattach to), so the session guard
        returned
        before the buffer — the one copy that survives — was ever
        consulted. The buffer needs no live session by construction; it
        exists precisely because the VM is gone.

        :param node_id: The planner node.
        :returns: Its turns, or ``None`` when there are none to record.
        """
        node = self.ctx.nodes.get(node_id)
        if node is None:
            click.echo(
                f'[plan-record] no session record: planner node '
                f"{node_id!r} is not among the run's nodes "
                f'({", ".join(sorted(self.ctx.nodes)) or "none"}).'
            )
            return None
        buffered = self._reader_turns().get(node_id)
        if buffered is not None:
            return list(buffered)
        if not node.session:
            click.echo(
                f'[plan-record] no session record: nothing was buffered '
                f'for {node_id!r} and it has no live session to read '
                f"(a resumed run cannot reattach to the planner's VM)."
            )
            return None
        # The shipped read_transcript swallows SwarmSessionError and
        # returns [], so an empty result is the DISPOSED session — but
        # the guard stays: this must never be able to fail a publish,
        # whatever client is injected.
        try:
            turns = self.ctx.sessions.read_transcript(node.session)
        except SwarmSessionError:
            click.echo(
                f'[plan-record] no session record: {node_id!r} was not '
                f'buffered and its session could not be read.'
            )
            return None
        if not turns:
            click.echo(
                f'[plan-record] no session record: nothing was buffered '
                f'for {node_id!r} and its session reads back empty (it '
                f'has been disposed).'
            )
        return turns

    def _commit(
        self, winner: str, path: str, doc: str, *, message: str, author: str
    ) -> bool:
        """
        Write *doc* at *path* on *winner*'s branch and commit it.

        :returns: Whether a commit was made; ``False`` when git refused
            or there was nothing new to commit.
        """
        wt = self.ctx.worktrees
        try:
            wt.write_tracked_file(
                wt.node_worktree_path(self.ctx.run_id, winner), path, doc,
            )
            committed = wt.commit_node(
                self.ctx.run_id, winner, message=message, author=author,
            )
        except click.ClickException:
            return False
        return bool(committed)


def _module_suffix(module: pipeline.Subtask | None) -> str:
    """`` [<id>] <title>`` for a module's record titles, else ``''``."""
    return f' [{module.id}] {module.title}' if module is not None else ''
