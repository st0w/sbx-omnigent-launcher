"""The briefing: what every agent turn is told about the run around it.

Each turn is handed the WHOLE plan's brief as its contract. In a
campaign it is also told which increment is its own and which rows
belong to others, and a builder is passed the designs of the readers
it depends on.
"""

from __future__ import annotations

from sbx_omnigent import pipeline
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.prompts.environment import setup_block
from sbx_omnigent.runner.prompts.writer import _DISPOSABLE_VM, _UNATTENDED


class Briefing:
    """
    The run-wide context written into agent turns.

    :param ctx: The run.
    :param progress: The run's progress, for the module list and the
        active module.
    """

    def __init__(self, ctx: RunContext, *, progress: RunProgress) -> None:
        self.ctx = ctx
        self._progress = progress

    def chunk_preamble(self) -> str:
        """
        A directive scoping a turn to the active module.

        :returns: The preamble, or ``''`` outside a campaign module.
        """
        sub = self._progress.active_subtask
        if sub is None:
            return ''
        # Claiming prior work on the FIRST increment sends an agent
        # hunting for artifacts that cannot exist, and a careful one
        # then (rightly) refuses to build against the missing baseline.
        #
        # But `active_is_first` is a POSITION in this run's row list,
        # not a claim about the tree, and the two came apart the moment
        # a campaign ran against a repo with earlier modules already
        # merged. On `ingestion-m3-1` every m3a agent was told the tree
        # was at base state while m0, m0b, m1 and m2 sat in it — handed
        # to reviewers vetting a module whose brief is "mirror the [m2]
        # pattern". So say only what the position proves: no earlier
        # ROW of this run has been built. The tree speaks for itself.
        prior = (
            'It is the FIRST increment, so none of the increments '
            'below has been built yet; do not go looking for their '
            'artifacts. Anything already in your worktree is earlier, '
            'separate work — treat it as frozen and build against it.'
            if self._progress.active_is_first
            else 'Earlier increments are already implemented in your '
            'worktree; later ones come afterwards.'
        )
        # The title is NOT repeated here: the table below carries it,
        # marked, and a plan whose rows are full scope paragraphs would
        # otherwise print the active one twice in every single turn.
        return (
            f'This is build increment [{sub.id}] of a multi-part plan. '
            f'{prior} Do ONLY this increment now — not the '
            'whole plan.\n\nThe full increment list, in order — the '
            f'marked row is yours:\n\n{self.increment_table(sub)}\n\n'
            'The Task below is the contract for the WHOLE plan, not for '
            'your increment alone. A requirement it states that another '
            "row owns is that row's job — not yours, and NOT missing."
            '\n\n'
        )

    def increment_table(self, sub: pipeline.Subtask) -> str:
        """
        The ordered increment list, the active row marked.

        Every role — writer, reviewer and judge alike — is handed the
        WHOLE plan's brief as its contract, with the active increment as
        a one-line qualifier above it. A reviewer reading a 15,000-word
        brief that says "pagination on every listing call" and seeing an
        unpaginated call has, from where it stands, found an unmet
        requirement that nobody owns; blocking on it is the faithful
        move. Observed live on `gcp-scope-topology-1`: the [topology]
        bug reviewer blocked on `ListServiceAccounts` pagination, which
        the plan assigns to [identities], and the writer — for whom a
        blocking finding is not optional — implemented it. The judge
        then scored that as a strength.

        Showing the other rows converts "nobody has done this" into
        "row [identities] owns this", which is the only form a reviewer
        can reasonably let pass. Exactly the reasoning behind
        :meth:`module_table` one level up, where a planner shown only
        its own row designed its neighbours' scope into its module.

        :param sub: The increment being worked.
        :returns: One ``[id] title`` row per increment: ``✓`` for one
            already published, ``▶`` for the active one, blank for one
            still to come.
        """
        rows = []
        for other in self._progress.subtasks:
            if other.id == sub.id:
                mark = '▶'
            elif other.id in self._progress.completed_chunks:
                mark = '✓'
            else:
                mark = ' '
            rows.append(f'    {mark} [{other.id}] {other.title}')
        return '\n'.join(rows)

    def module_table(self, sub: pipeline.Subtask) -> str:
        """
        The full ordered module list, with the active one marked.

        A per-module planner is otherwise shown ONLY its own row, so it
        cannot tell which adjacent work belongs to somebody else and
        reasonably designs the neighbours' scope into its module.
        Observed live: module 0's planner specified the storage layer
        and snapshot diffing that the human's own table assigns to
        module 1, and the test writer stopped to ask which bar to hold
        it to — the one time that conflict was caught rather than
        frozen into a test suite.

        :param sub: The module being designed.
        :returns: One ``[id] title`` row per module, the active one
            arrow-marked.
        """
        return '\n'.join(
            f'{"▶ " if other.id == sub.id else "  "}'
            f'[{other.id}] {other.title}'
            for other in self._progress.subtasks
        )

    def task_block(self) -> str:
        """
        The task every builder, reviewer and judge turn carries.

        :returns: The module preamble, the task and any acceptance
            contract, then the standing rules for an unattended run in
            a disposable VM, and the environment note.
        """
        block = f'{self.chunk_preamble()}Task:\n{self.ctx.config.task}'
        if self.ctx.config.acceptance:
            block += f'\n\nAcceptance contract:\n{self.ctx.config.acceptance}'
        return (
            f'{block}\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            + setup_block(self.ctx.config)
        )

    def reader_context(self, stage: pipeline.PipelineStage) -> str:
        """
        The designs of the readers *stage* needs, for its turn.

        Readers only: a contract check is recorded as its own kind, so
        its findings never reach a writer as design context.

        :param stage: The stage whose turn is being written.
        :returns: Each upstream reader's output under a heading, or
            ``''`` when there is none.
        """
        parts = []
        for need in stage.needs:
            node = self.ctx.nodes.get(need)
            if node and node.kind == 'reader' and node.output:
                parts.append(f'--- Design from {need} ---\n{node.output}')
        return '\n\n'.join(parts)
