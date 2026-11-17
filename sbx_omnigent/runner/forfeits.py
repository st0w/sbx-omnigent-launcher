"""Forfeits: a candidate whose review never agreed is withdrawn.

Two writers are run so a judge can choose between them on evidence. A
review that spends its round budget withdraws the candidate it was
vetting, as long as a rival is still in the field, and the judge then
chooses from what is left, with the withdrawal on record (#47).
"""

from __future__ import annotations

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import _Blocked
from sbx_omnigent.runner.stage_kinds import stage_kind


class Forfeits:
    """
    The candidates withdrawn from judging, and why.

    :param ctx: The run.

    :ivar forfeited: Nodes withdrawn because their review never reached
        consensus — the blocked review stage and the writer it was
        vetting. A forfeited candidate is excluded from judging:
        comparing a vetted branch against an unvetted one is not a
        choice.
    :ivar forfeits: Each withdrawn candidate's ``(review stage,
        rounds)``, so the selection records the reduced field it was
        made from (#47).
    """

    def __init__(self, ctx: RunContext) -> None:
        self.ctx = ctx
        self.forfeited: set[str] = set()
        self.forfeits: dict[str, tuple[str, int]] = {}

    def forfeit_blocked_reviews(
        self,
        stage: pipeline.PipelineStage,
        blocked: list[_Blocked],
    ) -> bool:
        """
        Drop the candidates whose reviews blocked, if a sibling cleared.

        Two writers are run so a judge can choose between them on
        evidence. Before this, a review that spent its round budget
        raised `_Blocked` all the way out of the run — so a campaign
        could end with one candidate approved by every reviewer,
        gate-verified and unpublished, because the OTHER one could not
        converge. Seen twice: once with a candidate that was never
        reviewed at all, and once with a candidate approved on round one
        discarded beside a sibling that blocked four times.

        A blocked review is a forfeit by that candidate, not a
        verdict on the field. If at least one sibling review cleared,
        the run continues with the survivors and the judge picks
        among them.

        Returns True when the stage may proceed. When NO sibling
        cleared, returns False and the caller re-raises — nothing was
        vetted, so there is nothing to judge.

        :param stage: The parallel parent whose sub-stages are reviews.
        :param blocked: The `_Blocked` raised by its sub-stages.
        :returns: Whether the run may continue without these candidates.
        """
        blocked_ids = {exc.stage_id for exc in blocked}
        survivors = [
            sub.id
            for sub in stage.parallel
            if sub.id not in blocked_ids
            and (node := self.ctx.nodes.get(sub.id)) is not None
            and node.kind == 'review'
            and node.verdict != 'BLOCKING'
        ]
        if not survivors:
            return False
        for exc in blocked:
            sub = self.ctx.stage_by_id.get(exc.stage_id)
            target = sub.on_block if sub is not None else None
            self.forfeited.add(exc.stage_id)
            if target:
                self.forfeited.add(target)
                self.forfeits[target] = (exc.stage_id, exc.rounds)
            click.echo(
                f'[forfeit] {exc.stage_id}: no consensus after '
                f'{exc.rounds} round(s), so '
                f'{target or "its candidate"} is withdrawn. '
                f'{", ".join(survivors)} cleared, so the run continues '
                f'with the remaining candidate(s) — the judge is now '
                f'choosing from a REDUCED field, which is recorded with '
                f'the selection.'
            )
        return True

    def forfeit_to_a_judge(
        self, stage: pipeline.PipelineStage, exc: _Blocked
    ) -> bool:
        """
        Withdraw a top-level review's candidate if a judge has rivals.

        The forfeit used to be reached only for reviews grouped under
        ``parallel:``, while both shipped race examples declare their
        reviews as top-level stages. One candidate that could not
        converge then ended the whole run, and its rival's review never
        started (#47).

        Reviews run in order here, so whether a rival clears is not yet
        known. The candidate is withdrawn and the run continues. A
        candidate is only ever withdrawn while a rival remains, so the
        review that would withdraw the last one finds no rival and
        blocks the run itself, and a judge is never left with none.

        :param stage: The top-level review stage that blocked.
        :param exc: Its ``_Blocked``.
        :returns: Whether the run may continue without the candidate.
        """
        target = stage.on_block
        if not target:
            return False
        rivals = self.rival_candidates(target)
        if not rivals:
            return False
        with self.ctx.lock:
            self.forfeited.update((stage.id, target))
            self.forfeits[target] = (stage.id, exc.rounds)
        click.echo(
            f'[forfeit] {stage.id}: no consensus after {exc.rounds} '
            f'round(s), so {target} is withdrawn. The judge still has '
            f'{", ".join(rivals)}; if none of them clears review, the run '
            f'blocks. The reduced field is recorded with the selection.'
        )
        return True

    def rival_candidates(self, target: str) -> list[str]:
        """
        Candidates still competing with *target* at any judge.

        Read from the DECLARED pipeline: a rival whose review has not
        run yet still counts.

        :param target: The writer node being withdrawn.
        :returns: Rival writer ids not already withdrawn, in order.
        """
        rivals: list[str] = []
        for judge in self.ctx.stage_by_id.values():
            if stage_kind(judge) != 'judge' or target not in judge.needs:
                continue
            for need in judge.needs:
                declared = self.ctx.stage_by_id.get(need)
                if (
                    need != target
                    and need not in self.forfeited
                    and need not in rivals
                    and declared is not None
                    and stage_kind(declared) == 'writer'
                ):
                    rivals.append(need)
        return rivals

    def judge_candidates(
        self, stage: pipeline.PipelineStage
    ) -> list[str]:
        """
        The competing writer nodes a judge compares, from its needs.

        :param stage: The judge stage.
        :returns: Each need that is a writer with a branch, and has not
            forfeited, in order.
        """
        return [
            need
            for need in stage.needs
            if self.ctx.nodes.get(need)
            and self.ctx.nodes[need].branch
            and self.ctx.nodes[need].kind == 'writer'
            # A candidate whose review never reached consensus
            # forfeited; judging it would compare a vetted branch
            # against an unvetted one and call the result a choice.
            and need not in self.forfeited
        ]
