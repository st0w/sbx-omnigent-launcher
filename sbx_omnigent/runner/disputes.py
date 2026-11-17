"""Disputes: a stage saying its contract cannot be met stops the run.

A ``DISPUTED:`` line means the contract itself is impossible, not that
the work is wrong. Re-driving the writer cannot fix that, so whichever
stage raises one, writer, reader, contract check, plan check or a
blocking reviewer, the run stops with the claims named and the
reasoning kept.
"""

from __future__ import annotations

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.captures import TurnCapture
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.parsing.verdicts import (
    dispute_near_misses,
    parse_disputes,
)
from sbx_omnigent.runner.records.results import NodeResult


class Disputes:
    """
    Halt the run on a dispute, and warn on one that cannot be read.

    :param ctx: The run.
    :param capture: Keeps the turn that raised a dispute, since the
        halt is about to end the run and dispose its VM.
    """

    def __init__(self, ctx: RunContext, *, capture: TurnCapture) -> None:
        self.ctx = ctx
        self._capture = capture

    def warn_near_miss(self, label: str, text: str | None) -> None:
        """
        Say so when a reply declares a dispute the parser cannot read.

        WARNS and returns. Halting here would mean acting on the bare
        word, which is the false-positive risk
        :data:`~sbx_omnigent.runner.parsing.verdicts._DISPUTE_RE`
        was tightened to avoid; saying nothing is what let a correct
        refusal be relayed until the round budget died, with nobody
        told why.

        :param label: The stage or reviewer whose reply it was.
        :param text: That reply.
        """
        missed = dispute_near_misses(text)
        if not missed:
            return
        click.echo(
            f'[dispute] {label}: its reply declares a dispute the '
            f'orchestrator cannot read, so the run is CONTINUING and '
            f'will keep relaying this finding until the round budget '
            f'is gone. A dispute is only heard as `DISPUTED:` at the '
            f'START OF A LINE, followed by the claim — in a heading or '
            f'as a label it is invisible. Found: '
            + '; '.join(repr(line) for line in missed)
        )

    def halt_on_writer(self, stage: pipeline.PipelineStage) -> None:
        """
        Stop when a WRITER says its own contract cannot be satisfied.

        :meth:`halt_on_review` covers the review path, but a writer
        meets an impossible contract first: it is the party that has to
        satisfy it. Its dispute was parsed by nothing and the run
        carried on to discover the same thing the expensive way.

        Observed on `ingestion-m2-3`. An implementer reported three
        unsatisfiable tests with file:line — a fixture value one test
        required persisted and another forbade, a SQL alias on the
        reserved word ``constraint`` that cannot parse, and a
        frozen-API call with the wrong signature. Every one correct.
        The run continued 51 minutes, reviewed both candidates and
        forfeited with nothing published; the sibling had made that same
        broken test pass by EDITING A FROZEN MODULE, which is the breach
        the frozen boundary exists to prevent. An impossible contract
        does not merely fail a candidate — it pushes one into the
        violation.

        Called AFTER the stage commits, so the disputed work is durable
        and a resume keeps it (that implementer had 2,013 insertions on
        its branch), and BEFORE the tests-only and
        implementation-present gates, which answer a dispute by
        re-driving a writer that cannot act on it.

        :param stage: The writer stage that just finished.
        :raises PipelineRunError: When the writer raised a dispute.
        """
        claims = self._stage_disputes(stage, 'disputed its own contract')
        if not claims:
            return
        listed = '\n'.join(f'  {claim}' for claim in claims)
        raise PipelineRunError(
            f'{stage.id}: the writer raised {len(claims)} dispute(s) — it '
            f'says its own contract is impossible, not that its code is '
            f'wrong:\n\n{listed}\n\nRe-driving {stage.id} cannot '
            f'resolve this: it is not the party that can change what is '
            f'disputed. A contract no implementation can satisfy does not '
            f'just fail this stage — it pressures the next one into '
            f'breaking a frozen boundary to get green. Its full reply is '
            f'in the run directory under turns/; read it, settle the '
            f'contract, and re-run.'
        )

    def halt_on_reader(self, stage: pipeline.PipelineStage) -> None:
        """
        Stop when a READER says the contract cannot be satisfied.

        Every reader turn ends with
        :data:`~sbx_omnigent.runner.prompts.writer._UNATTENDED`, which
        tells it to stop on a ``DISPUTED:`` line when a conflict cannot
        be resolved, and nothing read it: a reader that did as told was
        ignored, and the writers after it built against the contract it
        had just disputed. A read-only check of the tests before the
        writers (#50), or of the plan's claims after it (#49), is only
        worth running if its dispute stops the run.

        Called before the stage completes, so a resume runs the reader
        again once the contract is settled. Not for the planner: it is
        never told about the marker, and a human approves its plan.

        :param stage: The reader stage that just finished.
        :raises PipelineRunError: When the reader raised a dispute.
        """
        claims = self._stage_disputes(stage, 'disputed the contract')
        if not claims:
            return
        listed = '\n'.join(f'  {claim}' for claim in claims)
        role = 'contract check' if stage.checks else 'reader'
        raise PipelineRunError(
            f'{stage.id}: the {role} raised {len(claims)} dispute(s) — it '
            f'says the contract the stages after it would build on cannot '
            f'be satisfied:\n\n{listed}\n\nNothing after {stage.id} has '
            f'run. Its full reply is in the run directory under turns/; '
            f'read it, settle the contract, and re-run. A --resume runs '
            f'{stage.id} again.'
        )

    def halt_on_plan(
        self,
        stage: pipeline.PipelineStage,
        disputes: list[tuple[str, str]],
    ) -> None:
        """
        Stop when a plan nobody can revise has disputed claims.

        Unattended planning, or a plan finished in an earlier attempt:
        there is no human in the planning session to settle a dispute.

        :param stage: The plan stage.
        :param disputes: ``(check id, claim)`` per dispute.
        :raises PipelineRunError: When there is any dispute.
        """
        if not disputes:
            return
        listed = '\n'.join(f'  [{cid}] {claim}' for cid, claim in disputes)
        raise PipelineRunError(
            f'{stage.id}: the plan check raised {len(disputes)} '
            f'dispute(s) — a claim the repository contradicts, or two '
            f'sections that contradict each other:\n\n{listed}\n\n'
            f'Nothing after the plan has run, and nobody is in the '
            f'planning session to settle it. The full reports are in the '
            f'run directory under reviews/. Correct the brief, or plan '
            f'interactively, and re-run.'
        )

    def halt_on_review(
        self,
        stage: pipeline.PipelineStage,
        outputs: dict[str, str],
        blocking: list[str],
    ) -> None:
        """
        Stop when a blocking reviewer says the CONTRACT cannot be met.

        A BLOCKING verdict means the code is wrong, and re-driving the
        writer is the answer. A dispute means the writer's own contract
        is impossible — a test no implementation can pass, two stages
        disagreeing — and re-driving the writer cannot resolve it: it is
        not the party able to change the thing at fault.

        Without this the run spends its whole review budget relaying an
        unanswerable finding. Both [m2] campaigns did that: 9 and
        10 correct disputes, ignored, four blocked rounds on every
        candidate, and every candidate forfeited with nothing published.

        Only a BLOCKING reviewer's dispute counts. One raised beside an
        APPROVAL is an observation, and the run is not stuck on it.

        :param stage: The review stage that just blocked.
        :param outputs: Each reviewer's reply, by agent name.
        :param blocking: The reviewers that returned BLOCKING.
        :raises PipelineRunError: When a blocking reviewer disputes.
        """
        disputes = [
            (name, claim)
            for name in blocking
            for claim in parse_disputes(outputs.get(name))
        ]
        if not disputes:
            # Only a BLOCKING reviewer's, for the same reason the halt
            # is: one raised beside an approval is an observation, and
            # the run is not stuck on it.
            for name in blocking:
                self.warn_near_miss(name, outputs.get(name))
            return
        self.ctx.nodes[stage.id] = NodeResult(
            stage.id, 'review', verdict='BLOCKING'
        )
        listed = '\n'.join(
            f'  [{name}] {claim}' for name, claim in disputes
        )
        target = stage.on_block or 'the writer'
        click.echo(
            f'[review] {stage.id}: HALTING — a blocking reviewer says the '
            f'contract itself cannot be satisfied. Re-driving {target} '
            f'cannot fix that, so the remaining rounds would be spent '
            f'relaying it.'
        )
        raise PipelineRunError(
            f'{stage.id}: {len(disputes)} dispute(s) raised alongside a '
            f'BLOCKING verdict — a stage says its own contract is '
            f'impossible, not that the code is wrong:\n\n{listed}\n\n'
            f'Re-driving {target} cannot resolve this: it is not the '
            f'party that can change what is disputed. Read the reviewer '
            f'reports, settle the contract, and re-run.'
        )

    def _stage_disputes(
        self, stage: pipeline.PipelineStage, captured_as: str
    ) -> tuple[str, ...]:
        """
        The disputes in a finished stage's reply, its reply captured.

        Warns on a near miss and returns nothing, so the caller only
        has to word its halt.

        :param stage: The writer or reader stage that just finished.
        :param captured_as: Why the turn is captured, for its record.
        :returns: The claims, or ``()`` when there are none.
        """
        node = self.ctx.nodes.get(stage.id)
        output = node.output if node else None
        claims = parse_disputes(output)
        if not claims:
            self.warn_near_miss(stage.id, output)
            return ()
        # The claim after the marker is one line. The REASONING that
        # makes it actionable — which test, which two clauses, why no
        # code satisfies both — is the rest of the reply, and this
        # stage is about to end the run and dispose the VM. Capture
        # before raising, the same reason plan approval does.
        # Live on `ingestion-m2-5` this was missing: the halt fired
        # correctly and the human was left with "m2 cannot be marked
        # complete under the binding constraints" and no way to learn
        # why.
        if node is not None and node.session:
            self._capture.turn(node.session, captured_as)
        return claims
