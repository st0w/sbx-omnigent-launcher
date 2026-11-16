"""The pre-publish verification gate, and looping a writer back to it.

Every other gate downstream of a writer is an agent's word. This one
runs the project's own test/coverage command in a clean, disposable
sandbox on exactly the branch that would publish, and believes only the
exit status — see :mod:`sbx_omnigent.verify` for why it cannot run on
the host or in an agent's own VM.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

import click

from sbx_omnigent import pipeline, verify
from sbx_omnigent.defaults import DEFAULT_EGRESS_ALLOW, DEFAULT_HOST_IMAGE
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.prompts.environment import setup_block
from sbx_omnigent.runner.prompts.writer import (
    _FIX_MUST_VERIFY,
    _FIX_NO_WEAKENING,
)
from sbx_omnigent.runner.redrive import RedriveWriter
from sbx_omnigent.runner.stage_kinds import stage_kind

#: Stage kinds that check a writer's branch, and so run again after a
#: verify-gate fix changes it (see :meth:`VerifyGate.rechecks_after`).
_RECHECK_KINDS = frozenset({'review', 'scan', 'verify'})


class VerifyGate:
    """
    Run the gate on a branch, and loop its writer back until it passes.

    A failure is closed by the writer that PRODUCED the branch (the
    refactor node, typically): it is the only writer that runs after
    an implementation exists, so adding tests there cannot invalidate a
    competition that already happened. Then every stage after it that
    checks its branch (review, scan, triage) runs again in pipeline
    order, on the changed branch, before the gate re-runs (see
    :meth:`rechecks_after`).

    :param ctx: The run.
    :param egress: The operator's egress allowlist, or ``None`` for the
        built-in default.
    :param redrive_writer: Re-drives a writer stage with findings.
    :param rerun_stage: Runs a stage again from scratch, its earlier
        completion cleared.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        egress: tuple[str, ...] | None,
        redrive_writer: RedriveWriter,
        rerun_stage: Callable[[pipeline.PipelineStage], None],
    ) -> None:
        self.ctx = ctx
        self._egress = egress
        self._redrive_writer = redrive_writer
        self._rerun_stage = rerun_stage

    def check(
        self, winner: str, stages: Sequence[pipeline.PipelineStage]
    ) -> verify.VerifyOutcome | None:
        """
        Run the gate on *winner*'s branch; loop back until it passes.

        :param winner: The node whose branch is about to publish.
        :param stages: The stages that ran, in order: the module's in a
            campaign, else the build stages.
        :returns: The passing outcome, the evidence a pull request
            shows, or ``None`` when no gate is configured.
        :raises PipelineRunError: If the gate cannot be run, cannot be
            fixed by anyone, or still fails after the round cap — in
            every case WITHOUT publishing.
        """
        spec = self.ctx.config.verify
        if spec is None:
            return None
        fixer = self._fixer(winner)
        max_rounds = self.ctx.max_rounds
        for attempt in range(1, max_rounds + 1):
            try:
                outcome = self._run(winner, spec)
            except verify.VerifyError as exc:
                # Infrastructure, not a verdict: it says nothing about
                # the branch, so never loop a writer back over it.
                raise PipelineRunError(
                    f'could not run the verification gate for '
                    f'{winner!r}: {exc}'
                ) from exc
            if outcome.ok:
                ran = ', '.join(s.label for s in outcome.steps) or 'gate'
                click.echo(f'[verify] {winner}: passed ({ran}).')
                return outcome
            why = (
                'timed out'
                if outcome.timed_out
                else f'exited {outcome.exit_code}'
            )
            if fixer is None:
                raise PipelineRunError(
                    f'the verification gate failed on {winner!r} ({why}) '
                    f'and no writer stage produced that branch, so '
                    f'nothing in the pipeline can close the gap. Not '
                    f'publishing.\n\n{outcome.output}'
                )
            if attempt == max_rounds:
                raise PipelineRunError(
                    f'the verification gate still fails on {winner!r} '
                    f'({why}) after {max_rounds} attempt(s). Not '
                    f'publishing.\n\n{outcome.output}'
                )
            click.echo(
                f'[verify] {winner}: gate FAILED ({why}) — re-driving '
                f'{fixer} to close the gap.'
            )
            self._redrive_writer(
                fixer,
                self.findings(spec, outcome, winner),
                message=f'{fixer}: close the verification gap',
            )
            # The branch changed, so everything that checked it checks
            # it again before the gate re-runs.
            for stage in self.rechecks_after(fixer, stages):
                self._rerun_stage(stage)
        return None

    def _run(
        self, winner: str, spec: pipeline.VerifySpec
    ) -> verify.VerifyOutcome:
        """
        Cut a clean clone of *winner*'s branch and verify it.

        A FRESH clone, not the writer's own worktree: it verifies the
        committed state that actually ships, and inherits none of the
        multi-gigabyte build output the writer left behind.

        The gate is the one check that must not take an agent's word,
        so it builds from clean, in a copy on its sandbox's own disk
        (:func:`verify.on_own_disk`).

        :param winner: The node whose branch to verify.
        :param spec: The configured gate.
        :returns: The :class:`~sbx_omnigent.verify.VerifyOutcome`.
        """
        node_id = f'{winner}-verify'
        run_id = self.ctx.run_id
        workspace = self.ctx.worktrees.create_node_worktree(
            run_id, node_id, from_node=winner, replace=True,
        )
        click.echo(
            f'[verify] {winner}: running the gate in a disposable '
            f'sandbox (this installs a toolchain and builds from clean).'
        )
        return verify.run_verification(
            name=verify.sandbox_name(run_id, node_id),
            workspace=workspace,
            # spec.setup, NOT config.setup: the latter is prose
            # addressed to an agent and is not runnable (see
            # verify.build_script).
            script=verify.build_script(spec.setup, spec.rendered()),
            demo_script=(
                verify.build_script(spec.setup, demo)
                if (demo := spec.rendered_demo())
                else None
            ),
            # The pipeline's image when it has one: the gate then runs
            # on the same toolchain every agent built with (#38).
            image=(
                pipeline.image_tag(self.ctx.config)
                or spec.image
                or DEFAULT_HOST_IMAGE
            ),
            # The operator's list, not the built-in default: the gate
            # got the default even under `egress_allow: []` (#41).
            egress=(
                DEFAULT_EGRESS_ALLOW if self._egress is None else self._egress
            ),
            # The gate builds its own sandbox, so the SERVER's
            # per-sandbox limits never reach it — these are the only
            # way it gets any.
            cpus=spec.cpus,
            memory=spec.memory,
            timeout_s=spec.timeout_s,
        )

    def _fixer(self, winner: str) -> str | None:
        """The writer stage that produced *winner*, if it is one."""
        stage = self.ctx.stage_by_id.get(winner)
        if stage is not None and stage_kind(stage) == 'writer':
            return winner
        return None

    def findings(
        self,
        spec: pipeline.VerifySpec,
        outcome: verify.VerifyOutcome,
        winner: str = 'gate',
    ) -> str:
        """
        The evidence handed to the writer closing a gate failure.

        The excerpt is capped, and a capped excerpt is exactly what a
        noisy suite defeats — so the WHOLE capture is written beside
        the run first and the finding names the path. A writer told to
        "close the gap" without being shown the gap is being asked to
        guess, and this run produced that: 6000 characters of Postgres
        checkpoint logs and not one line naming a failing test.

        :param spec: The verify spec that ran.
        :param outcome: What the gate reported.
        :param winner: Node whose branch was gated, for the filename.
        :returns: The instruction text.
        """
        why = (
            'did not finish within its budget'
            if outcome.timed_out
            else f'exited {outcome.exit_code}'
        )
        where = ''
        full = '\n\n'.join(
            f'=== {st.label} (exit {st.exit_code}) ===\n'
            f'{st.full_output or st.output}'
            for st in outcome.steps
        )
        relpath = f'turns/{winner}.verify.txt'
        if full.strip() and self.ctx.worktrees.write_run_artifact(
            self.ctx.run_id, relpath, full
        ):
            where = (
                f'\n\nThe excerpt below is capped. The COMPLETE gate '
                f'output is on the host that drove this run, at '
                f'<run-dir>/{relpath} — if the excerpt does not show '
                f'you which test failed, say so in your reply rather '
                f'than guessing at a fix.'
            )
        return (
            'The pre-publish VERIFICATION GATE failed on your branch. '
            "This is not a reviewer's opinion: it ran mechanically, "
            'in '
            'a clean disposable sandbox, against exactly the branch that '
            f'would publish.\n\nCommand:\n{spec.rendered()}\n\nIt '
            f'{why}.{where} Output:\n{outcome.output}\n\nClose the '
            'gap by '
            'ADDING tests. You may add new tests; you may NOT modify, '
            'weaken, skip, or delete an existing test, and you may NOT '
            'delete or hollow out production code to make a coverage '
            'number go up — either of those is worse than the shortfall '
            'you were asked to fix. If some uncovered code genuinely '
            'cannot be exercised at this layer, say so explicitly and '
            'explain why rather than finishing quietly.'
            f'\n\n{_FIX_NO_WEAKENING}\n\n{_FIX_MUST_VERIFY}'
            + setup_block(self.ctx.config)
        )

    def rechecks_after(
        self, fixer: str, stages: Sequence[pipeline.PipelineStage]
    ) -> list[pipeline.PipelineStage]:
        """
        The stages that check *fixer*'s branch, in the order they run.

        Every review, scan and triage (``verifies:``) stage that depends
        on *fixer*, through ``needs`` or ``from``, directly or through
        other stages. A gate fix changes the branch each of them
        checked, so each runs again. Only the first review used to,
        which let a gate fix ship unscanned and untriaged. Writers,
        judges and readers are left alone: they produce work rather
        than check it, and re-running a judge could change what ships.

        :param fixer: The writer the gate re-drove.
        :param stages: The stages that ran, in order.
        :returns: The stages to run again, in pipeline order.
        """
        flat = [
            stage for stage in pipeline._iter_stages(tuple(stages))
            if stage.run and not stage.parallel
        ]
        by_id = {stage.id: stage for stage in flat}
        return [
            stage for stage in flat
            if stage_kind(stage) in _RECHECK_KINDS
            and _depends_on(stage.id, fixer, by_id)
        ]


def _depends_on(
    stage_id: str,
    target: str,
    by_id: dict[str, pipeline.PipelineStage],
) -> bool:
    """
    Whether *stage_id* depends on *target*, by ``needs`` or ``from``.

    :param stage_id: The stage to ask about.
    :param target: The stage it may depend on.
    :param by_id: The stages to follow edges through.
    :returns: ``True`` when *target* is upstream of *stage_id*.
    """
    seen: set[str] = set()
    pending = [stage_id]
    while pending:
        stage = by_id.get(pending.pop())
        if stage is None or stage.id in seen:
            continue
        seen.add(stage.id)
        upstream = [*stage.needs]
        if stage.from_branch:
            upstream.append(stage.from_branch)
        if target in upstream:
            return True
        pending.extend(upstream)
    return False
