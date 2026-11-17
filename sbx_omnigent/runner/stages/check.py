"""The check stage: reading a contract or a plan before anyone builds.

A ``checks:`` stage reads what an earlier stage froze, read-only, and
says whether anything could satisfy it. Naming a tests stage, it reads
the suite beside the brief and the plan before two writers each spend
a stage building against it (#50). Naming a planner, it checks the
plan's claims against the repository inside the plan's approval (#49).
"""

from __future__ import annotations

from collections.abc import Callable

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.briefing import Briefing
from sbx_omnigent.runner.captures import TurnCapture
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.disputes import Disputes
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.lineage import seed_from, seed_ref
from sbx_omnigent.runner.parsing.findings import parse_findings
from sbx_omnigent.runner.parsing.verdicts import (
    _check_done,
    check_finished,
    parse_disputes,
)
from sbx_omnigent.runner.prompts.checks import (
    _CHECK_COMPLETE,
    _CHECK_DISPUTED,
    _CHECK_FINISH_ASK,
    _CHECK_RULES,
    _CHECK_VERDICT,
    _PLAN_CHECK_RULES,
)
from sbx_omnigent.runner.prompts.environment import setup_block
from sbx_omnigent.runner.prompts.review import _REVIEW_MOUNT
from sbx_omnigent.runner.prompts.writer import _DISPOSABLE_VM, _UNATTENDED
from sbx_omnigent.runner.records.results import NodeResult
from sbx_omnigent.runner.review_book import ReviewBook
from sbx_omnigent.runner.session_driver import SessionDriver
from sbx_omnigent.runner.stage_kinds import is_planner


class CheckStage:
    """
    Run contract checks and plan checks.

    :param ctx: The run.
    :param book: Records each check beside the reviewers' reports.
    :param driver: Creates the check's session and drives its turns.
    :param capture: Keeps the turn of a check that never finished.
    :param briefing: The increment preamble and upstream designs.
    :param disputes: Stops the run on a dispute.
    :param resume: Whether this is a ``--resume``, whose contract check
        replaces the clone an earlier attempt left.
    :param release_session: Frees a finished plan check's microVM.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        book: ReviewBook,
        driver: SessionDriver,
        capture: TurnCapture,
        briefing: Briefing,
        disputes: Disputes,
        resume: bool,
        release_session: Callable[[pipeline.PipelineStage, str], None],
    ) -> None:
        self.ctx = ctx
        self._book = book
        self._driver = driver
        self._capture = capture
        self._briefing = briefing
        self._disputes = disputes
        self._resume = resume
        self._release_session = release_session

    def run(self, stage: pipeline.PipelineStage) -> None:
        """
        Read the frozen contract once, before any writer builds on it.

        A ``checks:`` stage reads the tests stage's suite beside the
        brief and the plan (#50). A contract that cannot be satisfied is
        a dispute, and the run stops here, before two writers spend a
        full stage each on it. Weaker-but-satisfiable problems (a test
        that already passes, one asserting on source text, a requirement
        with no test) are recorded in its ``FINDINGS:`` block, and the
        run goes on.

        A check that lets the writers go on is recorded beside the
        reviewers' reports, so its findings reach the findings ledger
        and the verifier like theirs. ``turns/`` alone would lose them:
        a completed run deletes its run directory. It never reaches the
        writers as design context — :meth:`Briefing.reader_context`
        passes on readers only — and it is not counted as a vote on the
        branch.

        :param stage: The stage declaring ``checks:``.
        :raises PipelineRunError: When the check raised a dispute.
        """
        assert stage.checks is not None
        target = self.ctx.stage_by_id[stage.checks]
        if is_planner(target, self.ctx.config):
            # Reached only when the plan finished in an earlier attempt:
            # a live plan stage runs its checks inside its approval.
            node = self.ctx.nodes.get(target.id)
            plan = node.output if node is not None else ''
            self._disputes.halt_on_plan(
                target,
                [
                    (stage.id, claim)
                    for claim in self.run_plan_check(stage, plan)
                ],
            )
            return
        wt = self.ctx.worktrees.create_node_worktree(
            self.ctx.run_id, stage.id, from_node=stage.checks,
            replace=self._resume,
        )
        session = self._driver.create(stage.run[0], wt, 'ro', stage.id)
        first = self._driver.drive(
            session, self.instruction(stage), done=_check_done
        )
        out = self._finish(stage.id, session, first)
        self.ctx.nodes[stage.id] = NodeResult(
            stage.id, 'check', worktree=wt, session=session, output=out
        )
        self._disputes.halt_on_reader(stage)
        self._halt_on_unfinished(stage, session, out)
        self._book.record_review(
            stage.id, stage.run[0], self._book.next_round(stage.id),
            _CHECK_VERDICT, session, out,
        )
        findings = parse_findings(out)
        click.echo(
            f'[check] {stage.id}: no dispute, so the writers go ahead; '
            f'{len(findings)} finding(s) recorded with the review '
            f'reports.'
        )

    def run_plan_check(
        self, check: pipeline.PipelineStage, plan: str
    ) -> tuple[str, ...]:
        """
        Drive one plan check on a fresh read-only clone, and record it.

        A fresh clone each round: a re-check after the human settled a
        dispute must not read a tree the last round's agent touched.

        The clone is cut from the planner's own seed, so the check reads
        exactly the tree the plan was written against: in every module,
        every round, and on a resume that runs only the check. Its own
        seed would be wrong in a campaign: a check is not seeded from
        the campaign tip, and its only need is a planner, which has no
        branch, so it fell back to the base branch. Live, a later
        module's check then reported the earlier modules' work missing.

        :param check: The check stage.
        :param plan: The plan to check.
        :returns: Its disputes; empty when it found none.
        """
        assert check.checks is not None
        planner = self.ctx.stage_by_id[check.checks]
        wt = self.ctx.worktrees.create_node_worktree(
            self.ctx.run_id, check.id,
            from_node=seed_from(planner, self.ctx.nodes),
            replace=True,
        )
        session = self._driver.create(check.run[0], wt, 'ro', check.id)
        out = self._finish(
            check.id, session,
            self._driver.drive(
                session,
                self.plan_instruction(check, plan),
                done=_check_done,
            ),
        )
        claims = parse_disputes(out)
        if not claims and not check_finished(out):
            # Counted as a problem: interactive approval reopens with
            # the human told why, and unattended planning stops.
            claim = (
                f'{check.id} did not finish: its reply never ended with '
                f'{_CHECK_COMPLETE}, so the plan was not fully checked.'
            )
            claims = (claim,)
            out = f'{claim}\n\nIts last reply:\n\n{out}'
        elif not claims:
            self._disputes.warn_near_miss(check.id, out)
        self.ctx.nodes[check.id] = NodeResult(
            check.id, 'check', worktree=wt, session=session, output=out
        )
        self._book.record_review(
            check.id, check.run[0], self._book.next_round(check.id),
            _CHECK_DISPUTED if claims else _CHECK_VERDICT, session, out,
        )
        self._release_session(check, 'check')
        return claims

    def plan_checks(
        self, stage: pipeline.PipelineStage
    ) -> list[pipeline.PipelineStage]:
        """
        The ``checks:`` stages that name *stage*, a planner.

        Looked up by id across every registered stage, so a campaign's
        renamed per-module check finds its own module's plan.

        :param stage: The plan stage.
        :returns: The checks naming it, in declaration order.
        """
        return [
            check for check in self.ctx.stage_by_id.values()
            if check.checks == stage.id
        ]

    def _finish(self, check_id: str, session: str, out: str) -> str:
        """
        Ask a check once more when it neither disputed nor finished.

        A dispute is never asked again: the claim is the evidence, and
        it stops the run or reopens approval either way. Only a pass
        has to be said, so only a reply with neither is sent
        :data:`_CHECK_FINISH_ASK`, and its answer replaces the reply.

        :param check_id: The check stage's id.
        :param session: Its session, still alive.
        :param out: Its reply.
        :returns: The reply to judge: *out*, or the answer to the ask.
        """
        if parse_disputes(out) or check_finished(out):
            return out
        click.echo(
            f'[check] {check_id}: its reply neither raised a dispute nor '
            f'ended with {_CHECK_COMPLETE}, so it has not finished; '
            f'asking it once to finish.'
        )
        return self._driver.drive(session, _CHECK_FINISH_ASK, done=_check_done)

    def _halt_on_unfinished(
        self, stage: pipeline.PipelineStage, session: str, out: str
    ) -> None:
        """
        Stop when a contract check never said it finished.

        Called after the dispute halt, so a reply reaching here has no
        dispute. Without :data:`_CHECK_COMPLETE` it is not a pass, so
        nothing after the check may run on it. The stage is not
        recorded as complete, so ``--resume`` runs it again.

        :param stage: The contract check stage.
        :param session: Its session, still alive, to capture.
        :param out: Its reply after the one ask to finish.
        :raises PipelineRunError: When the reply never said it finished.
        """
        if check_finished(out):
            return
        self._capture.turn(session, 'did not finish its check')
        raise PipelineRunError(
            f'{stage.id}: the contract check did not finish. Neither its '
            f'reply nor the one it was asked for ends with a line reading '
            f'{_CHECK_COMPLETE}, so it cannot count as a pass, and nothing '
            f'after {stage.id} has run. Its reply is in the run directory '
            f'under turns/. A --resume runs {stage.id} again.'
        )

    def instruction(self, stage: pipeline.PipelineStage) -> str:
        """
        Frame a contract check's turn: check it, and build nothing.

        Carries everything the tests stage worked from and what it
        wrote, so the check reads the same contract the writers will
        get. The halting rules are in the turn rather than only in the
        template, so a pipeline that gives the checker its own prompt
        still stops on a contract that cannot be met.

        :param stage: The stage declaring ``checks:``.
        :returns: The turn text.
        """
        assert stage.checks is not None
        target = self.ctx.stage_by_id[stage.checks]
        try:
            changed = self.ctx.worktrees.node_diff_files(
                self.ctx.run_id, target.id, against=seed_ref(self.ctx, target)
            )
        except click.ClickException:
            changed = []
        files = (
            '\n'.join(f'  - {path}' for path in sorted(changed))
            or '  (none could be listed; read the branch history)'
        )
        design = self._briefing.reader_context(target)
        acceptance = (
            f'\n\nAcceptance contract:\n{self.ctx.config.acceptance}'
            if self.ctx.config.acceptance
            else ''
        )
        preamble = self._briefing.chunk_preamble()
        return (
            f'{preamble}CHECK THE CONTRACT before anyone '
            f'builds against it. Do NOT implement anything and do not '
            f'change the tests: your mount is read-only, and the writers '
            f'start only after you reply. Stage {target.id!r} wrote a '
            f'frozen test suite for the brief below; two writers will '
            f'each build against it. Read the brief, the acceptance '
            f'contract, the plan and the suite together, run the suite '
            f'against this unmodified tree, and decide whether any '
            f'implementation could satisfy all of them.\n\n'
            f'Brief:\n{self.ctx.config.task}{acceptance}'
            + (f'\n\n{design}' if design else '')
            + f'\n\nFiles {target.id!r} added or changed:\n{files}'
            f'{_CHECK_RULES}{_REVIEW_MOUNT}'
            f'\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            + setup_block(self.ctx.config)
        )

    def plan_instruction(
        self, stage: pipeline.PipelineStage, plan: str
    ) -> str:
        """
        Frame a plan check's turn: check its facts, build nothing.

        The plan is in the turn because it is not in the tree yet: the
        check runs before the plan of record is committed.

        :param stage: The stage declaring ``checks: <planner stage>``.
        :param plan: The consolidated plan the human approved.
        :returns: The turn text.
        """
        acceptance = (
            f'\n\nAcceptance contract:\n{self.ctx.config.acceptance}'
            if self.ctx.config.acceptance
            else ''
        )
        preamble = self._briefing.chunk_preamble()
        return (
            f'{preamble}CHECK THE PLAN before it becomes the '
            f'plan of record. Do NOT implement anything: your mount is '
            f'read-only, and it is the repository as it stands before any '
            f'of the plan is built. A human has approved the plan below, '
            f'pending this check; every later stage will build on it.'
            f'\n\nBrief:\n{self.ctx.config.task}{acceptance}'
            f'\n\nThe plan ({stage.checks!r}):\n{plan}'
            f'{_PLAN_CHECK_RULES}{_REVIEW_MOUNT}'
            f'\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            + setup_block(self.ctx.config)
        )
