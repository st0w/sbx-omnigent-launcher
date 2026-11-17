"""The review book: every vote and judge pick, and each stage's budget.

A reviewer's or judge's session is disposed moments after it decides,
and disposing a session deletes its transcript. What it decided, and
why, is written down here first: to the run directory at once, and
into the run state, so the copy committed at publish survives a resume.
"""

from __future__ import annotations

from collections.abc import Callable

import click

from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.lineage import review_target
from sbx_omnigent.runner.parsing.findings import (
    FindingSections,
    report_names_nothing,
)
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.records.judging import JudgePick
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.render.judging import render_judge_decision
from sbx_omnigent.runner.render.reviews import render_review_records
from sbx_omnigent.swarm_session import SwarmSessionError


class ReviewBook:
    """
    The run's recorded votes and judge picks, and its round budgets.

    :param ctx: The run.
    :param progress: The run's progress; records belong to its active
        module.
    :param save_state: Persists the run's state after a vote.

    :ivar reviews: Every reviewer vote cast this run, with the report
        behind it. Buffered rather than written to a branch as it
        happens: a competition is still live during review, so nothing
        may commit to a candidate's branch mid-flight. Written to the
        run dir immediately and committed to the winner at publish.
    :ivar picks: The judge's choices, so the two-writer race can be
        evaluated over a series of modules rather than trusted.
    :ivar reviewed_ok: Writer nodes whose review stage reached APPROVED
        consensus, so a judge can be told its candidates are already
        verified and need not be re-built. Persisted with the run
        state: losing it on a resume left a judge untold, and an
        untold judge starts a workspace build it cannot finish inside
        its one turn. Re-driving a writer drops it from here the
        moment its branch changes, which keeps the claim honest.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        progress: RunProgress,
        save_state: Callable[[], None],
    ) -> None:
        self.ctx = ctx
        self._progress = progress
        self._save_state = save_state
        self.reviews: list[ReviewRecord] = []
        self.picks: list[JudgePick] = []
        self.reviewed_ok: set[str] = set()
        #: Review rounds spent per stage, THIS PROCESS.
        #:
        #: Deliberately not in the run state. The cap bounds the
        #: branch, and a resume is a deliberate human act that buys
        #: another budget — persisting this would reverse that. See
        #: :meth:`spend_round`.
        self._rounds: dict[str, int] = {}

    def rounds_spent(self, stage_id: str) -> int:
        """
        How many rounds a stage has spent in this process.

        :param stage_id: The review stage.
        :returns: The count, ``0`` before its first.
        """
        with self.ctx.lock:
            return self._rounds.get(stage_id, 0)

    def add(self, record: ReviewRecord) -> None:
        """
        Keep a vote, write it to the run directory, and save the state.

        :param record: The vote.
        """
        # Reviewers of a stage vote concurrently, and the round number
        # is derived by WALKING this list.
        with self.ctx.lock:
            self.reviews.append(record)
        self.ctx.worktrees.write_run_artifact(
            self.ctx.run_id,
            f'reviews/{record.slug}.md',
            render_review_records([record], title=f'Review — {record.label}'),
        )
        self._save_state()

    def module_reviews(self) -> list[ReviewRecord]:
        """:returns: The votes belonging to the publishing module."""
        module = self._progress.module_id
        return [r for r in self.reviews if r.chunk == module]

    def module_picks(self) -> list[JudgePick]:
        """:returns: The publishing module's judge picks."""
        module = self._progress.module_id
        return [p for p in self.picks if p.chunk == module]

    def next_round(self, stage_id: str) -> int:
        """
        The next round number for a review stage, across re-entries.

        Derived from the records themselves rather than a counter, so it
        survives a resume for free: the votes are restored from the
        run state, and a resumed run continues the numbering instead of
        starting over and overwriting what the earlier attempt wrote.

        :param stage_id: The review stage's id (already namespaced per
            module, so modules cannot collide).
        :returns: One past the highest round recorded for that stage.
        """
        with self.ctx.lock:
            prior = [
                r.round_no for r in self.reviews
                if r.stage == stage_id
            ]
        return max(prior, default=0) + 1

    def spend_round(self, stage_id: str) -> int:
        """
        Charge one round to a review stage's budget, and report it.

        Separate from :meth:`next_round`, which derives the
        RECORDED number from the records themselves and therefore
        survives a resume. This one must not: the budget bounds the
        branch within a run, and a resume starts fresh.

        It is also not a local, which is the defect. Held in a local,
        the budget was reset by anything that re-entered the review —
        a parallel sibling failing, a gate retry —
        while the recorded number kept climbing. One branch reached
        six recorded rounds against a cap of three.

        :param stage_id: The review stage being charged.
        :returns: How many rounds that stage has now spent.
        """
        with self.ctx.lock:
            spent = self._rounds.get(stage_id, 0) + 1
            self._rounds[stage_id] = spent
        return spent

    def record_review(
        self,
        stage_id: str,
        reviewer: str,
        round_no: int,
        verdict: str | None,
        session: str,
        fallback: str,
    ) -> None:
        """
        Capture a reviewer's report before its session is disposed.

        Written to the run dir straight away, so a run that blocks or
        crashes still explains itself; buffered in memory (and in the
        run state, so a resume keeps it) for the copy committed to the
        publishing branch. Best-effort throughout — a lost record must
        never fail a review that otherwise decided cleanly.

        :param stage_id: The review stage's id.
        :param reviewer: The agent name that voted.
        :param round_no: Which review round this was (1-based).
        :param verdict: The parsed verdict, or ``None``.
        :param session: The reviewer's session, still alive.
        :param fallback: The reply text already captured, used when the
            transcript cannot be read at all.
        """
        try:
            turns = self.ctx.sessions.read_transcript(session)
        except SwarmSessionError:
            turns = []
        if not turns and fallback.strip():
            turns = [('assistant', fallback.strip())]
        # Best-effort: a review whose stage cannot be resolved still
        # records its vote, just without naming the candidate.
        stage = self.ctx.stage_by_id.get(stage_id)
        try:
            target = review_target(stage, self.ctx.nodes) if stage else None
        except PipelineRunError:
            target = None
        sections = FindingSections.of(fallback)
        # A blocking report is kept WHOLE. Its findings are prose under
        # whatever heading the reviewer chose, so there is no list to
        # lift — and the three sections above are explicitly the
        # non-blocking ones, which left the record of a block empty.
        blocking_report = fallback.strip() if verdict == 'BLOCKING' else ''
        if blocking_report and report_names_nothing(blocking_report):
            click.echo(
                f'[review] {stage_id}-{reviewer}: BLOCKING, but the '
                f'report named nothing — no finding, file or line for '
                f'the writer to address or a verifier to check. Its '
                f'reply is in the run directory.'
            )
        record = ReviewRecord(
            chunk=self._progress.module_id,
            stage=stage_id,
            reviewer=reviewer,
            round_no=round_no,
            verdict=verdict,
            turns=tuple(turns),
            findings=sections.all,
            defects=sections.defects,
            later_increment=sections.later_increment,
            premises=sections.premises,
            environment=sections.environment,
            target=target,
            blocking_report=blocking_report,
        )
        self.add(record)

    def record_pick(
        self,
        stage_id: str,
        candidates: list[str],
        selected: str,
        stated: str | None,
        reasoning: str,
        retained: tuple[tuple[str, str], ...] = (),
        *,
        forfeited: tuple[tuple[str, str, int], ...] = (),
        uncontested: bool = False,
    ) -> None:
        """
        Record a judge's choice before its session is disposed.

        Written to the run dir straight away so a run that fails after
        the judge still explains what it picked, and buffered (and
        persisted, so a resume keeps it) for the copy committed to the
        publishing branch — the same treatment reviewer reports get.

        :param stage_id: The judge stage's id.
        :param candidates: The competing node ids it compared.
        :param selected: The node id that will publish.
        :param stated: What the judge's own SELECT line named, or
            ``None``. Recorded separately so the runner's fail-safe
            substitution cannot be mistaken for a real preference.
        :param reasoning: The judge's reply. Passed in rather than read
            back off the node, because this runs BEFORE the node exists
            — recording has to happen before the branch alias so a run
            that fails in between still explains itself.
        :param retained: ``(node, bundle-path)`` for each preserved
            loser, so the record says where the losing code lives.
        :param forfeited: ``(candidate, review, rounds)`` for each
            candidate withdrawn before judging.
        :param uncontested: Only one candidate was left; no judge ran.
        """
        pick = JudgePick(
            chunk=self._progress.module_id,
            stage=stage_id,
            candidates=tuple(candidates),
            selected=selected,
            stated=stated,
            reasoning=reasoning,
            retained=retained,
            forfeited=forfeited,
            uncontested=uncontested,
        )
        self.picks.append(pick)
        self.ctx.worktrees.write_run_artifact(
            self.ctx.run_id,
            f'judging/{stage_id}.md',
            render_judge_decision([pick], title=f'Judging — {stage_id}'),
        )
        if pick.uncontested:
            # The runner has already said why no judge ran.
            return
        if pick.honored:
            click.echo(
                f'[judge] {stage_id}: selected {selected} from '
                f'{", ".join(candidates)}.'
            )
        else:
            click.echo(
                f'[judge] {stage_id}: no usable SELECT line (said '
                f'{stated!r}) — falling back to {selected}. This is '
                f'recorded as an ABSENT decision, not a preference.'
            )
