"""The judge stage: choosing one candidate implementation to ship.

Competing writers build the same frozen contract independently, and a
judge compares them and names one with a ``SELECT:`` line. The winner
publishes as the judge's own branch; every loser is kept as a bundle.
A judge that never decides halts the run rather than ship the first
candidate by default.
"""

from __future__ import annotations

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.briefing import Briefing
from sbx_omnigent.runner.captures import TurnCapture
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.forfeits import Forfeits
from sbx_omnigent.runner.lineage import seed_from
from sbx_omnigent.runner.parsing.verdicts import parse_select
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.records.results import NodeResult
from sbx_omnigent.runner.review_book import ReviewBook
from sbx_omnigent.runner.session_driver import SessionDriver


class JudgeStage:
    """
    Run a judge stage.

    :param ctx: The run.
    :param progress: The run's progress: the active module, and the
        judge as the last node that produced a branch.
    :param book: Records the pick, and says which candidates already
        cleared review.
    :param driver: Creates the judge's session and drives its turns.
    :param capture: Keeps the turn of a judge that never decided.
    :param briefing: The task block every judge turn carries.
    :param forfeits: The candidates still in the field.
    :param resume: Whether this is a ``--resume``, whose judge replaces
        the comparison tree an earlier attempt left.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        progress: RunProgress,
        book: ReviewBook,
        driver: SessionDriver,
        capture: TurnCapture,
        briefing: Briefing,
        forfeits: Forfeits,
        resume: bool,
    ) -> None:
        self.ctx = ctx
        self._progress = progress
        self._book = book
        self._driver = driver
        self._capture = capture
        self._briefing = briefing
        self._forfeits = forfeits
        self._resume = resume

    def run(self, stage: pipeline.PipelineStage) -> None:
        """
        Choose one candidate, and publish it as the judge's branch.

        :param stage: The judge stage.
        :raises PipelineRunError: If there is no candidate, one was
            built against another contract, or the judge never stated a
            ``SELECT:`` line for one of them.
        """
        candidates = self._forfeits.judge_candidates(stage)
        forfeited = tuple(
            (need, *self._forfeits.forfeits[need])
            for need in stage.needs
            if need in self._forfeits.forfeits
        )
        if not candidates:
            raise PipelineRunError(
                f'judge {stage.id!r} has no writer candidates in needs'
            )
        self._require_one_contract(stage, candidates)
        if forfeited and len(candidates) == 1:
            self._select_uncontested(stage, candidates[0], forfeited)
            return
        wt = self.ctx.worktrees.create_judge_worktree(
            self.ctx.run_id, stage.id, candidates, replace=self._resume
        )
        agent = stage.run[0]
        session = self._driver.create(agent, wt, 'ro', stage.id)
        out = self._driver.drive(
            session, self.instruction(stage, candidates)
        )
        stated = parse_select(out, candidates)
        if stated is None:
            # One nudge before the default takes over. A judge that
            # answered without deciding — "I have launched the tests in
            # the background ... I will process the results as soon as
            # they are available" — has not refused to choose, it has
            # misjudged how many turns it gets. Asking once converts a
            # silent default into an actual decision; the alternative
            # is discarding its judgement over a misunderstanding.
            click.echo(
                f'[judge] {stage.id}: no SELECT line in the first '
                f'reply — asking once more before halting.'
            )
            retry = self._driver.drive(
                session, retry_instruction(candidates)
            )
            again = parse_select(retry, candidates)
            if again is not None:
                stated = again
                out = f'{out}\n\n{retry}'
        if stated not in candidates:
            # HALT, do not guess. This used to fall back to
            # `candidates[0]`, which turned "nobody judged" into a pick
            # made by list order — indistinguishable, in the record,
            # from a real decision. Observed live on
            # `gcp-scope-topology-1`: the judge said it had started
            # verification builds and would analyse them "once
            # compilation completes", and impl-a shipped because it was
            # first in `needs`.
            #
            # Halting is affordable precisely here: every candidate is
            # committed to the hub and both reached their review gates,
            # so nothing is lost by stopping, and a human choosing
            # between two reviewed branches is a minute's work. A silent
            # default is the expensive outcome, not the stop.
            self._capture.turn(
                session, 'the judge never stated a SELECT line'
            )
            click.echo(
                f'[judge] {stage.id}: NO DECISION after a retry — '
                f'halting rather than shipping the first candidate by '
                f'default. Candidates: {", ".join(candidates)}. Their '
                f'branches are on the hub and their reviews are in the '
                f'run state; pick one and resume, or re-run this stage.'
            )
            raise PipelineRunError(
                f'judge {stage.id!r} stated no SELECT line for any of '
                f'{", ".join(candidates)} — refusing to pick one by '
                f'position. See the captured turn in the run directory.'
            )
        sel = stated
        # Record BEFORE the branch alias: the judge's session is
        # disposed moments from now and its reasoning goes with it, and
        # a run that fails after this point should still explain what
        # it picked.
        # Retain the losers BEFORE anything else touches the hub. Their
        # branches live only there, and the run directory holds the only
        # copy — teardown deletes it.
        retained = self._retain_losers(candidates, sel)
        self._book.record_pick(
            stage.id, candidates, sel, stated, out, retained,
            forfeited=forfeited,
        )
        # Publish the winner as the judge node's OWN hub branch, so a
        # downstream stage can seed `from:` the judge (refactor the
        # winner, review it) — a judge otherwise leaves no branch to
        # inherit.
        self.ctx.worktrees.alias_node_branch(self.ctx.run_id, stage.id, sel)
        self.ctx.nodes[stage.id] = NodeResult(
            stage.id,
            'judge',
            branch=self.ctx.nodes[sel].branch,
            worktree=wt,
            session=session,
            output=out,
            selected=sel,
        )
        self._progress.last_branch_node = stage.id

    def _select_uncontested(
        self,
        stage: pipeline.PipelineStage,
        selected: str,
        forfeited: tuple[tuple[str, str, int], ...],
    ) -> None:
        """
        Select the one candidate left, without booting a judge.

        A judge VM choosing between one candidate buys nothing, and its
        record would read as a judgment. Recorded as uncontested, with
        the candidates that were withdrawn (#47).

        :param stage: The judge stage.
        :param selected: The only candidate left.
        :param forfeited: ``(candidate, review, rounds)`` per forfeit.
        """
        withdrawn = ', '.join(
            f'{cand} ({review}, {rounds} round(s))'
            for cand, review, rounds in forfeited
        )
        click.echo(
            f'[judge] {stage.id}: {selected} is the only candidate left '
            f'after {withdrawn} withdrew, so no judge turn runs. '
            f'Recorded as UNCONTESTED, not as a judgment.'
        )
        self._book.record_pick(
            stage.id, [selected], selected, None, '',
            forfeited=forfeited, uncontested=True,
        )
        self.ctx.worktrees.alias_node_branch(
            self.ctx.run_id, stage.id, selected
        )
        self.ctx.nodes[stage.id] = NodeResult(
            stage.id,
            'judge',
            branch=self.ctx.nodes[selected].branch,
            selected=selected,
        )
        self._progress.last_branch_node = stage.id

    def _retain_losers(
        self, candidates: list[str], selected: str
    ) -> tuple[tuple[str, str], ...]:
        """
        Preserve every candidate that did not win, as a git bundle.

        Only the winner publishes, so without this a complete, reviewed,
        test-passing implementation of the same frozen contract — by a
        different model — is deleted with the run hub. It is the only
        copy, and it is the evidence any future argument about model
        choice would actually want.

        Best-effort: a run that produced a shippable winner must not
        fail because an archive could not be written. A failure is
        reported loudly rather than swallowed, because the whole point
        is that this data is otherwise unrecoverable.

        :param candidates: Every node the judge compared.
        :param selected: The node that won.
        :returns: ``(node, bundle-path)`` for each loser retained.
        """
        kept: list[tuple[str, str]] = []
        for node in candidates:
            if node == selected:
                continue
            try:
                path = self.ctx.worktrees.retain_node_bundle(
                    self.ctx.run_id,
                    node,
                    # `base_branch:` is optional in a pipeline.yaml, and
                    # a None here would bundle against the literal ref
                    # 'None'. Same fallback as lineage.seed_ref.
                    against=self.ctx.config.base_branch or 'main',
                )
            except Exception as exc:
                # Loud, never fatal. The run produced a shippable
                # winner; losing the archive is a real loss and must be
                # SAID, but it is not a reason to fail the publish.
                click.echo(
                    f"[retain] {node}: could NOT be preserved ({exc})."
                    f" Its branch lives only on this run's hub and"
                    f" will go with it at teardown."
                )
                continue
            if path is None:
                continue
            kept.append((node, path))
            branch = self.ctx.worktrees.node_branch(self.ctx.run_id, node)
            click.echo(
                f'[retain] {node}: kept at {path} — restore with '
                f"'git fetch {path} "
                f'refs/heads/{branch}'
                f":refs/heads/{node}'."
            )
        return tuple(kept)

    def _require_one_contract(
        self, stage: pipeline.PipelineStage, candidates: list[str]
    ) -> None:
        """
        Refuse to judge candidates built against different contracts.

        A branch that does not contain its seed is not a worse
        implementation of the same thing — it is an implementation of
        something else, and picking between the two produces a record
        that reads like a judgement.

        Only candidates with a resolvable seed are checked: writers
        that legitimately cut from base have no shared contract to
        compare, which is the ordinary two-writer race.

        :param stage: The judge stage.
        :param candidates: The competing writer nodes.
        :raises PipelineRunError: When a candidate lacks its seed.
        """
        for cand in candidates:
            cand_stage = self.ctx.stage_by_id.get(cand)
            if cand_stage is None:
                continue
            seed = seed_from(cand_stage, self.ctx.nodes)
            if seed is None or seed not in self.ctx.nodes:
                continue
            if self.ctx.worktrees.branch_contains(
                self.ctx.run_id, cand, ancestor=seed
            ):
                continue
            raise PipelineRunError(
                f'judge {stage.id!r} cannot compare {cand!r}: its '
                f'branch does not contain {seed!r}, so it was built '
                f'without the contract its rivals were held to. '
                f'Judging it would compare two different contracts '
                f'and record the result as a choice.'
            )

    def instruction(
        self, stage: pipeline.PipelineStage, candidates: list[str]
    ) -> str:
        """
        The turn handed to a judge.

        Says two things the template cannot know. First, whether the
        candidates have ALREADY been through their reviewer gates —
        ``templates/judge.md`` asks "which candidate genuinely passes
        the suite", and a judge that reads that with no other context
        starts a build to find out. On a Rust workspace that is slow
        enough that one judge backgrounded it and reported progress
        instead of deciding. The suite has already been run, twice per
        candidate, by reviewers who each installed a toolchain to do
        it; re-running it is minutes of guest time for an answer
        already on record.

        Second, that this is its ONLY turn. The judge asked to "process
        the results as soon as they are available" — a perfectly normal
        thing to say in a conversation, and fatal here, because nothing
        follows. Without a SELECT line the first candidate used to win
        by default; now the run halts, which a judge must be told.

        :param stage: The judge stage.
        :param candidates: The writer nodes being judged.
        :returns: The instruction text.
        """
        listed = ', '.join(f'./{c}' for c in candidates)
        instr = (
            self._briefing.task_block()
            + f'\n\nCompare the candidate implementations in {listed} '
            'against this contract, then end your reply with a line '
            'reading exactly:\n\n    SELECT: <id>\n\nwhere <id> is '
            f'one of: {", ".join(candidates)}. The colon is part of '
            'the line, the id is copied verbatim, and nothing else '
            'goes on it.'
        )
        if self._all_reviewed(candidates):
            instr += (
                '\n\nEVERY candidate here has already passed its own '
                'review gate: each was independently verified by '
                'reviewers who installed a toolchain and RAN the test '
                'suite and the project gate against that exact branch. '
                'Do NOT build or re-run the suite — the answer is '
                'already on record, and re-deriving it costs minutes of '
                'guest time and tells you nothing new. Judge on the '
                'DIFFERENCES between the candidates: design, '
                'simplicity, dependency minimalism, how each will read '
                'to the next person, and what each does at the edges. '
                'Read the code, do not run it.'
                + self._scope_clause()
            )
        instr += (
            '\n\nThis is your ONLY turn. Nothing follows it, so do not '
            'start work in the background and do not say you will '
            'report once something finishes — there is no later message '
            'and anything unfinished is simply lost. Decide now, on '
            'what you can see. A reply without a SELECT line is not a '
            'deferral: the run HALTS, no candidate is chosen, and a '
            'human has to come and do this by hand. Your judgement is '
            'not merely discarded — the pipeline stops. If you are '
            'genuinely unsure, still SELECT, and say what you could '
            'not check.'
        )
        return instr

    def _scope_clause(self) -> str:
        """
        Make staying inside the increment a judging criterion.

        Unstated, the model reaches for it inconsistently: on
        `gcp-scope-topology-1` the same judge penalised a candidate for
        being "out of scope for this increment" in [transport] and then
        praised the other one's later-increment work as a strength in
        [topology]. Naming it, and saying which direction it counts in,
        removes the coin flip.

        :returns: The clause, or ``''`` outside a campaign.
        """
        if self._progress.active_subtask is None:
            return ''
        return (
            ' Weigh also WHETHER EACH STAYED INSIDE THE MARKED '
            'INCREMENT. A candidate that implemented a later '
            "increment's work is not thereby ahead: that work was "
            'reviewed against the wrong contract, and the increment '
            'that owns it will inherit something its own plan did not '
            'choose. Count it as a cost, not a bonus — and count it the '
            'same way for every candidate.'
        )

    def _all_reviewed(self, candidates: list[str]) -> bool:
        """
        Whether every candidate already cleared a review stage.

        A judge does not necessarily run downstream of reviewers — the
        pipeline decides that — so the "already verified, do not
        re-run" claim is only made when it is TRUE for every candidate.

        It now SURVIVES a resume (it is in the run state), because
        losing it was not the harmless understatement it was written to
        be: an untold judge starts a workspace build, cannot finish it
        inside its one turn, and states no SELECT. What keeps the claim
        honest across a resume is that re-driving a writer drops a
        candidate the moment its branch changes.

        :param candidates: The writer nodes being judged.
        :returns: ``True`` when each reached APPROVED consensus here.
        """
        return bool(candidates) and all(
            c in self._book.reviewed_ok for c in candidates
        )


def retry_instruction(candidates: list[str]) -> str:
    """
    The one follow-up a judge gets when it did not decide.

    Deliberately asks for nothing but the line, and SHOWS it
    rather than naming it: the first version said "end with the
    SELECT line", and a judge that had already chosen wrote
    ``SELECT core-contracts-impl-a`` without the colon, which
    parsed as nothing. A judge that deferred has usually done the
    reading; what it lacked was either the knowledge that no further
    turn was coming, or the exact format.

    :param candidates: The writer node ids being judged.
    :returns: The instruction text.
    """
    return (
        'No decision was recorded from your reply: it carried no '
        'line in the required form. This is the LAST turn — there '
        'is no further message, and nothing you started in the '
        'background will be waited for or read. Decide now, from '
        'what you have already seen; a decision on partial '
        'evidence is worth far more than none, because without '
        'that line the run HALTS, no candidate is chosen, and a '
        'human has to make this choice by hand.\n\nReply with a '
        'one-paragraph rationale, then a final line reading '
        'exactly:\n\n    SELECT: <id>\n\nwhere <id> is one '
        f'of: {", ".join(candidates)}. Include the colon, copy '
        'the id verbatim, and put nothing else on that line — no '
        'backticks, no bold, no trailing prose.'
    )
