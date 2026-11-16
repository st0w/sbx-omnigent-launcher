"""Deterministic DAG runner for declarative pipelines.

Drives a parsed ``pipeline.yaml`` to completion on the
branch-as-artifact model (see ``docs/PIPELINES.md``): each
WRITER node works in its own isolated worktree + branch (never a
shared filesystem), reviewers and judges read a branch ``:ro``, and
the trusted host plane does all git.

The runner is the deterministic control plane: it walks the stages,
provisions each node's mount, drives one turn at a time, parses the
``VERDICT:`` / ``SELECT:`` lines, loops a review gate back to its
writer, and publishes the final branch. It replaces the prose
coordinator for a *defined* pipeline; the coordinator stays for
interactive work.

A pipeline with a ``task:`` runs to completion. Without one, the runner
PROVISIONS the topology (each node's isolated worktree + microVM
session) and hands back the role→session bindings for a human (or the
coordinator) to drive — the VMs are left up.

Node kinds (derived from a stage):
- **reader** — a read-only producer (e.g. a planner): mounts a tree
  ``:ro`` and emits text (a design) used as downstream context.
- **writer** (``write: true``) — an isolated ``rw`` worktree cut from an
  upstream branch (inheritance) or the base; committed to its branch.
- **review** (``gate:``/``on_block:``) — one or more reviewers mount the
  target writer's branch ``:ro`` and vote; a block loops back to the
  writer with the findings, up to a round cap.
- **judge** (``selects:``) — mounts a compare tree of competing branches
  and picks a winner.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import functools
import threading
import time
from collections.abc import Callable, Sequence
from pathlib import Path

import click

from sbx_omnigent import (
    agy,
    disk_metrics,
    orphans,
    pipeline,
)
from sbx_omnigent.runner import state
from sbx_omnigent.runner.campaign import Campaign, CampaignHooks
from sbx_omnigent.runner.captures import TurnCapture
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import (
    PipelineRunError,
    _Blocked,
)
from sbx_omnigent.runner.failure_notes import (
    FailureNotes,
    lost_not_failed,
)
from sbx_omnigent.runner.filing import FindingFiler
from sbx_omnigent.runner.gate import VerifyGate
from sbx_omnigent.runner.harness_record import HarnessRecord
from sbx_omnigent.runner.ledger import DecisionLedger
from sbx_omnigent.runner.limits import (
    _DEFAULT_MAX_ROUNDS,
    _LAUNCH_ATTEMPTS,
    _MAX_PARALLEL_NODES,
    _MAX_SILENT_REVIEW_ROUNDS,
    _REVIEW_RETRY_BACKOFF_S,
    _REVIEW_TURN_ATTEMPTS,
    _VERDICT_POLL_CEILING_S,
    _VERDICT_POLL_DEADLINE_S,
    _VERDICT_POLL_INTERVAL_S,
    _WRITER_RETRY_BACKOFF_S,
    _WRITER_TURN_ATTEMPTS,
)
from sbx_omnigent.runner.lineage import review_target, seed_from, seed_ref
from sbx_omnigent.runner.parsing.dispositions import (
    parse_blocking_dispositions,
    parse_dispositions,
)
from sbx_omnigent.runner.parsing.findings import (
    parse_findings,
)
from sbx_omnigent.runner.parsing.plans import (
    _PLAN_REPLY_TAIL,
    parse_subtasks,
    select_plan_of_record,
)
from sbx_omnigent.runner.parsing.text import (
    _excerpt,
    _printable,
)
from sbx_omnigent.runner.parsing.verdicts import (
    _UNVERIFIED,
    _check_done,
    _review_done,
    _writer_done,
    check_finished,
    dispute_near_misses,
    parse_disputes,
    parse_select,
    parse_verdict,
    work_finished,
)
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.prompts.checks import (
    _APPROVED_AS_IS,
    _CHECK_COMPLETE,
    _CHECK_DISPUTED,
    _CHECK_FINISH_ASK,
    _CHECK_RULES,
    _CHECK_VERDICT,
    _PLAN_CHECK_RULES,
    _PLAN_DECISION_PHRASES,
)
from sbx_omnigent.runner.prompts.environment import setup_block
from sbx_omnigent.runner.prompts.review import (
    _ENVIRONMENT_ASK,
    _FINDINGS_ASK,
    _REVIEW_DISPUTE,
    _REVIEW_MOUNT,
    _REVIEW_NOT_THE_GATE,
    _REVIEW_NOT_WEIGHTED,
    _REVIEW_VERDICT_LINE,
    _REVIEW_VERIFY,
    _VERDICT_NUDGE,
)
from sbx_omnigent.runner.prompts.triage import blocking_block, findings_block
from sbx_omnigent.runner.prompts.writer import (
    _DISPOSABLE_VM,
    _FIX_MUST_VERIFY,
    _FIX_NO_WEAKENING,
    _FROZEN_TESTS_CONTRACT,
    _UNATTENDED,
    _WORK_COMPLETE,
    _WRITER_BUILD,
    _WRITER_FINISH,
    _WRITER_FINISH_ASK,
)
from sbx_omnigent.runner.publish import Publisher
from sbx_omnigent.runner.records.results import (
    NodeResult,
    RunResult,
)
from sbx_omnigent.runner.records.reviews import (
    ReviewRecord,
)
from sbx_omnigent.runner.render.pr_body import (
    environment_by_stage,
)
from sbx_omnigent.runner.repo_records import RepoRecords
from sbx_omnigent.runner.review_book import ReviewBook
from sbx_omnigent.runner.security_records import SecurityRecords
from sbx_omnigent.runner.session_disposal import SessionDisposal
from sbx_omnigent.runner.session_driver import SessionDriver
from sbx_omnigent.runner.session_registry import SessionRegistry
from sbx_omnigent.runner.stage_kinds import (
    is_planner,
    stage_kind,
    writer_is_terminal,
)
from sbx_omnigent.runner.stages.scan import ScanStage
from sbx_omnigent.runner.triage import FindingTriage
from sbx_omnigent.runner.writer_policy import WriterPolicy
from sbx_omnigent.swarm_session import (
    SwarmRunnerUnavailable,
    SwarmSessionClient,
    SwarmSessionError,
)
from sbx_omnigent.worktrees import (
    WorktreeManager,
)


class PipelineRunner:
    """
    Execute a parsed pipeline on the branch-as-artifact model.

    :param config: The parsed pipeline.
    :param session_client: Managed-session driver.
    :param worktree_manager: Branch-model worktree manager (its
        ``worktree_root`` MUST equal the server's worktree_root).
    :param run_id: Unique run id (dir + branch component).
    :param agent_ids: ``{agent_name: registered_id}`` (see
        :func:`~sbx_omnigent.runner.agents.resolve_agent_ids`).
    :param publish_repo: Push target for publish; defaults to the
        pipeline's ``repo``.
    :param max_review_rounds: Review loop-back cap before blocking.
    :param turn_timeout: Per-turn timeout (seconds).
    :param keep: Leave sessions + worktrees for inspection at the end.
    :param resume: Continue a previous run of this id: reuse its hub,
        skip the stages it already finished, and re-cut only what is
        left. See :meth:`_load_state`.
    :param swap_age_s: Returns the agy swap secret's age in seconds (or
        ``None`` when unknown), checked before every agy turn. Defaults
        to :func:`agy.harvest_age_s`; injected in tests.
    """

    def __init__(
        self,
        config: pipeline.PipelineConfig,
        *,
        session_client: SwarmSessionClient,
        worktree_manager: WorktreeManager,
        run_id: str,
        agent_ids: dict[str, str],
        publish_repo: str | None = None,
        max_review_rounds: int = _DEFAULT_MAX_ROUNDS,
        turn_timeout: float = 1800.0,
        keep: bool = False,
        interactive_plan: bool = True,
        resume: bool = False,
        swap_age_s: Callable[[], float | None] | None = None,
        gate_egress: tuple[str, ...] | None = None,
    ) -> None:
        self._config = config
        self._progress = RunProgress()
        self._registry = SessionRegistry()
        self._sc = session_client
        self._wt = worktree_manager
        self._run_id = run_id
        self._publish_repo = publish_repo or config.repo
        self._max_rounds = max_review_rounds
        self._keep = keep
        #: Block the PLANNING phase (only) on human approval: the
        #: planner asks questions, a human answers + posts APPROVED,
        #: and nothing downstream starts until then. Off for automated
        #: runs (and unit tests).
        self._interactive_plan = interactive_plan
        #: Continue a previous attempt rather than starting clean.
        self._resume = resume
        #: Stage ids whose stage FULLY completed. The authority on what
        #: a resume may skip — deliberately not "has a branch", since a
        #: pre-warmed writer has a branch before it has ever run.
        self._completed: set[str] = set()
        #: Nodes withdrawn because their review never reached consensus
        #: — the blocked review stage and the writer it was vetting. A
        #: forfeited candidate is excluded from judging: comparing a
        #: vetted branch against an unvetted one is not a choice.
        self._forfeited: set[str] = set()
        #: Each withdrawn candidate's ``(review stage, rounds)``, so the
        #: selection records the reduced field it was made from (#47).
        self._forfeits: dict[str, tuple[str, int]] = {}
        #: Writer nodes whose review stage reached APPROVED consensus.
        #: Read by :meth:`_all_reviewed` so a judge can be told its
        #: candidates are already verified and need not be re-built.
        #: Deliberately NOT persisted: on a resume it starts empty, the
        #: judge simply is not told, and it verifies for itself as it
        #: always did. Degrading to the slower-but-correct behaviour is
        #: the right failure for a claim about what was checked.
        self._reviewed_ok: set[str] = set()
        #: The operator's allowlist for the verify gate, as the server
        #: published it (see :func:`read_operator_egress`). ``None``
        #: only when built outside ``main()``, which keeps the default.
        self._gate_egress = gate_egress
        self._nodes: dict[str, NodeResult] = {}
        #: node id -> its transcript, captured just before the session
        #: was freed. Disposing DELETES a session, so a reader whose
        #: conversation is committed later (the planner) must have it
        #: read out first.
        self._reader_turns: dict[str, tuple[tuple[str, str], ...]] = {}
        #: Set on Ctrl-C. Only the main thread sees the interrupt; the
        #: workers of a parallel stage read this before booting a VM or
        #: driving a turn, and stop instead.
        self._interrupted = threading.Event()
        self._bindings: list[dict[str, str]] = []
        #: Node ids pre-warmed during planning — disposed at campaign
        #: start (their un-namespaced VMs are replaced per chunk).
        self._prewarmed: set[str] = set()
        #: Guards run bookkeeping while nodes of one stage run at the
        #: same time (see :meth:`_parallel`). Most individual mutations
        #: are atomic under CPython; the state SNAPSHOT is not — it
        #: walks the session list and the node map, so an append from
        #: another node mid-walk raises "changed size during
        #: iteration". Re-entrant because the guarded paths nest:
        #: creating a session saves state, and so does stage completion.
        self._lock = threading.RLock()
        #: Caps microVMs driven at once ACROSS the whole run, not per
        #: call. Parallelism nests — a parallel block of review stages
        #: each running two reviewers is four guests — and a per-call
        #: cap would bound each level while their product ran away.
        self._node_slots = threading.Semaphore(_MAX_PARALLEL_NODES)
        self._stage_by_id: dict[str, pipeline.PipelineStage] = {}
        for stage in pipeline._iter_stages(config.stages):
            if stage.run or stage.parallel:
                self._stage_by_id[stage.id] = stage
        self._ctx = RunContext(
            config=config,
            worktrees=worktree_manager,
            sessions=session_client,
            run_id=run_id,
            nodes=self._nodes,
            stage_by_id=self._stage_by_id,
            lock=self._lock,
            max_rounds=max_review_rounds,
        )
        self._book = ReviewBook(
            self._ctx, progress=self._progress, save_state=self._save_state
        )
        self._harness = HarnessRecord(
            self._ctx, registry=self._registry, save_state=self._save_state
        )
        self._capture = TurnCapture(
            self._ctx, registry=self._registry, harness=self._harness
        )
        self._notes = FailureNotes(
            self._ctx, registry=self._registry, keep=keep
        )
        self._driver = SessionDriver(
            self._ctx,
            registry=self._registry,
            harness=self._harness,
            capture=self._capture,
            notes=self._notes,
            agent_ids=agent_ids,
            turn_timeout=turn_timeout,
            swap_age_s=swap_age_s or agy.harvest_age_s,
            interrupted=self._interrupted,
            save_state=self._save_state,
        )
        self._disposal = SessionDisposal(
            self._ctx,
            registry=self._registry,
            keep=keep,
            save_state=self._save_state,
        )
        self._scan = ScanStage(
            self._ctx,
            progress=self._progress,
            book=self._book,
            driver=self._driver,
            disposal=self._disposal,
            capture=self._capture,
            node_slots=self._node_slots,
            redrive_writer=self._redrive_writer,
            run_review=self._run_review,
        )
        self._ledger = DecisionLedger(self._ctx)
        self._policy = WriterPolicy(
            self._ctx, redrive_writer=self._redrive_writer
        )
        self._triage = FindingTriage(chunk_reviews=self._book.module_reviews)
        self._filing = FindingFiler(
            self._ctx,
            publish_repo=self._publish_repo,
            triage=self._triage,
            chunk_reviews=self._book.module_reviews,
        )
        self._records = RepoRecords(
            self._ctx,
            chunk_reviews=self._book.module_reviews,
            chunk_picks=self._book.module_picks,
            reader_turns=lambda: self._reader_turns,
        )
        self._security = SecurityRecords(
            self._ctx,
            publish_repo=self._publish_repo,
            triage=self._triage,
            filing=self._filing,
            chunk_reviews=self._book.module_reviews,
        )
        self._publisher = Publisher(
            self._ctx,
            publish_repo=self._publish_repo,
            records=self._records,
            ledger=self._ledger,
            security=self._security,
            filing=self._filing,
            chunk_reviews=self._book.module_reviews,
            chunk_picks=self._book.module_picks,
            save_state=self._save_state,
        )
        self._publisher.preflight()
        self._campaign = Campaign(
            self._ctx,
            progress=self._progress,
            publisher=self._publisher,
            filing=self._filing,
            keep=keep,
            hooks=CampaignHooks(
                run_stage=self._exec_top_level,
                gate=self._verify_winner,
                save_state=self._save_state,
                dispose_prewarmed=self._dispose_prewarmed,
                session_count=lambda: len(self._registry.sessions),
                dispose_sessions_since=(
                    lambda mark: self._disposal.dispose(
                        self._registry.sessions[mark:]
                    )
                ),
                sample_disk=self._sample_disk,
            ),
        )
        self._gate = VerifyGate(
            self._ctx,
            egress=gate_egress,
            redrive_writer=self._redrive_writer,
            rerun_stage=self._rerun_stage,
        )


    # ── public entry ──────────────────────────────────────────────

    def run(self) -> RunResult:
        """
        Run the pipeline: to completion with a task, else provision.

        With a ``task:`` the runner drives every stage to consensus and
        publishes. Without one it provisions each node (its worktree +
        microVM session) and returns the role→session bindings, leaving
        the VMs up for a human/coordinator to drive.

        :returns: The :class:`RunResult` (``status`` is ``'completed'``,
            ``'blocked'``, or ``'provisioned'``).
        :raises PipelineRunError: On a setup/turn/git failure.
        """
        self._wt.create_run(
            self._run_id, self._config.repo, reuse=self._resume
        )
        if self._resume:
            self._load_state()
        if self._config.task:
            return self._drive_to_completion()
        return self._provision_only()

    # ── run-to-completion ─────────────────────────────────────────

    def _drive_to_completion(self) -> RunResult:
        blocked: str | None = None
        finished = False
        stages = self._config.stages
        planner = [s for s in stages if is_planner(s, self._config)]
        build = [s for s in stages if not is_planner(s, self._config)]
        try:
            try:
                if self._config.subtasks:
                    # Per-module mode: the human supplied the module
                    # list, so the WHOLE pipeline (its own planner +
                    # build) loops once per module — each module planned
                    # in-loop against the frozen prior modules.
                    self._progress.subtasks = list(self._config.subtasks)
                    self._campaign.run(
                        list(self._config.stages), per_module=True
                    )
                else:
                    # The planner runs ONCE (+ proposes the chunk list).
                    for stage in planner:
                        self._exec_top_level(stage)
                    if self._campaign.applies(build):
                        self._campaign.run(build, per_module=False)
                    else:
                        for stage in build:
                            self._exec_top_level(stage)
                        self._verify_publish_target(build)
                        self._publish()
                finished = True
            except _Blocked as exc:
                blocked = exc.stage_id
        finally:
            # Anything short of a clean finish — a raise, or a blocked
            # gate — keeps the run directory, because that is the only
            # copy of what --resume needs.
            self._disposal.teardown(preserve_run=not finished)
        return RunResult(
            run_id=self._run_id,
            status='blocked' if blocked else 'completed',
            published='; '.join(self._publisher.published) or None,
            final_node=self._progress.last_branch_node,
            blocked_stage=blocked,
            nodes=dict(self._nodes),
        )

    def _load_state(self) -> None:
        """
        Restore a previous attempt's bookkeeping.

        What is restored, and what is refused, is
        :func:`sbx_omnigent.runner.state.decode`'s to decide. The active
        module is not restored: the campaign loop sets it again as it
        reaches each module.

        :raises PipelineRunError: If there is no state to resume, or it
            was written by an incompatible version.
        """
        restored = state.decode(
            self._wt.read_run_state(self._run_id), self._run_id
        )
        self._completed = restored.completed
        self._progress.completed_chunks = restored.completed_chunks
        self._publisher.chunk_publishes = restored.chunk_publishes
        self._reviewed_ok = restored.reviewed_ok
        self._registry.stale = restored.sessions
        self._ledger.decisions = restored.decisions
        self._ledger.referrals = restored.referrals
        self._triage.dispositions = restored.dispositions
        self._triage.blocking_claims = restored.blocking_claims
        self._harness.recorded = restored.harness_versions
        self._book.reviews = restored.reviews
        self._reader_turns = restored.reader_turns
        self._book.picks = restored.picks
        self._progress.plan_of_record = restored.plan_of_record
        self._publisher.published = restored.published
        self._progress.last_branch_node = restored.last_branch_node
        self._progress.subtasks = restored.subtasks
        self._nodes.update(restored.nodes)
        done = len(self._completed)
        published = len(self._progress.completed_chunks)
        click.echo(
            f'[resume] run {self._run_id}: {done} stage(s) already '
            f'complete, {published} chunk(s) published'
            + (
                ' — plan of record restored'
                if self._progress.plan_of_record
                else ''
            )
        )
        self._disposal.dispose_stale()

    def _state_payload(self) -> dict:
        """
        Snapshot everything a run cannot rebuild from git.

        Sessions are recorded for ONE purpose: so a resume can dispose
        the VMs the previous attempt left running. Under --keep they
        accumulate for the whole run, which is exactly the leak a
        resume needs to clear, and undisposed handles ride along with
        them. See :mod:`sbx_omnigent.runner.state` for the rest.

        :returns: A JSON-serializable snapshot of the run.
        """
        return state.encode(
            state.RunState(
                run_id=self._run_id,
                pipeline=self._config.name,
                plan_of_record=self._progress.plan_of_record,
                subtasks=self._progress.subtasks,
                active_subtask=(
                    self._progress.module_id
                ),
                active_is_first=self._progress.active_is_first,
                completed=self._completed,
                sessions=list(self._registry.outstanding()),
                decisions=self._ledger.decisions,
                referrals=self._ledger.referrals,
                dispositions=self._triage.dispositions,
                blocking_claims=self._triage.blocking_claims,
                harness_versions=self._harness.recorded,
                completed_chunks=self._progress.completed_chunks,
                chunk_publishes=self._publisher.chunk_publishes,
                reviewed_ok=self._reviewed_ok,
                reviews=self._book.reviews,
                reader_turns=self._reader_turns,
                picks=self._book.picks,
                last_branch_node=self._progress.last_branch_node,
                published=self._publisher.published,
                nodes=self._nodes,
            )
        )

    def _save_state(self) -> None:
        """
        Persist the run snapshot; never fail the run over it.

        Called as the run advances rather than at the end, so a crash
        leaves behind what has been achieved so far — most importantly
        the plan a human already sat through and approved.

        Locked: the payload walks the session list and the node map,
        and nodes of one stage now run at the same time, so an append
        from another node mid-walk would raise "changed size during
        iteration" — a crash in the one routine whose whole job is to
        make a crash survivable.
        """
        with self._lock:
            self._wt.write_run_state(self._run_id, self._state_payload())

    def _verify_winner(
        self, winner: str, stages: Sequence[pipeline.PipelineStage]
    ) -> None:
        """
        Run the pre-publish gate on *winner* and keep its evidence.

        See :class:`sbx_omnigent.runner.gate.VerifyGate`.

        :param winner: The node whose branch is about to publish.
        :param stages: The stages that ran, in order.
        :raises PipelineRunError: If the gate does not pass.
        """
        outcome = self._gate.check(winner, stages)
        if outcome is not None:
            self._publisher.evidence[winner] = outcome

    def _rerun_stage(self, stage: pipeline.PipelineStage) -> None:
        """
        Run *stage* again, its earlier completion cleared.

        :meth:`_exec_stage` skips anything already recorded done.

        :param stage: The stage.
        """
        self._completed.discard(stage.id)
        self._exec_stage(stage)

    def _verify_publish_target(
        self, stages: Sequence[pipeline.PipelineStage]
    ) -> None:
        """
        Run the gate on the branch a single-pass run will publish.

        :param stages: The stages that ran, in order.
        """
        if self._config.verify is None:
            return
        node_id = self._resolve_publish_node()
        node = self._nodes.get(node_id) if node_id else None
        if node is None:
            return
        branch_node = node.selected if node.kind == 'judge' else node_id
        if branch_node:
            self._verify_winner(branch_node, stages)

    def _release_completed_session(
        self, stage: pipeline.PipelineStage, kind: str
    ) -> None:
        """
        Free a finished reader's or judge's microVM straight away.

        Sessions used to live until the CHUNK published, which for a
        reader or a judge is pure waste: neither is ever driven again.
        Every turn on one is driven inside its own stage, and the
        two loop-back paths — a blocking review and a failed
        verification gate — resolve only to WRITERS. So a judge that
        finished picking sat on a full guest through the refactor, the
        final review and the verification gate: the heaviest part of
        the module, and exactly when the memory is wanted.

        That is not a rounding error. Observed live on a 17 GB host: a
        completed judge held 6 GB while the refactor's linker was being
        OOM-killed, and the gate — the single heaviest job in the run —
        was about to ask for 8 GB more.

        A writer is released only when nothing can drive it again —
        see :meth:`_writer_never_re_driven`. One that a review gate or
        the verification gate can loop back to keeps its session, so
        the fix turn inherits the review context.

        Best-effort. A delete that fails leaves the session tracked, so
        teardown and the next ``--resume`` still try again.

        :param stage: The stage that just completed.
        :param kind: Its :func:`stage_kind`.
        """
        # A verifier and a contract check belong with the reader and
        # the judge: each is driven once, inside its own stage, and
        # nothing loops back to it. Left out of this tuple either would
        # hold a full guest through publish — the exact waste described
        # above.
        if self._keep or kind not in (
            'reader', 'judge', 'writer', 'verify', 'check'
        ):
            return
        if kind == 'writer':
            if not self._writer_never_re_driven(stage):
                return
            # Freeing the VM ends the agent's writes, and a
            # native-terminal writer keeps working past its own settle.
            # Land whatever arrived late before the guest goes.
            self._reconcile_late_writes(stage.id)
        node = self._nodes.get(stage.id)
        session = node.session if node is not None else None
        if session is None or session in self._registry.released:
            return
        if kind == 'reader':
            # The planner's whole conversation is committed beside its
            # plan at publish, and disposing DELETES the session — so
            # read it out here or that record dies with the VM.
            try:
                turns = self._sc.read_transcript(session)
            except SwarmSessionError:
                turns = []
            if turns:
                self._reader_turns[stage.id] = tuple(turns)
            else:
                # THE moment the record is lost. Everything downstream
                # reads an empty buffer and skips in silence, so say it
                # here, while the session that held it still exists.
                click.echo(
                    f'[plan-record] {stage.id}: its transcript read back '
                    f'EMPTY, so there is nothing to buffer. The design '
                    f'conversation dies with this VM.'
                )
        self._disposal.free(
            session,
            f'{stage.id}: freed its microVM — a {kind} is never '
            f're-driven, and holding it costs a full guest for the '
            f'rest of the module.',
        )

    def _writer_never_re_driven(
        self, stage: pipeline.PipelineStage
    ) -> bool:
        """
        Whether nothing left in the pipeline can drive this writer.

        Delegates to :func:`writer_is_terminal` so the release and the
        disk estimate cannot drift apart — the estimate exists to model
        what the runner actually does, and the last time one changed
        without the other it demanded 14 GB that no longer got used.

        :param stage: The writer stage that just completed, whose id
            carries the chunk prefix in a campaign.
        :returns: Whether it is safe to free its microVM.
        """
        stage_id = stage.id
        sub = self._progress.active_subtask
        if sub is not None and stage_id.startswith(f'{sub.id}-'):
            stage_id = stage_id[len(sub.id) + 1 :]
        return writer_is_terminal(self._config, stage_id)

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
                path = self._wt.retain_node_bundle(
                    self._run_id,
                    node,
                    # `base_branch:` is optional in a pipeline.yaml, and
                    # a None here would bundle against the literal ref
                    # 'None'. Same fallback as lineage.seed_ref.
                    against=self._config.base_branch or 'main',
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
            click.echo(
                f'[retain] {node}: kept at {path} — restore with '
                f"'git fetch {path} "
                f"refs/heads/{self._wt.node_branch(self._run_id, node)}"
                f":refs/heads/{node}'."
            )
        return tuple(kept)

    def _sample_disk(self, event: str) -> None:
        """
        Record what this run is holding on disk right now.

        Off unless :data:`sbx_omnigent.disk_metrics.ENABLE_ENV_VAR` is
        set — measuring a 26 GB build tree means walking its inodes, and
        a full cadre has five of them across ~8 boundaries.

        Sampled at every stage boundary rather than once at the end,
        because the preflight predicts a concurrent PEAK and only a time
        series shows one. Records the sbx snapshot store alongside, so
        the same run also answers whether ``per_vm_gb`` is right.

        Never raises, never fails a run: instrumentation that can cost a
        module is worse than no instrumentation.

        :param event: What boundary this is, e.g. ``"chunk-peak"``.
        """
        if not disk_metrics.enabled():
            return
        try:
            records = disk_metrics.sample(
                run_id=self._run_id,
                event=event,
                run_dir=self._wt.run_dir(self._run_id),
                chunk=self._progress.module_id,
                kinds={
                    node: result.kind
                    for node, result in self._nodes.items()
                },
                store_layers=orphans.layer_bytes(),
                free_path=Path.home(),
            )
            written = disk_metrics.append(
                self._wt.metrics_path(self._run_id), records
            )
        except Exception:
            # sample() never raises by contract. Guarded anyway:
            # instrumentation must never be what fails a run.
            return
        if written:
            click.echo(
                f'[disk] {event}: recorded {len(records)} measurement(s) '
                f'to {self._wt.metrics_path(self._run_id)}.'
            )

    def _chunk_preamble(self) -> str:
        """A directive scoping a builder turn to the active chunk."""
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
            f'marked row is yours:\n\n{self._increment_table(sub)}\n\n'
            'The Task below is the contract for the WHOLE plan, not for '
            'your increment alone. A requirement it states that another '
            "row owns is that row's job — not yours, and NOT missing."
            '\n\n'
        )

    def _increment_table(self, sub: pipeline.Subtask) -> str:
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
        :meth:`_module_table` one level up, where a planner shown only
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

    def _dispose_prewarmed(self) -> None:
        """
        Tear down writers pre-warmed during planning (campaign).

        A campaign's nodes are named ``<chunk>-<stage>``, so it can use
        none of these. Each VM goes through
        :meth:`SessionDisposal.free`, so teardown does not dispose it a
        second time, and the clones are removed now rather than left on
        disk until the run ends (#31). ``--keep`` keeps both.
        """
        prewarmed = sorted(self._prewarmed)
        for node_id in prewarmed:
            node = self._nodes.pop(node_id, None)
            if node and node.session:
                self._disposal.free(
                    node.session,
                    f'{node_id}: pre-warmed, but the plan chunked, so a '
                    f'per-chunk writer replaces it.',
                )
        self._prewarmed.clear()
        if self._keep or not prewarmed:
            return
        # Reclaiming disk must never fail a healthy run.
        with contextlib.suppress(click.ClickException):
            self._wt.dispose_node_worktrees(self._run_id, prewarmed)

    def _parallel(
        self, label: str, nodes: list[tuple[str, Callable[[], None]]]
    ) -> None:
        """
        Drive independent nodes of one stage at the same time.

        Falls through to a plain call for a single node, so a pipeline
        that declares no parallelism runs exactly as it did.

        EVERY node is awaited even after one fails. Returning early
        would leave the others driving turns in microVMs nothing is
        tracking any more — orphaned guests are the most expensive
        failure this launcher has, and a slow clean failure beats a
        fast dirty one. The first failure observed is the one raised;
        the rest are reported so a run that lost two nodes does not
        look like it lost one.

        :param label: Stage id, used to name the worker threads.
        :param nodes: ``(node_id, thunk)`` pairs to drive.
        :raises BaseException: The first failure any node raised.
        """
        if len(nodes) < 2:
            for _node_id, run in nodes:
                run()
            return
        click.echo(
            f'[parallel] {label}: driving '
            f'{", ".join(n for n, _ in nodes)} at the same time.'
        )
        failures: list[tuple[str, BaseException]] = []
        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=min(len(nodes), _MAX_PARALLEL_NODES),
            thread_name_prefix=f'node-{label}',
        )
        interrupted = False
        try:
            futures = {
                pool.submit(run): node_id
                for node_id, run in nodes
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except BaseException as exc:
                    failures.append((futures[future], exc))
        except KeyboardInterrupt:
            # Only this thread sees Ctrl-C. Waiting for the workers here
            # meant waiting out every turn in the stage, up to the turn
            # timeout, before anything stopped. Stop now: keep what each
            # turn was doing, start nothing new, and leave the VMs to
            # teardown, which runs as this propagates.
            interrupted = True
            self._interrupted.set()
            click.echo(
                f'[interrupt] {label}: stopping now, without waiting for '
                f'the turns still running.',
                err=True,
            )
            self._capture.interrupted()
            raise
        finally:
            pool.shutdown(wait=not interrupted, cancel_futures=interrupted)
        if not failures:
            return
        for node_id, exc in failures[1:]:
            click.echo(f'[parallel] {label}: {node_id} also failed: {exc}')
        raise failures[0][1]

    def _forfeit_blocked_reviews(
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
            and (node := self._nodes.get(sub.id)) is not None
            and node.kind == 'review'
            and node.verdict != 'BLOCKING'
        ]
        if not survivors:
            return False
        for exc in blocked:
            sub = self._stage_by_id.get(exc.stage_id)
            target = sub.on_block if sub is not None else None
            self._forfeited.add(exc.stage_id)
            if target:
                self._forfeited.add(target)
                self._forfeits[target] = (exc.stage_id, exc.rounds)
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

    def _exec_top_level(self, stage: pipeline.PipelineStage) -> None:
        """
        Run a top-level stage; a blocked review may forfeit a candidate.

        :param stage: A stage from the pipeline's top-level list.
        :raises _Blocked: When the blocked review has no rival candidate
            left to carry the run.
        """
        try:
            self._exec_stage(stage)
        except _Blocked as exc:
            if stage.parallel or stage_kind(stage) != 'review':
                raise
            if not self._forfeit_to_a_judge(stage, exc):
                raise

    def _forfeit_to_a_judge(
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
        rivals = self._rival_candidates(target)
        if not rivals:
            return False
        with self._lock:
            self._forfeited.update((stage.id, target))
            self._forfeits[target] = (stage.id, exc.rounds)
        click.echo(
            f'[forfeit] {stage.id}: no consensus after {exc.rounds} '
            f'round(s), so {target} is withdrawn. The judge still has '
            f'{", ".join(rivals)}; if none of them clears review, the run '
            f'blocks. The reduced field is recorded with the selection.'
        )
        return True

    def _rival_candidates(self, target: str) -> list[str]:
        """
        Candidates still competing with *target* at any judge.

        Read from the DECLARED pipeline: a rival whose review has not
        run yet still counts.

        :param target: The writer node being withdrawn.
        :returns: Rival writer ids not already withdrawn, in order.
        """
        rivals: list[str] = []
        for judge in self._stage_by_id.values():
            if stage_kind(judge) != 'judge' or target not in judge.needs:
                continue
            for need in judge.needs:
                declared = self._stage_by_id.get(need)
                if (
                    need != target
                    and need not in self._forfeited
                    and need not in rivals
                    and declared is not None
                    and stage_kind(declared) == 'writer'
                ):
                    rivals.append(need)
        return rivals

    def _exec_stage(self, stage: pipeline.PipelineStage) -> None:
        if stage.parallel:
            # `sub=sub` binds each node NOW; a bare closure over the
            # loop variable would hand every thread the last node.
            try:
                self._parallel(
                    stage.id,
                    [
                        (sub.id, lambda sub=sub: self._exec_stage(sub))
                        for sub in stage.parallel
                    ],
                )
            except _Blocked as exc:
                # Only a review gate raises this, and only one
                # candidate's review is in each sub-stage. `_parallel`
                # re-raises the first failure, so recover every
                # blocked sibling from the recorded verdicts rather
                # than that one exception.
                blocked = [exc] + [
                    _Blocked(sub.id, self._book.rounds_spent(sub.id))
                    for sub in stage.parallel
                    if sub.id != exc.stage_id
                    and (node := self._nodes.get(sub.id)) is not None
                    and node.verdict == 'BLOCKING'
                ]
                if not self._forfeit_blocked_reviews(stage, blocked):
                    raise
            return
        if self._skip_as_completed(stage):
            return
        kind = stage_kind(stage)
        # The cap counts nodes being DRIVEN, so it is charged to
        # whatever drives one — never to a container. A review stage
        # drives no turn of its own; each reviewer turn takes its own
        # slot (_review_turn). Charging the stage as well would spend
        # the cap on threads doing no work, and two review stages
        # running together would leave only half of it for the four
        # reviewers. It also keeps the cap deadlock-free at any nesting
        # depth: only leaves acquire, and a leaf never waits on
        # another leaf's slot. A scan is charged the same way: it runs
        # the review stages in its needs again after a fix, and each of
        # their turns takes a slot, so the scan takes one only for its
        # own turn (ScanStage._round).
        slot: contextlib.AbstractContextManager[object] = (
            contextlib.nullcontext() if kind in ('review', 'scan')
            else self._node_slots
        )
        # Spans the release below so a node that DOES hand its guest
        # back (a reader, a judge) has done so before the next node is
        # let in. A writer's guest deliberately outlives its slot: a
        # review block may re-drive it, and re-booting costs more than
        # an idle VM.
        run = {
            'writer': self._run_writer,
            'judge': self._run_judge,
            'verify': self._run_verify,
            'check': self._run_check,
            'scan': self._scan.run,
            'review': self._run_review,
        }.get(kind, self._run_reader)
        with slot:
            run(stage)
            with self._lock:
                self._completed.add(stage.id)
            # BEFORE the session release, so the sample sees the
            # worktree while the node is still whole. Off unless asked.
            self._sample_disk(f'stage-complete:{stage.id}')
            self._release_completed_session(stage, kind)
        # Checkpoint AS the run advances: a crash then costs only the
        # node that failed, not the approved plan behind it.
        self._save_state()

    def _skip_as_completed(self, stage: pipeline.PipelineStage) -> bool:
        """
        Whether a stage is skipped as already complete, saying why.

        A plan check is completed inside the plan stage's approval
        (#49), so reaching it afterwards is not a resume and says
        nothing. A contract check recorded complete without a reply
        that says it finished is NOT skipped: an older launcher recorded
        a pass for a check whose turn ended while it waited on a
        background test run, and that must not stand on resume.

        :param stage: The stage about to run.
        :returns: ``True`` to skip it.
        """
        if stage.id not in self._completed:
            return False
        target = self._stage_by_id.get(stage.checks) if stage.checks else None
        if target is not None and is_planner(target, self._config):
            return True
        node = self._nodes.get(stage.id)
        if target is not None and not check_finished(
            node.output if node is not None else None
        ):
            self._completed.discard(stage.id)
            self._nodes.pop(stage.id, None)
            click.echo(
                f'[resume] {stage.id}: recorded as complete, but its reply '
                f'never ended with {_CHECK_COMPLETE}, so it never finished. '
                f'Running it again.'
            )
            return False
        click.echo(f'[resume] skipping {stage.id} (already complete)')
        return True

    # ── provision-only ────────────────────────────────────────────

    def _provision_only(self) -> RunResult:
        """
        Provision every node's worktree + session; keep the VMs up.

        Drives no turns and commits nothing — it stands the topology up
        so a human/coordinator can drive it. Branch handoffs (``from``/
        ``needs`` inheritance) are seeded at provision time, so an
        un-driven upstream seeds from the base; drive top-to-bottom.

        :returns: A ``'provisioned'`` :class:`RunResult` with
            :attr:`RunResult.bindings`.
        :raises Exception: Re-raised after tearing down a partial
            provisioning.
        """
        try:
            for stage in self._config.stages:
                self._provision_stage(stage)
        except Exception:
            self._disposal.teardown(preserve_run=True)
            raise
        return RunResult(
            run_id=self._run_id,
            status='provisioned',
            nodes=dict(self._nodes),
            bindings=list(self._bindings),
        )

    def _provision_stage(self, stage: pipeline.PipelineStage) -> None:
        if stage.parallel:
            for sub in stage.parallel:
                self._provision_stage(sub)
            return
        kind = stage_kind(stage)
        if kind == 'writer':
            self._provision_node(stage, 'writer', 'rw')
        elif kind == 'reader':
            self._provision_node(stage, 'reader', 'ro')
        elif kind == 'check':
            self._provision_node(stage, 'check', 'ro')
        elif kind == 'scan':
            # A scan runs on a fresh clone of what ships, cut when the
            # scan runs; there is nothing to stand up in advance.
            return
        elif kind == 'review':
            target_wt = self._nodes[review_target(stage, self._nodes)].worktree
            assert target_wt is not None
            for reviewer in stage.run:
                label = f'{stage.id}-{reviewer}'
                session = self._driver.create(
                    reviewer, target_wt, 'ro', label
                )
                self._bind(label, reviewer, session, target_wt, 'ro')
            self._nodes[stage.id] = NodeResult(stage.id, 'review')
        else:  # judge
            candidates = self._judge_candidates(stage)
            if not candidates:
                return
            wt = self._wt.create_judge_worktree(
                self._run_id, stage.id, candidates,
                replace=self._resume,
            )
            session = self._driver.create(
                stage.run[0], wt, 'ro', stage.id
            )
            self._nodes[stage.id] = NodeResult(
                stage.id, 'judge', worktree=wt, session=session
            )
            self._bind(stage.id, stage.run[0], session, wt, 'ro')

    def _provision_node(
        self, stage: pipeline.PipelineStage, kind: str, mode: str
    ) -> None:
        agent = stage.run[0]
        wt = self._wt.create_node_worktree(
            self._run_id, stage.id, from_node=seed_from(stage, self._nodes),
            replace=self._resume,
        )
        session = self._driver.create(agent, wt, mode, stage.id)
        branch = (
            self._wt.node_branch(self._run_id, stage.id)
            if kind == 'writer'
            else None
        )
        self._nodes[stage.id] = NodeResult(
            stage.id, kind, branch=branch, worktree=wt, session=session
        )
        self._bind(stage.id, agent, session, wt, mode)

    def _bind(
        self, node: str, agent: str, session: str, worktree: str, mode: str
    ) -> None:
        self._bindings.append(
            {
                'node': node,
                'agent': agent,
                'session': session,
                'worktree': worktree,
                'mode': mode,
            }
        )

    # ── node executors ────────────────────────────────────────────

    def _verify_instruction(
        self,
        pending: list[tuple[ReviewRecord, int, str]],
        reports: list[tuple[ReviewRecord, str]] | None = None,
    ) -> str:
        """
        Ask for a conclusion on each raised finding, against the tree.

        The findings are NUMBERED here and answered by number. A
        echoing `topology/impl-a/bugs/r4#3` back correctly is a
        transcription task with a silent failure mode — a mistyped id
        re-files the finding it was meant to close.

        Blocking reports are asked about in the same turn, under their
        own header. They arrive as prose rather than a list, so the
        verifier enumerates the claims itself instead of a reviewer
        hitting a marker exactly.

        :param pending: ``(record, index, text)`` in presentation order.
        :param reports: ``(record, report)`` for each BLOCKING vote.
        :returns: The turn's instruction.
        """
        parts: list[str] = []
        if pending:
            parts.append(findings_block(pending))
        if reports:
            parts.append(blocking_block(reports))
        return '\n\n'.join(parts)

    def _run_verify(self, stage: pipeline.PipelineStage) -> None:
        """
        Check each raised finding against the code before it is filed.

        The gate exists because most non-blocking findings need no work
        and a human was deriving that one at a time, days later, without
        the branch in front of them. An agent holding the tree can do it
        while the context is still true.

        FAILS OPEN at every step. No findings, no agent, an empty reply,
        an unreadable one — each leaves the dispositions empty, and an
        empty disposition files everything exactly as before the gate
        existed. The gate may cost a wasted triage; it may not lose a
        finding.

        :param stage: The stage declaring ``verifies:``.
        """
        pending = self._triage.pending_findings()
        reports = self._triage.pending_blocking_reports()
        if not pending and not reports:
            self._completed.add(stage.id)
            return
        agent = stage.run[0]
        # Always a fresh clone: after a verify-gate fix this stage runs
        # again, and must read the branch as it now is, not the clone
        # its first run left behind.
        wt = self._wt.create_node_worktree(
            self._run_id, stage.id, from_node=seed_from(stage, self._nodes),
            replace=True,
        )
        session = self._driver.create(agent, wt, 'ro', stage.id)
        out = self._driver.drive(
            session, self._verify_instruction(pending, reports)
        )
        self._nodes[stage.id] = NodeResult(
            stage.id, 'verify', worktree=wt, session=session, output=out
        )
        verdicts = parse_dispositions(out)
        kept = self._triage.record_dispositions(pending, verdicts)
        if pending:
            click.echo(
                f'[verify] {stage.id}: {len(pending)} finding(s) checked, '
                f'{kept} to file, {len(pending) - kept} withheld with a '
                f'recorded reason.'
            )
        if reports:
            claims = self._triage.record_blocking_dispositions(
                reports, parse_blocking_dispositions(out)
            )
            click.echo(
                f'[verify] {stage.id}: {len(reports)} blocking report(s) '
                f'read against the shipping code, {claims} claim(s) that '
                f'still reproduce.'
            )

    def _run_reader(self, stage: pipeline.PipelineStage) -> None:
        agent = stage.run[0]
        wt = self._wt.create_node_worktree(
            self._run_id, stage.id, from_node=seed_from(stage, self._nodes),
            replace=self._resume,
        )
        if is_planner(stage, self._config):
            # BEFORE the instruction is built: the ledger block is
            # assembled from the ledger, so seeding after would hand
            # this planner an empty one.
            self._ledger.seed_from(wt)
        session = self._driver.create(agent, wt, 'ro', stage.id)
        out = self._driver.drive(session, self._reader_instruction(stage))
        node = NodeResult(
            stage.id, 'reader', worktree=wt, session=session, output=out
        )
        self._nodes[stage.id] = node
        if not is_planner(stage, self._config):
            self._halt_on_reader_dispute(stage)
        if is_planner(stage, self._config):
            in_module = self._progress.active_subtask is not None
            if self._interactive_plan:
                # The up-front planner posted its questions; the human
                # replies in the UI while these writer VMs boot, so
                # they are warm the instant the plan is approved. A
                # per-module planner runs inside the loop — its
                # (namespaced) writers aren't pre-warmable here, so it
                # just awaits approval.
                if not in_module and self._config.prewarm:
                    self._prewarm_writers()
                node.output = self._await_plan_approval(
                    session, out, stage
                )
            else:
                # Unattended: an agy planner's reply lags its single
                # turn, so a turn can return empty under load — leaving
                # the plan unshared and uncommitted. Read the SETTLED
                # session for the final plan (the same settled-capture
                # the interactive consolidation turn uses).
                self._sc.wait_for_session_idle(session)
                node.output = self._sc.read_latest_reply(session) or out
                # Nobody is in the session to settle a dispute, so one
                # can only stop the run.
                self._halt_on_plan_disputes(
                    stage, self._run_plan_checks(stage, node.output)
                )
            if not in_module:
                # Flat/single-pass: the up-front planner's final output
                # is THE plan of record (shared with builders, committed
                # on publish) and carries the ordered chunk list.
                self._progress.plan_of_record = node.output
                self._progress.subtasks = parse_subtasks(node.output)
                if self._progress.subtasks:
                    ids = ', '.join(s.id for s in self._progress.subtasks)
                    click.echo(
                        f'[plan] {len(self._progress.subtasks)} subtask(s) '
                        f'proposed: {ids}'
                    )
            else:
                # Per-module: this planner's design reaches THIS
                # module's builders via _reader_context (needs the
                # planner node) and is committed per-module in
                # Campaign._publish — it never overwrites the human
                # module list nor the plan of record. What it CAN
                # contribute beyond its own module is the decisions it
                # settled that bind later ones.
                self._ledger.record_decisions(
                    self._progress.module_id,
                    node.output,
                )

    def _run_check(self, stage: pipeline.PipelineStage) -> None:
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
        writers as design context — :meth:`_reader_context` passes on
        readers only — and it is not counted as a vote on the branch.

        :param stage: The stage declaring ``checks:``.
        :raises PipelineRunError: When the check raised a dispute.
        """
        assert stage.checks is not None
        target = self._stage_by_id[stage.checks]
        if is_planner(target, self._config):
            # Reached only when the plan finished in an earlier attempt:
            # a live plan stage runs its checks inside its approval.
            node = self._nodes.get(target.id)
            plan = node.output if node is not None else ''
            self._halt_on_plan_disputes(
                target,
                [
                    (stage.id, claim)
                    for claim in self._run_plan_check(stage, plan)
                ],
            )
            return
        wt = self._wt.create_node_worktree(
            self._run_id, stage.id, from_node=stage.checks,
            replace=self._resume,
        )
        session = self._driver.create(stage.run[0], wt, 'ro', stage.id)
        first = self._driver.drive(
            session, self._check_instruction(stage), done=_check_done
        )
        out = self._finish_check(stage.id, session, first)
        self._nodes[stage.id] = NodeResult(
            stage.id, 'check', worktree=wt, session=session, output=out
        )
        self._halt_on_reader_dispute(stage)
        self._halt_on_unfinished_check(stage, session, out)
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

    def _await_plan_approval(
        self,
        session: str,
        first_reply: str,
        stage: pipeline.PipelineStage | None = None,
    ) -> str:
        """
        Block the pipeline until a human approves the plan in the UI.

        The planner asks its questions; a human answers them directly in
        the Omnigent plan session and replies ``APPROVED`` once the plan
        is complete. After approval, the planner is driven ONE more turn
        to emit a clean, consolidated FINAL plan (the back-and-forth
        leaves the last message reading like a chat reply); that becomes
        the plan of record shared downstream and committed to the repo.

        The consolidation reply is captured from the SETTLED session,
        not the turn's streamed reply: a conversational agy planner is
        still replying to the human's APPROVED when the turn fires, and
        its reply lags — trusting the streamed reply captures that prior
        message. So it waits for the session to settle around the turn
        and reads the final assistant message. Falls back to the
        approval text, then the first reply, if nothing was captured.

        When a ``checks:`` stage names this plan (#49), every approval
        is followed by that check, before the plan is final. A dispute
        goes back to this session and approval reopens: the human
        settles it with the planner, and approves again, which
        re-consolidates and re-checks. :data:`_APPROVED_AS_IS` accepts
        the plan over the last check's disputes without another check.

        :param session: The planner session.
        :param first_reply: The planner's first reply, used last.
        :param stage: The plan stage, to find the checks that name it.
        :returns: The plan of record.
        """
        checks = self._plan_checks(stage) if stage is not None else []
        click.echo(
            f'[plan] Planner is awaiting your review in the Omnigent UI '
            f'(session {session}). Answer its questions there, then send '
            f'a message whose ENTIRE text is "APPROVED" — a turn that '
            f'says anything else will not release the gate, deliberately. '
            f'The pipeline is blocked until you do.'
            + (
                f' The plan is then checked by '
                f'{", ".join(c.id for c in checks)} before it is final.'
                if checks else ''
            )
        )
        names = ', '.join(c.id for c in checks)
        self._sc.post_notice(
            session,
            'Your move: reply exactly APPROVED when the plan is right',
            'Ask questions or ask for changes in normal messages first. '
            'When the plan is right, send a message that is exactly: '
            'APPROVED\n\nNothing else may be in that message, or it does '
            'not count. The planner then writes the final plan'
            + (
                f', and {names} checks it: a command for each claim the '
                f'plan makes about existing code. That takes a few '
                f'minutes.'
                if checks
                else ', and the pipeline moves on.'
            ),
        )
        disputes: list[tuple[str, str]] = []
        while True:
            approved, phrase = self._wait_for_approval(session, checks)
            self._sc.post_notice(
                session,
                'Approved: the planner is writing the final plan',
                'The planner is consolidating the final plan from this '
                'conversation (one more turn).',
            )
            plan = self._consolidate_plan(session, approved, first_reply)
            if not checks:
                self._sc.post_notice(
                    session, 'The plan is final: the pipeline moves on'
                )
                return plan
            assert stage is not None
            if disputes and phrase == _APPROVED_AS_IS:
                click.echo(
                    f'[plan] {stage.id}: accepted AS IS over the '
                    f'disputes the last check raised, without another '
                    f'check. The disputed round is in the reviews record.'
                )
                self._sc.post_notice(
                    session,
                    'Accepted as is: the plan is final',
                    f'Accepted over {len(disputes)} problem(s) from the '
                    f'last check, without another check. The pipeline '
                    f'moves on.',
                )
                return plan
            self._sc.post_notice(
                session,
                'Checking the plan (a few minutes): nothing to do yet',
                f'{names}: running a command for each claim the plan '
                f'makes about existing code, and reading its sections '
                f'against each other. The result appears here when it '
                f'finishes.',
            )
            disputes = self._run_plan_checks(stage, plan)
            if not disputes:
                click.echo(
                    f'[plan] {stage.id}: the plan check found no false '
                    f'claim or contradiction; the plan is final.'
                )
                self._sc.post_notice(
                    session,
                    'Plan check passed: the plan is final',
                    f'{names} found no false claim and no contradiction. '
                    f'The pipeline moves on.',
                )
                return plan
            self._report_plan_disputes(session, stage, disputes)

    def _wait_for_approval(
        self, session: str, checks: list[pipeline.PipelineStage]
    ) -> tuple[str, str]:
        """
        Wait for the human's approval, capturing the session on loss.

        :param session: The planner session.
        :param checks: The checks naming this plan; with any, the
            override phrase is accepted too.
        :returns: ``(approved plan text, phrase sent)``.
        """
        phrases = _PLAN_DECISION_PHRASES if checks else ('APPROVED',)
        try:
            decision = self._sc.wait_for_plan_decision(
                session, approval_phrases=phrases
            )
            approved = decision.plan
            click.echo(
                f'[plan] approval received from the session '
                f'({len(approved or "")} chars of approved plan); '
                f'consolidating.'
            )
        except KeyboardInterrupt:
            # The expensive one. A human sits in this conversation for
            # tens of minutes, and the plan reaches the run state only
            # once the stage COMPLETES — so interrupting here destroys
            # every question, answer and draft with the session.
            self._capture.turn(
                session, 'interrupted (Ctrl-C) awaiting plan approval'
            )
            raise
        except SwarmSessionError:
            # The same loss through a different door. The gate gives up
            # after a long silence, and that path used to propagate
            # straight past the capture above — the one handler written
            # precisely to stop this session being thrown away.
            self._capture.turn(
                session, 'timed out awaiting plan approval'
            )
            raise
        return approved, decision.phrase

    def _consolidate_plan(
        self, session: str, approved: str, first_reply: str
    ) -> str:
        """
        Drive the consolidation turn and choose the plan of record.

        :param session: The planner session.
        :param approved: The plan text the human approved.
        :param first_reply: The planner's first reply, used last.
        :returns: The consolidated plan.
        """
        # Let the planner's reply to APPROVED settle so the
        # consolidation turn doesn't race it, drive the turn, then let
        # THAT settle and read the final message (agy's reply lags).
        self._sc.wait_for_session_idle(session)
        self._driver.drive(session, self._plan_consolidation_instruction())
        self._sc.wait_for_session_idle(session)
        consolidated = self._sc.read_latest_reply(session)
        # Never trust the consolidation reply blind: a planner that has
        # already written the plan may just acknowledge the approval.
        plan = select_plan_of_record(
            consolidated,
            self._sc.read_assistant_replies(session, tail=_PLAN_REPLY_TAIL),
            approved,
        )
        if plan is not None and plan is not consolidated:
            click.echo(
                f'[plan] the consolidation turn returned only a short '
                f'reply ({len(consolidated or "")} chars); recovered the '
                f'full plan ({len(plan)} chars) from the session.'
            )
        return plan or approved or first_reply

    def _plan_checks(
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
            check for check in self._stage_by_id.values()
            if check.checks == stage.id
        ]

    def _run_plan_checks(
        self, stage: pipeline.PipelineStage, plan: str
    ) -> list[tuple[str, str]]:
        """
        Run every check that names this plan, side by side.

        Each check stage is marked complete here, so the DAG does not
        run it again after the plan stage.

        :param stage: The plan stage.
        :param plan: The plan to check.
        :returns: ``(check id, claim)`` per dispute, check by check.
        """
        checks = self._plan_checks(stage)
        found: dict[str, tuple[str, ...]] = {}

        def run(check: pipeline.PipelineStage) -> None:
            found[check.id] = self._run_plan_check(check, plan)

        self._parallel(
            f'{stage.id}-checks',
            [(check.id, lambda check=check: run(check)) for check in checks],
        )
        with self._lock:
            self._completed.update(check.id for check in checks)
        return [
            (check.id, claim)
            for check in checks
            for claim in found.get(check.id, ())
        ]

    def _run_plan_check(
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
        planner = self._stage_by_id[check.checks]
        wt = self._wt.create_node_worktree(
            self._run_id, check.id, from_node=seed_from(planner, self._nodes),
            replace=True,
        )
        session = self._driver.create(check.run[0], wt, 'ro', check.id)
        out = self._finish_check(
            check.id, session,
            self._driver.drive(
                session,
                self._plan_check_instruction(check, plan),
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
            self._warn_dispute_near_miss(check.id, out)
        self._nodes[check.id] = NodeResult(
            check.id, 'check', worktree=wt, session=session, output=out
        )
        self._book.record_review(
            check.id, check.run[0], self._book.next_round(check.id),
            _CHECK_DISPUTED if claims else _CHECK_VERDICT, session, out,
        )
        self._release_completed_session(check, 'check')
        return claims

    def _finish_check(self, check_id: str, session: str, out: str) -> str:
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

    def _halt_on_unfinished_check(
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

    def _report_plan_disputes(
        self,
        session: str,
        stage: pipeline.PipelineStage,
        disputes: list[tuple[str, str]],
    ) -> None:
        """
        Put a plan check's disputes before the human, in their session.

        Sent as one turn to the planner, so the planner can walk the
        human through each and propose a correction. The checker does
        not negotiate with the planner, and nothing is changed without
        the human: approval reopens, and their next reply decides.

        :param session: The planner session.
        :param stage: The plan stage.
        :param disputes: ``(check id, claim)`` per dispute.
        """
        reports = '\n\n'.join(
            f'--- {check_id} ---\n{self._nodes[check_id].output}'
            for check_id in dict.fromkeys(cid for cid, _c in disputes)
        )
        self._sc.post_notice(
            session,
            f'Plan check found {len(disputes)} problem(s): the planner '
            f'will propose fixes',
            'The report from the check follows, sent to the planner, '
            'which will walk you through each problem.',
        )
        self._driver.drive(
            session,
            f'From the pipeline, not the human: a read-only check of the '
            f'plan you just consolidated found {len(disputes)} problem(s): '
            f'a claim about the existing code that the repository '
            f'contradicts, or two sections that contradict each other. It '
            f'ran commands against the repository; its reports are '
            f'below.\n\n{reports}\n\nWalk the human through each problem '
            f'and propose what you would change, and wait for them. Do not '
            f'output a final plan yet. When they reply APPROVED, the plan '
            f'is consolidated and checked again; if they reply '
            f'{_APPROVED_AS_IS}, it is accepted as it stands.',
        )
        self._sc.post_notice(
            session,
            'Your move: reply APPROVED to re-check, or APPROVED AS IS to '
            'accept',
            'Settle the problems above with the planner, in normal '
            'messages. Then send a message that is exactly one of:\n\n'
            'APPROVED: the planner writes the final plan again, and it is '
            'checked again.\n\nAPPROVED AS IS: the plan is accepted as it '
            'stands, with no further check.',
        )
        listed = '\n'.join(f'  [{cid}] {claim}' for cid, claim in disputes)
        click.echo(
            f'[plan] {stage.id}: the plan check found {len(disputes)} '
            f'problem(s):\n{listed}\nThey are in the planning session '
            f'with the full reports. Settle them there, then reply '
            f'APPROVED (consolidated and checked again) or '
            f'{_APPROVED_AS_IS} (accepted without another check).'
        )

    def _halt_on_plan_disputes(
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

    def _plan_consolidation_instruction(self) -> str:
        """Ask the approved planner for a standalone plan (+ chunks)."""
        base = (
            'The plan is APPROVED. Output the COMPLETE, consolidated final '
            'design plan now as a single self-contained Markdown document '
            'that folds in every decision from our discussion. No '
            'questions, no "reply APPROVED", no conversational framing — '
            'this document is saved as the plan of record and handed to '
            'the builders. Still prose/design only: do NOT write code or '
            'files.\n\nCarry every artifact you produced earlier in this '
            'session forward VERBATIM: diagrams (mermaid included), '
            'schema and DDL blocks, table definitions, interface '
            'signatures, worked examples. This document is committed to '
            'the repository as the design record and must stand alone — '
            'a reader who never saw this conversation gets only what is '
            'in it. Consolidating means folding in the decisions we '
            'reached, NOT summarizing away detail: a shorter document '
            'that dropped a diagram is a worse one.'
        )
        sub = self._progress.active_subtask
        if sub is not None:
            # Per-module planner: consolidate THIS module's design only.
            # The module list is human-supplied, so no SUBTASKS block —
            # the design plus its work breakdown becomes the module's
            # tests (the TDD writer turns it into the failing suite).
            return base + (
                f'\n\nScope: this is module [{sub.id}] — {sub.title}. '
                'Design ONLY this module, treating the artifacts from '
                'prior modules already in your worktree as frozen, fixed '
                'contracts. Include a concrete work breakdown with '
                'mechanically verifiable done-criteria the test writer '
                'can turn directly into a failing test suite.'
                '\n\nFinally, if this module settled anything that '
                'CONSTRAINS a later module — a contract another module '
                'must consume, a convention, an alternative that was '
                'considered and rejected, an agreement reached with the '
                'human — end with a block headed exactly "DECISIONS FOR '
                'LATER MODULES:" and one "- <decision>" line each, each '
                'written so a planner who was NOT in this conversation '
                'can act on it. Omit the block entirely if this module '
                'settled nothing that binds later work.'
            )
        return base + (
            '\n\nThen, as the FINAL section, output an ordered '
            'implementation plan under a line reading exactly '
            '"SUBTASKS:" — one line per independently-buildable increment, '
            'each formatted "- [<short-id>] <one-line goal>" (e.g. '
            '"- [core] contracts and core skeleton"). Order them by '
            'DEPENDENCY — whatever later increments build on comes first — '
            'and the builders implement them one at a time in sequence.'
            '\n\nSize them deliberately, and err LARGE. Every increment '
            'pays a full build-and-review cycle: a microVM for every '
            'writer, reviewer and judge in it, each booting and installing '
            'a toolchain before it does any work of its own, and some of '
            'them compiling the project from clean. That cost is fixed '
            'per increment and identical whether the increment changes '
            'seven lines or seven hundred. So '
            'choose the LARGEST increment that can still be reviewed '
            'properly in one pass, not the smallest that can be separated: '
            'three increments that are one feature in three parts should '
            'usually be one. Split only for a reason you can name — an '
            'artifact that must be frozen before its consumer is written, a '
            'genuinely independent surface, or an increment so large that '
            'one review could not do it justice. If the work is genuinely a '
            'single increment, list exactly one.'
            # Layer 1 of the tests-only gate. A tests stage that cannot
            # write its suite against the existing public surface has
            # nowhere to go at RUN time: in a compiled language a test
            # naming a type that does not exist fails to build rather
            # than fails, taking every other test with it. Asked here so
            # the human meets it at the approval gate they already sit
            # through, instead of a campaign stopping at 3am. Measured:
            # across this project's history every greenfield module
            # needed new surface and every incremental chunk needed
            # none, so the answer is usually one word.
            '\n\nFor EACH increment, add a second line reading '
            '`SURFACE: none` or `SURFACE: <what must exist first>`. It '
            "answers one question: can that increment's tests be "
            'written against the public API that already exists when it '
            'starts? Adding a new crate, module or public type that '
            'nothing exposes yet means the answer is NOT none — name it. '
            'Everything else — new behaviour behind an existing '
            'interface, a new provider method on a trait that is already '
            'there, anything testable by observing calls and results — '
            'is `SURFACE: none`, which is the common case. This is not a '
            'request to design the API; it is a flag so a human can '
            'decide up front rather than a build stopping halfway.'
        )

    def _provision_writer(
        self, stage: pipeline.PipelineStage
    ) -> NodeResult:
        """
        Create a writer node's worktree + session, WITHOUT driving it.

        Shared by the normal writer path and pre-warming: it stands up
        the clone (seeded from the upstream branch as it stands now) and
        boots + warms the microVM. The node's turn is driven later.

        :param stage: The writer stage to provision.
        :returns: The recorded (un-driven) :class:`NodeResult`.
        """
        agent = stage.run[0]
        self._require_seed(stage)
        wt = self._wt.create_node_worktree(
            self._run_id, stage.id, from_node=seed_from(stage, self._nodes),
            replace=self._resume,
        )
        session = self._driver.create(agent, wt, 'rw', stage.id)
        node = NodeResult(
            stage.id,
            'writer',
            branch=self._wt.node_branch(self._run_id, stage.id),
            worktree=wt,
            session=session,
        )
        self._nodes[stage.id] = node
        return node

    def _prewarm_writers(self) -> None:
        """
        Boot every writer node's VM now, during the planning wait.

        Called once the interactive planner is awaiting human approval:
        each writer's microVM boots and its TUI warms while the human is
        still planning, so the moment the plan is approved the swarm is
        ready to drive rather than cold-start node by node. Writers
        are provisioned in stage order, so an upstream's branch exists
        before a downstream one seeds ``from`` it (the downstream clone
        starts at the upstream's base and is reseeded onto the real tip
        at drive time — see :meth:`_run_writer`). Judge/review nodes are
        NOT pre-warmed: they are gated on the writers finishing.

        A writer is pre-warmed ONLY when its seed branch already exists.
        Cutting a clone needs ``origin/pl/<run>/<seed>`` to be a real
        commit, and a writer seeded from a JUDGE (e.g. a refactor pass
        over the winner) has no such branch until that judge runs and
        aliases it — pre-warming it fails the whole run with "not a
        commit". Those writers are simply left to provision lazily at
        drive time, by which point the branch exists.
        """
        for stage in pipeline._iter_stages(self._config.stages):
            if (
                stage_kind(stage) != 'writer'
                or stage.id in self._nodes
            ):
                continue
            seed = seed_from(stage, self._nodes)
            if seed is not None and not self._has_branch(seed):
                continue
            self._provision_writer(stage)
            self._prewarmed.add(stage.id)

    def _has_branch(self, node_id: str) -> bool:
        """Whether *node_id* has a hub branch yet to clone from."""
        node = self._nodes.get(node_id)
        return bool(node and node.branch)

    def _run_writer(self, stage: pipeline.PipelineStage) -> None:
        node = self._nodes.get(stage.id)
        if node is None or node.session is None:
            node = self._provision_writer(stage)
        else:
            # Pre-warmed during planning: the clone was seeded from the
            # upstream's BASE (the upstream had not committed yet), so
            # reseed onto its now-committed tip before driving.
            seed = seed_from(stage, self._nodes)
            if seed is not None:
                self._wt.reseed_node_worktree(
                    self._run_id, stage.id, seed
                )
        self._drive_writer(stage, node)
        # A native-TUI writer (agy) keeps writing files after its turn
        # reports idle; wait for the worktree to settle so the commit
        # captures the finished tree, not an empty/partial one.
        self._wt.wait_for_node_settle(self._run_id, stage.id)
        assert node.session is not None
        self._halt_on_unfinished_writer(stage.id, node.session, node.output)
        self._commit(stage.id, f'{stage.id}: implement')
        # Before the gates below: both answer a problem by re-driving,
        # and a writer cannot re-drive its way out of a contract it
        # cannot satisfy.
        self._halt_on_writer_dispute(stage)
        self._policy.enforce(stage)
        self._progress.last_branch_node = stage.id

    def _drive_writer(
        self, stage: pipeline.PipelineStage, node: NodeResult
    ) -> None:
        """
        Drive a writer's turn, retrying once if the turn was lost.

        A failed turn is salvaged every time: the agent may already
        have written real work into the worktree, and a timeout can
        land AFTER most of a stage is done. Committing it puts that
        work on the node's branch, where it is durable and a resume
        picks it up, instead of dying with the worktree.

        A LOST turn is then retried on a fresh VM mounting the same
        worktree, which already holds the salvaged commit (see
        :data:`_WRITER_TURN_ATTEMPTS` for what counts as lost). The
        VM that lost it is freed first, and when it cannot be freed
        (``--keep``, or a failed dispose) there is no retry: two agents
        would be writing into one tree.

        :param stage: The writer stage.
        :param node: Its node, carrying a live session.
        :raises PipelineRunError: If the turn failed and was not lost,
            or the last attempt failed too.
        :raises SwarmSessionError: Likewise, for a turn that never came
            back.
        """
        attempt = launches = 0
        while True:
            assert node.session is not None
            try:
                node.output = self._finish_writer(
                    stage.id,
                    node.session,
                    self._driver.drive(
                        node.session,
                        self._writer_instruction(stage) + _WRITER_FINISH,
                        done=_writer_done,
                    ),
                )
                return
            except (PipelineRunError, SwarmSessionError) as exc:
                # SwarmSessionError too: a turn timeout raises it, and
                # salvaging only PipelineRunError lost exactly the work
                # the salvage exists for.
                self._commit_partial(stage.id)
                # A VM that never started a runner ran no turn, so it
                # spends a launch, not the lost-turn retry.
                never_started = isinstance(exc, SwarmRunnerUnavailable)
                if never_started:
                    launches += 1
                    exhausted = launches == _LAUNCH_ATTEMPTS
                else:
                    attempt += 1
                    exhausted = (
                        attempt == _WRITER_TURN_ATTEMPTS
                        or not lost_not_failed(exc)
                    )
                if exhausted or not self._disposal.free(
                    node.session,
                    f'{stage.id}: freeing the VM that lost its turn.',
                ):
                    raise
                what = (
                    f'its VM never started a runner (launch {launches} '
                    f'of {_LAUNCH_ATTEMPTS})' if never_started
                    else f'attempt {attempt} was lost'
                )
                click.echo(
                    f'[retry] {stage.id}: {what} ({exc}); its work is '
                    f'committed to the node branch, so waiting '
                    f'{_WRITER_RETRY_BACKOFF_S:.0f}s, then booting a '
                    f'fresh VM on the same worktree.'
                )
                # AFTER the guest is freed, so an outage is not waited
                # out while a dead VM holds its slot.
                time.sleep(_WRITER_RETRY_BACKOFF_S)
                assert node.worktree is not None
                node.session = self._driver.create(
                    stage.run[0], node.worktree, 'rw', stage.id
                )

    def _finish_writer(
        self, stage_id: str, session: str, out: str
    ) -> str:
        """
        Ask a writer once more when it neither disputed nor finished.

        A dispute is never asked again: it stops the run either way.
        Only a reply with neither is sent :data:`_WRITER_FINISH_ASK`,
        and its answer replaces the reply.

        :param stage_id: The writer stage's id.
        :param session: Its session, still alive.
        :param out: Its reply.
        :returns: The reply to act on: *out*, or the answer to the ask.
        """
        if parse_disputes(out) or work_finished(out):
            return out
        click.echo(
            f'[writer] {stage_id}: its reply did not end with '
            f'{_WORK_COMPLETE}, so it has not finished; asking it once to '
            f'finish before anything is committed.'
        )
        return self._driver.drive(
            session, _WRITER_FINISH_ASK, done=_writer_done
        )

    def _halt_on_unfinished_writer(
        self, stage_id: str, session: str, out: str
    ) -> None:
        """
        Stop when a writer never said it finished, keeping its work.

        A reply reaching here was already asked once to finish. Without
        :data:`_WORK_COMPLETE` the tree may still be mid-change, and a
        review or the gate would spend its rounds on it. The tree is
        committed as partial work and the stage is not recorded as
        complete, so ``--resume`` runs it again from that commit.

        :param stage_id: The writer stage's id.
        :param session: Its session, still alive, to capture.
        :param out: Its reply after the one ask to finish.
        :raises PipelineRunError: When the reply never said it finished.
        """
        if parse_disputes(out) or work_finished(out):
            return
        self._capture.turn(session, 'did not finish its turn')
        self._commit_partial(stage_id)
        raise PipelineRunError(
            f'{stage_id}: the writer did not finish its turn. Neither its '
            f'reply nor the one it was asked for ends with a line reading '
            f'{_WORK_COMPLETE}, so the tree may still be mid-change. It '
            f'is committed as partial work, and the run stops rather than '
            f'review it. Its turn is in the run directory under turns/; '
            f'--resume runs the stage again from that commit.'
        )

    def _commit_partial(self, node_id: str) -> None:
        """
        Commit whatever a failed writer left behind; never raise.

        Best-effort by construction: this runs while an error is already
        propagating, so it must not mask that error with one of its own.
        A clean worktree simply commits nothing.

        :param node_id: The writer whose tree to preserve, and the
            author the commit is attributed to — see :meth:`_commit` for
            why the agent's name must not appear here.
        """
        try:
            committed = self._wt.commit_node(
                self._run_id,
                node_id,
                message=f'{node_id}: partial work (turn did not complete)',
                author=f'{node_id} <{node_id}@pipeline.local>',
            )
        except click.ClickException:
            return
        if committed:
            click.echo(
                f'[salvage] {node_id}: the turn failed, but its work was '
                f'committed to the node branch — a resume continues from '
                f'it rather than redoing the stage.'
            )

    def _ensure_writer_session(
        self, stage: pipeline.PipelineStage
    ) -> None:
        """
        Give a restored writer a live session before driving it again.

        A node restored on RESUME deliberately carries no session — the
        VM it named belongs to a process that has exited. But its
        worktree and its committed branch survive, so a review that
        blocks on it (or the verification gate) must still be able to
        loop back. Attach a NEW session to the EXISTING worktree rather
        than re-provisioning: re-cutting the clone would discard exactly
        the work being looped back over.

        Without this, the first blocking review after a resume died on a
        bare ``AssertionError`` — the run's own state having correctly
        refused to record a dead handle.

        :param stage: The writer stage about to be re-driven.
        """
        node = self._nodes[stage.id]
        if node.session is not None or node.worktree is None:
            return
        click.echo(
            f'[resume] {stage.id}: re-attaching a session to its '
            f'existing worktree so the loop-back can run.'
        )
        node.session = self._driver.create(
            stage.run[0], node.worktree, 'rw', stage.id
        )

    def _redrive_writer(
        self,
        stage_id: str,
        findings: str,
        *,
        message: str | None = None,
        instruction: str | None = None,
    ) -> None:
        stage = self._stage_by_id[stage_id]
        node = self._nodes[stage_id]
        # The branch is about to change, so "every reviewer ran the
        # suite against this" stops being true. Harmless while it lived
        # only in memory for one stage; now that it is persisted and
        # survives a resume, a stale entry would tell a later judge not
        # to check a branch nobody has checked since.
        self._reviewed_ok.discard(stage_id)
        self._ensure_writer_session(stage)
        assert node.session is not None
        # A loop-back reuses the writer's session so its fix turn keeps
        # the prior review context — but a RESUMED run re-attaches the
        # node to a fresh session, so there is no history to keep. A
        # refactorer therefore has its contract restated on every fix
        # turn rather than once: it is the one writer whose findings
        # routinely ask for work it is forbidden to do, so the turn
        # that relays them is exactly the turn that must also say so.
        turn = instruction or self._fix_instruction(findings)
        if instruction is None and self._is_refactor(stage):
            turn = f'{self._refactor_contract()}\n\n{turn}'
        # Same reason, same shape: the tests stage is the other writer
        # whose findings routinely ask for work its contract forbids,
        # and a fix turn is exactly when it decides what to assert.
        if instruction is None and self._is_test_writer(stage):
            turn = f'{_FROZEN_TESTS_CONTRACT}\n\n{turn}'
        node.output = self._finish_writer(
            stage_id, node.session,
            self._driver.drive(
                node.session, turn + _WRITER_FINISH, done=_writer_done
            ),
        )
        # Kept even on success: this session is disposed at publish, and
        # what a writer did with a reviewer's findings is exactly what
        # someone reading the pull request later wants to see.
        self._capture.turn(node.session, 'loop-back fix turn')
        self._wt.wait_for_node_settle(self._run_id, stage_id)
        self._halt_on_unfinished_writer(stage_id, node.session, node.output)
        self._commit(
            stage_id, message or f'{stage_id}: address review'
        )

    def _review_turn(
        self,
        stage: pipeline.PipelineStage,
        reviewer: str,
        target_wt: str,
        round_no: int,
        outputs: dict[str, str],
        verdicts: dict[str, str | None],
        created: list[str],
    ) -> None:
        """
        One reviewer's whole turn: boot, review, vote, hand the VM back.

        Extracted so a stage's reviewers can run at the same time (see
        :meth:`_parallel`). They never needed ordering — each mounts
        the same branch read-only and votes independently — and running
        them in series made an increment wait through six reviewer
        turns end to end.

        *outputs* and *verdicts* are written under this reviewer's own
        key and read only once every reviewer has finished, so they
        need no lock of their own.

        :param stage: The review stage.
        :param reviewer: The reviewing agent's name.
        :param target_wt: The reviewed branch's worktree (mounted ro).
        :param round_no: Recorded review round.
        :param outputs: Per-reviewer report text, filled in place.
        :param verdicts: Per-reviewer verdict, filled in place.
        :param created: Collects this reviewer's session so the stage
            frees exactly its own guests. Unlike *outputs*, this IS
            shared with reviewers running concurrently, so it is
            appended under the lock.
        """
        # A reviewer drives its own turn, so it is what the cap counts.
        # Held boot to disposal: a reviewer is freed the moment it
        # votes, so its slot and its guest end together. Blocks rather
        # than failing — a stage declaring more reviewers than the host
        # can hold should run them in waves, not refuse to run.
        with self._node_slots:
            self._review_guest(stage, reviewer, target_wt, round_no,
                               outputs, verdicts, created)

    def _drive_reviewer(
        self,
        stage: pipeline.PipelineStage,
        reviewer: str,
        target_wt: str,
        label: str,
        created: list[str],
    ) -> tuple[str, str]:
        """
        Boot a reviewer and drive its turn, retrying once if it dies.

        A reviewer is the safe thing to retry and the one that keeps
        killing campaigns. Its mount is READ-ONLY, it wrote nothing, and
        no verdict was recorded — so a second attempt costs a turn and
        nothing else. It is also much cheaper than the first: the build
        cache is warm from the attempt that just died.

        Retried on ANY failure rather than only on a diagnosed one. The
        two live cases both reported `failed: None`, which is precisely
        the state where classification is impossible — and the cost of
        guessing wrong is one turn, against a whole campaign for not
        retrying. See :data:`_REVIEW_TURN_ATTEMPTS`.

        A guest that never started a runner ran no turn at all, so it
        is retried against its own allowance, :data:`_LAUNCH_ATTEMPTS`,
        and leaves the turn retry for a turn that actually ran.

        :param stage: The review stage.
        :param reviewer: The reviewing agent's name.
        :param target_wt: The reviewed branch's worktree (mounted ro).
        :param label: The node label used in logs.
        :param created: Collects every session booted here, so the stage
            frees the abandoned one too.
        :returns: ``(session, reply)`` for the attempt that succeeded.
        :raises PipelineRunError: If the last attempt also fails.
        :raises SwarmRunnerUnavailable: If every launch failed.
        """
        attempt = launches = 0
        while True:
            session = self._driver.create(reviewer, target_wt, 'ro', label)
            # Recorded the moment it exists, not once the turn succeeds,
            # so a reviewer that dies mid-review still hands its
            # guest to the stage backstop instead of leaking it.
            with self._lock:
                created.append(session)
            try:
                return session, self._driver.drive(
                    session, self._review_instruction(stage),
                    done=_review_done,
                )
            except SwarmRunnerUnavailable as exc:
                # No turn ran, so this spends a launch, not an attempt.
                launches += 1
                if launches == _LAUNCH_ATTEMPTS:
                    raise
                click.echo(
                    f'[review] {label}: its VM never started a runner '
                    f'(launch {launches} of {_LAUNCH_ATTEMPTS}: {exc}) — '
                    f'waiting {_REVIEW_RETRY_BACKOFF_S:.0f}s, then '
                    f'booting a fresh guest.'
                )
            except (PipelineRunError, SwarmSessionError) as exc:
                # SwarmSessionError as well as PipelineRunError: a turn
                # that never CAME BACK is as safe to retry as one that
                # came back failed, and every word of this method's
                # rationale covers it — read-only mount, nothing
                # written, no verdict recorded. It was escaping because
                # the two live in disjoint hierarchies, so the stage
                # died on the first dropped stream or server hiccup.
                attempt += 1
                if attempt == _REVIEW_TURN_ATTEMPTS:
                    raise
                click.echo(
                    f'[review] {label}: attempt {attempt} died '
                    f'({exc}); its mount was read-only and it recorded '
                    f'no verdict, so nothing is lost — waiting '
                    f'{_REVIEW_RETRY_BACKOFF_S:.0f}s, then booting a '
                    f'fresh guest and reviewing again.'
                )
            self._disposal.free(
                session, f'{label}: freeing the guest that died.'
            )
            # AFTER the guest is freed, so an outage is not waited out
            # while a dead VM holds its slot.
            time.sleep(_REVIEW_RETRY_BACKOFF_S)

    def _review_guest(
        self,
        stage: pipeline.PipelineStage,
        reviewer: str,
        target_wt: str,
        round_no: int,
        outputs: dict[str, str],
        verdicts: dict[str, str | None],
        created: list[str],
    ) -> None:
        """The turn itself, once a guest slot is in hand."""
        label = f'{stage.id}-{reviewer}'
        session, out = self._drive_reviewer(
            stage, reviewer, target_wt, label, created
        )
        verdict, text = self._reviewer_verdict(session, out, label)
        outputs[reviewer] = text
        verdicts[reviewer] = verdict
        # BEFORE the disposal below: disposing a session deletes its
        # transcript, so this is the last moment the report exists
        # anywhere.
        self._book.record_review(
            stage.id, reviewer, round_no, verdict, session, text
        )
        # As soon as it votes, not once the stage ends. A reviewer that
        # has voted is holding a full guest away from one still
        # working, and reviewers are told to EXECUTE what they review,
        # so the others need that memory. Observed live while reviewers
        # still ran in series: one voted APPROVED and its idle 6 GB
        # guest stayed up for 1h50m while the remaining reviewer's
        # build thrashed at load 18 with every linker at 0% CPU.
        self._disposal.free(
            session,
            f'{label}: voted — freeing its microVM now so the '
            f'reviewers still working are not starved.',
        )

    def _run_review(self, stage: pipeline.PipelineStage) -> None:
        target = review_target(stage, self._nodes)
        # The budget is charged to the STAGE, not to this call: a
        # gate failure re-enters here, and held in a local the count
        # started over while the recorded number kept climbing.
        #
        # The RECORDED number still comes from the records instead
        # (`ReviewBook.next_round`). Deriving it from the budget
        # labelled every re-review "round 1" — six of them on one
        # branch, which reads as six independent first rounds rather
        # than one block and the re-reviews that followed — and
        # collided the run-dir filenames, so each re-review overwrote
        # the last.
        inconclusive_rounds = 0
        while True:
            outputs: dict[str, str] = {}
            verdicts: dict[str, str | None] = {}
            round_no = self._book.next_round(stage.id)
            created: list[str] = []
            # Both per ROUND, not once per stage. A blocked round
            # re-drives the writer, so round two must read what that
            # fix turn produced — and a native-terminal writer can
            # still be writing when this round starts.
            self._reconcile_late_writes(target)
            target_wt = self._review_snapshot(
                stage.id, target, round_no
            )
            try:
                self._parallel(
                    stage.id,
                    [
                        (
                            f'{stage.id}-{reviewer}',
                            # partial binds every argument NOW. A
                            # closure would capture the enclosing retry
                            # loop's variables by reference and read
                            # whatever the NEXT round rebound them to.
                            functools.partial(
                                self._review_turn, stage, reviewer,
                                target_wt, round_no, outputs, verdicts,
                                created,
                            ),
                        )
                        for reviewer in stage.run
                    ],
                )
            finally:
                # Backstop: anything the per-reviewer release could not
                # free (a delete that failed) is retried here. BY NAME,
                # not by an index mark — review-a and review-b run
                # concurrently, and a mark taken by one slices away the
                # other's live guests.
                #
                # Before the snapshot goes: those guests have it
                # mounted. In a `finally` so a turn that raises frees
                # both rather than leaking a guest AND a clone.
                self._disposal.dispose(created)
                self._dispose_review_snapshot(stage.id, round_no)
            self._note_environment_limits(stage.id, round_no)
            # A reviewer that never stated a verdict did not vote
            # AGAINST the branch — it failed to review. Those are
            # different failures with different remedies, and
            # conflating them is expensive: the writer gets re-driven
            # over a branch nobody objected to, and is handed the
            # reviewer's own narration in place of findings. Observed
            # live, a coder replied "there is no actionable finding to
            # address — it names no defect, file, line, or assertion"
            # and was right.
            blocking = [r for r, v in verdicts.items() if v == 'BLOCKING']
            silent = [r for r, v in verdicts.items() if v is None]
            # The environment stopped these, not the code. Like
            # silence, it is no finding the writer can act on: live, a
            # coder handed "cargo is unavailable" changed a deferred
            # item instead, and the next round found a defect in it.
            unverified = [
                r for r, v in verdicts.items() if v == _UNVERIFIED
            ]
            if not blocking and not silent and not unverified:
                self._nodes[stage.id] = NodeResult(
                    stage.id, 'review', verdict='APPROVED'
                )
                # Every reviewer of this branch installed a toolchain
                # and ran the suite against it. Remember that, so a
                # downstream judge is not sent to re-derive it.
                self._reviewed_ok.add(target)
                return
            if not blocking:
                # Nobody objected; the review simply did not conclude.
                # Retry the REVIEWERS, not the writer.
                inconclusive_rounds += 1
                if inconclusive_rounds <= _MAX_SILENT_REVIEW_ROUNDS:
                    why = '; '.join(
                        part for part in (
                            f'{", ".join(silent)} stated no verdict'
                            if silent else '',
                            f'{", ".join(unverified)} could not verify '
                            f'(the environment, not the code)'
                            if unverified else '',
                        ) if part
                    )
                    click.echo(
                        f'[review] {stage.id}: {why}, and nobody blocked '
                        f'— re-running the review on fresh VMs rather '
                        f'than re-driving {stage.on_block or "the writer"} '
                        f'over findings that do not exist '
                        f'({inconclusive_rounds}/'
                        f'{_MAX_SILENT_REVIEW_ROUNDS}).'
                    )
                    continue
                if unverified:
                    reasons = '\n'.join(
                        f'  {r}: {_excerpt(outputs.get(r, ""))}'
                        for r in unverified
                    )
                    click.echo(
                        f'[review] {stage.id}: {", ".join(unverified)} '
                        f'could not verify the branch, across '
                        f'{_MAX_SILENT_REVIEW_ROUNDS + 1} attempts: the '
                        f'environment stopped the build or the tests, '
                        f'and no defect was found. Blocking the run — the '
                        f'writer cannot fix the sandbox. Fix the '
                        f'environment (toolchain, image, egress) and '
                        f'--resume. What stopped them:\n{reasons}'
                    )
                # Out of retries. Silence is still BLOCKING — the
                # safe default is intact — but it blocks the RUN. It
                # must never re-drive the writer, because there is
                # nothing to hand it: relaying narration that names no
                # defect, file, line or assertion is precisely the harm
                # this separation exists to stop.
                if silent:
                    click.echo(
                        f'[review] {stage.id}: {", ".join(silent)} never '
                        f'stated a verdict, across '
                        f'{_MAX_SILENT_REVIEW_ROUNDS + 1} attempts. '
                        f'Blocking — a review that did not conclude is '
                        f'not a finding anyone can act on. Their reports '
                        f'are in the run directory.'
                    )
                self._nodes[stage.id] = NodeResult(
                    stage.id, 'review', verdict='BLOCKING'
                )
                raise _Blocked(
                    stage.id, self._book.rounds_spent(stage.id) + 1
                )
            self._halt_on_dispute(stage, outputs, blocking)
            rounds = self._book.spend_round(stage.id)
            if rounds > self._max_rounds or not stage.on_block:
                self._nodes[stage.id] = NodeResult(
                    stage.id, 'review', verdict='BLOCKING'
                )
                raise _Blocked(stage.id, rounds)
            # ONLY explicit blockers supply findings.
            findings = '\n\n'.join(
                f'[{r}]\n{outputs[r]}' for r in blocking
            )
            self._redrive_writer(stage.on_block, findings)

    def _note_environment_limits(self, stage_id: str, round_no: int) -> None:
        """
        Say what the sandbox stopped this round's reviewers running.

        Only what is NEW to the stage: an item an earlier round already
        reported is not repeated, so each is said once per stage (#41).
        Derived from the records, so a resumed run does not repeat
        what it said before stopping.

        :param stage_id: The review stage whose round just voted.
        :param round_no: That round.
        """
        with self._lock:
            records = [r for r in self._book.reviews if r.stage == stage_id]
        earlier = {
            text
            for rec in records
            if rec.round_no < round_no
            for text in rec.environment
        }
        fresh = [
            (text, who)
            for _stage, items in environment_by_stage(
                [r for r in records if r.round_no == round_no]
            )
            for text, who in items
            if text not in earlier
        ]
        if not fresh:
            return
        listed = '\n'.join(
            f'  - {_printable(text)} [{", ".join(who)}]'
            for text, who in fresh
        )
        click.echo(
            f'[environment] {stage_id}: the sandbox stopped '
            f'{len(fresh)} supplementary check(s). They were not run, '
            f'and are not filed as findings:\n{listed}'
        )

    def _warn_dispute_near_miss(self, label: str, text: str | None) -> None:
        """
        Say so when a reply declares a dispute the parser cannot read.

        WARNS and returns. Halting here would mean acting on the bare
        word, which is the false-positive risk :data:`_DISPUTE_RE` was
        tightened to avoid; saying nothing is what let a correct
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

    def _halt_on_writer_dispute(self, stage: pipeline.PipelineStage) -> None:
        """
        Stop when a WRITER says its own contract cannot be satisfied.

        :meth:`_halt_on_dispute` covers the review path, but a writer
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

    def _halt_on_reader_dispute(self, stage: pipeline.PipelineStage) -> None:
        """
        Stop when a READER says the contract cannot be satisfied.

        Every reader turn ends with :data:`_UNATTENDED`, which tells it
        to stop on a ``DISPUTED:`` line when a conflict cannot be
        resolved, and nothing read it: a reader that did as told was
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
        node = self._nodes.get(stage.id)
        output = node.output if node else None
        claims = parse_disputes(output)
        if not claims:
            self._warn_dispute_near_miss(stage.id, output)
            return ()
        # The claim after the marker is one line. The REASONING that
        # makes it actionable — which test, which two clauses, why no
        # code satisfies both — is the rest of the reply, and this
        # stage is about to end the run and dispose the VM. Capture
        # before raising, the same reason `_await_plan_approval` does.
        # Live on `ingestion-m2-5` this was missing: the halt fired
        # correctly and the human was left with "m2 cannot be marked
        # complete under the binding constraints" and no way to learn
        # why.
        if node is not None and node.session:
            self._capture.turn(node.session, captured_as)
        return claims

    def _halt_on_dispute(
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
                self._warn_dispute_near_miss(name, outputs.get(name))
            return
        self._nodes[stage.id] = NodeResult(
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

    @staticmethod
    def _snapshot_label(stage_id: str, round_no: int) -> str:
        """
        The snapshot directory name for one review round.

        Keyed on the STAGE, not the reviewed node: two review stages
        can target one writer, and a stage id is unique by
        construction while a target is not.
        """
        return f'{stage_id}-r{round_no}'

    def _review_snapshot(
        self, stage_id: str, target: str, round_no: int
    ) -> str:
        """
        Cut the tree this review round reads, at the branch tip.

        Raises rather than falling back to the writer's live clone: the
        fallback is the behaviour this exists to remove, and taking it
        silently would leave a round racing a writer with nothing in
        the log to say so.

        :param stage_id: The review stage (names the snapshot).
        :param target: The writer node under review.
        :param round_no: The recorded review round.
        :returns: The snapshot path, mounted ``:ro`` by every reviewer.
        :raises PipelineRunError: If the snapshot cannot be cut.
        """
        label = self._snapshot_label(stage_id, round_no)
        try:
            return self._wt.create_review_snapshot(
                self._run_id, target, label=label
            )
        except click.ClickException as exc:
            raise PipelineRunError(
                f'could not snapshot {target!r} for review round '
                f'{round_no}: {exc}. Reviewers read a snapshot of the '
                f'branch so a writer still working cannot change the '
                f'tree under them, and the judge reads the same '
                f'commit — so this is not something to work around by '
                f'reading the live worktree.'
            ) from exc

    def _dispose_review_snapshot(
        self, stage_id: str, round_no: int
    ) -> None:
        """
        Remove a round's snapshot once its reviewers have voted.

        Best-effort: a clone that will not delete must never fail a
        review that decided cleanly. It is source-only, and a resume
        replaces it by name.

        :param stage_id: The review stage that named the snapshot.
        :param round_no: The recorded review round.
        """
        label = self._snapshot_label(stage_id, round_no)
        with contextlib.suppress(click.ClickException):
            self._wt.dispose_node_worktrees(self._run_id, [label])

    def _reconcile_late_writes(self, node_id: str) -> None:
        """
        Commit work that landed after a writer's stage finished.

        A native-terminal writer keeps working after its turn reports
        done AND after the settle-and-commit fires. Observed live: a
        writer committed a 31-line manifest at 23:58 and wrote the
        1080-line implementation at 00:26 — twenty-eight minutes later
        — leaving its branch a stub that a human had to fix by hand. No
        grace at commit time can cover a gap like that, so the branch
        is reconciled with the worktree at the moment it starts to
        matter: before reviewers read it.

        That placement is deliberate. Reviewers mount the WORKTREE
        ``:ro`` while the judge clones the BRANCH, so a divergence
        means the two disagree about what a candidate even is — one
        spends half an hour on code the other will never see.
        Reconciling here makes them agree.

        Best-effort throughout: a reconcile that cannot run must never
        fail a review that would otherwise proceed.

        :param node_id: The writer node about to be reviewed.
        """
        node = self._nodes.get(node_id)
        stage = self._stage_by_id.get(node_id)
        if node is None or node.kind != 'writer' or stage is None:
            return
        try:
            dirty = self._wt.node_is_dirty(self._run_id, node_id)
            # An agent that ran `git commit` itself leaves a CLEAN
            # tree that the hub has never seen. Asking only about
            # dirtiness read that as nothing to do, so the work
            # reached no reviewer, no judge and no publish.
            ahead = self._wt.node_ahead_of_hub(self._run_id, node_id)
            if not dirty and not ahead:
                return
            click.echo(
                f'[commit] {node_id}: '
                + (
                    'work landed after its stage finished'
                    if dirty
                    else 'the agent committed inside its VM and the '
                         'branch never saw it'
                )
                + ' — settling and committing it so the branch matches '
                'what the reviewers will read.'
            )
            if dirty:
                self._wt.wait_for_node_settle(self._run_id, node_id)
            self._commit(node_id, f'{node_id}: late write')
        except click.ClickException as exc:
            click.echo(f'[commit] {node_id}: could not reconcile: {exc}')

    def _reviewer_verdict(
        self, session: str, streamed: str, label: str
    ) -> tuple[str | None, str]:
        """
        Resolve ``(verdict, reply_text)`` for one reviewer turn.

        When the streamed reply already carries a ``VERDICT:`` token it
        is used directly — no added latency. Otherwise the real verdict
        may only be in the SETTLED session, for EITHER harness: an agy
        reviewer streams an opening narration and mirrors its verdict a
        beat later (premature-idle / reply-lag), and a Claude reviewer
        emits its verdict in a FINAL message that can sit behind
        intermediate tool-narration a mid-turn quiescence idle
        mis-captured as the streamed reply (observed live in
        mixed-models). Trusting a no-verdict stream reads that as a
        missing verdict → the gate blocks and loops back spuriously,
        wasting a build fix turn and a reviewer VM each round. So poll
        the SETTLED session (settle once, then re-read the recent
        replies) until a verdict token appears, bounded by a WALL-CLOCK
        deadline (:data:`_VERDICT_POLL_DEADLINE_S`) — not an attempt
        count, because agy's premature idles make each settle-wait
        return in seconds, so a fixed-attempt budget expires long before
        a slow review's verdict lands (~160s under load). ``None`` after
        the deadline stays blocking (safe). The returned text is the
        settled reply when polled (so the reviewer's real findings, not
        an opening narration, are what loop back to the writer).

        :param session: The reviewer session just driven.
        :param streamed: The turn's streamed reply.
        :param label: The reviewer's node label (for the capture log).
        :returns: ``(verdict, reply_text)`` — verdict is ``'APPROVED'``,
            ``'BLOCKING'``, or ``None``.
        """
        verdict = parse_verdict(streamed)
        if verdict is not None:
            self._log_verdict(label, verdict, 'stream')
            return verdict, streamed
        text = streamed
        self._sc.wait_for_session_idle(session)
        started = time.monotonic()
        deadline = started + _VERDICT_POLL_DEADLINE_S
        marker = self._driver.newest_item_id(session)
        while True:
            settled = self._sc.read_recent_reply_text(session)
            if settled:
                text = settled
            verdict = parse_verdict(settled)
            if verdict is not None:
                self._log_verdict(label, verdict, 'settled')
                return verdict, text
            now = time.monotonic()
            if (
                now >= deadline
                or now - started >= _VERDICT_POLL_CEILING_S
            ):
                break
            time.sleep(_VERDICT_POLL_INTERVAL_S)
            current = self._driver.newest_item_id(session)
            if current != marker:
                # It is still producing output, so it is not silent —
                # restart the silence clock. The ceiling above still
                # bounds a reviewer that talks without ever voting.
                marker = current
                deadline = time.monotonic() + _VERDICT_POLL_DEADLINE_S
        return self._nudge_for_verdict(session, label, text)

    def _nudge_for_verdict(
        self, session: str, label: str, text: str
    ) -> tuple[str | None, str]:
        """
        Ask a silent reviewer for a verdict before calling it a block.

        See :data:`_VERDICT_NUDGE`: a reviewer that actually executes
        what it reviews can outlive its own turn and stop mid-narration,
        and treating that as a vote against the branch is expensive and
        wrong. A failed nudge is not fatal — it just leaves the safe
        default in place.

        :param session: The reviewer session.
        :param label: Its node label (for the capture log).
        :param text: The best reply text captured so far.
        :returns: ``(verdict, reply_text)``.
        """
        click.echo(
            f'[review] {label}: no verdict yet — asking for one before '
            f'treating it as blocking.'
        )
        try:
            # inline: an agy reviewer would otherwise get this as a
            # pointer to a file it must choose to re-read — which is
            # exactly what it does not do when it thinks it is mid-work.
            nudged = self._driver.drive(
                session, _VERDICT_NUDGE, inline=True, done=_review_done
            )
        except PipelineRunError:
            self._log_verdict(label, None, 'none')
            return None, text
        verdict = parse_verdict(nudged)
        if verdict is None:
            self._sc.wait_for_session_idle(session)
            settled = self._sc.read_recent_reply_text(session)
            if settled:
                nudged = settled
            verdict = parse_verdict(settled)
        if verdict is None:
            self._log_verdict(label, None, 'none')
            return None, text
        self._log_verdict(label, verdict, 'nudged')
        # Keep BOTH: the reviewer's own findings live in the earlier
        # narration, and the nudge reply is often just the token.
        combined = f'{text}\n\n{nudged}'.strip() if text.strip() else nudged
        return verdict, combined

    @staticmethod
    def _log_verdict(label: str, verdict: str | None, source: str) -> None:
        """Record how a reviewer's verdict was captured (live runs)."""
        shown = verdict or 'NONE->BLOCKING'
        click.echo(f'[review] {label}: verdict={shown} (source={source})')

    def _run_judge(self, stage: pipeline.PipelineStage) -> None:
        candidates = self._judge_candidates(stage)
        forfeited = tuple(
            (need, *self._forfeits[need])
            for need in stage.needs
            if need in self._forfeits
        )
        if not candidates:
            raise PipelineRunError(
                f'judge {stage.id!r} has no writer candidates in needs'
            )
        self._require_one_contract(stage, candidates)
        if forfeited and len(candidates) == 1:
            self._select_uncontested(stage, candidates[0], forfeited)
            return
        wt = self._wt.create_judge_worktree(
            self._run_id, stage.id, candidates, replace=self._resume
        )
        agent = stage.run[0]
        session = self._driver.create(agent, wt, 'ro', stage.id)
        out = self._driver.drive(
            session, self._judge_instruction(stage, candidates)
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
                session, self._judge_retry_instruction(candidates)
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
        self._wt.alias_node_branch(self._run_id, stage.id, sel)
        self._nodes[stage.id] = NodeResult(
            stage.id,
            'judge',
            branch=self._nodes[sel].branch,
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
        self._wt.alias_node_branch(self._run_id, stage.id, selected)
        self._nodes[stage.id] = NodeResult(
            stage.id,
            'judge',
            branch=self._nodes[selected].branch,
            selected=selected,
        )
        self._progress.last_branch_node = stage.id

    # ── wiring helpers ────────────────────────────────────────────

    def _branch_producing_needs(
        self, stage: pipeline.PipelineStage
    ) -> list[str]:
        """
        The stages *stage* declares a need on that produce a branch.

        Read from the DECLARED pipeline, not from the nodes that have
        run: the bug this serves is precisely a node being absent, so
        asking the run what exists answers the wrong question.

        :param stage: The stage whose needs to classify.
        :returns: Ids of needed writer/judge stages, in declared order.
        """
        return [
            need
            for need in stage.needs
            if (needed := self._stage_by_id.get(need)) is not None
            and stage_kind(needed) in ('writer', 'judge')
        ]

    def _require_seed(self, stage: pipeline.PipelineStage) -> None:
        """
        Refuse to cut a writer that has lost the branch it inherits.

        ``seed_from`` returns ``None`` for two different situations:
        this writer legitimately starts from the base branch, and the
        writer it inherits from is missing. They are indistinguishable
        to the caller, and the second silently substitutes
        ``base_branch`` — which for a competing implementation means
        building with none of the frozen tests its rival is held to.
        The judge then compares work done under two contracts and
        reports it as a choice.

        There is no case where cutting a competing implementation from
        an unfrozen base is what was wanted, so this refuses instead.

        :param stage: The writer stage about to be provisioned.
        :raises PipelineRunError: When a needed writer/judge branch
            cannot be resolved.
        """
        if seed_from(stage, self._nodes) is not None:
            return
        missing = self._branch_producing_needs(stage)
        if not missing:
            return
        raise PipelineRunError(
            f'writer {stage.id!r} inherits from '
            f'{", ".join(repr(n) for n in missing)}, and no branch for '
            f'it can be resolved. Cutting from the base branch instead '
            f'would build this stage without anything its upstream '
            f'produced — for a competing writer, without the frozen '
            f'tests its rival is held to — so the run stops here. A '
            f'resume whose state lost that node is the usual cause.'
        )

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
            cand_stage = self._stage_by_id.get(cand)
            if cand_stage is None:
                continue
            seed = seed_from(cand_stage, self._nodes)
            if seed is None or seed not in self._nodes:
                continue
            if self._wt.branch_contains(
                self._run_id, cand, ancestor=seed
            ):
                continue
            raise PipelineRunError(
                f'judge {stage.id!r} cannot compare {cand!r}: its '
                f'branch does not contain {seed!r}, so it was built '
                f'without the contract its rivals were held to. '
                f'Judging it would compare two different contracts '
                f'and record the result as a choice.'
            )

    def _judge_candidates(
        self, stage: pipeline.PipelineStage
    ) -> list[str]:
        """The competing writer nodes a judge compares (from needs)."""
        return [
            need
            for need in stage.needs
            if self._nodes.get(need)
            and self._nodes[need].branch
            and self._nodes[need].kind == 'writer'
            # A candidate whose review never reached consensus
            # forfeited; judging it would compare a vetted branch
            # against an unvetted one and call the result a choice.
            and need not in self._forfeited
        ]

    # ── session + turn plumbing ───────────────────────────────────

    def _commit(self, node_id: str, message: str) -> None:
        """
        Commit a node's work, attributed to the NODE.

        Attributed to the node (``impl-a``) and never to the agent that
        happened to run it (``impl_claude``), because the judge and the
        reviewers can read this. Each judge candidate is a standalone
        clone with a real ``.git`` directory — deliberately, so git
        works inside the VM — so one ``git log`` in ``./impl-a`` printed
        the model family straight out of the author field, and the
        reviewers see the same through their ``:ro`` node mount. A judge
        that can tell which family wrote which candidate is not making
        the blind comparison the two-writer race assumes.

        The agent name is not a parameter at all, rather than a
        parameter callers are trusted to pass neutrally: the only way to
        keep this closed is to make the leak unexpressible. It is also
        simply more accurate — the node is what the branch is named for.

        :param node_id: The node whose worktree to commit.
        :param message: The commit message.
        """
        self._wt.commit_node(
            self._run_id,
            node_id,
            message=message,
            author=f'{node_id} <{node_id}@pipeline.local>',
        )

    # ── instruction composition ───────────────────────────────────

    def _task_block(self) -> str:
        block = f'{self._chunk_preamble()}Task:\n{self._config.task}'
        if self._config.acceptance:
            block += f'\n\nAcceptance contract:\n{self._config.acceptance}'
        return (
            f'{block}\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            + setup_block(self._config)
        )

    def _reader_context(self, stage: pipeline.PipelineStage) -> str:
        parts = []
        for need in stage.needs:
            node = self._nodes.get(need)
            if node and node.kind == 'reader' and node.output:
                parts.append(f'--- Design from {need} ---\n{node.output}')
        return '\n\n'.join(parts)

    def _reader_instruction(self, stage: pipeline.PipelineStage) -> str:
        # A planner must be told to PLAN the feature, not handed the raw
        # "Task: add <feature>" implementation directive — else the
        # concrete build instruction overrides its plan-only role prompt
        # and it tries to implement (or hunt for a writable path).
        if is_planner(stage, self._config):
            return self._planner_instruction(stage)
        ctx = self._reader_context(stage)
        return self._task_block() + (f'\n\n{ctx}' if ctx else '')

    def _check_instruction(self, stage: pipeline.PipelineStage) -> str:
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
        target = self._stage_by_id[stage.checks]
        try:
            changed = self._wt.node_diff_files(
                self._run_id, target.id, against=seed_ref(self._ctx, target)
            )
        except click.ClickException:
            changed = []
        files = (
            '\n'.join(f'  - {path}' for path in sorted(changed))
            or '  (none could be listed; read the branch history)'
        )
        design = self._reader_context(target)
        acceptance = (
            f'\n\nAcceptance contract:\n{self._config.acceptance}'
            if self._config.acceptance
            else ''
        )
        return (
            f'{self._chunk_preamble()}CHECK THE CONTRACT before anyone '
            f'builds against it. Do NOT implement anything and do not '
            f'change the tests: your mount is read-only, and the writers '
            f'start only after you reply. Stage {target.id!r} wrote a '
            f'frozen test suite for the brief below; two writers will '
            f'each build against it. Read the brief, the acceptance '
            f'contract, the plan and the suite together, run the suite '
            f'against this unmodified tree, and decide whether any '
            f'implementation could satisfy all of them.\n\n'
            f'Brief:\n{self._config.task}{acceptance}'
            + (f'\n\n{design}' if design else '')
            + f'\n\nFiles {target.id!r} added or changed:\n{files}'
            f'{_CHECK_RULES}{_REVIEW_MOUNT}'
            f'\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            + setup_block(self._config)
        )

    def _plan_check_instruction(
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
            f'\n\nAcceptance contract:\n{self._config.acceptance}'
            if self._config.acceptance
            else ''
        )
        return (
            f'{self._chunk_preamble()}CHECK THE PLAN before it becomes the '
            f'plan of record. Do NOT implement anything: your mount is '
            f'read-only, and it is the repository as it stands before any '
            f'of the plan is built. A human has approved the plan below, '
            f'pending this check; every later stage will build on it.'
            f'\n\nBrief:\n{self._config.task}{acceptance}'
            f'\n\nThe plan ({stage.checks!r}):\n{plan}'
            f'{_PLAN_CHECK_RULES}{_REVIEW_MOUNT}'
            f'\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            + setup_block(self._config)
        )

    def _module_table(self, sub: pipeline.Subtask) -> str:
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

    def _planner_instruction(self, stage: pipeline.PipelineStage) -> str:
        """
        Frame a planner node's turn as producing a design plan.

        Describes the feature to PLAN rather than telling the agent to
        build it, so the concrete instruction agrees with the plan-only
        role prompt: design in prose, implement nothing, write nothing.
        Kept deliberately terse — the ``planner`` template carries the
        full role rules, and the per-turn text must stay under agy's
        paste cap (:data:`_AGY_PASTE_SAFE_MAX`) so the planner turn
        renders inline instead of collapsing into a placeholder.
        """
        sub = self._progress.active_subtask
        if sub is not None:
            # Per-module planner: design THIS module against the frozen
            # prior modules already in the worktree. The task text is
            # the shared brief (context); the module row is the scope.
            ledger = self._ledger.planner_block(self._progress.subtasks)
            instr = (
                'Produce a DESIGN PLAN (prose only — do NOT write code, '
                'files, or commands; your role prompt has the full '
                'rules). If ambiguous, end with a QUESTIONS: block.\n\n'
                f'Module to design: [{sub.id}] {sub.title}\n\n'
                'The full module sequence for this component, yours '
                'marked with an arrow. Every other row is designed and '
                'built in its OWN run — do not design, implement, or '
                'pull their scope into yours:\n'
                f'{self._module_table(sub)}\n\n'
                + (
                    'This is the FIRST module of this run: no '
                    'earlier module of this run has been built, so '
                    'design from the brief rather than looking for '
                    'artifacts of the modules below. Anything already '
                    'committed in your worktree is earlier, separate '
                    'work — treat it as a fixed contract, and a '
                    'genuine need to change it is a halt-and-escalate '
                    'to the human, never a silent edit.'
                    if self._progress.active_is_first
                    else 'The artifacts from prior modules are already '
                    'implemented and FROZEN in your worktree — design '
                    'against them as fixed contracts; a genuine need to '
                    'change a frozen artifact is a halt-and-escalate to '
                    'the human, never a silent edit. Each earlier '
                    "module's approved design is committed under "
                    '`docs/plans/` in your worktree; read the ones your '
                    'module builds on.'
                )
                + (
                    ''
                    if not ledger
                    else '\n\nDecisions earlier modules already settled '
                    'with the human. These are BINDING — design against '
                    'them and do NOT re-open or re-ask them:\n'
                    f'{ledger}'
                )
                + '\n\nProject brief (shared context):\n'
                f'{self._config.task}'
            )
        else:
            instr = (
                'Produce a DESIGN PLAN (prose only — do NOT write code, '
                'files, or commands; your role prompt has the full '
                'rules). If ambiguous, end with a QUESTIONS: block.\n\n'
                f'Feature to plan:\n{self._config.task}'
            )
        if self._config.acceptance:
            instr += (
                '\n\nAcceptance contract (satisfy every clause):\n'
                f'{self._config.acceptance}'
            )
        instr += (
            '\n\nYour VM deliberately has NO project toolchain, and you do '
            'not need one: you are designing, not building. Do NOT install '
            'a compiler, package manager, or database, and do not try to '
            'build, run, or test anything — read the existing sources, '
            'docs, and plans as TEXT. A missing tool is expected here, '
            'never a problem for you to solve.'
        )
        if self._interactive_plan:
            instr += (
                '\n\nA human reviewer is in this session and answers your '
                'questions — ask what you need, then invite them to reply '
                '"APPROVED" to release the plan.'
            )
        ctx = self._reader_context(stage)
        if ctx:
            instr += f'\n\n{ctx}'
        return instr

    def _writer_instruction(self, stage: pipeline.PipelineStage) -> str:
        # A test-writer IS a writer node, but its turn must be framed as
        # "write tests for this task", NOT the raw implementation task —
        # else the concrete "Task: add <feature>" directive overrides
        # its write-tests-only role prompt and it builds the feature.
        if self._is_test_writer(stage):
            return self._test_writer_instruction(stage)
        # A refactor node is a writer seeded from the judge's winner;
        # its turn is "clean up this working code", NOT "implement the
        # feature" — same role-framing reason as the test writer.
        if self._is_refactor(stage):
            return self._refactor_instruction(stage)
        instr = f'{self._task_block()}\n\n{_WRITER_BUILD}'
        ctx = self._reader_context(stage)
        if ctx:
            instr += f'\n\n{ctx}'
        if self._has_upstream_tests(stage):
            instr += (
                '\n\nA failing test suite has already been written for '
                'you in this worktree and is the BINDING CONTRACT: make '
                'every test pass without weakening, skipping, or deleting '
                'any test. Any design plan above is guidance for how to '
                'structure your implementation — follow it, but if it '
                'ever conflicts with the tests, the TESTS win.'
            )
        return instr

    def _test_writer_instruction(
        self, stage: pipeline.PipelineStage
    ) -> str:
        """
        Frame a test-writer node's turn as writing a failing test suite.

        The turn describes the feature to TEST rather than telling the
        agent to build it, so the concrete instruction agrees with the
        test-author role prompt: add tests only, do not implement.
        """
        instr = (
            f'{self._chunk_preamble()}'
            'Write a failing TEST SUITE for the feature described below. '
            'A separate implementer will write the production code to make '
            'your tests pass — you must NOT implement the feature yourself '
            'or modify any production code (add test files only).\n\n'
            f'Feature to test:\n{self._config.task}'
        )
        if self._config.acceptance:
            instr += (
                '\n\nAcceptance contract (encode every clause as tests):\n'
                f'{self._config.acceptance}'
            )
        instr += (
            f'\n\n{_FROZEN_TESTS_CONTRACT}'
            f'\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}\n\n{_WRITER_BUILD}'
            + setup_block(self._config)
        )
        ctx = self._reader_context(stage)
        if ctx:
            instr += f'\n\n{ctx}'
        return instr

    @staticmethod
    def _refactor_contract() -> str:
        """
        The refactor contract, in the words that bound it.

        Held apart from :meth:`_refactor_instruction` because it must
        be repeated on EVERY turn, not stated once. A loop-back drives
        the writer with reviewer findings alone and leans on session
        history to carry the framing — and after a resume there is no
        history to lean on, because the node is re-attached to its
        worktree with a FRESH session. Observed live: a campaign
        resumed five times, and every fix turn after the first arrived
        as a bare list of blocking findings. The agent kept its role
        prompt and lost the one paragraph saying which of those
        findings were its to close.

        :returns: The contract paragraph.
        """
        return (
            'You are REFACTORING. Your job is to make working code '
            'better at producing THE SAME OUTPUTS FROM THE SAME '
            'INPUTS: simpler, with lower cyclomatic complexity and '
            'less duplication; clearer to read; cheaper to run; easier '
            'to audit and maintain. That is the whole job.\n\n'
            'You may NOT change behaviour, the public API, or the '
            'feature set. No new features, no new modules, no bug '
            '"fixes" that alter results, no scope beyond cleanup. You '
            'are not responsible for whether the implementation '
            'satisfies its plan, is complete, or is well designed — '
            'that was settled before this code reached you, by agents '
            'whose job it was. Every existing test must still pass: '
            'run them, keep them green, and do not weaken, skip, '
            'delete or rewrite one.'
        )

    def _refactor_instruction(
        self, stage: pipeline.PipelineStage
    ) -> str:
        """
        Frame a refactor node's turn as behavior-preserving cleanup.

        The winning implementation is already in the node's worktree
        (seeded from the judge). The turn describes the code to CLEAN
        UP — keep behavior, keep tests green, no new features — rather
        than the raw implementation task, so the agent polishes the
        existing code instead of re-implementing from scratch.

        :param stage: The refactor stage being driven.
        :returns: The first-turn instruction.
        """
        instr = (
            f'{self._chunk_preamble()}'
            'A COMPLETE, WORKING implementation (the winning candidate) '
            'is already in your worktree.\n\n'
            f'{self._refactor_contract()}\n\n'
            'The feature it implements (context only — do NOT '
            f're-implement it):\n{self._config.task}'
        )
        if self._config.acceptance:
            instr += (
                '\n\nThe behavior to PRESERVE (acceptance contract):\n'
                f'{self._config.acceptance}'
            )
        instr += (
            f'\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}\n\n{_WRITER_BUILD}'
            + setup_block(self._config)
        )
        ctx = self._reader_context(stage)
        if ctx:
            instr += f'\n\n{ctx}'
        return instr

    def _is_test_writer(self, stage: pipeline.PipelineStage) -> bool:
        """Whether this stage's own agent is the shipped test-writer."""
        if not stage.run:
            return False
        agent = self._config.agents.get(stage.run[0])
        return bool(agent and agent.template == 'tdd-writer')

    def _is_refactor(self, stage: pipeline.PipelineStage) -> bool:
        """Whether this stage's own agent is the shipped refactorer."""
        if not stage.run:
            return False
        agent = self._config.agents.get(stage.run[0])
        return bool(agent and agent.template == 'refactoring')

    def _has_upstream_tests(self, stage: pipeline.PipelineStage) -> bool:
        seed = seed_from(stage, self._nodes)
        if seed is None:
            return False
        seed_stage = self._stage_by_id.get(seed)
        if seed_stage is None or not seed_stage.run:
            return False
        agent = self._config.agents.get(seed_stage.run[0])
        return bool(agent and agent.template == 'tdd-writer')

    def _review_instruction(self, stage: pipeline.PipelineStage) -> str:
        # A refactor review asks a DIFFERENT question from an
        # implementation review, and asking the implementation one
        # cost a whole campaign. See _refactor_review_instruction.
        if self._reviews_a_refactor(stage):
            return self._refactor_review_instruction(stage)
        # Only inside a campaign: on a flat run there is no increment
        # list, so this would point at a "plan above" that is not there
        # — a dangling reference is worse than saying nothing.
        later = (
            ', or BELONGING TO A LATER INCREMENT of the plan above'
            if self._progress.active_subtask is not None
            else ''
        )
        return (
            self._task_block()
            + _REVIEW_MOUNT
            + '\n\nReview the working tree in your mount against this '
            'contract.'
            + _REVIEW_VERIFY
            + _REVIEW_NOT_THE_GATE
            + _REVIEW_NOT_WEIGHTED.format(later=later)
            + self._scope_block()
            + self._ledger.rulings_block(self._progress.subtasks)
            + _FINDINGS_ASK
            + _ENVIRONMENT_ASK
            + self._policy.guarded_block(stage)
            + _REVIEW_DISPUTE
            + _REVIEW_VERDICT_LINE
        )

    def _reviews_a_refactor(self, stage: pipeline.PipelineStage) -> bool:
        """
        Whether this review stage's target is the shipped refactorer.

        :param stage: The review stage about to run.
        :returns: ``True`` when the reviewed node is a refactor node.
        """
        # Deliberately total: this only SELECTS framing, so a target
        # that cannot be resolved yet degrades to the implementation
        # instruction rather than killing the turn.
        try:
            target = review_target(stage, self._nodes)
        except PipelineRunError:
            return False
        target_stage = self._stage_by_id.get(target)
        return target_stage is not None and self._is_refactor(target_stage)

    def _refactor_review_instruction(
        self, stage: pipeline.PipelineStage
    ) -> str:
        """
        Ask a refactor's reviewers the question a refactor answers.

        Every review stage used to receive one instruction: the whole
        task, the whole acceptance contract, and "review the working
        tree against this contract". For an implementation review that
        is right — the writer produced the tree, so the tree IS the
        change. For a REFACTOR review it is wrong twice over. The
        refactor changed a fraction of the tree, and whether the
        implementation satisfies the contract was already asked and
        answered, by consensus, at the implementation's own review.

        Asking it again, every round, against the whole tree, is what
        made a campaign unable to finish. Observed live on
        `ingestion-m2-5`: reviewers returned a BLOCKING verdict in
        every one of thirteen rounds, and the findings never repeated
        because they were never the same defect twice — each round
        re-read 7,900 lines against a 28,000-character brief and found
        the next real thing. Three of the last seven named code that
        did not exist when the judge picked the winner: they were
        defects in the REPAIRS, which is a loop that generates its own
        work. Each functional finding reached the one writer left
        after the judge, which can only close a functional defect by
        adding surface, and new surface is what the next round finds.

        So this instruction names the DIFF as the subject and says
        what a refactor is for. A defect on both sides of the diff is
        pre-existing: real, worth recording, not this gate's business.
        The changed files are listed from the host rather than left to
        the reviewer to derive, the same way guarded files are, so the
        scope is a fact rather than an act of good faith.

        :param stage: The review stage about to run.
        :returns: The full instruction for a refactor reviewer.
        """
        target = review_target(stage, self._nodes)
        changed = self._changed_files(target)
        listed = (
            '\n\nThe files this refactor touched:\n'
            + '\n'.join(f'  - {path}' for path in changed)
            if changed
            else ''
        )
        return (
            f'{self._chunk_preamble()}'
            'You are reviewing a REFACTOR. A complete implementation of '
            'this increment was reviewed on its own merits and APPROVED '
            'by consensus before it reached this stage; a separate '
            'writer has since cleaned it up without being permitted to '
            'change what it does.'
            + _REVIEW_MOUNT
            + listed
            + '\n\nYOUR SUBJECT IS THE CHANGE, NOT THE TREE. Compare '
            'the refactored code against what it replaced, side by '
            'side, and answer two questions only:\n'
            '  1. Is it functionally IDENTICAL — the same outputs for '
            'the same inputs, the same errors for the same failures, '
            'the same order, the same effects?\n'
            '  2. Is it BETTER — simpler, clearer, less duplicated, '
            'less complex, more efficient, easier to audit?\n\n'
            'A NO to the first is BLOCKING: behaviour moved, and that '
            'is the one thing a refactor may not do. A NO to the '
            'second is BLOCKING only if the change made the code '
            'WORSE; a refactor that improves little is not a defect.'
            + '\n\nYou are NOT assessing whether the implementation '
            'meets the plan, is feature-complete, or is well designed. '
            'That verdict was already returned upstream and is not '
            'yours to revisit. A defect that is present IDENTICALLY on '
            'both sides of this diff is PRE-EXISTING: it is not '
            'something this refactor did, it does not belong in your '
            'verdict, and blocking on it stops a release for a fault '
            'the change under review did not introduce. Record it in '
            'the NON-BLOCKING list, where the triage stage downstream '
            'will check it against the shipping code. That list is '
            'read; nothing you put there is lost.'
            + _REVIEW_VERIFY
            + _REVIEW_NOT_THE_GATE
            + _REVIEW_NOT_WEIGHTED.format(later='')
            + self._ledger.rulings_block(self._progress.subtasks)
            + _FINDINGS_ASK
            + _ENVIRONMENT_ASK
            + self._policy.guarded_block(stage)
            + _REVIEW_DISPUTE
            + _REVIEW_VERDICT_LINE
        )

    def _changed_files(self, target: str) -> list[str]:
        """
        The files a node changed against the branch it was cut from.

        Computed on the HOST, like :meth:`WriterPolicy.guarded_block`,
        so a reviewer is handed the scope rather than asked to derive
        it from refs its own clone may not carry.

        :param target: The writer node whose diff is wanted.
        :returns: Sorted repo-relative paths, or ``[]`` if undecidable.
        """
        target_stage = self._stage_by_id.get(target)
        if target_stage is None:
            return []
        try:
            return sorted(
                self._wt.node_diff_files(
                    self._run_id, target,
                    against=seed_ref(self._ctx, target_stage),
                )
            )
        except click.ClickException:
            # Never fail a review on a git hiccup; the reviewer still
            # has the tree and the rest of its instruction.
            return []

    def _judge_scope_clause(self) -> str:
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

    def _defer_block(self) -> str:
        """
        The one way a writer may answer a finding instead of fixing it.

        A blocking finding is otherwise unconditional, so a reviewer
        that asks for a later increment's work leaves the writer no move
        but to build it — which is how `gcp-scope-topology-1` acquired
        service-account pagination during a [topology] fix turn.

        Deliberately narrow: one prescribed form, the owning increment
        named, and nothing silent. The reviewer sees the deferral in the
        next round and can block again if it disagrees, so this shifts
        who must justify the scope call — it does not remove the veto.

        :returns: The paragraph, or ``''`` outside a campaign.
        """
        if self._progress.active_subtask is None:
            return ''
        return (
            'One finding may be answered instead of fixed, and only this '
            'one way: if a finding asks for work the increment list '
            'above assigns to a LATER increment, and this change did not '
            'introduce it, reply with `DEFERRED: [<increment-id>] <one '
            'line>` and leave the code alone. Nothing else counts as '
            'answering a finding — not "out of scope" on its own, not '
            'silence, and not a partial fix. Deferring is on the record '
            'and the reviewer sees it next round; if it disagrees it '
            'will block again and say why. Everything you do NOT defer, '
            'you fix.\n\n'
        )

    def _scope_block(self) -> str:
        """
        Tell a reviewer that the increment bounds its verdict.

        Only on a campaign run: outside one there is no increment and
        the whole brief really is this branch's job.

        Both halves are load-bearing. Without the first a reviewer
        blocks on a later increment's work and the writer, for whom a
        blocking finding is not optional, implements it — reviewed
        against the wrong contract, and inherited unplanned by the
        increment that owns it. Without the second, "that file belongs
        to a later increment" becomes a shield for something this change
        actually broke.

        :returns: The paragraph, or ``''`` outside a campaign.
        """
        if self._progress.active_subtask is None:
            return ''
        return (
            '\n\nScope is part of the contract, and it cuts both ways. '
            'The Task above is the whole plan; your verdict covers ONLY '
            'the marked increment. A requirement another increment owns '
            "is not this branch's debt — name it in the NON-BLOCKING "
            'list with the increment it belongs to, and do not hold the '
            'gate for it. But a defect this increment INTRODUCED is '
            'yours to block on wherever it lives in the tree: "that file '
            'belongs to a later increment" is not a shield for something '
            'this change broke. If you cannot tell which, say so and '
            'block — an honest uncertainty is worth a round.'
        )

    def _judge_instruction(
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
            self._task_block()
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
                + self._judge_scope_clause()
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

    @staticmethod
    def _judge_retry_instruction(candidates: list[str]) -> str:
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
        honest across a resume is that :meth:`_redrive_writer` drops a
        candidate the moment its branch changes.

        :param candidates: The writer nodes being judged.
        :returns: ``True`` when each reached APPROVED consensus here.
        """
        return bool(candidates) and all(
            c in self._reviewed_ok for c in candidates
        )

    def _fix_instruction(self, findings: str) -> str:
        """
        The turn handed to a writer whose review blocked.

        Carries the environment note as well as the findings: this turn
        is frequently the FIRST one a re-attached session ever sees, in
        a VM that has never had a toolchain installed. See
        :data:`_FIX_MUST_VERIFY`.

        :param findings: The blocking reviewers' reports.
        :returns: The instruction text.
        """
        return (
            'The reviewers reported BLOCKING issues. Address every one, '
            f'then stop.\n\n{findings}\n\n{self._defer_block()}'
            f'{_FIX_NO_WEAKENING}\n\n'
            f'{_FIX_MUST_VERIFY}\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            f'\n\n{_WRITER_BUILD}'
            + setup_block(self._config)
        )

    # ── publish + teardown ────────────────────────────────────────

    def _resolve_publish_node(self) -> str | None:
        want = self._config.publish.branch
        if want is not None:
            return want
        return self._progress.last_branch_node

    def _publish(self) -> str | None:
        if self._config.publish.mode == 'none':
            return None
        node_id = self._resolve_publish_node()
        if node_id is None:
            return None
        node = self._nodes.get(node_id)
        if node is None:
            raise PipelineRunError(
                f'publish target {node_id!r} did not run'
            )
        # A judge publishes its selected candidate's branch.
        branch_node = node.selected if node.kind == 'judge' else node_id
        if branch_node is None:
            return None
        return self._publisher.publish_run(
            branch_node,
            plan=self._progress.plan_of_record,
            module=self._progress.active_subtask,
        )
