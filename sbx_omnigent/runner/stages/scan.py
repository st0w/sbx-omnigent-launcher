"""The scan stage: the claude-security plugin over the branch to ship.

Once per module, after the review in its ``needs``, the plugin runs
over the changes since the module started, in a writable clone of the
writer it names. Critical and High findings go back to that writer,
the reviews run again, then the scan; Medium and Low are recorded as
findings for triage to check.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import PurePosixPath

import click

from sbx_omnigent import pipeline, security_scan
from sbx_omnigent.runner.captures import TurnCapture
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError, _Blocked
from sbx_omnigent.runner.paths import chunk_plan_path, security_folder
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.records.results import NodeResult
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.redrive import RedriveWriter
from sbx_omnigent.runner.review_book import ReviewBook
from sbx_omnigent.runner.session_disposal import SessionDisposal
from sbx_omnigent.runner.session_driver import SessionDriver
from sbx_omnigent.runner.stage_kinds import stage_kind


class ScanStage:
    """
    Run a ``scans:`` stage, round by round, until it passes or blocks.

    :param ctx: The run.
    :param progress: The run's progress, for the active module.
    :param book: Records each round as a vote, and counts the rounds.
    :param driver: Creates the plugin's session and drives its turn.
    :param disposal: Frees that session once it reports.
    :param capture: Keeps the turn of a scan that wrote no report.
    :param node_slots: Caps microVMs driven at once across the run; the
        scan holds one only for its own turn.
    :param redrive_writer: Sends Critical and High findings to the
        writer the scan names.
    :param run_review: Runs a review stage again after that fix.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        progress: RunProgress,
        book: ReviewBook,
        driver: SessionDriver,
        disposal: SessionDisposal,
        capture: TurnCapture,
        node_slots: threading.Semaphore,
        redrive_writer: RedriveWriter,
        run_review: Callable[[pipeline.PipelineStage], None],
    ) -> None:
        self.ctx = ctx
        self._progress = progress
        self._book = book
        self._driver = driver
        self._disposal = disposal
        self._capture = capture
        self._node_slots = node_slots
        self._redrive_writer = redrive_writer
        self._run_review = run_review

    def run(self, stage: pipeline.PipelineStage) -> None:
        """
        Scan the branch about to ship with the claude-security plugin.

        Once per module, after the review in its ``needs``: a writable
        clone of the writer it names, the plugin run over the changes
        since the module started, and the plugin's own results file
        read. Critical and High findings go back to the ``on_block``
        writer; then every review stage in the scan's ``needs`` runs
        again (the fix changed code they approved), then the scan. Past
        the round cap the run blocks for a human. Medium and Low are
        recorded as the scan's findings, so triage checks them against
        the shipping code and files what reproduces.

        :param stage: The ``scans:`` stage.
        :raises PipelineRunError: If the scan writes no report, or one
            that cannot be read.
        :raises _Blocked: If it still blocks after the round cap, or
            blocks with no writer to send its findings to.
        """
        assert stage.scans is not None and stage.scan_effort is not None
        while True:
            round_no = self._book.next_round(stage.id)
            findings = self._round(stage, round_no)
            blocking = [f for f in findings if f.blocks]
            if not blocking:
                self.ctx.nodes[stage.id] = NodeResult(
                    stage.id, 'scan', verdict='APPROVED'
                )
                return
            rounds = self._book.spend_round(stage.id)
            if rounds > self.ctx.max_rounds or not stage.on_block:
                self.ctx.nodes[stage.id] = NodeResult(
                    stage.id, 'scan', verdict='BLOCKING'
                )
                raise _Blocked(stage.id, rounds)
            click.echo(
                f'[scan] {stage.id}: {len(blocking)} Critical/High '
                f'finding(s); sending them to {stage.on_block}, then '
                f'reviewing and scanning again.'
            )
            self._redrive_writer(
                stage.on_block, security_scan.render_blocking(blocking)
            )
            for need in stage.needs:
                review = self.ctx.stage_by_id.get(need)
                if review is not None and stage_kind(review) == 'review':
                    self._run_review(review)

    def _round(
        self, stage: pipeline.PipelineStage, round_no: int
    ) -> tuple[security_scan.ScanFinding, ...]:
        """
        One scan: clone, drive the plugin, read and record its results.

        :param stage: The ``scans:`` stage.
        :param round_no: Which scan round this is.
        :returns: Every finding the scan reported.
        :raises PipelineRunError: If it wrote no report, or one that
            cannot be read.
        """
        assert stage.scans is not None and stage.scan_effort is not None
        base = self.module_start()
        worktree = self.ctx.worktrees.create_node_worktree(
            self.ctx.run_id, stage.id, from_node=stage.scans, replace=True
        )
        try:
            with self._node_slots:
                report = self._drive(stage, worktree, base)
            if report is None:
                raise PipelineRunError(
                    f'{stage.id}: the security scan wrote no report (no '
                    f'CLAUDE-SECURITY-*/{security_scan.RESULTS_NAME} in '
                    f'{worktree}). Its turn is in the run directory under '
                    f'turns/.'
                )
            findings = security_scan.parse_report(report.results)
        except security_scan.ScanReportError as exc:
            raise PipelineRunError(
                f'{stage.id}: could not read the report the security '
                f'scan wrote: {exc}'
            ) from exc
        saved = self._save_report(stage, round_no, report)
        self._record(stage, round_no, findings, base, saved)
        return findings

    def _save_report(
        self,
        stage: pipeline.PipelineStage,
        round_no: int,
        report: security_scan.ScanReport,
    ) -> tuple[str, ...]:
        """
        Save a round's report folder to the run directory, at once.

        The clone it was read from is replaced by the next round and
        removed with the run, so this copy is the one committed at
        publish.

        :param stage: The ``scans:`` stage.
        :param round_no: Which scan round this was.
        :param report: The folder read out of the clone.
        :returns: Each saved file's path under the run directory.
        :raises PipelineRunError: If a file cannot be saved.
        """
        folder = f'security/{stage.id}/r{round_no}-{report.stamp}'
        saved = []
        for name, text in report.files:
            relpath = f'{folder}/{name}'
            if not self.ctx.worktrees.write_run_artifact(
                self.ctx.run_id, relpath, text
            ):
                raise PipelineRunError(
                    f'{stage.id}: could not save the security scan report '
                    f'{relpath} to the run directory'
                )
            saved.append(relpath)
        return tuple(saved)

    def _drive(
        self, stage: pipeline.PipelineStage, worktree: str, base: str
    ) -> security_scan.ScanReport | None:
        """
        Run the plugin in a session of its own, then hand the VM back.

        :param stage: The ``scans:`` stage.
        :param worktree: The writable clone to scan.
        :param base: The commit to scan from.
        :returns: The report folder, or ``None`` if it wrote none (its
            turn is then kept in the run directory).
        :raises security_scan.ScanReportError: If the folder holds a
            file that cannot be read safely.
        """
        assert stage.scan_effort is not None
        session = self._driver.create(stage.run[0], worktree, 'rw', stage.id)
        try:
            self._driver.drive(
                session,
                security_scan.instruction(base=base, effort=stage.scan_effort),
            )
            report = security_scan.collect_report(worktree)
            if report is None:
                self._capture.turn(session, 'the scan wrote no report')
            return report
        finally:
            self._disposal.free(
                session, f'{stage.id}: the scan is done with its VM.'
            )

    def _record(
        self,
        stage: pipeline.PipelineStage,
        round_no: int,
        findings: tuple[security_scan.ScanFinding, ...],
        base: str,
        saved: tuple[str, ...],
    ) -> None:
        """
        Record a scan as a vote, so reports, triage and filing take it.

        BLOCKING with its Critical and High findings as the blocking
        report, APPROVED otherwise; Medium and Low are its findings,
        each one line tagged with its ``security-<severity>`` label.

        :param stage: The ``scans:`` stage.
        :param round_no: Which scan round this was.
        :param findings: Everything the scan reported.
        :param base: The commit it scanned from.
        :param saved: The round's report files under the run directory,
            committed beside the reviews record at publish.
        """
        assert stage.scan_effort is not None
        blocking = [f for f in findings if f.blocks]
        defects = tuple(
            security_scan.render_defect(f) for f in findings if not f.blocks
        )
        report = security_scan.render_report(
            findings, base=base, effort=stage.scan_effort, round_no=round_no
        )
        if saved:
            folder = PurePosixPath(saved[0]).parent.name
            report += (
                f'\n\nThe plugin report for this round (Markdown, JSONL, '
                f'SARIF and the revision stamp) is committed beside this '
                f'record in `{self.security_folder_name()}/{folder}/`.'
            )
        verdict = 'BLOCKING' if blocking else 'APPROVED'
        record = ReviewRecord(
            chunk=self._progress.module_id,
            stage=stage.id,
            reviewer=stage.run[0],
            round_no=round_no,
            verdict=verdict,
            turns=(('assistant', f'{report}\n\nVERDICT: {verdict}'),),
            findings=defects,
            defects=defects,
            target=stage.scans,
            blocking_report=(
                security_scan.render_blocking(blocking) if blocking else ''
            ),
            attachments=saved,
        )
        self._book.add(record)

    def module_start(self) -> str:
        """
        The commit the module being built started from.

        In a campaign after its first module, that is the campaign
        branch's tip: the previous module's winner, which this module's
        writers were cut from. Otherwise it is the base branch.

        :returns: A commit id on the run's hub.
        :raises PipelineRunError: If neither resolves.
        """
        if self._progress.active_subtask is not None:
            tip = self.ctx.worktrees.hub_branch_tip(
                self.ctx.run_id, 'campaign'
            )
            if tip:
                return tip
        base = self.ctx.config.base_branch or 'main'
        tip = self.ctx.worktrees.hub_ref_tip(self.ctx.run_id, base)
        if not tip:
            raise PipelineRunError(
                f'cannot find the base branch {base!r} on the hub of '
                f'run {self.ctx.run_id} to scan from'
            )
        return tip

    def security_folder_name(self) -> str:
        """The scan reports' folder name, for the record to cite."""
        return PurePosixPath(
            security_folder(
                chunk_plan_path(self.ctx.config, self._progress.active_subtask)
            )
        ).name
