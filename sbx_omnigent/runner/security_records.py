"""Security records: a module's scan reports, and what became of each.

Each scan round's report files are committed beside the module's plan,
and a summary is committed with them: every round, and every finding
with what triage concluded and what filing did. Both are best-effort,
like the other records: worth having, never worth failing a publish.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import PurePosixPath

import click

from sbx_omnigent import pipeline, security_scan, security_summary
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.filing import FindingFiler
from sbx_omnigent.runner.paths import security_folder
from sbx_omnigent.runner.records.findings import finding_id
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.stage_kinds import recorded_by_scan
from sbx_omnigent.runner.triage import FindingTriage


class SecurityRecords:
    """
    Commit a module's scan reports and its security summary.

    :param ctx: The run.
    :param publish_repo: The repository issues are filed on, or ``None``
        when there is none.
    :param triage: What the verifier concluded on each finding.
    :param filing: What filing did with each finding.
    :param chunk_reviews: The current module's review records, in
        review order.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        publish_repo: str | None,
        triage: FindingTriage,
        filing: FindingFiler,
        chunk_reviews: Callable[[], list[ReviewRecord]],
    ) -> None:
        self.ctx = ctx
        self._publish_repo = publish_repo
        self._triage = triage
        self._filing = filing
        self._chunk_reviews = chunk_reviews

    def commit_reports(
        self, winner: str, plan_path: str
    ) -> str | None:
        """
        Commit the publishing chunk's scan reports beside its reviews.

        One folder per round, ``<plan>-security/r<N>-<timestamp>/``,
        holding the files saved when the round's report was read. A
        saved file that has gone missing is reported and the rest are
        committed: the findings themselves are in the reviews record.

        :param winner: The node being published.
        :param plan_path: The plan artifact path, whose name the folder
            follows.
        :returns: The committed folder, or ``None``.
        """
        saved = [
            relpath
            for record in self._chunk_reviews()
            for relpath in record.attachments
        ]
        if not saved:
            return None
        folder = security_folder(plan_path)
        tree = self.ctx.worktrees.node_worktree_path(self.ctx.run_id, winner)
        written = 0
        try:
            for relpath in saved:
                text = self.ctx.worktrees.read_run_artifact(
                    self.ctx.run_id, relpath
                )
                round_dir = PurePosixPath(relpath).parent.name
                name = PurePosixPath(relpath).name
                if text is None:
                    click.echo(
                        f'[security] WARNING: {relpath} is missing from the '
                        f'run directory; it is not committed.'
                    )
                    continue
                self.ctx.worktrees.write_tracked_file(
                    tree, f'{folder}/{round_dir}/{name}', text
                )
                written += 1
            if not written:
                return None
            committed = self.ctx.worktrees.commit_node(
                self.ctx.run_id, winner,
                message='docs: add security scan reports',
                author='security-scan <security-scan@pipeline.local>',
            )
        except click.ClickException as exc:
            click.echo(
                f'[security] WARNING: could not commit the security scan '
                f'reports to {folder}: {exc}'
            )
            return None
        if not committed:
            return None
        click.echo(
            f'[security] committed {written} security scan report '
            f'file(s) to {folder}/.'
        )
        return folder

    def commit_summary(
        self,
        winner: str,
        plan_path: str,
        *,
        module: pipeline.Subtask | None,
    ) -> str | None:
        """
        Commit the chunk's security summary, after its findings filed.

        ``README.md`` in the ``-security`` folder: each scan round, and
        each finding with what became of it. Built after filing so it
        can link the issues. Best-effort like the other records.

        :param winner: The node being published.
        :param plan_path: The plan artifact path, whose name the folder
            follows.
        :param module: The module published, named in the title, or
            ``None`` outside a campaign.
        :returns: The committed path, or ``None``.
        """
        by_stage: dict[str, list[ReviewRecord]] = {}
        for rec in self._chunk_reviews():
            if recorded_by_scan(rec, self.ctx.stage_by_id):
                by_stage.setdefault(rec.stage, []).append(rec)
        if not by_stage:
            return None
        stages = [
            self._stage_view(stage_id, recs)
            for stage_id, recs in by_stage.items()
        ]
        sub = module
        title = self.ctx.config.name + (
            f' [{sub.id}] {sub.title}' if sub is not None else ''
        )
        doc = security_summary.render_summary(title=title, stages=stages)
        path = f'{security_folder(plan_path)}/README.md'
        try:
            self.ctx.worktrees.write_tracked_file(
                self.ctx.worktrees.node_worktree_path(self.ctx.run_id, winner),
                path,
                doc,
            )
            committed = self.ctx.worktrees.commit_node(
                self.ctx.run_id, winner,
                message='docs: add security scan summary',
                author='security-scan <security-scan@pipeline.local>',
            )
        except click.ClickException as exc:
            click.echo(
                f'[security] WARNING: could not commit the security scan '
                f'summary to {path}: {exc}'
            )
            return None
        if not committed:
            return None
        click.echo(f'[security] committed the security scan summary to '
                   f'{path}.')
        return path

    def _stage_view(
        self, stage_id: str, records: list[ReviewRecord]
    ) -> security_summary.ScanStage:
        """
        One scan stage's rounds and final outcomes, for the summary.

        :param stage_id: The ``scans:`` stage.
        :param records: Its records in this chunk.
        :returns: The view the summary renders.
        """
        records = sorted(records, key=lambda rec: rec.round_no)
        rounds = tuple(
            security_summary.ScanRound(
                number=rec.round_no,
                verdict=rec.verdict or 'APPROVED',
                folder=(
                    PurePosixPath(rec.attachments[0]).parent.name
                    if rec.attachments else None
                ),
                findings=self._round_findings(rec),
                triage=(
                    self._scan_triage(rec) if rec.verdict == 'BLOCKING'
                    else ''
                ),
            )
            for rec in records
        )
        final = records[-1]
        outcomes = tuple(
            self._minor_outcome(final, index)
            for index, _text in final.filed_findings()
        )
        return security_summary.ScanStage(stage_id, rounds, outcomes)

    def _round_findings(
        self, rec: ReviewRecord
    ) -> tuple[security_scan.ScanFinding, ...] | None:
        """A round's findings read back from its saved JSONL, if any."""
        for relpath in rec.attachments:
            if PurePosixPath(relpath).name != security_scan.RESULTS_NAME:
                continue
            text = self.ctx.worktrees.read_run_artifact(
                self.ctx.run_id, relpath
            )
            if text is None:
                return None
            try:
                return security_scan.parse_report(text)
            except security_scan.ScanReportError:
                return None
        return None

    def _scan_triage(self, rec: ReviewRecord) -> str:
        """
        What triage concluded on re-checking a blocking scan round.

        :param rec: A BLOCKING scan record.
        :returns: One line of Markdown.
        """
        claims = self._triage.blocking_claims_for(rec)
        if not claims and not any(
            stage.verifies for stage in pipeline._iter_stages(
                self.ctx.config.stages
            )
        ):
            return (
                'not re-checked: the pipeline has no triage stage '
                '(`verifies:`).'
            )
        if not claims:
            where = (
                f' It is named in {self._filing.blocking_summary_url}.'
                if self._filing.blocking_summary_url else ''
            )
            return f'triage reached no conclusion on this report.{where}'
        parts = []
        for ident, claim in claims:
            verdict = self._triage.dispositions.get(ident)
            text = f'`{verdict.verdict}`: {claim}' if verdict else claim
            outcome = self._filing.outcomes.get(ident)
            parts.append(f'{text} ({outcome})' if outcome else text)
        return '; '.join(parts)

    def _minor_outcome(self, rec: ReviewRecord, index: int) -> str:
        """
        What became of one of a final scan round's Medium/Low findings.

        :param rec: The final round's record.
        :param index: The finding's position in it.
        :returns: One line of Markdown.
        """
        ident = finding_id(rec, index)
        if self.ctx.config.publish.mode != 'pr' or not self._publish_repo:
            verdict = self._triage.dispositions.get(ident)
            triage = (
                f'; triage: `{verdict.verdict}`: {verdict.reason}'
                if verdict is not None else ''
            )
            return f'Recorded in the findings ledger{triage}'
        return self._filing.outcomes.get(
            ident, 'Not filed: the issue tracker could not be read'
        )
