"""Filing: putting a module's findings where a human looks.

After triage concludes, each finding that still reproduces becomes a
tracker issue, the ones it kept out are summarised with their reasons,
and blocking reports nobody concluded on are named in one issue. A
pipeline with no tracker appends its findings to a committed ledger
instead.
"""

from __future__ import annotations

from collections.abc import Callable

import click

from sbx_omnigent import security_scan
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.parsing.dispositions import Disposition
from sbx_omnigent.runner.paths import findings_ledger_path
from sbx_omnigent.runner.records.findings import (
    _ALREADY_TRACKED,
    _FINDING_LABEL,
    _filed,
    _withheld,
    finding_id,
    finding_marker,
)
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.render.issues import (
    render_blocking_issue,
    render_blocking_summary,
    render_finding_issue,
    render_findings_ledger,
    render_triage_summary,
    verdict_tally,
)
from sbx_omnigent.runner.stage_kinds import recorded_by_scan
from sbx_omnigent.runner.triage import FindingTriage


class FindingFiler:
    """
    File a module's triaged findings, and remember what became of each.

    :param ctx: The run.
    :param publish_repo: The repository issues are filed on, or ``None``
        when there is none.
    :param triage: The verifier's conclusions, which decide what files.
    :param chunk_reviews: The current module's review records, in
        review order.

    :ivar filed: ``(url, title)`` of each issue filed for the current
        module, which its pull request lists.
    :ivar outcomes: What became of each finding, by positional id, for
        the module's security summary.
    :ivar blocking_summary_url: The issue naming unconcluded blocking
        reports, once filed.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        publish_repo: str | None,
        triage: FindingTriage,
        chunk_reviews: Callable[[], list[ReviewRecord]],
    ) -> None:
        self.ctx = ctx
        self._publish_repo = publish_repo
        self._triage = triage
        self._chunk_reviews = chunk_reviews
        self.filed: list[tuple[str, str]] = []
        self.outcomes: dict[str, str] = {}
        self.blocking_summary_url: str | None = None
        #: Whether the ``security-*`` labels were checked for this run.
        self._labels_checked = False

    def start_module(self) -> None:
        """
        Forget the previous module's filing records as a module starts.

        Each chunk publishes its own pull request, which lists the
        issues filed for that chunk, and its own security summary, which
        names that chunk's outcomes. Kept for the whole run, they piled
        up: when two chunks published from one runner (after a resume),
        the second PR also listed every issue of the first.
        """
        self.filed = []
        self.outcomes = {}
        self.blocking_summary_url = None

    def publish(
        self, winner: str, report_doc: str | None
    ) -> None:
        """
        Put this chunk's non-blocking findings where a human looks.

        A tracker when the pipeline publishes to GitHub, the
        committed file otherwise. The file was the original design
        and the wrong shape: until the campaign thread carried each
        chunk's records forward, the ledger restarted every chunk, and
        even now it lives on one run's branches only. An issue tracker
        is cross-branch and cross-campaign by construction.

        The file survives for ``publish: local`` and ``mode: none``,
        where there is no tracker to file into.

        :param winner: The node whose branch publishes.
        :param report_doc: Repo path of the full reviewer reports.
        """
        records = self._to_publish()
        if self.ctx.config.publish.mode == 'pr' and self._publish_repo:
            if records:
                self.file_findings(records, report_doc)
            # Separately: a record can carry blocking claims and no
            # non-blocking findings at all, which is exactly the vote
            # that stopped a branch.
            self.file_blocking_claims(report_doc)
        elif records:
            self._commit_findings_ledger(winner, report_doc)

    def _to_publish(self) -> list[ReviewRecord]:
        """
        The chunk's records whose findings are filed or ledgered.

        A scan stage's earlier rounds are left out: they describe code
        the writer has since changed, and the final round reports again
        whatever still applies. Their findings stay in the reviews
        record and their blocking reports still reach triage.

        :returns: Records with findings, superseded scan rounds removed.
        """
        records = self._chunk_reviews()
        by_id = self.ctx.stage_by_id
        latest: dict[str, int] = {}
        for rec in records:
            if recorded_by_scan(rec, by_id):
                latest[rec.stage] = max(latest.get(rec.stage, 0), rec.round_no)
        return [
            rec for rec in records
            if rec.findings
            and not (
                recorded_by_scan(rec, by_id)
                and rec.round_no < latest[rec.stage]
            )
        ]

    def _issue_labels(
        self, rec: ReviewRecord, finding: str | None = None
    ) -> tuple[str, ...]:
        """
        The labels for an issue filed from *rec*.

        ``finding`` always; a scan's finding also its
        ``security-<severity>``, from its tag, or for a blocking claim
        (*finding* ``None``) from the most severe level in the round.

        :param rec: The vote the finding came from.
        :param finding: A non-blocking finding's text, or ``None`` for
            a blocking claim.
        :returns: The labels.
        """
        if not recorded_by_scan(rec, self.ctx.stage_by_id):
            return (_FINDING_LABEL,)
        label = (
            security_scan.blocking_label(rec.blocking_report)
            if finding is None
            else security_scan.defect_label(finding)
        )
        if label is None:
            return (_FINDING_LABEL,)
        self._ensure_security_labels()
        return (_FINDING_LABEL, label)

    def _ensure_security_labels(self) -> None:
        """Create any missing ``security-*`` label, once per run."""
        if self._labels_checked or not self._publish_repo:
            return
        self._labels_checked = True
        if not self.ctx.worktrees.ensure_labels(
            self._publish_repo, security_scan.SECURITY_LABELS
        ):
            click.echo(
                '[findings] WARNING: could not create the security-* '
                'labels on the publish repo. A scan finding is filed '
                'without labels if GitHub refuses one.'
            )

    def file_findings(
        self, records: list[ReviewRecord], report_doc: str | None
    ) -> None:
        """
        File each new finding as an issue, skipping ones already there.

        Dedup is on the positional id, matched against every issue body
        OPEN AND CLOSED. Closed matters most: an issue a human closed is
        a finding they have dealt with, and re-filing it next run is the
        one behaviour that would make the tracker worthless.

        An unreadable tracker files NOTHING. That is deliberate — a
        listing failure means every id looks absent, and filing on that
        basis is exactly how a tracker fills with duplicates. The
        findings are still in the reviewer report on the branch.

        :param records: This chunk's votes that raised findings.
        :param report_doc: Repo path of the full reviewer reports.
        """
        assert self._publish_repo is not None
        seen = self.ctx.worktrees.issue_bodies_text(self._publish_repo)
        if seen is None:
            click.echo(
                '[findings] could not read the issue tracker; filing '
                'nothing rather than risking duplicates. The findings '
                'are in the reviewer report on the branch.'
            )
            return
        raised = failed = 0
        withheld: list[tuple[ReviewRecord, str, str, Disposition]] = []
        for rec in records:
            for index, finding in rec.filed_findings():
                ident = finding_id(rec, index)
                if finding_marker(ident) in seen:
                    self.outcomes[ident] = _ALREADY_TRACKED
                    continue
                verdict = self._triage.dispositions.get(ident)
                if verdict is not None and not verdict.files:
                    self.outcomes[ident] = _withheld(verdict)
                    # A verifier read this against the shipping code and
                    # concluded it needs nobody. It is summarised below
                    # with its reason rather than filed — and it is NOT
                    # marked seen, so a later run that verifies
                    # differently can still raise it.
                    withheld.append((rec, ident, finding, verdict))
                    continue
                raised += 1
                title, body = render_finding_issue(
                    rec, finding, index,
                    run_id=self.ctx.run_id, report_doc=report_doc,
                )
                url = self.ctx.worktrees.create_issue(
                    self._publish_repo, title=title, body=body,
                    labels=self._issue_labels(rec, finding),
                )
                self.outcomes[ident] = _filed(url)
                if url:
                    self.filed.append((url, title))
                else:
                    failed += 1
        if withheld:
            self._file_triage_summary(withheld, report_doc)
        if not raised:
            return
        if failed:
            click.echo(
                f'[findings] filed {raised - failed} of {raised} '
                f'finding(s); {failed} could not be filed and remain in '
                f'the reviewer report on the branch.'
            )
        else:
            click.echo(
                f'[findings] filed {raised} non-blocking finding(s) as '
                f'issues — tracked for triage, nothing was changed.'
            )

    def _file_triage_summary(
        self,
        withheld: list[tuple[ReviewRecord, str, str, Disposition]],
        report_doc: str | None,
    ) -> None:
        """
        One issue naming every finding the gate kept out, and why.

        Deliberately one issue and not N. N is the cost this change
        exists to remove; zero is a silent drop, which is the one
        outcome worse than N. See :func:`render_triage_summary`.

        :param withheld: ``(record, id, text, disposition)`` each.
        :param report_doc: Repo path of the full reviewer reports.
        """
        assert self._publish_repo is not None
        chunk = withheld[0][0].chunk or self.ctx.run_id
        title, body = render_triage_summary(
            withheld, chunk=chunk, report_doc=report_doc,
        )
        url = self.ctx.worktrees.create_issue(
            self._publish_repo, title=title, body=body,
            labels=(_FINDING_LABEL,),
        )
        if url:
            self.filed.append((url, f'[{chunk}] triage summary'))
        click.echo(
            f'[findings] withheld {len(withheld)} finding(s) — '
            f'{verdict_tally(withheld)}; summarised in one issue with the '
            f'reason for each.'
        )

    def file_blocking_claims(self, report_doc: str | None) -> None:
        """
        File the blocking claims that survive against shipping code.

        A blocking finding is routed nowhere today: the pipeline
        assumes the loop-back fix turn closed it, which is true of the
        branch that was blocked and false for its competitor. When the
        judge selects the competitor, a defect a reviewer reproduced
        ships and reaches no tracker.

        Three outcomes, and each is visible:

        - ``reproduces`` — filed, with the same dedup marker and
          positional id every finding carries.
        - any other verdict — collected into the existing withheld
          summary, with the verifier's reason.
        - a report the verifier concluded nothing about — named in one
          summary issue, because a blocking finding is usually already
          fixed and filing each one would bury the tracker.

        :param report_doc: Repo path of the full reviewer reports.
        """
        if not self._publish_repo:
            return
        reports = self._triage.pending_blocking_reports()
        if not reports:
            return
        seen = self.ctx.worktrees.issue_bodies_text(self._publish_repo)
        if seen is None:
            click.echo(
                '[findings] could not read the issue tracker; filing no '
                'blocking claims rather than risking duplicates. They '
                'are in the reviewer report on the branch.'
            )
            return
        to_file, withheld, unconcluded = self._sort_blocking_claims(
            reports, seen
        )
        for rec, ident, claim in to_file:
            title, body = render_blocking_issue(
                rec, claim, ident,
                run_id=self.ctx.run_id, report_doc=report_doc,
            )
            url = self.ctx.worktrees.create_issue(
                self._publish_repo, title=title, body=body,
                labels=self._issue_labels(rec),
            )
            self.outcomes[ident] = _filed(url)
            if url:
                self.filed.append((url, title))
        for _rec, ident, _claim, verdict in withheld:
            self.outcomes[ident] = _withheld(verdict)
        if withheld:
            self._file_triage_summary(withheld, report_doc)
        if unconcluded:
            self._file_blocking_summary(unconcluded, report_doc)
        if to_file:
            click.echo(
                f'[findings] filed {len(to_file)} blocking claim(s) that '
                f'still reproduce against the code that shipped.'
            )

    def _sort_blocking_claims(
        self, reports: list[tuple[ReviewRecord, str]], seen: str
    ) -> tuple[
        list[tuple[ReviewRecord, str, str]],
        list[tuple[ReviewRecord, str, str, Disposition]],
        list[ReviewRecord],
    ]:
        """
        Sort verified blocking claims by what happens to each.

        :param reports: ``(record, report)`` for each BLOCKING vote.
        :param seen: Every issue body already in the tracker.
        :returns: ``(to_file, withheld, unconcluded)`` — claims that
            still reproduce, claims a verdict kept out with its reason,
            and the votes whose report got no conclusion at all.
        """
        to_file: list[tuple[ReviewRecord, str, str]] = []
        withheld: list[tuple[ReviewRecord, str, str, Disposition]] = []
        unconcluded: list[ReviewRecord] = []
        for rec, _text in reports:
            claims = self._triage.blocking_claims_for(rec)
            if not claims:
                unconcluded.append(rec)
                continue
            for ident, claim in claims:
                verdict = self._triage.dispositions.get(ident)
                # Already filed, or recorded with no verdict at all:
                # neither files, and neither is news to anyone.
                if verdict is None:
                    continue
                if finding_marker(ident) in seen:
                    self.outcomes[ident] = _ALREADY_TRACKED
                    continue
                if verdict.files:
                    to_file.append((rec, ident, claim))
                else:
                    withheld.append((rec, ident, claim, verdict))
        return to_file, withheld, unconcluded

    def _file_blocking_summary(
        self, unconcluded: list[ReviewRecord], report_doc: str | None
    ) -> None:
        """
        One issue naming every blocking report nobody concluded on.

        See :func:`render_blocking_summary`.

        :param unconcluded: The votes whose reports got no conclusion.
        :param report_doc: Repo path of the full reviewer reports.
        """
        assert self._publish_repo is not None
        chunk = unconcluded[0].chunk or self.ctx.run_id
        title, body = render_blocking_summary(
            unconcluded, chunk=chunk, report_doc=report_doc,
        )
        url = self.ctx.worktrees.create_issue(
            self._publish_repo, title=title, body=body,
            labels=(_FINDING_LABEL,),
        )
        self.blocking_summary_url = url
        if url:
            self.filed.append((url, f'[{chunk}] blocking reports summary'))
        click.echo(
            f'[findings] {len(unconcluded)} blocking report(s) were not '
            f'concluded on; summarised in one issue.'
        )

    def _commit_findings_ledger(
        self, winner: str, report_doc: str | None
    ) -> str | None:
        """
        Append this chunk's non-blocking findings to the ledger.

        Reads what is already on the branch and adds to it — see
        :func:`render_findings_ledger` for why nothing is ever
        rewritten.
        Best-effort like the other records: worth having, never worth
        failing a publish over.

        :param winner: The node whose branch publishes.
        :param report_doc: Repo path of the full reviewer reports.
        :returns: The committed path, or ``None`` when nothing was new.
        """
        records = self._to_publish()
        if not records:
            return None
        path = findings_ledger_path(self.ctx.config)
        tree = self.ctx.worktrees.node_worktree_path(self.ctx.run_id, winner)
        try:
            existing = self.ctx.worktrees.read_tracked_file(tree, path)
        except click.ClickException:
            return None
        doc = render_findings_ledger(
            records,
            title=f'Findings — {self.ctx.config.name}',
            existing=existing,
            report_doc=report_doc,
        )
        if doc is None:
            # Every finding is already on the branch — a re-published
            # chunk, or a resume. Writing nothing is the correct no-op.
            return None
        try:
            self.ctx.worktrees.write_tracked_file(tree, path, doc)
            committed = self.ctx.worktrees.commit_node(
                self.ctx.run_id, winner,
                message='docs: record non-blocking reviewer findings',
                author='reviewer <reviewer@pipeline.local>',
            )
        except click.ClickException:
            return None
        if not committed:
            return None
        added = sum(len(r.findings) for r in records)
        click.echo(
            f'[findings] appended up to {added} non-blocking finding(s) '
            f'to {path} — tracked for triage, nothing was changed.'
        )
        return path
