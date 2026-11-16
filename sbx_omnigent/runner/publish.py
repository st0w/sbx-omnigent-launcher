"""Publishing: a module's records, its branch, and its pull request.

Each module's winner is published to a branch of its own, with every
record of how it was built committed onto it first: the plan, the
decisions ledger, the planning session, the reviews, the security scan
and the judge's selection. In ``mode: pr`` a pull request is opened,
and a module built on an earlier one says so at the top.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

import click

from sbx_omnigent import pipeline, verify
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.filing import FindingFiler
from sbx_omnigent.runner.ledger import DecisionLedger
from sbx_omnigent.runner.paths import plan_artifact_path, session_record_path
from sbx_omnigent.runner.records.judging import JudgePick
from sbx_omnigent.runner.records.results import _COMMIT_RE, ChunkSpan
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.render.pr_body import render_pr_body, short_title
from sbx_omnigent.runner.repo_records import RepoRecords
from sbx_omnigent.runner.security_records import SecurityRecords
from sbx_omnigent.runner.stage_kinds import is_planner
from sbx_omnigent.worktrees import github_slug, pr_number


class Publisher:
    """
    Publish what a run built, and remember what it published.

    :param ctx: The run.
    :param publish_repo: Where branches are pushed and PRs opened.
    :param records: Commits the plan and the records beside it.
    :param ledger: The decisions ledger, committed with each module.
    :param security: Commits the scan reports and summary.
    :param filing: Files the findings, and lists what it filed.
    :param chunk_reviews: The current module's review records.
    :param chunk_picks: The current module's judge picks.
    :param save_state: Persists the run's state after a publish.

    :ivar chunk_publishes: ``{module: {'seed': commit, 'pr': url}}`` for
        each published module, so the next one's PR can link only its
        own changes.
    :ivar published: What each publish printed, in order.
    :ivar evidence: The verification gate's passing outcome, by node.
        Kept because that captured output IS the pull request's
        evidence; discarding it would leave the PR asserting the tests
        passed instead of showing it.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        publish_repo: str,
        records: RepoRecords,
        ledger: DecisionLedger,
        security: SecurityRecords,
        filing: FindingFiler,
        chunk_reviews: Callable[[], list[ReviewRecord]],
        chunk_picks: Callable[[], list[JudgePick]],
        save_state: Callable[[], None],
    ) -> None:
        self.ctx = ctx
        self._publish_repo = publish_repo
        self._records = records
        self._ledger = ledger
        self._security = security
        self._filing = filing
        self._chunk_reviews = chunk_reviews
        self._chunk_picks = chunk_picks
        self._save_state = save_state
        self.chunk_publishes: dict[str, dict[str, str]] = {}
        self.published: list[str] = []
        self.evidence: dict[str, verify.VerifyOutcome] = {}

    def preflight(self) -> None:
        """
        Refuse at startup if ``mode: pr`` cannot open a PR.

        The publish target defaults to the pipeline's ``repo:``, which
        is commonly a LOCAL PATH — and a local path has no
        ``owner/repo`` for ``gh -R``. That used to survive provisioning
        the whole build, then die inside ``publish_node`` at the END of
        the first chunk, throwing away a finished module. Parsing the
        slug here turns a run-ending surprise into a two-second error
        with the fix in it.

        :raises PipelineRunError: If PR mode has a non-GitHub target.
        """
        if self.ctx.config.publish.mode != 'pr':
            return
        try:
            github_slug(self._publish_repo)
        except click.ClickException as exc:
            raise PipelineRunError(
                f"publish mode 'pr' opens a GitHub PR, but the publish "
                f'target is {self._publish_repo!r}, which has no '
                f'owner/repo. Either pass --publish-repo with the '
                f'GitHub URL (keeping repo: local for a fast clone), or '
                f"set publish.mode to 'local' to push the branch "
                f'without opening a PR.'
            ) from exc

    def publish_module(
        self,
        sub: pipeline.Subtask,
        winner: str,
        *,
        first: bool,
        plan: str | None,
        plan_path: str,
        prior: str | None,
        subtasks: Sequence[pipeline.Subtask],
    ) -> None:
        """
        Publish one module's winner to ``pipeline/<run>-<module>``.

        Every record is committed onto the branch first, then it is
        pushed and, in ``mode: pr``, its pull request opened.

        :param sub: The module publishing.
        :param winner: The node whose branch it publishes.
        :param first: Whether it is the campaign's first module.
        :param plan: Its plan of record, or ``None`` when there is none
            to commit.
        :param plan_path: Where its plan and records go.
        :param prior: The module that published before it, or ``None``.
        :param subtasks: The campaign's modules, for the ledger.
        """
        if self.ctx.config.publish.mode == 'none':
            return
        if plan:
            self._records.commit_plan(winner, plan_path, plan)
        self._ledger.record_referrals(sub.id, self._chunk_reviews())
        self._ledger.commit(winner, subtasks)
        # Flat mode has ONE planner for the whole campaign, so its
        # session record is the same conversation every time, committed
        # once and carried to later chunks by the campaign thread.
        # Per-module mode has a planner per module, and a per-module
        # path, so it records every chunk.
        if self.ctx.config.subtasks or first:
            self._records.commit_planning_session(
                winner, plan_path, module=sub
            )
        review_doc = self._records.commit_reviews(
            winner, plan_path, module=sub
        )
        self._security.commit_reports(winner, plan_path)
        self._filing.publish(winner, review_doc)
        security_doc = self._security.commit_summary(
            winner, plan_path, module=sub
        )
        pick_doc = self._records.commit_selection(
            winner, plan_path, module=sub
        )
        open_pr = self.ctx.config.publish.mode == 'pr'
        span = self._span(sub, winner, prior) if open_pr else None
        body = self.pr_body(
            winner, plan_path,
            summary=f'**[{sub.id}]** {sub.title}',
            review_doc=review_doc,
            pick_doc=pick_doc,
            series_note=series_note(span, None) if span else None,
            security_doc=security_doc,
        )
        # The winner with every record above committed: the campaign
        # branch moves here once this returns, so it is the commit the
        # next chunk is built on, and where the next PR's own changes
        # start.
        seed = self.ctx.worktrees.hub_branch_tip(self.ctx.run_id, winner) or ''
        result = self.ctx.worktrees.publish_node(
            self.ctx.run_id, winner, self._publish_repo,
            title=(
                f'[pipeline] {self.ctx.config.name} — {sub.id}: '
                f'{short_title(sub.title)}'
            ),
            body=body,
            # Always the configured base. Stacking on the previous
            # chunk's branch closed this PR when that one merged with
            # its branch deleted.
            base_branch=self.ctx.config.base_branch,
            remote_branch=f'pipeline/{self.ctx.run_id}-{sub.id}',
            open_pr=open_pr,
        )
        self.chunk_publishes[sub.id] = {
            'seed': seed if _COMMIT_RE.fullmatch(seed) else '',
            'pr': result.strip() if open_pr else '',
        }
        self.published.append(result)
        if span is not None:
            self._link_changes(span, result, body)
        self._save_state()

    def publish_run(
        self,
        branch_node: str,
        *,
        plan: str | None,
        module: pipeline.Subtask | None,
    ) -> str:
        """
        Publish a single pass's branch, with its records.

        :param branch_node: The node whose branch ships.
        :param plan: The plan of record, or ``None`` when there is none.
        :param module: The active module, or ``None`` outside one.
        :returns: What :meth:`WorktreeManager.publish_node` printed: the
            pull request's URL, or the pushed branch.
        """
        # Commit the plan of record onto the branch about to ship, so
        # the approved design travels with the code in the PR.
        if plan:
            self._records.commit_plan(
                branch_node,
                plan_artifact_path(self.ctx.config),
                plan,
            )
            self._records.commit_planning_session(
                branch_node,
                plan_artifact_path(self.ctx.config),
                module=module,
            )
        # NOT gated on a plan existing: a pipeline with no planner still
        # has reviewers, and their reports are just as much the record.
        review_doc = self._records.commit_reviews(
            branch_node,
            plan_artifact_path(self.ctx.config),
            module=module,
        )
        self._security.commit_reports(
            branch_node, plan_artifact_path(self.ctx.config)
        )
        self._filing.publish(branch_node, review_doc)
        security_doc = self._security.commit_summary(
            branch_node,
            plan_artifact_path(self.ctx.config),
            module=module,
        )
        pick_doc = self._records.commit_selection(
            branch_node,
            plan_artifact_path(self.ctx.config),
            module=module,
        )
        result = self.ctx.worktrees.publish_node(
            self.ctx.run_id,
            branch_node,
            self._publish_repo,
            title=f'[pipeline] {self.ctx.config.name}',
            body=self.pr_body(
                branch_node,
                plan_artifact_path(self.ctx.config),
                summary=f'Pipeline `{self.ctx.config.name}`',
                task=self.ctx.config.task,
                review_doc=review_doc,
                pick_doc=pick_doc,
                security_doc=security_doc,
            ),
            base_branch=self.ctx.config.base_branch,
            open_pr=self.ctx.config.publish.mode == 'pr',
        )
        self.published.append(result)
        self._save_state()
        return result

    def _span(
        self, module: pipeline.Subtask, winner: str, prior: str | None
    ) -> ChunkSpan | None:
        """
        What a module's PR adds on top of the previous module.

        :param module: The module publishing.
        :param winner: The node whose branch this chunk publishes.
        :param prior: The module that published before it, or ``None``
            for the first.
        :returns: The span, or ``None`` for the first chunk, and when
            the base branch already holds the commit this chunk was
            built on: the PR then shows only its own changes, and a
            note telling the reviewer to merge the earlier PR first
            would be false. Its commits are ``''`` when either end is
            unknown or not a git object id, and the PR then goes
            without the link.
        """
        if prior is None:
            return None
        record = self.chunk_publishes.get(prior, {})
        start = record.get('seed', '')
        end = self.ctx.worktrees.hub_branch_tip(self.ctx.run_id, winner) or ''
        linkable = bool(
            _COMMIT_RE.fullmatch(start) and _COMMIT_RE.fullmatch(end)
        )
        base = self.ctx.config.base_branch or 'main'
        if linkable and self._publish_repo and (
            self.ctx.worktrees.base_contains(
                self._publish_repo, branch=base, commit=start
            )
        ):
            # Live: [k2] was merged before [k3] started, and [k3]'s PR
            # still said to merge [k2]'s first. Unknown keeps the note.
            click.echo(
                f'[publish] {module.id}: everything it was built on is '
                f'already in {base}, so its PR shows only its own '
                f'changes; no series note.'
            )
            return None
        return ChunkSpan(
            chunk=module.id,
            prior=prior,
            prior_pr=record.get('pr', ''),
            start=start if linkable else '',
            end=end if linkable else '',
            slug=github_slug(self._publish_repo),
            base=base,
        )

    def _link_changes(
        self, span: ChunkSpan, pr_url: str, body: str
    ) -> None:
        """
        Point the note at this PR's own view of its commits.

        Best-effort: the PR already exists and links the comparison
        view, so a failed edit is reported and the run carries on.

        :param span: What this chunk adds on the previous one.
        :param pr_url: The URL ``gh pr create`` printed.
        :param body: The description the PR was opened with.
        """
        if not (span.start and span.end):
            return
        number = pr_number(pr_url, span.slug)
        edited = (
            body.replace(
                series_note(span, None),
                series_note(span, number),
            )
            if number
            else body
        )
        try:
            if number is None:
                raise click.ClickException(
                    f'{pr_url.strip()!r} is not a pull request URL'
                )
            self.ctx.worktrees.edit_pr_body(self._publish_repo, pr_url, edited)
        except click.ClickException as exc:
            click.echo(
                f'[publish] {span.chunk}: opened {pr_url.strip()} but '
                f'could not add the link to its own changes '
                f'({exc.message}). Its description links the comparison '
                f'view instead.'
            )

    def pr_body(
        self,
        winner: str,
        plan_path: str,
        *,
        summary: str,
        task: str | None = None,
        review_doc: str | None = None,
        pick_doc: str | None = None,
        series_note: str | None = None,
        security_doc: str | None = None,
    ) -> str:
        """
        The pull-request body for a branch about to publish.

        :param winner: The node whose branch publishes (its verification
            evidence, if any, is embedded).
        :param plan_path: Repo path of this branch's plan of record.
        :param summary: One-line description of what shipped.
        :param task: The original ask, when it adds to the summary.
        :param review_doc: Repo path of the committed reviewer reports.
        :param series_note: What a chained chunk's PR says first.
        :param security_doc: Repo path of the security scan summary.
        :returns: The markdown body.
        """
        config = self.ctx.config
        has_plan = any(is_planner(stage, config) for stage in config.stages)
        return render_pr_body(
            summary=summary,
            series_note=series_note,
            task=task,
            review_doc=review_doc,
            reviews=self._votes(),
            picks=self._chunk_picks(),
            pick_doc=pick_doc,
            findings=list(self._filing.filed),
            plan_doc=plan_path if has_plan else None,
            session_doc=(
                session_record_path(plan_path)
                if has_plan
                else None
            ),
            outcome=self.evidence.get(winner),
            security_doc=security_doc,
        )

    def _votes(self) -> list[ReviewRecord]:
        """
        The publishing chunk's records that voted on its branch.

        A contract check is recorded beside the reviewers so its
        findings are kept, but it read the contract, not the branch, so
        the pull request's roster of approvals leaves it out.

        :returns: The chunk's review records, contract checks excluded.
        """
        return [
            r for r in self._chunk_reviews()
            if not (
                (stage := self.ctx.stage_by_id.get(r.stage)) is not None
                and stage.checks
            )
        ]


def series_note(span: ChunkSpan, number: int | None) -> str:
    """
    The top of a chained chunk's PR description.

    :param span: What this chunk adds on the previous one.
    :param number: This PR's number once it exists. Before then the
        link is GitHub's read-only comparison; after, the view of
        this PR's own commits, which can be reviewed and commented
        on like the PR itself.
    :returns: A markdown block quote.
    """
    prior_number = pr_number(span.prior_pr, span.slug)
    prior_ref = (
        f'#{prior_number}' if prior_number
        else f"[{span.prior}]'s pull request"
    )
    lines = [
        f'> **Part of a series.** This PR targets `{span.base}` and '
        f'is built on **[{span.prior}]** ({prior_ref}), so until that '
        f"merges it also shows [{span.prior}]'s changes.",
    ]
    if span.start and span.end:
        url = (
            f'https://github.com/{span.slug}/pull/{number}/files/'
            f'{span.start}..{span.end}'
            if number
            else f'https://github.com/{span.slug}/compare/'
            f'{span.start}...{span.end}'
        )
        lines += [
            '>',
            f"> **[Review [{span.chunk}]'s changes only]({url})**: "
            f"its own commits, without [{span.prior}]'s.",
        ]
    lines += [
        '>',
        f'> Merge {prior_ref} first. Once it has merged, this PR '
        f'shows only its own changes. If it was squashed or rebased, '
        f'press **Update branch** on this PR first.',
    ]
    return '\n'.join(lines)
