"""The decisions ledger: what each module settled for the ones after it.

A campaign builds one module at a time, and a later module's planner
must design against what an earlier one settled. Each approved plan's
``DECISIONS FOR LATER MODULES:`` block is recorded here, beside what
reviewers referred on to a later module, and the ledger is committed
onto each published module so the next one's worktree carries it.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.parsing.decisions import (
    parse_decisions,
    parse_decisions_doc,
)
from sbx_omnigent.runner.paths import decisions_ledger_path
from sbx_omnigent.runner.prompts.review import _REVIEW_SETTLED
from sbx_omnigent.runner.records.reviews import ReviewRecord


class DecisionLedger:
    """
    The decisions and referrals a campaign has recorded so far.

    :param ctx: The run.

    :ivar decisions: ``(module_id, decision)`` recorded by each
        module's approved plan as binding on the modules that follow.
        Threaded into every later planner's turn and committed as a
        ledger, since a decision made in one module's session reaches
        no other.
    :ivar referrals: Review observations addressed to a LATER module, as
        ``(raising module, text)``. Kept apart from *decisions* on
        purpose: a decision is BINDING and changing one is a
        halt-and-escalate, while this is one reviewer's non-blocking
        note. Merging them would promote a passing remark into a
        constraint nobody may alter, which is worse than the tracker
        they came from.
    """

    def __init__(self, ctx: RunContext) -> None:
        self.ctx = ctx
        self.decisions: list[tuple[str, str]] = []
        self.referrals: list[tuple[str, str]] = []

    @property
    def path(self) -> str:
        """The ledger's repo path; see :func:`decisions_ledger_path`."""
        return decisions_ledger_path(self.ctx.config)

    def record_decisions(
        self, module_id: str | None, plan: str | None
    ) -> None:
        """
        Record what a module settled that binds the later ones.

        :param module_id: The module whose plan this is, or ``None``
            outside a campaign module, where nothing is recorded.
        :param plan: The module's approved plan of record.
        """
        if module_id is None:
            return
        seen = {text for _module, text in self.decisions}
        fresh = [d for d in parse_decisions(plan) if d not in seen]
        if not fresh:
            return
        self.decisions.extend((module_id, d) for d in fresh)
        click.echo(
            f'[plan] [{module_id}] recorded {len(fresh)} decision(s) '
            f'binding on later modules.'
        )

    def seed_from(self, worktree: str) -> None:
        """
        Load decisions this campaign recorded in an EARLIER run.

        The decisions are otherwise filled only from run state, which is
        per-run — so a project that builds one module per run started
        every planner with an empty ledger and carried nothing forward,
        while the committed document sat in the worktree unread. The
        repo is the thing that survives a run; this is what reads it
        back.

        Deduplicated on text against what is already held, so a resumed
        run that has the decisions in its state does not double them.
        Best-effort: an unreadable or heading-less ledger seeds nothing
        and the run proceeds exactly as it did before, because a missing
        ledger is the normal first-module case.

        :param worktree: The planner node's clone, cut from the repo and
            therefore carrying whatever the last module published.
        """
        if not self.ctx.config.subtasks:
            return
        try:
            text = self.ctx.worktrees.read_tracked_file(worktree, self.path)
        except click.ClickException:
            return
        seen = {held for _module, held in self.decisions}
        fresh: list[tuple[str, str]] = []
        for module, item in parse_decisions_doc(text):
            if item in seen:
                continue
            seen.add(item)
            fresh.append((module, item))
        if not fresh:
            return
        self.decisions.extend(fresh)
        modules = sorted({module for module, _ in fresh})
        click.echo(
            f'[plan] carried {len(fresh)} decision(s) forward from '
            f'{", ".join(f"[{m}]" for m in modules)} — recorded by an '
            f'earlier run and read back from {self.path}.'
        )

    def record_referrals(
        self, module_id: str | None, reviews: Iterable[ReviewRecord]
    ) -> None:
        """
        Record what a module's reviewers addressed to a later module.

        Deduplicated on text, like :meth:`record_decisions`: both
        reviewers noticing the same thing, and a reviewer re-raising it
        each round, are the normal case, and the later planner needs the
        observation once rather than six times. That is a different
        judgement from :func:`finding_id`'s, which keeps duplicates
        BECAUSE a re-raise after a round of work is its own event — here
        the audience is a planner reading a ledger, not a human auditing
        a review.

        :param module_id: The module reviewed, or ``None`` outside a
            campaign module, where nothing is recorded.
        :param reviews: The module's review records.
        """
        if module_id is None:
            return
        seen = {text for _module, text in self.referrals}
        fresh: list[str] = []
        for rec in reviews:
            for text in rec.later_increment:
                if text not in seen:
                    seen.add(text)
                    fresh.append(text)
        if not fresh:
            return
        self.referrals.extend((module_id, text) for text in fresh)
        click.echo(
            f'[findings] [{module_id}] routed {len(fresh)} observation(s) '
            f'to the decisions ledger for a later module.'
        )

    def planner_block(self, subtasks: Sequence[pipeline.Subtask]) -> str:
        """
        The ledger, as a block for a planner turn.

        :param subtasks: The campaign's modules, for their titles.
        :returns: Decisions by module, then referrals marked as
            observations a planner may reject.
        """
        titles = {s.id: s.title for s in subtasks}
        parts = []
        for module_id, items in _by_module(self.decisions):
            head = f'[{module_id}] {titles.get(module_id, "")}'.strip()
            body = '\n'.join(f'  - {text}' for text in items)
            parts.append(f'{head}\n{body}')
        referrals = _by_module(self.referrals)
        if referrals:
            parts.append(
                'Raised by REVIEW for a later module. These are '
                'observations, NOT decisions: nothing here binds you, '
                'and you may judge any of them wrong. Say what you '
                'decided about the ones in your scope.'
            )
            for module_id, items in referrals:
                head = f'raised in [{module_id}]'
                body = '\n'.join(f'  - {text}' for text in items)
                parts.append(f'{head}\n{body}')
        return '\n'.join(parts)

    def rulings_block(self, subtasks: Sequence[pipeline.Subtask]) -> str:
        """
        The settled decisions, framed for a reviewer turn.

        :meth:`planner_block` serves the PLANNER, which is also shown
        REFERRALS — open observations it may accept or reject. A
        reviewer must not see those: a referral is explicitly not
        binding, and one printed beside real rulings reads as a closed
        question. Decisions only, therefore.

        :param subtasks: The campaign's modules, for their titles.
        :returns: The block, or ``''`` when nothing has been settled.
            An empty heading would assert that nothing was, which is a
            claim; silence is not.
        """
        titles = {s.id: s.title for s in subtasks}
        parts = []
        for module_id, items in _by_module(self.decisions):
            head = f'[{module_id}] {titles.get(module_id, "")}'.strip()
            body = '\n'.join(f'  - {text}' for text in items)
            parts.append(f'{head}\n{body}')
        if not parts:
            return ''
        return _REVIEW_SETTLED + '\n'.join(parts)

    def document(self, subtasks: Sequence[pipeline.Subtask]) -> str:
        """
        The ledger as the committed Markdown document.

        :param subtasks: The campaign's modules, for their titles.
        :returns: The document, which :func:`parse_decisions_doc` reads
            back into the decisions.
        """
        titles = {s.id: s.title for s in subtasks}
        lines = [
            f'# Decisions carried forward — {self.ctx.config.name}',
            '',
            "Recorded by each module's approved plan as BINDING on the "
            'modules that follow. A later module designs against these; '
            'a genuine need to change one is a halt-and-escalate to the '
            'human, never a silent edit.',
        ]
        for module_id, items in _by_module(self.decisions):
            title = titles.get(module_id, '')
            lines += ['', f'## [{module_id}] {title}'.rstrip(), '']
            lines += [f'- {text}' for text in items]
        referrals = _by_module(self.referrals)
        if referrals:
            lines += [
                '',
                '# Raised by review, for a later module',
                '',
                'A reviewer noticed these and said they belong to a '
                'module that had not been planned yet, so they are '
                'routed here rather than filed as issues addressed to '
                'nobody. They are NOT binding, unlike everything above: '
                'the module that owns one decides what to do about it, '
                'and deciding it is wrong is a legitimate outcome.',
            ]
            for module_id, items in referrals:
                title = titles.get(module_id, '')
                head = f'## Raised in [{module_id}] {title}'.rstrip()
                lines += ['', head, '']
                lines += [f'- {text}' for text in items]
        return '\n'.join(lines) + '\n'

    def commit(
        self, winner: str, subtasks: Sequence[pipeline.Subtask]
    ) -> None:
        """
        Commit the ledger onto a published module.

        The ledger rides the campaign thread, so every later module's
        worktree carries every decision recorded before it — and the
        human reviewing the PRs gets one document instead of having to
        reconstruct the reasoning from N separate plans.

        :param winner: The node whose branch this module publishes.
        :param subtasks: The campaign's modules, for their titles.
        """
        if not (
            self.ctx.config.subtasks and (self.decisions or self.referrals)
        ):
            return
        wt = self.ctx.worktrees
        wt.write_tracked_file(
            wt.node_worktree_path(self.ctx.run_id, winner),
            self.path,
            self.document(subtasks),
        )
        wt.commit_node(
            self.ctx.run_id, winner,
            message='docs: record decisions binding later modules',
            author='planner <planner@pipeline.local>',
        )


def _by_module(entries: list[tuple[str, str]]) -> list[tuple[str, list[str]]]:
    """
    Group ``(module, text)`` entries by module, in module order.

    :param entries: The entries, in the order recorded.
    :returns: ``(module, [text, …])`` for each module.
    """
    grouped: dict[str, list[str]] = {}
    for module_id, text in entries:
        grouped.setdefault(module_id, []).append(text)
    return list(grouped.items())
