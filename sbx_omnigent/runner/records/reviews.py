"""One reviewer's vote and the report behind it."""

from __future__ import annotations

from dataclasses import dataclass

from sbx_omnigent.runner.parsing.text import _strings


@dataclass(frozen=True)
class ReviewRecord:
    """
    One reviewer's vote and the report behind it.

    Captured because the reviewer does not survive its own vote: its
    microVM is disposed as soon as the votes are in, and disposing a
    session deletes the transcript with it. Nothing else in the run
    keeps the reasoning — an APPROVED verdict used to be reduced to a
    single token and the report dropped on the spot, which is the
    reasoning behind a decision to SHIP.

    :param chunk: Module id this vote belongs to (``None`` outside a
        campaign), so a module's pull request carries only its own.
    :param stage: The review stage's id.
    :param reviewer: The agent name that voted.
    :param round_no: Which review round this was (1-based).
    :param verdict: ``'APPROVED'``, ``'BLOCKING'``, or ``None`` when the
        reviewer never stated one (treated as blocking).
    :param turns: ``(role, text)`` message pairs, oldest first.
    """

    chunk: str | None
    stage: str
    reviewer: str
    round_no: int
    verdict: str | None
    turns: tuple[tuple[str, str], ...] = ()
    #: The reviewer's NON-BLOCKING findings, lifted from its
    #: ``FINDINGS:`` block. Empty when it raised none, or when it did
    #: not use the marker.
    #:
    #: Kept as the UNION of the three sections below, which it predates
    #: — every existing consumer reads this one, and a resumed run's
    #: state carries it. The sections are what routing reads.
    findings: tuple[str, ...] = ()
    #: The union's three parts, in the reviewer's own words. Additive
    #: with empty defaults, so state written before the split restores
    #: as "uncategorized" rather than refusing to load — which is why
    #: RUN_STATE_VERSION does not move (see :meth:`from_dict`).
    defects: tuple[str, ...] = ()
    later_increment: tuple[str, ...] = ()
    premises: tuple[str, ...] = ()
    #: Checks the sandbox stopped this reviewer running. NOT in
    #: :attr:`findings`, so nothing files, ledgers or verifies them;
    #: they are surfaced once per stage instead (#41). Additive like the
    #: sections above.
    environment: tuple[str, ...] = ()
    #: The writer node this vote was cast against. Two candidates are
    #: reviewed per chunk, so a finding is meaningless without it.
    target: str | None = None
    #: The report behind a BLOCKING verdict, verbatim. Empty for an
    #: approval, whose observations are already sorted into the three
    #: sections above.
    #:
    #: Blocking findings have no marker — a reviewer writes prose under
    #: a heading — so there is nothing to lift into a list. Keeping the
    #: report whole is what lets triage read it later; the alternative
    #: was a new required marker, which is the channel #15 showed to be
    #: lossy. Additive with an empty default, so state written before
    #: it restores as "no report" and RUN_STATE_VERSION does not move.
    blocking_report: str = ''
    #: Files saved under the run directory with this record, committed
    #: beside the reviews record at publish: a security scan's own
    #: report folder. Additive with an empty default, like the above.
    attachments: tuple[str, ...] = ()

    def filed_findings(self) -> list[tuple[int, str]]:
        """
        The findings that belong in the tracker, with their ids' index.

        Everything except ``later_increment``, which is ROUTED to the
        decisions ledger instead: those items are addressed to a module
        that has not been planned yet, and an issue is the wrong shape
        for them — it waits for a human to notice it was meant for
        somebody else.

        The index is the position in :attr:`findings`, NOT in the
        returned list. :func:`finding_id` is positional, so numbering a
        filtered list would shift every id after the first routed item
        and re-file findings already closed. Filtering must never
        renumber.

        :returns: ``(index, text)`` pairs, in :attr:`findings` order.
        """
        routed = set(self.later_increment)
        return [
            (index, text)
            for index, text in enumerate(self.findings, start=1)
            if text not in routed
        ]

    @property
    def label(self) -> str:
        """``"<stage>-<reviewer>"`` — the node label used in logs."""
        return f'{self.stage}-{self.reviewer}'

    @property
    def slug(self) -> str:
        """
        A filename stem unique per reviewer and round.

        No module prefix: in a campaign the runner has already
        namespaced the stage id (``m1-review``), so the label carries
        the module. :attr:`chunk` exists for the pull-request filter,
        not for naming.
        """
        return f'{self.label}-r{self.round_no}'

    def as_dict(self) -> dict:
        """:returns: A JSON-serializable form for the run state."""
        return {
            'chunk': self.chunk,
            'stage': self.stage,
            'reviewer': self.reviewer,
            'round_no': self.round_no,
            'verdict': self.verdict,
            'turns': [list(pair) for pair in self.turns],
            'findings': list(self.findings),
            'defects': list(self.defects),
            'later_increment': list(self.later_increment),
            'premises': list(self.premises),
            'environment': list(self.environment),
            'target': self.target,
            'blocking_report': self.blocking_report,
            'attachments': list(self.attachments),
        }

    @classmethod
    def from_dict(cls, raw: dict) -> ReviewRecord | None:
        """
        Rebuild a record from run state, or ``None`` if unusable.

        Defensive by design: a resumed run must not die over a malformed
        bookkeeping entry.

        :param raw: One entry from the persisted ``reviews`` list.
        :returns: The record, or ``None``.
        """
        stage, reviewer = raw.get('stage'), raw.get('reviewer')
        if not isinstance(stage, str) or not isinstance(reviewer, str):
            return None
        turns = tuple(
            (pair[0], pair[1])
            for pair in raw.get('turns') or []
            if isinstance(pair, (list, tuple)) and len(pair) == 2
        )
        chunk = raw.get('chunk')
        verdict = raw.get('verdict')
        target = raw.get('target')
        return cls(
            chunk=chunk if isinstance(chunk, str) else None,
            stage=stage,
            reviewer=reviewer,
            round_no=int(raw.get('round_no') or 1),
            verdict=verdict if isinstance(verdict, str) else None,
            turns=turns,
            findings=_strings(raw.get('findings')),
            # Absent from state written before the split. An older
            # record restores with its findings uncategorized, which is
            # the pre-split behaviour exactly, so an in-flight run stays
            # resumable and RUN_STATE_VERSION does not move.
            defects=_strings(raw.get('defects')),
            later_increment=_strings(raw.get('later_increment')),
            premises=_strings(raw.get('premises')),
            environment=_strings(raw.get('environment')),
            target=target if isinstance(target, str) else None,
            blocking_report=(
                raw['blocking_report']
                if isinstance(raw.get('blocking_report'), str)
                else ''
            ),
            attachments=_strings(raw.get('attachments')),
        )
