"""A judge's choice between competing implementations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath

from sbx_omnigent.worktrees import RETAINED_DIR


@dataclass(frozen=True)
class JudgePick:
    """
    One judge's choice between competing implementations.

    The competing-writers design is the most expensive thing this
    pipeline does — it doubles the implementation stage AND its review
    stages — and whether that cost is repaid depends entirely on whether
    both candidates actually win sometimes. Until this record existed
    that question could not be answered from anything the pipeline
    produced: ``SELECT:`` appeared in no published artifact, and the
    only way to establish that the second writer had ever won was to
    notice its name among the commit authors in merged ``main`` — an
    accident of a metadata leak, not instrumentation.

    It also makes the JUDGE auditable. A judge that systematically
    favours one family is what would make the whole race theatre, and
    that is invisible without a series of these.

    :param chunk: Module id this pick belongs to (``None`` outside a
        campaign), so a module's pull request carries only its own.
    :param stage: The judge stage's id.
    :param candidates: The competing node ids it compared, in order.
    :param selected: The node id that won and will publish.
    :param stated: What the judge's own ``SELECT:`` line named, or
        ``None`` when it never stated one. Kept SEPARATE from
        *selected* because the runner used to fall back to the first
        candidate when the line was missing or named a non-candidate.
        It now halts instead, but picks recorded before that change
        still carry the substitution, which must not read as a real
        decision.
    :param reasoning: The judge's reply, which carries its per-candidate
        assessment. The session is disposed moments later.
    :param retained: ``(node, bundle-path)`` for each candidate that did
        NOT win and was preserved, so the record says where the losing
        implementation actually lives.
    :param forfeited: ``(candidate, review stage, rounds)`` for each
        candidate withdrawn before judging because its review reached no
        consensus. Without it a reduced field reads as a full one (#47).
    :param uncontested: Only one candidate was left, so no judge ran and
        *stated* is ``None``. Not a judgment, and not a failed one.
    """

    chunk: str | None
    stage: str
    candidates: tuple[str, ...]
    selected: str
    stated: str | None
    reasoning: str = ''
    retained: tuple[tuple[str, str], ...] = ()
    forfeited: tuple[tuple[str, str, int], ...] = ()
    uncontested: bool = False

    @property
    def honored(self) -> bool:
        """Whether the published winner is the one the judge named."""
        return self.stated == self.selected

    def as_dict(self) -> dict:
        """:returns: A JSON-serializable form for the run state."""
        return {
            'chunk': self.chunk,
            'stage': self.stage,
            'candidates': list(self.candidates),
            'selected': self.selected,
            'stated': self.stated,
            'reasoning': self.reasoning,
            'retained': [list(pair) for pair in self.retained],
            'forfeited': [list(entry) for entry in self.forfeited],
            'uncontested': self.uncontested,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> JudgePick | None:
        """
        Rebuild a pick from run state, or ``None`` if unusable.

        Defensive like :meth:`ReviewRecord.from_dict`: a resumed run
        must not die over a malformed bookkeeping entry.

        :param raw: One entry from the persisted ``judge_picks`` list.
        :returns: The pick, or ``None``.
        """
        stage, selected = raw.get('stage'), raw.get('selected')
        if not isinstance(stage, str) or not isinstance(selected, str):
            return None
        cands = raw.get('candidates')
        stated = raw.get('stated')
        chunk = raw.get('chunk')
        why = raw.get('reasoning')
        return cls(
            chunk=chunk if isinstance(chunk, str) else None,
            stage=stage,
            candidates=tuple(c for c in (cands or []) if isinstance(c, str)),
            selected=selected,
            stated=stated if isinstance(stated, str) else None,
            reasoning=why if isinstance(why, str) else '',
            retained=tuple(
                (pair[0], pair[1])
                for pair in raw.get('retained') or []
                if isinstance(pair, (list, tuple)) and len(pair) == 2
            ),
            # Both absent from state written before #47, which restores
            # an ordinary contested pick.
            forfeited=tuple(
                (entry[0], entry[1], entry[2])
                for entry in raw.get('forfeited') or []
                if isinstance(entry, (list, tuple))
                and len(entry) == 3
                and isinstance(entry[0], str)
                and isinstance(entry[1], str)
                and type(entry[2]) is int
            ),
            uncontested=raw.get('uncontested') is True,
        )


def _withdrawn(pick: JudgePick) -> str:
    """
    Name each candidate withdrawn before *pick*, with why.

    :param pick: The selection.
    :returns: e.g. ```impl-a` (`review-a`, no consensus after 2
        round(s))``, or ``''`` when nothing was withdrawn.
    """
    return ', '.join(
        f'`{cand}` (`{review}`, no consensus after {rounds} round(s))'
        for cand, review, rounds in pick.forfeited
    )


def retained_bundle_ref(path: str) -> str:
    """
    A retained bundle's path relative to the canonical root.

    The selection record and the PR description are published, so they
    must not carry the runner host's own directories. The full path
    stays in the console and the run state, on that host.

    :param path: The bundle's absolute path, e.g.
        ``/<canonical root>/_retained/<run>/<node>.bundle``.
    :returns: ``_retained/<run>/<node>.bundle``, or the file name alone
        for a bundle outside that tree.
    """
    parts = PurePosixPath(path).parts
    if RETAINED_DIR in parts:
        tail = parts[len(parts) - 1 - parts[::-1].index(RETAINED_DIR):]
        return str(PurePosixPath(*tail))
    return PurePosixPath(path).name
