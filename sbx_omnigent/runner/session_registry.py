"""The session registry: what the run knows about each agent session.

Every agent turn runs in a session on its own microVM. The run keeps a
few facts about each one: what it is for, which agent and worktree it
was created with, whether it has been freed, and whether its delete
failed. The runner holds its lock around every change to these.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field

from sbx_omnigent import pipeline


@dataclass
class SessionRegistry:
    """
    Every session the run created, and what it knows of each.

    :param sessions: Every session created, in order. The campaign
        frees a module's sessions by slicing this list from the length
        it had when the module started, so an entry is only ever
        removed when it was created after every such mark.
    :param released: Sessions already freed at stage completion (a
        finished reader or judge). Held as a SET rather than removed
        from *sessions*, because the campaign frees a module's sessions
        by slicing that list by index — dropping an element from the
        middle would shift the marks and free the wrong sessions.
    :param undisposed: Sessions whose delete FAILED. Kept so they stay
        in the run state and get another chance — at teardown, and
        again on the next --resume. Held apart from *sessions* so the
        campaign's mark-based slicing keeps its index semantics.
    :param stale: Sessions a PREVIOUS attempt of this run left behind,
        read from its state on resume. For DISPOSAL ONLY — never
        driven, never treated as live.
    :param labels: session -> node label, for naming a captured
        transcript.
    :param agents: The agent spec behind each session, so a launch can
        be checked against what was actually requested for it.
    :param worktrees: session -> its node worktree path, so an agy turn
        can be staged as a file the agent reads.
    :param agy: agy sessions get the native-TUI first-turn hardening
        (settle + resubmit); tracked so only each session's FIRST turn
        pays.
    :param driven: Sessions that have had a turn.
    :param in_turn: Sessions with a turn running now, so an interrupt
        can keep what each one was doing before teardown deletes it.
    :param captures: label -> how many captures it already has, so a
        writer that is re-driven twice does not overwrite its own first
        record.
    """

    sessions: list[str] = field(default_factory=list)
    released: set[str] = field(default_factory=set)
    undisposed: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)
    agents: dict[str, pipeline.PipelineAgent] = field(default_factory=dict)
    worktrees: dict[str, str] = field(default_factory=dict)
    agy: set[str] = field(default_factory=set)
    driven: set[str] = field(default_factory=set)
    in_turn: set[str] = field(default_factory=set)
    captures: dict[str, int] = field(default_factory=dict)

    def register(
        self,
        session: str,
        *,
        label: str,
        agent: pipeline.PipelineAgent,
        worktree: str,
    ) -> None:
        """
        Record a session the run just created.

        :param session: Its id.
        :param label: The node it serves.
        :param agent: The agent it was created for.
        :param worktree: The node worktree mounted into it.
        """
        self.sessions.append(session)
        self.worktrees[session] = worktree
        self.labels[session] = label
        self.agents[session] = agent

    def label_of(self, session: str) -> str:
        """
        The node a session serves, for messages and file names.

        :param session: Its id.
        :returns: Its label, or the id itself when it has none.
        """
        return self.labels.get(session, session)

    def outstanding(self) -> Iterator[str]:
        """
        Every session not yet freed, failed deletes included.

        What the run state records, so a resume can dispose of them,
        and what teardown disposes.

        :returns: Each once, in creation order, then failed deletes.
        """
        for session in dict.fromkeys(self.sessions + self.undisposed):
            if session not in self.released:
                yield session

    def forget(self, session: str) -> None:
        """
        Drop what only a live session needs, once it is freed.

        Its label stays: a capture or record written later may still
        name it.

        :param session: Its id.
        """
        self.agy.discard(session)
        self.worktrees.pop(session, None)

    def first_turn(self, session: str) -> bool:
        """
        Whether *session*'s turn now is its first, and mark it driven.

        :param session: Its id.
        :returns: ``True`` once per session.
        """
        first = session not in self.driven
        self.driven.add(session)
        return first
