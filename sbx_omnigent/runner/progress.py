"""How far a run has got: its modules, the active one, what published.

The runner changes these as a run advances, and several parts of it
read them: the campaign loop, the turns it writes, publishing, and run
state. One record, with named transitions, keeps the changes in one
place.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sbx_omnigent.pipeline import Subtask


@dataclass
class RunProgress:
    """
    The run's progress: modules, the one being built, and what shipped.

    :param subtasks: Ordered build chunks: planner-proposed (flat
        campaign, see :func:`parse_subtasks`) or human-supplied modules
        (per-module mode, ``config.subtasks``). Empty = single pass.
    :param active_subtask: The chunk currently being built (campaign
        mode), so the builder instructions can scope the turn to it.
        ``None`` = single pass / not in a chunk.
    :param active_is_first: Whether the active chunk is the FIRST one —
        nothing has been built yet, so a turn must not be told to build
        on prior work.
    :param completed_chunks: Chunk ids already built AND published.
    :param plan_of_record: The approved, consolidated plan of record
        (the planner's final output) — shared with builders and
        committed to the published branch at publish. ``None`` until a
        planner node runs.
    :param last_branch_node: The last node that produced a branch: what
        publishes when the pipeline names no branch.
    """

    subtasks: list[Subtask] = field(default_factory=list)
    active_subtask: Subtask | None = None
    active_is_first: bool = False
    completed_chunks: set[str] = field(default_factory=set)
    plan_of_record: str | None = None
    last_branch_node: str | None = None

    @property
    def module_id(self) -> str | None:
        """The active module's id, or ``None`` outside a module."""
        return self.active_subtask.id if self.active_subtask else None

    def start_module(self, sub: Subtask, *, first: bool) -> None:
        """
        Make *sub* the module being built.

        :param sub: The module.
        :param first: Whether it is the campaign's first module.
        """
        self.active_subtask = sub
        self.active_is_first = first

    def end_module(self) -> None:
        """Leave the active module: the run is between modules."""
        self.active_subtask = None
        self.active_is_first = False

    def prior_module(self) -> str | None:
        """
        The module that published last before the active one.

        Derived from the modules that have published rather than
        remembered, so a resumed campaign finds it too. The module list
        supplies the order; a set of published ids alone would not
        (``m10`` sorts before ``m2``).

        :returns: Its id, or ``None`` for the first module and outside
            a module.
        """
        active = self.active_subtask
        if active is None:
            return None
        prior = None
        for sub in self.subtasks:
            if sub.id == active.id:
                break
            if sub.id in self.completed_chunks:
                prior = sub.id
        return prior
