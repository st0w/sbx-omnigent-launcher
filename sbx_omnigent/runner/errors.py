"""Exceptions the runner raises, and its internal block signal."""

from __future__ import annotations


class PipelineRunError(Exception):
    """A pipeline run failed (setup, a turn, or a git step)."""


class RunInterrupted(KeyboardInterrupt):
    """
    The run was interrupted (Ctrl-C), so nothing new may start.

    Raised in a worker thread that tries to boot a VM or drive a turn
    after the human pressed Ctrl-C. A :exc:`KeyboardInterrupt`, so the
    retry handlers, which catch :class:`PipelineRunError` and
    :class:`SwarmSessionError`, let it through: a worker whose session
    teardown just disposed must not read that as a lost turn and boot a
    fresh VM to try again.
    """


class LostTurn(PipelineRunError):
    """
    A turn that was lost rather than failed by its agent.

    It failed with no error from the agent, or the runner under it went
    offline. Nothing about it says the next attempt fails too, so a
    writer retries it (see :data:`_WRITER_TURN_ATTEMPTS`).
    """


class _Blocked(Exception):
    """Internal: a review gate never reached consensus; stop."""

    def __init__(self, stage_id: str, rounds: int) -> None:
        super().__init__(stage_id)
        self.stage_id = stage_id
        self.rounds = rounds
