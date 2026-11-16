"""What a failed turn's message says about why it failed.

"failed: None" is the least useful sentence a run can produce. These
read what the session, its VM and the error text say, and turn it into
the reason and, where there is one, the fix: a rejected credential, a
usage limit, or a terminal that never started.
"""

from __future__ import annotations

from sbx_omnigent import guest_log
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import LostTurn
from sbx_omnigent.runner.sandboxes import sandbox_for
from sbx_omnigent.runner.session_registry import SessionRegistry
from sbx_omnigent.swarm_session import (
    SwarmSessionError,
    SwarmTurnTimeout,
    looks_like_rate_limit,
)


class FailureNotes:
    """
    The notes appended to a failed turn's message.

    :param ctx: The run.
    :param registry: The run's sessions, for each one's agent.
    :param keep: ``--keep``: the run's VMs outlive it.
    """

    def __init__(
        self, ctx: RunContext, *, registry: SessionRegistry, keep: bool
    ) -> None:
        self.ctx = ctx
        self._registry = registry
        self._keep = keep

    def snapshot(self, session: str) -> dict | None:
        """
        The session's state after a failed turn, or ``None``.

        :param session: The session whose turn just failed.
        :returns: The snapshot, or ``None`` when it could not be read.
        """
        try:
            return self.ctx.sessions.get_status(session)
        except SwarmSessionError:
            return None

    def auth(
        self, session: str, error: str | None, snap: dict | None
    ) -> str:
        """
        Name the rejected credential behind a failed turn, if any.

        A dead credential fails as a harness that never starts (#26).
        Where the reason shows up depends on the harness:

        * Claude: Claude Code fires ``StopFailure`` and Omnigent makes
          its "Failed to authenticate. API Error: 401 ..." the turn's
          error, and the session's ``last_task_error``. Checked first,
          since it costs nothing.
        * Codex: only the runner log inside the VM held the ``401`` on
          2026-09-14, so that is read next, with one bounded
          ``sbx exec``.

        Best-effort like the pane capture: it runs while a turn is
        already failing, so any error here yields no note rather than a
        different failure. ``KeyboardInterrupt`` still gets out.

        :param session: The session whose turn just failed.
        :param error: The failed turn's error, if any.
        :param snap: The session snapshot, or ``None`` if unreadable.
        :returns: A parenthesised note for the failure message, or
            ``''``.
        """
        reported = (snap or {}).get('last_task_error')
        phrase = guest_log.auth_failure(error)
        where = "the turn's error"
        if phrase is None and isinstance(reported, str):
            phrase = guest_log.auth_failure(reported)
            where = "the session's last error"
        if phrase is None:
            sandbox = sandbox_for(self.ctx.sessions, session)
            if sandbox is None:
                return ''
            try:
                phrase = guest_log.auth_failure(
                    guest_log.read_runner_log(sandbox)
                )
            except Exception:
                return ''
            if phrase is None:
                return ''
            where = f'the runner log in {sandbox}'
        agent = self._registry.agents.get(session)
        harness = agent.harness if agent is not None else ''
        return (
            f' ({where} shows an authentication failure, "{phrase}": '
            f'the credential this agent runs on was rejected. To renew '
            f'it, {guest_log.relogin_hint(harness)})'
        )

    def rate_limit(
        self, session: str, error: str | None, snap: dict | None
    ) -> str:
        """
        Say so when a failed turn reads like a usage or rate limit.

        Only the swarm CLI used to check, so a throttled pipeline turn
        read exactly like a failed one, and nothing said which account's
        limit it was (#35). Reads the turn's error and the session's
        ``last_task_error`` only: the agent's reply and its pane hold
        the task itself, where "quota" or "capacity" can be the subject.
        A hint, not a verdict: a turn with error text is already treated
        as the agent's failure and not retried.

        :param session: The session whose turn just failed.
        :param error: The failed turn's error, if any.
        :param snap: The session snapshot, or ``None`` if unreadable.
        :returns: A parenthesised note for the failure message, or
            ``''``.
        """
        reported = (snap or {}).get('last_task_error')
        if not looks_like_rate_limit(
            error, reported if isinstance(reported, str) else None
        ):
            return ''
        agent = self._registry.agents.get(session)
        harness = agent.harness if agent is not None else ''
        return (
            f' (this reads like a usage or rate limit, not a defect in '
            f'the work: {guest_log.usage_limit_hint(harness)}, then '
            f'resume the run with --resume)'
        )

    def no_pane(self, session: str) -> str:
        """
        Why a failed turn has no pane, and where its real error is.

        This used to say "the sandbox was already gone" whenever no pane
        came back. Seen live twice with the VM running: the terminal had
        never started (tmux refused its launch command), so there was no
        pane to read. The harness logs the reason inside the VM, and the
        message has to point there.

        :param session: The session whose turn failed.
        :returns: A parenthesised note for the failure message.
        """
        sandbox = sandbox_for(self.ctx.sessions, session)
        if sandbox is None:
            return (
                " (no pane captured — the session's VM could not be "
                'located)'
            )
        kept = (
            'This run keeps its VMs (--keep), so the VM is still there.'
            if self._keep
            else 'Teardown removes the VM, so re-run with --keep to read it.'
        )
        return (
            f' (no pane captured from {sandbox}: most often the harness '
            f'terminal never started. Its own error is in the runner log '
            f"inside the VM: sbx exec {sandbox} -- sh -lc 'tail -120 "
            f"~/.omnigent/logs/runner/runner-*.log'. {kept})"
        )


def session_failure_note(snap: dict | None) -> str:
    """
    What the SESSION says about a turn that gave no reason.

    A failed turn arrives as a status edge carrying no error, which the
    runner then reports verbatim as ``failed: None`` — the least useful
    sentence it can produce. The session itself is still readable at
    that moment and carries three fields nobody consulted:
    ``last_task_error`` (read only by the swarm CLI), ``runner_online``
    (read nowhere at all), and ``sandbox_status``.

    Reading them turns "failed: None" into a sentence, and separates the
    two cases that matter: the AGENT failed, or its runner went away
    underneath it.

    :param snap: The session snapshot, or ``None`` when unreadable.
    :returns: A note to append to the error, or ``''``.
    """
    if snap is None:
        return ' (the session could not be read for a reason)'
    bits: list[str] = []
    err = snap.get('last_task_error')
    if isinstance(err, str) and err.strip():
        bits.append(f'last_task_error={err.strip()[:300]}')
    if snap.get('runner_online') is False:
        bits.append(
            'its RUNNER IS OFFLINE — the turn did not fail so much '
            'as lose the process running it'
        )
    sandbox = snap.get('sandbox_status')
    if isinstance(sandbox, str) and sandbox:
        bits.append(f'sandbox={sandbox}')
    return f' ({"; ".join(bits)})' if bits else ''


def turn_was_lost(error: str | None, snap: dict | None) -> bool:
    """
    Whether a failed turn was lost rather than failed by its agent.

    Lost means the runner under the turn went offline, or neither the
    turn nor the session gives any error. An unreadable session with no
    error on the turn reads as lost: "failed: None" is exactly the state
    a retry is for, and the retry costs one turn.

    :param error: The failed turn's error, if any.
    :param snap: The session snapshot, or ``None`` when unreadable.
    :returns: Whether the turn was lost.
    """
    snap = snap or {}
    if snap.get('runner_online') is False:
        return True
    reported = snap.get('last_task_error')
    return not (error and error.strip()) and not (
        isinstance(reported, str) and reported.strip()
    )


def lost_not_failed(exc: Exception) -> bool:
    """
    Whether a writer's failed turn is one worth a second attempt.

    :param exc: What the turn raised.
    :returns: ``True`` for a lost turn or a turn that never came back,
        ``False`` for an agent error or a turn that ran out of time.
    """
    if isinstance(exc, SwarmSessionError):
        return not isinstance(exc, SwarmTurnTimeout)
    return isinstance(exc, LostTurn)
