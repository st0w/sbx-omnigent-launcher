"""The session driver: creating agent sessions and driving their turns.

Every agent turn runs in a session on its own microVM. This creates
each one for its agent and worktree, registers it, and drives turns on
it: a turn the agent ended on background work is followed until that
work finishes, an agy turn is handed over as a file, and a failed turn
comes back with the reason and the evidence kept.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import replace

import click

from sbx_omnigent import agy, background, claude, codex, pane, readback
from sbx_omnigent.launch_args import launch_args_for
from sbx_omnigent.runner.captures import TurnCapture
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import (
    LostTurn,
    PipelineRunError,
    RunInterrupted,
)
from sbx_omnigent.runner.failure_notes import (
    FailureNotes,
    session_failure_note,
    turn_was_lost,
)
from sbx_omnigent.runner.harness_record import HarnessRecord
from sbx_omnigent.runner.limits import (
    _FIRST_TURN_MAX_RESUBMITS,
    _FIRST_TURN_REDELIVER_DELAY_S,
)
from sbx_omnigent.runner.parsing.text import _single_line
from sbx_omnigent.runner.sandboxes import sandbox_for
from sbx_omnigent.runner.session_registry import SessionRegistry
from sbx_omnigent.swarm import credential_kind_for, mount_sentinel
from sbx_omnigent.swarm_session import SwarmSessionError

#: Filename a node worktree carries an agy turn in (read by the agent,
#: never pasted). Added to the clone's ``.git/info/exclude`` so it is
#: neither committed nor seen by the settle-wait's ``git status``.
_AGY_TASK_FILE = 'OMNI_TASK.md'


class SessionDriver:
    """
    Create agent sessions and drive their turns.

    :param ctx: The run.
    :param registry: Where each session is recorded.
    :param harness: Records the VM's CLI versions after a first turn.
    :param capture: Keeps a failed turn's transcript and pane.
    :param notes: Explains a failed turn.
    :param agent_ids: ``{agent_name: registered_id}``.
    :param turn_timeout: How long one turn may run, in seconds.
    :param swap_age_s: Age of the agy swap secret, guarding every agy
        turn; ``None`` when it has never been recorded.
    :param interrupted: Set on Ctrl-C. Only the main thread sees the
        interrupt; the workers of a parallel stage read this before
        booting a VM or driving a turn, and stop instead.
    :param save_state: Persists the run's state.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        registry: SessionRegistry,
        harness: HarnessRecord,
        capture: TurnCapture,
        notes: FailureNotes,
        agent_ids: Mapping[str, str],
        turn_timeout: float,
        swap_age_s: Callable[[], float | None],
        interrupted: threading.Event,
        save_state: Callable[[], None],
    ) -> None:
        self.ctx = ctx
        self._registry = registry
        self._harness = harness
        self._capture = capture
        self._notes = notes
        self._agent_ids = agent_ids
        self._turn_timeout = turn_timeout
        self._swap_age_s = swap_age_s
        self._interrupted = interrupted
        self._save_state = save_state

    def refuse_if_interrupted(self, label: str) -> None:
        """
        Start nothing once the run has been interrupted.

        :param label: The node about to boot a VM or drive a turn.
        :raises RunInterrupted: When Ctrl-C has been pressed.
        """
        if self._interrupted.is_set():
            raise RunInterrupted(
                f'{label}: the run was interrupted, so nothing new starts'
            )

    def create(
        self,
        agent_name: str,
        worktree: str,
        mode: str,
        label: str,
    ) -> str:
        """
        Create and register a session for *agent_name* on *worktree*.

        :param agent_name: The pipeline agent to run.
        :param worktree: The node worktree mounted into its VM.
        :param mode: ``'rw'`` for a writer, ``'ro'`` for a reader.
        :param label: The node it serves, for titles and file names.
        :returns: The new session's id.
        :raises RunInterrupted: When Ctrl-C has been pressed.
        """
        self.refuse_if_interrupted(label)
        agent = self.ctx.config.agents[agent_name]
        is_agy = agent.harness in agy.AGY_HARNESSES
        credential = credential_kind_for(agent.harness)
        session = self.ctx.sessions.create(
            agent_id=self._agent_ids[agent_name],
            workspace=mount_sentinel(
                worktree, mode,
                credential=credential,
                # A Claude VM's /model picker lists only the family
                # aliases unless told; the launcher adds this model so
                # Omnigent launches it instead of the default.
                model=agent.model if credential is None else None,
            ),
            title=f'{self.ctx.run_id}/{label}',
            terminal_launch_args=list(
                launch_args_for(agent.harness, agent.effort)
            ),
            model_override=agent.model,
            reasoning_effort=agent.effort,
        )
        # One critical section for the whole registration: the
        # save below snapshots exactly these collections, so a node
        # registering concurrently must not land halfway through.
        with self.ctx.lock:
            self._registry.register(
                session, label=label, agent=agent, worktree=worktree
            )
        # Persist NOW, not at the next stage boundary. State is
        # otherwise written only when a stage or review round
        # completes, so a session created inside that window is
        # invisible to --resume forever: the stale-session disposal
        # disposes what the state file recorded, and nothing else knows
        # this VM exists. Observed live — a reviewer session created at
        # 01:24, the minute the machine crashed, survived every
        # subsequent resume and had to be found and deleted by hand.
        # One cheap write buys a VM that can always be reclaimed.
        self._save_state()
        if is_agy:
            self._registry.agy.add(session)
            # Warm the just-created agy TUI before it is driven, so the
            # first turn's paste hits a ready composer (see the module
            # note + wait_for_terminal_ready).
            self.ctx.sessions.wait_for_terminal_ready(session)
        return session

    def drive(
        self,
        session: str,
        instruction: str,
        *,
        inline: bool = False,
        done: Callable[[str], bool] | None = None,
    ) -> str:
        """
        Run one turn on *session* and return its reply.

        A turn the agent ended on background work it started is
        followed until the agent is woken and replies (see
        :mod:`sbx_omnigent.background`), so the reply returned is the
        one it gave once that work finished.

        :param session: The session to drive.
        :param instruction: The turn text.
        :param inline: Paste the turn directly instead of staging it as
            a file for an agy session. For SHORT, urgent turns whose
            whole point is that the agent acts on them right now: the
            file indirection buries them. See :data:`_VERDICT_NUDGE`.
        :param done: Whether a reply is finished, so a finished reply
            never waits on a server the agent left running. ``None``
            waits until no background work this turn started is left.
        :returns: The assistant's reply.
        :raises PipelineRunError: If the turn failed.
        """
        self.refuse_if_interrupted(
            self._registry.label_of(session)
        )
        is_agy = session in self._registry.agy
        # agy drops a multi-line paste before it renders AND collapses
        # an over-long paste into an unsubmittable placeholder — so an
        # agy turn is never pasted whole: it is handed over as a file
        # the agent reads and only a tiny pointer is pasted (see
        # _agy_deliverable). A first agy turn also gets a re-delivery
        # net if it fails before starting.
        if is_agy:
            self._require_fresh_swap(session)
            instruction = (
                # Flattened, not staged: agy still cannot take a
                # multi-line paste, but a one-line turn renders fine and
                # arrives where the agent will actually read it.
                _single_line(instruction)
                if inline
                else self._agy_deliverable(session, instruction)
            )
        first_turn = self._registry.first_turn(session)
        since = self.newest_item_id(session)
        deadline = time.monotonic() + self._turn_timeout
        with self.ctx.lock:
            self._registry.in_turn.add(session)
        try:
            result = self.ctx.sessions.send_and_wait(
                session,
                instruction,
                timeout=self._turn_timeout,
                max_resubmits=(
                    _FIRST_TURN_MAX_RESUBMITS if first_turn else 0
                ),
                redeliver_delay_s=(
                    _FIRST_TURN_REDELIVER_DELAY_S if first_turn else 0.0
                ),
            )
            # Inside the try, so the session stays "in turn" for Ctrl-C
            # while the agent's background work runs.
            if result.ok and not claude.api_error_reply(result.reply):
                waited = self._await_background(
                    session, since, result.reply or '', done, deadline
                )
                if waited != (result.reply or ''):
                    result = replace(result, reply=waited)
        except SwarmSessionError as exc:
            # The turn never came back. Teardown is seconds away and
            # will delete this session, so capture NOW or the only
            # evidence is the timeout line itself.
            self._capture.turn(
                session,
                f'the turn did not return: {exc}',
                with_pane=True,
            )
            raise
        except KeyboardInterrupt:
            # Ctrl-C on a turn that LOOKS stuck is exactly when someone
            # wants to know what it was doing — and it is the one exit
            # that skipped this, because KeyboardInterrupt is not an
            # Exception. Observed live: a reviewer that had installed a
            # toolchain, built the workspace and launched the suite was
            # interrupted, and its transcript had to be pulled out of
            # the API by hand before teardown deleted it.
            self._capture.turn(
                session,
                'interrupted (Ctrl-C) mid-turn',
                with_pane=True,
            )
            raise
        finally:
            with self.ctx.lock:
                self._registry.in_turn.discard(session)
        if first_turn and result.ok:
            # AFTER the first turn, not at create: the TUI has not
            # finished drawing when a session is created, so the pane
            # carries nothing to compare and the check would silently
            # never fire (observed — it read an empty pane every time).
            # One turn late still beats stage 6 of 8, and costs no wait.
            self._verify_launch(session)
            self._harness.record_first_turn(session)
        api_error = claude.api_error_reply(result.reply) if result.ok else None
        if api_error is not None:
            # The reply is Claude Code's refusal, not work. It can come
            # back before the server's failed status does, so it is
            # read here rather than trusted to that race.
            result = replace(result, status='failed', error=api_error)
        if not result.ok:
            snap = self._notes.snapshot(session)
            note = session_failure_note(snap) + claude.version_hint(
                result.error
            )
            auth = self._notes.auth(session, result.error, snap)
            # A rejected credential is the more specific reading, so a
            # rate-limit note is only added when there is none.
            limit = '' if auth else self._notes.rate_limit(
                session, result.error, snap
            )
            note += limit
            pane_path = self._capture.turn(
                session,
                f'the turn failed: {result.error}{note}{auth}',
                with_pane=True,
            )
            # Name the pane IN the error. A bare "failed: None" is what
            # cost a day on the codex-3 run — the blocking migration
            # picker was on screen the whole time and nothing
            # pointed at it.
            # Say why there is no pane rather than dropping the
            # suffix. A bare 'failed: None' with nothing to open is what
            # the pane capture exists to prevent, and a capture that
            # returned nothing silently reproduced it.
            where = (
                f' — see {pane_path}' if pane_path
                else self._notes.no_pane(session)
            )
            # A rejected credential is not a lost turn: retrying it on a
            # fresh VM would only fail the same way.
            error = (
                LostTurn if not auth and turn_was_lost(result.error, snap)
                else PipelineRunError
            )
            raise error(
                f'turn on {session} failed: '
                f'{result.error}{note}{auth}{where}'
            )
        return result.reply

    def newest_item_id(self, session: str) -> str | None:
        """
        Id of the session's newest item, as an activity marker.

        A DELTA, not a snapshot: comparing this across polls is what
        distinguishes a reviewer that is working from one that has gone
        quiet. Status cannot — a settled session reads idle between
        tool rounds — and the item TYPE cannot either, because a
        reviewer mid-build looks identical to one that stopped.

        :param session: The reviewer session.
        :returns: The newest item's id, or ``None`` when the session
            has none, cannot be read, or gives an id that is not text.
        """
        try:
            items = self.ctx.sessions.read_items(session, tail=1)
        except SwarmSessionError:
            return None
        ident = items[-1].get('id') if items else None
        return ident if isinstance(ident, str) else None

    def _verify_launch(self, session: str) -> None:
        """
        Say so when a harness did not launch with what we asked for.

        The launcher asks for a model, an effort and a permission mode,
        and until now never checked it got any of them. Four separate
        failures in two days turned on exactly that: a mode downgraded
        because the model lacked a capability, an effort discarded, a
        model substituted. Every one was invisible in the launcher's
        logs and plainly on the agent's screen.

        WARNS, never raises. It reads a TUI mid-draw, so a false alarm
        is entirely possible and must not be able to fail a run — and a
        read-back that cries wolf is one somebody switches off, after
        which the next silent substitution costs another day.
        :func:`readback.launch_mismatches` is conservative for the same
        reason: what it cannot read, it does not report.

        :param session: The session whose first turn just completed.
        """
        agent = self._registry.agents.get(session)
        if agent is None:
            return
        label = self._registry.label_of(session)
        sandbox = sandbox_for(self.ctx.sessions, session)
        if sandbox is None:
            return
        # capture_pane never raises by contract. Guarded anyway: this
        # read-back only warns, and must never be what fails a run.
        try:
            text = pane.capture_pane(sandbox) or ''
        except Exception:
            return
        mode = None
        if agent.harness not in agy.AGY_HARNESSES | codex.CODEX_HARNESSES:
            args = list(launch_args_for(agent.harness, agent.effort))
            if '--permission-mode' in args:
                mode = args[args.index('--permission-mode') + 1]
        for why in readback.launch_mismatches(
            agent.harness,
            text,
            model=agent.model,
            effort=agent.effort,
            permission_mode=mode,
        ):
            click.echo(f'[launch] {label}: {why}')

    def _require_fresh_swap(self, session: str) -> None:
        """
        Refuse an agy turn whose swap token has already expired.

        An agy agent VM carries only a placeholder; the sbx proxy swaps
        in the harvested token on the wire. Once that token expires, agy
        fails to authenticate INSIDE ITS OWN TUI — which the bridge
        never surfaces as a failed session — so the turn simply hangs to
        the turn timeout and reports a misleading "did not complete",
        with an empty session and no error anywhere. Burning 30 minutes
        to learn nothing is strictly worse than stopping here.

        No grace wait: :data:`agy.MAX_SWAP_AGE_S` already allows half an
        interval of slack past a healthy refresh, so exceeding it means
        the harvester is not working, not that it is mid-cycle.

        :param session: The agy session about to be driven.
        :raises PipelineRunError: If the secret is stale or unknown.
        """
        age = self._swap_age_s()
        if age is not None and age <= agy.MAX_SWAP_AGE_S:
            return
        seen = (
            'has never been recorded'
            if age is None
            else f'was last refreshed {age / 60:.0f} min ago'
        )
        raise PipelineRunError(
            f'refusing to drive agy session {session}: the swap secret '
            f'{seen} (limit {agy.MAX_SWAP_AGE_S / 60:.0f} min), so its '
            f'access token has expired and agy would fail to '
            f'authenticate inside its own TUI — the turn would hang for '
            f'{self._turn_timeout:.0f}s and report nothing. The token '
            f'harvester is not refreshing: check {agy.HARVEST_LOG}, and '
            f"if the trusted box cannot re-mint, run 'agy /login' on it."
        )

    def _agy_deliverable(self, session: str, instruction: str) -> str:
        """
        Deliver an agy turn as a file + a tiny pointer to paste.

        agy fails a turn at paste time whenever the paste is multi-line
        (dropped before it renders) or long (collapses into a
        ``[Pasted text …]`` placeholder the bridge cannot verify), so
        nothing substantial is ever pasted. The full turn — original
        formatting intact, which reads better than a flattened blob — is
        written to :data:`_AGY_TASK_FILE` at the root of the agent's own
        worktree, and only a short one-line pointer telling the agent to
        read it is pasted. The file is git-ignored in the node's clone,
        so a writer never commits it and the settle-wait never mistakes
        it for the agent's output. Reading a task file is a normal,
        allowed action for every agy role (the planner may read, just
        not write).

        :param session: The agy session being driven.
        :param instruction: The full turn text.
        :returns: The one-line pointer to paste (or, defensively, the
            flattened turn if the session has no worktree to stage
            into).
        """
        worktree = self._registry.worktrees.get(session)
        if worktree is None:
            # No worktree to stage into (should not happen — every
            # session is created with one): flatten and paste, so the
            # turn is at least attempted rather than lost.
            return _single_line(instruction)
        self.ctx.worktrees.write_ignored_file(
            worktree, _AGY_TASK_FILE, instruction
        )
        return _single_line(
            f'Your full instructions are in the file {_AGY_TASK_FILE} at '
            'the root of your workspace (your current directory). Read '
            'that file now, then carry out the task exactly as it '
            'describes.'
        )

    def _await_background(
        self,
        session: str,
        since: str | None,
        reply: str,
        done: Callable[[str], bool] | None,
        deadline: float,
    ) -> str:
        """
        Wait out the background work a turn ended on.

        See :mod:`sbx_omnigent.background`.

        :param session: The session whose turn just ended.
        :param since: Its newest item before the turn.
        :param reply: The turn's reply.
        :param done: Whether a reply is finished; ``None`` waits until
            no background work this turn started is left.
        :param deadline: When the turn's own timeout runs out.
        :returns: The reply, with any woken replies appended.
        """
        label = self._registry.label_of(session)
        try:
            return background.await_background(
                self.ctx.sessions,
                session,
                since=since,
                reply=reply,
                done=done or (lambda _reply: False),
                deadline=deadline,
                label=label,
                stop=lambda: self.refuse_if_interrupted(label),
                poll_s=background.POLL_S,
            )
        except SwarmSessionError as exc:
            # Following the work is best-effort: without it the reply is
            # handled exactly as it was before this wait existed.
            click.echo(
                f'[background] {label}: could not follow its background '
                f'work ({exc}); using the reply as it stands.'
            )
            return reply
