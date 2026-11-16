"""Session disposal: handing each microVM back, and never losing track.

A session's VM is expensive and useless once nothing will drive it
again, so it is handed back as early as that is known: a finished
reader or judge at once, a module's sessions when it publishes, the
rest at teardown. A delete that fails keeps the session tracked, so
teardown and the next --resume try again.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

import click

from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.session_registry import SessionRegistry
from sbx_omnigent.swarm_session import SwarmSessionError


class SessionDisposal:
    """
    Free, dispose and tear down the run's sessions.

    :param ctx: The run.
    :param registry: The run's sessions.
    :param keep: ``--keep``: every VM and the run directory stay.
    :param save_state: Persists the run's state.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        registry: SessionRegistry,
        keep: bool,
        save_state: Callable[[], None],
    ) -> None:
        self.ctx = ctx
        self._registry = registry
        self._keep = keep
        self._save_state = save_state

    def free(self, session: str, why: str) -> bool:
        """
        Dispose one session and stop treating it as live.

        The single place a VM is handed back early. A delete that FAILS
        deliberately leaves the session in the registry, so the
        chunk disposal and teardown both try again — a microVM that may
        still be running must never stop being tracked.

        :param session: The session to free.
        :param why: One line for the log, naming what was freed and why.
        :returns: Whether the microVM was actually released.
        """
        if self._keep or session in self._registry.released:
            return False
        try:
            self.ctx.sessions.dispose(session)
        except SwarmSessionError:
            return False
        self._registry.released.add(session)
        self._registry.forget(session)
        click.echo(f'[cleanup] {why}')
        return True

    def dispose(self, sessions: Sequence[str]) -> None:
        """
        Tear down exactly the microVMs named, wherever they sit.

        The identity-based counterpart to
        :meth:`Campaign._release_sessions`. Two review stages running at
        the same time cannot both free their guests by an index mark:
        each takes its mark as the length of the registry's session
        list, so the stage that finishes second slices from ITS mark
        and deletes the sessions the first one is still driving.
        Naming the sessions removes the ambiguity — a stage frees what
        it created and nothing else.

        Outstanding marks survive this. A mark is taken when a chunk
        begins and the sessions removed here are always created after
        it, so they sit at an index at or beyond every live mark, and
        dropping them leaves ``_sessions[mark:]`` meaning what it did
        before: everything still live that the chunk created.

        The delete itself is deliberately OUTSIDE the lock. It is a
        network round trip per guest, and holding the lock across it
        would serialize the very teardown running stages in parallel
        exists to overlap.

        :param sessions: The sessions to free. Already-freed ones are
            skipped, so this is safe as a backstop after each turn has
            released its own guest.
        """
        if self._keep or not sessions:
            return
        with self.ctx.lock:
            doomed = set(sessions)
            live = [s for s in sessions if s not in self._registry.released]
            # Freed already by the turn that owned them; forget the
            # bookkeeping now that nothing will ask about them again.
            self._registry.released.difference_update(doomed)
        failed: list[str] = []
        for session in live:
            try:
                self.ctx.sessions.dispose(session)
            except SwarmSessionError:
                # The delete failed, so the microVM may well still be
                # up. Dropping the handle here would strand it: it is
                # cut from the state file at the next write, and then
                # nothing — not teardown, not a later --resume — knows
                # it exists. That is how a run leaks gigabytes of image
                # store with no record of what is holding them.
                failed.append(session)
        with self.ctx.lock:
            for session in live:
                self._registry.forget(session)
            self._registry.sessions[:] = [
                s for s in self._registry.sessions if s not in doomed
            ]
            if failed:
                self._registry.undisposed.extend(failed)
        if failed:
            click.echo(
                f'[cleanup] {len(failed)} microVM(s) refused to '
                f'dispose; keeping them in the run state so teardown '
                f'and --resume can try again.'
            )
        self._save_state()

    def dispose_stale(self) -> None:
        """
        Tear down the VMs the previous attempt of this run left up.

        ``--keep`` plus ``--resume`` leaks by construction: the earlier
        process kept its microVMs deliberately, and the resuming
        process starts with an empty session list, so nothing it owns
        would ever dispose them — not at a chunk boundary, not at
        teardown. They would sit holding disk (and their sandbox
        runtimes) for the whole continued run, on top of the VMs it
        stands up itself. Observed live: 12 microVMs from a finished
        module still running eight hours later, on a host at 100%
        capacity — the same exhaustion that remounts guest filesystems
        read-only.

        Best-effort and never fatal: a resume must not fail because a
        VM it was cleaning is already gone (``dispose`` treats a 404 as
        success), and ``--keep`` is honored, since the human asked for
        the previous attempt's VMs to stay.
        """
        stale, self._registry.stale = self._registry.stale, []
        if not stale:
            return
        if self._keep:
            click.echo(
                f'[resume] --keep: leaving {len(stale)} microVM(s) from '
                f'the previous attempt up (they are no longer tracked, '
                f'so nothing will dispose them later).'
            )
            return
        gone = 0
        for session in stale:
            try:
                self.ctx.sessions.dispose(session)
                gone += 1
            except SwarmSessionError:
                pass  # already torn down, or the server lost it
        click.echo(
            f'[resume] disposed {gone}/{len(stale)} microVM(s) left '
            f'running by the previous attempt.'
        )

    def teardown(self, *, preserve_run: bool = False) -> None:
        """
        Dispose the run's microVMs — and its directory only if done.

        The VMs always go: they are expensive and useless the moment
        this process ends. The RUN DIRECTORY is different. It holds the
        hub clone carrying every node branch, and state.json — together,
        the only record of what the run achieved and the only thing
        ``--resume`` can read. Removing it on a FAILED run destroys
        exactly what resume exists for.

        Observed live: a run died standing up one session, teardown
        wiped the directory, and a finished module's approved plan, its
        frozen test suite, and both competing implementations went with
        it — none of it published, so nothing remained anywhere. Until
        now only ``--keep`` prevented that, which made resume-after-
        failure work by accident rather than by design.

        :param preserve_run: Keep the run directory (a failed or
            blocked run). ``--keep`` still preserves everything.
        """
        if self._keep:
            return
        for session in self._registry.outstanding():
            try:
                self.ctx.sessions.dispose(session)
            except SwarmSessionError:
                pass
        if preserve_run:
            click.echo(
                f"[cleanup] disposed this run's microVMs; kept "
                f'{self.ctx.worktrees.run_dir(self.ctx.run_id)} so '
                f'--run-id {self.ctx.run_id} --resume can continue it.'
            )
            return
        try:
            self.ctx.worktrees.dispose_run(self.ctx.run_id)
        except click.ClickException:
            pass
