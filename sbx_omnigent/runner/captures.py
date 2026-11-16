"""Captures: a session's transcript and screen, kept before teardown.

Disposing a session deletes it, and teardown disposes everything
moments after a run fails. A turn that failed, timed out or was
interrupted, and every loop-back fix turn, is written to the run
directory first, with the harness's TUI pane when the turn did not
report, because that screen is then the only evidence.
"""

from __future__ import annotations

import click

from sbx_omnigent import harness_versions, pane
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.harness_record import HarnessRecord, read_versions
from sbx_omnigent.runner.render.sessions import render_turn_capture
from sbx_omnigent.runner.sandboxes import sandbox_for
from sbx_omnigent.runner.session_registry import SessionRegistry
from sbx_omnigent.swarm_session import SwarmSessionError


class TurnCapture:
    """
    Write sessions' transcripts and panes to the run directory.

    :param ctx: The run.
    :param registry: The run's sessions, for labels and running turns.
    :param harness: Records the VM's CLI versions read with a pane.
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        registry: SessionRegistry,
        harness: HarnessRecord,
    ) -> None:
        self.ctx = ctx
        self._registry = registry
        self._harness = harness

    def turn(
        self, session: str, reason: str, *, with_pane: bool = False
    ) -> str | None:
        """
        Write a session's transcript to the run dir, best-effort.

        Called at the moments the record is about to be lost: a turn
        that failed or timed out (teardown is seconds away), a turn or
        a planning session interrupted with Ctrl-C, and every loop-back
        fix turn (its session is disposed at publish, and that turn is
        the one a human most wants to read afterwards).

        Never raises — a diagnostic that can fail a run is worse than
        no diagnostic — but it deliberately does NOT swallow a
        :exc:`KeyboardInterrupt`. Someone pressing Ctrl-C a second time
        wants out now, not one more bounded HTTP read.

        :param session: The session to read.
        :param reason: Why it is being captured.
        :param with_pane: Also capture the harness's TUI pane. Set on
            the paths where the turn did NOT report — a timeout, a
            failure, an interrupt — because that is exactly when the
            transcript is empty and the screen is the only evidence.
            Left off for routine captures so a healthy run does not pay
            an ``sbx exec`` round-trip per review round.
        :returns: The pane artifact's path when one was written, so the
            caller can name it in the error a human will actually read.
        """
        label = self._registry.label_of(session)
        try:
            turns = self.ctx.sessions.read_transcript(session)
        except SwarmSessionError:
            turns = []
        seen = self._registry.captures.get(label, 0) + 1
        self._registry.captures[label] = seen
        suffix = '' if seen == 1 else f'-{seen}'
        artifact = f'turns/{label}{suffix}.md'
        # Transcript FIRST, so the guarantee this method already made is
        # never traded for the new one: if the pane read is interrupted
        # or hangs, the conversation is already on disk.
        if self.ctx.worktrees.write_run_artifact(
            self.ctx.run_id,
            artifact,
            render_turn_capture(
                turns, title=f'Turn — {label}', reason=reason
            ),
        ):
            click.echo(
                f'[capture] wrote {label}{suffix} to the run directory '
                f'({len(turns)} message(s)): {reason}'
            )
        if not with_pane:
            return None
        pane_path = self._pane(session, artifact)
        if pane_path is not None:
            click.echo(
                f'[capture] wrote the {label} TUI pane to {pane_path} — '
                f'read it FIRST: a harness blocked on a keystroke '
                f'reports nothing anywhere else.'
            )
        return pane_path

    def interrupted(self) -> None:
        """
        Keep the record of every turn still running at Ctrl-C.

        The workers driving them never see the interrupt, so their own
        capture never runs, and teardown is about to delete the
        sessions. A second Ctrl-C stops this too.
        """
        with self.ctx.lock:
            running = sorted(self._registry.in_turn)
        for session in running:
            self.turn(
                session, 'interrupted (Ctrl-C) mid-turn', with_pane=True
            )

    def _pane(self, session: str, artifact: str) -> str | None:
        """
        Write the session's TUI pane beside its transcript.

        The transcript is empty in exactly the case this exists for — a
        harness blocked on a keystroke never produces a message — so the
        pane is often the ONLY evidence. See :mod:`sbx_omnigent.pane`.

        :param session: The session whose VM to read.
        :param artifact: Repo-relative path of the transcript artifact,
            used to derive the pane's own name.
        :returns: The pane artifact's path, or ``None`` if none written.
        """
        sandbox = sandbox_for(self.ctx.sessions, session)
        if sandbox is None:
            return None
        # Read before the pane, and kept even when there is no pane: a
        # failed turn is the one whose versions matter most (#17).
        versions = read_versions(sandbox)
        self._harness.note(session, versions)
        # capture_pane never raises by contract. Guarded anyway: this
        # runs while a turn is already failing, and a regression in the
        # diagnostic must not replace the failure it was diagnosing.
        try:
            text = pane.capture_pane(sandbox)
        except Exception:
            return None
        if not text:
            return None
        path = artifact.removesuffix('.md') + '.pane.txt'
        header = (
            f'# TUI pane — {sandbox}\n'
            f'# Captured because the turn did not report. A native '
            f'harness blocked on a keystroke goes silent on every other '
            f'channel, so this screen is usually the whole diagnosis.\n'
            "# Nothing was typed into it — answering on the "
            "human's behalf is not this tool's job.\n"
            f'# Harness CLIs installed in this VM now: '
            f'{harness_versions.format_versions(versions)}\n'
        )
        if harness_versions.pending_self_update(text):
            header += (
                '# The pane shows an installed update waiting on a '
                'restart, so the process running here is older than '
                'the version above.\n'
            )
        header += '\n'
        # If the screen is a modal picker, SAY so. The failure a human
        # otherwise sees describes the paste mechanism and buries the
        # thing to do, with the picker dumped raw at the end of a
        # RuntimeError. They are watching this console; they may never
        # open the failed message bubble.
        label = self._registry.label_of(session)
        prompt = pane.modal_prompt(text)
        if prompt is not None:
            guidance = pane.blocked_on_prompt_message(label, prompt)
            click.echo(f'[blocked] {guidance}')
            header += (
                '# THIS IS A MODAL PROMPT. A typed turn cannot answer '
                'it — open the pane and choose there.\n\n'
            )
        if self.ctx.worktrees.write_run_artifact(
            self.ctx.run_id, path, header + text
        ):
            return path
        return None
