"""The harness-version record: which CLI versions each session ran.

A native harness can update itself mid-session, and a run that wedges
is far easier to diagnose against the versions it actually ran. Each
node's first reading is kept, persisted with the run state, and
compared with what later sessions read (#17).
"""

from __future__ import annotations

from collections.abc import Callable

import click

from sbx_omnigent import harness_versions
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.sandboxes import sandbox_for
from sbx_omnigent.runner.session_registry import SessionRegistry


class HarnessRecord:
    """
    Keep each node's harness CLI versions, and say when one moved.

    :param ctx: The run.
    :param registry: The run's sessions, for each one's node and agent.
    :param save_state: Persists the run's state after a first reading.

    :ivar recorded: CLI versions read from each session's VM, keyed by
        session label: ``{'runs': cli, 'versions': {cli: version}}``.
        Persisted, so a wedged run can still be compared with the
        known-good set after its VMs are gone (#17).
    """

    def __init__(
        self,
        ctx: RunContext,
        *,
        registry: SessionRegistry,
        save_state: Callable[[], None],
    ) -> None:
        self.ctx = ctx
        self._registry = registry
        self._save_state = save_state
        self.recorded: dict[str, dict[str, object]] = {}
        #: CLIs already reported as off the known-good set, THIS
        #: PROCESS: once per CLI, not once per session.
        self._reported: set[str] = set()

    def record_first_turn(self, session: str) -> None:
        """
        Record the CLI versions in *session*'s VM after its first turn.

        One ``sbx exec`` per session; see
        :meth:`note`.

        :param session: The session whose first turn just completed.
        """
        sandbox = sandbox_for(self.ctx.sessions, session)
        if sandbox is None:
            return
        self.note(
            session, read_versions(sandbox)
        )

    def note(
        self, session: str, versions: dict[str, str]
    ) -> None:
        """
        Keep *session*'s versions and say when one has moved.

        Only the CLI the session drives is compared, against:

        * the version this session recorded at its first turn, when it
          has one: the CLI updated itself mid-session;
        * otherwise an earlier session in this run driving the same CLI,
          including an earlier session of this same node: a retry's
          dead VM, or the previous attempt a resume restored;
        * the known-good set, once per CLI per process.

        Readings are kept per node, so a node's new session replaces its
        earlier one's. Each carries the session it came from, in memory
        only: a resume restores readings that belong to no live session,
        and comparing a new VM with one of those as "its first turn"
        called a pinned VM a mid-session self-update.

        WARNS, never raises, and the first reading for a session is the
        one kept.

        :param session: The session the versions were read from.
        :param versions: ``{cli: version}`` from its VM.
        """
        agent = self._registry.agents.get(session)
        if agent is None or not versions:
            return
        label = self._registry.label_of(session)
        cli = harness_versions.cli_for_harness(agent.harness)
        with self.ctx.lock:
            recorded = self.recorded.get(label)
            first = (
                recorded
                if recorded is not None
                and recorded.get('session') == session
                else None
            )
            warnings = self._warnings(
                label, cli, versions.get(cli), first
            )
            if first is None:
                self.recorded[label] = {
                    'runs': cli,
                    'versions': dict(versions),
                    'session': session,
                }
        for warning in warnings:
            click.echo(f'[versions] {warning}')
        if first is None:
            self._save_state()

    def _warnings(
        self,
        label: str,
        cli: str,
        now: str | None,
        first: dict[str, object] | None,
    ) -> list[str]:
        """
        What to say about *cli* at version *now* on *label*.

        Called under ``self.ctx.lock``.

        :param label: The session's label.
        :param cli: The CLI the session drives.
        :param now: Its version as just read, or ``None``.
        :param first: The session's own earlier record, if any.
        :returns: The warnings, possibly none.
        """
        if now is None:
            return []
        if first is not None:
            before = _recorded_version(first, cli)
            if before is None or before == now:
                return []
            return [
                f'{label}: {cli} {now} is installed now, but it ran '
                f'{before} at its first turn. It updated itself '
                f'mid-session.'
            ]
        warnings: list[str] = []
        for other, entry in self.recorded.items():
            before = _recorded_version(entry, cli)
            if entry.get('runs') == cli and before not in (None, now):
                ran = (
                    f'an earlier session of {label}' if other == label
                    else other
                )
                warnings.append(
                    f'{label}: {cli} {now}, but {ran} ran {before} '
                    f'earlier in this run.'
                )
                break
        known = harness_versions.KNOWN_GOOD.get(cli)
        if known not in (None, now) and cli not in self._reported:
            self._reported.add(cli)
            warnings.append(
                f'{cli} {now} is not the known-good {known} recorded '
                f'{harness_versions.KNOWN_GOOD_RECORDED}. If this run '
                f'misbehaves, compare against docs/HARNESS-VERSIONS.md.'
            )
        return warnings


def read_versions(sandbox: str) -> dict[str, str]:
    """
    Read *sandbox*'s harness CLI versions; never raises.

    :param sandbox: The microVM name.
    :returns: ``{cli: version}``, empty when nothing could be read.
    """
    # read_versions never raises by contract. Guarded anyway: the
    # record is for later comparison, and must never fail a run.
    try:
        return harness_versions.read_versions(sandbox)
    except Exception:
        return {}


def _recorded_version(entry: dict[str, object], cli: str) -> str | None:
    """
    The version of *cli* in one recorded harness-versions entry.

    :param entry: ``{'runs': cli, 'versions': {cli: version}}``.
    :param cli: The CLI to look up.
    :returns: Its version, or ``None`` when the entry has none.
    """
    versions = entry.get('versions')
    if not isinstance(versions, dict):
        return None
    version = versions.get(cli)
    return version if isinstance(version, str) else None
