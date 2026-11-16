"""Codex and agy credentials, checked or set up before a run."""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import click

from sbx_omnigent import agy, codex, pipeline


def agy_agent_names(config: pipeline.PipelineConfig) -> list[str]:
    """
    The pipeline's antigravity-native agent names, sorted.

    :param config: The parsed pipeline.
    :returns: Agent names bound to an agy harness (empty when none).
    """
    return sorted(
        name
        for name, a in config.agents.items()
        if a.harness in agy.AGY_HARNESSES
    )


def codex_agent_names(config: pipeline.PipelineConfig) -> list[str]:
    """
    The pipeline's codex-native agent names, sorted.

    :param config: The parsed pipeline.
    :returns: Agent names bound to a Codex harness (empty when none).
    """
    return sorted(
        name
        for name, a in config.agents.items()
        if a.harness in codex.CODEX_HARNESSES
    )


def preflight_codex_auth(
    config: pipeline.PipelineConfig,
    *,
    path: Path | None = None,
    now: datetime | None = None,
    echo: Callable[[str], None] = click.echo,
) -> None:
    """
    Refuse to start a Codex pipeline on a dead or dying credential.

    Deliberately runs BEFORE any microVM is provisioned. Codex access
    tokens last ~240 hours, so unlike agy there is no refresh daemon to
    lean on — the credential is checked once, up front, and a run that
    cannot finish is stopped before it costs anything.

    Two outcomes:

    * missing or expired -> refuse, naming the exact remedy
    * expiring within six hours -> WARN and continue, so a long campaign
      is not started on a token that dies partway through and fails
      every turn after it

    A pipeline with no Codex agents is a no-op.

    :param config: The parsed pipeline.
    :param path: Override for the host credential file (tests).
    :param now: Override for the clock (tests).
    :param echo: Output sink (injected in tests).
    :raises click.ClickException: If there is no usable credential.
    """
    names = codex_agent_names(config)
    if not names:
        return
    try:
        warning = codex.preflight(path=path, now=now)
    except codex.CodexAuthError as exc:
        raise click.ClickException(
            f'this pipeline has Codex agent(s) '
            f'({", ".join(names)}) but {exc}'
        ) from exc
    if warning:
        echo(f'[preflight] {warning}')


def preflight_codex_login(
    config: pipeline.PipelineConfig,
    *,
    skip: bool,
    echo: Callable[[str], None] = click.echo,
) -> None:
    """
    Refuse to start a Codex pipeline on a login the server rejects.

    :func:`preflight_codex_auth` reads the access token's expiry and
    cannot see a login the server has already rejected; on 2026-09-14 a
    dead refresh token passed it, and every Codex turn then failed as a
    startup timeout (#26). This runs ``codex doctor``'s authenticated
    handshake instead (see :func:`codex.probe_login`), before any
    microVM. A pipeline with no Codex agents is a no-op.

    :param config: The parsed pipeline.
    :param skip: ``--skip-codex-check``: say the login was not checked,
        and do not probe.
    :param echo: Output sink (injected in tests).
    :raises click.ClickException: If the handshake failed; the message
        names the agents, the re-login command and the skip flag.
    """
    names = codex_agent_names(config)
    if not names:
        return
    if skip:
        echo(
            f'[preflight] codex: {codex.SKIP_LOGIN_CHECK_FLAG}: the login '
            f'was not checked against the server.'
        )
        return
    try:
        warning = codex.probe_login()
    except codex.CodexAuthError as exc:
        raise click.ClickException(
            f'this pipeline has Codex agent(s) '
            f'({", ".join(names)}) but {exc}'
        ) from exc
    if warning:
        echo(f'[preflight] {warning}')


def ensure_agy_harvester(
    config: pipeline.PipelineConfig,
    *,
    auto: bool = True,
    stamp_path: Path | None = None,
    lock_path: Path | None = None,
    log_path: Path | None = None,
    deadline_s: float = 180.0,
    spawn: Callable[..., subprocess.Popen[bytes]] | None = None,
    wait: Callable[..., bool] | None = None,
    now: float | None = None,
) -> subprocess.Popen[bytes] | None:
    """
    Guarantee a fresh agy swap secret, starting the harvester if needed.

    An agy pipeline has a hidden prerequisite: a harvester loop must be
    running or every agy turn dies unauthenticated — silently, inside
    agy's own TUI, so the runner just blocks to the turn timeout. Making
    the human remember to background a second command is a footgun, so
    the runner starts one itself and stops it when the run ends.

    Skipped entirely for a pipeline with no agy agents, and a no-op when
    a harvester is already running and the secret is fresh (that loop
    owns refreshing). When one is running but the secret has gone stale,
    this waits for its next refresh rather than starting a competing one
    — only a single harvester may poke the box (see
    :func:`agy.acquire_harvest_lock`).

    The decision is "is a harvester RUNNING", never "is the secret fresh
    right now": a fresh secret with nothing refreshing it expires about
    an hour in, stranding every agy turn after that.

    :param config: The parsed pipeline.
    :param auto: Start one when missing; ``False`` restores the old
        behavior of refusing with instructions (for a harvester run on
        another host).
    :param stamp_path: Harvest stamp; ``None`` uses the default.
    :param lock_path: Harvester lock; ``None`` uses the default.
    :param log_path: Where an auto-started harvester logs.
    :param deadline_s: How long to wait for the first refresh.
    :param spawn: Launcher (injected in tests).
    :param wait: Freshness waiter (injected in tests).
    :param now: Epoch seconds to measure staleness from.
    :returns: The harvester this call started (the caller MUST stop it
        via :func:`agy.stop_harvester`), or ``None`` when it started
        none.
    :raises click.ClickException: If the secret cannot be made fresh.
    """
    if not agy_agent_names(config):
        return None
    # Gate on whether a harvester is RUNNING — NOT on whether the secret
    # happens to be fresh this second. Freshness at startup says nothing
    # about freshness 90 minutes in: an agy token lives about an hour,
    # and a run that starts no harvester because the secret looked fine
    # goes stale mid-flight. Every agy turn driven after that then hangs
    # silently to the turn timeout. Observed exactly that: last refresh
    # 23:32, the agy coder driven 00:53, dead at 1800s with an empty
    # session.
    running = agy.harvester_running(path=lock_path)
    age = agy.harvest_age_s(path=stamp_path, now=now)
    fresh = age is not None and age <= agy.MAX_SWAP_AGE_S
    proc: subprocess.Popen[bytes] | None = None
    if running:
        # Someone already owns refreshing for the whole run.
        if fresh:
            click.echo('[agy] a harvester is already running.')
            return None
        click.echo(
            '[agy] a harvester is running but the swap secret is stale '
            '— waiting for its next refresh.'
        )
    elif not auto:
        # Opted out: refuse when stale, else trust the caller that
        # something off-host keeps it fresh.
        preflight_agy(config, stamp_path=stamp_path, now=now)
        return None
    else:
        proc = (spawn or agy.spawn_harvester)(log_path=log_path)
        click.echo(
            f'[agy] started a token harvester (pid {proc.pid}); '
            f'logging to {log_path or agy.HARVEST_LOG}'
        )
    if fresh:
        # Already usable — start driving now; the harvester just
        # started keeps it fresh for the rest of the run.
        click.echo('[agy] swap secret is fresh.')
        return proc
    if not (wait or agy.wait_for_fresh_swap)(
        deadline_s=deadline_s, stamp_path=stamp_path
    ):
        agy.stop_harvester(proc)
        raise click.ClickException(
            f'the agy swap secret did not go fresh within '
            f'{deadline_s:.0f}s, so agent(s) '
            f'{", ".join(agy_agent_names(config))} would fail to '
            f'authenticate and hang until the turn timeout. Check the '
            f'harvester log ({log_path or agy.HARVEST_LOG}) — a trusted '
            f"box that cannot refresh needs 'agy /login' run on it."
        )
    click.echo('[agy] swap secret is fresh.')
    return proc


def preflight_agy(
    config: pipeline.PipelineConfig,
    *,
    stamp_path: Path | None = None,
    now: float | None = None,
) -> None:
    """
    Refuse to start an agy pipeline whose swap secret has gone stale.

    An agy agent VM carries only a PLACEHOLDER token; the sbx swap
    secret supplies the real one on the wire, and only a running
    harvester (``omni-sbx-agy harvest``) keeps that secret fresh. With
    no harvester the token expires and every agy turn dies
    unauthenticated — but agy reports that inside its own TUI, which the
    bridge never surfaces as a failed session, so the runner just blocks
    to the per-turn timeout (30 min) and then reports a misleading "did
    not complete" (observed live). Checking the harvest stamp up front
    turns the single most common agy failure into an actionable error in
    seconds, before one VM is provisioned. No-op for a pipeline with no
    agy agents.

    :param config: The parsed pipeline.
    :param stamp_path: Harvest stamp; ``None`` uses the default.
    :param now: Epoch seconds to measure from; ``None`` = the clock.
    :raises click.ClickException: When the pipeline declares an agy
        agent and the swap secret's freshness is unknown or too old.
    """
    agy_agents = agy_agent_names(config)
    if not agy_agents:
        return
    age = agy.harvest_age_s(path=stamp_path, now=now)
    if age is not None and age <= agy.MAX_SWAP_AGE_S:
        return
    when = (
        'has never been refreshed on this host'
        if age is None
        else f'was last refreshed {age / 60:.0f} min ago'
    )
    raise click.ClickException(
        f'pipeline {config.name!r} runs antigravity-native agent(s) '
        f'({", ".join(agy_agents)}), but the agy swap secret {when} '
        f'(stale after {agy.MAX_SWAP_AGE_S / 60:.0f} min). Its access '
        'token has almost certainly expired, so every agy turn would '
        'fail to authenticate and hang until the turn timeout.\n'
        '  Start the token harvester, then re-run:\n'
        '      omni-sbx-agy harvest         # always-on refresh loop '
        '(recommended)\n'
        '      omni-sbx-agy harvest --once  # one refresh, for a short '
        'run\n'
        '  Pass --skip-agy-check to run anyway (e.g. the harvester runs '
        'on another host).'
    )
