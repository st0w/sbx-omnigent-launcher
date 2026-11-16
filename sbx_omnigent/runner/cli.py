"""The omni-sbx-pipeline command: preflight, then drive the run."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable

import click

from sbx_omnigent import agy, pipeline
from sbx_omnigent.defaults import DEFAULT_EGRESS_ALLOW
from sbx_omnigent.runner.agents import resolve_agent_ids
from sbx_omnigent.runner.preflight.disk import preflight_disk
from sbx_omnigent.runner.preflight.egress import (
    preflight_egress,
    read_operator_egress,
)
from sbx_omnigent.runner.preflight.harnesses import (
    ensure_agy_harvester,
    preflight_codex_auth,
    preflight_codex_login,
)
from sbx_omnigent.runner.preflight.publish import resolve_publish_token
from sbx_omnigent.runner.preflight.sandbox import (
    ensure_pipeline_image,
    preflight_sbx,
)
from sbx_omnigent.runner.resume import (
    _resume_worktree_count,
    reclaim_for_resume,
)
from sbx_omnigent.runner.run import PipelineRunner
from sbx_omnigent.swarm_session import SwarmSessionClient
from sbx_omnigent.worktrees import WorktreeManager


@click.command('pipeline')
@click.option(
    '-c',
    '--config',
    'config_path',
    required=True,
    type=click.Path(exists=True),
    help='Path to the pipeline.yaml.',
)
@click.option(
    '--server',
    envvar='OMNI_SERVER',
    default='http://localhost:6767',
    show_default=True,
)
@click.option(
    '--canonical-root', envvar='OMNI_SBX_CANONICAL_ROOT', required=True
)
@click.option(
    '--worktree-root', envvar='OMNI_SBX_WORKTREE_ROOT', required=True
)
@click.option('--run-id', default=None, help='Unique run id (default: name).')
@click.option(
    '--publish-repo', default=None, help='Push target (default: repo).'
)
@click.option(
    '--publish-token-file',
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help='File holding ONLY the token used to push and open PRs, so a '
    'dedicated pipeline identity ships the work instead of yours. '
    'Keeps the secret out of shell history and ps.',
)
@click.option(
    '--publish-token-command',
    default=None,
    help='Command whose stdout is that token (a keychain, pass, vault, '
    'or secret-manager read). Run as argv, never through a shell.',
)
@click.option(
    '--resume',
    is_flag=True,
    help='Continue the run with this --run-id instead of starting '
    'clean: reuse its hub, skip the stages it already finished, and '
    're-cut only what is left.',
)
@click.option(
    '--turn-timeout',
    type=float,
    default=None,
    help="Seconds one agent turn may take (overrides the pipeline's "
    'turn_timeout). Raise it when stages stop to ask you questions — '
    'that wait is spent inside the turn budget.',
)
@click.option(
    '--skip-disk-check',
    is_flag=True,
    help='Skip the preflight that refuses to start when free disk '
    'cannot cover the run (a host that fills mid-run leaves guest '
    'filesystems remounted read-only).',
)
@click.option(
    '--no-auto-harvest',
    is_flag=True,
    help='Do not start an agy token harvester for this run; refuse '
    'instead when the swap secret is stale (use when a harvester '
    'already runs elsewhere).',
)
@click.option('--keep', is_flag=True, help='Leave VMs + worktrees.')
@click.option(
    '--no-interactive-plan',
    is_flag=True,
    help='Do not block the planning phase on human approval (automated '
    "runs); use the planner's single-turn output as-is.",
)
@click.option(
    '--no-auto-approve',
    is_flag=True,
    help='Fail a turn that opens an interactive prompt instead of '
    'answering it. The default answers, because the microVM is the '
    'containment boundary — a prompt inside one is pure latency. Use '
    'this to watch what agents actually ask for.',
)
@click.option(
    '--skip-agy-check',
    is_flag=True,
    help='Skip the preflight that refuses to start an agy pipeline on a '
    'stale swap secret (use when the harvester runs on another host).',
)
@click.option(
    '--skip-codex-check',
    is_flag=True,
    help='Skip the check that the server accepts the host Codex login '
    '(the WebSocket handshake in `codex doctor`). For a network that '
    'blocks WebSockets but reaches Codex over HTTPS. An expired access '
    'token is still refused.',
)
def main(
    config_path: str,
    server: str,
    canonical_root: str,
    worktree_root: str,
    run_id: str | None,
    publish_repo: str | None,
    publish_token_file: str | None,
    publish_token_command: str | None,
    resume: bool,
    turn_timeout: float | None,
    skip_disk_check: bool,
    no_auto_harvest: bool,
    keep: bool,
    no_interactive_plan: bool,
    no_auto_approve: bool,
    skip_agy_check: bool,
    skip_codex_check: bool,
) -> None:
    """Fire a pipeline.yaml; provision-only when it has no task."""
    config = pipeline.load_pipeline(config_path)

    def publish_token_provider() -> str | None:
        return resolve_publish_token(
            publish_token_file, publish_token_command
        )

    # Resolve ONCE now to validate: a bad path or a failing keychain
    # read should cost two seconds, not a finished module. The VALUE is
    # deliberately discarded — publish re-reads it at push time, hours
    # later, because a token captured here can be rotated or expired by
    # then and a run that publishes at the end has no way to notice.
    publish_token_provider()
    # A config problem, so it refuses in seconds, before sbx is asked.
    operator_egress = read_operator_egress(config, worktree_root)
    # Before anything touches sbx, including the resume reclaim below,
    # which disposes VMs through the server.
    preflight_sbx()
    on_disk = 0
    if resume:
        # BEFORE the gate, not inside runner.run() behind it: a resume
        # was refused by space the resume itself would have freed
        # seconds later.
        resumed = run_id or config.name
        manager = WorktreeManager(
            canonical_root=canonical_root,
            worktree_root=worktree_root,
            default_branch=config.base_branch or 'main',
        )
        reclaim_for_resume(
            run_id=resumed,
            canonical_root=canonical_root,
            worktree_root=worktree_root,
            server=server,
            keep=keep,
            default_branch=config.base_branch or 'main',
            manager=manager,
            config=config,
        )
        # Counted AFTER the reclaim, from what is left, rather than
        # counted before and adjusted by what the reclaim reports.
        on_disk = _resume_worktree_count(
            worktree_root, resumed, manager.read_run_state(resumed)
        )
    if not skip_disk_check:
        # A host that fills mid-run corrupts the VMs it is hosting, so
        # refuse here rather than discover it as read-only filesystems.
        preflight_disk(config, keep=keep, worktrees_on_disk=max(0, on_disk))
    # Before ANY VM is provisioned: a Codex pipeline on a dead token
    # would otherwise fail every turn, minutes and several microVMs in.
    preflight_codex_auth(config)
    preflight_codex_login(config, skip=skip_codex_check)
    if operator_egress is not None:
        # Last, because it boots a VM: a host the setup or gate needs
        # is found missing here, not after every review has passed.
        preflight_egress(
            config, operator_egress, run_id=run_id or config.name
        )
    # After the egress check, whose hosts the build needs; before the
    # harvester and the run, so a failed build costs no turn.
    ensure_pipeline_image(
        config,
        worktree_root,
        DEFAULT_EGRESS_ALLOW if operator_egress is None else operator_egress,
    )
    harvester: subprocess.Popen[bytes] | None = None
    if not skip_agy_check:
        # An agy pipeline needs a live harvester or every agy turn dies
        # unauthenticated. Start one (and stop it at the end) rather
        # than making the human remember a second background command.
        harvester = ensure_agy_harvester(
            config, auto=not no_auto_harvest
        )
    try:
        _drive(
            config,
            resume=resume,
            turn_timeout=turn_timeout,
            server=server,
            canonical_root=canonical_root,
            worktree_root=worktree_root,
            run_id=run_id,
            publish_repo=publish_repo,
            publish_token=publish_token_provider,
            keep=keep,
            no_interactive_plan=no_interactive_plan,
            no_auto_approve=no_auto_approve,
            gate_egress=operator_egress,
        )
    except KeyboardInterrupt:
        interrupted = True
    else:
        interrupted = False
    finally:
        agy.stop_harvester(harvester)
    if interrupted:
        # Teardown has run and the harvester is stopped. A worker of a
        # parallel stage can still be inside a turn's HTTP wait, and the
        # interpreter would join it before exiting, which is the wait
        # Ctrl-C is meant to skip.
        click.echo(
            '[interrupt] stopped. --resume continues from the last '
            'completed stage.',
            err=True,
        )
        _exit_now(130)


def _exit_now(code: int) -> None:
    """
    End the process without waiting for any thread.

    :param code: The exit status; 130 for Ctrl-C, by shell convention.
    """
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


def _worktree_manager(
    config: pipeline.PipelineConfig,
    *,
    canonical_root: str,
    worktree_root: str,
    publish_token: str | Callable[[], str | None] | None,
) -> WorktreeManager:
    """
    The host-side git manager for one run.

    :param config: The parsed pipeline.
    :param canonical_root: Host dir holding the mirrors.
    :param worktree_root: Host dir holding the run's worktrees.
    :param publish_token: The publish identity's token, or a reader.
    :returns: The manager.
    """
    return WorktreeManager(
        canonical_root=canonical_root,
        worktree_root=worktree_root,
        default_branch=config.base_branch or 'main',
        publish_token=publish_token,
    )


def _drive(
    config: pipeline.PipelineConfig,
    *,
    resume: bool = False,
    turn_timeout: float | None = None,
    server: str,
    canonical_root: str,
    worktree_root: str,
    run_id: str | None,
    publish_repo: str | None,
    publish_token: str | Callable[[], str | None] | None,
    keep: bool,
    no_interactive_plan: bool,
    no_auto_approve: bool,
    gate_egress: tuple[str, ...] | None = None,
) -> None:
    """Build the runner, drive the run, and report its outcome."""
    client = SwarmSessionClient(server, auto_approve=not no_auto_approve)
    ids = resolve_agent_ids(config, client.list_builtin_agents())
    runner = PipelineRunner(
        config,
        session_client=client,
        worktree_manager=_worktree_manager(
            config,
            canonical_root=canonical_root,
            worktree_root=worktree_root,
            publish_token=publish_token,
        ),
        run_id=run_id or config.name,
        agent_ids=ids,
        publish_repo=publish_repo,
        keep=keep,
        interactive_plan=not no_interactive_plan,
        resume=resume,
        gate_egress=gate_egress,
        **(
            {'turn_timeout': turn_timeout}
            if turn_timeout is not None
            else {'turn_timeout': config.turn_timeout}
            if config.turn_timeout is not None
            else {}
        ),
    )
    result = runner.run()
    click.echo(f'status: {result.status}')
    if result.status == 'provisioned':
        click.echo(
            'provisioned (VMs kept up; drive via the UI or '
            'omni-sbx-swarm send):'
        )
        for b in result.bindings:
            click.echo(
                f"  {b['node']} [{b['mode']}] agent={b['agent']} "
                f"session={b['session']}"
            )
        return
    if result.blocked_stage:
        click.echo(f'blocked at: {result.blocked_stage}')
    if result.published:
        click.echo(f'published: {result.published}')
