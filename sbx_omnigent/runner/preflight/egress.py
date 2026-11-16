"""The operator's egress allowlist, checked before any VM starts."""

from __future__ import annotations

import click

from sbx_omnigent import egress, pipeline
from sbx_omnigent.defaults import DEFAULT_HOST_IMAGE


def read_operator_egress(
    config: pipeline.PipelineConfig, worktree_root: str
) -> tuple[str, ...] | None:
    """
    Read the operator's allowlist, for the verify gate and ``egress:``.

    The runner is a separate process from the server and never saw its
    config, so the gate got the built-in default whatever the operator
    had set, including ``[]`` (#41). The server publishes its resolved
    ``sandbox.sbx.egress_allow`` at startup (see
    :func:`sbx_omnigent.egress.publish_policy`).

    :param config: The parsed pipeline.
    :param worktree_root: The runner's ``--worktree-root``, which must
        be the server's ``sbx.worktree_root``.
    :returns: The operator's allowlist, or ``None`` when the pipeline
        has no ``verify:`` gate to give it to, no ``egress:`` hosts to
        check against it, and no ``image:`` to build with it.
    :raises click.ClickException: If the list is missing or unreadable:
        a gate run on a guessed allowlist is what this replaces.
    """
    if config.verify is None and not config.egress and config.image is None:
        return None
    try:
        return egress.read_policy(worktree_root)
    except egress.PolicyError as exc:
        raise click.ClickException(str(exc)) from exc


def preflight_egress(
    config: pipeline.PipelineConfig,
    allowlist: tuple[str, ...],
    *,
    run_id: str,
) -> None:
    """
    Refuse a run whose VMs cannot reach a host it declares.

    Every gap entry in the default allowlist was added after a live run
    failed mid-campaign for want of it (#41). A pipeline that declares
    ``egress:`` has those hosts checked by sbx's own authorizer, in a
    short-lived sandbox scoped like the gate, before any turn.

    :param config: The parsed pipeline; no ``egress:`` checks nothing.
    :param allowlist: The operator's resolved allowlist.
    :param run_id: The run id, for the check sandbox's name.
    :raises click.ClickException: If a host is denied, or the check
        cannot run.
    """
    if not config.egress:
        return
    n = len(config.egress)
    click.echo(
        f'[egress] checking {n} declared host(s) in a short-lived '
        f"sandbox scoped like this run's VMs..."
    )
    # The pipeline image's base, not the image: it may not be built yet,
    # and the build itself needs these hosts.
    image_ref = (
        (config.image.base if config.image else None)
        or (config.verify.image if config.verify else None)
        or DEFAULT_HOST_IMAGE
    )
    try:
        verdicts = egress.check_declared_hosts(
            name=egress.check_box_name(run_id),
            hosts=config.egress,
            allowlist=allowlist,
            image=image_ref,
        )
    except egress.EgressCheckError as exc:
        raise click.ClickException(
            f'could not check the declared egress hosts: {exc}'
        ) from exc
    denied = [v for v in verdicts if not v.allowed]
    if denied:
        lines = '\n'.join(f'  - {v.host}: {v.reason}' for v in denied)
        raise click.ClickException(
            f'{len(denied)} of {n} declared egress host(s) are not '
            f"allowed for this run's VMs:\n{lines}\n"
            f"Add them to 'sandbox.sbx.egress_allow' in the server "
            f'config and restart the server (a non-empty list replaces '
            f'the default, so keep the default entries you still need), '
            f'or drop them from egress: if the pipeline no longer needs '
            f'them.'
        )
    click.echo(f'[egress] all {n} declared host(s) are allowed.')
