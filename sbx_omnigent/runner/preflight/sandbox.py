"""The sbx CLI and the pipeline image, ready before a run."""

from __future__ import annotations

import click

from sbx_omnigent import image, pipeline, sbx_cli


def ensure_pipeline_image(
    config: pipeline.PipelineConfig,
    worktree_root: str,
    egress_allow: tuple[str, ...],
) -> None:
    """
    Make sure the pipeline's image exists before any turn (#38).

    Built once per recipe: the template's name is a fingerprint of the
    recipe, so an unchanged one is reused and a changed one is a new
    build. First the server must be booting its agents from that same
    image; otherwise the agents and the gate would run on different
    toolchains, and that is refused before anything is built.

    :param config: The parsed pipeline; no ``image:`` does nothing.
    :param worktree_root: The runner's ``--worktree-root``, where the
        server published the image its agents boot.
    :param egress_allow: Hosts the build may reach: the gate's list.
    :raises click.ClickException: If the server boots another image,
        or the image cannot be checked for or built.
    """
    tag = pipeline.image_tag(config)
    if tag is None or config.image is None:
        return
    try:
        served = image.read_agent_image(worktree_root)
    except image.ImageError as exc:
        raise click.ClickException(str(exc)) from exc
    if served != tag:
        raise click.ClickException(
            f"the server's agents boot {served}, but this pipeline's "
            f'image is {tag}. The image: block changed after the server '
            f'started, or the server was not started with this file. '
            f'Restart the server with --pipeline pointing at this file.'
        )
    held = image.try_build_lock(worktree_root)
    if held is None:
        # The server (or another runner) is building it: wait for that
        # build rather than starting a second one.
        click.echo(f'[image] {tag} is being built elsewhere; waiting for it.')
        held = image.acquire_build_lock(worktree_root)
    try:
        if image.image_built(tag):
            image.release_build_lock(held)
            click.echo(f'[image] using {tag}')
            return
        click.echo(
            f'[image] building {tag} from {config.image.base}: runs '
            f'image.setup once per recipe (often 10-20 minutes); later '
            f'runs reuse it.'
        )
        image.ensure_image(
            tag=tag,
            spec=config.image,
            name=image.build_box_name(config.name),
            egress=egress_allow,
            worktree_root=worktree_root,
            held=held,
        )
    except image.ImageError as exc:
        raise click.ClickException(
            f'could not build the pipeline image: {exc}'
        ) from exc
    click.echo(f'[image] built {tag}')


def preflight_sbx() -> None:
    """
    Refuse to start unless the sbx daemon answers (#28).

    Runs before anything else touches sbx. A stuck daemon otherwise
    shows up minutes later as a pipeline turn failing with
    ``runner_unavailable``, because ``sbx create`` never returned. That
    error does not name sbx, so the cause had to be diagnosed by hand.

    :raises sbx_cli.SbxNotResponding: If ``sbx ls`` does not answer
        within :data:`sbx_cli.PROBE_TIMEOUT_S`. The message says how to
        recover the daemon.
    :raises click.ClickException: If ``sbx`` cannot be run, or its
        listing fails.
    """
    sbx_cli.require_responsive()
