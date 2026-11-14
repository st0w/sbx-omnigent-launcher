"""Build a pipeline's toolchain image into a local sbx template (#38).

Without one, every coder, reviewer and gate sandbox installs the same
toolchain at run time: on a live Rust campaign the gate's setup alone
took 12 min 52 s, on every attempt. A pipeline's ``image:`` block names
a base image and a setup; this module runs that setup once, in a
throwaway sandbox, and saves the result as a local template. Booting a
sandbox from it takes about a second.

The build: create a sandbox from the base with nothing mounted, scope
its egress like the gate's, run the setup, stop the sandbox (sbx saves
only a stopped one), save it as the template, and always remove it.
The template's name comes from :func:`sbx_omnigent.pipeline.image_tag`,
so a changed recipe is a new name and an unchanged one is never rebuilt.
"""

from __future__ import annotations

import fcntl
import json
import os
import secrets
import subprocess
import tempfile
import time
from typing import IO

import click

from sbx_omnigent import sbx_cli
from sbx_omnigent.pipeline import ImageSpec, _sanitize
from sbx_omnigent.verify import clamp_lines, salient_tail, scrub

#: ``sbx template save``: a spike saved a 3 GB sandbox in 145 s, so a
#: management budget would call a healthy save a stuck daemon.
SAVE_TIMEOUT_S = 1800.0

#: sbx's limit on a sandbox name.
_NAME_MAX = 56

#: How much of a failed setup's output the error carries.
_OUTPUT_TAIL = 6000

#: Where the server publishes the image its agents boot, in its
#: ``sbx.worktree_root``, for the runner to compare with its own.
AGENT_IMAGE_FILE = '.sbx-omnigent-image.json'

#: Bumped when the file's shape changes.
_AGENT_IMAGE_VERSION = 1

#: Held, exclusively, by whichever process is building the image: the
#: server or a runner. The other waits on it rather than building twice,
#: and a session booting the image waits on it rather than failing.
BUILD_LOCK_FILE = '.sbx-omnigent-image.lock'

#: Why the last build failed, so a waiting session can say so.
BUILD_FAILURE_FILE = '.sbx-omnigent-image-failure.json'

#: What every pipeline image's name starts with (see
#: :func:`sbx_omnigent.pipeline.image_tag`).
_PIPELINE_IMAGE_PREFIX = 'localhost/omni-sbx-'

#: How long a session waits for a build in progress: the setup's
#: default timeout plus the save.
WAIT_TIMEOUT_S = 3600.0 + SAVE_TIMEOUT_S


class ImageError(Exception):
    """The pipeline image could not be checked for or built."""


def publish_agent_image(worktree_root: str, agent_image: str) -> None:
    """
    Write the image the server's agents boot, for the runner to read.

    The runner builds the pipeline image, and the server boots agents
    from whatever it was started with; they are separate processes. A
    pipeline whose recipe changed after the server started would
    otherwise run its agents on the old image and its gate on the new
    one. Written to a temporary file and renamed into place, so a
    runner never reads it half-written. Readable by the owner only.

    :param worktree_root: The server's ``sbx.worktree_root``; created
        if it does not exist yet.
    :param agent_image: The image every agent sandbox boots.
    :raises OSError: If the file cannot be written.
    """
    os.makedirs(worktree_root, exist_ok=True)
    fd, staged = tempfile.mkstemp(
        dir=worktree_root, prefix=f'{AGENT_IMAGE_FILE}.'
    )
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            json.dump(
                {
                    'version': _AGENT_IMAGE_VERSION,
                    'agent_image': agent_image,
                },
                fh,
            )
        os.replace(staged, os.path.join(worktree_root, AGENT_IMAGE_FILE))
    except BaseException:
        if os.path.exists(staged):
            os.unlink(staged)
        raise


def read_agent_image(worktree_root: str) -> str:
    """
    Read the image the server said its agents boot.

    :param worktree_root: The runner's ``--worktree-root``.
    :returns: The image reference.
    :raises ImageError: If the file is missing or malformed.
    """
    path = os.path.join(worktree_root, AGENT_IMAGE_FILE)
    try:
        with open(path, encoding='utf-8') as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        raise ImageError(
            f'the server has not published the image its agents boot to '
            f'{path}. Restart the server (omni-sbx server --pipeline '
            f"<this pipeline file>) so it writes one, and check that "
            f"'sandbox.sbx.worktree_root' in its config is the same "
            f'directory as --worktree-root.'
        ) from None
    except (OSError, ValueError) as exc:
        raise ImageError(f'cannot read {path}: {exc}') from exc
    agent_image = raw.get('agent_image') if isinstance(raw, dict) else None
    if (
        not isinstance(raw, dict)
        or raw.get('version') != _AGENT_IMAGE_VERSION
        or not isinstance(agent_image, str)
        or not agent_image
    ):
        raise ImageError(
            f'{path} is not an agent image this runner can read; restart '
            f'the server so it writes a current one.'
        )
    return agent_image


def try_build_lock(worktree_root: str) -> IO[str] | None:
    """
    Take the build lock if nobody holds it.

    :param worktree_root: The server's ``sbx.worktree_root``.
    :returns: The open lock file, held until
        :func:`release_build_lock`; ``None`` when another process holds
        it.
    """
    handle = _open_lock(worktree_root)
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def acquire_build_lock(worktree_root: str) -> IO[str]:
    """
    Take the build lock, waiting for whoever holds it.

    :param worktree_root: The server's ``sbx.worktree_root``.
    :returns: The open lock file, held until :func:`release_build_lock`.
    """
    handle = _open_lock(worktree_root)
    fcntl.flock(handle, fcntl.LOCK_EX)
    return handle


def is_pipeline_image(ref: str) -> bool:
    """
    Whether *ref* names a pipeline image this module builds.

    :param ref: A template reference.
    :returns: ``True`` for
        ``localhost/omni-sbx-<pipeline>:<fingerprint>``.
    """
    return ref.startswith(_PIPELINE_IMAGE_PREFIX)


def release_build_lock(handle: IO[str]) -> None:
    """
    Release a lock from :func:`try_build_lock`.

    :param handle: The open lock file.
    """
    try:
        fcntl.flock(handle, fcntl.LOCK_UN)
    finally:
        handle.close()


def _open_lock(worktree_root: str) -> IO[str]:
    os.makedirs(worktree_root, exist_ok=True)
    # Held across calls and closed by release_build_lock, so not a
    # `with` block.
    return open(
        os.path.join(worktree_root, BUILD_LOCK_FILE), 'a', encoding='utf-8'
    )


def ensure_image(
    *,
    tag: str,
    spec: ImageSpec,
    name: str,
    egress: tuple[str, ...],
    worktree_root: str,
    held: IO[str] | None = None,
) -> bool:
    """
    Build *tag* unless it exists, one builder at a time.

    Waits for the build lock (or uses *held*, already taken), then
    checks again: whoever held it may have just built the image. A
    failed build is recorded for :func:`wait_for_image` to report, and a
    successful one clears that record.

    :param tag: The template name.
    :param spec: The pipeline's ``image:`` block.
    :param name: The build sandbox's name, from :func:`build_box_name`.
    :param egress: Hosts the build may reach.
    :param worktree_root: Where the lock and failure record live.
    :param held: The lock, when the caller already holds it; released
        here either way.
    :returns: ``True`` when this call built it, ``False`` when it was
        already there.
    :raises ImageError: If the build fails.
    """
    handle = held if held is not None else acquire_build_lock(worktree_root)
    try:
        if image_built(tag):
            _clear_failure(worktree_root)
            return False
        try:
            build_image(tag=tag, spec=spec, name=name, egress=egress)
        except ImageError as exc:
            _record_failure(worktree_root, tag, str(exc))
            raise
        _clear_failure(worktree_root)
        return True
    finally:
        release_build_lock(handle)


def wait_for_image(
    tag: str,
    worktree_root: str,
    *,
    timeout_s: float,
    poll_s: float = 5.0,
) -> None:
    """
    Return once *tag* exists, waiting out a build in progress.

    :param tag: The template a session is about to boot.
    :param worktree_root: Where the lock and failure record live.
    :param timeout_s: How long to wait for a build in progress.
    :param poll_s: Seconds between looks at the lock.
    :raises ImageError: If the build failed, no build is running and
        the image is missing, or the build is still running at
        *timeout_s*.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        if image_built(tag):
            return
        handle = try_build_lock(worktree_root)
        if handle is not None:
            release_build_lock(handle)
            if image_built(tag):
                return
            failure = _read_failure(worktree_root, tag)
            if failure is not None:
                raise ImageError(
                    f'the pipeline image {tag} failed to build: {failure}'
                )
            raise ImageError(
                f'the pipeline image {tag} is not built, and nothing is '
                f'building it. Restart the server (omni-sbx server '
                f'--pipeline <file>) so it builds it.'
            )
        if time.monotonic() >= deadline:
            raise ImageError(
                f'the pipeline image {tag} is still building after '
                f'{timeout_s:.0f}s.'
            )
        time.sleep(poll_s)


def _record_failure(worktree_root: str, tag: str, reason: str) -> None:
    path = os.path.join(worktree_root, BUILD_FAILURE_FILE)
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump({'tag': tag, 'reason': reason}, fh)


def _clear_failure(worktree_root: str) -> None:
    try:
        os.unlink(os.path.join(worktree_root, BUILD_FAILURE_FILE))
    except FileNotFoundError:
        pass


def _read_failure(worktree_root: str, tag: str) -> str | None:
    try:
        with open(
            os.path.join(worktree_root, BUILD_FAILURE_FILE), encoding='utf-8'
        ) as fh:
            raw = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict) or raw.get('tag') != tag:
        return None
    reason = raw.get('reason')
    return reason if isinstance(reason, str) else None


def build_box_name(pipeline_name: str) -> str:
    """
    A unique sbx name for one image build's sandbox.

    Random-suffixed, so a box a killed build left behind never blocks
    the next one.

    :param pipeline_name: The pipeline's name.
    :returns: e.g. ``"image-build-discover-3fa2c1"``.
    """
    suffix = secrets.token_hex(3)
    stem = f'image-build-{_sanitize(pipeline_name)}'
    return f'{stem[: _NAME_MAX - len(suffix) - 1].rstrip("-")}-{suffix}'


def image_built(tag: str, *, runner: sbx_cli.Runner | None = None) -> bool:
    """
    Whether sbx's local template store holds *tag*.

    :param tag: The full template name, e.g.
        ``localhost/omni-sbx-demo:0123456789abcdef``.
    :param runner: Subprocess runner (injected in tests).
    :returns: ``True`` when the template exists.
    :raises ImageError: If sbx cannot list its templates, or its
        listing cannot be read.
    """
    proc = _sbx(
        ['sbx', 'template', 'ls', '--json'],
        timeout_s=sbx_cli.MANAGE_TIMEOUT_S,
        failure='could not list the sbx templates',
        runner=runner,
    )
    try:
        listing = json.loads(proc.stdout or '')
    except ValueError as exc:
        raise ImageError(
            f'could not read the sbx template listing: {exc}'
        ) from exc
    if not isinstance(listing, dict):
        raise ImageError('could not read the sbx template listing')
    images = listing.get('images') or []
    if not isinstance(images, list):
        raise ImageError('could not read the sbx template listing')
    for entry in images:
        if not isinstance(entry, dict):
            raise ImageError('could not read the sbx template listing')
        if f'{entry.get("repository")}:{entry.get("tag")}' == tag:
            return True
    return False


def build_image(
    *,
    tag: str,
    spec: ImageSpec,
    name: str,
    egress: tuple[str, ...],
    runner: sbx_cli.Runner | None = None,
) -> None:
    """
    Build *spec* into the local template *tag*.

    :param tag: The template name to save as.
    :param spec: The pipeline's ``image:`` block.
    :param name: The build sandbox's name, from :func:`build_box_name`.
    :param egress: Hosts the build may reach; the gate's list, so the
        setup can reach what ``verify.setup`` could. Empty adds no rule.
    :param runner: Subprocess runner (injected in tests).
    :raises ImageError: If any step fails; nothing is saved, and the
        build sandbox is removed.
    """
    create = [
        'sbx', 'create', 'shell', '--template', spec.base,
        '--name', name, '--quiet',
    ]
    if spec.cpus is not None:
        create += ['--cpus', str(spec.cpus)]
    if spec.memory:
        create += ['--memory', spec.memory]
    try:
        _sbx(
            create,
            timeout_s=sbx_cli.CREATE_TIMEOUT_S,
            failure=f'could not create the image build sandbox {name!r}',
            runner=runner,
        )
        if egress:
            _sbx(
                [
                    'sbx', 'policy', 'allow', 'network',
                    '--sandbox', name, ','.join(egress),
                ],
                timeout_s=sbx_cli.MANAGE_TIMEOUT_S,
                failure=f'could not scope egress for {name!r}',
                runner=runner,
            )
        _run_setup(name, spec, runner)
        _sbx(
            ['sbx', 'stop', name],
            timeout_s=sbx_cli.MANAGE_TIMEOUT_S,
            failure=f'could not stop the image build sandbox {name!r}',
            runner=runner,
        )
        _sbx(
            ['sbx', 'template', 'save', name, tag],
            timeout_s=SAVE_TIMEOUT_S,
            failure=f'could not save the image as {tag!r}',
            runner=runner,
        )
    finally:
        _remove(name, runner)


def _run_setup(
    name: str, spec: ImageSpec, runner: sbx_cli.Runner | None
) -> None:
    """
    Run ``image.setup`` in the build sandbox, or raise.

    :param name: The build sandbox.
    :param spec: The image block.
    :param runner: Subprocess runner (injected in tests).
    :raises ImageError: If the setup exits non-zero or runs past its
        timeout.
    """
    try:
        proc = sbx_cli.run(
            ['sbx', 'exec', name, '--', 'sh', '-c', f'set -e\n{spec.setup}'],
            timeout_s=spec.timeout_s,
            runner=runner,
        )
    except sbx_cli.SbxNotResponding as exc:
        # The setup is the project's program, not a daemon call: past
        # its budget, it is the setup that is too slow or stuck.
        raise ImageError(
            f'image.setup did not finish within {spec.timeout_s:.0f}s; '
            f'raise image.timeout if it needs longer'
        ) from exc
    except OSError as exc:
        raise ImageError(f'could not run image.setup: {exc}') from exc
    if proc.returncode != 0:
        combined = ((proc.stdout or '') + (proc.stderr or '')).strip()
        raise ImageError(
            f'image.setup failed (exit {proc.returncode}); nothing was '
            f'saved. Its output:\n'
            + salient_tail(clamp_lines(scrub(combined)), _OUTPUT_TAIL)
        )


def _sbx(
    argv: list[str],
    *,
    timeout_s: float,
    failure: str,
    runner: sbx_cli.Runner | None,
) -> subprocess.CompletedProcess[str]:
    """
    Run one ``sbx`` management call for the build, or raise.

    :param argv: The ``sbx`` argv.
    :param timeout_s: Budget in seconds.
    :param failure: What could not be done; the error starts with it.
    :param runner: Subprocess runner (injected in tests).
    :returns: The finished process.
    :raises ImageError: If it hangs, cannot run, or exits non-zero.
    """
    try:
        proc = sbx_cli.run(argv, timeout_s=timeout_s, runner=runner)
    except (sbx_cli.SbxNotResponding, OSError) as exc:
        raise ImageError(f'{failure}: {exc}') from exc
    if proc.returncode != 0:
        raise ImageError(
            f'{failure} (rc={proc.returncode}): '
            f'{(proc.stderr or "").strip()}'
        )
    return proc


def _remove(name: str, runner: sbx_cli.Runner | None) -> None:
    """
    Remove the build sandbox, saying so if it cannot be removed.

    :param name: The build sandbox.
    :param runner: Subprocess runner (injected in tests).
    """
    try:
        proc = sbx_cli.run(
            ['sbx', 'rm', '--force', name],
            timeout_s=sbx_cli.MANAGE_TIMEOUT_S,
            runner=runner,
        )
        problem = (
            f'rc={proc.returncode}: {(proc.stderr or "").strip()}'
            if proc.returncode != 0
            else None
        )
    except (sbx_cli.SbxNotResponding, OSError) as exc:
        problem = str(exc)
    if problem is not None:
        click.echo(
            f'[image] WARNING: could not remove the build sandbox '
            f'{name!r} ({problem}). Remove it with: sbx rm --force {name}',
            err=True,
        )
