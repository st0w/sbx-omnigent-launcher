"""The token a run publishes its pull requests with."""

from __future__ import annotations

import shlex
import subprocess
from pathlib import Path

from sbx_omnigent.runner.errors import PipelineRunError


def resolve_publish_token(
    token_file: str | None, token_command: str | None
) -> str | None:
    """
    Resolve the publish identity's token from a file or a command.

    Keeps the secret OFF the command line: the CLI carries a path or a
    retrieval command, never the token itself, so nothing sensitive
    reaches shell history or ``ps``. The command is run as argv (split
    with :func:`shlex.split`), NOT through a shell, and both options are
    CLI-only — deliberately never ``pipeline.yaml`` keys, since a token
    command in a shareable pipeline file would make merely running that
    file arbitrary code execution on the host.

    Any credential store works, which keeps this portable: a keychain
    read, ``pass``, ``vault``, a cloud secret manager, or a mode-0600
    file. The launcher never learns which.

    :param token_file: Path to a file containing ONLY the token.
    :param token_command: Command whose stdout is the token.
    :returns: The token, or ``None`` when neither is set — publish then
        uses whatever git/gh credentials the host already has.
    :raises PipelineRunError: If both are set, the file/command fails,
        or the resolved token is empty.
    """
    if token_file and token_command:
        raise PipelineRunError(
            'set only one of --publish-token-file / '
            '--publish-token-command'
        )
    if token_file:
        try:
            token = Path(token_file).read_text(encoding='utf-8')
        except OSError as exc:
            raise PipelineRunError(
                f'cannot read --publish-token-file {token_file!r}'
            ) from exc
    elif token_command:
        argv = shlex.split(token_command)
        if not argv:
            raise PipelineRunError('--publish-token-command is empty')
        try:
            proc = subprocess.run(
                argv, capture_output=True, text=True, check=False
            )
        except OSError as exc:
            raise PipelineRunError(
                f'--publish-token-command could not run {argv[0]!r}'
            ) from exc
        if proc.returncode != 0:
            raise PipelineRunError(
                f'--publish-token-command exited {proc.returncode}: '
                f'{proc.stderr.strip()}'
            )
        token = proc.stdout
    else:
        return None
    token = token.strip()
    if not token:
        raise PipelineRunError('the publish token resolved empty')
    return token
