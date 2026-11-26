"""The pipeline runner's one client for the Omnigent server.

Built from the command's options: who to share each session with, and,
on a server with accounts, the machine client to sign in as. Checked
while the arguments are parsed or before any preflight, so a mistake in
either costs seconds.
"""

from __future__ import annotations

from pathlib import Path

import click

from sbx_omnigent.server_auth import (
    BearerTransport,
    MachineClient,
    MachineToken,
)
from sbx_omnigent.swarm_session import (
    SwarmSessionClient,
    Transport,
    UrllibTransport,
    check_account_name,
)


def share_target(
    ctx: click.Context, param: click.Parameter, value: str | None
) -> str | None:
    """
    Check ``--share-with`` while the arguments are parsed.

    :param ctx: The click context.
    :param param: The option.
    :param value: The account name, or ``None``.
    :returns: *value*.
    :raises click.BadParameter: For a name Omnigent would not accept.
    """
    if value is None:
        return None
    try:
        return check_account_name(value)
    except ValueError as exc:
        raise click.BadParameter(str(exc)) from exc


def machine_client(
    client_id: str | None, secret_file: str | None
) -> MachineClient | None:
    """
    The machine client the two options name, if they name one.

    :param client_id: ``--machine-client-id``, or ``None``.
    :param secret_file: ``--machine-secret-file``, or ``None``.
    :returns: The client, or ``None`` when neither is given.
    :raises click.UsageError: When only one is given, or the id is
        blank.
    """
    if client_id is None and secret_file is None:
        return None
    if client_id is None or secret_file is None:
        raise click.UsageError(
            '--machine-client-id and --machine-secret-file go together: '
            'give both to sign in to a server with accounts, or neither'
        )
    try:
        return MachineClient(client_id, Path(secret_file))
    except ValueError as exc:
        raise click.UsageError(str(exc)) from exc


def session_client(
    server: str,
    *,
    machine: MachineClient | None,
    share_with: str | None,
    auto_approve: bool,
) -> SwarmSessionClient:
    """
    The run's one client for the Omnigent server.

    With a machine client, it signs in here, before any preflight: a
    refused client then fails in seconds, rather than after the egress
    check has booted a VM. Every request carries the token from then
    on (see :mod:`sbx_omnigent.server_auth`). Without one, nothing is
    sent, which is all a server without accounts needs.

    :param server: The server URL.
    :param machine: The machine client to sign in as, or ``None``.
    :param share_with: Account to share each session with, or ``None``.
    :param auto_approve: Answer interactive prompts in a turn.
    :returns: The client.
    :raises MachineSignInFailed: When the server refuses the machine
        client, with what to fix.
    """
    inner = UrllibTransport()
    transport: Transport = inner
    if machine is not None:
        token = MachineToken(server, machine, inner)
        token.current()
        transport = BearerTransport(inner, token)
    return SwarmSessionClient(
        server,
        transport=transport,
        auto_approve=auto_approve,
        share_with=share_with,
    )
