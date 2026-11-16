"""Which microVM backs an agent session."""

from __future__ import annotations

from sbx_omnigent.swarm_session import SwarmSessionClient


def sandbox_for(client: SwarmSessionClient, session: str) -> str | None:
    """
    The microVM name backing *session*, or ``None``.

    A session names only its HOST; the sandbox is named after that
    host, so this is two reads: the session for its ``host_id``, and
    the host list for the name the launcher gave it. Deliberately
    NOT derived from the id by string surgery — the naming rule is
    the server's, and a convention baked in here would rot silently.

    Best-effort to the point of catching everything short of a
    KeyboardInterrupt. This runs only when a turn has ALREADY
    failed, so the one outcome that must be impossible is this
    lookup turning a diagnosable failure into a different one —
    and the ways it can go wrong are open-ended: an older server
    with no ``/v1/hosts``, a response shape that changed, an
    injected client that does not
    implement these reads at all. Every one of them means the same
    thing to the caller: no pane. ``KeyboardInterrupt`` is not an
    ``Exception`` and still gets out, matching
    :meth:`TurnCapture.turn` — a second Ctrl-C means now.

    :param client: The session client.
:param session: The session to locate.
    :returns: The sandbox name, e.g. ``"managed-cb683c32"``.
    """
    try:
        host = client.session_host_id(session)
        return client.host_name(host) if host else None
    except Exception:
        return None
