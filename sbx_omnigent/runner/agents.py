"""Which registered agent each of a pipeline's agents runs as."""

from __future__ import annotations

from sbx_omnigent import pipeline
from sbx_omnigent.runner.errors import PipelineRunError


def resolve_agent_ids(
    config: pipeline.PipelineConfig, catalog: list[dict[str, object]]
) -> dict[str, str]:
    """
    Map each declared agent to its registered id from the catalog.

    Agents register at server start (``--pipeline``) under their
    namespaced names, so this matches ``namespaced_agent_name`` against
    ``GET /v1/agents``.

    :param config: The parsed pipeline.
    :param catalog: ``GET /v1/agents`` objects (``id``/``name``).
    :returns: ``{agent_name: agent_id}``.
    :raises PipelineRunError: When any agent is not registered (with the
        fix: start the server with ``--pipeline``).
    """
    by_name: dict[str, str] = {}
    for entry in catalog:
        name, agent_id = entry.get('name'), entry.get('id')
        if isinstance(name, str) and isinstance(agent_id, str):
            by_name[name] = agent_id
    ids: dict[str, str] = {}
    missing: list[str] = []
    for name in config.agents:
        spec_name = pipeline.namespaced_agent_name(config.name, name)
        agent_id = by_name.get(spec_name)
        if agent_id is None:
            missing.append(spec_name)
        else:
            ids[name] = agent_id
    if missing:
        raise PipelineRunError(
            'pipeline agents not registered: '
            + ', '.join(sorted(missing))
            + '. Start the server with '
            f'`omni-sbx server --pipeline {config.source_path}`.'
        )
    return ids
