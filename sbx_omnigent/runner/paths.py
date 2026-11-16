"""Where the runner commits its records in the project's repo."""

from __future__ import annotations

from pathlib import PurePosixPath

from sbx_omnigent import pipeline


def plan_artifact_path(config: pipeline.PipelineConfig) -> str:
    """
    The repo path the plan of record is committed to on publish.

    :param config: The parsed pipeline.
    :returns: ``plan_artifact:`` when set, else
        ``docs/plans/<pipeline>.md``.
    """
    return config.plan_artifact or f'docs/plans/{config.name}.md'


def decisions_ledger_path(config: pipeline.PipelineConfig) -> str:
    """
    The decisions ledger's repo path — ONE per pipeline.

    :param config: The parsed pipeline.
    :returns: e.g. ``docs/plans/ingestion-decisions.md``.
    """
    artifact = PurePosixPath(plan_artifact_path(config))
    return str(
        artifact.with_name(f'{artifact.stem}-decisions{artifact.suffix}')
    )
