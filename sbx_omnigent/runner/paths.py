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


def findings_ledger_path(config: pipeline.PipelineConfig) -> str:
    """
    The findings ledger's repo path — ONE per pipeline.

    Deliberately not derived from the chunk's plan path the way the
    reviews and selection records are. Those are per-chunk on purpose;
    this must accumulate across every chunk AND every run, because the
    repo is the only thing that survives a run and each chunk's branch
    carries the ledger forward on its base.

    :param config: The parsed pipeline.
    :returns: e.g. ``docs/plans/discover-findings.md``.
    """
    p = PurePosixPath(plan_artifact_path(config))
    return str(p.with_name(f'{p.stem}-findings{p.suffix}'))


def security_folder(plan_path: str) -> str:
    """
    The scan reports' folder: ``<plan>-security`` beside the plan.

    :param plan_path: A module's plan artifact path.
    :returns: e.g. ``docs/plans/discover-m1-security``.
    """
    p = PurePosixPath(plan_path)
    return str(p.with_name(f'{p.stem}-security'))


def chunk_plan_path(
    config: pipeline.PipelineConfig, module: pipeline.Subtask | None
) -> str:
    """
    The repo path a module's plan of record is committed to.

    :param config: The parsed pipeline.
    :param module: The module, or ``None`` outside a campaign.
    :returns: ``…-<module>.md`` in per-module mode, otherwise the run's
        own plan path.
    """
    if module is not None and config.subtasks:
        p = PurePosixPath(plan_artifact_path(config))
        return str(p.with_name(f'{p.stem}-{module.id}{p.suffix}'))
    return plan_artifact_path(config)


def chunk_artifact_base(
    config: pipeline.PipelineConfig,
    module: pipeline.Subtask | None,
    plan_path: str,
) -> str:
    """
    The base path a module's OWN records hang off.

    Per-module records must not share a path across modules. Each
    module publishes its own pull request, and the campaign thread
    carries every earlier module's records into the later ones, so a
    shared path would be one file rewritten by every module: each
    module's record would replace the one before it, and two pull
    requests merged out of order would conflict on it.

    Per-module mode never hit this because its *plan_path* is already
    per-module (``discover-m1.md``), which these records inherit; it is
    passed through untouched here so the module id is not folded in
    twice. Flat campaign mode shares one plan_path across every module,
    so the module id is folded in HERE.

    :param config: The parsed pipeline.
    :param module: The module, or ``None`` outside a campaign.
    :param plan_path: The module's plan artifact path.
    :returns: The base path to derive per-module record names from.
    """
    if module is None or config.subtasks:
        return plan_path
    p = PurePosixPath(plan_path)
    return str(p.with_name(f'{p.stem}-{module.id}{p.suffix}'))


def reviews_record_path(
    config: pipeline.PipelineConfig,
    module: pipeline.Subtask | None,
    plan_path: str,
) -> str:
    """The reviewer reports' repo path, beside the plan's."""
    p = PurePosixPath(chunk_artifact_base(config, module, plan_path))
    return str(p.with_name(f'{p.stem}-reviews{p.suffix}'))


def selection_record_path(
    config: pipeline.PipelineConfig,
    module: pipeline.Subtask | None,
    plan_path: str,
) -> str:
    """The judge record's repo path, beside the reviewers'."""
    p = PurePosixPath(chunk_artifact_base(config, module, plan_path))
    return str(p.with_name(f'{p.stem}-selection{p.suffix}'))


def session_record_path(plan_path: str) -> str:
    """The planning-session record's repo path, beside the plan's."""
    p = PurePosixPath(plan_path)
    return str(p.with_name(f'{p.stem}-session{p.suffix}'))
