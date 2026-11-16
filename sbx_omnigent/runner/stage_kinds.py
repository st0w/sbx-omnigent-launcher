"""What kind each stage is, and which writers nothing drives again."""

from __future__ import annotations

from sbx_omnigent import pipeline
from sbx_omnigent.runner.records.reviews import ReviewRecord


def stage_kind(stage: pipeline.PipelineStage) -> str:
    """
    What kind of stage *stage* is, from what it declares.

    The first match wins, in this order, because some stages declare
    more than one: a scan also names the writer it blocks back to, so
    it is checked before review.

    :param stage: The stage.
    :returns: ``'writer'``, ``'judge'``, ``'verify'``, ``'check'``,
        ``'scan'``, ``'review'`` or ``'reader'``.
    """
    if stage.write:
        return 'writer'
    if stage.selects:
        return 'judge'
    if stage.verifies:
        return 'verify'
    if stage.checks:
        return 'check'
    if stage.scans:
        return 'scan'
    if stage.gate or stage.on_block:
        return 'review'
    return 'reader'


def writer_is_terminal(
    config: pipeline.PipelineConfig, stage_id: str
) -> bool:
    """
    Whether nothing in the DAG can drive this writer again.

    Exactly two things re-drive a writer: a review stage naming it in
    ``on_block``, and the verification gate, which loops back to the
    writer that produced the branch being published. A writer that is
    neither is finished the moment its stage completes — in the live
    agent cadre that is the TDD writer, which held a full guest from
    a few minutes into a module until it published, on a host already
    over-committed.

    Deliberately conservative wherever the gate's target cannot be
    named from the DAG alone: a pipeline that publishes "the last
    writer", or that publishes a JUDGE's pick (resolved only once the
    judge has run), could loop back to a writer this cannot identify,
    so in those shapes no writer counts as terminal.

    :param config: The parsed pipeline.
    :param stage_id: The writer stage's id, WITHOUT any chunk prefix.
    :returns: Whether its microVM can be freed when its stage ends.
    """
    stages = list(pipeline._iter_stages(config.stages))
    if any(s.on_block == stage_id for s in stages):
        return False
    want = config.publish.branch
    if want is None:
        return False
    target = next((s for s in stages if s.id == want), None)
    if target is None or stage_kind(target) != 'writer':
        return False
    return stage_id != want


def recorded_by_scan(
    rec: ReviewRecord, stage_by_id: dict[str, pipeline.PipelineStage]
) -> bool:
    """
    Whether *rec* was recorded by a ``scans:`` stage.

    :param rec: A review record.
    :param stage_by_id: The run's stages, by id.
    :returns: ``True`` for a scan's record.
    """
    stage = stage_by_id.get(rec.stage)
    return stage is not None and stage.scans is not None
