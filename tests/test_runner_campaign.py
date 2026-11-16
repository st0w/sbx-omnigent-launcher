"""The campaign: building a pipeline once per module, threaded together.

Run: python -m unittest tests.test_runner_campaign
"""

from __future__ import annotations

import shutil
import tempfile
import threading
import unittest
from collections.abc import Sequence
from pathlib import Path
from unittest import mock

import click

from sbx_omnigent import pipeline
from sbx_omnigent.pipeline import PipelineStage, Subtask
from sbx_omnigent.runner.campaign import (
    Campaign,
    CampaignHooks,
    namespace_stages,
)
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.records.results import NodeResult

_PIPELINE = """\
name: mods
repo: ./proj
{publish}task: |
  the shared project brief
{subtasks}agents:
  plan: {{template: planner, model: claude-sonnet-5}}
  build: {{template: coder, model: claude-sonnet-5}}
  sec: {{template: security-reviewer, model: claude-sonnet-5}}
stages:
  - {{id: plan, run: plan}}
  - {{id: build, run: build, write: true, needs: [plan]}}
  - {{id: review, run: [sec], needs: [build], gate: consensus,
      on_block: build}}
"""

_SUBTASKS = (
    'subtasks:\n'
    '  - {id: m0, title: contracts and core}\n'
    '  - {id: m1, title: storage and schema}\n'
)

_M0 = Subtask(id='m0', title='contracts and core')
_M1 = Subtask(id='m1', title='storage and schema')


class TestNamespacing(unittest.TestCase):
    """Each module runs its own copy of the stages, so branches never
    collide: ``<module>-<stage>``."""

    def stage(self, **kw: object) -> PipelineStage:
        return PipelineStage(**kw)

    def one(self, stage: PipelineStage, *, planner=(), writers=(),
            seed: bool = False) -> PipelineStage:
        (out,) = namespace_stages(
            'm1', [stage], set(planner), set(writers), seed_campaign=seed,
        )
        return out

    def test_the_id_and_every_reference_are_prefixed(self) -> None:
        out = self.one(self.stage(
            id='scan', run=('sec',), needs=('review',), on_block='build',
            scans='build', checks='plan',
        ))
        self.assertEqual(
            (out.id, out.needs, out.on_block, out.scans, out.checks),
            ('m1-scan', ('m1-review',), 'm1-build', 'm1-build', 'm1-plan'),
        )

    def test_a_scan_is_pointed_at_its_own_modules_writer(self) -> None:
        out = self.one(
            self.stage(id='scan', run=('sec',), scans='build',
                       needs=('review',), on_block='build'),
            writers={'build'},
        )
        self.assertEqual((out.scans, out.on_block), ('m1-build', 'm1-build'))

    def test_a_shared_planner_keeps_its_name(self) -> None:
        # A flat campaign plans once, up front, for every module.
        out = self.one(
            self.stage(id='build', run=('b',), write=True, needs=('plan',)),
            planner={'plan'},
        )
        self.assertEqual(out.needs, ('plan',))

    def test_a_parallel_group_is_prefixed_through(self) -> None:
        group = self.stage(id='impl', parallel=(
            self.stage(id='impl-a', run=('a',), write=True),
            self.stage(id='impl-b', run=('b',), write=True),
        ))
        out = self.one(group)
        self.assertEqual(
            (out.id, [s.id for s in out.parallel]),
            ('m1-impl', ['m1-impl-a', 'm1-impl-b']),
        )

    def test_a_later_modules_entry_node_builds_on_the_thread(self) -> None:
        out = self.one(self.stage(id='plan', run=('p',)), seed=True)
        self.assertEqual(out.from_branch, 'campaign')

    def test_the_first_module_starts_from_the_base(self) -> None:
        out = self.one(self.stage(id='plan', run=('p',)), seed=False)
        self.assertIsNone(out.from_branch)

    def test_a_node_after_an_in_module_writer_is_not_reseeded(self) -> None:
        out = self.one(
            self.stage(id='refactor', run=('r',), write=True,
                       needs=('build',)),
            writers={'build'}, seed=True,
        )
        self.assertIsNone(out.from_branch)

    def test_an_explicit_from_is_kept_and_prefixed(self) -> None:
        out = self.one(
            self.stage(id='build', run=('b',), write=True,
                       from_branch='tests'),
            seed=True,
        )
        self.assertEqual(out.from_branch, 'm1-tests')

    def test_a_review_does_not_seed(self) -> None:
        out = self.one(
            self.stage(id='review', run=('s',), gate='consensus',
                       on_block='build'),
            seed=True,
        )
        self.assertIsNone(out.from_branch)


class _FakeWorktrees:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.refuse_dispose = False

    def alias_node_branch(self, run_id: str, alias: str, node: str) -> None:
        self.events.append(f'alias {alias} -> {node}')

    def dispose_node_worktrees(self, run_id, node_ids) -> int:
        if self.refuse_dispose:
            raise click.ClickException('busy')
        self.events.append(f'reclaim {sorted(node_ids)}')
        return len(node_ids)


class _CampaignTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        echo = mock.patch.object(click, 'echo')
        echo.start()
        self.addCleanup(echo.stop)
        self.events: list[str] = []
        self.wt = _FakeWorktrees(self.events)
        self.nodes: dict[str, NodeResult] = {}
        self.stage_by_id: dict[str, PipelineStage] = {}
        self.progress = RunProgress()
        self.publisher = mock.Mock()
        self.publisher.publish_module.side_effect = (
            lambda sub, winner, **kw: self.events.append(
                f'publish {sub.id} {winner} plan={kw["plan"]!r}'
            )
        )
        self.filing = mock.Mock()
        self.sessions = 0

    def config(self, *, per_module: bool = True,
               branch: str | None = None) -> pipeline.PipelineConfig:
        publish = (
            f'publish:\n  mode: local\n  branch: {branch}\n'
            if branch else 'publish: local\n'
        )
        path = self.root / 'pipeline.yaml'
        path.write_text(_PIPELINE.format(
            subtasks=_SUBTASKS if per_module else '', publish=publish,
        ), encoding='utf-8')
        return pipeline.load_pipeline(path)

    def run_stage(self, stage: PipelineStage) -> None:
        self.events.append(f'run {stage.id}')
        self.sessions += 1
        kind = 'writer' if stage.write else 'reader'
        self.nodes[stage.id] = NodeResult(
            stage.id, kind, output=f'out {stage.id}',
        )
        if stage.write:
            self.progress.last_branch_node = stage.id

    def gate(self, winner: str, stages: Sequence[PipelineStage]) -> None:
        self.events.append(f'gate {winner}')

    def campaign(self, cfg: pipeline.PipelineConfig, *,
                 keep: bool = False) -> Campaign:
        ctx = RunContext(
            config=cfg, worktrees=self.wt, sessions=mock.Mock(),
            run_id='r1', nodes=self.nodes, stage_by_id=self.stage_by_id,
            lock=threading.RLock(), max_rounds=3,
        )
        hooks = CampaignHooks(
            run_stage=self.run_stage,
            gate=self.gate,
            save_state=lambda: self.events.append('save'),
            dispose_prewarmed=lambda: self.events.append('free prewarmed'),
            session_count=lambda: self.sessions,
            dispose_sessions_since=(
                lambda mark: self.events.append(f'free sessions from {mark}')
            ),
            sample_disk=lambda event: self.events.append(f'disk {event}'),
        )
        return Campaign(
            ctx, progress=self.progress, publisher=self.publisher,
            filing=self.filing, keep=keep, hooks=hooks,
        )

    def module(self, module_id: str) -> list[str]:
        """This module's events, from its first stage to its reclaim."""
        start = self.events.index(f'run {module_id}-plan')
        end = next(
            i for i, e in enumerate(self.events)
            if i > start and e.startswith('reclaim')
        )
        return self.events[start:end + 1]


class TestWhenACampaignRuns(_CampaignTest):
    def test_two_or_more_modules_and_something_to_build(self) -> None:
        campaign = self.campaign(self.config())
        build = [PipelineStage(id='build', run=('b',), write=True)]
        self.progress.subtasks = [_M0, _M1]
        self.assertTrue(campaign.applies(build))

    def test_one_module_runs_the_single_pass(self) -> None:
        campaign = self.campaign(self.config())
        self.progress.subtasks = [_M0]
        self.assertFalse(campaign.applies(
            [PipelineStage(id='build', run=('b',), write=True)],
        ))

    def test_nothing_to_build_runs_the_single_pass(self) -> None:
        campaign = self.campaign(self.config())
        self.progress.subtasks = [_M0, _M1]
        self.assertFalse(campaign.applies([]))


class TestAModule(_CampaignTest):
    def run_both(self, **kw: bool) -> Campaign:
        cfg = self.config()
        campaign = self.campaign(cfg, **kw)
        self.progress.subtasks = [_M0, _M1]
        campaign.run(list(cfg.stages), per_module=True)
        return campaign

    def test_each_module_runs_in_order_then_gates_and_publishes(self) -> None:
        self.run_both()
        self.assertEqual(self.module('m1'), [
            'run m1-plan', 'run m1-build', 'run m1-review',
            'gate m1-build',
            "publish m1 m1-build plan='out m1-plan'",
            'alias campaign -> m1-build',
            'save',
            'free sessions from 3',
            'disk chunk-peak',
            "reclaim ['m1-build', 'm1-build-verify', 'm1-plan', "
            "'m1-review']",
        ])

    def test_the_thread_moves_only_after_the_records_publish(self) -> None:
        # The next planner is told the records are in its worktree.
        self.run_both()
        events = self.module('m0')
        self.assertLess(events.index("publish m0 m0-build plan='out m0-plan'"),
                        events.index('alias campaign -> m0-build'))

    def test_completion_is_saved_before_anything_is_reclaimed(self) -> None:
        # A resume between the two skips the module anyway.
        campaign = self.run_both()
        events = self.module('m0')
        self.assertLess(events.index('save'),
                        events.index('free sessions from 0'))
        self.assertEqual(campaign.progress.completed_chunks, {'m0', 'm1'})

    def test_the_pre_warmed_writers_are_freed_first(self) -> None:
        self.run_both()
        self.assertEqual(self.events[0], 'free prewarmed')

    def test_namespaced_stages_are_registered_for_loop_backs(self) -> None:
        self.run_both()
        self.assertEqual(self.stage_by_id['m1-build'].on_block, None)
        self.assertEqual(self.stage_by_id['m1-review'].on_block, 'm1-build')

    def test_each_module_starts_with_clean_filing_records(self) -> None:
        self.run_both()
        self.assertEqual(self.filing.start_module.call_count, 2)

    def test_no_module_is_active_afterwards(self) -> None:
        self.run_both()
        self.assertIsNone(self.progress.module_id)

    def test_only_the_first_module_is_first(self) -> None:
        self.run_both()
        firsts = [c.kwargs['first']
                  for c in self.publisher.publish_module.call_args_list]
        self.assertEqual(firsts, [True, False])

    def test_the_prior_module_is_named_when_publishing(self) -> None:
        self.run_both()
        priors = [c.kwargs['prior']
                  for c in self.publisher.publish_module.call_args_list]
        self.assertEqual(priors, [None, 'm0'])

    def test_keep_keeps_every_vm_and_clone(self) -> None:
        self.run_both(keep=True)
        self.assertFalse(any(
            e.startswith(('free sessions', 'reclaim')) for e in self.events
        ))

    def test_a_refused_reclaim_does_not_fail_the_run(self) -> None:
        self.wt.refuse_dispose = True
        self.run_both()
        self.assertEqual(self.progress.completed_chunks, {'m0', 'm1'})


class TestAResumedCampaign(_CampaignTest):
    def test_a_published_module_is_skipped(self) -> None:
        cfg = self.config()
        campaign = self.campaign(cfg)
        self.progress.subtasks = [_M0, _M1]
        self.progress.completed_chunks = {'m0'}
        campaign.run(list(cfg.stages), per_module=True)
        self.assertNotIn('run m0-plan', self.events)
        self.assertIn('run m1-plan', self.events)

    def test_the_next_module_still_builds_on_the_thread(self) -> None:
        cfg = self.config()
        campaign = self.campaign(cfg)
        self.progress.subtasks = [_M0, _M1]
        self.progress.completed_chunks = {'m0'}
        campaign.run(list(cfg.stages), per_module=True)
        self.assertEqual(self.stage_by_id['m1-plan'].from_branch, 'campaign')


class TestAFlatCampaign(_CampaignTest):
    """One planner up front, for every module."""

    def run_flat(self) -> None:
        cfg = self.config(per_module=False)
        campaign = self.campaign(cfg)
        self.progress.subtasks = [_M0, _M1]
        self.progress.plan_of_record = '# the one plan'
        build = [s for s in cfg.stages if s.id != 'plan']
        campaign.run(build, per_module=False)

    def test_the_shared_planner_is_not_rerun(self) -> None:
        self.run_flat()
        self.assertNotIn('run m0-plan', self.events)
        self.assertEqual(self.stage_by_id['m0-build'].needs, ('plan',))

    def test_the_plan_goes_with_the_first_module_only(self) -> None:
        self.run_flat()
        plans = [c.kwargs['plan']
                 for c in self.publisher.publish_module.call_args_list]
        self.assertEqual(plans, ['# the one plan', None])


class TestTheWinner(_CampaignTest):
    def test_a_configured_branch_is_the_modules_own(self) -> None:
        cfg = self.config(branch='build')
        campaign = self.campaign(cfg)
        self.progress.subtasks = [_M0, _M1]
        self.progress.last_branch_node = 'something-else'
        campaign.run(list(cfg.stages), per_module=True)
        self.assertIn('gate m1-build', self.events)

    def test_a_judge_publishes_its_selected_branch(self) -> None:
        cfg = self.config()
        campaign = self.campaign(cfg)
        self.nodes['m0-pick'] = NodeResult('m0-pick', 'judge',
                                           selected='m0-impl-b')
        self.progress.last_branch_node = 'm0-pick'
        self.assertEqual(campaign.winner('m0'), 'm0-impl-b')

    def test_a_judge_that_selected_nothing_stops_the_run(self) -> None:
        campaign = self.campaign(self.config())
        self.nodes['m0-pick'] = NodeResult('m0-pick', 'judge')
        self.progress.last_branch_node = 'm0-pick'
        with self.assertRaises(PipelineRunError) as caught:
            campaign.winner('m0')
        self.assertIn("'m0-pick' selected no candidate", str(caught.exception))

    def test_a_module_that_produced_no_branch_stops_the_run(self) -> None:
        campaign = self.campaign(self.config())
        with self.assertRaises(PipelineRunError) as caught:
            campaign.winner('m0')
        self.assertIn("chunk 'm0' produced no branch", str(caught.exception))


if __name__ == '__main__':
    unittest.main()
