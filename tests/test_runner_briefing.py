"""The briefing: what every agent turn is told about the run around it.

Run: python -m unittest tests.test_runner_briefing
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

from sbx_omnigent.pipeline import PipelineStage, Subtask
from sbx_omnigent.runner.briefing import Briefing
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.prompts.writer import _DISPOSABLE_VM, _UNATTENDED
from sbx_omnigent.runner.records.results import NodeResult

_MODULES = [
    Subtask(id='transport', title='do the transport work'),
    Subtask(id='topology', title='do the topology work'),
    Subtask(id='identities', title='do the identities work'),
]


class _BriefingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = mock.Mock()
        self.config.task = 'Build the scope collector.'
        self.config.acceptance = None
        self.config.setup = None
        self.nodes: dict[str, NodeResult] = {}
        self.progress = RunProgress(subtasks=list(_MODULES))
        ctx = RunContext(
            config=self.config, worktrees=mock.Mock(), sessions=mock.Mock(),
            run_id='r1', nodes=self.nodes, stage_by_id={},
            lock=threading.RLock(), max_rounds=3,
        )
        self.briefing = Briefing(ctx, progress=self.progress)

    def at(self, module: str, published: tuple[str, ...] = ()) -> None:
        self.progress.completed_chunks = set(published)
        sub = next(s for s in _MODULES if s.id == module)
        self.progress.start_module(sub, first=module == 'transport')


class TestTheIncrementPreamble(_BriefingTest):
    """Every role is handed the WHOLE plan's brief as its contract, so
    it must be told which row is its own."""

    def test_outside_a_campaign_there_is_none(self) -> None:
        self.assertEqual(self.briefing.chunk_preamble(), '')

    def test_every_increment_is_named(self) -> None:
        self.at('topology', ('transport',))
        pre = self.briefing.chunk_preamble()
        for sub in _MODULES:
            with self.subTest(increment=sub.id):
                self.assertIn(f'[{sub.id}]', pre)

    def test_the_rows_say_done_yours_and_waiting(self) -> None:
        self.at('topology', ('transport',))
        pre = self.briefing.chunk_preamble()
        self.assertIn('✓ [transport]', pre)
        self.assertIn('▶ [topology]', pre)
        self.assertIn('  [identities]', pre)

    def test_the_active_title_is_printed_once(self) -> None:
        self.at('topology', ('transport',))
        pre = self.briefing.chunk_preamble()
        self.assertEqual(pre.count('do the topology work'), 1)

    def test_another_rows_requirement_is_not_missing(self) -> None:
        self.at('topology', ('transport',))
        self.assertIn('NOT missing', self.briefing.chunk_preamble())

    def test_the_first_increment_is_told_nothing_is_built(self) -> None:
        self.at('transport')
        pre = self.briefing.chunk_preamble()
        self.assertIn('It is the FIRST increment', pre)
        self.assertNotIn('Earlier increments are already implemented', pre)

    def test_a_later_increment_builds_on_earlier_ones(self) -> None:
        self.at('topology', ('transport',))
        self.assertIn('Earlier increments are already implemented',
                      self.briefing.chunk_preamble())


class TestTheModuleTable(_BriefingTest):
    def test_only_the_active_module_is_marked(self) -> None:
        table = self.briefing.module_table(_MODULES[1])
        self.assertEqual(table.splitlines(), [
            '  [transport] do the transport work',
            '▶ [topology] do the topology work',
            '  [identities] do the identities work',
        ])


class TestTheTaskBlock(_BriefingTest):
    def test_it_carries_the_task_and_the_standing_rules(self) -> None:
        block = self.briefing.task_block()
        self.assertTrue(block.startswith('Task:\nBuild the scope collector.'))
        self.assertIn(_UNATTENDED, block)
        self.assertIn(_DISPOSABLE_VM, block)

    def test_an_acceptance_contract_is_added(self) -> None:
        self.config.acceptance = 'handles ranges'
        self.assertIn('Acceptance contract:\nhandles ranges',
                      self.briefing.task_block())

    def test_the_setup_note_is_added(self) -> None:
        self.config.setup = 'apt-get install cargo'
        self.assertIn('apt-get install cargo', self.briefing.task_block())

    def test_in_a_campaign_the_preamble_comes_first(self) -> None:
        self.at('topology', ('transport',))
        block = self.briefing.task_block()
        self.assertTrue(block.startswith('This is build increment'))
        self.assertLess(block.index('▶ [topology]'), block.index('Task:'))


class TestTheReaderContext(_BriefingTest):
    def stage(self, *needs: str) -> PipelineStage:
        return PipelineStage(id='build', run=('b',), write=True, needs=needs)

    def test_each_upstream_readers_design_is_passed_on(self) -> None:
        self.nodes['plan'] = NodeResult('plan', 'reader', output='# Plan')
        self.nodes['spec'] = NodeResult('spec', 'reader', output='# Spec')
        self.assertEqual(
            self.briefing.reader_context(self.stage('plan', 'spec')),
            '--- Design from plan ---\n# Plan\n\n'
            '--- Design from spec ---\n# Spec',
        )

    def test_a_writers_output_is_not_design(self) -> None:
        self.nodes['tests'] = NodeResult('tests', 'writer', output='done')
        self.assertEqual(self.briefing.reader_context(self.stage('tests')),
                         '')

    def test_a_contract_checks_findings_are_not_design(self) -> None:
        self.nodes['check'] = NodeResult('check', 'check', output='x')
        self.assertEqual(self.briefing.reader_context(self.stage('check')),
                         '')

    def test_a_reader_that_said_nothing_is_skipped(self) -> None:
        self.nodes['plan'] = NodeResult('plan', 'reader', output='')
        self.assertEqual(self.briefing.reader_context(self.stage('plan')),
                         '')

    def test_a_need_that_has_not_run_is_skipped(self) -> None:
        self.assertEqual(self.briefing.reader_context(self.stage('plan')),
                         '')


if __name__ == '__main__':
    unittest.main()
