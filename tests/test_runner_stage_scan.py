"""The scan stage: the claude-security plugin over the branch to ship.

Run: python -m unittest tests.test_runner_stage_scan
"""

from __future__ import annotations

import json
import threading
import unittest
from unittest import mock

import click

from sbx_omnigent import security_scan
from sbx_omnigent.pipeline import PipelineStage, Subtask
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError, _Blocked
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.stages.scan import ScanStage

_STAMP = '20261116-090000'
_BASE = 'a' * 40
_TIP = 'b' * 40


def _jsonl(*severities: str) -> str:
    return ''.join(
        json.dumps({
            'id': f'F{i}', 'title': f'{sev.title()} problem', 'file': 'a.py',
            'line': 10 + i, 'severity': sev, 'category': 'SQL Injection',
            'description': 'the input reaches the query.',
            'exploit_scenario': "name=' OR 1=1", 'cwe_id': 'CWE-89',
            'recommendation': 'bind it.',
        }) + '\n'
        for i, sev in enumerate(severities, start=1)
    )


def _report(*severities: str) -> security_scan.ScanReport:
    return security_scan.ScanReport(
        name=f'CLAUDE-SECURITY-{_STAMP}',
        files=(
            (security_scan.RESULTS_NAME, _jsonl(*severities)),
            ('CLAUDE-SECURITY-RESULTS.md', '# report'),
        ),
    )


_SCAN = PipelineStage(
    id='scan', run=('sec',), scans='build', needs=('review',),
    on_block='build', scan_effort='medium',
)
_REVIEW = PipelineStage(
    id='review', run=('bugs',), needs=('build',), gate='consensus',
    on_block='build',
)


class _ScanTest(unittest.TestCase):
    def setUp(self) -> None:
        echo = mock.patch.object(click, 'echo')
        echo.start()
        self.addCleanup(echo.stop)
        self.wt = mock.Mock()
        self.wt.create_node_worktree.return_value = '/w/scan'
        self.wt.hub_ref_tip.return_value = _BASE
        self.wt.hub_branch_tip.return_value = _TIP
        self.wt.write_run_artifact.return_value = True
        self.config = mock.Mock()
        self.config.base_branch = 'main'
        self.config.plan_artifact = None
        self.config.name = 'demo'
        self.config.subtasks = ()
        self.progress = RunProgress()
        self.nodes: dict = {}
        self.book = mock.Mock()
        self.book.next_round.side_effect = self._next_round
        self.book.spend_round.side_effect = self._spend
        self.rounds = 0
        self.spent = 0
        self.driver = mock.Mock()
        self.driver.create.return_value = 'sess-scan'
        self.disposal = mock.Mock()
        self.capture = mock.Mock()
        self.redriven: list[tuple[str, str]] = []
        self.reviewed: list[str] = []
        #: What each successive scan reports.
        self.reports: list[security_scan.ScanReport | None] = []
        collect = mock.patch.object(
            security_scan, 'collect_report',
            side_effect=lambda _wt: self.reports.pop(0),
        )
        collect.start()
        self.addCleanup(collect.stop)

    def _next_round(self, _stage: str) -> int:
        self.rounds += 1
        return self.rounds

    def _spend(self, _stage: str) -> int:
        self.spent += 1
        return self.spent

    def scan(self, *, max_rounds: int = 3) -> ScanStage:
        ctx = RunContext(
            config=self.config, worktrees=self.wt, sessions=mock.Mock(),
            run_id='r1', nodes=self.nodes,
            stage_by_id={'scan': _SCAN, 'review': _REVIEW},
            lock=threading.RLock(), max_rounds=max_rounds,
        )
        return ScanStage(
            ctx,
            progress=self.progress,
            book=self.book,
            driver=self.driver,
            disposal=self.disposal,
            capture=self.capture,
            node_slots=threading.Semaphore(4),
            redrive_writer=lambda stage, findings, **_kw: (
                self.redriven.append((stage, findings))
            ),
            run_review=lambda stage: self.reviewed.append(stage.id),
        )

    def recorded(self) -> list[ReviewRecord]:
        return [c.args[0] for c in self.book.add.call_args_list]


class TestAScanThatPasses(_ScanTest):
    def test_it_approves_and_records_its_findings(self) -> None:
        self.reports = [_report('MEDIUM', 'LOW')]
        self.scan().run(_SCAN)
        self.assertEqual(self.nodes['scan'].verdict, 'APPROVED')
        (record,) = self.recorded()
        self.assertEqual(record.verdict, 'APPROVED')
        self.assertEqual(len(record.findings), 2)
        self.assertEqual(record.blocking_report, '')

    def test_it_scans_a_fresh_clone_of_the_writers_branch(self) -> None:
        self.reports = [_report()]
        self.scan().run(_SCAN)
        self.wt.create_node_worktree.assert_called_once_with(
            'r1', 'scan', from_node='build', replace=True,
        )

    def test_its_session_is_freed_once_it_reports(self) -> None:
        self.reports = [_report()]
        self.scan().run(_SCAN)
        self.driver.create.assert_called_once_with(
            'sec', '/w/scan', 'rw', 'scan',
        )
        self.disposal.free.assert_called_once()


class TestAScanThatBlocks(_ScanTest):
    def test_the_writer_fixes_then_the_reviews_and_scan_rerun(self) -> None:
        self.reports = [_report('HIGH'), _report('LOW')]
        self.scan().run(_SCAN)
        ((stage, findings),) = self.redriven
        self.assertEqual(stage, 'build')
        self.assertIn('High problem', findings)
        self.assertEqual(self.reviewed, ['review'])
        self.assertEqual(self.nodes['scan'].verdict, 'APPROVED')
        verdicts = [r.verdict for r in self.recorded()]
        self.assertEqual(verdicts, ['BLOCKING', 'APPROVED'])

    def test_medium_and_low_become_tagged_findings_to_file(self) -> None:
        self.reports = [_report('HIGH', 'MEDIUM', 'LOW'), _report()]
        self.scan().run(_SCAN)
        record = self.recorded()[0]
        self.assertEqual(
            [text.split(']')[0] for _i, text in record.filed_findings()],
            ['[security-medium', '[security-low'],
        )
        self.assertIn('High problem', record.blocking_report)
        self.assertNotIn('Medium problem', record.blocking_report)

    def test_a_blocking_round_keeps_its_report_for_triage(self) -> None:
        self.reports = [_report('CRITICAL'), _report()]
        self.scan().run(_SCAN)
        self.assertIn('Critical problem', self.recorded()[0].blocking_report)

    def test_past_the_round_cap_the_run_blocks(self) -> None:
        self.reports = [_report('HIGH')] * 2
        with self.assertRaises(_Blocked):
            self.scan(max_rounds=1).run(_SCAN)
        self.assertEqual(self.nodes['scan'].verdict, 'BLOCKING')

    def test_with_no_writer_to_send_it_to_the_run_blocks(self) -> None:
        stage = PipelineStage(
            id='scan', run=('sec',), scans='build', scan_effort='medium',
        )
        self.reports = [_report('HIGH')]
        with self.assertRaises(_Blocked):
            self.scan().run(stage)
        self.assertEqual(self.redriven, [])


class TestAScanThatCannotBeRead(_ScanTest):
    def test_no_report_stops_the_run_and_keeps_the_turn(self) -> None:
        self.reports = [None]
        with self.assertRaises(PipelineRunError) as caught:
            self.scan().run(_SCAN)
        self.assertIn('wrote no report', str(caught.exception))
        self.capture.turn.assert_called_once()
        self.disposal.free.assert_called_once()

    def test_an_unreadable_report_stops_the_run(self) -> None:
        bad = security_scan.ScanReport(
            name=f'CLAUDE-SECURITY-{_STAMP}',
            files=((security_scan.RESULTS_NAME, 'not json\n'),),
        )
        self.reports = [bad]
        with self.assertRaises(PipelineRunError) as caught:
            self.scan().run(_SCAN)
        self.assertIn('could not read the report', str(caught.exception))

    def test_a_report_that_cannot_be_saved_stops_the_run(self) -> None:
        self.reports = [_report()]
        self.wt.write_run_artifact.return_value = False
        with self.assertRaises(PipelineRunError) as caught:
            self.scan().run(_SCAN)
        self.assertIn('could not save', str(caught.exception))


class TestTheSavedReport(_ScanTest):
    def test_each_file_is_saved_under_its_round(self) -> None:
        self.reports = [_report()]
        self.scan().run(_SCAN)
        paths = [c.args[1] for c in self.wt.write_run_artifact.call_args_list]
        folder = f'security/scan/r1-{_STAMP}'
        self.assertIn(f'{folder}/{security_scan.RESULTS_NAME}', paths)
        self.assertEqual(
            self.recorded()[0].attachments[0],
            f'{folder}/{security_scan.RESULTS_NAME}',
        )

    def test_the_record_names_where_it_is_committed(self) -> None:
        self.reports = [_report()]
        self.scan().run(_SCAN)
        (text,) = (t for _role, t in self.recorded()[0].turns)
        self.assertIn(f'`demo-security/r1-{_STAMP}/`', text)


class TestWhereAModuleStarted(_ScanTest):
    def test_outside_a_campaign_it_is_the_base_branch(self) -> None:
        self.assertEqual(self.scan().module_start(), _BASE)
        self.wt.hub_ref_tip.assert_called_once_with('r1', 'main')

    def test_in_a_module_it_is_the_campaign_tip(self) -> None:
        self.progress.start_module(Subtask(id='m1', title='t'), first=False)
        self.assertEqual(self.scan().module_start(), _TIP)

    def test_the_first_module_falls_back_to_the_base(self) -> None:
        self.wt.hub_branch_tip.return_value = None
        self.progress.start_module(Subtask(id='m0', title='t'), first=True)
        self.assertEqual(self.scan().module_start(), _BASE)

    def test_no_base_on_the_hub_stops_the_run(self) -> None:
        self.wt.hub_ref_tip.return_value = None
        with self.assertRaises(PipelineRunError) as caught:
            self.scan().module_start()
        self.assertIn("base branch 'main'", str(caught.exception))

    def test_outside_a_campaign_it_is_the_runs_folder(self) -> None:
        self.assertEqual(self.scan().security_folder_name(), 'demo-security')

    def test_a_module_has_its_own_security_folder(self) -> None:
        self.config.subtasks = (Subtask(id='m1', title='two'),)
        self.progress.start_module(self.config.subtasks[0], first=False)
        self.assertEqual(
            self.scan().security_folder_name(), 'demo-m1-security',
        )


if __name__ == '__main__':
    unittest.main()
