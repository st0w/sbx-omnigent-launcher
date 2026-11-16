"""Filing: putting triaged findings where a human looks.

Run: python -m unittest tests.test_runner_filing
"""

from __future__ import annotations

import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.filing import FindingFiler
from sbx_omnigent.runner.parsing.dispositions import (
    DISPOSITION_ABSENT,
    DISPOSITION_FILED,
    Disposition,
)
from sbx_omnigent.runner.records.findings import (
    blocking_finding_id,
    finding_id,
    finding_marker,
)
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.triage import FindingTriage

_PIPELINE = """\
name: demo
repo: https://github.com/org/proj.git
base_branch: main
publish: {mode}
task: |
  add parse_ports
agents:
  build: {{template: coder, model: claude-sonnet-5}}
  sec: {{template: security-reviewer, model: claude-sonnet-5}}
stages:
  - {{id: build, run: build, write: true}}
  - {{id: review, run: [sec], needs: [build], gate: consensus,
      on_block: build}}
  - {{id: scan, run: sec, scans: build, needs: [review], on_block: build}}
image:
  from: ghcr.io/omnigent-ai/omnigent-host:v0.20.0
  setup: apt-get install -y jq
  claude_plugins: [claude-security@claude-plugins-official]
"""

_REPORT = (
    'The parser accepts a negative port in src/ports.rs:40 and the '
    'range check is skipped entirely.\n\nVERDICT: BLOCKING'
)


class _FakeTracker:
    """The worktree-manager calls filing makes, per their contracts."""

    def __init__(self) -> None:
        self.bodies = ''
        self.readable = True
        self.issues: list[tuple[str, str, tuple[str, ...]]] = []
        self.create_ok = True
        self.label_calls = 0
        self.files: dict[str, str] = {}
        self.written: list[tuple[str, str, str]] = []
        self.commits: list[str] = []

    def issue_bodies_text(self, repo: str) -> str | None:
        return self.bodies if self.readable else None

    def create_issue(self, repo, *, title, body, labels=()):
        self.issues.append((title, body, tuple(labels)))
        if not self.create_ok:
            return None
        return f'https://github.com/org/proj/issues/{len(self.issues)}'

    def ensure_labels(self, repo, labels) -> bool:
        self.label_calls += 1
        return True

    def node_worktree_path(self, run_id: str, node_id: str) -> str:
        return f'/w/{node_id}'

    def read_tracked_file(self, worktree: str, path: str) -> str | None:
        return self.files.get(path)

    def write_tracked_file(self, worktree: str, path: str, text: str):
        self.written.append((worktree, path, text))

    def commit_node(self, run_id, node_id, *, message, author=None):
        self.commits.append(message)
        return 'a' * 40


def _vote(stage: str = 'review', **kw: object) -> ReviewRecord:
    fields: dict[str, object] = {
        'chunk': 'm0', 'stage': stage, 'reviewer': 'sec', 'round_no': 1,
        'verdict': 'APPROVED', 'target': 'build',
    }
    fields.update(kw)
    return ReviewRecord(**fields)


class _FilingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        echo = mock.patch.object(click, 'echo')
        echo.start()
        self.addCleanup(echo.stop)
        self.wt = _FakeTracker()
        self.reviews: list[ReviewRecord] = []

    def filer(self, mode: str = 'pr') -> FindingFiler:
        path = self.root / 'pipeline.yaml'
        path.write_text(_PIPELINE.format(mode=mode), encoding='utf-8')
        cfg = pipeline.load_pipeline(path)
        ctx = RunContext(
            config=cfg, worktrees=self.wt, sessions=mock.Mock(), run_id='r1',
            nodes={},
            stage_by_id={s.id: s for s in pipeline._iter_stages(cfg.stages)},
            lock=threading.RLock(), max_rounds=3,
        )
        self.triage = FindingTriage(chunk_reviews=lambda: self.reviews)
        return FindingFiler(
            ctx, publish_repo='https://github.com/org/proj',
            triage=self.triage, chunk_reviews=lambda: self.reviews,
        )

    def finding(self, *texts: str, **kw: object) -> ReviewRecord:
        rec = _vote(findings=texts, defects=texts, **kw)
        self.reviews.append(rec)
        return rec


class TestFilingFindings(_FilingTest):
    def test_a_new_finding_is_filed_as_an_issue(self) -> None:
        filer = self.filer()
        rec = self.finding('ports are unchecked')
        filer.publish('build', 'docs/plans/demo-reviews.md')
        (title, body, labels), = self.wt.issues
        self.assertIn(finding_marker(finding_id(rec, 1)), body)
        self.assertEqual(labels, ('finding',))
        url = 'https://github.com/org/proj/issues/1'
        self.assertEqual(filer.filed, [(url, title)])
        self.assertEqual(filer.outcomes[finding_id(rec, 1)], f'Filed: {url}')

    def test_a_finding_already_in_the_tracker_is_not_filed_again(self) -> None:
        filer = self.filer()
        rec = self.finding('ports are unchecked')
        self.wt.bodies = finding_marker(finding_id(rec, 1))
        filer.publish('build', None)
        self.assertEqual(self.wt.issues, [])
        self.assertIn('Already tracked', filer.outcomes[finding_id(rec, 1)])

    def test_an_unreadable_tracker_files_nothing(self) -> None:
        # Every id would look absent: filing then fills the tracker
        # with duplicates.
        filer = self.filer()
        self.finding('ports are unchecked')
        self.wt.readable = False
        filer.publish('build', None)
        self.assertEqual(self.wt.issues, [])

    def test_a_withheld_finding_is_summarised_with_its_reason(self) -> None:
        filer = self.filer()
        rec = self.finding('ports are unchecked')
        self.triage.dispositions[finding_id(rec, 1)] = Disposition(
            DISPOSITION_ABSENT, 'checked ports.rs:40',
        )
        filer.publish('build', None)
        (title, body, _labels), = self.wt.issues
        self.assertIn('verified against the code and withheld', title)
        self.assertIn('checked ports.rs:40', body)
        self.assertIn('Withheld by triage', filer.outcomes[finding_id(rec, 1)])

    def test_a_failed_create_is_recorded_and_not_listed(self) -> None:
        filer = self.filer()
        rec = self.finding('ports are unchecked')
        self.wt.create_ok = False
        filer.publish('build', None)
        self.assertEqual(filer.filed, [])
        self.assertIn('Could not be filed', filer.outcomes[finding_id(rec, 1)])


class TestScanFindings(_FilingTest):
    def test_a_scan_finding_carries_its_severity_label(self) -> None:
        filer = self.filer()
        self.finding('[security-high] SQL built from input', stage='scan')
        filer.publish('build', None)
        (_title, _body, labels), = self.wt.issues
        self.assertEqual(labels, ('finding', 'security-high'))

    def test_the_labels_are_created_once_per_run(self) -> None:
        filer = self.filer()
        self.finding('[security-high] a', '[security-low] b', stage='scan')
        filer.publish('build', None)
        self.assertEqual(self.wt.label_calls, 1)

    def test_only_a_scans_latest_round_is_published(self) -> None:
        # Earlier rounds describe code the writer has since changed.
        filer = self.filer()
        self.finding('[security-low] old', stage='scan', round_no=1)
        self.finding('[security-low] new', stage='scan', round_no=2)
        filer.publish('build', None)
        titles = [title for title, _b, _l in self.wt.issues]
        self.assertEqual(len(titles), 1)
        self.assertIn('new', titles[0])


class TestFilingBlockingClaims(_FilingTest):
    def blocked(self, verdicts: dict) -> ReviewRecord:
        rec = _vote(verdict='BLOCKING', blocking_report=_REPORT)
        self.reviews.append(rec)
        self.triage.record_blocking_dispositions(
            self.triage.pending_blocking_reports(), verdicts,
        )
        return rec

    def test_a_claim_that_reproduces_is_filed(self) -> None:
        filer = self.filer()
        rec = self.blocked({(1, 1): Disposition(DISPOSITION_FILED, 'real')})
        filer.publish('build', None)
        (_title, body, _labels), = self.wt.issues
        ident = blocking_finding_id(rec, 1)
        self.assertIn(finding_marker(ident), body)
        self.assertTrue(filer.outcomes[ident].startswith('Filed: '))

    def test_a_report_nobody_concluded_on_is_named_in_one_issue(self) -> None:
        filer = self.filer()
        self.blocked({})
        filer.publish('build', None)
        (title, _body, _labels), = self.wt.issues
        self.assertIn('reached no conclusion', title)
        self.assertEqual(
            filer.blocking_summary_url, 'https://github.com/org/proj/issues/1',
        )

    def test_a_claim_triage_kept_out_is_summarised(self) -> None:
        filer = self.filer()
        rec = self.blocked({(1, 1): Disposition(DISPOSITION_ABSENT, 'fixed')})
        filer.publish('build', None)
        (title, _body, _labels), = self.wt.issues
        self.assertIn('withheld', title)
        self.assertIn('Withheld', filer.outcomes[blocking_finding_id(rec, 1)])


class TestEachModuleStartsClean(_FilingTest):
    def test_a_new_module_forgets_the_last_ones_records(self) -> None:
        # Each module's PR lists only its own issues.
        filer = self.filer()
        self.finding('ports are unchecked')
        filer.publish('build', None)
        filer.blocking_summary_url = 'https://github.com/org/proj/issues/9'
        filer.start_module()
        self.assertEqual(
            (filer.filed, filer.outcomes, filer.blocking_summary_url),
            ([], {}, None),
        )


class TestTheLedgerWithoutATracker(_FilingTest):
    def test_findings_are_appended_to_the_ledger(self) -> None:
        filer = self.filer(mode='local')
        self.finding('ports are unchecked')
        filer.publish('build', 'docs/plans/demo-reviews.md')
        (_tree, path, text), = self.wt.written
        self.assertEqual(path, 'docs/plans/demo-findings.md')
        self.assertIn('ports are unchecked', text)
        self.assertEqual(self.wt.issues, [])

    def test_a_ledger_that_already_holds_them_is_left_alone(self) -> None:
        filer = self.filer(mode='local')
        self.finding('ports are unchecked')
        filer.publish('build', None)
        self.wt.files['docs/plans/demo-findings.md'] = self.wt.written[0][2]
        filer.publish('build', None)
        self.assertEqual(len(self.wt.commits), 1)

    def test_no_findings_commits_nothing(self) -> None:
        self.filer(mode='local').publish('build', None)
        self.assertEqual(self.wt.written, [])


if __name__ == '__main__':
    unittest.main()
