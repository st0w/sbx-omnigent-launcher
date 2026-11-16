"""Security records: a module's scan reports, and what became of each.

Run: python -m unittest tests.test_runner_security_records
"""

from __future__ import annotations

import json
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
    DISPOSITION_FILED,
    Disposition,
)
from sbx_omnigent.runner.records.findings import (
    blocking_finding_id,
    finding_id,
)
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.security_records import SecurityRecords
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
  tri: {{template: verifier, model: claude-sonnet-5}}
stages:
  - {{id: build, run: build, write: true}}
  - {{id: scan, run: sec, scans: build, needs: [build], on_block: build}}
{triage}image:
  from: ghcr.io/omnigent-ai/omnigent-host:v0.20.0
  setup: apt-get install -y jq
  claude_plugins: [claude-security@claude-plugins-official]
"""

_TRIAGE = '  - {id: triage, run: tri, verifies: scan, needs: [scan]}\n'

_PLAN = 'docs/plans/demo-m0.md'
_FOLDER = 'docs/plans/demo-m0-security'

_REPORT = (
    '### F1 — HIGH — SQL built from input\n\nThe name reaches the query '
    'in src/db.rs:40 unescaped.\n\nVERDICT: BLOCKING'
)


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


class _FakeWorktrees:
    """The worktree-manager calls security records make."""

    def __init__(self) -> None:
        self.artifacts: dict[str, str] = {}
        self.written: dict[str, str] = {}
        self.commits: list[str] = []
        self.refuse = False

    def read_run_artifact(self, run_id: str, relpath: str) -> str | None:
        return self.artifacts.get(relpath)

    def node_worktree_path(self, run_id: str, node_id: str) -> str:
        return f'/w/{node_id}'

    def write_tracked_file(self, worktree: str, path: str, text: str):
        if self.refuse:
            raise click.ClickException('git refused the write')
        self.written[path] = text

    def commit_node(self, run_id, node_id, *, message, author=None):
        self.commits.append(message)
        return 'a' * 40


class _RecordsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        echo = mock.patch.object(click, 'echo')
        echo.start()
        self.addCleanup(echo.stop)
        self.wt = _FakeWorktrees()
        self.reviews: list[ReviewRecord] = []

    def records(
        self, *, mode: str = 'pr', triage: bool = True
    ) -> SecurityRecords:
        path = self.root / 'pipeline.yaml'
        path.write_text(
            _PIPELINE.format(mode=mode, triage=_TRIAGE if triage else ''),
            encoding='utf-8',
        )
        cfg = pipeline.load_pipeline(path)
        ctx = RunContext(
            config=cfg, worktrees=self.wt, sessions=mock.Mock(), run_id='r1',
            nodes={},
            stage_by_id={s.id: s for s in pipeline._iter_stages(cfg.stages)},
            lock=threading.RLock(), max_rounds=3,
        )
        self.triage = FindingTriage(chunk_reviews=lambda: self.reviews)
        self.filing = FindingFiler(
            ctx, publish_repo='https://github.com/org/proj',
            triage=self.triage, chunk_reviews=lambda: self.reviews,
        )
        return SecurityRecords(
            ctx, publish_repo='https://github.com/org/proj',
            triage=self.triage, filing=self.filing,
            chunk_reviews=lambda: self.reviews,
        )

    def scan_round(
        self, round_no: int, *, saved: str | None = None, **kw: object
    ) -> ReviewRecord:
        """A scan round's record; *saved* is its JSONL, when kept."""
        folder = f'scans/scan-r{round_no}/r{round_no}-20261115T0900'
        jsonl = f'{folder}/CLAUDE-SECURITY-RESULTS.jsonl'
        if saved is not None:
            self.wt.artifacts[jsonl] = saved
        fields: dict[str, object] = {
            'chunk': 'm0', 'stage': 'scan', 'reviewer': 'sec',
            'round_no': round_no, 'verdict': 'APPROVED', 'target': 'build',
            'attachments': (jsonl, f'{folder}/CLAUDE-SECURITY-RESULTS.md'),
        }
        fields.update(kw)
        rec = ReviewRecord(**fields)
        self.reviews.append(rec)
        return rec


class TestCommittingTheReports(_RecordsTest):
    def test_each_rounds_files_land_in_its_own_folder(self) -> None:
        records = self.records()
        rec = self.scan_round(1)
        for relpath in rec.attachments:
            self.wt.artifacts[relpath] = f'contents of {relpath}'
        folder = records.commit_reports('build', _PLAN)
        self.assertEqual(folder, _FOLDER)
        self.assertEqual(sorted(self.wt.written), [
            f'{_FOLDER}/r1-20261115T0900/CLAUDE-SECURITY-RESULTS.jsonl',
            f'{_FOLDER}/r1-20261115T0900/CLAUDE-SECURITY-RESULTS.md',
        ])
        self.assertEqual(self.wt.commits, ['docs: add security scan reports'])

    def test_a_missing_file_is_reported_and_the_rest_committed(self) -> None:
        records = self.records()
        rec = self.scan_round(1)
        self.wt.artifacts[rec.attachments[1]] = 'the markdown report'
        with mock.patch.object(click, 'echo') as echo:
            self.assertEqual(records.commit_reports('build', _PLAN), _FOLDER)
        printed = ' '.join(str(c.args[0]) for c in echo.call_args_list)
        self.assertIn(f'{rec.attachments[0]} is missing', printed)
        self.assertEqual(list(self.wt.written), [
            f'{_FOLDER}/r1-20261115T0900/CLAUDE-SECURITY-RESULTS.md',
        ])

    def test_no_scan_commits_nothing(self) -> None:
        records = self.records()
        self.assertIsNone(records.commit_reports('build', _PLAN))
        self.assertEqual(self.wt.commits, [])

    def test_nothing_saved_commits_nothing(self) -> None:
        records = self.records()
        self.scan_round(1, attachments=())
        self.assertIsNone(records.commit_reports('build', _PLAN))
        self.assertEqual(self.wt.commits, [])

    def test_a_refused_write_is_reported_not_raised(self) -> None:
        # Best-effort: never worth failing a publish over.
        records = self.records()
        rec = self.scan_round(1)
        self.wt.artifacts[rec.attachments[0]] = _jsonl('LOW')
        self.wt.refuse = True
        self.assertIsNone(records.commit_reports('build', _PLAN))


class TestTheSummary(_RecordsTest):
    def summary(self, records: SecurityRecords) -> str:
        path = records.commit_summary('build', _PLAN, module=None)
        self.assertEqual(path, f'{_FOLDER}/README.md')
        return self.wt.written[path]

    def test_no_scan_commits_no_summary(self) -> None:
        records = self.records()
        self.assertIsNone(records.commit_summary('build', _PLAN, module=None))

    def test_the_title_names_the_module(self) -> None:
        records = self.records()
        self.scan_round(1)
        records.commit_summary(
            'build', _PLAN, module=pipeline.Subtask(id='m0', title='core'),
        )
        doc = self.wt.written[f'{_FOLDER}/README.md']
        self.assertIn('# Security scan — demo [m0] core', doc)

    def test_a_final_round_finding_shows_what_filing_did(self) -> None:
        records = self.records()
        rec = self.scan_round(
            1, findings=('[security-low] a',), defects=('[security-low] a',),
            saved=_jsonl('LOW'),
        )
        self.filing.outcomes[finding_id(rec, 1)] = 'Filed: https://x/1'
        self.assertIn('Filed: https://x/1', self.summary(records))

    def test_an_unread_tracker_says_so(self) -> None:
        records = self.records()
        self.scan_round(
            1, findings=('[security-low] a',), defects=('[security-low] a',),
            saved=_jsonl('LOW'),
        )
        self.assertIn(
            'Not filed: the issue tracker could not be read',
            self.summary(records),
        )

    def test_without_a_tracker_the_ledger_and_triage_are_named(self) -> None:
        records = self.records(mode='local')
        rec = self.scan_round(
            1, findings=('[security-low] a',), defects=('[security-low] a',),
            saved=_jsonl('LOW'),
        )
        self.triage.dispositions[finding_id(rec, 1)] = Disposition(
            'absent', 'checked db.rs',
        )
        self.assertIn(
            'Recorded in the findings ledger; triage: `absent`: '
            'checked db.rs', self.summary(records),
        )

    def test_a_blocking_round_shows_triages_verdict(self) -> None:
        records = self.records()
        rec = self.scan_round(
            1, verdict='BLOCKING', blocking_report=_REPORT,
            saved=_jsonl('HIGH'),
        )
        ident = blocking_finding_id(rec, 1)
        self.triage.dispositions[ident] = Disposition(DISPOSITION_FILED, 'x')
        self.triage.blocking_claims[ident] = 'the name is unescaped'
        self.filing.outcomes[ident] = 'Filed: https://x/2'
        self.assertIn(
            '`reproduces`: the name is unescaped (Filed: https://x/2)',
            self.summary(records),
        )

    def test_a_blocking_round_nobody_concluded_on_points_at_it(self) -> None:
        records = self.records()
        self.scan_round(
            1, verdict='BLOCKING', blocking_report=_REPORT,
            saved=_jsonl('HIGH'),
        )
        self.filing.blocking_summary_url = 'https://x/3'
        self.assertIn(
            'triage reached no conclusion on this report. It is named in '
            'https://x/3.', self.summary(records),
        )

    def test_without_a_triage_stage_it_says_so(self) -> None:
        records = self.records(triage=False)
        self.scan_round(
            1, verdict='BLOCKING', blocking_report=_REPORT,
            saved=_jsonl('HIGH'),
        )
        self.assertIn('not re-checked', self.summary(records))

    def test_a_rounds_findings_are_read_back(self) -> None:
        records = self.records()
        rec = self.scan_round(1)
        self.wt.artifacts[rec.attachments[0]] = _jsonl('HIGH', 'LOW')
        doc = self.summary(records)
        self.assertIn('High problem', doc)
        self.assertIn('Low problem', doc)

    def test_a_refused_write_is_reported_not_raised(self) -> None:
        records = self.records()
        self.scan_round(1)
        self.wt.refuse = True
        self.assertIsNone(records.commit_summary('build', _PLAN, module=None))


if __name__ == '__main__':
    unittest.main()
