"""Reading a claude-security scan's results for the scan stage.

The fields are the plugin's JSONL record (claude-security 0.12.0,
``scripts/lib/finding.py``), one finding per line.

Run: .venv/bin/python -m unittest tests.test_security_scan
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

from sbx_omnigent import security_scan as ss


def _finding(fid: str, severity: str, **extra: object) -> dict[str, object]:
    record: dict[str, object] = {
        'id': fid,
        'title': f'{severity.title()} thing in /user',
        'impact': 'reads every user',
        'file': 'app.py',
        'line': 16,
        'description': 'name is interpolated into the query.',
        'exploit_scenario': "GET /user?name=' OR '1'='1",
        'preconditions': [],
        'category': 'SQL Injection',
        'severity': severity,
        'confidence': 'high',
        'recommendation': 'Use a bound parameter.',
        'cwe_id': 'CWE-89',
        'snippet': 'f"SELECT ... {name}"',
        'symbol': 'user',
        'declared_line': 14,
        'via_change': None,
        'other_cwe_ids': [],
        'claudeSecurityPluginFindingId': f'abc-{fid}',
    }
    record.update(extra)
    return record


def _jsonl(*records: dict[str, object]) -> str:
    return ''.join(json.dumps(r) + '\n' for r in records)


class TestTheReportIsParsed(unittest.TestCase):
    def test_each_line_is_a_finding(self) -> None:
        found = ss.parse_report(_jsonl(
            _finding('F1', 'CRITICAL'), _finding('F2', 'LOW')
        ))
        self.assertEqual(
            [(f.id, f.severity, f.file, f.line) for f in found],
            [('F1', 'CRITICAL', 'app.py', 16), ('F2', 'LOW', 'app.py', 16)],
        )

    def test_an_empty_report_is_no_findings(self) -> None:
        self.assertEqual(ss.parse_report(''), ())

    def test_critical_and_high_block_medium_and_low_do_not(self) -> None:
        found = ss.parse_report(_jsonl(*(
            _finding(f'F{i}', sev)
            for i, sev in enumerate(('CRITICAL', 'HIGH', 'MEDIUM', 'LOW'))
        )))
        self.assertEqual(
            [f.blocks for f in found], [True, True, False, False]
        )

    def test_severity_reads_in_any_case(self) -> None:
        self.assertEqual(
            ss.parse_report(_jsonl(_finding('F1', 'high')))[0].severity,
            'HIGH',
        )

    def test_a_malformed_report_is_refused(self) -> None:
        for text in ('not json\n', '[1, 2]\n',
                     _jsonl(_finding('F1', 'SEVERE')),
                     _jsonl({'id': 'F1', 'severity': 'HIGH'})):
            with self.subTest(text=text[:30]):
                with self.assertRaises(ss.ScanReportError):
                    ss.parse_report(text)

    def test_the_severity_label(self) -> None:
        found = ss.parse_report(_jsonl(_finding('F1', 'MEDIUM')))
        self.assertEqual(found[0].label, 'security-medium')


class TestTheReportIsCollected(unittest.TestCase):
    """The scan's whole report folder is read out of the clone before
    the clone goes, refusing anything that is not a plain file in it."""

    def setUp(self) -> None:
        self.wt = tempfile.mkdtemp(prefix='scan-wt-')
        self.addCleanup(shutil.rmtree, self.wt, True)

    def _dir(self, stamp: str) -> str:
        d = os.path.join(self.wt, f'CLAUDE-SECURITY-{stamp}')
        os.makedirs(d, exist_ok=True)
        return d

    def _write(self, stamp: str, name: str, text: str = 'x\n') -> str:
        path = os.path.join(self._dir(stamp), name)
        with open(path, 'w', encoding='utf-8') as fh:
            fh.write(text)
        return path

    def _full(self, stamp: str = '20261114-120000') -> None:
        self._write(stamp, ss.RESULTS_NAME, 'findings\n')
        self._write(stamp, 'CLAUDE-SECURITY-RESULTS.md', '# report\n')
        self._write(stamp, 'CLAUDE-SECURITY-RESULTS.sarif', '{}\n')
        self._write(stamp, 'CLAUDE-SECURITY-REVISION-0123456789ab.json', '{}')

    def test_none_when_the_scan_wrote_nothing(self) -> None:
        self.assertIsNone(ss.collect_report(self.wt))

    def test_the_report_files_and_the_revision_stamp_are_kept(self) -> None:
        self._full()
        report = ss.collect_report(self.wt)
        assert report is not None
        self.assertEqual(
            sorted(name for name, _text in report.files),
            [
                'CLAUDE-SECURITY-RESULTS.jsonl',
                'CLAUDE-SECURITY-RESULTS.md',
                'CLAUDE-SECURITY-RESULTS.sarif',
                'CLAUDE-SECURITY-REVISION-0123456789ab.json',
            ],
        )
        self.assertEqual(report.results, 'findings\n')
        self.assertEqual(report.stamp, '20261114-120000')

    def test_a_dirty_revision_stamp_is_kept(self) -> None:
        self._full()
        self._write('20261114-120000',
                    'CLAUDE-SECURITY-REVISION-0123456789ab-dirty.json')
        report = ss.collect_report(self.wt)
        assert report is not None
        self.assertIn(
            'CLAUDE-SECURITY-REVISION-0123456789ab-dirty.json',
            [name for name, _text in report.files],
        )

    def test_other_files_are_left_behind(self) -> None:
        self._full()
        self._write('20261114-120000', '.gitignore', '*\n')
        self._write('20261114-120000', 'notes.txt')
        report = ss.collect_report(self.wt)
        assert report is not None
        names = [name for name, _text in report.files]
        self.assertNotIn('.gitignore', names)
        self.assertNotIn('notes.txt', names)

    def test_the_newest_report_is_read(self) -> None:
        self._write('20261114-120000', ss.RESULTS_NAME, 'older\n')
        self._write('20261114-130000', ss.RESULTS_NAME, 'newer\n')
        report = ss.collect_report(self.wt)
        assert report is not None
        self.assertEqual(report.results, 'newer\n')

    def test_a_folder_without_results_is_skipped(self) -> None:
        self._write('20261114-120000', ss.RESULTS_NAME, 'real\n')
        self._write('20261114-130000', 'CLAUDE-SECURITY-RESULTS.md')
        report = ss.collect_report(self.wt)
        assert report is not None
        self.assertEqual(report.results, 'real\n')

    def test_a_symlinked_file_is_refused(self) -> None:
        # The clone is the agent's to write: a link could point at any
        # file on the host, which would then be committed.
        secret = os.path.join(self.wt, 'outside.txt')
        with open(secret, 'w', encoding='utf-8') as fh:
            fh.write('host secret\n')
        self._write('20261114-120000', ss.RESULTS_NAME)
        os.symlink(secret, os.path.join(
            self._dir('20261114-120000'), 'CLAUDE-SECURITY-RESULTS.md'
        ))
        with self.assertRaises(ss.ScanReportError) as caught:
            ss.collect_report(self.wt)
        self.assertIn('symlink', str(caught.exception))

    def test_a_symlinked_results_file_is_refused(self) -> None:
        secret = os.path.join(self.wt, 'outside.txt')
        with open(secret, 'w', encoding='utf-8') as fh:
            fh.write('host secret\n')
        os.symlink(secret, os.path.join(
            self._dir('20261114-120000'), ss.RESULTS_NAME
        ))
        with self.assertRaises(ss.ScanReportError):
            ss.collect_report(self.wt)

    def test_a_symlinked_folder_is_refused(self) -> None:
        elsewhere = tempfile.mkdtemp(prefix='scan-elsewhere-')
        self.addCleanup(shutil.rmtree, elsewhere, True)
        with open(os.path.join(elsewhere, ss.RESULTS_NAME), 'w') as fh:
            fh.write('x\n')
        os.symlink(elsewhere, os.path.join(
            self.wt, 'CLAUDE-SECURITY-20261114-120000'
        ))
        with self.assertRaises(ss.ScanReportError) as caught:
            ss.collect_report(self.wt)
        self.assertIn('symlink', str(caught.exception))

    def test_a_file_that_is_not_regular_is_refused(self) -> None:
        self._write('20261114-120000', ss.RESULTS_NAME)
        os.mkfifo(os.path.join(
            self._dir('20261114-120000'), 'CLAUDE-SECURITY-RESULTS.sarif'
        ))
        with self.assertRaises(ss.ScanReportError):
            ss.collect_report(self.wt)

    def test_an_oversized_file_is_refused(self) -> None:
        self._write('20261114-120000', ss.RESULTS_NAME, 'x' * 100)
        with mock.patch.object(ss, 'MAX_FILE_BYTES', 10):
            with self.assertRaises(ss.ScanReportError) as caught:
                ss.collect_report(self.wt)
        self.assertIn('larger', str(caught.exception))

    def test_a_file_that_is_not_utf8_is_refused(self) -> None:
        path = os.path.join(self._dir('20261114-120000'), ss.RESULTS_NAME)
        with open(path, 'wb') as fh:
            fh.write(b'\xff\xfe\x00')
        with self.assertRaises(ss.ScanReportError):
            ss.collect_report(self.wt)

    def test_an_odd_folder_name_is_ignored(self) -> None:
        self._write('20261114 120000', ss.RESULTS_NAME)
        self.assertIsNone(ss.collect_report(self.wt))


class TestTheTurn(unittest.TestCase):
    def test_it_names_the_base_effort_and_acknowledges_the_cost(self) -> None:
        text = ss.instruction(base='0123abc', effort='medium')
        self.assertTrue(text.startswith('/claude-security:claude-security '))
        self.assertIn('scan changes --base 0123abc', text)
        self.assertIn('at medium effort', text)
        self.assertIn('do not prompt me', text)
        self.assertIn('time and tokens', text)

    def test_a_base_that_is_not_a_commit_is_refused(self) -> None:
        for base in ('main; rm -rf /', '', 'HEAD~1', 'g' * 40):
            with self.subTest(base=base):
                with self.assertRaises(ValueError):
                    ss.instruction(base=base, effort='medium')

    def test_an_unknown_effort_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            ss.instruction(base='0123abc', effort='max')


class TestRendering(unittest.TestCase):
    def setUp(self) -> None:
        self.found = ss.parse_report(_jsonl(
            _finding('F1', 'CRITICAL', title='Command injection in /ping',
                     line=23, category='OS Command Injection',
                     cwe_id='CWE-78'),
            _finding('F2', 'MEDIUM', title='Verbose errors'),
        ))

    def test_blocking_findings_name_file_line_exploit_and_fix(self) -> None:
        text = ss.render_blocking([self.found[0]])
        for part in ('F1', 'CRITICAL', 'Command injection in /ping',
                     'app.py:23', 'CWE-78', "GET /user?name=' OR '1'='1",
                     'Use a bound parameter.'):
            with self.subTest(part=part):
                self.assertIn(part, text)

    def test_a_defect_is_one_tagged_line(self) -> None:
        line = ss.render_defect(self.found[1])
        self.assertTrue(line.startswith('[security-medium] '))
        self.assertIn('app.py:16', line)
        self.assertNotIn('\n', line)

    def test_the_report_lists_every_finding_and_the_scan(self) -> None:
        text = ss.render_report(
            self.found, base='0123abc', effort='medium', round_no=2
        )
        for part in ('F1', 'F2', '0123abc', 'medium', 'round 2'):
            with self.subTest(part=part):
                self.assertIn(part, text)

    def test_a_clean_report_says_so(self) -> None:
        text = ss.render_report((), base='0123abc', effort='low', round_no=1)
        self.assertIn('no findings', text)


if __name__ == '__main__':
    unittest.main()
