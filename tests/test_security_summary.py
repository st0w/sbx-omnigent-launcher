"""The module's security summary: every scan round, and what became of
each finding.

Run: .venv/bin/python -m unittest tests.test_security_summary
"""

from __future__ import annotations

import unittest

from sbx_omnigent import security_scan as ss
from sbx_omnigent import security_summary as S


def _f(fid: str, severity: str, title: str, *, plugin_id: str = '',
       line: int = 10, file: str = 'app.py') -> ss.ScanFinding:
    return ss.ScanFinding(
        id=fid, severity=severity, title=title, category='SQL Injection',
        file=file, line=line, description='d', exploit_scenario='e',
        recommendation='r', cwe_id='CWE-89', plugin_id=plugin_id,
    )


_HIGH = _f('F1', 'HIGH', 'Injection in /user', plugin_id='p-high')
_MED = _f('F2', 'MEDIUM', 'Verbose errors', plugin_id='p-med')
_LOW = _f('F3', 'LOW', 'Missing header', plugin_id='p-low')


def _stage(**kw: object) -> S.ScanStage:
    rounds = kw.pop('rounds', (
        S.ScanRound(1, 'BLOCKING', 'r1-20261114-120000',
                    (_HIGH, _MED, _LOW),
                    triage='`reproduces` — still interpolated (Filed: '
                           'https://gh/o/p/issues/9)'),
        S.ScanRound(2, 'APPROVED', 'r2-20261114-130000', (_MED,)),
    ))
    outcomes = kw.pop('outcomes', ('Filed: https://gh/o/p/issues/7',))
    assert not kw
    return S.ScanStage('scan', tuple(rounds), tuple(outcomes))


def _render(*stages: S.ScanStage) -> str:
    return S.render_summary(title='demo [m1] parse', stages=stages or
                            (_stage(),))


class TestTheRounds(unittest.TestCase):
    def test_each_round_has_its_result_counts_and_report(self) -> None:
        text = _render()
        self.assertIn(
            '| 1 | blocked | 0 | 1 | 1 | 1 | [r1-20261114-120000]'
            '(r1-20261114-120000/) |',
            text,
        )
        self.assertIn(
            '| 2 | passed | 0 | 0 | 1 | 0 | [r2-20261114-130000]'
            '(r2-20261114-130000/) |',
            text,
        )

    def test_an_unreadable_report_is_said_so(self) -> None:
        stage = _stage(rounds=(
            S.ScanRound(1, 'APPROVED', 'r1-1', None),
        ), outcomes=())
        text = _render(stage)
        self.assertIn('| 1 | passed | ? | ? | ? | ? |', text)
        self.assertIn('could not be read', text)


class TestCriticalAndHigh(unittest.TestCase):
    def test_a_fixed_finding_says_when_it_stopped_being_reported(
        self,
    ) -> None:
        text = _render()
        row = next(line for line in text.splitlines()
                   if 'Injection in /user' in line)
        self.assertIn('HIGH', row)
        self.assertIn('`app.py:10`', row)
        self.assertIn('round 1', row)
        self.assertIn('Not reported from round 2', row)

    def test_triage_of_each_blocking_round_is_shown(self) -> None:
        text = _render()
        self.assertIn("Triage's re-check of round 1", text)
        self.assertIn('https://gh/o/p/issues/9', text)

    def test_one_finding_across_rounds_is_one_row(self) -> None:
        later = _f('F7', 'HIGH', 'Injection in /user (reworded)',
                   plugin_id='p-high', line=12)
        stage = _stage(rounds=(
            S.ScanRound(1, 'BLOCKING', 'r1', (_HIGH,), triage='t1'),
            S.ScanRound(2, 'BLOCKING', 'r2', (later,), triage='t2'),
            S.ScanRound(3, 'APPROVED', 'r3', ()),
        ), outcomes=())
        text = _render(stage)
        rows = [
            line for line in text.splitlines() if 'Injection in /user' in line
        ]
        self.assertEqual(len(rows), 1, rows)
        self.assertIn('rounds 1 to 2', rows[0])
        self.assertIn('Not reported from round 3', rows[0])

    def test_without_a_plugin_id_file_and_title_match(self) -> None:
        one = _f('F1', 'HIGH', 'Same', line=5)
        two = _f('F4', 'HIGH', 'Same', line=9)
        stage = _stage(rounds=(
            S.ScanRound(1, 'BLOCKING', 'r1', (one,), triage='t'),
            S.ScanRound(2, 'BLOCKING', 'r2', (two,), triage='t'),
            S.ScanRound(3, 'APPROVED', 'r3', ()),
        ), outcomes=())
        rows = [
            line for line in _render(stage).splitlines() if ': Same |' in line
        ]
        self.assertEqual(len(rows), 1, rows)

    def test_none_says_so(self) -> None:
        stage = _stage(rounds=(
            S.ScanRound(1, 'APPROVED', 'r1', (_MED,)),
        ))
        self.assertIn('No Critical or High finding', _render(stage))


class TestMediumAndLow(unittest.TestCase):
    def test_the_final_rounds_carry_their_outcome(self) -> None:
        text = _render()
        row = next(
            line for line in text.splitlines()
            if 'Verbose errors' in line and 'Filed' in line
        )
        self.assertIn('MEDIUM', row)
        self.assertIn('https://gh/o/p/issues/7', row)

    def test_a_final_finding_shows_every_round_that_reported_it(
        self,
    ) -> None:
        text = _render()
        row = next(
            line for line in text.splitlines()
            if 'Verbose errors' in line and 'Filed' in line
        )
        self.assertIn('rounds 1 to 2', row)

    def test_earlier_rounds_only_are_marked_unfiled(self) -> None:
        text = _render()
        row = next(
            line for line in text.splitlines() if 'Missing header' in line
        )
        self.assertIn('round 1', row)
        self.assertIn('not filed', row)

    def test_a_missing_outcome_is_not_invented(self) -> None:
        stage = _stage(outcomes=())
        row = next(line for line in _render(stage).splitlines()
                   if 'Verbose errors' in line and 'rounds 1 to 2' in line)
        self.assertIn('unknown', row)


class TestSafety(unittest.TestCase):
    def test_table_cells_are_escaped(self) -> None:
        nasty = _f('F1', 'MEDIUM', 'a | b\nc', plugin_id='x')
        stage = _stage(rounds=(
            S.ScanRound(1, 'APPROVED', 'r1', (nasty,)),
        ), outcomes=('Filed: u',))
        text = _render(stage)
        self.assertIn('a \\| b c', text)

    def test_several_scan_stages_each_get_a_section(self) -> None:
        text = _render(_stage(), S.ScanStage('scan-2', (
            S.ScanRound(1, 'APPROVED', 'r1', ()),
        ), ()))
        self.assertIn('## `scan`', text)
        self.assertIn('## `scan-2`', text)


if __name__ == '__main__':
    unittest.main()
