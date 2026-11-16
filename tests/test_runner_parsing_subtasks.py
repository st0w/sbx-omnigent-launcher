"""The modules a planner proposes, read from its plan.

Run: python -m unittest tests.test_runner_parsing_subtasks
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.parsing.plans import parse_subtasks


class TestParseSubtasks(unittest.TestCase):
    def test_parse_subtasks_well_formed(self) -> None:
        plan = (
            '# Design\n\nsome prose\n\n'
            'SUBTASKS:\n'
            '- [core] contracts and core skeleton\n'
            '- [ingest] ingestion pipeline\n'
            '- [query] query engine\n'
        )
        subs = parse_subtasks(plan)
        self.assertEqual([s.id for s in subs], ['core', 'ingest', 'query'])
        self.assertEqual(subs[0].title, 'contracts and core skeleton')

    def test_parse_subtasks_numbered_and_blank_lines(self) -> None:
        plan = (
            'SUBTASKS:\n'
            '1. [m0] first\n'
            '\n'
            '2. [m1] second\n'
        )
        self.assertEqual([s.id for s in parse_subtasks(plan)], ['m0', 'm1'])

    def test_parse_subtasks_stops_at_trailing_prose(self) -> None:
        plan = (
            'SUBTASKS:\n'
            '- [a] first\n'
            'That concludes the plan; ship them in order.\n'
            '- [b] not part of the list\n'
        )
        self.assertEqual([s.id for s in parse_subtasks(plan)], ['a'])

    def test_parse_subtasks_sanitizes_and_dedupes_ids(self) -> None:
        plan = (
            'SUBTASKS:\n'
            '- [Core Module!] one\n'
            '- [Core Module?] two\n'   # sanitizes to the same id
        )
        ids = [s.id for s in parse_subtasks(plan)]
        self.assertEqual(ids, ['core-module', 'core-module-2'])

    def test_parse_subtasks_last_header_wins(self) -> None:
        plan = (
            'SUBTASKS:\n- [old] stale\n\ndiscarded prose\n\n'
            'SUBTASKS:\n- [new] real\n'
        )
        self.assertEqual([s.id for s in parse_subtasks(plan)], ['new'])

    def test_parse_subtasks_empty_and_missing(self) -> None:
        self.assertEqual(parse_subtasks(None), [])
        self.assertEqual(parse_subtasks(''), [])
        self.assertEqual(parse_subtasks('# plan\nno subtasks block'), [])
        self.assertEqual(parse_subtasks('SUBTASKS:\nnot an item\n'), [])


if __name__ == '__main__':
    unittest.main()
