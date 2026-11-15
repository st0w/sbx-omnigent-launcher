"""The decisions ledger: what earlier modules settled for later ones.

Run: python -m unittest tests.test_runner_parsing_decisions
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.parsing.decisions import parse_decisions
from tests.support.plans import plan_with_decisions


class TestParseDecisions(unittest.TestCase):
    """A decision reached in one module's session reaches no other
    unless it is lifted out and carried forward."""

    def test_parses_items_in_order(self) -> None:
        text = plan_with_decisions('use ULIDs', 'diff in the DB')
        self.assertEqual(
            parse_decisions(text), ['use ULIDs', 'diff in the DB']
        )

    def test_tolerates_a_decorated_header(self) -> None:
        # Models routinely wrap a requested header in ## or ** —
        # losing the whole block to a stray asterisk would silently
        # drop the decisions it names.
        for head in (
            '## DECISIONS FOR LATER MODULES:',
            '**DECISIONS FOR LATER MODULES:**',
            'decisions for later modules',
        ):
            with self.subTest(head=head):
                self.assertEqual(
                    parse_decisions(plan_with_decisions('x', header=head)),
                    ['x'],
                )

    def test_blank_lines_tolerated_prose_ends_the_block(self) -> None:
        text = (
            'DECISIONS FOR LATER MODULES:\n'
            '- one\n'
            '\n'
            '- two\n'
            'Some closing prose.\n'
            '- not a decision\n'
        )
        self.assertEqual(parse_decisions(text), ['one', 'two'])

    def test_numbered_items_accepted(self) -> None:
        text = 'DECISIONS FOR LATER MODULES:\n1. first\n2) second\n'
        self.assertEqual(parse_decisions(text), ['first', 'second'])

    def test_last_header_wins(self) -> None:
        text = (
            'DECISIONS FOR LATER MODULES:\n- stale\n\nmore prose\n\n'
            'DECISIONS FOR LATER MODULES:\n- current\n'
        )
        self.assertEqual(parse_decisions(text), ['current'])

    def test_absent_or_empty_yields_nothing(self) -> None:
        # A module that settled nothing cross-cutting is the normal
        # case, not a failure.
        self.assertEqual(parse_decisions('just a plan'), [])
        self.assertEqual(parse_decisions(''), [])
        self.assertEqual(parse_decisions(None), [])
        self.assertEqual(
            parse_decisions('DECISIONS FOR LATER MODULES:\n\nprose'), []
        )


if __name__ == '__main__':
    unittest.main()
