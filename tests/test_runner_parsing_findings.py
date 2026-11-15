"""Reviewer findings, sorted by the section they were reported under.

Run: python -m unittest tests.test_runner_parsing_findings
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.parsing.findings import (
    FindingSections,
    parse_findings,
)


class TestFindingSections(unittest.TestCase):
    """
    Sorting the non-blocking output by what each item IS.

    The pre-split blob asked for three different things at once and only
    the first was a defect. Sorting them where they are written is what
    lets a later-increment item be ROUTED to the planner that acts on
    it, and a premise be VERIFIED, instead of all three landing in one
    tracker for a human to re-read and re-classify.
    """

    def test_each_block_is_lifted_into_its_own_section(self) -> None:
        text = (
            'Looks right.\n\n'
            'DEFECTS:\n- the retry budget is undocumented\n\n'
            'LATER-INCREMENT:\n- pagination belongs to [m2]\n\n'
            'PREMISES:\n- I assumed build_providers cannot fail\n\n'
            'VERDICT: APPROVED'
        )
        got = FindingSections.of(text)
        self.assertEqual(got.defects, ('the retry budget is undocumented',))
        self.assertEqual(got.later_increment, ('pagination belongs to [m2]',))
        self.assertEqual(
            got.premises, ('I assumed build_providers cannot fail',)
        )

    def test_a_legacy_findings_block_is_read_as_defects(self) -> None:
        # The loudest bucket, deliberately. A reviewer that ignores the
        # new protocol, or a reply written before it existed, must lose
        # nothing, so an uncategorized item gets the MOST attention.
        got = FindingSections.of('FINDINGS:\n- something is wrong')
        self.assertEqual(got.defects, ('something is wrong',))
        self.assertEqual(got.later_increment, ())
        self.assertEqual(got.premises, ())

    def test_all_is_the_union_and_puts_defects_first(self) -> None:
        text = (
            'PREMISES:\n- p\n\n'
            'LATER-INCREMENT:\n- l\n\n'
            'DEFECTS:\n- d\n'
        )
        self.assertEqual(FindingSections.of(text).all, ('d', 'l', 'p'))

    def test_a_missing_block_is_empty_not_an_error(self) -> None:
        got = FindingSections.of('DEFECTS:\n- only this one')
        self.assertEqual(got.defects, ('only this one',))
        self.assertEqual(got.all, ('only this one',))

    def test_nothing_raised_yields_all_empty(self) -> None:
        for text in ('', None, 'VERDICT: APPROVED'):
            with self.subTest(text=text):
                self.assertEqual(FindingSections.of(text).all, ())

    def test_the_headers_may_be_decorated_like_every_other_marker(
        self,
    ) -> None:
        for header in ('LATER-INCREMENT:', '## Later-Increment',
                       '**LATER_INCREMENT:**', 'later increment'):
            with self.subTest(header=header):
                self.assertEqual(
                    FindingSections.of(f'{header}\n- x').later_increment,
                    ('x',),
                )

    def test_a_legacy_block_and_a_defects_block_both_count(self) -> None:
        # A reviewer that writes both is not made to choose; neither
        # list is dropped.
        got = FindingSections.of('DEFECTS:\n- a\n\nFINDINGS:\n- b')
        self.assertEqual(got.defects, ('a', 'b'))


class TestParseFindings(unittest.TestCase):
    """
    A reviewer's NON-BLOCKING findings are the lossy set: blocking ones
    are acted on in-round, these are archived and never read again.
    Lifted out with the same marker protocol as `VERDICT:` and
    `DECISIONS FOR LATER MODULES:` rather than by parsing prose.
    """

    def test_items_are_lifted_in_order(self) -> None:
        text = (
            'Looks fine overall.\n\n'
            'FINDINGS:\n'
            '- ListServiceAccounts is unpaginated (belongs to [identities])\n'
            '- the retry budget is not documented\n\n'
            'VERDICT: APPROVED'
        )
        self.assertEqual(
            parse_findings(text),
            ('ListServiceAccounts is unpaginated (belongs to [identities])',
             'the retry budget is not documented'),
        )

    def test_numbered_and_bulleted_items_both_count(self) -> None:
        text = 'FINDINGS:\n1. first\n2) second\n* third\n• fourth'
        self.assertEqual(
            parse_findings(text), ('first', 'second', 'third', 'fourth')
        )

    def test_the_last_block_wins(self) -> None:
        # Same rule as parse_decisions: a reviewer that restates
        # its list after thinking again means the later one.
        text = 'FINDINGS:\n- early\n\nOn reflection.\n\nFINDINGS:\n- late'
        self.assertEqual(parse_findings(text), ('late',))

    def test_blank_lines_inside_the_list_are_tolerated(self) -> None:
        text = 'FINDINGS:\n- one\n\n- two'
        self.assertEqual(parse_findings(text), ('one', 'two'))

    def test_prose_after_the_list_ends_it(self) -> None:
        text = 'FINDINGS:\n- one\nThat is all.\n- not a finding'
        self.assertEqual(parse_findings(text), ('one',))

    def test_the_header_may_be_decorated(self) -> None:
        for header in ('FINDINGS:', '## FINDINGS', '**FINDINGS:**',
                       '  findings :  '):
            with self.subTest(header=header):
                self.assertEqual(
                    parse_findings(f'{header}\n- x'), ('x',)
                )

    def test_no_block_yields_nothing(self) -> None:
        # NOT an error: a reviewer with nothing to raise is the normal
        # case. The caller falls back to keeping the whole report.
        for text in ('', None, 'VERDICT: APPROVED', 'findings were none'):
            with self.subTest(text=text):
                self.assertEqual(parse_findings(text), ())

    def test_an_empty_block_yields_nothing(self) -> None:
        self.assertEqual(parse_findings('FINDINGS:\n\nVERDICT: OK'), ())


if __name__ == '__main__':
    unittest.main()
