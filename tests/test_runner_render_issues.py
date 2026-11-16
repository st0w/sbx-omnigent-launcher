"""The ledger of findings, and the issues that summarise triage.

Run: python -m unittest tests.test_runner_render_issues
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.parsing.dispositions import (
    DISPOSITION_ABSENT,
    DISPOSITION_RECORDED,
    Disposition,
)
from sbx_omnigent.runner.records.findings import finding_id
from sbx_omnigent.runner.render.issues import (
    render_blocking_summary,
    render_findings_ledger,
    render_triage_summary,
    verdict_tally,
)
from tests.support.reviews import review_record


class TestTheFindingsLedgerIsAppendOnly(unittest.TestCase):
    """
    The ledger is a HUMAN-EDITED artifact: a person annotates status and
    reasoning on it, across modules and across runs. So the runner reads
    what is on the branch and adds to it. A renderer that rebuilt the
    document each run would clobber exactly what it exists to hold.
    """

    def _render(self, records, existing=None):
        return render_findings_ledger(
            records, title='Findings — demo', existing=existing,
            report_doc='docs/plans/demo-reviews.md',
        )

    def test_a_first_run_creates_the_document(self) -> None:
        doc = self._render([review_record(findings=['pagination is missing'])])
        self.assertIn('# Findings — demo', doc)
        self.assertIn('pagination is missing', doc)
        self.assertIn('**Status:** open', doc)

    def test_the_entry_says_which_candidate_and_which_reviewer(
        self,
    ) -> None:
        # A finding without the candidate it was raised against is
        # meaningless: two are reviewed per chunk.
        doc = self._render([review_record(target='impl-b', findings=['x'])])
        self.assertIn('`impl-b`', doc)
        self.assertIn('`bugs`', doc)
        self.assertIn('round 1', doc)

    def test_existing_content_is_carried_through_byte_for_byte(
        self,
    ) -> None:
        # THE critical property. Not "looks similar" — a prefix match on
        # the actual bytes.
        human = (
            '# Findings — demo\n\nMy own notes here.\n\n---\n\n'
            '## `[topology/impl-a/bugs/r1#1]`\n\n'
            '**Status:** accepted — fixing in [identities]\n\n'
            'pagination is missing\n'
        )
        doc = self._render(
            [review_record(findings=['pagination is missing']),
             review_record(reviewer='sec', findings=['a second, new one'])],
            existing=human,
        )
        self.assertTrue(
            doc.startswith(human.rstrip('\n')),
            "the human's existing bytes were not preserved",
        )
        self.assertIn('accepted — fixing in [identities]', doc)
        self.assertIn('a second, new one', doc)

    def test_a_finding_already_present_is_not_added_again(self) -> None:
        rec = review_record(findings=['pagination is missing'])
        first = self._render([rec])
        again = self._render([rec], existing=first)
        self.assertIsNone(again, 'a re-publish duplicated an entry')

    def test_an_id_the_human_moved_still_counts_as_present(self) -> None:
        # Substring match on the id, so reorganising the file — which is
        # the point of it being editable — never resurrects an entry.
        rec = review_record(findings=['pagination is missing'])
        ident = finding_id(rec, 1)
        scrambled = f'notes\n\n## Later\n\nsee `[{ident}]` — handled\n'
        self.assertIsNone(self._render([rec], existing=scrambled))

    def test_the_same_issue_from_two_reviewers_is_kept_twice(self) -> None:
        # Duplicates are provenance. Merging them is a judgement
        # call and it is the human's, not the runner's.
        doc = self._render([
            review_record(reviewer='bugs', findings=['pagination is missing']),
            review_record(reviewer='sec', findings=['pagination is missing']),
        ])
        self.assertEqual(doc.count('pagination is missing'), 2)

    def test_a_re_raise_in_a_later_round_is_a_separate_entry(self) -> None:
        # A finding raised again after a round of work is a different
        # event, not a duplicate to swallow.
        doc = self._render([
            review_record(round_no=1, findings=['still unpaginated']),
            review_record(round_no=2, findings=['still unpaginated']),
        ])
        self.assertIn('r1#1', doc)
        self.assertIn('r2#1', doc)

    def test_nothing_to_add_writes_nothing(self) -> None:
        self.assertIsNone(self._render([]))
        self.assertIsNone(self._render([review_record(findings=[])]))

    def test_the_header_says_the_runner_will_not_clobber_it(self) -> None:
        # The invitation to edit is the feature; without it a human has
        # no reason to trust their annotations will survive.
        doc = self._render([review_record(findings=['x'])])
        self.assertIn('only ever APPENDS', doc)
        self.assertIn('TRACKED, not fixed', doc)


class TestTheTriageSummary(unittest.TestCase):
    """One issue naming every finding triage kept out, and why: more
    provenance than a filed finding gets, not less."""

    def withheld(self):
        rec = review_record(reviewer='sec', round_no=2, chunk='m0')
        return [
            (rec, 'm0/impl-a/sec/r2#1', 'ports are unchecked',
             Disposition(DISPOSITION_ABSENT, 'checked ports.rs:40')),
            (rec, 'm0/impl-a/sec/r2#2', 'no retry',
             Disposition(DISPOSITION_RECORDED, '')),
        ]

    def test_the_tally_counts_each_verdict(self) -> None:
        self.assertEqual(
            verdict_tally(self.withheld()), '1 absent, 1 recorded',
        )

    def test_the_title_names_the_module_and_count(self) -> None:
        title, _body = render_triage_summary(
            self.withheld(), chunk='m0', report_doc=None,
        )
        self.assertEqual(
            title, '[m0] 2 finding(s) verified against the code and withheld',
        )

    def test_each_finding_carries_its_verdict_reason_and_id(self) -> None:
        _title, body = render_triage_summary(
            self.withheld(), chunk='m0', report_doc='docs/plans/r.md',
        )
        self.assertIn('### `absent` — ports are unchecked', body)
        self.assertIn('- Verifier said: checked ports.rs:40', body)
        self.assertIn('- Verifier said: _no reason given_', body)
        self.assertIn('- Finding id: `m0/impl-a/sec/r2#1`', body)
        self.assertIn('`docs/plans/r.md`', body)
        self.assertIn('the finding is still live', body)


class TestTheBlockingSummary(unittest.TestCase):
    def test_each_unconcluded_vote_is_named_with_its_ids(self) -> None:
        rec = review_record(reviewer='bugs', chunk='m0', verdict='BLOCKING')
        title, body = render_blocking_summary(
            [rec], chunk='m0', report_doc=None,
        )
        self.assertEqual(
            title, '[m0] 1 blocking report(s) reached no conclusion',
        )
        self.assertIn(
            '- `bugs` blocked `impl-a` in round 1 (ids '
            '`m0/impl-a/bugs/r1#B*`)', body,
        )


if __name__ == '__main__':
    unittest.main()
