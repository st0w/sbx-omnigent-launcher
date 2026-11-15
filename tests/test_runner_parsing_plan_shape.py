"""A plan of record must look like a design document, not a chat reply.

Run: python -m unittest tests.test_runner_parsing_plan_shape
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.parsing.plans import (
    _PLAN_MIN_CHARS,
    plan_shape_failures,
)
from tests.support.plans import plan_text


class TestTheRecapCheckOnlyReadsTheOpening(unittest.TestCase):
    """
    An acknowledgement LEADS with "the plan is complete". A design plan
    may mention the phrase once, in passing, on page forty.

    The recap patterns were unanchored, so they vetoed a document of any
    size for one substring anywhere in it. Measured on the recovered
    `ingestion-m2-4` plan: 127,683 characters, 52 sections, and the only
    match was at line 907 of 1,003 -- work item 12's "or a written
    statement of why fewer is complete". That plan was rejected as an
    acknowledgement.

    This is very likely what killed that run: the consolidation turn is
    asked to re-emit the whole plan, and the whole plan contains the
    phrase. The `approved` fallback added alongside this does NOT cover
    it -- the approved text carries the same sentence -- so both
    candidates were rejected for the same false positive.

    The original catch is preserved by construction: the recap it was
    built for was 1,774 characters, so it lies entirely inside the
    opening window.
    """

    _RECAP = (
        'The plan has been finalized and APPROVED by the user. The plan '
        'phase is complete. I updated src/core.rs and src/lib.rs.'
    )

    def test_the_live_recap_is_still_rejected(self) -> None:
        why = plan_shape_failures(self._RECAP)
        self.assertTrue(
            any('acknowledgement' in w for w in why),
            f'the recap this check exists for must stay rejected: {why}',
        )

    def test_a_recap_padded_past_the_window_is_still_rejected(self) -> None:
        # Padding an acknowledgement must not buy it a pass; it still
        # fails the structural checks even if the phrase scrolls away.
        why = plan_shape_failures(self._RECAP + ' Thanks. ' * 400)
        self.assertNotEqual(why, [])

    def test_a_long_plan_mentioning_the_phrase_late_passes(self) -> None:
        plan = (
            plan_text(6000)
            + '\n\n## Work items\n'
            + '- Cover each declared service, or give a written statement '
              'of why fewer is complete.\n'
        )
        self.assertEqual(plan_shape_failures(plan), [])

    def test_the_opening_is_still_read(self) -> None:
        # A plan-shaped body cannot rescue an acknowledgement that opens
        # by announcing itself.
        text = 'The plan is now complete.\n\n' + plan_text(6000)
        why = plan_shape_failures(text)
        self.assertTrue(any('acknowledgement' in w for w in why))


class TestASummaryOfAPlanIsNotAPlan(unittest.TestCase):
    """
    A well-formed SUMMARY of a design is not the design.

    The recap check catches a reply that reads as an acknowledgement.
    It does not catch a reply that is genuinely structured — headings,
    bullets, file names, real technical content — but whose substance
    lives in a document it only POINTS AT: "§3 package layout", "§11
    migration 0002", nine such references and not one of those sections
    present.

    That passed every structural check and became the plan of record
    for [m2] attempt 3. Both implementers and the test author were
    handed 3,006 characters of table of contents; the planner's closing
    line was "I wrote nothing to disk".

    The signal is that a plan CONTAINS its sections and a summary
    REFERS to them. Measured over this project's committed plans:

        ingestion-m0                    8 refs, 8 resolved
        ingestion-m0b                   6 refs, 6 resolved
        ingestion-m2-attempt-1         14 refs, 14 resolved
        ingestion-m1 (the acknowledgement)  2 refs, 0 resolved
        m2-3 planner output             9 refs, 0 resolved

    Every real plan resolves every reference it makes. Both failures
    resolve none — including the m1 acknowledgement this module already
    names, found independently by this check.
    """

    @staticmethod
    def _numbered(body: str) -> str:
        return (
            '## 1. Files\n'
            '- src/core.rs — the parser\n'
            '- src/lib.rs — exports it\n'
            '- tests/parse_test.rs — the cases\n\n'
            '## 2. Interfaces\n'
            '`parse` takes a spec and returns whole seconds.\n\n'
            '## 3. Algorithm\n'
            '1. Read the spec\n2. Split the units\n3. Sum them\n\n'
            '## 4. Edge cases\n'
            'An invalid suffix must fail; a malformed spec raises.\n\n'
            '## 5. Test strategy\n'
            'Unit tests cover each fixture.\n\n'
            + body
        )

    def test_a_plan_citing_its_own_sections_passes(self) -> None:
        # The common case: a real plan cross-references itself
        # constantly, and every reference resolves.
        text = self._numbered(
            'The parser in §1 is driven by the algorithm in §3, and '
            '§4 names the failure modes §5 must cover.\n'
        ) + 'Detail. ' * 400
        self.assertEqual(plan_shape_failures(text), [])

    def test_a_summary_citing_absent_sections_is_rejected(self) -> None:
        summary = (
            'Plan of record released.\n\n'
            '## What is frozen for the downstream stages\n'
            '- **Design** — §3 package layout, §4 the core module '
            'interfaces, §6 the six catalogs, §9 transport and the '
            'watermark grammar, §11 migration `0002`.\n'
            '- **Work items 1-25** with mechanical done-criteria.\n'
            '- **Gates G1-G22**, each with its named mutation.\n'
            '- The three closed questions (§19) and the fifteen '
            'departures (§2).\n\n'
            '## Where the tests stage starts\n'
            '- Items 2, 3, 7 need no SDK: the allow-list load in '
            '`catalogs/read_only_sdk_allowlist.yaml`, the transport '
            'protocol in `acme/component/transport.py`, and the '
            'watermark ordering property.\n'
            '- Items 4 and 17 need the pinned fixture on a real '
            'cluster; invalid input must raise.\n\n'
            "I wrote nothing to disk - this session is the plan's "
            'record.\n'
        ) + 'Everything above is settled and frozen. ' * 60
        why = plan_shape_failures(summary)
        self.assertTrue(
            any('section' in w for w in why),
            f'a summary of a plan must be rejected, got {why}',
        )

    def test_the_rejection_says_which_sections_are_missing(self) -> None:
        # The halt has to tell a human what to go get.
        summary = (
            '## Summary\n'
            '- The design is in §3 and §11 of the session.\n'
            '- Files: `src/core.rs`, `src/lib.rs`, `tests/t.rs`.\n'
            '- Steps: 1. read 2. split 3. sum\n'
            '- Edge cases: invalid input raises.\n'
            '- Test strategy: unit tests per fixture.\n'
            '- Interfaces: `parse` returns whole seconds.\n'
            '- Gates: each ships its own mutation.\n'
            '- Work items: 1 through 25, with done-criteria.\n'
            '- Escalation: a frozen edit halts the stage.\n'
        ) + 'Detail. ' * 400
        why = plan_shape_failures(summary)
        joined = ' '.join(why)
        self.assertIn('3', joined)
        self.assertIn('11', joined)

    def test_a_plan_that_cites_no_sections_is_unaffected(self) -> None:
        # Most plans never write "§" at all. The check must not invent
        # a requirement to number your headings.
        self.assertEqual(plan_shape_failures(plan_text(4000)), [])

    def test_a_mostly_resolving_plan_passes(self) -> None:
        # A real plan may cite the BRIEF's section numbering alongside
        # its own. Only a document that resolves NOTHING is a summary.
        text = self._numbered(
            'Per §7 of the brief, the parser in §1 must reject a '
            'malformed spec, and §3 says how.\n'
        ) + 'Detail. ' * 400
        self.assertEqual(plan_shape_failures(text), [])


class TestPlanShapeCheck(unittest.TestCase):
    """A plan is recognised by SHAPE, never by length."""

    def test_a_real_plan_passes(self) -> None:
        self.assertEqual(plan_shape_failures(plan_text(4000)), [])

    def test_the_live_recap_is_rejected(self) -> None:
        # Verbatim shape of the reply that became the design of record
        # for an entire Kubernetes provider. It cleared the old
        # 1500-char floor by 274 characters.
        recap = (
            'The design plan for **`[m4] Kubernetes provider`** has been '
            'finalized and **APPROVED** by the user.\n\n'
            '### Final Summary of the Approved Design Plan\n\n'
            '1. Module architecture\n2. Authentication\n3. Scope '
            'hierarchy\n4. Identities\n\n'
            'The plan phase for module `[m4]` is complete.\n'
        ) + ('padding to clear the old length floor. ' * 40)
        why = plan_shape_failures(recap)
        self.assertTrue(why)
        self.assertTrue(
            any('acknowledgement' in w for w in why), why
        )

    def test_length_alone_is_never_enough(self) -> None:
        # The whole point: a long reply that is not a design fails.
        why = plan_shape_failures('words. ' * 3000)
        self.assertTrue(why)

    def test_a_plan_that_names_no_files_is_rejected(self) -> None:
        # "The exact files to add or change" is the FIRST thing
        # templates/planner.md asks for.
        text = plan_text(4000).replace('src/core.rs', 'the core module')
        text = text.replace('src/lib.rs', 'the library root')
        text = text.replace('tests/parse_test.rs', 'the parser tests')
        self.assertTrue(
            any('names' in w and 'file' in w
                for w in plan_shape_failures(text))
        )

    def test_the_ingestion_m1_acknowledgement_is_rejected(self) -> None:
        """The reply that became an entire module's plan of record.

        Verbatim shape, and it beat every check that existed: 1,800
        characters against a 1,500 floor, it names files so the file
        check passes, and it scores exactly the three word-level
        signals needed — because an acknowledgement ABOUT a plan reuses
        the plan's own vocabulary. What it does not do is decompose.
        """
        ack = (
            'Released as the plan of record for [m1].\n\n'
            '**No files written.** The plan stage is interactive and '
            'read-only, so the document lives in this transcript for the '
            'pipeline to publish as `docs/plans/ingestion-m1.md`; I did '
            'not create it, and I touched nothing in the worktree.\n\n'
            '**What the tests stage inherits.** Thirteen work items, each '
            'with done-criteria written to be turned straight into '
            'failing tests. Three properties must fail when the property '
            'is removed: deleting the ledger insert must break '
            'idempotency, moving the checkpoint advance out of the '
            'transaction must break crash recovery, and a denied-event '
            'fixture must never produce a tuple.\n\n'
            '**Four frozen-artifact changes are pre-authorised** and no '
            'others: the `tests/unit/test_models.py` discovery rewrite, '
            'the `tests/support/schema.py` consistency check, the '
            '`acme/component/sdk_allowlist.py` fixes, and the '
            '`pyproject.toml` promotion. Anything else frozen that '
            'appears to need changing is a halt-and-escalate.\n\n'
            '**The single item worth disproportionate attention** is the '
            'confinement of permission-string construction. Everything '
            'else can be repaired by a later module; a vocabulary '
            'divergence cannot. If the tests stage writes only one scan '
            'test carefully, that is the one.\n\n'
            'The one thing I did persist is outside the repo: a memory '
            'note recording that the runner matches guarded paths with '
            '`fnmatch` rather than `glob`, since `runner.py` is not in '
            'this repository and that fact cannot be re-derived here. '
            'Every work item is mapped to at least one test file, and '
            'the done-criteria are written so the test author can turn '
            'each straight into a failing test rather than interpreting '
            'it. Nothing else in the worktree was touched.\n'
        )
        self.assertGreater(len(ack), _PLAN_MIN_CHARS)

        why = plan_shape_failures(ack)

        self.assertTrue(why, 'the acknowledgement was accepted as a plan')
        self.assertTrue(
            any('heading/bullet/step' in w for w in why), why
        )

    def test_prose_alone_is_not_a_plan_however_long(self) -> None:
        # The same failure in its general form: a reply carrying every
        # word-level signal but no structure is not decomposed, so it
        # cannot be handed to a test author as a specification.
        flat = '\n'.join(
            line.lstrip('#-* 0123456789.)')
            for line in plan_text(6000).splitlines()
        )

        why = plan_shape_failures(flat)

        self.assertTrue(
            any('heading/bullet/step' in w for w in why), why
        )

    def test_a_plan_with_sections_clears_the_structure_floor(self) -> None:
        # The other side of it: the check must not reject a real design.
        # Every published plan on this project carries 100+ such lines.
        self.assertEqual(plan_shape_failures(plan_text(4000)), [])

    def test_empty_is_rejected(self) -> None:
        self.assertEqual(plan_shape_failures(''), ['it is empty'])
        self.assertEqual(plan_shape_failures(None), ['it is empty'])


if __name__ == '__main__':
    unittest.main()
