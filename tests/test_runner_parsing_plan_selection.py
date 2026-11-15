"""Choosing the plan of record from a planning session's replies.

Run: python -m unittest tests.test_runner_parsing_plan_selection
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.parsing.decisions import parse_decisions
from sbx_omnigent.runner.parsing.plans import (
    plan_shape_failures,
    select_plan_of_record,
)
from tests.support.plans import plan_text


class TestARealPlanSurvivesTheWholeHandoff(unittest.TestCase):
    """
    One regression test for the path that has cost four planning cycles.

    Every plan-side defect found so far -- a summary accepted as a
    design, a recap pattern vetoing a 127KB plan for one substring, the
    approved text discarded, a horizontal rule lifted as a decision --
    was found by LOSING A CAMPAIGN, because nothing exercised the
    handoff with a plan the size and shape a real planner emits.

    So this builds one: numbered sections with cross-references, many
    named files, a DECISIONS block closed by a rule, and the phrase
    "is complete" used legitimately deep in the body. It then walks the
    whole path a plan takes between the approval gate and the builders.

    A change that would reject or mangle a real plan now fails here, in
    a second, instead of thirty minutes into a campaign.
    """

    @staticmethod
    def _realistic_plan() -> str:
        body = [plan_text(3000), '']
        for n in range(1, 18):
            body += [
                f'## {n}. Section {n}',
                f'Covered in §{max(1, n - 1)}; see also §{min(17, n + 1)}.',
                f'- `acme/component/mod{n}.py` — the {n}th module',
                f'- `tests/unit/test_mod{n}.py` — its cases',
                'An invalid input must raise; the test author verifies each.',
                '',
            ]
        body += [
            '## Work items',
            '- Cover each declared service, or give a written statement of '
            'why fewer is complete.',
            '',
            '## DECISIONS FOR LATER MODULES:',
            '',
            '- **The transport owns its watermark grammar.** An adapter '
            'must never parse one.',
            '- **Attestations never enter the success-only table.** They '
            'are a separate evidence kind.',
            '',
            '---',
            '',
            '## QUESTIONS:',
            'None; everything above is settled.',
        ]
        return '\n'.join(body)

    def test_it_is_recognised_as_a_plan(self) -> None:
        self.assertEqual(plan_shape_failures(self._realistic_plan()), [])

    def test_it_survives_selection_with_nothing_else_readable(self) -> None:
        # The live m2-4 shape: an ack consolidation and an empty reply
        # read. The approved text must come back intact.
        plan = self._realistic_plan()
        self.assertEqual(
            select_plan_of_record('The plan is complete.', [], plan), plan
        )

    def test_its_decisions_lift_cleanly(self) -> None:
        got = parse_decisions(self._realistic_plan())
        self.assertEqual(len(got), 2)
        self.assertTrue(
            all(len(d) > 40 for d in got),
            f'a rule or separator was lifted as a decision: {got}',
        )

    def test_a_horizontal_rule_does_not_become_a_decision(self) -> None:
        # `---` closing a DECISIONS block parsed as a bullet whose text
        # is `--`, which would land verbatim in the committed ledger
        # that [m3]-[m7] read.
        text = (
            'DECISIONS FOR LATER MODULES:\n'
            '- **A real decision.** With a reason attached to it.\n'
            '---\n'
            '## Next section\n'
        )
        self.assertEqual(len(parse_decisions(text)), 1)

    def test_it_is_large_enough_to_be_representative(self) -> None:
        # The defects only appear at real scale; a 400-char fixture
        # would have passed every one of them.
        self.assertGreater(len(self._realistic_plan()), 5000)


class TestTheApprovedTextIsNeverDiscarded(unittest.TestCase):
    """
    The text the HUMAN approved is always a candidate plan of record.

    `_await_plan_approval` already holds it: `wait_for_plan_approval`
    returns the last assistant message before the approval turn, which
    is the document the human read and said APPROVED to. It was then
    passed to nothing, and `select_plan_of_record` judged only the
    consolidation reply and a SECOND read of the session.

    Cost, live, on `ingestion-m2-4`: the gate returned 128,861
    characters of approved plan; the consolidation turn answered with an
    acknowledgement; `read_assistant_replies` came back empty -- it
    reads a 60-item tail where the approval gate reads 200, and it
    swallows a read error as `[]`. With no candidate left the run
    aborted, and the 128,861 characters were discarded while sitting in
    a local variable. The session was disposed at teardown, so the plan
    a human had spent the evening on was gone.

    A second read failing must never outrank the text already in hand.
    """

    @staticmethod
    def _plan() -> str:
        return plan_text(4000)

    def test_the_approved_text_is_used_when_no_reply_is_readable(self):
        # The live failure: replies empty, consolidation an ack.
        plan = self._plan()
        self.assertEqual(
            select_plan_of_record('The plan is complete.', [], plan),
            plan,
        )

    def test_it_no_longer_raises_when_the_approved_text_is_a_plan(self):
        # The regression raised PipelineRunError here, discarding an
        # approved plan it was holding.
        plan = self._plan()
        self.assertEqual(
            select_plan_of_record('Approved and complete.', [], plan),
            plan,
        )

    def test_a_compliant_consolidation_still_wins(self) -> None:
        # The common case must not move: a consolidation turn that
        # returned the document is still the plan of record.
        good = self._plan()
        self.assertEqual(
            select_plan_of_record(good, [], 'ignored'), good
        )

    def test_it_still_raises_when_nothing_anywhere_is_a_plan(self) -> None:
        # The guard must keep its teeth: an acknowledgement everywhere
        # is still fatal, which is the whole reason it exists.
        with self.assertRaises(PipelineRunError):
            select_plan_of_record('done', ['ok'], 'approved, thanks')

    def test_an_absent_approved_text_is_not_a_candidate(self) -> None:
        with self.assertRaises(PipelineRunError):
            select_plan_of_record('done', [], None)

    def test_it_never_outranks_a_later_reply_that_is_a_plan(self) -> None:
        # It is captured BEFORE the consolidation turn, so it is older
        # than anything in `replies`. Treating it as the latest
        # candidate made a pre-Q&A draft beat the revision that
        # superseded it -- caught by
        # test_short_ack_recovers_the_real_plan_for_builders.
        revision = plan_text(4000, 'the revision')
        stale = plan_text(4000, 'the first draft')
        self.assertEqual(
            select_plan_of_record('done', [stale, revision], stale),
            revision,
        )


class TestPlanOfRecordSelection(unittest.TestCase):
    """A chat acknowledgement must never become the plan of record."""

    def test_substantive_consolidation_is_used(self) -> None:
        doc = plan_text(4000, 'FINAL')
        self.assertIn(
            'FINAL', select_plan_of_record(doc, [plan_text(3000), doc])
        )

    def test_short_ack_falls_back_to_the_real_plan(self) -> None:
        # THE live regression, at the observed sizes: a 371-char "the
        # plan is released and frozen" blurb replaced a 12,412-char
        # design and was handed to every builder as the plan.
        draft = plan_text(14618, 'DRAFT')
        plan = plan_text(12412, 'PLAN')
        ack = 'The plan is released and frozen. ' * 11
        replies = [draft, 'just checking in. ' * 130, plan,
                   'still working on it. ' * 70, ack]
        self.assertIn('PLAN', select_plan_of_record(ack, replies))

    def test_latest_substantive_wins_not_longest(self) -> None:
        # A post-Q&A revision supersedes the first draft and is often
        # SHORTER — picking the longest would resurrect the draft.
        draft = plan_text(14618, 'DRAFT')
        revised = plan_text(12412, 'REVISED')
        ack = 'The plan is approved. ' * 5
        self.assertIn(
            'REVISED', select_plan_of_record(ack, [draft, revised, ack])
        )

    def test_consolidated_plan_shorter_than_the_draft_is_kept(self):
        # THE live regression: the consolidation turn DID emit a real
        # "Final Approved Design Plan" (5165), but an earlier verbose
        # draft (15088) had scaled the old bar to 7544, so it was
        # discarded for a PRE-approval draft (10276). A consolidated
        # plan is legitimately shorter than a rambling first draft.
        draft = plan_text(15088, 'DRAFT')
        revised = plan_text(10276, 'REVISED')
        final = plan_text(5165, 'FINAL')
        self.assertIn(
            'FINAL',
            select_plan_of_record(final, [draft, revised, 'ok', final]),
        )

    def test_mid_session_chatter_is_never_promoted(self) -> None:
        # Once the reply is known NOT to be a plan, a chatty status
        # update clears any length floor while being nothing like a
        # design. It has to fail on SHAPE.
        plan = plan_text(12412, 'PLAN')
        chatter = 'I have designed the plan and will now summarise. ' * 40
        ack = 'The plan is approved. ' * 5
        self.assertIn(
            'PLAN', select_plan_of_record(ack, [plan, chatter, ack])
        )

    def test_no_plan_anywhere_halts_the_run(self) -> None:
        # A plan that does not exist used to be indistinguishable from
        # one that does, and the run continued into eight agents' worth
        # of work either way.
        with self.assertRaises(PipelineRunError) as caught:
            select_plan_of_record('the plan is approved', ['hi', 'ok'])
        self.assertIn('no design plan', str(caught.exception))

    def test_the_halt_says_what_was_wrong(self) -> None:
        # A refusal that does not say why just moves the mystery.
        with self.assertRaises(PipelineRunError) as caught:
            select_plan_of_record('short', [])
        msg = str(caught.exception)
        self.assertIn('planner.md', msg)
        self.assertIn('characters', msg)


if __name__ == '__main__':
    unittest.main()
