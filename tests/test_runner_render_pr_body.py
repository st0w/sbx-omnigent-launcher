"""A pull request's title and description.

Run: python -m unittest tests.test_runner_render_pr_body
"""

from __future__ import annotations

import unittest

from sbx_omnigent import verify
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.render.pr_body import render_pr_body, short_title
from tests.support.reviews import AUDIT_LIMIT, SCAN_LIMIT


class TestRenderPrBody(unittest.TestCase):
    """A PR a reviewer can trust without reading the diff."""

    def _verified(self, *, demo_ok=True, out='test result: ok. 42 passed'):
        steps = [verify.StepOutcome('tests', 'cargo test', 0, out)]
        steps.append(
            verify.StepOutcome(
                'demo', './scripts/demo.sh', 0 if demo_ok else 1,
                'TLS1.3 X25519MLKEM768; wrote 17 rows',
            )
        )
        return verify.VerifyOutcome(
            ok=demo_ok, exit_code=0 if demo_ok else 1,
            output=out, steps=tuple(steps),
        )

    def _body(self, **kw):
        kw.setdefault('summary', '**[m1]** Storage & schema')
        kw.setdefault('outcome', self._verified())
        return render_pr_body(**kw)

    def test_the_evidence_is_the_captured_output(self) -> None:
        # Not "the tests passed" — the transcript of them passing.
        body = self._body()
        self.assertIn('test result: ok. 42 passed', body)
        self.assertIn('TLS1.3 X25519MLKEM768; wrote 17 rows', body)
        self.assertIn('cargo test', body)
        self.assertIn('Exit status **0**', body)

    def test_a_demonstration_is_shown_not_folded_away(self) -> None:
        # It is the thing a reviewer came to SEE; the test log is
        # corroboration and can be collapsed.
        body = self._body()
        demo_at = body.index('Proof it works')
        tests_at = body.index('How it was proven')
        self.assertLess(tests_at, demo_at)
        self.assertIn('<details><summary>Full output</summary>', body)
        self.assertNotIn('<details>', body[demo_at:])

    def test_it_links_the_design_and_the_reasoning(self) -> None:
        body = self._body(
            plan_doc='docs/plans/d-m1.md',
            session_doc='docs/plans/d-m1-session.md',
        )
        self.assertIn('`docs/plans/d-m1.md`', body)
        self.assertIn('`docs/plans/d-m1-session.md`', body)

    def test_no_gate_configured_still_renders(self) -> None:
        body = self._body(outcome=None)
        self.assertIn('[m1]', body)
        self.assertNotIn('How it was proven', body)

    def test_a_failing_step_is_labelled_as_such(self) -> None:
        body = self._body(outcome=self._verified(demo_ok=False))
        self.assertIn('Exit status **1** — FAILED.', body)

    def test_huge_output_is_capped_and_says_so(self) -> None:
        body = self._body(outcome=self._verified(out='x' * 50_000))
        self.assertLess(len(body), 40_000)
        self.assertIn('Truncated: showing the last', body)

    def test_a_fence_in_the_output_cannot_break_the_block(self) -> None:
        # Test output containing ``` would otherwise close the block
        # early and spill raw output into the prose.
        body = self._body(outcome=self._verified(out='before\n```\nafter'))
        self.assertIn('````', body)


class TestThePullRequestGroupsEnvironmentLimits(unittest.TestCase):
    def _rec(self, reviewer, stage='review-a', round_no=1, env=()):
        return ReviewRecord(
            chunk=None, stage=stage, reviewer=reviewer, round_no=round_no,
            verdict='APPROVED', environment=tuple(env),
        )

    def test_one_line_per_check_naming_everyone_who_raised_it(self) -> None:
        body = render_pr_body(summary='s', reviews=[
            self._rec('sec', env=[AUDIT_LIMIT]),
            self._rec('bugs', env=[AUDIT_LIMIT, SCAN_LIMIT]),
            self._rec('sec', round_no=2, env=[AUDIT_LIMIT]),
        ])
        self.assertEqual(body.count(AUDIT_LIMIT), 1, body)
        self.assertIn(f'- {AUDIT_LIMIT} (raised by `sec`, `bugs`)', body)
        self.assertIn(f'- {SCAN_LIMIT} (raised by `bugs`)', body)

    def test_stages_are_kept_apart(self) -> None:
        body = render_pr_body(summary='s', reviews=[
            self._rec('sec', env=[AUDIT_LIMIT]),
            self._rec('sec', stage='review-b', env=[AUDIT_LIMIT]),
        ])
        self.assertEqual(body.count(AUDIT_LIMIT), 2, body)
        self.assertLess(body.index('`review-a`'), body.index('`review-b`'))

    def test_no_limits_no_section(self) -> None:
        body = render_pr_body(summary='s', reviews=[self._rec('sec')])
        self.assertNotIn('Checks the sandbox could not run', body)


class TestShortTitle(unittest.TestCase):
    def test_it_keeps_the_leading_clause(self) -> None:
        self.assertEqual(
            short_title('Storage & schema — Postgres DDL + migrations'),
            'Storage & schema',
        )

    def test_a_long_single_clause_is_truncated(self) -> None:
        out = short_title('x' * 200)
        self.assertLessEqual(len(out), 60)
        self.assertTrue(out.endswith('…'))

    def test_a_short_title_is_untouched(self) -> None:
        self.assertEqual(short_title('AWS provider'), 'AWS provider')


if __name__ == '__main__':
    unittest.main()
