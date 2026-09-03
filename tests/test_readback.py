"""Tests for checking what a harness launched with against the request.

Every pane string below is REAL — captured from a live microVM during
the 2026-08-19 sweep that found these four failures. Synthetic samples
would only prove the regexes match themselves.
"""

from __future__ import annotations

import unittest

from sbx_omnigent import readback as rb

# ── real captures ────────────────────────────────────────────────────

CLAUDE_AUTO = '  ⏵⏵ auto mode on (shift+tab to cycle) · ← for agents'
CLAUDE_MANUAL = '  ⏸ manual mode on · ? for shortcuts · ← for agents'
CLAUDE_DONTASK = "  ⏵⏵ don't ask on (shift+tab to cycle) · ← for agents"
CLAUDE_ACCEPT = '  ⏵⏵ accept edits on (shift+tab to cycle) · ← for agents'

CODEX_XHIGH = (
    '╭─────────────────────────────────────────────────╮\n'
    '│ >_ OpenAI Codex (v0.148.0)                      │\n'
    '│                                                 │\n'
    '│ model:       gpt-5.6-sol xhigh   /model to change│\n'
    '│ directory:   /Users/user/sbx-worktrees/smoke-ws │\n'
    '│ permissions: YOLO mode                          │\n'
    '╰─────────────────────────────────────────────────╯\n'
)
CODEX_DEFAULT = CODEX_XHIGH.replace('gpt-5.6-sol xhigh', 'gpt-5.6-sol default')

AGY_SERVED_36 = (
    '      ▄▀▀▄        Antigravity CLI 1.0.10\n'
    '     ▀▀▀▀▀▀       user@example.com (Antigravity Business)\n'
    '    ▀▀▀▀▀▀▀▀      Gemini 3.6 Flash (High)\n'
    '   ▄▀▀    ▀▀▄     /Users/user/sbx-worktrees/smoke-ws\n'
)
AGY_STATUS_37 = '? for shortcuts                    Gemini 3.7 Flash · high'


class TestClaudeMode(unittest.TestCase):
    def test_each_mode_is_read_off_the_footer(self) -> None:
        for pane, want in (
            (CLAUDE_AUTO, 'auto'),
            (CLAUDE_MANUAL, 'manual'),
            (CLAUDE_DONTASK, 'dontAsk'),
            (CLAUDE_ACCEPT, 'acceptEdits'),
        ):
            with self.subTest(want=want):
                self.assertEqual(rb.claude_permission_mode(pane), want)

    def test_a_pane_without_a_footer_reads_nothing(self) -> None:
        self.assertIsNone(rb.claude_permission_mode('still booting...'))


class TestCodexHeader(unittest.TestCase):
    def test_model_and_effort_are_read_through_the_box_frame(self) -> None:
        # The line begins with U+2502, not an ASCII pipe — the first cut
        # of this regex missed every real pane because of it.
        self.assertEqual(
            rb.codex_model_effort(CODEX_XHIGH), ('gpt-5.6-sol', 'xhigh')
        )

    def test_codex_says_default_when_no_effort_landed(self) -> None:
        # 'default' is codex's own word for "nothing pinned", and that
        # IS the finding — it must survive verbatim.
        self.assertEqual(
            rb.codex_model_effort(CODEX_DEFAULT), ('gpt-5.6-sol', 'default')
        )


class TestAgyModel(unittest.TestCase):
    def test_the_served_model_is_read_from_the_banner(self) -> None:
        self.assertEqual(rb.agy_model(AGY_SERVED_36), 'gemini36flashhigh')

    def test_the_status_bar_form_reads_the_same(self) -> None:
        self.assertEqual(rb.agy_model(AGY_STATUS_37), 'gemini37flashhigh')

    def test_a_display_name_folds_onto_its_model_id(self) -> None:
        # 'Gemini 3.6 Flash (High)' and 'gemini-3.6-flash-high' are the
        # same model written two ways.
        self.assertEqual(
            rb.agy_model(AGY_SERVED_36), rb._fold('gemini-3.6-flash-high')
        )


class TestLaunchMismatches(unittest.TestCase):
    """The four live failures, each caught by its own harness's pane."""

    def test_claude_downgraded_to_manual_is_caught(self) -> None:
        # #28: a Haiku reviewer ran in manual mode and blocked on an
        # approval prompt for every tool call.
        why = rb.launch_mismatches(
            'claude-native', CLAUDE_MANUAL, permission_mode='auto'
        )
        self.assertTrue(why)
        self.assertIn('permission mode', why[0])

    def test_codex_effort_dropped_is_caught(self) -> None:
        # #34: xhigh was accepted, persisted, returned by the API — and
        # the turn ran at codex's default.
        why = rb.launch_mismatches(
            'codex-native', CODEX_DEFAULT,
            model='gpt-5.6-sol', effort='xhigh',
        )
        self.assertTrue(why)
        self.assertIn('reasoning effort', why[0])

    def test_agy_serving_a_different_model_is_caught(self) -> None:
        # #35: asked for 3.7, the pane and the model itself said 3.6.
        why = rb.launch_mismatches(
            'antigravity-native', AGY_SERVED_36,
            model='gemini-3.7-flash-high',
        )
        self.assertTrue(why)
        self.assertIn('model', why[0])

    def test_a_launch_that_honoured_the_request_says_nothing(self) -> None:
        for harness, pane, kw in (
            ('claude-native', CLAUDE_DONTASK, {'permission_mode': 'dontAsk'}),
            ('codex-native', CODEX_XHIGH,
             {'model': 'gpt-5.6-sol', 'effort': 'xhigh'}),
            ('antigravity-native', AGY_SERVED_36,
             {'model': 'gemini-3.6-flash-high'}),
        ):
            with self.subTest(harness=harness):
                self.assertEqual(
                    rb.launch_mismatches(harness, pane, **kw), []
                )

    def test_an_unreadable_pane_is_never_a_mismatch(self) -> None:
        # A read-back that cries wolf is one somebody switches off, and
        # then the next silent substitution costs another day.
        for pane in ('', '   \n\n', 'still booting'):
            with self.subTest(pane=pane):
                self.assertEqual(
                    rb.launch_mismatches(
                        'claude-native', pane, permission_mode='auto'
                    ),
                    [],
                )

    def test_nothing_requested_is_never_a_mismatch(self) -> None:
        self.assertEqual(
            rb.launch_mismatches('codex-native', CODEX_DEFAULT), []
        )


if __name__ == '__main__':
    unittest.main()
