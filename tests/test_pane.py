# ruff: noqa: RUF001
"""Tests for reading a native harness's TUI pane out of its VM.

The pointer glyphs below are the real characters these CLIs draw;
RUF001 is silenced file-wide because swapping them for ASCII would stop
these fixtures being captures."""

from __future__ import annotations

import subprocess
import unittest

from sbx_omnigent import pane


class _Proc:
    """Minimal stand-in for a CompletedProcess."""

    def __init__(self, stdout: str = '', returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode


def _runner(proc=None, raises=None):
    """A subprocess.run stand-in that records its argv."""
    calls: list[list[str]] = []

    def run(argv, **kwargs):
        calls.append(argv)
        if raises is not None:
            raise raises
        return proc if proc is not None else _Proc()

    run.calls = calls  # type: ignore[attr-defined]
    return run


class TestCaptureScript(unittest.TestCase):
    def test_it_reads_the_shared_native_terminal_socket(self) -> None:
        # One socket convention serves claude-native, codex-native and
        # antigravity-native — which is what makes this harness-agnostic
        # rather than another per-harness special case.
        script = pane.capture_script()
        self.assertIn(pane.TERMINAL_DIR_GLOB, script)
        self.assertIn('tmux.sock', script)
        self.assertIn(f'-t {pane.TMUX_SESSION}', script)

    def test_the_scrollback_depth_is_honoured(self) -> None:
        self.assertIn('-S -40', pane.capture_script(40))

    def test_the_scrollback_depth_cannot_be_injected(self) -> None:
        # It lands in a shell command, so it must be an int, not text.
        script = pane.capture_script(int('7'))
        self.assertIn('-S -7', script)

    def test_a_vm_with_no_terminal_says_so_and_exits_clean(self) -> None:
        # Reported as text, not as a failed command someone then has to
        # interpret — an SDK harness simply has no pane.
        script = pane.capture_script()
        self.assertIn(pane.NO_TERMINAL_MARKER, script)
        self.assertIn('exit 0', script)


class TestCaptureCommand(unittest.TestCase):
    def test_it_execs_into_the_named_sandbox(self) -> None:
        argv = pane.capture_command('managed-abc123')
        self.assertEqual(argv[:4], ['sbx', 'exec', 'managed-abc123', '--'])
        self.assertEqual(argv[4:6], ['sh', '-lc'])


class TestCapturePane(unittest.TestCase):
    def test_a_readable_pane_comes_back(self) -> None:
        run = _runner(_Proc('  1. Try new model\n  2. Use existing model\n'))
        out = pane.capture_pane('managed-abc', run=run)
        self.assertIn('Try new model', out or '')
        self.assertEqual(run.calls[0][2], 'managed-abc')

    def test_no_terminal_is_not_a_pane(self) -> None:
        run = _runner(_Proc(pane.NO_TERMINAL_MARKER))
        self.assertIsNone(pane.capture_pane('managed-abc', run=run))

    def test_an_unreadable_pane_is_not_a_pane(self) -> None:
        run = _runner(_Proc(pane.UNREADABLE_MARKER))
        self.assertIsNone(pane.capture_pane('managed-abc', run=run))

    def test_an_empty_pane_is_not_a_pane(self) -> None:
        run = _runner(_Proc('   \n\n'))
        self.assertIsNone(pane.capture_pane('managed-abc', run=run))

    def test_a_failed_exec_is_swallowed(self) -> None:
        run = _runner(_Proc('boom', returncode=1))
        self.assertIsNone(pane.capture_pane('managed-abc', run=run))

    def test_a_hung_vm_does_not_hang_the_capture(self) -> None:
        # This runs while a run is already failing and teardown waits.
        run = _runner(raises=subprocess.TimeoutExpired('sbx', 30))
        self.assertIsNone(pane.capture_pane('managed-abc', run=run))

    def test_a_missing_sbx_binary_is_swallowed(self) -> None:
        run = _runner(raises=OSError('no sbx'))
        self.assertIsNone(pane.capture_pane('managed-abc', run=run))

    def test_it_never_reads_stdin(self) -> None:
        # An exec that inherits a live stdin is what hangs the agy poke;
        # the same trap applies here.
        seen: dict[str, object] = {}

        def run(argv, **kwargs):
            seen.update(kwargs)
            return _Proc('pane')

        pane.capture_pane('managed-abc', run=run)
        self.assertEqual(seen.get('stdin'), subprocess.DEVNULL)


# ── real modal prompts, captured from live microVMs ──────────────────

AGY_PICKER = """Question 3/3: Which SBOM formats?
> 1. (Recommended) SPDX JSON + CycloneDX JSON + audit metadata
  2. Generate SPDX JSON only
  3. Generate CycloneDX JSON only
  4. Write-in...
  up/down Navigate . left Back . enter Select . esc Skip"""

CODEX_MIGRATION = """GPT-5.4 Mini will be deprecated soon

Codex now uses GPT-5.6 Luna in place of GPT-5.4 Mini.

Choose how you'd like Codex to proceed.

> 1. Try new model
  2. Use existing model

Use up/down to move, press enter to confirm"""

CLAUDE_TRUST = """ Quick safety check: is this a project you trust?

 ❯ 1. Yes, I trust this folder
   2. No, exit

 Enter to confirm · Esc to cancel"""

CLAUDE_APIKEY = """    Detected a custom API key in your environment

      1. Yes
    ❯ 2. No (recommended)

    Enter to confirm · Esc to cancel"""

HEALTHY = """● Auto mode lets Claude handle permission prompts automatically.
❯ Reply with exactly: OK
  ⏵⏵ don't ask on (shift+tab to cycle)"""


class TestModalPrompt(unittest.TestCase):
    """Every native harness blocks on a picker, and they look alike."""

    def test_each_real_prompt_is_recognised(self) -> None:
        for name, text in (
            ('agy question picker', AGY_PICKER),
            ('codex model migration', CODEX_MIGRATION),
            ('claude trust gate', CLAUDE_TRUST),
            ('claude api-key gate', CLAUDE_APIKEY),
        ):
            with self.subTest(prompt=name):
                self.assertIsNotNone(pane.modal_prompt(text))

    def test_a_working_pane_is_not_a_prompt(self) -> None:
        self.assertIsNone(pane.modal_prompt(HEALTHY))

    def test_prose_with_a_numbered_list_is_not_a_prompt(self) -> None:
        # A plan's ordered steps must never read as a picker.
        self.assertIsNone(pane.modal_prompt(
            '## Algorithm\n1. Read the spec\n2. Split units\n3. Sum them\n'
        ))

    def test_nothing_is_not_a_prompt(self) -> None:
        for text in (None, '', '   \n\n'):
            with self.subTest(text=text):
                self.assertIsNone(pane.modal_prompt(text))

    def test_it_matches_the_key_legend_not_the_question(self) -> None:
        # The question is the agent's words, in any language; the
        # legend is the CLI's. Same picker, different wording.
        translated = AGY_PICKER.replace(
            'Which SBOM formats?', 'Welche SBOM-Formate?'
        )
        self.assertIsNotNone(pane.modal_prompt(translated))

    def test_the_prompt_is_trimmed_to_the_picker(self) -> None:
        noisy = 'irrelevant scrollback\n' * 40 + CODEX_MIGRATION
        got = pane.modal_prompt(noisy) or ''
        self.assertNotIn('irrelevant scrollback', got)
        self.assertIn('Try new model', got)


class TestBlockedMessage(unittest.TestCase):
    def test_it_leads_with_the_action(self) -> None:
        # The failure a human sees otherwise describes the paste
        # mechanism and buries the thing to do (TASKS.md #12).
        msg = pane.blocked_on_prompt_message(
            'm5-plan', pane.modal_prompt(AGY_PICKER) or ''
        )
        head = msg.split('It is showing:')[0]
        self.assertIn('CANNOT answer', head)
        self.assertIn('Open this agent', head)
        self.assertIn('m5-plan', head)

    def test_it_says_nothing_was_answered_for_them(self) -> None:
        # The orchestrator must not choose on the human's behalf, and
        # they must not be left wondering whether it did.
        msg = pane.blocked_on_prompt_message('n', 'x')
        self.assertIn('Nothing was answered on your behalf', msg)

    def test_the_picker_comes_after_the_instruction(self) -> None:
        prompt = pane.modal_prompt(CODEX_MIGRATION) or ''
        msg = pane.blocked_on_prompt_message('build', prompt)
        self.assertLess(msg.index('Open this agent'), msg.index(prompt))


if __name__ == '__main__':
    unittest.main()
