"""Tests for reading harness CLI versions out of an agent VM (#17)."""

from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

from sbx_omnigent import harness_versions as hv

_DOC = Path(__file__).resolve().parents[1] / 'docs' / 'HARNESS-VERSIONS.md'


class _Proc:
    """Minimal stand-in for a CompletedProcess."""

    def __init__(self, stdout: str = '', returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode


def _runner(proc=None, raises=None):
    """A subprocess.run stand-in that records argv and kwargs."""
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        if raises is not None:
            raise raises
        return proc if proc is not None else _Proc()

    run.calls = calls  # type: ignore[attr-defined]
    return run


class TestParseVersions(unittest.TestCase):
    def test_each_cli_line_yields_its_version(self) -> None:
        out = (
            'claude=2.1.266 (Claude Code)\n'
            'codex=codex-cli 0.153.4\n'
            'agy=1.1.16\n'
        )
        self.assertEqual(
            hv.parse_versions(out),
            {'claude': '2.1.266', 'codex': '0.153.4', 'agy': '1.1.16'},
        )

    def test_a_missing_cli_is_left_out(self) -> None:
        # `command not found` goes to /dev/null, leaving `agy=` empty.
        out = 'claude=2.1.266 (Claude Code)\ncodex=\nagy=\n'
        self.assertEqual(hv.parse_versions(out), {'claude': '2.1.266'})

    def test_a_line_with_no_version_is_left_out(self) -> None:
        # Recording chatter as a version would make every later
        # comparison report a change that never happened.
        out = 'codex=[UNDICI-EHPA] Warning: experimental\n'
        self.assertEqual(hv.parse_versions(out), {})

    def test_an_unknown_name_is_ignored(self) -> None:
        self.assertEqual(hv.parse_versions('gemini=1.2.3\n'), {})

    def test_a_prerelease_suffix_is_kept(self) -> None:
        self.assertEqual(
            hv.parse_versions('claude=2.2.0-beta.1\n'),
            {'claude': '2.2.0-beta.1'},
        )

    def test_empty_output_is_no_versions(self) -> None:
        self.assertEqual(hv.parse_versions(''), {})


class TestVersionCommand(unittest.TestCase):
    def test_it_execs_a_login_shell_in_the_named_sandbox(self) -> None:
        # A login shell, as docs/HARNESS-VERSIONS.md does: the CLIs are
        # on the profile's PATH, not the bare exec one.
        argv = hv.version_command('managed-abc')
        self.assertEqual(
            argv[:6], ['sbx', 'exec', 'managed-abc', '--', 'sh', '-lc']
        )

    def test_the_sandbox_name_never_reaches_the_shell(self) -> None:
        argv = hv.version_command('x; rm -rf /')
        self.assertNotIn('rm -rf', argv[-1])

    def test_stderr_is_discarded_in_the_guest(self) -> None:
        # codex prints a Node warning on stderr that otherwise lands
        # where its version line should be.
        self.assertIn('2>/dev/null', hv.version_command('m')[-1])

    def test_every_cli_is_asked(self) -> None:
        script = hv.version_command('m')[-1]
        for cli in hv.CLIS:
            with self.subTest(cli=cli):
                self.assertIn(cli, script)


class TestReadVersions(unittest.TestCase):
    def test_a_readable_vm_returns_its_versions(self) -> None:
        run = _runner(_Proc('claude=2.1.266 (Claude Code)\n'))
        self.assertEqual(
            hv.read_versions('managed-abc', run=run),
            {'claude': '2.1.266'},
        )
        self.assertEqual(run.calls[0][0][2], 'managed-abc')

    def test_a_failed_exec_is_no_versions(self) -> None:
        run = _runner(_Proc('claude=2.1.266\n', returncode=1))
        self.assertEqual(hv.read_versions('m', run=run), {})

    def test_a_hung_vm_is_no_versions(self) -> None:
        run = _runner(raises=subprocess.TimeoutExpired('sbx', 30))
        self.assertEqual(hv.read_versions('m', run=run), {})

    def test_a_missing_sbx_binary_is_no_versions(self) -> None:
        run = _runner(raises=OSError('no sbx'))
        self.assertEqual(hv.read_versions('m', run=run), {})

    def test_it_never_reads_stdin_and_is_bounded(self) -> None:
        run = _runner(_Proc(''))
        hv.read_versions('m', run=run, timeout_s=7.0)
        kwargs = run.calls[0][1]
        self.assertEqual(kwargs.get('stdin'), subprocess.DEVNULL)
        self.assertEqual(kwargs.get('timeout'), 7.0)


class TestCliForHarness(unittest.TestCase):
    def test_each_native_harness_maps_to_its_cli(self) -> None:
        for harness, cli in (
            ('claude-native', 'claude'),
            ('codex-native', 'codex'),
            ('antigravity-native', 'agy'),
        ):
            with self.subTest(harness=harness):
                self.assertEqual(hv.cli_for_harness(harness), cli)


class TestFormatVersions(unittest.TestCase):
    def test_versions_read_in_cli_order(self) -> None:
        self.assertEqual(
            hv.format_versions({'agy': '1.1.16', 'claude': '2.1.266'}),
            'claude 2.1.266, agy 1.1.16',
        )

    def test_no_versions_says_so(self) -> None:
        self.assertEqual(hv.format_versions({}), 'none could be read')


class TestPendingSelfUpdate(unittest.TestCase):
    def test_the_update_notice_is_recognised(self) -> None:
        # Verbatim from three captured panes on one run.
        pane = (
            '                                          '
            '✔ Update installed · Restart to apply'
        )
        self.assertTrue(hv.pending_self_update(pane))

    def test_an_ordinary_pane_is_not_an_update(self) -> None:
        self.assertFalse(hv.pending_self_update('> implementing parse'))

    def test_no_pane_is_not_an_update(self) -> None:
        self.assertFalse(hv.pending_self_update(None))


class TestKnownGoodMatchesTheDocument(unittest.TestCase):
    """The constant and docs/HARNESS-VERSIONS.md must agree.

    The document is what a human updates after checking a new image;
    the constant is what a run compares against. A test is the only
    thing that keeps a second copy from going stale by itself."""

    def _section(self) -> str:
        text = _DOC.read_text(encoding='utf-8')
        start = text.index('## Known-good set')
        return text[start:text.index('\n## ', start + 1)]

    def test_the_recorded_date_matches(self) -> None:
        match = re.search(r'\*\*(\d{4}-\d{2}-\d{2})\*\*', self._section())
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), hv.KNOWN_GOOD_RECORDED)

    def test_every_version_matches(self) -> None:
        table: dict[str, str] = {}
        for line in self._section().splitlines():
            cells = [c.strip() for c in line.strip('|').split('|')]
            if len(cells) != 2:
                continue
            for cli in hv.CLIS:
                if cli in cells[0]:
                    table[cli] = cells[1]
        self.assertEqual(table, dict(hv.KNOWN_GOOD))


if __name__ == '__main__':
    unittest.main()
