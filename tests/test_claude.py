"""
The Claude launch-gate pre-acceptance seed (TASKS.md #39).

These run the seed program for real rather than grepping its source.
The whole point of the module is a side effect inside a guest, and a
substring assertion would have passed just as happily on a script that
wrote the wrong key or clobbered the file.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from sbx_omnigent import claude
from sbx_omnigent.swarm import _launch_args_for


def _seed(home: Path) -> subprocess.CompletedProcess[str]:
    """Run the seed program with *home* as ``$HOME``."""
    return subprocess.run(
        [sys.executable, '-c', claude.build_settings_seed_script()],
        env={**os.environ, 'HOME': str(home)},
        capture_output=True,
        text=True,
    )


class TestSettingsSeed(unittest.TestCase):
    """What the in-VM program does to ``~/.claude/settings.json``."""

    def setUp(self) -> None:
        self.home = Path(tempfile.mkdtemp(prefix='claude-home-'))
        self.settings = self.home / claude.SETTINGS_REL_PATH

    def test_creates_the_file_when_absent(self) -> None:
        # The normal case: a fresh microVM has never run Claude.
        proc = _seed(self.home)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(claude.SEED_OK_MARKER, proc.stdout)
        self.assertEqual(
            json.loads(self.settings.read_text()),
            {claude.SETTINGS_KEY: True},
        )

    def test_merges_into_existing_settings(self) -> None:
        # The host image may already ship settings. Clobbering them
        # would trade one silent launch failure for another.
        self.settings.parent.mkdir(parents=True)
        self.settings.write_text(
            json.dumps({'model': 'opus', 'effortLevel': 'xhigh'})
        )
        self.assertEqual(_seed(self.home).returncode, 0)
        self.assertEqual(
            json.loads(self.settings.read_text()),
            {
                'model': 'opus',
                'effortLevel': 'xhigh',
                claude.SETTINGS_KEY: True,
            },
        )

    def test_is_idempotent_and_does_not_rewrite(self) -> None:
        _seed(self.home)
        before = self.settings.stat().st_mtime_ns
        proc = _seed(self.home)
        self.assertEqual(proc.returncode, 0)
        self.assertIn(claude.SEED_OK_MARKER, proc.stdout)
        self.assertEqual(self.settings.stat().st_mtime_ns, before)

    def test_owner_only_permissions(self) -> None:
        _seed(self.home)
        self.assertEqual(self.settings.stat().st_mode & 0o777, 0o600)

    def test_tolerates_an_empty_file(self) -> None:
        self.settings.parent.mkdir(parents=True)
        self.settings.write_text('   \n')
        self.assertEqual(_seed(self.home).returncode, 0)
        self.assertIs(
            json.loads(self.settings.read_text())[claude.SETTINGS_KEY], True
        )

    def test_refuses_a_settings_file_that_is_not_an_object(self) -> None:
        # Fail loud rather than replace an unexpected config, matching
        # Omnigent's own stance in ensure_claude_workspace_trusted.
        self.settings.parent.mkdir(parents=True)
        self.settings.write_text('[1, 2, 3]')
        proc = _seed(self.home)
        self.assertNotEqual(proc.returncode, 0)
        self.assertNotIn(claude.SEED_OK_MARKER, proc.stdout)
        self.assertEqual(self.settings.read_text(), '[1, 2, 3]')

    def test_refuses_malformed_json(self) -> None:
        self.settings.parent.mkdir(parents=True)
        self.settings.write_text('{not json')
        proc = _seed(self.home)
        self.assertNotEqual(proc.returncode, 0)
        self.assertNotIn(claude.SEED_OK_MARKER, proc.stdout)

    def test_carries_no_secret_in_argv(self) -> None:
        # Unlike codex's real token, the payload is a fixed boolean, so
        # it may be a script literal — but assert it stays that way.
        script = claude.build_settings_seed_script()
        self.assertNotIn('stdin', script)
        self.assertIn(claude.SETTINGS_KEY, script)


class TestSeedIsWiredToTheClaudeMode(unittest.TestCase):
    """The seed exists only because of the mode we launch Claude in."""

    def test_the_key_matches_what_claude_persists(self) -> None:
        # Verified live 2026-08-22: accepting the bypass-permissions
        # dialog writes exactly this key into ~/.claude/settings.json,
        # and pre-seeding it suppresses the dialog outright.
        self.assertEqual(
            claude.SETTINGS_KEY, 'skipDangerousModePermissionPrompt'
        )

    def test_claude_is_launched_in_the_mode_that_needs_it(self) -> None:
        self.assertEqual(
            _launch_args_for('claude-native'),
            ('--permission-mode', 'bypassPermissions'),
        )


if __name__ == '__main__':
    unittest.main()
