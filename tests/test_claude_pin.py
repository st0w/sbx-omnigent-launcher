"""Pinning the Claude Code version inside a VM.

A model can need a newer Claude Code than the host image carries: the
image's 2.1.266 rejected Opus 5.5 with "version 2.1.280 or newer is
required", on every turn. These run the in-VM programs for real, under
the VM's own shell (dash) with a stand-in ``claude`` and ``npm`` on
``PATH``, because the point is a side effect inside a guest.
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

#: The VM's ``sh`` is dash, which lacks bash-only syntax; fall back to
#: ``/bin/sh`` only where dash is not installed.
_SHELL = '/bin/dash' if Path('/bin/dash').exists() else '/bin/sh'

_FAKE_CLAUDE = """#!/bin/sh
# Reports the version recorded in $FAKE_STATE, as the real CLI does.
echo "$(cat "$FAKE_STATE") (Claude Code)"
"""

_FAKE_NPM = """#!/bin/sh
# Records its argv; installs by writing the pinned version, or fails.
echo "$@" >> "$FAKE_NPM_LOG"
if [ -n "$FAKE_NPM_FAIL" ]; then
  echo "npm error code E404" >&2
  exit 1
fi
for spec; do :; done  # the last argument
echo "${spec##*@}" > "$FAKE_STATE"
"""


class _Guest:
    """A temp directory standing in for a VM with Claude installed."""

    def __init__(self, installed: str, *, npm_fails: bool = False) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='claude-pin-'))
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        for name, body in (('claude', _FAKE_CLAUDE), ('npm', _FAKE_NPM)):
            path = self.bin / name
            path.write_text(body, encoding='utf-8')
            path.chmod(0o755)
        self.state = self.root / 'installed'
        self.state.write_text(installed, encoding='utf-8')
        self.npm_log = self.root / 'npm.log'
        self.npm_fails = npm_fails

    def run(self, script: str) -> subprocess.CompletedProcess[str]:
        env = {
            'PATH': f'{self.bin}:/usr/bin:/bin',
            'TMPDIR': str(self.root),
            'FAKE_STATE': str(self.state),
            'FAKE_NPM_LOG': str(self.npm_log),
        }
        if self.npm_fails:
            env['FAKE_NPM_FAIL'] = '1'
        return subprocess.run(
            [_SHELL, '-c', script],
            env=env,
            capture_output=True,
            text=True,
        )

    def npm_calls(self) -> list[str]:
        if not self.npm_log.exists():
            return []
        return self.npm_log.read_text(encoding='utf-8').splitlines()


class TestTheVersionIsValidated(unittest.TestCase):
    """The version is interpolated into a shell command."""

    def test_an_exact_version_is_accepted(self) -> None:
        self.assertEqual(
            claude.validate_claude_version('2.1.280'), '2.1.280'
        )

    def test_anything_else_is_refused(self) -> None:
        for value in (
            'latest', 'stable', '2.1', '2.1.280.1', 'v2.1.280',
            ' 2.1.280', '2.1.280; rm -rf /', '2.1.280\n', '', '2.1.x',
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    claude.validate_claude_version(value)

    def test_a_non_string_is_refused(self) -> None:
        for value in (2.1, 280, None, ['2.1.280']):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    claude.validate_claude_version(value)

    def test_the_script_builder_validates_too(self) -> None:
        with self.assertRaises(ValueError):
            claude.build_version_pin_script('2.1.280 && reboot')


class TestThePinScript(unittest.TestCase):
    def test_an_older_claude_is_replaced_with_the_pin(self) -> None:
        guest = _Guest('2.1.266')
        proc = guest.run(claude.build_version_pin_script('2.1.280'))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(claude.PIN_OK_MARKER, proc.stdout)
        self.assertEqual(guest.state.read_text().strip(), '2.1.280')

    def test_it_installs_the_exact_version_with_npm(self) -> None:
        guest = _Guest('2.1.266')
        guest.run(claude.build_version_pin_script('2.1.280'))
        self.assertEqual(
            guest.npm_calls(),
            [
                'install -g --no-audit --no-fund '
                '@anthropic-ai/claude-code@2.1.280'
            ],
        )

    def test_a_newer_claude_is_brought_back_to_the_pin(self) -> None:
        # A pin is a pin: an image that already ships something newer
        # is moved to the tested version too.
        guest = _Guest('2.1.290')
        guest.run(claude.build_version_pin_script('2.1.280'))
        self.assertEqual(guest.state.read_text().strip(), '2.1.280')

    def test_the_pinned_version_already_there_skips_npm(self) -> None:
        guest = _Guest('2.1.280')
        proc = guest.run(claude.build_version_pin_script('2.1.280'))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(claude.PIN_OK_MARKER, proc.stdout)
        self.assertEqual(guest.npm_calls(), [])

    def test_an_npm_failure_fails_and_shows_its_output(self) -> None:
        guest = _Guest('2.1.266', npm_fails=True)
        proc = guest.run(claude.build_version_pin_script('2.1.280'))
        self.assertNotEqual(proc.returncode, 0)
        self.assertNotIn(claude.PIN_OK_MARKER, proc.stdout)
        self.assertIn('E404', proc.stdout)

    def test_a_version_that_did_not_change_fails(self) -> None:
        # npm can exit 0 and still leave another claude first on PATH.
        guest = _Guest('2.1.266')
        stuck = _FAKE_NPM.replace(
            'echo "${spec##*@}" > "$FAKE_STATE"', 'true'
        )
        (guest.bin / 'npm').write_text(stuck, encoding='utf-8')
        proc = guest.run(claude.build_version_pin_script('2.1.280'))
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn('2.1.266', proc.stdout)
        self.assertNotIn(claude.PIN_OK_MARKER, proc.stdout)

    def test_a_claude_that_reports_no_version_is_replaced(self) -> None:
        guest = _Guest('')
        proc = guest.run(claude.build_version_pin_script('2.1.280'))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(claude.PIN_OK_MARKER, proc.stdout)


def _seed(home: Path, **kw: bool) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, '-c', claude.build_settings_seed_script(**kw)],
        env={**os.environ, 'HOME': str(home)},
        capture_output=True,
        text=True,
    )


class TestTheAutoUpdaterCanBeTurnedOff(unittest.TestCase):
    """A pinned VM must not update itself mid-run: the run this came
    from had 2.1.280 in one VM and 2.1.266 in the next."""

    def setUp(self) -> None:
        self.home = Path(tempfile.mkdtemp(prefix='claude-home-'))
        self.settings = self.home / claude.SETTINGS_REL_PATH

    def _written(self) -> dict[str, object]:
        data = json.loads(self.settings.read_text(encoding='utf-8'))
        assert isinstance(data, dict)
        return data

    def test_off_by_default(self) -> None:
        self.assertEqual(_seed(self.home).returncode, 0)
        self.assertNotIn('env', self._written())

    def test_asked_for_it_sets_the_env_key(self) -> None:
        proc = _seed(self.home, disable_autoupdater=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(
            self._written()['env'], {claude.AUTOUPDATER_ENV: '1'}
        )
        self.assertIs(self._written()[claude.SETTINGS_KEY], True)

    def test_existing_env_entries_are_kept(self) -> None:
        self.settings.parent.mkdir(parents=True)
        self.settings.write_text(json.dumps({'env': {'FOO': 'bar'}}))
        _seed(self.home, disable_autoupdater=True)
        self.assertEqual(
            self._written()['env'],
            {'FOO': 'bar', claude.AUTOUPDATER_ENV: '1'},
        )

    def test_an_env_that_is_not_a_mapping_fails_loud(self) -> None:
        self.settings.parent.mkdir(parents=True)
        self.settings.write_text(json.dumps({'env': ['nope']}))
        proc = _seed(self.home, disable_autoupdater=True)
        self.assertNotEqual(proc.returncode, 0)
        self.assertNotIn(claude.SEED_OK_MARKER, proc.stdout)

    def test_it_is_idempotent(self) -> None:
        _seed(self.home, disable_autoupdater=True)
        before = self.settings.stat().st_mtime_ns
        proc = _seed(self.home, disable_autoupdater=True)
        self.assertIn(claude.SEED_OK_MARKER, proc.stdout)
        self.assertEqual(self.settings.stat().st_mtime_ns, before)


if __name__ == '__main__':
    unittest.main()
