"""Updating agy inside an agy VM, when ``sbx.agy_update`` is on.

The host image pins agy (1.1.16 on v0.19.1), and a model released
after that build is quietly substituted: ``gemini-4-argon-high`` was
served as ``Gemini 3.8 Flash (High)``. ``agy update`` in the VM moved it
to 1.4.7 in 8 s. These run the in-VM program for real, under the VM's
own shell (dash) with a stand-in ``agy`` on ``PATH``.

Run: .venv/bin/python -m unittest tests.test_agy_update
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from sbx_omnigent import agy

#: The VM's ``sh`` is dash, which lacks bash-only syntax; fall back to
#: ``/bin/sh`` only where dash is not installed.
_SHELL = '/bin/dash' if Path('/bin/dash').exists() else '/bin/sh'

_FAKE_AGY = """#!/bin/sh
# --version reports $FAKE_STATE; `update` installs $FAKE_LATEST, or fails.
case "$1" in
  --version) cat "$FAKE_STATE" ;;
  update)
    echo "$@" >> "$FAKE_LOG"
    if [ -n "$FAKE_FAIL" ]; then
      echo "x Update failed: could not reach the update server" >&2
      exit 1
    fi
    if [ "$(cat "$FAKE_STATE")" = "$FAKE_LATEST" ]; then
      echo "You are already on the latest version."
    else
      cat "$FAKE_LATEST_FILE" > "$FAKE_STATE"
      echo "Update successful! Please restart agy."
    fi ;;
esac
"""


class _Guest:
    """A temp directory standing in for a VM with agy installed."""

    def __init__(
        self, installed: str, latest: str = '1.4.7', *, fails: bool = False
    ) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='agy-update-'))
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        fake = self.bin / 'agy'
        fake.write_text(_FAKE_AGY, encoding='utf-8')
        fake.chmod(0o755)
        self.state = self.root / 'installed'
        self.state.write_text(installed, encoding='utf-8')
        self.latest = self.root / 'latest'
        self.latest.write_text(latest, encoding='utf-8')
        self.log = self.root / 'agy.log'
        self.env = {
            'PATH': f'{self.bin}:/usr/bin:/bin',
            'TMPDIR': str(self.root),
            'FAKE_STATE': str(self.state),
            'FAKE_LATEST': latest,
            'FAKE_LATEST_FILE': str(self.latest),
            'FAKE_LOG': str(self.log),
        }
        if fails:
            self.env['FAKE_FAIL'] = '1'

    def cleanup(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def run(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [_SHELL, '-c', agy.build_update_script()],
            env=self.env,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
        )

    def calls(self) -> list[str]:
        if not self.log.exists():
            return []
        return self.log.read_text(encoding='utf-8').splitlines()


class TestTheUpdateScript(unittest.TestCase):
    def _guest(
        self, installed: str, latest: str = '1.4.7', *, fails: bool = False
    ) -> _Guest:
        guest = _Guest(installed, latest, fails=fails)
        self.addCleanup(guest.cleanup)
        return guest

    def test_an_older_agy_is_updated(self) -> None:
        guest = self._guest('1.1.16')
        proc = guest.run()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(guest.state.read_text().strip(), '1.4.7')
        self.assertIn(f'{agy.UPDATE_OK_MARKER} 1.1.16 1.4.7', proc.stdout)

    def test_it_runs_agy_update_once(self) -> None:
        guest = self._guest('1.1.16')
        guest.run()
        self.assertEqual(guest.calls(), ['update'])

    def test_an_agy_already_on_the_latest_passes(self) -> None:
        guest = self._guest('1.4.7')
        proc = guest.run()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn(f'{agy.UPDATE_OK_MARKER} 1.4.7 1.4.7', proc.stdout)

    def test_a_failed_update_fails_and_shows_its_output(self) -> None:
        guest = self._guest('1.1.16', fails=True)
        proc = guest.run()
        self.assertNotEqual(proc.returncode, 0)
        self.assertNotIn(agy.UPDATE_OK_MARKER, proc.stdout)
        self.assertIn('could not reach the update server', proc.stdout)

    def test_an_agy_that_reports_no_version_afterwards_fails(self) -> None:
        guest = self._guest('1.1.16', latest='')
        proc = guest.run()
        self.assertNotEqual(proc.returncode, 0)
        self.assertNotIn(agy.UPDATE_OK_MARKER, proc.stdout)
        self.assertIn('no version', proc.stdout)


class TestTheUpdateEgress(unittest.TestCase):
    def test_it_names_the_updater_and_the_download_host(self) -> None:
        # Measured on 2026-10-25 from `sbx policy log` in a VM whose
        # only per-VM rule was the updater: these two, nothing else.
        self.assertEqual(
            agy.UPDATE_EGRESS,
            (
                'antigravity-cli-auto-updater-974169037036.us-central1.run.app',
                'storage.googleapis.com',
            ),
        )


if __name__ == '__main__':
    unittest.main()
