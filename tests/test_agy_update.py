"""Updating agy inside an agy VM, when ``sbx.agy_update`` is on.

The host image pins agy (1.1.16 on v0.19.1), and a model released
after that build is quietly substituted: ``gemini-4-argon-high`` was
served as ``Gemini 3.8 Flash (High)``. ``agy update`` in the VM moved it
to 1.4.7 in 8 s. These run the in-VM program for real, under the VM's
own shell (dash) with a stand-in ``agy`` on ``PATH``.

Run: .venv/bin/python -m unittest tests.test_agy_update
"""

from __future__ import annotations

import ast
import json
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
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


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return int(sock.getsockname()[1])


class TestTheMetadataStandIn(unittest.TestCase):
    """agy 1.4.x signs in through Application Default Credentials.

    It dropped the token file the placeholder used to live in, so the
    VM runs a metadata stand-in that hands agy the SAME inert
    placeholder; sbx swaps the real token in on the wire, as before.
    These start the real program on a free port and query it as
    Google's auth library does.
    """

    def setUp(self) -> None:
        self.port = _free_port()
        self.proc = subprocess.Popen(
            [sys.executable, '-c',
             agy.build_metadata_stub('proj-1', port=self.port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self.addCleanup(self._stop)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(('127.0.0.1', self.port), 0.2):
                    return
            except OSError:
                time.sleep(0.05)
        self.fail('the stand-in never started listening')

    def _stop(self) -> None:
        self.proc.terminate()
        self.proc.wait(timeout=5)
        if self.proc.stderr is not None:
            self.proc.stderr.close()

    def _get(self, path: str) -> tuple[int, str, str]:
        req = urllib.request.Request(
            f'http://127.0.0.1:{self.port}{path}',
            headers={'Metadata-Flavor': 'Google'},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return (
                    resp.status,
                    resp.headers.get('Metadata-Flavor', ''),
                    resp.read().decode(),
                )
        except urllib.error.HTTPError as exc:
            with exc:
                return exc.code, '', exc.read().decode()

    def test_the_token_is_the_inert_placeholder(self) -> None:
        status, flavor, body = self._get(
            '/computeMetadata/v1/instance/service-accounts/default/token'
            '?scopes=https://www.googleapis.com/auth/cloud-platform'
        )
        self.assertEqual((status, flavor), (200, 'Google'))
        token = json.loads(body)
        self.assertEqual(token['access_token'], agy.PLACEHOLDER_TOKEN)
        self.assertEqual(token['token_type'], 'Bearer')
        self.assertGreater(token['expires_in'], 0)

    def test_it_reports_the_project(self) -> None:
        self.assertEqual(
            self._get('/computeMetadata/v1/project/project-id')[2], 'proj-1'
        )

    def test_it_answers_the_identity_lookups(self) -> None:
        base = '/computeMetadata/v1/instance/service-accounts/default'
        self.assertEqual(self._get(f'{base}/email')[0], 200)
        self.assertIn('cloud-platform', self._get(f'{base}/scopes')[2])

    def test_anything_else_is_not_found(self) -> None:
        self.assertEqual(
            self._get('/computeMetadata/v1/instance/attributes/x')[0], 404
        )

    def test_a_request_without_the_flavor_header_is_refused(self) -> None:
        req = urllib.request.Request(
            f'http://127.0.0.1:{self.port}/computeMetadata/v1/'
            'instance/service-accounts/default/token'
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(req, timeout=5)
        with caught.exception:
            self.assertEqual(caught.exception.code, 403)


class TestTheStandInListensOnLoopbackOnly(unittest.TestCase):
    def test_it_binds_127_0_0_1(self) -> None:
        # Read off the generated program rather than probed: a test
        # cannot portably reach a VM's other interfaces.
        tree = ast.parse(agy.build_metadata_stub('proj-1', port=4321))
        binds = [
            node.args[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == 'HTTPServer'
        ]
        self.assertEqual(len(binds), 1)
        self.assertEqual(ast.literal_eval(binds[0]), ('127.0.0.1', 4321))


class TestTheAdcEnvironment(unittest.TestCase):
    def test_it_points_agy_at_the_stand_in(self) -> None:
        self.assertEqual(
            dict(agy.adc_launch_env('proj-1')),
            {
                'AGY_ADC_AUTH': 'true',
                'GCE_METADATA_HOST': f'127.0.0.1:{agy.ADC_METADATA_PORT}',
                'GOOGLE_CLOUD_PROJECT': 'proj-1',
                'ANTIGRAVITY_PROJECT_ID': 'proj-1',
            },
        )


if __name__ == '__main__':
    unittest.main()
