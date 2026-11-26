"""``scripts/run-runner.sh``: environment in, runner arguments out.

Runs the real script under ``/bin/sh`` with ``OMNI_LAUNCHER_PY``
pointing at a fake interpreter that records its arguments, so nothing
here starts the runner. Run from the repository root.
"""

from __future__ import annotations

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

_SCRIPT = Path('scripts/run-runner.sh')

_FAKE_PYTHON = '#!/bin/sh\nprintf \'%s\\n\' "$@" > "$OMNI_TEST_ARGV"\n'


class _ScriptTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        python = self.tmp / 'python'
        python.write_text(_FAKE_PYTHON)
        python.chmod(stat.S_IRWXU)
        self.argv_file = self.tmp / 'argv'
        self.env = {
            'PATH': os.environ.get('PATH', '/usr/bin:/bin'),
            'OMNI_TEST_ARGV': str(self.argv_file),
            'OMNI_PIPELINE': '/repos/demo/omnigent/pipeline.live.yaml',
            'OMNI_PUBLISH_REPO': 'https://github.com/org/demo',
            'OMNI_LAUNCHER_PY': str(python),
            'OMNI_CANONICAL_ROOT': '/srv/canonical',
            'OMNI_WORKTREE_ROOT': '/srv/worktrees',
            'OMNI_SERVER': 'http://127.0.0.1:6767',
            'OMNI_MACHINE_CLIENT_ID': 'pipeline',
            'OMNI_MACHINE_SECRET_FILE': '/etc/omni/machine-secret',
            'OMNI_SHARE_WITH': 'alice',
            'PUBLISH_TOKEN_CMD': '/usr/bin/cat /etc/token',
        }

    def run_script(
        self, *args: str, **env: str | None
    ) -> subprocess.CompletedProcess[str]:
        merged = dict(self.env)
        for name, value in env.items():
            if value is None:
                merged.pop(name, None)
            else:
                merged[name] = value
        return subprocess.run(
            ['/bin/sh', str(_SCRIPT), *args],
            env=merged, capture_output=True, text=True, check=False,
            timeout=30,
        )

    def runner_argv(self) -> list[str]:
        return self.argv_file.read_text().splitlines()


class TestTheRunnerIsCalled(_ScriptTest):
    def test_each_setting_reaches_the_runner(self) -> None:
        result = self.run_script('--run-id', 'demo-1')
        self.assertEqual(result.returncode, 0, result.stderr)
        argv = self.runner_argv()
        for flag, value in (
            ('--server', 'http://127.0.0.1:6767'),
            ('--machine-client-id', 'pipeline'),
            ('--machine-secret-file', '/etc/omni/machine-secret'),
            ('--share-with', 'alice'),
        ):
            with self.subTest(flag=flag):
                self.assertEqual(argv[argv.index(flag) + 1], value)

    def test_the_run_id_is_passed_on(self) -> None:
        self.run_script('--run-id', 'demo-1')
        self.assertEqual(self.runner_argv()[-2:], ['--run-id', 'demo-1'])


class TestItRefuses(_ScriptTest):
    def test_without_a_server(self) -> None:
        result = self.run_script('--run-id', 'demo-1', OMNI_SERVER=None)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('OMNI_SERVER', result.stderr)
        self.assertFalse(self.argv_file.exists())

    def test_without_each_of_the_other_settings(self) -> None:
        for name in (
            'OMNI_SHARE_WITH', 'OMNI_MACHINE_CLIENT_ID',
            'OMNI_MACHINE_SECRET_FILE',
        ):
            with self.subTest(name=name):
                result = self.run_script('--run-id', 'demo-1', **{name: None})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(name, result.stderr)

    def test_a_relative_secret_file(self) -> None:
        result = self.run_script(
            '--run-id', 'demo-1', OMNI_MACHINE_SECRET_FILE='secret'
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn('OMNI_MACHINE_SECRET_FILE', result.stderr)

    def test_a_server_that_is_not_http(self) -> None:
        for server in ('file:///etc/passwd', '127.0.0.1:6767', ' '):
            with self.subTest(server=server):
                result = self.run_script(
                    '--run-id', 'demo-1', OMNI_SERVER=server
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn('OMNI_SERVER', result.stderr)


if __name__ == '__main__':
    unittest.main()
