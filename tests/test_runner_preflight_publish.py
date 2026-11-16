"""The token a run publishes its pull requests with.

Run: python -m unittest tests.test_runner_preflight_publish
"""

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.preflight.publish import resolve_publish_token


class TestResolvePublishToken(unittest.TestCase):
    """Token comes from a file or a command — never the command line."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='tok-'))

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _file(self, text: str) -> str:
        p = self.root / 'tok'
        p.write_text(text, encoding='utf-8')
        return str(p)

    def test_neither_option_means_host_credentials(self) -> None:
        self.assertIsNone(resolve_publish_token(None, None))

    def test_from_file(self) -> None:
        tok = resolve_publish_token(self._file('secret\n'), None)
        self.assertEqual(tok, 'secret')  # trailing newline stripped

    def test_missing_file_errors(self) -> None:
        with self.assertRaises(PipelineRunError):
            resolve_publish_token(str(self.root / 'nope'), None)

    def test_empty_file_errors(self) -> None:
        with self.assertRaises(PipelineRunError):
            resolve_publish_token(self._file('  \n'), None)

    def test_from_command(self) -> None:
        tok = resolve_publish_token(None, 'echo secret')
        self.assertEqual(tok, 'secret')

    def test_command_is_argv_not_a_shell(self) -> None:
        # A shell would pipe; argv passes the bar through literally.
        tok = resolve_publish_token(None, 'echo a|b')
        self.assertEqual(tok, 'a|b')

    def test_failing_command_errors(self) -> None:
        with self.assertRaises(PipelineRunError):
            resolve_publish_token(None, 'false')

    def test_unknown_command_errors(self) -> None:
        with self.assertRaises(PipelineRunError):
            resolve_publish_token(None, 'sbx-no-such-binary-xyz')

    def test_command_with_empty_output_errors(self) -> None:
        with self.assertRaises(PipelineRunError):
            resolve_publish_token(None, 'echo')

    def test_both_options_error(self) -> None:
        with self.assertRaises(PipelineRunError):
            resolve_publish_token(self._file('s'), 'echo s')


if __name__ == '__main__':
    unittest.main()
