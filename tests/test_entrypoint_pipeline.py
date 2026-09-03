"""Entrypoint ``--pipeline`` flag + registration tests.

Run: .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from omnigent.spec import parse

from sbx_omnigent import entrypoint as E

_PIPE = (
    'name: p\n'
    'repo: ./p\n'
    'agents:\n'
    '  build: {template: coder, model: claude-sonnet-5}\n'
    '  sec: {template: security-reviewer}\n'
)


class TestExtractPipelineFlag(unittest.TestCase):
    def test_long_flag(self) -> None:
        argv, p = E._extract_pipeline_flag(['server', '--pipeline', 'x.yaml'])
        self.assertEqual(argv, ['server'])
        self.assertEqual(p, 'x.yaml')

    def test_short_flag(self) -> None:
        argv, p = E._extract_pipeline_flag(['server', '-P', 'y.yaml'])
        self.assertEqual((argv, p), (['server'], 'y.yaml'))

    def test_equals_form(self) -> None:
        argv, p = E._extract_pipeline_flag(['server', '--pipeline=z.yaml'])
        self.assertEqual((argv, p), (['server'], 'z.yaml'))

    def test_absent(self) -> None:
        argv, p = E._extract_pipeline_flag(['server', '-c', 'cfg.yaml'])
        self.assertEqual(argv, ['server', '-c', 'cfg.yaml'])
        self.assertIsNone(p)

    def test_missing_value_raises(self) -> None:
        with self.assertRaises(SystemExit):
            E._extract_pipeline_flag(['server', '--pipeline'])

    def test_last_occurrence_wins(self) -> None:
        _, p = E._extract_pipeline_flag(
            ['--pipeline', 'a.yaml', '--pipeline', 'b.yaml']
        )
        self.assertEqual(p, 'b.yaml')

    def test_strips_only_the_flag(self) -> None:
        argv, _ = E._extract_pipeline_flag(
            ['server', '-c', 'cfg.yaml', '--pipeline', 'p.yaml', '--port', '9']
        )
        self.assertEqual(argv, ['server', '-c', 'cfg.yaml', '--port', '9'])


class TestRegisterPipelineAgents(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='ep-pl-'))
        self.pipe = self.root / 'pipeline.yaml'
        self.pipe.write_text(_PIPE, encoding='utf-8')
        self.dest = self.root / 'agents-out'

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_materializes_and_appends_env(self) -> None:
        with mock.patch.object(E, '_PIPELINE_AGENTS_ROOT', self.dest), \
                mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(E._BUILTIN_AGENT_DIRS_ENV, None)
            mapping = E.register_pipeline_agents(str(self.pipe))
            env = os.environ[E._BUILTIN_AGENT_DIRS_ENV].split(os.pathsep)
        self.assertEqual(set(mapping), {'build', 'sec'})
        for spec in mapping.values():
            bundle = self.dest / 'p' / spec
            self.assertIn(str(bundle), env)
            self.assertEqual(parse(bundle).name, spec)

    def test_bad_pipeline_exits(self) -> None:
        bad = self.root / 'bad.yaml'
        bad.write_text('agents: {}\n', encoding='utf-8')  # no repo
        with mock.patch.object(E, '_PIPELINE_AGENTS_ROOT', self.dest):
            with self.assertRaises(SystemExit):
                E.register_pipeline_agents(str(bad))


if __name__ == '__main__':
    unittest.main()
