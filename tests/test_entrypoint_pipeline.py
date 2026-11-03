"""Entrypoint ``--pipeline`` flag + registration tests.

Run: .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import contextlib
import io
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from omnigent.spec import parse

from sbx_omnigent import entrypoint as E
from sbx_omnigent import image, pipeline

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


_IMAGE_BLOCK = (
    'image:\n'
    '  from: ghcr.io/omnigent-ai/omnigent-host:v0.20.0\n'
    '  setup: apt-get install -y jq\n'
)
_SERVER_URL = 'http://host.docker.internal:6767'


class TestThePipelinesImageReachesItsAgents(unittest.TestCase):
    """A server started with ``--pipeline`` boots its agents from that
    pipeline's image, and publishes which image it gave them (#38)."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='ep-img-'))
        self.addCleanup(shutil.rmtree, self.root, True)
        self.wt = str(self.root / 'wt')
        env = mock.patch.dict(os.environ, {}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        for name in (E._PIPELINE_IMAGE_ENV, E._PIPELINE_IMAGE_BASE_ENV):
            os.environ.pop(name, None)
        agents = mock.patch.object(
            E, '_PIPELINE_AGENTS_ROOT', self.root / 'agents'
        )
        agents.start()
        self.addCleanup(agents.stop)

    def _register(self, text: str) -> pipeline.PipelineConfig:
        path = self.root / 'pipeline.yaml'
        path.write_text(text, encoding='utf-8')
        E.register_pipeline_agents(str(path))
        return pipeline.load_pipeline(str(path))

    def _agent_image(self, **sbx: object) -> str:
        deployment = E._build_sbx_config({
            'server_url': _SERVER_URL,
            'sbx': {'worktree_root': self.wt, **sbx},
        })
        launcher = deployment.launcher_factory()
        return launcher._image

    def test_agents_boot_the_pipelines_image(self) -> None:
        cfg = self._register(_PIPE + _IMAGE_BLOCK)
        self.assertEqual(self._agent_image(), pipeline.image_tag(cfg))

    def test_the_server_publishes_it_for_the_runner(self) -> None:
        cfg = self._register(_PIPE + _IMAGE_BLOCK)
        self._agent_image()
        self.assertEqual(
            image.read_agent_image(self.wt), pipeline.image_tag(cfg)
        )

    def test_without_an_image_the_config_decides(self) -> None:
        self._register(_PIPE)
        self.assertEqual(
            self._agent_image(image='example.invalid/host:2'),
            'example.invalid/host:2',
        )
        self.assertEqual(
            image.read_agent_image(self.wt), 'example.invalid/host:2'
        )

    def test_a_pipeline_without_one_clears_an_earlier_one(self) -> None:
        self._register(_PIPE + _IMAGE_BLOCK)
        self._register(_PIPE)
        self.assertEqual(self._agent_image(), E.DEFAULT_HOST_IMAGE)

    def test_a_different_base_is_pointed_out(self) -> None:
        self._register(_PIPE + _IMAGE_BLOCK)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self._agent_image(image='ghcr.io/omnigent-ai/omnigent-host:v9')
        self.assertIn('image.from', err.getvalue())
        self.assertIn('omnigent-host:v9', err.getvalue())

    def test_a_matching_base_is_quiet(self) -> None:
        self._register(_PIPE + _IMAGE_BLOCK)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self._agent_image(
                image='ghcr.io/omnigent-ai/omnigent-host:v0.20.0'
            )
        self.assertNotIn('image.from', err.getvalue())


if __name__ == '__main__':
    unittest.main()
