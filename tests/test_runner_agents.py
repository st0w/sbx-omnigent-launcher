"""Which registered agent each of a pipeline's agents runs as.

Run: python -m unittest tests.test_runner_agents
"""

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from sbx_omnigent import pipeline
from sbx_omnigent.runner.agents import resolve_agent_ids
from sbx_omnigent.runner.errors import PipelineRunError

_PIPELINE = """\
name: m
repo: ./p
agents:
  build: {template: coder}
  sec: {template: security-reviewer}
"""


class TestResolvingAgentIds(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        path = root / 'pipeline.yaml'
        path.write_text(_PIPELINE, encoding='utf-8')
        self.cfg = pipeline.load_pipeline(path)

    def entry(self, agent: str, agent_id: str) -> dict[str, object]:
        name = pipeline.namespaced_agent_name(self.cfg.name, agent)
        return {'id': agent_id, 'name': name}

    def test_each_agent_maps_to_its_registered_id(self) -> None:
        ids = resolve_agent_ids(self.cfg, [
            self.entry('build', 'ag_b'), self.entry('sec', 'ag_s'),
        ])
        self.assertEqual(ids, {'build': 'ag_b', 'sec': 'ag_s'})

    def test_an_empty_catalog_is_refused_with_the_fix(self) -> None:
        with self.assertRaises(PipelineRunError) as caught:
            resolve_agent_ids(self.cfg, [])
        self.assertIn('--pipeline', str(caught.exception))

    def test_every_missing_agent_is_named(self) -> None:
        with self.assertRaises(PipelineRunError) as caught:
            resolve_agent_ids(self.cfg, [self.entry('build', 'ag_b')])
        message = str(caught.exception)
        self.assertIn(
            pipeline.namespaced_agent_name(self.cfg.name, 'sec'), message,
        )
        self.assertNotIn(
            pipeline.namespaced_agent_name(self.cfg.name, 'build'), message,
        )

    def test_an_entry_of_the_wrong_shape_is_ignored(self) -> None:
        # A catalog entry without string name and id is not an agent.
        with self.assertRaises(PipelineRunError):
            resolve_agent_ids(self.cfg, [
                {'id': 7, 'name': 'x'}, {'name': None},
                self.entry('build', 'ag_b'),
            ])

    def test_another_pipelines_agents_do_not_count(self) -> None:
        other = pipeline.namespaced_agent_name('other', 'build')
        with self.assertRaises(PipelineRunError):
            resolve_agent_ids(self.cfg, [{'id': 'ag_x', 'name': other}])


if __name__ == '__main__':
    unittest.main()
