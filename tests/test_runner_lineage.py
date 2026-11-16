"""Which branch a stage builds on, and which a review inspects.

Run: python -m unittest tests.test_runner_lineage
"""

from __future__ import annotations

import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from sbx_omnigent import pipeline
from sbx_omnigent.pipeline import PipelineStage
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.lineage import review_target, seed_from, seed_ref
from sbx_omnigent.runner.records.results import NodeResult

_PIPELINE = """\
name: demo
repo: ./proj
publish: local
{base}task: |
  add parse_ports
agents:
  build: {{template: coder, model: claude-sonnet-5}}
stages:
  - {{id: build, run: build, write: true}}
"""


def _nodes(**branches: str | None) -> dict[str, NodeResult]:
    return {
        node: NodeResult(node, 'writer' if branch else 'reader',
                         branch=branch)
        for node, branch in branches.items()
    }


class TestSeedFrom(unittest.TestCase):
    def test_an_explicit_from_wins(self) -> None:
        stage = PipelineStage(id='build', from_branch='tests',
                              needs=('plan',))
        self.assertEqual(seed_from(stage, _nodes(plan=None)), 'tests')

    def test_else_the_first_need_with_a_branch(self) -> None:
        stage = PipelineStage(id='refactor', needs=('plan', 'build'))
        nodes = _nodes(plan=None, build='pl/r1/build')
        self.assertEqual(seed_from(stage, nodes), 'build')

    def test_a_need_that_has_not_run_is_passed_over(self) -> None:
        stage = PipelineStage(id='refactor', needs=('build',))
        self.assertIsNone(seed_from(stage, {}))

    def test_no_writer_upstream_cuts_from_the_base(self) -> None:
        stage = PipelineStage(id='plan', needs=('brief',))
        self.assertIsNone(seed_from(stage, _nodes(brief=None)))


class TestReviewTarget(unittest.TestCase):
    def test_it_is_the_first_need_with_a_branch(self) -> None:
        stage = PipelineStage(id='review', needs=('plan', 'build'),
                              gate='consensus', on_block='build')
        nodes = _nodes(plan=None, build='pl/r1/build')
        self.assertEqual(review_target(stage, nodes), 'build')

    def test_a_review_with_no_writer_is_refused(self) -> None:
        stage = PipelineStage(id='review', needs=('plan',),
                              gate='consensus')
        with self.assertRaises(PipelineRunError) as caught:
            review_target(stage, _nodes(plan=None))
        self.assertIn("review 'review' has no writer", str(caught.exception))


class TestSeedRef(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.wt = mock.Mock()
        self.wt.node_branch.side_effect = lambda run, node: f'pl/{run}/{node}'

    def ctx(self, *, base: str | None,
            nodes: dict[str, NodeResult]) -> RunContext:
        path = self.root / 'pipeline.yaml'
        path.write_text(_PIPELINE.format(
            base=f'base_branch: {base}\n' if base else '',
        ), encoding='utf-8')
        return RunContext(
            config=pipeline.load_pipeline(path), worktrees=self.wt,
            sessions=mock.Mock(), run_id='r1', nodes=nodes, stage_by_id={},
            lock=threading.RLock(), max_rounds=3,
        )

    def test_a_writer_upstream_is_its_hub_branch(self) -> None:
        ctx = self.ctx(base='main', nodes=_nodes(tests='pl/r1/tests'))
        stage = PipelineStage(id='build', needs=('tests',))
        self.assertEqual(seed_ref(ctx, stage), 'pl/r1/tests')

    def test_else_the_configured_base_branch(self) -> None:
        ctx = self.ctx(base='develop', nodes={})
        self.assertEqual(seed_ref(ctx, PipelineStage(id='build')), 'develop')

    def test_with_no_base_configured_it_is_main(self) -> None:
        ctx = self.ctx(base=None, nodes={})
        self.assertEqual(seed_ref(ctx, PipelineStage(id='build')), 'main')


if __name__ == '__main__':
    unittest.main()
