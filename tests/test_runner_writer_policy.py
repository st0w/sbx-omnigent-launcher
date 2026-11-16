"""What a writer's branch may and must contain, checked from its diff.

Run: python -m unittest tests.test_runner_writer_policy
"""

from __future__ import annotations

import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.prompts.policy import (
    coverage_instruction,
    noop_findings,
    tests_only_instruction,
)
from sbx_omnigent.runner.records.results import NodeResult
from sbx_omnigent.runner.writer_policy import (
    WriterPolicy,
    is_generated,
    is_guarded,
    is_test_path,
)

_PIPELINE = """\
name: demo
repo: ./proj
publish: local
{extra}task: |
  add parse_ports
agents:
  build: {{template: coder, model: claude-sonnet-5}}
  sec: {{template: security-reviewer, model: claude-sonnet-5}}
stages:
  - {{id: tests, run: build, write: true, tests_only: true}}
  - {{id: build, run: build, write: true, from: tests}}
  - {{id: review, run: [sec], needs: [build], gate: consensus,
      on_block: build}}
"""


def _config(root: Path, extra: str = '') -> pipeline.PipelineConfig:
    path = root / 'pipeline.yaml'
    path.write_text(_PIPELINE.format(extra=extra), encoding='utf-8')
    return pipeline.load_pipeline(path)


class _PathTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)


class TestWhatIsBuildOutput(_PathTest):
    def test_lock_files_are_generated(self) -> None:
        cfg = _config(self.root)
        for path in ('Cargo.lock', 'crates/x/Cargo.lock', 'go.sum',
                     'package-lock.json'):
            with self.subTest(path=path):
                self.assertTrue(is_generated(cfg, path))

    def test_source_is_not(self) -> None:
        self.assertFalse(is_generated(_config(self.root), 'src/lib.rs'))

    def test_a_pipelines_own_list_replaces_the_default(self) -> None:
        cfg = _config(self.root, 'generated: ["*.snap"]\n')
        self.assertTrue(is_generated(cfg, 'tests/x.snap'))
        self.assertFalse(is_generated(cfg, 'Cargo.lock'))


class TestWhatATestsStageMayWrite(_PathTest):
    def test_test_code_is_allowed(self) -> None:
        cfg = _config(self.root)
        for path in ('tests/a.rs', 'crates/x/tests/b.rs', 'ports_test.go',
                     'test_ports.py', 'ports.test.ts', 'conftest.py'):
            with self.subTest(path=path):
                self.assertTrue(is_test_path(cfg, path))

    def test_a_dependency_manifest_is_allowed(self) -> None:
        # A test needing a new dev-dependency must declare it.
        cfg = _config(self.root)
        self.assertTrue(is_test_path(cfg, 'crates/x/Cargo.toml'))

    def test_production_code_is_not(self) -> None:
        self.assertFalse(is_test_path(_config(self.root), 'src/ports.rs'))

    def test_a_pipelines_own_list_replaces_the_test_globs(self) -> None:
        cfg = _config(self.root, 'test_paths: ["spec/**"]\n')
        self.assertTrue(is_test_path(cfg, 'spec/ports_spec.rb'))
        self.assertFalse(is_test_path(cfg, 'tests/a.rs'))
        self.assertTrue(is_test_path(cfg, 'Cargo.toml'))


class TestWhatIsGuarded(_PathTest):
    def test_a_declared_check_is_guarded_by_name_or_path(self) -> None:
        cfg = _config(self.root,
                      'guarded: [".github/workflows/*", "deny.toml"]\n')
        self.assertTrue(is_guarded(cfg, '.github/workflows/ci.yml'))
        self.assertTrue(is_guarded(cfg, 'crates/x/deny.toml'))
        self.assertFalse(is_guarded(cfg, 'src/lib.rs'))

    def test_nothing_is_guarded_by_default(self) -> None:
        self.assertFalse(is_guarded(_config(self.root), 'deny.toml'))


class _PolicyTest(_PathTest):
    def setUp(self) -> None:
        super().setUp()
        echo = mock.patch.object(click, 'echo')
        echo.start()
        self.addCleanup(echo.stop)
        self.wt = mock.Mock()
        self.wt.node_branch.side_effect = lambda run, node: f'pl/{run}/{node}'
        #: What each successive diff returns: one list per call.
        self.diffs: list[list[str]] = []
        self.wt.node_diff_files.side_effect = lambda *a, **k: self.diffs.pop(0)
        self.added: list[list[str]] = []
        self.wt.node_added_lines.side_effect = (
            lambda *a, **k: self.added.pop(0)
        )
        self.redriven: list[tuple[str, str, str | None, str | None]] = []
        self.nodes = {
            'tests': NodeResult('tests', 'writer', branch='pl/r1/tests'),
            'build': NodeResult('build', 'writer', branch='pl/r1/build'),
        }

    def policy(self, extra: str = '') -> WriterPolicy:
        cfg = _config(self.root, extra)
        self.stages = {s.id: s for s in cfg.stages}
        ctx = RunContext(
            config=cfg, worktrees=self.wt, sessions=mock.Mock(), run_id='r1',
            nodes=self.nodes, stage_by_id=self.stages,
            lock=threading.RLock(), max_rounds=2,
        )

        def redrive(stage_id, findings, *, message=None, instruction=None):
            self.redriven.append((stage_id, findings, message, instruction))

        return WriterPolicy(ctx, redrive_writer=redrive)


class TestAWriterMustImplement(_PolicyTest):
    def test_a_branch_with_source_changes_passes(self) -> None:
        policy = self.policy()
        self.added = [[]]
        self.diffs = [['src/ports.rs', 'Cargo.lock']]
        policy.enforce(self.stages['build'])
        self.assertEqual(self.redriven, [])

    def test_only_build_output_is_re_driven_then_refused(self) -> None:
        policy = self.policy()
        self.added = [[]]
        self.diffs = [['Cargo.lock'], ['Cargo.lock']]
        with self.assertRaises(PipelineRunError) as caught:
            policy.enforce(self.stages['build'])
        self.assertIn(
            'build produced no implementation', str(caught.exception),
        )
        self.assertEqual(len(self.redriven), 2)
        self.assertIn('Cargo.lock', self.redriven[0][1])

    def test_it_is_diffed_against_where_it_was_cut(self) -> None:
        policy = self.policy()
        self.added = [[]]
        self.diffs = [['src/ports.rs']]
        policy.enforce(self.stages['build'])
        self.assertEqual(
            self.wt.node_diff_files.call_args.kwargs['against'],
            'pl/r1/tests',
        )

    def test_a_git_failure_never_fails_the_stage(self) -> None:
        policy = self.policy()
        self.added = [[]]
        self.wt.node_diff_files.side_effect = click.ClickException('git')
        policy.enforce(self.stages['build'])
        self.assertEqual(self.redriven, [])


class TestATestsStageWritesOnlyTests(_PolicyTest):
    def test_production_edits_are_given_back_once(self) -> None:
        policy = self.policy()
        self.added = [[]]
        self.diffs = [['tests/a.rs', 'src/x.rs'], ['tests/a.rs'],
                      ['tests/a.rs']]
        policy.enforce(self.stages['tests'])
        (stage, findings, message, instruction), = self.redriven
        self.assertEqual((stage, message),
                         ('tests', 'tests: restrict to test code'))
        self.assertIn('src/x.rs', findings)
        self.assertEqual(instruction, tests_only_instruction(findings))

    def test_production_edits_that_stay_are_refused(self) -> None:
        policy = self.policy()
        self.added = [[]]
        self.diffs = [['src/x.rs'], ['src/x.rs']]
        with self.assertRaises(PipelineRunError) as caught:
            policy.enforce(self.stages['tests'])
        self.assertIn('Nothing was reverted', str(caught.exception))

    def test_a_lock_file_is_excused(self) -> None:
        policy = self.policy()
        self.added = [[]]
        self.diffs = [['tests/a.rs', 'Cargo.lock'], ['tests/a.rs']]
        policy.enforce(self.stages['tests'])
        self.assertEqual(self.redriven, [])


class TestCoverageIsEarned(_PolicyTest):
    def test_a_suppression_marker_is_given_back(self) -> None:
        policy = self.policy()
        self.added = [['    x = 1  # pragma: no cover\n'], []]
        self.diffs = [['src/x.py']]
        policy.enforce(self.stages['build'])
        (_stage, findings, message, instruction), = self.redriven
        self.assertEqual(message, 'build: remove coverage suppression')
        self.assertIn('x = 1  # pragma: no cover', findings)
        self.assertEqual(instruction, coverage_instruction(findings))

    def test_a_marker_that_stays_is_refused(self) -> None:
        policy = self.policy()
        line = '// LCOV_EXCL_LINE'
        self.added = [[line], [line]]
        with self.assertRaises(PipelineRunError) as caught:
            policy.enforce(self.stages['build'])
        self.assertIn('kept them after being asked', str(caught.exception))

    def test_an_ordinary_comment_is_not_a_marker(self) -> None:
        policy = self.policy()
        self.added = [['# cover the edge case below']]
        self.diffs = [['src/x.py']]
        policy.enforce(self.stages['build'])
        self.assertEqual(self.redriven, [])


class TestGuardedChecksAreNamedToReviewers(_PolicyTest):
    def test_a_changed_check_is_listed(self) -> None:
        policy = self.policy('guarded: ["deny.toml"]\n')
        self.diffs = [['deny.toml', 'src/x.rs']]
        block = policy.guarded_block(self.stages['review'])
        self.assertIn('  - deny.toml', block)
        self.assertNotIn('src/x.rs', block)

    def test_nothing_guarded_changed_says_nothing(self) -> None:
        policy = self.policy('guarded: ["deny.toml"]\n')
        self.diffs = [['src/x.rs']]
        self.assertEqual(policy.guarded_block(self.stages['review']), '')

    def test_without_a_guarded_list_nothing_is_diffed(self) -> None:
        self.assertEqual(self.policy().guarded_block(self.stages['review']),
                         '')
        self.wt.node_diff_files.assert_not_called()


class TestTheTurnText(unittest.TestCase):
    def test_no_files_changed_says_so(self) -> None:
        self.assertIn('you changed NO files at all', noop_findings([]))

    def test_the_tests_only_turn_restates_the_frozen_contract(self) -> None:
        self.assertIn('FROZEN', tests_only_instruction('- src/x.py'))


if __name__ == '__main__':
    unittest.main()
