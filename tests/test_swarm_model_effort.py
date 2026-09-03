"""Per-agent model/effort sourcing + create-time application.

The swarm reads ``executor.model`` + ``llm.reasoning_effort`` from each
bound agent's bundle ``config.yaml`` and forwards them at session create
(``model_override`` / ``reasoning_effort``) — the Polly-style pin that
reaches native harnesses too. Run with:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sbx_omnigent import swarm


def _write_bundle(root: Path, name: str, body: str) -> None:
    d = root / name
    d.mkdir(parents=True)
    (d / 'config.yaml').write_text(body, encoding='utf-8')


class TestSpecModelEffort(unittest.TestCase):
    """`_spec_model_effort` reads model + effort, tolerating junk."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='bundles-'))

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_reads_executor_model_and_llm_effort(self) -> None:
        _write_bundle(
            self.root,
            'a',
            'executor:\n  model: claude-sonnet-5\n'
            'llm:\n  reasoning_effort: medium\n',
        )
        self.assertEqual(
            swarm._spec_model_effort(self.root / 'a' / 'config.yaml'),
            ('claude-sonnet-5', 'medium'),
        )

    def test_model_falls_back_to_llm_model(self) -> None:
        _write_bundle(self.root, 'a', 'llm:\n  model: claude-fable-5\n')
        model, effort = swarm._spec_model_effort(
            self.root / 'a' / 'config.yaml'
        )
        self.assertEqual(model, 'claude-fable-5')
        self.assertIsNone(effort)

    def test_missing_file_is_none_none(self) -> None:
        self.assertEqual(
            swarm._spec_model_effort(self.root / 'nope' / 'config.yaml'),
            (None, None),
        )

    def test_malformed_yaml_is_none_none(self) -> None:
        _write_bundle(self.root, 'a', 'executor: [unterminated\n')
        self.assertEqual(
            swarm._spec_model_effort(self.root / 'a' / 'config.yaml'),
            (None, None),
        )

    def test_non_mapping_top_level_is_none_none(self) -> None:
        _write_bundle(self.root, 'a', '- just\n- a\n- list\n')
        self.assertEqual(
            swarm._spec_model_effort(self.root / 'a' / 'config.yaml'),
            (None, None),
        )

    def test_non_string_values_ignored(self) -> None:
        # A mapping / list value must never be mistaken for a model or
        # effort string (omnigent's yaml patch coerces bare bools to
        # strings, so bool is not a reliable non-string probe here).
        _write_bundle(
            self.root,
            'a',
            'executor:\n  model:\n    nested: map\n'
            'llm:\n  reasoning_effort:\n    - a\n    - b\n',
        )
        self.assertEqual(
            swarm._spec_model_effort(self.root / 'a' / 'config.yaml'),
            (None, None),
        )


class TestModelEffortByRef(unittest.TestCase):
    """`_model_effort_by_ref` keys id + name; skips absent bundles."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='bundles-'))

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_keys_both_id_and_name(self) -> None:
        _write_bundle(
            self.root,
            'swarm-coder',
            'executor:\n  model: claude-sonnet-5\n'
            'llm:\n  reasoning_effort: medium\n',
        )
        agents = [
            {'id': 'ag_1', 'name': 'swarm-coder', 'harness': 'claude-native'}
        ]
        models, efforts = swarm._model_effort_by_ref(agents, self.root)
        self.assertEqual(models['ag_1'], 'claude-sonnet-5')
        self.assertEqual(models['swarm-coder'], 'claude-sonnet-5')
        self.assertEqual(efforts['ag_1'], 'medium')
        self.assertEqual(efforts['swarm-coder'], 'medium')

    def test_agent_without_bundle_absent(self) -> None:
        agents = [{'id': 'ag_1', 'name': 'no-bundle', 'harness': 'x'}]
        models, efforts = swarm._model_effort_by_ref(agents, self.root)
        self.assertEqual(models, {})
        self.assertEqual(efforts, {})

    def test_agent_with_only_effort(self) -> None:
        _write_bundle(
            self.root, 'rev', 'llm:\n  reasoning_effort: high\n'
        )
        agents = [{'id': 'ag_r', 'name': 'rev', 'harness': 'claude-native'}]
        models, efforts = swarm._model_effort_by_ref(agents, self.root)
        self.assertNotIn('ag_r', models)
        self.assertEqual(efforts['ag_r'], 'high')

    def test_env_override_dir(self) -> None:
        _write_bundle(self.root, 'a', 'executor:\n  model: m1\n')
        agents = [{'id': 'ag_1', 'name': 'a', 'harness': 'claude-native'}]
        with mock.patch.dict(
            os.environ, {swarm._AGENTS_DIR_ENV: str(self.root)}
        ):
            models, _ = swarm._model_effort_by_ref(agents)
        self.assertEqual(models['ag_1'], 'm1')


class TestOrchestratorAppliesModelEffort(unittest.TestCase):
    """start_swarm passes each agent's pinned model/effort to create."""

    def _create_calls(self, models, efforts) -> dict:
        sc = mock.Mock()
        sc.create.side_effect = lambda **kw: kw['agent_id']
        wt = mock.Mock()
        wt.create_swarm_worktree.return_value = '/wt/swarm-a'
        orch = swarm.SwarmOrchestrator(
            session_client=sc,
            worktree_manager=wt,
            coder_agent_id='ag_coder',
            reviewer_agent_id='ag_rev',
            agent_models=models,
            agent_efforts=efforts,
        )
        orch.start_swarm('s', 'repo', reviewer_roles=('security',))
        return {
            c.kwargs['agent_id']: c.kwargs
            for c in sc.create.call_args_list
        }

    def test_coder_and_reviewer_get_pins(self) -> None:
        calls = self._create_calls(
            {'ag_coder': 'claude-sonnet-5', 'ag_rev': 'claude-fable-5'},
            {'ag_coder': 'medium'},
        )
        self.assertEqual(
            calls['ag_coder']['model_override'], 'claude-sonnet-5'
        )
        self.assertEqual(calls['ag_coder']['reasoning_effort'], 'medium')
        self.assertEqual(
            calls['ag_rev']['model_override'], 'claude-fable-5'
        )
        # No effort pinned for the reviewer → None passed through.
        self.assertIsNone(calls['ag_rev']['reasoning_effort'])

    def test_no_maps_passes_none(self) -> None:
        calls = self._create_calls(None, None)
        self.assertIsNone(calls['ag_coder']['model_override'])
        self.assertIsNone(calls['ag_coder']['reasoning_effort'])


if __name__ == '__main__':
    unittest.main()
