"""Pipeline config parse + template + materialize tests.

Run: .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

import yaml
from omnigent.spec import parse

from sbx_omnigent import pipeline as P

_FULL = """\
version: 1
name: Mixed Models
repo: ./proj
publish: pr
task: |
  do the thing
acceptance: |
  it works
agents:
  plan:
    template: planner
    harness: antigravity-native
    model: gemini-3.5-flash
  build:
    template: coder
    model: claude-sonnet-5
    effort: medium
  sec:
    template: security-reviewer
    model: claude-fable-5
stages:
  - {id: plan, run: plan}
  - {id: build, run: build, write: true, needs: [plan]}
  - id: review
    run: [sec]
    needs: [build]
    gate: consensus
    on_block: build
"""


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='pl-'))

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _write(self, text: str, name: str = 'pipeline.yaml') -> Path:
        p = self.root / name
        p.write_text(text, encoding='utf-8')
        return p

    def _load(self, text: str) -> P.PipelineConfig:
        return P.load_pipeline(self._write(text))


class TestLoadPipeline(_Base):
    def test_full_config(self) -> None:
        cfg = self._load(_FULL)
        self.assertEqual(cfg.name, 'mixed-models')  # sanitized
        self.assertEqual(cfg.repo, './proj')
        self.assertEqual(cfg.publish.mode, 'pr')
        self.assertTrue(cfg.task)
        self.assertTrue(cfg.acceptance)
        self.assertEqual(list(cfg.agents), ['plan', 'build', 'sec'])
        self.assertEqual(
            cfg.agents['plan'].harness, 'antigravity-native'
        )
        self.assertEqual(cfg.agents['build'].model, 'claude-sonnet-5')
        self.assertEqual(cfg.agents['build'].effort, 'medium')
        self.assertEqual(
            [s.id for s in cfg.stages], ['plan', 'build', 'review']
        )
        review = cfg.stages[2]
        self.assertEqual(review.run, ('sec',))
        self.assertEqual(review.gate, 'consensus')
        self.assertEqual(review.on_block, 'build')
        self.assertTrue(cfg.stages[1].write)

    def test_default_stages_when_omitted(self) -> None:
        cfg = self._load(
            'repo: ./p\n'
            'agents:\n'
            '  build: {template: coder}\n'
            '  sec: {template: security-reviewer}\n'
        )
        ids = [s.id for s in cfg.stages]
        self.assertEqual(ids, ['build', 'review'])
        self.assertTrue(cfg.stages[0].write)
        self.assertEqual(cfg.stages[0].run, ('build',))
        self.assertEqual(cfg.stages[1].run, ('sec',))
        self.assertEqual(cfg.stages[1].on_block, 'build')

    def test_missing_repo_errors(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load('agents:\n  a: {template: coder}\n')

    def test_no_agents_errors(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load('repo: ./p\nagents: {}\n')

    def test_unknown_template_errors(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load('repo: ./p\nagents:\n  a: {template: nope}\n')

    def test_prompt_and_prompt_file_conflict(self) -> None:
        (self.root / 'p.md').write_text('hi', encoding='utf-8')
        with self.assertRaises(P.PipelineError):
            self._load(
                'repo: ./p\nagents:\n'
                '  a: {prompt: inline, prompt_file: ./p.md}\n'
            )

    def test_prompt_file_resolves(self) -> None:
        (self.root / 'p.md').write_text('CUSTOM PROMPT', encoding='utf-8')
        cfg = self._load(
            'repo: ./p\nagents:\n  a: {prompt_file: ./p.md, template: coder}\n'
        )
        # Inline/file override wins over the template.
        self.assertEqual(cfg.agents['a'].prompt, 'CUSTOM PROMPT')

    def test_inline_prompt_overrides_template(self) -> None:
        cfg = self._load(
            'repo: ./p\nagents:\n'
            '  a: {template: coder, prompt: "just this"}\n'
        )
        self.assertEqual(cfg.agents['a'].prompt, 'just this')

    def test_agent_without_template_or_prompt_errors(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load('repo: ./p\nagents:\n  a: {harness: claude-native}\n')

    def test_skills_missing_dir_errors(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load(
                'repo: ./p\nagents:\n'
                '  a: {template: coder, skills: ./nope}\n'
            )

    def test_skills_dir_resolves(self) -> None:
        (self.root / 'sk').mkdir()
        (self.root / 'sk' / 'S.md').write_text('x', encoding='utf-8')
        cfg = self._load(
            'repo: ./p\nagents:\n  a: {template: coder, skills: ./sk}\n'
        )
        self.assertIsNotNone(cfg.agents['a'].skills_dir)

    def test_default_harness(self) -> None:
        cfg = self._load('repo: ./p\nagents:\n  a: {template: coder}\n')
        self.assertEqual(cfg.agents['a'].harness, 'claude-native')

    def test_publish_string_and_mapping(self) -> None:
        base = 'repo: ./p\nagents:\n  build: {template: coder}\n'
        local = self._load(base + 'publish: local\n')
        self.assertEqual(local.publish.mode, 'local')
        cfg = self._load(base + 'publish:\n  mode: pr\n  branch: build\n')
        self.assertEqual(cfg.publish.mode, 'pr')
        self.assertEqual(cfg.publish.branch, 'build')

    def test_publish_invalid_mode(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load(
                'repo: ./p\nagents:\n  build: {template: coder}\n'
                'publish: bogus\n'
            )

    def test_plan_artifact_default_and_custom(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        self.assertIsNone(self._load(base).plan_artifact)  # default
        cfg = self._load(base + 'plan_artifact: docs/design/plan.md\n')
        self.assertEqual(cfg.plan_artifact, 'docs/design/plan.md')

    def test_plan_artifact_unsafe_path_errors(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        for bad in ('/etc/x.md', '../escape.md', 'docs/../../x'):
            with self.assertRaises(P.PipelineError):
                self._load(base + f'plan_artifact: {bad}\n')

    def test_context_parsed_and_optional(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        self.assertIsNone(self._load(base).context)  # default
        cfg = self._load(base + 'context: |\n  Django app; use pytest.\n')
        self.assertIn('Django app; use pytest.', cfg.context)

    def test_context_non_string_errors(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(base + 'context: [not, a, string]\n')

    def test_context_from_file(self) -> None:
        # context_file reads the shared context from a path relative to
        # the pipeline file (mirrors an agent's prompt_file).
        self._write('Django app; use pytest.\n', 'ctx.md')
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        cfg = self._load(base + 'context_file: ctx.md\n')
        self.assertIn('Django app; use pytest.', cfg.context)

    def test_context_and_context_file_conflict(self) -> None:
        self._write('x', 'ctx.md')
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(base + 'context: inline\ncontext_file: ctx.md\n')

    def test_context_file_missing_errors(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(base + 'context_file: nope.md\n')

    def test_task_and_acceptance_from_file(self) -> None:
        # task_file / acceptance_file mirror context_file (same helper).
        self._write('build the thing\n', 't.md')
        self._write('it works\n', 'a.md')
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        cfg = self._load(base + 'task_file: t.md\nacceptance_file: a.md\n')
        self.assertIn('build the thing', cfg.task)
        self.assertIn('it works', cfg.acceptance)

    def test_task_and_task_file_conflict(self) -> None:
        self._write('x', 't.md')
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(base + 'task: inline\ntask_file: t.md\n')

    def test_subtasks_default_empty(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        self.assertEqual(self._load(base).subtasks, ())

    def test_subtasks_inline_list(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        cfg = self._load(
            base + 'subtasks:\n'
            '  - {id: M0, title: Contracts & core}\n'
            '  - {id: m1, title: Storage & schema}\n'
        )
        self.assertEqual([s.id for s in cfg.subtasks], ['m0', 'm1'])
        self.assertEqual(cfg.subtasks[0].title, 'Contracts & core')

    def test_subtasks_inline_dedupes_ids(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        cfg = self._load(
            base + 'subtasks:\n'
            '  - {id: m, title: one}\n'
            '  - {id: m, title: two}\n'
        )
        self.assertEqual([s.id for s in cfg.subtasks], ['m', 'm-2'])

    def test_subtasks_from_file(self) -> None:
        self._write(
            '# Modules\n'
            '- [m0] Contracts & core\n'
            '- [m1] Storage & schema\n'
            'prose after is ignored as a non-item line\n',
            'mods.md',
        )
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        cfg = self._load(base + 'subtask_file: mods.md\n')
        self.assertEqual([s.id for s in cfg.subtasks], ['m0', 'm1'])
        self.assertEqual(cfg.subtasks[1].title, 'Storage & schema')

    def test_subtasks_and_file_conflict(self) -> None:
        self._write('- [m0] x\n', 'mods.md')
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(
                base + 'subtasks:\n  - {id: m0, title: x}\n'
                'subtask_file: mods.md\n'
            )

    def test_subtask_file_missing_errors(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(base + 'subtask_file: nope.md\n')

    def test_subtask_file_no_items_errors(self) -> None:
        self._write('just prose, no items here\n', 'mods.md')
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(base + 'subtask_file: mods.md\n')

    def test_subtasks_empty_list_errors(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(base + 'subtasks: []\n')

    def test_subtasks_missing_fields_error(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(base + 'subtasks:\n  - {id: m0}\n')
        with self.assertRaises(P.PipelineError):
            self._load(base + 'subtasks:\n  - {title: no id}\n')

    def test_turn_timeout_default_and_custom(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        self.assertIsNone(self._load(base).turn_timeout)
        self.assertEqual(self._load(base + 'turn_timeout: 3600\n')
                         .turn_timeout, 3600.0)

    def test_turn_timeout_must_be_a_positive_number(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        for bad in ('0', '-5', 'soon', 'true'):
            with self.assertRaises(P.PipelineError):
                self._load(base + f'turn_timeout: {bad}\n')

    def test_setup_inline_and_from_file(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        self.assertIsNone(self._load(base).setup)
        cfg = self._load(base + 'setup: |\n  install the toolchain\n')
        self.assertIn('install the toolchain', cfg.setup)
        self._write('rustup, then cargo\n', 'env.md')
        cfg = self._load(base + 'setup_file: env.md\n')
        self.assertIn('rustup, then cargo', cfg.setup)

    def test_setup_and_setup_file_conflict(self) -> None:
        self._write('x', 'env.md')
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(base + 'setup: inline\nsetup_file: env.md\n')

    def test_generated_default_and_custom(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        self.assertEqual(self._load(base).generated, ())  # runner default
        cfg = self._load(base + 'generated: ["*.lock", "gen/**"]\n')
        self.assertEqual(cfg.generated, ('*.lock', 'gen/**'))

    def test_generated_must_be_a_list_of_strings(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        for bad in ('"*.lock"', '{a: b}', '[1, 2]', '[""]'):
            with self.assertRaises(P.PipelineError):
                self._load(base + f'generated: {bad}\n')

    def test_guarded_default_and_custom(self) -> None:
        # No default set on purpose: which files ARE a check is a
        # property of the project, and a built-in list naming
        # `deny.toml` would teach the launcher one ecosystem's tooling.
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        self.assertEqual(self._load(base).guarded, ())
        cfg = self._load(
            base + 'guarded: ["deny.toml", ".github/workflows/*"]\n'
        )
        self.assertEqual(cfg.guarded, ('deny.toml', '.github/workflows/*'))

    def test_guarded_must_be_a_list_of_strings(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        for bad in ('"deny.toml"', '{a: b}', '[1, 2]', '[""]'):
            with self.assertRaises(P.PipelineError):
                self._load(base + f'guarded: {bad}\n')

    def test_verify_absent_by_default(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        self.assertIsNone(self._load(base).verify)

    def test_verify_parsed(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        cfg = self._load(
            base + 'verify:\n'
            '  coverage_min: 95\n'
            '  timeout: 2400\n'
            '  command: |\n'
            '    cov --fail-under {coverage_min}\n'
        )
        self.assertEqual(cfg.verify.coverage_min, 95.0)
        self.assertEqual(cfg.verify.timeout_s, 2400.0)
        self.assertIn('{coverage_min}', cfg.verify.command)

    def test_verify_threshold_is_substituted_not_formatted(self) -> None:
        # A real shell command is full of braces (${VAR}, awk),
        # str.format would corrupt or raise on them.
        spec = P.VerifySpec(
            command="cov --min {coverage_min} && awk '{print $1}' f",
            coverage_min=95.0,
        )
        self.assertEqual(
            spec.rendered(), "cov --min 95 && awk '{print $1}' f"
        )

    def test_threshold_survives_brace_heavy_commands(self) -> None:
        # The reason substitution is a literal replace: real commands
        # carry shell and JSON braces that str.format would corrupt or
        # raise on. Go has no threshold flag at all, so it compares the
        # number in awk; a jest threshold IS json.
        cases = {
            "awk '/^total:/ {if ($3+0 < {coverage_min}) exit 1}'":
                "awk '/^total:/ {if ($3+0 < 95) exit 1}'",
            'jest --coverageThreshold=\'{"global":{"lines":'
            "{coverage_min}}}'":
                'jest --coverageThreshold=\'{"global":{"lines":95}}\'',
        }
        for command, want in cases.items():
            with self.subTest(command=command):
                spec = P.VerifySpec(command=command, coverage_min=95.0)
                self.assertEqual(spec.rendered(), want)

    def test_a_gate_without_a_threshold_is_valid(self) -> None:
        # Tests-really-pass is the core value; no coverage tool.
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        cfg = self._load(base + 'verify:\n  command: make check\n')
        self.assertIsNone(cfg.verify.coverage_min)
        self.assertEqual(cfg.verify.rendered(), 'make check')

    def test_verify_threshold_keeps_a_fractional_value(self) -> None:
        spec = P.VerifySpec(command='c {coverage_min}', coverage_min=99.5)
        self.assertEqual(spec.rendered(), 'c 99.5')

    def test_verify_command_required(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(base + 'verify:\n  coverage_min: 95\n')

    def test_verify_placeholder_without_a_threshold_errors(self) -> None:
        # Otherwise a literal brace reaches the shell and fails
        # cryptically inside a microVM, minutes later.
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        with self.assertRaises(P.PipelineError):
            self._load(
                base + 'verify:\n  command: cov --min {coverage_min}\n'
            )

    def test_verify_threshold_must_be_a_sane_number(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        for bad in ('-1', '101', 'high', 'true'):
            with self.assertRaises(P.PipelineError):
                self._load(
                    base + f'verify:\n  command: c\n  coverage_min: {bad}\n'
                )

    def _gated(self, verify_block: str, setup: str = '') -> str:
        return (
            'repo: ./p\nagents:\n  a: {template: coder}\n'
            + setup
            + verify_block
        )

    _PROSE_SETUP = (
        'setup: |\n'
        '  This VM is a fresh sandbox with NO Rust toolchain. Install one:\n'
        '    curl -sSf https://sh.rustup.rs | sh -s -- -y\n'
    )

    def test_prose_setup_with_a_gate_and_no_verify_setup_is_refused(self):
        # The live failure: setup: is prose for AGENTS, and pasting it
        # into the gate's shell died on its first word ("sh: 2: This:
        # not found", exit 127) — which then looked like the branch
        # failing its tests and re-drove a writer three times.
        with self.assertRaises(P.PipelineError) as ctx:
            self._load(
                self._gated(
                    'verify:\n  command: cargo test\n', self._PROSE_SETUP
                )
            )
        msg = str(ctx.exception)
        self.assertIn('verify.setup', msg)
        self.assertIn('NOT shell', msg)

    def test_an_explicit_verify_setup_satisfies_it(self) -> None:
        cfg = self._load(
            self._gated(
                'verify:\n  command: cargo test\n'
                '  setup: |\n    curl -sSf https://sh.rustup.rs | sh\n',
                self._PROSE_SETUP,
            )
        )
        self.assertIn('rustup.rs', cfg.verify.setup)

    def test_an_empty_verify_setup_is_an_explicit_opt_out(self) -> None:
        # For a command that installs what it needs itself.
        cfg = self._load(
            self._gated(
                "verify:\n  command: ./scripts/verify.sh\n  setup: ''\n",
                self._PROSE_SETUP,
            )
        )
        self.assertEqual(cfg.verify.setup, '')

    def test_no_prose_setup_needs_no_verify_setup(self) -> None:
        # Nothing told the agents to install anything, so the gate is
        # not being asked to either.
        cfg = self._load(self._gated('verify:\n  command: make test\n'))
        self.assertIsNone(cfg.verify.setup)

    def test_a_non_string_verify_setup_is_rejected(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load(
                self._gated('verify:\n  command: t\n  setup: [a, b]\n')
            )

    def test_publish_stacks_by_default(self) -> None:
        # Right in both worlds: if modules merge promptly GitHub
        # re-targets the request, and if they queue the diffs stay
        # readable. A plain string mode gets it too.
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        self.assertTrue(self._load(base + 'publish: pr\n').publish.stack)
        self.assertTrue(
            self._load(base + 'publish:\n  mode: pr\n').publish.stack
        )

    def test_publish_stacking_can_be_turned_off(self) -> None:
        cfg = self._load(
            'repo: ./p\nagents:\n  a: {template: coder}\n'
            'publish:\n  mode: pr\n  stack: false\n'
        )
        self.assertFalse(cfg.publish.stack)

    def test_a_non_boolean_stack_is_refused(self) -> None:
        for bad in ("'yes'", '1', '[]'):
            with self.subTest(bad=bad):
                with self.assertRaises(P.PipelineError):
                    self._load(
                        'repo: ./p\nagents:\n  a: {template: coder}\n'
                        f'publish:\n  mode: pr\n  stack: {bad}\n'
                    )

    def test_verify_resource_limits_are_parsed(self) -> None:
        cfg = self._load(
            self._gated(
                'verify:\n  command: make test\n'
                "  cpus: 4\n  memory: '8g'\n"
            )
        )
        self.assertEqual((cfg.verify.cpus, cfg.verify.memory), (4, '8g'))

    def test_verify_resource_limits_default_to_unset(self) -> None:
        # Unset must stay unset: it means "leave sbx's default", and
        # inventing a number here would silently reshape every gate.
        cfg = self._load(self._gated('verify:\n  command: make test\n'))
        self.assertIsNone(cfg.verify.cpus)
        self.assertIsNone(cfg.verify.memory)

    def test_a_nonsense_cpu_count_is_refused(self) -> None:
        for bad in ('0', '-2', 'four', 'true'):
            with self.subTest(bad=bad):
                with self.assertRaises(P.PipelineError):
                    self._load(
                        self._gated(
                            f'verify:\n  command: t\n  cpus: {bad}\n'
                        )
                    )

    def test_a_nonsense_memory_limit_is_refused(self) -> None:
        # A bare number is the likely slip — sbx wants '8g', not 8.
        for bad in ("''", '8', '[]'):
            with self.subTest(bad=bad):
                with self.assertRaises(P.PipelineError):
                    self._load(
                        self._gated(
                            f'verify:\n  command: t\n  memory: {bad}\n'
                        )
                    )

    def test_disk_defaults_suit_a_compiled_project(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        disk = self._load(base).disk
        # Measured on a live Rust run: a builder's VM overlay holds
        # the toolchain it installs (rustup + registry + llvm-cov) and
        # the worktree holds target/. The old 1.5 GB/VM guess under-read
        # the peak by ~2.5x and let a run start that could not finish.
        self.assertEqual(disk.per_vm_gb, 3.5)
        # 4.0, not 2.0: every writer this project ever measured cost
        # MORE than 2.0 (2.2 GB smallest, 26 GB largest), so the old
        # default under-counted every instance of the compiled case
        # it claims to model. See DiskSpec for the calibration.
        self.assertEqual(disk.per_worktree_gb, 4.0)
        self.assertEqual(disk.headroom_gb, 5.0)

    def test_disk_values_are_overridable(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        disk = self._load(
            base + 'disk:\n'
            '  per_vm_gb: 1\n  per_worktree_gb: 0.2\n  headroom_gb: 3\n'
        ).disk
        self.assertEqual(
            (disk.per_vm_gb, disk.per_worktree_gb, disk.headroom_gb),
            (1.0, 0.2, 3.0),
        )

    def test_disk_partial_override_keeps_the_other_defaults(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        disk = self._load(base + 'disk:\n  per_worktree_gb: 0.1\n').disk
        self.assertEqual(disk.per_worktree_gb, 0.1)
        self.assertEqual(disk.per_vm_gb, 3.5)  # untouched

    def test_disk_zero_is_allowed(self) -> None:
        # Deliberately zeroing a term is a legitimate "this project does
        # not pay that cost" — only nonsense is rejected.
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        self.assertEqual(
            self._load(base + 'disk:\n  per_worktree_gb: 0\n')
            .disk.per_worktree_gb,
            0.0,
        )

    def test_disk_rejects_nonsense(self) -> None:
        base = 'repo: ./p\nagents:\n  a: {template: coder}\n'
        for bad in ('  per_vm_gb: -1', '  headroom_gb: lots',
                    '  per_worktree_gb: true'):
            with self.assertRaises(P.PipelineError):
                self._load(base + f'disk:\n{bad}\n')

    def test_stage_unknown_agent_errors(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load(
                'repo: ./p\nagents:\n  a: {template: coder}\n'
                'stages:\n  - {id: s, run: ghost}\n'
            )

    def test_stage_unknown_needs_errors(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load(
                'repo: ./p\nagents:\n  a: {template: coder}\n'
                'stages:\n  - {id: s, run: a, needs: [nope]}\n'
            )

    def test_duplicate_stage_id_errors(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load(
                'repo: ./p\nagents:\n  a: {template: coder}\n'
                'stages:\n  - {id: s, run: a}\n  - {id: s, run: a}\n'
            )

    def test_a_model_its_harness_cannot_run_is_rejected_at_load(
        self,
    ) -> None:
        # Omnigent enforces this at session CREATE, which on a full
        # cadre is stage 6 of 8 — ten microVMs in. Catch it at parse.
        for harness, model in (
            ('codex-native', 'claude-opus-5'),
            ('claude-native', 'gpt-5.6-sol'),
            ('antigravity-native', 'claude-opus-5'),
        ):
            with self.subTest(harness=harness, model=model):
                with self.assertRaises(P.PipelineError) as caught:
                    self._load(
                        'repo: ./p\nagents:\n'
                        f'  a: {{template: coder, harness: {harness}, '
                        f'model: {model}}}\n'
                        'stages:\n  - {id: s, run: a}\n'
                    )
                # The message must name the agent — a family reason with
                # no agent name is not actionable in an eight-seat file.
                self.assertIn("agent 'a'", str(caught.exception))

    def test_a_matching_model_and_harness_load_cleanly(self) -> None:
        # The guard must not reject the cadre we actually run.
        for harness, model in (
            ('claude-native', 'claude-opus-5'),
            ('codex-native', 'gpt-5.6-sol'),
            ('antigravity-native', 'gemini-3.6-flash-high'),
        ):
            with self.subTest(harness=harness, model=model):
                cfg = self._load(
                    'repo: ./p\nagents:\n'
                    f'  a: {{template: coder, harness: {harness}, '
                    f'model: {model}}}\n'
                    'stages:\n  - {id: s, run: a}\n'
                )
                self.assertEqual(cfg.agents['a'].model, model)

    def test_an_agent_with_no_model_is_not_family_checked(self) -> None:
        # No `model:` means the bundle's own default — nothing to check,
        # and guessing a family from the harness would be wrong.
        cfg = self._load(
            'repo: ./p\nagents:\n'
            '  a: {template: coder, harness: codex-native}\n'
            'stages:\n  - {id: s, run: a}\n'
        )
        self.assertIsNone(cfg.agents['a'].model)

    def test_parallel_competing_writers(self) -> None:
        cfg = self._load(
            'repo: ./p\n'
            'agents:\n'
            '  ca: {template: coder}\n'
            '  cb: {template: coder}\n'
            '  jg: {template: judge}\n'
            'stages:\n'
            '  - id: impl\n'
            '    parallel:\n'
            '      - {id: impl-a, run: ca, write: true}\n'
            '      - {id: impl-b, run: cb, write: true}\n'
            '  - id: pick\n'
            '    run: jg\n'
            '    needs: [impl-a, impl-b]\n'
            '    selects: branch\n'
        )
        impl = cfg.stages[0]
        self.assertEqual(len(impl.parallel), 2)
        self.assertEqual(impl.parallel[0].id, 'impl-a')
        self.assertTrue(impl.parallel[0].write)
        self.assertEqual(cfg.stages[1].selects, 'branch')


class TestDuplicateKeysAreRejected(_Base):
    """PyYAML accepts a repeated mapping key and keeps the LAST one, so
    a config can parse clean, validate clean, and still not mean what it
    says. Live on 2026-08-14 a `verify:` block ended up with two
    `command:` keys — the coverage gate just added, and the weaker
    command it was meant to replace. The gate ran the weak one,
    `coverage_min: 95` sat referenced by nothing, and it was caught only
    by rendering the gate program through the runner's real code
    path."""

    _BASE = 'repo: ./proj\nagents:\n  a: {template: coder}\n'

    def _reject(self, text: str) -> str:
        with self.assertRaises(P.PipelineError) as caught:
            self._load(self._BASE + text)
        return str(caught.exception)

    def test_the_live_bug_two_commands_under_verify(self) -> None:
        message = self._reject(
            'verify:\n'
            '  coverage_min: 95\n'
            '  command: ./scripts/verify.sh {coverage_min}\n'
            '  command: cargo test --workspace --locked\n'
        )
        self.assertIn("duplicate key 'command'", message)

    def test_the_message_names_both_line_numbers(self) -> None:
        # Without both, the operator has to eyeball a long file for a
        # key they already believe appears once.
        message = self._reject(
            'verify:\n  command: a\n  command: b\n'
        )
        self.assertIn('first defined on line 5', message)
        self.assertIn('redefined on line 6', message)

    def test_the_message_explains_that_the_last_one_silently_wins(
        self,
    ) -> None:
        # The consequence is the whole point: this IS valid YAML, which
        # is exactly why it needs explaining rather than just rejecting.
        message = self._reject('publish: local\npublish: pr\n')
        self.assertIn('keeps the LAST definition', message)

    def test_a_duplicate_is_caught_at_any_depth(self) -> None:
        self.assertIn(
            "duplicate key 'write'",
            self._reject(
                'stages:\n'
                '  - {id: build, run: a, write: true, write: false}\n'
            ),
        )

    def test_a_repeated_agent_name_no_longer_silently_drops_one(self) -> None:
        self.assertIn(
            "duplicate key 'a'",
            self._reject('  a: {template: judge}\n'),
        )

    def test_a_clean_config_still_loads(self) -> None:
        cfg = self._load(self._BASE + 'publish: local\n')
        self.assertEqual(cfg.publish.mode, 'local')

    def test_yaml_merge_keys_still_work(self) -> None:
        # `<<` has no constructor of its own — the base class resolves
        # it in flatten_mapping — so constructing it while checking for
        # duplicates raised "could not determine a constructor" and
        # broke every anchor/merge config that loads fine today.
        cfg = self._load(
            'repo: ./proj\n'
            '_d: &d {template: coder, model: claude-sonnet-5}\n'
            'agents:\n'
            '  a:\n'
            '    <<: *d\n'
            '    model: claude-opus-4-8\n'
        )
        # An explicit key overriding a MERGED one is legal YAML and must
        # not be mistaken for a duplicate.
        self.assertEqual(cfg.agents['a'].model, 'claude-opus-4-8')
        self.assertEqual(cfg.agents['a'].template, 'coder')


class TestBuildCacheConfig(_Base):
    """
    ``build_cache`` names directories carried between nodes as a warm
    build cache (TASKS.md #46, lever 2).
    """

    def test_absent_means_off(self) -> None:
        self.assertEqual(self._load(_FULL).build_cache, ())

    def test_names_are_parsed(self) -> None:
        cfg = self._load(_FULL + 'build_cache: [target]\n')
        self.assertEqual(cfg.build_cache, ('target',))

    def test_a_traversal_is_rejected(self) -> None:
        # These names are joined onto a worktree path AND onto the
        # canonical root, and a pipeline file is shareable content — so
        # anything that could address a directory outside them is
        # refused at load, not at use.
        for bad in ('../../etc', 'a/b', '..', '.', '/abs', 'x\\y'):
            with self.subTest(bad=bad):
                with self.assertRaises(P.PipelineError):
                    self._load(_FULL + f'build_cache: ["{bad}"]\n')

    def test_a_scalar_is_rejected(self) -> None:
        with self.assertRaises(P.PipelineError):
            self._load(_FULL + 'build_cache: target\n')


class TestTemplates(unittest.TestCase):
    def test_available_includes_core_roles(self) -> None:
        names = set(P.available_templates())
        for role in ('coder', 'tdd-writer', 'planner', 'judge'):
            self.assertIn(role, names)

    def test_unknown_template_raises(self) -> None:
        with self.assertRaises(P.PipelineError):
            P.template_prompt('does-not-exist')

    def test_reviewers_are_told_a_green_suite_is_not_enough(self) -> None:
        """
        Over the gcp-custom-roles-1 campaign: 46 reviewer turns, and
        across its SIX code increments not one blocking finding from
        either reviewer. The only two blocks landed on the DOCS
        increment — where there was no meaningful suite to stand
        behind, so the reviewer had to read, and immediately found
        real defects in both candidates (three missing caveats in one,
        a wrong SHA-256 in the other).

        The suite cannot BE the review: the tests were written from
        the same plan the implementation was, so passing them shows
        the code agrees with the plan, not that either is right.

        Hence a REPORTING requirement rather than "be more critical".
        The latter is unfalsifiable, and a lever asking an agent to
        exercise judgement about how much work to do was already tried
        and reverted (TASKS.md #46, lever 5). Agents comply readily
        with "state X in your reply" — SELECT, VERDICT, DISPUTED.
        """
        for role in ('security-reviewer', 'bug-reviewer'):
            with self.subTest(role=role):
                prompt = P.template_prompt(role)
                self.assertIn(
                    'A green suite is necessary, not sufficient', prompt
                )
                # Wrap-agnostic: the clause spans a line break in
                # the template, so match on its two halves.
                self.assertIn('NAME AT LEAST ONE THING YOU', prompt)
                self.assertIn('DOES NOT CHECK', prompt)
                # The escape hatch must stay explicit: a reviewer that
                # found nothing uncovered says so, rather than skipping
                # the requirement or inventing something to report.
                self.assertIn('say that plainly', prompt)

    def test_the_test_author_must_see_the_suite_go_green_once(self) -> None:
        """
        Three builds have now been lost to a suite no implementation
        could pass. The last shipped three such tests: a fixture string
        one test required stored and another forbade, a SQL alias on the
        reserved word `constraint` that cannot parse, and a call to a
        frozen function with the wrong signature.

        The previous lever asked for a per-assertion witness, and it
        missed all three: two were broken test CODE rather than
        unsatisfiable assertions, and the third was a contradiction
        between a PAIR of tests, each satisfiable alone. Per-assertion
        witnesses do not compose into a satisfiable suite. Only running
        the whole suite against something does.

        So the requirement is the green half of red-green, reported:
        build a throwaway stub, see the suite pass once, delete it.
        """
        prompt = P.template_prompt('tdd-writer')
        flat = ' '.join(prompt.split())

        self.assertIn('PROVE THE SUITE CAN GO GREEN', prompt)
        # Every test, not a sample, and really run -- not reasoned
        # about.
        self.assertIn('must have PASSED at least once', flat)
        self.assertIn('Run the WHOLE suite against it', flat)
        # The stub is the instrument; leaving it behind is the thing the
        # tests-only gate exists to catch, so the deletion is explicit.
        self.assertIn('DELETE the stub', prompt)
        # Both non-green outcomes must stay reachable, or the writer
        # resolves every conflict by weakening a test.
        self.assertIn('DISPUTED', prompt)
        self.assertIn('never weaken a test to reach green', flat.lower())

    def test_the_test_author_may_stub_but_never_commit_one(self) -> None:
        # The prohibition protects the JUDGE: a stub on the branch hands
        # both implementers one design. That is enforced against the
        # committed diff, so it must not be written as a ban on stubs
        # existing -- which is what blocked the green check for three
        # builds.
        prompt = P.template_prompt('tdd-writer')
        flat = ' '.join(prompt.split())

        self.assertIn('What you COMMIT is test files ONLY', flat)
        self.assertIn('two independent ones the judge exists to compare', flat)
        self.assertNotIn('not in any scratch, temp', flat)

    def test_reviewers_stay_author_blind(self) -> None:
        # The adversarial framing is deliberately author-BLIND, never
        # author-assuming: "treat this as junior work" would tell a
        # reviewer to weight evidence by an inferred property of the
        # author, which is what these lines exist to forbid.
        for role in ('security-reviewer', 'bug-reviewer', 'judge'):
            with self.subTest(role=role):
                # Whitespace-collapsed: the sentence wraps differently
                # in each template.
                prompt = ' '.join(P.template_prompt(role).split())
                self.assertIn('nothing about them is worth inferring', prompt)


class TestMaterialize(_Base):
    def test_materialize_namespaces_and_parses(self) -> None:
        cfg = self._load(_FULL)
        dest = self.root / 'out'
        mapping = P.materialize_agents(cfg, dest)
        self.assertEqual(mapping['build'], 'pl-mixed-models-build')
        for spec in mapping.values():
            bundle = dest / spec
            self.assertTrue((bundle / 'config.yaml').is_file())
            s = parse(bundle)
            self.assertEqual(s.name, spec)
        # agy planner harness survives materialization.
        plan = parse(dest / mapping['plan'])
        self.assertEqual(plan.executor.harness_kind, 'antigravity-native')

    def test_model_effort_not_baked_into_bundle(self) -> None:
        cfg = self._load(_FULL)
        dest = self.root / 'out'
        mapping = P.materialize_agents(cfg, dest)
        raw = yaml.safe_load(
            (dest / mapping['build'] / 'config.yaml').read_text()
        )
        self.assertNotIn('model', raw.get('executor', {}))
        self.assertNotIn('llm', raw)

    def test_skills_copied(self) -> None:
        (self.root / 'sk').mkdir()
        (self.root / 'sk' / 'S.md').write_text('x', encoding='utf-8')
        cfg = self._load(
            'repo: ./p\nagents:\n  a: {template: coder, skills: ./sk}\n'
        )
        dest = self.root / 'out'
        mapping = P.materialize_agents(cfg, dest)
        self.assertTrue((dest / mapping['a'] / 'skills' / 'S.md').is_file())

    def test_context_baked_into_every_agent_prompt(self) -> None:
        # A pipeline-wide context: is appended to EVERY agent's baked
        # system prompt (once), after its role prompt.
        cfg = self._load(
            _FULL + 'context: |\n  Repo is a Django app; use pytest.\n'
        )
        dest = self.root / 'out'
        mapping = P.materialize_agents(cfg, dest)
        self.assertEqual(len(mapping), 3)  # plan, build, sec
        for spec in mapping.values():
            raw = yaml.safe_load(
                (dest / spec / 'config.yaml').read_text()
            )
            prompt = raw['prompt']
            self.assertIn('Project context', prompt)  # delimiter
            self.assertIn('Repo is a Django app; use pytest.', prompt)
            # appended, not replacing — the role prompt is still there.
            self.assertTrue(prompt.index('Project context') > 0)

    def test_no_context_leaves_prompt_unchanged(self) -> None:
        cfg = self._load(_FULL)  # no context:
        dest = self.root / 'out'
        mapping = P.materialize_agents(cfg, dest)
        raw = yaml.safe_load(
            (dest / mapping['build'] / 'config.yaml').read_text()
        )
        self.assertNotIn('Project context', raw['prompt'])

    def test_namespaced_agent_name(self) -> None:
        self.assertEqual(
            P.namespaced_agent_name('My Pipe', 'Bug Hunter'),
            'pl-my-pipe-bug-hunter',
        )


if __name__ == '__main__':
    unittest.main()
