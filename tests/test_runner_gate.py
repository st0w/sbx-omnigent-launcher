"""The verification gate: run the project's own check before publishing.

Run: python -m unittest tests.test_runner_gate
"""

from __future__ import annotations

import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import click

from sbx_omnigent import pipeline, verify
from sbx_omnigent.defaults import DEFAULT_EGRESS_ALLOW, DEFAULT_HOST_IMAGE
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.gate import VerifyGate

_PIPELINE = """\
name: demo
repo: ./proj
publish: local
verify:
  coverage_min: 95
  command: cov --fail-under {coverage_min}
task: |
  add parse_ports
agents:
  plan: {template: planner, model: claude-sonnet-5}
  build: {template: coder, model: claude-sonnet-5}
  sec: {template: security-reviewer, model: claude-sonnet-5}
stages:
  - {id: plan, run: plan}
  - {id: build, run: build, write: true, needs: [plan]}
  - {id: review, run: [sec], needs: [build], gate: consensus, on_block: build}
"""


def _failed(output: str = 'FAILED test_ports', code: int = 1):
    step = verify.StepOutcome(
        'tests', 'cov', code, output, full_output=f'{output}\nfull log',
    )
    return verify.VerifyOutcome(
        ok=False, exit_code=code, output=output, steps=(step,),
    )


_PASSED = verify.VerifyOutcome(ok=True, exit_code=0, output='ok')


class _FakeWorktrees:
    """The two worktree-manager calls the gate makes."""

    def __init__(self) -> None:
        self.clones: list[tuple[str, str, str | None, bool]] = []
        self.artifacts: dict[str, str] = {}
        self.write_ok = True

    def create_node_worktree(
        self, run_id: str, node_id: str, *, from_node: str | None = None,
        replace: bool = False,
    ) -> str:
        self.clones.append((run_id, node_id, from_node, replace))
        return f'/w/{node_id}'

    def write_run_artifact(self, run_id: str, relpath: str, text: str) -> bool:
        if self.write_ok:
            self.artifacts[relpath] = text
        return self.write_ok


class _GateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.wt = _FakeWorktrees()
        self.redriven: list[tuple[str, str, str | None]] = []
        self.rerun: list[str] = []
        echo = mock.patch.object(click, 'echo')
        echo.start()
        self.addCleanup(echo.stop)

    def config(self, text: str = _PIPELINE) -> pipeline.PipelineConfig:
        path = self.root / 'pipeline.yaml'
        path.write_text(text, encoding='utf-8')
        return pipeline.load_pipeline(path)

    def gate(
        self, cfg: pipeline.PipelineConfig | None = None, *,
        max_rounds: int = 3, egress: tuple[str, ...] | None = None,
    ) -> VerifyGate:
        cfg = cfg or self.config()
        ctx = RunContext(
            config=cfg,
            worktrees=self.wt,
            sessions=mock.Mock(),
            run_id='r1',
            nodes={},
            stage_by_id={s.id: s for s in pipeline._iter_stages(cfg.stages)},
            lock=threading.RLock(),
            max_rounds=max_rounds,
        )

        def redrive(stage_id, findings, *, message=None, **_kw):
            self.redriven.append((stage_id, findings, message))

        return VerifyGate(
            ctx,
            egress=egress,
            redrive_writer=redrive,
            rerun_stage=lambda stage: self.rerun.append(stage.id),
        )

    def run_gate(self, outcomes, winner='build', **kw):
        gate = self.gate(**kw)
        with mock.patch.object(
            verify, 'run_verification', side_effect=outcomes,
        ) as ran:
            result = gate.check(winner, gate.ctx.config.stages)
        return result, ran


class TestAPassingGate(_GateTest):
    def test_no_gate_configured_runs_nothing(self) -> None:
        cfg = self.config(_PIPELINE.replace(
            'verify:\n  coverage_min: 95\n'
            '  command: cov --fail-under {coverage_min}\n', '',
        ))
        gate = self.gate(cfg)
        with mock.patch.object(verify, 'run_verification') as ran:
            self.assertIsNone(gate.check('build', cfg.stages))
        ran.assert_not_called()

    def test_a_pass_returns_its_evidence_and_redrives_nothing(self) -> None:
        result, _ran = self.run_gate([_PASSED])
        self.assertIs(result, _PASSED)
        self.assertEqual((self.redriven, self.rerun), ([], []))

    def test_it_runs_on_a_fresh_clone_of_the_branch(self) -> None:
        _result, ran = self.run_gate([_PASSED])
        self.assertEqual(
            self.wt.clones, [('r1', 'build-verify', 'build', True)],
        )
        kwargs = ran.call_args.kwargs
        self.assertEqual(kwargs['workspace'], '/w/build-verify')
        self.assertEqual(
            kwargs['name'], verify.sandbox_name('r1', 'build-verify'),
        )

    def test_with_no_image_it_runs_on_the_host_image(self) -> None:
        _result, ran = self.run_gate([_PASSED])
        self.assertEqual(ran.call_args.kwargs['image'], DEFAULT_HOST_IMAGE)

    def test_without_an_operator_list_it_gets_the_default_egress(self) -> None:
        _result, ran = self.run_gate([_PASSED])
        self.assertEqual(ran.call_args.kwargs['egress'], DEFAULT_EGRESS_ALLOW)

    def test_an_empty_operator_list_is_kept_empty(self) -> None:
        # The gate got the default even under `egress_allow: []` (#41).
        _result, ran = self.run_gate([_PASSED], egress=())
        self.assertEqual(ran.call_args.kwargs['egress'], ())


class TestAFailingGate(_GateTest):
    def test_the_writer_that_made_the_branch_closes_the_gap(self) -> None:
        result, _ran = self.run_gate([_failed(), _PASSED])
        self.assertIs(result, _PASSED)
        self.assertEqual(len(self.redriven), 1)
        stage, findings, message = self.redriven[0]
        self.assertEqual(stage, 'build')
        self.assertEqual(message, 'build: close the verification gap')
        self.assertIn('VERIFICATION GATE failed', findings)
        self.assertIn('FAILED test_ports', findings)

    def test_what_checked_the_branch_checks_it_again(self) -> None:
        self.run_gate([_failed(), _PASSED])
        self.assertEqual(self.rerun, ['review'])

    def test_a_branch_no_writer_made_cannot_be_fixed(self) -> None:
        with self.assertRaises(PipelineRunError) as caught:
            self.run_gate([_failed()], winner='plan')
        self.assertIn('no writer stage produced', str(caught.exception))
        self.assertEqual(self.redriven, [])

    def test_it_gives_up_after_the_round_cap(self) -> None:
        with self.assertRaises(PipelineRunError) as caught:
            self.run_gate([_failed()] * 2, max_rounds=2)
        self.assertIn('after 2 attempt(s)', str(caught.exception))
        self.assertEqual(len(self.redriven), 1)

    def test_a_gate_that_cannot_run_redrives_nobody(self) -> None:
        # Infrastructure, not a verdict on the branch.
        with self.assertRaises(PipelineRunError) as caught:
            self.run_gate(verify.VerifyError('sbx refused'))
        self.assertIn(
            'could not run the verification gate', str(caught.exception),
        )
        self.assertEqual(self.redriven, [])


class TestTheFixerIsShownTheGap(_GateTest):
    def findings(self) -> str:
        self.run_gate([_failed(), _PASSED])
        return self.redriven[0][1]

    def test_the_whole_capture_is_kept_and_named(self) -> None:
        findings = self.findings()
        self.assertIn('full log', self.wt.artifacts['turns/build.verify.txt'])
        self.assertIn('<run-dir>/turns/build.verify.txt', findings)

    def test_a_capture_that_could_not_be_kept_is_not_named(self) -> None:
        self.wt.write_ok = False
        self.assertNotIn('<run-dir>', self.findings())

    def test_the_setup_note_is_relayed(self) -> None:
        cfg = self.config(_PIPELINE.replace(
            'task: |', 'setup: |\n  apt-get install cargo\ntask: |',
        ).replace('verify:\n', 'verify:\n  setup: ""\n'))
        gate = self.gate(cfg)
        with mock.patch.object(
            verify, 'run_verification', side_effect=[_failed(), _PASSED],
        ):
            gate.check('build', cfg.stages)
        self.assertIn('apt-get install cargo', self.redriven[0][1])


class TestWhatIsRechecked(_GateTest):
    """Every review, scan and triage stage downstream of the fixer runs
    again; writers, judges and readers do not."""

    def stages(self) -> tuple[pipeline.PipelineStage, ...]:
        stage = pipeline.PipelineStage
        return (
            stage('plan', run=('plan',)),
            stage('build', run=('build',), write=True, needs=('plan',)),
            stage('review', run=('sec',), needs=('build',), gate='consensus',
                  on_block='build'),
            stage('docs', run=('plan',), needs=('build',)),
            stage('scan', run=('sec',), needs=('review',), scans='build',
                  on_block='build'),
            stage('triage', run=('sec',), needs=('scan',), verifies='scan'),
            stage('other', run=('sec',), needs=('plan',), gate='consensus',
                  on_block='plan'),
        )

    def test_checks_downstream_of_the_fixer_rerun_in_order(self) -> None:
        rechecked = self.gate().rechecks_after('build', self.stages())
        self.assertEqual(
            [s.id for s in rechecked], ['review', 'scan', 'triage'],
        )

    def test_a_branch_inherited_by_from_counts_as_downstream(self) -> None:
        stage = pipeline.PipelineStage
        stages = (
            stage('build', run=('build',), write=True),
            stage('refactor', run=('build',), write=True,
                  from_branch='build'),
            stage('review', run=('sec',), needs=('refactor',),
                  gate='consensus', on_block='refactor'),
        )
        rechecked = self.gate().rechecks_after('build', stages)
        self.assertEqual([s.id for s in rechecked], ['review'])


if __name__ == '__main__':
    unittest.main()
