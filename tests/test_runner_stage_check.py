"""The check stage: reading a contract or a plan before anyone builds.

Run: python -m unittest tests.test_runner_stage_check
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

import click

from sbx_omnigent.pipeline import PipelineAgent, PipelineStage
from sbx_omnigent.runner.briefing import Briefing
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.disputes import Disputes
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.parsing.verdicts import parse_disputes
from sbx_omnigent.runner.progress import RunProgress
from sbx_omnigent.runner.prompts.checks import (
    _CHECK_COMPLETE,
    _CHECK_DISPUTED,
    _CHECK_FINISH_ASK,
    _CHECK_VERDICT,
)
from sbx_omnigent.runner.prompts.review import _REVIEW_MOUNT
from sbx_omnigent.runner.prompts.writer import _UNATTENDED
from sbx_omnigent.runner.records.results import NodeResult
from sbx_omnigent.runner.stages.check import CheckStage

_DONE = f'The suite is sound.\n\n{_CHECK_COMPLETE}'
_DISPUTE = 'DISPUTED: test_ports.py:40 and the brief cannot both hold'

_TESTS = PipelineStage(id='tests', run=('tw',), write=True)
_CHECK = PipelineStage(id='check', run=('chk',), checks='tests',
                       needs=('tests',))
_PLAN = PipelineStage(id='plan', run=('planner',))
_PLAN_CHECK = PipelineStage(id='plan-check', run=('chk',), checks='plan',
                            needs=('plan',))


def _agent(template: str) -> PipelineAgent:
    agent = mock.Mock(spec=PipelineAgent)
    agent.template = template
    return agent


class _CheckTest(unittest.TestCase):
    def setUp(self) -> None:
        echo = mock.patch.object(click, 'echo')
        self.echo = echo.start()
        self.addCleanup(echo.stop)
        self.config = mock.Mock()
        self.config.task = 'Parse port ranges.'
        self.config.acceptance = 'handles 80-90'
        self.config.setup = None
        self.config.base_branch = 'main'
        self.config.agents = {
            'tw': _agent('tdd-writer'), 'chk': _agent('contract-checker'),
            'planner': _agent('planner'),
        }
        self.wt = mock.Mock()
        self.wt.create_node_worktree.return_value = '/w/check'
        self.wt.node_branch.side_effect = lambda run, node: f'pl/{run}/{node}'
        self.wt.node_diff_files.return_value = ['tests/test_ports.py']
        self.nodes: dict[str, NodeResult] = {
            'tests': NodeResult('tests', 'writer', branch='pl/r1/tests'),
        }
        self.driver = mock.Mock()
        self.driver.create.return_value = 'sess-chk'
        #: The check's successive replies.
        self.replies: list[str] = []
        self.driver.drive.side_effect = lambda *a, **k: self.replies.pop(0)
        self.book = mock.Mock()
        self.book.next_round.return_value = 1
        self.capture = mock.Mock()
        self.released: list[tuple[str, str]] = []

    def checks(self, *, resume: bool = False) -> CheckStage:
        ctx = RunContext(
            config=self.config, worktrees=self.wt, sessions=mock.Mock(),
            run_id='r1', nodes=self.nodes,
            stage_by_id={s.id: s for s in (_TESTS, _CHECK, _PLAN,
                                           _PLAN_CHECK)},
            lock=threading.RLock(), max_rounds=3,
        )
        return CheckStage(
            ctx,
            book=self.book,
            driver=self.driver,
            capture=self.capture,
            briefing=Briefing(ctx, progress=RunProgress()),
            disputes=Disputes(ctx, capture=self.capture),
            resume=resume,
            release_session=lambda stage, kind: (
                self.released.append((stage.id, kind))
            ),
        )

    def recorded_verdict(self) -> str:
        return self.book.record_review.call_args.args[3]


class TestAContractCheck(_CheckTest):
    def test_a_pass_lets_the_writers_go_and_is_recorded(self) -> None:
        self.replies = [_DONE]
        self.checks().run(_CHECK)
        self.assertEqual(self.nodes['check'].kind, 'check')
        self.assertEqual(self.recorded_verdict(), _CHECK_VERDICT)

    def test_it_reads_the_tests_branch_read_only(self) -> None:
        self.replies = [_DONE]
        self.checks().run(_CHECK)
        self.wt.create_node_worktree.assert_called_once_with(
            'r1', 'check', from_node='tests', replace=False,
        )
        self.driver.create.assert_called_once_with(
            'chk', '/w/check', 'ro', 'check',
        )

    def test_a_resumed_check_cuts_a_fresh_clone(self) -> None:
        self.replies = [_DONE]
        self.checks(resume=True).run(_CHECK)
        self.assertTrue(
            self.wt.create_node_worktree.call_args.kwargs['replace'],
        )

    def test_a_dispute_stops_the_run_before_any_writer(self) -> None:
        self.replies = [_DISPUTE]
        with self.assertRaises(PipelineRunError) as caught:
            self.checks().run(_CHECK)
        self.assertIn('the contract check raised', str(caught.exception))
        self.book.record_review.assert_not_called()

    def test_a_dispute_is_never_asked_again(self) -> None:
        self.replies = [_DISPUTE]
        with self.assertRaises(PipelineRunError):
            self.checks().run(_CHECK)
        self.assertEqual(self.driver.drive.call_count, 1)

    def test_a_reply_that_did_not_finish_is_asked_once(self) -> None:
        self.replies = ['Still reading the suite.', _DONE]
        self.checks().run(_CHECK)
        ask = self.driver.drive.call_args_list[1].args[1]
        self.assertEqual(ask, _CHECK_FINISH_ASK)
        self.assertEqual(self.nodes['check'].output, _DONE)

    def test_one_that_never_finishes_stops_the_run(self) -> None:
        self.replies = ['Still reading.', 'Still reading.']
        with self.assertRaises(PipelineRunError) as caught:
            self.checks().run(_CHECK)
        self.assertIn('did not finish', str(caught.exception))
        self.capture.turn.assert_called_once_with(
            'sess-chk', 'did not finish its check',
        )


class TestAPlanCheck(_CheckTest):
    def test_a_clean_plan_has_no_disputes(self) -> None:
        self.replies = [_DONE]
        claims = self.checks().run_plan_check(_PLAN_CHECK, '# The plan')
        self.assertEqual(claims, ())
        self.assertEqual(self.recorded_verdict(), _CHECK_VERDICT)

    def test_its_disputes_come_back_and_are_recorded(self) -> None:
        self.replies = [_DISPUTE]
        claims = self.checks().run_plan_check(_PLAN_CHECK, '# The plan')
        self.assertEqual(len(claims), 1)
        self.assertEqual(self.recorded_verdict(), _CHECK_DISPUTED)

    def test_one_that_never_finishes_counts_as_a_dispute(self) -> None:
        self.replies = ['Reading.', 'Reading.']
        (claim,) = self.checks().run_plan_check(_PLAN_CHECK, '# The plan')
        self.assertIn('plan-check did not finish', claim)
        self.assertIn('Its last reply', self.nodes['plan-check'].output)

    def test_it_reads_the_tree_the_plan_was_written_against(self) -> None:
        # Not its own seed: in a campaign that fell back to the base.
        self.nodes['plan'] = NodeResult('plan', 'reader', output='# P')
        self.replies = [_DONE]
        plan = PipelineStage(id='plan', run=('planner',),
                             from_branch='campaign')
        checks = self.checks()
        checks.ctx.stage_by_id['plan'] = plan
        checks.run_plan_check(_PLAN_CHECK, '# The plan')
        self.wt.create_node_worktree.assert_called_once_with(
            'r1', 'plan-check', from_node='campaign', replace=True,
        )

    def test_its_session_is_released_when_it_is_done(self) -> None:
        self.replies = [_DONE]
        self.checks().run_plan_check(_PLAN_CHECK, '# The plan')
        self.assertEqual(self.released, [('plan-check', 'check')])

    def test_the_checks_naming_a_plan_are_found(self) -> None:
        self.assertEqual(self.checks().plan_checks(_PLAN), [_PLAN_CHECK])
        self.assertEqual(self.checks().plan_checks(_TESTS), [_CHECK])


class TestAPlanCheckOnAResumedPlan(_CheckTest):
    """Reached only when the plan finished in an earlier attempt."""

    def test_it_checks_the_recorded_plan(self) -> None:
        self.nodes['plan'] = NodeResult('plan', 'reader', output='# Old')
        self.replies = [_DONE]
        self.checks().run(_PLAN_CHECK)
        turn = self.driver.drive.call_args_list[0].args[1]
        self.assertIn('# Old', turn)

    def test_a_dispute_stops_the_run(self) -> None:
        self.nodes['plan'] = NodeResult('plan', 'reader', output='# Old')
        self.replies = [_DISPUTE]
        with self.assertRaises(PipelineRunError) as caught:
            self.checks().run(_PLAN_CHECK)
        self.assertIn('[plan-check]', str(caught.exception))


class TestTheTurns(_CheckTest):
    def test_a_contract_check_sees_the_brief_and_the_files(self) -> None:
        turn = self.checks().instruction(_CHECK)
        self.assertIn('CHECK THE CONTRACT', turn)
        self.assertIn('Brief:\nParse port ranges.', turn)
        self.assertIn('Acceptance contract:\nhandles 80-90', turn)
        self.assertIn('  - tests/test_ports.py', turn)
        self.assertIn(_CHECK_COMPLETE, turn)

    def test_files_that_cannot_be_listed_say_so(self) -> None:
        self.wt.node_diff_files.side_effect = click.ClickException('git')
        turn = self.checks().instruction(_CHECK)
        self.assertIn('none could be listed', turn)

    def test_a_plan_check_carries_the_plan(self) -> None:
        turn = self.checks().plan_instruction(_PLAN_CHECK, '# The plan')
        self.assertIn('CHECK THE PLAN', turn)
        self.assertIn("The plan ('plan'):\n# The plan", turn)



class TestThePlanCheckTurn(_CheckTest):
    """What a plan checker is asked to do (#49)."""

    _PLAN = 'THE PLAN: port ranges use the Range type in ports.py.'

    def message(self) -> str:
        self.config.task = 'build parse_ports'
        self.config.acceptance = 'ranges are inclusive'
        self.config.setup = 'install the toolchain'
        return self.checks().plan_instruction(_PLAN_CHECK, self._PLAN)

    def test_it_carries_the_plan_and_the_brief(self) -> None:
        message = self.message()
        for part in (
            self._PLAN,                # not in the tree yet
            'build parse_ports',       # the brief
            'ranges are inclusive',    # the acceptance contract
            'install the toolchain',   # setup, to run its checks
        ):
            with self.subTest(part=part):
                self.assertIn(part, message)

    def test_it_says_the_mount_is_read_only(self) -> None:
        self.assertIn(_REVIEW_MOUNT, self.message())

    def test_it_carries_the_unattended_rules(self) -> None:
        self.assertIn(_UNATTENDED, self.message())

    def test_it_is_not_a_build_order(self) -> None:
        message = self.message()
        self.assertNotIn('Task:', message)
        self.assertIn('Do NOT implement', message)

    def test_every_claim_needs_a_command_and_its_output(self) -> None:
        message = self.message()
        self.assertIn('the command you ran', message)
        self.assertIn('its output', message)

    def test_it_asks_for_contradictions_between_sections(self) -> None:
        self.assertIn('contradict each other', self.message())

    def test_it_is_told_the_design_is_not_its_call(self) -> None:
        self.assertIn('not your call', self.message())

    def test_its_own_text_would_not_halt_the_run(self) -> None:
        self.assertEqual(parse_disputes(self.message()), ())

    def test_it_is_told_to_finish(self) -> None:
        message = self.message()
        self.assertIn(_CHECK_COMPLETE, message)
        self.assertIn('still running', message)

if __name__ == '__main__':
    unittest.main()
