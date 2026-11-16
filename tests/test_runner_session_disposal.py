"""Session disposal: handing each microVM back, and never losing track.

Run: python -m unittest tests.test_runner_session_disposal
"""

from __future__ import annotations

import threading
import unittest
from unittest import mock

import click

from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.session_disposal import SessionDisposal
from sbx_omnigent.runner.session_registry import SessionRegistry
from sbx_omnigent.swarm_session import SwarmSessionError


class _DisposalTest(unittest.TestCase):
    def setUp(self) -> None:
        echo = mock.patch.object(click, 'echo')
        self.echo = echo.start()
        self.addCleanup(echo.stop)
        self.sessions = mock.Mock()
        #: Sessions whose delete fails.
        self.refuse: set[str] = set()
        self.disposed: list[str] = []
        self.sessions.dispose.side_effect = self._dispose
        self.wt = mock.Mock()
        self.wt.run_dir.return_value = '/runs/r1'
        self.registry = SessionRegistry()
        self.saves = 0

    def _dispose(self, session: str) -> None:
        if session in self.refuse:
            raise SwarmSessionError(f'{session}: refused')
        self.disposed.append(session)

    def disposal(self, *, keep: bool = False) -> SessionDisposal:
        ctx = RunContext(
            config=mock.Mock(), worktrees=self.wt, sessions=self.sessions,
            run_id='r1', nodes={}, stage_by_id={}, lock=threading.RLock(),
            max_rounds=3,
        )

        def save() -> None:
            self.saves += 1

        return SessionDisposal(ctx, registry=self.registry, keep=keep,
                               save_state=save)

    def created(self, *sessions: str) -> None:
        for session in sessions:
            self.registry.register(session, label=session,
                                   agent=mock.Mock(), worktree='/w')
            self.registry.agy.add(session)

    def said(self) -> str:
        return '\n'.join(str(c.args[0]) for c in self.echo.call_args_list)


class TestFreeingOne(_DisposalTest):
    def test_it_is_disposed_and_no_longer_live(self) -> None:
        self.created('s1')
        self.assertTrue(self.disposal().free('s1', 'plan: done'))
        self.assertEqual(self.disposed, ['s1'])
        self.assertIn('s1', self.registry.released)
        self.assertNotIn('s1', self.registry.agy)
        self.assertIn('[cleanup] plan: done', self.said())

    def test_freeing_it_twice_deletes_it_once(self) -> None:
        self.created('s1')
        disposal = self.disposal()
        disposal.free('s1', 'first')
        self.assertFalse(disposal.free('s1', 'second'))
        self.assertEqual(self.disposed, ['s1'])

    def test_a_refused_delete_stays_tracked(self) -> None:
        # A microVM that may still be running must never stop being
        # tracked, so teardown and --resume try again.
        self.created('s1')
        self.refuse.add('s1')
        self.assertFalse(self.disposal().free('s1', 'done'))
        self.assertNotIn('s1', self.registry.released)
        self.assertEqual(list(self.registry.outstanding()), ['s1'])

    def test_keep_frees_nothing(self) -> None:
        self.created('s1')
        self.assertFalse(self.disposal(keep=True).free('s1', 'done'))
        self.assertEqual(self.disposed, [])


class TestDisposingNamedSessions(_DisposalTest):
    def test_only_the_named_sessions_go(self) -> None:
        # A stage frees what it created and leaves everything else.
        self.created('a-1', 'b-1', 'a-2', 'b-2')
        self.disposal().dispose(['a-1', 'a-2'])
        self.assertEqual(self.disposed, ['a-1', 'a-2'])
        self.assertEqual(self.registry.sessions, ['b-1', 'b-2'])

    def test_an_outstanding_module_mark_keeps_its_meaning(self) -> None:
        sessions = self.registry.sessions
        self.created('chunk0-1', 'chunk0-2')
        mark = len(sessions)
        self.created('stage-a', 'stage-b', 'other')
        self.disposal().dispose(['stage-a', 'stage-b'])
        self.assertEqual(sessions[mark:], ['other'])
        self.assertEqual(sessions[:mark], ['chunk0-1', 'chunk0-2'])

    def test_one_already_freed_is_not_deleted_again(self) -> None:
        self.created('s-1', 's-2')
        disposal = self.disposal()
        disposal.free('s-1', 'voted')
        disposal.dispose(['s-1', 's-2'])
        self.assertEqual(self.disposed, ['s-1', 's-2'])
        self.assertEqual(self.registry.sessions, [])
        self.assertEqual(self.registry.released, set())

    def test_a_refused_delete_is_kept_for_another_try(self) -> None:
        self.created('s-1', 's-2')
        self.refuse.add('s-1')
        self.disposal().dispose(['s-1', 's-2'])
        self.assertEqual(self.registry.undisposed, ['s-1'])
        self.assertEqual(list(self.registry.outstanding()), ['s-1'])
        self.assertIn('refused to dispose', self.said())

    def test_the_state_is_saved_after(self) -> None:
        self.created('s-1')
        self.disposal().dispose(['s-1'])
        self.assertEqual(self.saves, 1)

    def test_nothing_named_does_nothing(self) -> None:
        self.disposal().dispose([])
        self.assertEqual((self.disposed, self.saves), ([], 0))

    def test_keep_disposes_nothing(self) -> None:
        self.created('s-1')
        self.disposal(keep=True).dispose(['s-1'])
        self.assertEqual(self.disposed, [])


class TestAPreviousAttemptsSessions(_DisposalTest):
    def test_they_are_disposed_and_forgotten(self) -> None:
        self.registry.stale = ['old-1', 'old-2']
        self.disposal().dispose_stale()
        self.assertEqual(self.disposed, ['old-1', 'old-2'])
        self.assertEqual(self.registry.stale, [])
        self.assertIn('disposed 2/2 microVM(s)', self.said())

    def test_one_already_gone_is_not_fatal(self) -> None:
        self.registry.stale = ['old-1', 'old-2']
        self.refuse.add('old-1')
        self.disposal().dispose_stale()
        self.assertIn('disposed 1/2 microVM(s)', self.said())

    def test_keep_leaves_them_up_and_says_so(self) -> None:
        self.registry.stale = ['old-1']
        self.disposal(keep=True).dispose_stale()
        self.assertEqual(self.disposed, [])
        self.assertIn('no longer tracked', self.said())

    def test_none_says_nothing(self) -> None:
        self.disposal().dispose_stale()
        self.assertEqual(self.said(), '')


class TestTeardown(_DisposalTest):
    def test_every_outstanding_session_goes(self) -> None:
        self.created('s-1', 's-2')
        self.registry.undisposed.append('stuck')
        self.disposal().teardown()
        self.assertEqual(self.disposed, ['s-1', 's-2', 'stuck'])

    def test_a_freed_session_is_not_deleted_twice(self) -> None:
        self.created('s-1', 's-2')
        disposal = self.disposal()
        disposal.free('s-1', 'done')
        disposal.teardown()
        self.assertEqual(self.disposed, ['s-1', 's-2'])

    def test_a_finished_run_removes_its_directory(self) -> None:
        self.disposal().teardown()
        self.wt.dispose_run.assert_called_once_with('r1')

    def test_a_failed_run_keeps_it_for_resume(self) -> None:
        self.disposal().teardown(preserve_run=True)
        self.wt.dispose_run.assert_not_called()
        self.assertIn('--run-id r1 --resume', self.said())

    def test_a_refused_delete_does_not_stop_teardown(self) -> None:
        self.created('s-1', 's-2')
        self.refuse.add('s-1')
        self.disposal().teardown()
        self.assertEqual(self.disposed, ['s-2'])
        self.wt.dispose_run.assert_called_once()

    def test_a_directory_that_will_not_go_is_not_fatal(self) -> None:
        self.wt.dispose_run.side_effect = click.ClickException('busy')
        self.disposal().teardown()

    def test_keep_keeps_everything(self) -> None:
        self.created('s-1')
        self.disposal(keep=True).teardown()
        self.assertEqual(self.disposed, [])
        self.wt.dispose_run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
