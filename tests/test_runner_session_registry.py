"""The session registry: what the run knows about each agent session.

Run: python -m unittest tests.test_runner_session_registry
"""

from __future__ import annotations

import unittest
from unittest import mock

from sbx_omnigent.runner.session_registry import SessionRegistry


class TestRegistering(unittest.TestCase):
    def test_a_new_registry_knows_no_session(self) -> None:
        registry = SessionRegistry()
        self.assertEqual(registry.sessions, [])
        self.assertEqual(list(registry.outstanding()), [])

    def test_two_registries_share_nothing(self) -> None:
        first, second = SessionRegistry(), SessionRegistry()
        first.register('s1', label='build', agent=mock.sentinel.agent,
                       worktree='/w/build')
        self.assertEqual(
            (second.sessions, second.labels, second.agents,
             second.worktrees),
            ([], {}, {}, {}),
        )

    def test_a_session_is_kept_with_what_it_is_for(self) -> None:
        registry = SessionRegistry()
        registry.register('s1', label='build', agent=mock.sentinel.agent,
                          worktree='/w/build')
        self.assertEqual(registry.sessions, ['s1'])
        self.assertEqual(registry.label_of('s1'), 'build')
        self.assertIs(registry.agents['s1'], mock.sentinel.agent)
        self.assertEqual(registry.worktrees['s1'], '/w/build')

    def test_an_unknown_session_is_named_by_its_id(self) -> None:
        self.assertEqual(SessionRegistry().label_of('s9'), 's9')


class TestOutstanding(unittest.TestCase):
    """What the state records and teardown disposes: every session not
    yet freed, failed deletes included."""

    def test_released_sessions_are_not_outstanding(self) -> None:
        registry = SessionRegistry(sessions=['s1', 's2'], released={'s1'})
        self.assertEqual(list(registry.outstanding()), ['s2'])

    def test_a_failed_delete_stays_outstanding(self) -> None:
        registry = SessionRegistry(sessions=['s1'], undisposed=['s0'])
        self.assertEqual(list(registry.outstanding()), ['s1', 's0'])

    def test_a_session_is_listed_once(self) -> None:
        registry = SessionRegistry(sessions=['s1'], undisposed=['s1'])
        self.assertEqual(list(registry.outstanding()), ['s1'])


class TestForgetting(unittest.TestCase):
    def test_a_freed_session_loses_its_agy_and_worktree_entries(self) -> None:
        registry = SessionRegistry()
        registry.register('s1', label='build', agent=mock.sentinel.agent,
                          worktree='/w/build')
        registry.agy.add('s1')
        registry.forget('s1')
        self.assertEqual((registry.agy, registry.worktrees), (set(), {}))

    def test_its_label_is_kept_for_the_records(self) -> None:
        registry = SessionRegistry()
        registry.register('s1', label='build', agent=mock.sentinel.agent,
                          worktree='/w/build')
        registry.forget('s1')
        self.assertEqual(registry.label_of('s1'), 'build')

    def test_forgetting_an_unknown_session_is_harmless(self) -> None:
        SessionRegistry().forget('s9')


class TestTheFirstTurn(unittest.TestCase):
    def test_only_a_sessions_first_turn_is_first(self) -> None:
        registry = SessionRegistry()
        self.assertTrue(registry.first_turn('s1'))
        self.assertFalse(registry.first_turn('s1'))
        self.assertTrue(registry.first_turn('s2'))


if __name__ == '__main__':
    unittest.main()
