"""How far a run has got: its modules, the active one, what published.

Run: python -m unittest tests.test_runner_progress
"""

from __future__ import annotations

import unittest

from sbx_omnigent.pipeline import Subtask
from sbx_omnigent.runner.progress import RunProgress

_MODULES = [
    Subtask(id='m0', title='contracts and core'),
    Subtask(id='m1', title='storage and schema'),
    Subtask(id='m2', title='collectors'),
]


class TestANewRun(unittest.TestCase):
    def test_it_starts_outside_any_module(self) -> None:
        progress = RunProgress()
        self.assertIsNone(progress.active_subtask)
        self.assertIsNone(progress.module_id)
        self.assertFalse(progress.active_is_first)
        self.assertEqual(
            (progress.subtasks, progress.completed_chunks),
            ([], set()),
        )
        self.assertIsNone(progress.plan_of_record)
        self.assertIsNone(progress.last_branch_node)

    def test_two_runs_do_not_share_their_collections(self) -> None:
        first, second = RunProgress(), RunProgress()
        first.subtasks.append(_MODULES[0])
        first.completed_chunks.add('m0')
        self.assertEqual(
            (second.subtasks, second.completed_chunks), ([], set()),
        )


class TestTheActiveModule(unittest.TestCase):
    def test_starting_a_module_makes_it_active(self) -> None:
        progress = RunProgress(subtasks=list(_MODULES))
        progress.start_module(_MODULES[1], first=False)
        self.assertIs(progress.active_subtask, _MODULES[1])
        self.assertEqual(progress.module_id, 'm1')
        self.assertFalse(progress.active_is_first)

    def test_the_first_module_says_so(self) -> None:
        # Nothing has been built yet, so a turn must not be told to
        # build on prior work.
        progress = RunProgress(subtasks=list(_MODULES))
        progress.start_module(_MODULES[0], first=True)
        self.assertTrue(progress.active_is_first)

    def test_finishing_a_module_leaves_no_module_active(self) -> None:
        progress = RunProgress(subtasks=list(_MODULES))
        progress.start_module(_MODULES[0], first=True)
        progress.end_module()
        self.assertIsNone(progress.module_id)
        self.assertFalse(progress.active_is_first)


class TestThePriorModule(unittest.TestCase):
    """Derived from what has published rather than remembered, so a
    resumed campaign finds it too."""

    def progress(self, active: str, published: set[str]) -> RunProgress:
        progress = RunProgress(
            subtasks=list(_MODULES), completed_chunks=set(published),
        )
        sub = next(s for s in _MODULES if s.id == active)
        progress.start_module(sub, first=active == 'm0')
        return progress

    def test_the_first_module_has_none(self) -> None:
        self.assertIsNone(self.progress('m0', set()).prior_module())

    def test_it_is_the_last_one_published_before_the_active(self) -> None:
        self.assertEqual(
            self.progress('m2', {'m0', 'm1'}).prior_module(), 'm1',
        )

    def test_with_nothing_published_yet_there_is_none(self) -> None:
        self.assertIsNone(self.progress('m1', set()).prior_module())

    def test_a_module_that_never_published_is_skipped(self) -> None:
        self.assertEqual(self.progress('m2', {'m0'}).prior_module(), 'm0')

    def test_a_later_published_module_does_not_count(self) -> None:
        self.assertIsNone(self.progress('m0', {'m1', 'm2'}).prior_module())

    def test_the_module_list_gives_the_order_not_the_ids(self) -> None:
        # A set of published ids alone would sort m10 before m2.
        modules = [Subtask(id='m2', title='a'), Subtask(id='m10', title='b'),
                   Subtask(id='m11', title='c')]
        progress = RunProgress(subtasks=modules,
                               completed_chunks={'m2', 'm10'})
        progress.start_module(modules[2], first=False)
        self.assertEqual(progress.prior_module(), 'm10')

    def test_outside_a_module_there_is_none(self) -> None:
        progress = RunProgress(subtasks=list(_MODULES),
                               completed_chunks={'m0'})
        self.assertIsNone(progress.prior_module())


if __name__ == '__main__':
    unittest.main()
