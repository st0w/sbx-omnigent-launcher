"""Session transcripts kept as committable records.

Run: python -m unittest tests.test_runner_render_sessions
"""

from __future__ import annotations

import unittest

from sbx_omnigent.runner.render.sessions import render_planning_session

_SESSION_TURNS = [
    ('assistant', '# Draft\n```mermaid\ngraph TD\n  A-->B\n```'),
    ('user', 'Use ULIDs, not UUIDs.'),
    ('assistant', '# Revised\n```sql\nCREATE TABLE t (id TEXT);\n```'),
]


class TestRenderPlanningSession(unittest.TestCase):
    """The design record: the conversation, not just its summary."""

    def _doc(self, turns=None):
        return render_planning_session(
            _SESSION_TURNS if turns is None else turns,
            title='Planning session — demo [m1]',
            plan_doc='docs/plans/demo-m1.md',
        )

    def test_no_turns_renders_nothing(self) -> None:
        self.assertEqual(self._doc([]), '')

    def test_artifacts_survive_verbatim(self) -> None:
        # THE point: the consolidated plan drops what it makes
        # redundant. Observed live — a draft carried a mermaid diagram
        # and nine SQL blocks; the revision that replaced it carried
        # four and no diagram, and that is what would have shipped.
        doc = self._doc()
        self.assertIn('```mermaid', doc)
        self.assertIn('graph TD', doc)
        self.assertIn('CREATE TABLE t (id TEXT);', doc)

    def test_the_humans_side_is_kept_too(self) -> None:
        # The answers are half the reasoning and exist nowhere else.
        self.assertIn('Use ULIDs, not UUIDs.', self._doc())

    def test_turns_are_numbered_and_attributed(self) -> None:
        doc = self._doc()
        self.assertIn('## 1. Planner', doc)
        self.assertIn('## 2. Human', doc)
        self.assertIn('## 3. Planner', doc)

    def test_it_points_at_the_plan_of_record(self) -> None:
        self.assertIn('demo-m1.md', self._doc())

    def test_an_unknown_role_is_not_dropped(self) -> None:
        self.assertIn('note', self._doc([('note', 'from the system')]))


if __name__ == '__main__':
    unittest.main()
