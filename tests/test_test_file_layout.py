"""Every test file ends with its ``__main__`` guard.

Running one file directly, e.g. ``python tests/test_swarm.py``, stops
at ``unittest.main()``, which exits the interpreter. A test class
defined below the guard is never created, so its tests are skipped with
no message and the run still reports ``OK`` — with fewer tests. Four
files had one, and 23 tests went missing from direct runs that way.

``python -m unittest discover`` imports each module without running the
guard, so the full-suite count never showed it. This fails the suite
instead of relying on someone noticing a smaller number.

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import ast
import unittest
from pathlib import Path

#: The directory this file lives in: every sibling ``test_*.py`` is
#: checked, including this one.
_TESTS_DIR = Path(__file__).resolve().parent


def _is_main_guard(node: ast.stmt) -> bool:
    """
    Whether *node* is an ``if __name__ == '__main__':`` block.

    :param node: A top-level statement.
    :returns: ``True`` for the guard, in either operand order.
    """
    if not isinstance(node, ast.If):
        return False
    test = node.test
    if not isinstance(test, ast.Compare) or len(test.comparators) != 1:
        return False
    if not isinstance(test.ops[0], ast.Eq):
        return False
    sides = (test.left, test.comparators[0])
    names = [s for s in sides if isinstance(s, ast.Name)]
    consts = [s for s in sides if isinstance(s, ast.Constant)]
    return (
        len(names) == 1
        and names[0].id == '__name__'
        and len(consts) == 1
        and consts[0].value == '__main__'
    )


def statements_after_main_guard(source: str) -> list[str]:
    """
    The top-level statements that follow a ``__main__`` guard.

    :param source: A module's source text.
    :returns: A short description of each statement after the first
        guard, in order; empty when the guard is last or absent.
    :raises SyntaxError: If *source* does not parse.
    """
    body = ast.parse(source).body
    for index, node in enumerate(body):
        if _is_main_guard(node):
            return [
                f'line {later.lineno}: {type(later).__name__}'
                + (f' {later.name}' if hasattr(later, 'name') else '')
                for later in body[index + 1:]
            ]
    return []


class TestStatementsAfterMainGuard(unittest.TestCase):
    """The checker itself, on sources built for the purpose."""

    def test_a_class_after_the_guard_is_reported(self) -> None:
        source = (
            "import unittest\n\n"
            "if __name__ == '__main__':\n    unittest.main()\n\n"
            "class TestLate(unittest.TestCase):\n    pass\n"
        )
        self.assertEqual(
            statements_after_main_guard(source),
            ['line 6: ClassDef TestLate'],
        )

    def test_a_guard_that_is_last_is_clean(self) -> None:
        source = (
            "class TestEarly:\n    pass\n\n"
            "if __name__ == '__main__':\n    pass\n"
        )
        self.assertEqual(statements_after_main_guard(source), [])

    def test_a_file_with_no_guard_is_clean(self) -> None:
        self.assertEqual(
            statements_after_main_guard('class TestOnly:\n    pass\n'), []
        )

    def test_the_reversed_comparison_is_still_a_guard(self) -> None:
        source = (
            "if '__main__' == __name__:\n    pass\n\n"
            "class TestLate:\n    pass\n"
        )
        self.assertEqual(
            statements_after_main_guard(source),
            ['line 4: ClassDef TestLate'],
        )

    def test_an_unrelated_if_is_not_a_guard(self) -> None:
        source = (
            "if __name__ != '__main__':\n    pass\n\n"
            "class TestLate:\n    pass\n"
        )
        self.assertEqual(statements_after_main_guard(source), [])

    def test_source_that_does_not_parse_raises(self) -> None:
        with self.assertRaises(SyntaxError):
            statements_after_main_guard('class (:\n')


class TestEveryTestFileEndsWithItsGuard(unittest.TestCase):
    """The rule, applied to the real suite."""

    def test_the_suite_has_files_to_check(self) -> None:
        # A glob that silently matches nothing would pass every file.
        self.assertGreater(len(list(_TESTS_DIR.glob('test_*.py'))), 1)

    def test_no_test_file_defines_anything_after_its_guard(self) -> None:
        for path in sorted(_TESTS_DIR.glob('test_*.py')):
            with self.subTest(file=path.name):
                after = statements_after_main_guard(
                    path.read_text(encoding='utf-8')
                )
                self.assertEqual(
                    after, [],
                    f'{path.name} defines code after its __main__ guard; '
                    f'running the file directly would skip it',
                )


if __name__ == '__main__':
    unittest.main()
