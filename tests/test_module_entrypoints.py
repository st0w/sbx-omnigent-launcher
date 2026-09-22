"""Every ``python -m sbx_omnigent.<module>`` entry point runs cleanly.

The package ``__init__`` used to import :mod:`sbx_omnigent.launcher`,
which pulls in ``agy``, ``swarm``, ``swarm_session`` and ``worktrees``.
Running any of those with ``-m`` then executed the module a SECOND
time, as ``__main__`` — two copies of its globals and its classes —
and runpy said so on every invocation::

    RuntimeWarning: 'sbx_omnigent.agy' found in sys.modules after
    import of package 'sbx_omnigent', but prior to execution of
    'sbx_omnigent.agy'; this may result in unpredictable behaviour

Not cosmetic: ``spawn_harvester`` starts the harvester exactly that
way, so it landed in the harvest log on every spawn. And ``runner`` —
the entry point the README uses most — was clean only by luck: nothing
on the launcher's import chain happened to reach it yet.

Each case runs in a subprocess because the hazard is in how a fresh
interpreter starts, which an in-process import cannot reproduce.

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

import sbx_omnigent

#: The package source directory, for discovering ``__main__`` blocks.
_PACKAGE_DIR = Path(sbx_omnigent.__file__).resolve().parent

#: Every module with a ``__main__`` block. A new one belongs here —
#: :meth:`TestModuleEntryPoints.test_list_names_every_main_block`
#: fails until it is added.
_MAIN_MODULES = (
    'agy',
    'entrypoint',
    'runner',
    'swarm',
    'swarm_session',
    'worktrees',
)

#: Generous: ``entrypoint`` imports Omnigent's whole CLI.
_TIMEOUT_S = 120


def _python(*args: str) -> subprocess.CompletedProcess[str]:
    """
    Run this interpreter with every ``RuntimeWarning`` promoted to an
    error, so a warning fails the process instead of scrolling past.

    :param args: Arguments after the interpreter flags.
    :returns: The completed process, output captured as text.
    """
    return subprocess.run(
        [sys.executable, '-W', 'error::RuntimeWarning', *args],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT_S,
        check=False,
    )


class TestModuleEntryPoints(unittest.TestCase):
    """``python -m`` on each CLI module starts without a warning."""

    def test_every_main_module_runs_without_a_runtime_warning(self) -> None:
        for name in _MAIN_MODULES:
            with self.subTest(module=name):
                result = _python('-m', f'sbx_omnigent.{name}', '--help')
                self.assertEqual(result.returncode, 0, result.stderr[-800:])

    def test_list_names_every_main_block(self) -> None:
        # Guards the guard: a module that gains a __main__ block but
        # is missing from _MAIN_MODULES would regress unnoticed.
        found = sorted(
            path.stem
            for path in _PACKAGE_DIR.glob('*.py')
            if "if __name__ == '__main__'" in path.read_text(encoding='utf-8')
        )
        self.assertEqual(found, sorted(_MAIN_MODULES))


class TestEveryModuleImportsOnItsOwn(unittest.TestCase):
    """Each module imports first, in a fresh interpreter.

    An import cycle only shows when the module in the cycle is the one
    imported first, so each is imported alone. This is what makes it
    safe to keep every import at module level.
    """

    def test_every_module_imports_first_in_a_fresh_interpreter(
        self,
    ) -> None:
        modules = sorted(
            path.stem for path in _PACKAGE_DIR.glob('*.py')
            if path.stem != '__init__'
        )
        self.assertIn('defaults', modules)
        for name in modules:
            with self.subTest(module=name):
                result = _python('-c', f'import sbx_omnigent.{name}')
                self.assertEqual(
                    result.returncode, 0, result.stderr[-800:]
                )


class TestPackageRootImportsNothing(unittest.TestCase):
    """The root cause, pinned directly: the package init stays empty."""

    def test_importing_the_package_loads_no_submodule(self) -> None:
        result = _python(
            '-c',
            'import sys, sbx_omnigent; '
            "print(sorted(m for m in sys.modules "
            "if m.startswith('sbx_omnigent.')))",
        )
        self.assertEqual(result.stdout.strip(), '[]', result.stderr[-800:])


if __name__ == '__main__':
    unittest.main()
