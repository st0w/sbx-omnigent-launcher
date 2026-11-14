"""Every third-party module the package imports is declared.

The launcher installs into Omnigent's environment and once declared no
dependencies at all, so ``click`` and PyYAML arrived only because
Omnigent happened to require them. Omnigent itself stays undeclared on
purpose (see the note in ``pyproject.toml``); everything else the
package imports must be listed.

Run: .venv/bin/python -m unittest tests.test_packaging
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent

#: Import name -> distribution name, where they differ.
_DISTRIBUTION = {'yaml': 'pyyaml'}

#: Imported but deliberately undeclared: the environment the launcher
#: installs into provides it (see ``pyproject.toml``).
_PROVIDED = {'omnigent'}


def _imported_top_level() -> set[str]:
    """Every top-level module ``sbx_omnigent`` imports absolutely."""
    found: set[str] = set()
    for path in (_ROOT / 'sbx_omnigent').rglob('*.py'):
        tree = ast.parse(path.read_text(encoding='utf-8'), str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found.update(a.name.split('.')[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                assert node.module is not None
                found.add(node.module.split('.')[0])
    return found


def _declared() -> set[str]:
    """The distributions ``pyproject.toml`` declares, normalised."""
    with (_ROOT / 'pyproject.toml').open('rb') as handle:
        project = tomllib.load(handle)['project']
    return {
        re.split(r'[<>=!~;\[ ]', req, maxsplit=1)[0].lower()
        for req in project.get('dependencies', [])
    }


class TestDependenciesAreDeclared(unittest.TestCase):
    def test_every_third_party_import_is_declared(self) -> None:
        third_party = {
            name for name in _imported_top_level()
            if name not in sys.stdlib_module_names
            and name not in {'sbx_omnigent', '__future__'}
            and name not in _PROVIDED
        }
        missing = {
            _DISTRIBUTION.get(name, name) for name in third_party
        } - _declared()
        self.assertEqual(missing, set())

    def test_omnigent_stays_undeclared(self) -> None:
        # Declaring it lets an install swap the Omnigent already there.
        self.assertNotIn('omnigent', _declared())


if __name__ == '__main__':
    unittest.main()
