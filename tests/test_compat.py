"""Unit tests for :mod:`sbx_omnigent._compat`.

The shim exists to survive Omnigent's 140->45 subpackage regroup,
which moved three modules this package imports and left no aliases
behind. These tests pin the contract: each name resolves against
whichever layout is installed, and a name that is absent from BOTH
fails loud, naming both paths.

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import inspect
import unittest

from omnigent.onboarding.sandboxes.base import ExecModelHostLauncher

from sbx_omnigent import _compat


class TestResolvesAgainstInstalledOmnigent(unittest.TestCase):
    """Every straddled name resolves on the Omnigent that is here."""

    def test_model_family_mismatch_is_callable(self) -> None:
        self.assertTrue(callable(_compat.model_family_mismatch))

    def test_model_family_mismatch_still_rejects_a_mismatch(self) -> None:
        # Contract, not identity: a Claude model on the codex harness
        # is the case pipeline.py relies on being reported.
        self.assertIsNotNone(
            _compat.model_family_mismatch('codex', 'claude-opus-5')
        )

    def test_codex_efforts_is_a_nonempty_ladder(self) -> None:
        self.assertTrue(_compat.CODEX_EFFORTS)
        # Every ladder Omnigent ships carries the middle rungs.
        self.assertIn('high', _compat.CODEX_EFFORTS)
        self.assertIn('medium', _compat.CODEX_EFFORTS)

    def test_codex_efforts_stays_the_sdk_ladder_on_both_layouts(
        self,
    ) -> None:
        # Pinned on purpose: widening the gate to the native ladder
        # would change what reaches the codex CLI, and that decision
        # is not this shim's to make silently.
        self.assertNotIn('max', _compat.CODEX_EFFORTS)
        self.assertNotIn('ultra', _compat.CODEX_EFFORTS)

    def test_agy_bridge_module_name_matches_the_install(self) -> None:
        # Either real path, or None when neither is importable — but
        # never a wrong-looking name.
        name = _compat.agy_bridge_module_name()
        self.assertIn(
            name,
            (*_compat.AGY_BRIDGE_MODULES, None),
        )

    def test_load_agy_bridge_agrees_with_the_name_probe(self) -> None:
        module = _compat.load_agy_bridge()
        if module is None:
            self.assertIsNone(_compat.agy_bridge_module_name())
        else:
            self.assertEqual(
                module.__name__, _compat.agy_bridge_module_name()
            )


class TestFirstAttr(unittest.TestCase):
    """The resolver itself: first hit wins, total miss fails loud."""

    def test_returns_the_first_module_that_has_the_name(self) -> None:
        # ``os.sep`` exists; the bogus module ahead of it must be
        # skipped rather than raising.
        self.assertEqual(
            _compat._first_attr(
                ('sbx_omnigent.__nope__', 'os'), 'sep', 'thing'
            ),
            __import__('os').sep,
        )

    def test_missing_everywhere_raises_naming_every_path(self) -> None:
        with self.assertRaises(ImportError) as caught:
            _compat._first_attr(
                ('os', 'sys'), '__definitely_not_here__', 'the thing'
            )
        message = str(caught.exception)
        self.assertIn('the thing', message)
        self.assertIn('os', message)
        self.assertIn('sys', message)

    def test_default_is_returned_instead_of_raising(self) -> None:
        self.assertEqual(
            _compat._first_attr(
                ('os',), '__definitely_not_here__', 'x', default='fb'
            ),
            'fb',
        )


class TestStartHostShapeProbe(unittest.TestCase):
    """The probe must agree with the base class actually installed."""

    def test_probe_matches_the_installed_signature(self) -> None:
        expected = (
            'repos'
            in inspect.signature(ExecModelHostLauncher.start_host).parameters
        )
        self.assertEqual(_compat.base_start_host_takes_repos(), expected)

    def test_probe_is_a_bool(self) -> None:
        self.assertIsInstance(_compat.base_start_host_takes_repos(), bool)


class TestRepoWorkspace(unittest.TestCase):
    """The record Omnigent's newer ``start_host`` takes a list of."""

    def test_carries_the_three_fields_both_layouts_share(self) -> None:
        repo = _compat.repo_workspace(
            'https://github.com/org/repo', 'main', 'repo'
        )
        self.assertEqual(repo.url, 'https://github.com/org/repo')
        self.assertEqual(repo.branch, 'main')
        self.assertEqual(repo.repo_name, 'repo')

    def test_branch_may_be_none(self) -> None:
        repo = _compat.repo_workspace(
            'https://github.com/org/repo', None, 'repo'
        )
        self.assertIsNone(repo.branch)


if __name__ == '__main__':
    unittest.main()
