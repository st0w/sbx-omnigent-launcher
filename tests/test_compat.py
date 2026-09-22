"""Unit tests for :mod:`sbx_omnigent._compat`.

The shim exists to survive Omnigent's 140->45 subpackage regroup,
which moved three modules this package imports and left no aliases
behind. These tests pin the contract: each name resolves against
whichever layout is installed, and a name that is absent from BOTH
fails loud, naming both paths.

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import importlib
import inspect
import sys
import types
import unittest
from unittest import mock

from omnigent.onboarding.sandboxes.base import ExecModelHostLauncher

from sbx_omnigent import _compat, launcher


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


def _module(name: str, **attrs: object) -> types.ModuleType:
    """A stand-in module carrying *attrs*."""
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


class TestImportTimeFallbacks(unittest.TestCase):
    """The two names resolved at import fall back to the legacy path.

    `_compat` is re-executed with the new path made unimportable and a
    stand-in at the legacy one, then restored.
    """

    def tearDown(self) -> None:
        importlib.reload(_compat)

    def _reload_with(self, modules: dict[str, object]) -> None:
        with mock.patch.dict(sys.modules, modules):
            importlib.reload(_compat)

    def test_model_family_mismatch_falls_back(self) -> None:
        def legacy_guard(harness: str, model: str) -> str | None:
            return f'legacy: {harness} {model}'

        legacy = _module('omnigent.model_override',
                         model_family_mismatch=legacy_guard)
        self._reload_with({
            'omnigent.models.model_override': None,
            'omnigent.model_override': legacy,
        })
        self.assertEqual(
            _compat.model_family_mismatch('h', 'm'), 'legacy: h m'
        )

    def test_a_guard_that_is_not_callable_is_refused(self) -> None:
        legacy = _module('omnigent.model_override',
                         model_family_mismatch='not a function')
        with self.assertRaises(TypeError):
            self._reload_with({
                'omnigent.models.model_override': None,
                'omnigent.model_override': legacy,
            })

    def test_an_effort_ladder_that_is_not_names_is_refused(self) -> None:
        legacy = _module('omnigent.reasoning_effort', CODEX_EFFORTS=[1, 2])
        with self.assertRaises(TypeError):
            self._reload_with({
                'omnigent.util.reasoning_effort': None,
                'omnigent.reasoning_effort': legacy,
            })

    def test_codex_efforts_falls_back(self) -> None:
        legacy = _module('omnigent.reasoning_effort',
                         CODEX_EFFORTS=frozenset({'high'}))
        self._reload_with({
            'omnigent.util.reasoning_effort': None,
            'omnigent.reasoning_effort': legacy,
        })
        self.assertEqual(_compat.CODEX_EFFORTS, frozenset({'high'}))

    def test_neither_path_fails_naming_both(self) -> None:
        for new, old in (
            ('omnigent.models.model_override', 'omnigent.model_override'),
            ('omnigent.util.reasoning_effort', 'omnigent.reasoning_effort'),
        ):
            with self.subTest(new=new):
                with self.assertRaises(ImportError) as caught:
                    self._reload_with({new: None, old: None})
                self.assertIn(new, str(caught.exception))
                self.assertIn(old, str(caught.exception))


class TestAgyBridgeFallback(unittest.TestCase):
    """The bridge is tried in `AGY_BRIDGE_MODULES` order.

    The in-VM patch script reads that tuple, so the order it names and
    the order `load_agy_bridge` tries must be the same.
    """

    def _loaded_name(self) -> str:
        module = _compat.load_agy_bridge()
        if module is None:
            self.fail('no agy bridge was loaded')
        return module.__name__

    def test_the_first_path_wins(self) -> None:
        first, second = _compat.AGY_BRIDGE_MODULES
        with mock.patch.dict(sys.modules, {
            first: _module(first), second: _module(second),
        }):
            self.assertEqual(self._loaded_name(), first)

    def test_the_second_path_is_the_fallback(self) -> None:
        first, second = _compat.AGY_BRIDGE_MODULES
        with mock.patch.dict(sys.modules, {
            first: None, second: _module(second),
        }):
            self.assertEqual(self._loaded_name(), second)

    def test_neither_is_none_not_an_error(self) -> None:
        first, second = _compat.AGY_BRIDGE_MODULES
        with mock.patch.dict(sys.modules, {first: None, second: None}):
            self.assertIsNone(_compat.load_agy_bridge())


class TestRepoWorkspaceFallback(unittest.TestCase):
    """`RepoWorkspace` is tried in `_REPO_WORKSPACE_MODULES` order."""

    @staticmethod
    def _factory(
        *, url: str, branch: str | None, repo_name: str
    ) -> types.SimpleNamespace:
        return types.SimpleNamespace(
            url=url, branch=branch, repo_name=repo_name
        )

    def test_the_legacy_path_is_the_fallback(self) -> None:
        new, old = _compat._REPO_WORKSPACE_MODULES
        with mock.patch.dict(sys.modules, {
            new: None, old: _module(old, RepoWorkspace=self._factory),
        }):
            repo = _compat.repo_workspace('u', 'b', 'n')
        self.assertEqual(
            (repo.url, repo.branch, repo.repo_name), ('u', 'b', 'n')
        )

    def test_a_module_without_it_is_skipped(self) -> None:
        # Omnigent v0.13.0: the new module imports, and the record is
        # still at the legacy path.
        new, old = _compat._REPO_WORKSPACE_MODULES
        with mock.patch.dict(sys.modules, {
            new: _module(new), old: _module(old, RepoWorkspace=self._factory),
        }):
            repo = _compat.repo_workspace('u', None, 'n')
        self.assertEqual(repo.repo_name, 'n')

    def test_neither_module_defining_it_fails_naming_both(self) -> None:
        new, old = _compat._REPO_WORKSPACE_MODULES
        with mock.patch.dict(sys.modules, {
            new: _module(new), old: _module(old),
        }):
            with self.assertRaises(ImportError) as caught:
                _compat.repo_workspace('u', None, 'n')
        self.assertIn(new, str(caught.exception))
        self.assertIn(old, str(caught.exception))

    def test_neither_path_fails_naming_both(self) -> None:
        new, old = _compat._REPO_WORKSPACE_MODULES
        with mock.patch.dict(sys.modules, {new: None, old: None}):
            with self.assertRaises(ImportError) as caught:
                _compat.repo_workspace('u', None, 'n')
        self.assertIn(old, str(caught.exception))


class TestStartHostShapeProbe(unittest.TestCase):
    """The probe must agree with the base class actually installed."""

    def test_probe_matches_the_installed_signature(self) -> None:
        expected = (
            'repos'
            in inspect.signature(ExecModelHostLauncher.start_host).parameters
        )
        self.assertEqual(launcher.base_start_host_takes_repos(), expected)

    def test_probe_is_a_bool(self) -> None:
        self.assertIsInstance(launcher.base_start_host_takes_repos(), bool)


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
