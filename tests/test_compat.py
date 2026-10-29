"""Unit tests for :mod:`sbx_omnigent._compat`.

The oldest Omnigent this launcher supports, v0.13.0, is past the
140->45 subpackage regroup, so three of the four names that move
straddled come from their one current home (#56). ``RepoWorkspace``
still sits at a different path in v0.13.0 than in later releases, so
it is still resolved against whichever is installed, and a name absent
from BOTH fails loud, naming both paths.

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import inspect
import sys
import types
import unittest
from unittest import mock

from omnigent.models import model_override
from omnigent.onboarding.sandboxes.base import ExecModelHostLauncher
from omnigent.util import reasoning_effort

from sbx_omnigent import _compat, launcher


class TestResolvesAgainstInstalledOmnigent(unittest.TestCase):
    """Every name resolves on the Omnigent that is here."""

    def test_model_family_mismatch_is_omnigents_own(self) -> None:
        self.assertIs(
            _compat.model_family_mismatch,
            model_override.model_family_mismatch,
        )

    def test_model_family_mismatch_still_rejects_a_mismatch(self) -> None:
        # Contract, not identity: a Claude model on the codex harness
        # is the case pipeline.py relies on being reported.
        self.assertIsNotNone(
            _compat.model_family_mismatch('codex', 'claude-opus-5')
        )

    def test_codex_efforts_is_omnigents_ladder(self) -> None:
        self.assertEqual(
            _compat.CODEX_EFFORTS, frozenset(reasoning_effort.CODEX_EFFORTS)
        )

    def test_codex_efforts_is_a_nonempty_ladder(self) -> None:
        self.assertTrue(_compat.CODEX_EFFORTS)
        # Every ladder Omnigent ships carries the middle rungs.
        self.assertIn('high', _compat.CODEX_EFFORTS)
        self.assertIn('medium', _compat.CODEX_EFFORTS)

    def test_codex_efforts_stays_the_sdk_ladder(self) -> None:
        # Pinned on purpose: widening the gate to the native ladder
        # would change what reaches the codex CLI, and that decision
        # is not this module's to make silently.
        self.assertNotIn('max', _compat.CODEX_EFFORTS)
        self.assertNotIn('ultra', _compat.CODEX_EFFORTS)

    def test_agy_bridge_module_name_matches_the_install(self) -> None:
        # The one real path, or None when it is not importable.
        self.assertIn(
            _compat.agy_bridge_module_name(),
            (_compat.AGY_BRIDGE_MODULE, None),
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


#: Where the agy bridge lived before the regroup (Omnigent v0.12.0 and
#: earlier), which no supported release has.
_PRE_REGROUP_BRIDGE = 'omnigent.antigravity_native_bridge'


class TestTheAgyBridge(unittest.TestCase):
    """The bridge has one path on every supported release (#56)."""

    def test_it_is_the_harnesses_subpackage(self) -> None:
        self.assertEqual(
            _compat.AGY_BRIDGE_MODULE,
            'omnigent.harnesses.antigravity_native.bridge',
        )

    def test_it_is_loaded_from_there(self) -> None:
        name = _compat.AGY_BRIDGE_MODULE
        with mock.patch.dict(sys.modules, {name: _module(name)}):
            module = _compat.load_agy_bridge()
        self.assertIsNotNone(module)
        self.assertEqual(getattr(module, '__name__', None), name)

    def test_the_pre_regroup_path_is_never_tried(self) -> None:
        with mock.patch.dict(sys.modules, {
            _compat.AGY_BRIDGE_MODULE: None,
            _PRE_REGROUP_BRIDGE: _module(_PRE_REGROUP_BRIDGE),
        }):
            self.assertIsNone(_compat.load_agy_bridge())

    def test_absent_is_none_not_an_error(self) -> None:
        with mock.patch.dict(sys.modules, {_compat.AGY_BRIDGE_MODULE: None}):
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

    def test_carries_the_three_fields_both_paths_share(self) -> None:
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
