"""``sbx.claude_version``: config parsing and its startup refusals.

Builds a ``ManagedSandboxConfig`` via ``_build_sbx_config`` and inspects
the launcher its factory yields, as the other ``sbx.*`` keys are tested.

Run: .venv/bin/python -m unittest tests.test_entrypoint_claude_pin
"""

from __future__ import annotations

import unittest

from sbx_omnigent.entrypoint import _build_sbx_config
from sbx_omnigent.launcher import SbxLauncher


def _cfg(sbx: dict[str, object]) -> dict[str, object]:
    return {'server_url': 'http://host.docker.internal:6767', 'sbx': sbx}


def _launcher(sbx: dict[str, object]) -> SbxLauncher:
    """The launcher the sbx config's factory builds, typed as one."""
    launcher = _build_sbx_config(_cfg(sbx)).launcher_factory()
    if not isinstance(launcher, SbxLauncher):
        raise AssertionError(f'the factory built {launcher!r}')
    return launcher


class TestTheVersionReachesTheLauncher(unittest.TestCase):
    def test_unset_means_no_pin(self) -> None:
        launcher = _launcher({})
        self.assertIsNone(launcher._claude_version)

    def test_a_version_is_handed_to_the_launcher(self) -> None:
        launcher = _launcher({'claude_version': '2.1.280'})
        self.assertEqual(launcher._claude_version, '2.1.280')


class TestABadVersionRefusesToStart(unittest.TestCase):
    """It is interpolated into a shell command in every Claude VM."""

    def test_anything_but_an_exact_version_is_refused(self) -> None:
        for value in ('latest', '2.1', 'v2.1.280', '2.1.280; true', 2.1):
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as caught:
                    _build_sbx_config(_cfg({'claude_version': value}))
                self.assertIn(
                    "'sandbox.sbx.claude_version'", str(caught.exception)
                )


class TestAnAllowlistThatBlocksTheInstallRefusesToStart(unittest.TestCase):
    """The install comes from registry.npmjs.org. An explicit
    `egress_allow` without it would fail every Claude VM at start."""

    def test_an_allowlist_without_the_registry_is_refused(self) -> None:
        with self.assertRaises(ValueError) as caught:
            _build_sbx_config(_cfg({
                'claude_version': '2.1.280',
                'egress_allow': ['api.anthropic.com'],
            }))
        self.assertIn('registry.npmjs.org', str(caught.exception))

    def test_dial_back_only_lockdown_is_refused_too(self) -> None:
        with self.assertRaises(ValueError):
            _build_sbx_config(_cfg({
                'claude_version': '2.1.280', 'egress_allow': [],
            }))

    def test_the_default_allowlist_is_enough(self) -> None:
        _build_sbx_config(_cfg({'claude_version': '2.1.280'}))

    def test_an_allowlist_naming_the_registry_is_enough(self) -> None:
        for entry in (
            'registry.npmjs.org', 'registry.npmjs.org:443', '*.npmjs.org',
        ):
            with self.subTest(entry=entry):
                _build_sbx_config(_cfg({
                    'claude_version': '2.1.280',
                    'egress_allow': ['api.anthropic.com', entry],
                }))

    def test_no_pin_means_no_egress_requirement(self) -> None:
        _build_sbx_config(_cfg({'egress_allow': ['api.anthropic.com']}))


if __name__ == '__main__':
    unittest.main()
