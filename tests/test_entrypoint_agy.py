"""Stage-3 tests: ``sbx.agy_enabled`` config wiring.

Builds a ``ManagedSandboxConfig`` via ``_build_sbx_config`` and inspects
the launcher its factory yields. Run with:

    .venv/bin/python -m unittest discover -s tests
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


class TestAgyEnabledConfig(unittest.TestCase):
    """agy_enabled / agy_enterprise parse and reach the launcher."""

    def test_default_off(self) -> None:
        launcher = _launcher({})
        self.assertFalse(launcher._agy_enabled)
        self.assertFalse(launcher._agy_enterprise)

    def test_enabled_flows_to_launcher(self) -> None:
        launcher = _launcher({'agy_enabled': True})
        self.assertTrue(launcher._agy_enabled)

    def test_enterprise_flows_to_launcher(self) -> None:
        launcher = _launcher({'agy_enabled': True, 'agy_enterprise': True})
        self.assertTrue(launcher._agy_enterprise)

    def test_rejects_non_bool_agy_enabled(self) -> None:
        with self.assertRaises(ValueError):
            _build_sbx_config(_cfg({'agy_enabled': 'yes'}))

    def test_gcp_project_and_location_flow(self) -> None:
        launcher = _launcher({
            'agy_enabled': True,
            'agy_gcp_project': 'p-1',
            'agy_gcp_location': 'eu',
        })
        self.assertEqual(launcher._agy_gcp_project, 'p-1')
        self.assertEqual(launcher._agy_gcp_location, 'eu')

    def test_gcp_location_defaults_us(self) -> None:
        launcher = _launcher({'agy_enabled': True, 'agy_gcp_project': 'p-1'})
        self.assertEqual(launcher._agy_gcp_location, 'us')

    def test_rejects_empty_gcp_project(self) -> None:
        with self.assertRaises(ValueError):
            _build_sbx_config(_cfg({'agy_gcp_project': '   '}))


if __name__ == '__main__':
    unittest.main()
