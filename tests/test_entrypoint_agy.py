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



class TestAgyUpdateConfig(unittest.TestCase):
    """`sbx.agy_update` reaches the launcher, off unless asked for."""

    def test_default_off(self) -> None:
        self.assertFalse(_launcher({'agy_enabled': True})._agy_update)

    def test_true_reaches_the_launcher(self) -> None:
        launcher = _launcher({
            'agy_enabled': True, 'agy_update': True,
            'agy_gcp_project': 'p-1',
        })
        self.assertTrue(launcher._agy_update)

    def test_refused_without_a_gcp_project(self) -> None:
        # An updated agy signs in through Application Default
        # Credentials against a GCP project; nothing else is verified.
        with self.assertRaises(ValueError) as caught:
            _build_sbx_config(
                _cfg({'agy_enabled': True, 'agy_update': True})
            )
        self.assertIn('agy_gcp_project', str(caught.exception))

    def test_rejects_non_bool(self) -> None:
        for bad in ('yes', 1, 'true'):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    _build_sbx_config(_cfg({
                        'agy_enabled': True, 'agy_update': bad,
                        'agy_gcp_project': 'p-1',
                    }))

    def test_refused_without_agy_enabled(self) -> None:
        # With no agy VMs it would update nothing, so a config that
        # sets it is refused rather than quietly ignored.
        with self.assertRaises(ValueError) as caught:
            _build_sbx_config(_cfg({'agy_update': True}))
        self.assertIn('agy_enabled', str(caught.exception))

    def test_false_without_agy_enabled_is_fine(self) -> None:
        self.assertFalse(_launcher({'agy_update': False})._agy_update)


if __name__ == '__main__':
    unittest.main()
