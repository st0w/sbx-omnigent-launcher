"""Stage-3 tests: ``sbx.agy_enabled`` config wiring.

Builds a ``ManagedSandboxConfig`` via ``_build_sbx_config`` and inspects
the launcher its factory yields. Run with:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import contextlib
import io
import threading
import unittest
from unittest import mock

from sbx_omnigent import agy
from sbx_omnigent import entrypoint as E
from sbx_omnigent.entrypoint import _build_sbx_config
from sbx_omnigent.launcher import SbxLauncher

#: Stands in for the server's harvester in every test here: building a
#: config with agy enabled would otherwise start a real one.
_HARVESTER = mock.create_autospec(E._ServerHarvester, instance=True)
_PATCH = mock.patch.object(E, '_AGY_HARVESTER', _HARVESTER)


def setUpModule() -> None:
    _PATCH.start()


def tearDownModule() -> None:
    _PATCH.stop()


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



class TestAgyLocationConfig(unittest.TestCase):
    """The location names a Vertex AI host each agy VM's swap secret
    covers, so a malformed one is refused at startup."""

    def test_a_malformed_location_is_refused(self) -> None:
        with self.assertRaises(ValueError) as caught:
            _build_sbx_config(_cfg({
                'agy_enabled': True, 'agy_gcp_location': 'x.evil.com',
            }))
        self.assertIn('agy_gcp_location', str(caught.exception))

    def test_a_regional_location_is_accepted(self) -> None:
        launcher = _launcher({
            'agy_enabled': True, 'agy_gcp_location': 'us-central1',
        })
        self.assertEqual(launcher._agy_gcp_location, 'us-central1')


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

class _Proc:
    """A spawned harvester, as far as the server sees one."""

    pid = 4242


class TestTheServerKeepsAgyTokensFresh(unittest.TestCase):
    """
    With ``agy_enabled``, the server starts the agy token harvester
    itself and stops it when it exits, so nobody has to keep a second
    command running. One that is already running is used, not doubled.
    """

    def _harvester(self, *, running: bool = False) -> E._ServerHarvester:
        self.checks: list[int] = []
        self.spawned: list[_Proc] = []
        self.at_exit: list[tuple[object, tuple[object, ...]]] = []
        self.watched: list[int] = []

        def is_running() -> bool:
            self.checks.append(1)
            return running

        def spawn() -> _Proc:
            proc = _Proc()
            self.spawned.append(proc)
            return proc

        return E._ServerHarvester(
            running=is_running,
            spawn=spawn,
            register_exit=lambda fn, *args: self.at_exit.append((fn, args)),
            start_watch=lambda: self.watched.append(1),
        )

    def _ensure(self, harvester: E._ServerHarvester, enabled: bool) -> str:
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            harvester.ensure(enabled)
        return err.getvalue()

    def test_nothing_happens_without_agy(self) -> None:
        harvester = self._harvester()
        self.assertEqual(self._ensure(harvester, False), '')
        self.assertEqual((self.checks, self.spawned), ([], []))

    def test_it_starts_one(self) -> None:
        printed = self._ensure(self._harvester(), True)
        self.assertEqual(len(self.spawned), 1)
        self.assertIn('4242', printed)
        self.assertIn(str(agy.HARVEST_LOG), printed)

    def test_it_stops_the_one_it_started_when_the_server_exits(self) -> None:
        self._ensure(self._harvester(), True)
        self.assertEqual(
            self.at_exit, [(agy.stop_harvester, (self.spawned[0],))]
        )

    def test_it_watches_for_the_first_refresh(self) -> None:
        self._ensure(self._harvester(), True)
        self.assertEqual(self.watched, [1])

    def test_one_already_running_is_used(self) -> None:
        printed = self._ensure(self._harvester(running=True), True)
        self.assertEqual((self.spawned, self.at_exit), ([], []))
        self.assertIn('already running', printed)

    def test_it_starts_once_however_often_the_config_is_read(self) -> None:
        harvester = self._harvester()
        for _ in range(3):
            self._ensure(harvester, True)
        self.assertEqual((len(self.checks), len(self.spawned)), (1, 1))

    def test_concurrent_reads_start_one(self) -> None:
        harvester = self._harvester()
        threads = [
            threading.Thread(target=harvester.ensure, args=(True,))
            for _ in range(8)
        ]
        with contextlib.redirect_stderr(io.StringIO()):
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual(len(self.spawned), 1)


class TestTheFirstRefreshIsChecked(unittest.TestCase):
    """A harvester that cannot refresh says so in the server's output,
    instead of agy sessions failing to sign in with no explanation."""

    def _watch(self, fresh: bool) -> tuple[str, list[dict[str, object]]]:
        calls: list[dict[str, object]] = []

        def wait(**kwargs: object) -> bool:
            calls.append(kwargs)
            return fresh

        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            E._watch_first_refresh(wait=wait)
        return err.getvalue(), calls

    def test_a_refresh_is_quiet(self) -> None:
        printed, calls = self._watch(True)
        self.assertEqual(printed, '')
        self.assertEqual(calls[0]['deadline_s'], E._FIRST_REFRESH_S)

    def test_no_refresh_says_where_to_look_and_how_to_sign_in(self) -> None:
        printed, _calls = self._watch(False)
        self.assertIn('WARNING', printed)
        self.assertIn(str(agy.HARVEST_LOG), printed)
        self.assertIn('/login', printed)


class TestTheConfigStartsTheHarvester(unittest.TestCase):
    """Reading the sbx config hands ``agy_enabled`` to the server's
    harvester."""

    def test_enabled(self) -> None:
        _build_sbx_config(_cfg({'agy_enabled': True}))
        _HARVESTER.ensure.assert_called_with(True)

    def test_disabled(self) -> None:
        _build_sbx_config(_cfg({}))
        _HARVESTER.ensure.assert_called_with(False)


if __name__ == '__main__':
    unittest.main()
