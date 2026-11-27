"""The runner tests' stand-ins for this machine's real state."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sbx_omnigent import (
    agy,
    claude_plugins,
    codex,
    guest_log,
    harness_versions,
    orphans,
)
from tests.support.claude_plugins import PLUGIN_ID
from tests.support.host import isolate_from_host


class TestNothingReadsThisHost(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.host = isolate_from_host(self, self.root)

    def test_a_plugin_resolves_from_the_stand_in_home(self) -> None:
        plugin = claude_plugins.resolve(PLUGIN_ID)
        self.assertEqual(plugin.path, self.host.plugins.install.resolve())

    def test_the_harvest_lock_is_the_tests_own(self) -> None:
        self.assertEqual(agy.HARVEST_LOCK, self.root / 'agy-harvest.lock')

    def test_vm_reads_and_probes_answer_nothing(self) -> None:
        self.assertEqual(harness_versions.read_versions('box'), {})
        self.assertIsNone(codex.probe_login())
        self.assertIsNone(guest_log.read_runner_log('box'))
        self.assertIsNone(orphans.orphan_advice())

    def test_the_stand_ins_are_the_ones_a_test_scripts(self) -> None:
        self.host.read_versions.return_value = {'claude': '2.1.325'}
        self.assertEqual(
            harness_versions.read_versions('box'), {'claude': '2.1.325'}
        )


class TestEverythingIsPutBack(unittest.TestCase):
    def test_after_the_test_the_real_reads_return(self) -> None:
        real = (
            agy.HARVEST_LOCK, harness_versions.read_versions,
            codex.probe_login, guest_log.read_runner_log,
            orphans.orphan_advice, claude_plugins.resolve,
        )
        case = unittest.TestCase()
        with tempfile.TemporaryDirectory() as tmp:
            isolate_from_host(case, Path(tmp))
            case.doCleanups()
        self.assertEqual(
            (
                agy.HARVEST_LOCK, harness_versions.read_versions,
                codex.probe_login, guest_log.read_runner_log,
                orphans.orphan_advice, claude_plugins.resolve,
            ),
            real,
        )


if __name__ == '__main__':
    unittest.main()
