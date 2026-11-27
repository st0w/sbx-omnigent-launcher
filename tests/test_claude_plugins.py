"""Host Claude Code plugins, staged for a pipeline image (no network).

Run: .venv/bin/python -m unittest tests.test_claude_plugins
"""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from sbx_omnigent import claude_plugins as cp
from tests.support.claude_plugins import PLUGIN_ID, PluginHome

_ID = PLUGIN_ID


class TestAPluginIdIsValidated(unittest.TestCase):
    def test_a_real_id_passes(self) -> None:
        self.assertEqual(cp.validate_plugin_id(_ID), _ID)

    def test_anything_else_is_refused(self) -> None:
        for bad in ('claude-security', '@market', 'name@', 'a@b@c',
                    'Name@market', '../x@y', 'a b@c', '', None, 7):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    cp.validate_plugin_id(bad)


class TestAnInstalledPluginIsFound(unittest.TestCase):
    def setUp(self) -> None:
        self.h = PluginHome()
        self.addCleanup(self.h.cleanup)

    def test_it_resolves_to_its_install(self) -> None:
        plugin = cp.resolve(_ID, home=self.h.home)
        self.assertEqual(
            (plugin.name, plugin.version, plugin.path),
            ('claude-security', '0.12.0', self.h.install.resolve()),
        )

    def test_one_not_installed_is_refused_by_name(self) -> None:
        with self.assertRaises(cp.PluginError) as caught:
            cp.resolve('other@claude-plugins-official', home=self.h.home)
        self.assertIn('other@claude-plugins-official', str(caught.exception))
        self.assertIn('claude plugin install', str(caught.exception))

    def test_no_registry_is_refused(self) -> None:
        (self.h.plugins / 'installed_plugins.json').unlink()
        with self.assertRaises(cp.PluginError):
            cp.resolve(_ID, home=self.h.home)

    def test_an_install_outside_the_plugins_dir_is_refused(self) -> None:
        outside = Path(tempfile.mkdtemp(prefix='plugin-outside-'))
        self.addCleanup(shutil.rmtree, outside, True)
        self.h.register(_ID, outside, '0.12.0')
        with self.assertRaises(cp.PluginError) as caught:
            cp.resolve(_ID, home=self.h.home)
        self.assertIn('outside', str(caught.exception))

    def test_a_plugin_with_a_symlink_is_refused(self) -> None:
        (self.h.install / 'skills' / 'link').symlink_to('/etc/hosts')
        with self.assertRaises(cp.PluginError) as caught:
            cp.resolve(_ID, home=self.h.home)
        self.assertIn('symlink', str(caught.exception))


class TestTheDigest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = PluginHome()
        self.addCleanup(self.h.cleanup)

    def _digest(self) -> str:
        return cp.digest([cp.resolve(_ID, home=self.h.home)])

    def test_it_is_stable(self) -> None:
        self.assertEqual(self._digest(), self._digest())

    def test_it_changes_with_content(self) -> None:
        before = self._digest()
        (self.h.install / 'skills' / 'SKILL.md').write_text('scan more\n')
        self.assertNotEqual(self._digest(), before)

    def test_it_changes_with_the_version(self) -> None:
        before = self._digest()
        self.h.register(_ID, self.h.install, '0.13.0')
        self.assertNotEqual(self._digest(), before)

    def test_in_use_markers_do_not_count(self) -> None:
        before = self._digest()
        (self.h.install / '.in_use' / '9999').write_text('pid\n')
        self.assertEqual(self._digest(), before)


class TestStagingAMarketplace(unittest.TestCase):
    def setUp(self) -> None:
        self.h = PluginHome()
        self.addCleanup(self.h.cleanup)
        self.dest = Path(tempfile.mkdtemp(prefix='plugin-stage-'))
        self.addCleanup(shutil.rmtree, self.dest, True)

    def _stage(self) -> Path:
        return cp.stage_marketplace(
            [cp.resolve(_ID, home=self.h.home)], self.dest
        )

    def test_it_lists_each_plugin_from_a_local_source(self) -> None:
        root = self._stage()
        self.assertEqual(root.name, cp.STAGED_MARKETPLACE)
        manifest = json.loads(
            (root / '.claude-plugin' / 'marketplace.json').read_text()
        )
        self.assertEqual(manifest['name'], cp.STAGED_MARKETPLACE)
        self.assertEqual(
            manifest['plugins'],
            [{'name': 'claude-security', 'source': './plugins/claude-security',
              'version': '0.12.0'}],
        )

    def test_it_copies_the_plugin_without_in_use(self) -> None:
        root = self._stage()
        copy = root / 'plugins' / 'claude-security'
        self.assertEqual(
            (copy / 'skills' / 'SKILL.md').read_text(), 'scan things\n'
        )
        self.assertFalse((copy / '.in_use').exists())

    def test_the_installed_id_names_the_staged_marketplace(self) -> None:
        plugin = cp.resolve(_ID, home=self.h.home)
        self.assertEqual(
            cp.staged_id(plugin), f'claude-security@{cp.STAGED_MARKETPLACE}'
        )


if __name__ == '__main__':
    unittest.main()
