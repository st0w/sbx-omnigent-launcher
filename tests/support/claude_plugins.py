"""A stand-in home with one Claude Code plugin installed."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

#: The plugin the stand-in home has installed.
PLUGIN_ID = 'claude-security@claude-plugins-official'


class PluginHome:
    """A temporary home with one installed plugin."""

    def __init__(self) -> None:
        self.home = Path(tempfile.mkdtemp(prefix='plugin-home-'))
        self.plugins = self.home / '.claude' / 'plugins'
        self.install = (
            self.plugins / 'cache' / 'claude-plugins-official'
            / 'claude-security' / '0.12.0'
        )
        (self.install / '.claude-plugin').mkdir(parents=True)
        (self.install / '.claude-plugin' / 'plugin.json').write_text(
            json.dumps({'name': 'claude-security', 'version': '0.12.0'})
        )
        (self.install / 'skills').mkdir()
        (self.install / 'skills' / 'SKILL.md').write_text('scan things\n')
        (self.install / '.in_use').mkdir()
        (self.install / '.in_use' / '1203').write_text('pid\n')
        self.register(PLUGIN_ID, self.install, '0.12.0')

    def register(self, plugin_id: str, path: Path, version: str) -> None:
        """
        Record *plugin_id* as installed at *path*.

        :param plugin_id: ``<name>@<marketplace>``.
        :param path: Where it is installed.
        :param version: Its version.
        """
        registry = self.plugins / 'installed_plugins.json'
        data = (
            json.loads(registry.read_text())
            if registry.exists() else {'version': 2, 'plugins': {}}
        )
        data['plugins'][plugin_id] = [
            {'scope': 'user', 'installPath': str(path), 'version': version}
        ]
        registry.write_text(json.dumps(data))

    def cleanup(self) -> None:
        """Remove the stand-in home."""
        shutil.rmtree(self.home, ignore_errors=True)
