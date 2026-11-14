"""Host Claude Code plugins, staged for a pipeline image.

A pipeline's ``image.claude_plugins`` names plugins installed on the
host (``<name>@<marketplace>``). The VMs can't reach the marketplaces'
hosts, so each plugin is copied from the host's own install into a local
marketplace directory, which the image build copies into its sandbox and
installs from with ``claude plugin marketplace add`` and ``claude plugin
install``. Only what the user already installed and trusts on the host
is copied, and nothing is downloaded.

This module only reads the host and writes a staging directory; the
build itself is in :mod:`sbx_omnigent.image`.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

#: The local marketplace's name inside the VM; a plugin installed from
#: it is ``<name>@omni-sbx-plugins``, and its commands keep their names
#: (``/claude-security:claude-security``).
STAGED_MARKETPLACE = 'omni-sbx-plugins'

#: A plugin id as Claude Code prints it: ``<name>@<marketplace>``.
_ID_RE = re.compile(r'([a-z0-9][a-z0-9._-]*)@([a-z0-9][a-z0-9._-]*)')

#: Directories Claude Code keeps beside a plugin's files that are not
#: part of the plugin (``.in_use`` holds one entry per running session).
_SKIP_DIRS = frozenset({'.in_use'})


class PluginError(Exception):
    """A plugin cannot be found, or is not safe to copy."""


@dataclass(frozen=True)
class InstalledPlugin:
    """
    One plugin as installed on the host.

    :param plugin_id: ``<name>@<marketplace>``.
    :param name: The plugin's name.
    :param version: The version Claude Code recorded at install.
    :param path: Its install directory, resolved.
    """

    plugin_id: str
    name: str
    version: str
    path: Path


def validate_plugin_id(value: object) -> str:
    """
    Return *value* if it is a plugin id, else raise.

    :param value: e.g. ``"claude-security@claude-plugins-official"``.
    :returns: *value*, unchanged.
    :raises ValueError: If it is not ``<name>@<marketplace>`` in
        lowercase letters, digits, ``.``, ``_`` and ``-``.
    """
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise ValueError(
            f'a Claude Code plugin must be named <name>@<marketplace>, '
            f'like claude-security@claude-plugins-official; got {value!r}'
        )
    return value


def resolve(plugin_id: str, *, home: Path | None = None) -> InstalledPlugin:
    """
    Find a plugin's install on the host.

    :param plugin_id: ``<name>@<marketplace>``.
    :param home: The home directory to read; the user's when ``None``.
    :returns: The installed plugin.
    :raises PluginError: If it is not installed, its install sits
        outside ``~/.claude/plugins``, is missing, or holds a symlink.
    """
    validate_plugin_id(plugin_id)
    plugins_dir = (home or Path.home()) / '.claude' / 'plugins'
    registry = plugins_dir / 'installed_plugins.json'
    try:
        raw = json.loads(registry.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise PluginError(
            f'cannot read the installed Claude Code plugins ({registry}): '
            f'{exc}'
        ) from exc
    entries = (raw.get('plugins') or {}).get(plugin_id) if isinstance(
        raw, dict
    ) else None
    entry = entries[0] if isinstance(entries, list) and entries else None
    if not isinstance(entry, dict) or not isinstance(
        entry.get('installPath'), str
    ):
        raise PluginError(
            f'the Claude Code plugin {plugin_id} is not installed on this '
            f'host. Install it with: claude plugin install {plugin_id}'
        )
    root = plugins_dir.resolve()
    path = Path(entry['installPath']).resolve()
    if root not in path.parents or not path.is_dir():
        raise PluginError(
            f'the Claude Code plugin {plugin_id} is installed at {path}, '
            f'outside {root} or missing; it is not copied into the image.'
        )
    _refuse_symlinks(plugin_id, path)
    version = entry.get('version')
    return InstalledPlugin(
        plugin_id=plugin_id,
        name=plugin_id.split('@', 1)[0],
        version=version if isinstance(version, str) else '',
        path=path,
    )


def digest(plugins: Sequence[InstalledPlugin]) -> str:
    """
    A fingerprint of *plugins*: their ids, versions and files.

    :param plugins: The plugins an image installs.
    :returns: A hex SHA-256; it changes when any plugin's content or
        version does, and not when Claude Code's ``.in_use`` markers
        do.
    """
    sha = hashlib.sha256()
    for plugin in plugins:
        sha.update(f'{plugin.plugin_id}\n{plugin.version}\n'.encode())
        for rel in _files(plugin.path):
            sha.update(f'{rel}\n'.encode())
            sha.update((plugin.path / rel).read_bytes())
    return sha.hexdigest()


def staged_id(plugin: InstalledPlugin) -> str:
    """The id *plugin* has once installed from the staged market."""
    return f'{plugin.name}@{STAGED_MARKETPLACE}'


def stage_marketplace(
    plugins: Sequence[InstalledPlugin], dest: Path
) -> Path:
    """
    Write a local marketplace holding copies of *plugins*.

    :param plugins: The plugins to stage.
    :param dest: An existing, empty directory.
    :returns: The marketplace directory, ``dest/omni-sbx-plugins``.
    """
    root = dest / STAGED_MARKETPLACE
    (root / '.claude-plugin').mkdir(parents=True)
    listed = []
    for plugin in plugins:
        target = root / 'plugins' / plugin.name
        for rel in _files(plugin.path):
            (target / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(plugin.path / rel, target / rel)
        listed.append({
            'name': plugin.name,
            'source': f'./plugins/{plugin.name}',
            'version': plugin.version,
        })
    manifest = {
        'name': STAGED_MARKETPLACE,
        'owner': {'name': 'omni-sbx'},
        'plugins': listed,
    }
    (root / '.claude-plugin' / 'marketplace.json').write_text(
        json.dumps(manifest, indent=2), encoding='utf-8'
    )
    return root


def _files(path: Path) -> list[str]:
    """Every file under *path*, relative and sorted, skipping
    :data:`_SKIP_DIRS`."""
    out = []
    for dirpath, dirnames, filenames in os.walk(path):
        dirnames[:] = sorted(d for d in dirnames if d not in _SKIP_DIRS)
        for name in filenames:
            out.append(
                str((Path(dirpath) / name).relative_to(path).as_posix())
            )
    return sorted(out)


def _refuse_symlinks(plugin_id: str, path: Path) -> None:
    """Refuse a plugin holding a symlink, which could point anywhere."""
    for dirpath, dirnames, filenames in os.walk(path):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
        for name in dirnames + filenames:
            if (Path(dirpath) / name).is_symlink():
                raise PluginError(
                    f'the Claude Code plugin {plugin_id} holds a symlink '
                    f'({Path(dirpath) / name}); it is not copied into the '
                    f'image.'
                )
