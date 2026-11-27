"""Keep the pipeline runner's tests off this machine's real state.

Each stand-in replaces a read of something on whatever machine runs
the suite. Every one was added after a test came to depend on that
machine: it passed where the thing happened to exist and failed
elsewhere.
"""

from __future__ import annotations

import functools
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

from sbx_omnigent import (
    agy,
    claude_plugins,
    codex,
    guest_log,
    harness_versions,
    orphans,
)
from tests.support.claude_plugins import PluginHome


@dataclass(frozen=True)
class HostStandIns:
    """
    The stand-ins a test may script or inspect.

    :param read_versions: For :func:`harness_versions.read_versions`.
    :param probe_login: For :func:`codex.probe_login`.
    :param runner_log: For :func:`guest_log.read_runner_log`.
    :param orphan_advice: For :func:`orphans.orphan_advice`.
    :param plugins: The home Claude Code plugins are read from.
    """

    read_versions: mock.MagicMock
    probe_login: mock.MagicMock
    runner_log: mock.MagicMock
    orphan_advice: mock.MagicMock
    plugins: PluginHome


def isolate_from_host(case: unittest.TestCase, root: Path) -> HostStandIns:
    """
    Replace every read of this machine for the length of *case*.

    :param case: The test; each replacement is undone at its cleanup.
    :param root: The test's own scratch directory.
    :returns: The stand-ins.
    """
    # Never touch the host's real harvester lock: a harvester running
    # on this machine would otherwise make a CLI test block for the
    # full first-refresh deadline.
    case.enterContext(
        mock.patch.object(agy, 'HARVEST_LOCK', root / 'agy-harvest.lock')
    )
    # Never `sbx exec` into a real VM. A test that resolves a sandbox
    # name would otherwise read the versions out of whatever box on
    # this machine answers to it (#17).
    read_versions = mock.MagicMock(return_value={})
    case.enterContext(
        mock.patch.object(harness_versions, 'read_versions', read_versions)
    )
    # Never run the real `codex doctor` on this host.
    probe_login = mock.MagicMock(return_value=None)
    case.enterContext(mock.patch.object(codex, 'probe_login', probe_login))
    # Never `sbx exec` a real VM for its runner log.
    runner_log = mock.MagicMock(return_value=None)
    case.enterContext(
        mock.patch.object(guest_log, 'read_runner_log', runner_log)
    )
    # Never read this host's snapshot store or run its `sbx ls`. A disk
    # refusal names leaked guest disks, and without this the six tests
    # that refuse asked the real sbx on whatever machine ran the suite,
    # and depended on its store.
    orphan_advice = mock.MagicMock(return_value=None)
    case.enterContext(
        mock.patch.object(orphans, 'orphan_advice', orphan_advice)
    )
    # Never read this host's Claude Code plugins. A pipeline whose
    # image copies one in fingerprints the installed copy, so a gated
    # test passed only where the plugin happened to be installed.
    plugins = PluginHome()
    case.addCleanup(plugins.cleanup)
    case.enterContext(mock.patch.object(
        claude_plugins, 'resolve',
        functools.partial(claude_plugins.resolve, home=plugins.home),
    ))
    return HostStandIns(
        read_versions=read_versions,
        probe_login=probe_login,
        runner_log=runner_log,
        orphan_advice=orphan_advice,
        plugins=plugins,
    )
