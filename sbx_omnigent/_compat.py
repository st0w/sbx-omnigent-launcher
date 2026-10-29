"""Omnigent names that have moved, imported from one place.

Omnigent's ``refactor: group top-level omnigent modules into
subpackages (140 -> 45)`` moved three modules this package imports,
and left no alias at the old paths. This module once resolved each
against whichever layout was installed. The oldest release the
launcher supports, v0.13.0, is past that regroup, so they are now
plain imports from their one home (#56); a future move is fixed here,
and the call sites do not change.

``RepoWorkspace`` still straddles: v0.13.0 keeps it in
``omnigent.server.managed_hosts``, later releases in the sandbox types
module, so :func:`repo_workspace` still tries both. See
``docs/OMNIGENT-COMPATIBILITY.md``.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from types import ModuleType
from typing import Protocol, runtime_checkable

from omnigent.models.model_override import model_family_mismatch
from omnigent.util.reasoning_effort import (
    CODEX_EFFORTS as _OMNIGENT_CODEX_EFFORTS,
)

__all__ = [
    'AGY_BRIDGE_MODULE',
    'CODEX_EFFORTS',
    'RepoWorkspaceLike',
    'agy_bridge_module_name',
    'load_agy_bridge',
    'model_family_mismatch',
    'repo_workspace',
]

#: The agy (Antigravity) native bridge. Public because the in-VM patch
#: script imports it by name, inside a VM this process cannot probe.
AGY_BRIDGE_MODULE = 'omnigent.harnesses.antigravity_native.bridge'

#: ``RepoWorkspace`` -- the per-repo record Omnigent's newer
#: ``start_host`` takes a sequence of. It moved OUT of the server
#: package and into the sandbox types module, deliberately, so a
#: launcher can accept one without importing ``omnigent.server``.
#: Resolved lazily (see :func:`repo_workspace`) precisely to keep
#: that property: on the old layout the only source is the server
#: package, and ``omni-sbx-pipeline`` must not pay for importing it.
_REPO_WORKSPACE_MODULES = (
    'omnigent.onboarding.sandboxes.types',
    'omnigent.server.managed_hosts',
)

def _moved(described_as: str, name: str, paths: tuple[str, ...]) -> str:
    """
    The message for a name found at none of its known paths.

    :param described_as: Human phrase, e.g.
        ``"the repository-workspace record"``.
    :param name: The attribute looked for.
    :param paths: Every path tried, most-current first.
    :returns: The message, naming every path so the next Omnigent move
        is diagnosable from the traceback alone.
    """
    return (
        f'this Omnigent provides {described_as} ({name}) at none of: '
        f'{", ".join(paths)}. It likely moved again — add the new path '
        f'to sbx_omnigent/_compat.py.'
    )


def _resolve(
    name: str,
    described_as: str,
    paths: tuple[str, str],
    loaders: tuple[Callable[[], ModuleType], Callable[[], ModuleType]],
) -> object:
    """
    Return *name* from the first candidate module that defines it.

    Each loader imports one fixed module path. A candidate that does
    not import, or imports without defining *name*, is skipped: on
    each supported Omnigent release one of them is genuinely absent.

    :param name: The attribute to resolve.
    :param described_as: Human phrase for the failure message.
    :param paths: The two module paths, in the order *loaders* import
        them, for the message.
    :param loaders: One loader per path, most-current first.
    :returns: The attribute.
    :raises ImportError: When no candidate defines it.
    """
    for load in loaders:
        try:
            module = load()
        except ImportError:
            continue
        found = getattr(module, name, None)
        if found is not None:
            return found
    raise ImportError(_moved(described_as, name, paths))


@runtime_checkable
class RepoWorkspaceLike(Protocol):
    """
    The three fields every Omnigent ``RepoWorkspace`` carries.

    Named as a protocol rather than imported as a class because the
    class lives at a different path per release, and this package
    only ever reads these fields off it.

    Read-only, because Omnigent's record is a frozen dataclass: plain
    attributes would ask for settable fields, which a frozen record
    does not satisfy, and ``SbxLauncher.start_host`` would then accept
    less than the base class it overrides.
    """

    @property
    def url(self) -> str:
        """The clone URL, with any ``#<branch>`` fragment stripped."""
        ...

    @property
    def branch(self) -> str | None:
        """The branch to clone, or ``None`` for the default."""
        ...

    @property
    def repo_name(self) -> str:
        """The directory the clone lands in."""
        ...


def repo_workspace(
    url: str, branch: str | None, repo_name: str
) -> RepoWorkspaceLike:
    """
    Build this Omnigent's ``RepoWorkspace`` record for one repository.

    Resolved on call, not at import: on v0.13.0 the only source is
    ``omnigent.server.managed_hosts``, and pulling the server package
    into every ``_compat`` consumer (the standalone pipeline and swarm
    CLIs included) to satisfy a path taken only when a legacy caller
    meets a current Omnigent would be a poor trade.

    :param url: Clone URL with any ``#<branch>`` fragment stripped.
    :param branch: Branch to clone, or ``None`` for the default.
    :param repo_name: Directory the clone lands in.
    :returns: The record, typed by the fields this package reads.
    :raises ImportError: When no candidate module defines it.
    """
    factory = _resolve(
        'RepoWorkspace',
        'the repository-workspace record',
        _REPO_WORKSPACE_MODULES,
        (
            lambda: importlib.import_module(
                'omnigent.onboarding.sandboxes.types'
            ),
            lambda: importlib.import_module('omnigent.server.managed_hosts'),
        ),
    )
    if not callable(factory):
        raise TypeError(f'RepoWorkspace is not a class: {factory!r}')
    record = factory(url=url, branch=branch, repo_name=repo_name)
    if not isinstance(record, RepoWorkspaceLike):
        raise TypeError(f'RepoWorkspace built {record!r}')
    return record


def load_agy_bridge() -> ModuleType | None:
    """
    Import the agy native-bridge module.

    :returns: The bridge module, or ``None`` when this Omnigent has
        no agy bridge at all (it is an optional harness, so absence
        is not an error).
    """
    try:
        return importlib.import_module(AGY_BRIDGE_MODULE)
    except ImportError:
        return None


def agy_bridge_module_name() -> str | None:
    """
    The importable module path of the agy bridge on this install.

    :returns: :data:`AGY_BRIDGE_MODULE` when it imports, else ``None``.
    """
    module = load_agy_bridge()
    return None if module is None else module.__name__


#: Reasoning-effort levels codex accepts, used to gate what may be
#: interpolated into a ``-c model_reasoning_effort=...`` expression.
#:
#: Deliberately the SDK/Responses ladder, even though newer Omnigent
#: also publishes a wider ``CODEX_NATIVE_EFFORTS`` (reaching
#: ``max``/``ultra``) for the native CLI this launcher actually drives.
#: Widening the gate is a live behaviour change in the harness layer —
#: the one place this project has repeatedly lost days to (see
#: docs/HARNESS-VERSIONS.md) — so it is a decision of its own, not a
#: side effect of an import move.
CODEX_EFFORTS = frozenset(_OMNIGENT_CODEX_EFFORTS)
