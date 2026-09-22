"""Names whose Omnigent import path depends on the release installed.

Omnigent's ``refactor: group top-level omnigent modules into
subpackages (140 -> 45)`` moved three modules this package imports,
and left NO alias at the old paths -- the package's lazy re-exports
cover public names, not submodules, so ``import
omnigent.reasoning_effort`` is a hard ``ModuleNotFoundError`` on the
new layout. A plain import therefore pins this launcher to one side
of that refactor.

Every straddled name is resolved here instead, against whichever
layout is installed, so one launcher build serves both and an
Omnigent rollback needs no rollback here. Two of the three failures
would otherwise be loud at import (taking ``omni-sbx server`` with
them); the third is swallowed by an ``except ImportError`` and would
go silent, which is worse.

When the supported floor rises past the regroup, drop the old path
from each tuple below -- the call sites do not change. See
``docs/OMNIGENT-COMPATIBILITY.md``.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from types import ModuleType
from typing import Protocol, runtime_checkable

__all__ = [
    'AGY_BRIDGE_MODULES',
    'CODEX_EFFORTS',
    'RepoWorkspaceLike',
    'agy_bridge_module_name',
    'load_agy_bridge',
    'model_family_mismatch',
    'repo_workspace',
]

#: ``model_family_mismatch`` -- new path first, legacy second.
_MODEL_OVERRIDE_MODULES = (
    'omnigent.models.model_override',
    'omnigent.model_override',
)

#: The reasoning-effort ladders.
_REASONING_EFFORT_MODULES = (
    'omnigent.util.reasoning_effort',
    'omnigent.reasoning_effort',
)

#: The agy (Antigravity) native bridge. Public because callers probe
#: which name is live -- the in-VM patch script needs both spellings.
AGY_BRIDGE_MODULES = (
    'omnigent.harnesses.antigravity_native.bridge',
    'omnigent.antigravity_native_bridge',
)

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
    either Omnigent layout one of them is genuinely absent.

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
    """

    url: str
    branch: str | None
    repo_name: str


def repo_workspace(
    url: str, branch: str | None, repo_name: str
) -> RepoWorkspaceLike:
    """
    Build this Omnigent's ``RepoWorkspace`` record for one repository.

    Resolved on call, not at import: on the pre-regroup layout the
    only source is ``omnigent.server.managed_hosts``, and pulling the
    server package into every ``_compat`` consumer (the standalone
    pipeline and swarm CLIs included) to satisfy a path taken only
    when a legacy caller meets a current Omnigent would be a poor
    trade.

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
    Import the agy native-bridge module, wherever it now lives.

    :returns: The bridge module, or ``None`` when this Omnigent has
        no agy bridge at all (it is an optional harness, so absence
        is not an error).
    """
    try:
        return importlib.import_module(
            'omnigent.harnesses.antigravity_native.bridge'
        )
    except ImportError:
        pass
    try:
        return importlib.import_module('omnigent.antigravity_native_bridge')
    except ImportError:
        return None


def agy_bridge_module_name() -> str | None:
    """
    The importable module path of the agy bridge on this install.

    :returns: The live path from :data:`AGY_BRIDGE_MODULES`, or
        ``None`` when neither imports.
    """
    module = load_agy_bridge()
    return None if module is None else module.__name__



def _model_family_mismatch() -> Callable[[str, str], str | None]:
    """
    Omnigent's check that a model's family can run on a harness.

    :returns: The check, which returns a rejection reason or ``None``.
    :raises ImportError: When neither layout provides it.
    :raises TypeError: When what it provides is not callable.
    """
    found = _resolve(
        'model_family_mismatch',
        'the model-family guard',
        _MODEL_OVERRIDE_MODULES,
        (
            lambda: importlib.import_module('omnigent.models.model_override'),
            lambda: importlib.import_module('omnigent.model_override'),
        ),
    )
    if not callable(found):
        raise TypeError(f'model_family_mismatch is not callable: {found!r}')
    return found


def _codex_efforts() -> frozenset[str]:
    """
    Omnigent's codex reasoning-effort ladder.

    :returns: The effort names.
    :raises ImportError: When neither layout provides it.
    :raises TypeError: When what it provides is not a set of strings.
    """
    found = _resolve(
        'CODEX_EFFORTS',
        'the codex effort ladder',
        _REASONING_EFFORT_MODULES,
        (
            lambda: importlib.import_module('omnigent.util.reasoning_effort'),
            lambda: importlib.import_module('omnigent.reasoning_effort'),
        ),
    )
    if not isinstance(found, (set, frozenset)) or not all(
        isinstance(effort, str) for effort in found
    ):
        raise TypeError(f'CODEX_EFFORTS is not a set of names: {found!r}')
    return frozenset(found)


#: Rejection reason when a model's family cannot run on a harness, or
#: ``None`` when the pairing is fine. Used by the pipeline's
#: model-pinning guard.
model_family_mismatch = _model_family_mismatch()

#: Reasoning-effort levels codex accepts, used to gate what may be
#: interpolated into a ``-c model_reasoning_effort=...`` expression.
#:
#: Deliberately the SDK/Responses ladder on BOTH layouts, even though
#: newer Omnigent also publishes a wider ``CODEX_NATIVE_EFFORTS``
#: (reaching ``max``/``ultra``) for the native CLI this launcher
#: actually drives. Widening the gate is a live behaviour change in
#: the harness layer — the one place this project has repeatedly lost
#: days to (see docs/HARNESS-VERSIONS.md) — so it is a decision of its
#: own, not a side effect of an import move.
CODEX_EFFORTS = _codex_efforts()
