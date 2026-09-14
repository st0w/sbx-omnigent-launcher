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
import inspect
from types import ModuleType
from typing import Any, Protocol

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

#: Sentinel distinguishing "no default given" from ``default=None``,
#: which is a legitimate value for an optional module.
_UNSET = object()


def _first_attr(
    modules: tuple[str, ...],
    name: str,
    described_as: str,
    *,
    default: Any = _UNSET,
) -> Any:
    """
    Return *name* from the first module in *modules* that has it.

    A module that will not import is skipped, not fatal: on either
    layout one of the candidates is genuinely absent, and that is the
    normal case rather than an error.

    :param modules: Candidate module paths, most-current first.
    :param name: The attribute to resolve, e.g.
        ``"model_family_mismatch"``.
    :param described_as: Human phrase for the failure message, e.g.
        ``"the model-family guard"``.
    :param default: Returned instead of raising when *name* is absent
        everywhere. Omit to fail loud.
    :returns: The resolved attribute, or *default*.
    :raises ImportError: When no candidate provides *name* and no
        *default* was given. The message names every path tried, so
        the next Omnigent move is diagnosable from the traceback
        alone.
    """
    for module_path in modules:
        try:
            module = importlib.import_module(module_path)
        except ImportError:
            continue
        found = getattr(module, name, _UNSET)
        if found is not _UNSET:
            return found
    if default is not _UNSET:
        return default
    tried = ', '.join(modules)
    raise ImportError(
        f'this Omnigent provides {described_as} ({name}) at none of: '
        f'{tried}. It likely moved again — add the new path to '
        f'sbx_omnigent/_compat.py.'
    )


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
    factory = _first_attr(
        _REPO_WORKSPACE_MODULES,
        'RepoWorkspace',
        'the repository-workspace record',
    )
    return factory(url=url, branch=branch, repo_name=repo_name)


def base_start_host_takes_repos() -> bool:
    """
    Whether the installed ``start_host`` takes ``repos=``.

    Omnigent replaced ``start_host``'s ``repo_url``/``repo_branch``/
    ``repo_name`` keywords with a single ``repos`` sequence, so a
    subclass that delegates upward must send the shape the INSTALLED
    base declares. Read off the signature rather than guessed from a
    version string: the signature is the thing that actually has to
    match, and probing it costs one introspection per VM launch.

    :returns: ``True`` for the multi-repo signature, ``False`` for
        the legacy per-field one.
    """
    from omnigent.onboarding.sandboxes.base import (  # noqa: PLC0415
        ExecModelHostLauncher,
    )

    parameters = inspect.signature(
        ExecModelHostLauncher.start_host
    ).parameters
    return 'repos' in parameters


def load_agy_bridge() -> ModuleType | None:
    """
    Import the agy native-bridge module, wherever it now lives.

    :returns: The bridge module, or ``None`` when this Omnigent has
        no agy bridge at all (it is an optional harness, so absence
        is not an error).
    """
    for module_path in AGY_BRIDGE_MODULES:
        try:
            return importlib.import_module(module_path)
        except ImportError:
            continue
    return None


def agy_bridge_module_name() -> str | None:
    """
    The importable module path of the agy bridge on this install.

    :returns: The live path from :data:`AGY_BRIDGE_MODULES`, or
        ``None`` when neither imports.
    """
    module = load_agy_bridge()
    return None if module is None else module.__name__


#: Rejection reason when a model's family cannot run on a harness, or
#: ``None`` when the pairing is fine. Used by the pipeline's
#: model-pinning guard.
model_family_mismatch = _first_attr(
    _MODEL_OVERRIDE_MODULES,
    'model_family_mismatch',
    'the model-family guard',
)

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
CODEX_EFFORTS = _first_attr(
    _REASONING_EFFORT_MODULES,
    'CODEX_EFFORTS',
    'the codex effort ladder',
)
