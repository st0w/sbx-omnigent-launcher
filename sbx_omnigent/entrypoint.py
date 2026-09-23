"""Register the ``sbx`` provider, then run Omnigent's CLI.

Omnigent's stock server resolves its managed-sandbox provider through
:func:`omnigent.server.managed_hosts.parse_sandbox_config`. Omnigent
does load community providers from a registry, but only from modules
under ``omnigent.community.sandbox.*``, which a separately installed
package cannot add to. Rather than patch that source (which would
conflict on every ``git pull``), this entrypoint wraps the function at
process startup: ``provider: sbx`` is handled here, and every other
provider is delegated to the original implementation unchanged.

The wrapper builds the sbx deployment from the sbx block alone, so the
settings Omnigent parses outside a provider entry, ``sandbox.reaper``
and a ``providers:`` list, are refused beside ``sbx`` rather than
silently dropped (#54).

The wrap lands before the server command's function-local ``from
omnigent.server.managed_hosts import parse_sandbox_config`` executes,
so the command picks up the patched version. The only coupling to
Omnigent internals is three stable symbols — ``parse_sandbox_config``,
``ManagedSandboxConfig`` and ``ManagedSandboxDeployment``. If a
future release renames any of them,
this fails loudly at startup (pointing right here) instead of
breaking silently.

Run ``omni-sbx server ...`` exactly as you would ``omni server ...``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import omnigent.server.managed_hosts as managed_hosts
from omnigent.cli import cli
from omnigent.onboarding.provider_config import load_providers
from omnigent.server.managed_hosts import (
    ManagedSandboxConfig,
    ManagedSandboxDeployment,
    _parse_host_config,
)

from sbx_omnigent import pipeline
from sbx_omnigent._compat import load_agy_bridge
from sbx_omnigent.defaults import (
    DEFAULT_EGRESS_ALLOW,
    DEFAULT_HOST_IMAGE,
    SBX_BUNDLE_GAP_HOSTS,
)
from sbx_omnigent.launcher import DEFAULT_PROVISION_STAGGER_S, SbxLauncher

#: sbx sandboxes persist (create/stop with no platform lifetime cap),
#: so keep the launch token comfortably long-lived; it must outlive
#: the sandbox so a reconnecting host can always re-authenticate.
_TOKEN_TTL_S = 25 * 60 * 60  # 25 hours

#: Omnigent's env var naming extra built-in agent bundle dirs to seed at
#: server startup (os.pathsep-separated; each entry an agent directory).
#: We APPEND our packaged swarm agents to it — never overwrite.
_BUILTIN_AGENT_DIRS_ENV = 'OMNIGENT_BUILTIN_AGENT_DIRS'

#: Opt-out: any non-empty value skips auto-registering the bundled swarm
#: agents (for users who want only the microVM provider).
_NO_SWARM_AGENTS_ENV = 'OMNI_SBX_NO_SWARM_AGENTS'

#: Where a ``--pipeline`` config materializes its per-agent bundle dirs.
#: Under the user's home so the server can read them for its process
#: lifetime; replaced in place on each run of a given pipeline.
_PIPELINE_AGENTS_ROOT = Path.home() / '.sbx-swarm' / 'pipeline-agents'


def _as_int(value: object, field: str) -> int | None:
    """
    Coerce an optional YAML scalar to a positive int.

    :param value: Raw config value (may be ``None``).
    :param field: Dotted config key for error messages, e.g.
        ``"sbx.cpus"``.
    :returns: The int, or ``None`` when *value* is ``None``.
    :raises ValueError: If *value* is present but not a positive int.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(
            f"server config 'sandbox.{field}' must be a positive integer"
        )
    return value


def _as_stagger(value: object) -> float:
    """
    Coerce the optional ``sbx.provision_stagger_s`` to a non-negative
    float, defaulting to :data:`DEFAULT_PROVISION_STAGGER_S`.

    :param value: Raw config value (may be ``None`` when unset).
    :returns: The gap in seconds — the default when *value* is ``None``.
    :raises ValueError: If present but not a non-negative number.
    """
    if value is None:
        return DEFAULT_PROVISION_STAGGER_S
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            "server config 'sandbox.sbx.provision_stagger_s' must be a "
            'non-negative number of seconds'
        )
    if value < 0:
        raise ValueError(
            "server config 'sandbox.sbx.provision_stagger_s' must be a "
            'non-negative number of seconds'
        )
    return float(value)


def _as_egress_allow(value: object) -> tuple[str, ...]:
    """
    Resolve ``sbx.egress_allow`` to the per-VM allowlist hosts.

    Three-way knob (the launcher always adds the dial-back on top):

    - **unset** (``None``) → the curated default baseline
      (:data:`DEFAULT_EGRESS_ALLOW` — LLM endpoints + trusted package
      registries) so agents work out of the box.
    - **empty list** (``[]``) → ``()`` = **dial-back only**: block
      everything except the mandatory server connection (maximal
      lockdown).
    - **non-empty list** → those hosts (a stricter/custom set that
      REPLACES the default).

    :param value: Raw config value.
    :returns: The resolved allowlist hosts (may be empty for lockdown).
    :raises ValueError: If present but not a list of non-empty strings.
    """
    if value is None:
        return DEFAULT_EGRESS_ALLOW
    if value == []:
        return ()
    if not isinstance(value, list) or not all(
        isinstance(s, str) and s for s in value
    ):
        raise ValueError(
            "server config 'sandbox.sbx.egress_allow' must be a list of "
            'non-empty host strings'
        )
    return tuple(value)


def warn_on_dropped_gap_hosts(value: object) -> None:
    """
    Warn when an explicit allowlist omits a host sbx's bundles miss.

    A non-empty ``sbx.egress_allow`` REPLACES the default, including
    the entries that only exist to cover gaps in sbx's own bundles
    (:data:`SBX_BUNDLE_GAP_HOSTS`). Dropping one fails inside the guest
    with no signal at startup (#40). Advises only: it never adds a host,
    and ``[]`` (deliberate lockdown) and unset (the default) are silent.

    :param value: The raw ``sbx.egress_allow`` value, already validated.
    """
    if not isinstance(value, list) or not value:
        return
    missing = [host for host in SBX_BUNDLE_GAP_HOSTS if host not in value]
    if not missing:
        return
    lines = '\n'.join(
        f'    {host}: without it, {SBX_BUNDLE_GAP_HOSTS[host]}.'
        for host in missing
    )
    print(
        "[sbx-omnigent] NOTE: 'sandbox.sbx.egress_allow' replaces the "
        "default allowlist and omits host(s) that cover gaps in sbx's "
        'own bundles:\n'
        f'{lines}\n'
        '  Add them to the list if your agents need them. Nothing was '
        'added for you.',
        file=sys.stderr,
    )


def warn_on_inert_providers(host_config: dict[str, object] | None) -> None:
    """
    Warn about ``providers`` entries that are the default for nothing.

    An entry with no ``default:`` passes validation and is written into
    every guest, but Omnigent's launch routing only ever picks a
    provider that is the default for a model family. So the entry is
    ignored, and the symptom surfaces inside a VM, typically as a
    codex-native agent whose app-server never starts a thread (#34).

    Warns rather than refuses: a non-default entry beside a default one
    is legitimate, and a false refusal here would block every session.

    :param host_config: The validated ``sandbox.host_config`` mapping.
    """
    if not host_config or not isinstance(
        host_config.get('providers'), dict
    ):
        return
    inert = sorted(
        provider.name
        for provider in load_providers(host_config).values()
        if not provider.default_families
    )
    for name in inert:
        print(
            f"[sbx-omnigent] NOTE: 'sandbox.host_config.providers.{name}' "
            'is not the default for any model family, so Omnigent never '
            'routes a harness to it. Set `default: true`, or name the '
            'family it serves (e.g. `default: openai`).',
            file=sys.stderr,
        )


def warn_on_global_allow_rules() -> None:
    """
    Warn at server startup if broad global sbx allow rules exist.

    Global allow rules apply to EVERY sandbox — including swarm agent
    VMs — and are additive, so a per-sandbox allowlist cannot restrict
    below them (only an explicit per-sandbox deny can). This advises the
    operator to review them and offers the opt-in, destructive command
    to adopt a strict deny-all baseline. Best-effort: never blocks
    startup, and never changes any policy itself.
    """
    try:
        proc = subprocess.run(
            ['sbx', 'policy', 'ls', '--type', 'network'],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return
    if proc.returncode != 0:
        return
    # Heuristic: count global (APPLIES_TO 'all') allow rule lines.
    n = sum(
        1
        for line in proc.stdout.splitlines()
        if 'allow' in line
        and 'local' in line
        and _has_word(line, 'all')
    )
    if n == 0:
        return
    print(
        f'[sbx-omnigent] NOTE: {n} global sbx network allow rule(s) grant '
        'broad access to EVERY sandbox — including swarm agent VMs — and '
        'are additive, so a per-sandbox allowlist cannot restrict below '
        'them.\n'
        '  Review them:  sbx policy ls --type network\n'
        '  For a strict deny-all baseline (recommended for secure '
        'swarms):\n'
        '      sbx policy reset && sbx policy init deny-all\n'
        '  WARNING: that wipes ALL your sbx network rules and affects '
        'every sbx sandbox you have. This launcher re-scopes swarm VMs '
        'automatically; re-scope any other sandboxes you use yourself.',
        file=sys.stderr,
    )


def _has_word(line: str, word: str) -> bool:
    """Return whether *word* appears as a whitespace-delimited token."""
    return word in line.split()


def _as_bool(value: object, field: str) -> bool:
    """
    Coerce an optional YAML boolean, defaulting to ``False``.

    :param value: Raw config value (``None`` → ``False``).
    :param field: Dotted config key for error messages.
    :returns: The boolean.
    :raises ValueError: If present but not a bool.
    """
    if value is None:
        return False
    if not isinstance(value, bool):
        raise ValueError(
            f"server config 'sandbox.{field}' must be a boolean"
        )
    return value


def _as_opt_str(value: object, field: str) -> str | None:
    """
    Coerce an optional non-empty string config value.

    :param value: Raw config value (``None`` → ``None``).
    :param field: Dotted config key for error messages.
    :returns: The stripped string, or ``None`` when unset.
    :raises ValueError: If present but not a non-empty string.
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"server config 'sandbox.{field}' must be a non-empty string"
        )
    return value.strip()


def install_agy_enterprise_onboarding_patch() -> None:
    """
    Mark agy's ENTERPRISE onboarding complete for managed agy agents.

    Omnigent seeds ``enterpriseOnboardingComplete: False`` — it assumes
    a consumer account and does not drive enterprise onboarding. For an
    **enterprise / Business** Google account, agy then re-runs the
    enterprise onboarding (theme picker + EULA) on every fresh launch,
    which a headless managed microVM cannot answer — so the
    ``antigravity-native`` turn stalls before the prompt. Flip the
    seeded value to ``True`` at runtime (no Omnigent source change,
    mirroring :func:`install_sbx_provider`), reflecting that the
    account's enterprise onboarding is genuinely already done.

    Best-effort and increasingly redundant: Omnigent releases after
    the subpackage regroup seed the marker themselves (the isolated
    ``--gemini_dir`` seeder writes ``enterpriseOnboardingComplete:
    true`` in its synthetic fallback, and prefers the real-home
    marker this launcher already writes with the account's true
    value). Flipping the constant there is harmless — nothing on the
    live launch path reads it any more — so the call stays rather
    than becoming a version check. A no-op when the bridge module or
    the seed constant is absent, so it never breaks startup.
    """
    bridge = load_agy_bridge()
    if bridge is None:
        return
    state = getattr(bridge, '_AGY_ONBOARDING_COMPLETE_STATE', None)
    if isinstance(state, dict):
        state['enterpriseOnboardingComplete'] = True


def _parse_worktree_root(sbx: dict[str, Any]) -> str | None:
    """
    Extract and validate the optional ``sbx.worktree_root`` path.

    :param sbx: The parsed ``sandbox.sbx`` mapping.
    :returns: The cleaned absolute path, or ``None`` when unset.
    :raises ValueError: If present but not a non-empty absolute
        string (mirrors the fail-loud-at-startup contract).
    """
    value = sbx.get('worktree_root')
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            "server config 'sandbox.sbx.worktree_root' must be a "
            'non-empty string'
        )
    value = value.strip()
    if not os.path.isabs(value):
        raise ValueError(
            "server config 'sandbox.sbx.worktree_root' must be an "
            f'absolute path, got {value!r}'
        )
    return value


def _build_sbx_config(raw: dict[str, Any]) -> ManagedSandboxConfig:
    """
    Build a :class:`ManagedSandboxConfig` for ``provider: sbx``.

    :param raw: The parsed ``sandbox:`` mapping from the server
        config.
    :returns: A managed-sandbox config whose factory yields an
        :class:`SbxLauncher`.
    :raises ValueError: If required keys are missing or malformed
        (mirrors Omnigent's fail-loud-at-startup contract for config
        typos).
    """
    server_url = raw.get('server_url')
    if not isinstance(server_url, str) or not server_url.strip():
        raise ValueError(
            "server config 'sandbox.server_url' is required — the URL "
            'the sandboxed host dials back to. For a LOCAL server this '
            'is the host gateway (e.g. http://host.docker.internal:8000'
            '), NOT localhost.'
        )

    sbx = raw.get('sbx') or {}
    if not isinstance(sbx, dict):
        raise ValueError("server config 'sandbox.sbx' must be a mapping")

    image = sbx.get('image', DEFAULT_HOST_IMAGE)
    if not isinstance(image, str) or not image.strip():
        raise ValueError(
            "server config 'sandbox.sbx.image' must be a non-empty string"
        )

    profile = sbx.get('profile')
    if profile is not None and not isinstance(profile, str):
        raise ValueError(
            "server config 'sandbox.sbx.profile' must be a string"
        )

    memory = sbx.get('memory')
    if memory is not None and not isinstance(memory, str):
        raise ValueError(
            "server config 'sandbox.sbx.memory' must be a string, e.g. '8g'"
        )

    unset_env_raw = sbx.get('unset_env') or []
    if not isinstance(unset_env_raw, list) or not all(
        isinstance(s, str) for s in unset_env_raw
    ):
        raise ValueError(
            "server config 'sandbox.sbx.unset_env' must be a list of strings"
        )

    egress_allow = _as_egress_allow(sbx.get('egress_allow'))
    warn_on_dropped_gap_hosts(sbx.get('egress_allow'))
    worktree_root = _parse_worktree_root(sbx)
    cpus = _as_int(sbx.get('cpus'), 'sbx.cpus')
    provision_stagger_s = _as_stagger(sbx.get('provision_stagger_s'))
    agy_enabled = _as_bool(sbx.get('agy_enabled'), 'sbx.agy_enabled')
    agy_enterprise = _as_bool(sbx.get('agy_enterprise'), 'sbx.agy_enterprise')
    agy_gcp_project = _as_opt_str(
        sbx.get('agy_gcp_project'), 'sbx.agy_gcp_project'
    )
    agy_gcp_location = (
        _as_opt_str(sbx.get('agy_gcp_location'), 'sbx.agy_gcp_location')
        or 'us'
    )
    if agy_enterprise:
        install_agy_enterprise_onboarding_patch()

    host_config = _parse_host_config(raw)
    warn_on_inert_providers(host_config)

    def factory() -> SbxLauncher:
        return SbxLauncher(
            image=image,
            profile=profile,
            cpus=cpus,
            memory=memory,
            unset_env=tuple(unset_env_raw),
            worktree_root=worktree_root,
            provision_stagger_s=provision_stagger_s,
            egress_allow=egress_allow,
            scope_egress=True,
            agy_enabled=agy_enabled,
            agy_enterprise=agy_enterprise,
            agy_gcp_project=agy_gcp_project,
            agy_gcp_location=agy_gcp_location,
        )

    return ManagedSandboxConfig(
        server_url=server_url.strip().rstrip('/'),
        launcher_factory=factory,
        token_ttl_s=_TOKEN_TTL_S,
        managed_launch_supported=True,
        provider='sbx',
        # The verbatim in-sandbox ``~/.omnigent/config.yaml`` the
        # server injects before ``omnigent host`` starts. Upstream's
        # own parser builds this for every other provider; ours
        # constructs the config by hand and simply never read the
        # key, so nothing was ever written and every VM booted with
        # NO ~/.omnigent/config.yaml.
        #
        # That is not cosmetic. It is where the ``providers:`` block
        # lives, and the in-VM harness reads it to decide how a model
        # is reached. Live: a codex-native agent had no
        # ``providers.codex`` entry in its guest, so Omnigent could
        # not see the ChatGPT subscription the launcher had just
        # seeded, fell back to a gateway shape
        # (``-c model_provider="omnigent_provider"``) with no provider
        # definition behind it, and the Codex app-server never started a
        # thread — "startup timed out: TimeoutError", 44 seconds in.
        #
        # Upstream's `_parse_host_config` is reused rather than
        # reimplemented: it validates the providers block through the
        # real onboarding loader, refuses an inline `api_key`, and
        # round-trips the mapping through JSON so a YAML scalar that
        # cannot survive the trip fails at STARTUP rather than on
        # every launch.
        host_config=host_config,
    )


def install_sbx_provider() -> None:
    """
    Monkeypatch :func:`parse_sandbox_config` to handle ``sbx``.

    Idempotent: re-applying leaves a single wrapper in place. Every
    provider other than ``sbx`` is delegated to the original,
    untouched.
    """
    original: Callable[[object], ManagedSandboxDeployment | None] = (
        managed_hosts.parse_sandbox_config
    )
    # Guard against double-wrapping if called more than once.
    if isinstance(original, _SbxParseSandboxConfig):
        return
    managed_hosts.parse_sandbox_config = _SbxParseSandboxConfig(original)


class _SbxParseSandboxConfig:
    """
    ``parse_sandbox_config`` with the ``sbx`` provider added.

    A class rather than a closure so :func:`install_sbx_provider` can
    recognise its own wrapper and install it once.

    :param original: The parser being wrapped, which handles every
        provider other than ``sbx``.
    """

    def __init__(
        self, original: Callable[[object], ManagedSandboxDeployment | None]
    ) -> None:
        self._original = original

    def __call__(self, raw: object) -> ManagedSandboxDeployment | None:
        """
        Parse one ``sandbox:`` block.

        ``parse_sandbox_config`` returns a DEPLOYMENT, not a single
        config. Upstream split the two so a server can offer several
        providers side by side: ``ManagedSandboxConfig`` is one
        provider's entry, and ``ManagedSandboxDeployment`` is the
        collection the server calls ``.recorded(...)`` /
        ``.for_provider(...)`` on. Returning the bare entry raised
        ``'ManagedSandboxConfig' object has no attribute 'recorded'``
        on every session read.

        :param raw: The raw ``sandbox:`` mapping.
        :returns: The deployment, or what the original parser returns.
        :raises ValueError: For a setting beside ``sbx`` that this
            wrapper cannot honour (see :func:`_refuse_unsupported_sbx`).
        """
        if isinstance(raw, dict):
            _refuse_unsupported_sbx(raw)
            if raw.get('provider') == 'sbx':
                return ManagedSandboxDeployment.single(
                    _build_sbx_config(raw)
                )
        return self._original(raw)


def _refuse_unsupported_sbx(raw: dict[str, object]) -> None:
    """
    Refuse the ``sandbox:`` settings the ``sbx`` wrapper cannot honour.

    The wrapper builds an sbx deployment from the sbx block alone, so a
    setting Omnigent parses outside the provider entry never reaches
    it. These were dropped without a word, or refused with a message
    that blamed the wrong thing (#54). Refusing them at startup is the
    contract Omnigent keeps for a config it cannot honour.

    :param raw: The raw ``sandbox:`` mapping.
    :raises ValueError: For ``reaper`` or ``providers`` beside
        ``provider: sbx``, or ``sbx`` inside a ``providers:`` list.
    """
    if raw.get('provider') == 'sbx':
        if 'reaper' in raw:
            raise ValueError(
                "server config 'sandbox.reaper' is not supported with "
                "provider 'sbx': the sbx deployment has no reaper, so "
                "none of its settings would take effect. Remove it."
            )
        if 'providers' in raw:
            raise ValueError(
                "server config 'sandbox' must set either 'provider' or "
                "'providers', not both, and 'providers' cannot include "
                "'sbx'"
            )
        return
    entries = raw.get('providers')
    if isinstance(entries, list) and any(
        isinstance(entry, dict) and entry.get('provider') == 'sbx'
        for entry in entries
    ):
        raise ValueError(
            "server config 'sandbox.providers' cannot include 'sbx': "
            "sbx runs only as the sole provider. Use `provider: sbx` "
            "with an `sbx:` block instead of a list."
        )


def _bundled_agent_dirs() -> list[str]:
    """
    Discover the swarm agent directories shipped with this package.

    Every ``agents/<name>/config.yaml`` under the package root is an
    agent bundle, so a specialist reviewer cloned into ``agents/`` is
    picked up automatically. Resolved relative to this file (no absolute
    paths to configure); works for the editable install the README
    prescribes.

    :returns: Sorted absolute agent-directory paths, or ``[]`` when the
        bundle is absent (e.g. a stripped wheel).
    """
    agents_root = Path(__file__).resolve().parent.parent / 'agents'
    try:
        return sorted(str(p.parent) for p in agents_root.glob('*/config.yaml'))
    except OSError:
        return []


def register_bundled_agents() -> None:
    """
    Auto-register the bundled swarm agents (default on; opt-out).

    Appends the packaged agent dirs (:func:`_bundled_agent_dirs`) to
    Omnigent's :data:`_BUILTIN_AGENT_DIRS_ENV` so ``omni-sbx server``
    ships the coordinator / coder / reviewer without manual setup. The
    append is additive (a user's own entries are preserved) and
    idempotent (no duplicates on re-run). Set
    :data:`_NO_SWARM_AGENTS_ENV` to skip it and run the microVM provider
    only. Best-effort — never raises, so a missing bundle can't block
    ``omni-sbx``.
    """
    if os.environ.get(_NO_SWARM_AGENTS_ENV):
        return
    entries = _bundled_agent_dirs()
    _append_builtin_agent_dirs(entries)


def _append_builtin_agent_dirs(dirs: list[str]) -> None:
    """
    Additively, idempotently append *dirs* to the builtin-agent env.

    :param dirs: Absolute agent-directory paths to register at startup.
    """
    if not dirs:
        return
    existing = [
        part
        for part in os.environ.get(_BUILTIN_AGENT_DIRS_ENV, '').split(
            os.pathsep
        )
        if part
    ]
    merged = existing + [d for d in dirs if d not in existing]
    os.environ[_BUILTIN_AGENT_DIRS_ENV] = os.pathsep.join(merged)


def _extract_pipeline_flag(
    argv: list[str],
) -> tuple[list[str], str | None]:
    """
    Pull ``--pipeline PATH`` (``-P PATH`` / ``--pipeline=PATH``) out.

    The flag is ours, not Omnigent's, so it must be stripped before the
    stock CLI parses the rest.

    :param argv: Process args without ``argv[0]``.
    :returns: ``(cleaned_argv, pipeline_path_or_None)``; last wins.
    :raises SystemExit: When the flag is given with no path argument.
    """
    cleaned: list[str] = []
    pipeline: str | None = None
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok in ('--pipeline', '-P'):
            if i + 1 >= len(argv):
                raise SystemExit(f'{tok} requires a path argument')
            pipeline = argv[i + 1]
            i += 2
            continue
        if tok.startswith('--pipeline='):
            pipeline = tok.split('=', 1)[1]
            i += 1
            continue
        cleaned.append(tok)
        i += 1
    return cleaned, pipeline


def register_pipeline_agents(pipeline_path: str) -> dict[str, str]:
    """
    Materialize + register a pipeline's agents for server startup.

    Loads the ``pipeline.yaml``, writes each declared agent to a
    namespaced bundle dir under :data:`_PIPELINE_AGENTS_ROOT`, and
    appends those dirs to the builtin-agent env so the server registers
    them (deterministic ids) at app creation — the only way a managed,
    model-pinnable pipeline agent reaches the server.

    :param pipeline_path: Path to the pipeline file.
    :returns: ``{agent_name: namespaced_spec_name}``.
    :raises SystemExit: On a load/materialize error (fail loud at
        startup, pointing at the config).
    """
    try:
        config = pipeline.load_pipeline(pipeline_path)
        dest = _PIPELINE_AGENTS_ROOT / config.name
        mapping = pipeline.materialize_agents(config, dest)
    except pipeline.PipelineError as exc:
        raise SystemExit(f'pipeline error: {exc}') from exc
    _append_builtin_agent_dirs(
        sorted(str(dest / spec) for spec in mapping.values())
    )
    return mapping


def main() -> None:
    """
    Register the ``sbx`` provider + agents, then run the CLI.

    Invoked via the ``omni-sbx`` console script. All arguments are
    forwarded to Omnigent's Click group, so ``omni-sbx server ...``
    behaves exactly like ``omni server ...`` with ``provider: sbx``
    available and the swarm agents auto-registered.

    An optional ``--pipeline PATH`` (server only) materializes +
    registers a declarative pipeline's agents before startup, then is
    stripped so the stock CLI never sees it. All setup runs before
    ``cli()`` so the server picks it up at app creation.

    When starting the server, also advise (never change) on any broad
    global sbx allow rules that would undermine per-VM allowlists.
    """
    install_sbx_provider()
    register_bundled_agents()
    argv, pipeline_path = _extract_pipeline_flag(sys.argv[1:])
    if pipeline_path is not None:
        if 'server' not in argv:
            raise SystemExit('--pipeline is only valid with `server`')
        register_pipeline_agents(pipeline_path)
    sys.argv = [sys.argv[0], *argv]
    if 'server' in sys.argv[1:]:
        warn_on_global_allow_rules()
    cli()


if __name__ == '__main__':
    main()
