"""Sandbox provider backed by Docker Sandboxes (the ``sbx`` CLI).

Each Omnigent *managed host* runs inside its own ``sbx`` microVM.
Omnigent's default ``start_host`` bootstrap probes ``$HOME``, creates
a workspace, optionally clones the session repo, and launches
``omnigent host`` — all of it driven through the transport primitives
implemented here (``run`` / ``run_background``). So this launcher only
teaches Omnigent how to talk to ``sbx``; the host lifecycle is
inherited unchanged from the base class.

The microVM boots from the prebaked ``omnigent-host`` image, which
already carries omnigent + the coding-harness CLIs (``claude``),
``tmux`` (the claude-native bridge), ``git`` and ``curl``. That keeps
provisioning to a single ``sbx create`` call.
"""

from __future__ import annotations

import logging
import os
import shlex
import shutil
import subprocess
import tempfile
import threading
import time
from typing import TYPE_CHECKING, ClassVar
from urllib.parse import urlparse

import click
from omnigent.host.identity import (
    HOST_ID_ENV_VAR,
    HOST_NAME_ENV_VAR,
    HOST_TOKEN_ENV_VAR,
)
from omnigent.onboarding.sandboxes.base import (
    ExecModelHostLauncher,
    RemoteCommandResult,
    render_host_config_write_command,
)
from omnigent.onboarding.sandboxes.types import SandboxCapabilities

from sbx_omnigent import agy, claude, codex
from sbx_omnigent import swarm as swarm_mod

if TYPE_CHECKING:
    from collections.abc import Callable

_logger = logging.getLogger(__name__)

#: When set (any non-empty value) in the server environment,
#: ``terminate`` leaves the sandbox in place instead of removing it. A
#: launch-failure teardown otherwise deletes the box and its
#: ``/tmp/omnigent-host.log`` before it can be read; set this to inspect
#: why a host never came online.
_KEEP_SANDBOXES_ENV = 'SBX_KEEP_SANDBOXES'

#: Prebaked Omnigent host image (multi-arch: linux/amd64 +
#: linux/arm64, so it runs natively on Apple Silicon). Pin to
#: ``:vX.Y.Z`` or ``:sha-<short>`` in config to match your server
#: version and avoid host<->server protocol skew.
DEFAULT_HOST_IMAGE = 'ghcr.io/omnigent-ai/omnigent-host:latest'

#: Workspace-string sentinel marking a bind-mount directive. A
#: managed session whose ``workspace`` is
#: ``git@sbxmount:<abs-path>#<mode>`` asks this launcher to mount an
#: existing host worktree at ``<abs-path>`` (``mode`` = ``rw`` for a
#: coder, ``ro`` for a reviewer) instead of cloning a repo URL inside
#: the VM. The ``sbxmount`` sentinel host is a discriminator: real repo
#: URLs (``git@github.com:…``) do not match and fall through to the
#: inherited clone behavior. The string is chosen to pass Omnigent's
#: ``parse_repo_workspace`` grammar unchanged, so the value arrives
#: here as ``repo_url``/``repo_branch``.
_MOUNT_SENTINEL_PREFIX = 'git@sbxmount:'

#: Valid mount modes carried in the sentinel's ``#<mode>`` fragment.
_MOUNT_MODES = ('rw', 'ro')

#: Proxy env var NAMES forwarded from the ``omnigent host`` env to the
#: runner/harness via ``OMNIGENT_RUNNER_ENV_PASSTHROUGH``. Omnigent's
#: runner env allowlist strips these by default, so the coding harness
#: would bypass sbx's *forward* proxy — and sbx only performs credential
#: injection (placeholder → real secret header swap) on the forward
#: path. Without them the harness sends the sbx placeholder verbatim
#: and the provider rejects it. Forwarding routes harness traffic
#: through the forward proxy, where the swap happens on the wire — so
#: the real credential never enters the VM.
_PROXY_PASSTHROUGH_VARS = (
    'HTTP_PROXY',
    'HTTPS_PROXY',
    'NO_PROXY',
    'http_proxy',
    'https_proxy',
    'no_proxy',
    'NODE_USE_ENV_PROXY',
)

#: Hosts the runner/harness must reach DIRECTLY, never via the forward
#: proxy: the sbx proxy itself, loopback, the runner<->harness IPC host,
#: and the Docker host gateway (the dial-back for a local server). The
#: session's own server host is appended at launch. Everything else
#: (e.g. ``api.anthropic.com``) still routes through the proxy so
#: credential injection applies; the server tunnel stays direct.
_PROXY_BYPASS_HOSTS = (
    'localhost',
    '127.0.0.1',
    '::1',
    'gateway.docker.internal',
    'host.docker.internal',
    'harness.local',
)

#: Live host-launch subprocesses, kept referenced so the interpreter
#: does not GC a Popen and close its pipes — that would SIGPIPE the
#: ``sbx exec`` holding omnigent host alive. Pruned as hosts exit.
_HOST_PROCS: list[subprocess.Popen] = []
_HOST_PROCS_LOCK = threading.Lock()

#: Serializes ``sbx create`` across every launcher instance in this
#: process. A swarm start fires N managed launches near-simultaneously
#: (one per role), each in its own Omnigent provisioning thread; letting
#: their ``sbx create`` calls overlap makes the sbx daemon's concurrent
#: proxy injections race, which surfaces as an intermittent
#: ``500 … failed to inject network proxy`` that kills one VM. Held for
#: the whole create so only one runs at a time.
_CREATE_LOCK = threading.Lock()

#: Monotonic time the last ``sbx create`` finished (a one-slot mutable
#: holder). Seeds the optional settle gap between consecutive creates
#: (see ``provision_stagger_s``); ``0.0`` means "no create yet".
_LAST_CREATE_DONE = [0.0]

#: Default settle gap (seconds) between consecutive ``sbx create``
#: calls, on top of full serialization — gives the daemon room to
#: finish tearing down one injection's transient network state before
#: the next starts. Only back-to-back creates pay it; a lone launch
#: does not.
DEFAULT_PROVISION_STAGGER_S = 2.0

#: Curated per-VM network allowlist applied to managed VMs when
#: ``sbx.egress_allow`` is unset — the "reasonable baseline" so agents
#: work out of the box: the LLM endpoints the coding harness needs plus
#: common trusted package registries. A config list REPLACES this with
#: a stricter/custom set. The Omnigent dial-back is added automatically
#: on top (see :meth:`SbxLauncher._apply_egress`), so it is not listed.
DEFAULT_EGRESS_ALLOW: tuple[str, ...] = (
    # Claude coding harness (claude-native).
    'api.anthropic.com',
    'statsig.anthropic.com',
    'downloads.claude.ai',
    # Trusted package registries (coder builds / dependency installs).
    'registry.npmjs.org',
    'npmjs.org',
    'pypi.org',
    'files.pythonhosted.org',
    # uv, which every Python project here installs in its verify
    # setup. sbx's own default-package-managers bundle allows
    # `astral.sh:443` with no wildcard, but the installer 301s to
    # `releases.astral.sh` — so
    # `curl -LsSf https://astral.sh/uv/install.sh` yields a 125-byte
    # 403 page rather than a script, and uv never installs. Observed
    # live as a verify gate that could not run at all, which fails a
    # whole chunk as INFRASTRUCTURE after every review has already
    # passed.
    '**.astral.sh',
    # OSV, which `uv audit` / `cargo audit` query for advisories.
    # Without it the audit resolves the lockfile, fails the lookup
    # with a 403, and reports that it could not verify — which a
    # reviewer then files as a finding. Fifteen issues in one m0
    # campaign said exactly that.
    'api.osv.dev',
    # Debian apt, on PORT 80. sbx's own default-os-packages bundle
    # allows **.debian.org:443 but — unlike its Ubuntu entries, which
    # list :80 explicitly — never :80, and the host image's sources are
    # http://deb.debian.org. Without this every apt call is denied, so
    # an agent cannot install a toolchain: observed live as a reviewer
    # burning its whole turn hunting for a cargo that could never be
    # installed, then returning no VERDICT. Port 80 is safe here —
    # packages are GPG-signed, so only which packages are fetched is
    # disclosed, not their integrity.
    'deb.debian.org:80',
)


def _retain_host_process(proc: subprocess.Popen) -> None:
    """
    Keep *proc* alive and drain its output on a daemon thread.

    The foreground ``sbx exec`` that runs omnigent host must stay open
    for the host's lifetime (see :meth:`SbxLauncher.run_background`).
    Draining both prevents a full-pipe stall and lets us drop the
    process from the registry once the host exits.

    :param proc: The ``sbx exec`` process holding omnigent host.
    """
    with _HOST_PROCS_LOCK:
        _HOST_PROCS[:] = [p for p in _HOST_PROCS if p.poll() is None]
        _HOST_PROCS.append(proc)

    def _drain() -> None:
        try:
            if proc.stdout is not None:
                for _ in proc.stdout:
                    pass
        finally:
            proc.wait()
            with _HOST_PROCS_LOCK:
                if proc in _HOST_PROCS:
                    _HOST_PROCS.remove(proc)

    threading.Thread(target=_drain, name='sbx-host-drain', daemon=True).start()


class SbxLauncher(ExecModelHostLauncher):
    """
    Run Omnigent managed hosts in Docker Sandboxes (``sbx``) microVMs.

    :param image: Container image the microVM boots from. Defaults to
        :data:`DEFAULT_HOST_IMAGE`.
    :param profile: Optional ``sbx`` governance profile name applied
        to every sandbox (least-privilege footprint). ``None`` uses
        the ``sbx`` default.
    :param cpus: Optional CPU allocation per sandbox. ``None`` lets
        ``sbx`` auto-size.
    :param memory: Optional memory limit per sandbox in binary units,
        e.g. ``"8g"``. ``None`` lets ``sbx`` auto-size.
    :param unset_env: Environment variable names to strip from the
        ``omnigent host`` launch (via ``env -u``), so the whole
        process tree — runner, terminal, and the coding harness —
        never inherits them. For Claude subscription (``/login``)
        mode, strip ``ANTHROPIC_API_KEY`` (the sbx ``proxy-managed``
        sentinel that otherwise forces API-key mode) and
        ``CLAUDECODE``.
    :param worktree_root: Absolute host directory that holds per-swarm
        worktrees. A mount-sentinel workspace
        (``git@sbxmount:<path>#<mode>``) may only mount a path that
        resolves strictly UNDER this root — the allowlist choke point
        that keeps a sandbox from bind-mounting an arbitrary host
        directory. ``None`` disables the mount path (sentinel sessions
        fail loud).
    :param provision_stagger_s: Minimum spacing (seconds) between
        consecutive ``sbx create`` calls, on top of the serialization
        that :data:`_CREATE_LOCK` always enforces. Prevents a swarm's
        near-simultaneous launches from racing the sbx daemon's proxy
        injection. Defaults to :data:`DEFAULT_PROVISION_STAGGER_S`; set
        ``0`` to keep serialization but drop the extra gap.
    :param scope_egress: Master switch for per-sandbox egress scoping.
        ``False`` (default — direct/test use) applies no scoped rule.
        ``True`` (the config path always sets this) gives each managed
        VM a scoped ``sbx policy allow network --sandbox <box>`` for the
        derived Omnigent dial-back plus ``egress_allow``.
    :param egress_allow: The per-sandbox allowlist hosts, applied (with
        ``scope_egress``) on top of the dial-back. An **empty** tuple
        with scoping on means **dial-back only** — block everything but
        the mandatory server connection (max lockdown), NOT "no scoping"
        (that is ``scope_egress=False``). The config layer resolves
        ``sbx.egress_allow`` (unset → a curated default) into this; see
        ``entrypoint._as_egress_allow``.
    :param agy_enabled: When ``True``, seed each **agy-tagged** VM with
        the agy placeholder token + onboarding marker so an
        ``antigravity-native`` agent passes Omnigent's readiness gate
        (the wire swap supplies the real token). Only VMs the swarm tags
        via the ``-agy`` mount-sentinel suffix are seeded — a Claude VM
        is never touched. A no-op when ``False``.
    :param agy_enterprise: ``True`` for a Business/enterprise Google
        account — sets ``enterpriseOnboardingComplete`` in the seeded
        marker AND patches the in-VM bridge so agy's first-run
        onboarding wizard is skipped. Ignored unless ``agy_enabled``.
    :param agy_gcp_project: GCP project id seeded into the VM's agy
        ``settings.json`` — required for an enterprise/Vertex cascade
        (else a turn fails with ``invalid project ID``). ``None`` seeds
        no project. Ignored unless ``agy_enabled``.
    :param agy_gcp_location: GCP location for that settings block
        (default ``"us"``).

    Credentials are NOT handled here. ``sbx``'s host-side proxy injects
    them into the agent's outbound API calls from its own secret store
    (``sbx secret``); the real key never enters the microVM. Configure
    them out-of-band — see the README.
    """

    provider: ClassVar[str] = 'sbx'

    @property
    def capabilities(self) -> SandboxCapabilities:
        """
        What this provider actually supports.

        Declared explicitly rather than left to the base class's
        derivation. That derivation is a documented transition shim —
        it infers flags from legacy class vars and from which methods
        are overridden — and its default of
        ``supports_cli_bootstrap = True`` is WRONG for us: the CLI
        bootstrap path needs ``put`` / ``stream_exec`` /
        ``exec_foreground`` / ``wheel_install_command``, and this
        launcher implements none of them. It exists for one job,
        launching a managed host in an sbx microVM.

        ``classifies_runner_by_agent`` stays False deliberately:
        setting it makes the managed launch path thread an extra
        ``agent_name`` keyword into :meth:`start_host`, which this
        signature does not accept.

        :returns: The capability flags for the sbx provider.
        """
        return SandboxCapabilities(
            # No `omnigent sandbox create` / `connect` — see above.
            cli_bootstrap=False,
            # The whole point: server-managed host_type="managed"
            # sessions.
            managed_launch=True,
            # No local port bridge into the microVM for the App
            # OAuth callback.
            local_port_forward=False,
            # `sbx` can restart a stopped sandbox, but this launcher
            # never does — every session provisions a fresh one.
            resume_stopped=False,
            # `terminate` is overridden and really does
            # `sbx rm --force`.
            programmatic_terminate=True,
            file_copy=False,
            streaming_exec=False,
            foreground_exec=False,
            classifies_runner_by_agent=False,
        )

    # Managed-launch only: we do not implement the ``omnigent sandbox
    # create``/``connect`` CLI-bootstrap primitives (put / stream_exec
    # / exec_foreground / wheel install) — the prebaked image needs
    # none of them.
    supports_cli_bootstrap: ClassVar[bool] = False

    # ``sbx`` has no ssh -L style host->sandbox forwarding, and the
    # managed flow authenticates the host with a launch token rather
    # than the in-sandbox App OAuth dance, so this stays False.
    supports_local_port_forward: ClassVar[bool] = False

    # Left False for the first cut: ``sbx exec`` appears to auto-start
    # a stopped sandbox, but the stopped->running verb is unverified.
    # Flip to True once ``resume`` is confirmed (see the method below).
    can_resume: ClassVar[bool] = False

    def __init__(
        self,
        *,
        image: str = DEFAULT_HOST_IMAGE,
        profile: str | None = None,
        cpus: int | None = None,
        memory: str | None = None,
        unset_env: tuple[str, ...] = (),
        worktree_root: str | None = None,
        provision_stagger_s: float = DEFAULT_PROVISION_STAGGER_S,
        egress_allow: tuple[str, ...] = (),
        scope_egress: bool = False,
        agy_enabled: bool = False,
        agy_enterprise: bool = False,
        agy_gcp_project: str | None = None,
        agy_gcp_location: str = 'us',
    ) -> None:
        self._image = image
        self._profile = profile
        self._cpus = cpus
        self._memory = memory
        self._unset_env = tuple(unset_env)
        self._worktree_root = worktree_root
        self._provision_stagger_s = max(0.0, provision_stagger_s)
        self._egress_allow = tuple(egress_allow)
        self._scope_egress = scope_egress
        self._agy_enabled = agy_enabled
        self._agy_enterprise = agy_enterprise
        self._agy_gcp_project = agy_gcp_project
        self._agy_gcp_location = agy_gcp_location

    def prepare(self) -> None:
        """
        Verify the ``sbx`` CLI is installed and usable on the server.

        Idempotent — the managed flow calls this before every launch.

        :raises click.ClickException: If ``sbx`` is missing or not
            signed in.
        """
        if shutil.which('sbx') is None:
            raise click.ClickException(
                "The 'sbx' sandbox provider requires the Docker "
                "Sandboxes CLI on the server's PATH. Install it, then "
                'run `sbx login`.'
            )
        # `sbx ls` is a cheap liveness+auth probe; it exits non-zero
        # when the daemon is unreachable or the user is not signed in.
        probe = subprocess.run(['sbx', 'ls'], capture_output=True, text=True)
        if probe.returncode != 0:
            raise click.ClickException(
                '`sbx` is installed but not usable — run `sbx login` '
                f'as the user the server runs as. Details: '
                f'{probe.stderr.strip()}'
            )

    def provision(self, name: str) -> str:
        """
        Reserve a sandbox id for a managed host (defers creation).

        ``sbx create`` needs the host workspace PATH, which is only
        known in :meth:`start_host` (a mount sentinel, or the base
        clone bootstrap). So this only RESERVES the name and the box is
        materialized there — the model the base class blesses for
        entrypoint-as-host providers. It also lets the server register
        the launch token against the id before the box exists, closing
        the host dial-back race by construction.

        :param name: Human-readable sandbox label from the managed
            flow.
        :returns: The reserved sandbox id (``sbx`` addresses sandboxes
            by name, so the id equals *name*).
        """
        return name

    def start_host(
        self,
        sandbox_id: str,
        *,
        token: str,
        host_id: str,
        host_name: str,
        server_url: str,
        repo_url: str | None = None,
        repo_branch: str | None = None,
        repo_name: str | None = None,
        host_config: dict[str, object] | None = None,
        on_stage: Callable[[str], None] | None = None,
    ) -> str:
        """
        Create the sandbox and start ``omnigent host`` in it.

        A managed session whose workspace is the mount sentinel
        (``git@sbxmount:<abs-path>#<mode>``, see
        :data:`_MOUNT_SENTINEL_PREFIX`) gets an EXISTING host worktree
        bind-mounted into the microVM — ``rw`` for a coder, ``:ro`` for
        a reviewer (which also gets its own throwaway ``rw`` scratch as
        the required primary workspace, since ``sbx`` forbids ``:ro`` on
        the primary). The mount source must resolve strictly under the
        configured ``worktree_root`` (see
        :meth:`_resolve_worktree_path`).

        Any other ``repo_url`` (a real clone URL, or ``None``) takes the
        inherited path: create the box with a throwaway scratch mount,
        then let the base bootstrap probe ``$HOME``, make the workspace,
        optionally clone, and launch. Ordinary managed hosts behave
        exactly as before (the box is now created here rather than in
        :meth:`provision`, which is transparent to them).

        :param sandbox_id: The id reserved by :meth:`provision`.
        :param token: Launch token the host authenticates with.
        :param host_id: Server-chosen host identity.
        :param host_name: Server-chosen host display name.
        :param server_url: URL the host dials back to.
        :param repo_url: The session workspace as surfaced by
            ``parse_repo_workspace`` — the mount sentinel triggers the
            bind-mount path; anything else is delegated upstream.
        :param repo_branch: For the sentinel, the ``#<mode>`` fragment
            (``rw``/``ro``); otherwise the clone branch.
        :param repo_name: Derived repo dir name (unused when mounting).
        :param host_config: Verbatim in-sandbox
            ``~/.omnigent/config.yaml`` the server injects, or
            ``None``. Written before the host starts.
        :param on_stage: Progress observer; ``"starting"`` fires before
            the host launches.
        :returns: The in-sandbox workspace path — the mounted worktree
            for a sentinel, else whatever the base bootstrap returns.
        :raises click.ClickException: On a bad sentinel, a mount path
            outside ``worktree_root``, or a failed sandbox command.
        """
        if repo_url is None or not repo_url.startswith(_MOUNT_SENTINEL_PREFIX):
            # Non-sentinel: materialize the box with a throwaway scratch
            # mount (what provision used to do), then run the inherited
            # clone-in-VM bootstrap unchanged.
            self._create_sandbox(sandbox_id, [self._make_scratch(sandbox_id)])
            self._apply_egress(sandbox_id, server_url)
            # No agy seed on the non-sentinel (clone-in-VM) path: it
            # carries no harness signal, so an agy VM is
            # indistinguishable from a Claude one. Only the swarm mount
            # path tags agy VMs.
            return super().start_host(
                sandbox_id,
                token=token,
                host_id=host_id,
                host_name=host_name,
                server_url=server_url,
                repo_url=repo_url,
                repo_branch=repo_branch,
                repo_name=repo_name,
                host_config=host_config,
                on_stage=on_stage,
            )

        path, mode, credential = self._parse_mount_sentinel(
            repo_url, repo_branch
        )
        worktree = self._resolve_worktree_path(path)
        if mode == 'rw':
            # Coder: the worktree IS the primary, read-write.
            workspaces = [worktree]
        else:
            # Reviewer: primary must be rw, so give it a throwaway
            # scratch and mount the worktree read-only alongside it (at
            # the same absolute path the host sees).
            workspaces = [
                self._make_scratch(sandbox_id),
                f'{worktree}:ro',
            ]
        _logger.info(
            'sbx mount: sandbox=%s mode=%s worktree=%s',
            sandbox_id,
            mode,
            worktree,
        )
        self._create_sandbox(sandbox_id, workspaces)
        self._apply_egress(
            sandbox_id,
            server_url,
            extra=codex.CODEX_EGRESS if credential == 'codex' else (),
        )
        # Seed ONLY the credential this VM's own sentinel names — never
        # a Claude VM, and never the wrong harness's credential.
        if credential == 'agy':
            self._inject_agy_credentials(sandbox_id)
        elif credential == 'codex':
            self._inject_codex_credentials(sandbox_id)
        else:
            # Not a credential — Claude gets its from the sbx proxy.
            # This is the launch-gate pre-acceptance a headless Claude
            # needs before `bypassPermissions` will start at all.
            # `credential is None` is EXACTLY the set of VMs that get
            # the Claude launch args: `credential_kind_for` and
            # `_launch_args_for` both fall back to Claude for an
            # unresolved harness, so the two stay in step by
            # construction rather than by a second list to maintain.
            self._seed_claude_settings(sandbox_id)
        # The server OWNS this file and passes it on every managed
        # launch: it is the verbatim in-sandbox
        # ``~/.omnigent/config.yaml``, including the ``providers``
        # block. The inherited ``start_host`` writes it for the
        # delegated path above, but this mount path never reaches
        # the base — so accepting the argument and dropping it would
        # silently deprive the host of server-owned configuration.
        if host_config is not None:
            self.run(
                sandbox_id, render_host_config_write_command(host_config)
            )
        if on_stage is not None:
            on_stage('starting')
        self._launch_host(
            sandbox_id,
            token=token,
            host_id=host_id,
            host_name=host_name,
            server_url=server_url,
        )
        return worktree

    @staticmethod
    def _parse_mount_sentinel(
        repo_url: str, repo_branch: str | None
    ) -> tuple[str, str, str | None]:
        """
        Split a mount sentinel into path, mode, and credential kind.

        The ``#<mode>`` fragment optionally carries a ``-<kind>`` suffix
        (set by :func:`swarm.mount_sentinel`) naming which credential
        this VM should be seeded with, so a VM only ever receives its
        OWN harness's credential — a Claude VM is never seeded, and an
        agy VM never receives Codex's.

        This carried a bare agy boolean until Codex became a third
        supported harness; the launcher now has to know WHICH
        credential to install, not merely whether to install agy's.

        :param repo_url: A sentinel URL, e.g.
            ``"git@sbxmount:/srv/worktrees/swarm-a"``.
        :param repo_branch: The ``#<mode>`` fragment (``rw``/``ro``,
            optionally suffixed ``-agy`` / ``-codex``), or ``None`` →
            ``"rw"``.
        :returns: ``(path, mode, credential)`` where *credential* is
            ``'agy'``, ``'codex'``, or ``None``.
        :raises click.ClickException: If the mode is not one of
            :data:`_MOUNT_MODES`, or the path is empty.
        """
        path = repo_url[len(_MOUNT_SENTINEL_PREFIX) :]
        fragment = (repo_branch or 'rw').lower()
        credential: str | None = None
        for kind in swarm_mod.MOUNT_CREDENTIAL_KINDS:
            if fragment.endswith(f'-{kind}'):
                credential = kind
                fragment = fragment[: -len(f'-{kind}')]
                break
        mode = fragment
        if not path:
            raise click.ClickException(
                f'mount sentinel {repo_url!r} has an empty path'
            )
        if mode not in _MOUNT_MODES:
            raise click.ClickException(
                f'mount sentinel mode {mode!r} is invalid — '
                f'expected one of {_MOUNT_MODES}'
            )
        return path, mode, credential

    def _resolve_worktree_path(self, path: str) -> str:
        """
        Validate a sentinel mount path against the allowed root.

        Security choke point: the mount source must be an existing
        directory strictly UNDER the configured ``worktree_root``, with
        symlinks resolved (``realpath``) so a symlink inside the root
        cannot smuggle in an outside directory and ``..`` cannot escape.
        Mounting the root itself is refused too — that would expose
        every swarm's worktree to one VM.

        :param path: The absolute host path from the sentinel, e.g.
            ``"/srv/worktrees/swarm-a"``.
        :returns: The resolved (``realpath``) directory to mount.
        :raises click.ClickException: If ``worktree_root`` is unset, the
            path does not exist, or it resolves outside the root.
        """
        if self._worktree_root is None:
            raise click.ClickException(
                "a mount sentinel was used but 'sandbox.sbx."
                "worktree_root' is not configured — set it to the "
                'directory that holds per-swarm worktrees'
            )
        root = os.path.realpath(self._worktree_root)
        real = os.path.realpath(path)
        if not os.path.isdir(real):
            raise click.ClickException(f'worktree path does not exist: {path}')
        if not real.startswith(root + os.sep):
            raise click.ClickException(
                f'worktree path {path!r} resolves to {real!r}, which '
                f'is not under the allowed root {root!r}'
            )
        return real

    def _make_scratch(self, name: str) -> str:
        """
        Make a throwaway host dir to satisfy ``sbx create``'s primary.

        :param name: Sandbox name, used only as a filename hint.
        :returns: The new empty directory's absolute path.
        """
        return tempfile.mkdtemp(prefix=f'sbx-{name}-')

    def _create_sandbox_command(
        self, name: str, workspaces: list[str]
    ) -> list[str]:
        """
        Build the ``sbx create shell`` argv for a set of workspaces.

        Workspaces are passed as list args (never a shell string), so a
        ``<path>:ro`` suffix is a literal argument — immune to shell
        word-splitting or quote-modifier surprises.

        :param name: Sandbox name (``--name``).
        :param workspaces: Host PATH args in order; the first is the
            ``rw`` primary, later ones may carry a ``:ro`` suffix.
        :returns: The full ``sbx create`` argv.
        """
        command = [
            'sbx',
            'create',
            'shell',
            *workspaces,
            '--template',
            self._image,
            '--name',
            name,
            '--quiet',
        ]
        if self._profile is not None:
            command += ['--profile', self._profile]
        if self._cpus is not None:
            command += ['--cpus', str(self._cpus)]
        if self._memory is not None:
            command += ['--memory', self._memory]
        return command

    def _create_sandbox(self, name: str, workspaces: list[str]) -> None:
        """
        Materialize a sandbox mounting *workspaces*.

        Serialized process-wide via :data:`_CREATE_LOCK` (with an
        optional inter-create settle gap) so concurrent swarm launches
        cannot race the sbx daemon's proxy injection — see
        :meth:`_await_create_stagger`.

        :param name: Sandbox name.
        :param workspaces: Host PATH args (see
            :meth:`_create_sandbox_command`).
        :raises click.ClickException: If ``sbx create`` fails.
        """
        command = self._create_sandbox_command(name, workspaces)
        with _CREATE_LOCK:
            self._await_create_stagger()
            try:
                self._run_local(command, action=f'create sandbox {name!r}')
            finally:
                _LAST_CREATE_DONE[0] = time.monotonic()

    @staticmethod
    def _dialback_hosts(server_url: str) -> list[str]:
        """
        Derive the Omnigent dial-back host(s) from *server_url*.

        Every managed VM must reach the server to register, so these are
        always included in a scoped allowlist. Returns ``<host>:<port>``
        and, when the server is the Docker host gateway, also
        ``localhost:<port>`` — both are required (see the network-policy
        note in the README).

        :param server_url: The URL the host dials back to, e.g.
            ``"http://host.docker.internal:6767"``.
        :returns: The dial-back ``host:port`` entries (empty if the URL
            has no hostname).
        """
        parsed = urlparse(server_url)
        host = parsed.hostname
        if not host:
            return []
        port = parsed.port or (443 if parsed.scheme == 'https' else 80)
        hosts = [f'{host}:{port}']
        if host == 'host.docker.internal':
            hosts.append(f'localhost:{port}')
        return hosts

    def _apply_egress(
        self,
        name: str,
        server_url: str,
        extra: tuple[str, ...] = (),
    ) -> None:
        """
        Apply the per-sandbox network allowlist (scoped ``sbx policy``).

        A no-op unless ``scope_egress`` is on. When on, grants the box
        exactly the derived dial-back + ``egress_allow`` + *extra* and
        nothing else — the load-bearing allow under a deny-all baseline,
        additive under a permissive one. An empty ``egress_allow`` (with
        scoping on) grants dial-back only: max lockdown, not "no scope".

        :param name: The sandbox to scope the rule to.
        :param server_url: Used to derive the dial-back host(s).
        :param extra: Harness-specific hosts for THIS box only, e.g.
            :data:`sbx_omnigent.codex.CODEX_EGRESS`. Kept per-VM rather
            than added to the global allowlist so a Claude or agy box
            never gains reachability it has no use for.
        :raises click.ClickException: If ``sbx policy allow`` fails.
        """
        if not self._scope_egress:
            return
        hosts = [
            *self._dialback_hosts(server_url),
            *self._egress_allow,
            *extra,
        ]
        if not hosts:
            return
        self._run_local(
            [
                'sbx',
                'policy',
                'allow',
                'network',
                '--sandbox',
                name,
                ','.join(hosts),
            ],
            action=f'apply egress allowlist to {name!r}',
        )

    def _inject_agy_credentials(self, name: str) -> None:
        """
        Seed the agy (Antigravity) PLACEHOLDER credentials into a VM.

        A no-op unless ``agy_enabled``. Called only for a VM the swarm
        tagged agy (the ``-agy`` mount-sentinel suffix), so it never
        touches a Claude VM. Writes the placeholder token file + flat
        creds decoy + onboarding marker into the VM's ``~/.gemini`` (see
        :func:`agy.build_agent_seed_script`) so the agy agent passes
        Omnigent's readiness gate before its runner
        launches; the sbx proxy swaps a live token onto the wire at
        request time. Runs before the host launches.

        Fail-loud: agy support was explicitly enabled, so a failed seed
        would otherwise surface as a cryptic in-VM readiness failure
        seconds later. The placeholder is inert, so it is passed as a
        script literal (no secret in argv).

        :param name: The sandbox to seed.
        :raises click.ClickException: If the seed command cannot run or
            does not report success.
        """
        if not self._agy_enabled:
            return
        self._run_agy_inject(
            name,
            agy.build_agent_seed_script(
                enterprise=self._agy_enterprise,
                gcp_project=self._agy_gcp_project,
                gcp_location=self._agy_gcp_location,
            ),
            agy.SEED_OK_MARKER,
            'seed agy credentials',
        )
        # Patch the in-VM runner's bridge module before it imports.
        # Always: accept agy's collapsed "[Pasted text ...]" placeholder
        # as a rendered paste, or a long/multi-line message (a human's
        # reply to an interactive planner) is refused before submit.
        # Enterprise/GCP only: mark enterprise onboarding complete (skip
        # agy's first-run wizard) and, on older builds, add
        # settings.json to the seed-file copy list so the project
        # reaches the bridge. Best-effort (the marker prints even on a
        # no-op), so version drift never blocks the launch.
        self._run_agy_inject(
            name,
            agy.build_bridge_patch_script(
                enterprise=self._agy_enterprise,
                seed_settings=bool(self._agy_gcp_project),
            ),
            agy.BRIDGE_PATCH_OK_MARKER,
            'patch agy bridge module',
        )

    def _inject_codex_credentials(self, name: str) -> None:
        """
        Seed Codex (OpenAI) credentials into a VM.

        Called only for a VM whose mount sentinel names ``codex``, so it
        never touches a Claude or agy VM. Writes ``~/.codex/auth.json``
        with the host's REAL access token and an INERT placeholder
        refresh token, so the guest can call the API but cannot mint new
        credentials — the refresh token never leaves the host.

        Unlike agy's seed, whose placeholder is inert and can safely be
        a script literal, this payload is a REAL secret. It is passed on
        STDIN so it never appears in argv, where ``ps`` would show it,
        and it is never logged.

        Fail-loud: a Codex agent whose credential did not land fails its
        first turn with an opaque auth error minutes later.

        :param name: The sandbox to seed.
        :raises click.ClickException: If the host credential is missing
            or the seed does not report success.
        """
        try:
            payload = codex.build_agent_payload(codex.read_host_auth())
        except codex.CodexAuthError as exc:
            raise click.ClickException(
                f'cannot seed Codex credentials into {name!r}: {exc}\n'
                f'Re-authenticate on this host with:\n'
                f'  {codex.RELOGIN_HINT}'
            ) from exc
        try:
            proc = subprocess.run(
                ['sbx', 'exec', name, 'python3', '-c',
                 codex.build_seed_script()],
                input=payload,
                capture_output=True,
                text=True,
            )
        except OSError as exc:
            raise click.ClickException(
                f'failed to seed Codex credentials in {name!r}: {exc}'
            ) from exc
        if proc.returncode != 0 or codex.SEED_OK_MARKER not in proc.stdout:
            # Scrub before surfacing: the payload we just piped in is a
            # live token, and a guest traceback can echo its own stdin.
            detail = codex.redact(
                ((proc.stdout or '') + (proc.stderr or '')).strip()
            )
            raise click.ClickException(
                f'failed to seed Codex credentials in {name!r} '
                f'(exit {proc.returncode}): {detail[-800:]}'
            )

    def _seed_claude_settings(self, name: str) -> None:
        """
        Pre-accept Claude's bypass-permissions dialog inside a VM.

        Claude launches with ``--permission-mode bypassPermissions``
        (see :data:`sbx_omnigent.swarm._YOLO_LAUNCH_ARGS`), and the
        first launch in that mode renders a full-screen "Yes, I accept"
        warning. No one is at the terminal in a swarm VM, so without
        this the agent never reaches its prompt and the turn dies at
        the timeout. Omnigent pre-accepts Claude's other two launch
        gates and explicitly declines to touch permission ones, so this
        is ours. See :mod:`sbx_omnigent.claude` for the evidence.

        Fail-loud, like the credential seeds: a VM that missed this
        does not fail visibly, it sits on a dialog for the whole turn
        timeout and reports nothing — the most expensive failure shape
        this launcher has.

        :param name: The sandbox to seed.
        :raises click.ClickException: If the seed cannot run or does
            not report success.
        """
        try:
            proc = subprocess.run(
                ['sbx', 'exec', name, 'python3', '-c',
                 claude.build_settings_seed_script()],
                capture_output=True,
                text=True,
            )
        except OSError as exc:
            raise click.ClickException(
                f'failed to seed Claude settings in {name!r}: {exc}'
            ) from exc
        if proc.returncode != 0 or claude.SEED_OK_MARKER not in proc.stdout:
            detail = ((proc.stdout or '') + (proc.stderr or '')).strip()
            raise click.ClickException(
                f'failed to pre-accept the Claude bypass-permissions '
                f'dialog in {name!r} (exit {proc.returncode}): '
                f'{detail[-800:]}'
            )

    def _run_agy_inject(
        self, name: str, script: str, marker: str, action: str
    ) -> None:
        """
        Run an in-VM agy ``python3 -c`` script, checking its marker.

        :param name: Target sandbox.
        :param script: The in-VM program.
        :param marker: Sentinel the script prints on success.
        :param action: Human phrase for the error message.
        :raises click.ClickException: If the command cannot run, exits
            non-zero, or does not print *marker*.
        """
        try:
            proc = subprocess.run(
                ['sbx', 'exec', name, 'python3', '-c', script],
                capture_output=True,
                text=True,
            )
        except OSError as exc:
            raise click.ClickException(
                f'failed to {action} in {name!r}: {exc}'
            ) from exc
        if proc.returncode != 0 or marker not in proc.stdout:
            # Keep the TAIL, not the head: this runs a python3 program
            # in the VM, and a traceback names its exception on the LAST
            # line. Truncating from the front kept the frames and cut
            # the errno — an in-VM seed failure was reported as
            # '...os.makedirs...' with no way to tell ENOSPC from EROFS
            # from a real bug.
            detail = (proc.stderr or proc.stdout or '').strip()
            raise click.ClickException(
                f'failed to {action} in {name!r} '
                f'(rc={proc.returncode}): {detail[-800:]}'
            )

    def _await_create_stagger(self) -> None:
        """
        Sleep out any remaining settle gap before an ``sbx create``.

        Call while holding :data:`_CREATE_LOCK`. The first create (or
        one that follows a long-idle period) waits nothing; only a
        create that starts within ``provision_stagger_s`` of the prior
        completion sleeps the remainder.
        """
        if self._provision_stagger_s <= 0.0 or _LAST_CREATE_DONE[0] <= 0.0:
            return
        remaining = self._provision_stagger_s - (
            time.monotonic() - _LAST_CREATE_DONE[0]
        )
        if remaining > 0.0:
            time.sleep(remaining)

    def _launch_host(
        self,
        sandbox_id: str,
        *,
        token: str,
        host_id: str,
        host_name: str,
        server_url: str,
    ) -> None:
        """
        Start ``omnigent host`` in the sandbox with its identity+token.

        Mirrors the base bootstrap's launch step for the mount path
        (which skips the ``$HOME``-probe / clone): the ``HOST_*`` env
        assignments carry the server-issued identity and launch token,
        and :meth:`run_background` keeps the host alive for the
        sandbox's lifetime.

        :param sandbox_id: Target sandbox.
        :param token: Launch token.
        :param host_id: Server-chosen host id.
        :param host_name: Server-chosen host name.
        :param server_url: URL the host dials back to.
        """
        env_prefix = ' '.join(
            f'{key}={shlex.quote(value)}'
            for key, value in (
                (HOST_TOKEN_ENV_VAR, token),
                (HOST_ID_ENV_VAR, host_id),
                (HOST_NAME_ENV_VAR, host_name),
            )
        )
        self.run_background(
            sandbox_id,
            f'{env_prefix} omnigent host --server {shlex.quote(server_url)}',
        )

    def run(
        self, sandbox_id: str, command: str, *, check: bool = True
    ) -> RemoteCommandResult:
        """
        Run a shell command inside the sandbox and capture output.

        ``sbx exec`` forwards arguments and pipes stdio
        non-interactively. The "Sandbox ... started successfully"
        banner is written to STDERR, so stdout carries only the
        command's real output — safe for callers that parse it.

        :param sandbox_id: Target sandbox (its name).
        :param command: Shell command to execute remotely.
        :param check: When ``True``, raise on a non-zero exit.
        :returns: The command's exit code and captured output.
        :raises click.ClickException: If *check* is ``True`` and the
            command exits non-zero.
        """
        proc = subprocess.run(
            ['sbx', 'exec', sandbox_id, '--', 'sh', '-c', command],
            capture_output=True,
            text=True,
        )
        if check and proc.returncode != 0:
            raise click.ClickException(
                f'`sbx exec` in {sandbox_id!r} failed '
                f'(rc={proc.returncode}): {proc.stderr.strip()}'
            )
        return RemoteCommandResult(
            returncode=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
        )

    @staticmethod
    def _server_host_from_command(command: str) -> str | None:
        """
        Extract the ``--server`` URL's hostname from a launch command.

        Used to keep the session's own server host on the proxy-bypass
        list so the runner's dial-back tunnel stays direct.

        :param command: The host launch command, e.g.
            ``"… omnigent host --server http://host.docker.internal:6767"``.
        :returns: The server hostname, or ``None`` if not found.
        """
        try:
            tokens = shlex.split(command)
        except ValueError:
            return None
        for idx, token in enumerate(tokens):
            if token == '--server' and idx + 1 < len(tokens):
                return urlparse(tokens[idx + 1]).hostname
        return None

    def _proxy_launch_assignments(self, command: str) -> list[str]:
        """
        Build ``env`` assignments that forward sbx's proxy to agents.

        Sets a ``NO_PROXY`` covering the proxy itself, loopback, the
        runner<->harness IPC host, the Docker host gateway, and the
        session's server host (so the dial-back stays direct), and names
        the proxy vars in ``OMNIGENT_RUNNER_ENV_PASSTHROUGH`` so
        Omnigent's runner forwards them past its env allowlist to the
        harness. The proxy *values* come from the VM's own env; only the
        names (and the augmented ``NO_PROXY``) are set here.

        :param command: The host launch command (parsed for
            ``--server``).
        :returns: A list of ``NAME=value`` assignment tokens
            (shell-quoted) to place on the ``env`` line.
        """
        bypass = list(_PROXY_BYPASS_HOSTS)
        host = self._server_host_from_command(command)
        if host and host not in bypass:
            bypass.append(host)
        no_proxy = ','.join(bypass)
        names = ','.join(_PROXY_PASSTHROUGH_VARS)
        return [
            f'NO_PROXY={shlex.quote(no_proxy)}',
            f'no_proxy={shlex.quote(no_proxy)}',
            f'OMNIGENT_RUNNER_ENV_PASSTHROUGH={shlex.quote(names)}',
        ]

    def run_background(
        self,
        sandbox_id: str,
        command: str,
        *,
        log_path: str = '/tmp/omnigent-host.log',
    ) -> RemoteCommandResult:
        """
        Launch omnigent host so it survives the launching session.

        ``sbx exec`` reaps a backgrounded (``setsid nohup … &``)
        process when the exec session returns, so the base class detach
        drops the host moments after it registers. Instead we run the
        host in the FOREGROUND of a long-lived ``sbx exec`` and drain
        its output on a daemon thread: the exec session stays open for
        the host's lifetime, so the microVM keeps it alive.

        :param sandbox_id: Target sandbox.
        :param command: Host launch command, already carrying the
            ``HOST_*`` identity/token assignments from ``start_host``.
        :param log_path: In-VM file the host's output is written to
            (inspect with ``sbx exec <id> -- cat <log_path>``).
        :returns: A synthetic ``launched`` result.
        """
        # Wrap the launch in ``env`` to (1) drop inherited vars via
        # ``-u`` (e.g. the sbx ``proxy-managed`` ANTHROPIC_API_KEY
        # sentinel, so Claude uses its OAuth/``/login`` session), and
        # (2) forward sbx's proxy config to the runner/harness so
        # credential injection actually reaches the coding agent (see
        # :meth:`_proxy_launch_assignments`). Both apply to every
        # sbx-hosted launch, so the wrapper is unconditional.
        env_parts = ['env']
        env_parts += [f'-u {shlex.quote(name)}' for name in self._unset_env]
        env_parts += self._proxy_launch_assignments(command)
        launch = ' '.join(env_parts) + ' ' + command
        remote = f'{launch} > {shlex.quote(log_path)} 2>&1 < /dev/null'
        proc = subprocess.Popen(
            ['sbx', 'exec', sandbox_id, '--', 'sh', '-c', remote],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            text=True,
        )
        _retain_host_process(proc)
        return RemoteCommandResult(
            returncode=0, stdout='launched\n', stderr=''
        )

    def terminate(self, sandbox_id: str) -> None:
        """
        Remove a sandbox, releasing its microVM.

        Best-effort: teardown should not raise if the sandbox is
        already gone. Set :data:`_KEEP_SANDBOXES_ENV` to skip removal
        and keep the box (and its host log) for debugging.

        :param sandbox_id: The sandbox to remove.
        """
        if os.environ.get(_KEEP_SANDBOXES_ENV):
            click.echo(
                f'{_KEEP_SANDBOXES_ENV} set — keeping sandbox '
                f'{sandbox_id!r}. Inspect: sbx exec {sandbox_id} -- '
                f'cat /tmp/omnigent-host.log; remove: sbx rm -f '
                f'{sandbox_id}',
                err=True,
            )
            return
        self._run_local(
            ['sbx', 'rm', '--force', sandbox_id],
            action=f'remove sandbox {sandbox_id!r}',
            check=False,
        )

    @staticmethod
    def _run_local(
        command: list[str], *, action: str, check: bool = True
    ) -> None:
        """
        Run a local ``sbx`` management command on the server host.

        :param command: Full argv, e.g.
            ``["sbx", "rm", "--force", "name"]``.
        :param action: Human phrase for error messages, e.g.
            ``"create sandbox 'x'"``.
        :param check: When ``True``, raise on a non-zero exit.
        :raises click.ClickException: If *check* is ``True`` and the
            command exits non-zero.
        """
        proc = subprocess.run(command, capture_output=True, text=True)
        if check and proc.returncode != 0:
            raise click.ClickException(
                f'failed to {action} (rc={proc.returncode}): '
                f'{proc.stderr.strip()}'
            )
