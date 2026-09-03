"""Coordinate an A-prime swarm: coder VM (rw) + reviewer VMs (:ro).

The trusted-plane orchestrator that composes the three verified pieces —
the launcher's worktree mount, the worktree lifecycle
(:mod:`sbx_omnigent.worktrees`), and the managed-session driver
(:mod:`sbx_omnigent.swarm_session`) — into the collaborative loop:

- a **coder** runs in a microVM with the swarm worktree bind-mounted
  read-write (``git@sbxmount:<path>#rw``);
- one or more **reviewers** run in their own microVMs with the SAME
  worktree bind-mounted read-only (``…#ro``), so they read the coder's
  live (even uncommitted) work but cannot alter it (kernel-enforced);
- this coordinator (host-side, trusted) drives each agent's turns and
  publishes on consensus.

Two layers live here:

- :class:`SwarmOrchestrator` — the in-process mechanics (start / drive /
  publish / teardown), reused by a deterministic driver.
- a **registry-backed CLI** (:class:`SwarmRegistry` + the ``swarm``
  command group) so an LLM coordinator can drive swarms across separate
  ``sys_os_shell`` calls, addressing agents by role. State persists in a
  JSON registry so it survives context compaction.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import click
import yaml
from omnigent.reasoning_effort import CODEX_EFFORTS

from sbx_omnigent import agy, codex
from sbx_omnigent.swarm_session import (
    SwarmSessionClient,
    SwarmSessionError,
    SwarmTurnResult,
    looks_like_rate_limit,
)
from sbx_omnigent.worktrees import WorktreeManager

#: Default per-turn budget (seconds) — generous, since a turn may wait
#: on a fresh microVM to finish provisioning before it even starts.
_DEFAULT_TURN_TIMEOUT_S = 600.0

#: Default no-prompt launch args for swarm agents. Swarm agents are
#: headless — they cannot answer a permission prompt, so any prompt
#: hangs the turn. The mode must auto-approve EVERY tool (Bash too, not
#: just edits) without prompting.
#:
#: This is Omnigent's OWN value for claude-native, not a guess of ours.
#: ``_derive_terminal_launch_args_from_spec``
#: (``omnigent/server/routes/_sessions/helpers.py``) maps a spec's
#: ``permission_mode`` onto ``--permission-mode`` and states outright
#: that "YOLO uses ``bypassPermissions``"; Omnigent's own web
#: permission-mode selector sends exactly
#: ``["--permission-mode", "bypassPermissions"]``. That same function is
#: where :data:`_AGY_LAUNCH_ARGS` and :data:`_CODEX_LAUNCH_ARGS` already
#: agree with Omnigent — Claude was the one harness where this table
#: had drifted. Our sessions are the TOP-LEVEL kind, which keep the
#: launch args in the create body (a ``sub_agent_name`` spawn would
#: derive them from the spec and ignore the body instead), so passing
#: them here is the supported seam rather than a workaround.
#:
#: HISTORY — two wrong answers, both found live, both silent:
#: - ``auto``, until 2026-08-19. A Haiku 4.5 reviewer ran in MANUAL
#:   mode and blocked on every tool call: auto needs a model-side risk
#:   classifier Haiku does not implement, so Claude Code discarded the
#:   requested mode with no warning and no log line (TASKS.md #28).
#: - ``dontAsk``, until 2026-08-22. It does NOT auto-approve. It
#:   suppresses the PROMPT and then DENIES anything that would have
#:   raised one — "Permission to use Edit has been denied because
#:   Claude Code is running in don't ask mode". Read-only Bash still
#:   passed, so planners and reviewers looked healthy while every
#:   writer was refused; a coder burned two turns changing no files
#:   (TASKS.md #39).
#:
#: ``bypassPermissions`` opens a "Yes, I accept" dialog on first launch,
#: which is fatal headless — cleared by pre-seeding
#: ``skipDangerousModePermissionPrompt`` into the VM's Claude settings
#: (see :mod:`sbx_omnigent.claude`), the same way Omnigent already
#: pre-accepts Claude's onboarding and folder-trust gates. The other
#: half of the old objection — that managed settings reject the mode —
#: does not apply here: these microVMs carry no managed-settings file.
#:
#: ``acceptEdits`` remains wrong for its own reason: it auto-approves
#: file EDITS only, so a reviewer's ``git diff`` still prompts.
#:
#: What actually contains a bad tool call is the microVM boundary and a
#: reviewer's ``:ro`` mount — both hold at the kernel regardless of what
#: the agent is permitted to attempt. Omnigent's policy hook
#: deliberately returns "no opinion" on ALLOW so the harness's own
#: permission system still runs (``omnigent/native_policy_hook.py``:
#: emitting ``allow`` "would auto-approve the tool and suppress the
#: harness's native permission prompt"), which is precisely why the
#: harness mode has to carry this.
_YOLO_LAUNCH_ARGS = ('--permission-mode', 'bypassPermissions')

#: agy (Antigravity) equivalent of the YOLO args. agy does NOT accept
#: Claude's ``--permission-mode`` — it exits on the unknown flag at
#: launch (before binding its connect-RPC port), which the executor
#: reports as "the agy terminal is no longer running (the TUI exited)".
#: agy's auto-approve flag is ``--dangerously-skip-permissions``
#: instead. Safe for the same reason (microVM isolation + reviewer :ro).
_AGY_LAUNCH_ARGS = ('--dangerously-skip-permissions',)

#: codex equivalent. It rejects BOTH Claude's ``--permission-mode`` and
#: agy's ``--dangerously-skip-permissions``; this is the flag its own
#: help documents for an externally sandboxed environment. Verified
#: against codex-cli 0.147.0 in an sbx microVM: the turn header reports
#: ``approval: never, sandbox: danger-full-access``.
_CODEX_LAUNCH_ARGS = ('--dangerously-bypass-approvals-and-sandbox',)

#: codex config key for reasoning effort, set via the CLI's ``-c
#: <key=value>`` override because codex-native DROPS the session's
#: ``reasoning_effort``. The value is accepted, persisted, and reported
#: back by the API — ``GET /v1/sessions/<id>`` returned
#: ``"reasoning_effort": "xhigh"`` for a session whose codex status bar
#: read ``gpt-5.6-sol default`` — but nothing applies it to the TUI.
#: Upstream: omnigent-ai/omnigent#2800 (open, ``validated:reproduced``)
#: and #3536; note #3536 asserts native terminals are unaffected
#: because codex-native applies effort via
#: ``thread/settings/update``, which does not hold here.
#:
#: ``-c`` is an IN-MEMORY override — it is not written to the session's
#: ``config.toml``, so Omnigent's ``write_codex_config_model()``
#: cannot clobber it when it pins a model. Verified against
#: codex-cli 0.148.0 in an sbx microVM 2026-08-19: the turn header
#: reports ``model: gpt-5.6-sol xhigh`` with this arg, and
#: ``gpt-5.6-sol default`` without it.
_CODEX_EFFORT_CONFIG_KEY = 'model_reasoning_effort'

#: The role name used to address the coder in the role-based API/CLI.
_CODER_ROLE = 'coder'

#: Default registry directory (per-swarm JSON state for the CLI).
_DEFAULT_REGISTRY = os.path.join(os.path.expanduser('~'), '.sbx-swarm')

#: Env var selecting the default publish mode when the CLI ``--pr`` /
#: ``--no-pr`` flag is not given: ``local`` = push the reviewed branch
#: only (no GitHub PR); anything else (incl. unset) = open a GitHub
#: draft PR. Lets an end user choose once (local shop vs GitHub shop).
_PUBLISH_MODE_ENV = 'OMNI_SBX_PUBLISH_MODE'


def _resolve_open_pr(no_pr_flag: bool | None) -> bool:
    """
    Decide whether publish opens a GitHub PR.

    :param no_pr_flag: The CLI ``--no-pr`` flag — ``True`` (local),
        ``False`` (force PR via ``--pr``), or ``None`` (unset → env).
    :returns: ``True`` to open a GitHub draft PR, ``False`` for local
        push-only mode.
    """
    if no_pr_flag is not None:
        return not no_pr_flag
    return os.environ.get(_PUBLISH_MODE_ENV, '').strip().lower() != 'local'


@dataclass(frozen=True)
class Reviewer:
    """A reviewer agent: its role label and managed session id."""

    role: str
    session: str


@dataclass(frozen=True)
class SwarmHandle:
    """
    A live swarm: its worktree, the coder session, and reviewers.

    :param swarm_id: Swarm identifier (also the worktree dir + branch).
    :param repo_url: The repository the worktree was cut from.
    :param worktree_path: Absolute host path of the shared worktree.
    :param coder_session: The coder's managed session id.
    :param reviewers: The reviewer agents (role + session), each a
        :class:`Reviewer`.
    """

    swarm_id: str
    repo_url: str
    worktree_path: str
    coder_session: str
    reviewers: tuple[Reviewer, ...]

    def session_for(self, role: str) -> str:
        """
        Resolve a role name to its managed session id.

        :param role: ``"coder"`` or a reviewer role label.
        :returns: The session id for that role.
        :raises KeyError: If no agent has that role.
        """
        if role == _CODER_ROLE:
            return self.coder_session
        for reviewer in self.reviewers:
            if reviewer.role == role:
                return reviewer.session
        raise KeyError(f'no swarm agent with role {role!r}')

    def roles(self) -> list[str]:
        """:returns: All addressable role names (coder first)."""
        return [_CODER_ROLE, *(r.role for r in self.reviewers)]


#: Credential kinds a mount sentinel can request, by suffix. A VM is
#: seeded ONLY for the kind its own sentinel names, so a Claude VM is
#: never seeded and an agy VM never receives Codex credentials.
#:
#: This began as a bare ``agy: bool``. Adding Codex as a third harness
#: made the boolean insufficient — the launcher has to know WHICH
#: credential to install, not merely whether to install agy's.
MOUNT_CREDENTIAL_KINDS: tuple[str, ...] = ('agy', 'codex')


def credential_kind_for(harness: str | None) -> str | None:
    """
    Which credential a harness needs seeded into its VM.

    :param harness: The agent's harness id, or ``None`` when unresolved.
    :returns: ``'agy'``, ``'codex'``, or ``None`` for Claude and any
        harness that authenticates some other way.
    """
    if harness in agy.AGY_HARNESSES:
        return 'agy'
    if harness in codex.CODEX_HARNESSES:
        return 'codex'
    return None


def mount_sentinel(
    worktree_path: str,
    mode: str,
    *,
    credential: str | None = None,
) -> str:
    """
    Build the launcher mount sentinel for a worktree + mode.

    :param worktree_path: Absolute host worktree path (under the
        server's configured ``sbx.worktree_root``).
    :param mode: ``"rw"`` (coder) or ``"ro"`` (reviewer).
    :param credential: Which credential the launcher should seed into
        THIS VM — one of :data:`MOUNT_CREDENTIAL_KINDS`, or ``None`` to
        seed nothing (Claude). Tagged as a ``-<kind>`` suffix on the
        mode fragment, which round-trips Omnigent's branch validation.
    :returns: e.g. ``"git@sbxmount:/srv/worktrees/a#ro"``, or
        ``"…#rw-agy"`` / ``"…#rw-codex"`` when *credential* is set.
    :raises ValueError: If *mode* is not ``"rw"``/``"ro"``, or
        *credential* is not a known kind.
    """
    if mode not in ('rw', 'ro'):
        raise ValueError(f"mount mode must be 'rw' or 'ro', got {mode!r}")
    if credential is not None and credential not in MOUNT_CREDENTIAL_KINDS:
        raise ValueError(
            f'unknown mount credential {credential!r}; expected one of '
            f'{", ".join(MOUNT_CREDENTIAL_KINDS)} or None'
        )
    fragment = f'{mode}-{credential}' if credential else mode
    return f'git@sbxmount:{worktree_path}#{fragment}'


def last_assistant_text(client: SwarmSessionClient, session_id: str) -> str:
    """
    Return the most recent assistant message text of a session.

    :param client: The session driver.
    :param session_id: The session to read.
    :returns: The last assistant message's concatenated text, or ``""``.
    """
    texts: list[str] = []
    for item in client.read_items(session_id, tail=12):
        if item.get('type') != 'message' or item.get('role') != 'assistant':
            continue
        parts: list[str] = []
        for block in item.get('content') or []:
            if isinstance(block, dict):
                text = block.get('text') or block.get('output_text')
                if isinstance(text, str) and text:
                    parts.append(text)
        if parts:
            texts.append('\n'.join(parts))
    return texts[-1] if texts else ''


def _turn_note(
    result: SwarmTurnResult,
    reply: str,
    last_task_error: object,
) -> str | None:
    """
    A human-facing note when a turn looks off (esp. a rate limit).

    Distinguishes an infra hiccup — most often an LLM usage/rate limit
    that silently cut the turn short — from a real task failure, so a
    coordinator (or the human) doesn't chase a phantom bug. Scans the
    turn error, the session's ``last_task_error``, and the reply for
    rate-limit markers; also flags an empty reply and a plain ``failed``
    status.

    :param result: The terminal turn result.
    :param reply: The agent's reply text (may be ``""``).
    :param last_task_error: The session's ``last_task_error``, if any.
    :returns: A short note, or ``None`` when the turn looks normal.
    """
    err = last_task_error if isinstance(last_task_error, str) else None
    if looks_like_rate_limit(result.error, err, reply):
        return (
            'This turn shows signs of an LLM usage/rate limit, not a code '
            'bug — the model may have been throttled and the turn cut '
            'short. Check your Claude plan usage; the swarm is left '
            'registered so you can resume or dispose it, and you can retry '
            'the turn once capacity returns.'
        )
    if not result.ok:
        return (
            f'Turn ended with status {result.status!r}. This can be a '
            'transient/infra issue (including a usage/rate limit) rather '
            'than a task failure — inspect the error and consider a retry '
            'before treating it as a real failure.'
        )
    if not reply.strip():
        return (
            'Turn completed but the agent produced no reply text. This is '
            'often an interrupted turn (e.g. a usage/rate limit) rather '
            'than a real empty result — consider retrying.'
        )
    return None


def _dedupe_roles(roles: list[str]) -> list[str]:
    """
    Validate reviewer role names: non-empty, unique, not ``"coder"``.

    :param roles: Requested reviewer role labels.
    :returns: The roles unchanged when valid.
    :raises click.ClickException: On an empty, duplicate, or reserved
        role.
    """
    seen: set[str] = set()
    for role in roles:
        if not role or role == _CODER_ROLE:
            raise click.ClickException(
                f'invalid reviewer role {role!r} '
                f"(must be non-empty and not '{_CODER_ROLE}')"
            )
        if role in seen:
            raise click.ClickException(f'duplicate reviewer role {role!r}')
        seen.add(role)
    return roles


class SwarmOrchestrator:
    """
    Spin, drive, and tear down an A-prime swarm (coder + reviewers).

    :param session_client: Managed-session driver.
    :param worktree_manager: Worktree lifecycle manager. Its
        ``worktree_root`` MUST match the server's ``sbx.worktree_root``
        so the launcher allows the mount.
    :param coder_agent_id: Registered agent id for the coder.
    :param reviewer_agent_id: Default agent id bound to every reviewer
        role when :meth:`start_swarm` is called with ``reviewer_roles``
        (the single-spec path). Optional — omit it when you always pass
        an explicit per-role ``reviewers`` map instead.
    :param agent_launch_args: Native-terminal args for an agent whose
        harness is NOT in *agent_harnesses* (the Claude/codex default —
        see :data:`_YOLO_LAUNCH_ARGS`).
    :param agent_harnesses: Optional ``{agent-ref: harness}`` map (ids
        and/or names, from ``GET /v1/agents``). When given, each session
        gets harness-appropriate launch args (agy needs
        :data:`_AGY_LAUNCH_ARGS`, not Claude's ``--permission-mode``);
        when ``None``, every agent gets *agent_launch_args*.
    :param agent_models: Optional ``{agent-ref: model}`` map. When a
        bound agent has an entry, its session is created with that model
        pinned (``model_override``) — the Polly-style per-dispatch pin
        that reaches native harnesses too. Absent ref = spec default.
    :param agent_efforts: Optional ``{agent-ref: reasoning_effort}``
        map, applied the same way (``reasoning_effort`` at create).
        Honored by the Claude harnesses. Codex needs it a second way —
        it ignores the persisted value, so the same effort also rides
        its launch args (see :data:`_CODEX_EFFORT_CONFIG_KEY`). agy has
        no effort knob: for agy the effort IS the model id.
    """

    def __init__(
        self,
        *,
        session_client: SwarmSessionClient,
        worktree_manager: WorktreeManager,
        coder_agent_id: str,
        reviewer_agent_id: str = '',
        agent_launch_args: tuple[str, ...] = _YOLO_LAUNCH_ARGS,
        agent_harnesses: dict[str, str] | None = None,
        agent_models: dict[str, str] | None = None,
        agent_efforts: dict[str, str] | None = None,
    ) -> None:
        self._sc = session_client
        self._wt = worktree_manager
        self._coder_agent = coder_agent_id
        self._reviewer_agent = reviewer_agent_id
        self._agent_launch_args = list(agent_launch_args)
        self._agent_harnesses = agent_harnesses
        self._agent_models = agent_models or {}
        self._agent_efforts = agent_efforts or {}

    def _launch_args_for_agent(self, agent_id: str) -> list[str]:
        """
        The native-terminal launch args for one bound agent.

        Harness-appropriate when an ``agent_harnesses`` map was provided
        (agy vs codex vs Claude), else the constructor's default for
        every agent. A codex agent also carries its own pinned effort
        here — that is the only channel that reaches codex-native, so it
        is resolved PER AGENT rather than per harness (see
        :data:`_CODEX_EFFORT_CONFIG_KEY`).

        :param agent_id: The agent ref (id or name) being launched.
        :returns: The launch args for that agent.
        """
        if self._agent_harnesses is None:
            return self._agent_launch_args
        return list(
            _launch_args_for(
                self._agent_harnesses.get(agent_id),
                self._effort_for_agent(agent_id),
            )
        )

    def _is_agy_agent(self, agent_id: str) -> bool:
        """
        Whether *agent_id* runs the agy (Antigravity) harness.

        Drives the ``-agy`` mount-sentinel tag so the launcher seeds agy
        credentials into that agent's VM only. ``False`` when no harness
        map was provided (never tag on uncertainty).

        :param agent_id: The bound agent ref (id or name).
        :returns: ``True`` iff the harness is an agy one.
        """
        if self._agent_harnesses is None:
            return False
        return self._agent_harnesses.get(agent_id) in agy.AGY_HARNESSES

    def _credential_for_agent(self, agent_id: str) -> str | None:
        """
        Which credential this agent's VM needs seeded.

        Returns ``None`` when no harness map was provided — never seed
        on uncertainty, the same rule :meth:`_is_agy_agent` follows.

        :param agent_id: The bound agent.
        :returns: ``'agy'``, ``'codex'``, or ``None`` (Claude, or
            unknown).
        """
        if self._agent_harnesses is None:
            return None
        return credential_kind_for(self._agent_harnesses.get(agent_id))

    def _model_for_agent(self, agent_id: str) -> str | None:
        """Pinned model for *agent_id*, or ``None`` (spec default)."""
        return self._agent_models.get(agent_id)

    def _effort_for_agent(self, agent_id: str) -> str | None:
        """The pinned reasoning effort for *agent_id*, or ``None``."""
        return self._agent_efforts.get(agent_id)

    def _resolve_reviewer_agents(
        self,
        reviewer_roles: tuple[str, ...],
        reviewers: dict[str, str] | None,
    ) -> dict[str, str]:
        """
        Resolve the ``{role: agent_id}`` map for a swarm's reviewers.

        Two mutually exclusive sources: an explicit per-role *reviewers*
        map (a specialist team, distinct agent per role), or
        *reviewer_roles* bound to the constructor's default
        ``reviewer_agent_id`` (one spec for every role). Validates role
        names (non-empty, unique, not ``"coder"``) and that every role
        has a non-empty agent id.

        :param reviewer_roles: Role labels for the single-spec path.
        :param reviewers: Explicit role→agent_id map, or ``None``.
        :returns: The resolved, insertion-ordered role→agent_id map.
        :raises click.ClickException: On a bad/duplicate role, a missing
            agent id, or no reviewer agent for the single-spec path.
        """
        if reviewers is not None:
            # Dict keys are unique; still validate name rules.
            role_agents = dict(reviewers)
            _dedupe_roles(list(role_agents))
        else:
            if not self._reviewer_agent:
                raise click.ClickException(
                    'no reviewer agent configured — pass a per-role '
                    "'reviewers' map or set reviewer_agent_id"
                )
            # Validate the RAW list first so a duplicate role is caught
            # before it silently collapses into a single dict key.
            _dedupe_roles(list(reviewer_roles))
            role_agents = dict.fromkeys(reviewer_roles, self._reviewer_agent)
        for role, agent_id in role_agents.items():
            if not agent_id:
                raise click.ClickException(
                    f'reviewer role {role!r} has no agent id'
                )
        return role_agents

    def start_swarm(
        self,
        swarm_id: str,
        repo_url: str,
        *,
        base_branch: str | None = None,
        reviewer_roles: tuple[str, ...] = ('reviewer',),
        reviewers: dict[str, str] | None = None,
    ) -> SwarmHandle:
        """
        Cut the worktree and spin the coder (rw) + reviewer (:ro) VMs.

        All sessions are created up front so their microVMs provision
        concurrently; the first turn waits out any remaining provision.
        On a partial failure the worktree and every created session are
        cleaned up before re-raising.

        :param swarm_id: Swarm identifier.
        :param repo_url: Repository to cut the worktree from.
        :param base_branch: Branch to cut from; ``None`` = manager
            default.
        :param reviewer_roles: Role labels bound to the default reviewer
            agent (single-spec path). Ignored when *reviewers* is given.
        :param reviewers: Explicit ``{role: agent_id}`` map — one
            distinct specialist agent per role (a team). Overrides
            *reviewer_roles*.
        :returns: The live :class:`SwarmHandle`.
        :raises SwarmSessionError: If a session cannot be created.
        """
        role_agents = self._resolve_reviewer_agents(reviewer_roles, reviewers)
        worktree = self._wt.create_swarm_worktree(
            swarm_id, repo_url, base_branch
        )
        created: list[str] = []
        try:
            coder = self._sc.create(
                agent_id=self._coder_agent,
                workspace=mount_sentinel(
                    worktree, 'rw',
                    credential=self._credential_for_agent(
                        self._coder_agent
                    ),
                ),
                title=f'{swarm_id}/{_CODER_ROLE}',
                terminal_launch_args=self._launch_args_for_agent(
                    self._coder_agent
                ),
                model_override=self._model_for_agent(self._coder_agent),
                reasoning_effort=self._effort_for_agent(self._coder_agent),
            )
            created.append(coder)
            reviewer_list: list[Reviewer] = []
            for role, agent_id in role_agents.items():
                sid = self._sc.create(
                    agent_id=agent_id,
                    workspace=mount_sentinel(
                        worktree, 'ro',
                        credential=self._credential_for_agent(agent_id),
                    ),
                    title=f'{swarm_id}/{role}',
                    terminal_launch_args=self._launch_args_for_agent(
                        agent_id
                    ),
                    model_override=self._model_for_agent(agent_id),
                    reasoning_effort=self._effort_for_agent(agent_id),
                )
                created.append(sid)
                reviewer_list.append(Reviewer(role=role, session=sid))
        except Exception:
            for sid in created:
                self._dispose_quietly(sid)
            self._wt.dispose_swarm(swarm_id)
            raise
        return SwarmHandle(
            swarm_id=swarm_id,
            repo_url=repo_url,
            worktree_path=worktree,
            coder_session=coder,
            reviewers=tuple(reviewer_list),
        )

    def send(
        self,
        handle: SwarmHandle,
        role: str,
        instruction: str,
        *,
        timeout: float = _DEFAULT_TURN_TIMEOUT_S,
    ) -> SwarmTurnResult:
        """
        Drive one turn for the agent with *role*; block until done.

        :param handle: The live swarm.
        :param role: ``"coder"`` or a reviewer role label.
        :param instruction: The user message to send.
        :param timeout: Max seconds to await completion.
        :returns: The terminal :class:`SwarmTurnResult`.
        """
        return self._sc.send_and_wait(
            handle.session_for(role), instruction, timeout=timeout
        )

    def reply(self, handle: SwarmHandle, role: str) -> str:
        """
        Read *role*'s latest reply WITHOUT sending a new turn.

        For a turn you just drove, prefer the ``reply`` on the
        :class:`SwarmTurnResult` from :meth:`send` — it is captured
        against a pre-turn snapshot so it can't return a stale
        prior-turn message. This convenience reads whatever is latest.

        :returns: The most recent assistant text, or ``""``.
        """
        return last_assistant_text(self._sc, handle.session_for(role))

    # Convenience wrappers for the common two roles.

    def run_coder(
        self,
        handle: SwarmHandle,
        instruction: str,
        *,
        timeout: float = _DEFAULT_TURN_TIMEOUT_S,
    ) -> SwarmTurnResult:
        """Drive one coder turn."""
        return self.send(handle, _CODER_ROLE, instruction, timeout=timeout)

    def run_reviewer(
        self,
        handle: SwarmHandle,
        instruction: str,
        *,
        role: str = 'reviewer',
        timeout: float = _DEFAULT_TURN_TIMEOUT_S,
    ) -> SwarmTurnResult:
        """Drive one reviewer turn (reads the live :ro tree)."""
        return self.send(handle, role, instruction, timeout=timeout)

    def coder_reply(self, handle: SwarmHandle) -> str:
        """:returns: The coder's most recent assistant text."""
        return self.reply(handle, _CODER_ROLE)

    def reviewer_reply(
        self, handle: SwarmHandle, *, role: str = 'reviewer'
    ) -> str:
        """:returns: A reviewer's most recent assistant text."""
        return self.reply(handle, role)

    def commit(
        self,
        handle: SwarmHandle,
        message: str,
        *,
        author: str | None = None,
    ) -> bool:
        """
        Commit the swarm worktree's approved state on the host.

        The trusted plane owns git: the coder only edits files in its
        ``rw`` mount, and this turns the reviewed working tree into a
        commit (attributed to *author* if given). Call after consensus,
        before :meth:`publish`.

        :param handle: The live swarm.
        :param message: Commit message.
        :param author: Optional ``"Name <email>"`` recorded as the
            commit author (e.g. the coder identity).
        :returns: ``True`` if a commit was created, ``False`` on a clean
            tree (the coder produced no changes).
        """
        return self._wt.commit_worktree(
            handle.swarm_id, message=message, author=author
        )

    def publish(
        self,
        handle: SwarmHandle,
        *,
        title: str,
        body: str,
        base_branch: str | None = None,
        draft: bool = True,
        open_pr: bool = True,
    ) -> str:
        """
        Publish the swarm's approved task branch.

        Assumes the approved work is already committed (see
        :meth:`commit`). With ``open_pr`` (GitHub mode) it opens a draft
        PR; without it (local mode) it pushes the branch only — see
        :meth:`WorktreeManager.publish_swarm`.

        :returns: The PR URL (GitHub) or a pushed-branch summary
            (local).
        """
        return self._wt.publish_swarm(
            handle.swarm_id,
            handle.repo_url,
            title=title,
            body=body,
            base_branch=base_branch,
            draft=draft,
            open_pr=open_pr,
        )

    def teardown(
        self, handle: SwarmHandle, *, dispose_worktree: bool = True
    ) -> None:
        """
        Dispose every microVM session and (by default) the worktree.

        Best-effort on the sessions so one failure can't strand another
        VM or the worktree.

        :param handle: The swarm to tear down.
        :param dispose_worktree: Remove the host worktree too.
        """
        self._dispose_quietly(handle.coder_session)
        for reviewer in handle.reviewers:
            self._dispose_quietly(reviewer.session)
        if dispose_worktree:
            self._wt.dispose_swarm(handle.swarm_id)

    def _dispose_quietly(self, session_id: str) -> None:
        """Dispose a session, swallowing a dispose failure."""
        try:
            self._sc.dispose(session_id)
        except SwarmSessionError:
            pass


# ── Registry (per-swarm JSON state for the CLI) ───────────────────


class SwarmRegistry:
    """
    File-backed registry of live swarms (one JSON per swarm).

    Lets the stateless CLI reconstruct a swarm from just its id across
    separate ``sys_os_shell`` invocations. Each entry stores the handle
    plus the config (server URL + roots) needed to rebuild the driver.

    :param root: Directory holding ``<swarm_id>.json`` files.
    """

    def __init__(self, root: str) -> None:
        self._root = root

    def _path(self, swarm_id: str) -> str:
        return os.path.join(self._root, f'{swarm_id}.json')

    def save(self, entry: dict[str, object]) -> None:
        """Persist a swarm entry (keyed by its ``swarm_id``)."""
        os.makedirs(self._root, exist_ok=True)
        swarm_id = str(entry['swarm_id'])
        with open(self._path(swarm_id), 'w', encoding='utf-8') as fh:
            json.dump(entry, fh, indent=2)

    def load(self, swarm_id: str) -> dict[str, object]:
        """
        Load a swarm entry by id.

        :raises click.ClickException: If no such swarm is registered.
        """
        try:
            with open(self._path(swarm_id), encoding='utf-8') as fh:
                data: dict[str, object] = json.load(fh)
        except FileNotFoundError as exc:
            raise click.ClickException(
                f'no registered swarm {swarm_id!r} (in {self._root})'
            ) from exc
        return data

    def remove(self, swarm_id: str) -> None:
        """Delete a swarm entry (idempotent)."""
        try:
            os.remove(self._path(swarm_id))
        except FileNotFoundError:
            pass

    def list_ids(self) -> list[str]:
        """:returns: Registered swarm ids, sorted."""
        if not os.path.isdir(self._root):
            return []
        return sorted(
            name[:-5]
            for name in os.listdir(self._root)
            if name.endswith('.json')
        )


def handle_to_entry(
    handle: SwarmHandle,
    *,
    server: str,
    canonical_root: str,
    worktree_root: str,
) -> dict[str, object]:
    """Serialize a handle + its config into a registry entry."""
    return {
        'swarm_id': handle.swarm_id,
        'repo_url': handle.repo_url,
        'worktree_path': handle.worktree_path,
        'coder_session': handle.coder_session,
        'reviewers': [
            {'role': r.role, 'session': r.session} for r in handle.reviewers
        ],
        'server': server,
        'canonical_root': canonical_root,
        'worktree_root': worktree_root,
    }


def handle_from_entry(entry: dict[str, object]) -> SwarmHandle:
    """Rebuild a :class:`SwarmHandle` from a registry entry."""
    reviewers = tuple(
        Reviewer(role=str(r['role']), session=str(r['session']))
        for r in entry.get('reviewers', [])  # type: ignore[union-attr]
    )
    return SwarmHandle(
        swarm_id=str(entry['swarm_id']),
        repo_url=str(entry['repo_url']),
        worktree_path=str(entry['worktree_path']),
        coder_session=str(entry['coder_session']),
        reviewers=reviewers,
    )


# ── CLI (the coordinator drives this via sys_os_shell) ────────────


def _parse_reviewer_specs(entries: tuple[str, ...]) -> dict[str, str]:
    """
    Parse ``--reviewer role=agent_id`` entries into a role→id map.

    :param entries: Raw ``"role=agent_id"`` strings (repeatable flag).
    :returns: The parsed map, insertion-ordered; empty when *entries* is
        empty.
    :raises click.UsageError: On a malformed entry or a duplicate role.
    """
    specs: dict[str, str] = {}
    for entry in entries:
        role, sep, agent = entry.partition('=')
        role, agent = role.strip(), agent.strip()
        if not sep or not role or not agent:
            raise click.UsageError(
                f"--reviewer must be 'role=agent_id', got {entry!r}"
            )
        if role in specs:
            raise click.UsageError(f'duplicate --reviewer role {role!r}')
        specs[role] = agent
    return specs


def _agy_ack_enabled(flag: bool | None) -> bool:
    """
    Resolve whether the operator has acknowledged agy support.

    :param flag: The ``--agy/--no-agy`` value (``None`` when unset).
    :returns: *flag* when explicitly given, else the truthiness of the
        ``OMNI_SBX_AGY_ENABLED`` environment variable.
    """
    if flag is not None:
        return flag
    return os.environ.get('OMNI_SBX_AGY_ENABLED', '').strip().lower() in (
        '1',
        'true',
        'yes',
        'on',
    )


def _harness_by_ref(agents: list[dict[str, object]]) -> dict[str, str]:
    """
    Map each catalog agent's id AND name to its harness.

    :param agents: catalog dicts, each ``{id, name, harness}``.
    :returns: ``{ref: harness}`` keyed by both id and name (string
        harnesses only).
    """
    out: dict[str, str] = {}
    for agent in agents:
        harness = agent.get('harness')
        if not isinstance(harness, str):
            continue
        for key in ('id', 'name'):
            ref = agent.get(key)
            if isinstance(ref, str) and ref:
                out[ref] = harness
    return out


#: Env override for the bundle root the swarm CLI reads per-agent
#: ``executor.model`` / ``llm.reasoning_effort`` from. Defaults to the
#: packaged ``agents/`` (same resolution the launcher uses to register
#: bundles), so an editable install needs no configuration.
_AGENTS_DIR_ENV = 'OMNI_SBX_AGENTS_DIR'


def _bundled_agents_dir() -> Path:
    """
    The bundle root holding ``<name>/config.yaml`` agent dirs.

    From :data:`_AGENTS_DIR_ENV` when set, else package-relative
    (mirrors the launcher's ``_bundled_agent_dirs``) so it works for the
    editable install the README prescribes regardless of cwd.

    :returns: The agents-directory path (may not exist).
    """
    override = os.environ.get(_AGENTS_DIR_ENV)
    if override:
        return Path(override)
    return Path(__file__).resolve().parent.parent / 'agents'


def _spec_model_effort(config_path: Path) -> tuple[str | None, str | None]:
    """
    Read ``executor.model`` + ``llm.reasoning_effort`` from a bundle.

    Model falls back to ``llm.model`` (the parser treats either as the
    executor model). Best-effort: an absent, unreadable, or malformed
    bundle yields ``(None, None)`` so a swarm never fails to start
    over a model hint.

    :param config_path: The bundle's ``config.yaml`` path.
    :returns: ``(model, reasoning_effort)`` — each ``None`` when the
        bundle declares no non-empty string value.
    """
    try:
        raw = yaml.safe_load(config_path.read_text(encoding='utf-8'))
    except (OSError, yaml.YAMLError):
        return None, None
    if not isinstance(raw, dict):
        return None, None
    executor = raw.get('executor')
    llm = raw.get('llm')

    def _str(container: object, key: str) -> str | None:
        if isinstance(container, dict):
            val = container.get(key)
            if isinstance(val, str) and val:
                return val
        return None

    model = _str(executor, 'model') or _str(llm, 'model')
    effort = _str(llm, 'reasoning_effort')
    return model, effort


def _model_effort_by_ref(
    agents: list[dict[str, object]], agents_dir: Path | None = None
) -> tuple[dict[str, str], dict[str, str]]:
    """
    Map each catalog agent's id AND name to its declared model/effort.

    Reads the values from the agent's bundle
    ``<agents_dir>/<name>/config.yaml`` so the swarm can forward them at
    session create as ``model_override`` / ``reasoning_effort`` — the
    Polly-style per-dispatch pin that reaches even the native harnesses
    (which ignore a spec-declared model). An agent with no bundle or no
    declared value is simply absent from the maps (no override).

    :param agents: catalog dicts, each ``{id, name, harness}``.
    :param agents_dir: bundle root; defaults to packaged ``agents/``.
    :returns: ``({ref: model}, {ref: effort})`` keyed by id and name.
    """
    root = agents_dir if agents_dir is not None else _bundled_agents_dir()
    models: dict[str, str] = {}
    efforts: dict[str, str] = {}
    for agent in agents:
        name = agent.get('name')
        if not isinstance(name, str) or not name:
            continue
        model, effort = _spec_model_effort(root / name / 'config.yaml')
        if model is None and effort is None:
            continue
        refs = [
            ref
            for ref in (agent.get('id'), name)
            if isinstance(ref, str) and ref
        ]
        for ref in refs:
            if model is not None:
                models[ref] = model
            if effort is not None:
                efforts[ref] = effort
    return models, efforts


def _launch_args_for(
    harness: str | None, effort: str | None = None
) -> tuple[str, ...]:
    """
    YOLO (no-prompt) native-terminal launch args for a harness.

    Every CLI spells this differently and REJECTS the others\' spelling,
    so an unknown flag is not ignored — the process exits at launch:

    * agy (``antigravity-native``) rejects ``--permission-mode``
    * codex rejects it too, with ``error: unexpected argument
      \'--permission-mode\' found`` and exit 2. This function used to
      claim codex accepted Claude\'s flag; it does not, and every Codex
      agent would have died at launch. Its own help says
      ``--dangerously-bypass-approvals-and-sandbox`` is "intended solely
      for running in environments that are externally sandboxed", which
      is exactly what the microVM is.
    * Claude native (and any unresolved harness) take
      ``--permission-mode auto``

    Codex additionally carries its reasoning effort here, because that
    is the only channel that reaches it — see
    :data:`_CODEX_EFFORT_CONFIG_KEY`. An effort outside codex\'s own
    ladder is DROPPED rather than passed: the value is interpolated into
    a ``-c key=value`` config expression, so an unvalidated one
    would reach the CLI as config syntax. Claude and agy ignore
    *effort* here — Claude gets ``--effort`` from Omnigent\'s own
    launch path (verified: the session transcript records
    ``"effort":"xhigh"``), and agy has no effort knob at all, since
    for agy the effort IS the model id (``gemini-3.7-flash-high``).

    :param harness: The agent\'s harness id, or ``None`` when
        unresolved.
    :param effort: The agent\'s pinned reasoning effort, or ``None``.
        Used only for codex, and only when codex accepts it.
    :returns: The launch args for that harness.
    """
    if harness in agy.AGY_HARNESSES:
        return _AGY_LAUNCH_ARGS
    if harness in codex.CODEX_HARNESSES:
        if effort in CODEX_EFFORTS:
            return (
                *_CODEX_LAUNCH_ARGS,
                '-c',
                f'{_CODEX_EFFORT_CONFIG_KEY}="{effort}"',
            )
        return _CODEX_LAUNCH_ARGS
    return _YOLO_LAUNCH_ARGS


def _detect_agy_bindings(
    agents: list[dict[str, object]], bound_ids: list[str]
) -> list[str]:
    """
    Return the bound agent refs that use the agy (Antigravity) harness.

    Resolves each ref against the ``GET /v1/agents`` catalog by id OR
    name and keeps those whose harness is in :data:`agy.AGY_HARNESSES`.
    An unresolvable ref is ignored — best-effort, since the in-VM
    readiness gate is the real enforcement.

    :param agents: Catalog dicts, each ``{id, name, harness}``.
    :param bound_ids: Agent refs bound to this swarm (ids or names).
    :returns: The subset of *bound_ids* resolving to an agy agent, in
        input order, deduplicated.
    """
    harness_by_ref = _harness_by_ref(agents)
    seen: set[str] = set()
    agy_bound: list[str] = []
    for ref in bound_ids:
        if ref in seen:
            continue
        seen.add(ref)
        if harness_by_ref.get(ref) in agy.AGY_HARNESSES:
            agy_bound.append(ref)
    return agy_bound


def _guard_agy_bindings(
    agents: list[dict[str, object]], bound_ids: list[str]
) -> None:
    """
    Fail loud if an agy agent is bound without agy support acknowledged.

    :param agents: The built-in agent catalog (``GET /v1/agents``). An
        empty list (e.g. the lookup failed) detects nothing — the in-VM
        readiness gate remains the authoritative enforcement.
    :param bound_ids: Agent refs (ids or names) bound to this swarm.
    :raises click.UsageError: If an agy agent is bound (with guidance on
        enabling agy support).
    """
    agy_bound = _detect_agy_bindings(agents, bound_ids)
    if not agy_bound:
        return
    raise click.UsageError(
        f'agy (Antigravity) agent(s) {", ".join(sorted(agy_bound))} are '
        'bound, but agy support is not acknowledged. Antigravity agents '
        'authenticate via the harvest/proxy-swap path, which requires: '
        "(1) server config 'sandbox.sbx.agy_enabled: true', and (2) a "
        'running token harvester (`omni-sbx-agy harvest`). Once both are '
        'in place, re-run with --agy (or set OMNI_SBX_AGY_ENABLED=1).'
    )


def _read_message(message: str | None, message_file: str | None) -> str:
    """
    Read a turn message from ``--message`` or ``--message-file``.

    A *message_file* of ``-`` reads stdin (the usual CLI convention) so
    a coordinator can pipe a long, multi-line turn in without quoting it
    on argv — otherwise ``open('-')`` raises ``FileNotFoundError``.
    """
    if message is not None and message_file is not None:
        raise click.UsageError('pass only one of --message / --message-file')
    if message is not None:
        return message
    if message_file is not None:
        if message_file == '-':
            return sys.stdin.read()
        with open(message_file, encoding='utf-8') as fh:
            return fh.read()
    raise click.UsageError('one of --message / --message-file is required')


@click.group()
@click.option(
    '--registry',
    envvar='SBX_SWARM_REGISTRY',
    default=_DEFAULT_REGISTRY,
    show_default=True,
    help='Directory holding per-swarm state.',
)
@click.pass_context
def cli(ctx: click.Context, registry: str) -> None:
    """Coordinate A-prime swarms (coder rw + reviewers :ro)."""
    ctx.obj = SwarmRegistry(registry)


@cli.command('start')
@click.option('--swarm-id', required=True)
@click.option('--repo-url', required=True)
@click.option(
    '--server', envvar='OMNI_SERVER', default='http://localhost:6767'
)
@click.option(
    '--canonical-root',
    envvar='OMNI_SBX_CANONICAL_ROOT',
    required=True,
)
@click.option(
    '--worktree-root',
    envvar='OMNI_SBX_WORKTREE_ROOT',
    required=True,
    help='MUST match the server sbx.worktree_root.',
)
@click.option('--coder-agent', envvar='OMNI_SBX_CODER_AGENT', required=True)
@click.option(
    '--reviewer-agent',
    envvar='OMNI_SBX_REVIEWER_AGENT',
    default=None,
    help=(
        'Default agent for reviewer role(s) (single-spec path, with '
        '--reviewer-role). Omit when using per-role --reviewer.'
    ),
)
@click.option(
    '--reviewer-role',
    'reviewer_roles',
    multiple=True,
    help='Reviewer role label bound to --reviewer-agent; repeatable.',
)
@click.option(
    '--reviewer',
    'reviewer_specs_raw',
    multiple=True,
    help=(
        "Per-role specialist: 'role=agent_id' (repeatable). A distinct "
        'agent per role; use instead of --reviewer-agent/--reviewer-role.'
    ),
)
@click.option('--base-branch', default=None)
@click.option(
    '--agy/--no-agy',
    'agy_ack',
    default=None,
    help=(
        'Acknowledge that agy (Antigravity) support is enabled '
        'server-side (sbx.agy_enabled) and a token harvester is running. '
        'Defaults to OMNI_SBX_AGY_ENABLED. When unacknowledged and an agy '
        'agent is bound, start fails loud instead of a cryptic in-VM '
        'auth failure later.'
    ),
)
@click.pass_obj
def _start(
    registry: SwarmRegistry,
    swarm_id: str,
    repo_url: str,
    server: str,
    canonical_root: str,
    worktree_root: str,
    coder_agent: str,
    reviewer_agent: str | None,
    reviewer_roles: tuple[str, ...],
    reviewer_specs_raw: tuple[str, ...],
    base_branch: str | None,
    agy_ack: bool | None,
) -> None:
    """Cut a worktree, spin coder (rw) + reviewer(s) (:ro), register."""
    reviewer_specs = _parse_reviewer_specs(reviewer_specs_raw)
    # Two mutually exclusive ways to bind reviewers: an explicit
    # per-role map (a team) or one default agent across roles.
    if reviewer_specs and (reviewer_agent or reviewer_roles):
        raise click.UsageError(
            'use per-role --reviewer OR --reviewer-agent/--reviewer-role, '
            'not both'
        )
    if not reviewer_specs and not reviewer_agent:
        raise click.UsageError(
            'provide --reviewer role=agent_id (per-role), or '
            '--reviewer-agent (single spec for all reviewer roles)'
        )
    # Resolve bound agents' harnesses once (best-effort) — used both for
    # the fail-loud gate and to pick per-agent launch args (agy rejects
    # Claude's --permission-mode and would exit at launch).
    bound_ids = [
        coder_agent,
        *([reviewer_agent] if reviewer_agent else []),
        *reviewer_specs.values(),
    ]
    client = SwarmSessionClient(server)
    try:
        catalog = client.list_builtin_agents()
    except SwarmSessionError:
        catalog = []
    if not _agy_ack_enabled(agy_ack):
        _guard_agy_bindings(catalog, bound_ids)
    # Per-agent model/effort come from each bound agent's bundle
    # config.yaml and ride the session-create body (model_override /
    # reasoning_effort) — the Polly pin that reaches native harnesses.
    agent_models, agent_efforts = _model_effort_by_ref(catalog)
    orch = SwarmOrchestrator(
        session_client=client,
        worktree_manager=WorktreeManager(
            canonical_root=canonical_root, worktree_root=worktree_root
        ),
        coder_agent_id=coder_agent,
        reviewer_agent_id=reviewer_agent or '',
        agent_harnesses=_harness_by_ref(catalog),
        agent_models=agent_models,
        agent_efforts=agent_efforts,
    )
    handle = orch.start_swarm(
        swarm_id,
        repo_url,
        base_branch=base_branch,
        reviewer_roles=reviewer_roles or ('reviewer',),
        reviewers=reviewer_specs or None,
    )
    entry = handle_to_entry(
        handle,
        server=server,
        canonical_root=canonical_root,
        worktree_root=worktree_root,
    )
    registry.save(entry)
    click.echo(json.dumps(entry, indent=2))


@cli.command('send')
@click.option('--swarm-id', required=True)
@click.option('--role', required=True, help="'coder' or a reviewer role.")
@click.option('--message', default=None)
@click.option('--message-file', default=None)
@click.option('--timeout', type=float, default=_DEFAULT_TURN_TIMEOUT_S)
@click.pass_obj
def _send(
    registry: SwarmRegistry,
    swarm_id: str,
    role: str,
    message: str | None,
    message_file: str | None,
    timeout: float,
) -> None:
    """Send a turn to one agent; print {status, reply, note} JSON."""
    entry = registry.load(swarm_id)
    handle = handle_from_entry(entry)
    client = SwarmSessionClient(str(entry['server']))
    session = handle.session_for(role)
    result = client.send_and_wait(
        session, _read_message(message, message_file), timeout=timeout
    )
    # THIS turn's reply, captured by send_and_wait (id-newer-than the
    # pre-turn snapshot) so a multi-round loop never reads the prior
    # round's message.
    reply = result.reply
    # A turn that ended early/empty/failed may be a usage/rate limit,
    # not a bug — scan the error, last_task_error, and the reply so the
    # caller (and the human) can tell the two apart.
    last_task_error = None
    try:
        last_task_error = client.get_status(session).get('last_task_error')
    except SwarmSessionError:
        pass
    note = _turn_note(result, reply, last_task_error)
    payload: dict[str, object] = {
        'role': role,
        'status': result.status,
        'error': result.error,
        'reply': reply,
    }
    if note:
        payload['note'] = note
    click.echo(json.dumps(payload, indent=2))
    if note:
        click.echo(f'note: {note}', err=True)
    if not result.ok:
        raise SystemExit(1)


@cli.command('reply')
@click.option('--swarm-id', required=True)
@click.option('--role', required=True)
@click.pass_obj
def _reply(registry: SwarmRegistry, swarm_id: str, role: str) -> None:
    """Print an agent's latest reply without sending a new turn."""
    entry = registry.load(swarm_id)
    handle = handle_from_entry(entry)
    client = SwarmSessionClient(str(entry['server']))
    click.echo(last_assistant_text(client, handle.session_for(role)))


@cli.command('commit')
@click.option('--swarm-id', required=True)
@click.option('--message', required=True)
@click.option(
    '--author', default=None, help="Commit author 'Name <email>' (the coder)."
)
@click.pass_obj
def _commit(
    registry: SwarmRegistry,
    swarm_id: str,
    message: str,
    author: str | None,
) -> None:
    """Commit the worktree's approved state on the host."""
    entry = registry.load(swarm_id)
    wt = WorktreeManager(
        canonical_root=str(entry['canonical_root']),
        worktree_root=str(entry['worktree_root']),
    )
    made = wt.commit_worktree(swarm_id, message=message, author=author)
    click.echo('committed' if made else 'nothing-to-commit')


@cli.command('publish')
@click.option('--swarm-id', required=True)
@click.option('--title', required=True)
@click.option('--body', default='')
@click.option('--base-branch', default=None)
@click.option('--ready', is_flag=True, help='Open ready (not draft).')
@click.option(
    '--no-pr/--pr',
    'no_pr',
    default=None,
    help=(
        'Local mode (push branch only, no GitHub PR) vs GitHub PR. '
        f'Unset = ${_PUBLISH_MODE_ENV} (local|github, default github).'
    ),
)
@click.pass_obj
def _publish(
    registry: SwarmRegistry,
    swarm_id: str,
    title: str,
    body: str,
    base_branch: str | None,
    ready: bool,
    no_pr: bool | None,
) -> None:
    """Publish the task branch: GitHub draft PR, or local push."""
    entry = registry.load(swarm_id)
    handle = handle_from_entry(entry)
    wt = WorktreeManager(
        canonical_root=str(entry['canonical_root']),
        worktree_root=str(entry['worktree_root']),
    )
    result = wt.publish_swarm(
        handle.swarm_id,
        handle.repo_url,
        title=title,
        body=body,
        base_branch=base_branch,
        draft=not ready,
        open_pr=_resolve_open_pr(no_pr),
    )
    click.echo(result)


@cli.command('dispose')
@click.option('--swarm-id', required=True)
@click.option('--keep-worktree', is_flag=True)
@click.pass_obj
def _dispose(
    registry: SwarmRegistry, swarm_id: str, keep_worktree: bool
) -> None:
    """Tear down every microVM + the worktree; unregister the swarm."""
    entry = registry.load(swarm_id)
    handle = handle_from_entry(entry)
    orch = SwarmOrchestrator(
        session_client=SwarmSessionClient(str(entry['server'])),
        worktree_manager=WorktreeManager(
            canonical_root=str(entry['canonical_root']),
            worktree_root=str(entry['worktree_root']),
        ),
        coder_agent_id='',
        reviewer_agent_id='',
    )
    orch.teardown(handle, dispose_worktree=not keep_worktree)
    registry.remove(swarm_id)
    click.echo(f'disposed {swarm_id}')


@cli.command('list')
@click.pass_obj
def _list(registry: SwarmRegistry) -> None:
    """List registered swarms and their roles."""
    for swarm_id in registry.list_ids():
        entry = registry.load(swarm_id)
        handle = handle_from_entry(entry)
        click.echo(f'{swarm_id}: {", ".join(handle.roles())}')


# ── Walking-skeleton demo ─────────────────────────────────────────

#: The marker the demo coder writes and the reviewer must read back —
#: proving the coder's rw-mount write reached the reviewer's :ro mount.
_DEMO_MARKER = 'hello from the coder'
_DEMO_FILE = 'GREETING.txt'

_DEMO_CODER_TASK = (
    'You are working in a git repository at your current working '
    'directory. Create a single file named '
    f'{_DEMO_FILE} whose entire contents is exactly this one line:\n'
    f'{_DEMO_MARKER}\n'
    'Do not create or modify any other file, and do not run any other '
    'commands. Then stop.'
)
_DEMO_REVIEWER_TASK = (
    'You are a strictly read-only reviewer. Read the file named '
    f'{_DEMO_FILE} in your current working directory and report its '
    'exact contents verbatim. Do not create, modify, or delete '
    'anything.'
)


@cli.command('demo')
@click.option(
    '--server', envvar='OMNI_SERVER', default='http://localhost:6767'
)
@click.option('--canonical-root', required=True)
@click.option(
    '--worktree-root',
    required=True,
    help='MUST match the server sbx.worktree_root.',
)
@click.option('--repo-url', required=True)
@click.option('--coder-agent', required=True)
@click.option('--reviewer-agent', required=True)
@click.option('--swarm-id', default='skel')
@click.option('--timeout', type=float, default=_DEFAULT_TURN_TIMEOUT_S)
@click.option('--keep', is_flag=True, help='Skip teardown (inspect VMs).')
def _demo(
    server: str,
    canonical_root: str,
    worktree_root: str,
    repo_url: str,
    coder_agent: str,
    reviewer_agent: str,
    swarm_id: str,
    timeout: float,
    keep: bool,
) -> None:
    """Run one A-prime round: coder writes a marker; reviewer reads."""
    orch = SwarmOrchestrator(
        session_client=SwarmSessionClient(server),
        worktree_manager=WorktreeManager(
            canonical_root=canonical_root, worktree_root=worktree_root
        ),
        coder_agent_id=coder_agent,
        reviewer_agent_id=reviewer_agent,
    )
    handle = orch.start_swarm(swarm_id, repo_url)
    click.echo(f'worktree:  {handle.worktree_path}')
    click.echo(f'coder:     {handle.coder_session}')
    click.echo(f'reviewer:  {handle.reviewers[0].session}')

    click.echo('--- coder implementing (rw mount) ---')
    cres = orch.run_coder(handle, _DEMO_CODER_TASK, timeout=timeout)
    click.echo(f'coder status: {cres.status}')
    click.echo(f'coder reply: {cres.reply[:200]!r}')

    click.echo('--- reviewer reviewing the live tree (:ro mount) ---')
    rres = orch.run_reviewer(handle, _DEMO_REVIEWER_TASK, timeout=timeout)
    review = rres.reply
    click.echo(f'reviewer status: {rres.status}')
    click.echo(f'reviewer reply: {review[:200]!r}')

    on_disk = ''
    path = os.path.join(handle.worktree_path, _DEMO_FILE)
    if os.path.isfile(path):
        with open(path, encoding='utf-8') as fh:
            on_disk = fh.read().strip()
    coder_wrote = on_disk == _DEMO_MARKER
    reviewer_saw = _DEMO_MARKER in review
    click.echo('=== A-prime shared-worktree check ===')
    click.echo(f'  coder wrote marker to the worktree (rw): {coder_wrote}')
    click.echo(f'  reviewer read it back via :ro:           {reviewer_saw}')
    ok = coder_wrote and reviewer_saw
    click.echo(f'  RESULT: {"PASS" if ok else "FAIL"}')

    if keep:
        click.echo(f'kept (inspect + tear down manually): {handle}')
    else:
        orch.teardown(handle)
        click.echo('torn down')
    if not ok:
        raise SystemExit(1)


def main() -> None:
    """Console entry point (``python -m sbx_omnigent.swarm``)."""
    cli()


if __name__ == '__main__':
    main()
