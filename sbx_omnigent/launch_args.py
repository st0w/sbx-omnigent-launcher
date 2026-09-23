"""Native-terminal launch args for each harness, and a codex effort's.

A swarm agent is headless, so its harness must start in a mode that
never stops to ask permission, and every CLI spells that differently.
Codex also takes its reasoning effort only as a launch arg. This lives
apart from :mod:`sbx_omnigent.swarm` so the session CLI in
:mod:`sbx_omnigent.swarm_session`, which ``swarm`` itself imports, can
build the same args without an import cycle (#55).
"""

from __future__ import annotations

from sbx_omnigent import agy, codex
from sbx_omnigent._compat import CODEX_EFFORTS

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
#: where :data:`AGY_LAUNCH_ARGS` and :data:`CODEX_LAUNCH_ARGS` already
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
YOLO_LAUNCH_ARGS = ('--permission-mode', 'bypassPermissions')

#: agy (Antigravity) equivalent of the YOLO args. agy does NOT accept
#: Claude's ``--permission-mode`` — it exits on the unknown flag at
#: launch (before binding its connect-RPC port), which the executor
#: reports as "the agy terminal is no longer running (the TUI exited)".
#: agy's auto-approve flag is ``--dangerously-skip-permissions``
#: instead. Safe for the same reason (microVM isolation + reviewer :ro).
AGY_LAUNCH_ARGS = ('--dangerously-skip-permissions',)

#: codex equivalent. It rejects BOTH Claude's ``--permission-mode`` and
#: agy's ``--dangerously-skip-permissions``; this is the flag its own
#: help documents for an externally sandboxed environment. Verified
#: against codex-cli 0.147.0 in an sbx microVM: the turn header reports
#: ``approval: never, sandbox: danger-full-access``.
CODEX_LAUNCH_ARGS = ('--dangerously-bypass-approvals-and-sandbox',)

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
CODEX_EFFORT_CONFIG_KEY = 'model_reasoning_effort'


def harness_by_ref(agents: list[dict[str, object]]) -> dict[str, str]:
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


def launch_args_for(
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
      :data:`YOLO_LAUNCH_ARGS`

    Codex additionally carries its reasoning effort here, because that
    is the only channel that reaches it — see
    :data:`CODEX_EFFORT_CONFIG_KEY`. An effort outside codex's own
    ladder is REFUSED, never passed: the value is interpolated into a
    ``-c key=value`` config expression, so an unvalidated one would
    reach the CLI as config syntax. It used to be dropped instead, and
    the turn then ran at codex's default with nothing said (#53). The
    pipeline loader and ``start`` refuse such an agent before any VM
    exists, so reaching the raise here is a bug. Claude and agy ignore
    *effort* here — Claude gets ``--effort`` from Omnigent\'s own
    launch path (verified: the session transcript records
    ``"effort":"xhigh"``). agy's effort is the tier in its model id
    (``gemini-3.8-flash-high``); its ``--effort`` flag sets the same
    thing and conflicts with a tiered id, so an agy agent with an effort
    is refused before launch instead (#6).

    :param harness: The agent\'s harness id, or ``None`` when
        unresolved.
    :param effort: The agent\'s pinned reasoning effort, or ``None``.
        Used only for codex.
    :returns: The launch args for that harness.
    :raises ValueError: If a codex agent pins an effort off codex's
        ladder.
    """
    if harness in agy.AGY_HARNESSES:
        return AGY_LAUNCH_ARGS
    if harness in codex.CODEX_HARNESSES:
        if effort is None:
            return CODEX_LAUNCH_ARGS
        if effort not in CODEX_EFFORTS:
            raise ValueError(
                f'effort {effort!r} is not one codex is launched with '
                f'({", ".join(sorted(CODEX_EFFORTS))})'
            )
        return (
            *CODEX_LAUNCH_ARGS,
            '-c',
            f'{CODEX_EFFORT_CONFIG_KEY}="{effort}"',
        )
    return YOLO_LAUNCH_ARGS
