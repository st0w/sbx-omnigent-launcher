# Swarm orchestrator bundle

Omnigent agent specs + a skill that turn the launcher's swarm machinery into a
usable **collaborative coding swarm**: one **coordinator** (trusted plane, on the
host) drives one **coder** (isolated microVM, read-write worktree) plus one or
more **reviewers** (isolated microVMs, the same worktree read-only) through
implement → review → revise to consensus, then commits the approved tree and
opens a **draft** PR for you to review and merge.

Nothing here modifies Omnigent source. The specs are registered as extra
built-in agents; the coordinator drives the coder/reviewer microVMs through the
`omni-sbx-swarm` CLI (this package), which is the only thing that touches
Omnigent — over its public HTTP API.

See [`../docs/COLLABORATIVE_SWARM_DESIGN.md`](../docs/COLLABORATIVE_SWARM_DESIGN.md)
for the full design and [`../docs/PIPELINES.md`](../docs/PIPELINES.md) for the
one-file declarative way to run the same swarm.

## What's here

| Path | Role |
| --- | --- |
| `swarm-coordinator/` | The orchestrator (unsandboxed, host-side). Runs the `review-loop` skill; owns all git + publish; never writes code, never merges. |
| `swarm-coordinator/skills/review-loop/` | The procedure the coordinator follows: start swarm → (optional plan) → implement → review → iterate to consensus → commit (host) → publish → notify → dispose. |
| `swarm-coder/` | The implementer, run in a microVM with the worktree mounted `rw`. Only edits files; does not commit/push. |
| `swarm-planner/` | A read-only **planner** (`:ro`) — reads the code and produces a prose **design plan** (never code) for the coder, raising `QUESTIONS:` when the task is under-specified. Optional; used by the team example. |
| `swarm-reviewer-security/` | A read-only **security** reviewer (`:ro`) — audits for security issues, returns a `VERDICT:`. |
| `swarm-reviewer-bug/` | A read-only **correctness / bug** reviewer (`:ro`) — audits for logic/edge-case bugs, returns a `VERDICT:`. |
| `swarm-agy-coder/`, `swarm-agy-reviewer-bug/` | Antigravity (agy) variants of the coder / bug reviewer, on the `antigravity-native` harness. |

**Each specialist is a full, distinct agent** — its own spec, prompt, and
(optionally) its own bundled `skills/` — not just a role label. They are all
"reviewers" only in the sense that they mount the worktree **read-only**; a
security auditor, a bug hunter, a TDD/test author, and a refactoring reviewer do
very different jobs, so each gets its own agent.

**Grow the team:** add a new specialist by creating a directory here (e.g.
`swarm-reviewer-tdd/` with its own `config.yaml` and, if you like, a
`skills/<name>/SKILL.md`). It's auto-registered on the next server restart
(discovery globs `agents/*/config.yaml`). Then bind it to a role when you start a
swarm — see per-role binding below.

**Per-role agent binding.** A swarm maps each read-only role to a specific
agent, so distinct specialists coexist in one swarm:

```
omni-sbx-swarm start --swarm-id <id> --repo-url <repo> \
  --coder-agent <swarm-coder id> \
  --reviewer planner=<swarm-planner id> \
  --reviewer bug-hunter=<swarm-reviewer-bug id> \
  --reviewer security=<swarm-reviewer-security id> \
  --canonical-root <dir> --worktree-root <dir>
```

For the simple single-reviewer case, `--reviewer-agent <id> --reviewer-role
security` also works.

## Prerequisites

1. **Launcher installed + console scripts on PATH.** Reinstall this package so
   the `omni-sbx-swarm` / `omni-sbx-worktrees` entry points resolve (or the
   coordinator can call `python -m sbx_omnigent.swarm` instead):
   ```bash
   uv pip install -e /path/to/sbx-omnigent-launcher   # or: pip install -e . --no-deps
   ```
2. **Server configured for managed sbx hosts** with `sandbox.sbx.worktree_root`
   set (see the top-level [README](../README.md) and `config.sample.yaml`), and
   the **egress policy** + **subscription auth** in place (README "Network
   policy" and "Claude via subscription").
3. **`git`** on the host. For **GitHub mode** also `gh`, authenticated to the
   target repo (the coordinator opens the draft PR host-side). **Local mode**
   (`OMNI_SBX_PUBLISH_MODE=local`) needs neither `gh` nor GitHub — see
   "GitHub or local" below.

## Registration — automatic

`omni-sbx server` **auto-registers every agent in this `agents/` directory** — it
appends them to Omnigent's `OMNIGENT_BUILTIN_AGENT_DIRS` at startup (additively,
so your own entries are preserved). No manual step: just start the server.

```bash
omni-sbx server        # the coordinator/coder/reviewer are registered
```

- Any specialist you **clone into `agents/`** (e.g. `swarm-reviewer-bug-hunter`)
  is picked up automatically on the next restart — the discovery globs
  `agents/*/config.yaml`.
- **Opt out** (run only the microVM provider, no swarm agents): set
  `OMNI_SBX_NO_SWARM_AGENTS=1` before starting the server.
- Registration is idempotent and content-aware, so editing a spec and
  restarting refreshes it.

The agents then appear in `sys_agent_list` / the UI. Grab each `agent_id` (the
coder and reviewer ids feed the CLI; the coordinator is what you chat with).

## Where the coordinator runs, and how it gets config

The **coordinator runs on your local Omnigent host** (the machine running
`omnigent host`), not in a microVM — it needs host-side `git` + the
`omni-sbx-swarm` CLI + the worktree filesystem. When you open a
`swarm-coordinator` session in the UI it binds to that host automatically (like
any local agent). Only the **coder/reviewer workers** run in microVMs.

**Give the coordinator its config as part of the task** — the repo, the coder /
reviewer `agent_id`s (from `sys_agent_list`), the roots, and the publish mode.
The `review-loop` skill passes them to `omni-sbx-swarm` as explicit flags:

```
omni-sbx-swarm start --swarm-id <id> --repo-url <repo> \
  --coder-agent <coder_id> --reviewer-agent <reviewer_id> --reviewer-role security \
  --canonical-root <dir> --worktree-root <dir==sbx.worktree_root>
```

The CLI *also* reads these as env-var fallbacks, but **the coordinator does not
automatically inherit them**: the local host runs stock `omnigent host`, whose
runner env allowlist strips non-allowlisted vars. To use the env fallbacks, set
them **and** add their names to `OMNIGENT_RUNNER_ENV_PASSTHROUGH` in the
environment where you launch `omnigent host` — otherwise pass them as flags.

| Var (flag) | Meaning |
| --- | --- |
| `OMNI_SBX_WORKTREE_ROOT` (`--worktree-root`) | Per-swarm worktree dir. **Must equal** `sandbox.sbx.worktree_root`. |
| `OMNI_SBX_CANONICAL_ROOT` (`--canonical-root`) | Dir for the bare canonical mirrors. |
| `OMNI_SBX_CODER_AGENT` (`--coder-agent`) | `agent_id` for the coder role. |
| `OMNI_SBX_REVIEWER_AGENT` (`--reviewer-agent`) | `agent_id` for the reviewer role(s). |
| `OMNI_SERVER` (`--server`) | Omnigent server URL (default `http://localhost:6767`). |
| `OMNI_SBX_PUBLISH_MODE` (`--pr`/`--no-pr`) | `github` (default) opens a draft PR at publish; `local` pushes the reviewed branch only (no GitHub, no `gh`) for you to merge locally. |

### GitHub or local — your choice

The swarm is identical either way (coder + reviewers, worktree, consensus,
commit); only **publish** differs, and it follows the repo you point a swarm at
plus `OMNI_SBX_PUBLISH_MODE`:

- **Local (no GitHub):** set `OMNI_SBX_PUBLISH_MODE=local` and point swarms at a
  local repo path (`--repo-url /path/to/myproject`). At consensus the coordinator
  pushes the reviewed `task/<swarm>` branch into that repo and tells you to merge
  it (`git merge task/<swarm>`). No GitHub, no `gh`, no network, no auth.
- **GitHub:** leave `OMNI_SBX_PUBLISH_MODE` unset (or `=github`), authenticate
  `gh`, and point swarms at the GitHub URL. At consensus it pushes the branch and
  opens a **draft** PR for you to review and merge.

In both modes nothing is published until the swarm reaches consensus, and the
coordinator never merges.

## Use it

Chat with the **swarm-coordinator** agent and give it a task plus the repo, e.g.

> Implement `<task>` in `https://github.com/org/repo`. Acceptance: `<contract>`.
> Use a security reviewer and a bug-hunter.

It runs the `review-loop` skill: cuts a worktree, spins the coder + reviewers,
iterates to consensus on each reviewer's `VERDICT:` line, commits the approved
tree (attributed to the coder), opens a **draft** PR, and reports the URL for you
to review and merge. Run several tasks at once with distinct swarm ids for
parallel workstreams.

## Notes

- **No prompts, by construction.** Coder/reviewer turns run non-interactively
  (`--permission-mode auto`, passed by the CLI — auto-approves every tool,
  including Bash like `git diff`, with no prompt, while Omnigent's policy hooks
  still enforce denies). The microVM + the reviewer's kernel-enforced `:ro` mount
  are the real safety boundary.
- **Human-in-the-loop is preserved.** The coordinator opens a *draft* PR and
  notifies you; it never marks ready and never merges.
- **Cross-swarm isolation** falls out of the topology: distinct swarms mount
  distinct host worktrees, so agents in one cannot see another's code.

## Troubleshooting

**A turn fails, comes back empty, or the coordinator stops mid-loop — is it a
bug or a rate limit?** Often it's an **LLM usage/rate limit** silently cutting a
turn short, not a defect. Signs:

- `omni-sbx-swarm send` prints a **`note`** field (and a stderr `note:` line) —
  it scans the turn error, the session's `last_task_error`, and the reply for
  rate-limit markers (quota, `429`, "overloaded", "usage limit", …) and warns
  you when it matches.
- The coordinator ran `start` (its VMs came up) but its turn ended early with no
  further progress — the coordinator's own turn was likely throttled.

What to do: **don't chase a phantom bug or loop-retry.** Check your Claude plan
usage; the swarm stays registered (run `omni-sbx-swarm list`), so once capacity
returns you can resume it or `omni-sbx-swarm dispose --swarm-id <id>` to free its
microVMs. The coordinator is instructed to report a suspected rate limit to you
rather than treat it as a task failure.
