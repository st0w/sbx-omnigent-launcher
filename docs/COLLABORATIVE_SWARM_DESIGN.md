# Collaborative Swarm Sandboxes — Design Record

**Status:** implemented and in use — this package ships the collaborative swarm
(setup in [`../README.md`](../README.md); the declarative-pipeline way to run it
in [`PIPELINES.md`](./PIPELINES.md)).
**Audience:** a human or LLM picking this up cold. It captures not just *what*
we're building but *why*, what we rejected, and the caveats we hit — so the
reasoning survives a lost session.

---

## 1. Goal

Let Omnigent orchestrate **coding swarms**. A swarm is one **coder** agent plus
several **reviewer** agents (security, bug-hunting, contract, etc.). The coder
implements; the reviewers inspect the coder's *live* work and feed findings back
to Omnigent, which relays fixes to the coder; the loop runs until the swarm
agrees the task is done. Multiple swarms run in parallel on **distinct
workstreams**.

Each agent runs in its own `sbx` microVM (strong isolation for untrusted agent
code execution), but the coder and its reviewers must **share a single live view
of the code** so reviewers see work in progress without anything being committed
to the shared remote first.

## 2. Hard constraints

These shaped every decision below.

1. **No modification of Omnigent source.** Omnigent updates frequently; a fork or
   patch would need constant rebasing. All work lives in this launcher package
   plus orchestrator *config* (agent specs + skills) authored outside the
   Omnigent tree. The only permitted coupling is the existing runtime wrap in
   `entrypoint.py` (monkeypatching `parse_sandbox_config` at startup — already
   how this package registers the `sbx` provider).
2. **GitHub stays clean.** Only stable, swarm-approved, **human-merged** code and
   history ever reach GitHub. No work-in-progress branches, no pre-approval PRs.
3. **Human-in-the-loop.** The human approves PRs and merges. Automation may open
   a **draft** PR only after the swarm agrees the task is complete.
4. **Least privilege.** The microVM boundary must stay meaningful; nothing gets a
   broader grant than it needs.

## 3. Background that made the design possible

### 3.1 Omnigent has two isolation layers
- **Local OS sandbox** (`os_env.sandbox`: `linux_bwrap` / `darwin_seatbelt` /
  `windows_jobobject` / `none`) — confines an agent's shell to `read_roots` /
  `write_roots` on the **shared local filesystem**. Kernel-shared; weaker than a
  VM.
- **Managed host sandbox** (`host_type=managed` → this package's `SbxLauncher`) —
  the whole `omnigent host` runs in an `sbx` microVM with its **own**
  filesystem. This is the isolation we want for agent code.

### 3.2 `sbx` mount model (verified empirically)
- `sbx create shell PATH [PATH:ro ...]` bind-mounts host paths into the microVM
  **at the same absolute path**, with **filesystem passthrough** — "changes in
  either direction are instant, no sync process."
- The **primary** workspace is always `rw`; `:ro` is honored only on
  **additional** paths.
- `--clone` is the alternative: an in-container read-only clone wired back to the
  host via a `sandbox-<name>` git-daemon remote. We do **not** use it (see §6).

### 3.3 Omnigent already ships the pattern (`examples/polly`)
polly is a coding orchestrator: per-task git worktree, cross-vendor reviewer,
PR-per-implementer, and it **never merges** (the human does). Two lessons we
carried over: the *trusted-orchestrator / worker* split, and "never merge."
One lesson we deliberately diverge from: polly hands reviewers the **diff as
text** and runs everything **unsandboxed on one filesystem**; we instead give
reviewers a **live read-only mount** inside **microVMs**.

### 3.4 Why this needed new plumbing beyond polly

**Polly and Omnigent are powerful out of the box, and we reuse them wholesale.**
The orchestration *pattern* here is polly's (a tech-lead that writes no code,
delegates to workers, gets an independent reviewer, never merges), and the
coordinator/coder/reviewer specs, the `review-loop` skill, and the guardrails are
all standard Omnigent agent/skill machinery — not new invention. This project is
**one added layer: microVM-grade isolation for the untrusted code the agents
execute**, plus a live shared worktree so reviewers can inspect work in progress.

That single security requirement — **each agent in its own microVM** — is the
whole reason for the custom code. Polly's model quietly depends on two things a
microVM removes: **co-location** (sub-agents share the orchestrator's runner) and
a **shared filesystem** (everything runs `sandbox: none` on one disk). Take those
away and specific polly mechanisms stop working — and each module we wrote is the
replacement for exactly one of them:

| Polly / Omnigent, off-the-shelf | Why the microVM boundary breaks it | What we added |
|---|---|---|
| `sys_session_send` spawns a **native sub-agent** | **Verified constraint:** a session with a `parent_session_id` inherits the parent's runner and *co-locates* — the managed-VM launch fires only when there is **no** parent. So "native sub-agent" and "its own microVM" are **mutually exclusive**. | `swarm_session.py` — create each agent as a **top-level managed session over HTTP**, drive it via SSE (`send-and-wait`). |
| Worktrees on **one shared filesystem** | microVMs have **separate** filesystems — no shared tree for coder + reviewer to both see. | `launcher.py` mount path — bind-mount **one host worktree** into each VM (coder `rw`, reviewers `:ro`). |
| Reviewer gets the **diff as text** | We want reviewers on the *live, uncommitted* tree — impossible without a shared filesystem. | The shared `:ro` mount → reviewers read live work **and are kernel-blocked from corrupting it** (something polly's shared-rw model cannot offer). |
| Native **inbox** coordination | The inbox works because sub-agents are *children on the orchestrator's runner*; ours are top-level (row 1), so `sys_session_send`-by-id / the inbox don't apply. | HTTP/SSE `send-and-wait` drive loop. |
| Implementers open their **own PRs** (git + creds on the shared FS) | The coder VM is **untrusted** — deliberately no git creds, no git-write, no egress. | `worktrees.py` — the coder only edits; commits reach the host via the mount; the **trusted host plane** commits + publishes. |

Two further bits are pure "microVM tax" that polly never pays because its workers
inherit the host environment: a **fresh VM has no Claude credentials** (→ the
zero-exposure subscription-token-via-proxy auth, §10 / README), and the in-VM
harness bypassed sbx's credential proxy (→ the launcher's proxy-env forwarding).

**The honest boundary of what's new.** Off-the-shelf Omnigent fully supports
managed microVM hosts *and* polly-style orchestration — **separately**. What it
has no native path for is **combining** them: an orchestrator spawning *multiple
isolated-microVM* agents that *share a live worktree*. There is no "spawn a
sub-agent into its own VM" (the co-location constraint) and no "share a worktree
across VMs." Those two gaps are all we fill — through Omnigent's **public seams**
(the `SandboxLauncher` interface, the HTTP session API, the workspace-string
sentinel), with **zero Omnigent source changes**.

In one line: **it's polly, but its workers are hardened microVMs instead of local
processes, and reviewers get a live shared tree they physically cannot corrupt.**
The extra code is the cost of that isolation boundary; in exchange you get real
isolation for untrusted agent code and *provable* read-only review.

## 4. Key empirical findings

- **Concurrent shared-worktree mounting works.** Two microVMs mounting the same
  host dir simultaneously (one `rw`, one `:ro`): both saw initial content; a
  write in the `rw` VM appeared live in the `:ro` VM; a host-side edit reached
  both; the `:ro` VM's write failed with `Read-only file system` and nothing
  reached the host. Git worked in both — coder `git commit` in the `rw` VM was
  instantly visible to the reviewer's `git log`/`git diff` on the `:ro` mount.
- **The earlier "inconclusive" result was a shell artifact, not an sbx limit.**
  `"$WORK:ro"` under zsh triggers the `${var:r}` (remove-extension) modifier,
  silently turning the path into `…shareo`. Brace-delimiting (`"${WORK}:ro"`)
  fixed it. The launcher builds mount args via `subprocess` **list args**, which
  never invoke a shell, so this cannot recur in the real code.
- **`mkfs.erofs` is expected.** `sbx` converts each pulled image layer to an
  EROFS image for the microVM; macOS Gatekeeper / Little Snitch may prompt on
  `mkfs.erofs`. Allowing it lets images extract. Not a defect.
- **Egress quirk (documented in README).** A local server's host dial-back needs
  **both** `host.docker.internal:6767` **and** `localhost:6767` allowed in the
  `sbx` egress policy; removing the localhost rule breaks the tunnel. Unrelated
  to swarms but part of the same system's setup.

## 5. Architecture — two planes

```
                    ┌───────────────── GitHub ─────────────────┐
                    │  stable main + history only.               │
                    │  Written ONLY at publish. Human merges.     │
                    └───────────────────▲────────────────────────┘
                                        │ push task/<swarm> + draft PR
                                        │ (host-side, ONLY after consensus)
════════ HOST — trusted control plane (unsandboxed) ═══════╪══════════════════
                                        │
  Top orchestrator ──delegates──► per-swarm coordinators    │ incremental fetch
     (decompose tasks)                (run review loop,      ▼
                                       decide done, publish) ┌───────────────────┐
                                                             │ canonical bare     │
                                                             │ mirror (origin=GH) │
                                                             └─────────┬─────────┘
                                                    local clone -b task/<swarm>
                                                             ▼ per swarm
                                                   /srv/worktrees/<swarm>
                                                             │ bind mount
════════ EXECUTION PLANE — microVMs (untrusted) ════════════╪═════════════════
                             rw ▼                        ro ▼ (+ own scratch rw)
                       Coder VM                    Reviewer VM(s)
                       implements, commits locally  read log/diff/tree, report up
```

- **Trusted control plane** (host, unsandboxed, `caller_process` like polly):
  top orchestrator + per-swarm coordinators. Owns all git, the canonical mirror,
  worktree lifecycle, review-loop logic, consensus, and publish.
- **Execution plane** (microVMs): coders and reviewers. Pure workers. No git
  remote, no GitHub creds, no git egress.

**Isolation falls out of the topology:** swarm-A and swarm-B mount *different*
host directories, so their agents can't see each other's code; within a swarm
only the coder's mount is `rw`; only the host control plane can write GitHub, and
only at consensus.

## 6. Code in, code out

### Code IN — bind mount, not clone-in-VM
The coder's worktree is a directory on the host, bind-mounted into the coder VM
`rw` and into each reviewer VM `:ro`. Reviewers therefore see committed **and
uncommitted** work live.

### Code OUT — the coder only EDITS; the trusted plane commits + pushes
The coder writes **files** in its `rw` worktree; it does not run git. The
trusted plane owns every git operation (consistent with §5). Three steps, all
after consensus and all host-side:
1. **VM → host: automatic, no push, no egress.** The worktree *is* the host
   directory, so a coder's file edits land on the host instantly via the mount.
   There is no "push out of the sandbox." This is the decisive reason we use
   bind-mount over `sbx --clone` (the latter would need a git-daemon and VM→host
   egress).
2. **commit: host-side, trusted plane** (`WorktreeManager.commit_worktree`). On
   consensus the coordinator stages + commits the approved working tree on the
   host, attributing authorship to the coder (`--author`) while the committer is
   the coordinator identity. This is deliberately **not** delegated to the coder
   agent: a load-bearing commit must be reliable tested code, not a
   natural-language instruction an agent may skip (the deterministic proof caught
   exactly this). It also means the untrusted coder VM needs **no git write
   capability at all** — strictly less privilege.
3. **push + publish: host-side, at consensus only** (`publish_swarm`). The
   coordinator pushes `task/<swarm>` **straight from the worktree to `repo_url`**
   — an explicit `src:dst` refspec, so only the task branch moves, never the
   base. Then, by publish mode (`OMNI_SBX_PUBLISH_MODE` / `--pr`/`--no-pr`):
   **GitHub mode** opens a **draft** PR (`gh pr create --draft`); **local mode**
   (`open_pr=False`) stops after the push — `repo_url` is a local repo, the human
   merges `task/<swarm>` there, and GitHub/`gh` are never involved. Both modes
   are host-side, publish nothing before consensus, and never merge. `ensure_
   canonical` already accepts either a GitHub URL or a local path, so only this
   last step is mode-specific.

### Why a **standalone clone** (not a linked `git worktree`)
A linked worktree's `.git` is a *file* pointing into the main repo's object
store. Mount only the worktree and git breaks in the VM (the object store isn't
mounted). A standalone clone has a self-contained `.git`, so git works inside the
VM and write-back is a normal fetch/push. (Harden reviewer git with
`GIT_OPTIONAL_LOCKS=0` against read-only index refresh.)

### Why a **bare mirror** canonical (fetch-only)
"Bare" ≠ empty. A bare repo holds full history/branches but no working tree —
GitHub's server-side format. The canonical is a `git clone --mirror` of the
GitHub repo, refreshed **only** by `fetch --prune` (never pushed to). That keeps
per-swarm clones fast and disk-cheap (local, hardlinked objects) and keeps the
mirror a pure reflection of upstream — so a refresh can never prune a
`task/<swarm>` branch that hasn't reached GitHub yet. (An earlier draft routed
publish worktree→canonical→GitHub; pushing un-published task branches *into* a
`--mirror` is what the prune hazard would come from, so publish goes worktree→
GitHub directly instead — see Code OUT above.)

## 7. The workspace-string sentinel

**Problem:** the only per-session input that reaches the launcher is the session
`workspace` string (surfaced to `start_host` as `repo_url` / `repo_branch` /
`repo_name`). There is no field for "mount this host path, read-only."

**Mechanism:** the coordinator sets the sub-session workspace to a URL-shaped
sentinel:

```
git@sbxmount:/srv/worktrees/<swarm>#<rw|ro>
```

**Verified** against `parse_repo_workspace` (`managed_hosts.py:507`): it matches
the `git@<host>:<path>` grammar (host=`sbxmount`), the `#` fragment passes
`_validate_clone_branch`, and the name derives cleanly. `start_host` receives
`repo_url=git@sbxmount:/srv/worktrees/<swarm>`, `repo_branch=<mode>`. Validation
is purely **syntactic** — no network reachability check fires at session-create.
The launcher recognizes the `git@sbxmount:` prefix, extracts the path and mode,
and mounts; any real repo URL (`git@github.com:…`) does not match and is left to
the inherited behavior.

**Security invariant:** the launcher **allowlists** the mount source — the
extracted path must `realpath` under a configured `sbx.worktree_root`
(`/srv/worktrees`), rejecting `..` escapes and symlinks. This is the one choke
point that stops a confused/compromised agent prompt from mounting an arbitrary
host directory into a VM.

This URL-sentinel is the single inelegant seam of the design. It is contained
entirely within this package and the orchestrator config; it touches no Omnigent
source.

### 7.1 How the coordinator creates a swarm-agent session (Stage 0 finding)

Original assumption: the coordinator spawns coders/reviewers as **native
sub-agents** (`sys_session_create` / `sys_session_send`). **This does not work**
(verified in `omnigent/tools/builtins/spawn.py`):

- Those tools expose no `host_type` / `workspace` — only `agent_id`/`title`/
  `message`. There is no way to request a managed host or set a workspace.
- Sub-agents **co-locate on the parent's runner** (`SysSessionCreateTool`: *"The
  child inherits the caller's runner"*), so a spawned sub-agent runs wherever the
  coordinator runs — **not** in its own microVM.

The managed-microVM path is the HTTP **`POST /v1/sessions`** endpoint.
`SessionCreateRequest` (`schemas.py:1113`) accepts `parent_session_id` +
`host_type="managed"` + `workspace` **together** (the `_check_managed_host_fields`
validator confirms managed + repo-URL-workspace is the intended shape, with
`host_id` absent). So the coordinator — unsandboxed, with `sys_os_shell` — creates
each coder/reviewer via a thin **host-side helper** that calls that endpoint.

**Critical constraint — isolation and native drive are mutually exclusive**
(traced through `sessions.py:12152` + `tool_dispatch.py:1629`): a session created
**with** `parent_session_id` inherits the parent's `runner_id` (co-location) and
the managed-sandbox launch fires **only when `runner_id is None`** — so a child
gets **no microVM of its own**. Native `sys_session_send`-by-`session_id`, in
turn, requires the target to be a **direct child** (`parent_session_id == caller`).
The two can't both hold: per-agent microVM isolation ⇒ **top-level** managed
sessions (no parent) ⇒ **not** drivable by the native tool. The coordinator
therefore drives each agent **over HTTP** through the helper (see §7.2), not via
`sys_session_send`.

**Chosen coordinator model: Option A** — an LLM coordinator agent (polly-style,
unsandboxed) that creates/drives swarm agents through this helper tool, keeping
the flow as close to stock Omnigent as possible. The helper hides the HTTP
mechanics so the LLM never hand-writes requests. (Rejected: Option B, an external
Python program driving the whole loop over HTTP — simpler/more deterministic but
less "agentic," further from how Omnigent works out of the box.)

### 7.2 The drive primitive: SSE `send-and-wait` (built + live-verified)

`sbx_omnigent/swarm_session.py` (`SwarmSessionClient` + a `click` CLI the
coordinator invokes via `sys_os_shell`) is the trusted-plane primitive that runs
one agent turn and knows when it finished. Commands: `create` / `send-and-wait` /
`read` / `status` / `dispose`.

Turn completion is **push-based, not polled**. `send-and-wait`:
1. subscribes to the session SSE stream `GET /v1/sessions/{id}/stream`;
2. waits for the **ready heartbeat** the server emits the instant the subscriber
   slot registers — so the `running`→`idle` edge cannot be missed;
3. **then** posts the turn to `POST /v1/sessions/{id}/events`;
4. returns on a terminal `session.status` (`idle` = done, `failed` = error). An
   `idle` counts only **after** an active (`running`/`waiting`) edge, so a stale
   prior-turn `idle` can't end the wait early.

All async plumbing (SSE parse, reader thread, subscribe/post ordering, terminal
state machine, stream-close snapshot fallback) lives in this **tested** helper, so
the LLM coordinator can't mis-sequence it — the reliability lever that keeps A′'s
iterative review loop from being brittle.

**Live-verified** end to end through the real CLI + server: `create` → managed VM
provisioned → `send-and-wait` caught the real `running`→`idle` edge and returned
`idle` in ~40 s → `read` showed the reply → `dispose` tore the VM down. 14 unit
tests (hermetic, real parser + reader thread + subscribe-before-post ordering).

**Finding the live run caught:** `POST /events` **long-polls while a managed
session provisions** — it returns only once the runner binds (~tens of seconds),
so the turn-post must carry the full turn budget, not a short unary timeout. (A
hand-rolled LLM poll-loop would likely have mis-set this; it's now absorbed in the
helper.)

## 8. Decisions and rationale

| Decision | Choice | Why | Rejected alternative |
|---|---|---|---|
| Code-in mechanism | Host bind-mount of a per-swarm clone | Live shared tree + microVM isolation + no premature commit; write-back needs no VM egress | `sbx --clone` (git-daemon) — reviewers see only committed code, separate clones, needs VM egress |
| Clone type | Standalone clone | Self-contained `.git` works in-VM and for write-back | Linked `git worktree` — breaks in-VM (object store unmounted) |
| Canonical | Local **bare** mirror (Option A) | Fast/cheap local task clones; WIP stays off GitHub during long runs; single controlled publish boundary; incremental `fetch` keeps it fresh | Option B (clone per task straight from GitHub) — network per clone, needs GitHub creds at clone time |
| Reviewer access | Live `:ro` mount of coder's tree | Sees uncommitted work, can run/browse it, physically cannot write | polly's diff-as-text — no live tree, can't run WIP |
| Isolation tier | microVM per agent | Untrusted agent code; the reason `sbx` exists here | Local OS sandbox (bwrap/seatbelt) — kernel-shared, weaker |
| Control/exec split | Trusted host coordinators; workers in VMs | Keep git/publish/mount decisions out of sandboxes | Coder-as-swarm-lead — puts infra control inside a sandbox |
| Publish gate | Consensus → push + **draft** PR → notify human | Honors HITL; human reviews & merges | Auto-merge (violates HITL); human does everything (loses the convenience of a staged draft PR) |
| Swarm coordination | Per-swarm coordinator agents | Matches "distinct groups"; scales | Single flat orchestrator running every loop |
| Swarm-agent creation | Coordinator agent → host-side helper → HTTP `POST /v1/sessions` (managed + workspace, **top-level / no parent**) (Option A) | Native sub-agent tools can't create managed/microVM sessions; HTTP create can, and keeps the coordinator on stock Omnigent rails. Top-level (no parent) is required — a parent link co-locates the child and skips VM provisioning | Native `sys_session_create` (no `host_type`/`workspace`, co-locates — **impossible**); external Python orchestrator (Option B — less agentic) |
| Swarm-agent drive | HTTP **SSE `send-and-wait`** via the helper (§7.2) | Top-level managed sessions aren't drivable by native `sys_session_send`-by-id; SSE is push-based so turn edges aren't missed; all async logic sits in tested code | Native `sys_session_send`-by-id (needs a child → co-located, no VM); raw polling (poll-race brittleness) |
| Provider-integration | Launcher subclass + startup wrap + orchestrator config | Zero Omnigent source edits | Add a typed mount-spec field to Omnigent's managed-launch path (clean, but a source change — **out of scope**) |

## 9. Security model

- **Untrusted:** coder/reviewer agent code. Confined to a microVM; sees only its
  swarm's worktree (coder `rw`, reviewer `:ro`) plus a scratch dir; no GitHub
  creds; no git remote; no git egress. The only egress it needs is the
  credential-proxied LLM traffic `sbx` already brokers, plus the host dial-back.
- **Trusted:** coordinators (host, unsandboxed). Hold GitHub creds and run git.
  They only ever update `task/*` branches and open **draft** PRs — never push
  `main`, never merge (mirrors polly's blast-radius guardrails).
- **Choke points:** the launcher's mount-root allowlist (no arbitrary host paths
  into VMs); the bare canonical (no accidental checked-out-branch clobber); the
  human merge gate (nothing lands on `main` without review).

## 10. Caveats, risks, open questions

- **Managed-session creation + drive — RESOLVED.** Native sub-agent tools
  *cannot* create managed/microVM sessions (no `host_type`/`workspace`; they
  co-locate) — see §7.1. The no-source path is the coordinator calling HTTP
  `POST /v1/sessions` (managed + workspace, **top-level**) and driving via the
  SSE `send-and-wait` helper (§7.2). **Proven live:** Stage 0 — HTTP managed-create
  returns 201 and reaches the instrumented `start_host` intact; and the drive
  primitive — `create → send-and-wait (running→idle in ~40 s) → read → dispose`
  end to end through the real CLI + server. Native `sys_session_send`-by-id is
  **not** an option for isolated agents (it needs a child, which co-locates).
- **A′ vs B (agents-per-VM) — RESOLVED to A′.** Two placements were both verified
  possible: **B** = one VM/swarm with a lead + co-located native sub-agents
  (simplest coordination, native inbox), and **A′** = coder VM (`rw`) + a separate
  reviewer VM (`:ro`). We chose **A′** because it gives **kernel-enforced** reviewer
  read-only (the sbx `:ro` mount) — inside a shared VM (B) `bwrap` and `mount` are
  blocked (namespace/mount syscalls denied, even `--privileged`), so B could only
  offer a software/convention RO gate. A′ costs a 2nd VM per swarm and a
  host-mediated loop, both de-risked by the §7.2 `send-and-wait` helper. B remains
  a viable future "shared VM" mode if provable RO is ever not required.
- **Base drift / merge conflicts.** Parallel swarms cut from the same base can
  conflict at integration. Mitigations: disjoint file scope per swarm (polly's
  rule), incremental `fetch` before cutting, optional rebase-onto-main before
  publish, and the human merge gate. Not caused by the sandbox model.
- **Standalone clones require an explicit write-back.** Approved commits live
  only in the task clone until the coordinator fetches/pushes them out — the
  publish step is mandatory, not optional (cheap/idempotent locally).
- **Reviewer writes fail by design — VERIFIED live.** A reviewer's harness must
  write scratch to `$HOME` / `/tmp`, not the `:ro` worktree. Set
  `GIT_OPTIONAL_LOCKS=0` for reviewer git. Confirmed end to end: a full-YOLO
  reviewer agent that tried to `Edit`/`Write` the mounted tree got
  `EROFS: read-only file system` on every attempt (it fails at Claude Code's
  atomic-write temp file, before the real file is touched — no partial write),
  reported it and degraded gracefully (turn still `idle`), the worktree stayed
  byte-for-byte intact, and a raw shell write in the VM was blocked too. The
  sandbox enforces at the kernel; it never relies on the agent cooperating.
- **Mirror staleness** if the incremental fetch is skipped before cutting a
  swarm — make the fetch part of swarm creation.
- **Agent auth in fresh VMs — RESOLVED (prerequisite for Stage 4).** A fresh
  microVM has no Claude credentials, so `claude-native` would re-prompt `/login`
  per VM. Solved with a subscription **OAuth token** (`claude setup-token`)
  injected via an sbx **custom secret** (`set-custom` — placeholder in the VM,
  real token swapped on the wire, so the agent never sees it; entered via a
  hidden prompt for zero history/argv exposure). Required a launcher fix:
  Omnigent's runner strips the sbx proxy env vars, so credential injection
  (forward-proxy-only) never reached the harness — the launcher now forwards them
  via `OMNIGENT_RUNNER_ENV_PASSTHROUGH` with a `NO_PROXY` that keeps the dial-back
  direct. Verified end to end (agent authenticates, no `/login`, VM holds only the
  placeholder). See README "Claude via subscription". Applies to any credentialed
  provider, not just Claude.
- **Agent launch mode (headless, no prompts) — RESOLVED: `--permission-mode
  auto`.** Swarm agents run headless, so any permission prompt hangs the turn.
  Omnigent's policy hook deliberately does NOT auto-approve tools (it only
  denies/asks and otherwise defers consent to the harness's permission mode —
  `native_policy_hook.py`), so the *harness* mode is what silences prompts.
  Options: `bypassPermissions` / `--dangerously-skip-permissions` no-op under
  managed Claude settings; `acceptEdits` auto-approves file **edits only**, so a
  reviewer's Bash (`git diff`) still prompts and **stalls** (found live in the
  Basic-example test — an earlier note here wrongly claimed `acceptEdits`
  auto-approved Bash); **`auto`** auto-approves every tool without prompting, is
  allowed under managed settings, and still lets Omnigent policy **deny**
  dangerous ops (only the human-consent step is auto-satisfied). Verified live: a
  claude-native agent ran a Bash command under `auto` with no prompt. Safe by
  construction regardless (coder's microVM + reviewer's kernel-enforced `:ro`).
- **Full loop — DONE (live), autonomously.** Beyond the walking skeleton
  (coder writes rw → reviewer reads `:ro`) and the deterministic multi-round proof
  (plant bug → BLOCKING → fix → APPROVED → commit → write-back), an **LLM
  `swarm-coordinator` ran the entire `review-loop` on its own** — start → implement
  → security review → consensus → trusted-plane commit → publish → dispose — in
  local mode, ground truth verified, clean teardown. Stage-4 facts worth carrying:
  - **The coordinator runs on the LOCAL host, not in a VM.** Create its session
    bound to the local Omnigent host (`host_id=<local host>` + a real host
    `workspace`, exactly like a UI session) — NOT `host_type=managed`. A managed
    coordinator would run *inside* a microVM and its `sys_os_shell` couldn't reach
    the host worktrees / git / CLI. (A session with no host 503s "No runner
    bound".) Only the coder/reviewer *workers* are microVMs.
  - **Config reaches the coordinator via flags, not ambient env.** The local host
    runs stock `omnigent host`, whose runner env allowlist strips non-allowlisted
    vars — so `OMNI_SBX_*` do **not** auto-reach the coordinator's shell unless
    forwarded via `OMNIGENT_RUNNER_ENV_PASSTHROUGH` where `omnigent host` runs. The
    coordinator therefore takes its config (repo, agent ids, roots, publish mode)
    as explicit CLI flags from its task. (Verified live with flags.)
  - **Publish modes:** GitHub draft PR **or** local push-only (`--no-pr` /
    `OMNI_SBX_PUBLISH_MODE=local`, no `gh`/GitHub) — the end user's choice; only
    `publish` differs between modes (see §6).
  - **Post-turn empty-reply race — fixed.** `send-and-wait` returns on the `idle`
    edge, but a native-terminal harness persists its final message a beat later, so
    an immediate read can be empty; `reply_after_turn` (bounded retry) closes it.
- **Follow-up live checks — both PASS.** *Multi-swarm cross-isolation:* two
  concurrent swarms (4 VMs); each VM's `worktree_root` lists **only its own**
  worktree and cannot read the other swarm's secret — cross-swarm isolation holds
  at the mount level. *GitHub-mode publish:* the autonomous coordinator opened a
  real **draft** PR (`isDraft`, base `main`, head `task/<swarm>`) against a live
  GitHub repo, with `main` untouched — confirming the GitHub path alongside local.
- **Not addressed yet:** resume/keep-alive of dormant swarm hosts; reviewer
  suggested-patch write-back; auto-rebase.

## 11. Where each piece lives (no Omnigent source touched)

| Piece | Location |
|---|---|
| Sentinel parse, mount mechanics, allowlist | `sbx_omnigent/launcher.py` (`SbxLauncher.provision`/`start_host` override) |
| Swarm-agent create/drive helper (HTTP + SSE `send-and-wait`) | `sbx_omnigent/swarm_session.py` — `SwarmSessionClient` + `python -m sbx_omnigent.swarm_session` CLI (coordinator drives via `sys_os_shell`; see §7.2) |
| Provider registration + bundled-agent auto-register | `sbx_omnigent/entrypoint.py` — startup wrap for `provider: sbx`, and `register_bundled_agents()` appends the packaged `agents/*/config.yaml` to `OMNIGENT_BUILTIN_AGENT_DIRS` (opt-out `OMNI_SBX_NO_SWARM_AGENTS`) |
| Canonical mirror + worktree lifecycle + commit/publish | `sbx_omnigent/worktrees.py` — `WorktreeManager` + `omni-sbx-worktrees` CLI (host-side git; publish opens a GitHub draft PR or, with `open_pr=False`, pushes local-only) |
| A′ orchestrator (mechanics) + registry CLI + demo | `sbx_omnigent/swarm.py` — `SwarmOrchestrator` + registry-backed `omni-sbx-swarm` CLI (`start`/`send`/`commit`/`publish`/`dispose`/`list`) + `demo`. **Live-verified** end to end. |
| Coordinator / coder / reviewer specs + `review-loop` skill | `agents/` — **auto-registered** at server startup (see entrypoint row); the coordinator (trusted plane, runs on the local host) follows the `review-loop` skill and drives workers through the `omni-sbx-swarm` CLI. polly is the template, not edited. |
| Setup (egress policy, secrets, image) | `README.md`, `config.sample.yaml` |

See [`../README.md`](../README.md) for setup and
[`PIPELINES.md`](./PIPELINES.md) for the declarative-pipeline way to run it.
