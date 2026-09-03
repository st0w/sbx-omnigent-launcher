# sbx-omnigent-launcher

Run [Omnigent](https://github.com/omnigent-ai/omnigent) **managed hosts inside
Docker Sandboxes (`sbx`) microVMs** — one isolated microVM per coding agent —
without forking or patching Omnigent. On top of that isolation, this package
gives you a **declarative pipeline** layer:
describe a whole coding swarm (who's on it, each agent's model and skills, how
their work flows through a DAG) in a single `pipeline.yaml`, and fire it with one
command. Each writer agent works in its own microVM on its own isolated git
branch; a trusted host plane does all git and publishes a reviewed branch or a
draft PR — still with **zero Omnigent source changes**.

**Two ways to run a swarm:**

- **Declarative pipelines (recommended)** — one `pipeline.yaml`, one command. Jump
  to the [Quickstart](#quickstart), then [Defining pipelines](#defining-pipelines--the-dag-model)
  and the deep reference in [`docs/PIPELINES.md`](./docs/PIPELINES.md).
- **Coordinator-driven swarms** — chat with a `swarm-coordinator` agent that runs
  the review loop for you (the original flow). See
  [Coordinator-driven swarms](#coordinator-driven-swarms-alternative) and
  [`agents/README.md`](./agents/README.md).

Running it on a server rather than a laptop — placement, sizing, tunneled UI
access, and the host state that does not travel with a `git clone`:
[`docs/CLOUD.md`](./docs/CLOUD.md), with ready-made AWS Terraform in
[`deploy/aws/`](./deploy/aws/).

Both run on the same microVM-host foundation, described below.

---

## Why this exists

Omnigent's `claude-native` harness does not spawn Claude as a stream-json
subprocess — it launches Claude Code in a **tmux pane** (with an MCP bridge and
hooks) and injects web-UI messages via `tmux send-keys`. That bridge is built
from host filesystem paths and a host-local server URL. So you cannot cleanly
"wrap just `claude` in a sandbox" from the outside — the bridge would be split
across the sandbox boundary.

The robust alternative is to move the **entire host** (runner + Claude + bridge)
into the sandbox and let only the host↔server dial-back cross the boundary.
Omnigent already supports exactly this through its **managed sandbox host**
mechanism and a pluggable `SandboxLauncher` interface. This package implements
that interface for `sbx` and registers it — all from outside the Omnigent tree.

## How it works

Three pieces:

1. **`SbxLauncher`** (`sbx_omnigent/launcher.py`) — a
   `SandboxLauncher` subclass that teaches Omnigent to talk to `sbx`:
   - `prepare()` → verify the `sbx` CLI is installed and logged in
   - `provision(name)` → **reserve** the sandbox id (creation is deferred to
     `start_host`, so the launch token registers before the box exists)
   - `start_host(...)` → create the microVM and launch `omnigent host` in it. For
     an ordinary managed session it mounts a throwaway scratch dir and delegates
     to Omnigent's inherited bootstrap (probe `$HOME`, make the workspace,
     optional `git clone`). For a **swarm/pipeline** session — a
     `git@sbxmount:<path>#<rw|ro>` workspace — it instead **bind-mounts an
     existing host worktree** into the VM (writer `rw`, reviewer/judge `:ro`),
     gated by the `sbx.worktree_root` allowlist.
   - `run(id, cmd)` → `sbx exec <id> -- sh -c '<cmd>'`
   - `run_background(...)` → launch `omnigent host` and forward `sbx`'s proxy env
     past the runner allowlist so credential injection reaches the harness (see
     [Credentials](#credentials))
   - `terminate(id)` → `sbx rm --force <id>`

2. **`entrypoint.py`** — a console script (`omni-sbx`) that, at startup: (a) wraps
   `omnigent.server.managed_hosts.parse_sandbox_config` so `provider: sbx` is
   handled here and every other provider is delegated to the original; (b)
   auto-registers the bundled swarm agents by appending the packaged `agents/`
   dirs to `OMNIGENT_BUILTIN_AGENT_DIRS` (opt-out with `OMNI_SBX_NO_SWARM_AGENTS=1`);
   and (c) when passed **`--pipeline pipeline.yaml`**, materializes and registers
   that pipeline's per-agent bundles before the server starts. Then it calls
   Omnigent's normal CLI. You run `omni-sbx server ...` in place of `omni server
   ...`.

3. **The pipeline runner** (`sbx_omnigent/runner.py`, `python -m
   sbx_omnigent.runner` / `omni-sbx-pipeline`) — a client-side orchestrator that
   binds the registered agents, provisions each DAG node's isolated worktree +
   microVM, drives the stages to completion, does all git on the trusted host
   plane, and publishes. See [Defining pipelines](#defining-pipelines--the-dag-model).

Coupling to Omnigent stays on **public, stable seams**, and nothing is copied
into the Omnigent tree — so `git pull` on Omnigent never conflicts. The provider
wrap depends on two symbols (`parse_sandbox_config` / `ManagedSandboxConfig`) and
fails **loudly at startup** if either is renamed; the swarm layer rides the
`SandboxLauncher` interface, the workspace-string convention, the HTTP session
API, and the `OMNIGENT_BUILTIN_AGENT_DIRS` env var.

## Requirements

- The `sbx` CLI installed on the server host and signed in (`sbx login`).
- [Omnigent](https://github.com/omnigent-ai/omnigent) installed in a Python
  environment (the one that provides `omni`). Omnigent is the open-source agent
  framework and meta-harness that actually runs the agents; this package is a
  provider for it, adding microVM isolation and the pipeline layer on top.
- Network path from a microVM back to your server (for a local server, via the
  Docker host gateway `host.docker.internal`) **plus an `sbx` egress policy that
  permits it** — see [Network policy](#network-policy). Without the policy the
  microVM can't reach the server and every managed session hangs.
- For **Antigravity (agy)** agents only: the one-time `agy /login` and a running
  token harvester — see [Antigravity](#antigravity-agy-egress). Claude-only
  pipelines (like the Quickstart) need none of this.

## Quickstart

Get a reviewed change on a branch with the minimal all-Claude pipeline
([`examples/quickstart/`](./examples/quickstart/) — one coder + one reviewer, no
Antigravity, no human-approval gate).

**1. Install** this package into the **same environment** Omnigent lives in:

```bash
which omni                                   # -> /path/to/env/bin/omni
uv pip install -e /path/to/sbx-omnigent-launcher
#   (or, with the venv active:  pip install -e . --no-deps)
```

This installs the console scripts `omni-sbx`, `omni-sbx-pipeline`, `omni-sbx-agy`,
`omni-sbx-swarm`, `omni-sbx-worktrees`. (If a pre-existing editable install
predates the `omni-sbx-pipeline` entry point and it's missing from your PATH, use
`python -m sbx_omnigent.runner` — identical flags — or reinstall.)

**2. Configure the provider.** Add the `sandbox:` block from
[`config.sample.yaml`](./config.sample.yaml) to your Omnigent server config
(`~/.omnigent/config.yaml` or `./.omnigent/config.yaml`). Set an absolute
`worktree_root` — the swarm layer needs it:

```yaml
sandbox:
  provider: sbx
  server_url: http://host.docker.internal:6767   # host gateway, NOT localhost
  sbx:
    image: ghcr.io/omnigent-ai/omnigent-host:latest
    worktree_root: /srv/swarm/worktrees          # absolute host dir
    unset_env: [ANTHROPIC_API_KEY, CLAUDECODE]    # for Claude via subscription
```

**3. Two one-time setup steps** the swarm can't run without:

- **[Network policy](#network-policy)** — allow the microVM's dial-back
  (`sbx policy allow network "host.docker.internal:6767,localhost:6767"`).
- **[Credentials](#credentials)** — store your Claude secret so the microVM agents
  can authenticate (for subscriptions, the zero-`/login`
  [`setup-token` path](#claude-via-subscription-no-per-vm-login)).

**4. Point the example at your repo.** Edit
[`examples/quickstart/pipeline.yaml`](./examples/quickstart/pipeline.yaml): set
`repo:` to your project and fill in `task:` + `acceptance:`.

**5. Run it** — two commands: the server registers the pipeline's agents at
startup, then the runner fires it.

```bash
# Start the server WITH the pipeline (registers its agents), same flags as `omni server`:
omni-sbx server -c <your-config.yaml> --pipeline examples/quickstart/pipeline.yaml

# Fire the run (--keep leaves the VMs + worktrees up for inspection):
python -m sbx_omnigent.runner -c examples/quickstart/pipeline.yaml \
  --canonical-root /srv/swarm/canonical \
  --worktree-root  /srv/swarm/worktrees      # must equal sbx.worktree_root
```

In a **campaign**, each module's pull request is stacked on the module below it
(`publish.stack`, on by default), so it shows only that module's work rather than
every unmerged module before it. See
[`docs/PIPELINES.md`](./docs/PIPELINES.md#publish).

The coder implements your task on an isolated branch, the reviewer reads it `:ro`
and must `VERDICT: APPROVED` (a block loops findings back for another round), and
the reviewed branch is published (`publish: local` pushes `pipeline/quickstart`;
`publish: pr` opens a draft PR). Every microVM is torn down at the end — but
each reviewer's full report is captured before its VM goes, kept in the run
directory and committed beside the plan, so the reasoning behind a decision to
ship outlives the reviewer that made it.

**Next:** the richer examples add the full cadre —
[`mixed-models/`](./examples/mixed-models/) (heterogeneous models + interactive
planning) and [`tdd-race/`](./examples/tdd-race/) (a TDD writer feeding two
competing coders + a judge). Both use Antigravity, so they also need the
[agy harvester](#antigravity-token-harvester-omni-sbx-agy).

## Defining pipelines — the DAG model

A pipeline is one `pipeline.yaml`: a set of **agents** and a **DAG of stages**
that route their work along git branches. The full reference is
[`docs/PIPELINES.md`](./docs/PIPELINES.md); the essentials:

```yaml
version: 1
name: my-pipeline          # optional; defaults to the file stem (the agent namespace)
repo: /path/to/project     # or a GitHub URL — worktrees are cut from here
base_branch: main          # optional; branch to cut from / publish onto
publish: pr                # pr | local | none  (or a { mode, branch, stack } map)

task: |                    # optional — omit task+acceptance for provision-only
  What to build.
acceptance: |              # optional — the done contract, relayed to every agent
  How it's judged complete.
context: |                 # optional — project facts baked into EVERY agent's
  Stack, conventions, etc.  #   system prompt (see "Configuring agents")

agents:                    # WHO is on the pipeline
  plan:  { template: planner,  harness: antigravity-native, model: gemini-3.5-flash }
  build: { template: coder,    model: claude-sonnet-5, effort: medium }
  sec:   { template: security-reviewer, model: claude-sonnet-5 }

stages:                    # the DAG — what runs, in what order, with what edges
  - { id: plan,  run: plan }
  - { id: build, run: build, write: true, needs: [plan] }
  - id: review
    run: [sec]
    needs: [build]
    gate: consensus
    on_block: build
```

### Configuring & customizing agents

Each `agents:` entry is one participant:

- **Prompt** — exactly one of `template:` (a shipped role, below), an inline
  `prompt:`, or a `prompt_file:` (path relative to the yaml). Templates are the
  quick path; `prompt`/`prompt_file` fully customize behavior.
- **`harness:`** — `claude-native` (default), `antigravity-native` (agy), or
  `codex-native`.
- **`model:` / `effort:`** — pinned per session at create time (not baked into the
  bundle, which native harnesses ignore). `model` reaches every harness; `effort`
  is honored by the Claude harnesses — **agy has no effort knob**, so omit it there.
- **`skills:`** — a directory copied verbatim into the agent's bundle
  (Polly-style), to layer repo- or role-specific guidance on top of the base
  prompt. See [`examples/tdd-race/skills/tdd/`](./examples/tdd-race/skills/tdd/).

Prompt customization has **two levels**: per-agent (`prompt`/`skills`, above) and
**pipeline-wide** — a top-level **`context:`** field is baked into *every* agent's
system prompt (project facts all roles share: stack, conventions, what not to
touch). It lives in the stable system-prompt prefix, so it's cheap for multi-turn
agents and identical for single-turn ones; editing it needs a server restart
(like any prompt change). Details in
[`docs/PIPELINES.md`](./docs/PIPELINES.md#shared-agent-context).

`task:`, `acceptance:`, and `context:` each also accept a **`<key>_file:`** variant
(`task_file:`/`acceptance_file:`/`context_file:`) that reads the text from a path
relative to the pipeline file — handy for a long, reusable brief kept in its own
file (give only one of each inline/file pair).

**Shipped templates** (name them in `template:`, override with `prompt`/`skills`):

| Template | Role |
| --- | --- |
| `planner` | Read-only design plan (prose, no code). Triggers [interactive planning](#interactive-planning). |
| `tdd-writer` | Writes **tests only** from the plan; never implementation. |
| `coder` | Implements to the acceptance contract; makes upstream tests pass. |
| `security-reviewer` | Read-only security audit; ends with `VERDICT:`. |
| `bug-reviewer` | Read-only correctness audit; ends with `VERDICT:`. |
| `judge` | Compares competing candidate branches; ends with `SELECT: <id>`. |
| `refactoring` | Behavior-preserving cleanup of working code; keeps tests green. |

### The stages (the DAG)

Each stage is one node (or a `parallel:` fan-out of nodes). A node's **kind** is
inferred from its keys:

| Kind | Trigger | Mount | Does |
| --- | --- | --- | --- |
| **reader** | (default) | `:ro` | Produces text (e.g. a planner's design) consumed downstream as context. |
| **writer** | `write: true` | isolated `rw` worktree + branch | Implements; its committed branch is the artifact. |
| **review** | `gate:` / `on_block:` | `:ro` on the target writer's branch | One or more reviewers vote via `VERDICT:`; a block loops findings back to `on_block`. |
| **judge** | `selects:` | `:ro` compare tree | Reads competing writer branches side by side and picks a winner via `SELECT:`. |

Edges are `needs:` (and the explicit `from:` seed override). **Branches are the
artifacts**: a writer's worktree is *cut from* its upstream branch (a coder
inherits the TDD writer's tests), competing writers each get an isolated branch
off the same seed, and a judge checks out every candidate side by side. The
trusted host plane does **all** git — agents never run it.

**Omit `stages:` entirely** and the runner synthesizes the classic swarm from the
agents: a `build` writer (the first coder/tdd-writer, or an agent named
`build`/`coder`) → a `review` consensus stage over everyone else, looping back on
a block. So the smallest pipeline is just `agents:` + `repo:`.

### Interactive planning

If a pipeline includes a **`planner`**, the plan stage is **interactive by
default**: the planner posts its design and questions in the Omnigent UI session
`<name>/plan`, and **nothing downstream runs until a human replies `APPROVED`**
there (up to 1 hour). On approval the planner emits a clean consolidated plan
that's shared with every builder. So **drive a planner pipeline live** — or pass
**`--no-interactive-plan`** to run unattended (single-turn plan, no gate). While
you plan, the writer VMs pre-warm in the background so the swarm is ready the
instant you approve.

The planner's whole session is committed too, as **`docs/plans/<name>-session.md`**
— every draft, your answers, and the diagrams a consolidated plan would summarize
away (messages only, never tool output). See
[The planning session record](./docs/PIPELINES.md#the-planning-session-record).

The approved plan is also committed to **`docs/plans/<name>.md`** on the published
branch (override with the top-level `plan_artifact:` key), so the design travels
with the code.

### Publishing & running

`publish:` selects the output: `local` (push the branch under `pipeline/<run>`),
`pr` (push + draft GitHub PR), or `none`. PR mode needs a **GitHub** target —
either `repo:` is a GitHub URL, or `repo:` stays local (fast clone) and you point
publishing at GitHub with `--publish-repo <url>`. A mismatch is refused at startup
rather than after the build. Fire with the runner:

```bash
omni-sbx server -c <config.yaml> --pipeline my-pipeline.yaml     # register agents (startup)
python -m sbx_omnigent.runner -c my-pipeline.yaml \              # fire the run
  --canonical-root /srv/swarm/canonical --worktree-root /srv/swarm/worktrees
```

Editing an agent's prompt/harness/skills needs a **server restart** (agents
register at startup); model/effort/task/DAG edits are picked up by the runner with
no restart. Key runner flags: `--keep` (leave VMs up), `--run-id`,
`--publish-repo`, `--publish-token-file` / `--publish-token-command`,
`--no-interactive-plan`, `--no-auto-harvest`, `--resume`, `--turn-timeout`,
`--server`. Full flag table and the provision-only mode (omit `task:`) are in
[`docs/PIPELINES.md`](./docs/PIPELINES.md).

By default the push and the PR use whatever git/`gh` credentials the host already
has — that is, **yours**. To have the pipeline ship under its own account instead,
give the runner a token without putting it on the command line; see
[Publishing identity](./docs/PIPELINES.md#publishing-identity).

### Resuming a failed run

A run is long and mostly unattended, so one late failure should not cost you the
whole thing — least of all a planning gate you already sat through. Re-run the
same command with the **same `--run-id`**, plus `--resume`:

```bash
python -m sbx_omnigent.runner -c my-pipeline.yaml --run-id discover --resume \
  --canonical-root ~/sbx-canonical --worktree-root ~/sbx-worktrees
```

It reuses the run's existing clone, **skips every stage that already finished**
(and every campaign module already published), and re-runs only what is left. An
approved plan of record, a judge's pick, and how far a campaign got are all
restored, so nothing asks you the same question twice.

Two things make that possible, both automatic:

- **A failed turn no longer throws away its work.** A writer's tree is committed
  to its branch even when the turn errors — so a stage that produced a full test
  suite and then hit its timeout keeps it, and the resume re-cuts that stage
  *from its own branch* to carry on rather than start over.
- **A run that doesn't finish keeps its directory.** Teardown disposes the
  microVMs either way, but the hub clone and `state.json` — the only copy of
  what resume reads — survive a failure or a block, and the runner prints the
  path and the command to continue with.
- **Progress is checkpointed as the run goes**, to `<run>/state.json` beside the
  hub clone (never in your repo — no agent sees it, no node commits it).

Fix whatever broke first: a resume re-runs the failed stage, so it needs the
cause gone. Runner-side fixes are picked up automatically (the runner reloads
its code); agent prompt/harness edits need a server restart, as always.

Resume refuses rather than guesses — no state file (a run predating this, or one
whose directory was removed) or a state from a different build both stop with a
clear message, and you start a fresh `--run-id`. Details and the exact
skip semantics: [Resuming a run](./docs/PIPELINES.md#resuming-a-run).

> **Also worth knowing:** a stage that stops to ask you a question spends that
> wait inside its turn budget. If your pipeline is interactive, raise
> `turn_timeout:` (or pass `--turn-timeout`) above the 1800s default so a
> question you take ten minutes to answer can't kill the stage that asked it.

### Verifying that a writer actually implemented

Every gate after a writer is an agent's self-report, so the runner adds the one
check it can make honestly: after a writer commits, it diffs the branch against
the tree it was cut from and requires a real source change, not just lockfiles.
A no-op is re-driven with the evidence and then fails the stage — because a
writer that quietly did nothing will otherwise be approved by reviewers who
never ran anything either. Reviewer templates now refuse to approve a change
they could not execute, and **`setup:`** in the pipeline tells every builder and
reviewer how to install the toolchain they need — in prose, addressed to an
agent. The gate needs the same thing in **shell**, under `verify.setup:`; a
pipeline with both a `setup:` block and a `verify:` gate must set it, and says
so at load time rather than failing the gate on a sentence.

**`verify:`** goes further: before anything publishes, the runner runs your own
test/coverage command in a **fresh disposable microVM** on a clean clone of the
branch (size it with `verify.cpus`/`verify.memory` — the gate builds its own box,
so the server's per-sandbox limits do not reach it), and publishes only on exit 0 — no agent reports a number, and nothing
executes on the host. Add **`verify.demo`** and a second (hermetic) command runs
in the same sandbox, with its output becoming the PR's **proof the thing runs** —
alongside the test transcript and links to the design and the planning session. A shortfall loops back to the writer that produced the
branch, which may add tests but not weaken existing ones. See
[Verification](./docs/PIPELINES.md#verification--the-one-check-the-orchestrator-makes-itself).

## Network policy

`sbx` microVMs run under a default-deny egress policy: outbound traffic is
blocked unless a policy rule permits the destination. The sandboxed host must
dial back to your Omnigent server, so that destination has to be allowed
explicitly — **without this rule every managed session hangs** before the host
tunnel connects (the microVM reaches nothing, so `omnigent host` never
registers).

Allow exactly the host and port your `server_url` names — nothing wider. In
practice a local server needs **both** `host.docker.internal:6767` and
`localhost:6767` allowed: the dial-back only succeeds with both rules present,
and removing the `localhost:6767` rule breaks the connection even though
`server_url` names `host.docker.internal`. (The exact reason is unconfirmed —
likely how `sbx`'s network layer resolves the host gateway when it evaluates
the policy — but the requirement is observed, so allow both.)

```bash
# Match the port to server_url. 6767 is Omnigent's default local-server port.
sbx policy allow network "host.docker.internal:6767,localhost:6767"
```

Then confirm both rules are in place:

```bash
sbx policy ls --type network        # expect host.docker.internal:6767 AND localhost:6767
```

**Scope to the port, not the whole host.** `host.docker.internal` is the entire
Docker host gateway, so a bare `sbx policy allow network host.docker.internal`
(no port) lets the sandboxed agent reach *every* service listening on your
machine — other dev servers, databases, SSH, other containers' published ports.
The host needs only the one server port, so the `:6767` suffix keeps the
sandbox to a least-privilege footprint. If you already added the broad rule,
remove it and keep only the scoped one:

```bash
sbx policy rm network --resource host.docker.internal   # drop the port-less rule
```

Keep the rule **global** (the default — all sandboxes). Managed sessions
provision a fresh sandbox with a generated name each time, so a `--sandbox`
-scoped rule wouldn't match the next session's box.

For a **remote** server (not `host.docker.internal`), allow that hostname and
port instead, e.g. `sbx policy allow network omnigent.example.com:443`.

### Antigravity (agy) egress

Only if you run **Antigravity** agents (`harness: antigravity-native`, which
drives the `agy` CLI already bundled in the `omnigent-host` image). agy
authenticates with Google OAuth and calls the Gemini / CloudCode APIs, so the
default-deny microVM additionally needs those domains allowed — otherwise
`agy /login` and every turn fail:

```bash
sbx policy allow network "*.googleapis.com,accounts.google.com,antigravity.google,antigravity-unleash.goog,*.googleusercontent.com,antigravity-cli-auto-updater-974169037036.us-central1.run.app"
```

This covers OAuth token exchange (`oauth2.googleapis.com`), the model APIs
(`cloudcode-pa` / `cloudaicompanion.googleapis.com`), feature flags, profile
assets, and agy's self-updater. It's on top of the dial-back rule above, not a
replacement. agy stores its OAuth token in the microVM at
`~/.gemini/antigravity-cli/antigravity-oauth-token`; auth is **not** an API key
(no proxy-injected secret like the Claude path — see [Credentials](#credentials)).
Claude-based agents don't need any of this.

### Antigravity token harvester (`omni-sbx-agy`)

To run agy agents **zero-exposure** — no durable Google credential ever on a
YOLO agent VM — auth uses **harvest / proxy-swap**. Each agent VM carries only a
fixed **placeholder** access token; an sbx custom secret swaps that placeholder
for a **real**, freshly-harvested token in the outbound `Authorization: Bearer`
header on the wire (agy does not cert-pin, so the sbx MITM is accepted). The
real token is short-lived (~1h), so one **trusted auth-agy box** holds the
*only* refresh token and self-refreshes; the host-side `omni-sbx-agy` helper
keeps the swap secret's value current.

The swap secret is scoped to `**.googleapis.com` (plus `*.googleapis.com` and
the explicit Vertex/CloudCode hosts). The `**` (any number of labels) is
**required** — enterprise/GCP accounts route the model call to the 3-label
regional host `aiplatform.us.rep.googleapis.com`, which a single-`*` wildcard
misses, yielding `UNAUTHENTICATED (401)`.

**One-time setup** — stand up the trusted box and log in once:

```bash
omni-sbx-agy bootstrap                      # creates agy-auth-trusted (isolated,
                                            # NOT an Omnigent host), scopes egress,
                                            # seeds the swap secret placeholder
sbx exec -it agy-auth-trusted agy           # run '/login', complete browser consent
```

**Always-on** — keep the swap secret fresh. **A pipeline run starts one for
you**: if the pipeline declares any agy agent and the swap secret is stale, the
runner launches a harvester, waits for the first refresh, and stops it when the
run ends (`--no-auto-harvest` opts out). Run one yourself only for non-pipeline
work, or to keep the secret warm between runs:

```bash
omni-sbx-agy harvest                        # ~30-min force-refresh loop
omni-sbx-agy harvest --once                 # single cycle (cron / verification)
```

Only **one** harvester may run at a time — each cycle rewrites the trusted box's
token file before forcing a re-mint, so two would race. That is enforced with an
exclusive lock (`~/.sbx-swarm/agy-harvest.lock`): a second one refuses to start,
and the runner detects a harvester you already have running and waits for it
instead of competing. An auto-started one logs to `~/.sbx-swarm/agy-harvest.log`.

Each cycle force-expires the trusted box's on-disk token, runs `agy models` (the
lightest authenticated action) to make agy re-mint via OAuth, reads the fresh
access token off the box, and writes it into the swap secret's value **on
stdin** — the token never appears in a process argument or a log line (only a
redacted `sha256:…` fingerprint is printed). The trusted box's minimal
steady-state egress is just `oauth2.googleapis.com` + `cloudcode-pa.googleapis.com`;
`bootstrap` opens a wider set for the one-time interactive login.

If the trusted box's refresh token is revoked/expired, `harvest` prints a loud
`RE-LOGIN REQUIRED` and keeps probing — re-run `sbx exec -it agy-auth-trusted agy`
and `/login`, and it recovers on the next cycle. The swap secret uses a **fixed**
placeholder, so each refresh **updates it in place** (no duplicate placeholders —
keep exactly one; see [Credentials](#credentials)).

### Running agy agents

> For the full design + the debugging record of everything it took to get agy
> working (auth, onboarding, GCP project, reply capture, and the non-obvious
> gotchas), see [`docs/ANTIGRAVITY.md`](./docs/ANTIGRAVITY.md).

Once the harvester is live, opt the **server** into seeding agy agents (in your
`sandbox:` block):

```yaml
sandbox:
  sbx:
    agy_enabled: true                # enable agy seeding (applied only to agy VMs)
    agy_enterprise: true             # set ONLY for a Business/enterprise Google account
    agy_gcp_project: your-project-id  # REQUIRED for a Business/Vertex account
    agy_gcp_location: us             # optional, defaults to us
```

With `agy_enabled` on, the launcher writes the **placeholder** OAuth token +
onboarding marker into each **agy** VM's `~/.gemini` at creation, so an
`antigravity-native` agent passes Omnigent's readiness gate before its runner
launches (the real token arrives via the wire swap). Only agy VMs are seeded —
the swarm tags them with an `-agy` mount-sentinel suffix, so a Claude agent in
the same server is never touched. `agy_enterprise` sets `enterpriseOnboardingComplete` in
the seeded marker — **required** on a Business/enterprise account, or agy re-runs
enterprise onboarding (theme picker + EULA) that a headless VM cannot answer.

On a Business/enterprise (Vertex) account you must **also** set `agy_gcp_project`
— the GCP project agy runs cascades against, or a turn fails with `invalid
project ID` (model *listing* works without it, but running the agent does not).
The launcher seeds it into the VM's agy `settings.json`. When `agy_enterprise` is
on (or a project is set), the launcher additionally patches the in-VM Omnigent
bridge module at VM creation — before the runner imports it — so agy's first-run
onboarding wizard is skipped and the project settings reach agy's per-session
dir (a runtime, per-VM, best-effort edit inside the throwaway sandbox; it never
touches your Omnigent install).

Now name `harness: antigravity-native` on any pipeline agent (e.g. the `plan` and
`impl-b` agents in [`examples/tdd-race/`](./examples/tdd-race/)), or bind the
bundled **`swarm-agy-coder`** / **`swarm-agy-reviewer-bug`** agents in a
coordinator swarm. For the coordinator flow, acknowledge agy support with `--agy`
(or `OMNI_SBX_AGY_ENABLED=1`):

```bash
omni-sbx-swarm start --agy --swarm-id demo --repo-url … \
  --coder-agent swarm-agy-coder --reviewer-agent swarm-agy-reviewer-bug …
```

If an agy agent is bound **without** `--agy`/the env var, `start` **fails loud**
up front (server-side `sbx.agy_enabled` is the real enforcement; this just turns
a cryptic in-VM auth failure into an actionable start-time error). Agy agents
need the harvester **running** — a stale/absent swap secret means every turn
401s.

### Per-VM allowlists and the deny-all baseline

The launcher scopes every managed VM to a per-sandbox allowlist —
[`sbx.egress_allow`](#configuration-reference) (a curated default of LLM +
package-registry hosts unless you override it) plus the dial-back. This is the
tight, per-agent boundary.

> **Debian `apt` (port 80).** The default allowlist includes
> `deb.debian.org:80`. sbx's own `default-os-packages` bundle allows
> `**.debian.org:443` but — unlike its Ubuntu entries, which list `:80`
> explicitly — never port 80, and the host image's apt sources are
> `http://deb.debian.org`. Without it every `apt` call is denied and an agent
> cannot install a toolchain: seen live as a reviewer spending its entire turn
> hunting for a `cargo` that could never be installed, then returning no
> `VERDICT` — which the runner reads as BLOCKING. Port 80 is safe here: Debian
> packages are GPG-signed, so only *which* packages you fetch is disclosed,
> never their integrity. If you **replace** `sbx.egress_allow` with a custom
> list, carry this entry over.

That boundary only *restricts* if there are no **broad global allow rules**
underneath it: global allows apply to every sandbox and are additive, so a
scoped allowlist can't reduce below them. sbx ships permissive global bundles
(`default-ai-services`, etc.). On `omni-sbx server` startup the launcher
**warns** (never changes) when such global allows exist and advises the opt-in,
one-time command to adopt a strict deny-all baseline:

```bash
sbx policy reset && sbx policy init deny-all   # wipes ALL global sbx rules
```

This affects **every** sbx sandbox you have — the launcher re-scopes swarm VMs
automatically, but re-scope any other sandboxes you use yourself. It's your
call; the launcher only advises.

## Configuration reference

All keys live under `sandbox:` in your server config.

| Key | Required | Meaning |
| --- | --- | --- |
| `provider` | yes | Must be `sbx`. |
| `server_url` | yes | URL the microVM dials back to. Local server → `http://host.docker.internal:<port>`. |
| `sbx.image` | no | Boot image. Default `ghcr.io/omnigent-ai/omnigent-host:latest`. **Pin to match your server version.** |
| `sbx.profile` | no | `sbx` governance profile (least privilege). Manage with `sbx policy`. |
| `sbx.cpus` | no | CPU cap per sandbox (positive int). |
| `sbx.memory` | no | Memory cap per sandbox, e.g. `8g`. |
| `sbx.worktree_root` | no | Absolute host dir holding per-swarm worktrees. Enables the collaborative-swarm/pipeline mount path: a `git@sbxmount:<path>#<rw\|ro>` workspace bind-mounts `<path>` (which must resolve strictly under this root) into the microVM. Omit to disable mounting. |
| `sbx.provision_stagger_s` | no | Settle gap (seconds) between consecutive `sbx create` calls, on top of the serialization always applied. Prevents a swarm's near-simultaneous VM launches from racing the sbx daemon's proxy injection (an intermittent `500 … failed to inject network proxy`). Default `2.0`; set `0` to keep serialization but drop the extra gap. |
| `sbx.egress_allow` | no | Per-sandbox network **allowlist** applied (scoped) to every managed VM, **plus the derived Omnigent dial-back**. Three-way: **unset → a curated default baseline** (LLM endpoints + trusted package registries — `DEFAULT_EGRESS_ALLOW`) so agents work out of the box; **`[]` (empty) → dial-back only** (block everything except the mandatory server connection — max lockdown); **a non-empty list → those hosts** (a stricter/custom set replacing the default), e.g. `[api.anthropic.com]`. Additive under a permissive baseline; the VM's only reachable set under a **deny-all** baseline. See [Network policy](#network-policy). |
| `sbx.unset_env` | no | Env var names stripped from the host launch (`env -u`). For Claude `/login`, use `[ANTHROPIC_API_KEY, CLAUDECODE]`. See [Credentials](#credentials). |
| `sbx.agy_enabled` | no | Default `false`. When `true`, seed each **agy** VM with the agy (Antigravity) **placeholder** OAuth token + onboarding marker at creation, so an `antigravity-native` agent passes Omnigent's readiness gate (the real token arrives via the wire swap). Only agy VMs are touched — the swarm tags them with an `-agy` mount-sentinel suffix; a Claude VM in the same server is never seeded. Requires a running harvester (`omni-sbx-agy`). See [Running agy agents](#running-agy-agents). |
| `sbx.agy_enterprise` | no | Default `false`. Sets `enterpriseOnboardingComplete` in the seeded onboarding marker (and patches the in-VM bridge so agy's first-run wizard is skipped) — set `true` for a **Business/enterprise** Google account, else agy re-runs enterprise onboarding a headless VM can't answer. Ignored unless `agy_enabled`. |
| `sbx.agy_gcp_project` | no | GCP project id seeded into the VM's agy `settings.json` — **required for a Business/enterprise (Vertex) account** or an agy turn fails with `invalid project ID` (model listing works without it; running a cascade does not). Unset seeds no project. Ignored unless `agy_enabled`. |
| `sbx.agy_gcp_location` | no | GCP location for the seeded project block. Default `us`. Ignored unless `agy_enabled`. |

Credentials are intentionally **not** a launcher config key — see [Credentials](#credentials).

### Environment variables

Read from the **server process** environment (not the YAML config):

| Var | Effect |
| --- | --- |
| `SBX_KEEP_SANDBOXES` | Any non-empty value keeps a sandbox in place instead of removing it on teardown. By default a **failed** managed launch deletes the box (and its in-VM `/tmp/omnigent-host.log`) before you can read it; set this to inspect why a host never came online. |
| `OMNI_SBX_NO_SWARM_AGENTS` | Any non-empty value skips auto-registering the bundled swarm agents at startup (run the microVM provider only). |

The runner and swarm coordinator read a further set of `OMNI_SBX_*` vars (agent
ids, roots, publish mode) — documented in [`docs/PIPELINES.md`](./docs/PIPELINES.md)
and [`agents/README.md`](./agents/README.md).

When `SBX_KEEP_SANDBOXES` is set, a failed launch prints the exact follow-up commands, e.g.:

```bash
sbx exec <name> -- cat /tmp/omnigent-host.log   # why the host never registered
sbx rm -f <name>                                # remove it when done
```

## Coordinator-driven swarms (alternative)

Before the declarative pipeline, the same review-loop ran through a
**`swarm-coordinator`** agent you chat with: you open a coordinator session, give
it a task + repo + the coder/reviewer agent ids, and it drives the loop and
publishes. The coordinator/coder/reviewer agents are auto-registered on
`omni-sbx server`. This flow is still fully supported and is handy for
interactive, exploratory work; the pipeline replaces it for a *defined* swarm you
want to fire repeatably. See [`agents/README.md`](./agents/README.md) (setup +
usage), [`docs/COLLABORATIVE_SWARM_DESIGN.md`](./docs/COLLABORATIVE_SWARM_DESIGN.md)
(the full design + rationale), and the [`basic/`](./examples/basic/) /
[`team/`](./examples/team/) examples.

## How this was figured out (background)

This design came from tracing the Omnigent source rather than guessing. The key
findings, in order:

1. **`claude-native` is a bridge, not a spawner.**
   `omnigent/inner/claude_native_executor.py` says it "does not launch Claude
   itself" — a wrapper launches Claude Code in a tmux pane and the executor
   injects messages via `tmux send-keys`. That killed the idea of wrapping
   `claude` in a sandbox from outside, because the bridge is host-path-based.

2. **The launch command is pluggable — but on-host.**
   `omnigent/claude_launcher.py` (`resolve_claude_launch` + the
   `OMNIGENT_CLAUDE_LAUNCHER` entry-point plugin, used by Databricks' `isaac`)
   can rewrite the launch command. But it only wraps the on-host `claude`
   process while the runner/bridge stay on the host — good for an on-host
   wrapper, not for moving execution into a microVM.

3. **Omnigent already runs hosts in remote sandboxes.**
   `omni sandbox` (`create`/`connect`) provisions a sandbox and runs
   `omnigent host` inside it, registering it with the server. The pluggable
   layer is `omnigent/onboarding/sandboxes/base.py` (`SandboxLauncher`), with a
   prebaked multi-arch host image `ghcr.io/omnigent-ai/omnigent-host:latest`.
   `sbx` is not a built-in provider, but the interface is exactly what a new one
   implements.

4. **Managed hosts accept an injected launcher.**
   `omnigent/server/managed_hosts.py` builds a `ManagedSandboxConfig` (which
   carries a `launcher_factory`) and is documented as supporting
   deployment-injected custom launchers. The stock server reads it from YAML via
   `parse_sandbox_config` (`omnigent/cli.py` → `create_app(sandbox_config=...)`),
   whose provider dispatch is hardcoded — hence the startup wrap instead of a
   source edit.

5. **The `omnigent-host:latest` image was probed and passed.** In an `sbx`
   sandbox booted from it: `omnigent 0.4.0`, and `claude`, `tmux`, `git`, `curl`
   all present, `$HOME` (`/root`) writable, `omnigent host --server <url>`
   available. `sbx exec` forwards stdin and args, and its startup banner goes to
   stderr (so stdout stays clean for protocol traffic). A local server was
   reachable from the sandbox via `host.docker.internal` (HTTP 403 = reached,
   just unauthenticated — which the managed launch token resolves) **once an
   egress policy allowed that host:port** — under the default-deny policy the
   dial-back is blocked and the host never registers (see
   [Network policy](#network-policy)).

## Credentials

Credentials are handled entirely by `sbx`, **not** by this launcher. `sbx` runs
an HTTP/HTTPS proxy on the host that intercepts the agent's outbound API calls,
looks up the matching secret on the host, and overwrites the auth header before
forwarding — *"the real credential stays on the host; the sandbox sees only a
sentinel value."* So no key is ever placed in the microVM's environment or on
its process table. (Full model:
<https://docs.docker.com/ai/sandboxes/security/credentials/>.)

> **The launcher makes this actually reach the agent.** sbx performs the header
> swap only on its **forward** proxy, but Omnigent's runner strips the proxy env
> vars (`HTTPS_PROXY`/`NODE_USE_ENV_PROXY`/…) before spawning the harness — so the
> agent would bypass the forward proxy and send the sbx placeholder verbatim
> (auth fails). This launcher forwards those vars past the runner's allowlist
> (`OMNIGENT_RUNNER_ENV_PASSTHROUGH`, automatic — no config), keeping only the
> server dial-back and runner↔harness IPC on `NO_PROXY`. Without it, sbx
> credential injection reaches **no** Omnigent harness, for any provider.

You store secrets once, out-of-band, on the server host. Two forms:

**Built-in services** — `sbx` knows the provider's domains and header. Store the
key globally and every new sandbox is covered:

```bash
echo "$ANTHROPIC_API_KEY" | sbx secret set -g anthropic
echo "$GEMINI_API_KEY"    | sbx secret set -g google   # generativelanguage.googleapis.com, …
```

**Custom** — use this when the agent reads a specific env var and/or you want to
pin the exact host. It also sets that env var to a **placeholder inside the VM**,
which matters for Omnigent: an agent spec with `api_key: ${GEMINI_API_KEY}`
needs *something* to resolve at config time, and the proxy then swaps the real
value on the wire:

```bash
sbx secret set-custom -g \
  --host generativelanguage.googleapis.com \
  --env GEMINI_API_KEY \
  --value "$GEMINI_API_KEY"
```

Notes:
- **Global (`-g`) secrets apply at sandbox *creation*.** Change one → recreate
  the sandbox (new session) for it to take effect.
- **Placeholder vs. validation.** If the harness validates the key's *format* at
  boot, a sentinel like `proxy-managed` may not pass; `set-custom` lets you
  shape the placeholder. This is the most likely thing to tune for a given
  harness — verify with a real turn.
- **`git` / GitHub** works the same way via `sbx secret set -g github`.

### Claude via subscription (no per-VM `/login`)

To run Claude on a **subscription** (not an API key) without an interactive
`/login` in every fresh microVM, use a long-lived subscription **OAuth token**
injected through the sbx proxy — the agent authenticates automatically and never
sees the real token. Verified end-to-end (`claude-native`; works for `claude-sdk`
too).

One-time setup on the server host:

```bash
# 1. Mint a long-lived subscription token (interactive OAuth, one-time).
claude setup-token                       # prints an sk-ant-oat… token

# 2. Store it zero-exposure: OMIT --value so the token is read from a HIDDEN
#    prompt (no shell history, no argv), and shape the placeholder like a real
#    token so Claude Code's format check passes.
sbx secret set-custom -g \
  --host api.anthropic.com \
  --env CLAUDE_CODE_OAUTH_TOKEN \
  --placeholder 'sk-ant-oat01-{rand}'
#    → "Enter secret:"  (paste the token, press Enter — input is hidden)
```

Keep `unset_env: [ANTHROPIC_API_KEY, CLAUDECODE]` in your `sbx:` config so the
baked `ANTHROPIC_API_KEY=proxy-managed` sentinel can't force API-key mode over
the OAuth token:

```yaml
sbx:
  unset_env: [ANTHROPIC_API_KEY, CLAUDECODE]
```

Notes:
- **Do NOT** store a built-in `anthropic` secret for this — it is API-key-only
  (`SBX_CRED_ANTHROPIC_MODE=apikey`, injects `x-api-key`) and cannot carry a
  subscription bearer; it also re-introduces the `ANTHROPIC_API_KEY` sentinel.
- **Exposure:** the token exists only encrypted at rest in sbx's store on the
  host — never in the VM (the agent sees only the placeholder, proxy-swapped on
  the wire), never in shell history/argv (the hidden `set-custom` prompt).
- **One rule only:** if `set-custom` is run twice you get two placeholders for
  the same env var; the injected value and the swap rule can then disagree and
  auth fails. Keep exactly one (`sbx secret ls`; remove extras with
  `sbx secret rm -g --placeholder <value> -f`).
- Works because the launcher forwards the sbx proxy env to the harness (see the
  callout above) — that is what lets the swap reach Claude Code.

**Alternative (manual):** you can instead run `/login` in each fresh session's
terminal. It works, but re-prompts per VM (the token lives in the VM's
per-sandbox `~/.claude`), so the `setup-token` path above is preferred.

### Publish token: reading it without a prompt

`--publish-token-command` is re-run **at push time** (TASKS.md #43), hours
after the run began, so a token rotated mid-run is picked up and a rejected
push can retry against the current value. That imposes two requirements that
look contradictory:

- it must read the **live store** — never a file you copied the token into.
  The copy goes stale the moment you rotate, and the retry then re-reads the
  same stale value and correctly reports that nothing changed.
- it must answer with **no GUI prompt** — a keychain dialog raised hours into
  an unattended run blocks the publish with nobody there to click it.

Both hold if the store answers silently *for one program only*.
`tools/keychain-token` is that program: a signed binary that reads one generic
password. Grant the keychain item to it and to nothing else, and
`/usr/bin/security` — along with every other program — still prompts for that
item.

**It cannot be a shell script.** A keychain ACL authorizes the running
process's code identity, and for a script that identity is the interpreter
(`/bin/sh`) — so "trust my script" would trust every script on the machine.

Build and install it:

```bash
sh tools/build-keychain-token.sh          # → ~/.local/libexec/keychain-token, mode 0700
```

Grant it the item, once. `-T ""` drops the trust the creating program would
otherwise inherit, and `-w` last with no value makes `security` prompt for the
secret so it never reaches argv:

```bash
security find-generic-password -w -s <service>        # copy the current value
security delete-generic-password -s <service>
security add-generic-password -a <account> -s <service> \
    -T "" -T "$HOME/.local/libexec/keychain-token" -w
```

Point the runner at it. Double quotes matter: the runner splits this with
`shlex` and runs it as **argv**, so nothing in it is shell-expanded at read
time — let your shell expand `$HOME` at assignment instead.

```bash
export PUBLISH_TOKEN_CMD="$HOME/.local/libexec/keychain-token -s <service> -a <account>"
```

Verify both directions:

```bash
"$HOME/.local/libexec/keychain-token" -s <service> -a <account>   # prints the token
security find-generic-password -w -s <service>                   # must PROMPT
```

Rotation stays one command, and preserves the grant:

```bash
security add-generic-password -a <account> -s <service> -U -w
```

**Measured, on a throwaway item:**

| caller | result |
| --- | --- |
| `keychain-token` | silent read |
| a different binary, **same signing certificate** | prompts |
| `/usr/bin/security` | prompts |

Trust is per **binary** — not per certificate, and not per user.

**`chmod 0700`, applied by the build script.** Anyone who can *execute* this
binary reads the token with no prompt, which is precisely its purpose, so
execute permission is the last boundary left and it belongs to you alone.
`clang` leaves 0755 behind; the script fixes it.

**A real signing identity, never ad-hoc.** The ACL records the binary's
designated requirement. Signed with an identity that is
`identifier "com.example.token" and anchor apple generic and certificate
leaf[subject.CN] = …`, with no cdhash term — so it keeps matching across
rebuilds. Verified by rebuilding at a different optimization level (cdhash
`82e03c…` → `e9b386…`) and reading the item again, silently. Ad-hoc signing
yields a cdhash requirement instead, so every recompile would break the grant
and the next publish would start prompting; the build script refuses it.

**What this does not do.** It scopes access to one binary, not to one caller:
anything that can run it gets the token. Replacing the binary changes its
signature, so the keychain prompts rather than trusting the replacement —
tamper-evident, not tamper-proof. Re-signing with a different identifier or
certificate breaks the grant, so re-run the one-time step.

## Disk requirements

A microVM's disk is **thin-provisioned**: the guest sees a roomy filesystem
while the host grows the backing file on demand. If the host runs out mid-run,
the guest's writes fail as I/O errors and ext4 protects itself by **remounting
read-only** — after which the agent silently fails to write anything and the
harness reports an opaque `[Errno 30] Read-only file system`. Nothing in that
chain names disk as the cause.

So the runner **checks free space before provisioning** and refuses with the
numbers when it doesn't add up:

```
not enough free disk to run this pipeline: 9.5 GB free, about 56.5 GB needed:
9 microVM(s) at ~3.5 GB, 5 host worktree(s) at ~4.0 GB (a writer builds into
its clone), plus 5 GB headroom.
```

A run occupies the host **twice over**, and the estimate counts both terms:

| Term | How many | Default each |
| --- | --- | --- |
| microVMs | one per node that is still re-drivable, plus one for the verification gate, plus the *largest single* review stage's reviewers. A reviewer is freed once its round votes; a reader, a judge, and any writer no gate can loop back to are freed when their stage completes. Under `--keep` nothing is freed and they all sum | 3.5 GB |
| host worktrees | one per **writer** — the only nodes that build into their clone. A reader's or judge's tree is ~100 KB (a node clone is local, so its objects are hardlinked); a reviewer cuts none, it mounts the writer's | 4.0 GB |
| headroom | for the host itself | 5 GB |

Counting only the VMs is what let a run start on ~23 GB and exhaust the disk two
modules later — the worktrees were the larger half.

**The defaults assume a compiled project**, and they are *working* figures, not
idle ones. An idle microVM is ~1.2 GB — but a VM whose agent installs the
toolchain your `setup:` names holds that in its own overlay (rustup + a cargo
registry + `cargo-llvm-cov` measured ~3.6 GB), and the worktree separately holds
`target/`. Both are real and neither double-counts the other: the toolchain is
inside the VM, `target/` is on the host mount.

**`per_worktree_gb` is the term most worth tuning, and the spread is enormous.**
Measured on one Rust pipeline: 2.2, 2.5 and 3.1 GB per writer on a single-crate
workspace — but 8.9 GB for the node that also ran the coverage gate, and 26 GB
for a writer once that workspace reached five crates and 381 dependencies. Two
things drive it: a **coverage gate on a compiled language builds a second full
tree** (`cargo llvm-cov` uses its own target dir, so that node costs roughly
double — `pytest --cov` and `vitest --coverage` add nothing), and the dependency
graph plus incremental artifacts piling up across review rounds. The 4.0 default
suits a small compiled project; measure yours and set `disk:` rather than
assuming.

A Python or TypeScript project pays far less on both terms, so tune the per-unit
numbers rather than living with a wildly pessimistic floor:

```yaml
disk:
  per_worktree_gb: 0.2      # nothing heavy lands in the tree
  per_vm_gb: 1.5            # no toolchain to install
  headroom_gb: 5
```

### Measure your own numbers instead of guessing

Every figure above came from measuring one project. Yours will differ, and the
launcher can tell you by how much:

```bash
OMNI_SBX_DISK_METRICS=1 python -m sbx_omnigent.runner -c pipeline.yaml ...
```

It samples every node worktree, the sbx snapshot store, and host free space at
**each stage boundary** — plus a `chunk-peak` sample taken one line before a
module's reclaim, which is the concurrent peak the preflight is actually trying
to predict. Records land as JSONL in `<canonical_root>/_metrics/<run>.jsonl`,
outside the run directory so a *successful* run does not delete its own
evidence:

```json
{"bytes": 26843545600, "chunk": "m6", "event": "chunk-peak", "kind": "writer",
 "node": "impl-a", "run": "discover-6", "t": 1787353546.359, "what": "worktree"}
```

Off by default: measuring a 26 GB build tree means walking its inodes, and a
full cadre has five of them across roughly eight boundaries. Turn it on for one
run, read the peak, set `disk:` from it, turn it off.

A campaign reclaims **both** as each chunk publishes — its microVMs *and* its
node clones — so the peak is one module's worth rather than the sum. `--keep`
suppresses both reclaims, so every module accumulates; the preflight multiplies
by the module count when you pass it, instead of letting you discover that at
module four. Override the whole check with `--skip-disk-check`.

If a laptop is the constraint rather than the pipeline, [`docs/CLOUD.md`](./docs/CLOUD.md)
covers moving the whole stack to a dedicated machine — including the two config keys
(`sandbox.sbx.cpus` and `sandbox.sbx.memory`) that size the microVMs themselves.

## Known gaps / follow-ups

These are deliberately left as TODOs; the core provision/run/terminate path
works without them.

- **`resume` / `keep_alive`.** `sbx exec` appears to auto-start a stopped
  sandbox, but the stopped→running verb is unconfirmed, so `can_resume=False`
  for now. Verifying it lets dormant managed hosts survive idle instead of being
  recreated.
- **`--non-interactive` on the host.** If your server is behind an auth proxy
  that the launch token doesn't satisfy, `omnigent host` can drop into an
  interactive browser login and hang a backgrounded launch. A small `start_host`
  override adding `--non-interactive` fails fast instead.

## Maintenance

The three harness CLIs the microVM runs — `claude`, `codex`, `agy` — are NOT
version-pinned, and all three have changed behaviour under this project mid-flight
(a retired model that wedged the TUI, a permission mode silently downgraded, a
`--model` grammar that started requiring `--effort`). They also update themselves
at runtime inside the VM, so pinning the image is not enough on its own.
[`docs/HARNESS-VERSIONS.md`](./docs/HARNESS-VERSIONS.md) records the known-good
set to compare a wedged run against, and the one operational rule worth
remembering: **a run that times out on the FIRST turn of a harness gets its pane
read before anything else is investigated.**

Update Omnigent as usual:

```bash
cd /path/to/omnigent && git pull      # + `uv sync` if deps changed
# this package: nothing to do
```

Your files never live in the Omnigent tree, so there is nothing to stash or
rebase. Keep the `sbx.image` tag pinned to your server version to avoid
host↔server protocol skew. If Omnigent renames `parse_sandbox_config` or changes
`ManagedSandboxConfig`, `omni-sbx` errors at startup — fix the one wrapper in
`entrypoint.py`.

### Running the tests

The suite must run against the environment **Omnigent lives in** — that is where
`omnigent`, `click`, and `yaml` are importable. With that venv on hand:

```bash
# Simplest — stdlib only, nothing extra to install:
/path/to/omnigent-venv/bin/python -m unittest discover -s tests

# pytest (nicer output) and the linter, borrowed just for the run:
uv run --no-project --python /path/to/omnigent-venv/bin/python \
    --with pytest pytest tests/ -q
uv run --no-project --python /path/to/omnigent-venv/bin/python \
    --with ruff ruff check sbx_omnigent/ tests/
```

> **Plain `uv run` inside this directory does not work.** This package declares
> no dependencies on purpose (see the note in `pyproject.toml` — it imports the
> `omnigent` already in your env rather than pinning it), so `uv run` builds a
> fresh *empty* venv and every test module dies on `ModuleNotFoundError: No
> module named 'click'`. The `--no-project --python <omnigent-venv>` pair is what
> makes it resolve. For the same reason there is no `uv.lock` here: this is a
> `uv pip install -e .` package, not a uv-managed project.

## Layout

```
sbx-omnigent-launcher/
├── pyproject.toml            # package + console scripts (omni-sbx,
│                             #   omni-sbx-pipeline, omni-sbx-agy,
│                             #   omni-sbx-swarm, omni-sbx-worktrees)
├── config.sample.yaml        # the `sandbox:` block to merge into your config
├── README.md                 # this file — provider + pipelines + setup
├── docs/
│   ├── PIPELINES.md          # declarative-pipeline reference (the DAG model)
│   ├── ANTIGRAVITY.md        # agy design + the full debugging record
│   └── COLLABORATIVE_SWARM_DESIGN.md   # coordinator-swarm design + rationale
├── agents/                   # coordinator-swarm agents (auto-registered on server start)
│   ├── README.md             # coordinator-swarm setup + usage
│   ├── swarm-coordinator/    #   trusted-plane orchestrator + review-loop skill
│   ├── swarm-coder/          #   implementer (rw worktree)
│   ├── swarm-planner/        #   read-only planner (:ro)
│   ├── swarm-reviewer-bug/   #   read-only correctness/bug reviewer (:ro)
│   ├── swarm-reviewer-security/   # read-only security reviewer (:ro)
│   ├── swarm-agy-coder/      #   agy (antigravity-native) implementer
│   └── swarm-agy-reviewer-bug/    # agy correctness reviewer
├── examples/                 # copy-pasteable scenario recipes
│   ├── quickstart/           #   minimal all-Claude pipeline (start here)
│   ├── mixed-models/         #   pipeline: heterogeneous models + interactive plan
│   ├── tdd-race/             #   pipeline: TDD writer → competing coders → judge
│   ├── full-cadre/           #   pipeline: every role — plan+TDD+2 coders+sec+bugs+judge
│   ├── per-module/           #   pipeline: full cadre once per module (campaign)
│   ├── basic/                #   coordinator: coder + one reviewer
│   └── team/                 #   coordinator: planner + coder + two reviewers
├── scripts/                  # stage0 probe helper
├── tools/                    # keychain-token.c + build script:
│                             #   signed keychain reader so the publish
│                             #   token is read live, with no GUI prompt
├── tests/                    # unittest suite (launcher, worktrees, swarm, pipeline, …)
└── sbx_omnigent/
    ├── __init__.py
    ├── launcher.py           # SbxLauncher: sbx transport + worktree mounting
    ├── entrypoint.py         # provider wrap + agent auto-register + `omni-sbx`
    ├── pipeline.py           # pipeline.yaml parser, templates, agent materialization
    ├── runner.py             # PipelineRunner: the DAG executor + `omni-sbx-pipeline`
    ├── worktrees.py          # host-side git: canonical mirror, worktrees, publish
    ├── swarm_session.py      # create/drive managed sessions over HTTP+SSE
    ├── swarm.py              # SwarmOrchestrator + `omni-sbx-swarm` CLI (coordinator)
    ├── agy.py                # `omni-sbx-agy`: trusted-box bootstrap + token harvester
    └── templates/            # shipped role prompts (planner, coder, tdd-writer, …)
```
