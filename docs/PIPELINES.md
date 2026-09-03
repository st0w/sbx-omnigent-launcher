# Declarative pipelines

A **pipeline** lets you describe a whole swarm — who's on it, what each agent
runs, and how their work flows — in a single `pipeline.yaml`, then fire it with
one command. No coordinator chat, no agent ids to copy, no long CLI kickoff.
It's the Polly-style "define it in YAML, run it" experience for sbx swarms.

```
omni-sbx server -c config.yaml --pipeline pipeline.yaml   # registers the agents
python -m sbx_omnigent.runner -c pipeline.yaml \          # fires the run
  --canonical-root /srv/swarm/canonical \
  --worktree-root  /srv/swarm/worktrees
```

> The runner is also exposed as the `omni-sbx-pipeline` console script
> (`omni-sbx-pipeline -c pipeline.yaml …`). If it isn't on your PATH yet — an
> older editable install can predate the entry point — either reinstall
> (`uv pip install -e .`) or use the `python -m sbx_omnigent.runner` form above,
> which always works. Both take the same flags.

Worked examples: [`../examples/quickstart/`](../examples/quickstart/) (the
minimal all-Claude pipeline — start here),
[`../examples/mixed-models/`](../examples/mixed-models/) (a heterogeneous review
swarm), and [`../examples/tdd-race/`](../examples/tdd-race/) (a TDD writer feeding
two competing coders, judged and merged).

## Why two commands

Registering an agent that can run in a managed microVM **and** carry a pinned
model requires the agent to exist when the server starts — Omnigent has no
"register an agent on a running server" API for managed agents. So the flow
splits cleanly:

- **`omni-sbx server --pipeline pipeline.yaml`** — at startup, materializes each
  declared agent into a namespaced bundle (under `~/.sbx-swarm/pipeline-agents/`)
  and registers it. Namespacing (`pl-<pipeline>-<agent>`) means a pipeline's
  inline `coder` can never clobber a shipped `agents/` bundle of the same name.
  The server stays up.
- **the runner** (`python -m sbx_omnigent.runner` / `omni-sbx-pipeline`) — binds
  the registered agents, provisions the worktrees + microVMs, drives the DAG to
  completion, and publishes. Fails loud if the agents aren't registered yet (with
  the exact `--pipeline` command to run).

Same file to both. If you edit `pipeline.yaml`'s agents (prompts, harness,
skills), restart the server so it re-registers; model/effort/task changes and
DAG/stage edits are picked up by the runner without a restart.

## Anatomy of a pipeline.yaml

```yaml
version: 1
name: my-pipeline          # optional; defaults to the file stem. Namespace root.
repo: /path/to/project     # or a GitHub URL — worktrees are cut from here
base_branch: main          # optional; default branch to cut from / publish onto
publish: pr                # pr | local | none  (or a mapping; see below)
                           #   { mode: pr, branch: pick, stack: true }
plan_artifact: docs/plans/my-pipeline.md   # optional; where the approved plan is
                                           # committed (default docs/plans/<name>.md)

task: |                    # optional — see "Task-optional" below
  What to build.
acceptance: |              # optional — the done contract, relayed to every agent
  How it's judged complete.
context: |                 # optional — project-wide guidance baked into every
  Facts every agent shares.  #   agent's system prompt (see "Shared agent context")
setup: |                   # optional — how an agent prepares its VM before
  curl -sSf https://sh.rustup.rs | sh -s -- -y   #   working (see "Verification")
generated: ["*.lock"]      # optional — files that don't count as implementation
guarded: ["deny.toml"]     # optional — files that ARE a check (see Guarded checks)
disk:                      # optional — per-unit estimates for the startup disk
  per_worktree_gb: 0.2     #   preflight; the defaults suit a COMPILED project
verify:                    # optional — the mechanical gate run before publish
  coverage_min: 95         #   substituted for {coverage_min} in the command
  setup: |                 #   SHELL (not prose) — prepares the gate's own VM
    curl -sSf https://sh.rustup.rs | sh -s -- -y
    . "$HOME/.cargo/env"
  command: |               #   exit 0 publishes; anything else does not
    cargo test --workspace
    cargo llvm-cov --workspace --fail-under-lines {coverage_min}
  demo: ./scripts/demo.sh  #   optional — its output becomes the PR's proof
# task/acceptance/context each accept a *_file variant (task_file:, acceptance_file:,
# context_file:) that reads the text from a path relative to this yaml — pick ONE
# of the pair. See "Text from a file" below.

agents:                    # WHO is on the pipeline
  <name>:
    template: coder        # a shipped role prompt (see Templates), OR:
    prompt: |              #   an inline prompt, OR
      ...
    prompt_file: ./p.md    #   a prompt read from a file (relative to this yaml)
    harness: claude-native # default: claude-native
    model: claude-sonnet-5 # optional; pinned at session create
    effort: medium         # optional; Claude harnesses only (agy has no knob)
    skills: ./skills/tdd   # optional; a dir copied into the agent's bundle

stages:                    # the DAG — what runs, in what order, with what edges
  - { id: plan,  run: plan }
  - { id: build, run: build, write: true, needs: [plan] }
  - id: review
    run: [sec, bugs]
    needs: [build]
    gate: consensus
    on_block: build
```

### `agents:`

Each entry names one participant. The **prompt** comes from a shipped
`template`, an inline `prompt`, or a `prompt_file` (exactly one). `skills:`
points at a directory that's copied into the agent's bundle verbatim,
Polly-style, to augment the base prompt with repo- or role-specific guidance.

The **harness** defaults to `claude-native`; set `antigravity-native` for an agy
agent or `codex-native` for Codex. Model and effort are **not** baked into the
bundle — native harnesses ignore a spec-declared model. The runner applies them
per session at create time (`model_override` / `reasoning_effort`), which reaches
every harness: `--model` at launch for the native CLIs, spawn env for the SDK.
Effort is honored by the Claude harnesses; **agy has no effort knob**, so it's
omitted there rather than declared and silently ignored.

### `stages:` — the DAG

Each stage is one node (or a `parallel:` fan-out of nodes). A node's **kind** is
inferred from its keys:

| Kind | Trigger | Mount | Does |
| --- | --- | --- | --- |
| **reader** | (default) | `:ro` | Produces text (e.g. a planner's design) consumed downstream as context. |
| **writer** | `write: true` | isolated `rw` worktree + branch | Implements; its committed branch is the artifact. |
| **review** | `gate:` / `on_block:` | `:ro` on the target writer's branch | One or more reviewers vote via a `VERDICT:` line; a block loops findings back to `on_block`. |
| **judge** | `selects:` | `:ro` compare tree | Reads competing writer branches side by side and picks a winner via a `SELECT:` line. |

Stage keys:

- **`id`** — unique; also the branch/dir label.
- **`run`** — one agent name (solo node) or a list (a review group sharing a gate).
- **`write: true`** — mark a writer (isolated `rw` worktree on its own branch).
- **`needs: [id, ...]`** — DAG edges. For a writer, the first upstream *writer*
  branch in `needs` seeds this node's worktree (inheritance). For a review, the
  first writer in `needs` is the tree under review. For a judge, the writer nodes
  in `needs` are the candidates.
- **`from: <id>`** — override the seed branch explicitly (which upstream branch a
  writer's worktree is cut from).
- **`gate: consensus`** + **`on_block: <writer-id>`** — a review stage: all
  reviewers must `VERDICT: APPROVED`; any block relays the findings to the writer
  and re-runs, up to a round cap.
- **`selects: branch`** — a judge: picks a winning candidate branch. The judge
  publishes its winner as its **own** branch (`pl/<run>/<judge-id>`), so a later
  stage can build on the outcome — a writer can seed `from: <judge-id>` (e.g. a
  `refactoring` pass on the winner) and a review can gate it before publish.
- **`parallel: [ <stage>, ... ]`** — concurrent isolated sub-nodes (competing
  writers).

**Omit `stages:` entirely** and the runner synthesizes the classic swarm:
`build` (the first coder/tdd-writer agent, or one named `build`/`coder`) →
`review` (every other agent, consensus, loop-back). So a minimal pipeline is just
`agents:` + `repo:`.

### `publish:`

- `local` — push the winning/last branch to `repo` under `pipeline/<run>`; you
  merge it when ready.
- `pr` — push + open a **draft** GitHub PR (needs `gh` authed; `repo` a GitHub URL).
- `none` — leave the branch, publish nothing.

Or a mapping to publish a specific node's branch: `publish: { mode: pr, branch: pick }`.

**In a campaign, each module's request is stacked on the one below it.** Every
module used to target the repo's base branch, which is correct only once the
previous module has merged. While earlier requests are open, a later one shows
their code as its own — measured on a live five-module build, the AWS module's
request came out at **55 files and +12,524 lines** when its own work was 28
files. Nobody can review that.

So module *N*'s request is opened against `pipeline/<run>-<module N-1>`, and it
shows only what that module added. GitHub re-targets it to the real base once
the branch below it merges, so the stack unwinds itself bottom-up.

It is also safe if you merge promptly and **delete** each branch, which is the
common case: a base that is gone would make `gh pr create` fail outright, so the
runner checks the remote first and falls back to the repo's base branch, saying
so. A lookup it cannot perform at all counts as absent — falling back always
yields a valid request, whereas assuming the base is still there loses the
publish.

Turn it off with `publish: { mode: pr, stack: false }` to have every module
target the repo's base branch as before.

`pr` mode needs a **GitHub** target. That is the pipeline's `repo:` by default, so
a local-path `repo:` plus `mode: pr` cannot open a PR — the runner refuses this at
**startup**, before provisioning anything, rather than failing after the build.
Keep `repo:` local for a fast clone and publish to GitHub with `--publish-repo`.

### Publishing identity

By default the `git push` and `gh pr create` run on whatever git/`gh` credentials
the host already has — i.e. the human who started the run. To publish as a
**dedicated pipeline account** instead, hand the runner that account's token
without putting it on the command line:

```bash
python -m sbx_omnigent.runner -c my-pipeline.yaml \
  --canonical-root /srv/swarm/canonical --worktree-root /srv/swarm/worktrees \
  --publish-repo https://github.com/org/proj \
  --publish-token-command 'secret-tool lookup service pipeline-pat'
```

- **`--publish-token-command`** — run as **argv** (never through a shell); its
  stdout is the token.
- **`--publish-token-file`** — read the token from a mode-`0600` file instead.

The secret never reaches your shell history or `ps`; it is injected into **only**
the `git push` and `gh pr create` children — not the runner's own environment, so
no other subprocess inherits it — and it is scrubbed from any failure message,
since git and gh can echo a credential back in their errors. A bad path or a
failing retrieval fails in seconds, before provisioning.

Both are **CLI flags only, never `pipeline.yaml` keys**: a token command inside a
shareable pipeline file would turn merely running that file into arbitrary code
execution on the host.

Any credential store works, so this stays portable:

| Store | `--publish-token-command` |
| --- | --- |
| libsecret (GNOME Keyring) | `secret-tool lookup service pipeline-pat` |
| `pass` | `pass show ci/pipeline-pat` |
| macOS Keychain | `security find-generic-password -w -s pipeline-pat` |
| HashiCorp Vault | `vault kv get -field=token secret/ci/pipeline` |
| AWS Secrets Manager | `aws secretsmanager get-secret-value --secret-id ci/pipeline --query SecretString --output text` |

The token stays in the trusted host plane. It is never mounted or injected into a
microVM — agents never run git.

## Shared agent context

A top-level **`context:`** is project-wide guidance — facts every agent should
share regardless of role (the stack, conventions, where the module under work
lives, what must not change). It is **baked into every agent's system prompt**
once, at materialize time, appended after each role prompt as a delimited
section:

```yaml
context: |
  This is a Django 5 service; the module under work is net.py.
  Use pytest, follow PEP8, never touch files under migrations/.
```

Because it lives in the stable system-prompt prefix rather than being re-sent in
each turn's user message, it is **smaller for multi-turn agents** (the
interactive planner, loop-back fixes) — one copy in the window instead of one per
turn — and cache-friendly, while being identical for single-turn nodes. It
composes with per-agent `skills:`/`prompt` (role- or agent-specific additions
layered on top).

Since it's baked into the bundle, **editing `context:` needs a server restart**
to re-register the agents (like any prompt/skills change) — negligible for stable
project info. For context that varies per run, put it in `task:`/`acceptance:`
instead (those are relayed per-turn, no restart).

### Text from a file

Any of the three top-level text fields can be read from a file instead of pasted
inline, via a **`<key>_file:`** sibling — `task_file:`, `acceptance_file:`, and
`context_file:` — mirroring an agent's `prompt`/`prompt_file`. The path is
relative to the pipeline file; give **only one** of each inline/file pair. Handy
for a long, reusable brief kept in its own Markdown file:

```yaml
task_file: docs/tasks/parse-ports.md
acceptance_file: docs/tasks/parse-ports-acceptance.md
context_file: docs/project-context.md   # shared brief, one place to edit
```

## Interactive planning

If a pipeline has a **`planner`**-templated agent, the plan stage is
**interactive by default** — a human shapes the plan before any code is written:

1. The planner posts its design and clarifying questions in the Omnigent UI
   session titled `<name>/plan`.
2. You answer there, iterating with the planner. **Nothing downstream runs** —
   the runner blocks (up to 1 hour) until you reply **`APPROVED`** in that
   session.
3. On approval the planner is driven one more turn to emit a **clean,
   consolidated final plan**, which becomes the *plan of record* shared with
   every builder and committed to the repo (below).

So drive a planner pipeline **live** — don't fire it and walk away. To run
unattended instead, pass **`--no-interactive-plan`**: the planner's single-turn
output is used as-is with no human gate (useful for CI / automation).

**Pre-warm.** While you're answering the planner, the runner boots the downstream
**writer** VMs in the background (they take time to start, especially agy), so
the swarm is warm the instant you approve instead of cold-starting node by node.
`from:`-seeded writers are reseeded onto the upstream's committed tip before they
run, so they still land exactly one commit above it.

### Plan of record

On approval the planner is driven one more turn to re-emit its design as a
standalone document. A conversational planner (agy, typically) sometimes answers
that turn with an acknowledgement — *"the plan is approved and frozen"* — instead
of the plan; the runner refuses to accept such a reply as the plan of record and
recovers the last substantive design from the session, reporting that it did.
Otherwise a few hundred characters of chat would be handed to every builder AS
the design.

That consolidation turn also tells the planner to carry every artifact it
produced forward **verbatim** — diagrams, DDL, table definitions, worked examples
— because consolidating means folding in the decisions, not summarizing away the
detail.

When a planner runs, its approved (consolidated) plan is committed to the branch
being published as **`docs/plans/<name>.md`** — a `docs: add plan of record`
commit made just before publish — so the approved design travels with the code in
the PR. Override the path with the top-level **`plan_artifact:`** key. Skipped
when `publish: none` (nothing is published).

### The judge's selection, and the implementation that lost

A judge picks ONE candidate and only that branch publishes. Two artifacts keep
the rest of the story, because otherwise the choice and the losing work both
vanish with the run:

- **`docs/plans/<plan>-selection.md`**, committed beside the reviewer reports:
  which candidates were compared, which won, what the judge said, and the
  judge's reasoning in full. The pull request carries a `## Selection` summary
  of the same. The value is the SERIES — one entry says nothing, ten tell you
  whether a second writer earns its microVMs.

  The record distinguishes what the judge **stated** from what actually shipped.
  When a reply carries no usable `SELECT:` line the runner keeps the FIRST
  candidate and says so loudly; that is a fallback, not a preference, and the
  record calls it an absent decision so a series cannot be read as favouring
  whichever node happens to be listed first.

- **The losing branch itself**, as a git bundle under
  `<canonical_root>/_retained/<run>/<node>.bundle`. It is complete, reviewed and
  test-passing — an independent implementation of the same frozen contract — and
  the run hub holds the only copy until teardown deletes it. Deliberately kept
  OUTSIDE `worktree_root`, since everything the launcher removes is constrained
  to that root, so no teardown can sweep it.

  A delta against the base branch, so it is small (hundreds of bytes, not the
  repo). Restore one with:

  ```bash
  git fetch <bundle> refs/heads/pl/<run>/<node>:refs/heads/loser
  git diff main loser
  ```

  Nothing ever deletes these — prune by hand when a run's comparison data stops
  being interesting.

### The planning session record

The plan of record is a **summary**, and every rewrite drops what the last one
made redundant. Observed live: a module's design draft carried a mermaid diagram
and nine SQL blocks; the revision that superseded it carried four and no diagram
— and the revision is what would have reached the repo.

So the planner's **whole conversation** is committed beside it, as
**`docs/plans/<name>-session.md`** (per module:
`docs/plans/<name>-<module>-session.md`): every draft, the questions it asked,
your answers, and the artifacts that did not survive consolidation. The plan
stays the canonical design; the session is the reasoning behind it, including the
alternatives that were rejected and why.

It records **messages only — never tool calls or their output.** A tool result
can carry file contents, environment, or command output from inside the VM, and
this file goes into your repository; what the human and the planner said to each
other is the design record, the rest is machinery. Best-effort throughout: an
unreadable session or a git error skips the file rather than failing a publish.

## Campaigns — build in sequence, one chunk at a time

One run, one impl turn (~30-min budget) can't converge on a whole component. A
**campaign** splits the work into ordered, turn-sized chunks and runs the build
cycle **once per chunk**, each building on the last. Chunks thread through one
accumulating hub branch and each publishes **separately** to
`pipeline/<run>-<chunk>` (or its own draft PR). A block in one chunk stops the
campaign there — the chunks that already shipped stay shipped. There are two ways
to get the chunk list:

- **Flat (planner-proposed).** Give the pipeline a single `task:` and a `planner`.
  On approval, the planner ends its plan of record with a machine-readable
  `SUBTASKS:` block (`- [<id>] <goal>` per line); the runner parses it and loops
  the **build** stages per chunk, under one shared plan of record. Engages at ≥2
  proposed subtasks; 0–1 is the ordinary single pass. Use this when the
  decomposition is the planner's call.

- **Per-module (you supply the list).** Set **`subtasks:`** (inline
  `[{id, title}, …]`) or **`subtask_file:`** (a file of `- [<id>] <title>` lines)
  in the config. Now the runner loops the **whole** pipeline — including its
  planner — once per module: each module's planner runs **in-loop**, interactive
  (it blocks on your `APPROVED` **per module**), designing that module against the
  **frozen** prior modules, and its design feeds that module's single TDD writer
  (**one build cycle per module** — the module's work breakdown becomes its
  tests). Each module commits its own plan to `docs/plans/<name>-<module>.md`. Use
  this when you already know the modules and each deserves its own design pass.
  See [`examples/per-module/`](../examples/per-module/).

**Threading.** Chunk 0 seeds from the base branch; each later chunk's entry
node(s) seed `from:` the campaign tip (the prior chunk's winner), so module N is
built against modules 0…N-1. A chunk's winner (the judge's selection, else its
last writer) is aliased onto the thread and published. Node ids are namespaced
per chunk (`pl/<run>/<chunk>-tests`, …) so branches never collide.

### What a module's planner knows about the others

Each module is planned in its own session, so nothing crosses between them by
accident. Three things are threaded deliberately:

- **The whole module table.** Every per-module planner is shown the full
  ordered list with its own row marked, and told the others are built in their
  own runs. Without it a planner sees only its own one-line scope and
  reasonably designs its neighbours' work into its module — which then gets
  frozen into that module's test suite.
- **The earlier designs.** Each module's approved plan is committed to
  `docs/plans/<name>-<module>.md` on its branch, so the campaign thread carries
  them into every later module's worktree; later planners are pointed at the
  directory and told the designs are binding.
- **A decisions ledger.** A module's consolidation turn is asked to end with a
  block headed `DECISIONS FOR LATER MODULES:` listing anything it settled that
  **constrains** later work — a contract another module consumes, a convention,
  a rejected alternative, an agreement reached with you. The runner parses it,
  inlines the accumulated ledger into every later planner's turn as binding,
  and commits it to `docs/plans/<name>-decisions.md`, which threads forward. So
  a decision you make once, in module 0's session, is not re-litigated in
  module 3 — and you get one reviewable document rather than reconstructing the
  reasoning from N separate plans.

Decisions you already know before the run starts belong in the shared brief
(`task_file`) instead — the ledger is for what emerges *during* a module.

**A chunk's VMs *and* worktrees are reclaimed when it publishes.** Both are
dead weight by then: its work is committed to a hub branch and pushed, the next
chunk seeds from the hub rather than from any clone, and a resume skips a
completed chunk wholesale. Holding the VMs would cost one per node per chunk
for the whole run (a 6-module full cadre peaks at ~72 live microVMs); holding
the clones makes DISK cumulative — 2.2-26 GB per writer node for a compiled
language, which ran a host out of space two modules into a six-module run. That
failure does not surface as a disk error, but as guest filesystems remounting
read-only.

So a campaign's footprint is **one chunk's worth at a time**, and the
[disk preflight](../README.md#disk-requirements) sizes against that. Branches
are never touched — the hub is the durable artifact, and every reclaimed clone
is reproducible from it. `--keep` opts out and holds everything, and reclaiming
happens only AFTER the chunk's completion is persisted, so a crash in between
leaves a resume that skips the chunk anyway.

Campaigns are long, expensive jobs (the full cadre × N chunks, pausing for
approval per module) — drive them **live**.

## Resuming a run

A pipeline run is long and mostly unattended, so losing one to a single
late failure is expensive — especially when a human already sat through a
planning gate. Two things make that recoverable.

**Work is never discarded because a turn failed.** A writer's tree is committed
to its node branch even when the turn errors, tagged `partial work (turn did
not complete)`. Without this a timeout throws away everything the agent wrote:
the commit step comes *after* the turn, so a stage that produced a complete
test suite and then overran its budget would leave an empty branch and a full
worktree.

**A run that does not finish keeps its directory.** Teardown always disposes the
microVMs — they are expensive and useless once the process ends — but the run
directory holds the hub clone with every node branch, plus `state.json`, and
that is the only copy of what a resume reads. It is removed only after a clean
finish; a failure or a blocked gate preserves it and prints the path. (Before
this, only `--keep` prevented the deletion, which made resume-after-failure work
by accident: a run that died standing up one session took a finished module's
plan, tests, and both implementations with it.)

**Bookkeeping is checkpointed as the run advances**, to `<run>/state.json` —
which stages finished, the approved plan of record, judge selections, campaign
position, published chunks. It lives in the run directory, never the repo, so
no node commits it and no agent sees it.

Then resume with the same run id:

```bash
python -m sbx_omnigent.runner -c my-pipeline.yaml --run-id discover --resume \
  --canonical-root ~/sbx-canonical --worktree-root ~/sbx-worktrees
```

The runner reuses the existing hub, skips every stage already recorded
complete, re-cuts only what is left, and skips campaign chunks already
published. A stage that failed is re-cut **from its own branch**, so the agent
picks up its partial work instead of starting over.

**A resume cleans up after the attempt it continues.** `--keep` plus `--resume`
leaks by construction: the earlier process deliberately kept its microVMs, and
the resuming process starts with an empty session list, so nothing it owns would
ever dispose them — not at a chunk boundary, not at teardown. They would hold
disk for the whole continued run, on top of the VMs it stands up itself
(observed live: 12 microVMs from a finished module still up eight hours later,
on a host at 100% capacity). So the state file records session ids **for
disposal only** — never reattached to, since a session belongs to a VM a later
process cannot drive — and a resume tears them down before it starts. Pass
`--keep` again to keep them, and it says so rather than silently leaking.

A stage counts as complete only when its whole stage finished — never merely
"has a branch", since a pre-warmed writer has a branch before it has ever run.
Incomplete nodes are deliberately not restored, so nothing downstream can
inherit from a stage that never finished. Resume refuses outright when there is
no state file (a run predating this, or whose directory was removed) or when
the state was written by a different `RUN_STATE_VERSION` — better to start
clean than to resume from something misread.

## Turn timeout

Each agent turn gets `turn_timeout` seconds (default 1800). Raise it for a
pipeline whose stages stop and ask you questions — **that wait is spent inside
the turn's budget**, so a stage that asks one clarifying question can exhaust
30 minutes on human response time and be killed while still working:

```yaml
turn_timeout: 3600      # seconds; --turn-timeout overrides it
```

## Verification — the one check the orchestrator makes itself

Every gate downstream of a writer is an **agent's word**: a reviewer's
`VERDICT:` line, a coder's "the tests pass". Nothing in the DAG executes the
suite, so a writer that does nothing and reports success can sail through.
Observed live: an implementer left both crate roots holding the test author's
*"intentionally empty"* placeholders, changed only `Cargo.lock`, reported
success — and collected `VERDICT: APPROVED` from **two independent reviewers**.
Only the judge caught it, by diffing.

Three things close that gap.

**The no-op guard (automatic).** After a writer commits, the runner compares its
branch to the tree it was cut from and requires at least one changed file that
is not build output. A no-op is re-driven with the specific evidence ("the only
files you changed were … `Cargo.lock`"), up to the review round cap, then fails
the stage rather than handing an empty branch to reviewers. Tune what counts as
build output with **`generated:`** (glob patterns, matched against the full path
and the basename; default covers `*.lock`, `*.sum`, `package-lock.json`,
`npm-shrinkwrap.json`, `pnpm-lock.yaml`).

**Guarded checks (opt-in).** A blocking finding closed by editing the thing that
produced it leaves every gate green and no trace. Live, a writer closed a
blocking supply-chain finding by appending three advisory ids to the auditor's
ignore list and deleting the comment block documenting the one pre-existing
acceptance; both reviewers then approved the branch without mentioning either,
and it shipped. Declare the files that *are* a check with **`guarded:`** and the
runner diffs the branch under review and names every one it touched, as a
required review item:

```yaml
guarded:                       # optional — files that ARE a check
  - .cargo/audit.toml          # suppression lists
  - deny.toml
  - .github/workflows/*        # CI definitions
  - scripts/verify.sh          # the gate itself
```

This never *forbids* the edit — a module whose job is adding CI must edit CI. It
only makes passing over one in silence impossible: the reviewer is told to diff
each file and state explicitly whether it is a genuine fix or a way to stop a
check reporting, and that approving one without mentioning it is itself a review
failure. There is **no default set**, deliberately: which files constitute a
check is a property of the project, and shipping a list naming `deny.toml` would
teach the launcher one ecosystem's tooling. Writers are told the matching rule
directly too — no test, gate config, linter config, CI workflow, or
ignore/allow-list entry may be weakened to close a finding, and any legitimate
change to one must be disclosed in the reply.

**Guard the pipeline's own contract, not only the repo's checks.** Everything
above is a gate's *configuration*; `pipeline.yaml` is the gate's *terms*. When
it lives inside the repo it builds — a common layout, since a pipeline wants
versioning alongside the code it drives — a module that finds a stage contract
inconvenient can edit the acceptance criterion instead of meeting it, and
nothing downstream will mention it. That is the supply-chain move one level up:
same silence, larger blast radius, because the contract binds every later
module too. List the directory:

```yaml
guarded:
  - omnigent/*                 # the pipeline yaml and its prompt files
```

A pipeline defined outside the repo it builds loses nothing by listing it: the
runner only diffs the branch under review, so a path that never appears there
costs nothing.

This is deliberately **pure git inspection in the trusted plane**. The obvious
alternative — having the runner run `cargo test` in the node's worktree — would
execute agent-authored code on the host with the publish token in the
environment. That is a sandbox escape, so the guard checks only what it can
observe without running anything.

**Reviewers must verify (prompt-enforced).** The shipped `security-reviewer` and
`bug-reviewer` templates, and the runner's own review turn, now forbid returning
`VERDICT: APPROVED` on a change the reviewer did not execute. Missing tooling is
a reason to install it or to block — never to approve on inspection — and a stub
or leftover placeholder is a blocking finding in its own right.

**A silent reviewer gets asked, not assumed.** Making reviewers do real work has
a cost: installing Postgres and running an instrumented coverage build outlives
the turn, so the reviewer's last message is mid-narration ("Running code coverage
check with `cargo-llvm-cov`. I will wait for it to complete.") and carries no
`VERDICT:`. A missing verdict counts as BLOCKING — the safe default — but paying
that on silence alone is expensive and usually wrong: it re-drives a coder, runs
the whole review again, and hands the coder narration in place of findings. So
when the settle-poll window expires the runner spends **one turn asking for the
verdict**, and only a reviewer that still will not answer is treated as blocking.
When it does answer, the loop-back carries *both* the earlier narration (where
the findings actually are) and the verdict reply.

The nudge is **pasted inline**, not staged as a task file. An agy turn is
normally written to `OMNI_TASK.md` with only a pointer pasted, because agy drops
multi-line pastes — right for a task, wrong for this. A reviewer that believes it
is mid-work does not stop to re-read a file: three consecutive rounds of an agy
reviewer recorded no verdict at all, and not one of their transcripts contained
the nudge text. It is still flattened to a single line, because the paste
constraint is real; only the file indirection is skipped.

**A reviewer still producing output is not silent.** Reviewers must EXECUTE
what they review, so a review legitimately runs for tens of minutes. Timing that
against a wall clock punished the reviewers doing the most work: one agy reviewer
recorded **no verdict in three consecutive rounds** while its reports read
*"Running `cargo test -p discover-k8s -j 2` in the background … I will wait for
it to complete."* Each expiry cost a full extra round plus a re-driven coder.

The verdict wait is therefore a **silence** deadline, not a total budget: it
restarts whenever the reviewer emits anything. That is measured as a *delta* —
the id of the newest item, compared across polls — because neither status nor
item type can tell a reviewer mid-build from one that stopped. An absolute
ceiling still bounds a reviewer that narrates forever without ever voting.

**A reviewer that never voted did not vote AGAINST the branch.** It failed to
review, and that is a different failure with a different remedy. Conflating them
re-drove the coder over a branch nobody had objected to and handed it the
reviewer's own narration as findings — a coder replied, correctly, *"there is no
actionable finding to address — it names no defect, file, line, or assertion."*

So silence now re-runs the **review**, not the writer. If it still does not
conclude after the retries, the run **blocks**: silence remains BLOCKING, the
safe default, but it never re-drives a writer, because there is nothing to hand
one. Only an explicit `VERDICT: BLOCKING` produces findings, and a silent
reviewer standing beside a real blocker contributes nothing to them.

**A reviewer that has voted is freed immediately — the moment IT votes, not
when the round decides.** Review rounds create fresh sessions each time, so a
blocked two-reviewer gate holding its VMs to the end of the chunk peaks at
`reviewers x rounds` microVMs on one branch.

Freeing them per ROUND was still not enough. Reviewers run one at a time, and
are required to *execute* what they review — so a reviewer that has already
voted sits on a full guest the reviewer still building needs. Observed live: a
reviewer voted `APPROVED` and its idle 6 GB guest stayed up for **1h50m** while
the remaining reviewer's build thrashed at load 18 with every linker at 0% CPU,
on a host holding three guests in 17 GB. Each reviewer's VM now goes back as
soon as its own verdict is recorded; the end-of-round disposal stays as the
backstop for a delete that failed. `--keep` still keeps everything.

**Every agent except the planner is told it runs unattended.** A coder hit a
genuine requirements conflict, resolved it correctly, verified the suite green —
and then opened a modal asking permission for the change it had *already made*.
Nothing surfaced that question to the console, and the reviewer could not see it
either: it blocked five consecutive rounds on the change being "un-escalated"
while the escalation sat in a modal neither of them could reach.

The instruction is therefore not merely *don't ask* but **your reply is the only
channel that reaches anyone** — a human, a reviewer, or the record. Resolve
conflicts using the frozen tests and the invariants as tie-breakers, state the
decision and the reasoning in the reply, or label it `DISPUTED` and stop.

The **planner is deliberately excluded**: its interactive approval gate is the
design, and it is the one stage with a human genuinely attending.

Belt and braces, a turn that opens a prompt anyway is now **detected and failed
fast** rather than waiting out the turn budget — the runner reads the session's
pending elicitations, which it checks *before* status, because a session reads
`running` for as long as its modal is open. The error carries the question, so
it appears on the console instead of only in the web UI.

**A writer may not close a finding by deleting the check.** A blocking finding
reaches a writer as a mandate, and that is dangerous when the finding is wrong.
Observed live: a reviewer told a coder to drop `GetGroup` from its permissions
manifest, but the module's frozen test asserts that set *exactly* — "no more, no
less" — so obeying meant deleting the assertion, which the coder duly did. The
next reviewer caught it and named the correct disposition: *escalation as a
test/contract dispute, not editing the manifest and the frozen test.* Every
loop-back now says the suite is frozen, that a finding satisfiable only by
changing a frozen test is either wrong or a contract dispute, and that reporting
it as **disputed** is a correct outcome. Silently removing an assertion is worse
than leaving a finding open — the check that would have caught the next
regression goes with it.

**A writer's branch is reconciled with its worktree before anyone reads it.** A
native-terminal writer keeps working after its turn reports done *and* after the
settle-and-commit fires: one committed a 31-line manifest at 23:58 and wrote the
1080-line implementation at 00:26, leaving its branch a stub. No grace at commit
time covers a 28-minute gap, so the reconcile happens where the divergence
starts to matter — immediately before a review stage creates its reviewers.
That placement is the point: reviewers mount the **worktree** `:ro` while the
judge clones the **branch**, so a divergence means they disagree about what a
candidate even is, and one spends half an hour on code the other never sees.
Late work is settled and committed as `<node>: late write`; a clean worktree
costs nothing, and a reconcile that fails never fails the review.

**A finished VM is freed, not held to publish.** A reader, a judge, and any
writer nothing can loop back to are done the moment their stage completes —
every turn on one happens inside its own stage, and only two things re-drive a
writer: a review stage naming it in `on_block`, and the verification gate
looping back to the writer that produced the published branch. Holding the rest
until the chunk published cost a full guest each through the refactor, the final
review and the gate: the heaviest stretch of a module and exactly when the
memory is wanted. Observed live on a 17 GB host, a finished judge held 6 GB
while a refactor's linker was being OOM-killed.

The rule is deliberately conservative where the DAG cannot answer it: a pipeline
that publishes "the last writer", or a judge's pick, could loop back to a writer
that cannot be named statically, so in those shapes no writer is released.
`--keep` still keeps everything, and the disk preflight uses the same rule, so
the estimate and the behavior cannot drift apart.

**Every report is written down first.** Disposing a session *deletes* it, so a
reviewer does not outlive its own vote — and an `APPROVED` verdict used to be
reduced to a single token with the report dropped on the spot, which is the
reasoning behind a decision to ship. Each report is now captured before the VM
goes, and kept in two places:

- **`<run>/reviews/<stage>-<reviewer>-r<n>.md`**, written as each round decides.
  The run directory survives a failed run, so a blocked run keeps the reports
  that explain *why* it blocked. Also carried in `state.json`, so `--resume`
  still publishes the rounds that ran before the crash.
- **`docs/plans/<pipeline>[-<module>]-reviews.md`**, committed onto the branch
  being published and linked from the pull request, which also carries the
  roster — so a reader sees that round 1 blocked without opening anything.

Records use the messages-only transcript reader: no tool calls and no tool
output, because a tool result can carry file contents or environment from inside
the VM and this is committed to a repository.

Nothing is written to a candidate's branch mid-review — a competition is still
live at that point — so the buffer is committed once, to the winner, at publish.

**Writers' turns are captured too, when they are about to be lost.** A session
is DELETED when its microVM is disposed, and teardown disposes everything
moments after a run ends — so a turn that failed or never returned leaves
nothing behind but its own timeout line. Twice that cost an hour with no way to
learn why. The runner now writes `<run>/turns/<node>.md` at the moments the
record is about to vanish:

- a turn that **failed** or **never returned**, captured before the exception
  propagates into teardown;
- a turn, or an interactive **planning session**, interrupted with **Ctrl-C** —
  the plan reaches the run state only when its stage *completes*, so
  interrupting an approval that a human has spent half an hour on used to
  destroy every question, answer and draft along with the session; and
- every **loop-back fix turn**, pass or fail, since that session is disposed at
  publish and what a writer did with a reviewer's findings is exactly what
  someone reading the pull request later wants to see.

Repeated re-drives get `-2`, `-3` suffixes rather than overwriting. These are
diagnostics and stay in the run directory — unlike reviewer reports, they are
not committed to the branch. Every step is best-effort: a diagnostic that can
fail a run is worse than no diagnostic.

**`setup:` tells them how.** A fresh microVM has no project toolchain, and an
agent that cannot build is exactly where verification quietly turns into
guessing. `setup:` (or `setup_file:`) states how to prepare the VM, and is
relayed into every builder and reviewer turn along with the fact that egress for
it is already allowed:

```yaml
setup: |
  This VM has no Rust toolchain preinstalled. Install one before you begin:
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    . "$HOME/.cargo/env"
  Then `cargo test` must run for real before you report any result.
```

**Size the gate's sandbox, because nothing else will.** The gate builds its own
box rather than going through the server, so `sandbox.sbx.cpus` and
`sandbox.sbx.memory` — which cap every *agent* VM — never reach it. Left unset,
sbx gives a guest **every host CPU** while capping its memory at **half the
host**, and most build tools set their job count from the CPU count: that many
compilers and linkers at once, inside a guest with half the machine's memory.

The failure is silent and lands in the wrong place. Observed live: the gate's
linker was killed while the *same branch* built and tested cleanly in a capped
agent VM — and the runner has no way to tell that from a failing suite, so it
blamed the branch and re-drove a writer, which duly started editing the
project's build config to work around a limit that was never the code's fault.
Set `verify.cpus` and `verify.memory` to whatever the agent VMs get.

**`setup:` is PROSE, and `verify.setup:` is SHELL.** They look like they should
be the same field and they are not. `setup:` is addressed to an agent — it can
explain, insist, and say what is not acceptable, and it is relayed into turns as
text. The gate has no agent to read it: it runs one `sh -c` program.

This was learned the hard way. `setup:` used to be pasted into the gate's
script, which produced:

```
sh: 2: This: not found
```

— exit **127** on the first word of a sentence, before the project's command was
ever reached. That looked exactly like a failing test suite, so the runner
re-drove the writer to "fix" it, re-ran the whole review gate, and repeated
three times before failing the run. Nothing was wrong with the code.

Two things now prevent a repeat:

- **A pipeline with a `setup:` block and a `verify:` gate must set
  `verify.setup`** — loading it fails otherwise, before a single VM is
  provisioned. Set `verify.setup: ""` to state explicitly that `verify.command`
  installs what it needs itself (a committed repo script, say).
- **One giant line cannot eat the evidence.** Output handed back to a writer is
  capped, keeping the *tail* — right for a test suite, whose summary is at the
  end, and wrong the moment a single line is enormous. A build tool that fails
  to link prints the whole failing compiler invocation as one line of ten
  thousand characters, so the cap kept that and dropped the diagnostic
  explaining it: measured at 10,382 characters of evidence containing not one
  `file:line` pointer. Over-long lines are now shortened individually, first,
  so the budget goes to text a reader can act on.
- **A prologue that dies is infrastructure, not a verdict.** The gate echoes a
  marker between the prologue and the command; if the marker never appears, the
  command never ran, and that fails the RUN rather than looping a writer over a
  broken setup script it cannot fix.

**`verify:` is the gate that actually runs the tests.** Everything above makes
dishonesty detectable; this makes correctness *provable*. Before a branch
publishes, the runner stands up a **fresh, disposable microVM** on a clean clone
of exactly that branch, runs `verify.setup` followed by `verify.command`, and
believes only the exit status:

```yaml
verify:
  coverage_min: 95
  timeout: 2400            # seconds; overrunning fails the gate
  cpus: 4                  # the gate's OWN sandbox — see below
  memory: '8g'
  setup: |                 # SHELL: the gate's VM has no toolchain either
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    . "$HOME/.cargo/env"
  command: |
    cargo test --workspace --locked
    cargo llvm-cov --workspace --locked --fail-under-lines {coverage_min}
```

`{coverage_min}` is substituted (by literal replace, not `str.format` — a real
shell command is full of braces), so the threshold lives in one place. The tool
enforces its own threshold and the runner reads only the exit code: no number is
ever parsed out of agent output, and no agent is asked to report one.

**Any language.** The command is yours; the launcher never learns what a test or
a coverage report is. Starting points — check the exact flags against your own
toolchain, these are not verified here:

| Ecosystem | `verify.command` |
| --- | --- |
| Rust | `cargo test --workspace --locked`<br>`cargo llvm-cov --workspace --locked --fail-under-lines {coverage_min}` |
| Python | `pytest --cov=<pkg> --cov-fail-under={coverage_min}` |
| Go | `go test -coverprofile=c.out ./...`<br>`go tool cover -func=c.out \| awk '/^total:/ {gsub(/%/,"",$3); if ($3+0 < {coverage_min}) exit 1}'` |
| Node (jest) | `jest --coverage --coverageThreshold='{"global":{"lines":{coverage_min}}}'` |
| Node (vitest) | `vitest run --coverage.thresholds.lines={coverage_min}` |
| .NET | `dotnet test /p:CollectCoverage=true /p:Threshold={coverage_min}` |
| Java | configure the JaCoCo `check` rule in your `pom.xml`, then just `mvn -q verify` |

Two things those illustrate. **Go has no threshold flag** — you compare the number
yourself and `exit 1`, which is fine because the gate only reads the exit status.
And the Go and jest commands are full of braces (`awk '{...}'`, JSON) that a
`str.format` substitution would corrupt or raise on; the literal replace exists
for exactly them.

**`coverage_min` is optional.** Omit it and the gate is purely "the tests really
pass" — still the single most valuable thing here, and it needs no coverage
tooling at all:

```yaml
verify:
  command: make check
```

**Who repairs a failing gate — and why `acceptance:` must not contradict it.**
A gate failure is closed by the writer that PRODUCED the branch, which after a
judge is the refactor node: it is the only writer running once an
implementation exists, so tests added there cannot invalidate a competition
that already happened, and its review stage votes again on the changed branch
before the gate re-runs. Write an `acceptance:` that forbids that repair — *"the
test suite is frozen after the tests stage"* — and any module below
`coverage_min` becomes unpublishable: the gate fails, the runner routes the fix
to the refactor node, and that fix is the thing the reviewers were told to
refuse. Live, this halted a module at review-r with both readings defensible
and neither actionable. If your contract freezes tests, say which half is
which — weakening an existing test (changing, deleting, relaxing, skipping) is
a frozen-artifact change; adding coverage the gate demands is not.

### Polyglot repos: delegate to the repo

For a repo spanning several languages, do NOT inline the detection logic in
`pipeline.yaml`. Call a script the repo owns:

```yaml
verify:
  coverage_min: 95
  command: ./scripts/verify.sh {coverage_min}
```

The polyglot logic then lives with the code that changes, you and CI run the
byte-identical gate locally, and — because the script is in the agents'
worktree — a writer told to close a coverage gap can READ the bar it is held to
instead of guessing. A campaign whose modules use different languages needs no
config change either: the script branches on what is actually present.

```sh
#!/bin/sh
# scripts/verify.sh — the gate. Usage: ./scripts/verify.sh [min-coverage]
set -eu
MIN="${1:-95}"
ran=0

if [ -f Cargo.toml ]; then
    ran=1
    cargo test --workspace --locked
    cargo llvm-cov --workspace --locked --fail-under-lines "$MIN"
fi

if [ -f pyproject.toml ]; then
    ran=1
    pytest --cov --cov-fail-under="$MIN"
fi

if [ -f package.json ]; then
    ran=1
    npm ci
    npx vitest run --coverage.thresholds.lines="$MIN"
fi

# A detection-based gate that detects nothing must FAIL, not pass.
[ "$ran" = 1 ] || { echo 'verify: no toolchain detected' >&2; exit 1; }
```

Two traps that shape it. A script that detects nothing must **fail** — otherwise
a branch publishes having run zero tests, which is the exact failure the gate
exists to prevent, reintroduced one level down. And `[ -f x ] && cmd` aborts the
whole script under `set -e` when `x` is absent (the `&&` list returns non-zero),
so use `if`/`fi`.

*Where* it runs is the design. On the host it would execute agent-authored code
in the trusted plane beside the publish token. In an agent's own VM it would
trust an environment that agent had write access to for an hour — a shimmed
`cargo` on `PATH` defeats the check. So it gets its own sandbox, created from the
committed branch (not the writer's worktree, which also keeps gigabytes of build
output out of it), given **no credentials of any kind**, and destroyed in a
`finally` even on timeout.

**A failure loops back to the writer that produced the branch** — the refactor
node, typically. That is the only writer running after an implementation exists,
so adding tests there cannot invalidate a competition that already happened. It
is told to ADD tests and explicitly forbidden from modifying an existing test or
hollowing out production code to move the number; its review gate then votes
again on the changed branch before the gate re-runs. Bounded by the review round
cap, after which the run stops **without publishing**.

A sandbox that will not start is treated as infrastructure, never as a verdict —
it says nothing about the branch, so it fails the run rather than looping a
writer back over something they cannot fix.

### The pull request as evidence

A passing suite shows the code satisfies its tests. It does not show a reviewer
the thing *running*. **`verify.demo`** is a second command, run in the **same
sandbox** right after the gate passes — same clean checkout, so what it exercises
is exactly what was just tested — and its captured output becomes the pull
request's proof:

```yaml
verify:
  command: ./scripts/verify.sh {coverage_min}
  demo: ./scripts/demo.sh
```

A demonstration is expected to be **hermetic**: it stands up whatever it needs
*inside* the sandbox — a database, a TLS server, fixtures — so it proves real
sockets, a real handshake, and real queries without reaching a live endpoint or
holding a credential, and reproduces identically for whoever reads the PR. A
non-zero exit fails the gate, because publishing "proof it works" beside a
failing demonstration is worse than not publishing.

The PR body is then assembled from what actually ran:

- **What shipped** — the module and its scope.
- **How it was designed** — links to `docs/plans/<name>-<module>.md` and the
  session record beside it.
- **How it was proven** — the gate's command, its exit status, and its captured
  output (folded into a `<details>` block; coverage tables live here, if your
  command emits one).
- **Proof it works** — the demonstration's command and output, shown open,
  because it is the thing the reviewer came to see.

Output is ANSI-stripped, scrubbed of secret-shaped fragments (a connection proof
prints connection strings), tail-capped per step with an explicit truncation
note, and fenced with a fence long enough to survive backticks in the payload.

None of this makes an agent honest — it makes dishonesty **detectable at the
stage that produced it**, instead of three gates later.

## The branch-as-artifact model

Every **writer** works in its own git worktree on its own branch, in its own
microVM — writers never share a filesystem, so two autonomous coders can run
concurrently without racing. Reviewers and judges mount a specific branch `:ro`.
Branches are the artifacts that flow along DAG edges:

- an edge (`needs`/`from`) means the downstream writer's worktree is **cut from**
  the upstream branch — e.g. a coder inherits the TDD writer's tests;
- competing writers each get an isolated branch off the same seed;
- a **judge** checks out every candidate branch side by side and picks one;
- the orchestrator (trusted host plane) does **all** git — commit each writer's
  tree to its branch, cut downstream worktrees, merge, select, publish. Agents
  never run git.

## Task-optional execution

- **With `task:`** — the runner drives every stage to completion (plan → write →
  review/loop → judge) and publishes.
- **Without `task:`** (omit `task:` and `acceptance:`) — the runner provisions
  the whole topology (each node's isolated worktree + microVM session, writers
  `rw`, readers/reviewers/judges `:ro`), drives nothing, and prints the
  role→session bindings for you to drive by hand (UI or `omni-sbx-swarm send`).

The same file thus both spins up a swarm to poke at and runs the full loop to a PR.

## Templates

Shipped role prompts you can name in `agents.<name>.template` (each written for
the isolated-branch model — the agent works on its own branch; the orchestrator
commits/merges):

| Template | Role |
| --- | --- |
| `planner` | Read-only design plan (prose, no code); asks questions (interactive) or raises `QUESTIONS:` (unattended). |
| `tdd-writer` | Writes **tests only** from the plan; never implementation. |
| `coder` | Implements to the acceptance contract; makes upstream tests pass. |
| `security-reviewer` | Read-only security audit; ends with `VERDICT:`. |
| `bug-reviewer` | Read-only correctness audit; ends with `VERDICT:`. |
| `judge` | Compares competing candidate branches; ends with `SELECT: <id>`. |
| `refactoring` | Behavior-preserving cleanup of working code; keeps all tests green. |

Override any of them with an inline `prompt`/`prompt_file`, and augment with
`skills:`.

## Runner flags

`python -m sbx_omnigent.runner` (or `omni-sbx-pipeline`):

| Flag | Meaning |
| --- | --- |
| `-c, --config` | Path to the `pipeline.yaml` (required). |
| `--canonical-root` | Host dir for the canonical repo mirrors (required; `OMNI_SBX_CANONICAL_ROOT`). |
| `--worktree-root` | Host dir for per-run worktrees (required; must equal `sbx.worktree_root`; `OMNI_SBX_WORKTREE_ROOT`). |
| `--server` | Omnigent server URL (default `http://localhost:6767`; `OMNI_SERVER`). |
| `--run-id` | Unique run id (default: the pipeline `name`). |
| `--publish-repo` | Push target for publish (default: `repo`). |
| `--keep` | Leave the microVMs + worktrees up for inspection instead of tearing down. |
| `--no-interactive-plan` | Don't block the plan stage on human approval — use the planner's single-turn output as-is. |
| `--no-auto-harvest` | Don't start an agy token harvester for this run; refuse instead when the swap secret is stale (use when one runs elsewhere). |
| `--skip-disk-check` | Skip the preflight that refuses to start when free disk can't cover the run's microVMs. |
| `--resume` | Continue the run with this `--run-id` instead of starting clean (see [Resuming a run](#resuming-a-run)). |
| `--turn-timeout` | Seconds one agent turn may take, overriding the pipeline's `turn_timeout`. |

## Agy notes

Agy (`antigravity-native`) agents need the harvest/proxy-swap auth path — the
one-time `agy /login` on the trusted box and a running token harvester — plus
`agy_enabled: true` in the server's `sandbox` block. Model is pinnable
(`--model gemini-3.5-flash`); reasoning effort is not. See
[`ANTIGRAVITY.md`](./ANTIGRAVITY.md).

**The harvester is automatic.** If a pipeline declares any agy agent, the runner
checks the swap secret at startup and, when it is stale, starts
`omni-sbx-agy harvest` itself, blocks until the first refresh lands, and stops it
when the run ends. Without a live harvester the token expires and every agy turn
fails to authenticate *inside agy's own TUI* — which the bridge never surfaces as
a failed session, so the run would simply block to the 30-minute turn timeout and
report a misleading "did not complete."

Only one harvester may poke the trusted box (concurrent pokes race on its token
file), so this is guarded by an exclusive lock. A harvester you started yourself
is detected and waited on, never duplicated. Pass **`--no-auto-harvest`** to
restore the old behavior of refusing with instructions — the right choice when
the harvester runs on a different host.
