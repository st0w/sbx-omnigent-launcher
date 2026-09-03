# Mixed-models pipeline — a specialist team on different model tiers, in one file

The classic coordinator + coder + reviewers swarm, recast as a **declarative
pipeline**. No coordinator agent to chat with, no agent ids to copy, no long CLI
kickoff — you describe the team in [`pipeline.yaml`](./pipeline.yaml) and one
command fires it.

```
plan ──▶ build ──▶ review (consensus) ──▶ publish
(agy)   (Sonnet5)   sec: Sonnet 5  +  bugs: Gemini 3.5
                    ── block? loop findings back to build ──
```

| Role | Harness | Model | Effort |
| --- | --- | --- | --- |
| `plan` | antigravity-native | Gemini 3.5 Flash | — (agy default) |
| `build` | claude-native | Claude Sonnet 5 | **medium** |
| `sec` | claude-native | Claude Sonnet 5 | — |
| `bugs` | antigravity-native | Gemini 3.5 Flash | — (agy default) |

Every role pins its own model; the `build` coder also pins reasoning effort
(`medium`) — `sec` is Claude too but leaves effort at the default, and agy
exposes no effort knob at all (see [`../../docs/ANTIGRAVITY.md`](../../docs/ANTIGRAVITY.md)).
How the pipeline model works is in [`../../docs/PIPELINES.md`](../../docs/PIPELINES.md).

> **New to the launcher?** Start with [`../quickstart/`](../quickstart/) — a
> minimal all-Claude pipeline with no agy and no human-approval gate. This
> example adds agy agents and interactive planning on top.

## Setup

1. Complete the top-level [README](../../README.md) foundation: install the
   launcher, the egress **network policy**, and **Claude via subscription**
   credentials.
2. **Antigravity** — `plan` and `bugs` are agy, so also do the agy path: the
   one-time `agy /login` on the trusted box and a running **token harvester**
   (`omni-sbx-agy harvest`). Without it the two agy VMs fail their readiness gate.
3. Merge [`config.sample.yaml`](./config.sample.yaml) into your server config
   (real absolute `worktree_root`; `agy_enabled: true` is set).
4. Edit [`pipeline.yaml`](./pipeline.yaml): set `repo:` to your project and,
   optionally, the `task:` + `acceptance:`.

## Run it — and be at the UI

This pipeline has a **`planner`**, so by default the `plan` stage is
**interactive**: the planner posts a design and clarifying questions in the
Omnigent UI session titled `mixed-models/plan`, then **blocks until you answer
and reply `APPROVED`** there (up to 1 hour). So drive the run when you can sit
with it — or pass `--no-interactive-plan` to run it unattended (single-turn plan,
no human gate).

```bash
# 1. Start the server WITH the pipeline (registers plan/build/sec/bugs).
omni-sbx server -c <your-config.yaml> --pipeline examples/mixed-models/pipeline.yaml

# 2. Fire the run. `omni-sbx-pipeline` is the console script; the module form
#    below always works even if the script isn't on your PATH yet.
python -m sbx_omnigent.runner -c examples/mixed-models/pipeline.yaml \
  --canonical-root /srv/swarm/canonical \
  --worktree-root  /srv/swarm/worktrees     # must equal sbx.worktree_root
```

`publish:` in the file selects the output: `local` pushes the reviewed branch
(`pipeline/mixed-models`), `pr` opens a draft GitHub PR. Add `--keep` to leave
the VMs + worktrees up for inspection.

**Provision-only:** delete the `task:`/`acceptance:` from the file and the run
stands the VMs up and prints the role→session bindings for you to drive by hand
(via the UI or `omni-sbx-swarm send`) instead of running to completion.

## What you should see

The `plan` stage (Gemini 3.5) posts a design + questions and **waits for your
`APPROVED`** in the `mixed-models/plan` session; while you plan, the writer VM is
pre-warmed in the background. On approval, the planner emits a clean consolidated
plan, `build` (Sonnet 5, medium) implements it on its **own isolated branch**,
and `sec` (Sonnet 5) and `bugs` (Gemini 3.5) review it `:ro` — both must end
`VERDICT: APPROVED`; a block loops their findings back to `build` for another
round. On consensus the runner commits `build`'s branch, **also commits the
approved plan to `docs/plans/mixed-models.md`** on that branch (so the design
ships with the code — override the path with `plan_artifact:`), and publishes it.
Every microVM is torn down at the end unless you passed `--keep`.

To confirm the pins: `plan`/`bugs` launched `agy --model gemini-3.5-flash`,
`build`/`sec` both launched `claude --model claude-sonnet-5`, and
`build` ran at medium effort.
