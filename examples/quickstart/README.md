# Quickstart — the minimal working pipeline (one coder + one reviewer)

The smallest pipeline that does real work: a **coder** implements your task on an
isolated branch, a **reviewer** reads that branch read-only and must approve, and
the reviewed branch is published. It is the fastest way to confirm your setup and
start using the launcher on your own code.

```
build (coder) ──▶ review (reviewer, must APPROVE) ──▶ publish
                  └── blocks? loop findings back to build ──┘
```

**Why this is the starting point:** it is **all-Claude** (no Antigravity, so no
`agy` login or token harvester) and has **no planner** (so nothing blocks waiting
for you — it runs unattended to a branch/PR). The [`mixed-models/`](../mixed-models/)
and [`tdd-race/`](../tdd-race/) examples add the full cadre (agy agents,
interactive planning, competing writers + a judge) once you want it.

## Setup

Just the top-level [README](../../README.md) foundation — no agy needed:

1. Install the launcher into your Omnigent environment.
2. Add the `sandbox:` block to your server config (see
   [`../../config.sample.yaml`](../../config.sample.yaml)); set an absolute
   `sbx.worktree_root`.
3. The egress **network policy** (`sbx policy allow network …`).
4. **Claude** credentials — the [subscription (`setup-token`) path](../../README.md#claude-via-subscription-no-per-vm-login)
   or an API key.

Then edit [`pipeline.yaml`](./pipeline.yaml): set `repo:` to your project and
fill in `task:` + `acceptance:`.

## Run it

Two commands — the server registers the pipeline's agents at startup, then the
runner fires it:

```bash
# 1. Start the server WITH the pipeline (registers build + reviewer).
omni-sbx server -c <your-config.yaml> --pipeline examples/quickstart/pipeline.yaml

# 2. Fire the run. `omni-sbx-pipeline` is the console script; the module form
#    below always works even if the script isn't on your PATH yet.
python -m sbx_omnigent.runner -c examples/quickstart/pipeline.yaml \
  --canonical-root /srv/swarm/canonical \
  --worktree-root  /srv/swarm/worktrees     # must equal sbx.worktree_root
```

Add `--keep` to leave the microVMs + worktrees up for inspection afterwards.

## What you should see

`build` (Sonnet 5) implements your task on its **own isolated branch**;
`reviewer` (Sonnet 5) reads that branch `:ro` and ends `VERDICT: APPROVED` — or,
if it finds a problem, `VERDICT: BLOCKING`, which loops its findings back to
`build` for another round (up to a cap). On approval the runner commits `build`'s
branch and publishes it (`publish: local` pushes `pipeline/quickstart`; set
`publish: pr` for a draft GitHub PR). Every microVM is torn down at the end
(`sbx ls` shows none) unless you passed `--keep`.

This pipeline has no planner, so it runs start-to-finish with no human gate. Add
a `planner` agent (as `mixed-models` and `tdd-race` do) and the plan stage will
pause for you to review and reply `APPROVED` in the Omnigent UI — see
[`../../docs/PIPELINES.md`](../../docs/PIPELINES.md#interactive-planning).
