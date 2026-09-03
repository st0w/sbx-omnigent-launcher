# TDD + competing writers — the DAG's full power in one file

This pipeline shows what the declarative model unlocks beyond a single
coder + reviewers: a **dedicated TDD agent** writes the tests, then **two coders
implement the same task independently** — each in its own isolated worktree and
branch — and a **judge** picks the winner to publish.

```
plan ─▶ tests ─┬─▶ impl-a  (Claude Sonnet 5, own branch) ─┐
 (agy)  (TDD)   └─▶ impl-b  (agy / Gemini 3.5, own branch) ─┴─▶ pick (judge) ─▶ publish
```

Both coders' worktrees are **cut from the tests branch** (inheritance), so each
starts with the failing tests but writes on its own branch — writers never share
a filesystem. The judge mounts both candidate branches read-only and selects
one; the orchestrator publishes that branch. See
[`../../docs/PIPELINES.md`](../../docs/PIPELINES.md) for the model.

| Node | Agent (template) | Harness | Model | Writes? |
| --- | --- | --- | --- | --- |
| `plan` | planner | antigravity-native | Gemini 3.5 Flash | no (`:ro` design) |
| `tests` | tdd-writer + `skills/tdd` | claude-native | Claude Sonnet 5, high | yes — tests only |
| `impl-a` | coder | claude-native | Claude Sonnet 5, high | yes — own branch |
| `impl-b` | coder | antigravity-native | Gemini 3.5 Flash | yes — own branch |
| `pick` | judge | claude-native | Claude Opus 4.8 | no (selects a branch) |

The `tests` agent carries a per-agent [`skills/tdd`](./skills/tdd/SKILL.md)
directory — a Polly-style skills override layered on top of the `tdd-writer`
template to pin how it writes tests.

> **New to the launcher?** Start with [`../quickstart/`](../quickstart/) — a
> minimal all-Claude pipeline with no agy and no human-approval gate. This
> example is the fullest one: agy agents, interactive planning, competing
> writers, and a judge.

## Setup

1. Complete the top-level [README](../../README.md) foundation: install the
   launcher, the egress **network policy**, and **Claude via subscription**
   credentials.
2. **Antigravity** — `plan` and `impl-b` are agy, so also do the agy path: the
   one-time `agy /login` on the trusted box and a running **token harvester**
   (`omni-sbx-agy harvest`). Use the [`mixed-models`](../mixed-models/) example's
   [`config.sample.yaml`](../mixed-models/config.sample.yaml) as your server
   `sandbox:` block (it sets `agy_enabled: true`), adjusting paths.
3. Edit [`pipeline.yaml`](./pipeline.yaml): set `repo:` to your project and,
   optionally, the `task:` + `acceptance:`.

## Run it — and be at the UI

This pipeline has a **`planner`**, so by default the `plan` stage is
**interactive**: the planner posts a design and clarifying questions in the
Omnigent UI session titled `tdd-race/plan`, then **blocks until you answer and
reply `APPROVED`** there (up to 1 hour). Drive the run when you can sit with it —
or pass `--no-interactive-plan` to run it unattended (single-turn plan, no gate).

```bash
# 1. Start the server WITH the pipeline (registers plan/tdd/impl_claude/impl_agy/judge).
omni-sbx server -c <your-config.yaml> --pipeline examples/tdd-race/pipeline.yaml

# 2. Fire the run. `omni-sbx-pipeline` is the console script; the module form
#    below always works even if the script isn't on your PATH yet.
python -m sbx_omnigent.runner -c examples/tdd-race/pipeline.yaml \
  --canonical-root /srv/swarm/canonical \
  --worktree-root  /srv/swarm/worktrees     # must equal sbx.worktree_root
```

Add `--keep` to leave the microVMs + worktrees up for inspection afterwards.

## What you should see

The `plan` stage (Gemini 3.5) posts a design + questions and **waits for your
`APPROVED`** in the `tdd-race/plan` session; while you plan, the tests and both
impl VMs are pre-warmed in the background. On approval, the planner emits a clean
consolidated plan and the DAG runs: `tests` (Sonnet 5) writes a test suite on its
own branch that fails against the unimplemented code; `impl-a` (Claude) and
`impl-b` (agy) each cut a fresh branch **from the tests branch** and implement in
parallel to make those tests pass; `pick` (Opus 4.8) reads both candidate
branches side by side (`./impl-a`, `./impl-b`) and ends with `SELECT: impl-a` or
`SELECT: impl-b`. The runner publishes the winning branch (`local` push, or a
draft PR if `publish: pr`) and **also commits the approved plan to
`docs/plans/tdd-race.md`** on it (override the path with `plan_artifact:`). Every
microVM is torn down at the end (`sbx ls` shows none) unless you passed `--keep`.
