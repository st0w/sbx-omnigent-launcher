# Full cadre — the whole swarm in one pipeline

Every role the launcher ships, working together on one task: a **planner**
whose claims are **checked** before its plan is final, a dedicated **TDD**
author, a **contract check** of its suite, **two competing coders** (Claude vs
Antigravity) each
on its own isolated branch, a **security** reviewer and a **bug** reviewer gating
*each* candidate, a **judge** that picks the winner, a **refactor** agent that
polishes it, and one final review before publish.

```
plan ─▶ tests ─▶ check ─┬─▶ impl-a (Claude) ─▶ review-a [sec+bugs] ─┐
 (agy)  (TDD)   (ro)    └─▶ impl-b (agy)    ─▶ review-b [sec+bugs] ─┴─▶ pick ─▶ refactor ─▶ review-r ─▶ publish
                             each review loops findings back to its OWN impl on a block   (winner)   [sec+bugs]
```

| Node | Template | Harness | Model | Role |
| --- | --- | --- | --- | --- |
| `plan` | planner | antigravity-native | Gemini 3.8 Flash, high | read-only design (interactive) |
| `plan-check` | plan-checker | claude-native | Claude Sonnet 5 | checks the plan's claims about existing code, inside your approval (`:ro`) |
| `tests` | tdd-writer | claude-native | Claude Sonnet 5, high | writes a failing test suite |
| `check` | contract-checker | claude-native | Claude Sonnet 5 | checks the suite, brief and plan can all be met (`:ro`) |
| `impl-a` | coder | claude-native | Claude Sonnet 5, high | competing coder, own branch |
| `impl-b` | coder | antigravity-native | Gemini 3.8 Flash, high | competing coder, own branch |
| `sec` | security-reviewer | claude-native | Claude Sonnet 5 | security audit (`:ro`) |
| `bugs` | bug-reviewer | antigravity-native | Gemini 3.8 Flash, high | correctness audit (`:ro`) |
| `judge` | judge | claude-native | Claude Opus 4.8 | picks the winning branch |
| `refactor` | refactoring | claude-native | Claude Sonnet 5, high | cleans up the winner (behavior-preserving) |

## The plan check inside your approval

When you reply `APPROVED` to the planner, `plan-check` runs a command against
the repository for each claim the plan makes about existing code, before the
plan is final. If one is false, or two sections contradict each other, its
report goes into your planning session and approval reopens. Settle it with the
planner, then reply `APPROVED` to have the plan checked again, or
`APPROVED AS IS` to accept it without another check.

## The contract check before the coders

`check` reads the frozen suite beside the brief and the plan before either coder
starts. If two tests can't both pass, a test contradicts the brief, or the brief
contradicts itself, it writes `DISPUTED:` and the run stops there, before two
implement stages are spent on a contract nobody can meet. Weaker problems, such
as a test that already passes or a requirement with no test, go in its
`FINDINGS:` list, and the run goes on. Those land in the findings ledger beside
the reviewers' findings. Both coders list
`check` in `needs`, which is what makes them wait for it.

## The review-before-judge, refactor-after-judge shape

Two review points, for two different reasons:

- **Before the judge** — each competing impl is vetted by `sec` + `bugs`, and a
  block loops findings back to *that specific* impl (`on_block: impl-a`/`impl-b`).
  Since a review gate must loop to a fixed writer, this only works per-candidate,
  before the winner is chosen — so the judge always picks between two already
  clean impls.
- **After the judge** — the `refactor` agent cleans up the *winner*
  (behavior-preserving; all tests stay green), then one final `sec` + `bugs`
  review vets the refactor before publish (refactoring can quietly regress). This
  is enabled by the judge publishing its winner as its **own branch** (so the
  `refactor` writer can seed `from: pick` and the review can mount it) — otherwise
  a judge leaves no branch to build on.

The refactor runs **once**, on the shipped code, and is itself reviewed. See
[`../../docs/PIPELINES.md`](../../docs/PIPELINES.md) for the DAG model.

## Setup

This is the fullest example — it needs the complete foundation **plus**
Antigravity:

1. Top-level [README](../../README.md) foundation: install the launcher, the
   egress **network policy**, and **Claude via subscription** credentials.
2. **Antigravity** (`plan`, `impl-b`, `bugs` are agy): the one-time `agy /login`
   on the trusted box and a running **token harvester** (`omni-sbx-agy harvest`).
   Use the [`mixed-models`](../mixed-models/) example's
   [`config.sample.yaml`](../mixed-models/config.sample.yaml) as your server
   `sandbox:` block (it sets `agy_enabled: true`).
3. **Resources**: this pipeline can hold **~14 microVMs** over its life (plan +
   plan-check + tests + check + 2 impls + 4 impl-reviewers + judge + refactor +
   2 refactor-reviewers).
   Make sure `sbx.worktree_root` is set and your host can spare the memory. Prefer
   `sbx.provision_stagger_s` at its default so the near-simultaneous VM launches
   don't race the sbx proxy injector.
4. Edit [`pipeline.yaml`](./pipeline.yaml): set `repo:` to your project and fill
   in `task:` + `acceptance:` (and optionally `context:`).

> **No Antigravity?** Swap the `antigravity-native` agents for `claude-native`
> (give `impl_agy` a different model/effort so the two coders still differ) and
> drop `agy_enabled` from your server config — then it needs no agy login or
> harvester.

## Run it — and be at the UI

It has a `planner`, so the plan stage is **interactive**: drive the
`full-cadre/plan` session in the Omnigent UI, then reply **`APPROVED`**. Nothing
downstream runs until you do (up to 1 hour). While you plan, the writer VMs
(tests, both impls, refactor) pre-warm in the background.

```bash
# 1. Start the server WITH the pipeline (registers all 8 agents).
omni-sbx server -c <your-config.yaml> --pipeline examples/full-cadre/pipeline.yaml

# 2. Fire the run. `omni-sbx-pipeline` is the console script; the module form
#    below always works even if the script isn't on your PATH yet.
python -m sbx_omnigent.runner -c examples/full-cadre/pipeline.yaml \
  --canonical-root /srv/swarm/canonical \
  --worktree-root  /srv/swarm/worktrees     # must equal sbx.worktree_root
```

Add `--keep` to leave the microVMs + worktrees up for inspection afterwards.

## What you should see

`plan` (Gemini 3.8) posts a design + questions and waits for your `APPROVED`, then
emits a consolidated final plan. `tests` (Sonnet 5) writes a failing suite on its
own branch. `impl-a` (Claude) and `impl-b` (agy) each cut a branch **from the
tests branch** and implement to make the suite pass. `review-a` then `review-b`
run `sec` + `bugs` over each impl — both must end `VERDICT: APPROVED`, or the
findings loop back to *that* impl for another round. `pick` (Opus 4.8) reads the
two vetted branches side by side and ends with `SELECT: impl-a`/`SELECT: impl-b`;
the winner is published as its own `pick` branch. `refactor` (Sonnet 5) cuts a
branch **from `pick`** and cleans up the winner without changing behavior (tests
stay green), then `review-r` vets the refactor with `sec` + `bugs`. The runner
publishes the **refactored** branch (`pr` opens a draft PR; `local` pushes
`pipeline/full-cadre`) and commits the approved plan to
`docs/plans/full-cadre.md`. Every microVM is torn down at the end unless you
passed `--keep`.
