# Per-module pipeline

One component is too big for a single run, but it splits into ordered,
dependent modules. **You** supply the module list; the runner runs the
full cadre — planner → TDD → two competing coders → security+bug reviews
→ judge → refactor → final review — **once per module**, in order. Each
module is planned in-loop against the **frozen** artifacts of the modules
before it, built to green, and published to its own branch.

```
for each module (in dependency order):
  plan ─▶ tests ─┬─▶ impl-a ─▶ review-a ─┐
        (module) └─▶ impl-b ─▶ review-b ─┴─▶ pick ─▶ refactor ─▶ review-r ─▶ publish pipeline/<run>-<module>
   └─ planner reads the accumulated tip (prior modules, frozen), designs THIS module
```

## The files

| File | Role |
| --- | --- |
| [`pipeline.yaml`](./pipeline.yaml) | The cadre + wiring. `subtask_file` is what makes it per-module. |
| [`modules.md`](./modules.md) | **You edit this.** The ordered module table — one `- [<id>] <title>` line each, in dependency order. |
| [`brief.md`](./brief.md) | The shared whole-component brief every module's planner gets (invariants, data model, stage directives) — **not** the module list. |
| [`context.md`](./context.md) | Short, stable "system prompt" baked into every agent. |

## What makes it per-module

The one line that switches modes is **`subtask_file: modules.md`** (you
could also inline the list as `subtasks: [{id, title}, …]`). When the
config supplies a module list:

- The **planner runs inside the loop, per module** — interactive, so it
  **blocks on your `APPROVED` once per module**. It designs only that
  module, treating earlier modules as fixed contracts.
- Each module's planner design feeds that module's **single TDD writer**
  (design + work breakdown → the failing suite) — **one build cycle per
  module**, not a nested sub-loop.
- Modules **thread**: module N's worktree seeds from the accumulated tip
  of modules 0…N-1, so each builds on the last. Each module's own plan is
  committed to `docs/plans/<name>-<module>.md` on its branch.
- Each module **publishes separately** to `pipeline/<run>-<module>` (a
  draft PR here). A block in one module stops the run there; the modules
  that already shipped stay shipped.

### vs. the flat campaign

Drop `subtask_file`/`subtasks` and give the pipeline a plain `task`, and a
**single up-front planner** proposes the chunk list itself (a `SUBTASKS:`
block), then the runner loops only the **build** stages per chunk (one
shared plan of record). Use the flat campaign when the decomposition is
the planner's call; use **per-module** when you already know the modules
and each deserves its own design pass against the frozen prior work.

## Run it

This is a long, expensive job — up to ~12 microVMs **per module**, and it
pauses for your approval once per module — so drive it live:

```bash
omni-sbx server --pipeline examples/per-module/pipeline.yaml
# then, from another shell, the runner (see ../../docs/PIPELINES.md):
omni-sbx-pipeline examples/per-module/pipeline.yaml
```

Edit `repo:` in `pipeline.yaml` first, and fill in `modules.md`,
`brief.md`, and `context.md`. Needs the agy harvester (three agy agents);
see the top-level [README](../../README.md) for setup, or swap the
`antigravity-native` agents for `claude-native` to run with no agy.

> **Tip.** Start with two small modules to shake out the wiring before
> committing to a long multi-module run — you approve each module's plan
> as it comes, and can stop after any module (earlier ones stay published).
