# Examples

Complete, copy-pasteable recipes. There are two ways to run a swarm, and the
examples come in both flavors:

- **Declarative pipelines (recommended)** — describe the whole swarm in one
  `pipeline.yaml` (who's on it, each agent's model/skills, how work flows), then
  fire it with `omni-sbx server --pipeline …` + the runner. No coordinator chat,
  no agent ids, no long CLI kickoff. See [`../docs/PIPELINES.md`](../docs/PIPELINES.md).
- **Coordinator-driven swarms** — chat with a `swarm-coordinator` agent that runs
  the review loop for you (the original flow).

| Example | Kind | What it shows |
| --- | --- | --- |
| [`quickstart/`](./quickstart/) | pipeline | **Start here.** The minimal working pipeline — one coder + one reviewer, **all Claude** (no agy) and **no planner** (runs unattended). Matches the top-level [Quickstart](../README.md#quickstart). |
| [`mixed-models/`](./mixed-models/) | pipeline | A heterogeneous review swarm in one file: planner (Gemini 3.5) → coder (Sonnet 5, medium) → consensus review (Sonnet 5 security + Gemini 3.5 bug), loop-back on block. Demonstrates **per-agent model + effort pinning** and **interactive planning**. Needs the agy harvester. |
| [`tdd-race/`](./tdd-race/) | pipeline | The DAG's full power: a **TDD writer** feeds **two competing coders** (each on its own isolated branch), a **judge** picks the winner. Demonstrates isolated writers, branch inheritance, a **per-agent skills override**, and interactive planning. Needs the agy harvester. |
| [`full-cadre/`](./full-cadre/) | pipeline | **The kitchen sink** — every role at once: planner + TDD + two competing coders, each vetted by a **security + bug** consensus gate; a **judge** picks the winner, a **refactor** agent polishes it, and one final review ships it. The heaviest example (~12 microVMs). Needs the agy harvester. |
| [`per-module/`](./per-module/) | pipeline | **A component, module by module.** You supply an ordered module table; the full cadre runs **once per module**, each module planned in-loop against the **frozen** prior modules and published to its own branch. Demonstrates **per-module planning** (`subtask_file:`) and the **campaign** thread. The full-cadre shape × N modules. Needs the agy harvester. |
| [`codex-smoke/`](./codex-smoke/) | pipeline | **A Codex writer.** The smallest pipeline that exercises the `codex-native` harness end to end — codex writes, Claude reviews, verify gates, publish. Demonstrates **Codex credential seeding** and per-agent harness selection. Needs `codex login` on the host; no agy. |
| [`basic/`](./basic/) | coordinator | The simplest coordinator swarm — one coder, one security reviewer, to consensus, driven from a chat session. |
| [`team/`](./team/) | coordinator | A specialist **team**: planner + coder + two distinct reviewers (correctness + security) that must both approve. |

All examples share the same foundation — install the launcher, configure the
`sandbox:` provider, set the network policy + credentials. See the top-level
[README](../README.md) for that setup; the pipeline examples additionally use
[`../docs/PIPELINES.md`](../docs/PIPELINES.md), and the coordinator examples use
[`../agents/README.md`](../agents/README.md) for how the agents work. Each
example's README lists only what's specific to it.

> **Interactive planning (pipeline examples with a `planner`).** By default a
> pipeline that has a planner **blocks on you**: the planner posts questions in
> the Omnigent UI session `<name>/plan` and waits for your `APPROVED` reply
> before anything downstream runs. Drive those runs live, or pass
> `--no-interactive-plan` to run unattended. The `quickstart` example has no
> planner, so it runs start-to-finish with no gate. On publish, a planner's
> approved plan is committed to `docs/plans/<name>.md` on the shipped branch.

Every example can publish to **GitHub** (a draft PR) or **locally** (push the
reviewed branch, no GitHub) — the pipeline examples set this with `publish:` in
their `pipeline.yaml`; the coordinator examples show both in their README.

The **Team** example binds a distinct specialist agent per role using
`omni-sbx-swarm`'s per-role flag (`--reviewer <role>=<agent_id>`, repeatable) —
supported directly; see [`team/README.md`](./team/).
