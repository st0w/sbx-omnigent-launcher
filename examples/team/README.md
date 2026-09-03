# Team example — planner + coder + two distinct reviewers

A specialist **team** in one swarm. A **planner** shapes the change, a **coder**
implements it, and two *distinct* reviewers — a **correctness/bug** reviewer and
a **security** reviewer — must **both** approve before the coordinator commits
and publishes. Every read-only specialist is its own agent (own spec, prompt, and
optionally its own skills), not just a role label.

> This is a **coordinator-driven** swarm (you chat with a `swarm-coordinator`).
> For the same team expressed as a one-file declarative pipeline, see
> [`../mixed-models/`](../mixed-models/) and [`../../docs/PIPELINES.md`](../../docs/PIPELINES.md).

```
              swarm-coordinator  (host, trusted plane)
                       │  drives: plan → implement → review → consensus
   ┌───────────┬───────┴───────┬───────────────────┐
 swarm-planner │          swarm-reviewer-bug   swarm-reviewer-security
  (:ro plan)   │           (:ro verdict)        (:ro verdict)
          swarm-coder
          (rw implement)
```

Flow: **plan** (planner reads the code, drafts a prose design plan — no code —
and raises `QUESTIONS:`; the coordinator brings the plan and its proposed answers
to **you** and waits for your approval — planning is an interactive discussion,
not autonomous) → **implement** (once you approve the plan, the coder works to it
+ the contract) → **review** (bug + security reviewers each read the live `:ro`
tree and return `VERDICT:`) → iterate to consensus (both APPROVED) → **commit**
(trusted plane, coder as author) → **publish** (draft PR or local) → **dispose**.
Everything after your plan approval runs autonomously.

## Agents used

All ship in [`../../agents/`](../../agents/) and are **auto-registered** on
`omni-sbx server`:

- `swarm-coordinator` — the orchestrator you chat with.
- `swarm-planner` — reads the code, produces the prose design plan (no code).
- `swarm-coder` — the implementer.
- `swarm-reviewer-bug` — correctness / bug review.
- `swarm-reviewer-security` — security review.

Add more specialists (TDD, refactoring, API-contract, …) by dropping another
`agents/swarm-reviewer-*/` directory — each with its own prompt and, if you want,
its own `skills/` — and binding it to a role below. See
[`../../agents/README.md`](../../agents/README.md).

## Per-role agent binding

This example maps **a different agent to each role**, using `omni-sbx-swarm`'s
per-role form (`--reviewer <role>=<agent_id>`, repeatable):

```
--reviewer planner=<swarm-planner id> \
--reviewer bug-hunter=<swarm-reviewer-bug id> \
--reviewer security=<swarm-reviewer-security id>
```

That per-role form is what lets each role bind a *distinct* specialist. (The
single-spec form `--reviewer-agent <id> --reviewer-role <name>` binds one spec to
every role and can't give distinct specialists, so use the per-role form here.)

## Setup

Same foundation as [Basic](../basic/): install the launcher, network policy,
credentials; merge [`config.sample.yaml`](./config.sample.yaml); start
`omni-sbx server`. Then note the ids for `swarm-planner`, `swarm-coder`,
`swarm-reviewer-bug`, and `swarm-reviewer-security` (`sys_agent_list`).

## Run it

Open a **swarm-coordinator** session and give it the task + the per-role config.
For example:

> Run one swarm to completion using the review-loop skill. Config:
> `--swarm-id team1`, repo `/path/to/your/project` (local),
> `--coder-agent <swarm-coder id>`,
> `--reviewer planner=<swarm-planner id>`,
> `--reviewer bug-hunter=<swarm-reviewer-bug id>`,
> `--reviewer security=<swarm-reviewer-security id>`,
> `--canonical-root /srv/swarm/canonical`,
> `--worktree-root /srv/swarm/worktrees`, publish **locally** (`--no-pr`).
>
> Task: add a `parse_ports(spec)` helper to `net.py` that turns a string like
> `"22,80,1000-1002"` into a sorted list of unique ints `[22, 80, 1000, 1001,
> 1002]`. Acceptance: handles single ports and `a-b` ranges, dedupes, sorts,
> rejects out-of-range (<1 or >65535) and malformed input with a `ValueError`,
> and changes nothing else.
>
> Have the **planner** plan first, relay the plan to the coder, then have **both**
> reviewers approve before you commit (author `swarm coder <coder@swarm.local>`),
> publish `--no-pr`, and dispose.

**Publish mode** works exactly as in Basic: local (`--no-pr`, into a local repo)
or GitHub (`gh` authed, drop `--no-pr` → draft PR). The coordinator never merges.

## What you should see

The planner returns a prose design plan (no code); the coder implements `parse_ports`;
**both** reviewers end `VERDICT: APPROVED` (correctness *and* security) — possibly
after one revision round if a reviewer flags an edge case (e.g. a missing range
bound); the reviewed change lands on `task/team1` with `main` untouched; and all
four microVMs are torn down at the end.
