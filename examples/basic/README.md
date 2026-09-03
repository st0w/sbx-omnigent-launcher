# Basic example — coder + one reviewer

The simplest working swarm: a **coordinator** delegates a task to one **coder**
(read-write worktree) and one **security reviewer** (read-only), iterates to
consensus, then commits and publishes for you to merge. This is exactly the setup
the top-level [Quickstart](../../README.md#quickstart) walks through.

```
         swarm-coordinator  (host, trusted plane)
                 │  drives the review-loop
        ┌────────┴─────────┐
   swarm-coder          swarm-reviewer-security
   (microVM, rw)        (microVM, :ro — same worktree)
```

## Agents used

All three ship in [`../../agents/`](../../agents/) and are **auto-registered**
when you start `omni-sbx server` — nothing to install:

- `swarm-coordinator` — the orchestrator you chat with.
- `swarm-coder` — the implementer.
- `swarm-reviewer-security` — the reviewer.

## Setup

1. Complete the foundation once (top-level [README](../../README.md)): install
   the launcher, the egress **Network policy**, and **Claude via subscription**
   credentials.
2. Merge [`config.sample.yaml`](./config.sample.yaml) into your server config
   (set `worktree_root` to a real absolute dir on your host).
3. Start the server:
   ```bash
   omni-sbx server
   ```
4. Grab the coder + reviewer agent ids:
   ```bash
   omni-sbx-swarm --help >/dev/null   # confirms the CLI is on PATH
   # in the UI or via the API: sys_agent_list  →  note the ids for
   #   swarm-coder  and  swarm-reviewer-security
   ```

## Run it

Open a **swarm-coordinator** session (in the Omnigent UI) and give it the task,
the repo, and the two agent ids. For example:

> Run one swarm to completion using the review-loop skill. Start it with
> `--coder-agent <swarm-coder id> --reviewer-agent <swarm-reviewer-security id>
> --reviewer-role security` (one reviewer). Config: `--swarm-id demo1`, repo
> `/path/to/your/project` (local), `--canonical-root /srv/swarm/canonical`,
> `--worktree-root /srv/swarm/worktrees`, publish **locally** (`--no-pr`).
>
> Task: add a function `safe_divide(a, b)` to `calc.py` that returns `a / b`,
> except it returns `None` when `b` is 0 (no exception). Acceptance: the function
> exists, returns `None` on a zero divisor, and nothing else changes. Have the
> security reviewer approve, commit (author `swarm coder <coder@swarm.local>`),
> publish `--no-pr`, and dispose.

The coordinator runs the loop and reports back. **Publish mode:**

- **Local** (shown above) — point `--repo-url` at a local repo and pass
  `--no-pr`; the reviewed `task/demo1` branch lands in your repo to merge. No
  GitHub, no `gh`.
- **GitHub** — point `--repo-url` at a GitHub URL, have `gh` authenticated, and
  drop `--no-pr` (or set `OMNI_SBX_PUBLISH_MODE=github`); it opens a **draft PR**.

Either way the coordinator never merges — the branch/PR is yours to review.

## What you should see

`calc.py` gains `safe_divide` on branch `task/demo1`; `main` is untouched; the
reviewer's turn ends `VERDICT: APPROVED`; and the swarm's microVMs are torn down
at the end (`sbx ls` shows none).
