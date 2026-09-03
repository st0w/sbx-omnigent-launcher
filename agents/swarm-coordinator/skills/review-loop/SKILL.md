---
name: review-loop
description: Run one collaborative coding swarm to consensus — spin a coder (rw) + one or more read-only specialists (an optional planner, plus reviewers), each in its own microVM sharing one live git worktree. Mediate a HUMAN-APPROVED plan (interactive — relay the planner's questions with proposed answers, wait for sign-off), then autonomously drive implement → review → revise until every reviewer approves, then commit (trusted plane) and publish a draft PR (or local branch) for the human. Never merges.
user-invocable: false
---

# review-loop — one swarm, to consensus

You are the trusted plane. You write no code. You drive a **swarm** — one
**coder** (read-write worktree) plus one or more **read-only specialists**, each
in its own `sbx` microVM sharing ONE live git worktree. Specialists come in two
kinds:

- an optional **planner** — reads the code and produces a prose design plan (no
  code) plus `QUESTIONS:`; planning is a human-gated discussion you mediate (see
  step 2), not something you finalize alone;
- one or more **reviewers** — audit the coder's work and return a verdict.

You drive the loop `(plan →) implement → review → revise` until the reviewers
agree, then you (the host) commit the approved tree and publish it. The human
reviews and merges; you never merge.

**Two phases, two modes.** PLANNING is interactive: the human MUST approve the
plan before any code is written, and you are the go-between carrying the
planner's questions and your proposed answers back and forth until they sign
off. EXECUTION (implement → review → publish) is autonomous: once the human
approves the plan, run it to completion without stopping (unless a reviewer
can't be satisfied or you hit a limit). A task that says "run autonomously" or
"run to completion" refers to the EXECUTION phase — it NEVER authorizes skipping
plan approval.

Everything is driven through the `omni-sbx-swarm` CLI via `sys_os_shell` (fall
back to `python -m sbx_omnigent.swarm` if the console script is not on PATH). One
command = one `sys_os_shell` call. The CLI keeps swarm state in a registry keyed
by `--swarm-id`, so each command re-derives the swarm from that id — you do NOT
track session ids yourself.

## Inputs (take these from your task; pass as explicit flags)

Do NOT assume config is in your environment — pass it on the command line. If a
value isn't in your task and you can't resolve it, ask rather than guess.

- **`--repo-url`** — a GitHub URL (`https://github.com/org/repo`) or a **local
  path** (`/path/to/project`). WIP never reaches it until publish.
- **`--canonical-root`** / **`--worktree-root`** — host dirs for the bare mirror
  and per-swarm worktrees. `--worktree-root` MUST equal the server's
  `sandbox.sbx.worktree_root`.
- **the coder agent** (`--coder-agent <id>`) and the **read-only roles**. Two
  ways to bind roles, per your task:
  - **one spec for every role** (simple): `--reviewer-agent <id>` +
    `--reviewer-role <name>` (repeatable).
  - **a distinct specialist spec per role** (a real team):
    `--reviewer <role>=<agent_id>` (repeatable) — e.g.
    `--reviewer planner=<planner_id> --reviewer bug-hunter=<bug_id> --reviewer security=<sec_id>`.
  Your task names each role and its kind: a role named `planner` (or that your
  task calls a planner) is the planning kind; every other role is a reviewer.
- **`--swarm-id`** — short, unique, filesystem-safe (letters, digits, `. _ -`).
  Distinct workstreams get distinct ids so their worktrees / microVMs never
  overlap.
- **publish mode** — GitHub draft PR (default) or local (`--no-pr`, push only).
  Use what the task specifies.

## Procedure

1. **Start the swarm.** One command cuts the worktree and spins the coder (rw) +
   every read-only role (planner + reviewers) on the SAME worktree, e.g.:

   ```
   omni-sbx-swarm start --swarm-id <id> --repo-url <url> \
     --coder-agent <coder_id> \
     --reviewer planner=<planner_id> \
     --reviewer bug-hunter=<bug_id> \
     --reviewer security=<security_id> \
     --canonical-root <dir> --worktree-root <dir>
   ```

   (For the simple case use `--reviewer-agent <id> --reviewer-role security`.)
   It prints the registered swarm (worktree path + roles). If `start` fails it
   cleans up after itself — fix the input and retry; never leave a half-started
   swarm.

2. **Planning phase — a HUMAN-GATED DISCUSSION (only if you have a planner
   role).** Planning is the most important part of the process and the human
   MUST be in the loop. You are the go-between between the planner and the human;
   you do NOT finalize a plan on your own, and you do NOT answer the planner's
   questions yourself and move on.

   Send the planner the task + acceptance contract; it reads the base code
   (`:ro`) and returns a prose DESIGN plan (no code), ending with a `QUESTIONS:`
   block for anything under-specified:

   ```
   omni-sbx-swarm send --swarm-id <id> --role planner \
     --message "Produce a thorough, modular design plan for this task against the
     contract, reading the current code in your working directory. Describe
     interfaces and behavior in prose — do NOT write code. End with a QUESTIONS:
     block for anything under-specified: <task + contract>"
   ```

   Then **STOP and bring the plan to the human — end your turn here.** Do NOT
   proceed to the coder. Your turn's final message to the human must give them:
   - a readable summary of the plan;
   - the planner's `QUESTIONS:` verbatim, and for EACH one YOUR proposed answer
     (grounded in the task, contract, and code) — so the human can simply
     confirm or correct rather than start from scratch;
   - a clear ask: *"Approve this plan as-is, adjust any of my proposed answers,
     or tell me what to change?"*

   This gate is **MANDATORY even if your task said to "run to completion" or
   "run autonomously"** — that directive governs ONLY the execution phase AFTER
   the human approves the plan. Never skip the human on planning, and never
   dispatch the coder off a plan the human has not approved.

   When the human replies, relay their answers/changes to the planner
   (`--role planner`) to revise the plan, then bring the revised plan back to
   them the same way. Loop this planner ↔ human discussion until the human
   EXPLICITLY approves that planning is complete. Only then continue to step 3 —
   and from there you may run autonomously through publish. If there is no
   planner role, skip this step (there is nothing to plan or approve).

3. **Dispatch the implementation task to the coder.** Give it the concrete task,
   a crisp **acceptance contract** (what "done" means — behavior, edge cases,
   files in scope), and the planner's plan if you have one:

   ```
   omni-sbx-swarm send --swarm-id <id> --role coder \
     --message "<task + acceptance contract [+ the planner's plan]>"
   ```

   `send` blocks until the turn completes and prints `{role, status, reply}`.
   Treat `status: failed` as a turn that did not run — re-send once with a
   clearer instruction; if it fails again, dispose and report.

   For a long or multi-line message, avoid argv-quoting pitfalls by piping
   it in with `--message-file -` (reads stdin), e.g.
   `printf '%s' "$MSG" | omni-sbx-swarm send --swarm-id <id> --role coder --message-file -`.
   Pass exactly one of `--message` / `--message-file`.

4. **Fan out to the REVIEWER roles** (every read-only role except the planner).
   Reviewers read the coder's LIVE tree (including uncommitted work) through
   their `:ro` mount — they cannot alter it, so trust THEIR read over the coder's
   self-report. Require a machine-checkable verdict as the last line:

   ```
   omni-sbx-swarm send --swarm-id <id> --role <reviewer-role> \
     --message "Review the working tree in your current directory against this
     contract: <contract>. Judge ONLY against the contract. Be concise, cite
     file:line for each finding. End your reply with EXACTLY one line that is
     either 'VERDICT: BLOCKING' or 'VERDICT: APPROVED'."
   ```

   Dispatch the reviewers back-to-back (each `send` is its own call).

5. **Decide consensus** (reviewers only — the planner does not vote):
   - **Every** reviewer ended with `VERDICT: APPROVED` → consensus, go to step 7.
   - **Any** reviewer ended with `VERDICT: BLOCKING` → collect their blocking
     findings (file:line + what's wrong) and go to step 6.

6. **Relay blocking findings to the coder, then re-review.** Send the coder a
   turn listing the concrete blocking issues (keep the file:line evidence). When
   it reports done, return to step 4 and re-review with the SAME reviewers.
   Iterate. Cap it: after a few rounds without convergence (≈3–4), STOP, dispose,
   and tell the human the swarm could not reach consensus, with the outstanding
   blocking findings.

7. **Commit the approved tree — YOU do this, not the coder.** The coder only
   edits files; committing is the trusted plane's job (reliable, and it keeps
   git-write out of the untrusted VM). Attribute authorship to the coder:

   ```
   omni-sbx-swarm commit --swarm-id <id> \
     --message "<concise, imperative summary of the change>" \
     --author "swarm coder <coder@swarm.local>"
   ```

   `nothing-to-commit` means the coder produced no changes — investigate rather
   than publishing an empty branch.

8. **Publish the reviewed branch.** Push `task/<swarm>` and, per the publish mode,
   open a GitHub draft PR or leave it for a local merge:

   ```
   omni-sbx-swarm publish --swarm-id <id> \
     --title "<PR title>" --body "<what changed, how the swarm verified it>"
   ```

   Mode = `OMNI_SBX_PUBLISH_MODE` (`github` → draft PR; `local` → push only),
   overridable with `--pr` / `--no-pr`. It **never merges** and **never opens a
   non-draft PR**. It prints the PR URL (GitHub) or a "pushed branch — merge when
   ready" summary (local). If the task said keep it local / off GitHub, pass
   `--no-pr`.

9. **Notify the human and stop.** Report what `publish` printed, a one-line
   summary of what changed, and which reviewers checked what (e.g. "bug-hunter +
   security both APPROVED"). Make clear it is **for their review and merge** — you
   did not merge, and any PR is a draft.

10. **Tear down.** Once published (the branch lives on the remote / local repo),
    dispose the swarm to free its microVMs:

    ```
    omni-sbx-swarm dispose --swarm-id <id>
    ```

    Keep the worktree only if you still need it (`--keep-worktree`).

## Rules and cautions

- **You never write code and never merge.** Implementation is the coder's; merge
  is the human's. You plan-relay, coordinate, commit the approved tree, publish.
- **Planning is human-approved; execution is autonomous.** Never dispatch the
  coder until the human has explicitly approved the plan — bring the planner's
  questions up with your proposed answers and wait. "Run to completion" /
  "autonomous" directives govern only the post-approval execution phase; they
  never authorize skipping plan approval.
- **The planner plans; it does not vote.** Only reviewer roles produce a
  `VERDICT:`; consensus is over reviewers.
- **Nothing reaches the remote before consensus.** Commit/publish happen only
  after every reviewer APPROVES.
- **Trust the reviewers' `:ro` read, not the coder's claim.** A coder may report
  a change it didn't land; the reviewer reads the real tree.
- **One turn per `send`.** Each `send` blocks for that agent's whole turn (the
  first send to a role may wait out microVM provisioning — expected, not a hang).
  Don't fire a second `send` to the same role before the first returns.
- **Distinct swarms are independent.** For parallel workstreams, run this whole
  procedure once per task with a distinct `--swarm-id`. Agents in one swarm
  physically cannot see another swarm's worktree.
- **Always dispose** a swarm you finish or abandon, so its microVMs don't leak.
- **Watch for rate/usage limits — don't mistake them for bugs.** A turn that
  fails, returns an empty reply, or ends abruptly may be an LLM usage/rate limit
  that cut the turn short, NOT a task failure. `omni-sbx-swarm send` adds a
  `note` field (and a stderr line) when it detects this — if you see it, or a
  reply/error mentioning a usage/rate limit, quota, 429, or "overloaded":
  **tell the human plainly that it looks like a rate limit** (not a code
  problem), do NOT loop retrying, and leave the swarm registered so they can
  resume or dispose it. If YOUR OWN turn is ending early for the same reason, say
  so when you next run. On resume, if `omni-sbx-swarm list` shows a swarm you
  didn't finish, report it to the human and offer to continue or dispose it.
