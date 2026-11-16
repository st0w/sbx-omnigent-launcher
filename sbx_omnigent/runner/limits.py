"""Tuning constants: round caps, turn retries, polling, disk sizing."""

from __future__ import annotations

#: Default max review rounds before a gate gives up (blocked).
_DEFAULT_MAX_ROUNDS = 3

#: How many times a review that produced NO verdict is re-run before
#: its silence is finally treated as blocking. Cheap compared to the
#: alternative: a spurious loop-back costs a coder turn AND a full
#: review round, and lands the writer with narration instead of
#: findings.
_MAX_SILENT_REVIEW_ROUNDS = 2

#: Rough disk one agent microVM costs the host (its image snapshot +
#: writable overlay). An IDLE VM is ~1.2 GiB, which is what this used to
#: assume — but an agent that installs the toolchain it was told to
#: install puts that in its own overlay (rustup + a cargo registry +
#: llvm-cov measured ~3.6 GiB), and the preflight's whole job is to
#: refuse a run that cannot finish. So this is the WORKING figure, not
#: the idle one; ``disk.per_vm_gb`` tunes it per project (an interpreted
#: project pays far less). Under-reading it let runs start that filled
#: the host mid-module, which surfaces as guest filesystems remounting
#: read-only rather than as any legible error.
_VM_DISK_BYTES = 3_500_000_000

#: Headroom left for the host itself on top of the run's VMs. A machine
#: that ends a run at zero free is a machine that corrupted something.
_DISK_FLOOR_BYTES = 5_000_000_000

#: Rough disk one WRITER node's HOST worktree costs: its clone plus
#: whatever the agent builds inside it. This is the term a VM-only
#: estimate misses entirely, and it dominates for a compiled language —
#: two modules of a 12-VM cadre held ~19 GB, most of it worktrees, while
#: the check was still reporting ~23 GB as sufficient. Both defaults are
#: overridable per pipeline (``disk:``), since the runner cannot know
#: what a project builds.
#:
#: Was 2.0 GB, which measurement put BELOW the smallest writer this
#: project has ever produced (2.2 GB) and 13x under the largest
#: (26 GB) — see :class:`sbx_omnigent.pipeline.DiskSpec` for the full
#: calibration and the two things that multiply it.
_WORKTREE_DISK_BYTES = 4_000_000_000

#: agy (native-TUI) turn-delivery hardening, all launcher-side. The agy
#: bridge types a turn into the TUI and waits a few seconds for the
#: paste to RENDER in the composer before Enter, then fails the turn if
#: it did not (``session.status:failed error:null``). Two paste shapes
#: trip that check under microVM load: a MULTI-LINE paste does not
#: redraw within the window, and a LONG paste (observed live: ~1099
#: chars, vs a 744-char turn that rendered) collapses into a
#: "[Pasted text #N … chars]" PLACEHOLDER the bridge cannot verify.
#: Rather than tune around agy's exact limits, NOTHING substantial is
#: ever pasted: every agy turn is written to a file the agent reads
#: (:data:`_AGY_TASK_FILE`) and only a tiny one-line pointer is pasted
#: (:func:`_single_line` keeps even that pointer single-line). The file
#: also keeps the turn's original formatting, which reads better than a
#: flattened blob. Two cheap safety nets back the pointer up: WARM the
#: TUI at create (wait for its terminal to report running), and allow
#: one re-delivery if a first turn still fails before starting (the agy
#: bridge clears the composer before each paste, so it never doubles the
#: message).
#:
#: A session's FIRST turn gets one re-delivery when it fails or stalls
#: BEFORE the turn starts — the dropped-submit signature of a native TUI
#: still cold-starting. Not agy-specific: a Claude Code terminal is
#: created lazily at first-message time, so it cannot be pre-warmed the
#: way an agy terminal can, and a cold one that misses the executor's
#: 30s ready window fails the turn outright ("input prompt never
#: rendered in 195 polls, 195 empty captures. The message was not
#: delivered") — killing a whole run over a boot race. The retry lands
#: on a terminal that is warm by then.
_FIRST_TURN_MAX_RESUBMITS = 1
_FIRST_TURN_REDELIVER_DELAY_S = 0.0

#: When a REVIEWER's streamed reply carries no ``VERDICT:`` token, the
#: real verdict may only be in the SETTLED session — for EITHER harness:
#: agy's reply lags the turn (premature idle / reply lag — see
#: swarm_session), and a Claude review turn emits its verdict in a final
#: message that can sit behind intermediate tool-narration a mid-turn
#: quiescence idle mis-captured as the streamed reply (observed live in
#: mixed-models). So poll the SETTLED session until a verdict token
#: appears, bounded by a WALL-CLOCK deadline — NOT an attempt count: agy
#: fires premature idle beats, so each settle-wait returns in seconds
#: during a mid-work pause, and a fixed 3-attempt budget expired in
#: ~15-30s while a slow agy review under heavy load emitted its verdict
#: only at ~160s (observed live in full-cadre). 300s covered that, but
#: not a reviewer that EXECUTES what it reviews — installing Postgres
#: and running an instrumented coverage build ran well past it, and the
#: expiry then cost a full extra review round plus a re-driven coder. A
#: reviewer that really is dead is still capped: the deadline falls
#: through to :data:`_VERDICT_NUDGE`, and a reviewer still silent after
#: that is blocking (safe). The interval paces re-reads of the (cheap)
#: settled item store between polls.
#: SILENCE deadline, not a total budget. Reviewers were told to
#: EXECUTE what they review, so a review legitimately runs for tens of
#: minutes — installing a toolchain, building a workspace, running an
#: instrumented suite. Measuring that against a wall clock punished the
#: reviewers doing the most work: observed live, one agy reviewer
#: recorded NO verdict in three consecutive rounds while its reports
#: read "Running `cargo test -p discover-k8s -j 2` in the background
#: ... I will wait for it to complete." Each expiry cost a full extra
#: round plus a re-driven coder.
#:
#: So the clock restarts whenever the reviewer produces anything (see
#: :meth:`PipelineRunner._newest_item_id`). A reviewer emitting items
#: is not silent, whatever it is emitting.
_VERDICT_POLL_DEADLINE_S = 900.0
_VERDICT_POLL_INTERVAL_S = 5.0

#: Absolute ceiling on the verdict wait regardless of activity, so a
#: reviewer that narrates forever without ever voting still terminates
#: rather than holding its microVM to the turn budget.
_VERDICT_POLL_CEILING_S = 3600.0

#: Attempts at one reviewer's turn before the stage fails. A reviewer is
#: the safe thing to retry: read-only mount, nothing written, and no
#: verdict recorded — so a second attempt costs a turn and nothing
#: else. It is also cheaper than the first: the build cache is warm.
#:
#: Twice now a single reviewer has aborted a multi-hour campaign while
#: its three siblings approved: `topology-review-a-sec` and
#: `identities-review-a-sec`, both reported as the least useful sentence
#: the runner can produce — "failed: None". Both showed a CLEAN
#: (code=1000) runner websocket close on the server, and the second was
#: exactly 3600.07s after that session's last activity, which is a
#: timeout firing rather than a transient fault. It is NOT the
#: verdict ceiling above (that runs after the turn, in another code
#: path) nor the server's relay read timeout (45s); the true source
#: is still unproven. The retry is right regardless of which it is.
_REVIEW_TURN_ATTEMPTS = 2

#: Seconds to wait before re-driving a reviewer whose turn died.
#:
#: The retry above exists for a guest that died; it also catches the
#: case where the MODEL PROVIDER is down, and those want opposite
#: timing. A dead guest is ready to replace immediately. A provider
#: returning 529 is not, and two attempts seconds apart are one
#: attempt with extra steps. Observed live on `ingestion-m2-5`: both
#: attempts of a review stage failed inside a few minutes during an
#: acknowledged Anthropic incident, spending the whole budget while
#: the incident had another hour to run.
#:
#: Long enough to outlast a blip rather than to ride out an incident:
#: a minute and a half is the difference between two attempts and one
#: attempt with extra steps, and no wait short of abandoning the run
#: would have covered today's outage. Flat rather than exponential
#: because with two attempts there is nothing to grow. The cost when
#: the fault was NOT the provider is 90 idle seconds against a whole
#: stage.
_REVIEW_RETRY_BACKOFF_S = 90.0

#: Attempts at one writer's turn when the turn was LOST (see
#: :class:`LostTurn`) rather than failed by the agent. Observed on a
#: resume: both parallel writers failed within the same second with
#: `failed: None`, their runners gone, and the implement stage was lost
#: over work that was intact on disk (#32).
#:
#: Narrower than the reviewer retry on purpose. A writer's turn can run
#: the whole turn budget, so a turn that TIMED OUT, or where the agent
#: reported an error, is salvaged and not retried: a second attempt
#: would spend a second full budget on what will likely fail again.
_WRITER_TURN_ATTEMPTS = 2

#: Seconds to wait before re-driving a writer whose turn was lost. The
#: same reasoning as :data:`_REVIEW_RETRY_BACKOFF_S`: long enough to
#: outlast a provider blip, and paid after the dead guest is freed.
_WRITER_RETRY_BACKOFF_S = 90.0

#: Launches of one reviewer or writer whose VM never started a runner
#: (:class:`SwarmRunnerUnavailable`), counted apart from its turn
#: attempts. The server gives a launched runner 30 s to connect, and on
#: a loaded host three reviewer VMs missed that in one run: no turn ran,
#: but each failure spent the reviewer's one retry, and a candidate
#: lost its review to the host's load. Each retry waits the role's
#: backoff first, which is also time for the load to drain.
_LAUNCH_ATTEMPTS = 3

#: Most nodes of ONE stage driven at the same time.
#:
#: Independent nodes of a stage — the competing writers of a
#: ``parallel:`` block, the reviewers of a review stage — used to run
#: strictly one after another, so an increment serialized six reviewer
#: turns and two implementation turns that never needed ordering.
#:
#: The old reason for serializing was memory, and it did not survive
#: measurement: guests touched 0.9-1.3 GB each during a Rust build
#: against a 6 GB cap, on a 16 GB host.
#:
#: Kept as a cap rather than removed: a pipeline may declare more
#: parallel nodes than a host can hold, and the failure mode there is
#: a thrashing host rather than a clean error.
#:
#: It counts nodes BEING DRIVEN — the turns actually burning CPU and
#: memory — not microVMs in existence. A completed writer's guest is
#: deliberately kept up in case a review block re-drives it, so live
#: guests legitimately outnumber the cap; what must stay bounded is
#: how many are working at once.
#:
#: It is charged only to the units that drive a turn: a node's stage,
#: and each reviewer turn. Never to a container — a ``parallel:``
#: block of review stages drives nothing itself, and charging it too
#: would both waste the cap on threads doing no work and, nested
#: deeply enough, starve the leaves that do. Because only leaves
#: acquire, and a leaf never waits on another leaf's slot, the cap
#: cannot deadlock at any nesting depth.
_MAX_PARALLEL_NODES = 4
