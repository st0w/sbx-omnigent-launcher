"""Deterministic DAG runner for declarative pipelines.

Drives a parsed ``pipeline.yaml`` to completion on the
branch-as-artifact model (see ``docs/PIPELINES.md``): each
WRITER node works in its own isolated worktree + branch (never a
shared filesystem), reviewers and judges read a branch ``:ro``, and
the trusted host plane does all git.

The runner is the deterministic control plane: it walks the stages,
provisions each node's mount, drives one turn at a time, parses the
``VERDICT:`` / ``SELECT:`` lines, loops a review gate back to its
writer, and publishes the final branch. It replaces the prose
coordinator for a *defined* pipeline; the coordinator stays for
interactive work.

A pipeline with a ``task:`` runs to completion. Without one, the runner
PROVISIONS the topology (each node's isolated worktree + microVM
session) and hands back the role→session bindings for a human (or the
coordinator) to drive — the VMs are left up.

Node kinds (derived from a stage):
- **reader** — a read-only producer (e.g. a planner): mounts a tree
  ``:ro`` and emits text (a design) used as downstream context.
- **writer** (``write: true``) — an isolated ``rw`` worktree cut from an
  upstream branch (inheritance) or the base; committed to its branch.
- **review** (``gate:``/``on_block:``) — one or more reviewers mount the
  target writer's branch ``:ro`` and vote; a block loops back to the
  writer with the findings, up to a round cap.
- **judge** (``selects:``) — mounts a compare tree of competing branches
  and picks a winner.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import fnmatch
import functools
import re
import shlex
import shutil
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path, PurePosixPath

import click

from sbx_omnigent import (
    agy,
    codex,
    disk_metrics,
    orphans,
    pane,
    pipeline,
    readback,
    verify,
)
from sbx_omnigent.swarm import (
    _launch_args_for,
    credential_kind_for,
    mount_sentinel,
)
from sbx_omnigent.swarm_session import SwarmSessionClient, SwarmSessionError
from sbx_omnigent.worktrees import (
    WorktreeManager,
    _repo_name,
    github_slug,
)

#: Terminal VERDICT / SELECT markers agents end their reply with. The
#: token after ``VERDICT:`` is captured broadly and normalized below:
#: models routinely paraphrase (``PASS`` / ``LGTM`` / ``FAIL``) instead
#: of the exact ``APPROVED`` / ``BLOCKING`` the templates request, and
#: an over-strict match silently reads an approval as a block.
_VERDICT_RE = re.compile(r'VERDICT:\s*([A-Za-z]+)', re.IGNORECASE)
_SELECT_RE = re.compile(r'SELECT:\s*([A-Za-z0-9._-]+)')

#: The planner's ordered chunk list (see :func:`parse_subtasks`): a line
#: reading exactly ``SUBTASKS:`` followed by ``- [<id>] <goal>`` items
#: (a ``1. [<id>] …`` numbered variant is accepted).
_SUBTASKS_HEADER_RE = re.compile(r'^[ \t]*SUBTASKS:[ \t]*$', re.MULTILINE)

#: A per-module planner's cross-module decisions (see
#: :func:`parse_decisions`): a line reading ``DECISIONS FOR LATER
#: MODULES:`` followed by ``- <decision>`` items. Heading/emphasis
#: marks around it are tolerated — models routinely wrap a requested
#: header in ``##`` or ``**``, and losing the whole block to a stray
#: asterisk would silently drop the decisions it names.
_DECISIONS_HEADER_RE = re.compile(
    r'^[ \t]*#{0,6}[ \t]*\*{0,2}[ \t]*'
    r'DECISIONS FOR LATER MODULES[ \t]*:?[ \t]*\*{0,2}[ \t]*$',
    re.MULTILINE | re.IGNORECASE,
)
#: A markdown thematic break closing a section. It has to be checked
#: BEFORE the item pattern, because `---` matches "bullet `-`, text
#: `--`" and was lifted into the committed ledger as a decision reading
#: `--`.
_THEMATIC_BREAK_RE = re.compile(r'^[ \t]*(?:-{3,}|\*{3,}|_{3,})[ \t]*$')

_DECISION_ITEM_RE = re.compile(
    r'^[ \t]*(?:[-*\u2022]|\d+[.)])[ \t]*(?P<text>.+?)[ \t]*$'
)

#: A reviewer's NON-BLOCKING findings, lifted the same way. Items reuse
#: :data:`_DECISION_ITEM_RE` — one list syntax across every
#: marker is one less thing for a reviewer to get wrong.
_FINDINGS_HEADER_RE = re.compile(
    r'^[ \t]*#{0,6}[ \t]*\*{0,2}[ \t]*'
    r'FINDINGS[ \t]*:?[ \t]*\*{0,2}[ \t]*$',
    re.MULTILINE | re.IGNORECASE,
)

#: The three kinds a reviewer sorts its non-blocking observations into.
#: One blob was the wrong shape: :data:`_FINDINGS_ASK` solicits three
#: different things, and only the first is a defect. Sorting them where
#: they are WRITTEN is free; sorting them afterwards means reading every
#: item to work out which kind it was, which is the cost this split
#: exists to remove. Same lifting protocol as FINDINGS, so a reviewer
#: has one list syntax to remember across every marker.
_DEFECTS_HEADER_RE = re.compile(
    r'^[ \t]*#{0,6}[ \t]*\*{0,2}[ \t]*'
    r'DEFECTS[ \t]*:?[ \t]*\*{0,2}[ \t]*$',
    re.MULTILINE | re.IGNORECASE,
)
_LATER_HEADER_RE = re.compile(
    r'^[ \t]*#{0,6}[ \t]*\*{0,2}[ \t]*'
    r'LATER[- _]?INCREMENT[ \t]*:?[ \t]*\*{0,2}[ \t]*$',
    re.MULTILINE | re.IGNORECASE,
)
_PREMISES_HEADER_RE = re.compile(
    r'^[ \t]*#{0,6}[ \t]*\*{0,2}[ \t]*'
    r'PREMISES[ \t]*:?[ \t]*\*{0,2}[ \t]*$',
    re.MULTILINE | re.IGNORECASE,
)

#: Verdict-token synonyms, normalized to the two canonical values.
_APPROVE_WORDS = frozenset({
    'APPROVED', 'APPROVE', 'PASS', 'PASSED', 'PASSES',
    'ACCEPT', 'ACCEPTED', 'LGTM', 'OK',
})
_BLOCK_WORDS = frozenset({
    'BLOCKING', 'BLOCK', 'BLOCKED', 'FAIL', 'FAILED', 'FAILS',
    'REJECT', 'REJECTED',
})

#: Default max review rounds before a gate gives up (blocked).
_DEFAULT_MAX_ROUNDS = 3

#: How many times a review that produced NO verdict is re-run before
#: its silence is finally treated as blocking. Cheap compared to the
#: alternative: a spurious loop-back costs a coder turn AND a full
#: review round, and lands the writer with narration instead of
#: findings.
_MAX_SILENT_REVIEW_ROUNDS = 2

#: Glob patterns for files that are BUILD OUTPUT, not implementation.
#: A writer whose whole diff is these did not do the work — observed
#: live: a coder reported success having changed only ``Cargo.lock``
#: (its crate roots still held the test writer's "intentionally empty"
#: placeholders), and TWO reviewers approved the branch anyway. Config
#: ``generated:`` replaces this set.
#: What a ``tests_only`` stage may change when the pipeline declares
#: no ``test_paths`` of its own. Deliberately only TEST code: dependency
#: manifests are handled by :data:`_GENERATED_GLOBS` and the ordinary
#: guarded-file review, because a test needing a new dev-dependency must
#: edit ``Cargo.toml`` and every greenfield module in this project's
#: history did exactly that.
_TEST_PATH_GLOBS: tuple[str, ...] = (
    'tests/**',
    '**/tests/**',
    '*_test.*',
    'test_*.*',
    '*.test.*',
    '*_spec.*',
    'conftest.py',
)

#: Dependency manifests a ``tests_only`` stage may also change. A test
#: needing a new dev-dependency MUST declare it — every greenfield
#: module here did, and replaying the gate over them showed
#: two (m1, m5) that would have been blocked for doing nothing else at
#: all. Not folded into :data:`_TEST_PATH_GLOBS` (a manifest is not test
#: code) nor :data:`_GENERATED_GLOBS` (nor build output).
#:
#: The compensating control already exists: manifests are in this
#: project's ``guarded:`` list, so any edit is named to the reviewers as
#: a required review item. Allowing them here does not make them
#: invisible, it just stops them halting the stage.
_DEPENDENCY_MANIFEST_GLOBS: tuple[str, ...] = (
    'Cargo.toml',
    'pyproject.toml',
    'setup.cfg',
    'package.json',
    'go.mod',
    'Gemfile',
    'build.gradle',
    'build.gradle.kts',
    'pom.xml',
)

_GENERATED_GLOBS: tuple[str, ...] = (
    '*.lock',
    '*.sum',
    'package-lock.json',
    'npm-shrinkwrap.json',
    'pnpm-lock.yaml',
)

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
#: A plan of record must look like a DOCUMENT, not a chat reply. An agy
#: planner that has ALREADY written the plan tends to answer the
#: consolidation turn with an acknowledgement instead of re-emitting it
#: — observed live: a 371-char "the plan is released and frozen" blurb
#: silently replaced a 12,412-char design, and every builder downstream
#: was handed the blurb.
#:
#: Two DIFFERENT bars, because the two decisions differ:
#:
#: * Accepting the consolidation turn's own reply uses the ABSOLUTE
#:   floor alone. Scaling it to the session's longest reply rejects the
#:   real thing, since a consolidated plan is legitimately SHORTER than
#:   a verbose first draft — observed live: a genuine 5,165-char "Final
#:   Approved Design Plan" was discarded for a 10,276-char PRE-approval
#:   draft, because an earlier 15,088-char draft had raised the bar to
#:   7,544. Substituting a draft for the approved plan is silent, and
#:   worse than passing a wordy ack, which the builders visibly reject.
#: * CHOOSING A FALLBACK, once the reply is already known not to be a
#:   plan, scales with the richest reply: mid-session chatter ("I have
#:   designed the plan and…") clears the floor easily while being
#:   nothing like the design, so the floor alone would hand the builders
#:   a status update.
_PLAN_MIN_CHARS = 1500
#: The relative substance bar this once used is RETIRED: it existed
#: to keep mid-session chatter from being promoted when the
#: consolidation reply was rejected, and the structural check below
#: does that better — chatter fails on shape, at any length.

#: Length alone is not enough, and a live failure proved it: the design
#: of record for an entire Kubernetes provider was a 1,774-char RECAP
#: ("has been finalized and APPROVED … the plan phase is complete") that
#: cleared the 1500-char floor by 274 characters. The TDD writer and
#: both implementers built the module from it. A status update will
#: always beat a character count, so the floor is now a FAST REJECT and
#: the decision is structural — does the reply contain the things
#: ``templates/planner.md`` demands? (TASKS.md #29)
#:
#: Calibrated against the six real plans this pipeline has published:
#: the recap names 2 distinct files and uses 3 acknowledgement phrases;
#: every genuine plan names 7-14 files and uses none.
#: How much of a reply the recap patterns are applied to.
#:
#: An acknowledgement LEADS with "the plan is complete"; a design plan
#: may use the phrase once, in passing, deep in the body. Unanchored,
#: the patterns vetoed a 127,683-character plan for a single substring
#: at line 907 of 1,003 -- work item 12's "a written statement of why
#: fewer is complete" -- and the approved-text fallback could not help,
#: because the approved text carried the same sentence.
#:
#: The recap this check was built for is 1,774 characters, so it lies
#: entirely inside this window and is still caught. A padded
#: acknowledgement is not rescued either: it still fails the file,
#: structure and signal checks.
_PLAN_RECAP_WINDOW = 2000

_PLAN_RECAP_RE = re.compile(
    r'\b(has been (?:finalized|approved)'
    r'|is (?:now )?(?:complete|finalized)'
    r'|approved by the user'
    r'|plan phase .{0,20}(?:is )?complete'
    r'|final summary of the approved'
    r'|the plan is (?:approved|frozen|released))\b',
    re.I,
)

#: A plan names the files it changes — the FIRST thing planner.md asks
#: for. Distinct paths, so one file repeated does not count as many.
#:
#: The bar is ONE, not "several". It was three, calibrated on six large
#: Rust modules whose plans name 7-14 files — and that immediately
#: rejected a real, human-APPROVED plan for a one-file task, which is
#: the expensive mistake: halting a good run is worse than passing a bad
#: plan, because the reviewers and the verify gate still stand behind a
#: bad plan while a halt stops everything. How many files a plan touches
#: is a property of the TASK, not of whether it is a plan.
_PLAN_FILE_RE = re.compile(
    r'(?:^|[\s`(])([\w./-]+\.(?:rs|py|ts|tsx|js|jsx|go|java|rb|c|h|cpp'
    r'|cs|php|swift|kt|scala|sh|sql|toml|yaml|yml|json|ini|cfg|md))\b',
    re.I | re.M,
)
_PLAN_MIN_FILES = 1

#: The shape of the six things planner.md demands. Deliberately loose
#: word-level signals rather than a semantic read: this must hold for
#: any planner model, and a brittle check that only passes one model's
#: house style is worse than the length test it replaces.
_PLAN_SIGNALS: dict[str, re.Pattern[str]] = {
    'files it changes': _PLAN_FILE_RE,
    'interfaces (takes/returns/responsibility)': re.compile(
        r'\b(takes?|returns?|accepts?|responsibilit(?:y|ies)'
        r'|signature|parameters?)\b',
        re.I,
    ),
    'ordered steps': re.compile(r'^\s*(?:\d+[.)]|step\s+\d+)\s', re.I | re.M),
    'decomposition into pieces': re.compile(
        r'^\s{0,3}(?:#{2,6}\s|[-*]\s)', re.M
    ),
    'edge cases / failure modes': re.compile(
        r'\b(edge case|failure mode|error (?:case|behaviou?r|handling)'
        r'|invalid|malformed|must (?:fail|reject)|raises?)\b',
        re.I,
    ),
    'a test strategy': re.compile(
        r'\b(test strateg|unit test|integration test|test author'
        r'|fixtures?|must verify|test case)\b',
        re.I,
    ),
}
#: How many of the six must show. Not all six: a genuine plan may cover
#: an item in words these patterns do not match, and rejecting a real
#: design is the more expensive mistake. Three separates every published
#: plan from the recap when paired with the file and recap checks.
_PLAN_MIN_SIGNALS = 3

#: A heading, a bullet, or a numbered step: any line that puts the reply
#: into PARTS rather than paragraphs.
_PLAN_STRUCTURE_RE = re.compile(
    r'^\s{0,3}(?:#{1,6}\s|[-*]\s|\d+[.)]\s)', re.M
)

#: How many such lines a design plan must carry.
#:
#: This is the check the keyword census could not be: an
#: acknowledgement ABOUT a plan reuses the plan's vocabulary — it names
#: the same files, says "frozen", says "the tests stage" — and so it
#: scores on word-level signals no matter how they are tuned. What it
#: does NOT do is decompose. planner.md asks for files, interfaces,
#: ordered steps, independently buildable pieces, edge cases and a test
#: strategy; a reply covering those is in parts, and a reply about them
#: is prose.
#:
#: Measured on this project's published plans, which is where the
#: margin comes from:
#:
#:     ingestion-m1  (the acknowledgement)     0 structural lines
#:     ingestion-m0                          103
#:     ingestion-m0b                         109
#:     ingestion-m1  (the real plan)         465
#:
#: Eight sits three orders of magnitude below the smallest real plan and
#: above a recap that has none, so it rejects the failure without
#: coming near a genuine design.
_PLAN_MIN_STRUCTURE = 8


#: How deep to scan a planner session for the plan of record.
#:
#: Matches the approval gate's own window (`_PLAN_APPROVAL_TAIL`). They
#: read the SAME session for the same document, and the default 60 is
#: narrower — so the gate could find a 128,861-character plan that the
#: selection read, moments later, could not. Live on `ingestion-m2-4`.
_PLAN_REPLY_TAIL = 200


#: A cross-reference to a numbered section: ``§11``, ``section 4.2``.
_PLAN_SECTION_REF_RE = re.compile(r'(?:§|\bsection\s+)(\d+(?:\.\d+)*)', re.I)

#: A numbered section the document DEFINES: ``## 11. Migration``.
_PLAN_SECTION_HEADING_RE = re.compile(
    r'^\s{0,3}#{1,6}\s+\**(\d+(?:\.\d+)*)[.)\s]', re.M
)


def plan_shape_failures(text: str | None) -> list[str]:
    """
    Why *text* does not read like a design plan (``[]`` if it does).

    Structural, not semantic: it asks whether the reply has the SHAPE
    ``templates/planner.md`` demands, because the thing being guarded
    against — an acknowledgement standing in for the design — is a
    shape failure. See :data:`_PLAN_RECAP_RE`.

    :param text: A planner reply, or ``None``.
    :returns: Human-readable failures, most decisive first. Empty means
        it passes.
    """
    if not text or not text.strip():
        return ['it is empty']
    why: list[str] = []
    if len(text) < _PLAN_MIN_CHARS:
        why.append(
            f'it is {len(text)} characters, under the {_PLAN_MIN_CHARS} '
            f'floor'
        )
    recap = _PLAN_RECAP_RE.findall(text[:_PLAN_RECAP_WINDOW])
    if recap:
        why.append(
            f'it reads as an acknowledgement, not a design '
            f'(says {", ".join(sorted({r.lower() for r in recap}))!s})'
        )
    files = {m.lower() for m in _PLAN_FILE_RE.findall(text)}
    if len(files) < _PLAN_MIN_FILES:
        why.append(
            'it names no files; a plan says which files change'
        )
    structure = len(_PLAN_STRUCTURE_RE.findall(text))
    if structure < _PLAN_MIN_STRUCTURE:
        why.append(
            f'it is {structure} heading/bullet/step line(s), under the '
            f'{_PLAN_MIN_STRUCTURE} floor; a design plan is in parts, '
            f'an acknowledgement about one is prose'
        )
    missing = [
        name for name, rx in _PLAN_SIGNALS.items() if not rx.search(text)
    ]
    if len(_PLAN_SIGNALS) - len(missing) < _PLAN_MIN_SIGNALS:
        why.append(f'it is missing: {"; ".join(missing)}')
    # A plan CONTAINS its sections; a summary REFERS to them. Only a
    # document that resolves NOTHING it cites is describing another
    # one -- a real plan citing the brief's numbering alongside its own
    # still resolves its own, and a plan that never writes "§" is
    # unaffected.
    refs = {m.split('.')[0] for m in _PLAN_SECTION_REF_RE.findall(text)}
    if refs:
        defined = {
            m.split('.')[0]
            for m in _PLAN_SECTION_HEADING_RE.findall(text)
        }
        if not refs & defined:
            listed = ', '.join(f'§{n}' for n in sorted(refs, key=int))
            why.append(
                f'it points at {len(refs)} section(s) it does not '
                f'contain ({listed}); a plan carries its sections, a '
                f'summary of one refers to them'
            )
    return why

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

#: A session's FIRST turn gets one re-delivery when it fails or stalls
#: BEFORE the turn starts — the dropped-submit signature of a native TUI
#: still cold-starting. Not agy-specific: a Claude Code terminal is
#: created lazily at first-message time, so it cannot be pre-warmed the
#: way an agy terminal can, and a cold one that misses the executor's
#: 30s ready window fails the turn outright ("input prompt never
#: rendered in 195 polls, 195 empty captures. The message was not
#: delivered") — killing a whole run over a boot race. The retry lands
#: on a terminal that is warm by then.
#: Schema version of the run-state file (see
#: :meth:`PipelineRunner._save_state`). Bumped when the payload's shape
#: changes, so a reader can refuse state it does not understand rather
#: than resume from a misread one.
RUN_STATE_VERSION = 1

#: Heading each verification step gets in a pull-request body, and
#: whether its output is collapsed. A demonstration is the thing the
#: reviewer came to SEE, so it is shown open; a full test log is
#: corroboration, so it is folded away.
_PR_STEPS = {
    'tests': ('How it was proven', True),
    'demo': ('Proof it works', False),
}

#: Per-step cap on captured output embedded in a pull-request body.
#: GitHub allows ~65k for the whole body; two steps at this size leave
#: room for the prose and never risk a rejected create.
_PR_STEP_CHARS = 12_000

_FIRST_TURN_MAX_RESUBMITS = 1
_FIRST_TURN_REDELIVER_DELAY_S = 0.0

#: Filename a node worktree carries an agy turn in (read by the agent,
#: never pasted). Added to the clone's ``.git/info/exclude`` so it is
#: neither committed nor seen by the settle-wait's ``git status``.
_AGY_TASK_FILE = 'OMNI_TASK.md'

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

#: Asked of a reviewer that finished its turn without stating a verdict.
#: Since reviewers were told to EXECUTE what they review, they install
#: toolchains and run instrumented builds — work that outlives the turn,
#: leaving the last message mid-narration ("Running code coverage check
#: with cargo-llvm-cov. I will wait for it to complete."). Reading that
#: as BLOCKING re-drives a coder and re-runs the whole review over a
#: reviewer that simply had not finished speaking, and hands the coder
#: narration in place of findings. One cheap turn is worth far more than
#: that, and BLOCKING remains the answer if it still will not say.
_VERDICT_NUDGE = (
    'You ended your turn without a verdict line. Reply with NOTHING but '
    'your verdict now, on one line: `VERDICT: APPROVED` if the change '
    'meets the contract and you verified it, or `VERDICT: BLOCKING` '
    'followed by the specific findings — including "I could not finish '
    'verifying", if that is what happened. Do not resume reviewing; just '
    'state where you got to.'
)


#: Demanded of a writer being re-driven to fix a block or a failed gate.
#: A loop-back very often lands in a FRESH microVM — after a --resume
#: the writer's own session is gone and :meth:`_ensure_writer_session`
#: re-attaches a new one — and a fresh VM has no project toolchain.
#: Given findings and nothing else, a coder replied: "I could not
#: verify by running the suite — cargo/rustc/rustup are all absent from
#: this environment. The change is a self-contained, logically-verified
#: fix",
#: and the round accepted it. Nothing downstream catches that either:
#: :meth:`_require_implementation` proves only that the diff is
#: non-empty, never that any of it compiled. A fix nobody compiled is a
#: guess wearing a commit message, and it is handed straight to a judge
#: as if it were a tested change.
_FIX_MUST_VERIFY = (
    'Before you report back you MUST build the project and run its '
    'tests, and your reply MUST state the command you ran and its '
    'result. If the toolchain is missing, INSTALL it — the environment '
    'note below says how and the egress for it is already open, so '
    '"cargo/rustc are not available in this environment" is a step you '
    'skipped, not an outcome. If you genuinely cannot get the suite to '
    'run, say so plainly and stop: report the fix as UNVERIFIED rather '
    'than finishing quietly, so nobody mistakes it for a tested change.'
)


#: Added to every loop-back so a writer cannot "fix" a finding by
#: deleting the check that produced it. Observed live: a reviewer told
#: m2-impl-b to drop `GetGroup` from its permissions manifest, but the
#: frozen test asserts that set exactly — "no more, no less" — so
#: obeying necessarily meant editing the frozen test, which the writer
#: duly did. The finding was simply WRONG, and the next reviewer said
#: the right disposition was "escalation as a test/contract dispute,
#: not editing the manifest and the frozen test". A blocking finding is
#: handed over as a mandate today, so a writer needs an explicit way to
#: refuse one: silently removing an assertion is far worse than leaving
#: a finding open, because the check that would have caught the next
#: regression goes with it.
#:
#: WIDENED after a live build shipped a silenced security gate. The rule
#: was scoped to TESTS, so a writer closed a blocking supply-chain
#: finding without touching one: it added three advisory ids to the
#: audit tool's ignore list and deleted the comment block documenting
#: why the one pre-existing entry was justified. Every gate went green.
#: Nothing in the pipeline noticed, because nothing had told the writer
#: that a gate's own configuration is a check, or that removing one
#: quietly is not an option.
_FIX_NO_WEAKENING = (
    'Nothing that CHECKS this work may be weakened in order to close a '
    'finding. That covers the test suite — including the assertions and '
    'expected values inside a test — and equally the configuration of '
    'any gate, linter, auditor, or CI workflow, and every suppression, '
    'ignore, allow-list, or exception entry those read. Adding an entry '
    'that makes a failing check stop reporting is NOT a fix. It removes '
    'the thing that would have caught the next regression, and it is '
    'worse than leaving the finding open, because the next person sees '
    'a green gate and believes it. If a finding can only be satisfied '
    'that way, then it is either wrong or it is a contract dispute, and '
    'either way it is NOT yours to resolve: say so plainly, name the '
    'check and what it conflicts with, leave it alone, and address the '
    'findings that do not require it. Reporting a finding as disputed '
    'is a correct and expected outcome. Obeying it is not.\n\nIf you '
    'change any such file for a legitimate reason — the check itself is '
    'genuinely wrong, or fixing it IS the task — you MUST say so '
    'explicitly in your reply: name the file, quote what you changed, '
    'and state why it is a real fix and not a silencer. Deleting an '
    'existing comment or rationale from one of those files is never '
    'incidental; if you remove one, say what it said and why it no '
    'longer applies. An undisclosed change to a check is treated as a '
    'failed turn.'
)


#: Told to every agent EXCEPT the planner, whose interactive approval
#: gate is the design. Nobody else has a human on the other end.
#:
#: Observed live: a coder hit a genuine conflict — an invariant barred
#: the CLI from naming platform SDK types, while the frozen provider
#: exposed no constructor that avoided it — resolved it correctly with
#: an additive constructor, verified the whole suite green, and then
#: opened a modal asking permission for the change it had ALREADY made.
#: Nothing surfaced that question to the console, so the turn sat until
#: someone happened to look at the web UI. Worse, the reviewer could
#: not see the question either: it blocked five consecutive rounds on
#: the change being "un-escalated", each round costing a toolchain
#: install and a full rebuild, while the escalation sat in a modal
#: neither of them could reach.
#:
#: So the rule is not merely "do not ask" — it is "your REPLY is the
#: only channel that reaches anyone". A decision stated there is seen
#: by the human, the reviewers, and the record; anything else is lost.
#: Appended beside :data:`_UNATTENDED`. The VM's disposability is the
#: load-bearing fact: an agent that knows the machine is thrown away
#: has no reason to tidy it, and tidying is what gets turns killed.
#:
#: Claude Code hard-blocks destructive commands on critical paths, and
#: says so in terms that leave no way out: "This command would remove a
#: critical system directory. This requires explicit approval and
#: CANNOT be auto-allowed by permission rules." No permission mode, no
#: allowlist, no settings key suppresses it — so prevention is the only
#: lever there is. Observed live on gcp-custom-roles-1 (TASKS.md #40): a
#: coder built a swapfile to survive a large Rust build, removed it
#: afterwards, and lost the turn to a modal nobody could answer.
_DISPOSABLE_VM = (
    'This VM is DISPOSABLE — it is destroyed when the run ends, and '
    'nothing outside your worktree survives or is inspected. So do not '
    'clean up after yourself outside it: a temp file, cache, package '
    'or swap file you created is not worth removing, and removing one '
    'is pure downside. A destructive command on an absolute path '
    'outside your worktree opens a harness confirmation that NO '
    'permission mode can bypass — nobody is there to answer it, and '
    'the turn dies on the prompt. If you are short of memory or disk, '
    'do not engineer around it: say so in your reply, which is the '
    'channel that reaches a human.'
)

#: Most nodes of ONE stage driven at the same time.
#:
#: Independent nodes of a stage — the competing writers of a
#: ``parallel:`` block, the reviewers of a review stage — used to run
#: strictly one after another, so an increment serialized six reviewer
#: turns and two implementation turns that never needed ordering.
#:
#: The old reason for serializing was memory, and it did not survive
#: measurement: guests touched 0.9-1.3 GB each during a Rust build
#: against a 6 GB cap, on a 16 GB host. The warm build cache shortens
#: the peak further — a seeded workspace rebuild is 16s where a
#: from-clean one is 121s.
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

#: Asked of every reviewer, every round. The NON-BLOCKING list is the
#: lossy set: a blocking finding is acted on inside the round, while one
#: raised beside an APPROVED verdict is archived and never read again.
#: Observed live — a reviewer approved a change and noted, correctly,
#: "no pagination … truncated results would be silently dropped at
#: enterprise scale", against a candidate that went on to lose the
#: judge's pick. Nothing then asked whether the winner paginated either.
#:
#: The reasoning behind an approval is asked for too, because a wrong
#: premise under an APPROVE is invisible today: one reviewer approved a
#: reordering because `build_providers` "uses infallible constructors",
#: which is false — two of the three return `Result` — and was
#: right only by luck.
_FINDINGS_ASK = (
    '\n\nSeparately from your verdict, end your reply with everything '
    'you noticed that is NOT blocking, sorted into these three blocks. '
    'Write each header on its own line followed by `- ` items, and omit '
    'a block you have nothing for.\n\n'
    '`DEFECTS:` — things you believe are wrong in THIS code, that you '
    'are not blocking on.\n'
    '`LATER-INCREMENT:` — things a LATER module owns. Name the module '
    'if you can. These are routed to the decisions ledger that every '
    'later planner reads, so this is how you tell the module that will '
    'act on it, not a human who has to work out who you meant.\n'
    '`PREMISES:` — assumptions your APPROVAL rests on that a reader '
    'should check. Most hold; the point is that a wrong one becomes '
    'visible instead of invisible. State the premise, not a worry.\n\n'
    'These are RECORDED, not acted on: nothing you raise obliges anyone '
    'to change code now, and nothing you leave out will be seen again. '
    'Sort honestly rather than putting everything in `DEFECTS:` — a '
    "defect list padded with other people's work is how a real defect "
    'gets lost. If you are unsure which block an item belongs in, '
    '`DEFECTS:` is the safe choice.'
)

#: What a reviewer must NOT re-run, and why it is not its job.
#:
#: A reviewer is handed the whole task, success criteria included, and
#: told to check the branch against it. When one of those criteria is
#: "the project gate passes", a conscientious reviewer runs the gate —
#: and on this project that is clippy, the full suite, AND a complete
#: coverage-instrumented rebuild, after `cargo install cargo-llvm-cov`
#: compiles from source. Six reviewers per increment did exactly that,
#: and the verification sandbox then ran the same command a seventh
#: time on the winner, and CI an eighth on the pull request.
#:
#: Nothing about the sixth run is more true than the first. The gate is
#: measured once, afterwards, in a clean sandbox on the branch that
#: actually ships — which is the only measurement that means anything.
#: What a reviewer uniquely provides is judgement about whether the code
#: does what it claims, and it cannot spend an hour on that if it spends
#: the hour recompiling.
_REVIEW_NOT_THE_GATE = (
    '\n\nRUN THE TESTS, NOT THE COVERAGE GATE. Execute the suite '
    'covering what you are reviewing — that is what stops an empty '
    'implementation being approved, and it is not optional. But do NOT '
    "run the project's coverage/acceptance gate, and do not install a "
    'coverage tool to do it. If a success criterion in the task above '
    'says the gate passes, THAT CRITERION IS NOT YOURS: the gate is run '
    'once after the judge, in a clean sandbox, on the branch that '
    'actually ships, and again by CI on the pull request. Re-running it '
    'here proves nothing that those runs do not, and it is the single '
    'most expensive thing you can do with your turn — on this workspace '
    'it is a full instrumented rebuild, and your turn is not unlimited. '
    'Spend it on what only you can do: reading the code and deciding '
    'whether it is right. If the tests themselves will not run, that IS '
    'your finding — block and say so.'
)

_UNATTENDED = (
    'You are running UNATTENDED in an automated pipeline. No human is '
    'watching this turn and there is no interactive channel: a '
    'question, a confirmation prompt, or a request for approval will '
    'not be seen, and it stalls the pipeline until the turn times out. '
    'When requirements conflict, resolve it yourself — the frozen '
    'tests and the stated invariants are the tie-breakers — then STATE '
    'THE DECISION AND YOUR REASONING IN YOUR REPLY and carry on. If it '
    'genuinely cannot be resolved that way, say so in your reply, '
    'label it DISPUTED, and stop. Your reply is the only channel that '
    'reaches a human OR a reviewer: a decision you do not write there '
    'is invisible to both, and a reviewer who cannot see why you did '
    'something is right to block on it.'
)


class PipelineRunError(Exception):
    """A pipeline run failed (setup, a turn, or a git step)."""


class _Blocked(Exception):
    """Internal: a review gate never reached consensus; stop."""

    def __init__(self, stage_id: str, rounds: int) -> None:
        super().__init__(stage_id)
        self.stage_id = stage_id
        self.rounds = rounds


def _single_line(text: str) -> str:
    """
    Collapse *text* to a single line (whitespace runs → one space).

    An agy turn is typed into its TUI and the bridge waits a few seconds
    for the paste to render before submitting; a multi-line paste does
    not redraw in time under load and the turn is dropped unsubmitted.
    Flattening a turn to one line makes it render instantly. Harmless
    for an LLM prompt — newlines carry no meaning the model needs here.

    :param text: The turn message.
    :returns: The message with every whitespace run replaced by a single
        space and the ends stripped.
    """
    return ' '.join(text.split())


def parse_verdict(text: str | None) -> str | None:
    """
    Return the LAST recognized ``VERDICT:`` value, normalized, or None.

    Each token after a ``VERDICT:`` marker is mapped to the canonical
    ``'APPROVED'`` / ``'BLOCKING'`` via :data:`_APPROVE_WORDS` /
    :data:`_BLOCK_WORDS`, so a paraphrased ``VERDICT: PASS`` reads as an
    approval. The last *recognized* token wins; an unknown token (e.g.
    a stray ``verdict:`` mention) is ignored rather than clobbering a
    real verdict. No recognized verdict yields ``None`` — the caller
    treats that as blocking (safe).

    :param text: An agent reply.
    :returns: ``'APPROVED'`` / ``'BLOCKING'``, or ``None`` when absent
        or unrecognized.
    """
    result: str | None = None
    for token in _VERDICT_RE.findall(text or ''):
        upper = token.upper()
        if upper in _APPROVE_WORDS:
            result = 'APPROVED'
        elif upper in _BLOCK_WORDS:
            result = 'BLOCKING'
    return result


def _named_candidate(line: str, candidates: list[str]) -> str | None:
    """
    The one candidate id *line* names, or ``None`` if not exactly one.

    Ids are matched as literal substrings, so decoration around them —
    backticks, asterisks, a trailing full stop — does not matter. When
    one matched id is a strict substring of another (``impl-a`` inside
    ``m0-impl-a``), only the longest survives; anything still ambiguous
    is refused rather than guessed at.

    :param line: One line of a judge reply.
    :param candidates: The writer node ids being judged.
    :returns: The named candidate, or ``None``.
    """
    hits = [c for c in candidates if c and c in line]
    widest = [
        c for c in hits
        if not any(other != c and c in other for other in hits)
    ]
    return widest[0] if len(widest) == 1 else None


def parse_select(
    text: str | None, candidates: list[str] | None = None
) -> str | None:
    """
    The candidate a judge selected in *text*, or ``None``.

    Two tiers, both requiring the judge to have written ``SELECT``:

    1. the exact ``SELECT: <id>`` the templates ask for;
    2. failing that, and only when *candidates* is given, the last line
       carrying ``SELECT`` that names exactly one of them literally.

    Tier 2 exists because a judge that genuinely decided should not
    lose its decision to punctuation. Observed live on
    ``core-contracts-pick``: gemini produced 2,062 characters of real
    comparison and ended with ``SELECT core-contracts-impl-a`` — no
    colon — so tier 1 found nothing, the vote was discarded and the
    first candidate won by default (TASKS.md #44). ``SELECT: `impl-a` ``
    and ``SELECT: **impl-a**`` failed the same way.

    It cannot false-positive: tier 2 needs BOTH the marker and a
    literal id the runner already knows, so prose like "I will SELECT
    the safer candidate" names nothing and matches nothing. Widening
    tier 1's own pattern instead — making the colon optional — would
    have captured "the" from that sentence.

    :param text: A judge reply.
    :param candidates: The writer node ids being judged. Omit to get
        tier 1 only, unvalidated (the pre-existing behaviour).
    :returns: The selected stage id, or ``None`` when absent.
    """
    found = _SELECT_RE.findall(text or '')
    if not candidates:
        return found[-1] if found else None
    # With the candidate list in hand, a tier-1 capture that is not one
    # of them is not a decision — it is the regex eating the next word
    # of a sentence ('SELECT: whichever is safer' yields 'whichever').
    # Fall through to tier 2 rather than returning it, so a real choice
    # further down the reply is still found.
    valid = [f for f in found if f in candidates]
    if valid:
        return valid[-1]
    for line in reversed((text or '').splitlines()):
        if 'SELECT' not in line:
            continue
        named = _named_candidate(line, candidates)
        if named is not None:
            return named
    return None


def parse_subtasks(text: str | None) -> list[pipeline.Subtask]:
    """
    Parse the planner's ordered ``SUBTASKS:`` block into chunks.

    The consolidation turn asks the approved planner to end its plan of
    record with a block titled exactly ``SUBTASKS:`` followed by one
    ``- [<id>] <goal>`` line per independently-buildable increment
    (a numbered ``1. [<id>] …`` variant is accepted too). The LAST such
    header wins; blank lines inside the list are tolerated; the first
    non-blank line that is not an item ends the block (prose after the
    list is ignored). Ids are lowercased to ``[a-z0-9-]`` and de-duped
    so they are safe to namespace branches with. A missing/empty block
    yields ``[]`` — the runner then falls back to a single pass.

    :param text: The planner's consolidated plan of record.
    :returns: The ordered chunks, or ``[]`` when none are declared.
    """
    if not text:
        return []
    matches = list(_SUBTASKS_HEADER_RE.finditer(text))
    if not matches:
        return []
    header = matches[-1]  # the LAST header is the authoritative list
    block: list[str] = []
    for line in text[header.end():].splitlines():
        if not line.strip():
            continue  # tolerate blank lines within the list
        if pipeline._SUBTASK_ITEM_RE.match(line) is None:
            break  # prose after the list ends the block
        block.append(line)
    return pipeline.parse_subtask_items('\n'.join(block))


def select_plan_of_record(
    consolidated: str | None,
    replies: list[str],
    approved: str | None = None,
) -> str | None:
    """
    Pick the planner's real design document, not an acknowledgement.

    The consolidation turn asks the approved planner to re-emit its plan
    as a standalone document, and *consolidated* is what it said. A
    compliant planner returns the document and it is used as-is. A
    conversational one (agy, typically) answers "the plan is approved
    and frozen" — a few hundred characters that would then be handed to
    every builder AS the plan. So when the consolidation reply is not
    substantive, fall back to the LAST substantive thing the planner
    said instead.

    Acceptance is STRUCTURAL (:func:`plan_shape_failures`), not a
    length test. A length test is what let a 1,774-char recap become
    the design of record for an entire module: it cleared the
    1500-char floor by 274 characters and the guard concluded the turn
    had complied. A status update can always beat a character count.
    Latest-not-longest matters when falling back: a planner's post-Q&A
    revision supersedes its first draft and is often shorter.

    *approved* is the text the HUMAN approved, and it is always a
    candidate. The caller already holds it, so a SECOND read of the
    session that comes back short or empty must never outrank it.
    Live on `ingestion-m2-4`: the gate returned 128,861 characters of
    approved plan, the consolidation turn answered with an
    acknowledgement, `read_assistant_replies` returned `[]` — it scans a
    60-item tail where the approval gate scans 200, and swallows a read
    error as empty — and the run aborted while that plan sat unused in
    the caller's own local. The session was disposed at teardown.

    :param consolidated: The consolidation turn's reply.
    :param replies: Every assistant message in the planner session,
        oldest first.
    :param approved: The text the human approved, if the caller has it.
    :returns: The plan of record — the consolidation reply when it is
        one, else the LATEST candidate that is.
    :raises PipelineRunError: When no reply in the session is a design
        plan. Deliberately fatal: a plan that does not exist used to be
        indistinguishable from one that does, and the run continued
        into eight agents' worth of work either way.
    """
    if not plan_shape_failures(consolidated):
        return consolidated  # the turn complied
    # Latest, not longest: a post-Q&A revision supersedes the first
    # draft and is often SHORTER, so picking the longest resurrects a
    # pre-approval draft. Shape decides which replies are eligible;
    # order decides between them.
    passing = [
        r for r in replies if r and r.strip() and not plan_shape_failures(r)
    ]
    if passing:
        return passing[-1]
    # Last resort, and ONLY that: the text the human approved. It is
    # captured before the consolidation turn, so it must not outrank a
    # later reply that is a plan -- appending it to the pool instead
    # made an earlier draft win over a post-Q&A revision.
    if approved and not plan_shape_failures(approved):
        return approved
    why = plan_shape_failures(consolidated)
    raise PipelineRunError(
        'the planner produced no design plan. Its consolidation reply '
        f'is not one ({"; ".join(why)}), and no earlier reply in the '
        'session is either.\n\n'
        'Refusing to continue: the plan of record is handed verbatim to '
        'the test author and to every implementer, so a run that '
        'proceeds here builds an entire module from an acknowledgement '
        '— which is exactly what happened once, silently, and the '
        "module's real design was lost with the session.\n\n"
        'Read the planning session, then re-run the planner. '
        "templates/planner.md lists what the reply must contain."
    )


#: A decisions-ledger module heading: ``## [m1] Ingestion core``.
#:
#: Anchored on ``## [`` so it cannot match the referrals section's
#: ``## Raised in [m1] ...`` subheads. Those are recorded as explicitly
#: NOT binding, and reading them back as decisions would promote a
#: reviewer's passing note into a constraint no later module may alter.
_LEDGER_MODULE_RE = re.compile(r'^##[ \t]+\[([^\]]+)\]', re.M)


#: A stage claiming its OWN CONTRACT is impossible, not that the code is
#: wrong. Every role prompt ends "label it DISPUTED, and stop", so the
#: bare word is in almost every report as the instruction echoed
#: back — the marker must be the word FOLLOWED BY a claim.
#:
#: Measured on four campaigns, and the separation is total:
#:
#:     [m0b]  shipped     16 occurrences, 16 echoes,  0 claims
#:     [m1]   shipped     18 occurrences, 18 echoes,  0 claims
#:     [m2] 1 forfeited   18 occurrences,  0 echoes,  9 claims
#:     [m2] 2 forfeited   12 occurrences,  0 echoes, 10 claims
#:
#: Halting on the bare word would have killed both shipped builds.
#: Halting on a claim stops both forfeits in their first round.
_DISPUTE_RE = re.compile(
    r'^[ \t]*(?:[-*\u2022]|\d+[.)])?[ \t]*\*{0,2}DISPUTED\*{0,2}'
    r'[ \t]*[:\u2014\u2013-][ \t]*(?P<claim>\S.*?)[ \t]*$',
    re.MULTILINE,
)


def parse_disputes(text: str | None) -> tuple[str, ...]:
    """
    Claims that a stage's own contract cannot be satisfied.

    Distinct from a BLOCKING verdict, which says the code is wrong
    and is answered by re-driving the writer. A dispute says the
    CONTRACT is wrong — a test no implementation can pass, two stages
    disagreeing — and re-driving the writer cannot resolve it, because
    the writer is not the party who can change it.

    That distinction was expensive to learn. Two campaigns raised 9 and
    10 disputes, every one correct, and both spent their whole review
    budget re-driving writers who could not act on them before
    forfeiting every candidate.

    :param text: A stage's reply.
    :returns: The claim after each marker, in order; empty when the
        reply only echoes the instruction that asks for the marker.
    """
    if not text:
        return ()
    return tuple(
        m.group('claim').strip() for m in _DISPUTE_RE.finditer(text)
    )


def parse_decisions_doc(text: str | None) -> list[tuple[str, str]]:
    """
    Read a committed decisions ledger back into ``(module, text)``.

    The inverse of :meth:`PipelineRunner._decisions_doc`, and the reason
    the ledger can survive a run at all: ``_decisions`` is otherwise
    populated only from run state, so a campaign that builds ONE module
    per run starts empty every time and carries nothing forward
    (TASKS.md #75).

    Stops at the second top-level heading. Everything after it is the
    referrals section, which the writer marks as not binding — see
    :data:`_LEDGER_MODULE_RE`.

    An item is a bullet or numbered line, the same forms
    :func:`parse_decisions` accepts. Continuation lines are NOT joined
    on, and that is deliberate rather than incidental: the ledger is
    human-edited, and an indented note beneath an item — the annotation
    the file exists to hold — would otherwise be swallowed into the
    decision above it.

    :param text: The committed ledger, or ``None`` when none exists.
    :returns: ``(module id, decision)`` pairs in document order. Empty
        for a missing or heading-less document, which is the normal
        first-module case rather than a failure.
    """
    if not text:
        return []
    body = text
    headings = [
        m.start() for m in re.finditer(r'^#[ \t]', body, re.M)
    ]
    if len(headings) > 1:
        body = body[: headings[1]]
    out: list[tuple[str, str]] = []
    module: str | None = None
    for line in body.splitlines():
        heading = _LEDGER_MODULE_RE.match(line)
        if heading is not None:
            module = heading.group(1).strip()
            continue
        if module is None:
            continue
        item = _DECISION_ITEM_RE.match(line)
        if item is not None:
            value = item.group('text').strip()
            if value:
                out.append((module, value))
    return out


def parse_decisions(text: str | None) -> list[str]:
    """
    Parse a planner's ``DECISIONS FOR LATER MODULES:`` block.

    Each module is planned in its own session, so a decision reached
    with the human while designing module N is invisible to module N+1's
    planner unless it is carried forward deliberately. The per-module
    consolidation turn asks for the decisions that BIND later modules
    under that header; this lifts them out so the runner can thread them
    into every later planner and commit them as one reviewable ledger.

    The LAST such header wins; blank lines inside the list are
    tolerated; the first non-blank line that is not an item ends the
    block. A missing/empty block yields ``[]`` — a module that settled
    nothing cross-cutting is the normal case, not a failure.

    :param text: The module's consolidated plan of record.
    :returns: The decisions, in order, or ``[]`` when none are declared.
    """
    return list(_lift_marked_items(text, _DECISIONS_HEADER_RE))


#: What a verifier may conclude about one finding, and whether that
#: conclusion keeps it out of the tracker.
#:
#: Only FILED dispositions become issues. The rest are recorded with
#: their reason and summarised for the module, so the judgement is
#: auditable — the point is not to make findings disappear, it is to
#: stop a human re-deriving a conclusion an agent already reached with
#: the code in front of it.
DISPOSITION_FILED = 'reproduces'
DISPOSITION_ABSENT = 'absent'
DISPOSITION_RECORDED = 'recorded'
DISPOSITION_DECIDED = 'decided'
DISPOSITION_DUPLICATE = 'duplicate'

#: Spellings normalized to the five above, so a verifier that writes
#: "already documented" is not silently misread as an unknown verdict
#: and re-filed.
_DISPOSITION_WORDS = {
    'reproduces': DISPOSITION_FILED,
    'reproduce': DISPOSITION_FILED,
    'reproduced': DISPOSITION_FILED,
    'confirmed': DISPOSITION_FILED,
    'real': DISPOSITION_FILED,
    'absent': DISPOSITION_ABSENT,
    'stale': DISPOSITION_ABSENT,
    'gone': DISPOSITION_ABSENT,
    'not-reproduced': DISPOSITION_ABSENT,
    'recorded': DISPOSITION_RECORDED,
    'documented': DISPOSITION_RECORDED,
    'decided': DISPOSITION_DECIDED,
    'ruled': DISPOSITION_DECIDED,
    'duplicate': DISPOSITION_DUPLICATE,
    'dupe': DISPOSITION_DUPLICATE,
}

_DISPOSITIONS_HEADER_RE = re.compile(
    r'^[ \t]*#{0,6}[ \t]*\*{0,2}[ \t]*'
    r'DISPOSITIONS[ \t]*:?[ \t]*\*{0,2}[ \t]*$',
    re.MULTILINE | re.IGNORECASE,
)

#: ``- 3: recorded — sdk_ast.py:120 already says so``. The number is the
#: position the finding was PRESENTED at, not its positional id: a
#: verifier echoing `topology/impl-a/bugs/r4#3` back correctly is a
#: transcription task, and one that gets it wrong silently re-files.
#:
#: Everything after the number is captured as ONE phrase rather than a
#: verdict token and a reason. A verifier writes "already documented in
#: models.py" and "duplicate of 1", not the tidy `verdict — reason` a
#: stricter pattern assumes; the word is then found inside the phrase
#: (see :func:`_verdict_in`), so a natural sentence is read rather than
#: dropped.
_DISPOSITION_ITEM_RE = re.compile(
    r'^[ \t]*(?:[-*\u2022]|\d+[.)])?[ \t]*'
    r'(?P<index>\d+)[ \t]*[:.)][ \t]*'
    r'(?P<phrase>.+)$'
)

#: Separators a reason may follow its verdict with, stripped so the
#: recorded reason does not start with punctuation.
_REASON_LEAD_RE = re.compile(r'^[ \t]*[\u2014\u2013:,-]*[ \t]*')


def _verdict_in(phrase: str) -> tuple[str, str] | None:
    """
    The first known verdict word in *phrase*, and what follows it.

    Word-by-word rather than a prefix match, because a verifier's
    conclusion is a sentence: "already documented in models.py" carries
    its verdict in the second word. Scanning in order means the first
    conclusion stated wins, which is the one a reader would take too.

    :param phrase: Everything the verifier wrote after the number.
    :returns: ``(verdict, reason)``, or ``None`` when no known word
        appears — which files the finding, per
        :func:`parse_dispositions`.
    """
    for match in re.finditer(r'[A-Za-z][A-Za-z-]*', phrase):
        verdict = _DISPOSITION_WORDS.get(match.group(0).lower())
        if verdict is not None:
            rest = phrase[match.end():]
            return verdict, _REASON_LEAD_RE.sub('', rest).strip()
    return None


@dataclass(frozen=True)
class Disposition:
    """
    One verifier's conclusion about one finding.

    :param verdict: One of the five ``DISPOSITION_*`` values.
    :param reason: What the verifier checked, in its own words. Recorded
        whatever the verdict — a finding kept OUT of the tracker needs
        its reason more than one that goes in, because nobody will see
        the finding again to judge the call.
    """

    verdict: str
    reason: str = ''

    @property
    def files(self) -> bool:
        """Whether this conclusion still becomes an issue."""
        return self.verdict == DISPOSITION_FILED


def parse_dispositions(text: str | None) -> dict[int, Disposition]:
    """
    Parse a verifier's ``DISPOSITIONS:`` block.

    FAIL OPEN, and deliberately so: an index this returns nothing for is
    filed exactly as it is today. A verifier that answers nothing, dies,
    or writes an unrecognised verdict must cost a wasted triage, never a
    lost finding — the tracker is where a non-blocking finding survives,
    and silently dropping one is the single outcome this whole change
    must not produce.

    :param text: The verifier's reply.
    :returns: Presented-position to conclusion. Missing keys mean "no
        conclusion", which the caller reads as "file it".
    """
    out: dict[int, Disposition] = {}
    if not text:
        return out
    matches = list(_DISPOSITIONS_HEADER_RE.finditer(text))
    if not matches:
        return out
    for line in text[matches[-1].end():].splitlines():
        if not line.strip():
            continue
        item = _DISPOSITION_ITEM_RE.match(line)
        if item is None:
            break
        found = _verdict_in(item.group('phrase'))
        if found is None:
            # No known word is not a licence to guess. Leaving the index
            # absent files the finding, and CONTINUING matters: one
            # unreadable line must not discard the conclusions below it.
            continue
        verdict, reason = found
        out[int(item.group('index'))] = Disposition(
            verdict=verdict, reason=reason
        )
    return out


def _strings(value: object) -> tuple[str, ...]:
    """
    The string items of a persisted list, dropping anything else.

    :param value: A value read from run state, of any shape.
    :returns: Its string items, in order, or ``()``.
    """
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _lift_marked_items(
    text: str | None, header: re.Pattern[str]
) -> tuple[str, ...]:
    """
    Items under the LAST occurrence of *header*.

    The one protocol every marker in this module shares: the last header
    wins, blank lines inside the list are tolerated, and the first
    non-blank line that is not an item ends the block. Extracted so a
    new marker cannot accidentally get a different one — three of them
    were about to be written by hand.

    :param text: The reply to read.
    :param header: The marker's compiled header pattern.
    :returns: The items, in order, or ``()`` when the marker is absent.
    """
    if not text:
        return ()
    matches = list(header.finditer(text))
    if not matches:
        return ()
    out: list[str] = []
    for line in text[matches[-1].end():].splitlines():
        if not line.strip():
            continue  # tolerate blank lines within the list
        if _THEMATIC_BREAK_RE.match(line):
            break  # a rule closes the section, it is not an item
        item = _DECISION_ITEM_RE.match(line)
        if item is None:
            break  # prose after the list ends the block
        value = item.group('text').strip()
        if value:
            out.append(value)
    return tuple(out)


@dataclass(frozen=True)
class FindingSections:
    """
    One reviewer's non-blocking output, sorted by what it IS.

    :param defects: Things the reviewer believes are wrong here.
    :param later_increment: Things a later module owns. These are routed
        to the decisions ledger, which every later planner reads,
        rather than to the issue tracker where they wait for a human to
        notice they were addressed to someone else.
    :param premises: Assumptions an APPROVAL rests on. Mostly they hold;
        the value is that a wrong one becomes visible instead of
        invisible, so they are verified rather than triaged.
    """

    defects: tuple[str, ...] = ()
    later_increment: tuple[str, ...] = ()
    premises: tuple[str, ...] = ()

    @property
    def all(self) -> tuple[str, ...]:
        """Every item, defects first — the pre-split list."""
        return self.defects + self.later_increment + self.premises

    @classmethod
    def of(cls, text: str | None) -> FindingSections:
        """
        Sort one reply's non-blocking observations.

        A bare ``FINDINGS:`` block — what every reviewer wrote before
        the split, and what one ignoring the instruction still
        writes — is read as DEFECTS. Deliberately the loudest
        bucket: an uncategorized item gets the most attention,
        never the least, so a reviewer that does not follow the
        new protocol loses nothing.

        :param text: The reviewer's full report.
        :returns: The sorted sections; all-empty when it raised nothing.
        """
        return cls(
            defects=(
                _lift_marked_items(text, _DEFECTS_HEADER_RE)
                + _lift_marked_items(text, _FINDINGS_HEADER_RE)
            ),
            later_increment=_lift_marked_items(text, _LATER_HEADER_RE),
            premises=_lift_marked_items(text, _PREMISES_HEADER_RE),
        )


def parse_findings(text: str | None) -> tuple[str, ...]:
    """
    Parse a reviewer's ``FINDINGS:`` block.

    These are the NON-BLOCKING observations — the lossy set. A blocking
    finding is acted on inside the round; one raised alongside an
    APPROVED verdict is archived to the run directory and never read
    again, so a real defect noticed by a careful reviewer simply
    evaporates (TASKS.md #10). Lifting them out under a marker is how
    they reach the ledger.

    Same protocol as :func:`parse_decisions`, deliberately: the LAST
    header wins, blank lines inside the list are tolerated, and
    the first non-blank line that is not an item ends the block.

    :param text: The reviewer's full report.
    :returns: The findings in order, or ``()`` when none are declared.
        Empty is the NORMAL case — a reviewer with nothing to raise is
        not a failure, and the caller decides what to do about it.
    """
    return _lift_marked_items(text, _FINDINGS_HEADER_RE)


@dataclass(frozen=True)
class JudgePick:
    """
    One judge's choice between competing implementations.

    The competing-writers design is the most expensive thing this
    pipeline does — it doubles the implementation stage AND its review
    stages — and whether that cost is repaid depends entirely on whether
    both candidates actually win sometimes. Until this record existed
    that question could not be answered from anything the pipeline
    produced: ``SELECT:`` appeared in no published artifact, and the
    only way to establish that the second writer had ever won was to
    notice its name among the commit authors in merged ``main`` — an
    accident of a metadata leak (TASKS.md #33), not instrumentation.

    It also makes the JUDGE auditable. A judge that systematically
    favours one family is what would make the whole race theatre, and
    that is invisible without a series of these.

    :param chunk: Module id this pick belongs to (``None`` outside a
        campaign), so a module's pull request carries only its own.
    :param stage: The judge stage's id.
    :param candidates: The competing node ids it compared, in order.
    :param selected: The node id that won and will publish.
    :param stated: What the judge's own ``SELECT:`` line named, or
        ``None`` when it never stated one. Kept SEPARATE from
        *selected* because the runner falls back to the first candidate
        when the line is missing or names a non-candidate — a silent
        substitution that would otherwise read as a real decision.
    :param reasoning: The judge's reply, which carries its per-candidate
        assessment. The session is disposed moments later.
    :param retained: ``(node, bundle-path)`` for each candidate that did
        NOT win and was preserved, so the record says where the losing
        implementation actually lives.
    """

    chunk: str | None
    stage: str
    candidates: tuple[str, ...]
    selected: str
    stated: str | None
    reasoning: str = ''
    retained: tuple[tuple[str, str], ...] = ()

    @property
    def honored(self) -> bool:
        """Whether the published winner is the one the judge named."""
        return self.stated == self.selected

    def as_dict(self) -> dict:
        """:returns: A JSON-serializable form for the run state."""
        return {
            'chunk': self.chunk,
            'stage': self.stage,
            'candidates': list(self.candidates),
            'selected': self.selected,
            'stated': self.stated,
            'reasoning': self.reasoning,
            'retained': [list(pair) for pair in self.retained],
        }

    @classmethod
    def from_dict(cls, raw: dict) -> JudgePick | None:
        """
        Rebuild a pick from run state, or ``None`` if unusable.

        Defensive like :meth:`ReviewRecord.from_dict`: a resumed run
        must not die over a malformed bookkeeping entry.

        :param raw: One entry from the persisted ``judge_picks`` list.
        :returns: The pick, or ``None``.
        """
        stage, selected = raw.get('stage'), raw.get('selected')
        if not isinstance(stage, str) or not isinstance(selected, str):
            return None
        cands = raw.get('candidates')
        stated = raw.get('stated')
        chunk = raw.get('chunk')
        why = raw.get('reasoning')
        return cls(
            chunk=chunk if isinstance(chunk, str) else None,
            stage=stage,
            candidates=tuple(c for c in (cands or []) if isinstance(c, str)),
            selected=selected,
            stated=stated if isinstance(stated, str) else None,
            reasoning=why if isinstance(why, str) else '',
            retained=tuple(
                (pair[0], pair[1])
                for pair in raw.get('retained') or []
                if isinstance(pair, (list, tuple)) and len(pair) == 2
            ),
        )


@dataclass(frozen=True)
class ReviewRecord:
    """
    One reviewer's vote and the report behind it.

    Captured because the reviewer does not survive its own vote: its
    microVM is disposed as soon as the votes are in, and disposing a
    session deletes the transcript with it. Nothing else in the run
    keeps the reasoning — an APPROVED verdict used to be reduced to a
    single token and the report dropped on the spot, which is the
    reasoning behind a decision to SHIP.

    :param chunk: Module id this vote belongs to (``None`` outside a
        campaign), so a module's pull request carries only its own.
    :param stage: The review stage's id.
    :param reviewer: The agent name that voted.
    :param round_no: Which review round this was (1-based).
    :param verdict: ``'APPROVED'``, ``'BLOCKING'``, or ``None`` when the
        reviewer never stated one (treated as blocking).
    :param turns: ``(role, text)`` message pairs, oldest first.
    """

    chunk: str | None
    stage: str
    reviewer: str
    round_no: int
    verdict: str | None
    turns: tuple[tuple[str, str], ...] = ()
    #: The reviewer's NON-BLOCKING findings, lifted from its
    #: ``FINDINGS:`` block. Empty when it raised none, or when it did
    #: not use the marker.
    #:
    #: Kept as the UNION of the three sections below, which it predates
    #: — every existing consumer reads this one, and a resumed run's
    #: state carries it. The sections are what routing reads.
    findings: tuple[str, ...] = ()
    #: The union's three parts, in the reviewer's own words. Additive
    #: with empty defaults, so state written before the split restores
    #: as "uncategorized" rather than refusing to load — which is why
    #: RUN_STATE_VERSION does not move (see :meth:`from_dict`).
    defects: tuple[str, ...] = ()
    later_increment: tuple[str, ...] = ()
    premises: tuple[str, ...] = ()
    #: The writer node this vote was cast against. Two candidates are
    #: reviewed per chunk, so a finding is meaningless without it.
    target: str | None = None

    def filed_findings(self) -> list[tuple[int, str]]:
        """
        The findings that belong in the tracker, with their ids' index.

        Everything except ``later_increment``, which is ROUTED to the
        decisions ledger instead: those items are addressed to a module
        that has not been planned yet, and an issue is the wrong shape
        for them — it waits for a human to notice it was meant for
        somebody else.

        The index is the position in :attr:`findings`, NOT in the
        returned list. :func:`finding_id` is positional, so numbering a
        filtered list would shift every id after the first routed item
        and re-file findings already closed. Filtering must never
        renumber.

        :returns: ``(index, text)`` pairs, in :attr:`findings` order.
        """
        routed = set(self.later_increment)
        return [
            (index, text)
            for index, text in enumerate(self.findings, start=1)
            if text not in routed
        ]

    @property
    def label(self) -> str:
        """``"<stage>-<reviewer>"`` — the node label used in logs."""
        return f'{self.stage}-{self.reviewer}'

    @property
    def slug(self) -> str:
        """
        A filename stem unique per reviewer and round.

        No module prefix: in a campaign the runner has already
        namespaced the stage id (``m1-review``), so the label carries
        the module. :attr:`chunk` exists for the pull-request filter,
        not for naming.
        """
        return f'{self.label}-r{self.round_no}'

    def as_dict(self) -> dict:
        """:returns: A JSON-serializable form for the run state."""
        return {
            'chunk': self.chunk,
            'stage': self.stage,
            'reviewer': self.reviewer,
            'round_no': self.round_no,
            'verdict': self.verdict,
            'turns': [list(pair) for pair in self.turns],
            'findings': list(self.findings),
            'defects': list(self.defects),
            'later_increment': list(self.later_increment),
            'premises': list(self.premises),
            'target': self.target,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> ReviewRecord | None:
        """
        Rebuild a record from run state, or ``None`` if unusable.

        Defensive by design: a resumed run must not die over a malformed
        bookkeeping entry.

        :param raw: One entry from the persisted ``reviews`` list.
        :returns: The record, or ``None``.
        """
        stage, reviewer = raw.get('stage'), raw.get('reviewer')
        if not isinstance(stage, str) or not isinstance(reviewer, str):
            return None
        turns = tuple(
            (pair[0], pair[1])
            for pair in raw.get('turns') or []
            if isinstance(pair, (list, tuple)) and len(pair) == 2
        )
        chunk = raw.get('chunk')
        verdict = raw.get('verdict')
        target = raw.get('target')
        return cls(
            chunk=chunk if isinstance(chunk, str) else None,
            stage=stage,
            reviewer=reviewer,
            round_no=int(raw.get('round_no') or 1),
            verdict=verdict if isinstance(verdict, str) else None,
            turns=turns,
            findings=_strings(raw.get('findings')),
            # Absent from state written before the split. An older
            # record restores with its findings uncategorized, which is
            # the pre-split behaviour exactly, so an in-flight run stays
            # resumable and RUN_STATE_VERSION does not move.
            defects=_strings(raw.get('defects')),
            later_increment=_strings(raw.get('later_increment')),
            premises=_strings(raw.get('premises')),
            target=target if isinstance(target, str) else None,
        )


def render_judge_decision(
    picks: list[JudgePick], *, title: str
) -> str:
    """
    Render the judge's choices as a durable record.

    Deliberately leads with the machine-readable summary line: the value
    of this artifact is the SERIES, not any single entry, so the first
    thing a reader (or a grep across modules) meets is one line each.

    :param picks: The picks to record, in stage order.
    :param title: Heading for the document.
    :returns: The markdown document, or ``''`` when there is nothing.
    """
    if not picks:
        return ''
    parts = [
        f'# {title}',
        '',
        'Which implementation won, and why. Two writers build the same '
        'frozen contract on isolated branches and a judge picks one; '
        'the loser is not published, so without this the choice leaves '
        'no trace at all.',
        '',
        '| stage | candidates | selected | judge said |',
        '| --- | --- | --- | --- |',
    ]
    for pick in picks:
        stated = f'`{pick.stated}`' if pick.stated else '_none stated_'
        cands = ', '.join(f'`{c}`' for c in pick.candidates)
        parts.append(
            f'| `{pick.stage}` | {cands} '
            f'| `{pick.selected}` | {stated} |'
        )
    for pick in picks:
        parts += [
            '', '---', '',
            f'## `{pick.stage}` — selected `{pick.selected}`',
        ]
        if not pick.honored:
            # The runner substitutes the first candidate when the judge
            # never stated a usable SELECT. That is a fallback, not a
            # decision, and a series that silently counted it as one
            # would misreport the race.
            parts += [
                '',
                "> **Not the judge's stated choice.** It named "
                f'`{pick.stated or "nothing"}`, which is not one of the '
                f'candidates, so the runner fell back to the first one. '
                f'Read this as an ABSENT decision, not a preference.',
            ]
        parts += ['', pick.reasoning.strip() or '_The judge said nothing._']
        if pick.retained:
            parts += [
                '',
                '### The implementation(s) not chosen',
                '',
                'Kept as git bundles — they are complete, reviewed and '
                'test-passing, and this is the only copy. Restore one '
                'with `git fetch <bundle> '
                'refs/heads/<branch>:refs/heads/<name>`.',
                '',
            ]
            parts += [f'- `{node}` — `{path}`' for node, path in pick.retained]
    return '\n'.join(parts) + '\n'


def render_turn_capture(
    turns: list[tuple[str, str]], *, title: str, reason: str
) -> str:
    """
    Render a session's conversation as a diagnostic record.

    Kept for the turns nobody can otherwise inspect. Disposing a
    session DELETES it, and teardown disposes everything moments after
    a run fails — so a writer that hung or failed leaves nothing behind
    but the timeout line. That has now happened twice: a refactor turn
    the human wanted to read and could not, and a coder turn that
    burned a full hour whose cause is still unknown because its
    transcript was gone before anyone could look.

    Reviewers already get this treatment (see
    :func:`render_review_records`); this is the same guarantee for
    every other turn.

    :param turns: ``(role, text)`` pairs oldest-first — messages only,
        never tool calls or their output.
    :param title: Heading for the document.
    :param reason: Why the turn was captured.
    :returns: The markdown document.
    """
    # Neutral on purpose. A `user` message is the RUNNER's instruction
    # in a writer's turn but the HUMAN's own words in a planning
    # session, and the item store cannot tell them apart — so naming
    # either one would credit a human's design decision to the
    # orchestrator, or vice versa, in the record kept precisely to
    # settle who decided what.
    label = {'user': 'User', 'assistant': 'Agent'}
    parts = [
        f'# {title}',
        '',
        f'**Captured because:** {reason}',
        '',
        'The session behind this turn is deleted when its microVM is '
        'disposed, which happens moments after a run ends. This is the '
        'only surviving record of what the agent was doing.',
        '',
        'Messages only — no tool calls and no tool output.',
    ]
    if not turns:
        parts += ['', '_The session could not be read._']
    for turn, (role, text) in enumerate(turns, start=1):
        parts += [
            '',
            '---',
            '',
            f'## {turn}. {label.get(role, role)}',
            '',
            text.strip(),
        ]
    return '\n'.join(parts) + '\n'


def finding_id(rec: ReviewRecord, index: int) -> str:
    """
    A stable, positional id for one finding.

    Positional rather than content-derived, deliberately. The same issue
    is often raised by both reviewers and re-raised each round, and
    keying on text would silently merge those — but a re-raise after a
    round of work is a DIFFERENT event, and dropping it is worse than
    carrying the duplicate. Duplicates here are provenance; a human
    merges them, the runner never does (TASKS.md #10).

    :param rec: The vote the finding came from.
    :param index: 1-based position within that reviewer's list.
    :returns: e.g. ``topology/topology-impl-a/bugs/r4#3``.
    """
    return (
        f'{rec.chunk or "-"}/{rec.target or "-"}/'
        f'{rec.reviewer}/r{rec.round_no}#{index}'
    )


#: Label applied to every filed finding, so triage state can live on
#: the issue rather than in a `Status:` line somebody has to parse. A
#: repo without it makes `gh` refuse the whole call, so
#: :meth:`WorktreeManager.create_issue` retries once without it.
_FINDING_LABEL = 'finding'


def finding_marker(ident: str) -> str:
    """
    The machine-readable identity carried by a filed finding.

    An HTML comment: exact to match on, and invisible in the rendered
    issue, so a human editing the body cannot accidentally break dedup
    by reformatting around it.

    :param ident: The positional id from :func:`finding_id`.
    :returns: The marker line.
    """
    return f'<!-- finding-id: {ident} -->'


def _issue_title_text(finding: str, limit: int = 88) -> str:
    """
    A finding's first line, fit to be an issue title.

    Reviewers write markdown, and a title opening with a literal ``**``
    reads as noise in a tracker list. Emphasis is stripped, the leading
    SENTENCE is preferred over a hard character cut — reviewers tend to
    lead with the claim and follow with the detail — and a cut that has
    to happen lands on a word boundary rather than mid-token.

    :param finding: The finding text.
    :param limit: Longest title to produce.
    :returns: The title text.
    """
    text = ' '.join(finding.split()).replace('**', '').replace('`', '')
    head, sep, _rest = text.partition('. ')
    if sep and len(head) <= limit:
        return head
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(' ', 1)[0].rstrip(' ,;:')
    return f'{cut}…'


def render_finding_issue(
    rec: ReviewRecord,
    finding: str,
    index: int,
    *,
    run_id: str,
    report_doc: str | None = None,
) -> tuple[str, str]:
    """
    One non-blocking finding as a GitHub issue.

    :param rec: The vote it came from.
    :param finding: The finding text.
    :param index: 1-based position in that reviewer's list.
    :param run_id: The campaign that raised it.
    :param report_doc: Repo path of the full reviewer report.
    :returns: ``(title, body)``.
    """
    ident = finding_id(rec, index)
    head = _issue_title_text(finding)
    where = f'[{rec.chunk}] ' if rec.chunk else ''
    raised = f'`{rec.target}`' if rec.target else 'an unknown candidate'
    body = [
        finding.strip(),
        '',
        '---',
        '',
        f'**Raised against:** {raised} by `{rec.reviewer}`, review round '
        f'{rec.round_no}, alongside a verdict of '
        f'{rec.verdict or "none stated"}.',
    ]
    if report_doc:
        body.append(f'**Full report:** `{report_doc}`')
    body += [
        f'**Campaign:** `{run_id}`',
        '',
        'Raised by a reviewer as NON-BLOCKING and filed automatically. '
        'Nothing has been acted on — a blocking finding is handled '
        'inside its review round, and this is the set that used to be '
        'archived and never read again. Triage it as you would any '
        'other issue; closing it is how the pipeline learns not to '
        'raise it again.',
        '',
        finding_marker(ident),
    ]
    return f'{where}{head}', '\n'.join(body)


def render_findings_ledger(
    records: list[ReviewRecord],
    *,
    title: str,
    existing: str | None,
    report_doc: str | None = None,
) -> str | None:
    """
    Append new findings to the ledger, never rewriting what is there.

    APPEND-ONLY is the whole contract. This is a HUMAN-EDITED artifact —
    a person annotates status and reasoning on it, across modules and
    across runs — so *existing* is carried through byte for byte and new
    entries are added after it. A renderer that rebuilt the document
    each run would clobber exactly the reasoning it exists to hold.

    Entries are sections rather than table rows, which is a deliberate
    departure from the sketch in TASKS.md #10. A table only stays valid
    if every future append lands immediately after the last row, so the
    first person to write a paragraph under it breaks every later
    append. Sections append correctly after anything, which is what
    "human-editable" has to mean if the constraint above is real.

    :param records: Every vote for this chunk, in order cast.
    :param title: Heading, used only when creating the document.
    :param existing: The ledger already on the branch, or ``None``.
    :param report_doc: Repo path of the full reports, cross-referenced.
    :returns: The complete new document, or ``None`` when there is
        nothing new to add — the caller then writes nothing at all.
    """
    body = existing or ''
    fresh: list[str] = []
    for rec in records:
        for index, finding in enumerate(rec.findings, start=1):
            ident = finding_id(rec, index)
            # Substring match is sufficient and is the point: an id that
            # survives anywhere in the human's prose counts as present,
            # so moving a row or annotating it never resurrects it.
            if f'`[{ident}]`' in body:
                continue
            raised = (
                f'`{rec.target}`' if rec.target else '_unknown candidate_'
            )
            fresh += [
                '',
                '---',
                '',
                f'## `[{ident}]`',
                '',
                '**Status:** open',
                '',
                f'**Raised against:** {raised} by `{rec.reviewer}`, '
                f'round {rec.round_no}, verdict '
                f'{rec.verdict or "none stated"}',
            ]
            if report_doc:
                fresh += ['', f'**Full report:** `{report_doc}`']
            fresh += ['', finding.strip()]
    if not fresh:
        return None
    if body:
        return body.rstrip('\n') + '\n' + '\n'.join(fresh) + '\n'
    header = [
        f'# {title}',
        '',
        'Non-blocking findings raised by reviewers, across every module '
        'and every run. A blocking finding is acted on inside its review '
        'round; one raised beside an APPROVED verdict used to be '
        'archived and never read again, which is how a real defect '
        'noticed by a careful reviewer evaporated.',
        '',
        'Nothing here has been acted on. These are TRACKED, not fixed — '
        'an agent unilaterally changing code for everything a reviewer '
        'mentioned is worse than the findings being missed.',
        '',
        '**This file is yours to edit.** The runner only ever APPENDS; '
        'it never rewrites, reorders or removes an entry. Change a '
        '`Status:` line, add notes, group things — none of it will be '
        'clobbered. Each entry carries an id, and an entry whose id is '
        'still somewhere in this file is never added again, so an id is '
        'the one thing worth keeping.',
        '',
        'Ids are positional (`chunk/candidate/reviewer/round#n`), so the '
        'same issue raised by two reviewers, or re-raised after a round '
        'of work, appears more than once. That is provenance: merging '
        'them is a judgement call, and it is yours.',
    ]
    return '\n'.join(header + fresh) + '\n'


def render_review_records(
    records: list[ReviewRecord], *, title: str
) -> str:
    """
    Render reviewer reports as a committable record.

    :param records: The votes to document, in the order they were cast.
    :param title: Heading for the document.
    :returns: The markdown document, or ``''`` for no records.
    """
    if not records:
        return ''
    parts = [
        f'# {title}',
        '',
        'Every reviewer that voted on this branch, and the report behind '
        'each vote. Reviewers ran read-only against the branch itself '
        'and were required to EXECUTE what they reviewed — a verdict '
        'reached by inspection alone is not one this pipeline accepts. '
        'All of them had to approve before the branch could publish.',
        '',
        'Kept here because the reviewers do not outlive their own votes: '
        'each microVM is disposed as soon as the round is decided, and '
        'this is the only surviving record of what was checked.',
        '',
        'Messages only — no tool calls and no tool output.',
        '',
        '| Reviewer | Round | Verdict |',
        '| --- | --- | --- |',
    ]
    for rec in records:
        parts.append(
            f'| `{rec.label}` | {rec.round_no} | '
            f'{rec.verdict or "none stated"} |'
        )
    for rec in records:
        parts += [
            '',
            '---',
            '',
            f'## `{rec.label}` — round {rec.round_no} — '
            f'{rec.verdict or "no verdict stated"}',
        ]
        if not rec.turns:
            parts += [
                '',
                '_The session could not be read; only the verdict was '
                'captured._',
            ]
        for role, text in rec.turns:
            if role == 'assistant':
                parts += ['', text.strip()]
            else:
                # The runner's own turn: identical for every reviewer in
                # a stage, so it is folded away rather than repeated at
                # full length above each report.
                parts += [
                    '',
                    '<details><summary>What the reviewer was asked'
                    '</summary>',
                    '',
                    text.strip(),
                    '',
                    '</details>',
                ]
    return '\n'.join(parts) + '\n'


def _fenced(text: str, limit: int = _PR_STEP_CHARS) -> str:
    """
    A fenced code block of *text*, tail-capped, saying so when it cuts.

    The fence grows past any backticks in the payload: test output
    containing a fence would otherwise close the block early and spill
    raw output into the pull-request prose.

    :param text: Captured command output.
    :param limit: Max characters to embed. The TAIL is kept — that is
        where a summary line and a coverage table live.
    :returns: The markdown block.
    """
    body = text.strip() or '(no output)'
    note = ''
    if len(body) > limit:
        note = (
            f'\n\n_Truncated: showing the last {limit:,} of '
            f'{len(body):,} characters._'
        )
        body = body[-limit:]
    fence = '```'
    while fence in body:
        fence += '`'
    return f'{fence}\n{body}\n{fence}{note}'


def _pr_selection_lines(
    picks: list[JudgePick] | None, pick_doc: str | None
) -> list[str]:
    """
    The pull request's "Selection" section.

    Which implementation won. A reviewer of this PR is looking at ONE
    branch; without this there is nothing to say another was built,
    reviewed to the same bar, and set aside.

    :param picks: The judge picks for this chunk, if any.
    :param pick_doc: Repo path of the committed reasoning, if committed.
    :returns: Lines to append, empty when there was no judge.
    """
    if not picks:
        return []
    parts = ['', '## Selection', '']
    for pick in picks:
        others = [c for c in pick.candidates if c != pick.selected]
        beat = (
            f' over {", ".join(f"`{c}`" for c in others)}' if others else ''
        )
        if pick.honored:
            parts.append(
                f'- `{pick.stage}`: the judge chose '
                f'`{pick.selected}`{beat}.'
            )
        else:
            parts.append(
                f'- `{pick.stage}`: **no decision** — the judge stated '
                f'no usable choice, so `{pick.selected}` was taken as '
                f'the fallback (first candidate).'
            )
    kept = [pair for pick in picks for pair in pick.retained]
    if kept:
        parts += [
            '',
            'The implementation(s) not chosen were kept for comparison '
            '(complete, reviewed, and the only copy):',
            '',
        ]
        parts += [f'- `{node}` — `{path}`' for node, path in kept]
    if pick_doc:
        parts += ['', f'The judge reasoning in full: `{pick_doc}`']
    return parts


def _pr_finding_lines(
    findings: list[tuple[str, str]] | None,
) -> list[str]:
    """
    The pull request's "Findings raised" section.

    A tracker nobody is pointed at is barely better than no tracker,
    and this is the one place a reviewer of the PR is certainly looking.

    :param findings: ``(url, title)`` per finding filed for this chunk.
    :returns: Lines to append, empty when nothing was filed.
    """
    if not findings:
        return []
    return [
        '',
        '## Findings raised (not acted on)',
        '',
        'Reviewers approved this branch AND raised the following as '
        'NON-BLOCKING. Nothing here was changed — they are filed for '
        'triage, and closing one is how the pipeline learns not to '
        'raise it again.',
        '',
    ] + [f'- {url} — {title}' for url, title in findings]


def render_pr_body(
    *,
    summary: str,
    task: str | None = None,
    plan_doc: str | None = None,
    session_doc: str | None = None,
    review_doc: str | None = None,
    reviews: list[ReviewRecord] | None = None,
    picks: list[JudgePick] | None = None,
    pick_doc: str | None = None,
    findings: list[tuple[str, str]] | None = None,
    outcome: verify.VerifyOutcome | None = None,
) -> str:
    """
    Assemble a pull request a reviewer can trust without reading code.

    These PRs used to carry one line: the module's title. A reviewer got
    a diff and no way to tell what was claimed, how it was checked, or
    whether the thing runs — and every other signal in this pipeline is
    an agent's WORD, which is exactly what cannot be trusted (an
    implementation that was never written once collected two approvals).

    So the body carries CAPTURED OUTPUT from commands that ran
    mechanically, in a fresh sandbox, on a clean clone of the branch
    being proposed: "the tests pass" as a transcript rather than an
    assertion, and — where the pipeline configures a demonstration — the
    built thing actually running.

    :param summary: One-line description of what shipped.
    :param task: The original ask, when it adds to the summary.
    :param plan_doc: Repo path of the approved design, if committed.
    :param session_doc: Repo path of the planning session record.
    :param review_doc: Repo path of the committed reviewer reports.
    :param reviews: The votes cast on this branch, for the roster — a
        reader should see that round 1 blocked without opening a file.
    :param outcome: The verification result whose steps are the
        evidence; ``None`` when the pipeline has no gate.
    :returns: The markdown body.
    """
    parts = [
        summary.strip(),
        '',
        'Built by an automated pipeline. Everything below is captured '
        'output from commands that ran in a fresh sandbox, on a clean '
        'clone of this branch, with no credentials — evidence, not a '
        'summary of it.',
    ]
    if task and task.strip():
        parts += ['', '## What was asked for', '', task.strip()]
    if plan_doc or session_doc:
        parts += ['', '## How it was designed', '']
        if plan_doc:
            parts.append(f'- Approved design of record: `{plan_doc}`')
        if session_doc:
            parts.append(
                f'- The conversation behind it — drafts, questions, and '
                f'rejected alternatives: `{session_doc}`'
            )
    if reviews:
        parts += [
            '',
            '## Who reviewed it',
            '',
            'Independent reviewers voted on this branch, read-only, and '
            'every one had to approve before it could publish. A '
            'reviewer that could not run what it reviewed was required '
            'to block rather than approve on inspection.',
            '',
            '| Reviewer | Round | Verdict |',
            '| --- | --- | --- |',
        ]
        parts += [
            f'| `{r.label}` | {r.round_no} | '
            f'{r.verdict or "none stated"} |'
            for r in reviews
        ]
        if review_doc:
            parts += [
                '',
                f'Each report in full — what was checked, what was run, '
                f'and what was found: `{review_doc}`',
            ]
    parts += _pr_selection_lines(picks, pick_doc)
    parts += _pr_finding_lines(findings)
    for step in outcome.steps if outcome else ():
        heading, collapse = _PR_STEPS.get(
            step.label, (step.label.title(), True)
        )
        lines = step.command.strip().splitlines()
        parts += ['', f'## {heading}', '']
        if len(lines) == 1:
            parts.append(f'Command: `{lines[0]}`')
        else:
            parts += ['Command:', '', _fenced(step.command, limit=2000)]
        parts += [
            '',
            f'Exit status **{step.exit_code}** — '
            f'{"passed" if step.ok else "FAILED"}.',
            '',
        ]
        block = _fenced(step.output)
        if collapse:
            parts += [
                '<details><summary>Full output</summary>',
                '',
                block,
                '',
                '</details>',
            ]
        else:
            parts.append(block)
    return '\n'.join(parts).rstrip() + '\n'


def short_title(text: str, limit: int = 60) -> str:
    """
    The leading clause of a module title, for a pull-request title.

    A module row is a whole scope paragraph; pasting it whole produced a
    PR title hundreds of characters long that no list view could show.

    :param text: The module title.
    :param limit: Max characters to keep.
    :returns: The shortened title.
    """
    head = text.split('—')[0].strip() or text.strip()
    return head if len(head) <= limit else head[: limit - 1].rstrip() + '…'


def render_planning_session(
    turns: list[tuple[str, str]], *, title: str, plan_doc: str
) -> str:
    """
    Render a planner session as a committable design record.

    The plan of record is a SUMMARY, and each rewrite drops what the
    last one made redundant — diagrams, rejected alternatives, and the
    human's reasoning go with it. Observed live: a module's design draft
    carried a mermaid diagram and nine SQL blocks, the revision that
    superseded it carried four and no diagram, and the revision is what
    would have reached the repo. Keeping the conversation beside the
    summary means the reasoning survives the summarizing.

    :param turns: ``(role, text)`` pairs oldest-first (see
        :meth:`sbx_omnigent.swarm_session.SwarmSessionClient
        .read_transcript`).
    :param title: Heading for the document.
    :param plan_doc: Repo path of the plan of record, cross-referenced.
    :returns: The markdown document, or ``''`` for no turns.
    """
    if not turns:
        return ''
    label = {'user': 'Human', 'assistant': 'Planner'}
    plan_name = PurePosixPath(plan_doc).name
    parts = [
        f'# {title}',
        '',
        f'The planning conversation behind [`{plan_name}`]({plan_name}) '
        '— every draft, the questions asked and answered, and the '
        'artifacts produced along the way.',
        '',
        'The plan of record is the approved SUMMARY of this discussion. '
        'This is the reasoning behind it, kept because consolidating a '
        'plan drops what it makes redundant, and a diagram or a rejected '
        'alternative is exactly what a later reader wants back.',
        '',
        'Messages only — no tool calls and no tool output.',
    ]
    for turn, (role, text) in enumerate(turns, start=1):
        parts += [
            '',
            '---',
            '',
            f'## {turn}. {label.get(role, role)}',
            '',
            text.strip(),
        ]
    return '\n'.join(parts) + '\n'


def resolve_agent_ids(
    config: pipeline.PipelineConfig, catalog: list[dict[str, object]]
) -> dict[str, str]:
    """
    Map each declared agent to its registered id from the catalog.

    Agents register at server start (``--pipeline``) under their
    namespaced names, so this matches ``namespaced_agent_name`` against
    ``GET /v1/agents``.

    :param config: The parsed pipeline.
    :param catalog: ``GET /v1/agents`` objects (``id``/``name``).
    :returns: ``{agent_name: agent_id}``.
    :raises PipelineRunError: When any agent is not registered (with the
        fix: start the server with ``--pipeline``).
    """
    by_name = {
        a.get('name'): a.get('id')
        for a in catalog
        if isinstance(a.get('name'), str) and isinstance(a.get('id'), str)
    }
    ids: dict[str, str] = {}
    missing: list[str] = []
    for name in config.agents:
        spec_name = pipeline.namespaced_agent_name(config.name, name)
        agent_id = by_name.get(spec_name)
        if agent_id is None:
            missing.append(spec_name)
        else:
            ids[name] = agent_id
    if missing:
        raise PipelineRunError(
            'pipeline agents not registered: '
            + ', '.join(sorted(missing))
            + '. Start the server with '
            f'`omni-sbx server --pipeline {config.source_path}`.'
        )
    return ids


@dataclass
class NodeResult:
    """Execution record for one pipeline node."""

    node_id: str
    kind: str
    branch: str | None = None
    worktree: str | None = None
    session: str | None = None
    output: str = ''
    verdict: str | None = None
    selected: str | None = None


@dataclass
class RunResult:
    """Outcome of a pipeline run."""

    run_id: str
    status: str
    published: str | None = None
    final_node: str | None = None
    blocked_stage: str | None = None
    nodes: dict[str, NodeResult] = field(default_factory=dict)
    bindings: list[dict[str, str]] = field(default_factory=list)


class PipelineRunner:
    """
    Execute a parsed pipeline on the branch-as-artifact model.

    :param config: The parsed pipeline.
    :param session_client: Managed-session driver.
    :param worktree_manager: Branch-model worktree manager (its
        ``worktree_root`` MUST equal the server's worktree_root).
    :param run_id: Unique run id (dir + branch component).
    :param agent_ids: ``{agent_name: registered_id}`` (see
        :func:`resolve_agent_ids`).
    :param publish_repo: Push target for publish; defaults to the
        pipeline's ``repo``.
    :param max_review_rounds: Review loop-back cap before blocking.
    :param turn_timeout: Per-turn timeout (seconds).
    :param keep: Leave sessions + worktrees for inspection at the end.
    :param resume: Continue a previous run of this id: reuse its hub,
        skip the stages it already finished, and re-cut only what is
        left. See :meth:`_load_state`.
    :param swap_age_s: Returns the agy swap secret's age in seconds (or
        ``None`` when unknown), checked before every agy turn. Defaults
        to :func:`agy.harvest_age_s`; injected in tests.
    """

    def __init__(
        self,
        config: pipeline.PipelineConfig,
        *,
        session_client: SwarmSessionClient,
        worktree_manager: WorktreeManager,
        run_id: str,
        agent_ids: dict[str, str],
        publish_repo: str | None = None,
        max_review_rounds: int = _DEFAULT_MAX_ROUNDS,
        turn_timeout: float = 1800.0,
        keep: bool = False,
        interactive_plan: bool = True,
        resume: bool = False,
        swap_age_s: Callable[[], float | None] | None = None,
    ) -> None:
        self._config = config
        self._sc = session_client
        self._wt = worktree_manager
        self._run_id = run_id
        self._agent_ids = agent_ids
        self._publish_repo = publish_repo or config.repo
        self._preflight_publish()
        self._max_rounds = max_review_rounds
        self._turn_timeout = turn_timeout
        self._keep = keep
        #: Block the PLANNING phase (only) on human approval: the
        #: planner asks questions, a human answers + posts APPROVED,
        #: and nothing downstream starts until then. Off for automated
        #: runs (and unit tests).
        self._interactive_plan = interactive_plan
        #: Continue a previous attempt rather than starting clean.
        self._resume = resume
        #: Stage ids whose stage FULLY completed. The authority on what
        #: a resume may skip — deliberately not "has a branch", since a
        #: pre-warmed writer has a branch before it has ever run.
        self._completed: set[str] = set()
        #: Chunk ids already built AND published.
        self._completed_chunks: set[str] = set()
        #: Nodes withdrawn because their review never reached consensus
        #: — the blocked review stage and the writer it was vetting. A
        #: forfeited candidate is excluded from judging: comparing a
        #: vetted branch against an unvetted one is not a choice.
        self._forfeited: set[str] = set()
        #: Sessions a PREVIOUS attempt of this run left behind, read
        #: from its state on resume. For DISPOSAL ONLY — never driven,
        #: never treated as live (see :meth:`_dispose_stale_sessions`).
        self._stale_sessions: list[str] = []
        #: winner node -> the verification result that let it publish.
        #: Kept because that captured output IS the pull request's
        #: evidence; discarding it would leave the PR asserting the
        #: tests passed instead of showing it.
        self._evidence: dict[str, verify.VerifyOutcome] = {}
        #: Every reviewer vote cast this run, with the report behind it.
        #: Buffered rather than written to a branch as it happens: a
        #: competition is still live during review, so nothing may
        #: commit to a candidate's branch mid-flight. Written to the run
        #: dir immediately and committed to the winner at publish.
        self._reviews: list[ReviewRecord] = []
        #: Writer nodes whose review stage reached APPROVED consensus.
        #: Read by :meth:`_all_reviewed` so a judge can be told its
        #: candidates are already verified and need not be re-built.
        #: Deliberately NOT persisted: on a resume it starts empty, the
        #: judge simply is not told, and it verifies for itself as it
        #: always did. Degrading to the slower-but-correct behaviour is
        #: the right failure for a claim about what was checked.
        self._reviewed_ok: set[str] = set()
        #: (url, title) per finding filed this run, for the PR body.
        self._filed_findings: list[tuple[str, str]] = []
        #: The judge's choices, so the two-writer race can be
        #: evaluated over a series of modules rather than trusted.
        self._picks: list[JudgePick] = []
        #: Age of the agy swap secret, guarding every agy turn.
        self._swap_age_s = swap_age_s or agy.harvest_age_s
        self._nodes: dict[str, NodeResult] = {}
        self._sessions: list[str] = []
        #: Sessions already freed at stage completion (a finished
        #: reader or judge). Held as a SET rather than removed from
        #: :attr:`_sessions`, because :meth:`_dispose_chunk_sessions`
        #: slices that list by index — dropping an element from the
        #: middle would shift the marks and free the wrong sessions.
        self._released: set[str] = set()
        #: node id -> its transcript, captured just before the session
        #: was freed. Disposing DELETES a session, so a reader whose
        #: conversation is committed later (the planner) must have it
        #: read out first.
        self._reader_turns: dict[str, tuple[tuple[str, str], ...]] = {}
        #: The agent spec behind each session, so a launch can be
        #: checked against what was actually requested for it.
        self._session_agent: dict[str, pipeline.PipelineAgent] = {}
        #: Sessions whose delete FAILED. Kept so they stay in the run
        #: state and get another chance — at teardown, and again on the
        #: next --resume. Held apart from :attr:`_sessions` so the
        #: mark-based slicing in :meth:`_dispose_chunk_sessions` keeps
        #: its index semantics.
        self._undisposed: list[str] = []
        #: agy sessions get the native-TUI first-turn hardening (settle
        #: + resubmit); tracked so only each session's FIRST turn pays.
        self._agy_sessions: set[str] = set()
        #: session id → its node worktree path, so an agy turn can be
        #: staged as a file the agent reads (see _agy_deliverable).
        self._session_worktree: dict[str, str] = {}
        #: session -> node label, for naming a captured transcript.
        self._session_label: dict[str, str] = {}
        #: label -> how many captures it already has, so a writer that
        #: is re-driven twice does not overwrite its own first record.
        self._captures: dict[str, int] = {}
        self._driven: set[str] = set()
        self._bindings: list[dict[str, str]] = []
        self._last_branch_node: str | None = None
        #: The approved, consolidated plan of record (planner's final
        #: output) — shared with builders and committed to the published
        #: branch at publish. None until a planner node runs.
        self._plan_of_record: str | None = None
        #: Ordered build chunks: planner-proposed (flat campaign, see
        #: :func:`parse_subtasks`) or human-supplied modules (per-module
        #: mode, ``config.subtasks``). Empty = single pass.
        self._subtasks: list[pipeline.Subtask] = []
        #: The chunk currently being built (campaign mode), so the
        #: builder instructions can scope the turn to it. None = single
        #: pass / not in a chunk.
        self._active_subtask: pipeline.Subtask | None = None
        #: Whether the active chunk is the FIRST one — nothing has been
        #: built yet, so a turn must not be told to build on prior work.
        self._active_is_first = False
        #: ``(module_id, decision)`` recorded by each module's approved
        #: plan as binding on the modules that follow. Threaded into
        #: every later planner's turn and committed as a ledger, since a
        #: decision made in one module's session reaches no other.
        self._decisions: list[tuple[str, str]] = []
        #: Review observations addressed to a LATER module, as
        #: ``(raising module, text)``. Kept apart from ``_decisions``
        #: on purpose: a decision is BINDING and changing one is a
        #: halt-and-escalate, while this is one reviewer's non-blocking
        #: note. Merging them would promote a passing remark into a
        #: constraint nobody may alter, which is worse than the tracker
        #: they came from.
        self._referrals: list[tuple[str, str]] = []
        #: A verifier's conclusion per finding, by positional id. Absent
        #: means "no conclusion", which files the finding — the gate's
        #: fail-open rule lives in that absence rather than in a flag.
        self._dispositions: dict[str, Disposition] = {}
        #: Node ids pre-warmed during planning — disposed at campaign
        #: start (their un-namespaced VMs are replaced per chunk).
        self._prewarmed: set[str] = set()
        #: Guards run bookkeeping while nodes of one stage run at the
        #: same time (see :meth:`_parallel`). Most individual mutations
        #: are atomic under CPython; the state SNAPSHOT is not — it
        #: walks the session list and the node map, so an append from
        #: another node mid-walk raises "changed size during
        #: iteration". Re-entrant because the guarded paths nest:
        #: _create_session saves state, and so does stage completion.
        self._lock = threading.RLock()
        #: Caps microVMs driven at once ACROSS the whole run, not per
        #: call. Parallelism nests — a parallel block of review stages
        #: each running two reviewers is four guests — and a per-call
        #: cap would bound each level while their product ran away.
        self._node_slots = threading.Semaphore(_MAX_PARALLEL_NODES)
        #: Publish results accumulated across passes/chunks.
        self._published: list[str] = []
        self._stage_by_id: dict[str, pipeline.PipelineStage] = {}
        for stage in pipeline._iter_stages(config.stages):
            if stage.run or stage.parallel:
                self._stage_by_id[stage.id] = stage

    def _preflight_publish(self) -> None:
        """
        Refuse at startup if ``mode: pr`` cannot open a PR.

        The publish target defaults to the pipeline's ``repo:``, which
        is commonly a LOCAL PATH — and a local path has no
        ``owner/repo`` for ``gh -R``. That used to survive provisioning
        the whole build, then die inside ``publish_node`` at the END of
        the first chunk, throwing away a finished module. Parsing the
        slug here turns a run-ending surprise into a two-second error
        with the fix in it.

        :raises PipelineRunError: If PR mode has a non-GitHub target.
        """
        if self._config.publish.mode != 'pr':
            return
        try:
            github_slug(self._publish_repo)
        except click.ClickException as exc:
            raise PipelineRunError(
                f"publish mode 'pr' opens a GitHub PR, but the publish "
                f'target is {self._publish_repo!r}, which has no '
                f'owner/repo. Either pass --publish-repo with the '
                f'GitHub URL (keeping repo: local for a fast clone), or '
                f"set publish.mode to 'local' to push the branch "
                f'without opening a PR.'
            ) from exc

    # ── public entry ──────────────────────────────────────────────

    def run(self) -> RunResult:
        """
        Run the pipeline: to completion with a task, else provision.

        With a ``task:`` the runner drives every stage to consensus and
        publishes. Without one it provisions each node (its worktree +
        microVM session) and returns the role→session bindings, leaving
        the VMs up for a human/coordinator to drive.

        :returns: The :class:`RunResult` (``status`` is ``'completed'``,
            ``'blocked'``, or ``'provisioned'``).
        :raises PipelineRunError: On a setup/turn/git failure.
        """
        self._wt.create_run(
            self._run_id, self._config.repo, reuse=self._resume
        )
        if self._resume:
            self._load_state()
        if self._config.task:
            return self._drive_to_completion()
        return self._provision_only()

    # ── run-to-completion ─────────────────────────────────────────

    def _drive_to_completion(self) -> RunResult:
        blocked: str | None = None
        finished = False
        planner = [s for s in self._config.stages if self._is_planner(s)]
        build = [s for s in self._config.stages if not self._is_planner(s)]
        try:
            try:
                if self._config.subtasks:
                    # Per-module mode: the human supplied the module
                    # list, so the WHOLE pipeline (its own planner +
                    # build) loops once per module — each module planned
                    # in-loop against the frozen prior modules.
                    self._subtasks = list(self._config.subtasks)
                    self._run_campaign(
                        list(self._config.stages), per_module=True
                    )
                else:
                    # The planner runs ONCE (+ proposes the chunk list).
                    for stage in planner:
                        self._exec_stage(stage)
                    if self._is_campaign(build):
                        self._run_campaign(build, per_module=False)
                    else:
                        for stage in build:
                            self._exec_stage(stage)
                        self._verify_publish_target()
                        self._publish()
                finished = True
            except _Blocked as exc:
                blocked = exc.stage_id
        finally:
            # Anything short of a clean finish — a raise, or a blocked
            # gate — keeps the run directory, because that is the only
            # copy of what --resume needs.
            self._teardown(preserve_run=not finished)
        return RunResult(
            run_id=self._run_id,
            status='blocked' if blocked else 'completed',
            published='; '.join(self._published) or None,
            final_node=self._last_branch_node,
            blocked_stage=blocked,
            nodes=dict(self._nodes),
        )

    def _is_campaign(self, build_stages: list[pipeline.PipelineStage]) -> bool:
        """
        Whether to run the build cycle once per proposed chunk.

        Engages only when the planner proposed ≥2 chunks and there are
        build stages to loop; 0/1 chunks run the single pass unchanged
        (so existing single-increment pipelines are untouched).
        """
        return len(self._subtasks) >= 2 and bool(build_stages)

    def _run_campaign(
        self,
        loop_stages: list[pipeline.PipelineStage],
        *,
        per_module: bool,
    ) -> None:
        """
        Run *loop_stages* once per chunk, threaded and published.

        Each chunk runs an id-namespaced copy of *loop_stages*
        (``<chunk>-<node>``) so their branches never collide. The chunks
        thread through one accumulating hub branch ``campaign``: chunk
        0's entry node seeds from base, each later chunk's entry node
        seeds ``from`` the campaign tip (the prior chunk's winner), and
        after every chunk the winner is aliased onto ``campaign``. Each
        chunk publishes separately. A chunk that blocks stops the
        campaign there (earlier chunks stay published).

        Two modes share this loop:

        * flat campaign (``per_module=False``): a single up-front
          planner proposed the chunk list; *loop_stages* are the
          non-planner build stages, and refs to that shared planner
          stay un-namespaced.
        * per-module (``per_module=True``): the human supplied the
          module list; *loop_stages* are the FULL pipeline including
          its planner, so every ref is namespaced and each module runs
          its own planner (in-loop, against the frozen prior modules).

        :param loop_stages: The top-level stages to run per chunk.
        :param per_module: Whether the planner is inside the loop.
        """
        # The pre-warmed (un-namespaced) writers are replaced by
        # per-chunk ones — free their VMs so they don't idle here.
        self._dispose_prewarmed()
        # In per-module mode the planner is looped, so its refs must be
        # namespaced too (each module has its own); in flat mode the
        # shared up-front planner's refs stay un-prefixed.
        planner_ids = (
            set()
            if per_module
            else {s.id for s in self._config.stages if self._is_planner(s)}
        )
        writer_ids = {
            s.id for s in pipeline._iter_stages(loop_stages) if s.write
        }
        for index, sub in enumerate(self._subtasks):
            if sub.id in self._completed_chunks:
                click.echo(
                    f'[resume] skipping chunk [{sub.id}] (already '
                    f'published)'
                )
                continue
            self._active_subtask = sub
            self._active_is_first = index == 0
            mark = len(self._sessions)
            staged = self._namespace_stages(
                sub.id, loop_stages, planner_ids, writer_ids,
                seed_campaign=index > 0,
            )
            # Register the namespaced stages so a review loop-back can
            # look its writer up by the namespaced id.
            for st in pipeline._iter_stages(staged):
                if st.run or st.parallel:
                    self._stage_by_id[st.id] = st
            for st in staged:
                self._exec_stage(st)
            winner = self._resolve_campaign_winner(sub.id)
            # Gate BEFORE the thread advances: a chunk that cannot pass
            # verification must not become the base every later module
            # is built on.
            self._verify_winner(winner)
            # Advance the campaign thread to this chunk's winner so the
            # next chunk builds on it.
            self._wt.alias_node_branch(self._run_id, 'campaign', winner)
            self._publish_chunk(sub, winner, first=index == 0)
            self._completed_chunks.add(sub.id)
            # Persist the completion NOW, not at the next stage's end.
            # The next stage is the following module's PLANNER, which
            # blocks on the human — so deferring this leaves a window
            # (potentially an hour of approval time) where the on-disk
            # state says a published chunk never finished. A resume in
            # that window rebuilds the whole module and publishes a
            # DUPLICATE PR for work that already shipped.
            self._save_state()
            # Published: this chunk's VMs AND clones are dead weight.
            self._dispose_chunk_sessions(mark)
            # The module's PEAK: every tree this chunk built is still
            # on disk, one line before the reclaim takes them.
            self._sample_disk('chunk-peak')
            self._dispose_chunk_worktrees(staged, winner)
            self._active_subtask = None
            self._active_is_first = False

    def _load_state(self) -> None:
        """
        Restore a previous attempt's bookkeeping.

        Only stages recorded as COMPLETE are restored. An incomplete
        node is deliberately left out of ``_nodes``: it must look like
        it never ran, so it is re-cut and re-driven rather than treated
        as an upstream that downstream stages can inherit from.

        :raises PipelineRunError: If there is no state to resume, or it
            was written by an incompatible version — better to refuse
            than to resume from something misread.
        """
        state = self._wt.read_run_state(self._run_id)
        if state is None:
            raise PipelineRunError(
                f'cannot resume run {self._run_id!r}: no usable state '
                f'file. Runs started before state was recorded, or whose '
                f'run directory was removed, cannot be resumed — start a '
                f'fresh run id instead.'
            )
        version = state.get('version')
        if version != RUN_STATE_VERSION:
            raise PipelineRunError(
                f'cannot resume run {self._run_id!r}: its state is '
                f'version {version!r}, this build writes '
                f'{RUN_STATE_VERSION}.'
            )
        self._completed = set(state.get('completed') or [])
        self._completed_chunks = set(state.get('completed_chunks') or [])
        # Absent from a state written before this key existed, which
        # restores the old (empty) behaviour rather than failing — so
        # RUN_STATE_VERSION does NOT move and an in-flight run stays
        # resumable.
        self._reviewed_ok = {
            c for c in state.get('reviewed_ok') or [] if isinstance(c, str)
        }
        self._stale_sessions = [
            s for s in state.get('sessions') or [] if isinstance(s, str)
        ]
        self._decisions = [
            (d['module'], d['text'])
            for d in state.get('decisions') or []
            if isinstance(d, dict) and d.get('module') and d.get('text')
        ]
        # Absent from state written before referrals existed, which
        # restores the old (empty) behaviour rather than failing — so
        # RUN_STATE_VERSION does NOT move and an in-flight run stays
        # resumable.
        self._referrals = [
            (d['module'], d['text'])
            for d in state.get('referrals') or []
            if isinstance(d, dict) and d.get('module') and d.get('text')
        ]
        # Absent before the gate existed. Empty restores the pre-gate
        # behaviour (everything files), so RUN_STATE_VERSION stays put.
        self._dispositions = {
            ident: Disposition(
                verdict=str(raw.get('verdict') or ''),
                reason=str(raw.get('reason') or ''),
            )
            for ident, raw in (state.get('dispositions') or {}).items()
            if isinstance(raw, dict)
        }
        self._reviews = [
            rec
            for rec in (
                ReviewRecord.from_dict(raw)
                for raw in state.get('reviews') or []
                if isinstance(raw, dict)
            )
            if rec is not None
        ]
        self._reader_turns = {
            node: tuple(
                (pair[0], pair[1])
                for pair in turns
                if isinstance(pair, (list, tuple)) and len(pair) == 2
            )
            for node, turns in (state.get('reader_turns') or {}).items()
            if isinstance(node, str) and isinstance(turns, list)
        }
        self._picks = [
            pick
            for pick in (
                JudgePick.from_dict(raw)
                for raw in state.get('judge_picks') or []
                if isinstance(raw, dict)
            )
            if pick is not None
        ]
        self._plan_of_record = state.get('plan_of_record')
        self._published = list(state.get('published') or [])
        self._last_branch_node = state.get('last_branch_node')
        self._subtasks = [
            pipeline.Subtask(id=s['id'], title=s['title'])
            for s in state.get('subtasks') or []
            if isinstance(s, dict) and s.get('id') and s.get('title')
        ]
        for node_id, raw in (state.get('nodes') or {}).items():
            if node_id not in self._completed or not isinstance(raw, dict):
                continue
            self._nodes[node_id] = NodeResult(
                node_id,
                raw.get('kind') or 'reader',
                branch=raw.get('branch'),
                worktree=raw.get('worktree'),
                output=raw.get('output') or '',
                verdict=raw.get('verdict'),
                selected=raw.get('selected'),
            )
        done = len(self._completed)
        click.echo(
            f'[resume] run {self._run_id}: {done} stage(s) already '
            f'complete, {len(self._completed_chunks)} chunk(s) published'
            + (' — plan of record restored' if self._plan_of_record else '')
        )
        self._dispose_stale_sessions()

    def _dispose_stale_sessions(self) -> None:
        """
        Tear down the VMs the previous attempt of this run left up.

        ``--keep`` plus ``--resume`` leaks by construction: the earlier
        process kept its microVMs deliberately, and the resuming
        process starts with an empty session list, so nothing it owns
        would ever dispose them — not at a chunk boundary, not at
        teardown. They would sit holding disk (and their sandbox
        runtimes) for the whole continued run, on top of the VMs it
        stands up itself. Observed live: 12 microVMs from a finished
        module still running eight hours later, on a host at 100%
        capacity — the same exhaustion that remounts guest filesystems
        read-only.

        Best-effort and never fatal: a resume must not fail because a
        VM it was cleaning is already gone (``dispose`` treats a 404 as
        success), and ``--keep`` is honored, since the human asked for
        the previous attempt's VMs to stay.
        """
        stale, self._stale_sessions = self._stale_sessions, []
        if not stale:
            return
        if self._keep:
            click.echo(
                f'[resume] --keep: leaving {len(stale)} microVM(s) from '
                f'the previous attempt up (they are no longer tracked, '
                f'so nothing will dispose them later).'
            )
            return
        gone = 0
        for session in stale:
            try:
                self._sc.dispose(session)
                gone += 1
            except SwarmSessionError:
                pass  # already torn down, or the server lost it
        click.echo(
            f'[resume] disposed {gone}/{len(stale)} microVM(s) left '
            f'running by the previous attempt.'
        )

    def _state_payload(self) -> dict:
        """
        Snapshot everything a run cannot rebuild from git.

        Node BRANCHES survive a crash on their own — every writer's tree
        is committed to the hub. What does not survive is what only ever
        lived in memory: which nodes ran, an approved plan of record, a
        judge's selection, how far a campaign got. That is what this
        captures.

        Sessions are recorded for ONE purpose: so a resume can dispose
        the VMs the previous attempt left running. They are never
        reattached to — a session belongs to a VM a later process
        cannot drive — and are deliberately kept out of the per-node
        records so nothing downstream can mistake one for live.

        :returns: A JSON-serializable snapshot of the run.
        """
        return {
            'version': RUN_STATE_VERSION,
            'run_id': self._run_id,
            'pipeline': self._config.name,
            'plan_of_record': self._plan_of_record,
            'subtasks': [
                {'id': s.id, 'title': s.title} for s in self._subtasks
            ],
            'active_subtask': (
                self._active_subtask.id if self._active_subtask else None
            ),
            'active_is_first': self._active_is_first,
            'completed': sorted(self._completed),
            # Cleanup handles only — see the docstring. Under --keep
            # these accumulate for the whole run, which is exactly the
            # leak a resume needs to clear.
            # Undisposed handles ride along: this key is what a
            # resume reads to reclaim the previous attempt's VMs.
            'sessions': [
                s
                for s in dict.fromkeys(self._sessions + self._undisposed)
                if s not in self._released
            ],
            'decisions': [
                {'module': module_id, 'text': text}
                for module_id, text in self._decisions
            ],
            'referrals': [
                {'module': module_id, 'text': text}
                for module_id, text in self._referrals
            ],
            'dispositions': {
                ident: {'verdict': d.verdict, 'reason': d.reason}
                for ident, d in self._dispositions.items()
            },
            'completed_chunks': sorted(self._completed_chunks),
            # Which writers already cleared a review gate IN THIS RUN.
            # Held in memory it was lost on --resume, and the judge was
            # then told nothing — so it started a verification build it
            # could never finish inside one turn, stated no SELECT, and
            # the first candidate won by default. That is TASKS.md #41
            # arriving through the resume path (#53).
            'reviewed_ok': sorted(self._reviewed_ok),
            'reviews': [r.as_dict() for r in self._reviews],
            # The planner's conversation is buffered in memory the
            # moment its VM is freed, and that VM is then DELETED — so
            # this buffer is the only copy. Without it in the state, a
            # --resume past the plan stage publishes with no session
            # record at all: the stage is already `completed` so it
            # never re-runs, and the session it would re-read is gone.
            'reader_turns': {
                node: [list(pair) for pair in turns]
                for node, turns in self._reader_turns.items()
            },
            'judge_picks': [p.as_dict() for p in self._picks],
            'last_branch_node': self._last_branch_node,
            'published': list(self._published),
            'nodes': {
                node_id: {
                    'kind': node.kind,
                    'branch': node.branch,
                    'worktree': node.worktree,
                    'output': node.output,
                    'verdict': node.verdict,
                    'selected': node.selected,
                }
                for node_id, node in self._nodes.items()
            },
        }

    def _save_state(self) -> None:
        """
        Persist the run snapshot; never fail the run over it.

        Called as the run advances rather than at the end, so a crash
        leaves behind what has been achieved so far — most importantly
        the plan a human already sat through and approved.

        Locked: the payload walks the session list and the node map,
        and nodes of one stage now run at the same time, so an append
        from another node mid-walk would raise "changed size during
        iteration" — a crash in the one routine whose whole job is to
        make a crash survivable.
        """
        with self._lock:
            self._wt.write_run_state(self._run_id, self._state_payload())

    def _verify_winner(self, winner: str) -> None:
        """
        Run the mechanical pre-publish gate; loop back until it passes.

        Every other gate downstream of a writer is an agent's word. This
        one runs the project's own test/coverage command in a clean,
        disposable sandbox on exactly the branch that would publish, and
        believes only the exit status — see :mod:`sbx_omnigent.verify`
        for why it cannot run on the host or in an agent's own VM.

        A failure is closed by the writer that PRODUCED the branch (the
        refactor node, typically): it is the only writer that runs after
        an implementation exists, so adding tests there cannot
        invalidate a competition that already happened. Its review gate
        then votes again on the changed branch before the gate re-runs.

        :param winner: The node whose branch is about to publish.
        :raises PipelineRunError: If the gate cannot be run, cannot be
            fixed by anyone, or still fails after the round cap — in
            every case WITHOUT publishing.
        """
        spec = self._config.verify
        if spec is None:
            return
        fixer = self._verify_fixer(winner)
        for attempt in range(1, self._max_rounds + 1):
            try:
                outcome = self._run_verification(winner, spec)
            except verify.VerifyError as exc:
                # Infrastructure, not a verdict: it says nothing about
                # the branch, so never loop a writer back over it.
                raise PipelineRunError(
                    f'could not run the verification gate for '
                    f'{winner!r}: {exc}'
                ) from exc
            if outcome.ok:
                self._evidence[winner] = outcome
                ran = ', '.join(s.label for s in outcome.steps) or 'gate'
                click.echo(f'[verify] {winner}: passed ({ran}).')
                # The gate builds the workspace from clean AND runs the
                # instrumented coverage pass, so its build directory is
                # the most complete one this run produces — the best
                # possible seed for the next chunk.
                self._refresh_build_cache(f'{winner}-verify')
                return
            why = (
                'timed out'
                if outcome.timed_out
                else f'exited {outcome.exit_code}'
            )
            if fixer is None:
                raise PipelineRunError(
                    f'the verification gate failed on {winner!r} ({why}) '
                    f'and no writer stage produced that branch, so '
                    f'nothing in the pipeline can close the gap. Not '
                    f'publishing.\n\n{outcome.output}'
                )
            if attempt == self._max_rounds:
                raise PipelineRunError(
                    f'the verification gate still fails on {winner!r} '
                    f'({why}) after {self._max_rounds} attempt(s). Not '
                    f'publishing.\n\n{outcome.output}'
                )
            click.echo(
                f'[verify] {winner}: gate FAILED ({why}) — re-driving '
                f'{fixer} to close the gap.'
            )
            self._redrive_writer(
                fixer,
                self._verify_findings(spec, outcome, winner),
                message=f'{fixer}: close the verification gap',
            )
            gate = self._review_gate_for(fixer)
            if gate is not None:
                # The branch changed, so its reviewers must vote again
                # before the gate re-runs. Clear the completion first —
                # _exec_stage skips anything already recorded done.
                self._completed.discard(gate.id)
                self._exec_stage(gate)

    def _run_verification(
        self, winner: str, spec: pipeline.VerifySpec
    ) -> verify.VerifyOutcome:
        """
        Cut a clean clone of *winner*'s branch and verify it.

        A FRESH clone, not the writer's own worktree: it verifies the
        committed state that actually ships, and inherits none of the
        multi-gigabyte build output the writer left behind.

        :param winner: The node whose branch to verify.
        :param spec: The configured gate.
        :returns: The :class:`~sbx_omnigent.verify.VerifyOutcome`.
        """
        from sbx_omnigent.launcher import (  # noqa: PLC0415
            DEFAULT_EGRESS_ALLOW,
            DEFAULT_HOST_IMAGE,
        )

        node_id = f'{winner}-verify'
        workspace = self._wt.create_node_worktree(
            self._run_id, node_id, from_node=winner, replace=True
        )
        click.echo(
            f'[verify] {winner}: running the gate in a disposable '
            f'sandbox (this installs a toolchain and builds from clean).'
        )
        return verify.run_verification(
            name=verify.sandbox_name(self._run_id, node_id),
            workspace=workspace,
            # spec.setup, NOT self._config.setup: the latter is prose
            # addressed to an agent and is not runnable (see
            # verify.build_script).
            script=verify.build_script(spec.setup, spec.rendered()),
            demo_script=(
                verify.build_script(spec.setup, demo)
                if (demo := spec.rendered_demo())
                else None
            ),
            image=spec.image or DEFAULT_HOST_IMAGE,
            egress=DEFAULT_EGRESS_ALLOW,
            # The gate builds its own sandbox, so the SERVER's
            # per-sandbox limits never reach it — these are the only
            # way it gets any.
            cpus=spec.cpus,
            memory=spec.memory,
            timeout_s=spec.timeout_s,
        )

    def _verify_fixer(self, winner: str) -> str | None:
        """The writer stage that produced *winner*, if it is one."""
        stage = self._stage_by_id.get(winner)
        if stage is not None and self._stage_kind(stage) == 'writer':
            return winner
        return None

    def _review_gate_for(
        self, node_id: str
    ) -> pipeline.PipelineStage | None:
        """The review stage that gates *node_id*, if any."""
        for stage in self._stage_by_id.values():
            if (
                stage.on_block == node_id
                and self._stage_kind(stage) == 'review'
            ):
                return stage
        return None

    def _verify_findings(
        self,
        spec: pipeline.VerifySpec,
        outcome: verify.VerifyOutcome,
        winner: str = 'gate',
    ) -> str:
        """
        The evidence handed to the writer closing a gate failure.

        The excerpt is capped, and a capped excerpt is exactly what a
        noisy suite defeats — so the WHOLE capture is written beside
        the run first and the finding names the path. A writer told to
        "close the gap" without being shown the gap is being asked to
        guess, and this run produced that: 6000 characters of Postgres
        checkpoint logs and not one line naming a failing test
        (TASKS.md #42).

        :param spec: The verify spec that ran.
        :param outcome: What the gate reported.
        :param winner: Node whose branch was gated, for the filename.
        :returns: The instruction text.
        """
        why = (
            'did not finish within its budget'
            if outcome.timed_out
            else f'exited {outcome.exit_code}'
        )
        where = ''
        full = '\n\n'.join(
            f'=== {st.label} (exit {st.exit_code}) ===\n'
            f'{st.full_output or st.output}'
            for st in outcome.steps
        )
        relpath = f'turns/{winner}.verify.txt'
        if full.strip() and self._wt.write_run_artifact(
            self._run_id, relpath, full
        ):
            where = (
                f'\n\nThe excerpt below is capped. The COMPLETE gate '
                f'output is on the host that drove this run, at '
                f'<run-dir>/{relpath} — if the excerpt does not show '
                f'you which test failed, say so in your reply rather '
                f'than guessing at a fix.'
            )
        return (
            'The pre-publish VERIFICATION GATE failed on your branch. '
            "This is not a reviewer's opinion: it ran mechanically, "
            'in '
            'a clean disposable sandbox, against exactly the branch that '
            f'would publish.\n\nCommand:\n{spec.rendered()}\n\nIt '
            f'{why}.{where} Output:\n{outcome.output}\n\nClose the '
            'gap by '
            'ADDING tests. You may add new tests; you may NOT modify, '
            'weaken, skip, or delete an existing test, and you may NOT '
            'delete or hollow out production code to make a coverage '
            'number go up — either of those is worse than the shortfall '
            'you were asked to fix. If some uncovered code genuinely '
            'cannot be exercised at this layer, say so explicitly and '
            'explain why rather than finishing quietly.'
            f'\n\n{_FIX_NO_WEAKENING}\n\n{_FIX_MUST_VERIFY}'
            + self._setup_block()
        )

    def _verify_publish_target(self) -> None:
        """Run the gate on the branch a single-pass run will publish."""
        if self._config.verify is None:
            return
        node_id = self._resolve_publish_node()
        node = self._nodes.get(node_id) if node_id else None
        if node is None:
            return
        branch_node = node.selected if node.kind == 'judge' else node_id
        if branch_node:
            self._verify_winner(branch_node)

    def _release_completed_session(
        self, stage: pipeline.PipelineStage, kind: str
    ) -> None:
        """
        Free a finished reader's or judge's microVM straight away.

        Sessions used to live until the CHUNK published, which for a
        reader or a judge is pure waste: neither is ever driven again.
        Every ``_drive`` on one happens inside its own stage, and the
        two loop-back paths — a blocking review and a failed
        verification gate — resolve only to WRITERS. So a judge that
        finished picking sat on a full guest through the refactor, the
        final review and the verification gate: the heaviest part of
        the module, and exactly when the memory is wanted.

        That is not a rounding error. Observed live on a 17 GB host: a
        completed judge held 6 GB while the refactor's linker was being
        OOM-killed, and the gate — the single heaviest job in the run —
        was about to ask for 8 GB more.

        A writer is released only when nothing can drive it again —
        see :meth:`_writer_never_re_driven`. One that a review gate or
        the verification gate can loop back to keeps its session, so
        the fix turn inherits the review context.

        Best-effort. A delete that fails leaves the session tracked, so
        teardown and the next ``--resume`` still try again.

        :param stage: The stage that just completed.
        :param kind: Its :meth:`_stage_kind`.
        """
        # A verifier belongs with the reader and the judge: it is
        # driven once, inside its own stage, and nothing loops back to
        # it. Left out of this tuple it would hold a full guest through
        # publish — the exact waste described above.
        if self._keep or kind not in (
            'reader', 'judge', 'writer', 'verify'
        ):
            return
        if kind == 'writer':
            if not self._writer_never_re_driven(stage):
                return
            # Freeing the VM ends the agent's writes, and a
            # native-terminal writer keeps working past its own settle.
            # Land whatever arrived late before the guest goes.
            self._reconcile_late_writes(stage.id)
        node = self._nodes.get(stage.id)
        session = node.session if node is not None else None
        if session is None or session in self._released:
            return
        if kind == 'reader':
            # The planner's whole conversation is committed beside its
            # plan at publish, and disposing DELETES the session — so
            # read it out here or that record dies with the VM.
            try:
                turns = self._sc.read_transcript(session)
            except SwarmSessionError:
                turns = []
            if turns:
                self._reader_turns[stage.id] = tuple(turns)
            else:
                # THE moment the record is lost. Everything downstream
                # reads an empty buffer and skips in silence, so say it
                # here, while the session that held it still exists.
                click.echo(
                    f'[plan-record] {stage.id}: its transcript read back '
                    f'EMPTY, so there is nothing to buffer. The design '
                    f'conversation dies with this VM.'
                )
        self._free_session(
            session,
            f'{stage.id}: freed its microVM — a {kind} is never '
            f're-driven, and holding it costs a full guest for the '
            f'rest of the module.',
        )

    def _free_session(self, session: str, why: str) -> bool:
        """
        Dispose one session and stop treating it as live.

        The single place a VM is handed back early. A delete that FAILS
        deliberately leaves the session in :attr:`_sessions`, so the
        chunk disposal and teardown both try again — a microVM that may
        still be running must never stop being tracked.

        :param session: The session to free.
        :param why: One line for the log, naming what was freed and why.
        :returns: Whether the microVM was actually released.
        """
        if self._keep or session in self._released:
            return False
        try:
            self._sc.dispose(session)
        except SwarmSessionError:
            return False
        self._released.add(session)
        self._agy_sessions.discard(session)
        self._session_worktree.pop(session, None)
        click.echo(f'[cleanup] {why}')
        return True

    def _writer_never_re_driven(
        self, stage: pipeline.PipelineStage
    ) -> bool:
        """
        Whether nothing left in the pipeline can drive this writer.

        Delegates to :func:`writer_is_terminal` so the release and the
        disk estimate cannot drift apart — the estimate exists to model
        what the runner actually does, and the last time one changed
        without the other it demanded 14 GB that no longer got used.

        :param stage: The writer stage that just completed, whose id
            carries the chunk prefix in a campaign.
        :returns: Whether it is safe to free its microVM.
        """
        stage_id = stage.id
        sub = self._active_subtask
        if sub is not None and stage_id.startswith(f'{sub.id}-'):
            stage_id = stage_id[len(sub.id) + 1 :]
        return writer_is_terminal(self._config, stage_id)

    def _dispose_sessions(self, sessions: Sequence[str]) -> None:
        """
        Tear down exactly the microVMs named, wherever they sit.

        The identity-based counterpart to
        :meth:`_dispose_chunk_sessions`. Two review stages running at
        the same time cannot both free their guests by an index mark:
        each takes its mark as ``len(self._sessions)``, so the stage
        that finishes second slices from ITS mark and deletes the
        sessions the first one is still driving. Naming the sessions
        removes the ambiguity — a stage frees what it created and
        nothing else.

        Outstanding marks survive this. A mark is taken when a chunk
        begins and the sessions removed here are always created after
        it, so they sit at an index at or beyond every live mark, and
        dropping them leaves ``_sessions[mark:]`` meaning what it did
        before: everything still live that the chunk created.

        The delete itself is deliberately OUTSIDE the lock. It is a
        network round trip per guest, and holding the lock across it
        would serialize the very teardown running stages in parallel
        exists to overlap.

        :param sessions: The sessions to free. Already-freed ones are
            skipped, so this is safe as a backstop after each turn has
            released its own guest.
        """
        if self._keep or not sessions:
            return
        with self._lock:
            doomed = set(sessions)
            live = [s for s in sessions if s not in self._released]
            # Freed already by the turn that owned them; forget the
            # bookkeeping now that nothing will ask about them again.
            self._released.difference_update(doomed)
        failed: list[str] = []
        for session in live:
            try:
                self._sc.dispose(session)
            except SwarmSessionError:
                # The delete failed, so the microVM may well still be
                # up. Dropping the handle here would strand it: it is
                # cut from the state file at the next write, and then
                # nothing — not teardown, not a later --resume — knows
                # it exists. That is how a run leaks gigabytes of image
                # store with no record of what is holding them.
                failed.append(session)
        with self._lock:
            for session in live:
                self._agy_sessions.discard(session)
                self._session_worktree.pop(session, None)
            self._sessions[:] = [
                s for s in self._sessions if s not in doomed
            ]
            if failed:
                self._undisposed.extend(failed)
        if failed:
            click.echo(
                f'[cleanup] {len(failed)} microVM(s) refused to '
                f'dispose; keeping them in the run state so teardown '
                f'and --resume can try again.'
            )
        self._save_state()

    def _dispose_chunk_sessions(self, mark: int) -> None:
        """
        Tear down the VMs a finished chunk created.

        A published chunk's VMs are dead weight: its work is committed
        to a branch and pushed, and the next chunk seeds from the hub,
        never from a node's clone. Holding them cost a VM per node per
        chunk for the WHOLE run — a 6-module full cadre peaked at ~72
        live microVMs and exhausted the host's disk, which surfaces as
        guest filesystems remounting read-only rather than as any
        legible error. ``--keep`` still keeps everything.

        :param mark: ``len(self._sessions)`` when the chunk started;
            everything appended since belongs to it.
        """
        if self._keep:
            return
        self._dispose_sessions(self._sessions[mark:])

    def _dispose_chunk_worktrees(
        self, staged: list[pipeline.PipelineStage], winner: str
    ) -> None:
        """
        Reclaim a published chunk's node clones.

        Its work is committed to hub branches and pushed, the next chunk
        seeds from the hub rather than from any clone, and a resume
        skips a completed chunk wholesale — so the clones are dead
        weight the moment it publishes. Keeping them makes disk cost
        CUMULATIVE across a campaign: 2.2-26 GB per writer node for a
        compiled language, which ran a host out of space two modules
        into a six-module run. Reclaiming here makes the cost
        per-module instead.

        Called only AFTER the chunk's completion is persisted, so a
        crash in between leaves a resume that skips the chunk anyway.
        Branches are never touched; ``--keep`` keeps everything.

        :param staged: The chunk's namespaced stages.
        :param winner: The node whose branch published (its
            verification clone, if any, is named after it).
        """
        if self._keep:
            return
        node_ids = [
            st.id
            for st in pipeline._iter_stages(staged)
            if st.run or st.parallel
        ]
        # The gate cuts its own throwaway clone; sweep it up too.
        node_ids.append(f'{winner}-verify')
        try:
            freed = self._wt.dispose_node_worktrees(self._run_id, node_ids)
        except click.ClickException:
            return  # reclaiming disk must never fail a healthy run
        if freed:
            click.echo(
                f'[cleanup] reclaimed {freed} worktree(s) from the '
                f'published chunk (their branches are on the hub).'
            )

    def _namespace_stages(
        self,
        chunk_id: str,
        stages: list[pipeline.PipelineStage],
        planner_ids: set[str],
        writer_ids: set[str],
        *,
        seed_campaign: bool,
    ) -> list[pipeline.PipelineStage]:
        """Return *stages* with ids + refs namespaced for the chunk."""
        out: list[pipeline.PipelineStage] = []
        for stage in stages:
            if stage.parallel:
                subs = tuple(
                    self._namespace_one(
                        chunk_id, sub, planner_ids, writer_ids,
                        seed_campaign=seed_campaign,
                    )
                    for sub in stage.parallel
                )
                out.append(
                    replace(stage, id=f'{chunk_id}-{stage.id}', parallel=subs)
                )
            else:
                out.append(
                    self._namespace_one(
                        chunk_id, stage, planner_ids, writer_ids,
                        seed_campaign=seed_campaign,
                    )
                )
        return out

    def _namespace_one(
        self,
        chunk_id: str,
        stage: pipeline.PipelineStage,
        planner_ids: set[str],
        writer_ids: set[str],
        *,
        seed_campaign: bool,
    ) -> pipeline.PipelineStage:
        """Namespace one stage's id + references (planner refs stay)."""
        def ns(ref: str) -> str:
            return ref if ref in planner_ids else f'{chunk_id}-{ref}'

        new_from = ns(stage.from_branch) if stage.from_branch else None
        # A chunk-entry node that cuts its own worktree (a reader like
        # the per-module planner, or the entry writer) with no in-chunk
        # writer upstream and no explicit from seeds from the campaign
        # tip on chunks after the first — so it designs/builds against
        # the frozen prior chunks. Reviews/judges don't cut from a seed,
        # so leaving their from unset is a no-op for them.
        if (
            seed_campaign
            and self._stage_kind(stage) in ('reader', 'writer')
            and stage.from_branch is None
            and not any(n in writer_ids for n in stage.needs)
        ):
            new_from = 'campaign'
        return replace(
            stage,
            id=f'{chunk_id}-{stage.id}',
            needs=tuple(ns(n) for n in stage.needs),
            from_branch=new_from,
            on_block=ns(stage.on_block) if stage.on_block else None,
        )

    def _resolve_campaign_winner(self, chunk_id: str) -> str:
        """The node id whose branch this chunk publishes/threads."""
        want = self._config.publish.branch
        node_id = (
            f'{chunk_id}-{want}' if want else self._last_branch_node
        )
        node = self._nodes.get(node_id) if node_id else None
        if node is None:
            raise PipelineRunError(
                f'chunk {chunk_id!r} produced no branch to publish'
            )
        return node.selected if node.kind == 'judge' else node_id

    def _publish_chunk(
        self, sub: pipeline.Subtask, winner: str, *, first: bool
    ) -> None:
        """Publish one chunk's winner to ``pipeline/<run>-<chunk>``."""
        if self._config.publish.mode == 'none':
            return
        plan, plan_path = self._chunk_plan_of_record(sub, first=first)
        if plan:
            self._wt.write_tracked_file(
                self._wt.node_worktree_path(self._run_id, winner),
                plan_path,
                plan,
            )
            self._wt.commit_node(
                self._run_id, winner,
                message='docs: add plan of record',
                author='planner <planner@pipeline.local>',
            )
        self._record_referrals()
        self._commit_decisions_ledger(winner)
        # Flat mode has ONE planner for the whole campaign, so its
        # session record is the same conversation every time. Committing
        # it per chunk rewrote one path from every branch — the same
        # add/add collision the per-chunk names above remove — while
        # adding nothing. Per-module mode has a planner per module, and
        # a per-module path, so it still records every chunk.
        if self._config.subtasks or first:
            self._commit_planning_session(winner, plan_path)
        review_doc = self._commit_review_records(winner, plan_path)
        self._publish_findings(winner, review_doc)
        pick_doc = self._commit_judge_record(winner, plan_path)
        result = self._wt.publish_node(
            self._run_id, winner, self._publish_repo,
            title=(
                f'[pipeline] {self._config.name} — {sub.id}: '
                f'{short_title(sub.title)}'
            ),
            body=self._pr_body(
                winner, plan_path,
                summary=f'**[{sub.id}]** {sub.title}',
                review_doc=review_doc,
                pick_doc=pick_doc,
            ),
            base_branch=self._stack_base(),
            base_fallback=self._config.base_branch,
            remote_branch=self._chunk_remote_branch(sub.id),
            open_pr=self._config.publish.mode == 'pr',
        )
        self._published.append(result)
        self._save_state()

    def _chunk_remote_branch(self, chunk_id: str) -> str:
        """:returns: The remote branch a module publishes to."""
        return f'pipeline/{self._run_id}-{chunk_id}'

    def _stack_base(self) -> str | None:
        """
        The base this module's pull request should open against.

        Every module's request used to target the repo's base branch,
        which is right only once the previous one has merged. While
        earlier requests are open, a later one shows their code as its
        own — measured on a live build at 55 files and +12,524 lines
        for a module whose own work was 28 files. Basing it on the
        module below shows just that module, and GitHub re-targets it
        to the real base once that branch merges.

        DERIVED from the modules that have published rather than
        remembered, so it needs no state of its own and a campaign
        resumed from an older run still stacks instead of dropping the
        next module back onto the repo's base branch. The module list
        supplies the order; a set of published ids alone would not
        (``m10`` sorts before ``m2``).

        :returns: The remote branch of the last module to publish
            before the active one, or ``None`` for the repo's base
            branch (the first module, or stacking off).
        """
        active = self._active_subtask
        if not self._config.publish.stack or active is None:
            return None
        prior = None
        for sub in self._subtasks:
            if sub.id == active.id:
                break
            if sub.id in self._completed_chunks:
                prior = sub.id
        return self._chunk_remote_branch(prior) if prior else None

    def _pr_body(
        self,
        winner: str,
        plan_path: str,
        *,
        summary: str,
        task: str | None = None,
        review_doc: str | None = None,
        pick_doc: str | None = None,
    ) -> str:
        """
        The pull-request body for a branch about to publish.

        :param winner: The node whose branch publishes (its verification
            evidence, if any, is embedded).
        :param plan_path: Repo path of this branch's plan of record.
        :param summary: One-line description of what shipped.
        :param task: The original ask, when it adds to the summary.
        :param review_doc: Repo path of the committed reviewer reports.
        :returns: The markdown body.
        """
        has_plan = self._planner_node_id() is not None
        return render_pr_body(
            summary=summary,
            task=task,
            review_doc=review_doc,
            reviews=self._chunk_reviews(),
            picks=self._chunk_picks(),
            pick_doc=pick_doc,
            findings=list(self._filed_findings),
            plan_doc=plan_path if has_plan else None,
            session_doc=(
                self._session_artifact_path(plan_path)
                if has_plan
                else None
            ),
            outcome=self._evidence.get(winner),
        )

    def _next_review_round(self, stage_id: str) -> int:
        """
        The next round number for a review stage, across re-entries.

        Derived from the records themselves rather than a counter, so it
        survives a resume for free: ``_reviews`` is restored from the
        run state, and a resumed run continues the numbering instead of
        starting over and overwriting what the earlier attempt wrote.

        :param stage_id: The review stage's id (already namespaced per
            module, so modules cannot collide).
        :returns: One past the highest round recorded for that stage.
        """
        with self._lock:
            prior = [
                r.round_no for r in self._reviews
                if r.stage == stage_id
            ]
        return max(prior, default=0) + 1

    def _sandbox_for_session(self, session: str) -> str | None:
        """
        The microVM name backing *session*, or ``None``.

        A session names only its HOST; the sandbox is named after that
        host, so this is two reads: the session for its ``host_id``, and
        the host list for the name the launcher gave it. Deliberately
        NOT derived from the id by string surgery — the naming rule is
        the server's, and a convention baked in here would rot silently.

        Best-effort to the point of catching everything short of a
        KeyboardInterrupt. This runs only when a turn has ALREADY
        failed, so the one outcome that must be impossible is this
        lookup turning a diagnosable failure into a different one —
        and the ways it can go wrong are open-ended: an older server
        with no ``/v1/hosts``, a response shape that changed, an
        injected client that does not
        implement these reads at all. Every one of them means the same
        thing to the caller: no pane. ``KeyboardInterrupt`` is not an
        ``Exception`` and still gets out, matching
        :meth:`_capture_turn` — a second Ctrl-C means now.

        :param session: The session to locate.
        :returns: The sandbox name, e.g. ``"managed-cb683c32"``.
        """
        try:
            host = self._sc.session_host_id(session)
            return self._sc.host_name(host) if host else None
        except Exception:
            return None

    def _capture_pane(self, session: str, artifact: str) -> str | None:
        """
        Write the session's TUI pane beside its transcript.

        The transcript is empty in exactly the case this exists for — a
        harness blocked on a keystroke never produces a message — so the
        pane is often the ONLY evidence. See :mod:`sbx_omnigent.pane`.

        :param session: The session whose VM to read.
        :param artifact: Repo-relative path of the transcript artifact,
            used to derive the pane's own name.
        :returns: The pane artifact's path, or ``None`` if none written.
        """
        sandbox = self._sandbox_for_session(session)
        if sandbox is None:
            return None
        try:
            text = pane.capture_pane(sandbox)
        except Exception:  # pragma: no cover - capture_pane is total
            return None
        if not text:
            return None
        path = artifact.removesuffix('.md') + '.pane.txt'
        header = (
            f'# TUI pane — {sandbox}\n'
            f'# Captured because the turn did not report. A native '
            f'harness blocked on a keystroke goes silent on every other '
            f'channel, so this screen is usually the whole diagnosis.\n'
            "# Nothing was typed into it — answering on the "
            "human's behalf is not this tool's job.\n\n"
        )
        # If the screen is a modal picker, SAY so. The failure a human
        # otherwise sees describes the paste mechanism and buries the
        # thing to do, with the picker dumped raw at the end of a
        # RuntimeError (TASKS.md #12). They are watching this console;
        # they may never open the failed message bubble.
        label = self._session_label.get(session, session)
        prompt = pane.modal_prompt(text)
        if prompt is not None:
            guidance = pane.blocked_on_prompt_message(label, prompt)
            click.echo(f'[blocked] {guidance}')
            header += (
                '# THIS IS A MODAL PROMPT. A typed turn cannot answer '
                'it — open the pane and choose there.\n\n'
            )
        if self._wt.write_run_artifact(self._run_id, path, header + text):
            return path
        return None

    def _capture_turn(
        self, session: str, reason: str, *, with_pane: bool = False
    ) -> str | None:
        """
        Write a session's transcript to the run dir, best-effort.

        Called at the moments the record is about to be lost: a turn
        that failed or timed out (teardown is seconds away), a turn or
        a planning session interrupted with Ctrl-C, and every loop-back
        fix turn (its session is disposed at publish, and that turn is
        the one a human most wants to read afterwards).

        Never raises — a diagnostic that can fail a run is worse than
        no diagnostic — but it deliberately does NOT swallow a
        :exc:`KeyboardInterrupt`. Someone pressing Ctrl-C a second time
        wants out now, not one more bounded HTTP read.

        :param session: The session to read.
        :param reason: Why it is being captured.
        :param with_pane: Also capture the harness's TUI pane. Set on
            the paths where the turn did NOT report — a timeout, a
            failure, an interrupt — because that is exactly when the
            transcript is empty and the screen is the only evidence.
            Left off for routine captures so a healthy run does not pay
            an ``sbx exec`` round-trip per review round.
        :returns: The pane artifact's path when one was written, so the
            caller can name it in the error a human will actually read.
        """
        label = self._session_label.get(session, session)
        try:
            turns = self._sc.read_transcript(session)
        except SwarmSessionError:
            turns = []
        seen = self._captures.get(label, 0) + 1
        self._captures[label] = seen
        suffix = '' if seen == 1 else f'-{seen}'
        artifact = f'turns/{label}{suffix}.md'
        # Transcript FIRST, so the guarantee this method already made is
        # never traded for the new one: if the pane read is interrupted
        # or hangs, the conversation is already on disk.
        if self._wt.write_run_artifact(
            self._run_id,
            artifact,
            render_turn_capture(
                turns, title=f'Turn — {label}', reason=reason
            ),
        ):
            click.echo(
                f'[capture] wrote {label}{suffix} to the run directory '
                f'({len(turns)} message(s)): {reason}'
            )
        if not with_pane:
            return None
        pane_path = self._capture_pane(session, artifact)
        if pane_path is not None:
            click.echo(
                f'[capture] wrote the {label} TUI pane to {pane_path} — '
                f'read it FIRST: a harness blocked on a keystroke '
                f'reports nothing anywhere else.'
            )
        return pane_path

    def _record_review(
        self,
        stage_id: str,
        reviewer: str,
        round_no: int,
        verdict: str | None,
        session: str,
        fallback: str,
    ) -> None:
        """
        Capture a reviewer's report before its session is disposed.

        Written to the run dir straight away, so a run that blocks or
        crashes still explains itself; buffered in memory (and in the
        run state, so a resume keeps it) for the copy committed to the
        publishing branch. Best-effort throughout — a lost record must
        never fail a review that otherwise decided cleanly.

        :param stage_id: The review stage's id.
        :param reviewer: The agent name that voted.
        :param round_no: Which review round this was (1-based).
        :param verdict: The parsed verdict, or ``None``.
        :param session: The reviewer's session, still alive.
        :param fallback: The reply text already captured, used when the
            transcript cannot be read at all.
        """
        try:
            turns = self._sc.read_transcript(session)
        except SwarmSessionError:
            turns = []
        if not turns and fallback.strip():
            turns = [('assistant', fallback.strip())]
        # Best-effort: a review whose stage cannot be resolved still
        # records its vote, just without naming the candidate.
        stage = self._stage_by_id.get(stage_id)
        try:
            target = self._review_target(stage) if stage else None
        except PipelineRunError:
            target = None
        sections = FindingSections.of(fallback)
        record = ReviewRecord(
            chunk=self._active_subtask.id if self._active_subtask else None,
            stage=stage_id,
            reviewer=reviewer,
            round_no=round_no,
            verdict=verdict,
            turns=tuple(turns),
            findings=sections.all,
            defects=sections.defects,
            later_increment=sections.later_increment,
            premises=sections.premises,
            target=target,
        )
        # Reviewers of a stage vote concurrently, and the round
        # number below is derived by WALKING this list.
        with self._lock:
            self._reviews.append(record)
        self._wt.write_run_artifact(
            self._run_id,
            f'reviews/{record.slug}.md',
            render_review_records(
                [record], title=f'Review — {record.label}'
            ),
        )
        self._save_state()

    def _chunk_reviews(self) -> list[ReviewRecord]:
        """:returns: The votes belonging to the publishing chunk."""
        chunk = self._active_subtask.id if self._active_subtask else None
        return [r for r in self._reviews if r.chunk == chunk]

    def _commit_review_records(
        self, winner: str, plan_path: str
    ) -> str | None:
        """
        Commit the reviewer reports onto the branch about to publish.

        Best-effort like the planning-session record: worth having,
        never worth failing a publish over.

        :param winner: The node whose branch publishes.
        :param plan_path: The plan artifact path, whose name this record
            is derived from (it needs no plan to exist).
        :returns: The committed repo path, or ``None``.
        """
        records = self._chunk_reviews()
        if not records:
            return None
        sub = self._active_subtask
        title = f'Reviews — {self._config.name}'
        if sub is not None:
            title += f' [{sub.id}] {sub.title}'
        doc = render_review_records(records, title=title)
        path = self._review_artifact_path(plan_path)
        try:
            self._wt.write_tracked_file(
                self._wt.node_worktree_path(self._run_id, winner),
                path,
                doc,
            )
            committed = self._wt.commit_node(
                self._run_id, winner,
                message='docs: add reviewer reports',
                author='reviewer <reviewer@pipeline.local>',
            )
        except click.ClickException:
            return None
        if not committed:
            return None
        click.echo(
            f'[review] committed {len(records)} reviewer report(s) '
            f'to {path}.'
        )
        return path

    def _commit_judge_record(
        self, winner: str, plan_path: str
    ) -> str | None:
        """
        Commit the judge's choice onto the branch about to publish.

        Best-effort like the reviewer reports: worth having, never worth
        failing a publish over. A pipeline with no judge writes nothing.

        :param winner: The node whose branch publishes.
        :param plan_path: The plan artifact path, whose name this record
            is derived from (it needs no plan to exist).
        :returns: The committed repo path, or ``None``.
        """
        picks = self._chunk_picks()
        if not picks:
            return None
        sub = self._active_subtask
        title = f'Selection — {self._config.name}'
        if sub is not None:
            title += f' [{sub.id}] {sub.title}'
        doc = render_judge_decision(picks, title=title)
        path = self._decision_artifact_path(plan_path)
        try:
            self._wt.write_tracked_file(
                self._wt.node_worktree_path(self._run_id, winner),
                path,
                doc,
            )
            committed = self._wt.commit_node(
                self._run_id, winner,
                message='docs: add the judge selection',
                # A role, like the planner and reviewer records — never
                # the judge AGENT's name, which would put the deciding
                # model back into git log (TASKS.md #33).
                author='judge <judge@pipeline.local>',
            )
        except click.ClickException:
            return None
        if not committed:
            return None
        click.echo(
            f'[judge] committed {len(picks)} selection(s) to {path}.'
        )
        return path

    def _retain_losers(
        self, candidates: list[str], selected: str
    ) -> tuple[tuple[str, str], ...]:
        """
        Preserve every candidate that did not win, as a git bundle.

        Only the winner publishes, so without this a complete, reviewed,
        test-passing implementation of the same frozen contract — by a
        different model — is deleted with the run hub. It is the only
        copy, and it is the evidence any future argument about model
        choice would actually want (TASKS.md #32).

        Best-effort: a run that produced a shippable winner must not
        fail because an archive could not be written. A failure is
        reported loudly rather than swallowed, because the whole point
        is that this data is otherwise unrecoverable.

        :param candidates: Every node the judge compared.
        :param selected: The node that won.
        :returns: ``(node, bundle-path)`` for each loser retained.
        """
        kept: list[tuple[str, str]] = []
        for node in candidates:
            if node == selected:
                continue
            try:
                path = self._wt.retain_node_bundle(
                    self._run_id,
                    node,
                    # `base_branch:` is optional in a pipeline.yaml, and
                    # a None here would bundle against the literal ref
                    # 'None'. Same fallback as _seed_ref.
                    against=self._config.base_branch or 'main',
                )
            except Exception as exc:
                # Loud, never fatal. The run produced a shippable
                # winner; losing the archive is a real loss and must be
                # SAID, but it is not a reason to fail the publish.
                click.echo(
                    f"[retain] {node}: could NOT be preserved ({exc})."
                    f" Its branch lives only on this run's hub and"
                    f" will go with it at teardown."
                )
                continue
            if path is None:
                continue
            kept.append((node, path))
            click.echo(
                f'[retain] {node}: kept at {path} — restore with '
                f"'git fetch {path} "
                f"refs/heads/{self._wt.node_branch(self._run_id, node)}"
                f":refs/heads/{node}'."
            )
        return tuple(kept)

    def _sample_disk(self, event: str) -> None:
        """
        Record what this run is holding on disk right now.

        Off unless :data:`sbx_omnigent.disk_metrics.ENABLE_ENV_VAR` is
        set — measuring a 26 GB build tree means walking its inodes, and
        a full cadre has five of them across ~8 boundaries.

        Sampled at every stage boundary rather than once at the end,
        because the preflight predicts a concurrent PEAK and only a time
        series shows one. Records the sbx snapshot store alongside, so
        the same run also answers whether ``per_vm_gb`` is right.

        Never raises, never fails a run: instrumentation that can cost a
        module is worse than no instrumentation (TASKS.md #6, #36).

        :param event: What boundary this is, e.g. ``"chunk-peak"``.
        """
        if not disk_metrics.enabled():
            return
        try:
            records = disk_metrics.sample(
                run_id=self._run_id,
                event=event,
                run_dir=self._wt.run_dir(self._run_id),
                chunk=(
                    self._active_subtask.id
                    if self._active_subtask
                    else None
                ),
                kinds={
                    node: result.kind
                    for node, result in self._nodes.items()
                },
                store_layers=orphans.layer_bytes(),
                free_path=Path.home(),
            )
            written = disk_metrics.append(
                self._wt.metrics_path(self._run_id), records
            )
        except Exception:  # pragma: no cover - sample() is total
            return
        if written:
            click.echo(
                f'[disk] {event}: recorded {len(records)} measurement(s) '
                f'to {self._wt.metrics_path(self._run_id)}.'
            )

    def _record_pick(
        self,
        stage_id: str,
        candidates: list[str],
        selected: str,
        stated: str | None,
        reasoning: str,
        retained: tuple[tuple[str, str], ...] = (),
    ) -> None:
        """
        Record a judge's choice before its session is disposed.

        Written to the run dir straight away so a run that fails after
        the judge still explains what it picked, and buffered (and
        persisted, so a resume keeps it) for the copy committed to the
        publishing branch — the same treatment reviewer reports get.

        :param stage_id: The judge stage's id.
        :param candidates: The competing node ids it compared.
        :param selected: The node id that will publish.
        :param stated: What the judge's own SELECT line named, or
            ``None``. Recorded separately so the runner's fail-safe
            substitution cannot be mistaken for a real preference.
        :param reasoning: The judge's reply. Passed in rather than read
            back off the node, because this runs BEFORE the node exists
            — recording has to happen before the branch alias so a run
            that fails in between still explains itself.
        :param retained: ``(node, bundle-path)`` for each preserved
            loser, so the record says where the losing code lives.
        """
        pick = JudgePick(
            chunk=self._active_subtask.id if self._active_subtask else None,
            stage=stage_id,
            candidates=tuple(candidates),
            selected=selected,
            stated=stated,
            reasoning=reasoning,
            retained=retained,
        )
        self._picks.append(pick)
        self._wt.write_run_artifact(
            self._run_id,
            f'judging/{stage_id}.md',
            render_judge_decision([pick], title=f'Judging — {stage_id}'),
        )
        if pick.honored:
            click.echo(
                f'[judge] {stage_id}: selected {selected} from '
                f'{", ".join(candidates)}.'
            )
        else:
            click.echo(
                f'[judge] {stage_id}: no usable SELECT line (said '
                f'{stated!r}) — falling back to {selected}. This is '
                f'recorded as an ABSENT decision, not a preference.'
            )

    def _chunk_picks(self) -> list[JudgePick]:
        """This chunk's judge picks (all of them outside a campaign)."""
        chunk = self._active_subtask.id if self._active_subtask else None
        return [p for p in self._picks if p.chunk == chunk]

    def _chunk_artifact_base(self, plan_path: str) -> str:
        """
        The base path this chunk's OWN records hang off.

        Per-chunk records must not share a path across chunks. Each
        chunk publishes its own pull request, and nothing threads the
        doc commits forward — the campaign branch is aliased to the
        winner BEFORE ``_publish_chunk`` commits them — so two chunks
        writing one path do not modify a shared file, they each CREATE
        it. Git sees the same path added on both sides and every merge
        after the first conflicts (TASKS.md #45).

        Per-module mode never hit this because its *plan_path* is
        already per-module (``discover-m1.md``), which these records
        inherit; it is passed through untouched here so the chunk id is
        not folded in twice. Flat campaign mode shares one plan_path
        across every chunk, so the chunk id is folded in HERE.

        :param plan_path: The chunk's plan artifact path.
        :returns: The base path to derive per-chunk record names from.
        """
        sub = self._active_subtask
        if sub is None or self._config.subtasks:
            return plan_path
        p = PurePosixPath(plan_path)
        return str(p.with_name(f'{p.stem}-{sub.id}{p.suffix}'))

    def _decision_artifact_path(self, plan_path: str) -> str:
        """The judge record's repo path, beside the reviewers'."""
        p = PurePosixPath(self._chunk_artifact_base(plan_path))
        return str(p.with_name(f'{p.stem}-selection{p.suffix}'))

    def _review_artifact_path(self, plan_path: str) -> str:
        """The reviewer reports' repo path, beside the plan's."""
        p = PurePosixPath(self._chunk_artifact_base(plan_path))
        return str(p.with_name(f'{p.stem}-reviews{p.suffix}'))

    def _session_artifact_path(self, plan_path: str) -> str:
        """The session record's repo path, beside the plan's."""
        p = PurePosixPath(plan_path)
        return str(p.with_name(f'{p.stem}-session{p.suffix}'))

    def _planner_node_id(self) -> str | None:
        """The planner node for the active chunk, else the run's."""
        for stage in self._config.stages:
            if not self._is_planner(stage):
                continue
            sub = self._active_subtask
            if sub is not None and self._config.subtasks:
                return f'{sub.id}-{stage.id}'
            return stage.id
        return None

    def _planning_turns(
        self, node_id: str
    ) -> list[tuple[str, str]] | None:
        """
        The planner's conversation, or ``None`` with the reason logged.

        The BUFFER first, and a live session only as the fallback. The
        two used to be the other way round, which is why no planning
        session record ever reached the repo: a resumed run restores its
        nodes WITHOUT a session (deliberately — see
        ``test_state_never_records_a_session``: a session handle names
        a VM the new run cannot reattach to), so the session guard
        returned
        before the buffer — the one copy that survives — was ever
        consulted. The buffer needs no live session by construction; it
        exists precisely because the VM is gone.

        :param node_id: The planner node.
        :returns: Its turns, or ``None`` when there are none to record.
        """
        node = self._nodes.get(node_id)
        if node is None:
            click.echo(
                f'[plan-record] no session record: planner node '
                f"{node_id!r} is not among the run's nodes "
                f'({", ".join(sorted(self._nodes)) or "none"}).'
            )
            return None
        buffered = self._reader_turns.get(node_id)
        if buffered is not None:
            return list(buffered)
        if not node.session:
            click.echo(
                f'[plan-record] no session record: nothing was buffered '
                f'for {node_id!r} and it has no live session to read '
                f"(a resumed run cannot reattach to the planner's VM)."
            )
            return None
        # The shipped read_transcript swallows SwarmSessionError and
        # returns [], so an empty result is the DISPOSED session — but
        # the guard stays: this must never be able to fail a publish,
        # whatever client is injected.
        try:
            turns = self._sc.read_transcript(node.session)
        except SwarmSessionError:
            click.echo(
                f'[plan-record] no session record: {node_id!r} was not '
                f'buffered and its session could not be read.'
            )
            return None
        if not turns:
            click.echo(
                f'[plan-record] no session record: nothing was buffered '
                f'for {node_id!r} and its session reads back empty (it '
                f'has been disposed).'
            )
        return turns

    def _commit_planning_session(
        self, winner: str, plan_path: str
    ) -> None:
        """
        Commit the planner's whole conversation beside its plan.

        Best-effort by construction: a design record is worth having,
        but never worth failing a publish over, so an unreadable session
        or a git error simply skips it.

        :param winner: The node whose branch publishes.
        :param plan_path: Repo path of this plan of record.
        """
        node_id = self._planner_node_id()
        if node_id is None:
            return  # no planner in this pipeline: nothing to record
        turns = self._planning_turns(node_id)
        if turns is None:
            return
        sub = self._active_subtask
        title = f'Planning session — {self._config.name}'
        if sub is not None:
            title += f' [{sub.id}] {sub.title}'
        doc = render_planning_session(
            turns, title=title, plan_doc=plan_path
        )
        if not doc:
            click.echo(
                f'[plan-record] no session record: {node_id!r} produced '
                f'{len(turns)} message(s), which rendered to nothing.'
            )
            return
        path = self._session_artifact_path(plan_path)
        try:
            self._wt.write_tracked_file(
                self._wt.node_worktree_path(self._run_id, winner),
                path,
                doc,
            )
            committed = self._wt.commit_node(
                self._run_id, winner,
                message='docs: add planning session record',
                author='planner <planner@pipeline.local>',
            )
        except click.ClickException:
            return
        if committed:
            click.echo(
                f'[plan] committed the planning session '
                f'({len(turns)} turns) to {path}.'
            )

    def _chunk_plan_of_record(
        self, sub: pipeline.Subtask, *, first: bool
    ) -> tuple[str | None, str]:
        """
        The plan text + repo path to commit for a chunk, if any.

        Per-module mode commits each module's OWN planner design to a
        per-module path (``…-<module>.md``), so modules don't clobber
        one another's plan doc as the campaign thread accumulates. Flat
        mode commits the single shared plan of record once, on chunk 0's
        branch (later chunks inherit it through the thread).
        """
        if self._config.subtasks:
            node = self._nodes.get(f'{sub.id}-plan')
            plan = node.output if node else None
            p = PurePosixPath(self._plan_artifact_path())
            return plan, str(p.with_name(f'{p.stem}-{sub.id}{p.suffix}'))
        plan = self._plan_of_record if first else None
        return plan, self._plan_artifact_path()

    def _record_decisions(self, plan: str | None) -> None:
        """
        Record what this module settled that binds the later ones.

        :param plan: The module's approved plan of record.
        """
        sub = self._active_subtask
        if sub is None:
            return
        seen = {text for _module, text in self._decisions}
        fresh = [d for d in parse_decisions(plan) if d not in seen]
        if not fresh:
            return
        self._decisions.extend((sub.id, d) for d in fresh)
        click.echo(
            f'[plan] [{sub.id}] recorded {len(fresh)} decision(s) '
            f'binding on later modules.'
        )

    def _seed_decisions_from_ledger(self, worktree: str) -> None:
        """
        Load decisions this campaign recorded in an EARLIER run.

        `_decisions` is otherwise filled only from run state, which is
        per-run — so a project that builds one module per run started
        every planner with an empty ledger and carried nothing forward,
        while the committed document sat in the worktree unread
        (TASKS.md #75). The repo is the thing that survives a run; this
        is what reads it back.

        Deduplicated on text against what is already held, so a resumed
        run that has the decisions in its state does not double them.
        Best-effort: an unreadable or heading-less ledger seeds nothing
        and the run proceeds exactly as it did before, because a missing
        ledger is the normal first-module case.

        :param worktree: The planner node's clone, cut from the repo and
            therefore carrying whatever the last module published.
        """
        if not self._config.subtasks:
            return
        try:
            text = self._wt.read_tracked_file(
                worktree, self._decisions_ledger_path()
            )
        except click.ClickException:
            return
        seen = {held for _module, held in self._decisions}
        fresh: list[tuple[str, str]] = []
        for module, item in parse_decisions_doc(text):
            if item in seen:
                continue
            seen.add(item)
            fresh.append((module, item))
        if not fresh:
            return
        self._decisions.extend(fresh)
        modules = sorted({module for module, _ in fresh})
        click.echo(
            f'[plan] carried {len(fresh)} decision(s) forward from '
            f'{", ".join(f"[{m}]" for m in modules)} — recorded by an '
            f'earlier run and read back from '
            f'{self._decisions_ledger_path()}.'
        )

    def _record_referrals(self) -> None:
        """
        Record what this chunk's reviewers addressed to a later module.

        Deduplicated on text, like :meth:`_record_decisions`: both
        reviewers noticing the same thing, and a reviewer re-raising it
        each round, are the normal case, and the later planner needs the
        observation once rather than six times. That is a different
        judgement from :func:`finding_id`'s, which keeps duplicates
        BECAUSE a re-raise after a round of work is its own event — here
        the audience is a planner reading a ledger, not a human auditing
        a review.
        """
        sub = self._active_subtask
        if sub is None:
            return
        seen = {text for _module, text in self._referrals}
        fresh: list[str] = []
        for rec in self._chunk_reviews():
            for text in rec.later_increment:
                if text not in seen:
                    seen.add(text)
                    fresh.append(text)
        if not fresh:
            return
        self._referrals.extend((sub.id, text) for text in fresh)
        click.echo(
            f'[findings] [{sub.id}] routed {len(fresh)} observation(s) '
            f'to the decisions ledger for a later module.'
        )

    def _referrals_by_module(self) -> list[tuple[str, list[str]]]:
        """The recorded referrals grouped by module, in module order."""
        grouped: dict[str, list[str]] = {}
        for module_id, text in self._referrals:
            grouped.setdefault(module_id, []).append(text)
        return list(grouped.items())

    def _decisions_by_module(self) -> list[tuple[str, list[str]]]:
        """The recorded decisions grouped by module, in module order."""
        grouped: dict[str, list[str]] = {}
        for module_id, text in self._decisions:
            grouped.setdefault(module_id, []).append(text)
        return list(grouped.items())

    def _decisions_block(self) -> str:
        """The decisions ledger, as a block for a planner turn."""
        titles = {s.id: s.title for s in self._subtasks}
        parts = []
        for module_id, items in self._decisions_by_module():
            head = f'[{module_id}] {titles.get(module_id, "")}'.strip()
            body = '\n'.join(f'  - {text}' for text in items)
            parts.append(f'{head}\n{body}')
        referrals = self._referrals_by_module()
        if referrals:
            parts.append(
                'Raised by REVIEW for a later module. These are '
                'observations, NOT decisions: nothing here binds you, '
                'and you may judge any of them wrong. Say what you '
                'decided about the ones in your scope.'
            )
            for module_id, items in referrals:
                head = f'raised in [{module_id}]'
                body = '\n'.join(f'  - {text}' for text in items)
                parts.append(f'{head}\n{body}')
        return '\n'.join(parts)

    def _decisions_doc(self) -> str:
        """The decisions ledger as the committed Markdown document."""
        titles = {s.id: s.title for s in self._subtasks}
        lines = [
            f'# Decisions carried forward — {self._config.name}',
            '',
            "Recorded by each module's approved plan as BINDING on the "
            'modules that follow. A later module designs against these; '
            'a genuine need to change one is a halt-and-escalate to the '
            'human, never a silent edit.',
        ]
        for module_id, items in self._decisions_by_module():
            title = titles.get(module_id, '')
            lines += ['', f'## [{module_id}] {title}'.rstrip(), '']
            lines += [f'- {text}' for text in items]
        referrals = self._referrals_by_module()
        if referrals:
            lines += [
                '',
                '# Raised by review, for a later module',
                '',
                'A reviewer noticed these and said they belong to a '
                'module that had not been planned yet, so they are '
                'routed here rather than filed as issues addressed to '
                'nobody. They are NOT binding, unlike everything above: '
                'the module that owns one decides what to do about it, '
                'and deciding it is wrong is a legitimate outcome.',
            ]
            for module_id, items in referrals:
                title = titles.get(module_id, '')
                head = f'## Raised in [{module_id}] {title}'.rstrip()
                lines += ['', head, '']
                lines += [f'- {text}' for text in items]
        return '\n'.join(lines) + '\n'

    def _findings_ledger_path(self) -> str:
        """
        The findings ledger's repo path — ONE per pipeline.

        Deliberately not derived from the chunk's plan path the way the
        reviews and selection records are. Those are per-chunk on
        purpose; this must accumulate across every chunk AND every run,
        because the repo is the only thing that survives a run and each
        chunk's branch carries the ledger forward on its base.

        :returns: e.g. ``docs/plans/discover-findings.md``.
        """
        p = PurePosixPath(self._plan_artifact_path())
        return str(p.with_name(f'{p.stem}-findings{p.suffix}'))

    def _publish_findings(
        self, winner: str, report_doc: str | None
    ) -> None:
        """
        Put this chunk's non-blocking findings where a human looks.

        A tracker when the pipeline publishes to GitHub, the
        committed file otherwise. The file was the original design
        and the wrong shape: a chunk's branch is cut from the prior
        IMPLEMENTATION tip, so it never carried that chunk's docs
        commits, so the ledger restarted each time — and two chunks
        adding the same path collide on merge (TASKS.md #58). An issue
        tracker is cross-branch and cross-campaign by construction.

        The file survives for ``publish: local`` and ``mode: none``,
        where there is no tracker to file into.

        :param winner: The node whose branch publishes.
        :param report_doc: Repo path of the full reviewer reports.
        """
        records = [r for r in self._chunk_reviews() if r.findings]
        if not records:
            return
        if self._config.publish.mode == 'pr' and self._publish_repo:
            self._file_findings_as_issues(records, report_doc)
        else:
            self._commit_findings_ledger(winner, report_doc)

    def _file_findings_as_issues(
        self, records: list[ReviewRecord], report_doc: str | None
    ) -> None:
        """
        File each new finding as an issue, skipping ones already there.

        Dedup is on the positional id, matched against every issue body
        OPEN AND CLOSED. Closed matters most: an issue a human closed is
        a finding they have dealt with, and re-filing it next run is the
        one behaviour that would make the tracker worthless.

        An unreadable tracker files NOTHING. That is deliberate — a
        listing failure means every id looks absent, and filing on that
        basis is exactly how a tracker fills with duplicates. The
        findings are still in the reviewer report on the branch.

        :param records: This chunk's votes that raised findings.
        :param report_doc: Repo path of the full reviewer reports.
        """
        assert self._publish_repo is not None
        seen = self._wt.issue_bodies_text(self._publish_repo)
        if seen is None:
            click.echo(
                '[findings] could not read the issue tracker; filing '
                'nothing rather than risking duplicates. The findings '
                'are in the reviewer report on the branch.'
            )
            return
        raised = failed = 0
        withheld: list[tuple[ReviewRecord, str, str, Disposition]] = []
        for rec in records:
            for index, finding in rec.filed_findings():
                ident = finding_id(rec, index)
                if finding_marker(ident) in seen:
                    continue
                verdict = self._dispositions.get(ident)
                if verdict is not None and not verdict.files:
                    # A verifier read this against the shipping code and
                    # concluded it needs nobody. It is summarised below
                    # with its reason rather than filed — and it is NOT
                    # marked seen, so a later run that verifies
                    # differently can still raise it.
                    withheld.append((rec, ident, finding, verdict))
                    continue
                raised += 1
                title, body = render_finding_issue(
                    rec, finding, index,
                    run_id=self._run_id, report_doc=report_doc,
                )
                url = self._wt.create_issue(
                    self._publish_repo, title=title, body=body,
                    label=_FINDING_LABEL,
                )
                if url:
                    self._filed_findings.append((url, title))
                else:
                    failed += 1
        if withheld:
            self._file_triage_summary(withheld, report_doc)
        if not raised:
            return
        if failed:
            click.echo(
                f'[findings] filed {raised - failed} of {raised} '
                f'finding(s); {failed} could not be filed and remain in '
                f'the reviewer report on the branch.'
            )
        else:
            click.echo(
                f'[findings] filed {raised} non-blocking finding(s) as '
                f'issues — tracked for triage, nothing was changed.'
            )

    def _file_triage_summary(
        self,
        withheld: list[tuple[ReviewRecord, str, str, Disposition]],
        report_doc: str | None,
    ) -> None:
        """
        One issue naming every finding the gate kept out, and why.

        The gate's whole risk is that it withholds something it should
        not have, so a withheld finding gets MORE visible provenance
        than a filed one, not less: the verdict, the verifier's reason,
        the reviewer who raised it, and the positional id, in one issue
        a human can read in a minute and reopen from.

        Deliberately one issue and not N. N is the cost this change
        exists to remove; zero is a silent drop, which is the one
        outcome worse than N.

        :param withheld: ``(record, id, text, disposition)`` each.
        :param report_doc: Repo path of the full reviewer reports.
        """
        assert self._publish_repo is not None
        chunk = withheld[0][0].chunk or self._run_id
        by_verdict: dict[str, int] = {}
        for _rec, _ident, _text, verdict in withheld:
            by_verdict[verdict.verdict] = (
                by_verdict.get(verdict.verdict, 0) + 1
            )
        tally = ', '.join(
            f'{count} {name}' for name, count in sorted(by_verdict.items())
        )
        lines = [
            f'A verification pass read {len(withheld)} non-blocking '
            f'finding(s) against the code that shipped for `{chunk}` and '
            f'concluded each needed no issue of its own: {tally}.',
            '',
            'Nothing here was discarded. Each is reproduced below with '
            'the verdict and the reason behind it, and each is still in '
            'the full reviewer report. **If a verdict looks wrong, the '
            'finding is still live** — reopen it by filing it from here; '
            'the runner did not mark any of these as seen, so a later '
            'run that concludes differently will raise it again.',
            '',
        ]
        for rec, ident, text, verdict in withheld:
            lines += [
                f'### `{verdict.verdict}` — {text}',
                '',
                f'- Raised by `{rec.reviewer}` in round {rec.round_no}'
                f' against `{rec.target or chunk}`',
                f'- Verifier said: {verdict.reason or "_no reason given_"}',
                f'- Finding id: `{ident}`',
                '',
            ]
        if report_doc:
            lines += [
                f'Full reviewer reports, including every finding whether '
                f'filed or not: `{report_doc}`.',
            ]
        url = self._wt.create_issue(
            self._publish_repo,
            title=(
                f'[{chunk}] {len(withheld)} finding(s) verified against '
                f'the code and withheld'
            ),
            body='\n'.join(lines),
            label=_FINDING_LABEL,
        )
        if url:
            self._filed_findings.append((url, f'[{chunk}] triage summary'))
        click.echo(
            f'[findings] withheld {len(withheld)} finding(s) — {tally}; '
            f'summarised in one issue with the reason for each.'
        )

    def _commit_findings_ledger(
        self, winner: str, report_doc: str | None
    ) -> str | None:
        """
        Append this chunk's non-blocking findings to the ledger.

        Reads what is already on the branch and adds to it — see
        :func:`render_findings_ledger` for why nothing is ever
        rewritten.
        Best-effort like the other records: worth having, never worth
        failing a publish over.

        :param winner: The node whose branch publishes.
        :param report_doc: Repo path of the full reviewer reports.
        :returns: The committed path, or ``None`` when nothing was new.
        """
        records = [r for r in self._chunk_reviews() if r.findings]
        if not records:
            return None
        path = self._findings_ledger_path()
        tree = self._wt.node_worktree_path(self._run_id, winner)
        try:
            existing = self._wt.read_tracked_file(tree, path)
        except click.ClickException:
            return None
        doc = render_findings_ledger(
            records,
            title=f'Findings — {self._config.name}',
            existing=existing,
            report_doc=report_doc,
        )
        if doc is None:
            # Every finding is already on the branch — a re-published
            # chunk, or a resume. Writing nothing is the correct no-op.
            return None
        try:
            self._wt.write_tracked_file(tree, path, doc)
            committed = self._wt.commit_node(
                self._run_id, winner,
                message='docs: record non-blocking reviewer findings',
                author='reviewer <reviewer@pipeline.local>',
            )
        except click.ClickException:
            return None
        if not committed:
            return None
        added = sum(len(r.findings) for r in records)
        click.echo(
            f'[findings] appended up to {added} non-blocking finding(s) '
            f'to {path} — tracked for triage, nothing was changed.'
        )
        return path

    def _decisions_ledger_path(self) -> str:
        """
        The decisions ledger's repo path — ONE per pipeline.

        :returns: e.g. ``docs/plans/ingestion-decisions.md``.
        """
        artifact = PurePosixPath(self._plan_artifact_path())
        return str(
            artifact.with_name(
                f'{artifact.stem}-decisions{artifact.suffix}'
            )
        )

    def _commit_decisions_ledger(self, winner: str) -> None:
        """
        Commit the accumulated decisions onto a published module.

        The ledger rides the campaign thread, so every later module's
        worktree carries every decision recorded before it — and the
        human reviewing the PRs gets one document instead of having to
        reconstruct the reasoning from N separate plans.

        :param winner: The node whose branch this module publishes.
        """
        if not (
            self._config.subtasks
            and (self._decisions or self._referrals)
        ):
            return
        self._wt.write_tracked_file(
            self._wt.node_worktree_path(self._run_id, winner),
            self._decisions_ledger_path(),
            self._decisions_doc(),
        )
        self._wt.commit_node(
            self._run_id, winner,
            message='docs: record decisions binding later modules',
            author='planner <planner@pipeline.local>',
        )

    def _chunk_preamble(self) -> str:
        """A directive scoping a builder turn to the active chunk."""
        sub = self._active_subtask
        if sub is None:
            return ''
        # Claiming prior work on the FIRST increment sends an agent
        # hunting for artifacts that cannot exist, and a careful one
        # then (rightly) refuses to build against the missing baseline.
        prior = (
            'Nothing has been built yet — this is the first increment, '
            'so the repo is at its base state; do not go looking for '
            'earlier work.'
            if self._active_is_first
            else 'Earlier increments are already implemented in your '
            'worktree; later ones come afterwards.'
        )
        # The title is NOT repeated here: the table below carries it,
        # marked, and a plan whose rows are full scope paragraphs would
        # otherwise print the active one twice in every single turn.
        return (
            f'This is build increment [{sub.id}] of a multi-part plan. '
            f'{prior} Do ONLY this increment now — not the '
            'whole plan.\n\nThe full increment list, in order — the '
            f'marked row is yours:\n\n{self._increment_table(sub)}\n\n'
            'The Task below is the contract for the WHOLE plan, not for '
            'your increment alone. A requirement it states that another '
            "row owns is that row's job — not yours, and NOT missing."
            '\n\n'
        )

    def _increment_table(self, sub: pipeline.Subtask) -> str:
        """
        The ordered increment list, the active row marked.

        Every role — writer, reviewer and judge alike — is handed the
        WHOLE plan's brief as its contract, with the active increment as
        a one-line qualifier above it. A reviewer reading a 15,000-word
        brief that says "pagination on every listing call" and seeing an
        unpaginated call has, from where it stands, found an unmet
        requirement that nobody owns; blocking on it is the faithful
        move. Observed live on `gcp-scope-topology-1`: the [topology]
        bug reviewer blocked on `ListServiceAccounts` pagination, which
        the plan assigns to [identities], and the writer — for whom a
        blocking finding is not optional — implemented it. The judge
        then scored that as a strength.

        Showing the other rows converts "nobody has done this" into
        "row [identities] owns this", which is the only form a reviewer
        can reasonably let pass. Exactly the reasoning behind
        :meth:`_module_table` one level up, where a planner shown only
        its own row designed its neighbours' scope into its module.

        :param sub: The increment being worked.
        :returns: One ``[id] title`` row per increment: ``✓`` for one
            already published, ``▶`` for the active one, blank for one
            still to come.
        """
        rows = []
        for other in self._subtasks:
            if other.id == sub.id:
                mark = '▶'
            elif other.id in self._completed_chunks:
                mark = '✓'
            else:
                mark = ' '
            rows.append(f'    {mark} [{other.id}] {other.title}')
        return '\n'.join(rows)

    def _dispose_prewarmed(self) -> None:
        """Tear down writers pre-warmed during planning (campaign)."""
        for node_id in self._prewarmed:
            node = self._nodes.pop(node_id, None)
            if node and node.session:
                try:
                    self._sc.dispose(node.session)
                except SwarmSessionError:
                    pass
        self._prewarmed.clear()

    def _refresh_build_cache(self, node_id: str) -> None:
        """
        Update the warm build cache from a node that just finished.

        Best-effort and quiet unless something was actually cached:
        the cache is an optimization, and a run must never fail — or
        get noisier — because one did not take.

        :param node_id: The node whose worktree to refresh from.
        """
        if not self._config.build_cache:
            return
        # No existence check here: refresh_build_cache already skips
        # every entry it cannot find, so a second guard would only make
        # this untestable without touching the real filesystem.
        names = self._wt.refresh_build_cache(
            self._wt.node_worktree_path(self._run_id, node_id)
        )
        if names:
            click.echo(
                f'[cache] {node_id}: refreshed the warm build cache '
                f'({", ".join(names)}) — the next node starts from it '
                f'instead of compiling from clean.'
            )

    def _parallel(
        self, label: str, nodes: list[tuple[str, Callable[[], None]]]
    ) -> None:
        """
        Drive independent nodes of one stage at the same time.

        Falls through to a plain call for a single node, so a pipeline
        that declares no parallelism runs exactly as it did.

        EVERY node is awaited even after one fails. Returning early
        would leave the others driving turns in microVMs nothing is
        tracking any more — orphaned guests are the most expensive
        failure this launcher has, and a slow clean failure beats a
        fast dirty one. The first failure observed is the one raised;
        the rest are reported so a run that lost two nodes does not
        look like it lost one.

        :param label: Stage id, used to name the worker threads.
        :param nodes: ``(node_id, thunk)`` pairs to drive.
        :raises BaseException: The first failure any node raised.
        """
        if len(nodes) < 2:
            for _node_id, run in nodes:
                run()
            return
        click.echo(
            f'[parallel] {label}: driving '
            f'{", ".join(n for n, _ in nodes)} at the same time.'
        )
        failures: list[tuple[str, BaseException]] = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(len(nodes), _MAX_PARALLEL_NODES),
            thread_name_prefix=f'node-{label}',
        ) as pool:
            futures = {
                pool.submit(run): node_id
                for node_id, run in nodes
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except BaseException as exc:
                    failures.append((futures[future], exc))
        if not failures:
            return
        for node_id, exc in failures[1:]:
            click.echo(f'[parallel] {label}: {node_id} also failed: {exc}')
        raise failures[0][1]

    def _forfeit_blocked_reviews(
        self,
        stage: pipeline.PipelineStage,
        blocked: list[_Blocked],
    ) -> bool:
        """
        Drop the candidates whose reviews blocked, if a sibling cleared.

        Two writers are run so a judge can choose between them on
        evidence. Before this, a review that spent its round budget
        raised `_Blocked` all the way out of the run — so a campaign
        could end with one candidate approved by every reviewer,
        gate-verified and unpublished, because the OTHER one could not
        converge. Seen twice: once with a candidate that was never
        reviewed at all, and once with a candidate approved on round one
        discarded beside a sibling that blocked four times.

        A blocked review is a forfeit by that candidate, not a
        verdict on the field. If at least one sibling review cleared,
        the run continues with the survivors and the judge picks
        among them.

        Returns True when the stage may proceed. When NO sibling
        cleared, returns False and the caller re-raises — nothing was
        vetted, so there is nothing to judge.

        :param stage: The parallel parent whose sub-stages are reviews.
        :param blocked: The `_Blocked` raised by its sub-stages.
        :returns: Whether the run may continue without these candidates.
        """
        blocked_ids = {exc.stage_id for exc in blocked}
        survivors = [
            sub.id
            for sub in stage.parallel
            if sub.id not in blocked_ids
            and (node := self._nodes.get(sub.id)) is not None
            and node.kind == 'review'
            and node.verdict != 'BLOCKING'
        ]
        if not survivors:
            return False
        for exc in blocked:
            sub = self._stage_by_id.get(exc.stage_id)
            target = sub.on_block if sub is not None else None
            self._forfeited.add(exc.stage_id)
            if target:
                self._forfeited.add(target)
            click.echo(
                f'[forfeit] {exc.stage_id}: no consensus after '
                f'{exc.rounds} round(s), so '
                f'{target or "its candidate"} is withdrawn. '
                f'{", ".join(survivors)} cleared, so the run continues '
                f'with the remaining candidate(s) — the judge is now '
                f'choosing from a REDUCED field, which is recorded with '
                f'the selection.'
            )
        return True

    def _exec_stage(self, stage: pipeline.PipelineStage) -> None:
        if stage.parallel:
            # `sub=sub` binds each node NOW; a bare closure over the
            # loop variable would hand every thread the last node.
            try:
                self._parallel(
                    stage.id,
                    [
                        (sub.id, lambda sub=sub: self._exec_stage(sub))
                        for sub in stage.parallel
                    ],
                )
            except _Blocked as exc:
                # Only a review gate raises this, and only one
                # candidate's review is in each sub-stage. `_parallel`
                # re-raises the first failure, so recover every
                # blocked sibling from the recorded verdicts rather
                # than that one exception.
                blocked = [exc] + [
                    _Blocked(sub.id, 0)
                    for sub in stage.parallel
                    if sub.id != exc.stage_id
                    and (node := self._nodes.get(sub.id)) is not None
                    and node.verdict == 'BLOCKING'
                ]
                if not self._forfeit_blocked_reviews(stage, blocked):
                    raise
            return
        if stage.id in self._completed:
            click.echo(f'[resume] skipping {stage.id} (already complete)')
            return
        kind = self._stage_kind(stage)
        # The cap counts nodes being DRIVEN, so it is charged to
        # whatever drives one — never to a container. A review stage
        # drives no turn of its own; each reviewer turn takes its own
        # slot (_review_turn). Charging the stage as well would spend
        # the cap on threads doing no work, and two review stages
        # running together would leave only half of it for the four
        # reviewers. It also keeps the cap deadlock-free at any nesting
        # depth: only leaves acquire, and a leaf never waits on
        # another leaf's slot.
        slot: contextlib.AbstractContextManager[object] = (
            contextlib.nullcontext() if kind == 'review'
            else self._node_slots
        )
        # Spans the release below so a node that DOES hand its guest
        # back (a reader, a judge) has done so before the next node is
        # let in. A writer's guest deliberately outlives its slot: a
        # review block may re-drive it, and re-booting costs more than
        # an idle VM.
        with slot:
            if kind == 'writer':
                self._run_writer(stage)
            elif kind == 'judge':
                self._run_judge(stage)
            elif kind == 'verify':
                self._run_verify(stage)
            elif kind == 'review':
                self._run_review(stage)
            else:
                self._run_reader(stage)
            with self._lock:
                self._completed.add(stage.id)
            # BEFORE the session release, so the sample sees the
            # worktree while the node is still whole. Off unless asked.
            self._sample_disk(f'stage-complete:{stage.id}')
            # Hand this node's build directory to the ones that follow.
            # Only a COMPLETED stage refreshes it, so a node that died
            # mid-build cannot leave a torn cache behind. A node with
            # nothing built (a reader, a :ro reviewer) is a no-op.
            self._refresh_build_cache(stage.id)
            self._release_completed_session(stage, kind)
        # Checkpoint AS the run advances: a crash then costs only the
        # node that failed, not the approved plan behind it.
        self._save_state()

    @staticmethod
    def _stage_kind(stage: pipeline.PipelineStage) -> str:
        if stage.write:
            return 'writer'
        if stage.selects:
            return 'judge'
        if stage.verifies:
            return 'verify'
        if stage.gate or stage.on_block:
            return 'review'
        return 'reader'

    # ── provision-only ────────────────────────────────────────────

    def _provision_only(self) -> RunResult:
        """
        Provision every node's worktree + session; keep the VMs up.

        Drives no turns and commits nothing — it stands the topology up
        so a human/coordinator can drive it. Branch handoffs (``from``/
        ``needs`` inheritance) are seeded at provision time, so an
        un-driven upstream seeds from the base; drive top-to-bottom.

        :returns: A ``'provisioned'`` :class:`RunResult` with
            :attr:`RunResult.bindings`.
        :raises Exception: Re-raised after tearing down a partial
            provisioning.
        """
        try:
            for stage in self._config.stages:
                self._provision_stage(stage)
        except Exception:
            self._teardown(preserve_run=True)
            raise
        return RunResult(
            run_id=self._run_id,
            status='provisioned',
            nodes=dict(self._nodes),
            bindings=list(self._bindings),
        )

    def _provision_stage(self, stage: pipeline.PipelineStage) -> None:
        if stage.parallel:
            for sub in stage.parallel:
                self._provision_stage(sub)
            return
        kind = self._stage_kind(stage)
        if kind == 'writer':
            self._provision_node(stage, 'writer', 'rw')
        elif kind == 'reader':
            self._provision_node(stage, 'reader', 'ro')
        elif kind == 'review':
            target_wt = self._nodes[self._review_target(stage)].worktree
            assert target_wt is not None
            for reviewer in stage.run:
                label = f'{stage.id}-{reviewer}'
                session = self._create_session(
                    reviewer, target_wt, 'ro', label
                )
                self._bind(label, reviewer, session, target_wt, 'ro')
            self._nodes[stage.id] = NodeResult(stage.id, 'review')
        else:  # judge
            candidates = self._judge_candidates(stage)
            if not candidates:
                return
            wt = self._wt.create_judge_worktree(
                self._run_id, stage.id, candidates,
                replace=self._resume,
            )
            session = self._create_session(
                stage.run[0], wt, 'ro', stage.id
            )
            self._nodes[stage.id] = NodeResult(
                stage.id, 'judge', worktree=wt, session=session
            )
            self._bind(stage.id, stage.run[0], session, wt, 'ro')

    def _provision_node(
        self, stage: pipeline.PipelineStage, kind: str, mode: str
    ) -> None:
        agent = stage.run[0]
        wt = self._wt.create_node_worktree(
            self._run_id, stage.id, from_node=self._seed_from(stage),
            replace=self._resume,
        )
        session = self._create_session(agent, wt, mode, stage.id)
        branch = (
            self._wt.node_branch(self._run_id, stage.id)
            if kind == 'writer'
            else None
        )
        self._nodes[stage.id] = NodeResult(
            stage.id, kind, branch=branch, worktree=wt, session=session
        )
        self._bind(stage.id, agent, session, wt, mode)

    def _bind(
        self, node: str, agent: str, session: str, worktree: str, mode: str
    ) -> None:
        self._bindings.append(
            {
                'node': node,
                'agent': agent,
                'session': session,
                'worktree': worktree,
                'mode': mode,
            }
        )

    # ── node executors ────────────────────────────────────────────

    def _verify_instruction(
        self, pending: list[tuple[ReviewRecord, int, str]]
    ) -> str:
        """
        Ask for a conclusion on each raised finding, against the tree.

        The findings are NUMBERED here and answered by number. A
        echoing `topology/impl-a/bugs/r4#3` back correctly is a
        transcription task with a silent failure mode — a mistyped id
        re-files the finding it was meant to close.

        :param pending: ``(record, index, text)`` in presentation order.
        :returns: The turn's instruction.
        """
        listing = '\n'.join(
            f'{position}. [raised by {rec.reviewer} in round '
            f'{rec.round_no} against {rec.target or "this module"}] '
            f'{text}'
            for position, (rec, _index, text) in enumerate(
                pending, start=1
            )
        )
        return (
            'Your mount is the code that is about to ship. Below are the '
            "NON-BLOCKING findings this module's reviewers raised "
            'alongside their approvals. Nobody has acted on them, and '
            'each one that reaches the tracker costs a human a triage.\n\n'
            'Check each against the code IN YOUR MOUNT and say what you '
            'found. Read the file. Run something if it settles it. A '
            "conclusion you reached by reasoning about the finding's own "
            'wording, without opening the code, is worth nothing here — '
            'that is exactly the triage you are supposed to save.\n\n'
            f'{listing}\n\n'
            'End your reply with a `DISPOSITIONS:` block, one line per '
            'finding, `- <number>: <verdict> — <what you checked>`, '
            'using one of these verdicts:\n\n'
            '`reproduces` — it is real and present in this code.\n'
            '`absent` — it is not true of this code. Say what you looked '
            'at. A finding raised against an earlier round, or against '
            'the candidate that lost, often lands here.\n'
            '`recorded` — real, but the codebase already documents it '
            'deliberately: a docstring, a plan of record, a named test. '
            'CITE THE PLACE, file and line. Not "this is known" — where.\n'
            '`decided` — it argues against a decision already recorded '
            'in the repository. Cite that decision.\n'
            '`duplicate` — the same defect as another number in this '
            'list. Name which.\n\n'
            'Only `reproduces` is filed. The other four are recorded '
            'with your reason and read by a human later, so the reason '
            'IS the deliverable: a withheld finding is one nobody will '
            'look at again, and your sentence is all that stands behind '
            'that. When you cannot tell, say `reproduces` — a wasted '
            'triage is cheap and a lost defect is not. Omit a number '
            'entirely and it is filed, which is the same safe outcome.'
        )

    def _pending_findings(self) -> list[tuple[ReviewRecord, int, str]]:
        """
        What this chunk's reviewers raised that would be filed.

        Referrals are already routed to the decisions ledger, so the
        verifier is not asked about them: it would be checking an
        observation against the wrong module's code.

        :returns: ``(record, index, text)``, in review order.
        """
        return [
            (rec, index, text)
            for rec in self._chunk_reviews()
            for index, text in rec.filed_findings()
        ]

    def _run_verify(self, stage: pipeline.PipelineStage) -> None:
        """
        Check each raised finding against the code before it is filed.

        The gate exists because most non-blocking findings need no work
        and a human was deriving that one at a time, days later, without
        the branch in front of them. An agent holding the tree can do it
        while the context is still true.

        FAILS OPEN at every step. No findings, no agent, an empty reply,
        an unreadable one — each leaves the dispositions empty, and an
        empty disposition files everything exactly as before the gate
        existed. The gate may cost a wasted triage; it may not lose a
        finding.

        :param stage: The stage declaring ``verifies:``.
        """
        pending = self._pending_findings()
        if not pending:
            self._completed.add(stage.id)
            return
        agent = stage.run[0]
        wt = self._wt.create_node_worktree(
            self._run_id, stage.id, from_node=self._seed_from(stage),
            replace=self._resume,
        )
        session = self._create_session(agent, wt, 'ro', stage.id)
        out = self._drive(session, self._verify_instruction(pending))
        self._nodes[stage.id] = NodeResult(
            stage.id, 'verify', worktree=wt, session=session, output=out
        )
        verdicts = parse_dispositions(out)
        kept = self._record_dispositions(pending, verdicts)
        click.echo(
            f'[verify] {stage.id}: {len(pending)} finding(s) checked, '
            f'{kept} to file, {len(pending) - kept} withheld with a '
            f'recorded reason.'
        )

    def _record_dispositions(
        self,
        pending: list[tuple[ReviewRecord, int, str]],
        verdicts: dict[int, Disposition],
    ) -> int:
        """
        Store one conclusion per finding, keyed by its positional id.

        :param pending: What the verifier was shown, in the order shown.
        :param verdicts: Its conclusions, by presented position.
        :returns: How many findings still file.
        """
        kept = 0
        for position, (rec, index, _text) in enumerate(pending, start=1):
            verdict = verdicts.get(position)
            if verdict is None or verdict.files:
                kept += 1
            if verdict is not None:
                self._dispositions[finding_id(rec, index)] = verdict
        return kept

    def _run_reader(self, stage: pipeline.PipelineStage) -> None:
        agent = stage.run[0]
        wt = self._wt.create_node_worktree(
            self._run_id, stage.id, from_node=self._seed_from(stage),
            replace=self._resume,
        )
        if self._is_planner(stage):
            # BEFORE the instruction is built: the ledger block is
            # assembled from `_decisions`, so seeding after would hand
            # this planner an empty one.
            self._seed_decisions_from_ledger(wt)
        session = self._create_session(agent, wt, 'ro', stage.id)
        out = self._drive(session, self._reader_instruction(stage))
        node = NodeResult(
            stage.id, 'reader', worktree=wt, session=session, output=out
        )
        self._nodes[stage.id] = node
        if self._is_planner(stage):
            in_module = self._active_subtask is not None
            if self._interactive_plan:
                # The up-front planner posted its questions; the human
                # replies in the UI while these writer VMs boot, so
                # they are warm the instant the plan is approved. A
                # per-module planner runs inside the loop — its
                # (namespaced) writers aren't pre-warmable here, so it
                # just awaits approval.
                if not in_module:
                    self._prewarm_writers()
                node.output = self._await_plan_approval(session, out)
            else:
                # Unattended: an agy planner's reply lags its single
                # turn, so _drive can return empty under load — leaving
                # the plan unshared and uncommitted. Read the SETTLED
                # session for the final plan (the same settled-capture
                # the interactive consolidation turn uses).
                self._sc.wait_for_session_idle(session)
                node.output = self._sc.read_latest_reply(session) or out
            if not in_module:
                # Flat/single-pass: the up-front planner's final output
                # is THE plan of record (shared with builders, committed
                # on publish) and carries the ordered chunk list.
                self._plan_of_record = node.output
                self._subtasks = parse_subtasks(node.output)
                if self._subtasks:
                    ids = ', '.join(s.id for s in self._subtasks)
                    click.echo(
                        f'[plan] {len(self._subtasks)} subtask(s) '
                        f'proposed: {ids}'
                    )
            else:
                # Per-module: this planner's design reaches THIS
                # module's builders via _reader_context (needs the
                # planner node) and is committed per-module in
                # _publish_chunk — it never overwrites the human module
                # list nor the plan of record. What it CAN contribute
                # beyond its own module is the decisions it settled that
                # bind later ones.
                self._record_decisions(node.output)

    def _await_plan_approval(self, session: str, first_reply: str) -> str:
        """
        Block the pipeline until a human approves the plan in the UI.

        The planner asks its questions; a human answers them directly in
        the Omnigent plan session and replies ``APPROVED`` once the plan
        is complete. After approval, the planner is driven ONE more turn
        to emit a clean, consolidated FINAL plan (the back-and-forth
        leaves the last message reading like a chat reply); that becomes
        the plan of record shared downstream and committed to the repo.

        The consolidation reply is captured from the SETTLED session,
        not the turn's streamed reply: a conversational agy planner is
        still replying to the human's APPROVED when the turn fires, and
        its reply lags — trusting the streamed reply captures that prior
        message. So it waits for the session to settle around the turn
        and reads the final assistant message. Falls back to the
        approval text, then the first reply, if nothing was captured.
        """
        click.echo(
            f'[plan] Planner is awaiting your review in the Omnigent UI '
            f'(session {session}). Answer its questions there, then send '
            f'a message whose ENTIRE text is "APPROVED" — a turn that '
            f'says anything else will not release the gate, deliberately. '
            f'The pipeline is blocked until you do.'
        )
        try:
            approved = self._sc.wait_for_plan_approval(session)
            click.echo(
                f'[plan] approval received from the session '
                f'({len(approved or "")} chars of approved plan); '
                f'consolidating.'
            )
        except KeyboardInterrupt:
            # The expensive one. A human sits in this conversation for
            # tens of minutes, and the plan reaches the run state only
            # once the stage COMPLETES — so interrupting here destroys
            # every question, answer and draft with the session.
            self._capture_turn(
                session, 'interrupted (Ctrl-C) awaiting plan approval'
            )
            raise
        except SwarmSessionError:
            # The same loss through a different door. The gate gives up
            # after a long silence, and that path used to propagate
            # straight past the capture above — the one handler written
            # precisely to stop this session being thrown away.
            self._capture_turn(
                session, 'timed out awaiting plan approval'
            )
            raise
        # Let the planner's reply to APPROVED settle so the
        # consolidation turn doesn't race it, drive the turn, then let
        # THAT settle and read the final message (agy's reply lags).
        self._sc.wait_for_session_idle(session)
        self._drive(session, self._plan_consolidation_instruction())
        self._sc.wait_for_session_idle(session)
        consolidated = self._sc.read_latest_reply(session)
        # Never trust the consolidation reply blind: a planner that has
        # already written the plan may just acknowledge the approval.
        plan = select_plan_of_record(
            consolidated,
            self._sc.read_assistant_replies(session, tail=_PLAN_REPLY_TAIL),
            approved,
        )
        if plan is not None and plan is not consolidated:
            click.echo(
                f'[plan] the consolidation turn returned only a short '
                f'reply ({len(consolidated or "")} chars); recovered the '
                f'full plan ({len(plan)} chars) from the session.'
            )
        return plan or approved or first_reply

    def _plan_consolidation_instruction(self) -> str:
        """Ask the approved planner for a standalone plan (+ chunks)."""
        base = (
            'The plan is APPROVED. Output the COMPLETE, consolidated final '
            'design plan now as a single self-contained Markdown document '
            'that folds in every decision from our discussion. No '
            'questions, no "reply APPROVED", no conversational framing — '
            'this document is saved as the plan of record and handed to '
            'the builders. Still prose/design only: do NOT write code or '
            'files.\n\nCarry every artifact you produced earlier in this '
            'session forward VERBATIM: diagrams (mermaid included), '
            'schema and DDL blocks, table definitions, interface '
            'signatures, worked examples. This document is committed to '
            'the repository as the design record and must stand alone — '
            'a reader who never saw this conversation gets only what is '
            'in it. Consolidating means folding in the decisions we '
            'reached, NOT summarizing away detail: a shorter document '
            'that dropped a diagram is a worse one.'
        )
        sub = self._active_subtask
        if sub is not None:
            # Per-module planner: consolidate THIS module's design only.
            # The module list is human-supplied, so no SUBTASKS block —
            # the design plus its work breakdown becomes the module's
            # tests (the TDD writer turns it into the failing suite).
            return base + (
                f'\n\nScope: this is module [{sub.id}] — {sub.title}. '
                'Design ONLY this module, treating the artifacts from '
                'prior modules already in your worktree as frozen, fixed '
                'contracts. Include a concrete work breakdown with '
                'mechanically verifiable done-criteria the test writer '
                'can turn directly into a failing test suite.'
                '\n\nFinally, if this module settled anything that '
                'CONSTRAINS a later module — a contract another module '
                'must consume, a convention, an alternative that was '
                'considered and rejected, an agreement reached with the '
                'human — end with a block headed exactly "DECISIONS FOR '
                'LATER MODULES:" and one "- <decision>" line each, each '
                'written so a planner who was NOT in this conversation '
                'can act on it. Omit the block entirely if this module '
                'settled nothing that binds later work.'
            )
        return base + (
            '\n\nThen, as the FINAL section, output an ordered '
            'implementation plan under a line reading exactly '
            '"SUBTASKS:" — one line per independently-buildable increment, '
            'each formatted "- [<short-id>] <one-line goal>" (e.g. '
            '"- [core] contracts and core skeleton"). Order them by '
            'DEPENDENCY — whatever later increments build on comes first — '
            'and the builders implement them one at a time in sequence.'
            '\n\nSize them deliberately, and err LARGE. Every increment '
            'pays a full build-and-review cycle: roughly a dozen microVMs, '
            'each installing a toolchain and compiling the project from '
            'clean before it does any work of its own. That cost is fixed '
            'per increment and identical whether the increment changes '
            'seven lines or seven hundred — measured on a Rust workspace it '
            "was about two thirds of each increment's wall-clock time. So "
            'choose the LARGEST increment that can still be reviewed '
            'properly in one pass, not the smallest that can be separated: '
            'three increments that are one feature in three parts should '
            'usually be one. Split only for a reason you can name — an '
            'artifact that must be frozen before its consumer is written, a '
            'genuinely independent surface, or an increment so large that '
            'one review could not do it justice. If the work is genuinely a '
            'single increment, list exactly one.'
            # Layer 1 of the tests-only gate. A tests stage that cannot
            # write its suite against the existing public surface has
            # nowhere to go at RUN time: in a compiled language a test
            # naming a type that does not exist fails to build rather
            # than fails, taking every other test with it. Asked here so
            # the human meets it at the approval gate they already sit
            # through, instead of a campaign stopping at 3am. Measured:
            # across this project's history every greenfield module
            # needed new surface and every incremental chunk needed
            # none, so the answer is usually one word.
            '\n\nFor EACH increment, add a second line reading '
            '`SURFACE: none` or `SURFACE: <what must exist first>`. It '
            "answers one question: can that increment's tests be "
            'written against the public API that already exists when it '
            'starts? Adding a new crate, module or public type that '
            'nothing exposes yet means the answer is NOT none — name it. '
            'Everything else — new behaviour behind an existing '
            'interface, a new provider method on a trait that is already '
            'there, anything testable by observing calls and results — '
            'is `SURFACE: none`, which is the common case. This is not a '
            'request to design the API; it is a flag so a human can '
            'decide up front rather than a build stopping halfway.'
        )

    def _provision_writer(
        self, stage: pipeline.PipelineStage
    ) -> NodeResult:
        """
        Create a writer node's worktree + session, WITHOUT driving it.

        Shared by the normal writer path and pre-warming: it stands up
        the clone (seeded from the upstream branch as it stands now) and
        boots + warms the microVM. The node's turn is driven later.

        :param stage: The writer stage to provision.
        :returns: The recorded (un-driven) :class:`NodeResult`.
        """
        agent = stage.run[0]
        wt = self._wt.create_node_worktree(
            self._run_id, stage.id, from_node=self._seed_from(stage),
            replace=self._resume,
        )
        session = self._create_session(agent, wt, 'rw', stage.id)
        node = NodeResult(
            stage.id,
            'writer',
            branch=self._wt.node_branch(self._run_id, stage.id),
            worktree=wt,
            session=session,
        )
        self._nodes[stage.id] = node
        return node

    def _prewarm_writers(self) -> None:
        """
        Boot every writer node's VM now, during the planning wait.

        Called once the interactive planner is awaiting human approval:
        each writer's microVM boots and its TUI warms while the human is
        still planning, so the moment the plan is approved the swarm is
        ready to drive rather than cold-start node by node. Writers
        are provisioned in stage order, so an upstream's branch exists
        before a downstream one seeds ``from`` it (the downstream clone
        starts at the upstream's base and is reseeded onto the real tip
        at drive time — see :meth:`_run_writer`). Judge/review nodes are
        NOT pre-warmed: they are gated on the writers finishing.

        A writer is pre-warmed ONLY when its seed branch already exists.
        Cutting a clone needs ``origin/pl/<run>/<seed>`` to be a real
        commit, and a writer seeded from a JUDGE (e.g. a refactor pass
        over the winner) has no such branch until that judge runs and
        aliases it — pre-warming it fails the whole run with "not a
        commit". Those writers are simply left to provision lazily at
        drive time, by which point the branch exists.
        """
        for stage in pipeline._iter_stages(self._config.stages):
            if (
                self._stage_kind(stage) != 'writer'
                or stage.id in self._nodes
            ):
                continue
            seed = self._seed_from(stage)
            if seed is not None and not self._has_branch(seed):
                continue
            self._provision_writer(stage)
            self._prewarmed.add(stage.id)

    def _has_branch(self, node_id: str) -> bool:
        """Whether *node_id* has a hub branch yet to clone from."""
        node = self._nodes.get(node_id)
        return bool(node and node.branch)

    def _run_writer(self, stage: pipeline.PipelineStage) -> None:
        node = self._nodes.get(stage.id)
        if node is None or node.session is None:
            node = self._provision_writer(stage)
        else:
            # Pre-warmed during planning: the clone was seeded from the
            # upstream's BASE (the upstream had not committed yet), so
            # reseed onto its now-committed tip before driving.
            seed = self._seed_from(stage)
            if seed is not None:
                self._wt.reseed_node_worktree(
                    self._run_id, stage.id, seed
                )
        assert node.session is not None
        try:
            node.output = self._drive(
                node.session, self._writer_instruction(stage)
            )
        except PipelineRunError:
            # The turn failed, but the agent may already have written
            # real work into the worktree — a timeout can land AFTER
            # most of a stage is done. Committing it puts that work on
            # the node's branch, where it is durable and a resume picks
            # it up, instead of dying with the worktree.
            self._commit_partial(stage.id)
            raise
        # A native-TUI writer (agy) keeps writing files after its turn
        # reports idle; wait for the worktree to settle so the commit
        # captures the finished tree, not an empty/partial one.
        self._wt.wait_for_node_settle(self._run_id, stage.id)
        self._commit(stage.id, f'{stage.id}: implement')
        # Before the gates below: both answer a problem by re-driving,
        # and a writer cannot re-drive its way out of a contract it
        # cannot satisfy.
        self._halt_on_writer_dispute(stage)
        if stage.tests_only:
            self._enforce_tests_only(stage)
        self._require_implementation(stage)
        self._last_branch_node = stage.id

    def _seed_ref(self, stage: pipeline.PipelineStage) -> str:
        """The hub ref this writer's branch was cut from."""
        seed = self._seed_from(stage)
        if seed is not None:
            return self._wt.node_branch(self._run_id, seed)
        return self._config.base_branch or 'main'

    def _is_generated(self, path: str) -> bool:
        """Whether *path* is build output rather than implementation."""
        globs = self._config.generated or _GENERATED_GLOBS
        name = path.rsplit('/', 1)[-1]
        return any(
            fnmatch.fnmatch(path, g) or fnmatch.fnmatch(name, g)
            for g in globs
        )

    def _is_test_path(self, path: str) -> bool:
        """Whether *path* is something a tests-only stage may write."""
        globs = (
            self._config.test_paths or _TEST_PATH_GLOBS
        ) + _DEPENDENCY_MANIFEST_GLOBS
        name = path.rsplit('/', 1)[-1]
        return any(
            fnmatch.fnmatch(path, g) or fnmatch.fnmatch(name, g)
            for g in globs
        )

    def _non_test_changes(self, stage: pipeline.PipelineStage) -> list[str]:
        """
        Production files a tests-only stage changed.

        Diffed against the ref the writer was cut from, so this is what
        THIS stage did rather than what its base already carried.
        Generated files (lockfiles) are excused — a test that adds a
        dev-dependency legitimately moves them.

        :param stage: The tests-only writer stage that just committed.
        :returns: Sorted repo-relative paths, or ``[]``.
        """
        try:
            changed = self._wt.node_diff_files(
                self._run_id, stage.id, against=self._seed_ref(stage)
            )
        except click.ClickException:
            # A git hiccup must not fail a stage that may be perfectly
            # fine; the gate is a check, not a source of flakiness.
            return []
        return sorted(
            f for f in changed
            if not self._is_test_path(f) and not self._is_generated(f)
        )

    def _enforce_tests_only(self, stage: pipeline.PipelineStage) -> None:
        """
        Hold a tests-only stage to test code, re-driving if it strays.

        The pipeline's central claim is that two models implement the
        same frozen contract independently and a judge picks between
        them. A tests stage that ships the implementation hands BOTH
        writers one design — the judge then compares two edits of the
        same code, and the second model's independent take never exists.
        The comparison still runs; it stops measuring what it claims to.

        Observed live on `gcp-scope-topology-1`: `identities-tests`
        committed a 114-line `ServiceAccountCache` into
        `providers/gcp/src/service_accounts.rs`. Its instruction already
        said "you must NOT implement the feature yourself or modify any
        production code", and its own test file never referenced the new
        type — the suite compiled and failed against unmodified source,
        so nothing forced it. A prompt is a request; this is the check.

        RE-DRIVEN rather than halted on the first violation, because an
        unattended run should not stop for something the writer can undo
        itself. Nothing is auto-reverted: the runner rewriting an
        agent's commit would leave a suite referencing code the runner
        had just deleted, and the agent is the one that knows which of
        its own changes to keep.

        :param stage: The tests-only stage that just committed.
        :raises PipelineRunError: If it strays a second time.
        """
        stray = self._non_test_changes(stage)
        if not stray:
            return
        listed = '\n'.join(f'  - {path}' for path in stray)
        click.echo(
            f'[tests-only] {stage.id}: changed {len(stray)} production '
            f'file(s); asking it to remove them and keep the tests.'
        )
        self._redrive_writer(
            stage.id,
            listed,
            message=f'{stage.id}: restrict to test code',
            instruction=self._tests_only_instruction(listed),
        )
        stray = self._non_test_changes(stage)
        if not stray:
            click.echo(f'[tests-only] {stage.id}: now test code only.')
            return
        listed = '\n'.join(f'  - {path}' for path in stray)
        raise PipelineRunError(
            f'{stage.id} is a tests-only stage but still changes '
            f'production code after being asked to stop:\n{listed}\n'
            f'Nothing was reverted — the branch is intact for you to '
            f'inspect. Either the tests genuinely need this surface to '
            f'exist (in which case the plan should say so for this '
            f'increment), or the stage is implementing the feature.'
        )

    def _tests_only_instruction(self, listed: str) -> str:
        """
        The turn asking a tests stage to give back its production edits.

        :param listed: The offending paths, one bullet per line.
        :returns: The instruction.
        """
        return (
            'This is a TESTS-ONLY stage, and your commit changed '
            'production code:\n\n' + listed + '\n\nRemove those '
            'changes and keep your tests. A separate implementer writes '
            'the production code to make them pass; two implementers do '
            'it independently and a judge compares them, so anything you '
            'implement here is inherited by BOTH and the comparison '
            'stops meaning anything.\n\nWrite the tests against the '
            'EXISTING public surface — assert on observable behaviour '
            '(calls made, values returned, records produced) rather than '
            'on internals that do not exist yet. That is also the better '
            'contract: it does not presuppose one implementation.\n\n'
            'If a test genuinely CANNOT be written without new '
            'production surface, do not add it. Say so in your reply, '
            'name exactly what is needed and why, and leave that test '
            'out. Stopping to ask is correct here; implementing is not.'
            f'\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
        )

    def _is_guarded(self, path: str) -> bool:
        """Whether *path* is one of the project's declared checks."""
        name = path.rsplit('/', 1)[-1]
        return any(
            fnmatch.fnmatch(path, g) or fnmatch.fnmatch(name, g)
            for g in self._config.guarded
        )

    def _guarded_changes(
        self, stage: pipeline.PipelineStage
    ) -> list[str]:
        """
        Declared checks the branch under review changed.

        Diffed against the ref the writer was cut from, so this is what
        THIS module did, not what the base branch already carried.

        :param stage: The review stage about to run.
        :returns: Sorted repo-relative paths, or ``[]``.
        """
        if not self._config.guarded:
            return []
        target = self._review_target(stage)
        target_stage = self._stage_by_id.get(target)
        if target_stage is None:
            return []
        try:
            changed = self._wt.node_diff_files(
                self._run_id, target, against=self._seed_ref(target_stage)
            )
        except click.ClickException:
            # Never fail a review on a git hiccup; the reviewer still
            # has the tree and the rest of its instruction.
            return []
        return sorted(f for f in changed if self._is_guarded(f))

    def _guarded_block(self, stage: pipeline.PipelineStage) -> str:
        """
        Name the checks this branch changed, for the reviewers.

        A blocking finding closed by editing the thing that produced it
        is the one failure mode that leaves every gate green and no
        trace. Live, m5 appended three advisory ids to an auditor's
        ignore list and deleted the block documenting the one
        pre-existing acceptance; two reviewers then approved the branch
        without mentioning either. Naming the files does not forbid the
        edit — a module whose job IS adding CI must edit CI — it only
        makes passing over one silently impossible.

        :param stage: The review stage about to run.
        :returns: The block, or ``''`` when nothing guarded changed.
        """
        hits = self._guarded_changes(stage)
        if not hits:
            return ''
        listed = '\n'.join(f'  - {path}' for path in hits)
        return (
            '\n\nThis branch CHANGED files this project declares to be '
            'CHECKS — the configuration of a gate, linter, auditor or CI '
            'workflow, or a suppression list one of them reads:\n\n'
            + listed
            + '\n\nEach is a required review item, not background noise. '
            'Diff it and state, explicitly, whether it is a genuine fix '
            'or a way to stop a check reporting. Closing a finding by '
            'adding an ignore or allow-list entry, relaxing a threshold, '
            'disabling a step, or deleting a documented rationale is not '
            'a fix: it removes the thing that would have caught the next '
            'regression, and the correct verdict is BLOCKING. If the '
            'change is legitimate, say so and say why. Approving one of '
            'these without mentioning it in your reply is itself a '
            'review failure.'
        )

    def _require_implementation(
        self, stage: pipeline.PipelineStage
    ) -> None:
        """
        Refuse to pass on a writer that did not actually implement.

        Every gate in this pipeline downstream of a writer is an agent's
        WORD — a reviewer's ``VERDICT:``, a coder's "tests pass". None
        of them executes anything. So a writer that does nothing and
        reports success sails through: observed live, an implementer
        left both crate roots holding the test author's "intentionally
        empty" placeholder, changed only ``Cargo.lock``, and collected
        ``VERDICT: APPROVED`` from two independent reviewers. Only the
        judge caught it, by diffing.

        This is the one check the ORCHESTRATOR can make honestly,
        because it needs no execution: compare the branch to the tree it
        was cut from and require at least one changed file that is not
        build output. Pure git inspection in the trusted plane — running
        the suite instead would mean executing agent-authored code on
        the host, with the publish token in the environment.

        A no-op is re-driven with the specific evidence, up to the
        review round cap, then fails the stage rather than handing an
        empty branch to reviewers who will rubber-stamp it.

        :param stage: The writer stage just committed.
        :raises PipelineRunError: If it never implements anything.
        """
        if not stage.write:
            return
        against = self._seed_ref(stage)
        for _round in range(self._max_rounds):
            try:
                changed = self._wt.node_diff_files(
                    self._run_id, stage.id, against=against
                )
            except click.ClickException:
                return  # cannot tell; never fail a run on a git hiccup
            if [f for f in changed if not self._is_generated(f)]:
                return
            click.echo(
                f'[verify] {stage.id}: no implementation — changed '
                f'{len(changed)} file(s), all build output. Re-driving.'
            )
            self._redrive_writer(
                stage.id,
                self._noop_findings(changed),
                message=f'{stage.id}: implement (retry)',
            )
        raise PipelineRunError(
            f'{stage.id} produced no implementation: after '
            f'{self._max_rounds} attempt(s) its branch still differs '
            f'from {against!r} only in generated files. It reported '
            f'success without writing production code — do not trust a '
            f'downstream review of this branch.'
        )

    def _noop_findings(self, changed: list[str]) -> str:
        """Evidence handed back to a writer that implemented nothing."""
        if changed:
            what = (
                'the only files you changed were generated build output:\n'
                + '\n'.join(f'  - {f}' for f in changed[:20])
            )
        else:
            what = 'you changed NO files at all.'
        return (
            'Your turn did not implement anything. Comparing your branch '
            f'against the tree you started from, {what}\n\n'
            'The failing test suite already in your worktree is the '
            'binding contract: write the production code that makes it '
            'pass. Do not report success, and do not stop, until you '
            'have written real source changes. If something blocks you '
            '(a missing toolchain, an unclear contract), say so '
            'explicitly instead of finishing quietly.'
        )

    def _commit_partial(self, node_id: str) -> None:
        """
        Commit whatever a failed writer left behind; never raise.

        Best-effort by construction: this runs while an error is already
        propagating, so it must not mask that error with one of its own.
        A clean worktree simply commits nothing.

        :param node_id: The writer whose tree to preserve, and the
            author the commit is attributed to — see :meth:`_commit` for
            why the agent's name must not appear here.
        """
        try:
            committed = self._wt.commit_node(
                self._run_id,
                node_id,
                message=f'{node_id}: partial work (turn did not complete)',
                author=f'{node_id} <{node_id}@pipeline.local>',
            )
        except click.ClickException:
            return
        if committed:
            click.echo(
                f'[salvage] {node_id}: the turn failed, but its work was '
                f'committed to the node branch — a resume continues from '
                f'it rather than redoing the stage.'
            )

    def _ensure_writer_session(
        self, stage: pipeline.PipelineStage
    ) -> None:
        """
        Give a restored writer a live session before driving it again.

        A node restored on RESUME deliberately carries no session — the
        VM it named belongs to a process that has exited. But its
        worktree and its committed branch survive, so a review that
        blocks on it (or the verification gate) must still be able to
        loop back. Attach a NEW session to the EXISTING worktree rather
        than re-provisioning: re-cutting the clone would discard exactly
        the work being looped back over.

        Without this, the first blocking review after a resume died on a
        bare ``AssertionError`` — the run's own state having correctly
        refused to record a dead handle.

        :param stage: The writer stage about to be re-driven.
        """
        node = self._nodes[stage.id]
        if node.session is not None or node.worktree is None:
            return
        click.echo(
            f'[resume] {stage.id}: re-attaching a session to its '
            f'existing worktree so the loop-back can run.'
        )
        node.session = self._create_session(
            stage.run[0], node.worktree, 'rw', stage.id
        )

    def _redrive_writer(
        self,
        stage_id: str,
        findings: str,
        *,
        message: str | None = None,
        instruction: str | None = None,
    ) -> None:
        stage = self._stage_by_id[stage_id]
        node = self._nodes[stage_id]
        # The branch is about to change, so "every reviewer ran the
        # suite against this" stops being true. Harmless while it lived
        # only in memory for one stage; now that it is persisted and
        # survives a resume, a stale entry would tell a later judge not
        # to check a branch nobody has checked since.
        self._reviewed_ok.discard(stage_id)
        self._ensure_writer_session(stage)
        assert node.session is not None
        # A loop-back reuses the writer's session so its fix turn keeps
        # the prior review context.
        node.output = self._drive(
            node.session, instruction or self._fix_instruction(findings)
        )
        # Kept even on success: this session is disposed at publish, and
        # what a writer did with a reviewer's findings is exactly what
        # someone reading the pull request later wants to see.
        self._capture_turn(node.session, 'loop-back fix turn')
        self._wt.wait_for_node_settle(self._run_id, stage_id)
        self._commit(
            stage_id, message or f'{stage_id}: address review'
        )

    def _review_turn(
        self,
        stage: pipeline.PipelineStage,
        reviewer: str,
        target_wt: str,
        round_no: int,
        outputs: dict[str, str],
        verdicts: dict[str, str | None],
        created: list[str],
    ) -> None:
        """
        One reviewer's whole turn: boot, review, vote, hand the VM back.

        Extracted so a stage's reviewers can run at the same time (see
        :meth:`_parallel`). They never needed ordering — each mounts
        the same branch read-only and votes independently — and running
        them in series made an increment wait through six reviewer
        turns end to end.

        *outputs* and *verdicts* are written under this reviewer's own
        key and read only once every reviewer has finished, so they
        need no lock of their own.

        :param stage: The review stage.
        :param reviewer: The reviewing agent's name.
        :param target_wt: The reviewed branch's worktree (mounted ro).
        :param round_no: Recorded review round.
        :param outputs: Per-reviewer report text, filled in place.
        :param verdicts: Per-reviewer verdict, filled in place.
        :param created: Collects this reviewer's session so the stage
            frees exactly its own guests. Unlike *outputs*, this IS
            shared with reviewers running concurrently, so it is
            appended under the lock.
        """
        # A reviewer drives its own turn, so it is what the cap counts.
        # Held boot to disposal: a reviewer is freed the moment it
        # votes, so its slot and its guest end together. Blocks rather
        # than failing — a stage declaring more reviewers than the host
        # can hold should run them in waves, not refuse to run.
        with self._node_slots:
            self._review_guest(stage, reviewer, target_wt, round_no,
                               outputs, verdicts, created)

    def _drive_reviewer(
        self,
        stage: pipeline.PipelineStage,
        reviewer: str,
        target_wt: str,
        label: str,
        created: list[str],
    ) -> tuple[str, str]:
        """
        Boot a reviewer and drive its turn, retrying once if it dies.

        A reviewer is the safe thing to retry and the one that keeps
        killing campaigns. Its mount is READ-ONLY, it wrote nothing, and
        no verdict was recorded — so a second attempt costs a turn and
        nothing else. It is also much cheaper than the first: the build
        cache is warm from the attempt that just died.

        Retried on ANY failure rather than only on a diagnosed one. The
        two live cases both reported `failed: None`, which is precisely
        the state where classification is impossible — and the cost of
        guessing wrong is one turn, against a whole campaign for not
        retrying. See :data:`_REVIEW_TURN_ATTEMPTS`.

        :param stage: The review stage.
        :param reviewer: The reviewing agent's name.
        :param target_wt: The reviewed branch's worktree (mounted ro).
        :param label: The node label used in logs.
        :param created: Collects every session booted here, so the stage
            frees the abandoned one too.
        :returns: ``(session, reply)`` for the attempt that succeeded.
        :raises PipelineRunError: If the last attempt also fails.
        """
        for attempt in range(1, _REVIEW_TURN_ATTEMPTS + 1):
            session = self._create_session(reviewer, target_wt, 'ro', label)
            # Recorded the moment it exists, not once the turn succeeds,
            # so a reviewer that dies mid-review still hands its
            # guest to the stage backstop instead of leaking it.
            with self._lock:
                created.append(session)
            try:
                return session, self._drive(
                    session, self._review_instruction(stage)
                )
            except PipelineRunError as exc:
                if attempt == _REVIEW_TURN_ATTEMPTS:
                    raise
                click.echo(
                    f'[review] {label}: attempt {attempt} died '
                    f'({exc}); its mount was read-only and it recorded '
                    f'no verdict, so nothing is lost — booting a fresh '
                    f'guest and reviewing again.'
                )
                self._free_session(
                    session, f'{label}: freeing the guest that died.'
                )
        raise AssertionError('unreachable')

    def _review_guest(
        self,
        stage: pipeline.PipelineStage,
        reviewer: str,
        target_wt: str,
        round_no: int,
        outputs: dict[str, str],
        verdicts: dict[str, str | None],
        created: list[str],
    ) -> None:
        """The turn itself, once a guest slot is in hand."""
        label = f'{stage.id}-{reviewer}'
        session, out = self._drive_reviewer(
            stage, reviewer, target_wt, label, created
        )
        verdict, text = self._reviewer_verdict(session, out, label)
        outputs[reviewer] = text
        verdicts[reviewer] = verdict
        # BEFORE the disposal below: disposing a session deletes its
        # transcript, so this is the last moment the report exists
        # anywhere.
        self._record_review(
            stage.id, reviewer, round_no, verdict, session, text
        )
        # As soon as it votes, not once the stage ends. A reviewer that
        # has voted is holding a full guest away from one still
        # working, and reviewers are told to EXECUTE what they review,
        # so the others need that memory. Observed live while reviewers
        # still ran in series: one voted APPROVED and its idle 6 GB
        # guest stayed up for 1h50m while the remaining reviewer's
        # build thrashed at load 18 with every linker at 0% CPU.
        self._free_session(
            session,
            f'{label}: voted — freeing its microVM now so the '
            f'reviewers still working are not starved.',
        )

    def _run_review(self, stage: pipeline.PipelineStage) -> None:
        target = self._review_target(stage)
        self._reconcile_late_writes(target)
        target_wt = self._nodes[target].worktree
        assert target_wt is not None
        # Local budget only. The RECORDED round number must not come
        # from it: a gate failure re-enters this method with a fresh
        # budget, and reusing the local count labelled every re-review
        # "round 1" — six of them on one branch, which reads as six
        # independent first rounds instead of one block and the
        # re-reviews that followed. It also collided the run-dir
        # filenames, so each re-review overwrote the last.
        rounds = 0
        silent_rounds = 0
        while True:
            outputs: dict[str, str] = {}
            verdicts: dict[str, str | None] = {}
            round_no = self._next_review_round(stage.id)
            created: list[str] = []
            self._parallel(
                stage.id,
                [
                    (
                        f'{stage.id}-{reviewer}',
                        # partial binds every argument NOW. A closure
                        # would capture the enclosing retry loop's
                        # variables by reference and read whatever the
                        # NEXT round rebound them to.
                        functools.partial(
                            self._review_turn, stage, reviewer,
                            target_wt, round_no, outputs, verdicts,
                            created,
                        ),
                    )
                    for reviewer in stage.run
                ],
            )
            # Backstop: anything the per-reviewer release could not free
            # (a delete that failed) is retried here. BY NAME, not by an
            # index mark — review-a and review-b run concurrently, and a
            # mark taken by one slices away the other's live guests.
            self._dispose_sessions(created)
            # A reviewer that never stated a verdict did not vote
            # AGAINST the branch — it failed to review. Those are
            # different failures with different remedies, and
            # conflating them is expensive: the writer gets re-driven
            # over a branch nobody objected to, and is handed the
            # reviewer's own narration in place of findings. Observed
            # live, a coder replied "there is no actionable finding to
            # address — it names no defect, file, line, or assertion"
            # and was right.
            blocking = [r for r, v in verdicts.items() if v == 'BLOCKING']
            silent = [r for r, v in verdicts.items() if v is None]
            if not blocking and not silent:
                self._nodes[stage.id] = NodeResult(
                    stage.id, 'review', verdict='APPROVED'
                )
                # Every reviewer of this branch installed a toolchain
                # and ran the suite against it. Remember that, so a
                # downstream judge is not sent to re-derive it.
                self._reviewed_ok.add(target)
                return
            if not blocking:
                # Nobody objected; the review simply did not conclude.
                # Retry the REVIEWERS, not the writer.
                silent_rounds += 1
                if silent_rounds <= _MAX_SILENT_REVIEW_ROUNDS:
                    click.echo(
                        f'[review] {stage.id}: {", ".join(silent)} '
                        f'stated no verdict and nobody blocked — '
                        f're-running the review rather than re-driving '
                        f'{stage.on_block or "the writer"} over findings '
                        f'that do not exist '
                        f'({silent_rounds}/{_MAX_SILENT_REVIEW_ROUNDS}).'
                    )
                    continue
                # Out of retries. Silence is still BLOCKING — the
                # safe default is intact — but it blocks the RUN. It
                # must never re-drive the writer, because there is
                # nothing to hand it: relaying narration that names no
                # defect, file, line or assertion is precisely the harm
                # this separation exists to stop.
                click.echo(
                    f'[review] {stage.id}: {", ".join(silent)} never '
                    f'stated a verdict, across '
                    f'{_MAX_SILENT_REVIEW_ROUNDS + 1} attempts. Blocking '
                    f'— a review that did not conclude is not a finding '
                    f'anyone can act on. Their reports are in the run '
                    f'directory.'
                )
                self._nodes[stage.id] = NodeResult(
                    stage.id, 'review', verdict='BLOCKING'
                )
                raise _Blocked(stage.id, rounds + 1)
            self._halt_on_dispute(stage, outputs, blocking)
            rounds += 1
            if rounds > self._max_rounds or not stage.on_block:
                self._nodes[stage.id] = NodeResult(
                    stage.id, 'review', verdict='BLOCKING'
                )
                raise _Blocked(stage.id, rounds)
            # ONLY explicit blockers supply findings.
            findings = '\n\n'.join(
                f'[{r}]\n{outputs[r]}' for r in blocking
            )
            self._redrive_writer(stage.on_block, findings)

    def _halt_on_writer_dispute(self, stage: pipeline.PipelineStage) -> None:
        """
        Stop when a WRITER says its own contract cannot be satisfied.

        :meth:`_halt_on_dispute` covers the review path, but a writer
        meets an impossible contract first: it is the party that has to
        satisfy it. Its dispute was parsed by nothing and the run
        carried on to discover the same thing the expensive way.

        Observed on `ingestion-m2-3`. An implementer reported three
        unsatisfiable tests with file:line — a fixture value one test
        required persisted and another forbade, a SQL alias on the
        reserved word ``constraint`` that cannot parse, and a
        frozen-API call with the wrong signature. Every one correct.
        The run continued 51 minutes, reviewed both candidates and
        forfeited with nothing published; the sibling had made that same
        broken test pass by EDITING A FROZEN MODULE, which is the breach
        the frozen boundary exists to prevent. An impossible contract
        does not merely fail a candidate — it pushes one into the
        violation.

        Called AFTER the stage commits, so the disputed work is durable
        and a resume keeps it (that implementer had 2,013 insertions on
        its branch), and BEFORE the tests-only and
        implementation-present gates, which answer a dispute by
        re-driving a writer that cannot act on it.

        :param stage: The writer stage that just finished.
        :raises PipelineRunError: When the writer raised a dispute.
        """
        node = self._nodes.get(stage.id)
        claims = parse_disputes(node.output if node else None)
        if not claims:
            return
        listed = '\n'.join(f'  {claim}' for claim in claims)
        click.echo(
            f'[{stage.id}] HALTING — the writer says the contract it was '
            f'given cannot be satisfied. Re-driving it cannot fix that.'
        )
        raise PipelineRunError(
            f'{stage.id}: the writer raised {len(claims)} dispute(s) — it '
            f'says its own contract is impossible, not that its code is '
            f'wrong:\n\n{listed}\n\nRe-driving {stage.id} cannot '
            f'resolve this: it is not the party that can change what is '
            f'disputed. A contract no implementation can satisfy does not '
            f'just fail this stage — it pressures the next one into '
            f'breaking a frozen boundary to get green. Read the reply, '
            f'settle the contract, and re-run.'
        )

    def _halt_on_dispute(
        self,
        stage: pipeline.PipelineStage,
        outputs: dict[str, str],
        blocking: list[str],
    ) -> None:
        """
        Stop when a blocking reviewer says the CONTRACT cannot be met.

        A BLOCKING verdict means the code is wrong, and re-driving the
        writer is the answer. A dispute means the writer's own contract
        is impossible — a test no implementation can pass, two stages
        disagreeing — and re-driving the writer cannot resolve it: it is
        not the party able to change the thing at fault.

        Without this the run spends its whole review budget relaying an
        unanswerable finding. Both [m2] campaigns did that: 9 and
        10 correct disputes, ignored, four blocked rounds on every
        candidate, and every candidate forfeited with nothing published.

        Only a BLOCKING reviewer's dispute counts. One raised beside an
        APPROVAL is an observation, and the run is not stuck on it.

        :param stage: The review stage that just blocked.
        :param outputs: Each reviewer's reply, by agent name.
        :param blocking: The reviewers that returned BLOCKING.
        :raises PipelineRunError: When a blocking reviewer disputes.
        """
        disputes = [
            (name, claim)
            for name in blocking
            for claim in parse_disputes(outputs.get(name))
        ]
        if not disputes:
            return
        self._nodes[stage.id] = NodeResult(
            stage.id, 'review', verdict='BLOCKING'
        )
        listed = '\n'.join(
            f'  [{name}] {claim}' for name, claim in disputes
        )
        target = stage.on_block or 'the writer'
        click.echo(
            f'[review] {stage.id}: HALTING — a blocking reviewer says the '
            f'contract itself cannot be satisfied. Re-driving {target} '
            f'cannot fix that, so the remaining rounds would be spent '
            f'relaying it.'
        )
        raise PipelineRunError(
            f'{stage.id}: {len(disputes)} dispute(s) raised alongside a '
            f'BLOCKING verdict — a stage says its own contract is '
            f'impossible, not that the code is wrong:\n\n{listed}\n\n'
            f'Re-driving {target} cannot resolve this: it is not the '
            f'party that can change what is disputed. Read the reviewer '
            f'reports, settle the contract, and re-run.'
        )

    def _reconcile_late_writes(self, node_id: str) -> None:
        """
        Commit work that landed after a writer's stage finished.

        A native-terminal writer keeps working after its turn reports
        done AND after the settle-and-commit fires. Observed live: a
        writer committed a 31-line manifest at 23:58 and wrote the
        1080-line implementation at 00:26 — twenty-eight minutes later
        — leaving its branch a stub that a human had to fix by hand. No
        grace at commit time can cover a gap like that, so the branch
        is reconciled with the worktree at the moment it starts to
        matter: before reviewers read it.

        That placement is deliberate. Reviewers mount the WORKTREE
        ``:ro`` while the judge clones the BRANCH, so a divergence
        means the two disagree about what a candidate even is — one
        spends half an hour on code the other will never see.
        Reconciling here makes them agree.

        Best-effort throughout: a reconcile that cannot run must never
        fail a review that would otherwise proceed.

        :param node_id: The writer node about to be reviewed.
        """
        node = self._nodes.get(node_id)
        stage = self._stage_by_id.get(node_id)
        if node is None or node.kind != 'writer' or stage is None:
            return
        try:
            if not self._wt.node_is_dirty(self._run_id, node_id):
                return
            click.echo(
                f'[commit] {node_id}: work landed after its stage '
                f'finished — settling and committing it so the branch '
                f'matches what the reviewers will read.'
            )
            self._wt.wait_for_node_settle(self._run_id, node_id)
            self._commit(node_id, f'{node_id}: late write')
        except click.ClickException as exc:
            click.echo(f'[commit] {node_id}: could not reconcile: {exc}')

    def _reviewer_verdict(
        self, session: str, streamed: str, label: str
    ) -> tuple[str | None, str]:
        """
        Resolve ``(verdict, reply_text)`` for one reviewer turn.

        When the streamed reply already carries a ``VERDICT:`` token it
        is used directly — no added latency. Otherwise the real verdict
        may only be in the SETTLED session, for EITHER harness: an agy
        reviewer streams an opening narration and mirrors its verdict a
        beat later (premature-idle / reply-lag), and a Claude reviewer
        emits its verdict in a FINAL message that can sit behind
        intermediate tool-narration a mid-turn quiescence idle
        mis-captured as the streamed reply (observed live in
        mixed-models). Trusting a no-verdict stream reads that as a
        missing verdict → the gate blocks and loops back spuriously,
        wasting a build fix turn and a reviewer VM each round. So poll
        the SETTLED session (settle once, then re-read the recent
        replies) until a verdict token appears, bounded by a WALL-CLOCK
        deadline (:data:`_VERDICT_POLL_DEADLINE_S`) — not an attempt
        count, because agy's premature idles make each settle-wait
        return in seconds, so a fixed-attempt budget expires long before
        a slow review's verdict lands (~160s under load). ``None`` after
        the deadline stays blocking (safe). The returned text is the
        settled reply when polled (so the reviewer's real findings, not
        an opening narration, are what loop back to the writer).

        :param session: The reviewer session just driven.
        :param streamed: The turn's streamed reply from :meth:`_drive`.
        :param label: The reviewer's node label (for the capture log).
        :returns: ``(verdict, reply_text)`` — verdict is ``'APPROVED'``,
            ``'BLOCKING'``, or ``None``.
        """
        verdict = parse_verdict(streamed)
        if verdict is not None:
            self._log_verdict(label, verdict, 'stream')
            return verdict, streamed
        text = streamed
        self._sc.wait_for_session_idle(session)
        started = time.monotonic()
        deadline = started + _VERDICT_POLL_DEADLINE_S
        marker = self._newest_item_id(session)
        while True:
            settled = self._sc.read_recent_reply_text(session)
            if settled:
                text = settled
            verdict = parse_verdict(settled)
            if verdict is not None:
                self._log_verdict(label, verdict, 'settled')
                return verdict, text
            now = time.monotonic()
            if (
                now >= deadline
                or now - started >= _VERDICT_POLL_CEILING_S
            ):
                break
            time.sleep(_VERDICT_POLL_INTERVAL_S)
            current = self._newest_item_id(session)
            if current != marker:
                # It is still producing output, so it is not silent —
                # restart the silence clock. The ceiling above still
                # bounds a reviewer that talks without ever voting.
                marker = current
                deadline = time.monotonic() + _VERDICT_POLL_DEADLINE_S
        return self._nudge_for_verdict(session, label, text)

    def _newest_item_id(self, session: str) -> str | None:
        """
        Id of the session's newest item, as an activity marker.

        A DELTA, not a snapshot: comparing this across polls is what
        distinguishes a reviewer that is working from one that has gone
        quiet. Status cannot — a settled session reads idle between
        tool rounds — and the item TYPE cannot either, because a
        reviewer mid-build looks identical to one that stopped.

        :param session: The reviewer session.
        :returns: The newest item's id, or ``None`` when the session
            has none or cannot be read.
        """
        try:
            items = self._sc.read_items(session, tail=1)
        except SwarmSessionError:
            return None
        return items[-1].get('id') if items else None

    def _nudge_for_verdict(
        self, session: str, label: str, text: str
    ) -> tuple[str | None, str]:
        """
        Ask a silent reviewer for a verdict before calling it a block.

        See :data:`_VERDICT_NUDGE`: a reviewer that actually executes
        what it reviews can outlive its own turn and stop mid-narration,
        and treating that as a vote against the branch is expensive and
        wrong. A failed nudge is not fatal — it just leaves the safe
        default in place.

        :param session: The reviewer session.
        :param label: Its node label (for the capture log).
        :param text: The best reply text captured so far.
        :returns: ``(verdict, reply_text)``.
        """
        click.echo(
            f'[review] {label}: no verdict yet — asking for one before '
            f'treating it as blocking.'
        )
        try:
            # inline: an agy reviewer would otherwise get this as a
            # pointer to a file it must choose to re-read — which is
            # exactly what it does not do when it thinks it is mid-work.
            nudged = self._drive(session, _VERDICT_NUDGE, inline=True)
        except PipelineRunError:
            self._log_verdict(label, None, 'none')
            return None, text
        verdict = parse_verdict(nudged)
        if verdict is None:
            self._sc.wait_for_session_idle(session)
            settled = self._sc.read_recent_reply_text(session)
            if settled:
                nudged = settled
            verdict = parse_verdict(settled)
        if verdict is None:
            self._log_verdict(label, None, 'none')
            return None, text
        self._log_verdict(label, verdict, 'nudged')
        # Keep BOTH: the reviewer's own findings live in the earlier
        # narration, and the nudge reply is often just the token.
        combined = f'{text}\n\n{nudged}'.strip() if text.strip() else nudged
        return verdict, combined

    @staticmethod
    def _log_verdict(label: str, verdict: str | None, source: str) -> None:
        """Record how a reviewer's verdict was captured (live runs)."""
        shown = verdict or 'NONE->BLOCKING'
        click.echo(f'[review] {label}: verdict={shown} (source={source})')

    def _run_judge(self, stage: pipeline.PipelineStage) -> None:
        candidates = self._judge_candidates(stage)
        if not candidates:
            raise PipelineRunError(
                f'judge {stage.id!r} has no writer candidates in needs'
            )
        wt = self._wt.create_judge_worktree(
            self._run_id, stage.id, candidates, replace=self._resume
        )
        agent = stage.run[0]
        session = self._create_session(agent, wt, 'ro', stage.id)
        out = self._drive(
            session, self._judge_instruction(stage, candidates)
        )
        stated = parse_select(out, candidates)
        if stated is None:
            # One nudge before the default takes over. A judge that
            # answered without deciding — "I have launched the tests in
            # the background ... I will process the results as soon as
            # they are available" — has not refused to choose, it has
            # misjudged how many turns it gets. Asking once converts a
            # silent default into an actual decision; the alternative
            # is discarding its judgement over a misunderstanding
            # (TASKS.md #41).
            click.echo(
                f'[judge] {stage.id}: no SELECT line in the first '
                f'reply — asking once more before halting.'
            )
            retry = self._drive(
                session, self._judge_retry_instruction(candidates)
            )
            again = parse_select(retry, candidates)
            if again is not None:
                stated = again
                out = f'{out}\n\n{retry}'
        if stated not in candidates:
            # HALT, do not guess. This used to fall back to
            # `candidates[0]`, which turned "nobody judged" into a pick
            # made by list order — indistinguishable, in the record,
            # from a real decision. Observed live on
            # `gcp-scope-topology-1`: the judge said it had started
            # verification builds and would analyse them "once
            # compilation completes", and impl-a shipped because it was
            # first in `needs`.
            #
            # Halting is affordable precisely here: every candidate is
            # committed to the hub and both reached their review gates,
            # so nothing is lost by stopping, and a human choosing
            # between two reviewed branches is a minute's work. A silent
            # default is the expensive outcome, not the stop.
            self._capture_turn(
                session, 'the judge never stated a SELECT line'
            )
            click.echo(
                f'[judge] {stage.id}: NO DECISION after a retry — '
                f'halting rather than shipping the first candidate by '
                f'default. Candidates: {", ".join(candidates)}. Their '
                f'branches are on the hub and their reviews are in the '
                f'run state; pick one and resume, or re-run this stage.'
            )
            raise PipelineRunError(
                f'judge {stage.id!r} stated no SELECT line for any of '
                f'{", ".join(candidates)} — refusing to pick one by '
                f'position. See the captured turn in the run directory.'
            )
        sel = stated
        # Record BEFORE the branch alias: the judge's session is
        # disposed moments from now and its reasoning goes with it, and
        # a run that fails after this point should still explain what
        # it picked.
        # Retain the losers BEFORE anything else touches the hub. Their
        # branches live only there, and the run directory holds the only
        # copy — teardown deletes it (TASKS.md #32).
        retained = self._retain_losers(candidates, sel)
        self._record_pick(stage.id, candidates, sel, stated, out, retained)
        # Publish the winner as the judge node's OWN hub branch, so a
        # downstream stage can seed `from:` the judge (refactor the
        # winner, review it) — a judge otherwise leaves no branch to
        # inherit.
        self._wt.alias_node_branch(self._run_id, stage.id, sel)
        self._nodes[stage.id] = NodeResult(
            stage.id,
            'judge',
            branch=self._nodes[sel].branch,
            worktree=wt,
            session=session,
            output=out,
            selected=sel,
        )
        self._last_branch_node = stage.id

    # ── wiring helpers ────────────────────────────────────────────

    def _seed_from(self, stage: pipeline.PipelineStage) -> str | None:
        """The upstream WRITER branch a node's worktree is cut from."""
        if stage.from_branch:
            return stage.from_branch
        for need in stage.needs:
            node = self._nodes.get(need)
            if node and node.branch:
                return need
        return None

    def _review_target(self, stage: pipeline.PipelineStage) -> str:
        """The writer node a review stage inspects (first in needs)."""
        for need in stage.needs:
            node = self._nodes.get(need)
            if node and node.branch:
                return need
        raise PipelineRunError(
            f'review {stage.id!r} has no writer in needs'
        )

    def _judge_candidates(
        self, stage: pipeline.PipelineStage
    ) -> list[str]:
        """The competing writer nodes a judge compares (from needs)."""
        return [
            need
            for need in stage.needs
            if self._nodes.get(need)
            and self._nodes[need].branch
            and self._nodes[need].kind == 'writer'
            # A candidate whose review never reached consensus
            # forfeited; judging it would compare a vetted branch
            # against an unvetted one and call the result a choice.
            and need not in self._forfeited
        ]

    # ── session + turn plumbing ───────────────────────────────────

    def _verify_launch(self, session: str) -> None:
        """
        Say so when a harness did not launch with what we asked for.

        The launcher asks for a model, an effort and a permission mode,
        and until now never checked it got any of them. Four separate
        failures in two days turned on exactly that (TASKS.md #27, #28,
        #34, #35): a mode downgraded because the model lacked a
        capability, an effort discarded, a model substituted. Every one
        was invisible in the launcher's logs and plainly on the agent's
        screen.

        WARNS, never raises. It reads a TUI mid-draw, so a false alarm
        is entirely possible and must not be able to fail a run — and a
        read-back that cries wolf is one somebody switches off, after
        which the next silent substitution costs another day.
        :func:`readback.launch_mismatches` is conservative for the same
        reason: what it cannot read, it does not report.

        :param session: The session whose first turn just completed.
        """
        agent = self._session_agent.get(session)
        if agent is None:
            return
        label = self._session_label.get(session, session)
        sandbox = self._sandbox_for_session(session)
        if sandbox is None:
            return
        try:
            text = pane.capture_pane(sandbox) or ''
        except Exception:  # pragma: no cover - capture_pane is total
            return
        mode = None
        if agent.harness not in agy.AGY_HARNESSES | codex.CODEX_HARNESSES:
            args = list(_launch_args_for(agent.harness, agent.effort))
            if '--permission-mode' in args:
                mode = args[args.index('--permission-mode') + 1]
        for why in readback.launch_mismatches(
            agent.harness,
            text,
            model=agent.model,
            effort=agent.effort,
            permission_mode=mode,
        ):
            click.echo(f'[launch] {label}: {why}')

    def _create_session(
        self, agent_name: str, worktree: str, mode: str, label: str
    ) -> str:
        agent = self._config.agents[agent_name]
        is_agy = agent.harness in agy.AGY_HARNESSES
        session = self._sc.create(
            agent_id=self._agent_ids[agent_name],
            workspace=mount_sentinel(
                worktree, mode,
                credential=credential_kind_for(agent.harness),
            ),
            title=f'{self._run_id}/{label}',
            terminal_launch_args=list(
                _launch_args_for(agent.harness, agent.effort)
            ),
            model_override=agent.model,
            reasoning_effort=agent.effort,
        )
        # One critical section for the whole registration: the
        # save below snapshots exactly these collections, so a node
        # registering concurrently must not land halfway through.
        with self._lock:
            self._sessions.append(session)
            self._session_worktree[session] = worktree
            self._session_label[session] = label
            self._session_agent[session] = agent
        # Persist NOW, not at the next stage boundary. State is
        # otherwise written only when a stage or review round
        # completes, so a session created inside that window is
        # invisible to --resume forever: `_dispose_stale_sessions`
        # disposes what the state file recorded, and nothing else knows
        # this VM exists. Observed live — a reviewer session created at
        # 01:24, the minute the machine crashed, survived every
        # subsequent resume and had to be found and deleted by hand.
        # One cheap write buys a VM that can always be reclaimed.
        self._save_state()
        if is_agy:
            self._agy_sessions.add(session)
            # Warm the just-created agy TUI before it is driven, so the
            # first turn's paste hits a ready composer (see the module
            # note + wait_for_terminal_ready).
            self._sc.wait_for_terminal_ready(session)
        return session

    def _drive(
        self, session: str, instruction: str, *, inline: bool = False
    ) -> str:
        """
        Run one turn on *session* and return its reply.

        :param session: The session to drive.
        :param instruction: The turn text.
        :param inline: Paste the turn directly instead of staging it as
            a file for an agy session. For SHORT, urgent turns whose
            whole point is that the agent acts on them right now: the
            file indirection buries them. See :data:`_VERDICT_NUDGE`.
        :returns: The assistant's reply.
        :raises PipelineRunError: If the turn failed.
        """
        is_agy = session in self._agy_sessions
        # agy drops a multi-line paste before it renders AND collapses
        # an over-long paste into an unsubmittable placeholder — so an
        # agy turn is never pasted whole: it is handed over as a file
        # the agent reads and only a tiny pointer is pasted (see
        # _agy_deliverable). A first agy turn also gets a re-delivery
        # net if it fails before starting.
        if is_agy:
            self._require_fresh_swap(session)
            instruction = (
                # Flattened, not staged: agy still cannot take a
                # multi-line paste, but a one-line turn renders fine and
                # arrives where the agent will actually read it.
                _single_line(instruction)
                if inline
                else self._agy_deliverable(session, instruction)
            )
        first_turn = session not in self._driven
        self._driven.add(session)
        try:
            result = self._sc.send_and_wait(
                session,
                instruction,
                timeout=self._turn_timeout,
                max_resubmits=(
                    _FIRST_TURN_MAX_RESUBMITS if first_turn else 0
                ),
                redeliver_delay_s=(
                    _FIRST_TURN_REDELIVER_DELAY_S if first_turn else 0.0
                ),
            )
        except SwarmSessionError as exc:
            # The turn never came back. Teardown is seconds away and
            # will delete this session, so capture NOW or the only
            # evidence is the timeout line itself.
            self._capture_turn(
                session,
                f'the turn did not return: {exc}',
                with_pane=True,
            )
            raise
        except KeyboardInterrupt:
            # Ctrl-C on a turn that LOOKS stuck is exactly when someone
            # wants to know what it was doing — and it is the one exit
            # that skipped this, because KeyboardInterrupt is not an
            # Exception. Observed live: a reviewer that had installed a
            # toolchain, built the workspace and launched the suite was
            # interrupted, and its transcript had to be pulled out of
            # the API by hand before teardown deleted it.
            self._capture_turn(
                session,
                'interrupted (Ctrl-C) mid-turn',
                with_pane=True,
            )
            raise
        if first_turn and result.ok:
            # AFTER the first turn, not at create: the TUI has not
            # finished drawing when a session is created, so the pane
            # carries nothing to compare and the check would silently
            # never fire (observed — it read an empty pane every time).
            # One turn late still beats stage 6 of 8, and costs no wait.
            self._verify_launch(session)
        if not result.ok:
            note = self._session_failure_note(session)
            pane_path = self._capture_turn(
                session,
                f'the turn failed: {result.error}{note}',
                with_pane=True,
            )
            # Name the pane IN the error. A bare "failed: None" is what
            # cost a day on the codex-3 run (TASKS.md #26/#27) — the
            # blocking migration picker was on screen the whole time and
            # nothing pointed at it.
            # Say why there is no pane rather than dropping the
            # suffix. A bare 'failed: None' with nothing to open is what
            # #26 exists to prevent, and a capture that returned nothing
            # silently reproduced it.
            where = (
                f' — see {pane_path}' if pane_path
                else ' (no pane captured — the sandbox was already gone)'
            )
            raise PipelineRunError(
                f'turn on {session} failed: {result.error}{note}{where}'
            )
        return result.reply

    def _session_failure_note(self, session: str) -> str:
        """
        What the SESSION says about a turn that gave no reason.

        A failed turn arrives as a status edge carrying no error, which
        the runner then reports verbatim as ``failed: None`` — the least
        useful sentence it can produce. The session itself is still
        readable at that moment and carries three fields nobody
        consulted: ``last_task_error`` (read only by the swarm CLI),
        ``runner_online`` (read nowhere at all), and ``sandbox_status``.

        Reading them turns "failed: None" into a sentence, and separates
        the two cases that matter: the AGENT failed, or its runner went
        away underneath it.

        :param session: The session whose turn just failed.
        :returns: A note to append to the error, or ``''``.
        """
        try:
            snap = self._sc.get_status(session)
        except SwarmSessionError:
            return ' (the session could not be read for a reason)'
        bits: list[str] = []
        err = snap.get('last_task_error')
        if isinstance(err, str) and err.strip():
            bits.append(f'last_task_error={err.strip()[:300]}')
        if snap.get('runner_online') is False:
            bits.append(
                'its RUNNER IS OFFLINE — the turn did not fail so much '
                'as lose the process running it'
            )
        sandbox = snap.get('sandbox_status')
        if isinstance(sandbox, str) and sandbox:
            bits.append(f'sandbox={sandbox}')
        return f' ({"; ".join(bits)})' if bits else ''

    def _runner_vanished(self, session: str) -> bool:
        """
        Whether a failed turn lost its runner rather than failing.

        :param session: The session whose turn just failed.
        :returns: ``True`` when the runner is known to be offline.
        """
        try:
            return self._sc.get_status(session).get('runner_online') is False
        except SwarmSessionError:
            # Unreadable is not evidence either way, and guessing "yes"
            # would retry every genuine failure.
            return False

    def _require_fresh_swap(self, session: str) -> None:
        """
        Refuse an agy turn whose swap token has already expired.

        An agy agent VM carries only a placeholder; the sbx proxy swaps
        in the harvested token on the wire. Once that token expires, agy
        fails to authenticate INSIDE ITS OWN TUI — which the bridge
        never surfaces as a failed session — so the turn simply hangs to
        the turn timeout and reports a misleading "did not complete",
        with an empty session and no error anywhere. Burning 30 minutes
        to learn nothing is strictly worse than stopping here.

        No grace wait: :data:`agy.MAX_SWAP_AGE_S` already allows half an
        interval of slack past a healthy refresh, so exceeding it means
        the harvester is not working, not that it is mid-cycle.

        :param session: The agy session about to be driven.
        :raises PipelineRunError: If the secret is stale or unknown.
        """
        age = self._swap_age_s()
        if age is not None and age <= agy.MAX_SWAP_AGE_S:
            return
        seen = (
            'has never been recorded'
            if age is None
            else f'was last refreshed {age / 60:.0f} min ago'
        )
        raise PipelineRunError(
            f'refusing to drive agy session {session}: the swap secret '
            f'{seen} (limit {agy.MAX_SWAP_AGE_S / 60:.0f} min), so its '
            f'access token has expired and agy would fail to '
            f'authenticate inside its own TUI — the turn would hang for '
            f'{self._turn_timeout:.0f}s and report nothing. The token '
            f'harvester is not refreshing: check {agy.HARVEST_LOG}, and '
            f"if the trusted box cannot re-mint, run 'agy /login' on it."
        )

    def _agy_deliverable(self, session: str, instruction: str) -> str:
        """
        Deliver an agy turn as a file + a tiny pointer to paste.

        agy fails a turn at paste time whenever the paste is multi-line
        (dropped before it renders) or long (collapses into a
        ``[Pasted text …]`` placeholder the bridge cannot verify), so
        nothing substantial is ever pasted. The full turn — original
        formatting intact, which reads better than a flattened blob — is
        written to :data:`_AGY_TASK_FILE` at the root of the agent's own
        worktree, and only a short one-line pointer telling the agent to
        read it is pasted. The file is git-ignored in the node's clone,
        so a writer never commits it and the settle-wait never mistakes
        it for the agent's output. Reading a task file is a normal,
        allowed action for every agy role (the planner may read, just
        not write).

        :param session: The agy session being driven.
        :param instruction: The full turn text.
        :returns: The one-line pointer to paste (or, defensively, the
            flattened turn if the session has no worktree to stage
            into).
        """
        worktree = self._session_worktree.get(session)
        if worktree is None:
            # No worktree to stage into (should not happen — every
            # session is created with one): flatten and paste, so the
            # turn is at least attempted rather than lost.
            return _single_line(instruction)
        self._wt.write_ignored_file(worktree, _AGY_TASK_FILE, instruction)
        return _single_line(
            f'Your full instructions are in the file {_AGY_TASK_FILE} at '
            'the root of your workspace (your current directory). Read '
            'that file now, then carry out the task exactly as it '
            'describes.'
        )

    def _commit(self, node_id: str, message: str) -> None:
        """
        Commit a node's work, attributed to the NODE.

        Attributed to the node (``impl-a``) and never to the agent that
        happened to run it (``impl_claude``), because the judge and the
        reviewers can read this. Each judge candidate is a standalone
        clone with a real ``.git`` directory — deliberately, so git
        works inside the VM — so one ``git log`` in ``./impl-a`` printed
        the model family straight out of the author field, and the
        reviewers see the same through their ``:ro`` node mount. A judge
        that can tell which family wrote which candidate is not making
        the blind comparison the two-writer race assumes (TASKS.md #33).

        The agent name is not a parameter at all, rather than a
        parameter callers are trusted to pass neutrally: the only way to
        keep this closed is to make the leak unexpressible. It is also
        simply more accurate — the node is what the branch is named for.

        :param node_id: The node whose worktree to commit.
        :param message: The commit message.
        """
        self._wt.commit_node(
            self._run_id,
            node_id,
            message=message,
            author=f'{node_id} <{node_id}@pipeline.local>',
        )

    # ── instruction composition ───────────────────────────────────

    def _setup_block(self) -> str:
        """
        The environment/bootstrap note, or ``''`` when unconfigured.

        Relayed to every builder and reviewer. A VM that lacks a
        compiler is exactly where an agent stops verifying and starts
        guessing — observed live, reviewers reported "no toolchain in
        this environment" and then approved by reading the diff, while
        the implementers in identical VMs had installed one and
        compiled. Saying what to install, and that egress allows it,
        removes the excuse.
        """
        if not self._config.setup:
            return ''
        return (
            '\n\nEnvironment — prepare your VM before you start (it is '
            'a fresh sandbox, and outbound access for these steps is '
            f'already allowed):\n{self._config.setup}'
        )

    def _task_block(self) -> str:
        block = f'{self._chunk_preamble()}Task:\n{self._config.task}'
        if self._config.acceptance:
            block += f'\n\nAcceptance contract:\n{self._config.acceptance}'
        return (
            f'{block}\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            + self._setup_block()
        )

    def _reader_context(self, stage: pipeline.PipelineStage) -> str:
        parts = []
        for need in stage.needs:
            node = self._nodes.get(need)
            if node and node.kind == 'reader' and node.output:
                parts.append(f'--- Design from {need} ---\n{node.output}')
        return '\n\n'.join(parts)

    def _reader_instruction(self, stage: pipeline.PipelineStage) -> str:
        # A planner must be told to PLAN the feature, not handed the raw
        # "Task: add <feature>" implementation directive — else the
        # concrete build instruction overrides its plan-only role prompt
        # and it tries to implement (or hunt for a writable path).
        if self._is_planner(stage):
            return self._planner_instruction(stage)
        ctx = self._reader_context(stage)
        return self._task_block() + (f'\n\n{ctx}' if ctx else '')

    def _module_table(self, sub: pipeline.Subtask) -> str:
        """
        The full ordered module list, with the active one marked.

        A per-module planner is otherwise shown ONLY its own row, so it
        cannot tell which adjacent work belongs to somebody else and
        reasonably designs the neighbours' scope into its module.
        Observed live: module 0's planner specified the storage layer
        and snapshot diffing that the human's own table assigns to
        module 1, and the test writer stopped to ask which bar to hold
        it to — the one time that conflict was caught rather than
        frozen into a test suite.

        :param sub: The module being designed.
        :returns: One ``[id] title`` row per module, the active one
            arrow-marked.
        """
        return '\n'.join(
            f'{"▶ " if other.id == sub.id else "  "}'
            f'[{other.id}] {other.title}'
            for other in self._subtasks
        )

    def _planner_instruction(self, stage: pipeline.PipelineStage) -> str:
        """
        Frame a planner node's turn as producing a design plan.

        Describes the feature to PLAN rather than telling the agent to
        build it, so the concrete instruction agrees with the plan-only
        role prompt: design in prose, implement nothing, write nothing.
        Kept deliberately terse — the ``planner`` template carries the
        full role rules, and the per-turn text must stay under agy's
        paste cap (:data:`_AGY_PASTE_SAFE_MAX`) so the planner turn
        renders inline instead of collapsing into a placeholder.
        """
        sub = self._active_subtask
        if sub is not None:
            # Per-module planner: design THIS module against the frozen
            # prior modules already in the worktree. The task text is
            # the shared brief (context); the module row is the scope.
            ledger = self._decisions_block()
            instr = (
                'Produce a DESIGN PLAN (prose only — do NOT write code, '
                'files, or commands; your role prompt has the full '
                'rules). If ambiguous, end with a QUESTIONS: block.\n\n'
                f'Module to design: [{sub.id}] {sub.title}\n\n'
                'The full module sequence for this component, yours '
                'marked with an arrow. Every other row is designed and '
                'built in its OWN run — do not design, implement, or '
                'pull their scope into yours:\n'
                f'{self._module_table(sub)}\n\n'
                + (
                    'This is the FIRST module: the repo is at its base '
                    'state and nothing has been built yet, so design '
                    'from the brief rather than looking for existing '
                    'artifacts.'
                    if self._active_is_first
                    else 'The artifacts from prior modules are already '
                    'implemented and FROZEN in your worktree — design '
                    'against them as fixed contracts; a genuine need to '
                    'change a frozen artifact is a halt-and-escalate to '
                    'the human, never a silent edit. Each earlier '
                    "module's approved design is committed under "
                    '`docs/plans/` in your worktree; read the ones your '
                    'module builds on.'
                )
                + (
                    ''
                    if not ledger
                    else '\n\nDecisions earlier modules already settled '
                    'with the human. These are BINDING — design against '
                    'them and do NOT re-open or re-ask them:\n'
                    f'{ledger}'
                )
                + '\n\nProject brief (shared context):\n'
                f'{self._config.task}'
            )
        else:
            instr = (
                'Produce a DESIGN PLAN (prose only — do NOT write code, '
                'files, or commands; your role prompt has the full '
                'rules). If ambiguous, end with a QUESTIONS: block.\n\n'
                f'Feature to plan:\n{self._config.task}'
            )
        if self._config.acceptance:
            instr += (
                '\n\nAcceptance contract (satisfy every clause):\n'
                f'{self._config.acceptance}'
            )
        instr += (
            '\n\nYour VM deliberately has NO project toolchain, and you do '
            'not need one: you are designing, not building. Do NOT install '
            'a compiler, package manager, or database, and do not try to '
            'build, run, or test anything — read the existing sources, '
            'docs, and plans as TEXT. A missing tool is expected here, '
            'never a problem for you to solve.'
        )
        if self._interactive_plan:
            instr += (
                '\n\nA human reviewer is in this session and answers your '
                'questions — ask what you need, then invite them to reply '
                '"APPROVED" to release the plan.'
            )
        ctx = self._reader_context(stage)
        if ctx:
            instr += f'\n\n{ctx}'
        return instr

    def _is_planner(self, stage: pipeline.PipelineStage) -> bool:
        """Whether this stage's own agent is the shipped planner."""
        if not stage.run:
            return False
        agent = self._config.agents.get(stage.run[0])
        return bool(agent and agent.template == 'planner')

    def _writer_instruction(self, stage: pipeline.PipelineStage) -> str:
        # A test-writer IS a writer node, but its turn must be framed as
        # "write tests for this task", NOT the raw implementation task —
        # else the concrete "Task: add <feature>" directive overrides
        # its write-tests-only role prompt and it builds the feature.
        if self._is_test_writer(stage):
            return self._test_writer_instruction(stage)
        # A refactor node is a writer seeded from the judge's winner;
        # its turn is "clean up this working code", NOT "implement the
        # feature" — same role-framing reason as the test writer.
        if self._is_refactor(stage):
            return self._refactor_instruction(stage)
        instr = self._task_block()
        ctx = self._reader_context(stage)
        if ctx:
            instr += f'\n\n{ctx}'
        if self._has_upstream_tests(stage):
            instr += (
                '\n\nA failing test suite has already been written for '
                'you in this worktree and is the BINDING CONTRACT: make '
                'every test pass without weakening, skipping, or deleting '
                'any test. Any design plan above is guidance for how to '
                'structure your implementation — follow it, but if it '
                'ever conflicts with the tests, the TESTS win.'
            )
        return instr

    def _test_writer_instruction(
        self, stage: pipeline.PipelineStage
    ) -> str:
        """
        Frame a test-writer node's turn as writing a failing test suite.

        The turn describes the feature to TEST rather than telling the
        agent to build it, so the concrete instruction agrees with the
        test-author role prompt: add tests only, do not implement.
        """
        instr = (
            f'{self._chunk_preamble()}'
            'Write a failing TEST SUITE for the feature described below. '
            'A separate implementer will write the production code to make '
            'your tests pass — you must NOT implement the feature yourself '
            'or modify any production code (add test files only).\n\n'
            f'Feature to test:\n{self._config.task}'
        )
        if self._config.acceptance:
            instr += (
                '\n\nAcceptance contract (encode every clause as tests):\n'
                f'{self._config.acceptance}'
            )
        instr += (
            f'\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            + self._setup_block()
        )
        ctx = self._reader_context(stage)
        if ctx:
            instr += f'\n\n{ctx}'
        return instr

    def _refactor_instruction(
        self, stage: pipeline.PipelineStage
    ) -> str:
        """
        Frame a refactor node's turn as behavior-preserving cleanup.

        The winning implementation is already in the node's worktree
        (seeded from the judge). The turn describes the code to CLEAN UP
        — keep behavior, keep tests green, no new features — rather than
        the raw implementation task, so the agent polishes the existing
        code instead of re-implementing from scratch.
        """
        instr = (
            f'{self._chunk_preamble()}'
            'A COMPLETE, WORKING implementation (the winning candidate) '
            'is already in your worktree. REFACTOR it for clarity, '
            'structure, and maintainability WITHOUT changing behavior: no '
            'new features, no API changes, no scope beyond cleanup. Every '
            'existing test must still pass — run them and keep them green; '
            'do not weaken, skip, or delete any test.\n\nThe feature it '
            'implements (context only — do NOT re-implement it):\n'
            f'{self._config.task}'
        )
        if self._config.acceptance:
            instr += (
                '\n\nThe behavior to PRESERVE (acceptance contract):\n'
                f'{self._config.acceptance}'
            )
        instr += (
            f'\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            + self._setup_block()
        )
        ctx = self._reader_context(stage)
        if ctx:
            instr += f'\n\n{ctx}'
        return instr

    def _is_test_writer(self, stage: pipeline.PipelineStage) -> bool:
        """Whether this stage's own agent is the shipped test-writer."""
        if not stage.run:
            return False
        agent = self._config.agents.get(stage.run[0])
        return bool(agent and agent.template == 'tdd-writer')

    def _is_refactor(self, stage: pipeline.PipelineStage) -> bool:
        """Whether this stage's own agent is the shipped refactorer."""
        if not stage.run:
            return False
        agent = self._config.agents.get(stage.run[0])
        return bool(agent and agent.template == 'refactoring')

    def _has_upstream_tests(self, stage: pipeline.PipelineStage) -> bool:
        seed = self._seed_from(stage)
        if seed is None:
            return False
        seed_stage = self._stage_by_id.get(seed)
        if seed_stage is None or not seed_stage.run:
            return False
        agent = self._config.agents.get(seed_stage.run[0])
        return bool(agent and agent.template == 'tdd-writer')

    def _review_instruction(self, stage: pipeline.PipelineStage) -> str:
        # Only inside a campaign: on a flat run there is no increment
        # list, so this would point at a "plan above" that is not there
        # — a dangling reference is worse than saying nothing.
        later = (
            ', or BELONGING TO A LATER INCREMENT of the plan above'
            if self._active_subtask is not None
            else ''
        )
        return (
            self._task_block()
            + '\n\nYour mount is READ-ONLY: you can read and build the '
            'tree but not write into it. Build somewhere on the VM'
            "'s own disk instead — for Cargo, `export "
            'CARGO_TARGET_DIR=/tmp/review-target` before any cargo '
            'command; other toolchains have an equivalent. DO NOT COPY '
            'the tree anywhere first, and do not try to make the mount '
            'writable: cargo builds a read-only source directory fine '
            'with the target directory elsewhere, the copy is gigabytes '
            'for nothing, and one reviewer that reached for `rm -rf` to '
            'clear space stalled its whole turn on a permission prompt.'
            + '\n\nReview the working tree in your mount against this '
            'contract.\n\nBefore any verdict, CHECK THAT THE CODE IS '
            'REALLY THERE and that the test suite really passes — run '
            'it. If the tooling you need is missing, install it (see '
            'the environment notes above); your VM has network access '
            'for that. You may NOT return `VERDICT: APPROVED` on a '
            'change you could not execute, and "the code looks correct" '
            'is not a substitute for running it. If you could not '
            'verify, return `VERDICT: BLOCKING` and say exactly what '
            'stopped you. An empty or placeholder implementation that '
            'merely compiles-by-absence is a BLOCKING finding.'
            + _REVIEW_NOT_THE_GATE
            + '\n\nA '
            'verdict is not a weighted opinion. Every finding you report '
            'as BLOCKING must be one you would hold the release for. If '
            'you would discount a finding — as minor, pre-existing, '
            "external, upstream drift, someone else's module, not "
            f'attributable to this change{later} — then it is NOT '
            'blocking: put '
            'it in a clearly separate NON-BLOCKING list and keep it out '
            'of the reasoning for your verdict. Never state a blocking '
            'finding and argue it down in the same breath. The writer '
            'reads a hedged finding as permission to make the symptom go '
            'away rather than the cause, and a gate that was weakened to '
            'clear a finding you had already discounted is the worst '
            'outcome this pipeline can produce.'
            + self._scope_block()
            + _FINDINGS_ASK
            + self._guarded_block(stage)
            + '\n\nEnd '
            'your reply with a single verdict line, using one of these '
            'two exact tokens: `VERDICT: APPROVED` (the change meets '
            'the contract, and you verified it) or `VERDICT: BLOCKING` '
            '(it does not, or you could not verify).'
        )

    def _judge_scope_clause(self) -> str:
        """
        Make staying inside the increment a judging criterion.

        Unstated, the model reaches for it inconsistently: on
        `gcp-scope-topology-1` the same judge penalised a candidate for
        being "out of scope for this increment" in [transport] and then
        praised the other one's later-increment work as a strength in
        [topology]. Naming it, and saying which direction it counts in,
        removes the coin flip.

        :returns: The clause, or ``''`` outside a campaign.
        """
        if self._active_subtask is None:
            return ''
        return (
            ' Weigh also WHETHER EACH STAYED INSIDE THE MARKED '
            'INCREMENT. A candidate that implemented a later '
            "increment's work is not thereby ahead: that work was "
            'reviewed against the wrong contract, and the increment '
            'that owns it will inherit something its own plan did not '
            'choose. Count it as a cost, not a bonus — and count it the '
            'same way for every candidate.'
        )

    def _defer_block(self) -> str:
        """
        The one way a writer may answer a finding instead of fixing it.

        A blocking finding is otherwise unconditional, so a reviewer
        that asks for a later increment's work leaves the writer no move
        but to build it — which is how `gcp-scope-topology-1` acquired
        service-account pagination during a [topology] fix turn.

        Deliberately narrow: one prescribed form, the owning increment
        named, and nothing silent. The reviewer sees the deferral in the
        next round and can block again if it disagrees, so this shifts
        who must justify the scope call — it does not remove the veto.

        :returns: The paragraph, or ``''`` outside a campaign.
        """
        if self._active_subtask is None:
            return ''
        return (
            'One finding may be answered instead of fixed, and only this '
            'one way: if a finding asks for work the increment list '
            'above assigns to a LATER increment, and this change did not '
            'introduce it, reply with `DEFERRED: [<increment-id>] <one '
            'line>` and leave the code alone. Nothing else counts as '
            'answering a finding — not "out of scope" on its own, not '
            'silence, and not a partial fix. Deferring is on the record '
            'and the reviewer sees it next round; if it disagrees it '
            'will block again and say why. Everything you do NOT defer, '
            'you fix.\n\n'
        )

    def _scope_block(self) -> str:
        """
        Tell a reviewer that the increment bounds its verdict.

        Only on a campaign run: outside one there is no increment and
        the whole brief really is this branch's job.

        Both halves are load-bearing. Without the first a reviewer
        blocks on a later increment's work and the writer, for whom a
        blocking finding is not optional, implements it — reviewed
        against the wrong contract, and inherited unplanned by the
        increment that owns it. Without the second, "that file belongs
        to a later increment" becomes a shield for something this change
        actually broke.

        :returns: The paragraph, or ``''`` outside a campaign.
        """
        if self._active_subtask is None:
            return ''
        return (
            '\n\nScope is part of the contract, and it cuts both ways. '
            'The Task above is the whole plan; your verdict covers ONLY '
            'the marked increment. A requirement another increment owns '
            "is not this branch's debt — name it in the NON-BLOCKING "
            'list with the increment it belongs to, and do not hold the '
            'gate for it. But a defect this increment INTRODUCED is '
            'yours to block on wherever it lives in the tree: "that file '
            'belongs to a later increment" is not a shield for something '
            'this change broke. If you cannot tell which, say so and '
            'block — an honest uncertainty is worth a round.'
        )

    def _judge_instruction(
        self, stage: pipeline.PipelineStage, candidates: list[str]
    ) -> str:
        """
        The turn handed to a judge.

        Says two things the template cannot know. First, whether the
        candidates have ALREADY been through their reviewer gates —
        ``templates/judge.md`` asks "which candidate genuinely passes
        the suite", and a judge that reads that with no other context
        starts a build to find out. On a Rust workspace that is slow
        enough that one judge backgrounded it and reported progress
        instead of deciding. The suite has already been run, twice per
        candidate, by reviewers who each installed a toolchain to do
        it; re-running it is minutes of guest time for an answer
        already on record.

        Second, that this is its ONLY turn. The judge asked to "process
        the results as soon as they are available" — a perfectly normal
        thing to say in a conversation, and fatal here, because nothing
        follows. Without a SELECT line the first candidate wins by
        default and the judge's opinion is discarded (TASKS.md #41).

        :param stage: The judge stage.
        :param candidates: The writer nodes being judged.
        :returns: The instruction text.
        """
        listed = ', '.join(f'./{c}' for c in candidates)
        instr = (
            self._task_block()
            + f'\n\nCompare the candidate implementations in {listed} '
            'against this contract, then end your reply with a line '
            'reading exactly:\n\n    SELECT: <id>\n\nwhere <id> is '
            f'one of: {", ".join(candidates)}. The colon is part of '
            'the line, the id is copied verbatim, and nothing else '
            'goes on it.'
        )
        if self._all_reviewed(candidates):
            instr += (
                '\n\nEVERY candidate here has already passed its own '
                'review gate: each was independently verified by '
                'reviewers who installed a toolchain and RAN the test '
                'suite and the project gate against that exact branch. '
                'Do NOT build or re-run the suite — the answer is '
                'already on record, and re-deriving it costs minutes of '
                'guest time and tells you nothing new. Judge on the '
                'DIFFERENCES between the candidates: design, '
                'simplicity, dependency minimalism, how each will read '
                'to the next person, and what each does at the edges. '
                'Read the code, do not run it.'
                + self._judge_scope_clause()
            )
        instr += (
            '\n\nThis is your ONLY turn. Nothing follows it, so do not '
            'start work in the background and do not say you will '
            'report once something finishes — there is no later message '
            'and anything unfinished is simply lost. Decide now, on '
            'what you can see. A reply without a SELECT line is not a '
            'deferral: the run HALTS, no candidate is chosen, and a '
            'human has to come and do this by hand. Your judgement is '
            'not merely discarded — the pipeline stops. If you are '
            'genuinely unsure, still SELECT, and say what you could '
            'not check.'
        )
        return instr

    @staticmethod
    def _judge_retry_instruction(candidates: list[str]) -> str:
        """
        The one follow-up a judge gets when it did not decide.

        Deliberately asks for nothing but the line, and SHOWS it
        rather than naming it: the first version said "end with the
        SELECT line", and a judge that had already chosen wrote
        ``SELECT core-contracts-impl-a`` without the colon, which
        parsed as nothing (TASKS.md #44). A judge that deferred has
        usually done the reading; what it lacked was either the
        knowledge that no further turn was coming, or the exact format.

        :param candidates: The writer node ids being judged.
        :returns: The instruction text.
        """
        return (
            'No decision was recorded from your reply: it carried no '
            'line in the required form. This is the LAST turn — there '
            'is no further message, and nothing you started in the '
            'background will be waited for or read. Decide now, from '
            'what you have already seen; a decision on partial '
            'evidence is worth far more than none, because without '
            'that line the first candidate is taken by default and '
            'your judgement counts for nothing.\n\nReply with a '
            'one-paragraph rationale, then a final line reading '
            'exactly:\n\n    SELECT: <id>\n\nwhere <id> is one '
            f'of: {", ".join(candidates)}. Include the colon, copy '
            'the id verbatim, and put nothing else on that line — no '
            'backticks, no bold, no trailing prose.'
        )

    def _all_reviewed(self, candidates: list[str]) -> bool:
        """
        Whether every candidate already cleared a review stage.

        A judge does not necessarily run downstream of reviewers — the
        pipeline decides that — so the "already verified, do not
        re-run" claim is only made when it is TRUE for every candidate.

        It now SURVIVES a resume (it is in the run state), because
        losing it was not the harmless understatement it was written to
        be: an untold judge starts a workspace build, cannot finish it
        inside its one turn, and states no SELECT. What keeps the claim
        honest across a resume is that :meth:`_redrive_writer` drops a
        candidate the moment its branch changes.

        :param candidates: The writer nodes being judged.
        :returns: ``True`` when each reached APPROVED consensus here.
        """
        return bool(candidates) and all(
            c in self._reviewed_ok for c in candidates
        )

    def _fix_instruction(self, findings: str) -> str:
        """
        The turn handed to a writer whose review blocked.

        Carries the environment note as well as the findings: this turn
        is frequently the FIRST one a re-attached session ever sees, in
        a VM that has never had a toolchain installed. See
        :data:`_FIX_MUST_VERIFY`.

        :param findings: The blocking reviewers' reports.
        :returns: The instruction text.
        """
        return (
            'The reviewers reported BLOCKING issues. Address every one, '
            f'then stop.\n\n{findings}\n\n{self._defer_block()}'
            f'{_FIX_NO_WEAKENING}\n\n'
            f'{_FIX_MUST_VERIFY}\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}'
            + self._setup_block()
        )

    # ── publish + teardown ────────────────────────────────────────

    def _resolve_publish_node(self) -> str | None:
        want = self._config.publish.branch
        if want is not None:
            return want
        return self._last_branch_node

    def _plan_artifact_path(self) -> str:
        """Repo path the plan of record is committed to on publish."""
        return (
            self._config.plan_artifact
            or f'docs/plans/{self._config.name}.md'
        )

    def _publish(self) -> str | None:
        if self._config.publish.mode == 'none':
            return None
        node_id = self._resolve_publish_node()
        if node_id is None:
            return None
        node = self._nodes.get(node_id)
        if node is None:
            raise PipelineRunError(
                f'publish target {node_id!r} did not run'
            )
        # A judge publishes its selected candidate's branch.
        branch_node = node.selected if node.kind == 'judge' else node_id
        if branch_node is None:
            return None
        # Commit the plan of record onto the branch about to ship, so
        # the approved design travels with the code in the PR.
        if self._plan_of_record:
            self._wt.write_tracked_file(
                self._wt.node_worktree_path(self._run_id, branch_node),
                self._plan_artifact_path(),
                self._plan_of_record,
            )
            self._wt.commit_node(
                self._run_id,
                branch_node,
                message='docs: add plan of record',
                author='planner <planner@pipeline.local>',
            )
            self._commit_planning_session(
                branch_node, self._plan_artifact_path()
            )
        # NOT gated on a plan existing: a pipeline with no planner still
        # has reviewers, and their reports are just as much the record.
        review_doc = self._commit_review_records(
            branch_node, self._plan_artifact_path()
        )
        self._publish_findings(branch_node, review_doc)
        pick_doc = self._commit_judge_record(
            branch_node, self._plan_artifact_path()
        )
        result = self._wt.publish_node(
            self._run_id,
            branch_node,
            self._publish_repo,
            title=f'[pipeline] {self._config.name}',
            body=self._pr_body(
                branch_node,
                self._plan_artifact_path(),
                summary=f'Pipeline `{self._config.name}`',
                task=self._config.task,
                review_doc=review_doc,
                pick_doc=pick_doc,
            ),
            base_branch=self._config.base_branch,
            open_pr=self._config.publish.mode == 'pr',
        )
        self._published.append(result)
        self._save_state()
        return result

    def _teardown(self, *, preserve_run: bool = False) -> None:
        """
        Dispose the run's microVMs — and its directory only if done.

        The VMs always go: they are expensive and useless the moment
        this process ends. The RUN DIRECTORY is different. It holds the
        hub clone carrying every node branch, and state.json — together,
        the only record of what the run achieved and the only thing
        ``--resume`` can read. Removing it on a FAILED run destroys
        exactly what resume exists for.

        Observed live: a run died standing up one session, teardown
        wiped the directory, and a finished module's approved plan, its
        frozen test suite, and both competing implementations went with
        it — none of it published, so nothing remained anywhere. Until
        now only ``--keep`` prevented that, which made resume-after-
        failure work by accident rather than by design.

        :param preserve_run: Keep the run directory (a failed or
            blocked run). ``--keep`` still preserves everything.
        """
        if self._keep:
            return
        for session in dict.fromkeys(self._sessions + self._undisposed):
            if session in self._released:
                continue
            try:
                self._sc.dispose(session)
            except SwarmSessionError:
                pass
        if preserve_run:
            click.echo(
                f"[cleanup] disposed this run's microVMs; kept "
                f'{self._wt.run_dir(self._run_id)} so '
                f'--run-id {self._run_id} --resume can continue it.'
            )
            return
        try:
            self._wt.dispose_run(self._run_id)
        except click.ClickException:
            pass


# ── CLI ───────────────────────────────────────────────────────────


def writer_is_terminal(
    config: pipeline.PipelineConfig, stage_id: str
) -> bool:
    """
    Whether nothing in the DAG can drive this writer again.

    Exactly two things re-drive a writer: a review stage naming it in
    ``on_block``, and the verification gate, which loops back to the
    writer that produced the branch being published. A writer that is
    neither is finished the moment its stage completes — in the live
    agent cadre that is the TDD writer, which held a full guest from
    a few minutes into a module until it published, on a host already
    over-committed.

    Deliberately conservative wherever the gate's target cannot be
    named from the DAG alone: a pipeline that publishes "the last
    writer", or that publishes a JUDGE's pick (resolved only once the
    judge has run), could loop back to a writer this cannot identify,
    so in those shapes no writer counts as terminal.

    :param config: The parsed pipeline.
    :param stage_id: The writer stage's id, WITHOUT any chunk prefix.
    :returns: Whether its microVM can be freed when its stage ends.
    """
    stages = list(pipeline._iter_stages(config.stages))
    if any(s.on_block == stage_id for s in stages):
        return False
    want = config.publish.branch
    if want is None:
        return False
    target = next((s for s in stages if s.id == want), None)
    if target is None or PipelineRunner._stage_kind(target) != 'writer':
        return False
    return stage_id != want


def max_concurrent_vms(
    config: pipeline.PipelineConfig, *, reclaim: bool = True
) -> int:
    """
    How many microVMs one pass through the DAG stands up at once.

    One VM per node, and one per reviewer agent in a review stage.
    Campaign runs loop this same set per chunk and dispose each chunk's
    VMs as it publishes (see
    :meth:`PipelineRunner._dispose_chunk_sessions`), so a pass is also
    the campaign's peak — unless ``--keep`` suppresses that reclaim,
    which :func:`preflight_disk` accounts for.

Only WRITERS now sum. Everything else is freed before the peak:

    - A review stage's VMs go as soon as its votes are in, so two
      review stages are never up together and only the LARGEST one
      counts. ``parallel:`` does not change that — it means isolated
      BRANCHES, not concurrent execution; :meth:`_exec_stage` walks a
      group's children one at a time.
    - A reader or a judge is freed the moment its stage completes (see
      :meth:`PipelineRunner._release_completed_session`): neither is
      ever driven again, so holding one to publish was pure waste.
    - A writer nothing can loop back to is freed the same way (see
      :func:`writer_is_terminal`). Only writers a review gate or the
      verification gate can re-drive keep their VMs to publish.

    :param config: The parsed pipeline.
    :param reclaim: Whether finished VMs are freed (false under
        ``--keep``, where every node of every round accumulates).
    :returns: The peak concurrent VM count.
    """
    total = 0
    reviewers: list[int] = []
    for stage in pipeline._iter_stages(config.stages):
        if stage.parallel or not stage.run:
            continue  # a parallel group's children are yielded too
        kind = PipelineRunner._stage_kind(stage)
        if kind == 'review':
            if reclaim:
                reviewers.append(len(stage.run))
            else:
                total += len(stage.run)
        elif reclaim and (
            kind in ('reader', 'judge')
            or (kind == 'writer' and writer_is_terminal(config, stage.id))
        ):
            continue  # freed at stage completion, never re-driven
        else:
            total += 1
    total += max(reviewers, default=0)
    if config.verify is not None:
        # The pre-publish gate stands up one more sandbox, alongside the
        # chunk's nodes (they are disposed at publish, which is AFTER).
        total += 1
    return total


def writer_worktrees(config: pipeline.PipelineConfig) -> int:
    """
    How many BUILD worktrees one pass leaves on the host.

    Only writers are counted. Every node that cuts a clone pays for its
    checked-out tree, but a node clone is a LOCAL clone of the run's hub
    (hardlinked objects), so the repo itself is nearly free and the real
    cost is what the agent builds in it — which only a writer does.
    Measured: writer worktrees at 0.7 to 2.5 GB against reader and
    judge worktrees at 140 to 520 KB, four orders apart. Reviewers
    cut nothing at all; they mount the writer's tree ``:ro``.

    The verification gate's throwaway clone counts as a writer: it runs
    the project's build and test command from clean.

    :param config: The parsed pipeline.
    :returns: The build-worktree count for one pass.
    """
    total = sum(
        1
        for stage in pipeline._iter_stages(config.stages)
        if stage.run and PipelineRunner._stage_kind(stage) == 'writer'
    )
    if config.verify is not None:
        total += 1
    return total


def _resume_worktree_count(worktree_root: str, run_id: str) -> int:
    """
    How many node worktrees a resume already has on disk.

    Counted from the filesystem rather than the state file: what matters
    to the disk estimate is what is actually THERE, and a run that
    crashed can have state and reality disagree in either direction.

    :param worktree_root: Host dir holding the run directories.
    :param run_id: The run being resumed.
    :returns: The count, or ``0`` when the run directory is absent.
    """
    nodes = Path(worktree_root) / run_id / 'nodes'
    try:
        return sum(1 for child in nodes.iterdir() if child.is_dir())
    except OSError:
        return 0


def reclaim_for_resume(
    *,
    run_id: str,
    canonical_root: str,
    worktree_root: str,
    server: str,
    keep: bool,
    default_branch: str,
    client: SwarmSessionClient | None = None,
    manager: WorktreeManager | None = None,
    echo: Callable[[str], None] = click.echo,
) -> int:
    """
    Free what the previous attempt provably no longer needs.

    Runs BEFORE :func:`preflight_disk`, which is the whole point. The
    reclaim used to live inside ``runner.run()`` — downstream of the
    gate — so a resume was refused by space the resume itself would have
    freed seconds later. Observed live: a machine crashed mid-module
    leaving six orphaned microVMs holding ~26 GB, the host measured 15.6
    GB free against a 46.5 GB demand, and the reclaim that would have
    returned the 26 GB sat behind the refusal (TASKS.md #7).

    Two things are provably dead on a resume:

    * the microVMs the previous attempt left running — their ids are in
      the run state, so this needs no discovery
    * the worktrees of chunks already recorded complete — their branches
      are on the hub and their pull requests are open

    Best-effort throughout: a resume must not fail because something it
    was cleaning is already gone.

    :param run_id: The run being resumed.
    :param canonical_root: Host dir holding the bare mirrors.
    :param worktree_root: Host dir holding the run directories.
    :param server: Omnigent server URL.
    :param keep: Whether ``--keep`` is set — it suppresses every
        reclaim, because the human asked for the VMs to stay.
    :param default_branch: The manager's base branch.
    :param client: Session client (injected in tests).
    :param manager: Worktree manager (injected in tests).
    :param echo: Output sink (injected in tests).
    :returns: How many node worktrees were removed, so the caller can
        tell the preflight what is no longer on disk.
    """
    wt = manager or WorktreeManager(
        canonical_root=canonical_root,
        worktree_root=worktree_root,
        default_branch=default_branch,
    )
    state = wt.read_run_state(run_id)
    if not state:
        return 0
    if keep:
        echo(
            '[resume] --keep: reclaiming nothing before the disk check, '
            'since the previous attempt was asked to hold its VMs and '
            'worktrees.'
        )
        return 0
    sessions = [s for s in state.get('sessions') or [] if isinstance(s, str)]
    if sessions:
        sc = client or SwarmSessionClient(server)
        gone = 0
        for session in sessions:
            try:
                sc.dispose(session)
                gone += 1
            except SwarmSessionError:
                pass  # already torn down, or the server lost it
        echo(
            f'[resume] disposed {gone}/{len(sessions)} microVM(s) the '
            f'previous attempt left running, before measuring disk.'
        )
    freed = 0
    done = [
        c for c in state.get('completed_chunks') or []
        if isinstance(c, str)
    ]
    nodes = [n for n in state.get('nodes') or {} if isinstance(n, str)]
    for chunk in done:
        # A published chunk's branches are on the hub and its pull
        # request is open; its worktrees are the largest thing a resume
        # is still carrying and it can rebuild none of them.
        stale = [n for n in nodes if n == chunk or n.startswith(f'{chunk}-')]
        if not stale:
            continue
        try:
            freed += wt.dispose_node_worktrees(run_id, stale)
        except click.ClickException:
            continue
    if freed:
        echo(
            f'[resume] reclaimed {freed} worktree(s) from '
            f'{len(done)} published module(s) before measuring disk.'
        )
    return freed


def preflight_disk(
    config: pipeline.PipelineConfig,
    *,
    keep: bool = False,
    path: Path | None = None,
    usage: Callable[..., object] = shutil.disk_usage,
    per_vm_bytes: int | None = None,
    per_worktree_bytes: int | None = None,
    floor_bytes: int | None = None,
    worktrees_on_disk: int = 0,
) -> None:
    """
    Refuse to start a run the host has no disk to finish.

    A microVM's disk is thin-provisioned: the guest sees a roomy
    filesystem while the host grows the backing file on demand. When the
    host runs out, the guest's writes fail as I/O errors and ext4
    protects itself by REMOUNTING READ-ONLY mid-run. Nothing reports
    this as a disk problem — the agent simply starts failing to write,
    and the harness surfaces an opaque ``[Errno 30] Read-only file
    system`` on a temp file. Observed live: five of six VMs went
    read-only and the run died without ever naming the cause.

    So check up front, where the message can name it.

    The estimate has TWO terms, because a run occupies the host twice
    over: the microVMs, and the host worktrees they mount. Counting only
    the VMs is what let a run start on ~23 GB and then exhaust the disk
    two modules in — the worktrees were the larger half.

    :param config: The parsed pipeline (sizes both counts).
    :param keep: Whether ``--keep`` is set. It suppresses EVERY
        reclaim — each module's VMs and worktrees, and each review
        round's reviewers — so the estimate both stops collapsing
        reviewers to one stage and multiplies by the module count,
        rather than letting that be discovered at module four. (A
        planner-proposed campaign's chunk count is unknown until it
        runs, so that case is still counted as one pass.)
    :param path: Filesystem to measure; ``None`` uses the home
        directory, where sbx keeps its per-sandbox images.
    :param usage: ``shutil.disk_usage``-alike (injected in tests).
    :param per_vm_bytes: Disk per microVM; ``None`` takes the
        pipeline's ``disk:`` value.
    :param per_worktree_bytes: Disk per build worktree; ``None`` takes
        the pipeline's ``disk:`` value.
    :param floor_bytes: Headroom to leave the host; ``None`` takes the
        pipeline's ``disk:`` value.
    :param worktrees_on_disk: Writer worktrees a RESUME already has,
        subtracted from the estimate. A resumed run does not re-cut the
        trees it already carries, and demanding their space a second
        time is what refused a resume for roughly twice its honest
        need (TASKS.md #7).
    :raises click.ClickException: If free space is below the estimate.
    """
    gb = 1_000_000_000
    spec = config.disk
    per_vm = (
        int(spec.per_vm_gb * gb) if per_vm_bytes is None else per_vm_bytes
    )
    per_tree = (
        int(spec.per_worktree_gb * gb)
        if per_worktree_bytes is None
        else per_worktree_bytes
    )
    floor = (
        int(spec.headroom_gb * gb) if floor_bytes is None else floor_bytes
    )
    # --keep holds every module's VMs AND worktrees to the end of the
    # run; without it each module's are reclaimed as it publishes.
    passes = len(config.subtasks) if keep and config.subtasks else 1
    vms = max_concurrent_vms(config, reclaim=not keep) * passes
    trees = max(0, writer_worktrees(config) * passes - worktrees_on_disk)
    needed = floor + vms * per_vm + trees * per_tree
    free = usage(path or Path.home()).free
    if free >= needed:
        return
    held = (
        f' — {passes} modules held at once, because --keep never '
        f'reclaims them'
        if passes > 1
        else ''
    )
    advice = (
        "Drop --keep so each module's VMs and worktrees are reclaimed "
        'as it publishes, free some space, '
        if passes > 1
        else 'Free some space, '
    )
    # Name the leaked guest disks if there are any. They are invisible
    # to every reclaim the launcher can do and to `sbx ls` itself, so a
    # refusal that omits them sends someone hunting the wrong things.
    leaked = orphans.orphan_advice()
    raise click.ClickException(
        f'not enough free disk to run this pipeline: {free / gb:.1f} GB '
        f'free, about {needed / gb:.1f} GB needed{held}: {vms} '
        f'microVM(s) at ~{per_vm / gb:.1f} GB, {trees} host worktree(s) '
        f'at ~{per_tree / gb:.1f} GB (a writer builds into its clone), '
        f'plus {floor / gb:.0f} GB headroom. A microVM disk is thin-'
        f'provisioned, so running the host out mid-run does not fail '
        f'cleanly: the guest filesystem remounts READ-ONLY and the '
        f'agents start failing with opaque I/O errors. {advice}tune '
        f'`disk:` in the pipeline if these per-unit estimates do not fit '
        f'your project, or pass --skip-disk-check to override.'
        + (f'\n\n{leaked}' if leaked else '')
    )


def resolve_publish_token(
    token_file: str | None, token_command: str | None
) -> str | None:
    """
    Resolve the publish identity's token from a file or a command.

    Keeps the secret OFF the command line: the CLI carries a path or a
    retrieval command, never the token itself, so nothing sensitive
    reaches shell history or ``ps``. The command is run as argv (split
    with :func:`shlex.split`), NOT through a shell, and both options are
    CLI-only — deliberately never ``pipeline.yaml`` keys, since a token
    command in a shareable pipeline file would make merely running that
    file arbitrary code execution on the host.

    Any credential store works, which keeps this portable: a keychain
    read, ``pass``, ``vault``, a cloud secret manager, or a mode-0600
    file. The launcher never learns which.

    :param token_file: Path to a file containing ONLY the token.
    :param token_command: Command whose stdout is the token.
    :returns: The token, or ``None`` when neither is set — publish then
        uses whatever git/gh credentials the host already has.
    :raises PipelineRunError: If both are set, the file/command fails,
        or the resolved token is empty.
    """
    if token_file and token_command:
        raise PipelineRunError(
            'set only one of --publish-token-file / '
            '--publish-token-command'
        )
    if token_file:
        try:
            token = Path(token_file).read_text(encoding='utf-8')
        except OSError as exc:
            raise PipelineRunError(
                f'cannot read --publish-token-file {token_file!r}'
            ) from exc
    elif token_command:
        argv = shlex.split(token_command)
        if not argv:
            raise PipelineRunError('--publish-token-command is empty')
        try:
            proc = subprocess.run(
                argv, capture_output=True, text=True, check=False
            )
        except OSError as exc:
            raise PipelineRunError(
                f'--publish-token-command could not run {argv[0]!r}'
            ) from exc
        if proc.returncode != 0:
            raise PipelineRunError(
                f'--publish-token-command exited {proc.returncode}: '
                f'{proc.stderr.strip()}'
            )
        token = proc.stdout
    else:
        return None
    token = token.strip()
    if not token:
        raise PipelineRunError('the publish token resolved empty')
    return token


def agy_agent_names(config: pipeline.PipelineConfig) -> list[str]:
    """
    The pipeline's antigravity-native agent names, sorted.

    :param config: The parsed pipeline.
    :returns: Agent names bound to an agy harness (empty when none).
    """
    return sorted(
        name
        for name, a in config.agents.items()
        if a.harness in agy.AGY_HARNESSES
    )


def codex_agent_names(config: pipeline.PipelineConfig) -> list[str]:
    """
    The pipeline's codex-native agent names, sorted.

    :param config: The parsed pipeline.
    :returns: Agent names bound to a Codex harness (empty when none).
    """
    return sorted(
        name
        for name, a in config.agents.items()
        if a.harness in codex.CODEX_HARNESSES
    )


def preflight_codex_auth(
    config: pipeline.PipelineConfig,
    *,
    path: Path | None = None,
    now: datetime | None = None,
    echo: Callable[[str], None] = click.echo,
) -> None:
    """
    Refuse to start a Codex pipeline on a dead or dying credential.

    Deliberately runs BEFORE any microVM is provisioned. Codex access
    tokens last ~240 hours, so unlike agy there is no refresh daemon to
    lean on — the credential is checked once, up front, and a run that
    cannot finish is stopped before it costs anything.

    Two outcomes:

    * missing or expired -> refuse, naming the exact remedy
    * expiring within six hours -> WARN and continue, so a long campaign
      is not started on a token that dies partway through and fails
      every turn after it

    A pipeline with no Codex agents is a no-op.

    :param config: The parsed pipeline.
    :param path: Override for the host credential file (tests).
    :param now: Override for the clock (tests).
    :param echo: Output sink (injected in tests).
    :raises click.ClickException: If there is no usable credential.
    """
    names = codex_agent_names(config)
    if not names:
        return
    try:
        warning = codex.preflight(path=path, now=now)
    except codex.CodexAuthError as exc:
        raise click.ClickException(
            f'this pipeline has Codex agent(s) '
            f'({", ".join(names)}) but {exc}'
        ) from exc
    if warning:
        echo(f'[preflight] {warning}')


def stop_harvester(proc: subprocess.Popen[bytes] | None) -> None:
    """
    Stop a harvester this run started, if any.

    :param proc: The child from :func:`agy.spawn_harvester`, or ``None``
        when the run did not start one (nothing to stop).
    """
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
    except OSError:
        pass


def ensure_agy_harvester(
    config: pipeline.PipelineConfig,
    *,
    auto: bool = True,
    stamp_path: Path | None = None,
    lock_path: Path | None = None,
    log_path: Path | None = None,
    deadline_s: float = 180.0,
    spawn: Callable[..., subprocess.Popen[bytes]] | None = None,
    wait: Callable[..., bool] | None = None,
    now: float | None = None,
) -> subprocess.Popen[bytes] | None:
    """
    Guarantee a fresh agy swap secret, starting the harvester if needed.

    An agy pipeline has a hidden prerequisite: a harvester loop must be
    running or every agy turn dies unauthenticated — silently, inside
    agy's own TUI, so the runner just blocks to the turn timeout. Making
    the human remember to background a second command is a footgun, so
    the runner starts one itself and stops it when the run ends.

    Skipped entirely for a pipeline with no agy agents, and a no-op when
    a harvester is already running and the secret is fresh (that loop
    owns refreshing). When one is running but the secret has gone stale,
    this waits for its next refresh rather than starting a competing one
    — only a single harvester may poke the box (see
    :func:`agy.acquire_harvest_lock`).

    The decision is "is a harvester RUNNING", never "is the secret fresh
    right now": a fresh secret with nothing refreshing it expires about
    an hour in, stranding every agy turn after that.

    :param config: The parsed pipeline.
    :param auto: Start one when missing; ``False`` restores the old
        behavior of refusing with instructions (for a harvester run on
        another host).
    :param stamp_path: Harvest stamp; ``None`` uses the default.
    :param lock_path: Harvester lock; ``None`` uses the default.
    :param log_path: Where an auto-started harvester logs.
    :param deadline_s: How long to wait for the first refresh.
    :param spawn: Launcher (injected in tests).
    :param wait: Freshness waiter (injected in tests).
    :param now: Epoch seconds to measure staleness from.
    :returns: The harvester this call started (the caller MUST stop it
        via :func:`stop_harvester`), or ``None`` when it started none.
    :raises click.ClickException: If the secret cannot be made fresh.
    """
    if not agy_agent_names(config):
        return None
    # Gate on whether a harvester is RUNNING — NOT on whether the secret
    # happens to be fresh this second. Freshness at startup says nothing
    # about freshness 90 minutes in: an agy token lives about an hour,
    # and a run that starts no harvester because the secret looked fine
    # goes stale mid-flight. Every agy turn driven after that then hangs
    # silently to the turn timeout. Observed exactly that: last refresh
    # 23:32, the agy coder driven 00:53, dead at 1800s with an empty
    # session.
    running = agy.harvester_running(path=lock_path)
    age = agy.harvest_age_s(path=stamp_path, now=now)
    fresh = age is not None and age <= agy.MAX_SWAP_AGE_S
    proc: subprocess.Popen[bytes] | None = None
    if running:
        # Someone already owns refreshing for the whole run.
        if fresh:
            click.echo('[agy] a harvester is already running.')
            return None
        click.echo(
            '[agy] a harvester is running but the swap secret is stale '
            '— waiting for its next refresh.'
        )
    elif not auto:
        # Opted out: refuse when stale, else trust the caller that
        # something off-host keeps it fresh.
        preflight_agy(config, stamp_path=stamp_path, now=now)
        return None
    else:
        proc = (spawn or agy.spawn_harvester)(log_path=log_path)
        click.echo(
            f'[agy] started a token harvester (pid {proc.pid}); '
            f'logging to {log_path or agy.HARVEST_LOG}'
        )
    if fresh:
        # Already usable — start driving now; the harvester just
        # started keeps it fresh for the rest of the run.
        click.echo('[agy] swap secret is fresh.')
        return proc
    if not (wait or agy.wait_for_fresh_swap)(
        deadline_s=deadline_s, stamp_path=stamp_path
    ):
        stop_harvester(proc)
        raise click.ClickException(
            f'the agy swap secret did not go fresh within '
            f'{deadline_s:.0f}s, so agent(s) '
            f'{", ".join(agy_agent_names(config))} would fail to '
            f'authenticate and hang until the turn timeout. Check the '
            f'harvester log ({log_path or agy.HARVEST_LOG}) — a trusted '
            f"box that cannot refresh needs 'agy /login' run on it."
        )
    click.echo('[agy] swap secret is fresh.')
    return proc


def preflight_agy(
    config: pipeline.PipelineConfig,
    *,
    stamp_path: Path | None = None,
    now: float | None = None,
) -> None:
    """
    Refuse to start an agy pipeline whose swap secret has gone stale.

    An agy agent VM carries only a PLACEHOLDER token; the sbx swap
    secret supplies the real one on the wire, and only a running
    harvester (``omni-sbx-agy harvest``) keeps that secret fresh. With
    no harvester the token expires and every agy turn dies
    unauthenticated — but agy reports that inside its own TUI, which the
    bridge never surfaces as a failed session, so the runner just blocks
    to the per-turn timeout (30 min) and then reports a misleading "did
    not complete" (observed live). Checking the harvest stamp up front
    turns the single most common agy failure into an actionable error in
    seconds, before one VM is provisioned. No-op for a pipeline with no
    agy agents.

    :param config: The parsed pipeline.
    :param stamp_path: Harvest stamp; ``None`` uses the default.
    :param now: Epoch seconds to measure from; ``None`` = the clock.
    :raises click.ClickException: When the pipeline declares an agy
        agent and the swap secret's freshness is unknown or too old.
    """
    agy_agents = agy_agent_names(config)
    if not agy_agents:
        return
    age = agy.harvest_age_s(path=stamp_path, now=now)
    if age is not None and age <= agy.MAX_SWAP_AGE_S:
        return
    when = (
        'has never been refreshed on this host'
        if age is None
        else f'was last refreshed {age / 60:.0f} min ago'
    )
    raise click.ClickException(
        f'pipeline {config.name!r} runs antigravity-native agent(s) '
        f'({", ".join(agy_agents)}), but the agy swap secret {when} '
        f'(stale after {agy.MAX_SWAP_AGE_S / 60:.0f} min). Its access '
        'token has almost certainly expired, so every agy turn would '
        'fail to authenticate and hang until the turn timeout.\n'
        '  Start the token harvester, then re-run:\n'
        '      omni-sbx-agy harvest         # always-on refresh loop '
        '(recommended)\n'
        '      omni-sbx-agy harvest --once  # one refresh, for a short '
        'run\n'
        '  Pass --skip-agy-check to run anyway (e.g. the harvester runs '
        'on another host).'
    )


@click.command('pipeline')
@click.option(
    '-c',
    '--config',
    'config_path',
    required=True,
    type=click.Path(exists=True),
    help='Path to the pipeline.yaml.',
)
@click.option(
    '--server',
    envvar='OMNI_SERVER',
    default='http://localhost:6767',
    show_default=True,
)
@click.option(
    '--canonical-root', envvar='OMNI_SBX_CANONICAL_ROOT', required=True
)
@click.option(
    '--worktree-root', envvar='OMNI_SBX_WORKTREE_ROOT', required=True
)
@click.option('--run-id', default=None, help='Unique run id (default: name).')
@click.option(
    '--publish-repo', default=None, help='Push target (default: repo).'
)
@click.option(
    '--publish-token-file',
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help='File holding ONLY the token used to push and open PRs, so a '
    'dedicated pipeline identity ships the work instead of yours. '
    'Keeps the secret out of shell history and ps.',
)
@click.option(
    '--publish-token-command',
    default=None,
    help='Command whose stdout is that token (a keychain, pass, vault, '
    'or secret-manager read). Run as argv, never through a shell.',
)
@click.option(
    '--resume',
    is_flag=True,
    help='Continue the run with this --run-id instead of starting '
    'clean: reuse its hub, skip the stages it already finished, and '
    're-cut only what is left.',
)
@click.option(
    '--turn-timeout',
    type=float,
    default=None,
    help="Seconds one agent turn may take (overrides the pipeline's "
    'turn_timeout). Raise it when stages stop to ask you questions — '
    'that wait is spent inside the turn budget.',
)
@click.option(
    '--skip-disk-check',
    is_flag=True,
    help='Skip the preflight that refuses to start when free disk '
    'cannot cover the run (a host that fills mid-run leaves guest '
    'filesystems remounted read-only).',
)
@click.option(
    '--no-auto-harvest',
    is_flag=True,
    help='Do not start an agy token harvester for this run; refuse '
    'instead when the swap secret is stale (use when a harvester '
    'already runs elsewhere).',
)
@click.option('--keep', is_flag=True, help='Leave VMs + worktrees.')
@click.option(
    '--no-interactive-plan',
    is_flag=True,
    help='Do not block the planning phase on human approval (automated '
    "runs); use the planner's single-turn output as-is.",
)
@click.option(
    '--no-auto-approve',
    is_flag=True,
    help='Fail a turn that opens an interactive prompt instead of '
    'answering it. The default answers, because the microVM is the '
    'containment boundary — a prompt inside one is pure latency. Use '
    'this to watch what agents actually ask for.',
)
@click.option(
    '--skip-agy-check',
    is_flag=True,
    help='Skip the preflight that refuses to start an agy pipeline on a '
    'stale swap secret (use when the harvester runs on another host).',
)
def main(
    config_path: str,
    server: str,
    canonical_root: str,
    worktree_root: str,
    run_id: str | None,
    publish_repo: str | None,
    publish_token_file: str | None,
    publish_token_command: str | None,
    resume: bool,
    turn_timeout: float | None,
    skip_disk_check: bool,
    no_auto_harvest: bool,
    keep: bool,
    no_interactive_plan: bool,
    no_auto_approve: bool,
    skip_agy_check: bool,
) -> None:
    """Fire a pipeline.yaml; provision-only when it has no task."""
    config = pipeline.load_pipeline(config_path)

    def publish_token_provider() -> str | None:
        return resolve_publish_token(
            publish_token_file, publish_token_command
        )

    # Resolve ONCE now to validate: a bad path or a failing keychain
    # read should cost two seconds, not a finished module. The VALUE is
    # deliberately discarded — publish re-reads it at push time, hours
    # later, because a token captured here can be rotated or expired by
    # then and a run that publishes at the end has no way to notice
    # (TASKS.md #43).
    publish_token_provider()
    on_disk = 0
    if resume:
        # BEFORE the gate, not inside runner.run() behind it: a resume
        # was refused by space the resume itself would have freed
        # seconds later (TASKS.md #7).
        on_disk = _resume_worktree_count(worktree_root, run_id or config.name)
        on_disk -= reclaim_for_resume(
            run_id=run_id or config.name,
            canonical_root=canonical_root,
            worktree_root=worktree_root,
            server=server,
            keep=keep,
            default_branch=config.base_branch or 'main',
        )
    if not skip_disk_check:
        # A host that fills mid-run corrupts the VMs it is hosting, so
        # refuse here rather than discover it as read-only filesystems.
        preflight_disk(config, keep=keep, worktrees_on_disk=max(0, on_disk))
    # Before ANY VM is provisioned: a Codex pipeline on a dead token
    # would otherwise fail every turn, minutes and several microVMs in.
    preflight_codex_auth(config)
    harvester: subprocess.Popen[bytes] | None = None
    if not skip_agy_check:
        # An agy pipeline needs a live harvester or every agy turn dies
        # unauthenticated. Start one (and stop it at the end) rather
        # than making the human remember a second background command.
        harvester = ensure_agy_harvester(
            config, auto=not no_auto_harvest
        )
    try:
        _drive(
            config,
            resume=resume,
            turn_timeout=turn_timeout,
            server=server,
            canonical_root=canonical_root,
            worktree_root=worktree_root,
            run_id=run_id,
            publish_repo=publish_repo,
            publish_token=publish_token_provider,
            keep=keep,
            no_interactive_plan=no_interactive_plan,
            no_auto_approve=no_auto_approve,
        )
    finally:
        stop_harvester(harvester)


def _drive(
    config: pipeline.PipelineConfig,
    *,
    resume: bool = False,
    turn_timeout: float | None = None,
    server: str,
    canonical_root: str,
    worktree_root: str,
    run_id: str | None,
    publish_repo: str | None,
    publish_token: str | Callable[[], str | None] | None,
    keep: bool,
    no_interactive_plan: bool,
    no_auto_approve: bool,
) -> None:
    """Build the runner, drive the run, and report its outcome."""
    client = SwarmSessionClient(server, auto_approve=not no_auto_approve)
    ids = resolve_agent_ids(config, client.list_builtin_agents())
    runner = PipelineRunner(
        config,
        session_client=client,
        worktree_manager=WorktreeManager(
            canonical_root=canonical_root,
            worktree_root=worktree_root,
            default_branch=config.base_branch or 'main',
            publish_token=publish_token,
            build_cache=config.build_cache,
            build_cache_key=_repo_name(config.repo),
        ),
        run_id=run_id or config.name,
        agent_ids=ids,
        publish_repo=publish_repo,
        keep=keep,
        interactive_plan=not no_interactive_plan,
        resume=resume,
        **(
            {'turn_timeout': turn_timeout}
            if turn_timeout is not None
            else {'turn_timeout': config.turn_timeout}
            if config.turn_timeout is not None
            else {}
        ),
    )
    result = runner.run()
    click.echo(f'status: {result.status}')
    if result.status == 'provisioned':
        click.echo(
            'provisioned (VMs kept up; drive via the UI or '
            'omni-sbx-swarm send):'
        )
        for b in result.bindings:
            click.echo(
                f"  {b['node']} [{b['mode']}] agent={b['agent']} "
                f"session={b['session']}"
            )
        return
    if result.blocked_stage:
        click.echo(f'blocked at: {result.blocked_stage}')
    if result.published:
        click.echo(f'published: {result.published}')


if __name__ == '__main__':
    main()
