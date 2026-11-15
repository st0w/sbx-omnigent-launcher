"""Turn text for writers: finishing, fixing, and the frozen contract."""

from __future__ import annotations

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
#: lever there is. Observed live on gcp-custom-roles-1: a coder built a
#: swapfile to survive a large Rust build, removed it afterwards, and
#: lost the turn to a modal nobody could answer.
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

#: Where a writer builds, and how much room it has (#144).
#:
#: Its worktree is a mount from the host. Live, builds through it ran
#: many times slower than on the VM's own disk (the gate's clippy took
#: over 50 minutes through the mount, about one on its own disk), and
#: one read back a truncated object. Both coders moved their builds
#: off the mount on their own, and one filled its fixed 20 GB disk,
#: partly with a local coverage build the gate recomputes anyway.
_WRITER_BUILD = (
    "Build OUTSIDE your worktree, on this VM's own disk: for Cargo, "
    '`export CARGO_TARGET_DIR=$HOME/build-target` in every shell before '
    'any cargo command; other toolchains have an equivalent. Your '
    'worktree is mounted from the host, and building through it is '
    'many times slower. Edit and commit in the worktree as usual. This '
    "VM's disk is a fixed 20 GB, shared with the toolchain, so do NOT "
    'run a coverage build: the verify gate measures coverage in its own '
    'sandbox, and a local coverage build costs gigabytes for a number '
    'it recomputes.'
)

#: The line a writer's reply must end with to count as finished.
_WORK_COMPLETE = 'WORK COMPLETE'

#: Told to every writer turn, the first and every re-drive. Live
#: (k8s-clusters-1, k2-tests), a test writer ended its turn twice
#: saying it was waiting for a background compile, and both times the
#: runner committed the tree as finished: first with temporary stubs in
#: production code, then with them still there. Finished must now be
#: said, as a check's pass must (#143).
_WRITER_FINISH = (
    '\n\nWHEN YOU ARE DONE. Run long commands in the background if you '
    'like: if you end your turn while one is still running, the pipeline '
    'waits for it to finish and for the reply you give when you are '
    'woken. Your work is committed once your reply ends with a line '
    f'reading exactly `{_WORK_COMPLETE}`, so finish the work first, '
    'including removing anything temporary you added. A reply without '
    'that line, with nothing left running, is treated as unfinished: you '
    'are asked once to finish, and after that the run stops. A reply '
    'raising a `DISPUTED:` line needs no finish line.'
)

#: Sent once to a writer whose reply neither disputed nor said it was
#: finished.
_WRITER_FINISH_ASK = (
    f'Your reply did not end with a line reading exactly '
    f'`{_WORK_COMPLETE}`, so your turn is not finished and nothing has '
    f'been committed. Finish the work, remove anything temporary you '
    f'added, and end your reply with a line reading exactly '
    f'`{_WORK_COMPLETE}`.'
)
_UNATTENDED = (
    'You are running UNATTENDED in an automated pipeline. No human is '
    'watching this turn and there is no interactive channel: a '
    'question, a confirmation prompt, or a request for approval will '
    'not be seen, and it stalls the pipeline until the turn times out. '
    'When requirements conflict, resolve it yourself — the frozen '
    'tests and the stated invariants are the tie-breakers — then STATE '
    'THE DECISION AND YOUR REASONING IN YOUR REPLY and carry on. If it '
    'genuinely cannot be resolved that way, say so in your reply and '
    'stop — on a line of its own that BEGINS with `DISPUTED:` and then '
    'states the claim, as in `DISPUTED: closing this needs the frozen '
    'assertion at tests/x.py:50 changed`. That exact shape is what '
    'reaches the orchestrator; the word anywhere else, including in a '
    'heading, is invisible to it and your dispute is silently lost. '
    'Your reply is the only channel that '
    'reaches a human OR a reviewer: a decision you do not write there '
    'is invisible to both, and a reviewer who cannot see why you did '
    'something is right to block on it.'
)

#: What the tests stage is held to, restated on every one of its
#: turns.
#:
#: Its suite is the contract two implementers build against and every
#: reviewer judges against, and no downstream agent may change it — so
#: a wrong assertion is undone only by halting the run for a human
#: ruling. A wrong test costs more than a missing one, and nothing
#: said so.
#:
#: Five disputes on one campaign were the same shape: assertions about
#: the outside world, frozen by the one stage with no way to check
#: them. They passed at the tests stage, at both implementations and
#: in every reviewer's full run, because they are tautological — the
#: stage authors the claim and the test compares the claim against a
#: copy of itself. The first party with independent knowledge is a
#: reviewer, by which point the only move left is a dispute.
#:
#: Both halves are stated with examples. The forbidden half alone
#: reads as "write fewer tests", which is not the ask.
_FROZEN_TESTS_CONTRACT = (
    'YOUR TESTS ARE FROZEN. When this stage ends, every assertion you '
    'wrote becomes the contract two independent implementers build '
    'against and every reviewer judges against, and no downstream '
    'agent may change one — an implementer that edits your suite has '
    'broken the boundary the whole pipeline rests on. Correcting a '
    'wrong assertion means halting the run for a human ruling, so a '
    'test that is WRONG costs far more than a test that is missing.\n\n'
    'So assert the INVARIANT, never a value you cannot check from in '
    'here. You may freeze shape, bounds, refusals and floors: that a '
    'loader rejects a four-segment name, that an ambiguous key '
    'resolves to nothing rather than guessing, that a recorded set '
    'may not shrink, that a value round-trips unchanged. You may not '
    'freeze a claim about the outside world: that a particular vendor '
    'method maps to a particular permission, that a specific status '
    'means success, that an external identifier is spelled a certain '
    'way. You cannot verify those here, and a wrong one is wrong for '
    'both implementers at once.\n\n'
    'Facts belong in the DATA an implementer writes — the catalog, the '
    'mapping table, the fixture — where a reviewer that finds one '
    'wrong has it corrected by an ordinary loop-back. Assert that the '
    'data is well-formed, complete and self-consistent. Do not assert '
    'that a particular row of it is true.'
)
