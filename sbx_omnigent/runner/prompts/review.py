"""Turn text for reviewers: what to verify, report and vote."""

from __future__ import annotations

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
    'meets the contract and you verified it; `VERDICT: BLOCKING` '
    'followed by the specific findings — including "I could not finish '
    'verifying", if that is what happened; or `VERDICT: UNVERIFIED` '
    'followed by what stopped you, if the environment, not the code, '
    'stopped the build or the tests and you found no defect. Do not '
    'resume reviewing; just state where you got to.'
)

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

#: Where a reviewer puts a check the SANDBOX stopped (#41). Before it,
#: an audit that could not reach its advisory database was filed as a
#: finding against the code by every reviewer, every round: 15 of 174
#: filed findings in one module were that one sentence.
#:
#: The second half is what keeps it from becoming a way to approve
#: unexecuted code. The marker stays mid-sentence, so a reviewer
#: quoting this back lifts nothing.
_ENVIRONMENT_ASK = (
    '\n\nIf the SANDBOX, not the code, stopped you running a '
    'supplementary check — an audit whose advisory database sits behind '
    'a host the sandbox refuses, a scanner whose download is blocked — '
    'list it in a block headed `ENVIRONMENT-LIMITS:`, one `- ` item per '
    'check: what you tried to run and what stopped it, naming the host '
    'if one was refused. Each is recorded once for this review stage and '
    'shown on the pull request; it is not filed as a finding, so do not '
    'repeat it under `DEFECTS:`. This block is NEVER for the build or the '
    'test suite: if the environment stopped those, you could not '
    'verify, and your verdict is `VERDICT: UNVERIFIED`, never an '
    'approval. A check listed here was not run, so nothing in your '
    'verdict may rest on it.'
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
#: The read-only mount note, shared by every review instruction.
_REVIEW_MOUNT = (
    '\n\nYour mount is READ-ONLY: you can read and build the tree but '
    'not write into it. Build somewhere on the VM'
    "'s own disk instead — for Cargo, `export "
    'CARGO_TARGET_DIR=/tmp/review-target` before any cargo command; '
    'other toolchains have an equivalent. DO NOT COPY the tree anywhere '
    'first, and do not try to make the mount writable: cargo builds a '
    'read-only source directory fine with the target directory '
    'elsewhere, the copy is gigabytes for nothing, and one reviewer '
    'that reached for `rm -rf` to clear space stalled its whole turn on '
    'a permission prompt.'
)

#: Run it, do not read it. Shared by every review instruction.
_REVIEW_VERIFY = (
    '\n\nBefore any verdict, CHECK THAT THE CODE IS REALLY THERE and '
    'that the test suite really passes — run it. If the tooling you '
    'need is missing, install it (see the environment notes above); '
    'your VM can reach an allowlist of hosts for that. You may NOT '
    'return `VERDICT: '
    'APPROVED` on a change you could not execute, and "the code looks '
    'correct" is not a substitute for running it. If you could not '
    'verify, say exactly what stopped you, and pick the verdict by WHAT '
    'stopped you. A defect in this branch, including its own code '
    'failing to compile, is `VERDICT: BLOCKING`. The ENVIRONMENT — a '
    'toolchain you could not install, a dependency failing to build '
    "before any of this branch's code compiles, a host the sandbox "
    'refuses — with no defect found in the code, is `VERDICT: '
    'UNVERIFIED`. That is never an approval: the review runs again on a '
    'fresh VM, and the run stops for a human if it still cannot verify. '
    'It is not sent to the writer, who cannot fix the sandbox. An empty '
    'or placeholder implementation that merely compiles-by-absence is a '
    'BLOCKING finding.'
)

#: A verdict is a release decision, not a weighted opinion.
_REVIEW_NOT_WEIGHTED = (
    '\n\nA verdict is not a weighted opinion. Every finding you report '
    'as BLOCKING must be one you would hold the release for. If you '
    'would discount a finding — as minor, pre-existing, external, '
    "upstream drift, someone else's module, not attributable to this "
    'change{later} — then it is NOT blocking: put it in a clearly '
    'separate NON-BLOCKING list and keep it out of the reasoning for '
    'your verdict. Never state a blocking finding and argue it down in '
    'the same breath. The writer reads a hedged finding as permission '
    'to make the symptom go away rather than the cause, and a gate that '
    'was weakened to clear a finding you had already discounted is the '
    'worst outcome this pipeline can produce.'
)

#: The two tokens a review must end on.
#: Handed to every reviewer above the decisions ledger. A reviewer
#: that cannot see what the human already settled re-opens it:
#: observed on `ingestion-m3-1`, where the [m3c] planner recorded "one
#: frozen assertion was amended under explicit human authorization"
#: into the ledger exactly as asked, the ledger reached the next
#: PLANNER and nobody else, and both bug reviewers found the amended
#: assertion, could not know it was authorized, and raised DISPUTED.
#: That was the right move on the evidence they held; the evidence was
#: the bug.
#:
#: The second paragraph is the other half, and it is not optional. A
#: ruling fixes what the code may LOOK like, never whether it is
#: right, and a reviewer that reads the ledger as permission to
#: approve a defect is worse than one that never saw it.
_REVIEW_SETTLED = (
    '\n\nSETTLED ALREADY, with the human. Each line below was '
    'recorded by an approved plan — for this increment or for an '
    'earlier one — and BINDS the code you are reading. Read them '
    'BEFORE '
    'you block on something that looks like a contract violation: a '
    'frozen artifact amended under authorization, or a rule stricter '
    'than the brief reads literally, is a question already answered, '
    'and re-opening it spends a round arriving where the human '
    'already is.\n\nA settled decision is not a defence for a '
    'defect. It says what the code is ALLOWED to look like, never '
    'whether it is correct: if the code is wrong it is wrong whether '
    'or not a decision explains its shape. Block, and name the '
    'decision you read.\n\n'
)
_REVIEW_VERDICT_LINE = (
    '\n\nEnd your reply with a single verdict line, using one of these '
    'three exact tokens: `VERDICT: APPROVED` (the change meets the '
    'contract, and you verified it), `VERDICT: BLOCKING` (it does not, '
    'or you could not finish verifying it), or `VERDICT: UNVERIFIED` '
    '(the environment stopped the build or the tests, and you found no '
    'defect in the code).'
)

#: How a REVIEWER reports a contract that cannot be satisfied.
#:
#: `Disputes.halt_on_review` stops the run on a blocking reviewer's
#: dispute, but reviewers were never told how to raise one:
#: implementation reviewers met the marker only in :data:`_UNATTENDED`,
#: whose wording and example are a writer's, and refactor reviewers
#: never met it. A reviewer who could not name the channel blocked
#: instead, re-driving a writer who cannot change the contract until
#: the rounds ran out (#48).
#:
#: The bar is deliberately high, so a halt reaches a human with both
#: sides of the conflict on the page. The marker is kept mid-sentence:
#: the parser matches a line that STARTS with it, and a reviewer quoting
#: this paragraph back must not halt the run.
_REVIEW_DISPUTE = (
    '\n\nWHEN THE CONTRACT ITSELF IS WRONG. Block when the code is '
    'wrong. Sometimes the fault is instead in what the writer was '
    'handed: a frozen test that requires what another test, the brief '
    'or a stated invariant forbids, or a success criterion no '
    'implementation can meet. Re-driving the writer cannot fix that, '
    'because the writer may not change the contract. Raise a dispute '
    'instead, which stops the run for a human: a line of its own that '
    'begins with the marker `DISPUTED:` followed by a one-line claim, '
    'and still end with `VERDICT: BLOCKING`, because only a blocking '
    "reviewer's dispute halts the run. The bar is high. Below that "
    'line, name BOTH conflicting artifacts with file:line, quote the '
    'requirement from each, say which one is wrong, and say what a '
    'writer would have to do to satisfy both, or that nothing could. '
    'If you cannot do all four, it is not a dispute: block on the code. '
    'Never ask for the tests stage to be re-run to settle it. The '
    'easiest way for that stage to resolve a conflict is to weaken a '
    'test, and the writers have already built against the contract it '
    'would change.'
)
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
