"""Turn text for contract and plan checks, and their end markers."""

from __future__ import annotations

#: The line a check's reply must end with to count as a pass.
_CHECK_COMPLETE = 'CHECK COMPLETE'

#: Told to every check, contract and plan alike. Live, a Claude checker
#: ended its turn to wait for a background `cargo test`, saying it would
#: finish the report afterwards; the runner read that as the report,
#: found no dispute, and recorded a pass. A pass must now be said.
_CHECK_FINISH = (
    '\n\nWHEN YOU ARE DONE. Run long commands in the background if you '
    'like: if you end your turn while one is still running, the pipeline '
    'waits for it to finish and for the reply you give when you are '
    'woken. Your report is final only when it ends with a line reading '
    f'exactly `{_CHECK_COMPLETE}`. A reply without that line, with '
    'nothing left running, is treated as unfinished, never as a pass: '
    'you are asked once to finish it, and after that the check counts as '
    'not passed.'
)

#: Sent once to a check whose reply neither disputed nor said it was
#: finished.
_CHECK_FINISH_ASK = (
    f'Your reply did not end with a line reading exactly '
    f'`{_CHECK_COMPLETE}`, so the check is not finished and cannot count '
    f'as a pass. Finish the check, then reply with your complete report, '
    f'every dispute and finding included, ending with a line reading '
    f'exactly `{_CHECK_COMPLETE}`.'
)

#: What a contract check stops the run for, and what it only records
#: (#50). The marker is kept mid-sentence for the same reason as in
#: :data:`_REVIEW_DISPUTE`: a checker quoting this back must not halt.
_CHECK_RULES = (
    '\n\nWHAT STOPS THE RUN. The contract cannot be met when two tests '
    'cannot both pass, a test contradicts the brief or the acceptance '
    'contract, or the brief contradicts itself. For each, write a line '
    'of its own that begins with the marker `DISPUTED:` followed by a '
    'one-line claim, which stops the run for a human before any writer '
    'starts. Below it, name BOTH conflicting artifacts with file:line, '
    'quote the requirement from each, say which one is wrong, and say '
    'what an implementation would have to do to satisfy both, or that '
    'nothing could. If you cannot do all four, it is not a dispute.'
    '\n\nWHAT IS ONLY RECORDED. A test the tests stage added or changed '
    'that would already pass on this unmodified tree (the existing '
    'tests are meant to, so do not list those), a test asserting on '
    'source text or file layout instead of behaviour, and a requirement '
    'in the brief that no test covers make the suite weaker, but a '
    'writer can still meet it. List each under a line reading exactly '
    '`FINDINGS:`, one `- ` item per line, naming the test or '
    'requirement. The run continues '
    'and a human reads them. If you find nothing of either kind, say '
    'that plainly and how you checked.'
    + _CHECK_FINISH
)

#: What a plan check is asked to do (#49). Kept in the turn, not only
#: in the template, so a checker given its own prompt still knows what
#: stops the plan. The marker is mid-sentence, as in
#: :data:`_CHECK_RULES`.
_PLAN_CHECK_RULES = (
    '\n\nWHAT TO CHECK. For every claim the plan makes about existing '
    'code (a signature, a unit, which values are comparable, a limit, '
    'what a file already does), run a command that checks it against '
    'this tree, and report the claim, the command you ran and its '
    'output. Also read the sections against each other: two that '
    'contradict each other cannot both be built. Whether the design is '
    'right is not your call; the human has made it.'
    '\n\nWHAT STOPS THE PLAN. A claim the tree shows is false, or two '
    'sections that contradict each other. For each, write a line of its '
    'own that begins with the marker `DISPUTED:` followed by a one-line '
    'claim. Below it, quote the plan (naming the section), give the '
    'command and its output or both sections quoted, and say what is '
    'actually true. The human sees this in the planning session before '
    'the plan is final. A dispute you cannot back with output is a '
    'finding, not a dispute.'
    '\n\nWHAT IS RECORDED. List every claim that held under a line '
    'reading exactly `CLAIMS CHECKED:`, each with the command that '
    'showed it, and anything you could not check under a line reading '
    'exactly `FINDINGS:`, one `- ` item per line.'
    + _CHECK_FINISH
)

#: What a contract check's record says when it let the writers go on:
#: it read the contract, so it neither approved nor blocked a branch.
_CHECK_VERDICT = 'NO DISPUTE'

#: What a plan check's record says when it found a false claim or a
#: contradiction. Only a plan check is recorded on a dispute: the run
#: goes on, because the human settles it in the planning session (#49).
_CHECK_DISPUTED = 'DISPUTE RAISED'

#: The reply that accepts a plan over its check's disputes, without
#: another check (#49). A plain ``APPROVED`` re-consolidates and
#: re-checks.
_APPROVED_AS_IS = 'APPROVED AS IS'

#: The phrases that release the plan gate when the plan has a check.
_PLAN_DECISION_PHRASES = ('APPROVED', _APPROVED_AS_IS)
