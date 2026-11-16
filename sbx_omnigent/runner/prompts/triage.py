"""The verifier's turn: the findings and blocking reports to check."""

from __future__ import annotations

from sbx_omnigent.runner.records.reviews import ReviewRecord


def findings_block(
    pending: list[tuple[ReviewRecord, int, str]]
) -> str:
    """
    The numbered non-blocking findings and their answer format.

    :param pending: ``(record, index, text)`` in presentation order.
    :returns: One section of the verifier's turn.
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


def blocking_block(
    reports: list[tuple[ReviewRecord, str]]
) -> str:
    """
    The blocking reports and the per-claim answer format.

    :param reports: ``(record, report)`` in presentation order.
    :returns: One section of the verifier's turn.
    """
    listing = '\n\n'.join(
        f'Report {position} — `{rec.reviewer}` blocked '
        f'`{rec.target or "this module"}` in round {rec.round_no}:\n\n'
        f'{text.strip()}'
        for position, (rec, text) in enumerate(reports, start=1)
    )
    return (
        'Separately: these reviewers BLOCKED a candidate in this '
        'module. Each report was relayed to the writer it blocked, '
        'and that writer usually fixed it — but its COMPETITOR was '
        'never told, and the judge may well have selected the '
        'competitor. So every claim below is an open question '
        'about the code in your mount, whichever branch it was '
        'first raised against.\n\n'
        f'{listing}\n\n'
        'Read each report, and for EVERY distinct claim it makes, '
        'add one line to a `BLOCKING-DISPOSITIONS:` block at the '
        'end of your reply:\n\n'
        '`- B<report>.<claim>: <verdict> — <the claim in one '
        'sentence>, and what you checked`\n\n'
        'Number the claims yourself, in the order the report makes '
        'them: `B1.1`, `B1.2`, then `B2.1`. Use the same five '
        'verdicts as above, against THE CODE IN YOUR MOUNT.\n\n'
        'Only `reproduces` is filed, and the sentence you write '
        'becomes the issue somebody reads — so state the defect '
        'itself, with the file and line, rather than pointing back '
        'at the report. `absent` is the ordinary outcome for a '
        'claim the writer fixed, and saying so costs nothing. A '
        'claim you say nothing about is NOT filed, so a report you '
        'skip is a defect nobody sees again.'
    )
