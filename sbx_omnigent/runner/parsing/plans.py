"""A plan's shape, and the subtasks it proposes."""

from __future__ import annotations

import re

from sbx_omnigent import pipeline
from sbx_omnigent.runner.errors import PipelineRunError

#: The planner's ordered chunk list (see :func:`parse_subtasks`): a line
#: reading exactly ``SUBTASKS:`` followed by ``- [<id>] <goal>`` items
#: (a ``1. [<id>] …`` numbered variant is accepted).
_SUBTASKS_HEADER_RE = re.compile(r'^[ \t]*SUBTASKS:[ \t]*$', re.MULTILINE)

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
#: ``templates/planner.md`` demands?
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
