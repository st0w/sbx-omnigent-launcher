"""Verdicts, selections, finish markers and disputes in replies."""

from __future__ import annotations

import re

from sbx_omnigent.runner.parsing.text import _has_marker_line
from sbx_omnigent.runner.prompts.checks import _CHECK_COMPLETE
from sbx_omnigent.runner.prompts.writer import _WORK_COMPLETE

#: Terminal VERDICT / SELECT markers agents end their reply with. The
#: token after ``VERDICT:`` is captured broadly and normalized below:
#: models routinely paraphrase (``PASS`` / ``LGTM`` / ``FAIL``) instead
#: of the exact ``APPROVED`` / ``BLOCKING`` the templates request, and
#: an over-strict match silently reads an approval as a block.
_VERDICT_RE = re.compile(r'VERDICT:\s*([A-Za-z]+)', re.IGNORECASE)
_SELECT_RE = re.compile(r'SELECT:\s*([A-Za-z0-9._-]+)')

#: Verdict-token synonyms, normalized to the two canonical values.
_APPROVE_WORDS = frozenset({
    'APPROVED', 'APPROVE', 'PASS', 'PASSED', 'PASSES',
    'ACCEPT', 'ACCEPTED', 'LGTM', 'OK',
})
_BLOCK_WORDS = frozenset({
    'BLOCKING', 'BLOCK', 'BLOCKED', 'FAIL', 'FAILED', 'FAILS',
    'REJECT', 'REJECTED',
})

#: A reviewer the ENVIRONMENT stopped from building or running the
#: tests, with no defect found in the code. Never an approval; it goes
#: back to the reviewers, not the writer, who cannot fix the sandbox.
_UNVERIFIED = 'UNVERIFIED'


def parse_verdict(text: str | None) -> str | None:
    """
    Return the LAST recognized ``VERDICT:`` value, normalized, or None.

    Each token after a ``VERDICT:`` marker is mapped to the canonical
    ``'APPROVED'`` / ``'BLOCKING'`` via :data:`_APPROVE_WORDS` /
    :data:`_BLOCK_WORDS`, so a paraphrased ``VERDICT: PASS`` reads as an
    approval. ``UNVERIFIED`` stays itself (see :data:`_UNVERIFIED`).
    The last *recognized* token wins; an unknown token (e.g. a stray
    ``verdict:`` mention) is ignored rather than clobbering a real
    verdict. No recognized verdict yields ``None`` — the caller
    treats that as blocking (safe).

    :param text: An agent reply.
    :returns: ``'APPROVED'`` / ``'BLOCKING'`` / ``'UNVERIFIED'``, or
        ``None`` when absent or unrecognized.
    """
    result: str | None = None
    for token in _VERDICT_RE.findall(text or ''):
        upper = token.upper()
        if upper in _APPROVE_WORDS:
            result = 'APPROVED'
        elif upper in _BLOCK_WORDS:
            result = 'BLOCKING'
        elif upper == _UNVERIFIED:
            result = _UNVERIFIED
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
    first candidate won by default. ``SELECT: `impl-a` `` and
    ``SELECT: **impl-a**`` failed the same way.

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


def check_finished(text: str | None) -> bool:
    """
    Whether a check's reply says it finished: a ``CHECK COMPLETE`` line.

    :param text: A check's reply.
    :returns: ``True`` when some line is exactly the marker.
    """
    return _has_marker_line(text, _CHECK_COMPLETE)


def work_finished(text: str | None) -> bool:
    """
    Whether a writer's reply says it finished: a ``WORK COMPLETE`` line.

    :param text: A writer's reply.
    :returns: ``True`` when some line is exactly the marker.
    """
    return _has_marker_line(text, _WORK_COMPLETE)


def _check_done(text: str) -> bool:
    """A check's reply needs nothing more: it finished or disputed."""
    return check_finished(text) or bool(parse_disputes(text))


def _writer_done(text: str) -> bool:
    """A writer's reply needs nothing more: it finished or disputed."""
    return work_finished(text) or bool(parse_disputes(text))


def _review_done(text: str) -> bool:
    """A reviewer's reply needs nothing more: it states a verdict."""
    return parse_verdict(text) is not None


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


#: A line that DECLARES a dispute rather than mentioning one. The
#: bare word is in almost every healthy report as the instruction
#: echoed back — 34 echoes across the two shipped campaigns — so a
#: near miss has to be recognised by SHAPE, not by the word.
#:
#: Two shapes, both of which read as "this section is a dispute":
#: a markdown heading carrying the marker (what the live loss looked
#: like), and a line that is the marker and nothing else.
_DISPUTE_HEADING_RE = re.compile(r'^[ \t]*#{1,6}[ \t]')

#: Bullets, numbering, emphasis and trailing punctuation, stripped so
#: ``**DISPUTED**`` and ``- DISPUTED:`` reduce to the bare word.
_DISPUTE_DECOR_RE = re.compile(
    r'^[\s\-*\u2022\d.)#*_`\u2014\u2013:]+|[\s*_`.:\u2014\u2013-]+$'
)


def dispute_near_misses(text: str | None) -> tuple[str, ...]:
    """
    Lines that declare a dispute but do not parse as one.

    A writer that meets an impossible contract and says so in the wrong
    shape is lost in silence: the halt never fires, and the run spends
    its whole budget relaying a finding the writer has correctly
    refused. That happened live — the marker sat at the END of a
    markdown heading, which :data:`_DISPUTE_RE` does not match.

    Deliberately narrow. Loosening the PARSER is not an option: the
    bare word appears throughout healthy reports as the instruction
    echoed back, and halting on it would have killed two shipped
    builds. So this recognises only the shapes that read as a
    declaration, and its caller WARNS rather than halting — the cost
    of a false positive is one line of output.

    Returns nothing when a real dispute parsed: the halt has it, and
    there is nothing to report.

    :param text: A stage's reply.
    :returns: The offending lines, stripped, in order.
    """
    if not text or parse_disputes(text):
        return ()
    found: list[str] = []
    for line in text.splitlines():
        if 'DISPUTED' not in line:
            continue
        bare = _DISPUTE_DECOR_RE.sub('', line).strip()
        if _DISPUTE_HEADING_RE.match(line) or bare.upper() == 'DISPUTED':
            found.append(line.strip())
    return tuple(found)
