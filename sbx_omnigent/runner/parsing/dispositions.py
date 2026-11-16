"""The verifier's conclusion on each finding it checked."""

from __future__ import annotations

import re
from dataclasses import dataclass

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


#: The verifier's conclusions about BLOCKING claims, under a header of
#: their own so they cannot be confused with the numbered non-blocking
#: list above. `BLOCKING-` prefixes the word, so
#: :data:`_DISPOSITIONS_HEADER_RE` (anchored at line start) never
#: matches this line and each block stops cleanly at the other's header.
_BLOCKING_DISPOSITIONS_HEADER_RE = re.compile(
    r'^[ \t]*#{0,6}[ \t]*\*{0,2}[ \t]*'
    r'BLOCKING-DISPOSITIONS[ \t]*:?[ \t]*\*{0,2}[ \t]*$',
    re.MULTILINE | re.IGNORECASE,
)

#: ``- B1.2: absent — the winner coerces the timestamp``. Two numbers,
#: because a blocking report is prose: the verifier numbers the REPORT
#: it was given and each CLAIM it found inside it, so an id stays stable
#: without asking a reviewer to adopt a marker it has never used. The
#: ``B`` is optional — the header already says which block this is.
_BLOCKING_ITEM_RE = re.compile(
    r'^[ \t]*(?:[-*•]|\d+[.)])?[ \t]*'
    r'B?(?P<report>\d+)[ \t]*\.[ \t]*(?P<claim>\d+)[ \t]*[:.)][ \t]*'
    r'(?P<phrase>.+)$'
)


def parse_blocking_dispositions(
    text: str | None,
) -> dict[tuple[int, int], Disposition]:
    """
    Parse a verifier's ``BLOCKING-DISPOSITIONS:`` block.

    Same protocol as :func:`parse_dispositions` — last header wins, the
    first non-item line ends the block, an unreadable verdict is skipped
    rather than guessed at — but keyed by ``(report, claim)`` because
    the verifier enumerates the claims itself.

    Fails CLOSED, unlike its non-blocking sibling: a claim with no
    conclusion is not filed. A blocking finding was relayed to the
    writer and usually fixed, so filing every unconcluded one would
    bury the tracker in issues that are already closed. The caller
    surfaces the gap instead (see
    :func:`~sbx_omnigent.runner.render.issues.render_blocking_summary`).

    :param text: The verifier's reply.
    :returns: ``(report, claim)`` to conclusion; missing keys mean the
        verifier said nothing about that claim.
    """
    out: dict[tuple[int, int], Disposition] = {}
    if not text:
        return out
    matches = list(_BLOCKING_DISPOSITIONS_HEADER_RE.finditer(text))
    if not matches:
        return out
    for line in text[matches[-1].end():].splitlines():
        if not line.strip():
            continue
        item = _BLOCKING_ITEM_RE.match(line)
        if item is None:
            break
        found = _verdict_in(item.group('phrase'))
        if found is None:
            continue
        verdict, reason = found
        key = (int(item.group('report')), int(item.group('claim')))
        out[key] = Disposition(verdict=verdict, reason=reason)
    return out
