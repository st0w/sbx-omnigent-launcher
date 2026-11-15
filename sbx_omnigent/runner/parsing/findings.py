"""Reviewer findings, sorted by the section they were reported under."""

from __future__ import annotations

import re
from dataclasses import dataclass

from sbx_omnigent.runner.parsing.text import _lift_marked_items
from sbx_omnigent.runner.parsing.verdicts import _VERDICT_RE

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

#: Checks the SANDBOX stopped a reviewer running (#41): an audit whose
#: advisory database a blocked host serves, a tool whose download is
#: refused. Not a finding against the code, so it is kept out of the
#: union that is filed. Two words, not ``ENVIRONMENT``: reviewers
#: describe what they DID set up under an "Environment" heading, and
#: that must never read as what they could not run.
_ENVIRONMENT_HEADER_RE = re.compile(
    r'^[ \t]*#{0,6}[ \t]*\*{0,2}[ \t]*'
    r'ENVIRONMENT[- _]?LIMITS[ \t]*:?[ \t]*\*{0,2}[ \t]*$',
    re.MULTILINE | re.IGNORECASE,
)


def report_names_nothing(text: str | None) -> bool:
    """
    Whether a BLOCKING reply carries no finding anyone could act on.

    A verdict line and nothing else is the one review a writer cannot
    answer and a verifier cannot check. It used to be recorded exactly
    as a good block: a verdict, and empty lists beside it.

    :param text: The reviewer's reply.
    :returns: ``True`` when nothing but the verdict survives.
    """
    body = _VERDICT_RE.sub('', text or '')
    # Markdown scaffolding is not content: a lone "### Blocking
    # findings" heading over an empty section names nothing either.
    stripped = re.sub(r'[#*_`\-\u2014\u2013:\s]+', ' ', body)
    return len(stripped.strip()) < 40


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
    :param environment: Supplementary checks the sandbox stopped the
        reviewer running. Not part of :attr:`all`: they say nothing
        about the code, so they are surfaced once per stage rather than
        filed (#41).
    """

    defects: tuple[str, ...] = ()
    later_increment: tuple[str, ...] = ()
    premises: tuple[str, ...] = ()
    environment: tuple[str, ...] = ()

    @property
    def all(self) -> tuple[str, ...]:
        """Every finding, defects first — the pre-split list."""
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
            environment=_lift_marked_items(text, _ENVIRONMENT_HEADER_RE),
        )


def parse_findings(text: str | None) -> tuple[str, ...]:
    """
    Parse a reviewer's ``FINDINGS:`` block.

    These are the NON-BLOCKING observations — the lossy set. A blocking
    finding is acted on inside the round; one raised alongside an
    APPROVED verdict is archived to the run directory and never read
    again, so a real defect noticed by a careful reviewer simply
    evaporates. Lifting them out under a marker is how they reach the
    ledger.

    Same protocol as :func:`parse_decisions`, deliberately: the LAST
    header wins, blank lines inside the list are tolerated, and
    the first non-blank line that is not an item ends the block.

    :param text: The reviewer's full report.
    :returns: The findings in order, or ``()`` when none are declared.
        Empty is the NORMAL case — a reviewer with nothing to raise is
        not a failure, and the caller decides what to do about it.
    """
    return _lift_marked_items(text, _FINDINGS_HEADER_RE)
