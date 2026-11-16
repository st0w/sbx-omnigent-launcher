"""A pull request's title and description."""

from __future__ import annotations

from collections.abc import Sequence

from sbx_omnigent import verify
from sbx_omnigent.runner.records.judging import (
    JudgePick,
    _withdrawn,
    retained_bundle_ref,
)
from sbx_omnigent.runner.records.reviews import ReviewRecord

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
        if pick.uncontested:
            parts.append(
                f'- `{pick.stage}`: `{pick.selected}` was selected '
                f'uncontested, with no judge: every other candidate '
                f'was withdrawn — {_withdrawn(pick)}.'
            )
        elif pick.honored:
            withdrawn = (
                f' Withdrawn before judging: {_withdrawn(pick)}.'
                if pick.forfeited
                else ''
            )
            parts.append(
                f'- `{pick.stage}`: the judge chose '
                f'`{pick.selected}`{beat}.{withdrawn}'
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
            '(complete, reviewed, and the only copy), under the '
            "runner's `--canonical-root` on the machine that ran it:",
            '',
        ]
        parts += [
            f'- `{node}` — `{retained_bundle_ref(path)}`'
            for node, path in kept
        ]
    if pick_doc:
        parts += ['', f'The judge reasoning in full: `{pick_doc}`']
    return parts


def environment_by_stage(
    records: Sequence[ReviewRecord],
) -> list[tuple[str, list[tuple[str, list[str]]]]]:
    """
    Each stage's environment limits once, with who raised them.

    Deduplicated on text within a stage, like referrals: two reviewers
    hitting the same refused host, and one hitting it every round, are
    the normal case, and the reader needs it once (#41).

    :param records: Votes, in the order cast.
    :returns: ``(stage, [(text, reviewers)])`` in first-seen order;
        stages that raised none are left out.
    """
    grouped: dict[str, dict[str, list[str]]] = {}
    for rec in records:
        for text in rec.environment:
            who = grouped.setdefault(rec.stage, {}).setdefault(text, [])
            if rec.reviewer not in who:
                who.append(rec.reviewer)
    return [(stage, list(items.items())) for stage, items in grouped.items()]


def _pr_environment_lines(reviews: list[ReviewRecord] | None) -> list[str]:
    """
    The pull request's "Checks the sandbox could not run" section.

    :param reviews: The votes cast on this branch.
    :returns: Lines to append, empty when no reviewer raised one.
    """
    grouped = environment_by_stage(reviews or [])
    if not grouped:
        return []
    lines = [
        '',
        '## Checks the sandbox could not run',
        '',
        'Reviewers could not run these supplementary checks because of '
        'the sandbox, not the code, so none of them was run. Each is '
        'listed once per review stage and is not filed as a finding. A '
        'check that needs a host the sandbox refuses can run once that '
        'host is in `sandbox.sbx.egress_allow` in the server config; '
        "declare it under the pipeline's `egress:` too, so the next run "
        'checks it before any turn.',
    ]
    for stage, items in grouped:
        lines += ['', f'`{stage}`:', '']
        lines += [
            f'- {text} (raised by {", ".join(f"`{r}`" for r in who)})'
            for text, who in items
        ]
    return lines


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


def _pr_head_lines(summary: str, series_note: str | None) -> list[str]:
    """
    The opening lines of a PR description: summary, then series note.

    :param summary: One-line description of what shipped.
    :param series_note: A chained chunk's note, or ``None``.
    :returns: Lines, each block followed by a blank line.
    """
    lines = [summary.strip(), '']
    if series_note:
        lines += [series_note, '']
    return lines


def render_pr_body(
    *,
    summary: str,
    series_note: str | None = None,
    task: str | None = None,
    plan_doc: str | None = None,
    session_doc: str | None = None,
    review_doc: str | None = None,
    reviews: list[ReviewRecord] | None = None,
    picks: list[JudgePick] | None = None,
    pick_doc: str | None = None,
    findings: list[tuple[str, str]] | None = None,
    outcome: verify.VerifyOutcome | None = None,
    security_doc: str | None = None,
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
    :param series_note: For a chained chunk, how its PR relates to the
        previous one; shown straight after the summary.
    :param task: The original ask, when it adds to the summary.
    :param plan_doc: Repo path of the approved design, if committed.
    :param session_doc: Repo path of the planning session record.
    :param review_doc: Repo path of the committed reviewer reports.
    :param reviews: The votes cast on this branch, for the roster — a
        reader should see that round 1 blocked without opening a file.
    :param outcome: The verification result whose steps are the
        evidence; ``None`` when the pipeline has no gate.
    :param security_doc: Repo path of the security scan summary, when
        a scan ran.
    :returns: The markdown body.
    """
    parts = _pr_head_lines(summary, series_note)
    parts += [
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
    parts += _pr_security_lines(security_doc)
    parts += _pr_finding_lines(findings)
    parts += _pr_environment_lines(reviews)
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


def _pr_security_lines(security_doc: str | None) -> list[str]:
    """The pull request's pointer to the security scan summary."""
    if not security_doc:
        return []
    return [
        '',
        '## Security scan',
        '',
        f'Every claude-security scan round, each finding, and what became '
        f'of it (fixed in the run, filed, or withheld by triage, with the '
        f'reason): `{security_doc}`',
    ]


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
