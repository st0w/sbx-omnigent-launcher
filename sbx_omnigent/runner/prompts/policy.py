"""Turns that hold a writer to what its branch may and must contain."""

from __future__ import annotations

from collections.abc import Sequence

from sbx_omnigent.runner.prompts.writer import (
    _DISPOSABLE_VM,
    _FROZEN_TESTS_CONTRACT,
    _UNATTENDED,
    _WRITER_BUILD,
)


def coverage_instruction(listed: str) -> str:
    """
    The re-drive turn for a writer that suppressed coverage.

    :param listed: The markers it added, one per line.
    :returns: The instruction.
    """
    return (
        'You added coverage-exclusion markers:\n\n'
        f'{listed}\n\n'
        'Remove every one of them. A coverage floor measures whether '
        'code is exercised; marking a line uncounted moves the number '
        'without testing anything, and ships untested code behind a '
        'green gate. Earn the coverage instead: write the test if the '
        'suite is yours to extend, or restructure so the path is '
        'reachable — an SDK call behind an injected seam is testable '
        'with a plain fake. If you believe a line genuinely cannot be '
        'exercised, do NOT mark it: say so in your reply, label it '
        'DISPUTED, and stop, because that is a contract question a '
        'human settles.'
    )


def tests_only_instruction(listed: str) -> str:
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
        f'\n\n{_FROZEN_TESTS_CONTRACT}'
        f'\n\n{_UNATTENDED}\n\n{_DISPOSABLE_VM}\n\n{_WRITER_BUILD}'
    )


def noop_findings(changed: list[str]) -> str:
    """
    Evidence handed back to a writer that implemented nothing.

    :param changed: The files its branch changed, all build output.
    :returns: The findings for its re-drive.
    """
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


def guarded_block(hits: Sequence[str]) -> str:
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

    :param hits: The declared checks the branch changed.
    :returns: The block.
    """
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
