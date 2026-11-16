"""What a writer's branch may and must contain, checked from its diff.

Every gate downstream of a writer is an agent's word. These checks are
the orchestrator's own: pure git inspection of the branch against the
tree it was cut from, in the trusted plane. A writer that strays is
re-driven with the evidence, then refused if it strays again.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Iterable

import click

from sbx_omnigent import pipeline
from sbx_omnigent.runner.context import RunContext
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.lineage import review_target, seed_ref
from sbx_omnigent.runner.prompts.policy import (
    coverage_instruction,
    guarded_block,
    noop_findings,
    tests_only_instruction,
)
from sbx_omnigent.runner.redrive import RedriveWriter

#: Ways to tell a coverage tool not to count a line. Each ecosystem
#: spells the same escape hatch differently, and the launcher drives
#: Python, Rust and JS projects alike.
#:
#: These exist for genuinely unreachable code, and a writer short of a
#: coverage floor can reach for them instead of writing the test. The
#: gate cannot tell earned coverage from suppressed coverage — the
#: number moves either way — so the suppression itself is what gets
#: checked. Live on `ingestion-m2-5`: a candidate sat at 90.87% against
#: a 95% floor with the suite frozen, added 28 markers, and reported
#: 95.03%; the excluded lines included rule-version checks, source
#: discovery, and the first-poll branch of every source. Its sibling
#: reached 95.07% with none, which is the proof it was avoidable.
_COVERAGE_SUPPRESSORS = (
    'pragma: no cover',
    'pragma: no branch',
    'istanbul ignore',
    'tarpaulin_include',
    'codecov:ignore',
    'LCOV_EXCL',
    'coverage: ignore',
)


#: What a ``tests_only`` stage may change when the pipeline declares
#: no ``test_paths`` of its own. Deliberately only TEST code: dependency
#: manifests are handled by :data:`_GENERATED_GLOBS` and the ordinary
#: guarded-file review, because a test needing a new dev-dependency must
#: edit ``Cargo.toml`` and every greenfield module in this project's
#: history did exactly that.
_TEST_PATH_GLOBS: tuple[str, ...] = (
    'tests/**',
    '**/tests/**',
    '*_test.*',
    'test_*.*',
    '*.test.*',
    '*_spec.*',
    'conftest.py',
)

#: Dependency manifests a ``tests_only`` stage may also change. A test
#: needing a new dev-dependency MUST declare it — every greenfield
#: module here did, and replaying the gate over them showed
#: two (m1, m5) that would have been blocked for doing nothing else at
#: all. Not folded into :data:`_TEST_PATH_GLOBS` (a manifest is not test
#: code) nor :data:`_GENERATED_GLOBS` (nor build output).
#:
#: The compensating control already exists: manifests are in this
#: project's ``guarded:`` list, so any edit is named to the reviewers as
#: a required review item. Allowing them here does not make them
#: invisible, it just stops them halting the stage.
_DEPENDENCY_MANIFEST_GLOBS: tuple[str, ...] = (
    'Cargo.toml',
    'pyproject.toml',
    'setup.cfg',
    'package.json',
    'go.mod',
    'Gemfile',
    'build.gradle',
    'build.gradle.kts',
    'pom.xml',
)

#: Glob patterns for files that are BUILD OUTPUT, not implementation.
#: A writer whose whole diff is these did not do the work — observed
#: live: a coder reported success having changed only ``Cargo.lock``
#: (its crate roots still held the test writer's "intentionally empty"
#: placeholders), and TWO reviewers approved the branch anyway. Config
#: ``generated:`` replaces this set.
_GENERATED_GLOBS: tuple[str, ...] = (
    '*.lock',
    '*.sum',
    'package-lock.json',
    'npm-shrinkwrap.json',
    'pnpm-lock.yaml',
)


def _matches(path: str, globs: Iterable[str]) -> bool:
    """
    Whether *path*, or its file name alone, matches any of *globs*.

    :param path: A repo-relative path.
    :param globs: Glob patterns.
    :returns: ``True`` on the first match.
    """
    name = path.rsplit('/', 1)[-1]
    return any(
        fnmatch.fnmatch(path, g) or fnmatch.fnmatch(name, g) for g in globs
    )


def is_generated(config: pipeline.PipelineConfig, path: str) -> bool:
    """
    Whether *path* is build output rather than implementation.

    :param config: The pipeline; its ``generated:`` replaces the
        default set.
    :param path: A repo-relative path.
    :returns: ``True`` for build output.
    """
    return _matches(path, config.generated or _GENERATED_GLOBS)


def is_test_path(config: pipeline.PipelineConfig, path: str) -> bool:
    """
    Whether *path* is something a tests-only stage may write.

    :param config: The pipeline; its ``test_paths:`` replaces the
        default test globs. Dependency manifests are allowed either way.
    :param path: A repo-relative path.
    :returns: ``True`` for test code or a dependency manifest.
    """
    tests = config.test_paths or _TEST_PATH_GLOBS
    return _matches(path, tests + _DEPENDENCY_MANIFEST_GLOBS)


def is_guarded(config: pipeline.PipelineConfig, path: str) -> bool:
    """
    Whether *path* is one of the project's declared checks.

    :param config: The pipeline, whose ``guarded:`` lists them.
    :param path: A repo-relative path.
    :returns: ``True`` for a declared check.
    """
    return _matches(path, config.guarded)


class WriterPolicy:
    """
    Hold each writer's branch to what it may and must contain.

    :param ctx: The run.
    :param redrive_writer: Re-drives a writer with the evidence.
    """

    def __init__(
        self, ctx: RunContext, *, redrive_writer: RedriveWriter
    ) -> None:
        self.ctx = ctx
        self._redrive_writer = redrive_writer

    def enforce(self, stage: pipeline.PipelineStage) -> None:
        """
        Run every check on a writer that just committed.

        :param stage: The writer stage.
        :raises PipelineRunError: If a check still fails after the
            writer is re-driven.
        """
        self._enforce_no_coverage_suppression(stage)
        if stage.tests_only:
            self._enforce_tests_only(stage)
        self._require_implementation(stage)

    def guarded_block(self, stage: pipeline.PipelineStage) -> str:
        """
        Name the declared checks a review's branch changed.

        See :func:`~sbx_omnigent.runner.prompts.policy.guarded_block`.

        :param stage: The review stage about to run.
        :returns: The block, or ``''`` when nothing guarded changed.
        """
        hits = self._guarded_changes(stage)
        return guarded_block(hits) if hits else ''

    def _non_test_changes(self, stage: pipeline.PipelineStage) -> list[str]:
        """
        Production files a tests-only stage changed.

        Diffed against the ref the writer was cut from, so this is what
        THIS stage did rather than what its base already carried.
        Generated files (lockfiles) are excused — a test that adds a
        dev-dependency legitimately moves them.

        :param stage: The tests-only writer stage that just committed.
        :returns: Sorted repo-relative paths, or ``[]``.
        """
        try:
            changed = self.ctx.worktrees.node_diff_files(
                self.ctx.run_id, stage.id, against=seed_ref(self.ctx, stage)
            )
        except click.ClickException:
            # A git hiccup must not fail a stage that may be perfectly
            # fine; the gate is a check, not a source of flakiness.
            return []
        config = self.ctx.config
        return sorted(
            f for f in changed
            if not is_test_path(config, f) and not is_generated(config, f)
        )

    def _suppressed_coverage(
        self, stage: pipeline.PipelineStage
    ) -> list[str]:
        """
        Coverage-exclusion markers this writer's branch ADDED.

        :param stage: The writer stage that just committed.
        :returns: The offending added lines, stripped, in order.
        """
        try:
            added = self.ctx.worktrees.node_added_lines(
                self.ctx.run_id, stage.id, against=seed_ref(self.ctx, stage)
            )
        except click.ClickException:
            # A git hiccup must not fail a stage that may be fine; this
            # is a check, not a source of flakiness.
            return []
        return [
            line.strip()
            for line in added
            if any(m in line for m in _COVERAGE_SUPPRESSORS)
        ]

    def _enforce_no_coverage_suppression(
        self, stage: pipeline.PipelineStage
    ) -> None:
        """
        Refuse coverage bought with exclusion markers rather than tests.

        A coverage floor asks "is this code exercised". A writer that
        cannot reach the floor — because the suite is frozen and it may
        not add tests — can instead mark the uncovered lines uncounted
        and watch the number rise. The gate goes green over code nobody
        ran, which is worse than a red gate: it launders untested code
        as tested.

        Re-driven once rather than halted, on the same reasoning as
        :meth:`_enforce_tests_only` — the writer can undo this itself,
        and an unattended run should not stop for something it can fix.
        A writer that does it twice is not going to stop.

        Genuinely untestable code is a design signal, not a licence for
        this: an SDK boundary belongs behind an injected seam, which is
        how a sibling candidate hit the same floor with zero markers.

        :param stage: The writer stage that just committed.
        :raises PipelineRunError: If the markers survive a re-drive.
        """
        found = self._suppressed_coverage(stage)
        if not found:
            return
        listed = '\n'.join(f'  {line}' for line in found[:20])
        click.echo(
            f'[coverage] {stage.id}: added {len(found)} coverage-exclusion '
            f'marker(s); re-driving to earn the coverage instead.'
        )
        self._redrive_writer(
            stage.id,
            listed,
            message=f'{stage.id}: remove coverage suppression',
            instruction=coverage_instruction(listed),
        )
        found = self._suppressed_coverage(stage)
        if not found:
            click.echo(
                f'[coverage] {stage.id}: suppression removed.'
            )
            return
        raise PipelineRunError(
            f'{stage.id} added {len(found)} coverage-exclusion marker(s) '
            f'and kept them after being asked to remove them:\n\n'
            f'{listed}\n\nA coverage floor reached by marking lines '
            f'uncounted is not coverage — it ships untested code through '
            f'a green gate. Either test those paths, or restructure so '
            f'they are testable (an SDK call belongs behind an injected '
            f'seam). If some line truly cannot be exercised, that is a '
            f'contract question for a human, not a marker for a writer '
            f'to add on its own.'
        )

    def _enforce_tests_only(self, stage: pipeline.PipelineStage) -> None:
        """
        Hold a tests-only stage to test code, re-driving if it strays.

        The pipeline's central claim is that two models implement the
        same frozen contract independently and a judge picks between
        them. A tests stage that ships the implementation hands BOTH
        writers one design — the judge then compares two edits of the
        same code, and the second model's independent take never exists.
        The comparison still runs; it stops measuring what it claims to.

        Observed live on `gcp-scope-topology-1`: `identities-tests`
        committed a 114-line `ServiceAccountCache` into
        `providers/gcp/src/service_accounts.rs`. Its instruction already
        said "you must NOT implement the feature yourself or modify any
        production code", and its own test file never referenced the new
        type — the suite compiled and failed against unmodified source,
        so nothing forced it. A prompt is a request; this is the check.

        RE-DRIVEN rather than halted on the first violation, because an
        unattended run should not stop for something the writer can undo
        itself. Nothing is auto-reverted: the runner rewriting an
        agent's commit would leave a suite referencing code the runner
        had just deleted, and the agent is the one that knows which of
        its own changes to keep.

        :param stage: The tests-only stage that just committed.
        :raises PipelineRunError: If it strays a second time.
        """
        stray = self._non_test_changes(stage)
        if not stray:
            return
        listed = '\n'.join(f'  - {path}' for path in stray)
        click.echo(
            f'[tests-only] {stage.id}: changed {len(stray)} production '
            f'file(s); asking it to remove them and keep the tests.'
        )
        self._redrive_writer(
            stage.id,
            listed,
            message=f'{stage.id}: restrict to test code',
            instruction=tests_only_instruction(listed),
        )
        stray = self._non_test_changes(stage)
        if not stray:
            click.echo(f'[tests-only] {stage.id}: now test code only.')
            return
        listed = '\n'.join(f'  - {path}' for path in stray)
        raise PipelineRunError(
            f'{stage.id} is a tests-only stage but still changes '
            f'production code after being asked to stop:\n{listed}\n'
            f'Nothing was reverted — the branch is intact for you to '
            f'inspect. Either the tests genuinely need this surface to '
            f'exist (in which case the plan should say so for this '
            f'increment), or the stage is implementing the feature.'
        )

    def _guarded_changes(
        self, stage: pipeline.PipelineStage
    ) -> list[str]:
        """
        Declared checks the branch under review changed.

        Diffed against the ref the writer was cut from, so this is what
        THIS module did, not what the base branch already carried.

        :param stage: The review stage about to run.
        :returns: Sorted repo-relative paths, or ``[]``.
        """
        if not self.ctx.config.guarded:
            return []
        target = review_target(stage, self.ctx.nodes)
        target_stage = self.ctx.stage_by_id.get(target)
        if target_stage is None:
            return []
        try:
            changed = self.ctx.worktrees.node_diff_files(
                self.ctx.run_id, target,
                against=seed_ref(self.ctx, target_stage),
            )
        except click.ClickException:
            # Never fail a review on a git hiccup; the reviewer still
            # has the tree and the rest of its instruction.
            return []
        return sorted(f for f in changed if is_guarded(self.ctx.config, f))

    def _require_implementation(
        self, stage: pipeline.PipelineStage
    ) -> None:
        """
        Refuse to pass on a writer that did not actually implement.

        Every gate in this pipeline downstream of a writer is an agent's
        WORD — a reviewer's ``VERDICT:``, a coder's "tests pass". None
        of them executes anything. So a writer that does nothing and
        reports success sails through: observed live, an implementer
        left both crate roots holding the test author's "intentionally
        empty" placeholder, changed only ``Cargo.lock``, and collected
        ``VERDICT: APPROVED`` from two independent reviewers. Only the
        judge caught it, by diffing.

        This is the one check the ORCHESTRATOR can make honestly,
        because it needs no execution: compare the branch to the tree it
        was cut from and require at least one changed file that is not
        build output. Pure git inspection in the trusted plane — running
        the suite instead would mean executing agent-authored code on
        the host, with the publish token in the environment.

        A no-op is re-driven with the specific evidence, up to the
        review round cap, then fails the stage rather than handing an
        empty branch to reviewers who will rubber-stamp it.

        :param stage: The writer stage just committed.
        :raises PipelineRunError: If it never implements anything.
        """
        if not stage.write:
            return
        against = seed_ref(self.ctx, stage)
        for _round in range(self.ctx.max_rounds):
            try:
                changed = self.ctx.worktrees.node_diff_files(
                    self.ctx.run_id, stage.id, against=against
                )
            except click.ClickException:
                return  # cannot tell; never fail a run on a git hiccup
            if [f for f in changed if not is_generated(self.ctx.config, f)]:
                return
            click.echo(
                f'[verify] {stage.id}: no implementation — changed '
                f'{len(changed)} file(s), all build output. Re-driving.'
            )
            self._redrive_writer(
                stage.id,
                noop_findings(changed),
                message=f'{stage.id}: implement (retry)',
            )
        raise PipelineRunError(
            f'{stage.id} produced no implementation: after '
            f'{self.ctx.max_rounds} attempt(s) its branch still differs '
            f'from {against!r} only in generated files. It reported '
            f'success without writing production code — do not trust a '
            f'downstream review of this branch.'
        )
