"""Declarative pipeline config: parse, validate, materialize agents.

A ``pipeline.yaml`` declares a swarm/pipeline as ``agents:`` (who) +
``stages:`` (the DAG) + ``publish:``/``task:`` (what to do). This module
turns that file into typed config and materializes each declared agent
into a namespaced Omnigent bundle dir so the server registers it at
startup (the only way a managed, model-pinnable agent reaches the
server — see ``docs/PIPELINES.md``).

Design invariants this module encodes:

- Each agent's PROMPT comes from a shipped role ``template`` (e.g.
  ``coder``, ``tdd-writer``, ``planner``, ``security-reviewer``,
  ``bug-reviewer``, ``judge``) OR an inline ``prompt`` / ``prompt_file``
  override. Per-agent ``skills:`` (a directory) is copied in verbatim,
  Polly-style.
- Model / effort are NOT baked into the bundle — native harnesses ignore
  a spec-declared model, so the runner pins them per session at create
  (``model_override`` / ``reasoning_effort``). Materialization sets
  the prompt, harness, skills, and os_env.
- Agent names are NAMESPACED by the pipeline so an inline role can never
  clobber a shipped bundle (registration upserts by name).

Stage semantics (writer isolation, branch inheritance, gates) are parsed
here into typed form but EXECUTED by the runner (Stage 3).
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

import yaml
from omnigent.model_override import model_family_mismatch

#: Shipped role prompt templates live here (Path-relative, like the
#: packaged ``agents/`` bundles — works for the editable install).
_TEMPLATES_DIR = Path(__file__).resolve().parent / 'templates'

#: Default harness when neither the agent nor its template names one.
_DEFAULT_HARNESS = 'claude-native'

#: Reviewer roles vote; a planner/judge do not. Only used for defaults.
_DEFAULT_GATE = 'consensus'

#: Characters allowed in a sanitized name segment (dir + agent name).
_NAME_SANITIZE_RE = re.compile(r'[^a-z0-9]+')

#: One ``- [<id>] <title>`` subtask/module line (a ``1. [<id>] …``
#: numbered variant is accepted). Shared by the runner's planner-output
#: parser and the config ``subtask_file`` reader.
_SUBTASK_ITEM_RE = re.compile(
    r'^[ \t]*(?:[-*]|\d+[.)])[ \t]*\[(?P<id>[^\]]+)\]'
    r'[ \t]*(?P<title>.*?)[ \t]*$'
)


class _BlockDumper(yaml.SafeDumper):
    """A SafeDumper that renders multi-line strings as block scalars."""


def _repr_str(
    dumper: yaml.Dumper, data: str
) -> yaml.nodes.ScalarNode:
    """Represent multi-line strings with a ``|`` block scalar."""
    style = '|' if '\n' in data else None
    return dumper.represent_scalar(
        'tag:yaml.org,2002:str', data, style=style
    )


_BlockDumper.add_representer(str, _repr_str)


class PipelineError(Exception):
    """A pipeline config is missing, malformed, or inconsistent."""


class _DuplicateKey(yaml.YAMLError):
    """A mapping in the config defines the same key twice."""

    def __init__(self, key: object, first: yaml.Mark, again: yaml.Mark):
        self.key = key
        self.first = first
        self.again = again
        super().__init__(f'duplicate key {key!r}')


class _StrictLoader(yaml.SafeLoader):
    """
    A ``SafeLoader`` that REFUSES a repeated mapping key.

    PyYAML accepts a duplicate silently and keeps the LAST one, so a
    config can parse clean, validate clean, and still not mean what it
    says. Live on 2026-08-14 a ``verify:`` block ended up with two
    ``command:`` keys — the coverage gate the operator had just added
    and the weaker command it was meant to replace. The gate ran the
    weak one, ``coverage_min: 95`` sat referenced by nothing, and the
    only reason it was caught was rendering the gate program through
    the runner's real code path. A run would otherwise have published
    against a gate everyone believed was enforcing coverage.

    Duplicates are detected on the RAW pairs, before
    :meth:`SafeConstructor.flatten_mapping` resolves any ``<<`` merge —
    so this flags what is literally written twice in the file, and
    never an explicit key legitimately overriding a merged one.
    """

    #: The ``<<`` merge key. It has no constructor of its own (the base
    #: class resolves it in ``flatten_mapping``), so constructing it
    #: here would raise "could not determine a constructor" and break
    #: every anchor/merge config that loads fine today.
    _MERGE_TAG = 'tag:yaml.org,2002:merge'

    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False
    ) -> dict[object, object]:
        seen: dict[object, yaml.Mark] = {}
        for key_node, _value in node.value:
            if key_node.tag == self._MERGE_TAG:
                continue
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in seen
            except TypeError:
                # Unhashable key: let the base class raise its own
                # "found unhashable key" error rather than masking it.
                continue
            if duplicate:
                raise _DuplicateKey(key, seen[key], key_node.start_mark)
            seen[key] = key_node.start_mark
        return super().construct_mapping(node, deep=deep)


def available_templates() -> list[str]:
    """
    List the shipped role-template names (sorted).

    :returns: Template stems, e.g. ``['bug-reviewer', 'coder', ...]``,
        or ``[]`` when the templates dir is absent (stripped wheel).
    """
    try:
        return sorted(p.stem for p in _TEMPLATES_DIR.glob('*.md'))
    except OSError:
        return []


def template_prompt(name: str) -> str:
    """
    Load a shipped role template's prompt text.

    :param name: Template name, e.g. ``'coder'`` (no extension).
    :returns: The template's prompt text.
    :raises PipelineError: When no such template ships.
    """
    path = _TEMPLATES_DIR / f'{name}.md'
    try:
        return path.read_text(encoding='utf-8')
    except OSError as exc:
        raise PipelineError(
            f'unknown template {name!r}; available: '
            f'{", ".join(available_templates()) or "(none)"}'
        ) from exc


def _sanitize(segment: str) -> str:
    """Lowercase *segment* to ``[a-z0-9-]`` for a name/dir component."""
    out = _NAME_SANITIZE_RE.sub('-', segment.lower()).strip('-')
    return out or 'x'


@dataclass(frozen=True)
class Subtask:
    """
    One ordered build increment (a campaign chunk / module).

    Either proposed by the planner (flat campaign — see
    :func:`sbx_omnigent.runner.parse_subtasks`) or supplied by the human
    via the config ``subtasks:`` / ``subtask_file:`` (per-module mode).

    :param id: Sanitized, unique chunk id used to namespace the chunk's
        node branches/worktrees (e.g. ``m0`` → ``pl/<run>/m0-tests``).
    :param title: One-line goal, handed to the builders as the chunk's
        directive alongside the plan of record.
    """

    id: str
    title: str


def parse_subtask_items(text: str) -> list[Subtask]:
    """
    Parse ``- [<id>] <title>`` lines into ordered, unique subtasks.

    Every matching item line becomes a :class:`Subtask`; non-item lines
    are ignored (no header, no stop-at-prose — the caller pre-slices
    when those semantics are needed). Ids are lowercased to
    ``[a-z0-9-]`` and de-duplicated so they are safe to namespace
    branches with; an id that sanitizes to empty falls back to its
    0-based position.

    :param text: Text containing zero or more item lines.
    :returns: The ordered, de-duplicated subtasks.
    """
    out: list[Subtask] = []
    seen: set[str] = set()
    for line in text.splitlines():
        item = _SUBTASK_ITEM_RE.match(line)
        if item is None:
            continue
        raw_id = item.group('id').strip()
        sid = _NAME_SANITIZE_RE.sub('-', raw_id.lower()).strip('-')
        sid = sid or f's{len(out)}'
        base, n = sid, 2
        while sid in seen:
            sid, n = f'{base}-{n}', n + 1
        seen.add(sid)
        title = (item.group('title') or raw_id).strip()
        out.append(Subtask(id=sid, title=title))
    return out


@dataclass(frozen=True)
class DiskSpec:
    """
    Per-unit disk estimates for the startup preflight (``disk:``).

    The runner cannot know what a project builds, and the difference is
    an order of magnitude: a compiled writer leaves GIGABYTES of build
    output in its host worktree, while a Python one leaves almost
    nothing. These default to the COMPILED case so the check errs toward
    refusing, and a lighter project can lower them.

    WHAT ``per_worktree_gb`` IS CALIBRATED ON, because the number looks
    arbitrary otherwise. Measured on this project\'s own Rust pipeline:

    * single-crate workspace: 2.2, 2.5 and 3.1 GB per writer, and 8.9 GB
      for the node that also ran the coverage gate (7.0 GB
      ``target/debug`` + 2.0 GB ``target/llvm-cov-target``)
    * the SAME pipeline once that workspace reached five crates and 381
      dependencies: one writer\'s ``target/`` reached 26 GB

    So the observed range is 2.2-26 GB, and the 2.0 default this
    replaced sat below the SMALLEST writer ever measured — it
    under-counted every instance of the case it claims to model, which
    is the one thing a check that exists to refuse must not do. 4.0
    clears the single-crate cluster with margin; anything heavier is
    project-specific and belongs in a ``disk:`` block.

    Two things multiply it, and a reader tuning ``disk:`` needs both:

    * a COVERAGE gate on a compiled language builds a SECOND full tree
      (``cargo llvm-cov`` uses its own target dir), roughly doubling the
      node that runs it. ``pytest --cov`` and ``vitest --coverage`` add
      nothing, so this is not a general multiplier.
    * the dependency graph, plus incremental artifacts accumulating
      across review rounds — that is the whole 2.2 -> 26 GB span above,
      on one pipeline whose per-node work never changed.

    :param per_vm_gb: Disk one agent microVM costs the host (its image
        snapshot + writable overlay).
    :param per_worktree_gb: Disk one WRITER's host worktree costs — its
        clone plus whatever the agent builds inside it. Reader, judge,
        and reviewer worktrees are checkouts with no build output and
        are not counted (see
        :func:`sbx_omnigent.runner.writer_worktrees`).
    :param headroom_gb: Space left for the host itself.
    """

    per_vm_gb: float = 3.5
    per_worktree_gb: float = 4.0
    headroom_gb: float = 5.0


@dataclass(frozen=True)
class PipelineAgent:
    """
    One declared agent (a pipeline participant).

    :param name: Author-facing key from the ``agents:`` map.
    :param template: Shipped role template supplying the prompt, or
        ``None`` when an inline ``prompt`` is given instead.
    :param prompt: Resolved prompt text (from ``prompt``/``prompt_file``
        override, else the template). Always populated after parse.
    :param harness: Omnigent harness id (e.g. ``'claude-native'``,
        ``'antigravity-native'``, ``'codex-native'``).
    :param model: Optional model to pin at session create (not baked
        into the bundle). ``None`` = harness default.
    :param effort: Optional reasoning effort to pin at create. ``None``
        = default. Ignored by harnesses without an effort knob (agy).
    :param skills_dir: Absolute path to a skills directory to copy into
        the materialized bundle, or ``None``.
    """

    name: str
    template: str | None
    prompt: str
    harness: str
    model: str | None
    effort: str | None
    skills_dir: Path | None


@dataclass(frozen=True)
class PipelineStage:
    """
    One node in the pipeline DAG.

    Either a single node (``run`` set) or a fan-out of concurrent
    isolated sub-nodes (``parallel`` set) — never both.

    :param id: Unique stage id (also the branch/dir label).
    :param run: Agent name(s) acting in this stage. One name = a solo
        node; several = a review group sharing a gate.
    :param write: When true, the stage's (single) agent is a WRITER —
        it gets its own isolated rw worktree + branch. Readers mount a
        branch ``:ro``.
    :param needs: Upstream stage ids this stage depends on (DAG edges).
        Empty = depends on the previous listed stage (linear default).
    :param from_branch: Explicit upstream stage id whose branch seeds a
        writer's worktree; ``None`` = inferred from *needs*/previous.
    :param gate: Gate kind for a multi-agent stage, e.g. consensus.
    :param on_block: Stage id to loop back to when the gate blocks.
    :param selects: When set (e.g. ``'branch'``), the stage's agent is a
        JUDGE that picks a winning upstream branch.
    :param verifies: When set (``'findings'``), the stage's agent is a
        VERIFIER: it reads the non-blocking findings this module's
        reviewers raised and checks each against the code, so a finding
        that does not reproduce, or that the codebase already records,
        never becomes an issue somebody has to triage by hand.
    :param parallel: Concurrent sub-stages (each a full PipelineStage),
        for competing isolated writers.
    """

    id: str
    run: tuple[str, ...] = ()
    write: bool = False
    needs: tuple[str, ...] = ()
    from_branch: str | None = None
    gate: str | None = None
    on_block: str | None = None
    selects: str | None = None
    verifies: str | None = None
    #: Hold this writer to TEST code only — every path it changes must
    #: match ``test_paths`` (or ``generated``). Declared per stage
    #: rather than inferred from the template: a pipeline may run the
    #: tdd-writer template somewhere that legitimately is not gated.
    tests_only: bool = False
    parallel: tuple[PipelineStage, ...] = ()


@dataclass(frozen=True)
class PublishSpec:
    """
    How to publish the pipeline's result.

    :param mode: ``'pr'`` (draft PR) or ``'local'`` (push branch only)
        or ``'none'`` (leave the branch, publish nothing).
    :param branch: Stage id whose branch is published; ``None`` = the
        last writer/selected branch (resolved by the runner).
    :param stack: In a campaign, base each module's pull request on the
        PREVIOUS module's published branch instead of the repo's base
        branch.

        Without it, every module's PR targets ``main``, so a module
        opened while earlier ones are still unmerged shows their code
        as its own: on a live five-module build, m2's request came out
        at 55 files and 12,524 added lines when its own work was 28
        files. That is not reviewable, and it is exactly the case for
        anyone who wants requests to queue up rather than merge one at
        a time.

        Safe when they DO merge promptly, too: GitHub re-targets an
        open request to the base's own base once that base is merged
        and deleted, and the runner falls back to the repo's base
        branch when the intended one is already gone.
    """

    mode: str = 'none'
    branch: str | None = None
    stack: bool = True


@dataclass(frozen=True)
class PipelineConfig:
    """
    A fully-parsed ``pipeline.yaml``.

    :param name: Pipeline name (namespace for agents); defaults to the
        file stem.
    :param repo: Repo URL or local path to cut worktrees from.
    :param base_branch: Branch to cut from; ``None`` = repo default.
    :param agents: ``{name: PipelineAgent}`` in declaration order.
    :param stages: The DAG, in declaration order.
    :param publish: Publish spec.
    :param task: Optional task text; when present the runner runs to
        completion, else it provisions and hands off.
    :param acceptance: Optional acceptance-contract text.
    :param plan_artifact: Optional repo-relative path the approved plan
        of record is committed to on the published branch; ``None`` uses
        the default ``docs/plans/<name>.md``.
    :param context: Optional pipeline-wide project context baked into
        EVERY agent's system prompt at materialize (a project-wide
        guidance layer shared by all roles); ``None`` = no shared
        context. See :func:`_prompt_with_context`.
    :param subtasks: Human-supplied ordered modules (per-module campaign
        mode) from ``subtasks:`` / ``subtask_file:``; empty = none, so
        the runner uses a single pass or the planner-proposed (flat)
        campaign instead.
    :param disk: Per-unit estimates for the startup disk preflight.
        Defaults suit a compiled language; see :class:`DiskSpec`.
    :param verify: The mechanical gate run before publish — the
        project's own test/coverage command, executed in a disposable
        sandbox on the branch about to ship. ``None`` = no gate, and
        nothing in the pipeline ever executes the tests.
    :param turn_timeout: Seconds one agent turn may take before the
        runner gives up; ``None`` uses the runner default. Raise it for
        a pipeline whose stages stop to ask the human questions — that
        wait is spent inside the turn's budget.
    :param setup: How an agent prepares its microVM before working —
        toolchain installs and the like. Relayed into every builder and
        reviewer turn, because a VM that lacks a compiler is exactly
        where agents stop verifying and start guessing.
    :param generated: Glob patterns for files that do NOT count as
        implementation (lockfiles and other build output). Empty uses
        the runner's default set. See
        :meth:`sbx_omnigent.runner.PipelineRunner._require_implementation`.
    :param test_paths: Glob patterns a ``tests_only`` stage may change.
        Unset uses the built-in default. Dependency manifests are NOT
        listed here: a test needing a new dev-dependency must edit
        ``Cargo.toml``, and every greenfield module in this project's
        history did exactly that — they are allowed via ``generated``
        and the ordinary guarded-file review instead.
    :param guarded: Glob patterns for files that ARE a check — the
        configuration of a gate, linter, auditor or CI workflow, and the
        suppression/ignore lists they read. A writer editing one of
        these can turn a red gate green without fixing anything, so any
        change to them is named to the reviewers instead of passing
        silently. There is NO default set on purpose: which files
        constitute a check is a property of the project, not of this
        launcher, and hard-coding names like ``deny.toml`` would teach
        it one ecosystem's tooling. Empty disables the surfacing.
    """

    name: str
    repo: str
    base_branch: str | None
    agents: dict[str, PipelineAgent]
    stages: tuple[PipelineStage, ...]
    publish: PublishSpec
    task: str | None = None
    acceptance: str | None = None
    plan_artifact: str | None = None
    context: str | None = None
    setup: str | None = None
    generated: tuple[str, ...] = ()
    guarded: tuple[str, ...] = ()
    #: What a ``tests_only`` stage may change. Empty means the built-in
    #: default set; see ``runner._TEST_PATH_GLOBS``.
    test_paths: tuple[str, ...] = ()
    build_cache: tuple[str, ...] = ()
    subtasks: tuple[Subtask, ...] = ()
    turn_timeout: float | None = None
    verify: VerifySpec | None = None
    disk: DiskSpec = field(default_factory=DiskSpec)
    source_path: Path | None = field(default=None, compare=False)


def _require_mapping(value: object, what: str) -> dict[str, object]:
    """Return *value* as a dict or raise a :class:`PipelineError`."""
    if not isinstance(value, dict):
        raise PipelineError(f'{what} must be a mapping')
    return value


def _opt_str(value: object, what: str) -> str | None:
    """Coerce an optional YAML scalar to a non-empty string or None."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise PipelineError(f'{what} must be a non-empty string')
    return value.strip()


def _parse_agent(
    name: str, raw: object, *, base_dir: Path
) -> PipelineAgent:
    """
    Parse one ``agents:`` entry into a :class:`PipelineAgent`.

    :param name: The agent key.
    :param raw: The entry's mapping.
    :param base_dir: Directory the pipeline file lives in (resolves
        ``prompt_file`` / ``skills`` relative paths).
    :returns: The parsed agent (prompt already resolved).
    :raises PipelineError: On a malformed entry or unresolved
        template/prompt/skills.
    """
    entry = _require_mapping(raw, f'agent {name!r}')
    template = _opt_str(entry.get('template'), f'agent {name!r} template')
    inline = _opt_str(entry.get('prompt'), f'agent {name!r} prompt')
    prompt_file = _opt_str(
        entry.get('prompt_file'), f'agent {name!r} prompt_file'
    )
    if prompt_file is not None:
        if inline is not None:
            raise PipelineError(
                f'agent {name!r}: set only one of prompt / prompt_file'
            )
        fpath = (base_dir / prompt_file).resolve()
        try:
            inline = fpath.read_text(encoding='utf-8')
        except OSError as exc:
            raise PipelineError(
                f'agent {name!r}: cannot read prompt_file {prompt_file!r}'
            ) from exc
    if inline is not None:
        prompt = inline
    elif template is not None:
        prompt = template_prompt(template)
    else:
        raise PipelineError(
            f'agent {name!r} needs a template or a prompt/prompt_file'
        )

    harness = (
        _opt_str(entry.get('harness'), f'agent {name!r} harness')
        or _DEFAULT_HARNESS
    )
    skills_dir: Path | None = None
    skills = _opt_str(entry.get('skills'), f'agent {name!r} skills')
    if skills is not None:
        sdir = (base_dir / skills).resolve()
        if not sdir.is_dir():
            raise PipelineError(
                f'agent {name!r}: skills path {skills!r} is not a directory'
            )
        skills_dir = sdir
    return PipelineAgent(
        name=name,
        template=template,
        prompt=prompt,
        harness=harness,
        model=_opt_str(entry.get('model'), f'agent {name!r} model'),
        effort=_opt_str(entry.get('effort'), f'agent {name!r} effort'),
        skills_dir=skills_dir,
    )


def _str_tuple(value: object) -> tuple[str, ...] | None:
    """
    Coerce a name or list-of-names into a tuple of strings.

    :param value: A string, a list of strings, or ``None``.
    :returns: The tuple (empty for ``None``/``[]``), or ``None`` when
        *value* is neither a string nor an all-string list.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return tuple(value)
    return None


def _parse_stage(raw: object, *, index: int) -> PipelineStage:
    """
    Parse one ``stages:`` entry (recursively for ``parallel``).

    :param raw: The stage mapping.
    :param index: Position in the list (for a default id).
    :returns: The parsed stage.
    :raises PipelineError: On a malformed stage.
    """
    entry = _require_mapping(raw, f'stage #{index}')
    sid = _opt_str(entry.get('id'), f'stage #{index} id') or f'stage-{index}'

    if 'parallel' in entry:
        sub_raw = entry.get('parallel')
        if not isinstance(sub_raw, list) or not sub_raw:
            raise PipelineError(
                f'stage {sid!r}: parallel must be a non-empty list'
            )
        subs = tuple(
            _parse_stage(s, index=i) for i, s in enumerate(sub_raw)
        )
        return PipelineStage(id=sid, parallel=subs)

    run = _str_tuple(entry.get('run'))
    if run is None or not run:
        raise PipelineError(
            f'stage {sid!r}: run must be an agent name or list of names'
        )
    if not run:
        raise PipelineError(f'stage {sid!r}: run is empty')

    needs = _str_tuple(entry.get('needs', []))
    if needs is None:
        raise PipelineError(f'stage {sid!r}: needs must be a name or list')

    return PipelineStage(
        id=sid,
        run=run,
        write=bool(entry.get('write', False)),
        tests_only=bool(entry.get('tests_only', False)),
        needs=needs,
        from_branch=_opt_str(entry.get('from'), f'stage {sid!r} from'),
        gate=_opt_str(entry.get('gate'), f'stage {sid!r} gate'),
        on_block=_opt_str(entry.get('on_block'), f'stage {sid!r} on_block'),
        selects=_opt_str(entry.get('selects'), f'stage {sid!r} selects'),
        verifies=_opt_str(
            entry.get('verifies'), f'stage {sid!r} verifies'
        ),
    )


def _parse_publish(raw: object) -> PublishSpec:
    """Parse the ``publish:`` value (a string mode or a mapping)."""
    if raw is None:
        return PublishSpec()
    if isinstance(raw, str):
        mode = raw.strip().lower()
        if mode not in ('pr', 'local', 'none'):
            raise PipelineError(f'publish {raw!r} must be pr|local|none')
        return PublishSpec(mode=mode)
    entry = _require_mapping(raw, 'publish')
    mode = (_opt_str(entry.get('mode'), 'publish.mode') or 'pr').lower()
    if mode not in ('pr', 'local', 'none'):
        raise PipelineError(f'publish.mode {mode!r} must be pr|local|none')
    stack = entry.get('stack', True)
    if not isinstance(stack, bool):
        raise PipelineError('publish.stack must be true or false')
    return PublishSpec(
        stack=stack,
        mode=mode, branch=_opt_str(entry.get('branch'), 'publish.branch')
    )


def _default_stages(
    agents: dict[str, PipelineAgent],
) -> tuple[PipelineStage, ...]:
    """
    Synthesize the classic swarm when ``stages:`` is omitted.

    Heuristic: the first agent whose template is a writer kind (coder /
    tdd-writer) or named ``build``/``coder`` is the writer; each
    other agent is a reviewer in one consensus stage that loops back.

    :param agents: Declared agents.
    :returns: ``(build, review)`` stages.
    :raises PipelineError: When no writer can be identified.
    """
    writer_kinds = {'coder', 'tdd-writer'}
    writer = next(
        (
            a.name
            for a in agents.values()
            if a.template in writer_kinds or a.name in ('build', 'coder')
        ),
        None,
    )
    if writer is None:
        raise PipelineError(
            'no stages given and no writer agent found (add a coder '
            'template or an explicit stages: block)'
        )
    reviewers = tuple(n for n in agents if n != writer)
    stages = [PipelineStage(id='build', run=(writer,), write=True)]
    if reviewers:
        stages.append(
            PipelineStage(
                id='review',
                run=reviewers,
                needs=('build',),
                gate=_DEFAULT_GATE,
                on_block='build',
            )
        )
    return tuple(stages)


def _validate(config: PipelineConfig) -> None:  # noqa: C901
    """
    Cross-check stage references against declared agents + stage ids.

    :param config: The assembled config.
    :raises PipelineError: On an unknown agent/stage reference or a
        duplicate stage id.
    """
    seen_ids: set[str] = set()

    def check_stage(stage: PipelineStage) -> None:
        for sub in stage.parallel:
            check_stage(sub)
        if stage.parallel:
            if stage.id in seen_ids:
                raise PipelineError(f'duplicate stage id {stage.id!r}')
            seen_ids.add(stage.id)
            return
        if stage.id in seen_ids:
            raise PipelineError(f'duplicate stage id {stage.id!r}')
        seen_ids.add(stage.id)
        for agent in stage.run:
            if agent not in config.agents:
                raise PipelineError(
                    f'stage {stage.id!r} runs unknown agent {agent!r}'
                )

    for stage in config.stages:
        check_stage(stage)
    # needs / on_block / from resolve to known stage ids.
    for stage in _iter_stages(config.stages):
        for ref, what in (
            *[(n, 'needs') for n in stage.needs],
            *([(stage.on_block, 'on_block')] if stage.on_block else []),
            *([(stage.from_branch, 'from')] if stage.from_branch else []),
        ):
            if ref not in seen_ids:
                raise PipelineError(
                    f'stage {stage.id!r} {what} references unknown '
                    f'stage {ref!r}'
                )
    # A pinned model must belong to a family its harness can actually
    # run. Omnigent enforces this at session CREATE — the same
    # `model_family_mismatch` is its own dispatch guard — which on
    # a full cadre can be stage 6 of 8, ten microVMs into a run.
    # Checking it here turns that into a parse error naming the
    # offending agent, before anything is provisioned. Same reason
    # as the duplicate-key and inert-provider work: a config that
    # parses clean and dies later is the expensive kind of wrong.
    #
    # Agents with no `model:` are skipped — they run the bundle's own
    # default, so there is nothing to check.
    for agent_name, agent in config.agents.items():
        if not agent.model:
            continue
        reason = model_family_mismatch(agent.harness, agent.model)
        if reason is not None:
            raise PipelineError(f'agent {agent_name!r}: {reason}')


def _iter_stages(
    stages: tuple[PipelineStage, ...],
) -> list[PipelineStage]:
    """Flatten stages + their parallel sub-stages into one list."""
    out: list[PipelineStage] = []
    for stage in stages:
        out.append(stage)
        out.extend(stage.parallel)
    return out


def load_pipeline(path: str | Path) -> PipelineConfig:
    """
    Parse and validate a ``pipeline.yaml``.

    :param path: Path to the pipeline file.
    :returns: The validated :class:`PipelineConfig`.
    :raises PipelineError: When the file is missing, malformed, or
        internally inconsistent.
    """
    fpath = Path(path).resolve()
    try:
        raw_text = fpath.read_text(encoding='utf-8')
    except OSError as exc:
        raise PipelineError(f'cannot read pipeline file {path!r}') from exc
    try:
        raw = yaml.load(raw_text, _StrictLoader)
    except _DuplicateKey as exc:
        # Worth its own message: this IS valid YAML, which is exactly
        # the problem — the file parses and means something other than
        # what it appears to.
        raise PipelineError(
            f'{fpath.name}: duplicate key {exc.key!r}\n'
            f'  first defined on line {exc.first.line + 1}, '
            f'column {exc.first.column + 1}\n'
            f'  redefined on line {exc.again.line + 1}, '
            f'column {exc.again.column + 1}\n'
            f'YAML keeps the LAST definition and discards the first '
            f'silently, so this file does not mean what it looks like '
            f'it means. Delete one of them.'
        ) from exc
    except yaml.YAMLError as exc:
        raise PipelineError(f'invalid YAML in {path!r}: {exc}') from exc
    root = _require_mapping(raw, 'pipeline')

    repo = _opt_str(root.get('repo'), 'repo')
    if repo is None:
        raise PipelineError('pipeline: repo is required')

    agents_raw = _require_mapping(root.get('agents'), 'agents')
    if not agents_raw:
        raise PipelineError('pipeline: at least one agent is required')
    base_dir = fpath.parent
    agents = {
        name: _parse_agent(name, entry, base_dir=base_dir)
        for name, entry in agents_raw.items()
    }

    stages_raw = root.get('stages')
    if stages_raw is None:
        stages = _default_stages(agents)
    else:
        if not isinstance(stages_raw, list) or not stages_raw:
            raise PipelineError('stages must be a non-empty list')
        stages = tuple(
            _parse_stage(s, index=i) for i, s in enumerate(stages_raw)
        )

    name = _opt_str(root.get('name'), 'name') or _sanitize(fpath.stem)
    config = PipelineConfig(
        name=_sanitize(name),
        repo=repo,
        base_branch=_opt_str(root.get('base_branch'), 'base_branch'),
        agents=agents,
        stages=stages,
        publish=_parse_publish(root.get('publish')),
        task=_resolve_text_or_file(root, base_dir, 'task'),
        acceptance=_resolve_text_or_file(root, base_dir, 'acceptance'),
        plan_artifact=_parse_plan_artifact(root.get('plan_artifact')),
        context=_resolve_text_or_file(root, base_dir, 'context'),
        setup=_resolve_text_or_file(root, base_dir, 'setup'),
        generated=_parse_generated(root.get('generated')),
        guarded=_parse_globs(root.get('guarded'), 'guarded'),
        test_paths=_parse_globs(root.get('test_paths'), 'test_paths'),
        build_cache=_parse_cache_dirs(root.get('build_cache')),
        subtasks=_parse_subtasks_config(root, base_dir),
        turn_timeout=_parse_turn_timeout(root.get('turn_timeout')),
        verify=_parse_verify(root.get('verify')),
        disk=_parse_disk(root.get('disk')),
        source_path=fpath,
    )
    _validate(config)
    _validate_verify_setup(config)
    return config


def _parse_plan_artifact(value: object) -> str | None:
    """
    Validate the optional ``plan_artifact`` path.

    :param value: The raw ``plan_artifact`` field, or ``None``.
    :returns: A safe repo-relative path, or ``None`` for the default.
    :raises PipelineError: If it is not a relative, non-escaping path.
    """
    path = _opt_str(value, 'plan_artifact')
    if path is None:
        return None
    norm = PurePosixPath(path)
    if norm.is_absolute() or '..' in norm.parts:
        raise PipelineError(
            f'plan_artifact must be a relative path inside the repo: '
            f'{path!r}'
        )
    return path


def _parse_cache_dirs(value: object) -> tuple[str, ...]:
    """
    Validate the optional ``build_cache`` directory-name list.

    These names are joined onto a worktree path and onto the canonical
    root, so they are validated as BARE directory names — no
    separators, no ``.``/``..``, nothing absolute. A traversal here
    would let a pipeline file address any directory on the host, and a
    pipeline file is shareable content.

    :param value: The raw field, or ``None`` when unset.
    :returns: The directory names, or ``()`` when unset (cache off).
    :raises PipelineError: If it is not a list of bare directory names.
    """
    if value is None:
        return ()
    if not isinstance(value, list):
        raise PipelineError(
            'build_cache must be a list of directory names, e.g. '
            "['target']"
        )
    out = []
    for entry in value:
        if not isinstance(entry, str) or not entry:
            raise PipelineError(
                'build_cache entries must be non-empty strings'
            )
        # A separator check already covers the absolute case, so this
        # needs no path library.
        if '/' in entry or '\\' in entry or entry in ('.', '..'):
            raise PipelineError(
                f'build_cache entry {entry!r} must be a bare directory '
                f'name (no path separators, no . or ..)'
            )
        out.append(entry)
    return tuple(out)


def _parse_globs(value: object, what: str) -> tuple[str, ...]:
    """
    Validate an optional list of glob patterns.

    :param value: The raw field, or ``None`` when unset.
    :param what: Field name, for error messages.
    :returns: The patterns, or ``()`` when unset.
    :raises PipelineError: If it is not a list of non-empty strings.
    """
    if value is None:
        return ()
    if not isinstance(value, list):
        raise PipelineError(f'{what} must be a list of glob patterns')
    out = []
    for i, entry in enumerate(value):
        pattern = _opt_str(entry, f'{what}[{i}]')
        if pattern is None:
            raise PipelineError(f'{what}[{i}] must be a non-empty string')
        out.append(pattern)
    return tuple(out)


def _parse_generated(value: object) -> tuple[str, ...]:
    """
    Validate the optional ``generated:`` glob list.

    :param value: The raw field, or ``None`` for the runner default.
    :returns: The patterns, or ``()`` when unset.
    :raises PipelineError: If it is not a list of non-empty strings.
    """
    if value is None:
        return ()
    if not isinstance(value, list):
        raise PipelineError('generated must be a list of glob patterns')
    out = []
    for i, entry in enumerate(value):
        pattern = _opt_str(entry, f'generated[{i}]')
        if pattern is None:
            raise PipelineError(f'generated[{i}] must be a non-empty string')
        out.append(pattern)
    return tuple(out)


@dataclass(frozen=True)
class VerifySpec:
    """
    The mechanical pre-publish gate (``verify:``).

    :param command: Shell program run in a disposable sandbox on the
        branch about to publish. Exit 0 publishes; anything else does
        not. Language-agnostic by design — the launcher never learns
        what a test or a coverage report is, only what 0 means.
    :param demo: Optional command run after *command* passes, in the
        same sandbox. Its captured output goes into the pull request as
        the proof that the built thing actually RUNS — a passing suite
        shows the code satisfies its tests, not that a reviewer can see
        it work. A non-zero exit fails the gate.

        A demonstration is expected to be HERMETIC: it stands up
        whatever it needs inside the sandbox — a database, a TLS server,
        fixtures — so it proves real sockets, a real handshake and real
        queries without reaching a live endpoint or holding a
        credential, and reproduces identically for whoever reads the
        pull request.
    :param setup: SHELL run in the gate's sandbox before *command*, to
        install whatever the command needs in a fresh VM. Distinct from
        the pipeline's top-level ``setup:`` on purpose: that one is
        PROSE relayed into agent turns ("This VM has no Rust toolchain
        preinstalled. Install one before you do anything else: ..."),
        and pasting it into a shell produced ``sh: 2: This: not found``
        — a gate that could never run, whose exit 127 was then blamed on
        the branch and closed by re-driving a writer three times. An
        empty string states that *command* needs no prologue.
    :param cpus: CPUs the gate's sandbox gets; ``None`` leaves sbx's
        default, which is EVERY host CPU.
    :param memory: Memory limit for the gate's sandbox (e.g.
        ``"8g"``); ``None`` leaves sbx's default of half the host.

        These exist because the gate builds its own sandbox rather than
        going through the server, so the server's per-sandbox limits do
        not reach it — and the defaults are actively hostile to a build:
        a guest that sees every host CPU runs that many compile and
        link jobs at once inside a guest capped at half the host's
        memory. Observed live: the gate's linker was killed while the
        same branch built and tested cleanly in a capped agent VM, and
        the runner read that as the BRANCH failing and re-drove a writer
        to "fix" it.
    :param coverage_min: Substituted for every ``{coverage_min}`` in
        *command*, so the threshold lives in one place. ``None`` when
        the command carries no placeholder.
    :param timeout_s: Budget for the command; overrunning fails the
        gate (a suite that never terminates is the branch's problem).
    :param image: Template image for the verification sandbox;
        ``None`` uses the launcher's default host image.
    """

    command: str
    setup: str | None = None
    demo: str | None = None
    coverage_min: float | None = None
    timeout_s: float = 1800.0
    image: str | None = None
    cpus: int | None = None
    memory: str | None = None

    def rendered(self) -> str:
        """
        The gate command with ``{coverage_min}`` substituted.

        :returns: The command as it will run.
        """
        return self._render(self.command)

    def rendered_demo(self) -> str | None:
        """
        The demonstration command, substituted; ``None`` when unset.

        :returns: The command as it will run, or ``None``.
        """
        return self._render(self.demo) if self.demo else None

    def _render(self, text: str) -> str:
        """
        Substitute ``{coverage_min}`` into a command.

        Deliberately a literal replace, NOT ``str.format`` — a real
        shell command is full of braces (``${VAR}``, ``awk '{print}'``)
        and formatting one would corrupt it or raise.
        """
        if self.coverage_min is None:
            return text
        pretty = (
            f'{self.coverage_min:g}'
            if self.coverage_min % 1
            else f'{int(self.coverage_min)}'
        )
        return text.replace('{coverage_min}', pretty)


def _parse_disk(raw: object) -> DiskSpec:
    """
    Parse the optional ``disk:`` block.

    :param raw: The raw field, or ``None`` for the defaults.
    :returns: The :class:`DiskSpec`.
    :raises PipelineError: If a value is not a non-negative number. Zero
        is allowed — deliberately zeroing a term is a legitimate way to
        say "this project does not pay that cost".
    """
    if raw is None:
        return DiskSpec()
    entry = _require_mapping(raw, 'disk')
    values: dict[str, float] = {}
    for key in ('per_vm_gb', 'per_worktree_gb', 'headroom_gb'):
        value = entry.get(key)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PipelineError(f'disk.{key} must be a number')
        if value < 0:
            raise PipelineError(f'disk.{key} must not be negative')
        values[key] = float(value)
    return DiskSpec(**values)


def _validate_verify_setup(config: PipelineConfig) -> None:
    """
    Refuse a gate that has no way to prepare its own sandbox.

    The gate runs in a FRESH VM with no project toolchain. The builders
    are told how to install one by the top-level ``setup:``, but that
    text is prose addressed to an agent and cannot be run as a shell —
    it used to be pasted into the gate's script, which failed on its
    first word and was reported as the branch failing its tests.

    So when a pipeline tells its agents to install a toolchain, the gate
    must be told too, in shell. ``verify.setup: ""`` states explicitly
    that the command prepares itself (a repo script that installs what
    it needs) and silences this.

    :param config: The assembled pipeline.
    :raises PipelineError: If the gate would run with no prologue while
        the agents are being told to install a toolchain.
    """
    spec = config.verify
    if spec is None or spec.setup is not None:
        return
    if not (config.setup or '').strip():
        return
    raise PipelineError(
        'this pipeline has a setup: block, so its agents are told to '
        'install a toolchain — but verify.setup is not set, and the '
        'verification gate runs in a FRESH sandbox that has none. '
        'setup: is prose for agents and is NOT shell, so it cannot be '
        'reused here. Add the install commands as shell under '
        'verify.setup:, or set verify.setup: "" if verify.command '
        'installs what it needs itself.'
    )


def _parse_verify(raw: object) -> VerifySpec | None:
    """
    Parse the optional ``verify:`` block.

    :param raw: The raw field, or ``None`` for no gate.
    :returns: The :class:`VerifySpec`, or ``None``.
    :raises PipelineError: On a malformed block, an out-of-range
        threshold, or a ``{coverage_min}`` placeholder with no value to
        put in it (which would otherwise reach the shell as a literal
        brace and fail cryptically inside a microVM).
    """
    if raw is None:
        return None
    entry = _require_mapping(raw, 'verify')
    command = _opt_str(entry.get('command'), 'verify.command')
    if command is None:
        raise PipelineError('verify.command is required')
    minimum = entry.get('coverage_min')
    if minimum is not None:
        if isinstance(minimum, bool) or not isinstance(
            minimum, (int, float)
        ):
            raise PipelineError('verify.coverage_min must be a number')
        if not 0 <= minimum <= 100:
            raise PipelineError(
                'verify.coverage_min must be between 0 and 100'
            )
        minimum = float(minimum)
    demo = _opt_str(entry.get('demo'), 'verify.demo')
    setup = entry.get('setup')
    if setup is not None and not isinstance(setup, str):
        raise PipelineError('verify.setup must be a string')
    if '{coverage_min}' in (command + (demo or '')) and minimum is None:
        raise PipelineError(
            'verify.command uses {coverage_min} but verify.coverage_min '
            'is not set'
        )
    cpus = entry.get('cpus')
    if cpus is not None and (
        isinstance(cpus, bool) or not isinstance(cpus, int) or cpus < 1
    ):
        raise PipelineError('verify.cpus must be a positive integer')
    memory = entry.get('memory')
    if memory is not None and (
        not isinstance(memory, str) or not memory.strip()
    ):
        raise PipelineError(
            "verify.memory must be a non-empty string, e.g. '8g'"
        )
    timeout = _parse_turn_timeout(entry.get('timeout'))
    return VerifySpec(
        command=command,
        setup=setup,
        demo=demo,
        coverage_min=minimum,
        timeout_s=timeout if timeout is not None else 1800.0,
        image=_opt_str(entry.get('image'), 'verify.image'),
        cpus=cpus,
        memory=memory.strip() if memory else None,
    )


def _parse_turn_timeout(value: object) -> float | None:
    """
    Validate the optional ``turn_timeout`` (seconds).

    :param value: The raw field, or ``None`` for the runner default.
    :returns: A positive number of seconds, or ``None``.
    :raises PipelineError: If it is not a positive number.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PipelineError('turn_timeout must be a positive number')
    if value <= 0:
        raise PipelineError('turn_timeout must be a positive number')
    return float(value)


def _parse_subtasks_config(
    root: dict[str, object], base_dir: Path
) -> tuple[Subtask, ...]:
    """
    Parse the optional human-supplied module list (per-module campaign).

    ``subtasks:`` is an inline list of ``{id, title}`` mappings;
    ``subtask_file:`` points at a file of ``- [<id>] <title>`` lines
    (relative to the pipeline file). At most one may be set. Present
    and non-empty switches the runner into per-module mode: it loops
    the FULL pipeline (plan + build) once per module, in order.

    :param root: The parsed pipeline mapping.
    :param base_dir: Directory the pipeline file lives in (resolves
        ``subtask_file``).
    :returns: The ordered modules, or ``()`` when neither is set.
    :raises PipelineError: If both are set, the file cannot be read, an
        inline entry is malformed, or the result is empty.
    """
    inline = root.get('subtasks')
    path = _opt_str(root.get('subtask_file'), 'subtask_file')
    if path is not None:
        if inline is not None:
            raise PipelineError('set only one of subtasks / subtask_file')
        fpath = (base_dir / path).resolve()
        try:
            text = fpath.read_text(encoding='utf-8')
        except OSError as exc:
            raise PipelineError(
                f'cannot read subtask_file {path!r}'
            ) from exc
        items = parse_subtask_items(text)
        if not items:
            raise PipelineError(
                f'subtask_file {path!r} has no "- [<id>] <title>" items'
            )
        return tuple(items)
    if inline is None:
        return ()
    if not isinstance(inline, list) or not inline:
        raise PipelineError('subtasks must be a non-empty list')
    out: list[Subtask] = []
    seen: set[str] = set()
    for i, entry in enumerate(inline):
        item = _require_mapping(entry, f'subtasks[{i}]')
        sid = _opt_str(item.get('id'), f'subtasks[{i}].id')
        title = _opt_str(item.get('title'), f'subtasks[{i}].title')
        if sid is None or title is None:
            raise PipelineError(f'subtasks[{i}] needs id and title')
        clean = _sanitize(sid)
        base, n = clean, 2
        while clean in seen:
            clean, n = f'{base}-{n}', n + 1
        seen.add(clean)
        out.append(Subtask(id=clean, title=title))
    return tuple(out)


def _resolve_text_or_file(
    root: dict[str, object], base_dir: Path, key: str
) -> str | None:
    """
    Resolve a top-level text field as inline or from a ``<key>_file``.

    Mirrors an agent's ``prompt`` / ``prompt_file`` pair for the
    pipeline-wide text fields (``task``, ``acceptance``, ``context``):
    ``<key>:`` is inline text and ``<key>_file:`` reads it from a path
    relative to the pipeline file (handy for a long brief kept in its
    own file). At most one of the pair may be set.

    :param root: The parsed pipeline mapping.
    :param base_dir: Directory the pipeline file lives in (resolves the
        ``<key>_file`` relative path).
    :param key: The field name, e.g. ``'task'``.
    :returns: The text, or ``None`` when neither is set.
    :raises PipelineError: If both are set, or the file cannot be read.
    """
    inline = _opt_str(root.get(key), key)
    file_key = f'{key}_file'
    path = _opt_str(root.get(file_key), file_key)
    if path is None:
        return inline
    if inline is not None:
        raise PipelineError(f'set only one of {key} / {file_key}')
    fpath = (base_dir / path).resolve()
    try:
        return fpath.read_text(encoding='utf-8')
    except OSError as exc:
        raise PipelineError(
            f'cannot read {file_key} {path!r}'
        ) from exc


def namespaced_agent_name(pipeline_name: str, agent_name: str) -> str:
    """
    The registered agent name for a pipeline role (collision-safe).

    Namespacing keeps an inline role from upserting a shipped bundle of
    the same name (registration keys on name).

    :param pipeline_name: Sanitized pipeline name.
    :param agent_name: Author-facing agent key.
    :returns: e.g. ``'pl-mixed-models-build'``.
    """
    return f'pl-{_sanitize(pipeline_name)}-{_sanitize(agent_name)}'


def _prompt_with_context(prompt: str, context: str | None) -> str:
    """
    Bake the pipeline-wide project context into a role prompt.

    A pipeline-level ``context:`` is appended to EVERY agent's system
    prompt once, at materialize time, so shared project guidance rides
    in the stable (cacheable) system prefix rather than being re-sent in
    every turn's user message — smaller for multi-turn agents (the
    interactive planner, loop-backs), identical for single-turn ones,
    never larger. It is appended after the role prompt as a clearly
    delimited section so the agent's role identity stays primary.

    :param prompt: The agent's role prompt (template or inline).
    :param context: The pipeline ``context:`` text, or ``None``.
    :returns: The prompt, with the context section appended when set.
    """
    if not context:
        return prompt
    return (
        f'{prompt.rstrip()}\n\n'
        '--- Project context (applies to every agent in this pipeline) '
        f'---\n{context}'
    )


def _bundle_config_yaml(
    spec_name: str, agent: PipelineAgent, context: str | None = None
) -> str:
    """
    Render a minimal, valid agent ``config.yaml`` for one role.

    Model/effort are intentionally OMITTED — the runner pins them per
    session at create (native harnesses ignore a spec model). The
    pipeline-wide *context* is baked into the prompt here (see
    :func:`_prompt_with_context`).

    :param spec_name: The namespaced agent name.
    :param agent: The parsed agent.
    :param context: Pipeline-wide project context to bake into the
        prompt, or ``None``.
    :returns: YAML text.
    """
    doc = {
        'spec_version': 1,
        'name': spec_name,
        'description': (
            f'Pipeline agent {agent.name!r} '
            f'(harness {agent.harness}). Auto-materialized; do not edit.'
        ),
        'executor': {
            'type': 'omnigent',
            'config': {'harness': agent.harness},
        },
        'prompt': _prompt_with_context(agent.prompt, context),
        'os_env': {
            'type': 'caller_process',
            'cwd': '.',
            'sandbox': {'type': 'none'},
        },
    }
    return yaml.dump(
        doc,
        Dumper=_BlockDumper,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    )


def materialize_agents(
    config: PipelineConfig, dest_root: str | Path
) -> dict[str, str]:
    """
    Write each agent to a namespaced bundle dir under *dest_root*.

    Each dir gets a ``config.yaml`` (prompt + harness + os_env); when
    the agent declares ``skills:``, a copied ``skills/`` subtree. The
    server registers these dirs at startup (Stage 1 wiring), yielding
    stable ``builtin_agent_id(name)`` ids the runner binds.

    :param config: The parsed pipeline.
    :param dest_root: Directory to write bundle dirs into (created if
        absent). Existing per-agent dirs are replaced.
    :returns: ``{agent_name: namespaced_spec_name}`` for id resolution.
    :raises PipelineError: On a filesystem error writing a bundle.
    """
    root = Path(dest_root)
    root.mkdir(parents=True, exist_ok=True)
    mapping: dict[str, str] = {}
    for agent in config.agents.values():
        spec_name = namespaced_agent_name(config.name, agent.name)
        bundle_dir = root / spec_name
        try:
            if bundle_dir.exists():
                shutil.rmtree(bundle_dir)
            bundle_dir.mkdir(parents=True)
            (bundle_dir / 'config.yaml').write_text(
                _bundle_config_yaml(spec_name, agent, config.context),
                encoding='utf-8',
            )
            if agent.skills_dir is not None:
                shutil.copytree(
                    agent.skills_dir, bundle_dir / 'skills'
                )
        except OSError as exc:
            raise PipelineError(
                f'failed to materialize agent {agent.name!r}: {exc}'
            ) from exc
        mapping[agent.name] = spec_name
    return mapping
