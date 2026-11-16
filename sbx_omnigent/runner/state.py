"""The run-state record: what a run cannot rebuild from git.

Node BRANCHES survive a crash on their own — every writer's tree is
committed to the hub. What does not survive is what only ever lived in
memory: which nodes ran, an approved plan of record, a judge's
selection, how far a campaign got. :class:`RunState` holds that,
:func:`encode` writes it as the run advances, and :func:`decode` reads
it back for ``--resume``. The file itself is written and read by
:class:`sbx_omnigent.worktrees.WorktreeManager`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sbx_omnigent import harness_versions
from sbx_omnigent.pipeline import Subtask
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.parsing.dispositions import Disposition
from sbx_omnigent.runner.records.judging import JudgePick
from sbx_omnigent.runner.records.results import _COMMIT_RE, NodeResult
from sbx_omnigent.runner.records.reviews import ReviewRecord

#: Schema version of the run-state file. Bumped when the payload's
#: shape changes, so a reader can refuse state it does not understand
#: rather than resume from a misread one. A key ADDED later does not
#: bump it: its absence restores the old, empty behaviour, so a run
#: already in flight stays resumable.
RUN_STATE_VERSION = 1


@dataclass
class RunState:
    """
    Everything a run cannot rebuild from git.

    :param run_id: The run.
    :param pipeline: The pipeline's name.
    :param plan_of_record: The approved plan, or ``None``.
    :param subtasks: The campaign's modules, in order.
    :param active_subtask: The module in progress, by id. Written for a
        person reading the file; a resume does not restore it, because
        the campaign loop sets it again as it reaches each module.
    :param active_is_first: Whether that module is the first. Written
        and not restored, like *active_subtask*.
    :param completed: Stages that finished.
    :param sessions: Sessions left running, so a resume can dispose of
        their VMs. Never reattached to: a session belongs to a VM a
        later process cannot drive.
    :param decisions: ``(module, text)`` decisions for later modules.
    :param referrals: ``(module, text)`` questions referred to a human.
    :param dispositions: Triage's verdict on each finding, by id.
    :param blocking_claims: Each verified blocking claim, by id.
    :param harness_versions: ``{label: {'runs': cli, 'versions':
        {cli: version}}}``.
    :param completed_chunks: Modules already published.
    :param chunk_publishes: ``{chunk: {'seed': commit, 'pr': url}}``.
    :param reviewed_ok: Writers that cleared a review gate in this run.
    :param reviews: Every reviewer's vote and report.
    :param reader_turns: Each reader's buffered conversation, by node.
    :param picks: Each judge's choice.
    :param last_branch_node: The node whose branch publishes by default.
    :param published: Pull request URLs opened so far.
    :param nodes: Completed nodes, by id.
    """

    run_id: str
    pipeline: str
    plan_of_record: str | None = None
    subtasks: list[Subtask] = field(default_factory=list)
    active_subtask: str | None = None
    active_is_first: bool = False
    completed: set[str] = field(default_factory=set)
    sessions: list[str] = field(default_factory=list)
    decisions: list[tuple[str, str]] = field(default_factory=list)
    referrals: list[tuple[str, str]] = field(default_factory=list)
    dispositions: dict[str, Disposition] = field(default_factory=dict)
    blocking_claims: dict[str, str] = field(default_factory=dict)
    harness_versions: dict[str, dict[str, object]] = field(
        default_factory=dict
    )
    completed_chunks: set[str] = field(default_factory=set)
    chunk_publishes: dict[str, dict[str, str]] = field(default_factory=dict)
    reviewed_ok: set[str] = field(default_factory=set)
    reviews: list[ReviewRecord] = field(default_factory=list)
    reader_turns: dict[str, tuple[tuple[str, str], ...]] = field(
        default_factory=dict
    )
    picks: list[JudgePick] = field(default_factory=list)
    last_branch_node: str | None = None
    published: list[str] = field(default_factory=list)
    nodes: dict[str, NodeResult] = field(default_factory=dict)


def encode(state: RunState) -> dict:
    """
    The JSON-serializable form of *state*, as the state file holds it.

    :param state: The run's state.
    :returns: The payload, versioned with :data:`RUN_STATE_VERSION`.
    """
    return {
        'version': RUN_STATE_VERSION,
        'run_id': state.run_id,
        'pipeline': state.pipeline,
        'plan_of_record': state.plan_of_record,
        'subtasks': [{'id': s.id, 'title': s.title} for s in state.subtasks],
        'active_subtask': state.active_subtask,
        'active_is_first': state.active_is_first,
        'completed': sorted(state.completed),
        'sessions': list(state.sessions),
        'decisions': [
            {'module': module_id, 'text': text}
            for module_id, text in state.decisions
        ],
        'referrals': [
            {'module': module_id, 'text': text}
            for module_id, text in state.referrals
        ],
        'dispositions': {
            ident: {'verdict': d.verdict, 'reason': d.reason}
            for ident, d in state.dispositions.items()
        },
        'blocking_claims': dict(state.blocking_claims),
        'harness_versions': {
            label: {'runs': entry['runs'], 'versions': _copied(entry)}
            for label, entry in state.harness_versions.items()
        },
        'completed_chunks': sorted(state.completed_chunks),
        'chunk_publishes': {
            chunk: dict(entry)
            for chunk, entry in state.chunk_publishes.items()
        },
        # Which writers already cleared a review gate IN THIS RUN.
        # Held in memory it was lost on --resume, and the judge was
        # then told nothing — so it started a verification build it
        # could never finish inside one turn, stated no SELECT, and
        # the first candidate won by default.
        'reviewed_ok': sorted(state.reviewed_ok),
        'reviews': [r.as_dict() for r in state.reviews],
        # The planner's conversation is buffered in memory the moment
        # its VM is freed, and that VM is then DELETED — so this buffer
        # is the only copy. Without it in the state, a --resume past
        # the plan stage publishes with no session record at all: the
        # stage is already `completed` so it never re-runs, and the
        # session it would re-read is gone.
        'reader_turns': {
            node: [list(pair) for pair in turns]
            for node, turns in state.reader_turns.items()
        },
        'judge_picks': [p.as_dict() for p in state.picks],
        'last_branch_node': state.last_branch_node,
        'published': list(state.published),
        'nodes': {
            node_id: {
                'kind': node.kind,
                'branch': node.branch,
                'worktree': node.worktree,
                'output': node.output,
                'verdict': node.verdict,
                'selected': node.selected,
            }
            for node_id, node in state.nodes.items()
        },
    }


def _copied(entry: dict[str, object]) -> dict[object, object]:
    """:returns: A copy of a harness entry's ``versions`` mapping."""
    versions = entry['versions']
    return dict(versions) if isinstance(versions, dict) else {}


def decode(raw: dict | None, run_id: str) -> RunState:
    """
    Read a previous attempt's state back, refusing what it cannot trust.

    Malformed entries are dropped rather than trusted: a resumed run
    must not die over one bad bookkeeping entry. Only nodes recorded as
    COMPLETE are restored; an incomplete one must look like it never
    ran, so it is re-cut and re-driven rather than treated as an
    upstream that downstream stages can inherit from.

    :param raw: The state file's contents, or ``None`` when there is no
        usable file.
    :param run_id: The run being resumed, to name in a refusal.
    :returns: The restored state.
    :raises PipelineRunError: If there is no state, or it was written by
        an incompatible version — better to refuse than to resume from
        something misread.
    """
    if raw is None:
        raise PipelineRunError(
            f'cannot resume run {run_id!r}: no usable state '
            f'file. Runs started before state was recorded, or whose '
            f'run directory was removed, cannot be resumed — start a '
            f'fresh run id instead.'
        )
    version = raw.get('version')
    if version != RUN_STATE_VERSION:
        raise PipelineRunError(
            f'cannot resume run {run_id!r}: its state is '
            f'version {version!r}, this build writes '
            f'{RUN_STATE_VERSION}.'
        )
    completed = set(_strings(raw.get('completed')))
    name = raw.get('pipeline')
    active = raw.get('active_subtask')
    return RunState(
        run_id=run_id,
        pipeline=name if isinstance(name, str) else '',
        plan_of_record=raw.get('plan_of_record'),
        subtasks=_restore_subtasks(raw.get('subtasks')),
        active_subtask=active if isinstance(active, str) else None,
        active_is_first=raw.get('active_is_first') is True,
        completed=completed,
        sessions=_strings(raw.get('sessions')),
        # Absent from state written before referrals existed, which
        # restores the old (empty) behaviour rather than failing.
        decisions=_restore_module_notes(raw.get('decisions')),
        referrals=_restore_module_notes(raw.get('referrals')),
        # Absent before the gate existed. Empty restores the pre-gate
        # behaviour (everything files).
        dispositions=_restore_dispositions(raw.get('dispositions')),
        # Absent before blocking claims were verified. Empty restores
        # the old behaviour — no blocking claim files.
        blocking_claims={
            ident: text
            for ident, text in _mapping(raw.get('blocking_claims')).items()
            if isinstance(ident, str) and isinstance(text, str)
        },
        # Absent before versions were recorded (#17). Empty restores
        # "nothing recorded".
        harness_versions=_restore_harness_versions(
            raw.get('harness_versions')
        ),
        completed_chunks=set(_strings(raw.get('completed_chunks'))),
        # Absent before chunk PRs linked their own changes. Empty means
        # the next PR goes without the link.
        chunk_publishes=_restore_chunk_publishes(raw.get('chunk_publishes')),
        reviewed_ok=set(_strings(raw.get('reviewed_ok'))),
        reviews=_restore_reviews(raw.get('reviews')),
        reader_turns=_restore_reader_turns(raw.get('reader_turns')),
        picks=_restore_picks(raw.get('judge_picks')),
        last_branch_node=raw.get('last_branch_node'),
        published=_strings(raw.get('published')),
        nodes=_restore_nodes(raw.get('nodes'), completed),
    )


def _entries(raw: object) -> list[object]:
    """:returns: *raw* if it is a list, else none: the wrong type is
    dropped like a malformed entry, not iterated."""
    return raw if isinstance(raw, list) else []


def _strings(raw: object) -> list[str]:
    """
    The string entries of a list of ids or URLs.

    Every such list in the state goes through this: a string in place
    of the list would otherwise be read letter by letter, and a
    non-string entry in ``completed`` failed the resume outright.

    :param raw: The value from the state file.
    :returns: Its string entries if it is a list, else none.
    """
    return [entry for entry in _entries(raw) if isinstance(entry, str)]


def _mapping(raw: object) -> dict[object, object]:
    """:returns: *raw* if it is a mapping, else an empty one."""
    return raw if isinstance(raw, dict) else {}


def _restore_subtasks(raw: object) -> list[Subtask]:
    """:returns: The modules with both an id and a title."""
    return [
        Subtask(id=s['id'], title=s['title'])
        for s in _entries(raw)
        if isinstance(s, dict) and s.get('id') and s.get('title')
    ]


def _restore_module_notes(raw: object) -> list[tuple[str, str]]:
    """:returns: ``(module, text)`` for each entry carrying both."""
    return [
        (d['module'], d['text'])
        for d in _entries(raw)
        if isinstance(d, dict) and d.get('module') and d.get('text')
    ]


def _restore_dispositions(raw: object) -> dict[str, Disposition]:
    """:returns: Each verdict that is a mapping, by finding id."""
    return {
        ident: Disposition(
            verdict=str(entry.get('verdict') or ''),
            reason=str(entry.get('reason') or ''),
        )
        for ident, entry in _mapping(raw).items()
        if isinstance(ident, str) and isinstance(entry, dict)
    }


def _restore_reviews(raw: object) -> list[ReviewRecord]:
    """:returns: The reviews :meth:`ReviewRecord.from_dict` accepts."""
    restored = (
        ReviewRecord.from_dict(entry)
        for entry in _entries(raw)
        if isinstance(entry, dict)
    )
    return [rec for rec in restored if rec is not None]


def _restore_picks(raw: object) -> list[JudgePick]:
    """:returns: The picks :meth:`JudgePick.from_dict` accepts."""
    restored = (
        JudgePick.from_dict(entry)
        for entry in _entries(raw)
        if isinstance(entry, dict)
    )
    return [pick for pick in restored if pick is not None]


def _restore_reader_turns(
    raw: object,
) -> dict[str, tuple[tuple[str, str], ...]]:
    """:returns: Each reader's ``(role, text)`` pairs, by node."""
    return {
        node: tuple(
            (pair[0], pair[1])
            for pair in turns
            if isinstance(pair, (list, tuple)) and len(pair) == 2
        )
        for node, turns in _mapping(raw).items()
        if isinstance(node, str) and isinstance(turns, list)
    }


def _restore_nodes(raw: object, completed: set[str]) -> dict[str, NodeResult]:
    """:returns: The completed nodes whose record is a mapping."""
    return {
        node_id: NodeResult(
            node_id,
            entry.get('kind') or 'reader',
            branch=entry.get('branch'),
            worktree=entry.get('worktree'),
            output=entry.get('output') or '',
            verdict=entry.get('verdict'),
            selected=entry.get('selected'),
        )
        for node_id, entry in _mapping(raw).items()
        if isinstance(node_id, str)
        and node_id in completed
        and isinstance(entry, dict)
    }


def _restore_chunk_publishes(raw: object) -> dict[str, dict[str, str]]:
    """
    Rebuild each published chunk's record from run state.

    :param raw: The ``chunk_publishes`` value from the state file.
    :returns: ``{chunk: {'seed': commit, 'pr': url}}``. An entry that is
        not a mapping is dropped; a malformed field becomes ``''`` so
        the next chunk's PR simply goes without its link.
    """
    if not isinstance(raw, dict):
        return {}
    restored: dict[str, dict[str, str]] = {}
    for chunk, entry in raw.items():
        if not isinstance(chunk, str) or not isinstance(entry, dict):
            continue
        seed = entry.get('seed')
        pr = entry.get('pr')
        restored[chunk] = {
            'seed': seed
            if isinstance(seed, str) and _COMMIT_RE.fullmatch(seed)
            else '',
            'pr': pr if isinstance(pr, str) else '',
        }
    return restored


def _restore_harness_versions(raw: object) -> dict[str, dict[str, object]]:
    """
    Rebuild the recorded harness versions from run state.

    Anything malformed is dropped rather than trusted: a bad entry would
    otherwise be compared against, and reported as a version change
    that never happened.

    :param raw: The ``harness_versions`` value from the state file.
    :returns: ``{label: {'runs': cli, 'versions': {cli: version}}}``.
    """
    if not isinstance(raw, dict):
        return {}
    restored: dict[str, dict[str, object]] = {}
    for label, entry in raw.items():
        if not isinstance(label, str) or not isinstance(entry, dict):
            continue
        runs = entry.get('runs')
        versions = entry.get('versions')
        if runs not in harness_versions.CLIS or not isinstance(
            versions, dict
        ):
            continue
        kept = {
            cli: version
            for cli, version in versions.items()
            if cli in harness_versions.CLIS and isinstance(version, str)
        }
        if kept:
            restored[label] = {'runs': runs, 'versions': kept}
    return restored
