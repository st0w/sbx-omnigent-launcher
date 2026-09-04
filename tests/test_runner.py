"""DAG runner tests with fake session client + worktree manager.

Exercise the deterministic pipeline engine: node dispatch, isolated
writer worktrees, branch inheritance, consensus + loop-back, competing
writers + judge, publish, verdict parsing, and per-agent
model/effort/mount threading.

Run: .venv/bin/python -m unittest tests.test_runner
"""

from __future__ import annotations

import contextlib
import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import ClassVar
from unittest import mock

import click
from click.testing import CliRunner

from sbx_omnigent import pipeline
from sbx_omnigent import runner as R
from sbx_omnigent.swarm_session import SwarmSessionError, SwarmTurnResult


def _assert_plan_committed(case, wt, worktree, path, marker):
    """The plan of record reached *path*, carrying *marker*.

    Content equality was incidental to these tests: the fake now returns
    plan-SHAPED replies, because the runner refuses a reply that is not
    a design plan (TASKS.md #29).
    """
    for tree, rel, content in wt.tracked_files:
        if tree == worktree and rel == path and marker in content:
            return
    case.fail(
        f'no plan at {path} on {worktree} carrying {marker!r}; wrote '
        f'{[(t, r) for t, r, _c in wt.tracked_files]}'
    )


def _is_planner_label(label: str) -> bool:
    """Whether *label* names a planner node (``plan``, ``m1-plan``)."""
    return label == 'plan' or label.endswith('-plan')


class FakeWT:
    """Records branch-model worktree operations; returns fake paths."""

    def __init__(self) -> None:
        self.runs: list[str] = []
        #: (run, node, against) per retain call — proves the LOSER, and
        #: only the loser, was preserved.
        self.retained: list[tuple[str, str, str]] = []
        #: Nodes whose branch holds nothing beyond the base.
        self.retain_empty: set[str] = set()
        #: Nodes whose retention fails outright.
        self.retain_raises: set[str] = set()
        self.node_from: dict[str, str | None] = {}
        self.commits: list[tuple[str, str, str | None]] = []
        self.judges: dict[str, list[str]] = {}
        self.published: tuple[str, str, bool] | None = None
        self.disposed: list[str] = []
        #: (worktree_path, name, content) for each staged agent file.
        self.ignored_files: list[tuple[str, str, str]] = []
        #: (node_id, from_node) for each pre-warmed-node reseed.
        self.reseeds: list[tuple[str, str]] = []
        #: (worktree_path, relpath, content) for each tracked file.
        self.tracked_files: list[tuple[str, str, str]] = []
        #: Files a test wants to pretend are ALREADY on the branch —
        #: a ledger carried forward from an earlier run or module.
        self.branch_files: dict[str, str] = {}
        #: Issues filed through create_issue, in order.
        self.issues: list[dict] = []
        #: What issue_bodies_text returns. '' = no issues yet;
        #: None = the tracker could not be read at all.
        self.issue_bodies: str | None = ''
        self.issue_create_fails = False
        #: Worktrees the warm build cache was refreshed from.
        self.cache_refreshed: list[str] = []
        #: (alias_id, target_id) for each judge branch alias.
        self.aliases: list[tuple[str, str]] = []
        #: (node_id, remote_branch) for every publish_node call.
        self.publishes: list[tuple[str, str | None]] = []
        #: Each run-state snapshot written, in order.
        self.states: list[dict] = []
        #: relpath -> content, for run-dir artifacts (reviewer reports,
        #: captured turns).
        self.artifacts: dict[str, str] = {}
        #: Model a full disk / unwritable run dir.
        self.artifact_writes_fail = False
        #: Nodes whose worktree holds work not yet on the branch — a
        #: native-terminal writer that kept writing after its stage
        #: finished. Cleared by commit_node, as a real commit would.
        self.dirty_nodes: set[str] = set()
        #: node ids passed to wait_for_node_settle, in order.
        self.settled: list[str] = []
        #: PR title/body per publish, in order.
        self.pr_titles: list[str] = []
        self.pr_bodies: list[str] = []
        self.pr_bases: list[tuple[str | None, str | None, str | None]] = []
        #: State a resume should load; None = nothing to resume.
        self.state_to_load: dict | None = None
        #: node_id -> whether its worktree cut was a replacement.
        self.replaced: dict[str, bool] = {}
        #: Whether create_run was asked to reuse an existing hub.
        self.reused = False
        #: Make commit_node raise (salvage must not mask the failure).
        self.commit_raises = False
        #: node ids whose clones were reclaimed at a chunk's publish.
        self.reclaimed: list[str] = []
        #: node_id -> changed paths, or a list-of-lists consumed per
        #: call (so a re-drive can return a different result).
        self.diff_files: dict[str, list] = {}
        #: node -> lines the branch ADDED, for the coverage gate.
        self.added_lines: dict[str, list] = {}
        #: (node_id, against) for every diff query.
        self.diff_queries: list[tuple[str, str]] = []
        #: ordered ('settle'|'commit', node_id) events, to assert a
        #: writer's worktree is settled BEFORE it is committed.
        self.events: list[tuple[str, str]] = []

    def node_is_dirty(self, run_id, node_id) -> bool:
        return node_id in self.dirty_nodes

    def wait_for_node_settle(self, run_id, node_id, **kw) -> None:
        self.settled.append(node_id)
        self.events.append(('settle', node_id))

    def write_ignored_file(self, worktree_path, name, content) -> None:
        self.ignored_files.append((worktree_path, name, content))

    def create_run(
        self, run_id: str, repo: str, *, base_branch=None, reuse=False
    ) -> str:
        self.runs.append(run_id)
        self.reused = reuse
        return f'/wt/{run_id}/repo'

    def read_run_state(self, run_id):
        return self.state_to_load

    def create_node_worktree(
        self, run_id, node_id, *, from_node=None, base_branch=None,
        replace=False,
    ) -> str:
        self.node_from[node_id] = from_node
        self.replaced[node_id] = replace
        return f'/wt/{run_id}/nodes/{node_id}'

    def reseed_node_worktree(self, run_id, node_id, from_node) -> str:
        self.reseeds.append((node_id, from_node))
        return f'/wt/{run_id}/nodes/{node_id}'

    def refresh_build_cache(self, path) -> list[str]:
        self.cache_refreshed.append(path)
        return ['target']

    def node_worktree_path(self, run_id, node_id) -> str:
        return f'/wt/{run_id}/nodes/{node_id}'

    def write_tracked_file(self, worktree_path, relpath, content) -> None:
        self.tracked_files.append((worktree_path, relpath, content))

    def issue_bodies_text(self, repo_url, *, limit=500):
        # None means "could not read" — distinct from "no issues".
        return self.issue_bodies

    def create_issue(self, repo_url, *, title, body, label=None):
        if self.issue_create_fails:
            return None
        self.issues.append({'title': title, 'body': body, 'label': label})
        return f'https://github.com/org/proj/issues/{len(self.issues)}'

    def read_tracked_file(self, worktree_path, relpath) -> str | None:
        # Last write wins, mirroring a real filesystem — the ledger is
        # written once per publish and read back on the next one.
        for tree, rel, content in reversed(self.tracked_files):
            if (tree, rel) == (worktree_path, relpath):
                return content
        return self.branch_files.get(relpath)

    def node_branch(self, run_id, node_id) -> str:
        return f'pl/{run_id}/{node_id}'

    def node_added_lines(self, run_id, node_id, *, against):
        # A list of lists is a per-call SEQUENCE (the last repeats),
        # so a test can model a writer that removes the markers on
        # re-drive.
        val = self.added_lines.get(node_id, [])
        if val and isinstance(val[0], list):
            return val.pop(0) if len(val) > 1 else val[0]
        return val

    def node_diff_files(self, run_id, node_id, *, against):
        self.diff_queries.append((node_id, against))
        if node_id not in self.diff_files:
            return [f'src/{node_id}.py']  # default: real work happened
        val = self.diff_files[node_id]
        if val and isinstance(val[0], list):
            return val.pop(0) if len(val) > 1 else val[0]
        return val

    def commit_node(
        self, run_id, node_id, *, message, author=None, **kw
    ) -> bool:
        if self.commit_raises and 'partial work' in message:
            raise click.ClickException('nothing to commit')
        self.events.append(('commit', node_id))
        self.commits.append((node_id, message, author))
        self.dirty_nodes.discard(node_id)
        return True

    def create_judge_worktree(
        self, run_id, judge_id, candidates, *, replace=False
    ) -> str:
        self.judges[judge_id] = list(candidates)
        return f'/wt/{run_id}/nodes/{judge_id}'

    def alias_node_branch(self, run_id, alias_id, target_id) -> str:
        self.aliases.append((alias_id, target_id))
        return f'pl/{run_id}/{alias_id}'

    def publish_node(
        self, run_id, node_id, repo, *, title, body, base_branch=None,
        base_fallback=None, remote_branch=None, draft=True, open_pr=True,
    ) -> str:
        self.published = (node_id, repo, open_pr)
        #: (remote, base, fallback) per publish, in order — the stack.
        self.pr_bases.append((remote_branch, base_branch, base_fallback))
        dst = remote_branch or f'pipeline/{run_id}'
        self.publishes.append((node_id, remote_branch))
        self.pr_titles.append(title)
        self.pr_bodies.append(body)
        return f'Pushed {dst} ({node_id})'

    def metrics_path(self, run_id) -> str:
        return f'/can/_metrics/{run_id}.jsonl'

    def run_dir(self, run_id) -> str:
        return f'/wt/{run_id}'

    def retain_node_bundle(self, run_id, node_id, *, against):
        self.retained.append((run_id, node_id, against))
        if node_id in self.retain_empty:
            return None
        if node_id in self.retain_raises:
            raise click.ClickException(f'no such ref: {node_id}')
        return f'/can/_retained/{run_id}/{node_id}.bundle'

    def dispose_run(self, run_id) -> None:
        self.disposed.append(run_id)

    def dispose_node_worktrees(self, run_id, node_ids) -> int:
        # Record only: node_from is the historical record other
        # assertions read, so reclaiming must not rewrite it.
        wanted = [n for n in node_ids if n in self.node_from]
        self.reclaimed.extend(wanted)
        return len(wanted)

    def write_run_state(self, run_id, payload) -> bool:
        self.states.append(payload)
        return True

    def write_run_artifact(self, run_id, relpath, content) -> bool:
        if self.artifact_writes_fail:
            return False
        self.artifacts[relpath] = content
        return True


class FakeSC:
    """Scriptable session client. *replies* maps a stage label to a
    reply string or a list consumed per call."""

    def __init__(self, replies: dict[str, object]) -> None:
        self._replies = replies
        self.creates: list[dict] = []
        self.sent: list[tuple[str, str]] = []
        self.sent_calls: list[dict] = []
        self.disposed: list[str] = []
        #: ('create'|'dispose', label) in call order — lets a test
        #: assert a VM was freed BEFORE another was booted, which
        #: counting alone cannot show.
        self.events: list[tuple[str, str]] = []
        self.warmed: list[str] = []
        self.approvals: list[str] = []
        #: node labels already provisioned when approval began (proves
        #: writers were pre-warmed before the human approved).
        self.labels_at_approval: list[str] = []
        #: sessions passed to wait_for_session_idle, in order.
        self.idle_waits: list[str] = []
        #: Labels whose turn reports failure (a failed status).
        self.fail_labels: set[str] = set()
        #: Labels whose turn never returns at all — send_and_wait
        #: raises, as a real turn timeout does.
        self.raise_labels: set[str] = set()
        #: Labels whose item store keeps GROWING between polls — a
        #: reviewer that is still working (installing a toolchain,
        #: running a build), as opposed to one that has gone quiet.
        self.busy_labels: set[str] = set()
        self._ticks: dict[str, int] = {}
        #: Labels whose turn is interrupted by a human pressing Ctrl-C.
        #: KeyboardInterrupt is a BaseException, so an `except
        #: Exception` never sees it — which is the whole bug.
        self.interrupt_labels: set[str] = set()
        #: Labels whose TRANSCRIPT READ is interrupted: a second Ctrl-C,
        #: from someone who wants out now.
        self.interrupt_reads: set[str] = set()
        #: Ctrl-C while the human is answering the planner.
        self.interrupt_approval = False
        self.timeout_approval = False
        #: host_id per session, and host_id -> sandbox name. Empty by
        #: default so the pane path stays OFF unless a test opts in —
        #: the point being that it must never affect anything else.
        self.host_ids: dict[str, str] = {}
        self.host_names: dict[str, str] = {}
        #: label -> the session snapshot get_status returns. Used to say
        #: a runner went offline, or that last_task_error was set.
        self.status_for_label: dict[str, dict] = {}
        #: Host every session resolves to unless host_ids says otherwise
        #: — session ids are assigned by this fake, so a test that only
        #: cares "there IS a VM" should not have to guess them.
        self.default_host_id: str | None = None
        #: Set to return planner replies verbatim, for tests that drive
        #: the plan-shape guard itself.
        self.raw_plan_replies = False
        #: Session ids whose dispose() raises (already torn down).
        self.dispose_raises: set[str] = set()
        #: Every dispose ATTEMPT, including the ones that raised —
        #: `disposed` records only successes, so a retry is invisible
        #: without this.
        self.dispose_attempts: list[str] = []
        #: label -> the SETTLED latest reply read_latest_reply returns,
        #: modelling an agy reply that differs from the streamed one.
        self.settled_for_label: dict[str, str] = {}
        #: label -> a per-read sequence for read_recent_reply_text (the
        #: last value repeats), modelling a verdict that only lands on a
        #: LATER settled poll — the reviewer-verdict deadline loop must
        #: keep polling past the first read to catch it.
        self.settled_sequence: dict[str, list[str]] = {}
        self._last_reply: dict[str, str] = {}
        #: Assistant messages per session, oldest first (what
        #: read_assistant_replies returns).
        self._assistant: dict[str, list[str]] = {}
        #: label -> a FULL scripted conversation, for messages the
        #: runner never drove (e.g. a planner answering the human).
        self.conversation_for_label: dict[str, list[str]] = {}
        #: label -> [(role, text)] read_transcript returns.
        self.transcript_for_label: dict[str, list[tuple[str, str]]] = {}
        self._label: dict[str, str] = {}

    def wait_for_terminal_ready(self, session, **kw) -> bool:
        self.warmed.append(session)
        return True

    def wait_for_plan_approval(self, session, **kw) -> str:
        # Non-blocking fake: record the call and return the planner's
        # last scripted reply as the "approved" plan.
        if self.interrupt_approval:
            raise KeyboardInterrupt
        if self.timeout_approval:
            raise SwarmSessionError(
                'plan approval for conv_1: the session has been '
                'silent for 3600s'
            )
        self.approvals.append(session)
        self.labels_at_approval = [
            c['title'].split('/', 1)[-1] for c in self.creates
        ]
        return self._last_reply.get(session, '')

    def wait_for_session_idle(self, session, **kw) -> bool:
        self.idle_waits.append(session)
        return True

    def read_transcript(self, session, *, tail=400):
        if self._label.get(session, '') in self.interrupt_reads:
            raise KeyboardInterrupt
        if session in self.disposed:
            # dispose() DELETEs the session: its transcript is gone.
            # Modelled so a test can prove capture happened first.
            return []
        label = self._label.get(session, '')
        if label in self.transcript_for_label:
            return list(self.transcript_for_label[label])
        # Default: what this session actually said, as the planner.
        return [('assistant', r) for r in self._assistant.get(session, [])]

    def read_assistant_replies(self, session, *, tail=60) -> list[str]:
        label = self._label.get(session, '')
        if label in self.conversation_for_label:
            return [
                self._shaped(label, t)
                for t in self.conversation_for_label[label]
            ]
        out = list(self._assistant.get(session, []))
        settled = self.settled_for_label.get(label)
        if settled is not None:
            out.append(self._shaped(label, settled))
        return out

    def read_latest_reply(self, session) -> str:
        # A configured settled reply models agy's final (lagging)
        # message differing from the turn's streamed reply.
        label = self._label.get(session, '')
        if label in self.settled_for_label:
            return self._shaped(label, self.settled_for_label[label])
        return self._last_reply.get(session, '')

    def _shaped(self, label: str, text: str) -> str:
        """Plan shape around a planner's reply; see send_and_wait."""
        if _is_planner_label(label) and not self.raw_plan_replies:
            return _plan_text(2200, text)
        return text

    def read_items(self, session, *, tail=None):
        # Models the activity marker _newest_item_id reads. A label in
        # `busy_labels` gets a NEW id on every read, i.e. it is still
        # producing output — a reviewer mid-build, which must not be
        # mistaken for a silent one.
        label = self._label.get(session, '')
        if label in self.busy_labels:
            self._ticks[label] = self._ticks.get(label, 0) + 1
            return [{'id': f'{label}-{self._ticks[label]}'}]
        return [{'id': f'{label}-static'}]

    def read_recent_reply_text(self, session, **kw) -> str:
        # A per-read sequence (last value repeats) models a verdict that
        # only appears on a later settled poll; else the fixed settled
        # reply (agy's lagging final message).
        label = self._label.get(session, '')
        seq = self.settled_sequence.get(label)
        if seq:
            return seq.pop(0) if len(seq) > 1 else seq[0]
        return self.read_latest_reply(session)

    def create(
        self, *, agent_id, workspace=None, title=None,
        terminal_launch_args=None, model_override=None,
        reasoning_effort=None, parent_session_id=None,
    ) -> str:
        sid = f'sess-{len(self.creates)}'
        self.creates.append(
            {
                'sid': sid,
                'agent_id': agent_id,
                'workspace': workspace,
                'title': title,
                'launch': terminal_launch_args,
                'model': model_override,
                'effort': reasoning_effort,
            }
        )
        self._label[sid] = (title or '').split('/', 1)[-1]
        self.events.append(('create', self._label[sid]))
        return sid

    def send_and_wait(self, session, message, *, timeout=1800.0, **kw):
        self.sent.append((session, message))
        if self._label.get(session, '') in self.interrupt_labels:
            raise KeyboardInterrupt
        if self._label.get(session, '') in self.raise_labels:
            raise SwarmSessionError(
                f'turn on {session} did not complete within 3600s'
            )
        if self._label.get(session, '') in self.fail_labels:
            return SwarmTurnResult('failed', None, '')
        self.sent_calls.append({'session': session, 'label':
                                self._label.get(session, ''), **kw})
        label = self._label.get(session, '')
        val = self._replies.get(label, 'VERDICT: APPROVED')
        if isinstance(val, list):
            reply = val.pop(0) if val else 'VERDICT: APPROVED'
        else:
            reply = val
        # A PLANNER's reply is wrapped in plan shape, keeping the
        # fixture's marker inside it. The runner refuses a reply that is
        # not a design plan (TASKS.md #29), and a real planner never
        # answers 'P' — so a fixture that did would only be testing the
        # refusal. Tests of the guard itself pass real text directly.
        if _is_planner_label(label) and not self.raw_plan_replies:
            reply = _plan_text(2200, reply)
        self._last_reply[session] = reply
        self._assistant.setdefault(session, []).append(reply)
        return SwarmTurnResult('idle', None, reply)

    def get_status(self, session) -> dict:
        # Per-label overrides let a test say "this session's runner went
        # away" without faking the whole snapshot.
        label = self._label.get(session, '')
        return dict(self.status_for_label.get(label, {}))

    def session_host_id(self, session):
        return self.host_ids.get(session, self.default_host_id)

    def host_name(self, host_id):
        return self.host_names.get(host_id)

    def dispose(self, session) -> None:
        self.dispose_attempts.append(session)
        if session in self.dispose_raises:
            raise SwarmSessionError(f'gone: {session}')
        self.disposed.append(session)
        self.events.append(('dispose', self._label.get(session, '')))

    def label_of(self, sid: str) -> str:
        return self._label.get(sid, '')

    def message_for_label(self, label: str) -> str:
        sid = next(
            (s for s, lb in self._label.items() if lb == label), None
        )
        for sess, msg in self.sent:
            if sess == sid:
                return msg
        return ''


def _staged_file(
    wt: FakeWT, node_id: str, name: str = R._AGY_TASK_FILE
) -> str:
    """Content agy was handed as a file for *node_id*."""
    for path, fname, content in wt.ignored_files:
        if path.endswith(f'/nodes/{node_id}') and fname == name:
            return content
    return ''


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='rn-'))
        # Never touch the host's real harvester lock: a harvester
        # running on this machine would otherwise make a CLI test block
        # for the full first-refresh deadline.
        patcher = mock.patch.object(
            R.agy, 'HARVEST_LOCK', self.root / 'agy-harvest.lock'
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        # No test may spend the reviewer-retry backoff. It exists to
        # outlast a provider outage in production, which is minutes;
        # four tests drive a reviewer that dies on purpose, and paying
        # it would add six minutes to the suite for nothing.
        backoff = mock.patch.object(R, '_REVIEW_RETRY_BACKOFF_S', 0.0)
        backoff.start()
        self.addCleanup(backoff.stop)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _cfg(self, text: str) -> pipeline.PipelineConfig:
        p = self.root / 'pipeline.yaml'
        p.write_text(text, encoding='utf-8')
        return pipeline.load_pipeline(p)

    def _run(
        self, text, replies, *, settled=None, settled_seq=None,
        conversation=None, transcript=None, wt=None, sc=None, **kw,
    ):
        cfg = self._cfg(text)
        wt = wt if wt is not None else FakeWT()
        sc = sc if sc is not None else FakeSC(dict(replies))
        if settled:
            sc.settled_for_label.update(settled)
        if settled_seq:
            sc.settled_sequence.update(
                {k: list(v) for k, v in settled_seq.items()}
            )
        if conversation:
            sc.conversation_for_label.update(
                {k: list(v) for k, v in conversation.items()}
            )
        if transcript:
            sc.transcript_for_label.update(
                {k: list(v) for k, v in transcript.items()}
            )
        ids = {name: f'ag-{name}' for name in cfg.agents}
        # Deterministic: never consult this host's real harvest stamp.
        kw.setdefault('swap_age_s', lambda: 0.0)
        runner = R.PipelineRunner(
            cfg,
            session_client=sc,
            worktree_manager=wt,
            run_id='r1',
            agent_ids=ids,
            **kw,
        )
        return runner.run(), sc, wt


_LINEAR = """\
name: demo
repo: ./proj
publish: local
task: |
  add parse_ports
acceptance: |
  handles ranges
agents:
  plan:
    template: planner
    harness: antigravity-native
    model: gemini-3.5-flash
  build:
    template: coder
    model: claude-sonnet-5
    effort: medium
  sec:
    template: security-reviewer
    model: claude-fable-5
stages:
  - {id: plan, run: plan}
  - {id: build, run: build, write: true, needs: [plan]}
  - id: review
    run: [sec]
    needs: [build]
    gate: consensus
    on_block: build
"""


class TestLinear(_Base):
    def test_completes_and_publishes(self) -> None:
        result, _sc, wt = self._run(
            _LINEAR,
            {
                'plan': 'PLAN TEXT',
                'build': 'implemented',
                'review-sec': 'VERDICT: APPROVED',
            },
        )
        self.assertEqual(result.status, 'completed')
        # build committed (implement) + the plan of record; run
        # created + disposed.
        self.assertIn('r1', wt.runs)
        self.assertIn('r1', wt.disposed)
        msgs = [m for _, m, _ in wt.commits]
        self.assertIn('build: implement', msgs)
        self.assertIn('docs: add plan of record', msgs)
        self.assertEqual(wt.commits[0][0], 'build')
        # published the build branch, local mode (open_pr False).
        self.assertEqual(wt.published, ('build', './proj', False))

    def test_per_agent_model_effort_and_mounts(self) -> None:
        _, sc, _ = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
        )
        by_label = {c['title'].split('/', 1)[-1]: c for c in sc.creates}
        self.assertEqual(by_label['build']['model'], 'claude-sonnet-5')
        self.assertEqual(by_label['build']['effort'], 'medium')
        self.assertEqual(by_label['review-sec']['model'], 'claude-fable-5')
        self.assertEqual(by_label['plan']['model'], 'gemini-3.5-flash')
        # writer rw, reader/reviewer ro.
        self.assertIn('#rw', by_label['build']['workspace'])
        self.assertIn('#ro', by_label['plan']['workspace'])
        self.assertIn('#ro', by_label['review-sec']['workspace'])
        # agy planner gets the agy launch flag + mount tag.
        self.assertEqual(
            by_label['plan']['launch'], ['--dangerously-skip-permissions']
        )
        self.assertTrue(by_label['plan']['workspace'].endswith('-agy'))
        self.assertEqual(
            by_label['build']['launch'],
            ['--permission-mode', 'bypassPermissions'],
        )

    def test_planner_told_to_plan_not_implement(self) -> None:
        # The planner must be framed as producing a DESIGN PLAN, never
        # handed the raw "Task: add <feature>" implementation directive
        # (which makes it try to implement / hunt for a writable path).
        # The agy planner is delivered as a file it reads, so the
        # framing lives in that file and the pasted turn is a pointer.
        _, sc, wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
        )
        self.assertIn(R._AGY_TASK_FILE, sc.message_for_label('plan'))
        plan_file = _staged_file(wt, 'plan')
        self.assertIn('DESIGN PLAN', plan_file)
        self.assertIn('do NOT write code', plan_file)
        self.assertNotIn('Task:\nadd parse_ports', plan_file)
        # The coder (Claude), by contrast, still gets the task inline.
        build_msg = sc.message_for_label('build')
        self.assertIn('Task:', build_msg)
        self.assertNotIn('DESIGN PLAN', build_msg)

    def test_reviewer_mounts_writer_worktree(self) -> None:
        _, sc, _ = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
        )
        by_label = {c['title'].split('/', 1)[-1]: c for c in sc.creates}
        build_wt = by_label['build']['workspace'].split('#')[0]
        sec_wt = by_label['review-sec']['workspace'].split('#')[0]
        self.assertEqual(build_wt, sec_wt)  # same tree, different mode

    def test_only_agy_terminals_are_warmed_at_create(self) -> None:
        # An agy terminal auto-creates with the session, so it can be
        # awaited. A Claude terminal is created lazily at first-message
        # time — there is nothing to wait on, hence the retry net below.
        _, sc, _ = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
        )
        by_label = {c['title'].split('/', 1)[-1]: c for c in sc.creates}
        self.assertEqual(sc.warmed, [by_label['plan']['sid']])

    def test_every_first_turn_gets_the_redelivery_net(self) -> None:
        # THE live regression: a cold Claude Code terminal missed the
        # executor's 30s ready window ("input prompt never rendered"),
        # the turn failed before starting, and with no retry the whole
        # run died on a boot race. The net is not agy-specific.
        _, sc, _ = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
        )
        first: dict[str, dict] = {}
        for c in sc.sent_calls:
            first.setdefault(c['label'], c)
        for label in ('plan', 'build', 'review-sec'):
            self.assertEqual(
                first[label]['max_resubmits'],
                R._FIRST_TURN_MAX_RESUBMITS,
                f'{label} first turn must be re-deliverable',
            )

    def test_later_turns_are_not_redelivered(self) -> None:
        # Only the FIRST turn on a session is a plausible dropped
        # submit; a later one failing is a real failure. The planner is
        # driven twice (initial + consolidation), so it proves this.
        _, sc, _ = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
        )
        plan_calls = [c for c in sc.sent_calls if c['label'] == 'plan']
        self.assertGreaterEqual(len(plan_calls), 2)
        self.assertEqual(plan_calls[1]['max_resubmits'], 0)
        self.assertEqual(plan_calls[1]['redeliver_delay_s'], 0.0)

    def test_agy_turn_delivered_as_file_claude_inline(self) -> None:
        # An agy turn is handed over as a file it reads (a multi-line
        # or long paste is dropped/collapsed by the agy TUI); only a
        # tiny pointer is pasted. Claude turns keep full inline text.
        _, sc, wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
        )
        agy_msg = sc.message_for_label('plan')  # agy planner: the pointer
        self.assertNotIn('\n', agy_msg)  # pointer is a single line
        self.assertIn(R._AGY_TASK_FILE, agy_msg)  # points at the file
        self.assertNotIn('parse_ports', agy_msg)  # content is NOT pasted
        self.assertIn('parse_ports', _staged_file(wt, 'plan'))  # it is filed
        claude_msg = sc.message_for_label('build')  # claude coder
        self.assertIn('\n', claude_msg)  # task block newlines intact
        self.assertIn('parse_ports', claude_msg)  # full content inline

    def test_agy_turns_staged_as_ignored_files_agy_node_only(self) -> None:
        # Every agy turn (planner turn + the post-approval consolidation
        # turn) is delivered as a git-ignored task file, and ONLY on the
        # agy node (plan); Claude nodes are delivered inline (no file).
        _, _sc, wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
        )
        staged = [(p, n) for p, n, _ in wt.ignored_files]
        self.assertTrue(staged)
        self.assertTrue(
            all(
                p == '/wt/r1/nodes/plan' and n == R._AGY_TASK_FILE
                for p, n in staged
            )
        )


class TestParseDisputes(unittest.TestCase):
    """
    Telling "my contract is impossible" from the prompt asking for it.

    Every role prompt ends "label it DISPUTED, and stop", so the bare
    word is in nearly every report. Treating that as a dispute would
    halt almost every run; measured on four campaigns, both that
    SHIPPED contained the echo and no real claim.
    """

    def test_the_prompt_echo_is_not_a_dispute(self) -> None:
        # The exact phrase every role prompt ends with. If this reads as
        # a dispute, the runner halts on every review it ever sees.
        echo = (
            'If it truly cannot be resolved that way, say so in your '
            'reply, label it DISPUTED, and stop. Your reply is the only '
            'channel that reaches a human.'
        )
        self.assertEqual(R.parse_disputes(echo), ())

        # The same phrase WRAPPED so it begins a line. Only the required
        # `:`/dash separator rejects this one — the start-of-line anchor
        # does not — and reports do quote the prompt back at any width.
        wrapped = (
            'If it truly cannot be resolved that way, say so and\n'
            'DISPUTED, and stop. Your reply is the only channel.\n'
        )
        self.assertEqual(R.parse_disputes(wrapped), ())

    def test_a_claim_after_the_marker_is_a_dispute(self) -> None:
        got = R.parse_disputes(
            'DISPUTED: the frozen mapping test conflicts with the '
            'vocabulary, so this cannot be approved.'
        )
        self.assertEqual(len(got), 1)
        self.assertTrue(got[0].startswith('the frozen mapping test'))

    def test_the_marker_may_be_decorated_like_a_finding(self) -> None:
        for text in (
            '**DISPUTED — the frozen test is internally inconsistent.**',
            '1. **DISPUTED — the suite is unsatisfiable.** Details follow.',
            '- DISPUTED: stages disagree on a contract.',
            'DISPUTED - no catalog content can satisfy it.',
        ):
            with self.subTest(text=text[:34]):
                self.assertEqual(len(R.parse_disputes(text)), 1)

    def test_a_report_with_no_marker_yields_nothing(self) -> None:
        for text in ('', None, 'VERDICT: BLOCKING', 'nothing disputed'):
            with self.subTest(text=text):
                self.assertEqual(R.parse_disputes(text), ())

    def test_every_dispute_in_a_multi_finding_report_is_returned(
        self,
    ) -> None:
        got = R.parse_disputes(
            'DISPUTED: the first assertion cannot hold.\n'
            'Some prose in between.\n'
            'DISPUTED — the second one cannot either.\n'
        )
        self.assertEqual(len(got), 2)


_TWO_REVIEWERS = """\
name: pair
repo: ./proj
task: |
  build it
acceptance: |
  it works
agents:
  build: {template: coder, model: claude-sonnet-5}
  sec: {template: security-reviewer, model: claude-sonnet-5}
  bugs: {template: bug-reviewer, model: claude-sonnet-5}
stages:
  - {id: build, run: build, write: true}
  - id: review
    run: [sec, bugs]
    needs: [build]
    gate: consensus
    on_block: build
"""


class TestCoverageCannotBeSuppressed(_Base):
    """
    A writer may not buy the coverage gate with exclusion markers.

    The gate asks "is this code covered". It cannot tell coverage that
    was EARNED from coverage that was SUPPRESSED, so a writer short of
    the threshold can add `# pragma: no cover` until the number moves
    and ship untested code through a green gate.

    Observed live on `ingestion-m2-5`: one candidate sat at 90.87%
    against a 95% floor with the suite frozen, added 28 exclusions, and
    reported 95.03%. Among the excluded lines were the rule-version tie
    checks, source discovery, and `if checkpoint is None` — the first
    poll of every source. Its sibling reached 95.07% with zero.

    Re-driven once rather than halted, like the tests-only gate: the
    writer can undo this itself, and an unattended run should not stop
    for something it can fix.
    """

    def _wt_adding(self, lines):
        wt = FakeWT()
        wt.added_lines['build'] = lines
        return wt

    def test_an_added_pragma_re_drives_the_writer(self) -> None:
        # Present on the first look, gone after the re-drive: the run
        # continues rather than dying on something the writer can undo.
        wt = FakeWT()
        wt.added_lines['build'] = [
            ['    return x  # pragma: no cover'],
            [],
        ]
        sc = FakeSC(dict(_LINEAR_REPLIES))
        result, _sc, _wt = self._run(_LINEAR, {}, wt=wt, sc=sc)

        self.assertEqual(result.status, 'completed')
        self.assertTrue(
            any('coverage' in m.lower() for _s, m in sc.sent),
            'the writer was never told to remove the suppression',
        )

    def test_it_is_fatal_when_the_writer_keeps_them(self) -> None:
        wt = self._wt_adding(['    return x  # pragma: no cover'])
        with self.assertRaises(R.PipelineRunError) as caught:
            self._run(_LINEAR, dict(_LINEAR_REPLIES), wt=wt)
        self.assertIn('pragma: no cover', str(caught.exception))

    def test_other_ecosystems_are_caught_too(self) -> None:
        # The launcher runs Rust and JS projects, and each has its own
        # spelling of the same escape hatch.
        for marker in (
            '/* istanbul ignore next */',
            '#[cfg(not(tarpaulin_include))]',
            '// LCOV_EXCL_START',
        ):
            with self.subTest(marker=marker):
                wt = self._wt_adding([marker])
                with self.assertRaises(R.PipelineRunError):
                    self._run(_LINEAR, dict(_LINEAR_REPLIES), wt=wt)

    def test_a_writer_adding_none_is_untouched(self) -> None:
        wt = self._wt_adding(['    return x + 1'])
        result, _sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES), wt=wt)
        self.assertEqual(result.status, 'completed')

    def test_a_pre_existing_marker_is_not_the_writer_s_fault(self) -> None:
        # Only ADDED lines are inspected. A marker already in the base
        # tree is someone else's decision and must not fail this stage.
        wt = FakeWT()
        wt.added_lines['build'] = []
        result, _sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES), wt=wt)
        self.assertEqual(result.status, 'completed')


class TestAWriterDisputeHaltsTheRun(_Base):
    """
    A WRITER saying its contract is impossible stops the run too.

    The review path already halts on a blocking reviewer's dispute, but
    a writer meets an impossible contract FIRST — it is the party that
    has to satisfy it. Reading that dispute and carrying on spends the
    rest of the run discovering what it already said.

    Observed on `ingestion-m2-3`: an implementer reported three
    unsatisfiable tests with file:line — a fixture value one test
    required persisted and another forbade, a SQL alias on the reserved
    word `constraint` that cannot parse, and a frozen-API call with the
    wrong signature. All three were correct. The run continued 51 more
    minutes, reviewed both candidates and forfeited; the sibling had
    made the same broken test pass by EDITING A FROZEN MODULE.
    """

    def test_a_writer_dispute_halts_the_run(self) -> None:
        with self.assertRaises(R.PipelineRunError):
            self._run(
                _LINEAR,
                {
                    'plan': 'P',
                    'build': (
                        'DISPUTED: three frozen tests cannot be satisfied '
                        'without prohibited edits.'
                    ),
                },
            )

    def test_the_halt_names_the_claim_and_the_stage(self) -> None:
        # A halt nobody can act on is a different kind of waste.
        with self.assertRaises(R.PipelineRunError) as caught:
            self._run(
                _LINEAR,
                {
                    'plan': 'P',
                    'build': (
                        'DISPUTED: the fixture value is required by one '
                        'test and forbidden by another.'
                    ),
                },
            )

        message = str(caught.exception)
        self.assertIn('required by one test and forbidden', message)
        self.assertIn('build', message)

    def test_the_disputed_work_is_committed_before_the_halt(self) -> None:
        # The dispute is raised BESIDE real work -- the implementer that
        # found this had 2,013 insertions on its branch. Halting before
        # the commit would throw that away; a resume rebuilds it.
        wt = FakeWT()
        with self.assertRaises(R.PipelineRunError):
            self._run(
                _LINEAR,
                {'plan': 'P', 'build': 'DISPUTED: the contract is broken.'},
                wt=wt,
            )
        self.assertIn('build', [node for node, _m, _a in wt.commits])

    def test_the_disputing_writer_s_reply_is_captured(self) -> None:
        # The claim after the marker is ONE LINE; the reasoning that
        # makes it actionable is the rest of the reply. Halting
        # without capturing destroys it -- the same loss
        # `_await_plan_approval` captures for. Live on
        # `ingestion-m2-5`: the halt fired correctly, the human got
        # "m2 cannot be marked complete under the binding
        # constraints", and every word of why went with the session.
        wt = FakeWT()
        sc = FakeSC({
            'plan': 'P',
            'build': 'DISPUTED: the contract cannot be satisfied.',
        })
        sc.transcript_for_label['build'] = [
            ('assistant', 'DISPUTED: the contract cannot be satisfied.'),
            ('assistant', 'Specifically, test X asserts A while test Y '
                          'asserts not-A over the same fixture.'),
        ]
        with self.assertRaises(R.PipelineRunError):
            self._run(_LINEAR, {}, wt=wt, sc=sc)

        doc = wt.artifacts['turns/build.md']
        self.assertIn('test X asserts A while test Y', doc)
        self.assertIn('dispute', doc.lower())

    def test_a_writer_with_no_dispute_is_untouched(self) -> None:
        # The common case must not move: an ordinary reply runs on.
        result, _sc, _wt = self._run(
            _LINEAR, dict(_LINEAR_REPLIES)
        )
        self.assertEqual(result.status, 'completed')

    def test_a_disputing_tests_stage_halts_rather_than_re_driving(self):
        # _enforce_tests_only re-drives a strayed tests stage, and
        # _require_implementation re-drives an empty one. Neither can
        # answer "this contract is impossible", so the dispute is read
        # first -- otherwise the run spends its retries on the wrong
        # question.
        with self.assertRaises(R.PipelineRunError) as caught:
            self._run(
                _TDD_FULL,
                {
                    'plan': 'P',
                    'tests': (
                        'DISPUTED: the contract names two catalogs that '
                        'cannot both hold.'
                    ),
                    'build': 'b',
                },
            )
        self.assertIn('cannot both hold', str(caught.exception))


class TestADisputeHaltsInsteadOfReDriving(_Base):
    """
    A stage saying its CONTRACT is impossible stops the run.

    BLOCKING means the code is wrong and the writer can fix it. A
    dispute means the contract is wrong, and the writer is not the
    party that can change it — so relaying it burns the whole review
    budget. Two [m2]
    campaigns raised 9 and 10 correct disputes, ignored every one, and
    forfeited every candidate.
    """

    def test_a_blocking_dispute_halts_the_run(self) -> None:
        # Fatal, like the judge's no-decision halt: the alternative
        # is spending the rounds relaying an unanswerable finding.
        with self.assertRaises(R.PipelineRunError):
            self._run(
                _LINEAR,
                {
                    'plan': 'P',
                    'build': 'implemented',
                    'review-sec': (
                        'DISPUTED: the frozen test cannot be satisfied by '
                        'any implementation.\n\nVERDICT: BLOCKING'
                    ),
                },
            )

    def test_the_halt_names_the_disputed_claim(self) -> None:
        # A halt a human cannot act on is a different kind of waste.
        with self.assertRaises(R.PipelineRunError) as caught:
            self._run(
                _LINEAR,
                {
                    'plan': 'P',
                    'build': 'implemented',
                    'review-sec': (
                        'DISPUTED: no catalog content satisfies the '
                        'divergent-set assertion.\n\nVERDICT: BLOCKING'
                    ),
                },
            )

        message = str(caught.exception)
        self.assertIn('no catalog content satisfies', message)
        self.assertIn('[sec]', message)          # who raised it
        self.assertIn('review', message)         # which stage
        self.assertIn('build', message)          # who could NOT fix it

    def test_a_plain_block_still_re_drives_the_writer(self) -> None:
        # The common case must be untouched: a BLOCKING verdict with no
        # dispute is answered by the writer, exactly as before.
        result, _sc, wt = self._run(
            _LINEAR,
            {
                'plan': 'P',
                'build': ['implemented', 'fixed'],
                'review-sec': ['VERDICT: BLOCKING', 'VERDICT: APPROVED'],
            },
        )

        self.assertEqual(result.status, 'completed')
        kinds = [m for _, m, _ in wt.commits]
        self.assertEqual(kinds.count('build: address review'), 1)

    def test_only_a_BLOCKING_reviewer_s_dispute_halts(self) -> None:
        """One approves-with-dispute, the other blocks without one.

        The blocking finding is answerable by the writer, so the run
        should re-drive and continue. Counting the approving reviewer's
        dispute would halt a run that is not stuck — which is the
        difference between a useful halt and an annoying one.
        """
        result, _sc, _wt = self._run(
            _TWO_REVIEWERS,
            {
                'build': ['implemented', 'fixed'],
                'review-sec': (
                    'DISPUTED: I would have shaped the contract '
                    'differently.\n\nVERDICT: APPROVED'
                ),
                'review-bugs': ['VERDICT: BLOCKING', 'VERDICT: APPROVED'],
            },
        )

        self.assertEqual(result.status, 'completed')

    def test_a_dispute_beside_an_APPROVAL_does_not_halt(self) -> None:
        # The run is not stuck, so the dispute is an observation.
        # Halting on it would stop runs about to succeed.
        result, _sc, _wt = self._run(
            _LINEAR,
            {
                'plan': 'P',
                'build': 'implemented',
                'review-sec': (
                    'DISPUTED: I would have shaped this contract '
                    'differently.\n\nVERDICT: APPROVED'
                ),
            },
        )

        self.assertEqual(result.status, 'completed')


class TestLoopback(_Base):
    def test_block_then_approve(self) -> None:
        result, _sc, wt = self._run(
            _LINEAR,
            {
                'plan': 'P',
                'build': ['implemented', 'fixed'],
                'review-sec': ['VERDICT: BLOCKING', 'VERDICT: APPROVED'],
            },
        )
        self.assertEqual(result.status, 'completed')
        # build committed twice (implement + address review), plus the
        # plan of record committed onto the branch at publish.
        kinds = [m for _, m, _ in wt.commits]
        self.assertEqual(kinds.count('build: implement'), 1)
        self.assertEqual(kinds.count('build: address review'), 1)
        self.assertEqual(kinds.count('docs: add plan of record'), 1)
        self.assertEqual(wt.published[0], 'build')

    def test_blocked_after_max_rounds(self) -> None:
        result, _sc, wt = self._run(
            _LINEAR,
            {
                'plan': 'P',
                'build': 'implemented',
                'review-sec': 'VERDICT: BLOCKING',
            },
            max_review_rounds=2,
        )
        self.assertEqual(result.status, 'blocked')
        self.assertEqual(result.blocked_stage, 'review')
        self.assertIsNone(wt.published)
        # 1 initial + 2 redrive commits before giving up.
        self.assertEqual(len(wt.commits), 3)


_AGY_REVIEW = """\
name: agyrev
repo: ./proj
publish: local
task: |
  add parse_ports
acceptance: |
  handles ranges
agents:
  build:
    template: coder
    model: claude-sonnet-5
  bugs:
    template: bug-reviewer
    harness: antigravity-native
    model: gemini-3.5-flash
stages:
  - {id: build, run: build, write: true}
  - id: review
    run: [bugs]
    needs: [build]
    gate: consensus
    on_block: build
"""


class TestReviewVerdict(_Base):
    def test_agy_review_verdict_from_settled_session(self) -> None:
        # An agy reviewer streams an opening narration (no verdict) and
        # emits its VERDICT a beat later in the SETTLED session. The
        # gate must read that settled verdict, not treat the narration
        # as blocking (which spuriously loops back + leaks VMs).
        result, sc, wt = self._run(
            _AGY_REVIEW,
            {'build': 'impl',
             'review-bugs': 'I will check my permission grants first.'},
            settled={'review-bugs': 'Looks correct.\nVERDICT: APPROVED'},
        )
        self.assertEqual(result.status, 'completed')
        # No spurious loop-back: build committed once, no fix commit.
        kinds = [m for _, m, _ in wt.commits]
        self.assertNotIn('build: address review', kinds)
        # The reviewer session was settled before its verdict was read.
        bugs_sid = next(
            s for s, lb in sc._label.items() if lb == 'review-bugs'
        )
        self.assertIn(bugs_sid, sc.idle_waits)

    def test_claude_review_verdict_from_stream_no_settle(self) -> None:
        # A Claude reviewer's streamed reply already carries the
        # verdict, so it is used directly — no settle-wait on it.
        _result, sc, _ = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
        )
        sec_sid = next(
            s for s, lb in sc._label.items() if lb == 'review-sec'
        )
        self.assertNotIn(sec_sid, sc.idle_waits)

    def test_claude_review_verdict_recovered_from_settled(self) -> None:
        # The mm2 gap: a Claude reviewer's streamed reply can be an
        # intermediate tool-narration message with NO verdict (its
        # VERDICT lands in a later message a mid-turn quiescence idle
        # mis-captured past). The gate must recover the verdict from the
        # SETTLED session — same as for agy — not spuriously loop back.
        result, sc, wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b',
             'review-sec': "I'll review net.py. Let me check the stub."},
            settled={'review-sec': 'All cases pass.\nVERDICT: APPROVED'},
        )
        self.assertEqual(result.status, 'completed')
        self.assertNotIn(
            'build: address review', [m for _, m, _ in wt.commits]
        )
        sec_sid = next(
            s for s, lb in sc._label.items() if lb == 'review-sec'
        )
        self.assertIn(sec_sid, sc.idle_waits)  # settle-polled despite Claude

    def test_agy_verdict_appears_after_polls(self) -> None:
        # The verdict lands only on a LATER settled poll (agy's reply
        # lags well past the first read). The deadline loop must keep
        # polling past the first read to catch it — not give up early.
        with mock.patch('sbx_omnigent.runner.time.sleep'):
            result, sc, wt = self._run(
                _AGY_REVIEW,
                {'build': 'impl', 'review-bugs': 'narrating...'},
                settled_seq={'review-bugs': [
                    'still working...',
                    'reading files...',
                    'All good.\nVERDICT: APPROVED',
                ]},
            )
        self.assertEqual(result.status, 'completed')
        self.assertNotIn(
            'build: address review', [m for _, m, _ in wt.commits]
        )
        # it kept polling the settled store past the first read.
        bugs_sid = next(
            s for s, lb in sc._label.items() if lb == 'review-bugs'
        )
        self.assertEqual(sc.settled_sequence['review-bugs'], [
            'All good.\nVERDICT: APPROVED'
        ])  # sequence consumed down to the verdict
        self.assertIn(bugs_sid, sc.idle_waits)

    def test_agy_review_no_verdict_blocks(self) -> None:
        # If neither the streamed nor the settled reply ever carries a
        # verdict, the reviewer is treated as blocking (safe) after the
        # poll deadline. Patch the deadline to 0 so the loop makes one
        # read then breaks instead of polling for real minutes.
        with mock.patch(
            'sbx_omnigent.runner._VERDICT_POLL_DEADLINE_S', 0.0
        ), mock.patch('sbx_omnigent.runner.time.sleep'):
            result, _sc, _ = self._run(
                _AGY_REVIEW,
                {'build': 'impl', 'review-bugs': 'Still analyzing.'},
                settled={'review-bugs': 'No verdict in here.'},
                max_review_rounds=2,
            )
        self.assertEqual(result.status, 'blocked')

    def test_agy_blocking_loops_back_with_settled_findings(self) -> None:
        # On an agy BLOCKING verdict, the loop-back payload is the
        # SETTLED reply (its real findings), not the opening narration.
        _result, sc, _ = self._run(
            _AGY_REVIEW,
            {'build': ['impl', 'fixed'],
             'review-bugs': ['opening narration...', 'VERDICT: APPROVED']},
            settled={'review-bugs': 'Bug: off-by-one in the range end.'
                                    '\nVERDICT: BLOCKING'},
        )
        build_sid = next(
            s for s, lb in sc._label.items() if lb == 'build'
        )
        build_msgs = [m for s, m in sc.sent if s == build_sid]
        self.assertTrue(any('off-by-one' in m for m in build_msgs))


_COMPETE = """\
name: race
repo: https://github.com/org/proj.git
publish:
  branch: pick
task: |
  implement it
agents:
  ca: {template: coder, model: claude-sonnet-5}
  cb: {template: coder, harness: codex-native, model: gpt-5}
  jg: {template: judge, model: claude-opus-4-8}
stages:
  - id: impl
    parallel:
      - {id: impl-a, run: ca, write: true}
      - {id: impl-b, run: cb, write: true}
  - id: pick
    run: jg
    needs: [impl-a, impl-b]
    selects: branch
"""


_TDD_JUDGE = """\
name: tj
repo: ./proj
publish:
  mode: local
  branch: refactor
task: |
  implement parse_ports
agents:
  tw: {template: tdd-writer, model: claude-sonnet-5}
  ca: {template: coder, model: claude-sonnet-5}
  cb: {template: coder, model: claude-sonnet-5}
  jg: {template: judge, model: claude-opus-4-8}
  rf: {template: refactoring, model: claude-sonnet-5}
  sec: {template: security-reviewer, model: claude-fable-5}
stages:
  - {id: tests, run: tw, write: true}
  - id: impl
    parallel:
      - {id: impl-a, run: ca, write: true, from: tests}
      - {id: impl-b, run: cb, write: true, from: tests}
  - id: review-a
    run: [sec]
    needs: [impl-a]
    gate: consensus
    on_block: impl-a
  - id: review-b
    run: [sec]
    needs: [impl-b]
    gate: consensus
    on_block: impl-b
  - {id: pick, run: jg, needs: [impl-a, impl-b], selects: branch}
  - {id: refactor, run: rf, write: true, from: pick, needs: [pick]}
  - id: review-r
    run: [sec]
    needs: [refactor]
    gate: consensus
    on_block: refactor
"""



_JUDGE_REFACTOR = """\
name: jr
repo: ./proj
publish:
  mode: local
  branch: refactor
task: |
  implement parse_ports
acceptance: |
  handles ranges
agents:
  ca: {template: coder, model: claude-sonnet-5}
  cb: {template: coder, model: claude-sonnet-5}
  jg: {template: judge, model: claude-opus-4-8}
  rf: {template: refactoring, model: claude-sonnet-5}
  sec: {template: security-reviewer, model: claude-fable-5}
stages:
  - id: impl
    parallel:
      - {id: impl-a, run: ca, write: true}
      - {id: impl-b, run: cb, write: true}
  - {id: pick, run: jg, needs: [impl-a, impl-b], selects: branch}
  - {id: refactor, run: rf, write: true, from: pick, needs: [pick]}
  - id: review-r
    run: [sec]
    needs: [refactor]
    gate: consensus
    on_block: refactor
"""


class TestSilentReviewerIsAsked(_Base):
    """A reviewer told to EXECUTE what it reviews can outlive its own
    turn — installing a toolchain, running an instrumented build — and
    end mid-narration. Reading that silence as a vote against the
    branch re-drives a coder over nothing and hands it narration in
    place of findings."""

    def _silent(self, replies, **kw):
        # Deadline 0 so the poll loop reads once and falls through to
        # the nudge instead of polling for real minutes.
        with mock.patch(
            'sbx_omnigent.runner._VERDICT_POLL_DEADLINE_S', 0.0
        ), mock.patch('sbx_omnigent.runner.time.sleep'):
            return self._run(_LINEAR, replies, **kw)

    def test_a_verdict_given_when_asked_is_not_a_block(self) -> None:
        result, sc, wt = self._silent(
            {'plan': 'P', 'build': 'b', 'review-sec': [
                'Running coverage with cargo-llvm-cov. I will wait.',
                'VERDICT: APPROVED',
            ]},
            settled={'review-sec': 'Running coverage. I will wait.'},
        )
        self.assertEqual(result.status, 'completed')
        self.assertNotIn(
            'build: address review', [m for _, m, _ in wt.commits]
        )
        self.assertIn(R._VERDICT_NUDGE, [m for _s, m in sc.sent])

    def test_the_loop_back_carries_findings_not_just_the_token(self) -> None:
        # The findings live in the narration the reviewer was cut off
        # mid-way through; the nudge reply is often only the verdict
        # line. A writer handed just "VERDICT: BLOCKING" has nothing to
        # act on — which is exactly what the live run showed.
        _result, sc, _wt = self._silent(
            {'plan': 'P', 'build': 'b', 'review-sec': [
                'pool.rs:88 drops the guard before the await.',
                'VERDICT: BLOCKING — that race is real',
            ]},
            settled={'review-sec': 'pool.rs:88 drops the guard.'},
        )
        fix = [m for s, m in sc.sent if sc.label_of(s) == 'build'][-1]
        self.assertIn('drops the guard', fix)
        self.assertIn('that race is real', fix)

    def test_still_blocking_if_it_will_not_say(self) -> None:
        # The safe default has to survive the nudge: a string reply is
        # returned for every turn, so the nudge answers narration too.
        result, _sc, _wt = self._silent(
            {'plan': 'P', 'build': 'b', 'review-sec': 'Still analyzing.'},
            settled={'review-sec': 'No verdict in here.'},
            max_review_rounds=1,
        )
        self.assertEqual(result.status, 'blocked')

    def test_a_failed_nudge_turn_is_not_fatal(self) -> None:
        # The reviewer's VM can be gone by the time we ask (a wedged
        # sbx, a guest remounted read-only). That is not a reason to
        # fail the run, and it must not lose the findings already read.
        cfg = self._cfg(_LINEAR)
        runner = R.PipelineRunner(
            cfg, session_client=FakeSC({}), worktree_manager=FakeWT(),
            run_id='r1', agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        with mock.patch.object(
            runner, '_drive', side_effect=R.PipelineRunError('vm gone')
        ):
            verdict, text = runner._nudge_for_verdict(
                'sess-9', 'review-sec', 'pool.rs:88 drops the guard'
            )
        self.assertIsNone(verdict)  # the safe default still stands
        self.assertEqual(text, 'pool.rs:88 drops the guard')


_TWO_REVIEWS = """\
name: race
repo: https://github.com/org/proj.git
publish:
  branch: pick
task: |
  implement it
agents:
  ca: {template: coder, model: claude-sonnet-5}
  cb: {template: coder, model: claude-sonnet-5}
  sec: {template: security-reviewer, model: claude-fable-5}
  bugs: {template: bug-reviewer, model: claude-fable-5}
  jg: {template: judge, model: claude-opus-4-8}
stages:
  - id: impl
    parallel:
      - {id: impl-a, run: ca, write: true}
      - {id: impl-b, run: cb, write: true}
  - id: review
    parallel:
      - {id: review-a, run: [sec, bugs], needs: [impl-a], gate: consensus,
         on_block: impl-a}
      - {id: review-b, run: [sec, bugs], needs: [impl-b], gate: consensus,
         on_block: impl-b}
  - id: pick
    run: jg
    needs: [impl-a, impl-b, review-a, review-b]
    selects: branch
"""


_TWO_REVIEW_REPLIES = {
    'impl-a': 'A', 'impl-b': 'B',
    'review-a-sec': 'VERDICT: APPROVED',
    'review-a-bugs': 'VERDICT: APPROVED',
    'review-b-sec': 'VERDICT: APPROVED',
    'review-b-bugs': 'VERDICT: APPROVED',
    'pick': 'SELECT: impl-a',
}


class TestAStageFreesItsOwnSessionsOnly(_Base):
    """
    Review stages used to free their guests by an INDEX MARK into one
    shared session list — ``mark = len(self._sessions)`` on the way in,
    ``del self._sessions[mark:]`` on the way out.

    That is safe only while one review stage runs at a time. With
    review-a and review-b in a ``parallel:`` block both take a mark at
    roughly the same length, so whichever votes first slices from its
    own mark and deletes the sessions the other is still driving —
    tearing down a live reviewer's microVM mid-build. Naming the
    sessions removes the ambiguity.
    """

    def _runner(self, sc, cfg=None):
        cfg = cfg if cfg is not None else self._cfg(_TWO_REVIEWS)
        return R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=FakeWT(), run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )

    def test_only_the_named_sessions_are_freed(self) -> None:
        # The invariant the mark violated, stated directly: a stage
        # frees what it created and leaves everything else running.
        sc = FakeSC(dict(_TWO_REVIEW_REPLIES))
        runner = self._runner(sc)
        runner._sessions[:] = ['a-1', 'b-1', 'a-2', 'b-2']
        runner._dispose_sessions(['a-1', 'a-2'])
        self.assertEqual(sc.disposed, ['a-1', 'a-2'])
        self.assertEqual(runner._sessions, ['b-1', 'b-2'])

    def test_an_outstanding_chunk_mark_still_means_what_it_did(self)\
            -> None:
        # Chunk teardown still slices by index. Removing a stage's
        # sessions must not shift anything across a live mark, or the
        # next chunk boundary frees the wrong guests.
        sc = FakeSC(dict(_TWO_REVIEW_REPLIES))
        runner = self._runner(sc)
        runner._sessions[:] = ['chunk0-1', 'chunk0-2']
        mark = len(runner._sessions)
        runner._sessions.extend(['stage-a', 'stage-b', 'other'])
        runner._dispose_sessions(['stage-a', 'stage-b'])
        self.assertEqual(runner._sessions[mark:], ['other'])
        self.assertEqual(runner._sessions[:mark], ['chunk0-1', 'chunk0-2'])

    def test_a_session_already_freed_is_not_deleted_twice(self) -> None:
        # Each reviewer releases its own guest the moment it votes, so
        # the stage backstop must be a no-op for those.
        sc = FakeSC(dict(_TWO_REVIEW_REPLIES))
        runner = self._runner(sc)
        runner._sessions[:] = ['s-1', 's-2']
        runner._free_session('s-1', 'voted')
        self.assertEqual(sc.disposed, ['s-1'])
        runner._dispose_sessions(['s-1', 's-2'])
        self.assertEqual(sc.disposed, ['s-1', 's-2'])
        self.assertEqual(runner._sessions, [])

    def test_a_still_working_reviewer_is_not_torn_down(self) -> None:
        """
        The bug itself: review-a finishing must not kill review-b.

        review-b is held mid-turn — its guest is booted and driving,
        exactly like a reviewer part-way through a build — while
        review-a votes and its stage tears down. Under the index mark
        that teardown swept review-b's live session into the slice.
        """
        cfg = self._cfg(_TWO_REVIEWS)
        sc = FakeSC(dict(_TWO_REVIEW_REPLIES))
        in_flight: set[str] = set()
        killed_live: list[str] = []
        proceed = threading.Event()
        send, drop = sc.send_and_wait, sc.dispose

        def held(session, message, **kw):
            label = sc._label.get(session, '')
            in_flight.add(session)
            try:
                if label == 'review-b-sec':
                    # Bounded: nothing releases this when the bug is
                    # fixed, and the run must still finish.
                    proceed.wait(2.0)
                return send(session, message, **kw)
            finally:
                in_flight.discard(session)

        def watched(session):
            if session in in_flight:
                killed_live.append(sc._label.get(session, ''))
                proceed.set()
            return drop(session)

        sc.send_and_wait, sc.dispose = held, watched
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=FakeWT(), run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner.run()
        self.assertEqual(
            killed_live, [],
            f'a live reviewer was disposed mid-turn: {killed_live}',
        )
        self.assertEqual(
            len(sc.disposed), len(set(sc.disposed)),
            f'a session was disposed twice: {sc.disposed}',
        )
        for label in ('review-a-sec', 'review-b-sec'):
            with self.subTest(label=label):
                kinds = [k for k, lb in sc.events if lb == label]
                self.assertEqual(kinds, ['create', 'dispose'])

    def test_the_cap_pays_for_work_not_for_containers(self) -> None:
        """
        The node cap must be charged to turns, not to threads.

        A ``parallel:`` block of review stages drives nothing itself.
        While the cap was charged per parallel child, the two review
        containers each held a slot they did no work with, leaving only
        cap-minus-two for the four reviewers underneath — half the
        intended concurrency, and at a low enough cap or a deep enough
        nesting, none at all.

        Forced rather than hoped for: all four reviewers must meet at a
        barrier before any may return, which is reachable only if all
        four hold a slot at once. The cap is pinned to exactly four, so
        a single slot spent on a container leaves the barrier
        unsatisfiable and the run never finishes.
        """
        cfg = self._cfg(_TWO_REVIEWS)
        sc = FakeSC(dict(_TWO_REVIEW_REPLIES))
        gate = threading.Barrier(4, timeout=15)
        reached: list[str] = []
        send = sc.send_and_wait

        def rendezvous(session, message, **kw):
            label = sc._label.get(session, '')
            if label.startswith('review-'):
                reached.append(label)
                gate.wait()
            return send(session, message, **kw)

        sc.send_and_wait = rendezvous
        with mock.patch.object(R, '_MAX_PARALLEL_NODES', 4):
            runner = R.PipelineRunner(
                cfg, session_client=sc, worktree_manager=FakeWT(),
                run_id='r1',
                agent_ids={n: f'ag-{n}' for n in cfg.agents},
                swap_age_s=lambda: 0.0,
            )
            done, failed = threading.Event(), []

            def go():
                try:
                    runner.run()
                except BaseException as exc:
                    failed.append(exc)
                finally:
                    done.set()

            worker = threading.Thread(target=go, daemon=True)
            worker.start()
            self.assertTrue(
                done.wait(40),
                'the run never finished — the cap starved the reviewers',
            )
        self.assertEqual(failed, [])
        self.assertEqual(
            sorted(reached),
            ['review-a-bugs', 'review-a-sec',
             'review-b-bugs', 'review-b-sec'],
        )

    def test_the_two_review_stages_run_at_the_same_time(self) -> None:
        """
        The saving this unlocks, proved rather than assumed.

        review-a and review-b are independent — each reads its own
        writer's branch — so a barrier neither can pass alone holds
        only if they really overlap. Driven in series the first waits
        for a partner that cannot arrive, and this fails on the
        timeout instead of passing slowly.
        """
        cfg = self._cfg(_TWO_REVIEWS)
        sc = FakeSC(dict(_TWO_REVIEW_REPLIES))
        gate = threading.Barrier(2, timeout=10)
        reached: list[str] = []
        original = sc.send_and_wait

        def rendezvous(session, message, **kw):
            # One reviewer from each stage: the claim under test is that
            # the STAGES overlap, not that four reviewers fit.
            label = sc._label.get(session, '')
            if label.endswith('-sec'):
                reached.append(label)
                gate.wait()
            return original(session, message, **kw)

        sc.send_and_wait = rendezvous
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=FakeWT(), run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner.run()
        self.assertEqual(sorted(reached), ['review-a-sec', 'review-b-sec'])


class TestAReviewerIsFreedTheMomentItVotes(_Base):
    """A reviewer that has voted must hand its guest back at once.

    Reviewers are told to EXECUTE what they review, so one that has
    already voted is holding a full guest away from those still
    building. Observed live: a reviewer voted APPROVED and its idle
    6 GB guest stayed up for 1h50m while the remaining reviewer's
    build thrashed at load 18 with every linker at 0% CPU, on a host
    holding three guests in 17 GB.

    This used to be guaranteed by running reviewers ONE AT A TIME.
    They now run together (TASKS.md #46), so the guarantee is per
    reviewer rather than a global order."""

    _TWO = _LINEAR.replace(
        '  sec:\n', '  bugs:\n    template: bug-reviewer\n'
        '    model: claude-fable-5\n  sec:\n',
    ).replace('    run: [sec]', '    run: [sec, bugs]')

    def test_every_reviewer_is_freed_after_its_own_vote(self) -> None:
        # Order ACROSS reviewers is now a race, so assert the property
        # that still holds and still matters: each one is disposed, and
        # after its own create rather than at the end of the stage.
        _r, sc, _wt = self._run(
            self._TWO,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED',
             'review-bugs': 'VERDICT: APPROVED'},
        )
        for label in ('review-sec', 'review-bugs'):
            with self.subTest(label=label):
                kinds = [k for k, lb in sc.events if lb == label]
                self.assertEqual(kinds, ['create', 'dispose'])

    def test_the_reviewers_of_a_stage_run_at_the_same_time(self) -> None:
        """
        The saving, proved rather than assumed.

        A barrier both reviewers must reach before either may return:
        if they were driven one after the other the first would wait
        for a partner that cannot arrive, and this fails on the
        timeout instead of passing slowly.
        """
        cfg = self._cfg(self._TWO)
        wt, sc = FakeWT(), FakeSC(
            {'plan': 'P', 'build': 'b',
             'review-sec': 'VERDICT: APPROVED',
             'review-bugs': 'VERDICT: APPROVED'}
        )
        gate = threading.Barrier(2, timeout=10)
        reached: list[str] = []
        original = sc.send_and_wait

        def rendezvous(session, message, **kw):
            label = sc._label.get(session, '')
            if label.startswith('review-'):
                reached.append(label)
                gate.wait()          # raises BrokenBarrierError on timeout
            return original(session, message, **kw)

        sc.send_and_wait = rendezvous
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner.run()
        self.assertEqual(sorted(reached), ['review-bugs', 'review-sec'])

    def test_a_wedged_reviewer_is_still_retried_later(self) -> None:
        # A delete that fails means the VM may still be up; dropping the
        # handle here would strand a guest nothing knows about.
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        original = sc.create
        stuck: list[str] = []

        def wedge(**kw):
            sid = original(**kw)
            if 'review' in (kw.get('title') or ''):
                sc.dispose_raises.add(sid)
                stuck.append(sid)
            return sid

        sc.create = wedge
        R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        self.assertGreater(sc.dispose_attempts.count(stuck[0]), 1)
        self.assertIn(stuck[0], wt.states[-1]['sessions'])

    def test_keep_frees_no_reviewer(self) -> None:
        _r, sc, _wt = self._run(
            self._TWO,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: APPROVED',
             'review-bugs': 'VERDICT: APPROVED'},
            keep=True,
        )
        self.assertEqual(sc.dispose_attempts, [])


class TestTheNudgeReachesAnAgyReviewer(_Base):
    """An agy turn is staged as a file with only a pointer pasted,
    because agy drops multi-line pastes. That is right for a task and
    wrong for the verdict nudge: a reviewer that believes it is mid-work
    does not stop to re-read a file. Live, three consecutive rounds of
    an agy reviewer recorded no verdict at all, and not one of their
    transcripts contained the nudge text."""

    def _nudged(self, **kw):
        with mock.patch(
            'sbx_omnigent.runner._VERDICT_POLL_DEADLINE_S', 0.0
        ), mock.patch('sbx_omnigent.runner.time.sleep'):
            return self._run(
                _AGY_REVIEW,
                {'build': 'impl', 'review-bugs': [
                    'Running the suite. I will wait for it.',
                    'VERDICT: APPROVED',
                ]},
                settled={'review-bugs': 'Running the suite. I will wait.'},
                **kw,
            )

    def test_the_nudge_is_pasted_not_staged_as_a_file(self) -> None:
        _r, sc, _wt = self._nudged()
        pasted = [m for s, m in sc.sent if sc.label_of(s) == 'review-bugs']
        self.assertTrue(
            any('verdict' in m.lower() and 'OMNI_TASK' not in m
                for m in pasted),
            f'the nudge never reached the agent as text: {pasted}',
        )

    def test_it_is_still_a_single_line(self) -> None:
        # agy drops a multi-line paste before it renders, so bypassing
        # the file must not bypass the flattening.
        _r, sc, _wt = self._nudged()
        nudge = next(
            m for s, m in sc.sent
            if sc.label_of(s) == 'review-bugs' and 'verdict' in m.lower()
            and 'OMNI_TASK' not in m
        )
        self.assertNotIn('\n', nudge)

    def test_the_review_turn_itself_is_still_staged(self) -> None:
        # Only the nudge is inline; a full review turn is far too long
        # to paste and must keep going through the task file.
        _r, sc, _wt = self._nudged()
        first = next(
            m for s, m in sc.sent if sc.label_of(s) == 'review-bugs'
        )
        self.assertIn('OMNI_TASK', first)

    def test_a_claude_reviewer_is_unaffected(self) -> None:
        with mock.patch(
            'sbx_omnigent.runner._VERDICT_POLL_DEADLINE_S', 0.0
        ), mock.patch('sbx_omnigent.runner.time.sleep'):
            _r, sc, _wt = self._run(
                _LINEAR,
                {'plan': 'P', 'build': 'b', 'review-sec': [
                    'still going', 'VERDICT: APPROVED',
                ]},
                settled={'review-sec': 'still going'},
            )
        self.assertIn(R._VERDICT_NUDGE, [m for _s, m in sc.sent])


class TestAWorkingReviewerIsNotSilent(_Base):
    """Reviewers were told to EXECUTE what they review, so a review
    legitimately runs for tens of minutes. Measuring that against a
    wall clock punished the reviewers doing the most work: live, one
    agy reviewer recorded NO verdict in three consecutive rounds while
    its reports read: Running `cargo test -p discover-k8s -j 2` in the
    background ... I will wait for it to complete."""

    def _elapsed(self, *, busy: bool) -> float:
        """Wall-clock seconds the verdict wait takes.

        Measured in TIME, not poll count: ``time.sleep`` is mocked out
        so the loop spins freely, which makes an iteration count say
        nothing about the deadline. The deadline and ceiling are both
        wall-clock, so this is what they actually govern.
        """
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(
            {'plan': 'P', 'build': 'b', 'review-sec': 'still going'}
        )
        sc.settled_for_label['review-sec'] = 'no verdict here'
        if busy:
            sc.busy_labels.add('review-sec')
        began = time.monotonic()
        with mock.patch(
            'sbx_omnigent.runner._VERDICT_POLL_DEADLINE_S', 0.05
        ), mock.patch(
            'sbx_omnigent.runner._VERDICT_POLL_CEILING_S', 0.6
        ), mock.patch(
            'sbx_omnigent.runner._VERDICT_POLL_INTERVAL_S', 0.0
        ), mock.patch('sbx_omnigent.runner.time.sleep'):
            with contextlib.suppress(R.PipelineRunError):
                R.PipelineRunner(
                    cfg, session_client=sc, worktree_manager=wt,
                    run_id='r1',
                    agent_ids={n: f'ag-{n}' for n in cfg.agents},
                    max_review_rounds=1, swap_age_s=lambda: 0.0,
                ).run()
        return time.monotonic() - began

    def test_a_reviewer_still_producing_output_keeps_its_time(self):
        # THE fix: new items mean it is working, not silent, so the
        # silence clock restarts instead of expiring mid-build.
        quiet, busy = self._elapsed(busy=False), self._elapsed(busy=True)
        self.assertGreater(busy, quiet * 2)

    def test_a_genuinely_quiet_reviewer_still_expires(self) -> None:
        # The deadline must still bite, or a dead reviewer holds its
        # microVM until the turn budget runs out.
        self.assertLess(self._elapsed(busy=False), 1.0)

    def test_endless_narration_is_still_bounded(self) -> None:
        # A reviewer that talks forever without ever voting must not
        # hold its VM indefinitely — the ceiling caps it.
        self.assertLess(self._elapsed(busy=True), 10.0)


class TestASilentReviewerIsAFailedReview(_Base):
    """A reviewer that never stated a verdict did not vote AGAINST the
    branch — it failed to review. Conflating the two re-drives a coder
    over a branch nobody objected to and hands it the reviewer's own
    narration as findings. Live, a coder answered: "there is no
    actionable finding to address — it names no defect, file, line, or
    assertion", and it was right."""

    def _silent_run(self, **kw):
        # No verdict anywhere: streamed, settled, or after the nudge.
        with mock.patch(
            'sbx_omnigent.runner._VERDICT_POLL_DEADLINE_S', 0.0
        ), mock.patch('sbx_omnigent.runner.time.sleep'):
            return self._run(
                _LINEAR,
                {'plan': 'P', 'build': 'b',
                 'review-sec': 'I am waiting for cargo test to finish.'},
                settled={'review-sec': 'still waiting on the build'},
                **kw,
            )

    def test_the_review_re_runs_instead_of_the_writer(self) -> None:
        # THE fix. The coder must not be re-driven over a review that
        # simply did not conclude.
        _r, sc, wt = self._silent_run()
        self.assertNotIn(
            'build: address review', [m for _n, m, _a in wt.commits]
        )
        rounds = [lb for k, lb in sc.events
                  if k == 'create' and lb == 'review-sec']
        self.assertGreater(len(rounds), 1)   # the REVIEW was retried

    def test_narration_is_never_relayed_as_findings(self) -> None:
        _r, sc, _wt = self._silent_run()
        fixes = [m for s, m in sc.sent
                 if sc.label_of(s) == 'build'
                 and 'BLOCKING issues' in m]
        for f in fixes:
            self.assertNotIn('waiting on the build', f)

    def test_retries_are_bounded_and_silence_still_blocks(self) -> None:
        # The safe default survives: a review that never concludes
        # eventually blocks rather than looping forever.
        result, _sc, _wt = self._silent_run(max_review_rounds=1)
        self.assertEqual(result.status, 'blocked')

    def test_an_explicit_block_still_re_drives_the_writer(self) -> None:
        # Unchanged behaviour for a real finding — this is the path
        # that must not regress.
        _r, _sc, wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b',
             'review-sec': ['VERDICT: BLOCKING — pool.rs:88 races',
                            'VERDICT: APPROVED']},
        )
        self.assertIn(
            'build: address review', [m for _n, m, _a in wt.commits]
        )

    def test_a_blocker_beside_a_silent_reviewer_wins(self) -> None:
        # Mixed round: the blocker's findings go to the writer, and the
        # silent one's narration does not ride along with them.
        text = _LINEAR.replace(
            '  sec:\n', '  bugs:\n    template: bug-reviewer\n'
            '    model: claude-fable-5\n  sec:\n',
        ).replace('    run: [sec]', '    run: [sec, bugs]')
        with mock.patch(
            'sbx_omnigent.runner._VERDICT_POLL_DEADLINE_S', 0.0
        ), mock.patch('sbx_omnigent.runner.time.sleep'):
            _r, sc, _wt = self._run(
                text,
                {'plan': 'P', 'build': 'b',
                 'review-sec': ['VERDICT: BLOCKING — real defect',
                                'VERDICT: APPROVED'],
                 'review-bugs': 'still compiling, will report back'},
                settled={'review-bugs': 'still compiling'},
            )
        fix = next(m for s, m in sc.sent
                   if sc.label_of(s) == 'build' and 'BLOCKING issues' in m)
        self.assertIn('real defect', fix)
        self.assertNotIn('still compiling', fix)


class TestVotedReviewersAreFreed(_Base):
    """A reviewer that has voted is dead weight, and a blocked review
    LOOPS — each round creating fresh sessions. Held to the end of the
    chunk, three rounds of a two-reviewer gate hold six microVMs."""

    def _blocked_once(self, **kw):
        # Round 1 blocks, round 2 approves (the list runs out and the
        # fake defaults to APPROVED), so two rounds of reviewers exist.
        return self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b',
             'review-sec': ['VERDICT: BLOCKING — no bounds check']},
            **kw,
        )

    def test_a_rounds_vm_is_gone_before_the_next_round_boots(self) -> None:
        result, sc, _wt = self._blocked_once()
        self.assertEqual(result.status, 'completed')
        reviews = [k for k, lb in sc.events if lb == 'review-sec']
        self.assertEqual(reviews, ['create', 'dispose', 'create', 'dispose'])

    def test_keep_still_holds_every_reviewer(self) -> None:
        _result, sc, _wt = self._blocked_once(keep=True)
        self.assertEqual(sc.disposed, [])


class TestTurnCapture(_Base):
    """Disposing a session DELETES it, and teardown disposes everything
    moments after a run fails. Twice a turn burned an hour and left
    nothing behind but its own timeout line — the transcript was gone
    before anyone could ask what it had been doing."""

    def _runner(self, replies, **kw):
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(replies))
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, **kw,
        )
        return runner, sc, wt

    def test_a_turn_that_never_returns_is_captured(self) -> None:
        runner, sc, wt = self._runner({'plan': 'P'})
        sc.raise_labels.add('build')
        sc.transcript_for_label['build'] = [
            ('user', 'Implement it.'),
            ('assistant', 'Installing the toolchain first.'),
        ]
        with self.assertRaises(SwarmSessionError):
            runner.run()
        doc = wt.artifacts['turns/build.md']
        self.assertIn('Installing the toolchain first', doc)
        self.assertIn('did not return', doc)

    def test_a_failed_turn_is_captured(self) -> None:
        runner, sc, wt = self._runner({'plan': 'P'})
        sc.fail_labels.add('build')
        sc.transcript_for_label['build'] = [('assistant', 'got as far as')]
        with self.assertRaises(R.PipelineRunError):
            runner.run()
        self.assertIn('the turn failed', wt.artifacts['turns/build.md'])

    def test_a_failed_turn_also_captures_the_tui_pane(self) -> None:
        # The whole point of #26: a harness blocked on a keystroke never
        # produces a message, so the transcript is empty and the SCREEN
        # is the only evidence there is.
        runner, sc, wt = self._runner({'plan': 'P'})
        sc.fail_labels.add('build')
        sc.default_host_id = 'h1'
        sc.host_names['h1'] = 'managed-h1'
        with mock.patch.object(
            R.pane, 'capture_pane', return_value='> 1. Try new model'
        ) as capture:
            with self.assertRaises(R.PipelineRunError) as caught:
                runner.run()
        self.assertIn(
            'Try new model', wt.artifacts['turns/build.pane.txt']
        )
        # It must say WHICH VM, and it must not have typed anything.
        self.assertIn('managed-h1', wt.artifacts['turns/build.pane.txt'])
        # Not `called_once`: the launch read-back reads the same
        # pane at session create (#28/#34/#35). Neither one types.
        self.assertEqual(
            {c.args[0] for c in capture.call_args_list}, {'managed-h1'}
        )
        # And the error a human reads has to point at it — a bare
        # "failed: None" is what cost a day on the codex-3 run.
        self.assertIn('turns/build.pane.txt', str(caught.exception))

    def test_a_routine_capture_does_not_pay_for_a_pane(self) -> None:
        # A healthy run loops back through review rounds; reading a pane
        # on each one is an sbx round-trip for nothing.
        with mock.patch.object(R.pane, 'capture_pane') as capture:
            _r, _sc, wt = self._run(
                _LINEAR,
                {'plan': 'P', 'build': 'b',
                 'review-sec': ['VERDICT: BLOCKING — pool.rs:88 races']},
            )
        capture.assert_not_called()
        self.assertNotIn('turns/build.pane.txt', wt.artifacts)

    def test_a_session_with_no_resolvable_vm_still_captures(self) -> None:
        # host_ids is empty, so the pane cannot be located. The
        # transcript must land anyway — the diagnostic must never be
        # able to cost us the diagnostic we already had.
        runner, sc, wt = self._runner({'plan': 'P'})
        sc.fail_labels.add('build')
        sc.transcript_for_label['build'] = [('assistant', 'got as far as')]
        with self.assertRaises(R.PipelineRunError):
            runner.run()
        self.assertIn('got as far as', wt.artifacts['turns/build.md'])
        self.assertNotIn('turns/build.pane.txt', wt.artifacts)

    def test_a_host_lookup_that_explodes_is_not_fatal(self) -> None:
        # An older server, a changed response shape, an injected client
        # that does not implement the read at all — all of it means "no
        # pane", never "a different failure".
        runner, sc, wt = self._runner({'plan': 'P'})
        sc.fail_labels.add('build')
        with mock.patch.object(
            type(sc), 'session_host_id', side_effect=RuntimeError('boom')
        ):
            with self.assertRaises(R.PipelineRunError):
                runner.run()
        self.assertIn('the turn failed', wt.artifacts['turns/build.md'])

    def test_a_loop_back_fix_turn_is_kept_even_on_success(self) -> None:
        # Its session is disposed at publish, and what a writer did with
        # a reviewer's findings is what a later reader wants to see.
        _r, _sc, wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b',
             'review-sec': ['VERDICT: BLOCKING — pool.rs:88 races']},
        )
        self.assertIn('loop-back fix turn', wt.artifacts['turns/build.md'])

    def test_a_second_re_drive_does_not_overwrite_the_first(self) -> None:
        _r, _sc, wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': [
                'VERDICT: BLOCKING — one', 'VERDICT: BLOCKING — two',
            ]},
        )
        self.assertEqual(
            sorted(k for k in wt.artifacts if k.startswith('turns/')),
            ['turns/build-2.md', 'turns/build.md'],
        )

    def test_an_unreadable_session_still_leaves_a_record(self) -> None:
        # "could not be read" is itself the finding.
        runner, sc, wt = self._runner({'plan': 'P'})
        sc.raise_labels.add('build')
        sc.transcript_for_label['build'] = []
        with self.assertRaises(SwarmSessionError):
            runner.run()
        self.assertIn(
            'could not be read', wt.artifacts['turns/build.md']
        )

    def test_a_capture_that_cannot_be_written_is_not_fatal(self) -> None:
        # A diagnostic that can fail a run is worse than no diagnostic.
        runner, sc, wt = self._runner({'plan': 'P'})
        wt.artifact_writes_fail = True
        sc.fail_labels.add('build')
        with self.assertRaises(R.PipelineRunError) as ctx:
            runner.run()
        self.assertIn('turn on', str(ctx.exception))   # the REAL error
        self.assertEqual(wt.artifacts, {})


class TestLateWritesAreReconciled(_Base):
    """A native-terminal writer keeps working after its turn reports
    done AND after the settle-and-commit fires. Live, one committed a
    31-line manifest at 23:58 and wrote the 1080-line implementation at
    00:26 — its branch stayed a stub a human had to fix by hand. No
    grace at commit time covers a 28-minute gap, so the branch is
    reconciled where it starts to matter: before reviewers read it."""

    def _dirty(self, node, **kw):
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        original = wt.commit_node

        def commit_then_write_more(run_id, node_id, **ckw):
            # The writer's stage commits, THEN more work lands.
            out = original(run_id, node_id, **ckw)
            if node_id == node and 'implement' in ckw['message']:
                wt.dirty_nodes.add(node_id)
            return out

        wt.commit_node = commit_then_write_more
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, **kw,
        )
        return runner.run(), sc, wt

    def test_late_work_reaches_the_branch_before_review(self) -> None:
        result, _sc, wt = self._dirty('build')
        self.assertEqual(result.status, 'completed')
        self.assertIn(
            'build: late write', [m for _n, m, _a in wt.commits]
        )

    def test_it_lands_before_the_reviewer_is_even_created(self) -> None:
        # THE point of the fix: reviewers mount the WORKTREE while the
        # judge clones the BRANCH, so the reconcile has to beat the
        # reviewer or the two are judging different things.
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        original_commit, original_create = wt.commit_node, sc.create
        seen: dict[str, list[str]] = {'at_create': []}

        def commit_then_write_more(run_id, node_id, **ckw):
            out = original_commit(run_id, node_id, **ckw)
            if node_id == 'build' and 'implement' in ckw['message']:
                wt.dirty_nodes.add(node_id)
            return out

        def note_commits_at_create(**ckw):
            if 'review' in (ckw.get('title') or ''):
                seen['at_create'] = [m for _n, m, _a in wt.commits]
            return original_create(**ckw)

        wt.commit_node = commit_then_write_more
        sc.create = note_commits_at_create
        R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        self.assertIn('build: late write', seen['at_create'])

    def test_it_settles_before_committing_the_late_work(self) -> None:
        # The writer may still be mid-write when we notice; committing
        # immediately would capture another partial tree.
        _r, _sc, wt = self._dirty('build')
        self.assertEqual(wt.settled.count('build'), 2)  # stage + reconcile

    def test_a_clean_worktree_is_left_alone(self) -> None:
        _r, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertNotIn(
            'build: late write', [m for _n, m, _a in wt.commits]
        )
        self.assertEqual(wt.settled.count('build'), 1)  # no extra wait

    def test_a_reconcile_failure_never_fails_the_review(self) -> None:
        # Best-effort: a git error here must not take down a review
        # that would otherwise proceed.
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        wt.dirty_nodes.add('build')
        original = wt.commit_node

        def fail_the_reconcile(run_id, node_id, **ckw):
            if 'late write' in ckw['message']:
                raise click.ClickException('index.lock exists')
            return original(run_id, node_id, **ckw)

        wt.commit_node = fail_the_reconcile
        result = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        self.assertEqual(result.status, 'completed')

    def test_only_writers_are_reconciled(self) -> None:
        # A reader or judge has no branch of its own to reconcile.
        cfg = self._cfg(_LINEAR)
        runner = R.PipelineRunner(
            cfg, session_client=FakeSC({}), worktree_manager=(wt := FakeWT()),
            run_id='r1', agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner._nodes['plan'] = R.NodeResult('plan', 'reader')
        wt.dirty_nodes.add('plan')
        runner._reconcile_late_writes('plan')
        self.assertEqual(wt.commits, [])


class TestChecksAreNotTheWritersToWeaken(_Base):
    """A blocking finding is handed to a writer as a mandate. Live, a
    reviewer told m2-impl-b to drop `GetGroup` from its manifest — but
    the frozen test asserts that set exactly, so obeying meant deleting
    the assertion, which it did. The finding was simply wrong.

    WIDENED after m5. The rule covered TESTS only, so a writer closed a
    blocking supply-chain finding without touching one: it appended
    three advisory ids to the auditor's ignore list and deleted the
    comment block documenting why the one pre-existing entry was
    justified. Every gate went green and the branch shipped. A gate's
    own configuration is a check too, and removing one quietly is the
    single worst outcome this pipeline can produce."""

    def _fix_turn(self) -> str:
        _r, sc, _wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b',
             'review-sec': ['VERDICT: BLOCKING — drop GetGroup']},
        )
        return [m for s, m in sc.sent if sc.label_of(s) == 'build'][-1]

    def test_the_loop_back_forbids_weakening_a_frozen_test(self) -> None:
        fix = self._fix_turn()
        self.assertIn('Nothing that CHECKS this work', fix)
        self.assertIn('assertions and expected values', fix)

    def test_it_also_covers_gate_and_ci_configuration(self) -> None:
        # The m5 hole: the writer never touched a test.
        fix = self._fix_turn()
        for surface in ('gate', 'linter', 'auditor', 'CI workflow'):
            self.assertIn(surface, fix)

    def test_an_ignore_entry_is_named_as_not_a_fix(self) -> None:
        fix = self._fix_turn()
        self.assertIn('suppression', fix)
        self.assertIn('ignore, allow-list', fix)
        self.assertIn('is NOT a fix', fix)

    def test_touching_a_check_must_be_disclosed(self) -> None:
        # Banning the edit outright is wrong — a module whose whole job
        # is adding CI must edit CI. What must never happen is doing it
        # SILENTLY, so the rule targets non-disclosure, not the edit.
        fix = self._fix_turn()
        self.assertIn('you MUST say so', fix)
        self.assertIn('undisclosed change to a check', fix)

    def test_deleting_a_rationale_is_called_out_specifically(self) -> None:
        # m5 deleted an 18-line justification block along with the fix.
        fix = self._fix_turn()
        self.assertIn('Deleting an existing comment or rationale', fix)

    def test_it_offers_disputing_the_finding_as_a_way_out(self) -> None:
        # Without an explicit escape the writer has only two options:
        # obey, or appear to have ignored the review. Both are bad.
        fix = self._fix_turn()
        self.assertIn('contract dispute', fix)
        self.assertIn('disputed', fix)

    def test_the_findings_and_build_demand_still_survive(self) -> None:
        fix = self._fix_turn()
        self.assertIn('drop GetGroup', fix)
        self.assertIn('MUST build the project and run its tests', fix)


class TestAReviewerMayNotDiscountItsOwnFinding(_Base):
    """Live, m5's security reviewer found the supply-chain gate red,
    called it BLOCKING, and in the same paragraph wrote "this portion is
    partly external advisory drift ... I weight it less". The writer
    resolved the hedged finding by silencing the gate, and the next
    round approved. A verdict is not a weighted opinion: a finding you
    would discount is a NON-BLOCKING finding, and saying both at once
    reads to the writer as permission to make the symptom go away."""

    def _review_turn(self) -> str:
        _r, sc, _wt = self._run(_LINEAR, {'plan': 'P', 'build': 'b'})
        return next(
            m for s, m in sc.sent if sc.label_of(s) == 'review-sec'
        )

    def test_blocking_means_you_would_hold_the_release(self) -> None:
        rv = self._review_turn()
        self.assertIn('not a weighted opinion', rv)
        self.assertIn('would hold the release for', rv)

    def test_the_discounting_words_are_named_explicitly(self) -> None:
        rv = self._review_turn()
        for word in ('pre-existing', 'external', 'upstream drift'):
            self.assertIn(word, rv)

    def test_a_discounted_finding_goes_in_a_non_blocking_list(self) -> None:
        rv = self._review_turn()
        self.assertIn('NON-BLOCKING list', rv)
        self.assertIn('argue it down in the same breath', rv)


class TestGuardedChecksAreNamedToReviewers(_Base):
    """Instructions alone were not enough — that IS the m5 lesson. The
    writer already had a frozen-tests rule and routed around it by
    editing the auditor's ignore list instead of a test, and two
    reviewers approved the branch without mentioning the change.

    So the orchestrator now diffs the branch itself and names any
    declared check it touched. This never FORBIDS the edit: a module
    whose whole job is adding CI must edit CI. It only makes passing
    over one in silence impossible."""

    _GUARDED = _LINEAR.replace(
        'stages:\n',
        "guarded:\n"
        "  - '.cargo/audit.toml'\n"
        "  - 'deny.toml'\n"
        "  - '.github/workflows/*'\n"
        "stages:\n",
    )

    def _review_turn(self, diff, text=None):
        cfg = self._cfg(self._GUARDED if text is None else text)
        wt, sc = FakeWT(), FakeSC({'plan': 'P', 'build': 'b'})
        wt.diff_files['build'] = diff
        R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        return next(
            m for s, m in sc.sent if sc.label_of(s) == 'review-sec'
        )

    def test_a_changed_check_is_named_to_the_reviewer(self) -> None:
        rv = self._review_turn(['src/lib.rs', '.cargo/audit.toml'])
        self.assertIn('.cargo/audit.toml', rv)
        self.assertIn('required review item', rv)

    def test_a_glob_covers_ci_workflow_files(self) -> None:
        rv = self._review_turn(['.github/workflows/ci.yml'])
        self.assertIn('.github/workflows/ci.yml', rv)

    def test_the_reviewer_is_told_the_verdict_for_a_silencer(self) -> None:
        rv = self._review_turn(['deny.toml'])
        self.assertIn('the correct verdict is BLOCKING', rv)
        self.assertIn('deleting a documented rationale', rv)

    def test_approving_one_silently_is_named_a_review_failure(self) -> None:
        rv = self._review_turn(['deny.toml'])
        self.assertIn('without mentioning it in your reply', rv)

    def test_ordinary_source_changes_add_nothing(self) -> None:
        rv = self._review_turn(['src/lib.rs', 'src/main.rs'])
        self.assertNotIn('declares to be', rv)
        self.assertNotIn('required review item', rv)

    def test_a_project_that_declares_none_is_unaffected(self) -> None:
        # No `guarded:` in the config -> the surfacing is off entirely,
        # so the launcher never guesses which files are a check.
        rv = self._review_turn(['.cargo/audit.toml'], text=_LINEAR)
        self.assertNotIn('required review item', rv)

    def test_a_git_failure_never_breaks_the_review(self) -> None:
        cfg = self._cfg(self._GUARDED)
        wt, sc = FakeWT(), FakeSC({'plan': 'P', 'build': 'b'})

        def boom(*_a, **_kw):
            raise click.ClickException('bad revision')

        wt.node_diff_files = boom
        result = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        self.assertEqual(result.status, 'completed')


class TestSessionsAreAlwaysReclaimable(_Base):
    """A microVM nothing knows about cannot be reclaimed. Both holes
    here were found live: an orphaned reviewer session survived every
    resume and had to be deleted by hand, and separately a run leaked
    ~26 GB of image store with no record of what held it."""

    def test_a_session_is_in_the_state_before_its_stage_finishes(self):
        # State used to be written only at stage/round boundaries, so a
        # session created inside that window was invisible to --resume
        # forever. Observed live: a reviewer created at 01:24, the
        # minute the machine crashed, outlived every later resume.
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        seen: dict[str, list[str]] = {}
        original = sc.send_and_wait

        def note_state_at_first_turn(session, message, **kw):
            # Mid-stage: the VM exists, nothing has completed yet.
            seen.setdefault(
                'sessions', list(wt.states[-1]['sessions']) if wt.states
                else []
            )
            return original(session, message, **kw)

        sc.send_and_wait = note_state_at_first_turn
        R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        self.assertTrue(seen['sessions'])   # not [] — recorded already

    def test_a_failed_dispose_stays_in_the_run_state(self) -> None:
        # A delete that fails means the VM may still be UP. Dropping
        # the handle cuts it from the state file, and then nothing —
        # not teardown, not a later resume — knows it exists.
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        stuck: list[str] = []
        original = sc.create

        def wedge_the_reviewer(**kw):
            sid = original(**kw)
            if 'review' in (kw.get('title') or ''):
                sc.dispose_raises.add(sid)
                stuck.append(sid)
            return sid

        sc.create = wedge_the_reviewer
        R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        self.assertTrue(stuck)
        self.assertIn(stuck[0], wt.states[-1]['sessions'])

    def test_teardown_tries_an_undisposed_handle_again(self) -> None:
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        stuck: list[str] = []
        original = sc.create

        def wedge_the_reviewer(**kw):
            sid = original(**kw)
            if 'review' in (kw.get('title') or ''):
                sc.dispose_raises.add(sid)
                stuck.append(sid)
            return sid

        sc.create = wedge_the_reviewer
        R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        # Retried rather than dropped — a VM that refused to go may
        # still be running. It is now tried when the reviewer votes,
        # again as the round's backstop, and again at teardown; what
        # this pins is that a FAILED delete is never the last word.
        self.assertGreater(sc.dispose_attempts.count(stuck[0]), 1)
        self.assertNotIn(stuck[0], sc.disposed)          # never succeeded
        self.assertIn(stuck[0], wt.states[-1]['sessions'])  # a resume retries

    def test_a_session_that_disposed_cleanly_is_not_kept(self) -> None:
        # The retain path must not become a leak of its own: a VM that
        # really went away should leave the state file.
        _r, sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        reviewer = next(
            s for s, lb in sc._label.items() if lb == 'review-sec'
        )
        self.assertIn(reviewer, sc.disposed)
        self.assertNotIn(reviewer, wt.states[-1]['sessions'])

    def test_keep_records_every_session_and_disposes_none(self) -> None:
        _r, sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES), keep=True)
        self.assertEqual(sc.dispose_attempts, [])
        for sid in sc._label:
            self.assertIn(sid, wt.states[-1]['sessions'])


class TestInterruptedWorkIsCaptured(_Base):
    """Ctrl-C was the one exit that lost the record. KeyboardInterrupt
    is a BaseException, so the capture hung off `except
    SwarmSessionError` never saw it — and teardown deletes every
    session moments later. Observed live: a stalled reviewer's
    transcript had to be pulled out of the API by hand."""

    def _runner(self, text=_LINEAR, replies=None):
        cfg = self._cfg(text)
        wt, sc = FakeWT(), FakeSC(dict(replies or _LINEAR_REPLIES))
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        return runner, sc, wt

    def test_an_interrupted_turn_is_captured(self) -> None:
        runner, sc, wt = self._runner()
        sc.interrupt_labels.add('build')
        sc.transcript_for_label['build'] = [
            ('user', 'Implement it.'),
            ('assistant', 'Building the workspace; waiting on cargo.'),
        ]
        with self.assertRaises(KeyboardInterrupt):
            runner.run()
        doc = wt.artifacts['turns/build.md']
        self.assertIn('waiting on cargo', doc)
        self.assertIn('Ctrl-C', doc)

    def test_the_interrupt_still_reaches_the_human(self) -> None:
        # Capturing must not turn Ctrl-C into a swallowed error: the
        # run has to end, and the run dir has to survive so --resume
        # can pick it up.
        runner, sc, wt = self._runner()
        sc.interrupt_labels.add('build')
        with self.assertRaises(KeyboardInterrupt):
            runner.run()
        self.assertEqual(wt.disposed, [])   # run dir preserved

    def test_a_second_interrupt_is_not_swallowed(self) -> None:
        # Someone pressing Ctrl-C again wants OUT — not one more
        # bounded HTTP read. _capture_turn swallows SwarmSessionError,
        # never KeyboardInterrupt.
        runner, sc, _wt = self._runner()
        sc.interrupt_labels.add('build')
        sc.interrupt_reads.add('build')
        with self.assertRaises(KeyboardInterrupt):
            runner.run()

    def test_an_interrupted_planning_session_is_captured(self) -> None:
        # The expensive one: a human sits in this conversation for tens
        # of minutes, and the plan reaches the run state only once the
        # stage COMPLETES. Interrupting here used to destroy every
        # question, answer and draft along with the session.
        runner, sc, wt = self._runner(
            _TDD_FULL, {'plan': 'THE PLAN', 'tests': 't', 'build': 'b'}
        )
        sc.interrupt_approval = True
        sc.transcript_for_label['plan'] = [
            ('assistant', 'Should storage own the retry, or the caller?'),
            ('user', 'Storage. And require mTLS on that channel.'),
            ('assistant', 'Draft 2: the schema and the retry budget...'),
        ]
        with self.assertRaises(KeyboardInterrupt):
            runner.run()
        doc = wt.artifacts['turns/plan.md']
        self.assertIn('require mTLS on that channel', doc)  # the human
        self.assertIn('Draft 2', doc)                       # the drafts
        self.assertIn('awaiting plan approval', doc)

    def test_a_timed_out_planning_session_is_captured(self) -> None:
        # The same loss as Ctrl-C, through a different door. The wait
        # caps how long the session may sit SILENT, and hitting that cap
        # raised SwarmSessionError past the only handler here
        # (KeyboardInterrupt) — destroying the session unrecorded,
        # because the plan reaches run state only once the stage
        # COMPLETES.
        runner, sc, wt = self._runner(
            _TDD_FULL, {'plan': 'THE PLAN', 'tests': 't', 'build': 'b'}
        )
        sc.timeout_approval = True
        sc.transcript_for_label['plan'] = [
            ('assistant', 'Should storage own the retry, or the caller?'),
            ('user', 'Storage. And require mTLS on that channel.'),
            ('assistant', 'Draft 2: the schema and the retry budget...'),
        ]
        with self.assertRaises(SwarmSessionError):
            runner.run()
        doc = wt.artifacts['turns/plan.md']
        self.assertIn('require mTLS on that channel', doc)  # the human
        self.assertIn('Draft 2', doc)                       # the drafts
        self.assertIn('awaiting plan approval', doc)

    def test_the_humans_words_are_not_credited_to_the_runner(self):
        # A `user` message is the runner's instruction in a writer's
        # turn but the HUMAN's own words in a planning session. Naming
        # either would misattribute a design decision in the one record
        # kept to settle who decided what.
        runner, sc, wt = self._runner(
            _TDD_FULL, {'plan': 'P', 'tests': 't', 'build': 'b'}
        )
        sc.interrupt_approval = True
        sc.transcript_for_label['plan'] = [
            ('user', 'Require mTLS everywhere internal.'),
        ]
        with self.assertRaises(KeyboardInterrupt):
            runner.run()
        doc = wt.artifacts['turns/plan.md']
        self.assertIn('Require mTLS everywhere internal', doc)
        self.assertNotIn('Runner', doc)

    def test_a_clean_run_captures_nothing(self) -> None:
        _r, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertEqual(
            [k for k in wt.artifacts if k.startswith('turns/')], []
        )


class TestFinishedReadersAndJudgesAreFreed(_Base):
    """A reader or judge is never driven again once its stage is done —
    every drive on one is inside its own stage, and both loop-back
    paths resolve only to WRITERS. But their microVMs used to live
    until the CHUNK published: through the refactor, the final review
    and the verification gate, which is the heaviest part of the module
    and exactly when the memory is wanted. Observed live on a 17 GB
    host: a finished judge held 6 GB while the refactor's linker was
    being OOM-killed, with the gate about to ask for 8 GB more."""

    @staticmethod
    def _replies(**kw) -> dict:
        return {
            'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-a',
            'refactor': 'cleaned up',
            'review-r-sec': 'VERDICT: APPROVED',
            **kw,
        }

    def test_the_judges_vm_is_gone_before_the_refactor_boots(self) -> None:
        # THE point: the memory has to be back before the heavy build
        # starts, not at publish.
        _r, sc, _wt = self._run(_JUDGE_REFACTOR, self._replies())
        order = [
            (k, lb) for k, lb in sc.events
            if lb in ('pick', 'refactor') and k in ('create', 'dispose')
        ]
        self.assertEqual(
            order,
            [('create', 'pick'), ('dispose', 'pick'),
             ('create', 'refactor'), ('dispose', 'refactor')],
        )

    def test_a_writer_keeps_its_session_for_a_possible_re_drive(self):
        # A blocking review or a failed gate re-drives a writer, and
        # the fix turn is meant to inherit the review context. Freeing
        # it early would throw that away.
        _r, sc, _wt = self._run(
            _JUDGE_REFACTOR,
            self._replies(
                **{'review-r-sec': [
                    'VERDICT: BLOCKING — bounds',
                    'VERDICT: APPROVED',
                ]}
            ),
        )
        rf = next(s for s, lb in sc._label.items() if lb == 'refactor')
        # Its only dispose is the teardown one, at the very end.
        self.assertEqual(
            [lb for k, lb in sc.events
             if k == 'dispose' and lb == 'refactor'],
            ['refactor'],
        )
        self.assertIn(rf, sc.disposed)

    def test_a_reader_is_freed_too(self) -> None:
        _r, sc, _wt = self._run(
            _PLAN_JUDGE_REFACTOR,
            {'plan': 'PLAN', 'impl-a': 'A', 'impl-b': 'B',
             'pick': 'SELECT: impl-a', 'refactor': 'cleaned'},
        )
        self.assertIn(('dispose', 'plan'), sc.events)

    def test_the_planning_record_survives_being_freed(self) -> None:
        # Disposing DELETES a session, and the planner's conversation is
        # committed beside its plan at PUBLISH — long after. Read it out
        # before freeing the VM or that record dies with it.
        _r, sc, wt = self._run(
            _LINEAR,
            dict(_LINEAR_REPLIES),
            transcript={'plan': [
                ('assistant', 'Should storage own the retry?'),
                ('user', 'Storage. And require mTLS on that channel.'),
            ]},
        )
        self.assertIn(('dispose', 'plan'), sc.events)  # it WAS freed
        doc = next(
            c for _n, path, c in wt.tracked_files
            if path == 'docs/plans/demo-session.md'
        )
        self.assertIn('require mTLS on that channel', doc)

    def test_a_freed_session_is_not_named_in_the_run_state(self) -> None:
        # A resume reads that key to reclaim the previous attempt's
        # VMs; naming one that is already gone makes the reclaim count
        # a lie.
        _r, sc, wt = self._run(_JUDGE_REFACTOR, self._replies())
        pick = next(s for s, lb in sc._label.items() if lb == 'pick')
        mid = [st for st in wt.states if pick not in st['sessions']]
        self.assertTrue(mid, 'the freed judge was never dropped')

    def test_keep_frees_nothing(self) -> None:
        _r, sc, _wt = self._run(
            _JUDGE_REFACTOR, self._replies(), keep=True
        )
        self.assertEqual(sc.disposed, [])

    def test_a_failed_release_leaves_it_tracked_for_teardown(self) -> None:
        # A delete that fails means the VM may still be up; dropping it
        # here would strand a guest nothing knows about.
        cfg = self._cfg(_JUDGE_REFACTOR)
        wt, sc = FakeWT(), FakeSC(self._replies())
        original = sc.create

        def wedge_the_judge(**kw):
            sid = original(**kw)
            if (kw.get('title') or '').endswith('/pick'):
                sc.dispose_raises.add(sid)
            return sid

        sc.create = wedge_the_judge
        R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        pick = next(s for s, lb in sc._label.items() if lb == 'pick')
        # Tried at its stage, and again at teardown.
        self.assertGreaterEqual(sc.dispose_attempts.count(pick), 2)


class TestWriterIsTerminal(_Base):
    """Exactly two things re-drive a writer: a review stage naming it
    in on_block, and the verification gate looping back to the writer
    that produced the published branch. Anything else is finished when
    its stage ends — in the live cadre the TDD writer, which held a
    full guest from minutes into a module until it published."""

    def test_a_gated_writer_is_never_terminal(self) -> None:
        cfg = self._cfg(_TDD_JUDGE)        # publish.branch: refactor
        for gated in ('impl-a', 'impl-b'):
            with self.subTest(gated=gated):
                self.assertFalse(R.writer_is_terminal(cfg, gated))

    def test_the_publish_target_is_never_terminal(self) -> None:
        # The verification gate loops back to it.
        cfg = self._cfg(_TDD_JUDGE)
        self.assertFalse(R.writer_is_terminal(cfg, 'refactor'))

    def test_an_ungated_candidate_is_terminal(self) -> None:
        # _JUDGE_REFACTOR puts no review on its two coders. The judge
        # reads a fresh CLONE of each candidate, never their sessions,
        # and the gate loops back to `refactor` — so nothing drives
        # them again and holding them would be waste.
        cfg = self._cfg(_JUDGE_REFACTOR)
        self.assertTrue(R.writer_is_terminal(cfg, 'impl-a'))

    def test_an_untargetable_writer_is_terminal(self) -> None:
        # THE case: a tdd writer with no gate that nothing publishes.
        cfg = self._cfg(_TDD_JUDGE)
        self.assertTrue(R.writer_is_terminal(cfg, 'tests'))

    def test_publishing_the_last_writer_releases_nothing(self) -> None:
        # `publish.branch` unset means the gate's target is not known
        # until the run ends, so no writer may be assumed safe.
        cfg = self._cfg(_TDD_JUDGE.replace('  branch: refactor\n', ''))
        self.assertFalse(R.writer_is_terminal(cfg, 'tests'))

    def test_publishing_a_judge_pick_releases_nothing(self) -> None:
        # The winner is one of the judge's candidates, resolved only
        # once it has run — so no writer can be named safe statically.
        cfg = self._cfg(_TDD_JUDGE.replace('branch: refactor', 'branch: pick'))
        self.assertFalse(R.writer_is_terminal(cfg, 'tests'))

    def test_it_is_freed_at_stage_completion(self) -> None:
        _r, sc, _wt = self._run(
            _TDD_JUDGE,
            {'tests': 't', 'impl-a': 'A', 'impl-b': 'B',
             'pick': 'SELECT: impl-a', 'refactor': 'clean',
             'review-r-sec': 'VERDICT: APPROVED'},
        )
        order = [k for k, lb in sc.events if lb == 'tests']
        self.assertEqual(order, ['create', 'dispose'])

    def test_late_writes_are_landed_before_the_guest_goes(self) -> None:
        # Freeing the VM ends the agent's writes, and a native-terminal
        # writer keeps working past its own settle — so whatever
        # arrived late has to be committed first or it is lost.
        cfg = self._cfg(_TDD_JUDGE)
        wt, sc = FakeWT(), FakeSC({
            'tests': 't', 'impl-a': 'A', 'impl-b': 'B',
            'pick': 'SELECT: impl-a', 'refactor': 'clean',
            'review-r-sec': 'VERDICT: APPROVED',
        })
        original = wt.commit_node

        def commit_then_write_more(run_id, node_id, **kw):
            out = original(run_id, node_id, **kw)
            if node_id == 'tests' and 'implement' in kw['message']:
                wt.dirty_nodes.add(node_id)
            return out

        wt.commit_node = commit_then_write_more
        R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        msgs = [m for _n, m, _a in wt.commits]
        self.assertIn('tests: late write', msgs)
        self.assertLess(
            msgs.index('tests: late write'),
            len(msgs),                     # committed, not dropped
        )

    def test_a_gated_writer_keeps_its_vm_for_the_fix_turn(self) -> None:
        _r, sc, _wt = self._run(
            _TDD_JUDGE,
            {'tests': 't', 'impl-a': 'A', 'impl-b': 'B',
             'pick': 'SELECT: impl-a', 'refactor': 'clean',
             'review-r-sec': ['VERDICT: BLOCKING — x', 'VERDICT: APPROVED']},
        )
        # refactor is disposed once, at teardown — never mid-module.
        self.assertEqual(
            [k for k, lb in sc.events if lb == 'refactor'],
            ['create', 'dispose'],
        )

    def test_the_disk_estimate_uses_the_same_rule(self) -> None:
        # The estimate exists to model what the runner does; the last
        # time one changed without the other it demanded 14 GB that no
        # longer got used.
        cfg = self._cfg(_TDD_JUDGE)
        # Freed: writers impl-a/impl-b/refactor (all re-drivable) plus
        # the largest single review stage. `tests` is terminal and the
        # judge is a reader-shaped node, so neither is part of a peak.
        self.assertEqual(R.max_concurrent_vms(cfg), 4)
        # --keep frees nothing: 4 writers + judge + 3 reviewers.
        self.assertEqual(R.max_concurrent_vms(cfg, reclaim=False), 8)


class TestReviewRecords(_Base):
    """A reviewer's report is the reasoning behind a decision to ship,
    and the reviewer does not outlive its own vote — its microVM is
    disposed as soon as the round is decided, and disposing a session
    deletes the transcript. So the report is captured first and kept in
    two places: the run dir immediately, the repo at publish."""

    def _reviewed(self, replies=None, **kw):
        return self._run(
            _LINEAR,
            replies or {'plan': 'P', 'build': 'b',
                        'review-sec': 'Ran the suite.\nVERDICT: APPROVED'},
            transcript={'review-sec': [
                ('user', 'Review the branch.'),
                ('assistant', 'I installed the toolchain and ran it.'),
                ('assistant', 'Ran the suite.\nVERDICT: APPROVED'),
            ]},
            **kw,
        )

    def test_an_approving_report_is_kept(self) -> None:
        # THE case that was silently dropped: on APPROVED the outputs
        # dict was discarded on return, so the reasoning behind shipping
        # was the one thing never written down.
        _r, _sc, wt = self._reviewed()
        doc = wt.artifacts['reviews/review-sec-r1.md']
        self.assertIn('installed the toolchain and ran it', doc)
        self.assertIn('APPROVED', doc)

    def test_it_is_captured_before_the_vm_is_freed(self) -> None:
        # The fake returns nothing for a disposed session, exactly as a
        # deleted session does — so a report with content proves the
        # capture beat the disposal.
        _r, sc, wt = self._reviewed()
        self.assertTrue(sc.disposed)  # it WAS freed
        self.assertIn(
            'installed the toolchain',
            wt.artifacts['reviews/review-sec-r1.md'],
        )

    def test_the_runners_own_turn_is_folded_away(self) -> None:
        # Identical for every reviewer in a stage; repeating it at full
        # length above each report would bury the reports.
        _r, _sc, wt = self._reviewed()
        doc = wt.artifacts['reviews/review-sec-r1.md']
        self.assertIn('What the reviewer was asked', doc)
        self.assertLess(doc.index('<details>'), doc.index('Review the'))

    def test_every_round_is_recorded_separately(self) -> None:
        _r, _sc, wt = self._reviewed(
            {'plan': 'P', 'build': 'b', 'review-sec': [
                'pool.rs:88 races.\nVERDICT: BLOCKING',
                'Fixed now.\nVERDICT: APPROVED',
            ]},
        )
        self.assertEqual(
            sorted(k for k in wt.artifacts if k.startswith('reviews/')),
            ['reviews/review-sec-r1.md', 'reviews/review-sec-r2.md'],
        )

    def test_an_unreadable_session_falls_back_to_the_reply(self) -> None:
        # Better a partial record than none, and never a failed review.
        result, _sc, wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b',
             'review-sec': 'Checked it.\nVERDICT: APPROVED'},
            transcript={'review-sec': []},
        )
        self.assertEqual(result.status, 'completed')
        self.assertIn(
            'Checked it', wt.artifacts['reviews/review-sec-r1.md']
        )

    def test_the_reports_are_committed_to_the_published_branch(self) -> None:
        _r, _sc, wt = self._reviewed()
        self.assertIn(
            'docs/plans/demo-reviews.md',
            [path for _wt, path, _c in wt.tracked_files],
        )
        self.assertIn(
            'docs: add reviewer reports', [m for _, m, _ in wt.commits]
        )

    def test_the_pull_request_shows_the_roster_and_the_link(self) -> None:
        _r, _sc, wt = self._reviewed()
        body = wt.pr_bodies[-1]
        self.assertIn('Who reviewed it', body)
        self.assertIn('`review-sec`', body)
        self.assertIn('APPROVED', body)
        self.assertIn('docs/plans/demo-reviews.md', body)

    def test_a_blocked_round_is_visible_in_the_pull_request(self) -> None:
        # A reader should see that round 1 blocked without opening a
        # file — a clean roster hides the round that found something.
        _r, _sc, wt = self._reviewed(
            {'plan': 'P', 'build': 'b', 'review-sec': [
                'pool.rs:88 races.\nVERDICT: BLOCKING',
                'Fixed now.\nVERDICT: APPROVED',
            ]},
        )
        body = wt.pr_bodies[-1]
        self.assertIn('| `review-sec` | 1 | BLOCKING |', body)
        self.assertIn('| `review-sec` | 2 | APPROVED |', body)

    def test_they_survive_a_resume(self) -> None:
        # The run state carries them, so a resumed run still publishes
        # the rounds that ran before the crash.
        _r, _sc, wt = self._reviewed()
        reviews = wt.states[-1]['reviews']
        self.assertEqual(
            [(r['stage'], r['reviewer'], r['round_no'], r['verdict'])
             for r in reviews],
            [('review', 'sec', 1, 'APPROVED')],
        )
        restored = [R.ReviewRecord.from_dict(r) for r in reviews]
        self.assertIn(
            'installed the toolchain', restored[0].turns[1][1]
        )

    def test_malformed_state_is_skipped_not_fatal(self) -> None:
        self.assertIsNone(R.ReviewRecord.from_dict({'stage': 'review'}))
        self.assertIsNone(R.ReviewRecord.from_dict({}))


class TestStackedPullRequests(_Base):
    """Every module's request used to target the repo's base branch,
    which is right only once the previous one has merged. While earlier
    requests are open a later one shows their code as its own —
    measured live at 55 files / +12,524 for a module whose own work was
    28 files. Basing each on the module below it shows just that
    module."""

    def _modules(self, text=None, **kw):
        return self._run(
            text or _PER_MODULE,
            {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
            **kw,
        )

    def test_each_module_bases_on_the_one_below_it(self) -> None:
        _r, _sc, wt = self._modules()
        self.assertEqual(
            [(remote, base) for remote, base, _f in wt.pr_bases],
            [
                # The first has nothing below it: the repo's base.
                ('pipeline/r1-m0', None),
                ('pipeline/r1-m1', 'pipeline/r1-m0'),
            ],
        )

    def test_the_fallback_is_always_the_repo_base(self) -> None:
        # The branch below is routinely merged AND DELETED before the
        # next module publishes; a request against a base that is gone
        # fails outright, so the runner always names a way back.
        _r, _sc, wt = self._modules()
        self.assertEqual(
            [fallback for _r, _b, fallback in wt.pr_bases],
            [self._cfg(_PER_MODULE).base_branch] * 2,
        )

    def test_stacking_off_targets_the_repo_base_every_time(self) -> None:
        _r, _sc, wt = self._modules(
            _PER_MODULE.replace(
                'publish: local',
                'publish:\n  mode: local\n  stack: false',
            )
        )
        self.assertEqual(
            [base for _r, base, _f in wt.pr_bases], [None, None]
        )

    def _runner_at(self, active: str, published: set[str]):
        cfg = self._cfg(_PER_MODULE)
        runner = R.PipelineRunner(
            cfg, session_client=FakeSC({}), worktree_manager=FakeWT(),
            run_id='r1', agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner._subtasks = list(cfg.subtasks)
        runner._completed_chunks = set(published)
        runner._active_subtask = next(
            s for s in cfg.subtasks if s.id == active
        )
        return runner

    def test_a_resumed_campaign_continues_the_stack(self) -> None:
        # Derived from what has published, so a campaign resumed from a
        # state file written before stacking existed still stacks —
        # rather than dropping the next module back onto the repo base
        # and going cumulative again.
        self.assertEqual(
            self._runner_at('m1', {'m0'})._stack_base(), 'pipeline/r1-m0'
        )

    def test_the_first_module_has_nothing_below_it(self) -> None:
        self.assertIsNone(self._runner_at('m0', set())._stack_base())

    def test_it_takes_the_last_module_in_ORDER_not_sorted(self) -> None:
        # A set of ids alone would pick the wrong base: 'm10' sorts
        # before 'm2'. The module list supplies the real order.
        runner = self._runner_at('m1', {'m0'})
        runner._subtasks = [
            pipeline.Subtask(id=i, title=i) for i in ('m2', 'm10', 'm1')
        ]
        runner._completed_chunks = {'m2', 'm10'}
        self.assertEqual(runner._stack_base(), 'pipeline/r1-m10')

    def test_an_unpublished_module_is_never_a_base(self) -> None:
        # m0 blocked and never shipped; m1 must not stack onto a branch
        # no request exists for.
        self.assertIsNone(self._runner_at('m1', set())._stack_base())


class TestReviewRecordsPerModule(_Base):
    """Each module publishes separately, so each PR must carry its own
    reviews and only its own."""

    def _yaml(self) -> str:
        # _PER_MODULE is defined further down; built here rather than at
        # class-creation time.
        return _PER_MODULE.replace(
            '  build: {template: coder, model: claude-sonnet-5}',
            '  build: {template: coder, model: claude-sonnet-5}\n'
            '  sec: {template: security-reviewer, model: claude-fable-5}',
        ) + '  - id: review\n    run: [sec]\n    needs: [build]\n' \
            '    gate: consensus\n    on_block: build\n'

    def _modules(self):
        return self._run(
            self._yaml(),
            {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
        )

    def test_each_module_records_its_own_round(self) -> None:
        _r, _sc, wt = self._modules()
        self.assertEqual(
            sorted(k for k in wt.artifacts if k.startswith('reviews/')),
            ['reviews/m0-review-sec-r1.md', 'reviews/m1-review-sec-r1.md'],
        )

    def test_a_modules_pull_request_carries_only_its_own(self) -> None:
        # The buffer accumulates across the whole run; without filtering
        # by module, m1's PR would republish m0's reviews as its own.
        _r, _sc, wt = self._modules()
        m0_body, m1_body = wt.pr_bodies[0], wt.pr_bodies[1]
        self.assertIn('`m0-review-sec`', m0_body)
        self.assertNotIn('m1-review-sec', m0_body)
        self.assertIn('`m1-review-sec`', m1_body)
        self.assertNotIn('m0-review-sec', m1_body)

    def test_each_module_commits_its_own_record(self) -> None:
        _r, _sc, wt = self._modules()
        paths = [path for _wt, path, _c in wt.tracked_files]
        self.assertIn('docs/plans/mods-m0-reviews.md', paths)
        self.assertIn('docs/plans/mods-m1-reviews.md', paths)


class TestRenderReviewRecords(unittest.TestCase):
    def _rec(self, **kw):
        base = {
            'chunk': None, 'stage': 'review', 'reviewer': 'sec',
            'round_no': 1, 'verdict': 'APPROVED',
            'turns': (('assistant', 'It builds.'),),
        }
        return R.ReviewRecord(**{**base, **kw})

    def test_no_records_renders_nothing(self) -> None:
        self.assertEqual(R.render_review_records([], title='T'), '')

    def test_the_roster_lists_every_vote(self) -> None:
        doc = R.render_review_records(
            [self._rec(), self._rec(reviewer='bugs', verdict='BLOCKING')],
            title='T',
        )
        self.assertIn('| `review-sec` | 1 | APPROVED |', doc)
        self.assertIn('| `review-bugs` | 1 | BLOCKING |', doc)

    def test_a_silent_reviewer_is_recorded_as_such(self) -> None:
        # Not blank: "none stated" is itself the finding.
        doc = R.render_review_records([self._rec(verdict=None)], title='T')
        self.assertIn('none stated', doc)
        self.assertIn('no verdict stated', doc)

    def test_an_unreadable_session_says_so(self) -> None:
        doc = R.render_review_records([self._rec(turns=())], title='T')
        self.assertIn('could not be read', doc)

    def test_the_slug_keeps_rounds_apart(self) -> None:
        # The stage id already carries the module in a campaign, so the
        # record must not prefix it again ("m1-m1-review-sec").
        self.assertEqual(
            self._rec(chunk='m1', stage='m1-review', round_no=2).slug,
            'm1-review-sec-r2',
        )


_COMPETE_REVIEWED = """\
name: race
repo: https://github.com/org/proj.git
publish:
  branch: pick
task: |
  implement it
agents:
  ca: {template: coder, model: claude-sonnet-5}
  cb: {template: coder, harness: codex-native, model: gpt-5}
  sec: {template: security-reviewer, model: claude-fable-5}
  jg: {template: judge, model: claude-opus-4-8}
stages:
  - id: impl
    parallel:
      - {id: impl-a, run: ca, write: true}
      - {id: impl-b, run: cb, write: true}
  - {id: review-a, run: [sec], needs: [impl-a], on_block: impl-a}
  - {id: review-b, run: [sec], needs: [impl-b], on_block: impl-b}
  - id: pick
    run: jg
    needs: [impl-a, impl-b]
    selects: branch
"""


#: Both candidates reviewed and approved, judge decides cleanly.
_REVIEWED_REPLIES = {
    'impl-a': 'A', 'impl-b': 'B',
    'review-a-sec': 'VERDICT: APPROVED',
    'review-b-sec': 'VERDICT: APPROVED',
    'pick': 'SELECT: impl-a',
}


def _rec(reviewer='bugs', target='impl-a', round_no=1, findings=(),
         chunk='topology', verdict='APPROVED'):
    return R.ReviewRecord(
        chunk=chunk, stage=f'{chunk}-review-a', reviewer=reviewer,
        round_no=round_no, verdict=verdict, findings=tuple(findings),
        target=target,
    )


class TestTheFindingsLedgerIsAppendOnly(unittest.TestCase):
    """
    The ledger is a HUMAN-EDITED artifact: a person annotates status and
    reasoning on it, across modules and across runs. So the runner reads
    what is on the branch and adds to it. A renderer that rebuilt the
    document each run would clobber exactly what it exists to hold
    (TASKS.md #10).
    """

    def _render(self, records, existing=None):
        return R.render_findings_ledger(
            records, title='Findings — demo', existing=existing,
            report_doc='docs/plans/demo-reviews.md',
        )

    def test_a_first_run_creates_the_document(self) -> None:
        doc = self._render([_rec(findings=['pagination is missing'])])
        self.assertIn('# Findings — demo', doc)
        self.assertIn('pagination is missing', doc)
        self.assertIn('**Status:** open', doc)

    def test_the_entry_says_which_candidate_and_which_reviewer(
        self,
    ) -> None:
        # A finding without the candidate it was raised against is
        # meaningless: two are reviewed per chunk.
        doc = self._render([_rec(target='impl-b', findings=['x'])])
        self.assertIn('`impl-b`', doc)
        self.assertIn('`bugs`', doc)
        self.assertIn('round 1', doc)

    def test_existing_content_is_carried_through_byte_for_byte(
        self,
    ) -> None:
        # THE critical property. Not "looks similar" — a prefix match on
        # the actual bytes.
        human = (
            '# Findings — demo\n\nMy own notes here.\n\n---\n\n'
            '## `[topology/impl-a/bugs/r1#1]`\n\n'
            '**Status:** accepted — fixing in [identities]\n\n'
            'pagination is missing\n'
        )
        doc = self._render(
            [_rec(findings=['pagination is missing']),
             _rec(reviewer='sec', findings=['a second, new one'])],
            existing=human,
        )
        self.assertTrue(
            doc.startswith(human.rstrip('\n')),
            "the human's existing bytes were not preserved",
        )
        self.assertIn('accepted — fixing in [identities]', doc)
        self.assertIn('a second, new one', doc)

    def test_a_finding_already_present_is_not_added_again(self) -> None:
        rec = _rec(findings=['pagination is missing'])
        first = self._render([rec])
        again = self._render([rec], existing=first)
        self.assertIsNone(again, 'a re-publish duplicated an entry')

    def test_an_id_the_human_moved_still_counts_as_present(self) -> None:
        # Substring match on the id, so reorganising the file — which is
        # the point of it being editable — never resurrects an entry.
        rec = _rec(findings=['pagination is missing'])
        ident = R.finding_id(rec, 1)
        scrambled = f'notes\n\n## Later\n\nsee `[{ident}]` — handled\n'
        self.assertIsNone(self._render([rec], existing=scrambled))

    def test_the_same_issue_from_two_reviewers_is_kept_twice(self) -> None:
        # Duplicates are provenance. Merging them is a judgement
        # call and it is the human's, not the runner's.
        doc = self._render([
            _rec(reviewer='bugs', findings=['pagination is missing']),
            _rec(reviewer='sec', findings=['pagination is missing']),
        ])
        self.assertEqual(doc.count('pagination is missing'), 2)

    def test_a_re_raise_in_a_later_round_is_a_separate_entry(self) -> None:
        # A finding raised again after a round of work is a different
        # event, not a duplicate to swallow.
        doc = self._render([
            _rec(round_no=1, findings=['still unpaginated']),
            _rec(round_no=2, findings=['still unpaginated']),
        ])
        self.assertIn('r1#1', doc)
        self.assertIn('r2#1', doc)

    def test_nothing_to_add_writes_nothing(self) -> None:
        self.assertIsNone(self._render([]))
        self.assertIsNone(self._render([_rec(findings=[])]))

    def test_the_header_says_the_runner_will_not_clobber_it(self) -> None:
        # The invitation to edit is the feature; without it a human has
        # no reason to trust their annotations will survive.
        doc = self._render([_rec(findings=['x'])])
        self.assertIn('only ever APPENDS', doc)
        self.assertIn('TRACKED, not fixed', doc)


#: The same pipeline publishing LOCALLY, where there is no tracker to
#: file into and the committed ledger is still the right answer.
_COMPETE_REVIEWED_LOCAL = _COMPETE_REVIEWED.replace(
    'publish:\n  branch: pick\n',
    'publish:\n  mode: local\n  branch: pick\n',
).replace('repo: https://github.com/org/proj.git', 'repo: ./proj')


#: Both reviewers raise a non-blocking finding and still approve.
_FINDING_REPLIES = {
    'impl-a': 'A', 'impl-b': 'B',
    'review-a-sec': 'FINDINGS:\n- impl-a leaks a handle\n\n'
                    'VERDICT: APPROVED',
    'review-b-sec': 'FINDINGS:\n- impl-b skips pagination\n\n'
                    'VERDICT: APPROVED',
    'pick': 'SELECT: impl-a',
}


_ROUTED_REPLIES = {
    'impl-a': 'A', 'impl-b': 'B',
    'review-a-sec': 'DEFECTS:\n- impl-a leaks a handle\n\n'
                    'LATER-INCREMENT:\n- pagination belongs to [m2]\n\n'
                    'VERDICT: APPROVED',
    'review-b-sec': 'LATER-INCREMENT:\n- pagination belongs to [m2]\n\n'
                    'PREMISES:\n- I assumed the writer is single-threaded'
                    '\n\nVERDICT: APPROVED',
    'pick': 'SELECT: impl-a',
}


class TestDispositionsDecideWhatIsFiled(_Base):
    """
    The gate's conclusions reaching the tracker.

    The risk being managed is that the gate withholds something it
    should not have. So a withheld finding gets MORE provenance than a
    filed one — verdict, reason, reviewer, positional id — and stays
    raisable, rather than being quietly marked as dealt with.
    """

    def _runner(self, wt):
        cfg = self._cfg(_PER_MODULE)
        runner = R.PipelineRunner(
            cfg, session_client=FakeSC({}), worktree_manager=wt,
            run_id='r1', agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, publish_repo='https://gh/org/proj',
        )
        runner._subtasks = list(cfg.subtasks)
        runner._active_subtask = cfg.subtasks[0]
        runner._reviews = [
            R.ReviewRecord(
                chunk='m0', stage='review', reviewer='sec', round_no=1,
                verdict='APPROVED', target='impl-a',
                findings=('leaks a handle', 'no tests'),
                defects=('leaks a handle', 'no tests'),
            )
        ]
        return runner

    def _conclude(self, runner, verdict, reason):
        for rec in runner._chunk_reviews():
            for index, _text in rec.filed_findings():
                runner._dispositions[R.finding_id(rec, index)] = (
                    R.Disposition(verdict, reason)
                )

    def _file(self, runner, report_doc='docs/plans/mods-reviews.md'):
        runner._file_findings_as_issues(
            [r for r in runner._chunk_reviews() if r.findings], report_doc
        )

    def test_no_dispositions_at_all_files_everything(self) -> None:
        # The pre-gate behaviour, which is also what a dead verifier,
        # an empty reply and an unreadable one all produce.
        wt = FakeWT()
        self._file(self._runner(wt))
        titles = [i['title'] for i in wt.issues]
        self.assertEqual(len(titles), 2, titles)
        self.assertFalse(any('withheld' in t for t in titles), titles)

    def test_a_reproduces_verdict_files_exactly_as_before(self) -> None:
        wt = FakeWT()
        runner = self._runner(wt)
        self._conclude(runner, R.DISPOSITION_FILED, 'still there')
        self._file(runner)
        titles = [i['title'] for i in wt.issues]
        self.assertEqual(len(titles), 2, titles)
        self.assertFalse(any('withheld' in t for t in titles), titles)

    def test_withheld_findings_collapse_into_one_summary(self) -> None:
        # One issue, not N — N is the cost this exists to remove. And
        # not zero, which is the outcome worse than N.
        wt = FakeWT()
        runner = self._runner(wt)
        self._conclude(runner, R.DISPOSITION_ABSENT, 'not in this tree')
        self._file(runner)
        titles = [i['title'] for i in wt.issues]
        self.assertEqual(len(titles), 1, titles)
        self.assertIn('withheld', titles[0])
        self.assertIn('[m0]', titles[0])

    def test_the_summary_carries_the_reason_the_id_and_the_report(
        self,
    ) -> None:
        wt = FakeWT()
        runner = self._runner(wt)
        self._conclude(
            runner, R.DISPOSITION_RECORDED, 'pool.py:88 documents it'
        )
        self._file(runner)
        body = wt.issues[0]['body']
        self.assertIn('pool.py:88 documents it', body)
        self.assertIn('leaks a handle', body)
        self.assertIn('m0/impl-a/sec/r1#1', body)
        self.assertIn('docs/plans/mods-reviews.md', body)

    def test_the_summary_says_a_withheld_finding_is_still_live(
        self,
    ) -> None:
        # The gate's whole risk. A reader must know a wrong verdict is
        # recoverable, and that nothing was marked as dealt with.
        wt = FakeWT()
        runner = self._runner(wt)
        self._conclude(runner, R.DISPOSITION_ABSENT, 'checked, gone')
        self._file(runner)
        self.assertIn('still live', wt.issues[0]['body'])

    def test_a_mixed_verdict_files_one_and_summarises_the_other(
        self,
    ) -> None:
        wt = FakeWT()
        runner = self._runner(wt)
        rec = runner._chunk_reviews()[0]
        runner._dispositions[R.finding_id(rec, 1)] = R.Disposition(
            R.DISPOSITION_ABSENT, 'gone'
        )
        runner._dispositions[R.finding_id(rec, 2)] = R.Disposition(
            R.DISPOSITION_FILED, 'real'
        )
        self._file(runner)
        titles = sorted(i['title'] for i in wt.issues)
        self.assertEqual(len(titles), 2, titles)
        self.assertTrue(any('withheld' in t for t in titles), titles)
        self.assertTrue(any('no tests' in t for t in titles), titles)


class TestTheLedgerSurvivesARun(_Base):
    """
    Decisions recorded by an earlier RUN reach this run's planner.

    `_decisions` is filled from run state, which is per-run. A campaign
    that builds one module per run — the shape this project actually
    uses — therefore started every planner with an empty ledger, while
    the committed document sat in the worktree unread. Every decision
    recorded as BINDING on later modules had been binding nothing.
    """

    def _runner(self, wt):
        cfg = self._cfg(_PER_MODULE)
        runner = R.PipelineRunner(
            cfg, session_client=FakeSC({}), worktree_manager=wt,
            run_id='r2', agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner._subtasks = list(cfg.subtasks)
        runner._active_subtask = cfg.subtasks[-1]
        return runner

    def test_a_committed_ledger_reaches_the_next_run_s_planner(
        self,
    ) -> None:
        wt = FakeWT()
        earlier = self._runner(wt)
        earlier._active_subtask = earlier._subtasks[0]
        earlier._decisions = [('m0', 'tuples are upserted, never inserted')]
        committed = earlier._decisions_doc()

        wt.branch_files[earlier._decisions_ledger_path()] = committed
        later = self._runner(wt)
        later._seed_decisions_from_ledger('/w/plan')

        self.assertEqual(
            later._decisions, [('m0', 'tuples are upserted, never inserted')]
        )
        self.assertIn(
            'tuples are upserted, never inserted', later._decisions_block()
        )

    def test_the_planner_stage_actually_seeds_from_the_ledger(
        self,
    ) -> None:
        """The wiring, not just the method.

        Every other test here calls `_seed_decisions_from_ledger`
        directly, which proves it works and NOT that anything invokes
        it — the bug being fixed was precisely that nothing did. This
        drives a real campaign and reads what the planner was sent.
        """
        wt = FakeWT()
        wt.branch_files['docs/plans/mods-decisions.md'] = (
            '# Decisions carried forward — mods\n\n'
            'Recorded by each module.\n\n'
            '## [m0] contracts and core\n\n'
            '- SEEDED-DECISION-MARKER\n'
        )

        _r, sc, _w = self._run(
            _PER_MODULE,
            {'plan': _plan_text(3000), 'tw': 'tests', 'build': 'code'},
            wt=wt,
        )

        # The FIRST module planned in this run, specifically. Seeding
        # one stage too late still reaches the second module — by then
        # `_decisions` is populated — so asserting on any planner at all
        # would pass with the ordering wrong.
        self.assertIn(
            'SEEDED-DECISION-MARKER', sc.message_for_label('m0-plan')
        )

    def test_the_ledger_round_trips_through_the_document(self) -> None:
        # The document IS the channel, so writing and reading it back
        # has to be lossless — otherwise a decision degrades a little
        # on every module that passes it along.
        wt = FakeWT()
        source = self._runner(wt)
        source._decisions = [
            ('m0', 'first decision'),
            ('m0', 'second decision — with an em dash and `code`'),
            ('m1', 'third decision'),
        ]

        parsed = R.parse_decisions_doc(source._decisions_doc())

        self.assertEqual(parsed, source._decisions)

    def test_a_referral_is_not_read_back_as_a_decision(self) -> None:
        # Referrals are recorded as explicitly NOT binding. Reading one
        # back as a decision would promote a reviewer's passing note
        # into a constraint no later module may alter.
        wt = FakeWT()
        source = self._runner(wt)
        source._decisions = [('m0', 'a real decision')]
        source._referrals = [('m0', 'a reviewer note about [m1]')]

        parsed = R.parse_decisions_doc(source._decisions_doc())

        self.assertEqual(parsed, [('m0', 'a real decision')])

    def test_seeding_twice_does_not_double_a_decision(self) -> None:
        # A resumed run already holds them in state.
        wt = FakeWT()
        source = self._runner(wt)
        source._decisions = [('m0', 'only once')]
        wt.branch_files[source._decisions_ledger_path()] = (
            source._decisions_doc()
        )

        source._seed_decisions_from_ledger('/w/plan')

        self.assertEqual(source._decisions, [('m0', 'only once')])

    def test_no_ledger_yet_seeds_nothing_and_is_not_an_error(self) -> None:
        # The first module of a campaign, which is the normal case.
        wt = FakeWT()
        runner = self._runner(wt)

        runner._seed_decisions_from_ledger('/w/plan')

        self.assertEqual(runner._decisions, [])

    def test_a_human_annotation_is_not_swallowed_into_a_decision(
        self,
    ) -> None:
        # The ledger is human-edited — a person annotates status and
        # reasoning on it. An indented note under an item must not be
        # joined onto the decision above it.
        doc = (
            '# Decisions carried forward — mods\n\n'
            'Recorded by each module.\n\n'
            '## [m0] contracts\n\n'
            '- the real decision\n'
            '  <!-- amended by hand on some date -->\n'
            '  a wrapped human note\n'
        )

        self.assertEqual(
            R.parse_decisions_doc(doc), [('m0', 'the real decision')]
        )


class TestLaterIncrementIsRoutedNotFiled(_Base):
    """
    An observation addressed to a later module goes to the ledger.

    An issue is the wrong shape for it: the module that owns it has not
    been planned, so the issue sits in a tracker waiting for a human to
    notice it was meant for somebody else. The decisions ledger is
    already threaded into every later planner's turn, which is exactly
    the audience the reviewer was writing to.
    """

    def _run_gh(self, wt=None, replies=None):
        wt = wt if wt is not None else FakeWT()
        return (*self._run(
            _COMPETE_REVIEWED, dict(replies or _ROUTED_REPLIES), wt=wt
        ), wt)

    def test_a_routed_observation_is_not_filed_as_an_issue(self) -> None:
        *_ignored, wt = self._run_gh()
        titles = [i['title'] for i in wt.issues]
        self.assertFalse(
            any('pagination belongs to' in t for t in titles), titles
        )

    def test_defects_and_premises_are_still_filed(self) -> None:
        *_ignored, wt = self._run_gh()
        titles = [i['title'] for i in wt.issues]
        self.assertTrue(any('leaks a handle' in t for t in titles), titles)
        self.assertTrue(
            any('single-threaded' in t for t in titles), titles
        )

    def test_routing_does_not_renumber_the_findings_that_remain(
        self,
    ) -> None:
        # finding_id is positional. Numbering a FILTERED list would
        # shift every id after the first routed item, and an id that
        # shifts re-files a finding somebody already closed.
        rec = R.ReviewRecord(
            chunk='m1', stage='review-a', reviewer='sec', round_no=1,
            verdict='APPROVED',
            findings=('d', 'routed', 'p'),
            defects=('d',), later_increment=('routed',), premises=('p',),
        )
        self.assertEqual(rec.filed_findings(), [(1, 'd'), (3, 'p')])

    def _campaign_runner(self):
        # The ledger exists only in per-module mode — a flat pipeline
        # has one plan and nothing to carry forward to.
        cfg = self._cfg(_PER_MODULE)
        runner = R.PipelineRunner(
            cfg, session_client=FakeSC({}), worktree_manager=FakeWT(),
            run_id='r1', agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner._subtasks = list(cfg.subtasks)
        runner._active_subtask = cfg.subtasks[0]
        return runner

    def _voted(self, reviewer: str, *routed: str):
        return R.ReviewRecord(
            chunk='m0', stage='review', reviewer=reviewer, round_no=1,
            verdict='APPROVED', findings=routed, later_increment=routed,
        )

    def test_the_same_observation_from_both_reviewers_routes_once(
        self,
    ) -> None:
        # Both reviewers raise the identical note, and each re-raises it
        # every round. A planner needs it once; six copies is noise.
        runner = self._campaign_runner()
        runner._reviews = [
            self._voted('sec', 'pagination belongs to [m1]'),
            self._voted('bugs', 'pagination belongs to [m1]'),
        ]
        runner._record_referrals()
        self.assertEqual(
            runner._decisions_doc().count('pagination belongs to [m1]'), 1
        )

    def test_the_ledger_says_a_routed_item_does_not_bind(self) -> None:
        # A decision is binding and changing one is a halt-and-escalate.
        # A reviewer's passing note is not that, and a planner told
        # otherwise would design around an observation it should have
        # been free to reject.
        runner = self._campaign_runner()
        runner._reviews = [self._voted('sec', 'pagination belongs to [m1]')]
        runner._record_referrals()
        doc = runner._decisions_doc()
        self.assertIn('NOT binding', doc)
        self.assertIn('Raised by review', doc)
        self.assertIn('pagination belongs to [m1]', doc)

    def test_the_planner_block_separates_referrals_from_decisions(
        self,
    ) -> None:
        # Both reach the next planner in one block, so the block itself
        # has to say which of the two it is reading.
        runner = self._campaign_runner()
        runner._decisions = [('m0', 'tuples are upserted, never inserted')]
        runner._reviews = [self._voted('sec', 'pagination belongs to [m1]')]
        runner._record_referrals()
        block = runner._decisions_block()
        self.assertIn('tuples are upserted, never inserted', block)
        self.assertIn('pagination belongs to [m1]', block)
        self.assertIn('NOT decisions', block)


class TestFindingsAreFiledAsIssues(_Base):
    """
    The flat ledger was the wrong shape and #58 was the proof: a chunk's
    branch is cut from the previous chunk's implementation tip, so it
    never carried the previous chunk's docs, so the ledger restarted
    every chunk — and two chunks adding the same path collide on merge.
    A tracker is cross-branch and cross-campaign by construction.
    """

    def _run_gh(self, wt=None, replies=None):
        wt = wt if wt is not None else FakeWT()
        return (*self._run(
            _COMPETE_REVIEWED, dict(replies or _FINDING_REPLIES), wt=wt
        ), wt)

    def test_each_finding_becomes_an_issue(self) -> None:
        *_ignored, wt = self._run_gh()
        titles = [i['title'] for i in wt.issues]
        self.assertEqual(len(wt.issues), 2, titles)
        self.assertTrue(any('leaks a handle' in t for t in titles), titles)
        self.assertTrue(any('skips pagination' in t for t in titles), titles)

    def test_the_loser_s_findings_are_filed_too(self) -> None:
        # The loser's branch is never published, so this is the only
        # place its findings survive — and nobody has asked whether the
        # winner has the same defect.
        *_ignored, wt = self._run_gh()
        bodies = '\n'.join(i['body'] for i in wt.issues)
        self.assertIn('impl-b', bodies)

    def test_the_body_carries_an_invisible_identity(self) -> None:
        *_ignored, wt = self._run_gh()
        body = wt.issues[0]['body']
        self.assertIn('<!-- finding-id:', body)
        # An HTML comment: a human reformatting the issue cannot break
        # dedup by accident.
        self.assertTrue(body.rstrip().endswith('-->'))

    def test_a_finding_already_filed_is_not_filed_again(self) -> None:
        wt = FakeWT()
        rec = R.ReviewRecord(
            chunk=None, stage='review-a', reviewer='sec', round_no=1,
            verdict='APPROVED', findings=('impl-a leaks a handle',),
            target='impl-a',
        )
        wt.issue_bodies = R.finding_marker(R.finding_id(rec, 1))
        self._run_gh(wt=wt)
        filed = [i['title'] for i in wt.issues]
        self.assertEqual(len(filed), 1, f'a duplicate was filed: {filed}')

    def test_a_CLOSED_issue_still_counts_as_filed(self) -> None:
        # The one behaviour that would make the tracker worthless:
        # re-raising something a human has already dealt with. The
        # listing is state=all precisely for this.
        wt = FakeWT()
        rec = R.ReviewRecord(
            chunk=None, stage='review-b', reviewer='sec', round_no=1,
            verdict='APPROVED', findings=('impl-b skips pagination',),
            target='impl-b',
        )
        wt.issue_bodies = (
            'closed long ago\n' + R.finding_marker(R.finding_id(rec, 1))
        )
        self._run_gh(wt=wt)
        self.assertNotIn(
            'skips pagination',
            ' '.join(i['title'] for i in wt.issues),
        )

    def test_an_unreadable_tracker_files_NOTHING(self) -> None:
        # A listing failure makes every id look absent. Filing on that
        # basis is exactly how a tracker fills with duplicates.
        wt = FakeWT()
        wt.issue_bodies = None
        with mock.patch('sbx_omnigent.runner.click.echo') as echo:
            self._run_gh(wt=wt)
        self.assertEqual(wt.issues, [])
        said = ' '.join(str(c.args[0]) for c in echo.call_args_list)
        self.assertIn('risking duplicates', said)

    def test_a_tracker_outage_never_fails_the_publish(self) -> None:
        wt = FakeWT()
        wt.issue_create_fails = True
        result, _sc, _wt = self._run_gh(wt=wt)[:3]
        self.assertEqual(result.status, 'completed')

    def test_github_mode_writes_no_ledger_file(self) -> None:
        *_ignored, wt = self._run_gh()
        self.assertEqual(
            [r for _t, r, _c in wt.tracked_files if 'findings' in r], []
        )

    def test_the_title_is_readable_in_a_tracker_list(self) -> None:
        # Reviewers write markdown; a title opening with a literal ** is
        # noise in a list, and a mid-token cut is worse.
        self.assertEqual(
            R._issue_title_text(
                '**Terraform state migration is undocumented.** Adding '
                '`count` to the role moves its state address.'
            ),
            'Terraform state migration is undocumented',
        )
        long = 'word ' * 60
        self.assertTrue(R._issue_title_text(long).endswith('…'))
        self.assertNotIn('  ', R._issue_title_text(long))

    def test_the_pull_request_names_what_was_filed(self) -> None:
        # A tracker nobody is pointed at is barely better than none.
        *_ignored, wt = self._run_gh()
        body = wt.pr_bodies[-1]
        self.assertIn('Findings raised (not acted on)', body)
        self.assertIn('issues/1', body)
        self.assertIn('Nothing here was changed', body)

    def test_a_clean_review_adds_no_findings_section(self) -> None:
        *_ignored, wt = self._run_gh(replies={
            'impl-a': 'A', 'impl-b': 'B',
            'review-a-sec': 'VERDICT: APPROVED',
            'review-b-sec': 'VERDICT: APPROVED',
            'pick': 'SELECT: impl-a',
        })
        self.assertNotIn('Findings raised', wt.pr_bodies[-1])

    def test_the_issue_says_nothing_was_acted_on(self) -> None:
        *_ignored, wt = self._run_gh()
        body = wt.issues[0]['body']
        self.assertIn('Nothing has been acted on', body)
        self.assertIn('closing it', body)


class TestTheLedgerIsTheNoTrackerFallback(_Base):
    """
    With no GitHub to file into — `publish: local`, or `mode: none` —
    the committed ledger is still the right home for a finding, so it
    survives as the fallback (TASKS.md #58).
    """

    def _ledger(self, wt):
        return next(
            (c for _t, rel, c in wt.tracked_files
             if rel.endswith('-findings.md')),
            None,
        )

    def test_the_reviewers_are_asked_for_the_three_blocks(self) -> None:
        # One blob became three markers: only the first is a defect, and
        # sorting them where they are written is what lets the other two
        # be routed instead of triaged.
        _r, sc, _wt = self._run(_COMPETE_REVIEWED, dict(_FINDING_REPLIES))
        asked = sc.message_for_label('review-a-sec')
        for marker in ('DEFECTS:', 'LATER-INCREMENT:', 'PREMISES:'):
            with self.subTest(marker=marker):
                self.assertIn(marker, asked)
        self.assertIn('RECORDED, not acted on', asked)

    def test_findings_from_BOTH_candidates_are_recorded(self) -> None:
        # The loser's findings matter most: its branch is not published,
        # so this is the only place they survive — and nobody has asked
        # whether the winner has the same defect.
        _r, _sc, wt = self._run(
            _COMPETE_REVIEWED_LOCAL, dict(_FINDING_REPLIES))
        doc = self._ledger(wt)
        self.assertIsNotNone(doc, 'no findings ledger was committed')
        self.assertIn('impl-a leaks a handle', doc)
        self.assertIn('impl-b skips pagination', doc)

    def test_the_ledger_is_pipeline_level_not_per_chunk(self) -> None:
        # It has to accumulate across chunks and runs; a per-chunk path
        # would start empty every time.
        _r, _sc, wt = self._run(
            _COMPETE_REVIEWED_LOCAL, dict(_FINDING_REPLIES))
        rels = [rel for _t, rel, _c in wt.tracked_files
                if rel.endswith('-findings.md')]
        self.assertEqual(rels, ['docs/plans/race-findings.md'])

    def test_a_ledger_already_on_the_branch_is_extended_not_replaced(
        self,
    ) -> None:
        wt = FakeWT()
        wt.branch_files['docs/plans/race-findings.md'] = (
            '# Findings — race\n\nnotes from an earlier module\n'
        )
        self._run(_COMPETE_REVIEWED_LOCAL, dict(_FINDING_REPLIES), wt=wt)
        doc = self._ledger(wt)
        self.assertIn('notes from an earlier module', doc)
        self.assertIn('impl-a leaks a handle', doc)

    def test_a_reviewer_that_raised_nothing_costs_no_commit(self) -> None:
        _r, _sc, wt = self._run(
            _COMPETE_REVIEWED_LOCAL,
            {'impl-a': 'A', 'impl-b': 'B',
             'review-a-sec': 'VERDICT: APPROVED',
             'review-b-sec': 'VERDICT: APPROVED',
             'pick': 'SELECT: impl-a'},
        )
        self.assertIsNone(self._ledger(wt))


_TESTS_ONLY = """\
name: tdd
repo: ./proj
publish: local
task: |
  add the feature
agents:
  tw: {template: tdd-writer, model: claude-sonnet-5}
  ca: {template: coder, model: claude-sonnet-5}
stages:
  - {id: tests, run: tw, write: true, tests_only: true}
  - {id: build, run: ca, write: true, from: tests, needs: [tests]}
"""


class TestATestsStageMayOnlyWriteTests(_Base):
    """
    The pipeline's central claim is that two models implement one frozen
    contract independently and a judge picks between them. A tests stage
    that ships the implementation hands BOTH writers one design, so the
    judge compares two edits of the same code and the second model's
    independent take never exists.

    Observed on `gcp-scope-topology-1`: `identities-tests` committed a
    114-line `ServiceAccountCache` into production code. Its instruction
    already said "you must NOT implement the feature yourself", and its
    own tests never referenced the new type — the suite compiled and
    failed against unmodified source, so nothing forced it. The prompt
    was a request; this is the check.
    """

    #: A clean second pass still has the tests — an empty branch would
    #: trip `_require_implementation`, which is a different gate.
    _CLEAN: ClassVar[list[str]] = ['tests/x_test.rs']
    _REPLIES: ClassVar[dict[str, str]] = {
        'tests': 'wrote tests', 'build': 'implemented',
    }

    def _run_with(self, diff, text=_TESTS_ONLY):
        wt, sc = FakeWT(), FakeSC(dict(self._REPLIES))
        wt.diff_files['tests'] = diff
        return self._run(text, {}, wt=wt, sc=sc), sc, wt

    @staticmethod
    def _turns(sc, label='tests'):
        """Every message sent to *label* — message_for_label gives the
        first, and the re-drive is the second."""
        sid = next((s for s, lb in sc._label.items() if lb == label), None)
        return '\n'.join(m for sess, m in sc.sent if sess == sid)

    # ── the predicate ──────────────────────────────────────────────

    def test_test_code_passes(self) -> None:
        for path in ('tests/thing_test.rs', 'providers/gcp/tests/x.rs',
                     'core/tests/y_test.rs', 'test_thing.py',
                     'src/thing.test.ts', 'conftest.py'):
            with self.subTest(path=path):
                self._run_with([path])   # completes, no raise

    def test_production_code_does_not(self) -> None:
        _r, sc, _wt = self._run_with([['src/lib.rs'], self._CLEAN])
        self.assertIn('src/lib.rs', self._turns(sc))

    def test_a_dependency_manifest_is_excused(self) -> None:
        """
        A test needing a new dev-dependency MUST declare it. Replaying
        this gate over the project's history showed two greenfield
        modules (m1, m5) that would have been blocked for changing a
        `Cargo.toml` and nothing else whatsoever.

        Not invisible: manifests are in this project's `guarded:`
        list, so the edit is still named to the reviewers as a
        required review item. This only stops it halting the stage.
        """
        self._run_with(['tests/x_test.rs', 'Cargo.toml',
                        'providers/gcp/Cargo.toml'])

    def test_a_new_crate_root_is_NOT_excused(self) -> None:
        # The one thing the replay still blocks, and correctly: m0,
        # m2, m3 and m4 each created a crate, and you cannot test a
        # crate that does not exist. That is what SURFACE is for.
        _r, sc, _wt = self._run_with(
            [['providers/gcp/src/lib.rs'], self._CLEAN]
        )
        self.assertIn('providers/gcp/src/lib.rs', self._turns(sc))

    def test_a_lockfile_is_excused(self) -> None:
        # A test that adds a dev-dependency legitimately moves these,
        # and every greenfield module in this project's history did.
        self._run_with(['tests/x_test.rs', 'Cargo.lock'])

    def test_a_stage_without_the_flag_is_never_checked(self) -> None:
        # The gate is opt-in: an ordinary writer changes src by design.
        plain = _TESTS_ONLY.replace(', tests_only: true', '')
        self._run_with([['src/lib.rs'], ['src/lib.rs']], text=plain)

    # ── the re-drive ───────────────────────────────────────────────

    def test_it_is_re_driven_once_rather_than_halting(self) -> None:
        # An unattended run should not stop for something the writer can
        # undo itself. First diff strays, second is clean.
        _r, sc, _wt = self._run_with([['src/lib.rs'], ['tests/x_test.rs']])
        msg = self._turns(sc)
        self.assertIn('TESTS-ONLY stage', msg)
        self.assertIn('src/lib.rs', msg)

    def test_the_ask_explains_why_and_offers_the_way_out(self) -> None:
        _r, sc, _wt = self._run_with([['src/lib.rs'], self._CLEAN])
        msg = self._turns(sc)
        self.assertIn('inherited by BOTH', msg)
        self.assertIn('EXISTING public surface', msg)
        # It must be able to decline rather than implement anyway.
        self.assertIn('do not add it', msg)

    # ── the halt ───────────────────────────────────────────────────

    def test_a_second_violation_halts_and_names_the_paths(self) -> None:
        with self.assertRaises(R.PipelineRunError) as caught:
            self._run_with([['src/a.rs'], ['src/a.rs', 'src/b.rs']])
        said = str(caught.exception)
        self.assertIn('tests-only', said)
        self.assertIn('src/a.rs', said)
        self.assertIn('Nothing was reverted', said)

    # ── layer 1: the planner declares it up front ──────────────────

    def test_the_planner_is_asked_which_increments_need_new_surface(
        self,
    ) -> None:
        # So a human meets this at the approval gate they already sit
        # through, rather than a campaign stopping at 3am.
        r = object.__new__(R.PipelineRunner)
        r._active_subtask = None
        ask = R.PipelineRunner._plan_consolidation_instruction(r)
        self.assertIn('SURFACE:', ask)
        self.assertIn('SURFACE: none', ask)
        self.assertIn('decide up front', ask)


class TestAReviewerThatLostItsRunner(_Base):
    """
    Twice a single reviewer has aborted a multi-hour campaign while its
    three siblings approved — `topology-review-a-sec` and
    `identities-review-a-sec` — both reported as `failed: None`. Both
    showed a CLEAN (code=1000) runner websocket close server-side, and
    the second was exactly 3600.07s after that session's last activity,
    so a timeout rather than a transient fault.

    A reviewer is the safe thing to retry: read-only mount, nothing
    written, no verdict recorded.
    """

    def _sc_failing_first(self, label='review-sec'):
        sc = FakeSC(dict(_LINEAR_REPLIES))
        send = sc.send_and_wait
        seen: list[str] = []

        def once(session, message, **kw):
            if sc._label.get(session, '') == label and label not in seen:
                seen.append(label)
                return SwarmTurnResult('failed', None, '')
            return send(session, message, **kw)

        sc.send_and_wait = once
        return sc

    def test_a_dead_reviewer_is_retried_and_the_run_completes(self) -> None:
        sc = self._sc_failing_first()
        result, _sc, _wt = self._run(_LINEAR, {}, sc=sc)
        self.assertEqual(result.status, 'completed')

    def test_both_guests_are_tracked_so_neither_leaks(self) -> None:
        # The abandoned guest must still reach the stage backstop.
        sc = self._sc_failing_first()
        self._run(_LINEAR, {}, sc=sc)
        booted = [lb for k, lb in sc.events
                  if k == 'create' and lb == 'review-sec']
        freed = [lb for k, lb in sc.events
                 if k == 'dispose' and lb == 'review-sec']
        self.assertEqual(len(booted), 2, 'a second guest was not booted')
        self.assertEqual(len(freed), 2, 'a guest was leaked')

    def test_the_retry_is_announced_with_why_nothing_is_lost(self) -> None:
        sc = self._sc_failing_first()
        with mock.patch('sbx_omnigent.runner.click.echo') as echo:
            self._run(_LINEAR, {}, sc=sc)
        said = ' '.join(str(c.args[0]) for c in echo.call_args_list)
        self.assertIn('attempt 1 died', said)
        self.assertIn('recorded no verdict', said)

    def _sc_raising_first(self, label='review-sec'):
        """A reviewer whose turn never returns, rather than failing."""
        sc = FakeSC(dict(_LINEAR_REPLIES))
        send = sc.send_and_wait
        seen: list[str] = []

        def once(session, message, **kw):
            if sc._label.get(session, '') == label and label not in seen:
                seen.append(label)
                raise R.SwarmSessionError('stream closed mid-turn')
            return send(session, message, **kw)

        sc.send_and_wait = once
        return sc

    def test_a_transport_failure_is_retried_like_a_dead_guest(self) -> None:
        # A turn that never CAME BACK is as safe to retry as one that
        # came back failed — read-only mount, nothing written, no
        # verdict recorded. It escaped because SwarmSessionError and
        # PipelineRunError are in disjoint hierarchies, so the stage
        # died on the first dropped stream or server hiccup.
        sc = self._sc_raising_first()

        result, _sc, _wt = self._run(_LINEAR, {}, sc=sc)

        self.assertEqual(result.status, 'completed')

    def test_the_retry_waits_before_re_driving(self) -> None:
        # Two attempts seconds apart are one attempt with extra steps.
        # Observed live: both attempts of a review stage failed within
        # minutes of each other during a provider incident that had
        # another hour to run.
        sc = self._sc_failing_first()

        with mock.patch.object(R, '_REVIEW_RETRY_BACKOFF_S', 12.5):
            with mock.patch.object(R.time, 'sleep') as slept:
                self._run(_LINEAR, {}, sc=sc)

        self.assertIn(12.5, [c.args[0] for c in slept.call_args_list])

    def test_failing_twice_still_fails_the_stage(self) -> None:
        # Bounded: this is a retry, not a loop.
        sc = FakeSC(dict(_LINEAR_REPLIES))
        sc.fail_labels = {'review-sec'}
        with self.assertRaises(R.PipelineRunError):
            self._run(_LINEAR, {}, sc=sc)

    def test_the_error_carries_what_the_session_knows(self) -> None:
        # "failed: None" is the least useful sentence the runner can
        # produce, and the session was carrying the answer all along.
        sc = FakeSC(dict(_LINEAR_REPLIES))
        sc.fail_labels = {'build'}
        sc.status_for_label['build'] = {
            'runner_online': False,
            'last_task_error': 'runner went away',
            'sandbox_status': 'stopped',
        }
        with self.assertRaises(R.PipelineRunError) as caught:
            self._run(_LINEAR, {}, sc=sc)
        said = str(caught.exception)
        self.assertIn('RUNNER IS OFFLINE', said)
        self.assertIn('runner went away', said)
        self.assertIn('sandbox=stopped', said)

    def test_a_missing_pane_says_so_rather_than_going_quiet(self) -> None:
        # Dropping the suffix silently reproduces exactly the bare
        # "failed: None" that #26 exists to prevent.
        sc = FakeSC(dict(_LINEAR_REPLIES))
        sc.fail_labels = {'build'}
        with self.assertRaises(R.PipelineRunError) as caught:
            self._run(_LINEAR, {}, sc=sc)
        self.assertIn('no pane captured', str(caught.exception))


class TestAVerifiesStageParsesFromYaml(_Base):
    """The declaration a pipeline actually writes, end to end."""

    def _with_gate(self) -> str:
        # Built here, not as a class attribute: _PER_MODULE is defined
        # further down this module and would not exist at class-body
        # evaluation time.
        return (
            _PER_MODULE
            + '  - {id: triage, run: build, verifies: findings, '
              'needs: [build]}\n'
        )

    def test_the_stage_parses_and_routes_to_the_gate(self) -> None:
        cfg = self._cfg(self._with_gate())
        stage = next(s for s in cfg.stages if s.id == 'triage')
        self.assertEqual(stage.verifies, 'findings')
        self.assertEqual(R.PipelineRunner._stage_kind(stage), 'verify')

    def test_a_pipeline_without_one_is_unchanged(self) -> None:
        # The gate is opt-in: every existing pipeline keeps filing
        # exactly as it did.
        cfg = self._cfg(_PER_MODULE)
        self.assertTrue(all(s.verifies is None for s in cfg.stages))


class TestTheVerificationGate(_Base):
    """
    Checking a finding against the code before a human is asked to.

    Most non-blocking findings need no work, and a human was deriving
    that one at a time, days later, without the branch in front of them.
    An agent holding the tree can do it while the context is still true.

    Every test here is really about the same property: the gate may cost
    a wasted triage, it may NOT lose a finding.
    """

    def _runner(self):
        cfg = self._cfg(_PER_MODULE)
        runner = R.PipelineRunner(
            cfg, session_client=FakeSC({}), worktree_manager=FakeWT(),
            run_id='r1', agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner._subtasks = list(cfg.subtasks)
        runner._active_subtask = cfg.subtasks[0]
        runner._reviews = [
            R.ReviewRecord(
                chunk='m0', stage='review', reviewer='sec', round_no=1,
                verdict='APPROVED',
                findings=('leaks a handle', 'belongs to [m1]', 'no tests'),
                defects=('leaks a handle', 'no tests'),
                later_increment=('belongs to [m1]',),
            )
        ]
        return runner

    def test_a_referral_is_not_put_to_the_verifier(self) -> None:
        # It is already routed to the ledger, and checking it against
        # THIS module's code answers the wrong question.
        pending = self._runner()._pending_findings()
        self.assertEqual(
            [text for _rec, _i, text in pending],
            ['leaks a handle', 'no tests'],
        )

    def test_conclusions_are_stored_against_the_positional_id(
        self,
    ) -> None:
        runner = self._runner()
        pending = runner._pending_findings()
        kept = runner._record_dispositions(
            pending,
            {1: R.Disposition(R.DISPOSITION_ABSENT, 'gone in 4622247')},
        )
        self.assertEqual(kept, 1)
        stored = runner._dispositions[
            R.finding_id(pending[0][0], pending[0][1])
        ]
        self.assertEqual(stored.verdict, R.DISPOSITION_ABSENT)
        self.assertEqual(stored.reason, 'gone in 4622247')

    def test_a_finding_with_no_conclusion_still_counts_as_filed(
        self,
    ) -> None:
        # The fail-open rule, at the point it decides. An absent key is
        # not "unknown, ask later" — it is "file it", the same as today.
        runner = self._runner()
        self.assertEqual(
            runner._record_dispositions(runner._pending_findings(), {}), 2
        )

    def test_the_verifier_is_asked_by_NUMBER_not_by_id(self) -> None:
        # A verifier echoing 'm0/-/sec/r1#1' back correctly is a
        # transcription task with a silent failure mode: a mistyped id
        # re-files the finding it was meant to close.
        runner = self._runner()
        instruction = runner._verify_instruction(runner._pending_findings())
        self.assertIn('1. [raised by sec', instruction)
        self.assertIn('2. [raised by sec', instruction)
        self.assertNotIn('m0/-/sec/r1#1', instruction)

    def test_the_verifier_is_told_that_unsure_means_file_it(self) -> None:
        runner = self._runner()
        instruction = runner._verify_instruction(runner._pending_findings())
        self.assertIn('a wasted triage is cheap', instruction.lower())

    def test_a_verifies_stage_is_its_own_kind(self) -> None:
        stage = pipeline.PipelineStage(
            id='triage', run=('sec',), verifies='findings'
        )
        self.assertEqual(R.PipelineRunner._stage_kind(stage), 'verify')

    def test_its_microVM_is_freed_like_a_reader_s(self) -> None:
        # Nothing loops back to a verifier, so holding its guest through
        # publish is the waste _release_completed_session stops.
        runner = self._runner()
        runner._keep = False
        stage = pipeline.PipelineStage(
            id='triage', run=('sec',), verifies='findings'
        )
        runner._nodes['triage'] = R.NodeResult(
            'triage', 'verify', worktree='w', session='s-triage'
        )
        runner._release_completed_session(stage, 'verify')
        self.assertIn('s-triage', runner._released)


class TestParseDispositions(unittest.TestCase):
    """
    A verifier's conclusions, and the rule that it FAILS OPEN.

    Everything here protects one property: an index this parser does not
    return a conclusion for is filed exactly as it is today. A verifier
    that dies, answers nothing, or writes something unreadable costs a
    wasted triage — never a lost finding.
    """

    def test_each_conclusion_is_read_with_its_reason(self) -> None:
        got = R.parse_dispositions(
            'DISPOSITIONS:\n'
            '- 1: reproduces — still leaked at pool.py:88\n'
            '- 2: absent — removed in 4622247\n'
        )
        self.assertEqual(got[1].verdict, R.DISPOSITION_FILED)
        self.assertEqual(got[1].reason, 'still leaked at pool.py:88')
        self.assertEqual(got[2].verdict, R.DISPOSITION_ABSENT)
        self.assertFalse(got[2].files)

    def test_only_reproduces_still_files(self) -> None:
        got = R.parse_dispositions(
            'DISPOSITIONS:\n- 1: reproduces\n- 2: absent\n'
            '- 3: recorded\n- 4: decided\n- 5: duplicate\n'
        )
        self.assertEqual(
            [i for i in sorted(got) if got[i].files], [1]
        )

    def test_the_verdict_may_sit_inside_a_sentence(self) -> None:
        # A verifier writes a conclusion, not a token. "already
        # documented in models.py" carries its verdict in word two.
        got = R.parse_dispositions(
            'DISPOSITIONS:\n'
            '- 1: already documented in models.py\n'
            '- 2: duplicate of 1\n'
        )
        self.assertEqual(got[1].verdict, R.DISPOSITION_RECORDED)
        self.assertEqual(got[1].reason, 'in models.py')
        self.assertEqual(got[2].verdict, R.DISPOSITION_DUPLICATE)

    def test_an_unreadable_line_does_not_discard_the_ones_below_it(
        self,
    ) -> None:
        # The expensive failure: one line a verifier phrased oddly
        # silently swallowing every conclusion after it.
        got = R.parse_dispositions(
            'DISPOSITIONS:\n'
            '- 1: reproduces\n'
            '- 2: I could not tell\n'
            '- 3: absent — checked, it is gone\n'
        )
        self.assertNotIn(2, got)
        self.assertEqual(got[3].verdict, R.DISPOSITION_ABSENT)

    def test_no_block_at_all_yields_no_conclusions(self) -> None:
        # Which files everything — the pre-gate behaviour exactly.
        for text in ('', None, 'I ran out of turns.'):
            with self.subTest(text=text):
                self.assertEqual(R.parse_dispositions(text), {})

    def test_the_last_block_wins_like_every_other_marker(self) -> None:
        got = R.parse_dispositions(
            'DISPOSITIONS:\n- 1: absent\n\n'
            'On reflection.\n\nDISPOSITIONS:\n- 1: reproduces\n'
        )
        self.assertTrue(got[1].files)


class TestFindingSections(unittest.TestCase):
    """
    Sorting the non-blocking output by what each item IS.

    The pre-split blob asked for three different things at once and only
    the first was a defect. Sorting them where they are written is what
    lets a later-increment item be ROUTED to the planner that acts on
    it, and a premise be VERIFIED, instead of all three landing in one
    tracker for a human to re-read and re-classify.
    """

    def test_each_block_is_lifted_into_its_own_section(self) -> None:
        text = (
            'Looks right.\n\n'
            'DEFECTS:\n- the retry budget is undocumented\n\n'
            'LATER-INCREMENT:\n- pagination belongs to [m2]\n\n'
            'PREMISES:\n- I assumed build_providers cannot fail\n\n'
            'VERDICT: APPROVED'
        )
        got = R.FindingSections.of(text)
        self.assertEqual(got.defects, ('the retry budget is undocumented',))
        self.assertEqual(got.later_increment, ('pagination belongs to [m2]',))
        self.assertEqual(
            got.premises, ('I assumed build_providers cannot fail',)
        )

    def test_a_legacy_findings_block_is_read_as_defects(self) -> None:
        # The loudest bucket, deliberately. A reviewer that ignores the
        # new protocol, or a reply written before it existed, must lose
        # nothing, so an uncategorized item gets the MOST attention.
        got = R.FindingSections.of('FINDINGS:\n- something is wrong')
        self.assertEqual(got.defects, ('something is wrong',))
        self.assertEqual(got.later_increment, ())
        self.assertEqual(got.premises, ())

    def test_all_is_the_union_and_puts_defects_first(self) -> None:
        text = (
            'PREMISES:\n- p\n\n'
            'LATER-INCREMENT:\n- l\n\n'
            'DEFECTS:\n- d\n'
        )
        self.assertEqual(R.FindingSections.of(text).all, ('d', 'l', 'p'))

    def test_a_missing_block_is_empty_not_an_error(self) -> None:
        got = R.FindingSections.of('DEFECTS:\n- only this one')
        self.assertEqual(got.defects, ('only this one',))
        self.assertEqual(got.all, ('only this one',))

    def test_nothing_raised_yields_all_empty(self) -> None:
        for text in ('', None, 'VERDICT: APPROVED'):
            with self.subTest(text=text):
                self.assertEqual(R.FindingSections.of(text).all, ())

    def test_the_headers_may_be_decorated_like_every_other_marker(
        self,
    ) -> None:
        for header in ('LATER-INCREMENT:', '## Later-Increment',
                       '**LATER_INCREMENT:**', 'later increment'):
            with self.subTest(header=header):
                self.assertEqual(
                    R.FindingSections.of(f'{header}\n- x').later_increment,
                    ('x',),
                )

    def test_a_legacy_block_and_a_defects_block_both_count(self) -> None:
        # A reviewer that writes both is not made to choose; neither
        # list is dropped.
        got = R.FindingSections.of('DEFECTS:\n- a\n\nFINDINGS:\n- b')
        self.assertEqual(got.defects, ('a', 'b'))


class TestReviewRecordCarriesTheSections(unittest.TestCase):
    """
    The sections survive a resume, and their absence is not a failure.

    State written before the split has no section keys. It must restore
    as "uncategorized" rather than refusing to load — that is the whole
    reason RUN_STATE_VERSION does not move for this change, and a run
    was in flight when it landed.
    """

    def test_the_sections_round_trip_through_state(self) -> None:
        rec = R.ReviewRecord(
            chunk='m1', stage='review-a', reviewer='sec', round_no=1,
            verdict='APPROVED', findings=('d', 'l', 'p'),
            defects=('d',), later_increment=('l',), premises=('p',),
        )
        back = R.ReviewRecord.from_dict(rec.as_dict())
        self.assertEqual(back.defects, ('d',))
        self.assertEqual(back.later_increment, ('l',))
        self.assertEqual(back.premises, ('p',))
        self.assertEqual(back.findings, ('d', 'l', 'p'))

    def test_state_without_the_section_keys_still_loads(self) -> None:
        raw = {
            'chunk': 'm0b', 'stage': 'review-a', 'reviewer': 'sec',
            'round_no': 1, 'verdict': 'APPROVED',
            'findings': ['raised before the split'], 'turns': [],
        }
        back = R.ReviewRecord.from_dict(raw)
        self.assertEqual(back.findings, ('raised before the split',))
        self.assertEqual(back.defects, ())
        self.assertEqual(back.later_increment, ())
        self.assertEqual(back.premises, ())

    def test_a_malformed_section_entry_is_dropped_not_fatal(self) -> None:
        raw = {
            'stage': 'review-a', 'reviewer': 'sec', 'round_no': 1,
            'verdict': 'APPROVED', 'turns': [],
            'defects': ['keep', 7, None, 'also keep'],
        }
        self.assertEqual(
            R.ReviewRecord.from_dict(raw).defects, ('keep', 'also keep')
        )


class TestParseFindings(unittest.TestCase):
    """
    A reviewer's NON-BLOCKING findings are the lossy set: blocking ones
    are acted on in-round, these are archived and never read again.
    Lifted out with the same marker protocol as `VERDICT:` and
    `DECISIONS FOR LATER MODULES:` rather than by parsing prose.
    """

    def test_items_are_lifted_in_order(self) -> None:
        text = (
            'Looks fine overall.\n\n'
            'FINDINGS:\n'
            '- ListServiceAccounts is unpaginated (belongs to [identities])\n'
            '- the retry budget is not documented\n\n'
            'VERDICT: APPROVED'
        )
        self.assertEqual(
            R.parse_findings(text),
            ('ListServiceAccounts is unpaginated (belongs to [identities])',
             'the retry budget is not documented'),
        )

    def test_numbered_and_bulleted_items_both_count(self) -> None:
        text = 'FINDINGS:\n1. first\n2) second\n* third\n• fourth'
        self.assertEqual(
            R.parse_findings(text), ('first', 'second', 'third', 'fourth')
        )

    def test_the_last_block_wins(self) -> None:
        # Same rule as parse_decisions: a reviewer that restates
        # its list after thinking again means the later one.
        text = 'FINDINGS:\n- early\n\nOn reflection.\n\nFINDINGS:\n- late'
        self.assertEqual(R.parse_findings(text), ('late',))

    def test_blank_lines_inside_the_list_are_tolerated(self) -> None:
        text = 'FINDINGS:\n- one\n\n- two'
        self.assertEqual(R.parse_findings(text), ('one', 'two'))

    def test_prose_after_the_list_ends_it(self) -> None:
        text = 'FINDINGS:\n- one\nThat is all.\n- not a finding'
        self.assertEqual(R.parse_findings(text), ('one',))

    def test_the_header_may_be_decorated(self) -> None:
        for header in ('FINDINGS:', '## FINDINGS', '**FINDINGS:**',
                       '  findings :  '):
            with self.subTest(header=header):
                self.assertEqual(
                    R.parse_findings(f'{header}\n- x'), ('x',)
                )

    def test_no_block_yields_nothing(self) -> None:
        # NOT an error: a reviewer with nothing to raise is the normal
        # case. The caller falls back to keeping the whole report.
        for text in ('', None, 'VERDICT: APPROVED', 'findings were none'):
            with self.subTest(text=text):
                self.assertEqual(R.parse_findings(text), ())

    def test_an_empty_block_yields_nothing(self) -> None:
        self.assertEqual(R.parse_findings('FINDINGS:\n\nVERDICT: OK'), ())


class TestTheIncrementBoundsEveryRole(_Base):
    """
    Every role is handed the WHOLE plan's brief as its contract, with
    the active increment as a one-line qualifier above it. On
    `gcp-scope-topology-1` the [topology] bug reviewer read a brief
    saying "pagination on every listing call", found
    `ListServiceAccounts` unpaginated, and blocked — faithfully,
    because nothing told it that pagination is [identities]'s row.
    The writer, for whom a blocking finding is not optional,
    implemented it inside a [topology] fix turn, and the judge then
    scored that as a strength.
    """

    _SUBTASKS = ('transport', 'topology', 'identities')

    def _runner(self, active='topology', published=('transport',)):
        cfg = self._cfg(_COMPETE_REVIEWED)
        runner = R.PipelineRunner(
            cfg, session_client=FakeSC({}), worktree_manager=FakeWT(),
            run_id='r1', agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner._subtasks = [
            pipeline.Subtask(id=i, title=f'do the {i} work')
            for i in self._SUBTASKS
        ]
        runner._completed_chunks = set(published)
        runner._active_subtask = next(
            s for s in runner._subtasks if s.id == active
        )
        return runner

    # ── the list itself ────────────────────────────────────────────

    def test_every_increment_is_named_not_just_the_active_one(self) -> None:
        pre = self._runner()._chunk_preamble()
        for other in self._SUBTASKS:
            with self.subTest(increment=other):
                self.assertIn(f'[{other}]', pre)

    def test_the_rows_say_which_is_done_which_is_yours_which_waits(
        self,
    ) -> None:
        pre = self._runner()._chunk_preamble()
        self.assertIn('✓ [transport]', pre)
        self.assertIn('▶ [topology]', pre)
        self.assertIn('  [identities]', pre)

    def test_the_active_title_is_printed_once_not_twice(self) -> None:
        # The table carries every title, marked, so repeating the active
        # one in the opening sentence is pure duplication — and a plan
        # whose rows are full scope paragraphs pays it in every turn.
        # Observed at 739 characters on [identities].
        pre = self._runner()._chunk_preamble()
        self.assertEqual(pre.count('do the topology work'), 1)

    def test_it_says_another_row_s_requirement_is_not_missing(self) -> None:
        # The whole point: "nobody has done this" becomes "row
        # [identities] owns this", which is the only form a reviewer can
        # reasonably decline to block on.
        self.assertIn('NOT missing', self._runner()._chunk_preamble())

    def test_the_reviewer_and_the_judge_see_it_too_not_only_the_writer(
        self,
    ) -> None:
        r = self._runner()
        stage = r._stage_by_id['review-a']
        self.assertIn('[identities]', r._review_instruction(stage))
        self.assertIn(
            '[identities]',
            r._judge_instruction(
                r._stage_by_id['pick'], ['impl-a', 'impl-b']
            ),
        )

    # ── the reviewer's scope rule ──────────────────────────────────

    def test_a_later_increment_s_work_is_not_blocking(self) -> None:
        r = self._runner()
        msg = r._review_instruction(r._stage_by_id['review-a'])
        self.assertIn('BELONGING TO A LATER INCREMENT', msg)
        self.assertIn('do not hold the gate for it', msg)

    def test_but_a_defect_this_increment_introduced_still_is(self) -> None:
        # Without this half, "that file belongs to a later increment"
        # becomes a shield for something this change actually broke.
        r = self._runner()
        msg = r._review_instruction(r._stage_by_id['review-a'])
        self.assertIn('INTRODUCED is', msg)
        self.assertIn('is not a shield', msg)

    # ── the writer's one way out ───────────────────────────────────

    def test_the_writer_may_defer_a_later_increment_s_finding(self) -> None:
        msg = self._runner()._fix_instruction('some findings')
        self.assertIn('DEFERRED: [<increment-id>]', msg)

    def test_deferring_is_the_only_alternative_to_fixing(self) -> None:
        # A blocking finding stays unconditional in every other respect;
        # this is a named, recorded move, not permission to skip.
        msg = self._runner()._fix_instruction('some findings')
        self.assertIn('Everything you do NOT defer, you fix', msg)
        self.assertIn('not silence, and not a partial fix', msg)

    # ── the judge ──────────────────────────────────────────────────

    def test_the_judge_counts_scope_as_a_cost_not_a_bonus(self) -> None:
        r = self._runner()
        r._reviewed_ok.update({'impl-a', 'impl-b'})
        msg = r._judge_instruction(
            r._stage_by_id['pick'], ['impl-a', 'impl-b']
        )
        self.assertIn('STAYED INSIDE THE MARKED', msg)
        self.assertIn('not thereby ahead', msg)
        self.assertIn('same way for every candidate', msg)

    def test_the_judge_is_no_longer_told_the_first_candidate_wins(
        self,
    ) -> None:
        # It was, until the fallback was removed. A judge told its
        # silence merely discards its own opinion is being understated
        # to: silence now stops the run.
        r = self._runner()
        msg = r._judge_instruction(
            r._stage_by_id['pick'], ['impl-a', 'impl-b']
        )
        self.assertNotIn('taken by default', msg)
        self.assertIn('the run HALTS', msg)

    # ── and none of it outside a campaign ──────────────────────────

    def test_a_flat_run_hears_none_of_this(self) -> None:
        # No increments means no boundary to police, and the whole brief
        # really is this branch's job.
        r = self._runner()
        r._active_subtask = None
        stage = r._stage_by_id['review-a']
        self.assertEqual(r._chunk_preamble(), '')
        self.assertNotIn('LATER INCREMENT', r._review_instruction(stage))
        self.assertNotIn('DEFERRED:', r._fix_instruction('f'))
        self.assertNotIn(
            'STAYED INSIDE',
            r._judge_instruction(
                r._stage_by_id['pick'], ['impl-a', 'impl-b']
            ),
        )


class TestSettledDecisionsReachTheReviewers(_Base):
    """
    A reviewer that cannot see what the human already settled either
    re-litigates it or blocks on an authorized deviation.

    Observed live on `ingestion-m3-1`. The [m3c] planner recorded
    "One frozen assertion was amended under explicit human
    authorization (2026-09-04)" into the decisions ledger, exactly as
    it was asked to. `_decisions_block` fed that ledger to the next
    PLANNER and to nobody else, so both bug reviewers found the
    amended assertion, had no way to know it was authorized, and
    raised DISPUTED. That is the correct move on the evidence they
    held — and it halted the campaign on a question already answered.

    The ledger is where a human ruling lives. A reviewer is the role
    most likely to trip over one.
    """

    _RULING = (
        'One frozen assertion was amended under explicit human '
        'authorization: the integration test now expects no unmapped row'
    )

    def _runner(self, source=_COMPETE_REVIEWED, decisions=True):
        cfg = self._cfg(source)
        runner = R.PipelineRunner(
            cfg, session_client=FakeSC({}), worktree_manager=FakeWT(),
            run_id='r1', agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        if decisions:
            runner._decisions = [('m3c', self._RULING)]
        return runner

    def test_the_reviewer_is_told_what_the_human_already_settled(
        self,
    ) -> None:
        r = self._runner()
        msg = r._review_instruction(r._stage_by_id['review-a'])
        self.assertIn(self._RULING, msg)

    def test_the_refactor_reviewer_is_told_too(self) -> None:
        # The refactor review is a SEPARATE instruction, and it is the
        # one reviewing the last writer before publish — the worst
        # place to re-open a settled question.
        r = self._runner(source=_JUDGE_REFACTOR)
        r._nodes['refactor'] = R.NodeResult(
            'refactor', 'writer', branch='b/refactor'
        )
        msg = r._refactor_review_instruction(r._stage_by_id['review-r'])
        self.assertIn(self._RULING, msg)

    def test_a_settled_decision_is_not_a_defence_for_a_defect(
        self,
    ) -> None:
        # Without this half, handing a reviewer the ledger buys a
        # different failure: "a decision explains it" becomes a reason
        # to approve code that is actually wrong.
        r = self._runner()
        msg = r._review_instruction(r._stage_by_id['review-a'])
        self.assertIn('not a defence for a defect', msg)

    def test_a_run_with_no_settled_decisions_says_nothing(self) -> None:
        # A dangling empty heading reads as "nothing was settled",
        # which is a claim; silence is not.
        r = self._runner(decisions=False)
        msg = r._review_instruction(r._stage_by_id['review-a'])
        self.assertNotIn('not a defence for a defect', msg)

    def test_referrals_are_not_shown_to_reviewers_as_settled(
        self,
    ) -> None:
        # Referrals are explicitly NOT binding — "the module that owns
        # one decides what to do about it, and deciding it is wrong is
        # a legitimate outcome". Presenting one beside real rulings
        # would let a reviewer treat an open question as closed.
        r = self._runner()
        r._referrals = [('m3c', 'a reviewer wondered about pagination')]
        msg = r._review_instruction(r._stage_by_id['review-a'])
        self.assertNotIn('wondered about pagination', msg)



class TestTheReviewedClaimSurvivesAResume(_Base):
    """
    The judge is told "both candidates already passed their review gate,
    do not rebuild" only when that is true. The flag recording it lived
    in memory, so a --resume lost it — and an untold judge starts a
    workspace build it cannot finish inside its one turn, states no
    SELECT, and (before this change) the first candidate shipped by
    default.

    Observed live on `gcp-scope-topology-1`: both topology candidates
    had reached APPROVED/APPROVED, the run was resumed, and the judge
    replied only "I have initiated verification builds ... I will
    analyze the test and lint results once compilation completes."
    """

    def _finished_state(self):
        """A completed run's final state snapshot."""
        _r, _sc, wt = self._run(_COMPETE_REVIEWED, dict(_REVIEWED_REPLIES))
        return wt.states[-1]

    def _resumed(self, state, replies=None):
        cfg = self._cfg(_COMPETE_REVIEWED)
        wt, sc = FakeWT(), FakeSC(dict(replies or _REVIEWED_REPLIES))
        wt.state_to_load = state
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, resume=True,
        )
        return runner, sc, wt

    def test_the_claim_is_written_into_the_run_state(self) -> None:
        state = self._finished_state()
        self.assertEqual(
            sorted(state['reviewed_ok']), ['impl-a', 'impl-b']
        )

    def test_a_resume_restores_it(self) -> None:
        runner, _sc, _wt = self._resumed(self._finished_state())
        runner._load_state()
        self.assertEqual(runner._reviewed_ok, {'impl-a', 'impl-b'})
        self.assertTrue(runner._all_reviewed(['impl-a', 'impl-b']))

    def test_a_resumed_judge_is_told_not_to_rebuild(self) -> None:
        # The regression itself, end to end: re-run only the judge from
        # a restored state and assert it is told what the first process
        # knew. Without the persisted claim this message is absent and
        # the judge goes and builds.
        state = self._finished_state()
        state['completed'] = [
            c for c in state['completed'] if c != 'pick'
        ]
        runner, sc, _wt = self._resumed(state)
        runner.run()
        msg = sc.message_for_label('pick')
        self.assertIn('already passed its own review gate', msg)
        self.assertIn('Do NOT build or re-run the suite', msg)

    def test_a_state_predating_the_key_restores_empty(self) -> None:
        # RUN_STATE_VERSION deliberately did NOT move, so a run already
        # in flight stays resumable. The old shape must degrade to the
        # old behaviour rather than raise.
        state = self._finished_state()
        del state['reviewed_ok']
        runner, _sc, _wt = self._resumed(state)
        runner._load_state()
        self.assertEqual(runner._reviewed_ok, set())
        self.assertFalse(runner._all_reviewed(['impl-a', 'impl-b']))

    def test_a_re_driven_writer_loses_the_claim(self) -> None:
        """
        Persisting the claim is only safe because a changed branch drops
        it. A writer sent back over review findings is no longer the
        thing its reviewers ran the suite against, and a later judge
        must not be told otherwise.
        """
        _r, _sc, wt = self._run(
            _COMPETE_REVIEWED,
            {'impl-a': 'A', 'impl-b': 'B',
             # impl-a blocks once, is re-driven, then passes.
             'review-a-sec': ['VERDICT: BLOCKING', 'VERDICT: APPROVED'],
             'review-b-sec': 'VERDICT: APPROVED',
             'pick': 'SELECT: impl-a'},
        )
        # It ends up claimed again — but only because the SECOND review
        # approved the re-driven branch, not because the first did.
        self.assertEqual(
            sorted(wt.states[-1]['reviewed_ok']), ['impl-a', 'impl-b']
        )

    def test_the_claim_is_dropped_the_moment_the_branch_changes(
        self,
    ) -> None:
        # Directly, without a review round in the way: the discard is
        # what makes persistence honest, so assert it on its own.
        cfg = self._cfg(_COMPETE_REVIEWED)
        wt, sc = FakeWT(), FakeSC(dict(_REVIEWED_REPLIES))
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner._nodes['impl-a'] = R.NodeResult(
            'impl-a', 'writer', branch='b', worktree='/wt', session='s1',
        )
        runner._reviewed_ok.add('impl-a')
        runner._redrive_writer('impl-a', 'findings')
        self.assertNotIn('impl-a', runner._reviewed_ok)


class TestAJudgeThatDidNotDecide(_Base):
    """
    A judge replied "I have launched the cargo test execution in the
    background ... I will process the results as soon as they are
    available" and ended its turn. There is no later turn, so its
    opinion was discarded and the first candidate won by default
    (TASKS.md #41).
    """

    def test_it_is_asked_once_more_and_the_answer_counts(self) -> None:
        result, _sc, wt = self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B',
             'pick': ['I have launched the tests in the background and '
                      'will report once they finish.',
                      'SELECT: impl-b']},
        )
        self.assertEqual(result.status, 'completed')
        # impl-b won on the RETRY — without it the default (impl-a)
        # would have shipped and the judgement been thrown away.
        self.assertEqual(
            wt.published,
            ('impl-b', 'https://github.com/org/proj.git', True),
        )

    def test_silence_twice_halts_instead_of_shipping_a_default(
        self,
    ) -> None:
        """
        This used to assert the opposite — that the run completed and
        impl-a shipped. It was changed deliberately after
        `gcp-scope-topology-1`, where a judge said it had started
        verification builds it would analyse "once compilation
        completes", stated no SELECT, and impl-a was published because
        it came first in `needs`. The old path did record the
        non-decision honestly, but it recorded it while carrying on.

        Halting is cheap exactly here: every candidate is committed and
        both cleared their review gates, so a human picks between two
        reviewed branches. A default dressed as a decision is not cheap.
        """
        wt, sc = FakeWT(), FakeSC(
            {'impl-a': 'A', 'impl-b': 'B',
             'pick': ['still thinking', 'still thinking']},
        )
        with self.assertRaises(R.PipelineRunError) as caught:
            self._run(_COMPETE, {}, wt=wt, sc=sc)
        # The message has to name what to choose BETWEEN, or the human
        # is left reading a traceback to find the candidates.
        self.assertIn('impl-a', str(caught.exception))
        self.assertIn('impl-b', str(caught.exception))
        self.assertIsNone(wt.published)

    def test_the_judge_is_told_this_is_its_only_turn(self) -> None:
        _r, sc, _wt = self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-a'},
        )
        self.assertIn('ONLY turn', sc.message_for_label('pick'))

    def test_a_judge_downstream_of_review_is_told_not_to_rebuild(
        self,
    ) -> None:
        # The point of the change: both candidates were already built
        # and tested by their reviewers, so sending the judge to
        # re-derive that is minutes of guest time for a known answer.
        _r, sc, _wt = self._run(
            _COMPETE_REVIEWED,
            {'impl-a': 'A', 'impl-b': 'B',
             'review-a-sec': 'VERDICT: APPROVED',
             'review-b-sec': 'VERDICT: APPROVED',
             'pick': 'SELECT: impl-a'},
        )
        msg = sc.message_for_label('pick')
        self.assertIn('already passed its own review gate', msg)
        self.assertIn('Do NOT build or re-run the suite', msg)


    def test_it_is_NOT_told_candidates_were_reviewed_when_they_were_not(
        self,
    ) -> None:
        # The safety property. _COMPETE has no review stage, so the
        # "already verified, do not re-run" claim would be false — and
        # a judge told not to check something nobody checked is worse
        # than one that wastes a build.
        _r, sc, _wt = self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-a'},
        )
        self.assertNotIn(
            'already passed its own review gate',
            sc.message_for_label('pick'),
        )


class TestAJudgeThatDecidedButMisspelledIt(_Base):
    """
    core-contracts-pick produced 2,062 characters of real comparison —
    documentation, test coverage, trait placement — and ended with
    ``SELECT core-contracts-impl-a``. No colon, so the strict pattern
    found nothing, the vote was discarded, and the first candidate won
    by default. The outcome happened to match; the mechanism did not
    (TASKS.md #44).
    """

    def _pick(self, reply: str) -> str:
        """Run _COMPETE with *reply* as the judge turn."""
        _r, _sc, wt = self._run(
            _COMPETE, {'impl-a': 'A', 'impl-b': 'B', 'pick': reply}
        )
        return wt.published[0]

    def test_the_live_failure_now_counts(self) -> None:
        # impl-b on purpose: the fallback is impl-a, so this passes
        # only if the judge's OWN words were read.
        self.assertEqual(self._pick('SELECT impl-b'), 'impl-b')

    def test_decoration_around_the_id_counts(self) -> None:
        for reply in ('SELECT: `impl-b`', 'SELECT: **impl-b**',
                      '**SELECT: impl-b**', 'SELECT -> impl-b.'):
            with self.subTest(reply=reply):
                self.assertEqual(self._pick(reply), 'impl-b')

    def test_prose_mentioning_select_is_not_a_decision(self) -> None:
        # The reason tier 1's pattern was not simply loosened: with an
        # optional colon this sentence yields "the".
        wt, sc = FakeWT(), FakeSC(
            {'impl-a': 'A', 'impl-b': 'B',
             'pick': ['I will SELECT the safer candidate',
                      'still thinking']},
        )
        with self.assertRaises(R.PipelineRunError):
            self._run(_COMPETE, {}, wt=wt, sc=sc)
        self.assertIsNone(wt.published)     # nothing shipped
        self.assertIn('SELECT', sc.message_for_label('pick'))

    def test_naming_both_candidates_is_refused(self) -> None:
        # Refused, and now HALTS rather than falling through to the
        # first candidate: an ambiguous reply is not a decision, and
        # shipping one by list order records it as though it were.
        with self.assertRaises(R.PipelineRunError):
            self._run(
                _COMPETE,
                {'impl-a': 'A', 'impl-b': 'B',
                 'pick': 'SELECT impl-a or impl-b, hard to say'},
            )

    def test_the_judge_is_SHOWN_the_line_not_told_about_it(self) -> None:
        _r, sc, _wt = self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-a'},
        )
        msg = sc.message_for_label('pick')
        self.assertIn('SELECT: <id>', msg)
        self.assertIn('impl-a', msg)
        self.assertIn('impl-b', msg)

    def test_the_retry_shows_the_line_and_names_the_ids(self) -> None:
        instr = R.PipelineRunner._judge_retry_instruction(
            ['impl-a', 'impl-b']
        )
        self.assertIn('SELECT: <id>', instr)
        self.assertIn('impl-a', instr)
        self.assertIn('impl-b', instr)


_PS_CANDIDATES = ['m0-impl-a', 'm0-impl-b']


class TestParseSelect(unittest.TestCase):
    """Unit coverage for the two-tier parser."""

    C = _PS_CANDIDATES

    def test_tier_one_exact_form(self) -> None:
        self.assertEqual(
            R.parse_select('SELECT: m0-impl-b', self.C), 'm0-impl-b'
        )

    def test_tier_two_needs_the_marker_AND_a_known_id(self) -> None:
        self.assertEqual(
            R.parse_select('SELECT m0-impl-b', self.C), 'm0-impl-b'
        )
        self.assertIsNone(R.parse_select('m0-impl-b is better', self.C))
        self.assertIsNone(R.parse_select('SELECT the better one', self.C))

    def test_a_tier_one_capture_that_is_not_a_candidate_falls_through(
        self,
    ) -> None:
        # The regex eats the next word of a sentence; a real choice
        # further down must still win.
        self.assertEqual(
            R.parse_select(
                'SELECT: whichever is safer\n\nSELECT m0-impl-b', self.C
            ),
            'm0-impl-b',
        )

    def test_a_substring_id_never_shadows_the_longer_one(self) -> None:
        self.assertEqual(
            R.parse_select('SELECT m0-impl-a', ['impl-a', 'm0-impl-a']),
            'm0-impl-a',
        )

    def test_the_last_select_line_wins(self) -> None:
        self.assertEqual(
            R.parse_select(
                'SELECT m0-impl-a\nno wait\nSELECT m0-impl-b', self.C
            ),
            'm0-impl-b',
        )

    def test_without_candidates_the_old_behaviour_is_exact(self) -> None:
        # Callers that pass no candidate list get tier 1, unvalidated.
        self.assertEqual(R.parse_select('SELECT: anything'), 'anything')
        self.assertIsNone(R.parse_select('SELECT anything'))


class TestAParallelBlockRunsInParallel(_Base):
    """
    ``parallel:`` used to be a grouping, not a promise: the runner
    walked a block's children with a plain loop, so two competing
    writers that never needed ordering ran end to end (TASKS.md #46).
    """

    def test_both_writers_are_in_flight_at_once(self) -> None:
        # A barrier neither writer can pass alone. Driven in series the
        # first waits for a partner that cannot arrive, and this fails
        # on the timeout rather than passing slowly.
        cfg = self._cfg(_COMPETE)
        wt, sc = FakeWT(), FakeSC({'impl-a': 'A', 'impl-b': 'B',
                                   'pick': 'SELECT: impl-a'})
        gate = threading.Barrier(2, timeout=10)
        reached: list[str] = []
        original = sc.send_and_wait

        def rendezvous(session, message, **kw):
            label = sc._label.get(session, '')
            if label.startswith('impl-'):
                reached.append(label)
                gate.wait()
            return original(session, message, **kw)

        sc.send_and_wait = rendezvous
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        runner.run()
        self.assertEqual(sorted(reached), ['impl-a', 'impl-b'])

    def test_one_writer_failing_still_awaits_the_other(self) -> None:
        # Returning early would leave the surviving node driving a turn
        # in a microVM nothing is tracking any more, and an orphaned
        # guest is the most expensive failure this launcher has.
        cfg = self._cfg(_COMPETE)
        wt, sc = FakeWT(), FakeSC({'impl-b': 'B'})
        sc.fail_labels = {'impl-a'}
        finished: list[str] = []
        original = sc.send_and_wait

        def record(session, message, **kw):
            result = original(session, message, **kw)
            if sc._label.get(session, '') == 'impl-b':
                finished.append('impl-b')
            return result

        sc.send_and_wait = record
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        with self.assertRaises(R.PipelineRunError):
            runner.run()
        self.assertEqual(finished, ['impl-b'])


class TestCompetingWriters(_Base):
    def test_isolated_writers_and_judge_select(self) -> None:
        result, _sc, wt = self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-b'},
        )
        self.assertEqual(result.status, 'completed')
        # both writers cut isolated worktrees from base (no from_node).
        self.assertIsNone(wt.node_from['impl-a'])
        self.assertIsNone(wt.node_from['impl-b'])
        # judge compared both candidates.
        self.assertEqual(wt.judges['pick'], ['impl-a', 'impl-b'])
        # published the WINNER (impl-b), publish mode defaults to pr.
        self.assertEqual(
            wt.published,
            ('impl-b', 'https://github.com/org/proj.git', True),
        )

    def test_refactor_after_judge_seeds_from_winner(self) -> None:
        # A refactor writer runs on the judge's winner: the judge
        # aliases its branch to the winner, the refactor node seeds
        # `from: pick`, gets refactor-framed (not the raw task), and
        # the refactored branch is what publishes.
        result, sc, wt = self._run(
            _JUDGE_REFACTOR,
            {
                'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-b',
                'refactor': 'cleaned up',
                'review-r-sec': 'VERDICT: APPROVED',
            },
        )
        self.assertEqual(result.status, 'completed')
        # judge aliased its branch to the selected winner.
        self.assertIn(('pick', 'impl-b'), wt.aliases)
        # refactor seeded its worktree from the judge node.
        self.assertEqual(wt.node_from['refactor'], 'pick')
        # refactor got the cleanup framing, NOT the implement task.
        rf_msg = sc.message_for_label('refactor')
        self.assertIn('You are REFACTORING', rf_msg)
        self.assertIn('THE SAME OUTPUTS FROM THE SAME INPUTS', rf_msg)
        self.assertNotIn('Task:\nimplement parse_ports', rf_msg)
        # the refactored branch (reviewed) is what ships.
        self.assertEqual(wt.published[0], 'refactor')


_TDD = """\
name: tdd
repo: ./proj
publish: none
task: |
  build it
agents:
  tw: {template: tdd-writer, model: claude-sonnet-5}
  build: {template: coder, model: claude-sonnet-5}
stages:
  - {id: tests, run: tw, write: true}
  - {id: build, run: build, write: true, needs: [tests]}
"""


class TestTddInheritance(_Base):
    def test_build_inherits_tests_branch(self) -> None:
        _, sc, wt = self._run(_TDD, {'tests': 'wrote tests', 'build': 'impl'})
        # build's worktree is seeded from the tests branch.
        self.assertEqual(wt.node_from['build'], 'tests')
        # and the coder is told the tests are the binding contract.
        msg = sc.message_for_label('build')
        self.assertIn('BINDING CONTRACT', msg)
        self.assertIn('the TESTS win', msg)

    def test_writer_worktree_settled_before_commit(self) -> None:
        # Every writer node must have its worktree settled (agy keeps
        # writing after its turn goes idle) BEFORE it is committed, else
        # the commit captures an empty tree and the work is lost.
        _, _, wt = self._run(_TDD, {'tests': 'wrote tests', 'build': 'impl'})
        for node in ('tests', 'build'):
            self.assertIn(('settle', node), wt.events)
            self.assertIn(('commit', node), wt.events)
            self.assertLess(
                wt.events.index(('settle', node)),
                wt.events.index(('commit', node)),
                f'{node}: settle must precede commit',
            )

    def test_tdd_node_told_to_write_tests_not_implement(self) -> None:
        # The test-writer node must be framed as WRITING TESTS for the
        # feature, never handed the raw "Task: build it" implementation
        # directive (which would make it implement the feature).
        _, sc, _ = self._run(_TDD, {'tests': 'wrote tests', 'build': 'impl'})
        tdd_msg = sc.message_for_label('tests')
        self.assertIn('failing TEST SUITE', tdd_msg)
        self.assertIn('NOT implement', tdd_msg)
        # It is NOT given the bare "Task:" implementation framing.
        self.assertNotIn('Task:\nbuild it', tdd_msg)
        # The coder, by contrast, still gets the implementation task.
        build_msg = sc.message_for_label('build')
        self.assertIn('Task:', build_msg)
        self.assertNotIn('failing TEST SUITE', build_msg)


_TDD_FULL = """\
name: tddfull
repo: ./proj
publish: none
task: |
  build parse_ports
acceptance: |
  handles ranges
agents:
  plan: {template: planner, model: claude-sonnet-5}
  tw: {template: tdd-writer, model: claude-sonnet-5}
  build: {template: coder, model: claude-sonnet-5}
stages:
  - {id: plan, run: plan}
  - {id: tests, run: tw, write: true, needs: [plan]}
  - {id: build, run: build, write: true, from: tests, needs: [plan]}
"""


class TestInteractivePlanning(_Base):
    def test_planner_blocks_on_human_approval(self) -> None:
        # The planner node (only) must await human approval before the
        # pipeline advances; the approved plan becomes its output.
        _, sc, _ = self._run(
            _TDD_FULL,
            {'plan': 'THE PLAN', 'tests': 't', 'build': 'b'},
        )
        # exactly the planner session was awaited for approval.
        plan_sid = next(
            s for s, lb in sc._label.items() if lb == 'plan'
        )
        self.assertEqual(sc.approvals, [plan_sid])
        # and the planner turn invites the human to approve.
        self.assertIn('APPROVED', sc.message_for_label('plan'))

    def test_planner_told_the_missing_toolchain_is_expected(self) -> None:
        # Observed live: a planner found no cargo, searched the whole
        # filesystem for it, and downloaded the rustup installer —
        # burning the budget of the one stage that blocks on the human.
        # It writes nothing, so it never gets `setup:`; that silence let
        # it treat the absence as a problem to fix.
        _, sc, _ = self._run(
            _TDD_FULL,
            {'plan': 'THE PLAN', 'tests': 't', 'build': 'b'},
        )
        msg = sc.message_for_label('plan')
        self.assertIn('NO project toolchain', msg)
        self.assertIn('Do NOT install', msg)

    def test_builders_still_get_the_setup_block(self) -> None:
        # The don't-install rule is planner-only: a writer that must
        # compile still gets told how to prepare its VM.
        cfg_text = _TDD_FULL.replace(
            'publish: none',
            'publish: none\nsetup: |\n  install the rust toolchain',
        )
        _, sc, _ = self._run(
            cfg_text, {'plan': 'P', 'tests': 't', 'build': 'b'}
        )
        self.assertIn(
            'install the rust toolchain', sc.message_for_label('build')
        )
        self.assertNotIn(
            'install the rust toolchain', sc.message_for_label('plan')
        )

    def test_planner_turn_is_plan_framed_not_task(self) -> None:
        _, sc, _ = self._run(
            _TDD_FULL,
            {'plan': 'THE PLAN', 'tests': 't', 'build': 'b'},
        )
        plan_msg = sc.message_for_label('plan')
        self.assertIn('DESIGN PLAN', plan_msg)
        self.assertNotIn('Task:\nbuild parse_ports', plan_msg)

    def test_coder_gets_plan_as_design_context(self) -> None:
        # Q1=yes: the coder receives the plan as design guidance,
        # with the tests as the binding contract.
        _, sc, _ = self._run(
            _TDD_FULL,
            {'plan': 'THE PLAN', 'tests': 't', 'build': 'b'},
        )
        build_msg = sc.message_for_label('build')
        self.assertIn('Design from plan', build_msg)
        self.assertIn('THE PLAN', build_msg)
        self.assertIn('BINDING CONTRACT', build_msg)
        # the tdd node must NOT get a design-guidance framing meant
        # for the implementer.
        self.assertNotIn('BINDING CONTRACT', sc.message_for_label('tests'))

    def test_short_ack_recovers_the_real_plan_for_builders(self) -> None:
        # End to end: the planner answers the consolidation turn with an
        # acknowledgement instead of re-emitting its design. The
        # builders must still receive the real DOCUMENT — shipping them
        # the blurb is what made a live TDD writer refuse to build.
        plan = '# Design\n' + ('concrete detail. ' * 200)
        ack = 'The plan is approved and frozen; nothing further needed.'
        _, sc, _ = self._run(
            _TDD_FULL,
            {'plan': 'streamed', 'tests': 't', 'build': 'b'},
            settled={'plan': ack},
            conversation={'plan': ['early draft', plan, ack]},
        )
        build_msg = sc.message_for_label('build')
        self.assertIn('concrete detail.', build_msg)
        self.assertNotIn('approved and frozen', build_msg)

    def test_compliant_consolidation_is_used_verbatim(self) -> None:
        # The normal path is untouched: a planner that DOES re-emit its
        # plan has that reply used as-is, not second-guessed.
        doc = '# Consolidated\n' + ('final detail. ' * 200)
        _, sc, _ = self._run(
            _TDD_FULL,
            {'plan': 'streamed', 'tests': 't', 'build': 'b'},
            settled={'plan': doc},
            conversation={'plan': ['early draft', 'notes', doc]},
        )
        self.assertIn('final detail.', sc.message_for_label('build'))

    def test_interactive_plan_off_skips_approval(self) -> None:
        _, sc, _ = self._run(
            _TDD_FULL,
            {'plan': 'THE PLAN', 'tests': 't', 'build': 'b'},
            interactive_plan=False,
        )
        self.assertEqual(sc.approvals, [])


_PREWARM_REPLIES = {'plan': 'THE PLAN', 'tests': 't', 'build': 'b'}

#: Interactive planner + competing writers + judge + a refactor writer
#: seeded FROM that judge — the full-cadre shape, whose pre-warm the
#: judge-seeded writer must be excluded from.
_PLAN_JUDGE_REFACTOR = """\
name: pjr
repo: ./proj
publish: none
task: |
  implement it
agents:
  plan: {template: planner, model: claude-sonnet-5}
  ca: {template: coder, model: claude-sonnet-5}
  cb: {template: coder, model: claude-sonnet-5}
  jg: {template: judge, model: claude-opus-4-8}
  rf: {template: refactoring, model: claude-sonnet-5}
stages:
  - {id: plan, run: plan}
  - id: impl
    parallel:
      - {id: impl-a, run: ca, write: true, needs: [plan]}
      - {id: impl-b, run: cb, write: true, needs: [plan]}
  - {id: pick, run: jg, needs: [impl-a, impl-b], selects: branch}
  - {id: refactor, run: rf, write: true, from: pick, needs: [pick]}
"""


class TestPrewarm(_Base):
    def test_writers_prewarmed_before_approval(self) -> None:
        # Every writer node's VM is provisioned (booted) BEFORE the
        # human approval begins, so the swarm is warm the instant the
        # plan is approved, not cold-starting node by node afterwards.
        _, sc, _ = self._run(_TDD_FULL, dict(_PREWARM_REPLIES))
        self.assertIn('tests', sc.labels_at_approval)
        self.assertIn('build', sc.labels_at_approval)

    def test_prewarmed_impl_reseeded_onto_upstream(self) -> None:
        # A pre-warmed writer seeded 'from' an upstream is reseeded onto
        # the upstream's committed tip before it is driven; a writer
        # with no upstream (tests) is never reseeded.
        _, _sc, wt = self._run(_TDD_FULL, dict(_PREWARM_REPLIES))
        self.assertIn(('build', 'tests'), wt.reseeds)
        self.assertNotIn('tests', [node for node, _ in wt.reseeds])

    def test_prewarmed_writer_provisioned_once(self) -> None:
        # Pre-warming must not double-provision: each writer gets one
        # session (reused at drive time, not recreated).
        _, sc, _ = self._run(_TDD_FULL, dict(_PREWARM_REPLIES))
        labels = [c['title'].split('/', 1)[-1] for c in sc.creates]
        self.assertEqual(labels.count('tests'), 1)
        self.assertEqual(labels.count('build'), 1)

    def test_judge_seeded_writer_is_not_prewarmed(self) -> None:
        # Regression: pre-warm booted EVERY writer during planning,
        # including one seeded `from:` a judge — whose branch does not
        # exist until that judge runs and aliases it. The clone cut then
        # died with "origin/pl/<run>/pick is not a commit", killing the
        # whole interactive run (unattended runs skip pre-warm, so this
        # only bit the interactive + refactor-after-judge combination).
        result, sc, wt = self._run(
            _PLAN_JUDGE_REFACTOR,
            {
                'plan': 'PLAN', 'impl-a': 'A', 'impl-b': 'B',
                'pick': 'SELECT: impl-a', 'refactor': 'cleaned',
            },
        )
        self.assertEqual(result.status, 'completed')
        # The judge-seeded writer was NOT provisioned during planning...
        self.assertNotIn('refactor', sc.labels_at_approval)
        # ...but the base-seeded competing writers were.
        self.assertIn('impl-a', sc.labels_at_approval)
        self.assertIn('impl-b', sc.labels_at_approval)
        # and it still ran, cut from the judge's aliased branch.
        self.assertEqual(wt.node_from['refactor'], 'pick')
        self.assertIn(('pick', 'impl-a'), wt.aliases)

    def test_non_interactive_does_not_prewarm_or_reseed(self) -> None:
        # Without the interactive planner wait there is no idle window
        # to pre-warm in: writers provision lazily, nothing is reseeded.
        _, sc, wt = self._run(
            _TDD_FULL, dict(_PREWARM_REPLIES), interactive_plan=False
        )
        self.assertEqual(wt.reseeds, [])
        self.assertEqual(sc.labels_at_approval, [])


_LINEAR_REPLIES = {
    'plan': 'PLAN', 'build': 'b', 'review-sec': 'VERDICT: APPROVED',
}


class TestPlanArtifact(_Base):
    def test_consolidation_turn_driven_after_approval(self) -> None:
        # After approval the planner is driven ONE more turn to emit a
        # clean, consolidated final plan (so the plan of record is a
        # standalone doc, not the last chat-style reply).
        _, sc, _ = self._run(
            _TDD_FULL, {'plan': 'FINAL', 'tests': 't', 'build': 'b'}
        )
        plan_sid = next(s for s, lb in sc._label.items() if lb == 'plan')
        plan_sends = [m for s, m in sc.sent if s == plan_sid]
        self.assertEqual(len(plan_sends), 2)  # initial + consolidation
        self.assertIn('consolidated final', plan_sends[1].lower())

    def test_plan_of_record_committed_to_published_branch(self) -> None:
        # The plan of record is written to the SELECTED/published
        # branch's worktree at the default docs/plans/<name>.md and
        # committed as its own commit before publish.
        _, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        _assert_plan_committed(
            self, wt, '/wt/r1/nodes/build', 'docs/plans/demo.md', 'PLAN'
        )
        self.assertIn(
            'docs: add plan of record', [m for _, m, _ in wt.commits]
        )

    def test_plan_of_record_is_settled_message_not_streamed(self) -> None:
        # agy's consolidation reply lags its turn, so the streamed reply
        # can be a STALE prior message. The plan of record must come
        # from the SETTLED final message, not the streamed one.
        _, _sc, wt = self._run(
            _LINEAR,
            {'plan': 'STALE', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
            settled={'plan': 'FULL CONSOLIDATED PLAN'},
        )
        _assert_plan_committed(
            self, wt, '/wt/r1/nodes/build', 'docs/plans/demo.md',
            'FULL CONSOLIDATED PLAN',
        )

    def test_settled_plan_shared_downstream(self) -> None:
        # The settled plan (not the stale streamed reply) reaches the
        # builders as design context.
        _, sc, _ = self._run(
            _TDD_FULL,
            {'plan': 'STALE', 'tests': 't', 'build': 'b'},
            settled={'plan': 'REAL PLAN'},
        )
        build_msg = sc.message_for_label('build')
        self.assertIn('REAL PLAN', build_msg)
        self.assertNotIn('STALE', build_msg)

    def test_consolidation_turn_bracketed_by_idle_waits(self) -> None:
        # The plan session is settled (idle-waited) around the
        # consolidation turn — before it (so it doesn't race agy's reply
        # to APPROVED) and after it (so the final message is settled).
        _, sc, _ = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        plan_sid = next(s for s, lb in sc._label.items() if lb == 'plan')
        self.assertGreaterEqual(sc.idle_waits.count(plan_sid), 2)

    def test_plan_artifact_path_is_configurable(self) -> None:
        _, _sc, wt = self._run(
            _LINEAR + 'plan_artifact: design/PLAN.md\n',
            dict(_LINEAR_REPLIES),
        )
        _assert_plan_committed(
            self, wt, '/wt/r1/nodes/build', 'design/PLAN.md', 'PLAN'
        )

    def test_no_artifact_when_publish_none(self) -> None:
        # _TDD_FULL has a planner but publish: none — nothing written.
        _, _sc, wt = self._run(
            _TDD_FULL, {'plan': 'FINAL', 'tests': 't', 'build': 'b'}
        )
        self.assertEqual(wt.tracked_files, [])

    def test_non_interactive_still_writes_plain_plan(self) -> None:
        # Non-interactive: no consolidation turn; the planner's plain
        # reply is still the plan of record and is committed at publish.
        _, _sc, wt = self._run(
            _LINEAR, dict(_LINEAR_REPLIES), interactive_plan=False
        )
        _assert_plan_committed(
            self, wt, '/wt/r1/nodes/build', 'docs/plans/demo.md', 'PLAN'
        )

    def test_non_interactive_captures_plan_from_settled(self) -> None:
        # An agy planner's reply lags its single unattended turn, so
        # _drive returns empty. The plan must be recovered from the
        # SETTLED session so it is still shared with the builder AND
        # committed as the plan of record (not the empty stream).
        _, sc, wt = self._run(
            _LINEAR,
            {'plan': '', 'build': 'b', 'review-sec': 'VERDICT: APPROVED'},
            settled={'plan': 'SETTLED PLAN'},
            interactive_plan=False,
        )
        _assert_plan_committed(
            self, wt, '/wt/r1/nodes/build', 'docs/plans/demo.md',
            'SETTLED PLAN',
        )
        self.assertIn('SETTLED PLAN', sc.message_for_label('build'))
        plan_sid = next(
            s for s, lb in sc._label.items() if lb == 'plan'
        )
        self.assertIn(plan_sid, sc.idle_waits)


class TestAgyPreflight(_Base):
    """preflight_agy() refuses an agy pipeline on a stale swap secret,
    so the run fails in seconds instead of hanging to the turn timeout
    on an agy that cannot authenticate."""

    def _stamp(self, age_s: float | None):
        """A stamp file *age_s* old, or an absent one for ``None``."""
        path = self.root / 'agy-harvest.json'
        if age_s is not None:
            R.agy.record_harvest('f', path=path, now=1000.0 - age_s)
        return path

    def test_missing_stamp_refuses_with_actionable_message(self) -> None:
        cfg = self._cfg(_LINEAR)  # its planner is antigravity-native
        with self.assertRaises(click.ClickException) as ctx:
            R.preflight_agy(cfg, stamp_path=self._stamp(None), now=1000.0)
        msg = str(ctx.exception)
        self.assertIn('plan', msg)  # names the offending agent
        self.assertIn('omni-sbx-agy harvest', msg)  # the actual fix
        self.assertIn('--skip-agy-check', msg)  # the escape hatch

    def test_stale_stamp_refuses(self) -> None:
        cfg = self._cfg(_LINEAR)
        stale = R.agy.MAX_SWAP_AGE_S + 60
        with self.assertRaises(click.ClickException):
            R.preflight_agy(cfg, stamp_path=self._stamp(stale), now=1000.0)

    def test_fresh_stamp_passes(self) -> None:
        cfg = self._cfg(_LINEAR)
        R.preflight_agy(cfg, stamp_path=self._stamp(60.0), now=1000.0)

    def test_boundary_age_passes(self) -> None:
        # Exactly at the bound is still fresh — a healthy harvester
        # refreshing on cadence must never trip the guard.
        cfg = self._cfg(_LINEAR)
        at = R.agy.MAX_SWAP_AGE_S
        R.preflight_agy(cfg, stamp_path=self._stamp(at), now=1000.0)

    def test_no_agy_agents_is_a_noop(self) -> None:
        # An all-Claude pipeline must never be blocked by agy auth,
        # even with no stamp anywhere.
        cfg = self._cfg(_TDD)
        R.preflight_agy(cfg, stamp_path=self._stamp(None), now=1000.0)

    def test_cli_refuses_before_touching_the_server(self) -> None:
        # Wiring: the CLI must run the guard BEFORE it builds a client
        # or provisions anything — that is the whole point (fail in
        # seconds, not after a 30-min turn timeout). --no-auto-harvest
        # keeps the refuse-with-instructions behavior.
        cfg_path = self.root / 'pipeline.yaml'
        cfg_path.write_text(_LINEAR, encoding='utf-8')
        missing = self.root / 'no-such-stamp.json'
        with mock.patch.object(R.agy, 'HARVEST_STAMP', missing), \
                mock.patch.object(R, 'preflight_disk'), \
                mock.patch.object(R, 'SwarmSessionClient') as client:
            res = CliRunner().invoke(
                R.main,
                [
                    '-c', str(cfg_path),
                    '--canonical-root', str(self.root / 'c'),
                    '--worktree-root', str(self.root / 'w'),
                    '--no-auto-harvest',
                ],
            )
        self.assertEqual(res.exit_code, 1)
        self.assertIn('omni-sbx-agy harvest', res.output)
        client.assert_not_called()  # never reached the server

    def test_cli_skip_flag_bypasses_the_guard(self) -> None:
        cfg_path = self.root / 'pipeline.yaml'
        cfg_path.write_text(_LINEAR, encoding='utf-8')
        missing = self.root / 'no-such-stamp.json'
        with mock.patch.object(R.agy, 'HARVEST_STAMP', missing), \
                mock.patch.object(R, 'preflight_disk'), \
                mock.patch.object(R, 'SwarmSessionClient') as client:
            CliRunner().invoke(
                R.main,
                [
                    '-c', str(cfg_path),
                    '--canonical-root', str(self.root / 'c'),
                    '--worktree-root', str(self.root / 'w'),
                    '--skip-agy-check',
                ],
            )
        # Guard bypassed: it got past preflight to the server client.
        client.assert_called_once()


_CAMPAIGN = """\
name: camp
repo: ./proj
publish: local
task: |
  build the whole thing
acceptance: |
  it works
agents:
  plan: {template: planner, model: claude-sonnet-5}
  tw: {template: tdd-writer, model: claude-sonnet-5}
  build: {template: coder, model: claude-sonnet-5}
stages:
  - {id: plan, run: plan}
  - {id: tests, run: tw, write: true, needs: [plan]}
  - {id: build, run: build, write: true, from: tests, needs: [plan]}
"""


_CAMPAIGN_REVIEW = """\
name: camp
repo: ./proj
publish: local
task: |
  build the whole thing
acceptance: |
  it works
agents:
  plan: {template: planner, model: claude-sonnet-5}
  tw: {template: tdd-writer, model: claude-sonnet-5}
  build: {template: coder, model: claude-sonnet-5}
  sec: {template: security-reviewer, model: claude-sonnet-5}
stages:
  - {id: plan, run: plan}
  - {id: tests, run: tw, write: true, needs: [plan]}
  - {id: build, run: build, write: true, from: tests, needs: [plan]}
  - {id: rev, run: [sec], needs: [build], gate: consensus, on_block: build}
"""


def _plan_with_subtasks(*ids: str) -> str:
    lines = '\n'.join(f'- [{i}] build {i}' for i in ids)
    return f'# Design\n\nprose\n\nSUBTASKS:\n{lines}\n'


#: Per-module mode: the human supplies the module list, so the WHOLE
#: pipeline (its own planner + build) loops once per module.
_PER_MODULE = """\
name: mods
repo: ./proj
publish: local
task: |
  the shared project brief
acceptance: |
  module done when it passes review
subtasks:
  - {id: m0, title: contracts and core}
  - {id: m1, title: storage and schema}
agents:
  plan: {template: planner, model: claude-sonnet-5}
  tw: {template: tdd-writer, model: claude-sonnet-5}
  build: {template: coder, model: claude-sonnet-5}
stages:
  - {id: plan, run: plan}
  - {id: tests, run: tw, write: true, needs: [plan]}
  - {id: build, run: build, write: true, from: tests, needs: [plan]}
"""


class TestCampaign(_Base):
    """Sequential per-chunk build loop (≥2 proposed subtasks)."""

    def _campaign(self, *ids: str):
        # Non-interactive: the planner's plain reply carries the chunk
        # list, so no approval mocking is needed to reach the loop.
        return self._run(
            _CAMPAIGN,
            {'plan': _plan_with_subtasks(*ids)},
            interactive_plan=False,
        )

    def test_runs_a_build_pass_per_chunk(self) -> None:
        _, _sc, wt = self._campaign('m0', 'm1')
        for nid in ('m0-tests', 'm0-build', 'm1-tests', 'm1-build'):
            self.assertIn(nid, wt.node_from)
        # the un-namespaced stages never ran as their own nodes.
        self.assertNotIn('tests', wt.node_from)
        self.assertNotIn('build', wt.node_from)

    def test_first_chunk_seeds_base_later_from_campaign(self) -> None:
        _, _sc, wt = self._campaign('m0', 'm1')
        # chunk 0's entry writer cuts from base; chunk 1 from thread.
        self.assertIsNone(wt.node_from['m0-tests'])
        self.assertEqual(wt.node_from['m1-tests'], 'campaign')
        # in-chunk inheritance stays namespaced.
        self.assertEqual(wt.node_from['m0-build'], 'm0-tests')
        self.assertEqual(wt.node_from['m1-build'], 'm1-tests')

    def test_thread_advances_and_publishes_each_chunk(self) -> None:
        result, _sc, wt = self._campaign('m0', 'm1')
        self.assertEqual(result.status, 'completed')
        # campaign aliased to each chunk's winner (its last writer).
        self.assertIn(('campaign', 'm0-build'), wt.aliases)
        self.assertIn(('campaign', 'm1-build'), wt.aliases)
        # each chunk published to its OWN remote branch.
        remotes = [rb for _n, rb in wt.publishes]
        self.assertIn('pipeline/r1-m0', remotes)
        self.assertIn('pipeline/r1-m1', remotes)
        self.assertIn('r1-m0', result.published)
        self.assertIn('r1-m1', result.published)

    def test_chunk_directive_in_builder_turns(self) -> None:
        _, sc, _wt = self._campaign('m0', 'm1')
        self.assertIn('[m0]', sc.message_for_label('m0-tests'))
        self.assertIn('increment', sc.message_for_label('m0-build'))
        self.assertIn('[m1]', sc.message_for_label('m1-build'))

    def test_plan_of_record_committed_once_on_first_chunk(self) -> None:
        _, _sc, wt = self._campaign('m0', 'm1')
        por = [c for c in wt.commits if c[1] == 'docs: add plan of record']
        self.assertEqual(len(por), 1)
        self.assertEqual(por[0][0], 'm0-build')  # on chunk 0's winner
        plans = [
            p for _n, p, _c in wt.tracked_files
            if not p.endswith('-session.md')
        ]
        self.assertEqual(plans, ['docs/plans/camp.md'])

    def test_single_subtask_is_not_a_campaign(self) -> None:
        # 1 chunk → single pass: un-namespaced nodes, default name.
        _, _sc, wt = self._campaign('only')
        self.assertIn('tests', wt.node_from)
        self.assertNotIn('only-tests', wt.node_from)
        self.assertEqual([rb for _n, rb in wt.publishes], [None])
        self.assertEqual(wt.aliases, [])

    def test_chunk_block_stops_the_campaign(self) -> None:
        # A block in chunk 0's review never reaches chunk 1.
        result, _sc, wt = self._run(
            _CAMPAIGN_REVIEW,
            {
                'plan': _plan_with_subtasks('m0', 'm1'),
                'm0-rev-sec': 'VERDICT: BLOCKING',
            },
            interactive_plan=False,
            max_review_rounds=1,
        )
        self.assertEqual(result.status, 'blocked')
        self.assertNotIn('m1-tests', wt.node_from)  # chunk 1 never started


class TestPerChunkRecordPaths(_Base):
    """
    Two chunks must never write the same doc path.

    Nothing threads the doc commits forward — the campaign branch is
    aliased to the winner BEFORE the docs are committed — so two chunks
    sharing a path each CREATE it, and every merge after the first is an
    add/add conflict. Observed on gcp-custom-roles-1: PR #19 conflicted
    with main on discover-reviews.md, -selection.md and -session.md
    (TASKS.md #45).

    PER-MODULE mode is guarded by ``TestPerModule``, which already
    asserts ``docs/plans/mods-m0-reviews.md``: its plan_path is
    already per-module, so folding the id in a second time would
    break that assertion.
    """

    def _campaign_with_reviews(self, *ids: str):
        return self._run(
            _CAMPAIGN_REVIEW,
            {'plan': _plan_with_subtasks(*ids)},
            interactive_plan=False,
        )

    def test_no_doc_path_is_written_by_two_chunks(self) -> None:
        _r, _sc, wt = self._campaign_with_reviews('m0', 'm1')
        # (winner-node, path) pairs for every tracked doc write.
        by_path: dict[str, set[str]] = {}
        for node, path, _content in wt.tracked_files:
            by_path.setdefault(path, set()).add(node)
        shared = {
            path: nodes for path, nodes in by_path.items()
            if len({n.split('-')[0] for n in nodes}) > 1
        }
        self.assertEqual(shared, {}, f'written by >1 chunk: {shared}')

    def test_each_chunks_reviews_carry_its_own_id(self) -> None:
        _r, _sc, wt = self._campaign_with_reviews('m0', 'm1')
        paths = [p for _n, p, _c in wt.tracked_files]
        self.assertIn('docs/plans/camp-m0-reviews.md', paths)
        self.assertIn('docs/plans/camp-m1-reviews.md', paths)
        # ...and the shared name is never used by a chunk.
        self.assertNotIn('docs/plans/camp-reviews.md', paths)

    def test_the_shared_planning_session_is_committed_once(self) -> None:
        # Flat mode has ONE planner, so the record is the same
        # conversation every time; committing it per chunk rewrote one
        # path from every branch and added nothing.
        _r, _sc, wt = self._campaign_with_reviews('m0', 'm1')
        sessions = [
            p for _n, p, _c in wt.tracked_files if p.endswith('-session.md')
        ]
        self.assertEqual(sessions, ['docs/plans/camp-session.md'])

    def test_a_single_pass_run_keeps_the_plain_names(self) -> None:
        # No campaign, no chunk id: pre-existing names unchanged.
        _r, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        paths = [p for _n, p, _c in wt.tracked_files]
        self.assertTrue(
            any(p == 'docs/plans/demo-reviews.md' for p in paths), paths
        )


class TestPerModule(_Base):
    """Human-supplied modules: full pipeline looped once per module."""

    def _per_module(self, replies=None, **kw):
        # Non-interactive by default: each module planner's plain reply
        # is its design (no approval mocking needed to reach the build).
        return self._run(
            _PER_MODULE,
            replies or {'m0-plan': 'DESIGN M0', 'm1-plan': 'DESIGN M1'},
            interactive_plan=False,
            **kw,
        )

    def test_full_pipeline_loops_per_module(self) -> None:
        _, _sc, wt = self._per_module()
        for nid in (
            'm0-plan', 'm0-tests', 'm0-build',
            'm1-plan', 'm1-tests', 'm1-build',
        ):
            self.assertIn(nid, wt.node_from)
        # the un-namespaced stages never ran as their own nodes.
        for nid in ('plan', 'tests', 'build'):
            self.assertNotIn(nid, wt.node_from)

    def test_module_planner_and_entry_seed_frozen_prior(self) -> None:
        _, _sc, wt = self._per_module()
        # module 0 designs/builds from base; module 1 from the campaign
        # tip (module 0's frozen artifacts).
        self.assertIsNone(wt.node_from['m0-plan'])
        self.assertIsNone(wt.node_from['m0-tests'])
        self.assertEqual(wt.node_from['m1-plan'], 'campaign')
        self.assertEqual(wt.node_from['m1-tests'], 'campaign')
        # in-module inheritance stays namespaced.
        self.assertEqual(wt.node_from['m0-build'], 'm0-tests')
        self.assertEqual(wt.node_from['m1-build'], 'm1-tests')

    def test_module_plan_reaches_only_its_own_builders(self) -> None:
        _, sc, _ = self._per_module()
        self.assertIn('DESIGN M0', sc.message_for_label('m0-tests'))
        self.assertIn('DESIGN M0', sc.message_for_label('m0-build'))
        self.assertIn('DESIGN M1', sc.message_for_label('m1-tests'))
        # module 0's plan must not bleed into module 1's builders.
        self.assertNotIn('DESIGN M0', sc.message_for_label('m1-tests'))

    def test_each_module_publishes_and_threads(self) -> None:
        result, _sc, wt = self._per_module()
        self.assertEqual(result.status, 'completed')
        self.assertIn(('campaign', 'm0-build'), wt.aliases)
        self.assertIn(('campaign', 'm1-build'), wt.aliases)
        remotes = [rb for _n, rb in wt.publishes]
        self.assertIn('pipeline/r1-m0', remotes)
        self.assertIn('pipeline/r1-m1', remotes)

    def test_per_module_plan_committed_on_each_branch(self) -> None:
        _, _sc, wt = self._per_module()
        por = [c for c in wt.commits if c[1] == 'docs: add plan of record']
        # one plan of record per module, each on its own winner + doc.
        self.assertEqual({c[0] for c in por}, {'m0-build', 'm1-build'})
        _assert_plan_committed(
            self, wt, '/wt/r1/nodes/m0-build', 'docs/plans/mods-m0.md',
            'DESIGN M0',
        )
        _assert_plan_committed(
            self, wt, '/wt/r1/nodes/m1-build', 'docs/plans/mods-m1.md',
            'DESIGN M1',
        )

    def test_module_planner_turn_is_scoped(self) -> None:
        _, sc, _ = self._per_module()
        msg = sc.message_for_label('m0-plan')
        self.assertIn('DESIGN PLAN', msg)   # plan-framed, not implement
        self.assertIn('[m0]', msg)          # scoped to the module

    def test_only_later_modules_are_told_priors_are_frozen(self) -> None:
        # THE live regression: module 0 was told prior artifacts were
        # already implemented and FROZEN in its worktree. Nothing has
        # been built yet, so an agent that checks (correctly) refuses to
        # build against a baseline it cannot find.
        _, sc, _ = self._per_module()
        m0, m1 = (
            sc.message_for_label('m0-plan'),
            sc.message_for_label('m1-plan'),
        )
        self.assertIn('FIRST module', m0)
        self.assertNotIn('already implemented and FROZEN', m0)
        self.assertIn('already implemented and FROZEN', m1)

    def test_first_module_builders_not_told_prior_work_exists(self) -> None:
        _, sc, _ = self._per_module()
        m0, m1 = (
            sc.message_for_label('m0-tests'),
            sc.message_for_label('m1-tests'),
        )
        self.assertIn('FIRST increment', m0)
        self.assertNotIn('Earlier increments are already', m0)
        self.assertIn('Earlier increments are already', m1)

    def test_no_role_is_told_the_repo_is_at_its_base_state(self) -> None:
        # `_active_is_first` means "first row of THIS run", not "nothing
        # has ever been built here". A per-module campaign runs against
        # a repo whose earlier modules are merged and frozen: on
        # `ingestion-m3-1` every m3a agent was told the tree was at base
        # state while m0, m0b, m1 and m2 sat in it. The planner caught
        # the contradiction and said so in its first line; the reviewers
        # were handed it while vetting a module whose whole brief is
        # "mirror the [m2] pattern".
        #
        # Nothing may assert the tree's contents from row position. The
        # true statement — no EARLIER ROW of this run has been built —
        # is what the first-row branch is for.
        _, sc, _ = self._per_module()
        for label in ('m0-plan', 'm0-tests', 'm1-plan', 'm1-tests'):
            with self.subTest(label=label):
                msg = sc.message_for_label(label)
                self.assertNotIn('base state', msg)
                self.assertNotIn('Nothing has been built yet', msg)

    def test_the_first_row_still_scopes_itself_to_rows_not_the_tree(
        self,
    ) -> None:
        # The regression the first-row branch exists to stop: telling
        # row 0 that prior work is in its worktree sends it hunting for
        # artifacts of rows that genuinely do not exist, and a careful
        # agent then rightly refuses to build against a missing
        # baseline. Keep that guarantee — bound to the ROWS, which the
        # runner knows, not to the tree, which it does not.
        _, sc, _ = self._per_module()
        for label in ('m0-plan', 'm0-tests'):
            with self.subTest(label=label):
                msg = sc.message_for_label(label)
                self.assertIn('FIRST', msg)
                self.assertNotIn('already implemented and FROZEN', msg)

    def test_config_modules_not_overridden_by_planner_subtasks(self) -> None:
        # A per-module planner does NOT re-chunk: even if its output
        # carries a SUBTASKS block, the human module list stands.
        _, _sc, wt = self._per_module(
            replies={
                'm0-plan': _plan_with_subtasks('x', 'y', 'z'),
                'm1-plan': 'DESIGN M1',
            }
        )
        self.assertIn('m1-build', wt.node_from)      # the 2 config modules
        self.assertNotIn('x-build', wt.node_from)    # not the proposed ones
        self.assertNotIn('m0-x-build', wt.node_from)

    def test_interactive_awaits_approval_per_module(self) -> None:
        _, sc, _ = self._run(
            _PER_MODULE, {'m0-plan': 'D0', 'm1-plan': 'D1'}
        )
        # each module's planner (only) awaited human approval, in order.
        self.assertEqual(
            [sc.label_of(s) for s in sc.approvals], ['m0-plan', 'm1-plan']
        )

    def test_interactive_consolidation_is_per_module(self) -> None:
        _, sc, _ = self._run(
            _PER_MODULE, {'m0-plan': 'D0', 'm1-plan': 'D1'}
        )
        plan_sid = next(s for s, lb in sc._label.items() if lb == 'm0-plan')
        sends = [m for s, m in sc.sent if s == plan_sid]
        self.assertEqual(len(sends), 2)  # initial + consolidation
        # consolidation asks for THIS module's design, not a chunk list.
        self.assertIn('[m0]', sends[1])
        self.assertNotIn('SUBTASKS', sends[1])


class FakeHarvester:
    """Stand-in for the spawned harvester child process."""

    def __init__(self, pid: int = 4242) -> None:
        self.pid = pid
        self.terminated = False

    def terminate(self) -> None:
        self.terminated = True

    def wait(self, timeout=None) -> int:
        return 0

    def kill(self) -> None:
        self.terminated = True


class TestAgyAutoHarvest(_Base):
    """The runner starts (and stops) the agy harvester itself, so an
    agy pipeline has no hidden background prerequisite."""

    def setUp(self) -> None:
        super().setUp()
        self.lock = self.root / 'h.lock'
        self.log = self.root / 'h.log'
        self.spawned: list[FakeHarvester] = []

    def _spawn(self, **_kw) -> FakeHarvester:
        proc = FakeHarvester()
        self.spawned.append(proc)
        return proc

    def _stamp(self, age_s: float | None):
        path = self.root / 'agy-harvest.json'
        if age_s is not None:
            R.agy.record_harvest('f', path=path, now=1000.0 - age_s)
        return path

    def _ensure(self, cfg_text: str, age_s, *, fresh=True, **kw):
        return R.ensure_agy_harvester(
            self._cfg(cfg_text),
            stamp_path=self._stamp(age_s),
            lock_path=self.lock,
            log_path=self.log,
            spawn=self._spawn,
            wait=lambda **_k: fresh,
            now=1000.0,
            **kw,
        )

    def test_no_agy_agents_never_starts_one(self) -> None:
        # An all-Claude pipeline must not pay for agy plumbing.
        self.assertIsNone(self._ensure(_TDD, None))
        self.assertEqual(self.spawned, [])

    def test_fresh_secret_still_starts_one_when_none_runs(self) -> None:
        # THE REGRESSION: a fresh secret with nothing refreshing it
        # expires ~an hour in and strands every later agy turn. Start a
        # harvester anyway — freshness now says nothing about freshness
        # 90 minutes from now.
        proc = self._ensure(_LINEAR, 60.0)
        self.assertIs(proc, self.spawned[0])

    def test_fresh_secret_and_a_running_harvester_starts_nothing(self):
        # That loop already owns refreshing; a second would race on the
        # trusted box's token file.
        held = R.agy.acquire_harvest_lock(self.lock)
        self.addCleanup(held.close)
        self.assertIsNone(self._ensure(_LINEAR, 60.0))
        self.assertEqual(self.spawned, [])

    def test_fresh_secret_does_not_wait(self) -> None:
        # Already usable: drive immediately instead of burning the
        # first-refresh deadline.
        waited = []
        R.ensure_agy_harvester(
            self._cfg(_LINEAR),
            stamp_path=self._stamp(60.0),
            lock_path=self.lock,
            log_path=self.log,
            spawn=self._spawn,
            wait=lambda **_k: waited.append(1) or True,
            now=1000.0,
        )
        self.assertEqual(waited, [])

    def test_stale_secret_starts_a_harvester(self) -> None:
        proc = self._ensure(_LINEAR, 99999.0)
        self.assertIs(proc, self.spawned[0])
        self.assertFalse(proc.terminated)

    def test_missing_stamp_starts_a_harvester(self) -> None:
        self.assertIsNotNone(self._ensure(_LINEAR, None))
        self.assertEqual(len(self.spawned), 1)

    def test_never_competes_with_a_running_harvester(self) -> None:
        # Lock held elsewhere: wait for THAT loop, never start a rival.
        held = R.agy.acquire_harvest_lock(self.lock)
        self.addCleanup(held.close)
        self.assertIsNone(self._ensure(_LINEAR, 99999.0))
        self.assertEqual(self.spawned, [])

    def test_secret_never_freshens_stops_the_child_and_raises(self):
        with self.assertRaises(click.ClickException) as ctx:
            self._ensure(_LINEAR, None, fresh=False)
        self.assertTrue(self.spawned[0].terminated)  # no orphan
        self.assertIn('plan', str(ctx.exception))    # names the agent

    def test_auto_off_restores_the_refusal(self) -> None:
        with self.assertRaises(click.ClickException) as ctx:
            self._ensure(_LINEAR, None, auto=False)
        self.assertIn('omni-sbx-agy harvest', str(ctx.exception))
        self.assertEqual(self.spawned, [])

    def test_stop_harvester_is_a_noop_for_none(self) -> None:
        R.stop_harvester(None)  # the "we started nothing" path

    def test_cli_stops_the_harvester_it_started(self) -> None:
        # The child must not outlive the run that started it.
        cfg_path = self.root / 'pipeline.yaml'
        cfg_path.write_text(_LINEAR, encoding='utf-8')
        proc = FakeHarvester()
        with mock.patch.object(
            R, 'ensure_agy_harvester', return_value=proc
        ), mock.patch.object(R, 'preflight_disk'), \
                mock.patch.object(R, '_drive') as drive:
            res = CliRunner().invoke(
                R.main,
                [
                    '-c', str(cfg_path),
                    '--canonical-root', str(self.root / 'c'),
                    '--worktree-root', str(self.root / 'w'),
                ],
            )
        self.assertEqual(res.exit_code, 0)
        drive.assert_called_once()
        self.assertTrue(proc.terminated)

    def test_cli_stops_the_harvester_even_when_the_run_raises(self):
        cfg_path = self.root / 'pipeline.yaml'
        cfg_path.write_text(_LINEAR, encoding='utf-8')
        proc = FakeHarvester()
        with mock.patch.object(
            R, 'ensure_agy_harvester', return_value=proc
        ), mock.patch.object(R, 'preflight_disk'), \
                mock.patch.object(
            R, '_drive', side_effect=RuntimeError('boom')
        ):
            CliRunner().invoke(
                R.main,
                [
                    '-c', str(cfg_path),
                    '--canonical-root', str(self.root / 'c'),
                    '--worktree-root', str(self.root / 'w'),
                ],
            )
        self.assertTrue(proc.terminated)


class TestAgySwapGuard(_Base):
    """An agy turn is never pasted into a TUI that cannot authenticate:
    an expired swap token makes agy fail silently INSIDE its own UI, so
    the turn would hang to the timeout and report nothing."""

    def test_stale_secret_refuses_before_the_turn(self) -> None:
        with self.assertRaises(R.PipelineRunError) as ctx:
            self._run(
                _LINEAR, dict(_LINEAR_REPLIES), swap_age_s=lambda: 99999.0
            )
        msg = str(ctx.exception)
        self.assertIn('expired', msg)
        self.assertIn('harvester', msg)  # names what to go look at

    def test_never_harvested_refuses_too(self) -> None:
        # Unknown age is exactly as unusable as far-too-old.
        with self.assertRaises(R.PipelineRunError):
            self._run(
                _LINEAR, dict(_LINEAR_REPLIES), swap_age_s=lambda: None
            )

    def test_boundary_age_still_drives(self) -> None:
        result, _sc, _wt = self._run(
            _LINEAR,
            dict(_LINEAR_REPLIES),
            swap_age_s=lambda: R.agy.MAX_SWAP_AGE_S,
        )
        self.assertEqual(result.status, 'completed')

    def test_stale_secret_never_reaches_the_agy_session(self) -> None:
        # The point is to spend nothing: no turn, no staged task file.
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        runner = R.PipelineRunner(
            cfg,
            session_client=sc,
            worktree_manager=wt,
            run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 99999.0,
        )
        with self.assertRaises(R.PipelineRunError):
            runner.run()
        self.assertEqual(sc.sent, [])
        self.assertEqual(wt.ignored_files, [])

    def test_claude_only_pipeline_ignores_a_stale_secret(self) -> None:
        # No agy agent means no swap token is in play at all.
        result, _sc, _wt = self._run(
            _TDD,
            {'tests': 't', 'build': 'b'},
            swap_age_s=lambda: 99999.0,
        )
        self.assertEqual(result.status, 'completed')


def _plan_text(size: int, marker: str = '') -> str:
    """
    Plan-SHAPED text of roughly *size* chars.

    The selection rules are about relative size and recency, but the
    guard is now structural — so filler like ``'x' * 4000`` is correctly
    not a plan, and these fixtures have to look like one.
    """
    head = (
        '## Files\n'
        '- src/core.rs — the parser\n'
        '- src/lib.rs — exports it\n'
        '- tests/parse_test.rs — the cases\n\n'
        '## Interfaces\n'
        '`parse` takes a spec and returns whole seconds; its single '
        'responsibility is parsing.\n\n'
        '## Algorithm\n'
        '1. Read the spec\n2. Split the units\n3. Sum them\n\n'
        '## Edge cases\n'
        'An invalid suffix must fail; a malformed spec raises.\n\n'
        '## Test strategy\n'
        'The test author must verify each unit test case.\n\n'
        f'{marker}\n'
    )
    pad = max(0, size - len(head))
    return head + ('detail. ' * (pad // 8 + 1))[:pad]


#: The live pipeline's own shape: plan, a tests-only writer, two
#: isolated implementers, a consensus review per candidate, a judge, a
#: refactor, a review of the refactor, and a triage pass. Every other
#: fixture here is a slice of this; none was the whole thing, which is
#: how four defects in the plan-to-builder handoff reached production.
_LIVE_CADRE = """\
name: cadre
repo: ./proj
publish: none
task: |
  build the adapter
acceptance: |
  the gate is green
agents:
  plan: {template: planner, model: claude-sonnet-5}
  tdd:  {template: tdd-writer, model: claude-sonnet-5}
  ca:   {template: coder, model: claude-sonnet-5}
  cb:   {template: coder, model: claude-sonnet-5}
  sec:  {template: security-reviewer, model: claude-fable-5}
  bugs: {template: bug-reviewer, model: claude-sonnet-5}
  jg:   {template: judge, model: claude-opus-4-8}
  rf:   {template: refactoring, model: claude-sonnet-5}
  tri:  {template: verifier, model: claude-sonnet-5}
stages:
  - {id: plan, run: plan}
  - {id: tests, run: tdd, write: true, needs: [plan]}
  - id: implement
    parallel:
      - {id: impl-a, run: ca, write: true, from: tests, needs: [plan]}
      - {id: impl-b, run: cb, write: true, from: tests, needs: [plan]}
  - id: review
    parallel:
      - id: review-a
        run: [sec, bugs]
        needs: [impl-a]
        gate: consensus
        on_block: impl-a
      - id: review-b
        run: [sec, bugs]
        needs: [impl-b]
        gate: consensus
        on_block: impl-b
  - {id: pick, run: jg, needs: [impl-a, impl-b], selects: branch}
  - {id: refactor, run: rf, write: true, from: pick, needs: [pick]}
  - id: review-r
    run: [sec, bugs]
    needs: [refactor]
    gate: consensus
    on_block: refactor
  - id: triage
    run: tri
    verifies: findings
    from: refactor
    needs: [review-r]
"""


def _real_plan(sections: int = 18) -> str:
    """A plan the size and shape a planner actually emits.

    Every plan-side defect this suite now guards against was invisible
    to a short fixture: a summary that cited sections it did not
    contain, a recap pattern that vetoed 127KB for one phrase on page
    forty, a horizontal rule lifted into the decisions ledger. Scale and
    shape are the test, so this carries numbered cross-referenced
    sections, many
    named files, a DECISIONS block closed by a rule, and -- deliberately
    -- the phrase "is complete" used legitimately, deep in the body.
    """
    out = [
        '# [m9] Widget adapter — design plan',
        '',
        '**Module:** the widget adapter.',
        '',
    ]
    for n in range(1, sections + 1):
        out += [
            f'## {n}. Section {n}',
            '',
            f'Covered in §{max(1, n - 1)}; §{min(sections, n + 1)} carries '
            f'the consequence. See also §{sections}.',
            '',
            f'- `acme/component/mod{n}.py` — the {n}th module',
            f'- `tests/unit/test_mod{n}.py` — its cases',
            f'- `catalogs/rules_{n}.yaml` — the {n}th rule file',
            '',
            'An invalid input must raise; a malformed one is refused. The '
            'test author verifies each case, and the interface is a single '
            'function whose parameters are named below. ' + ('Detail. ' * 40),
            '',
        ]
    out += [
        '## Work items',
        '',
        '- Cover every declared service, or give a written statement of why '
        'fewer is complete.',
        '',
        '## DECISIONS FOR LATER MODULES:',
        '',
        '- **The transport owns its watermark grammar.** An adapter must '
        'never parse one, because only the transport knows its own format.',
        '- **Attestations never enter the success-only table.** They are a '
        'separate evidence kind and are reported as one.',
        '',
        '---',
        '',
        '## QUESTIONS:',
        '',
        'None; everything above is settled.',
    ]
    return '\n'.join(out)


def _real_review(verdict: str, *findings: str) -> str:
    """A reviewer reply shaped the way reviewers actually write them."""
    body = ['Review is blocking despite a green suite.' if findings
            else 'No blocking findings.', '']
    if findings:
        body += ['### Blocking findings', '']
        for i, f in enumerate(findings, 1):
            body += [f'{i}. {f}', '',
                     '   I reproduced the consequence against the branch, '
                     'and the green suite misses it because its scan only '
                     'recognises the bare form.', '']
    body += [
        '### Verification', '',
        '- `uv run --locked pytest`: **465 passed**.',
        '- `ruff check .`: passed.',
        '- `uv audit`: no known vulnerabilities.',
        '- No frozen tests were modified.',
        '',
        f'VERDICT: {verdict}',
    ]
    return '\n'.join(body)


def _real_judge(winner: str) -> str:
    """A judge reply: an evaluation that ends in the SELECT marker."""
    return '\n'.join([
        f'### Evaluation of `{winner}`', '',
        'Both candidates were read against the frozen contract.', '',
        '1. **Success-only boundary** — declaratively configured, with '
        'failure-first precedence and an ambiguous default.',
        '2. **Read-only gate** — every SDK call is allow-listed with a '
        'written justification.', '',
        f'SELECT: {winner}',
    ])


class TestTheLiveCadreCarriesRealArtifacts(_Base):
    """
    Drive the production DAG end to end with artifacts at real scale.

    Every fixture in this file before this one was a slice — the richest
    covered seven of the live pipeline's eight stages, and all of them
    fed one-word replies. That is why four defects in the
    plan-to-builder handoff reached production: a 400-character plan
    passes every check a
    127KB plan breaks.

    This drives all eight stages with a plan carrying numbered
    cross-referenced sections and the phrase "is complete" deep in its
    body, reviewer replies shaped the way reviewers write them, and a
    judge reply that ends in the SELECT marker. The assertions are about
    SURVIVAL: what an agent said reaches the agent that needs it.
    """

    def _replies(self, **over: str) -> dict[str, str]:
        base = {
            'plan': _real_plan(),
            'tests': 'suite written',
            'impl-a': 'implemented',
            'impl-b': 'implemented',
            'review-a-sec': _real_review('APPROVED'),
            'review-a-bugs': _real_review('APPROVED'),
            'review-b-sec': _real_review('APPROVED'),
            'review-b-bugs': _real_review('APPROVED'),
            'pick': _real_judge('impl-a'),
            'refactor': 'cleaned up',
            'review-r-sec': _real_review('APPROVED'),
            'review-r-bugs': _real_review('APPROVED'),
            'triage': 'checked',
        }
        base.update(over)
        return base

    def test_the_whole_cadre_runs(self) -> None:
        result, _sc, _wt = self._run(_LIVE_CADRE, self._replies())
        self.assertEqual(result.status, 'completed')

    def test_the_plan_reaches_the_builders_whole(self) -> None:
        # The m2-3 and m2-4 failures in one assertion: the builders
        # got a
        # 3,006-character table of contents in one run, and in the next
        # the runner destroyed the plan and aborted.
        plan = _real_plan()
        _r, sc, _wt = self._run(_LIVE_CADRE, self._replies(plan=plan))

        for node in ('tests', 'impl-a', 'impl-b'):
            sent = [m for s, m in sc.sent if sc.label_of(s) == node]
            self.assertTrue(sent, f'{node} was never driven')
            self.assertIn(
                'Section 12', sent[0],
                f'{node} did not receive the plan body',
            )

    def test_the_decisions_ledger_takes_no_separator(self) -> None:
        # `---` closing the DECISIONS block parsed as a bullet whose
        # text was `--`, and landed in the committed ledger that later
        # modules read.
        _r, _sc, wt = self._run(_LIVE_CADRE, self._replies())
        ledger = [
            v
            for k, v in wt.artifacts.items()
            if 'decision' in k.lower()
        ]
        for doc in ledger:
            for line in doc.splitlines():
                if line.strip().startswith('-'):
                    self.assertGreater(
                        len(line.strip()), 6,
                        f'a separator was lifted as a decision: {line!r}',
                    )

    def test_a_realistic_blocking_review_yields_its_findings(self) -> None:
        # A BLOCKING verdict whose findings do not survive extraction is
        # recorded as an empty block, which reads as a reviewer that
        # refuses to sign off rather than one that found two real bugs.
        reply = _real_review(
            'BLOCKING',
            'Credentials are declared with a decorator the frozen scan '
            'cannot see, so `repr()` prints the access key verbatim.',
        )
        self.assertEqual(R.parse_verdict(reply), 'BLOCKING')
        self.assertTrue(
            R.parse_findings(reply) or 'Blocking findings' in reply,
            'a realistic blocking reply carried no extractable finding',
        )

    def test_the_judge_pick_is_read_from_a_real_evaluation(self) -> None:
        # The marker sits under 3,000 characters of prose, not alone.
        _r, _sc, _wt = self._run(
            _LIVE_CADRE, self._replies(pick=_real_judge('impl-b'))
        )
        self.assertEqual(R.parse_select(_real_judge('impl-b')), 'impl-b')

    def test_the_cadre_plan_is_representative(self) -> None:
        # Renamed: this collided with an identically named method later
        # in the same class, so Python silently kept the other one and
        # this never ran. Two fixtures, two scales, two tests.
        # The defects only appear at scale. Pin the properties that made
        # them visible, so a future trim cannot quietly restore the
        # blindness.
        plan = _real_plan()
        self.assertGreater(len(plan), 8000)
        self.assertGreater(
            plan.index('is complete'), R._PLAN_RECAP_WINDOW,
            'the recap phrase must sit outside the opening window',
        )
        self.assertEqual(R.plan_shape_failures(plan), [])

    def test_a_refactor_review_is_scoped_to_the_diff(self) -> None:
        # review-r used to receive the implementation instruction: the
        # whole task, the whole acceptance contract, and "review the
        # working tree against this contract". So it re-read the entire
        # module every round against the entire brief and blocked on
        # whatever it found — a loop with no end, because a large
        # module always has one more real defect in it.
        _r, sc, _wt = self._run(_LIVE_CADRE, self._replies())

        sent = [m for s_, m in sc.sent if 'review-r' in sc.label_of(s_)]
        self.assertTrue(sent, 'review-r was never driven')
        self.assertIn('You are reviewing a REFACTOR', sent[0])
        self.assertIn('YOUR SUBJECT IS THE CHANGE', sent[0])
        self.assertIn('functionally IDENTICAL', sent[0])
        self.assertNotIn(
            'Review the working tree in your mount', sent[0]
        )

    def test_a_refactor_review_defers_a_pre_existing_defect(self) -> None:
        # The half that ends the loop: a defect on BOTH sides of the
        # diff is not something this refactor did.
        _r, sc, _wt = self._run(_LIVE_CADRE, self._replies())

        msg = next(
            m for s_, m in sc.sent if 'review-r' in sc.label_of(s_)
        )
        self.assertIn('PRE-EXISTING', msg)
        self.assertIn('NON-BLOCKING list', msg)

    def test_an_implementation_review_keeps_the_whole_contract(
        self,
    ) -> None:
        # The new framing must reach review-r ONLY. A writer that
        # produced the whole tree is rightly read against the whole
        # contract.
        _r, sc, _wt = self._run(_LIVE_CADRE, self._replies())

        msg = next(
            m for s_, m in sc.sent if 'review-a' in sc.label_of(s_)
        )
        self.assertIn('Review the working tree in your mount', msg)
        self.assertNotIn('You are reviewing a REFACTOR', msg)


class TestARealPlanSurvivesTheWholeHandoff(unittest.TestCase):
    """
    One regression test for the path that has cost four planning cycles.

    Every plan-side defect found so far -- a summary accepted as a
    design, a recap pattern vetoing a 127KB plan for one substring, the
    approved text discarded, a horizontal rule lifted as a decision --
    was found by LOSING A CAMPAIGN, because nothing exercised the
    handoff with a plan the size and shape a real planner emits.

    So this builds one: numbered sections with cross-references, many
    named files, a DECISIONS block closed by a rule, and the phrase
    "is complete" used legitimately deep in the body. It then walks the
    whole path a plan takes between the approval gate and the builders.

    A change that would reject or mangle a real plan now fails here, in
    a second, instead of thirty minutes into a campaign.
    """

    @staticmethod
    def _realistic_plan() -> str:
        body = [_plan_text(3000), '']
        for n in range(1, 18):
            body += [
                f'## {n}. Section {n}',
                f'Covered in §{max(1, n - 1)}; see also §{min(17, n + 1)}.',
                f'- `acme/component/mod{n}.py` — the {n}th module',
                f'- `tests/unit/test_mod{n}.py` — its cases',
                'An invalid input must raise; the test author verifies each.',
                '',
            ]
        body += [
            '## Work items',
            '- Cover each declared service, or give a written statement of '
            'why fewer is complete.',
            '',
            '## DECISIONS FOR LATER MODULES:',
            '',
            '- **The transport owns its watermark grammar.** An adapter '
            'must never parse one.',
            '- **Attestations never enter the success-only table.** They '
            'are a separate evidence kind.',
            '',
            '---',
            '',
            '## QUESTIONS:',
            'None; everything above is settled.',
        ]
        return '\n'.join(body)

    def test_it_is_recognised_as_a_plan(self) -> None:
        self.assertEqual(R.plan_shape_failures(self._realistic_plan()), [])

    def test_it_survives_selection_with_nothing_else_readable(self) -> None:
        # The live m2-4 shape: an ack consolidation and an empty reply
        # read. The approved text must come back intact.
        plan = self._realistic_plan()
        self.assertEqual(
            R.select_plan_of_record('The plan is complete.', [], plan), plan
        )

    def test_its_decisions_lift_cleanly(self) -> None:
        got = R.parse_decisions(self._realistic_plan())
        self.assertEqual(len(got), 2)
        self.assertTrue(
            all(len(d) > 40 for d in got),
            f'a rule or separator was lifted as a decision: {got}',
        )

    def test_a_horizontal_rule_does_not_become_a_decision(self) -> None:
        # `---` closing a DECISIONS block parsed as a bullet whose text
        # is `--`, which would land verbatim in the committed ledger
        # that [m3]-[m7] read.
        text = (
            'DECISIONS FOR LATER MODULES:\n'
            '- **A real decision.** With a reason attached to it.\n'
            '---\n'
            '## Next section\n'
        )
        self.assertEqual(len(R.parse_decisions(text)), 1)

    def test_it_is_large_enough_to_be_representative(self) -> None:
        # The defects only appear at real scale; a 400-char fixture
        # would have passed every one of them.
        self.assertGreater(len(self._realistic_plan()), 5000)


class TestTheRecapCheckOnlyReadsTheOpening(unittest.TestCase):
    """
    An acknowledgement LEADS with "the plan is complete". A design plan
    may mention the phrase once, in passing, on page forty.

    The recap patterns were unanchored, so they vetoed a document of any
    size for one substring anywhere in it. Measured on the recovered
    `ingestion-m2-4` plan: 127,683 characters, 52 sections, and the only
    match was at line 907 of 1,003 -- work item 12's "or a written
    statement of why fewer is complete". That plan was rejected as an
    acknowledgement.

    This is very likely what killed that run: the consolidation turn is
    asked to re-emit the whole plan, and the whole plan contains the
    phrase. The `approved` fallback added alongside this does NOT cover
    it -- the approved text carries the same sentence -- so both
    candidates were rejected for the same false positive.

    The original catch is preserved by construction: the recap it was
    built for was 1,774 characters, so it lies entirely inside the
    opening window.
    """

    _RECAP = (
        'The plan has been finalized and APPROVED by the user. The plan '
        'phase is complete. I updated src/core.rs and src/lib.rs.'
    )

    def test_the_live_recap_is_still_rejected(self) -> None:
        why = R.plan_shape_failures(self._RECAP)
        self.assertTrue(
            any('acknowledgement' in w for w in why),
            f'the recap this check exists for must stay rejected: {why}',
        )

    def test_a_recap_padded_past_the_window_is_still_rejected(self) -> None:
        # Padding an acknowledgement must not buy it a pass; it still
        # fails the structural checks even if the phrase scrolls away.
        why = R.plan_shape_failures(self._RECAP + ' Thanks. ' * 400)
        self.assertNotEqual(why, [])

    def test_a_long_plan_mentioning_the_phrase_late_passes(self) -> None:
        plan = (
            _plan_text(6000)
            + '\n\n## Work items\n'
            + '- Cover each declared service, or give a written statement '
              'of why fewer is complete.\n'
        )
        self.assertEqual(R.plan_shape_failures(plan), [])

    def test_the_opening_is_still_read(self) -> None:
        # A plan-shaped body cannot rescue an acknowledgement that opens
        # by announcing itself.
        text = 'The plan is now complete.\n\n' + _plan_text(6000)
        why = R.plan_shape_failures(text)
        self.assertTrue(any('acknowledgement' in w for w in why))


class TestTheApprovedTextIsNeverDiscarded(unittest.TestCase):
    """
    The text the HUMAN approved is always a candidate plan of record.

    `_await_plan_approval` already holds it: `wait_for_plan_approval`
    returns the last assistant message before the approval turn, which
    is the document the human read and said APPROVED to. It was then
    passed to nothing, and `select_plan_of_record` judged only the
    consolidation reply and a SECOND read of the session.

    Cost, live, on `ingestion-m2-4`: the gate returned 128,861
    characters of approved plan; the consolidation turn answered with an
    acknowledgement; `read_assistant_replies` came back empty -- it
    reads a 60-item tail where the approval gate reads 200, and it
    swallows a read error as `[]`. With no candidate left the run
    aborted, and the 128,861 characters were discarded while sitting in
    a local variable. The session was disposed at teardown, so the plan
    a human had spent the evening on was gone.

    A second read failing must never outrank the text already in hand.
    """

    @staticmethod
    def _plan() -> str:
        return _plan_text(4000)

    def test_the_approved_text_is_used_when_no_reply_is_readable(self):
        # The live failure: replies empty, consolidation an ack.
        plan = self._plan()
        self.assertEqual(
            R.select_plan_of_record('The plan is complete.', [], plan),
            plan,
        )

    def test_it_no_longer_raises_when_the_approved_text_is_a_plan(self):
        try:
            R.select_plan_of_record('Approved and complete.', [], self._plan())
        except R.PipelineRunError:  # pragma: no cover - the regression
            self.fail('discarded an approved plan it was holding')

    def test_a_compliant_consolidation_still_wins(self) -> None:
        # The common case must not move: a consolidation turn that
        # returned the document is still the plan of record.
        good = self._plan()
        self.assertEqual(
            R.select_plan_of_record(good, [], 'ignored'), good
        )

    def test_it_still_raises_when_nothing_anywhere_is_a_plan(self) -> None:
        # The guard must keep its teeth: an acknowledgement everywhere
        # is still fatal, which is the whole reason it exists.
        with self.assertRaises(R.PipelineRunError):
            R.select_plan_of_record('done', ['ok'], 'approved, thanks')

    def test_an_absent_approved_text_is_not_a_candidate(self) -> None:
        with self.assertRaises(R.PipelineRunError):
            R.select_plan_of_record('done', [], None)

    def test_it_never_outranks_a_later_reply_that_is_a_plan(self) -> None:
        # It is captured BEFORE the consolidation turn, so it is older
        # than anything in `replies`. Treating it as the latest
        # candidate made a pre-Q&A draft beat the revision that
        # superseded it -- caught by
        # test_short_ack_recovers_the_real_plan_for_builders.
        revision = _plan_text(4000, 'the revision')
        stale = _plan_text(4000, 'the first draft')
        self.assertEqual(
            R.select_plan_of_record('done', [stale, revision], stale),
            revision,
        )


class TestASummaryOfAPlanIsNotAPlan(unittest.TestCase):
    """
    A well-formed SUMMARY of a design is not the design.

    The recap check catches a reply that reads as an acknowledgement.
    It does not catch a reply that is genuinely structured — headings,
    bullets, file names, real technical content — but whose substance
    lives in a document it only POINTS AT: "§3 package layout", "§11
    migration 0002", nine such references and not one of those sections
    present.

    That passed every structural check and became the plan of record
    for [m2] attempt 3. Both implementers and the test author were
    handed 3,006 characters of table of contents; the planner's closing
    line was "I wrote nothing to disk".

    The signal is that a plan CONTAINS its sections and a summary
    REFERS to them. Measured over this project's committed plans:

        ingestion-m0                    8 refs, 8 resolved
        ingestion-m0b                   6 refs, 6 resolved
        ingestion-m2-attempt-1         14 refs, 14 resolved
        ingestion-m1 (the acknowledgement)  2 refs, 0 resolved
        m2-3 planner output             9 refs, 0 resolved

    Every real plan resolves every reference it makes. Both failures
    resolve none — including the m1 acknowledgement this module already
    names, found independently by this check.
    """

    @staticmethod
    def _numbered(body: str) -> str:
        return (
            '## 1. Files\n'
            '- src/core.rs — the parser\n'
            '- src/lib.rs — exports it\n'
            '- tests/parse_test.rs — the cases\n\n'
            '## 2. Interfaces\n'
            '`parse` takes a spec and returns whole seconds.\n\n'
            '## 3. Algorithm\n'
            '1. Read the spec\n2. Split the units\n3. Sum them\n\n'
            '## 4. Edge cases\n'
            'An invalid suffix must fail; a malformed spec raises.\n\n'
            '## 5. Test strategy\n'
            'Unit tests cover each fixture.\n\n'
            + body
        )

    def test_a_plan_citing_its_own_sections_passes(self) -> None:
        # The common case: a real plan cross-references itself
        # constantly, and every reference resolves.
        text = self._numbered(
            'The parser in §1 is driven by the algorithm in §3, and '
            '§4 names the failure modes §5 must cover.\n'
        ) + 'Detail. ' * 400
        self.assertEqual(R.plan_shape_failures(text), [])

    def test_a_summary_citing_absent_sections_is_rejected(self) -> None:
        summary = (
            'Plan of record released.\n\n'
            '## What is frozen for the downstream stages\n'
            '- **Design** — §3 package layout, §4 the core module '
            'interfaces, §6 the six catalogs, §9 transport and the '
            'watermark grammar, §11 migration `0002`.\n'
            '- **Work items 1-25** with mechanical done-criteria.\n'
            '- **Gates G1-G22**, each with its named mutation.\n'
            '- The three closed questions (§19) and the fifteen '
            'departures (§2).\n\n'
            '## Where the tests stage starts\n'
            '- Items 2, 3, 7 need no SDK: the allow-list load in '
            '`catalogs/read_only_sdk_allowlist.yaml`, the transport '
            'protocol in `acme/component/transport.py`, and the '
            'watermark ordering property.\n'
            '- Items 4 and 17 need the pinned fixture on a real '
            'cluster; invalid input must raise.\n\n'
            "I wrote nothing to disk - this session is the plan's "
            'record.\n'
        ) + 'Everything above is settled and frozen. ' * 60
        why = R.plan_shape_failures(summary)
        self.assertTrue(
            any('section' in w for w in why),
            f'a summary of a plan must be rejected, got {why}',
        )

    def test_the_rejection_says_which_sections_are_missing(self) -> None:
        # The halt has to tell a human what to go get.
        summary = (
            '## Summary\n'
            '- The design is in §3 and §11 of the session.\n'
            '- Files: `src/core.rs`, `src/lib.rs`, `tests/t.rs`.\n'
            '- Steps: 1. read 2. split 3. sum\n'
            '- Edge cases: invalid input raises.\n'
            '- Test strategy: unit tests per fixture.\n'
            '- Interfaces: `parse` returns whole seconds.\n'
            '- Gates: each ships its own mutation.\n'
            '- Work items: 1 through 25, with done-criteria.\n'
            '- Escalation: a frozen edit halts the stage.\n'
        ) + 'Detail. ' * 400
        why = R.plan_shape_failures(summary)
        joined = ' '.join(why)
        self.assertIn('3', joined)
        self.assertIn('11', joined)

    def test_a_plan_that_cites_no_sections_is_unaffected(self) -> None:
        # Most plans never write "§" at all. The check must not invent
        # a requirement to number your headings.
        self.assertEqual(R.plan_shape_failures(_plan_text(4000)), [])

    def test_a_mostly_resolving_plan_passes(self) -> None:
        # A real plan may cite the BRIEF's section numbering alongside
        # its own. Only a document that resolves NOTHING is a summary.
        text = self._numbered(
            'Per §7 of the brief, the parser in §1 must reject a '
            'malformed spec, and §3 says how.\n'
        ) + 'Detail. ' * 400
        self.assertEqual(R.plan_shape_failures(text), [])


class TestPlanShapeCheck(unittest.TestCase):
    """A plan is recognised by SHAPE, never by length (TASKS.md #29)."""

    def test_a_real_plan_passes(self) -> None:
        self.assertEqual(R.plan_shape_failures(_plan_text(4000)), [])

    def test_the_live_recap_is_rejected(self) -> None:
        # Verbatim shape of the reply that became the design of record
        # for an entire Kubernetes provider. It cleared the old
        # 1500-char floor by 274 characters.
        recap = (
            'The design plan for **`[m4] Kubernetes provider`** has been '
            'finalized and **APPROVED** by the user.\n\n'
            '### Final Summary of the Approved Design Plan\n\n'
            '1. Module architecture\n2. Authentication\n3. Scope '
            'hierarchy\n4. Identities\n\n'
            'The plan phase for module `[m4]` is complete.\n'
        ) + ('padding to clear the old length floor. ' * 40)
        why = R.plan_shape_failures(recap)
        self.assertTrue(why)
        self.assertTrue(
            any('acknowledgement' in w for w in why), why
        )

    def test_length_alone_is_never_enough(self) -> None:
        # The whole point: a long reply that is not a design fails.
        why = R.plan_shape_failures('words. ' * 3000)
        self.assertTrue(why)

    def test_a_plan_that_names_no_files_is_rejected(self) -> None:
        # "The exact files to add or change" is the FIRST thing
        # templates/planner.md asks for.
        text = _plan_text(4000).replace('src/core.rs', 'the core module')
        text = text.replace('src/lib.rs', 'the library root')
        text = text.replace('tests/parse_test.rs', 'the parser tests')
        self.assertTrue(
            any('names' in w and 'file' in w
                for w in R.plan_shape_failures(text))
        )

    def test_the_ingestion_m1_acknowledgement_is_rejected(self) -> None:
        """The reply that became an entire module's plan of record.

        Verbatim shape, and it beat every check that existed: 1,800
        characters against a 1,500 floor, it names files so the file
        check passes, and it scores exactly the three word-level
        signals needed — because an acknowledgement ABOUT a plan reuses
        the plan's own vocabulary. What it does not do is decompose.
        """
        ack = (
            'Released as the plan of record for [m1].\n\n'
            '**No files written.** The plan stage is interactive and '
            'read-only, so the document lives in this transcript for the '
            'pipeline to publish as `docs/plans/ingestion-m1.md`; I did '
            'not create it, and I touched nothing in the worktree.\n\n'
            '**What the tests stage inherits.** Thirteen work items, each '
            'with done-criteria written to be turned straight into '
            'failing tests. Three properties must fail when the property '
            'is removed: deleting the ledger insert must break '
            'idempotency, moving the checkpoint advance out of the '
            'transaction must break crash recovery, and a denied-event '
            'fixture must never produce a tuple.\n\n'
            '**Four frozen-artifact changes are pre-authorised** and no '
            'others: the `tests/unit/test_models.py` discovery rewrite, '
            'the `tests/support/schema.py` consistency check, the '
            '`acme/component/sdk_allowlist.py` fixes, and the '
            '`pyproject.toml` promotion. Anything else frozen that '
            'appears to need changing is a halt-and-escalate.\n\n'
            '**The single item worth disproportionate attention** is the '
            'confinement of permission-string construction. Everything '
            'else can be repaired by a later module; a vocabulary '
            'divergence cannot. If the tests stage writes only one scan '
            'test carefully, that is the one.\n\n'
            'The one thing I did persist is outside the repo: a memory '
            'note recording that the runner matches guarded paths with '
            '`fnmatch` rather than `glob`, since `runner.py` is not in '
            'this repository and that fact cannot be re-derived here. '
            'Every work item is mapped to at least one test file, and '
            'the done-criteria are written so the test author can turn '
            'each straight into a failing test rather than interpreting '
            'it. Nothing else in the worktree was touched.\n'
        )
        self.assertGreater(len(ack), R._PLAN_MIN_CHARS)

        why = R.plan_shape_failures(ack)

        self.assertTrue(why, 'the acknowledgement was accepted as a plan')
        self.assertTrue(
            any('heading/bullet/step' in w for w in why), why
        )

    def test_prose_alone_is_not_a_plan_however_long(self) -> None:
        # The same failure in its general form: a reply carrying every
        # word-level signal but no structure is not decomposed, so it
        # cannot be handed to a test author as a specification.
        flat = '\n'.join(
            line.lstrip('#-* 0123456789.)')
            for line in _plan_text(6000).splitlines()
        )

        why = R.plan_shape_failures(flat)

        self.assertTrue(
            any('heading/bullet/step' in w for w in why), why
        )

    def test_a_plan_with_sections_clears_the_structure_floor(self) -> None:
        # The other side of it: the check must not reject a real design.
        # Every published plan on this project carries 100+ such lines.
        self.assertEqual(R.plan_shape_failures(_plan_text(4000)), [])

    def test_empty_is_rejected(self) -> None:
        self.assertEqual(R.plan_shape_failures(''), ['it is empty'])
        self.assertEqual(R.plan_shape_failures(None), ['it is empty'])


class TestPlanOfRecordSelection(unittest.TestCase):
    """A chat acknowledgement must never become the plan of record."""

    def test_substantive_consolidation_is_used(self) -> None:
        doc = _plan_text(4000, 'FINAL')
        self.assertIn(
            'FINAL', R.select_plan_of_record(doc, [_plan_text(3000), doc])
        )

    def test_short_ack_falls_back_to_the_real_plan(self) -> None:
        # THE live regression, at the observed sizes: a 371-char "the
        # plan is released and frozen" blurb replaced a 12,412-char
        # design and was handed to every builder as the plan.
        draft = _plan_text(14618, 'DRAFT')
        plan = _plan_text(12412, 'PLAN')
        ack = 'The plan is released and frozen. ' * 11
        replies = [draft, 'just checking in. ' * 130, plan,
                   'still working on it. ' * 70, ack]
        self.assertIn('PLAN', R.select_plan_of_record(ack, replies))

    def test_latest_substantive_wins_not_longest(self) -> None:
        # A post-Q&A revision supersedes the first draft and is often
        # SHORTER — picking the longest would resurrect the draft.
        draft = _plan_text(14618, 'DRAFT')
        revised = _plan_text(12412, 'REVISED')
        ack = 'The plan is approved. ' * 5
        self.assertIn(
            'REVISED', R.select_plan_of_record(ack, [draft, revised, ack])
        )

    def test_consolidated_plan_shorter_than_the_draft_is_kept(self):
        # THE live regression: the consolidation turn DID emit a real
        # "Final Approved Design Plan" (5165), but an earlier verbose
        # draft (15088) had scaled the old bar to 7544, so it was
        # discarded for a PRE-approval draft (10276). A consolidated
        # plan is legitimately shorter than a rambling first draft.
        draft = _plan_text(15088, 'DRAFT')
        revised = _plan_text(10276, 'REVISED')
        final = _plan_text(5165, 'FINAL')
        self.assertIn(
            'FINAL',
            R.select_plan_of_record(final, [draft, revised, 'ok', final]),
        )

    def test_mid_session_chatter_is_never_promoted(self) -> None:
        # Once the reply is known NOT to be a plan, a chatty status
        # update clears any length floor while being nothing like a
        # design. It has to fail on SHAPE.
        plan = _plan_text(12412, 'PLAN')
        chatter = 'I have designed the plan and will now summarise. ' * 40
        ack = 'The plan is approved. ' * 5
        self.assertIn(
            'PLAN', R.select_plan_of_record(ack, [plan, chatter, ack])
        )

    def test_no_plan_anywhere_halts_the_run(self) -> None:
        # #29's point: a plan that does not exist used to be
        # indistinguishable from one that does, and the run continued
        # into eight agents' worth of work either way.
        with self.assertRaises(R.PipelineRunError) as caught:
            R.select_plan_of_record('the plan is approved', ['hi', 'ok'])
        self.assertIn('no design plan', str(caught.exception))

    def test_the_halt_says_what_was_wrong(self) -> None:
        # A refusal that does not say why just moves the mystery.
        with self.assertRaises(R.PipelineRunError) as caught:
            R.select_plan_of_record('short', [])
        msg = str(caught.exception)
        self.assertIn('planner.md', msg)
        self.assertIn('characters', msg)


class _Usage:
    """shutil.disk_usage stand-in with a fixed free byte count."""

    def __init__(self, free: int) -> None:
        self.free = free

    def __call__(self, _path):
        return self


class TestLosingImplementationsAreKept(_Base):
    """The implementation that loses is the only copy of itself.

    Two writers build the same frozen contract and only the winner
    publishes; the loser's branch lives on the run hub, which teardown
    deletes. It is complete, reviewed and test-passing, and it is the
    comparison data every later argument about model choice would want
    (TASKS.md #32).
    """

    def _race(self, **kw):
        return self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-b'},
            **kw,
        )

    def test_the_loser_is_retained(self) -> None:
        _r, _sc, wt = self._race()
        kept = [(n, a) for _run, n, a in wt.retained]
        self.assertEqual(kept, [('impl-a', 'main')])

    def test_the_winner_is_not_retained(self) -> None:
        # It publishes; a second copy would be noise.
        _r, _sc, wt = self._race()
        self.assertNotIn('impl-b', [n for _run, n, _a in wt.retained])

    def test_the_record_says_where_the_loser_lives(self) -> None:
        _r, _sc, wt = self._race()
        written = {t[1]: t[2] for t in wt.tracked_files}
        doc = written['docs/plans/race-selection.md']
        self.assertIn('not chosen', doc)
        self.assertIn('/can/_retained/r1/impl-a.bundle', doc)

    def test_the_pr_says_where_the_loser_lives(self) -> None:
        _r, _sc, wt = self._race()
        body = wt.pr_bodies[-1]
        self.assertIn('/can/_retained/r1/impl-a.bundle', body)

    def test_a_loser_that_wrote_nothing_is_not_archived(self) -> None:
        # git refuses an empty bundle, and a writer that produced
        # nothing beyond the base is not worth a file.
        wt = FakeWT()
        wt.retain_empty.add('impl-a')
        _r, _sc, wt = self._race(wt=wt)
        written = {t[1]: t[2] for t in wt.tracked_files}
        self.assertNotIn(
            '.bundle', written.get('docs/plans/race-selection.md', '')
        )

    def test_a_retention_failure_never_sinks_a_shippable_run(self) -> None:
        # The run produced a winner. Losing the archive is a real loss
        # and must be SAID, but it is not a reason to fail the publish.
        wt = FakeWT()
        wt.retain_raises.add('impl-a')
        result, _sc, wt = self._race(wt=wt)
        self.assertEqual(result.status, 'completed')
        self.assertIsNotNone(wt.published)

    def test_retention_survives_a_resume(self) -> None:
        _r, _sc, wt = self._race()
        picks = wt.states[-1]['judge_picks']
        self.assertEqual(
            picks[0]['retained'],
            [['impl-a', '/can/_retained/r1/impl-a.bundle']],
        )


class TestBlockedOnAModalPrompt(_Base):
    """A picker on screen must produce the ACTION, not a screenshot.

    The failure a human sees otherwise describes the paste mechanism and
    buries the thing to do (TASKS.md #12).
    """

    _PICKER = (
        'Choose how you would like Codex to proceed.\n\n'
        '> 1. Try new model\n'
        '  2. Use existing model\n\n'
        'Use up/down to move, press enter to confirm'
    )

    def _failed_turn_showing(self, pane_text):
        sc = FakeSC(dict(_LINEAR_REPLIES))
        sc.fail_labels.add('build')
        sc.default_host_id = 'h1'
        sc.host_names['h1'] = 'managed-h1'
        with mock.patch.object(
            R.pane, 'capture_pane', return_value=pane_text
        ):
            with mock.patch.object(R.click, 'echo') as echo:
                with self.assertRaises(R.PipelineRunError):
                    self._run(_LINEAR, dict(_LINEAR_REPLIES), sc=sc)
        said = ' '.join(
            str(c.args[0]) for c in echo.call_args_list if c.args
        )
        return said

    def test_a_picker_produces_actionable_guidance(self) -> None:
        said = self._failed_turn_showing(self._PICKER)
        self.assertIn('[blocked]', said)
        self.assertIn('CANNOT answer', said)
        self.assertIn('Open this agent', said)

    def test_the_guidance_shows_the_prompt_itself(self) -> None:
        said = self._failed_turn_showing(self._PICKER)
        self.assertIn('Try new model', said)

    def test_an_ordinary_failure_gets_no_prompt_guidance(self) -> None:
        # Most failures are not a picker; claiming one would send a
        # human to a pane with nothing to answer.
        said = self._failed_turn_showing('a traceback, no picker here')
        self.assertNotIn('[blocked]', said)


class TestLaunchIsVerified(_Base):
    """The launcher must check it got what it asked for (#28)."""

    def _run_with_pane(self, pane_text):
        sc = FakeSC(dict(_LINEAR_REPLIES))
        sc.default_host_id = 'h1'
        sc.host_names['h1'] = 'managed-h1'
        with mock.patch.object(
            R.pane, 'capture_pane', return_value=pane_text
        ):
            with mock.patch.object(R.click, 'echo') as echo:
                result, _sc, wt = self._run(
                    _LINEAR, dict(_LINEAR_REPLIES), sc=sc
                )
        said = ' '.join(
            str(c.args[0]) for c in echo.call_args_list if c.args
        )
        return result, said, wt

    def test_a_downgraded_permission_mode_is_reported(self) -> None:
        _r, said, _wt = self._run_with_pane('  ⏸ manual mode on')
        self.assertIn('permission mode', said)
        self.assertIn("asked for 'bypassPermissions'", said)

    def test_an_honoured_launch_is_silent(self) -> None:
        # The footer text is verbatim from a live VM, so this test also
        # pins the string readback matches bypassPermissions on.
        _r, said, _wt = self._run_with_pane(
            '  ⏵⏵ bypass permissions on (shift+tab to cycle)'
        )
        self.assertNotIn('[launch]', said)

    def test_a_mismatch_never_fails_the_run(self) -> None:
        # It reads a TUI mid-draw; a false alarm must not cost a run,
        # and a check that can fail one gets switched off.
        result, _said, _wt = self._run_with_pane('  ⏸ manual mode on')
        self.assertEqual(result.status, 'completed')

    def test_an_unreadable_pane_does_not_warn(self) -> None:
        _r, said, _wt = self._run_with_pane(None)
        self.assertNotIn('[launch]', said)


class TestDiskMetricsAreOptIn(_Base):
    """Recording what a run costs on disk (TASKS.md #36).

    Every disk figure this project has is a hand measurement taken
    during an incident, or an inference from one. One instrumented run
    replaces the lot — but measuring a 26 GB build tree walks its
    inodes, so a routine run must not pay for it.
    """

    @contextlib.contextmanager
    def _recording(self, *, layers=(1, 1), sample_raises=None):
        """Switch the recorder on; yields its (sample, append) mocks."""
        records = [{'x': 1}]
        with (
            mock.patch.object(
                R.disk_metrics, 'enabled', return_value=True
            ),
            mock.patch.object(
                R.orphans, 'layer_bytes', return_value=layers
            ),
            mock.patch.object(
                R.disk_metrics, 'sample',
                return_value=records, side_effect=sample_raises,
            ) as sample,
            mock.patch.object(
                R.disk_metrics, 'append', return_value=True
            ) as append,
        ):
            yield sample, append

    @staticmethod
    def _events(sample):
        return [c.kwargs['event'] for c in sample.call_args_list]

    def test_a_normal_run_records_nothing(self) -> None:
        with mock.patch.object(R.disk_metrics, 'append') as append:
            self._run(_LINEAR, dict(_LINEAR_REPLIES))
        append.assert_not_called()

    def test_switching_it_on_samples_every_stage_boundary(self) -> None:
        with self._recording() as (sample, _append):
            self._run(_LINEAR, dict(_LINEAR_REPLIES))
        events = self._events(sample)
        self.assertIn('stage-complete:plan', events)
        self.assertIn('stage-complete:build', events)

    def test_it_records_the_guest_store_alongside(self) -> None:
        # The same run also answers whether per_vm_gb is right.
        with self._recording(layers=(6, 30_000_000_000)) as (sample, _a):
            self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertEqual(
            sample.call_args_list[0].kwargs['store_layers'],
            (6, 30_000_000_000),
        )

    def test_a_campaign_samples_the_module_peak_before_reclaim(self) -> None:
        # Every tree the chunk built is still on disk one line
        # before the reclaim takes them — that IS the peak the
        # preflight predicts, and nothing else observes it.
        with self._recording() as (sample, _append):
            self._run(_PER_MODULE, {'m0-plan': 'D0', 'm1-plan': 'D1'})
        self.assertEqual(self._events(sample).count('chunk-peak'), 2)

    def test_the_record_lives_outside_the_run_dir(self) -> None:
        # A COMPLETED run deletes its own run directory, so a record
        # kept there would survive only failures (TASKS.md #30).
        with self._recording() as (_sample, append):
            self._run(_LINEAR, dict(_LINEAR_REPLIES))
        path = str(append.call_args_list[0].args[0])
        self.assertTrue(path.startswith('/can/'), path)
        self.assertNotIn('/wt/', path)

    def test_a_recorder_that_explodes_never_fails_the_run(self) -> None:
        with self._recording(sample_raises=RuntimeError('boom')):
            result, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertEqual(result.status, 'completed')
        self.assertIsNotNone(wt.published)


class TestJudgeSelectionIsRecorded(_Base):
    """The two-writer race must leave an audit trail.

    It is the most expensive thing the pipeline does — it doubles the
    implementation AND review stages — and whether that is repaid
    depends on whether both candidates ever win. Before this,
    ``SELECT:`` appeared in no published artifact at all (TASKS.md #31).
    """

    def _race(self, pick='SELECT: impl-b'):
        return self._run(
            _COMPETE, {'impl-a': 'A', 'impl-b': 'B', 'pick': pick}
        )

    def test_the_choice_lands_in_the_run_dir_immediately(self) -> None:
        # Written before the judge's VM is disposed, so a run that fails
        # after the judge still says what it picked.
        _r, _sc, wt = self._race()
        self.assertIn('impl-b', wt.artifacts['judging/pick.md'])

    def test_the_choice_is_committed_beside_the_reviews(self) -> None:
        _r, _sc, wt = self._race()
        written = {t[1]: t[2] for t in wt.tracked_files}
        self.assertIn('docs/plans/race-selection.md', written)
        doc = written['docs/plans/race-selection.md']
        self.assertIn('impl-a', doc)
        self.assertIn('impl-b', doc)

    def test_the_judges_reasoning_survives_its_session(self) -> None:
        _r, _sc, wt = self._race(
            'impl-b handles the empty case.\nSELECT: impl-b'
        )
        written = {t[1]: t[2] for t in wt.tracked_files}
        self.assertIn(
            'handles the empty case',
            written['docs/plans/race-selection.md'],
        )

    def test_the_pr_says_which_candidate_won(self) -> None:
        # A PR reviewer sees ONE branch; without this there is
        # nothing to say another was built and set aside.
        _r, _sc, wt = self._race()
        body = wt.pr_bodies[-1]
        self.assertIn('## Selection', body)
        self.assertIn('`impl-b`', body)
        self.assertIn('`impl-a`', body)

    def test_an_absent_select_is_not_recorded_as_a_preference(self) -> None:
        # Stronger than it used to be. The runner no longer falls back
        # to the first candidate, so there is no pick to misreport: the
        # stage halts, nothing is published, and no selection document
        # claims a winner. The judge's own words are still preserved —
        # _run_judge captures its turn to the run directory first.
        wt, sc = FakeWT(), FakeSC(
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'I could not decide.'},
        )
        with self.assertRaises(R.PipelineRunError):
            self._run(_COMPETE, {}, wt=wt, sc=sc)
        written = {t[1]: t[2] for t in wt.tracked_files}
        self.assertNotIn('docs/plans/race-selection.md', written)
        self.assertIsNone(wt.published)

    def test_a_pipeline_with_no_judge_records_nothing(self) -> None:
        _r, _sc, wt = self._run(_LINEAR, {'plan': 'P', 'build': 'b'})
        written = {t[1] for t in wt.tracked_files}
        self.assertFalse([w for w in written if 'selection' in w])

    def test_the_record_survives_a_resume(self) -> None:
        # It is persisted in run state, like the reviewer reports.
        _r, _sc, wt = self._race()
        picks = wt.states[-1]['judge_picks']
        self.assertEqual(picks[0]['selected'], 'impl-b')
        self.assertEqual(picks[0]['stated'], 'impl-b')
        self.assertEqual(picks[0]['candidates'], ['impl-a', 'impl-b'])


class TestJudgeCannotSeeTheAuthor(_Base):
    """Writer commits must not name the agent that produced them.

    Each judge candidate is a standalone clone with a real ``.git``
    directory, so ``git log`` works inside the judge's VM. Attributing a
    commit to ``impl_claude`` therefore hands the judge the model family
    it is supposed to be blind to (TASKS.md #33). Reviewers see the same
    through their read-only node mount.
    """

    def test_writer_commits_name_the_node_not_the_agent(self) -> None:
        # _COMPETE is the discriminating fixture: nodes impl-a / impl-b
        # are run by agents ca / cb, so a leak is visible.
        _result, _sc, wt = self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-b'},
        )
        writers = {
            c[0]: c[2] for c in wt.commits if c[2] and 'implement' in c[1]
        }
        self.assertIn('impl-a <impl-a@pipeline.local>', writers['impl-a'])
        self.assertIn('impl-b <impl-b@pipeline.local>', writers['impl-b'])

    def test_no_commit_anywhere_names_an_agent(self) -> None:
        # Belt and braces: one leak is as good as all of them, so assert
        # over EVERY commit rather than the two writer ones.
        _result, _sc, wt = self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-b'},
        )
        blob = ' '.join(f'{c[1]} {c[2] or ""}' for c in wt.commits)
        for agent in ('ca', 'cb', 'jg'):
            with self.subTest(agent=agent):
                self.assertNotIn(f'{agent}@pipeline.local', blob)

    def test_no_commit_leaks_a_model_or_harness_name(self) -> None:
        # The agent KEYS in _COMPETE are neutral, but their models are
        # not — nothing that reaches git may name one.
        _result, _sc, wt = self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-b'},
        )
        blob = ' '.join(
            f'{c[1]} {c[2] or ""}' for c in wt.commits
        ).lower()
        for token in ('claude', 'codex', 'gpt', 'sonnet', 'opus', 'gemini'):
            with self.subTest(token=token):
                self.assertNotIn(token, blob)


class TestResumeReclaimsBeforeMeasuring(_Base):
    """A resume must not be refused by space it would itself free.

    Observed live: a machine crashed mid-module leaving six orphaned
    microVMs holding ~26 GB; the host measured 15.6 GB free against a
    46.5 GB demand and refused — while the reclaim that would have
    returned the 26 GB sat downstream of the refusal (TASKS.md #7).
    """

    class _WT:
        def __init__(self, state):
            self._state = state
            self.removed: list[tuple[str, list[str]]] = []

        def read_run_state(self, run_id):
            return self._state

        def dispose_node_worktrees(self, run_id, node_ids):
            self.removed.append((run_id, list(node_ids)))
            return len(node_ids)

    class _SC:
        def __init__(self, raises=()):
            self.disposed: list[str] = []
            self._raises = set(raises)

        def dispose(self, session):
            if session in self._raises:
                raise SwarmSessionError('gone')
            self.disposed.append(session)

    def _state(self, **over):
        state = {
            'sessions': ['s1', 's2'],
            'completed_chunks': ['m0'],
            'nodes': {'m0-build': {}, 'm0-plan': {}, 'm1-build': {}},
        }
        state.update(over)
        return state

    def _reclaim(self, state, keep=False, sc=None, wt=None):
        wt = wt if wt is not None else self._WT(state)
        sc = sc if sc is not None else self._SC()
        said: list[str] = []
        freed = R.reclaim_for_resume(
            run_id='r1', canonical_root='/c', worktree_root='/wt',
            server='http://x', keep=keep, default_branch='main',
            client=sc, manager=wt, echo=said.append,
        )
        return freed, sc, wt, ' '.join(said)

    def test_the_previous_attempts_vms_are_disposed(self) -> None:
        _f, sc, _wt, said = self._reclaim(self._state())
        self.assertEqual(sc.disposed, ['s1', 's2'])
        self.assertIn('before measuring disk', said)

    def test_published_modules_give_up_their_worktrees(self) -> None:
        # Their branches are on the hub and their PR is open; the run
        # can rebuild none of it, and they are the largest thing left.
        _f, _sc, wt, _said = self._reclaim(self._state())
        self.assertEqual(wt.removed, [('r1', ['m0-build', 'm0-plan'])])

    def test_an_unpublished_module_is_left_alone(self) -> None:
        _f, _sc, wt, _said = self._reclaim(self._state())
        removed = [n for _r, nodes in wt.removed for n in nodes]
        self.assertNotIn('m1-build', removed)

    def test_keep_reclaims_nothing(self) -> None:
        # The human asked for the previous attempt's VMs to stay.
        freed, sc, wt, said = self._reclaim(self._state(), keep=True)
        self.assertEqual((freed, sc.disposed, wt.removed), (0, [], []))
        self.assertIn('--keep', said)

    def test_a_vm_already_gone_is_not_fatal(self) -> None:
        _f, sc, _wt, _said = self._reclaim(
            self._state(), sc=self._SC(raises={'s1'})
        )
        self.assertEqual(sc.disposed, ['s2'])

    def test_no_state_means_nothing_to_do(self) -> None:
        freed, sc, _wt, _said = self._reclaim(None)
        self.assertEqual((freed, sc.disposed), (0, []))


class TestPreflightIsResumeAware(_Base):
    def test_worktrees_already_on_disk_are_not_demanded_twice(self) -> None:
        # A resumed run does not re-cut the trees it already carries.
        cfg = self._cfg(_LINEAR)
        free = [0]

        def usage(_path):
            return _Usage(free[0])

        # Sized so the run fits ONLY once the tree is subtracted.
        free[0] = 5_000_000_000 + 2 * 3_500_000_000
        with self.assertRaises(click.ClickException):
            R.preflight_disk(cfg, usage=usage)
        R.preflight_disk(cfg, usage=usage, worktrees_on_disk=1)

    def test_it_never_demands_less_than_nothing(self) -> None:
        cfg = self._cfg(_LINEAR)
        R.preflight_disk(
            cfg,
            usage=lambda _p: _Usage(10 ** 12),
            worktrees_on_disk=999,
        )


class TestDiskPreflight(_Base):
    """A host that fills mid-run corrupts its guests: refuse first."""

    def test_only_writers_and_one_review_stage_sum(self) -> None:
        # _LINEAR is plan(reader) + build(writer) + review(sec). The
        # reader is freed the moment its stage completes and is never
        # driven again, so it is not part of any peak: build + sec = 2.
        self.assertEqual(R.max_concurrent_vms(self._cfg(_LINEAR)), 2)

    def test_keep_counts_the_reader_too(self) -> None:
        # --keep frees nothing, so everything accumulates.
        self.assertEqual(
            R.max_concurrent_vms(self._cfg(_LINEAR), reclaim=False), 3
        )

    def test_a_finished_judge_is_not_part_of_the_peak(self) -> None:
        # It has already picked; nothing re-drives a judge. Holding it
        # to publish is what put 6 GB out of reach of a refactor build
        # on a live 17 GB host.
        cfg = self._cfg(_COMPETE)          # impl-a + impl-b + pick
        self.assertEqual(R.max_concurrent_vms(cfg), 2)
        self.assertEqual(R.max_concurrent_vms(cfg, reclaim=False), 3)

    def test_counts_each_reviewer_in_a_consensus_gate(self) -> None:
        # A review stage runs one session PER reviewer agent, so a
        # two-reviewer gate costs two VMs, not one. Here: build +
        # (sec, bugs) = 3.
        cfg = self._cfg(
            'name: gate\nrepo: ./p\npublish: none\ntask: |\n  go\n'
            'agents:\n'
            '  build: {template: coder, model: claude-sonnet-5}\n'
            '  sec: {template: security-reviewer, model: claude-sonnet-5}\n'
            '  bugs: {template: bug-reviewer, model: claude-sonnet-5}\n'
            'stages:\n'
            '  - {id: build, run: build, write: true}\n'
            '  - id: rev\n'
            '    run: [sec, bugs]\n'
            '    needs: [build]\n'
            '    gate: consensus\n'
            '    on_block: build\n'
        )
        self.assertEqual(R.max_concurrent_vms(cfg), 3)

    def _three_gates(self) -> str:
        # The shape the live cadre uses: two isolated writers,
        # each with its own consensus gate, plus one on the refactor.
        return (
            'name: cadre\nrepo: ./p\npublish: none\ntask: |\n  go\n'
            'agents:\n'
            '  a: {template: coder, model: claude-sonnet-5}\n'
            '  b: {template: coder, model: claude-sonnet-5}\n'
            '  ref: {template: refactoring, model: claude-sonnet-5}\n'
            '  sec: {template: security-reviewer, model: claude-sonnet-5}\n'
            '  bugs: {template: bug-reviewer, model: claude-sonnet-5}\n'
            'stages:\n'
            '  - id: impl\n'
            '    parallel:\n'
            '      - {id: a, run: a, write: true}\n'
            '      - {id: b, run: b, write: true}\n'
            '  - id: rev-a\n    run: [sec, bugs]\n    needs: [a]\n'
            '    gate: consensus\n    on_block: a\n'
            '  - id: rev-b\n    run: [sec, bugs]\n    needs: [b]\n'
            '    gate: consensus\n    on_block: b\n'
            '  - {id: ref, run: ref, write: true, from: a, needs: [rev-a]}\n'
            '  - id: rev-r\n    run: [sec, bugs]\n    needs: [ref]\n'
            '    gate: consensus\n    on_block: ref\n'
        )

    def test_only_the_largest_review_stage_counts(self) -> None:
        # Reviewers are freed as soon as their votes are in, so the
        # three gates here are never up together: 3 writers + 2
        # reviewers, not 3 + 6. Summing them demanded 14 GB of disk
        # that no longer gets used, which refused runs that fit.
        cfg = self._cfg(self._three_gates())
        self.assertEqual(R.max_concurrent_vms(cfg), 5)

    def test_keep_sums_every_gate_because_nothing_is_freed(self) -> None:
        cfg = self._cfg(self._three_gates())
        self.assertEqual(R.max_concurrent_vms(cfg, reclaim=False), 9)

    def test_a_parallel_group_of_gates_is_still_sequential(self) -> None:
        # `parallel:` means isolated BRANCHES, not concurrent
        # execution — _exec_stage walks a group one child at a time —
        # so grouping gates does not raise the peak.
        cfg = self._cfg(
            'name: pg\nrepo: ./p\npublish: none\ntask: |\n  go\n'
            'agents:\n'
            '  a: {template: coder, model: claude-sonnet-5}\n'
            '  sec: {template: security-reviewer, model: claude-sonnet-5}\n'
            '  bugs: {template: bug-reviewer, model: claude-sonnet-5}\n'
            'stages:\n'
            '  - {id: a, run: a, write: true}\n'
            '  - id: gates\n'
            '    parallel:\n'
            '      - id: g1\n        run: [sec, bugs]\n'
            '        needs: [a]\n        gate: consensus\n'
            '        on_block: a\n'
            '      - id: g2\n        run: [sec, bugs]\n'
            '        needs: [a]\n        gate: consensus\n'
            '        on_block: a\n'
        )
        self.assertEqual(R.max_concurrent_vms(cfg), 3)  # a + 2, not a + 4

    def test_counts_both_writers_in_a_parallel_group(self) -> None:
        # A parallel group is counted by its CHILDREN, never twice —
        # here impl-a + impl-b (the judge is freed when it finishes).
        self.assertEqual(R.max_concurrent_vms(self._cfg(_COMPETE)), 2)

    def test_ample_disk_passes(self) -> None:
        R.preflight_disk(self._cfg(_LINEAR), usage=_Usage(500_000_000_000))

    def test_tight_disk_refuses_naming_the_numbers(self) -> None:
        with self.assertRaises(click.ClickException) as ctx:
            R.preflight_disk(self._cfg(_LINEAR), usage=_Usage(1_000_000))
        msg = str(ctx.exception)
        self.assertIn('free', msg)
        self.assertIn('needed', msg)
        self.assertIn('read-only', msg.lower())  # names the real symptom
        self.assertIn('--skip-disk-check', msg)  # and the override
        self.assertIn('worktree', msg)           # both terms are shown
        self.assertIn('microVM', msg)
        self.assertIn('disk:', msg)              # …and how to tune them

    def test_budget_covers_vms_and_worktrees_together(self) -> None:
        # _LINEAR peaks at 2 VMs (build + sec; the reader is freed)
        # and 1 build worktree (a reader cuts none it keeps, a reviewer
        # mounts the writer's).
        need = (
            5_000_000_000
            + 2 * R._VM_DISK_BYTES
            + 1 * R._WORKTREE_DISK_BYTES
        )
        R.preflight_disk(self._cfg(_LINEAR), usage=_Usage(need))
        with self.assertRaises(click.ClickException):
            R.preflight_disk(self._cfg(_LINEAR), usage=_Usage(need - 1))

    def test_worktrees_alone_can_refuse_a_run(self) -> None:
        # THE gap this closes: room for every microVM, but not for
        # the build trees they mount. The VM-only estimate said yes here
        # and the host filled two modules later.
        vms_only = 5_000_000_000 + 2 * R._VM_DISK_BYTES
        R.preflight_disk(
            self._cfg(_LINEAR), usage=_Usage(vms_only), per_worktree_bytes=0
        )
        with self.assertRaises(click.ClickException):
            R.preflight_disk(self._cfg(_LINEAR), usage=_Usage(vms_only))

    def test_counts_a_build_worktree_per_writer(self) -> None:
        self.assertEqual(R.writer_worktrees(self._cfg(_LINEAR)), 1)
        self.assertEqual(R.writer_worktrees(self._cfg(_TDD)), 2)

    def test_readers_judges_and_reviewers_cost_no_worktree(self) -> None:
        # _COMPETE is 2 writers + a judge; _LINEAR a reader + a writer +
        # a reviewer. A node clone is LOCAL (hardlinked objects),
        # so only the tree an agent BUILDS in is worth budgeting.
        self.assertEqual(R.writer_worktrees(self._cfg(_COMPETE)), 2)
        self.assertEqual(R.writer_worktrees(self._cfg(_LINEAR)), 1)

    def test_the_verify_gate_adds_a_build_worktree(self) -> None:
        # Its throwaway clone builds and tests from clean.
        self.assertEqual(
            R.writer_worktrees(self._cfg(_VERIFY)),
            R.writer_worktrees(self._cfg(_LINEAR)) + 1,
        )

    def test_keep_multiplies_by_the_module_count(self) -> None:
        # --keep suppresses BOTH per-chunk reclaims, so a campaign's
        # modules accumulate instead of peaking at one.
        cfg = self._cfg(_PER_MODULE)  # 2 modules, 3 VMs + 2 writers each
        one_module = (
            5_000_000_000
            + 3 * R._VM_DISK_BYTES
            + 2 * R._WORKTREE_DISK_BYTES
        )
        R.preflight_disk(cfg, usage=_Usage(one_module))
        with self.assertRaises(click.ClickException) as ctx:
            R.preflight_disk(cfg, keep=True, usage=_Usage(one_module))
        msg = str(ctx.exception)
        self.assertIn('2 modules held at once', msg)
        self.assertIn('--keep', msg)

    def test_keep_does_not_multiply_a_single_pass_run(self) -> None:
        # No modules to accumulate: --keep changes nothing.
        cfg = self._cfg(_LINEAR)
        need = (
            5_000_000_000
            + 3 * R._VM_DISK_BYTES
            + 1 * R._WORKTREE_DISK_BYTES
        )
        R.preflight_disk(cfg, keep=True, usage=_Usage(need))

    def test_the_pipeline_can_tune_the_per_unit_estimates(self) -> None:
        # The runner cannot know what a project builds; a Python repo
        # leaves almost nothing in its worktree.
        light = self._cfg(
            _LINEAR.replace(
                'publish: local',
                'publish: local\ndisk:\n'
                '  per_worktree_gb: 0.1\n  per_vm_gb: 0.5\n'
                '  headroom_gb: 1\n',
            )
        )
        self.assertEqual(light.disk.per_worktree_gb, 0.1)
        # 1 + 3*0.5 + 1*0.1 = 2.6 GB, far under the compiled default.
        R.preflight_disk(light, usage=_Usage(2_600_000_000))
        with self.assertRaises(click.ClickException):
            R.preflight_disk(self._cfg(_LINEAR), usage=_Usage(2_600_000_000))

    def test_cli_passes_keep_to_the_disk_guard(self) -> None:
        cfg_path = self.root / 'pipeline.yaml'
        cfg_path.write_text(_LINEAR, encoding='utf-8')
        with mock.patch.object(R, 'preflight_disk') as guard, \
                mock.patch.object(R, 'ensure_agy_harvester'), \
                mock.patch.object(R, '_drive'):
            CliRunner().invoke(
                R.main,
                [
                    '-c', str(cfg_path),
                    '--canonical-root', str(self.root / 'c'),
                    '--worktree-root', str(self.root / 'w'),
                    '--keep',
                ],
            )
        self.assertTrue(guard.call_args.kwargs['keep'])

    def test_cli_skip_flag_bypasses_the_disk_guard(self) -> None:
        cfg_path = self.root / 'pipeline.yaml'
        cfg_path.write_text(_LINEAR, encoding='utf-8')
        with mock.patch.object(
            R, 'preflight_disk', side_effect=AssertionError('ran')
        ) as guard, mock.patch.object(R, 'ensure_agy_harvester'), \
                mock.patch.object(R, '_drive'):
            res = CliRunner().invoke(
                R.main,
                [
                    '-c', str(cfg_path),
                    '--canonical-root', str(self.root / 'c'),
                    '--worktree-root', str(self.root / 'w'),
                    '--skip-disk-check',
                ],
            )
        self.assertEqual(res.exit_code, 0)
        guard.assert_not_called()


class TestChunkVmDisposal(_Base):
    """A published chunk's VMs are dead weight — hold them and a
    multi-module run exhausts the host's disk."""

    def _modules(self, **kw):
        return self._run(
            _PER_MODULE,
            {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
            **kw,
        )

    def test_each_chunk_disposes_its_own_vms(self) -> None:
        _, sc, _ = self._modules()
        # every session created is torn down by the end...
        self.assertEqual(sorted(set(sc.disposed)), sorted(sc._label))
        # ...and m0's are gone BEFORE m1's were ever created.
        m0 = [s for s, lb in sc._label.items() if lb.startswith('m0-')]
        m1 = [s for s, lb in sc._label.items() if lb.startswith('m1-')]
        self.assertTrue(m0 and m1)
        first_m1 = min(sc.creates.index(c) for c in sc.creates
                       if c['sid'] in m1)
        self.assertTrue(all(s in sc.disposed for s in m0))
        self.assertEqual(len(sc.creates) - 1 >= first_m1, True)

    def test_keep_leaves_every_chunk_vm_up(self) -> None:
        _, sc, _ = self._modules(keep=True)
        self.assertEqual(sc.disposed, [])


class TestRunStatePersistence(_Base):
    """Committed work survives a crash on its own; the bookkeeping
    around it does not — so it is checkpointed as the run advances."""

    def test_state_is_written_as_each_stage_completes(self) -> None:
        # Not once at the end: a crash must leave behind what was
        # already achieved.
        _, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        stages = len([s for s in ('plan', 'build', 'review') if s])
        self.assertGreaterEqual(len(wt.states), stages)

    def test_state_carries_the_approved_plan_of_record(self) -> None:
        # THE expensive thing to lose: a plan a human sat through.
        _, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertIn('PLAN', wt.states[-1]['plan_of_record'])

    def test_state_carries_node_branches_and_kinds(self) -> None:
        _, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        nodes = wt.states[-1]['nodes']
        self.assertEqual(nodes['build']['branch'], 'pl/r1/build')
        self.assertEqual(nodes['build']['kind'], 'writer')
        self.assertEqual(nodes['plan']['kind'], 'reader')

    def test_state_carries_a_judge_selection(self) -> None:
        # A judge's pick exists nowhere in git under its own name.
        _, _sc, wt = self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-b'},
        )
        self.assertEqual(wt.states[-1]['nodes']['pick']['selected'], 'impl-b')

    def test_state_never_records_a_session(self) -> None:
        # A session belongs to a VM a later run cannot reattach to;
        # recording one would invite a resume to trust a dead handle.
        _, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        for node in wt.states[-1]['nodes'].values():
            self.assertNotIn('session', node)

    def test_state_tracks_campaign_position(self) -> None:
        _, _sc, wt = self._run(
            _PER_MODULE,
            {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
        )
        ids = [s['id'] for s in wt.states[-1]['subtasks']]
        self.assertEqual(ids, ['m0', 'm1'])
        # Mid-module snapshots name the module being built.
        seen = {s['active_subtask'] for s in wt.states}
        self.assertIn('m0', seen)
        self.assertIn('m1', seen)

    def test_chunk_completion_persists_before_the_next_module(self):
        # The next stage is the following module's planner, which blocks
        # on the human — so a chunk's completion must reach disk at
        # publish, not whenever that planner finally returns. Otherwise
        # a resume in that window rebuilds the module and re-publishes.
        _, _sc, wt = self._run(
            _PER_MODULE,
            {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
        )
        first_m0 = next(
            i for i, s in enumerate(wt.states)
            if 'm0' in (s.get('completed_chunks') or [])
        )
        # ...and no m1 stage had completed by then.
        m1_done = wt.states[first_m0]['completed']
        self.assertFalse([c for c in m1_done if c.startswith('m1-')])

    def test_state_records_each_published_chunk(self) -> None:
        _, _sc, wt = self._run(
            _PER_MODULE,
            {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
        )
        published = ' '.join(wt.states[-1]['published'])
        self.assertIn('r1-m0', published)
        self.assertIn('r1-m1', published)

    def test_state_is_versioned(self) -> None:
        _, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertEqual(wt.states[-1]['version'], R.RUN_STATE_VERSION)


class TestResume(_Base):
    """A resume continues a run instead of repeating it."""

    def _finished_state(self, completed, **over):
        state = {
            'version': R.RUN_STATE_VERSION,
            'completed': list(completed),
            'completed_chunks': [],
            'plan_of_record': 'PLAN',
            'published': [],
            'last_branch_node': None,
            'subtasks': [],
            'nodes': {
                n: {
                    'kind': 'reader' if n == 'plan' else 'writer',
                    'branch': None if n == 'plan' else f'pl/r1/{n}',
                    'worktree': f'/wt/r1/nodes/{n}',
                    'output': 'PLAN' if n == 'plan' else 'done',
                    'verdict': None,
                    'selected': None,
                }
                for n in completed
            },
        }
        state.update(over)
        return state

    def _resume(self, state, replies=None, **kw):
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(replies or _LINEAR_REPLIES))
        wt.state_to_load = state
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, resume=True, **kw,
        )
        return runner.run(), sc, wt

    def test_a_resumed_run_still_commits_the_planning_session(
        self,
    ) -> None:
        # THE bug behind #30. A resumed run restores its nodes WITHOUT a
        # session (see test_state_never_records_a_session), and the
        # session guard used to run BEFORE the buffer — so the one copy
        # that survives was never consulted and no planning session
        # record ever reached the repo.
        state = self._finished_state(['plan'])
        state['reader_turns'] = {
            'plan': [
                ['user', 'Design it.'],
                ['assistant', 'Here is the design.'],
            ]
        }
        _r, _sc, wt = self._resume(state)
        written = {t[1]: t[2] for t in wt.tracked_files}
        self.assertIn('docs/plans/demo-session.md', written)
        self.assertIn(
            'Here is the design',
            written['docs/plans/demo-session.md'],
        )

    def test_the_planner_buffer_survives_a_resume(self) -> None:
        # It is in-memory only until it is persisted; the VM that held
        # the conversation is deleted the moment the stage completes.
        _r, _sc, wt = self._run(
            _LINEAR, dict(_LINEAR_REPLIES),
            transcript={'plan': [('user', 'Design it.'), ('assistant', 'D')]},
        )
        self.assertIn('plan', wt.states[-1]['reader_turns'])

    def test_a_resume_with_nothing_buffered_says_why(self) -> None:
        # It must not fail, but it must not be silent either: this is
        # the record that cannot be reconstructed afterwards.
        state = self._finished_state(['plan'])
        with mock.patch.object(R.click, 'echo') as echo:
            _r, _sc, wt = self._resume(state)
        said = ' '.join(str(c.args[0]) for c in echo.call_args_list if c.args)
        self.assertIn('no session record', said)
        self.assertNotIn(
            'docs/plans/demo-session.md',
            {t[1] for t in wt.tracked_files},
        )

    def test_completed_stages_are_not_re_run(self) -> None:
        _, sc, _ = self._resume(self._finished_state(['plan', 'build']))
        labels = {sc.label_of(s) for s, _m in sc.sent}
        self.assertNotIn('plan', labels)   # the expensive human gate
        self.assertNotIn('build', labels)
        self.assertIn('review-sec', labels)  # the stage that was left

    def test_resume_reuses_the_hub(self) -> None:
        _, _sc, wt = self._resume(self._finished_state(['plan']))
        self.assertTrue(wt.reused)

    def test_resume_restores_the_plan_of_record(self) -> None:
        # The whole point: a human's approved plan is not re-elicited.
        _, sc, _ = self._resume(self._finished_state(['plan']))
        self.assertIn('PLAN', sc.message_for_label('build'))
        self.assertEqual(sc.approvals, [])

    def test_a_re_cut_node_replaces_its_stale_worktree(self) -> None:
        # The failed attempt's clone is still on disk; re-cutting must
        # not trip over it.
        _, _sc, wt = self._resume(self._finished_state(['plan']))
        self.assertTrue(wt.replaced['build'])

    def test_incomplete_nodes_are_not_restored_as_upstreams(self) -> None:
        # 'build' has a branch in state but never completed: it must
        # look un-run, or a downstream would inherit from a stage that
        # never finished.
        state = self._finished_state(['plan'])
        state['nodes']['build'] = {
            'kind': 'writer', 'branch': 'pl/r1/build',
            'worktree': None, 'output': '', 'verdict': None,
            'selected': None,
        }
        _, sc, _ = self._resume(state)
        self.assertIn('build', {sc.label_of(s) for s, _m in sc.sent})

    def test_resume_without_state_refuses_clearly(self) -> None:
        with self.assertRaises(R.PipelineRunError) as ctx:
            self._resume(None)
        self.assertIn('no usable state', str(ctx.exception))

    def test_resume_refuses_a_foreign_state_version(self) -> None:
        state = self._finished_state(['plan'])
        state['version'] = R.RUN_STATE_VERSION + 99
        with self.assertRaises(R.PipelineRunError) as ctx:
            self._resume(state)
        self.assertIn('version', str(ctx.exception))

    def test_a_fresh_run_never_replaces_or_reuses(self) -> None:
        _, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertFalse(wt.reused)
        self.assertFalse(any(wt.replaced.values()))

    def test_published_chunks_are_not_rebuilt(self) -> None:
        cfg = self._cfg(_PER_MODULE)
        wt, sc = FakeWT(), FakeSC({'m0-plan': 'D0', 'm1-plan': 'D1'})
        wt.state_to_load = {
            'version': R.RUN_STATE_VERSION,
            'completed': [], 'completed_chunks': ['m0'],
            'plan_of_record': None, 'published': ['pipeline/r1-m0'],
            'last_branch_node': None,
            'subtasks': [
                {'id': 'm0', 'title': 'first'},
                {'id': 'm1', 'title': 'second'},
            ],
            'nodes': {},
        }
        R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            interactive_plan=False, swap_age_s=lambda: 0.0, resume=True,
        ).run()
        built = {sc.label_of(s) for s, _m in sc.sent}
        self.assertFalse([b for b in built if b.startswith('m0-')])
        self.assertIn('m1-plan', built)


class TestResumeLoopBack(_Base):
    """A block on a RESTORED writer must still loop back."""

    def _state(self, completed):
        return {
            'version': R.RUN_STATE_VERSION,
            'completed': list(completed), 'completed_chunks': [],
            'plan_of_record': 'PLAN', 'published': [],
            'last_branch_node': 'build', 'subtasks': [], 'decisions': [],
            'sessions': [],
            'nodes': {
                n: {
                    'kind': 'reader' if n == 'plan' else 'writer',
                    'branch': None if n == 'plan' else f'pl/r1/{n}',
                    'worktree': f'/wt/r1/nodes/{n}',
                    'output': 'PLAN' if n == 'plan' else 'done',
                    'verdict': None, 'selected': None,
                }
                for n in completed
            },
        }

    def _resume(self, replies):
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(replies))
        # plan + build finished; the REVIEW never ran (the shape a
        # crash mid-module leaves behind).
        wt.state_to_load = self._state(['plan', 'build'])
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, resume=True,
        )
        return runner, sc, wt

    def test_blocking_review_re_attaches_a_session(self) -> None:
        # THE bug: a restored node carries no session (its VM is gone),
        # and the loop-back asserted one existed — so the first block
        # after a resume died on a bare AssertionError.
        runner, sc, _wt = self._resume(
            {
                'build': 'fixed',
                'review-sec': ['VERDICT: BLOCKING', 'VERDICT: APPROVED'],
            }
        )
        self.assertEqual(runner.run().status, 'completed')
        self.assertIn('build', [sc.label_of(s) for s, _m in sc.sent])

    def test_the_loop_back_never_re_cuts_the_worktree(self) -> None:
        # Re-provisioning would discard the very work being fixed.
        runner, _sc, wt = self._resume(
            {
                'build': 'fixed',
                'review-sec': ['VERDICT: BLOCKING', 'VERDICT: APPROVED'],
            }
        )
        runner.run()
        self.assertNotIn('build', wt.node_from)

    def test_the_new_session_mounts_the_existing_worktree(self) -> None:
        runner, sc, _wt = self._resume(
            {
                'build': 'fixed',
                'review-sec': ['VERDICT: BLOCKING', 'VERDICT: APPROVED'],
            }
        )
        runner.run()
        created = next(
            c for c in sc.creates
            if c['title'].endswith('/build')
        )
        self.assertIn('/wt/r1/nodes/build', created['workspace'])


class TestResumeDisposesPriorVms(_Base):
    """--keep plus --resume leaks by construction: the earlier process
    kept its microVMs and the resuming one cannot see them."""

    def _state(self, sessions):
        return {
            'version': R.RUN_STATE_VERSION,
            'completed': ['plan'], 'completed_chunks': [],
            'plan_of_record': 'PLAN', 'published': [],
            'last_branch_node': None, 'subtasks': [], 'decisions': [],
            'sessions': sessions,
            'nodes': {
                'plan': {
                    'kind': 'reader', 'branch': None,
                    'worktree': '/wt/r1/nodes/plan', 'output': 'PLAN',
                    'verdict': None, 'selected': None,
                }
            },
        }

    def _run_resume(self, state, **kw):
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        wt.state_to_load = state
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, resume=True, **kw,
        )
        return runner.run(), sc, wt

    def test_state_records_sessions_for_cleanup(self) -> None:
        _, sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES), keep=True)
        recorded = wt.states[-1]['sessions']
        # every session the run created is recoverable for disposal.
        self.assertEqual(sorted(recorded), sorted(sc._label))

    def test_resume_disposes_the_prior_attempts_vms(self) -> None:
        # THE leak: 12 microVMs from a finished module were still up
        # eight hours later on a host at 100% capacity.
        _, sc, _ = self._run_resume(self._state(['old-1', 'old-2']))
        self.assertIn('old-1', sc.disposed)
        self.assertIn('old-2', sc.disposed)

    def test_stale_sessions_are_never_driven(self) -> None:
        # Disposal only — a stale handle must never be mistaken for a
        # live session and sent a turn.
        _, sc, _ = self._run_resume(self._state(['old-1']))
        self.assertNotIn('old-1', [s for s, _m in sc.sent])

    def test_keep_leaves_the_prior_vms_alone(self) -> None:
        # The human asked for them to stay; honor that.
        _, sc, _ = self._run_resume(self._state(['old-1']), keep=True)
        self.assertNotIn('old-1', sc.disposed)

    def test_a_dead_handle_does_not_fail_the_resume(self) -> None:
        # The VM may already be gone; cleaning up must never be fatal.
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        wt.state_to_load = self._state(['old-1'])
        sc.dispose_raises = {'old-1'}
        result = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, resume=True,
        ).run()
        self.assertEqual(result.status, 'completed')

    def test_state_without_sessions_resumes_clean(self) -> None:
        # Backward compatibility: state written before this change has
        # no "sessions" key. It must still resume — the schema version
        # is deliberately NOT bumped, since an absent optional key
        # degrades to exactly the old behavior (nothing to clean).
        state = self._state([])
        state.pop('sessions')
        result, sc, _ = self._run_resume(state)
        self.assertEqual(result.status, 'completed')
        # Only sessions THIS run created were torn down (its own
        # teardown); nothing foreign was touched.
        self.assertTrue(set(sc.disposed) <= set(sc._label))


class TestRequireImplementation(_Base):
    """Every gate downstream of a writer is an agent's WORD. This is
    the one check the orchestrator can make honestly."""

    def _build(self, diff, **kw):
        cfg = self._cfg(_TDD)
        wt, sc = FakeWT(), FakeSC({'tests': 't', 'build': 'b'})
        wt.diff_files['build'] = diff
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, **kw,
        )
        return runner, sc, wt

    def test_real_implementation_passes_untouched(self) -> None:
        runner, sc, _wt = self._build(['core/src/provider.rs'])
        self.assertEqual(runner.run().status, 'completed')
        build_turns = [c for c in sc.sent_calls if c['label'] == 'build']
        self.assertEqual(len(build_turns), 1)  # no re-drive

    def test_lockfile_only_is_not_an_implementation(self) -> None:
        # THE live failure: a coder changed only Cargo.lock, left the
        # test author's placeholders in place, reported success — and
        # two reviewers approved it.
        runner, _sc, _wt = self._build([['Cargo.lock']] * 6)
        with self.assertRaises(R.PipelineRunError) as ctx:
            runner.run()
        msg = str(ctx.exception)
        self.assertIn('no implementation', msg)
        self.assertIn('generated files', msg)

    def test_empty_diff_is_caught_too(self) -> None:
        runner, _sc, _wt = self._build([[]] * 6)
        with self.assertRaises(R.PipelineRunError):
            runner.run()

    def test_a_no_op_is_re_driven_and_can_recover(self) -> None:
        # First turn produced only build output; the re-drive lands.
        runner, sc, _wt = self._build([['Cargo.lock'], ['core/src/x.rs']])
        self.assertEqual(runner.run().status, 'completed')
        turns = [m for s, m in sc.sent if sc.label_of(s) == 'build']
        self.assertEqual(len(turns), 2)
        self.assertIn('did not implement anything', turns[1])
        self.assertIn('Cargo.lock', turns[1])  # names the evidence

    def test_retry_is_bounded_by_the_review_round_cap(self) -> None:
        runner, sc, _wt = self._build(
            [['Cargo.lock']] * 6, max_review_rounds=1
        )
        with self.assertRaises(R.PipelineRunError):
            runner.run()
        turns = [m for s, m in sc.sent if sc.label_of(s) == 'build']
        self.assertEqual(len(turns), 2)  # initial + one retry

    def test_generated_globs_are_configurable(self) -> None:
        cfg_text = _TDD.replace(
            'publish: none', 'publish: none\ngenerated: ["*.generated.go"]'
        )
        cfg = self._cfg(cfg_text)
        self.assertEqual(cfg.generated, ('*.generated.go',))
        wt, sc = FakeWT(), FakeSC({'tests': 't', 'build': 'b'})
        # A lockfile now COUNTS as implementation (not in the override).
        wt.diff_files['build'] = ['Cargo.lock']
        result = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        self.assertEqual(result.status, 'completed')

    def test_nested_generated_paths_match_by_basename(self) -> None:
        runner, _sc, _wt = self._build([['crates/core/Cargo.lock']] * 6)
        with self.assertRaises(R.PipelineRunError):
            runner.run()

    def test_diff_is_taken_against_the_inherited_branch(self) -> None:
        # build inherits from tests, so its diff must be vs THAT branch:
        # against base it would look like it wrote the whole test suite.
        runner, _sc, wt = self._build(['core/src/x.rs'])
        runner.run()
        self.assertIn(('build', 'pl/r1/tests'), wt.diff_queries)

    def test_a_git_failure_never_fails_the_run(self) -> None:
        # Cannot tell => do not block a run that is otherwise fine.
        cfg = self._cfg(_TDD)
        wt, sc = FakeWT(), FakeSC({'tests': 't', 'build': 'b'})

        def boom(*_a, **_kw):
            raise click.ClickException('bad revision')

        wt.node_diff_files = boom
        result = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        self.assertEqual(result.status, 'completed')

    def test_readers_and_reviewers_are_not_diff_checked(self) -> None:
        _, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        checked = {n for n, _a in wt.diff_queries}
        self.assertEqual(checked, {'build'})  # not plan, not review


class TestReviewerMustVerify(_Base):
    """Two reviewers approved a branch with no implementation."""

    def test_review_turn_forbids_approving_unverified(self) -> None:
        _, sc, _ = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        msg = sc.message_for_label('review-sec')
        self.assertIn('may NOT return `VERDICT: APPROVED`', msg)
        self.assertIn('install it', msg)          # no toolchain excuse
        self.assertIn('placeholder', msg)         # stub = BLOCKING

    def test_shipped_reviewer_templates_carry_the_rule(self) -> None:
        for name in ('security-reviewer', 'bug-reviewer'):
            body = pipeline.template_prompt(name)
            with self.subTest(template=name):
                self.assertIn('Verify before you vote', body)
                self.assertIn('INSTALL IT', body)
                self.assertIn('did not execute', body)

    def test_shipped_reviewer_templates_ask_for_findings(self) -> None:
        # Server-side: a change here needs a server restart to reach a
        # registered agent, unlike the runner's per-turn instruction.
        for name in ('security-reviewer', 'bug-reviewer'):
            body = pipeline.template_prompt(name)
            with self.subTest(template=name):
                self.assertIn('FINDINGS:', body)
                self.assertIn('RECORDED, NOT ACTED ON', body)
                # The premise-behind-an-approval half is the one a
                # reviewer would otherwise never think to write down.
                self.assertIn('premise your approval', body)


class TestAReviewerDoesNotReRunTheGate(_Base):
    """
    A reviewer gets the whole task, success criteria included. When one
    of those says "the project gate passes", a conscientious reviewer
    runs the gate — clippy, the full suite, and a coverage-instrumented
    rebuild after installing a coverage tool from source. Six reviewers
    per increment did that, the verification sandbox then ran the same
    command a seventh time on the winner, and CI an eighth on the PR.

    Nothing about the sixth run is more true than the first.
    """

    def test_the_reviewer_is_told_not_to_run_the_coverage_gate(
        self,
    ) -> None:
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        msg = sc.message_for_label('review-sec')
        self.assertIn('NOT THE COVERAGE GATE', msg)
        self.assertIn('do not install a coverage tool', msg)

    def test_it_is_relieved_of_the_criterion_by_name(self) -> None:
        # Without this it is caught between two instructions: verify the
        # branch against the task, and do not run the thing the task
        # says must pass.
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        msg = sc.message_for_label('review-sec')
        self.assertIn('THAT CRITERION IS NOT YOURS', msg)
        self.assertIn('after the judge', msg)

    def test_running_the_tests_is_still_mandatory(self) -> None:
        # The rule this must not weaken: two reviewers once approved a
        # branch that had no implementation at all.
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        msg = sc.message_for_label('review-sec')
        self.assertIn('RUN THE TESTS', msg)
        self.assertIn('not optional', msg)
        self.assertIn('may NOT return `VERDICT: APPROVED`', msg)

    def test_a_suite_that_will_not_run_is_still_a_finding(self) -> None:
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertIn(
            'that IS your finding', sc.message_for_label('review-sec')
        )


class TestReviewerIsToldWhereToBuild(_Base):
    """
    A reviewer's mount is READ-ONLY, and nothing used to say so or say
    where a build may go. Three reviewers improvised three different
    answers: `CARGO_TARGET_DIR=/tmp/cc-target`, a `cp -a` of the whole
    tree into a scratchpad, and an `rm -rf /work` that tripped Claude's
    destructive-command interlock and cost the turn (TASKS #47).

    Saying it once removes the motive AND the copy — a multi-gigabyte
    tree duplicated on every review turn, for nothing.
    """

    def test_the_reviewer_is_told_the_mount_is_read_only(self) -> None:
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        msg = sc.message_for_label('review-sec')
        self.assertIn('read-only', msg)

    def test_it_names_the_writable_place_to_build(self) -> None:
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        msg = sc.message_for_label('review-sec')
        self.assertIn('CARGO_TARGET_DIR', msg)
        self.assertIn('/tmp', msg)

    def test_it_says_not_to_copy_the_tree(self) -> None:
        # The copy is the expensive half, and the reason the destructive
        # command that cost a turn was reached for at all.
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        msg = sc.message_for_label('review-sec').lower()
        self.assertIn('do not copy', msg)


class TestAgentsAreToldTheyAreUnattended(_Base):
    """A coder hit a genuine requirements conflict, resolved it
    correctly, verified the suite green — and then opened a modal
    asking permission for the change it had already made. Nothing
    surfaced that to the console, and the REVIEWER could not see it
    either: it blocked five consecutive rounds on the change being
    "un-escalated" while the escalation sat in a modal neither could
    reach."""

    def test_a_writer_is_told(self) -> None:
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertIn('UNATTENDED', sc.message_for_label('build'))

    def test_a_reviewer_is_told(self) -> None:
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertIn('UNATTENDED', sc.message_for_label('review-sec'))

    def test_a_test_writer_is_told(self) -> None:
        # Framed by _test_writer_instruction, which bypasses the task
        # block entirely — so it needs the clause in its own right.
        _r, sc, _wt = self._run(_TDD, {'tests': 't', 'build': 'b'})
        self.assertIn('UNATTENDED', sc.message_for_label('tests'))

    def test_a_refactor_writer_is_told(self) -> None:
        # Same: _refactor_instruction bypasses the task block too.
        _r, sc, _wt = self._run(
            _JUDGE_REFACTOR,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-a',
             'refactor': 'clean', 'review-r-sec': 'VERDICT: APPROVED'},
        )
        self.assertIn('UNATTENDED', sc.message_for_label('refactor'))

    def test_a_judge_is_told(self) -> None:
        _r, sc, _wt = self._run(
            _COMPETE,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-a'},
        )
        self.assertIn('UNATTENDED', sc.message_for_label('pick'))

    def test_every_writer_is_told_the_vm_is_disposable(self) -> None:
        """
        The four paths that build. Three of them bypass the task
        block, which is how a clause added in one place misses the
        agents that most need it (TASKS.md #40).
        """
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertIn('DISPOSABLE', sc.message_for_label('build'))

        _r, sc, _wt = self._run(_TDD, {'tests': 't', 'build': 'b'})
        self.assertIn('DISPOSABLE', sc.message_for_label('tests'))

        _r, sc, _wt = self._run(
            _JUDGE_REFACTOR,
            {'impl-a': 'A', 'impl-b': 'B', 'pick': 'SELECT: impl-a',
             'refactor': 'clean', 'review-r-sec': 'VERDICT: APPROVED'},
        )
        self.assertIn('DISPOSABLE', sc.message_for_label('refactor'))

    def test_the_disposable_note_forbids_the_thing_that_killed_a_turn(
        self,
    ) -> None:
        # Not a spelling test: the guidance is worthless unless it
        # names the trap. A coder removed a swapfile it had created
        # and lost the turn to a modal no permission mode can bypass.
        self.assertIn('outside your worktree', R._DISPOSABLE_VM)
        self.assertIn('permission mode', R._DISPOSABLE_VM)
        self.assertIn('say so in your reply', R._DISPOSABLE_VM)

    def test_the_PLANNER_is_NOT_told(self) -> None:
        # THE one that must not regress. The planner's interactive
        # approval gate is the design — a human answers its questions
        # and replies APPROVED. Telling it nobody is watching would
        # break the one stage that is deliberately attended.
        # _TDD_FULL's planner is inline (a Claude harness), so the
        # turn text is the real instruction — _LINEAR's agy planner
        # stages its turn to a file and pastes only a pointer, which
        # would make this assertion vacuous.
        _r, sc, _wt = self._run(
            _TDD_FULL, {'plan': 'P', 'tests': 't', 'build': 'b'}
        )
        plan = sc.message_for_label('plan')
        self.assertNotIn('UNATTENDED', plan)
        self.assertIn('APPROVED', plan)   # still invited to be reviewed

    def test_a_loop_back_repeats_it(self) -> None:
        # The fix turn is where the conflict actually surfaces, and
        # after a --resume it is often the FIRST turn a re-attached
        # session sees.
        _r, sc, _wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b',
             'review-sec': ['VERDICT: BLOCKING — pool.rs:88 races']},
        )
        fix = [m for s, m in sc.sent if sc.label_of(s) == 'build'][-1]
        self.assertIn('UNATTENDED', fix)

    def test_it_names_the_reply_as_the_only_channel(self) -> None:
        # The remedy is not "do not ask" alone — a decision stated
        # anywhere but the reply is invisible to the reviewer too,
        # which is what produced the five-round loop.
        _r, sc, _wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        msg = sc.message_for_label('build')
        self.assertIn('IN YOUR REPLY', msg)
        self.assertIn('DISPUTED', msg)


class TestSetupBlock(_Base):
    """A VM that lacks a compiler is where agents stop verifying."""

    _SETUP = _TDD.replace(
        'publish: none',
        'publish: none\nsetup: |\n'
        '  curl -sSf https://sh.rustup.rs | sh -s -- -y',
    )

    def test_setup_reaches_coder_tdd_and_reviewer(self) -> None:
        cfg_text = _LINEAR.replace(
            'publish: local',
            'publish: local\nsetup: |\n  install the rust toolchain',
        )
        _, sc, _ = self._run(cfg_text, dict(_LINEAR_REPLIES))
        for label in ('build', 'review-sec'):
            with self.subTest(label=label):
                msg = sc.message_for_label(label)
                self.assertIn('install the rust toolchain', msg)
                self.assertIn('Environment', msg)

    def test_setup_reaches_the_test_writer(self) -> None:
        _, sc, _ = self._run(self._SETUP, {'tests': 't', 'build': 'b'})
        self.assertIn('sh.rustup.rs', sc.message_for_label('tests'))

    def test_absent_setup_adds_nothing(self) -> None:
        _, sc, _ = self._run(_TDD, {'tests': 't', 'build': 'b'})
        self.assertNotIn('Environment —', sc.message_for_label('build'))

    def test_a_loop_back_carries_the_environment_note(self) -> None:
        # A re-drive is often the FIRST turn a re-attached session sees,
        # in a VM that never had a toolchain: after a --resume the
        # writer's own session is gone. Handed findings alone, a coder
        # shipped a fix it could not compile ("cargo: command not
        # found") and called it "logically-verified".
        cfg_text = _LINEAR.replace(
            'publish: local',
            'publish: local\nsetup: |\n  install the rust toolchain',
        )
        _, sc, _ = self._run(
            cfg_text,
            {'plan': 'P', 'build': 'b',
             'review-sec': ['VERDICT: BLOCKING — pool.rs:88 races']},
        )
        fix = [m for s, m in sc.sent if sc.label_of(s) == 'build'][-1]
        self.assertIn('install the rust toolchain', fix)
        self.assertIn('pool.rs:88 races', fix)      # findings survive

    def test_a_loop_back_demands_the_fix_be_built_and_tested(self) -> None:
        _, sc, _ = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b',
             'review-sec': ['VERDICT: BLOCKING — pool.rs:88 races']},
        )
        fix = [m for s, m in sc.sent if sc.label_of(s) == 'build'][-1]
        self.assertIn('MUST build the project and run its tests', fix)
        self.assertIn('UNVERIFIED', fix)   # the honest way out, if stuck


_VERIFY = _LINEAR.replace(
    'publish: local',
    'publish: local\n'
    'verify:\n'
    '  coverage_min: 95\n'
    '  command: cov --fail-under {coverage_min}\n',
)


def _outcome(ok, code=0, output='', timed_out=False):
    return R.verify.VerifyOutcome(
        ok=ok, exit_code=code, output=output, timed_out=timed_out
    )


class TestChunkWorktreeReclaim(_Base):
    """A published chunk's clones are dead weight — keeping them makes
    disk cost cumulative across a campaign."""

    def _modules(self, **kw):
        return self._run(
            _PER_MODULE,
            {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
            **kw,
        )

    def test_a_published_chunk_gives_its_clones_back(self) -> None:
        _, _sc, wt = self._modules()
        self.assertIn('m0-tests', wt.reclaimed)
        self.assertIn('m0-build', wt.reclaimed)

    def test_the_next_chunk_is_untouched_while_it_runs(self) -> None:
        # m0's reclaim must not take m1's live clones with it.
        _, _sc, wt = self._modules()
        m0_at = wt.reclaimed.index('m0-build')
        m1_at = wt.reclaimed.index('m1-build')
        self.assertLess(m0_at, m1_at)

    def test_every_chunk_is_reclaimed_by_the_end(self) -> None:
        _, _sc, wt = self._modules()
        for node in ('m0-plan', 'm0-tests', 'm0-build',
                     'm1-plan', 'm1-tests', 'm1-build'):
            self.assertIn(node, wt.reclaimed)

    def test_keep_reclaims_nothing(self) -> None:
        _, _sc, wt = self._modules(keep=True)
        self.assertEqual(wt.reclaimed, [])

    def test_branches_are_never_deleted(self) -> None:
        # The hub is the durable artifact; only the clones go.
        _, _sc, wt = self._modules()
        self.assertIsNotNone(wt.published)
        self.assertTrue(wt.publishes)

    def test_reclaim_failure_never_fails_a_healthy_run(self) -> None:
        cfg = self._cfg(_PER_MODULE)
        wt, sc = FakeWT(), FakeSC({'m0-plan': 'D0', 'm1-plan': 'D1'})

        def boom(*_a, **_kw):
            raise click.ClickException('resolves outside root')

        wt.dispose_node_worktrees = boom
        result = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            interactive_plan=False, swap_age_s=lambda: 0.0,
        ).run()
        self.assertEqual(result.status, 'completed')

    def test_a_single_pass_run_reclaims_nothing_mid_run(self) -> None:
        # Nothing to stage-manage: teardown removes the whole run dir.
        _, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertEqual(wt.reclaimed, [])


class TestChunkGranularity(unittest.TestCase):
    """
    Increment cost is per increment, not per line: each one boots a
    full build-and-review cycle. The planner was asking for the
    smallest separable pieces and paying that toll seven times.
    """

    @staticmethod
    def _ask() -> str:
        r = object.__new__(R.PipelineRunner)
        r._active_subtask = None
        return R.PipelineRunner._plan_consolidation_instruction(r)

    def test_the_planner_is_told_to_err_large(self) -> None:
        ask = self._ask()
        self.assertIn('err LARGE', ask)
        self.assertIn('LARGEST increment', ask)

    def test_it_names_the_cost_rather_than_just_asserting_it(self) -> None:
        # A size instruction with no reason attached is one a planner
        # trades away against any competing pressure.
        ask = self._ask()
        self.assertIn('fixed per increment', ask)
        self.assertIn('seven lines or seven hundred', ask)

    def test_size_and_order_are_no_longer_conflated(self) -> None:
        # It used to say "order them smallest shippable increment
        # first", which reads as an instruction to make them small.
        ask = self._ask()
        self.assertNotIn('smallest shippable', ask)
        self.assertIn('Order them by DEPENDENCY', ask)

    def test_splitting_still_has_named_legitimate_reasons(self) -> None:
        # Err-large must not become never-split: a frozen artifact has
        # to land before the code that consumes it.
        ask = self._ask()
        self.assertIn('Split only for a reason you can name', ask)
        self.assertIn('frozen before its consumer', ask)


class TestWarmBuildCacheIsWired(_Base):
    """
    A finished node hands its build directory to the next one
    (TASKS.md #46, lever 2).
    """

    def test_every_completed_stage_refreshes_the_cache(self) -> None:
        _r, _sc, wt = self._run(
            _LINEAR.replace(
                'publish: local', 'publish: local\nbuild_cache: [target]'
            ),
            dict(_LINEAR_REPLIES),
        )
        nodes = [p.rsplit('/', 1)[-1] for p in wt.cache_refreshed]
        self.assertIn('build', nodes)

    def test_nothing_is_refreshed_when_the_cache_is_off(self) -> None:
        # Every existing pipeline: absent build_cache must stay a no-op,
        # so this cannot change behaviour for anyone who has not asked.
        _r, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertEqual(wt.cache_refreshed, [])

    def test_a_failed_stage_does_not_refresh(self) -> None:
        # Only a COMPLETED stage refreshes, so a node that died
        # mid-build cannot leave a torn cache for the next one.
        cfg = self._cfg(
            _LINEAR.replace(
                'publish: local', 'publish: local\nbuild_cache: [target]'
            )
        )
        wt, sc = FakeWT(), FakeSC({'plan': 'P'})
        sc.fail_labels = {'build'}
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        with self.assertRaises(R.PipelineRunError):
            runner.run()
        self.assertNotIn(
            'build', [p.rsplit('/', 1)[-1] for p in wt.cache_refreshed]
        )


class TestVerifyGate(_Base):
    """The one gate that does not take an agent's word: it runs the
    project's own command in a sandbox and reads the exit code."""

    def _build(self, text=_VERIFY, replies=None, **kw):
        cfg = self._cfg(text)
        wt, sc = FakeWT(), FakeSC(dict(replies or _LINEAR_REPLIES))
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, **kw,
        )
        return runner, sc, wt

    def _gate(self, outcomes):
        """Patch the sandbox call; return (contextmanager, calls)."""
        calls: list[dict] = []
        queue = list(outcomes)

        def fake(**kwargs):
            calls.append(kwargs)
            item = queue.pop(0) if len(queue) > 1 else queue[0]
            if isinstance(item, Exception):
                raise item
            return item

        return mock.patch.object(
            R.verify, 'run_verification', side_effect=fake
        ), calls

    # ── whose setup reaches the shell ─────────────────────────────

    def test_the_gate_runs_verify_setup_never_the_agent_prose(self) -> None:
        # THE regression: setup: is prose addressed to an agent, and it
        # was passed straight into the gate's sh -c program. Live, that
        # was "sh: 2: This: not found" — exit 127 before the project's
        # command ran, reported as the branch failing its tests, closed
        # by re-driving a writer three times.
        text = _VERIFY.replace(
            'verify:\n',
            'setup: |\n'
            '  This VM has NO toolchain. Install one before you begin:\n'
            '    install-it --now\n'
            'verify:\n  setup: install-it --now\n',
        )
        patch, calls = self._gate([_outcome(True)])
        runner, _sc, _wt = self._build(text)
        with patch:
            runner.run()
        script = calls[0]['script']
        self.assertIn('install-it --now', script)
        self.assertNotIn('This VM has NO toolchain', script)

    def test_a_demo_also_gets_the_shell_setup(self) -> None:
        # The demo runs in the same fresh sandbox and needs the same
        # toolchain; it must not be handed the prose either.
        text = _VERIFY.replace(
            'verify:\n',
            'setup: |\n  Prose for the agent.\n'
            'verify:\n  setup: install-it\n  demo: ./demo.sh\n',
        )
        patch, calls = self._gate([_outcome(True)])
        runner, _sc, _wt = self._build(text)
        with patch:
            runner.run()
        demo = calls[0]['demo_script']
        self.assertIn('install-it', demo)
        self.assertNotIn('Prose for the agent', demo)

    def test_the_gate_gets_the_pipelines_resource_limits(self) -> None:
        # The gate builds its own sandbox, so the SERVER's per-sandbox
        # limits never reach it. Live, that left it with every host CPU
        # and half the host's memory: its linker was killed on a branch
        # that built and tested cleanly in a capped agent VM, and the
        # runner blamed the branch and re-drove a writer over it.
        text = _VERIFY.replace(
            '  coverage_min: 95',
            "  cpus: 4\n  memory: '8g'\n  coverage_min: 95",
        )
        patch, calls = self._gate([_outcome(True)])
        runner, _sc, _wt = self._build(text)
        with patch:
            runner.run()
        self.assertEqual(calls[0]['cpus'], 4)
        self.assertEqual(calls[0]['memory'], '8g')

    def test_an_unconfigured_gate_passes_no_limits(self) -> None:
        patch, calls = self._gate([_outcome(True)])
        runner, _sc, _wt = self._build()
        with patch:
            runner.run()
        self.assertIsNone(calls[0]['cpus'])
        self.assertIsNone(calls[0]['memory'])

    # ── the happy path ────────────────────────────────────────────

    def test_no_verify_config_never_runs_a_gate(self) -> None:
        patch, calls = self._gate([_outcome(True)])
        runner, _sc, wt = self._build(_LINEAR)
        with patch:
            runner.run()
        self.assertEqual(calls, [])
        self.assertIsNotNone(wt.published)

    def test_passing_gate_publishes_without_a_re_drive(self) -> None:
        patch, calls = self._gate([_outcome(True)])
        runner, sc, wt = self._build()
        with patch:
            self.assertEqual(runner.run().status, 'completed')
        self.assertEqual(len(calls), 1)
        self.assertIsNotNone(wt.published)
        self.assertEqual(
            len([m for s, m in sc.sent if sc.label_of(s) == 'build']), 1
        )

    def test_gate_runs_the_configured_command_with_the_threshold(self):
        patch, calls = self._gate([_outcome(True)])
        runner, _sc, _wt = self._build()
        with patch:
            runner.run()
        self.assertIn('cov --fail-under 95', calls[0]['script'])

    def test_gate_verifies_a_fresh_clone_of_the_winner(self) -> None:
        # Not the writer's own worktree: verify exactly the COMMITTED
        # state that ships, without its multi-GB build output.
        patch, _calls = self._gate([_outcome(True)])
        runner, _sc, wt = self._build()
        with patch:
            runner.run()
        self.assertEqual(wt.node_from['build-verify'], 'build')
        self.assertTrue(wt.replaced['build-verify'])

    # ── the failure path ──────────────────────────────────────────

    def test_failure_re_drives_the_fixer_then_passes(self) -> None:
        patch, calls = self._gate(
            [_outcome(False, 2, 'lines 71% < 95%'), _outcome(True)]
        )
        runner, sc, wt = self._build()
        with patch:
            self.assertEqual(runner.run().status, 'completed')
        self.assertEqual(len(calls), 2)          # gate re-ran
        turns = [m for s, m in sc.sent if sc.label_of(s) == 'build']
        self.assertEqual(len(turns), 2)          # writer got a fix turn
        self.assertIn('lines 71% < 95%', turns[1])
        self.assertIsNotNone(wt.published)

    def test_the_fix_turn_forbids_silencing_the_gate(self) -> None:
        # A red gate is the most tempting thing in the pipeline to
        # silence, because the writer can edit the gate's own config and
        # watch it go green. The review loop-back said so; this path did
        # not, and this is the path m5 walked.
        patch, _calls = self._gate(
            [_outcome(False, 2, 'lines 71% < 95%'), _outcome(True)]
        )
        runner, sc, _wt = self._build()
        with patch:
            runner.run()
        fix = [m for s, m in sc.sent if sc.label_of(s) == 'build'][1]
        self.assertIn('Nothing that CHECKS this work', fix)
        self.assertIn('is NOT a fix', fix)
        self.assertIn('undisclosed change to a check', fix)

    def test_the_re_drive_re_runs_the_writers_review_gate(self) -> None:
        # The branch changed, so its reviewers must vote again before
        # the gate re-runs — otherwise unreviewed code can publish.
        patch, _calls = self._gate(
            [_outcome(False, 2, 'short'), _outcome(True)]
        )
        runner, sc, _wt = self._build()
        with patch:
            runner.run()
        reviews = [
            m for s, m in sc.sent if sc.label_of(s) == 'review-sec'
        ]
        self.assertEqual(len(reviews), 2)

    def test_a_re_review_is_numbered_round_two(self) -> None:
        # Live, every re-review recorded "round 1": the counter reset
        # each time the stage was re-entered, so PR #2's roster showed
        # six round-1 rows for one gate, and each re-review overwrote
        # the previous round's report file.
        patch, _calls = self._gate(
            [_outcome(False, 2, 'short'), _outcome(True)]
        )
        runner, _sc, wt = self._build()
        with patch:
            runner.run()
        rounds = [
            (r.stage, r.round_no) for r in runner._reviews
        ]
        self.assertEqual(rounds, [('review', 1), ('review', 2)])
        # ...so the two rounds land in DIFFERENT run-dir files.
        self.assertEqual(
            sorted(k for k in wt.artifacts if k.startswith('reviews/')),
            ['reviews/review-sec-r1.md', 'reviews/review-sec-r2.md'],
        )

    def test_the_numbering_survives_a_resume(self) -> None:
        # _reviews is restored from the run state, so a resumed run must
        # continue the count rather than start over and overwrite what
        # the earlier attempt recorded.
        runner, _sc, _wt = self._build()
        runner._reviews = [
            R.ReviewRecord(chunk=None, stage='review', reviewer='sec',
                           round_no=n, verdict='APPROVED')
            for n in (1, 2)
        ]
        self.assertEqual(runner._next_review_round('review'), 3)
        self.assertEqual(runner._next_review_round('other'), 1)

    def test_persistent_failure_blocks_the_publish(self) -> None:
        patch, calls = self._gate([_outcome(False, 2, 'lines 40%')])
        runner, _sc, wt = self._build()
        with patch, self.assertRaises(R.PipelineRunError) as ctx:
            runner.run()
        self.assertIn('still fails', str(ctx.exception))
        self.assertIn('lines 40%', str(ctx.exception))
        self.assertIsNone(wt.published)      # nothing shipped
        self.assertEqual(len(calls), 3)      # bounded by the round cap

    def test_a_timeout_is_a_failed_gate_not_a_crash(self) -> None:
        patch, _calls = self._gate(
            [_outcome(False, -1, 'killed', timed_out=True), _outcome(True)]
        )
        runner, sc, _wt = self._build()
        with patch:
            runner.run()
        turns = [m for s, m in sc.sent if sc.label_of(s) == 'build']
        self.assertIn('did not finish within its budget', turns[1])

    def test_infrastructure_failure_never_loops_back_a_writer(self) -> None:
        # A sandbox that would not start says NOTHING about the branch.
        patch, _calls = self._gate([R.verify.VerifyError('no daemon')])
        runner, sc, wt = self._build()
        with patch, self.assertRaises(R.PipelineRunError) as ctx:
            runner.run()
        self.assertIn(
            'could not run the verification gate', str(ctx.exception)
        )
        self.assertEqual(
            len([m for s, m in sc.sent if sc.label_of(s) == 'build']), 1
        )
        self.assertIsNone(wt.published)

    def test_the_gate_re_drive_carries_setup_and_the_demand(self) -> None:
        # Same gap on the other loop-back: a gate failure re-drives a
        # writer that may be in a brand-new VM, and "close the gap by
        # adding tests" is impossible without a toolchain.
        text = _VERIFY.replace(
            'publish: local',
            'publish: local\nsetup: |\n  install the rust toolchain',
        ).replace(
            '  coverage_min: 95',
            "  setup: ''\n  coverage_min: 95",   # the load-time guard
        )
        patch, _calls = self._gate([_outcome(False, 2, 'lines 40%')])
        runner, sc, _wt = self._build(text)
        with patch, self.assertRaises(R.PipelineRunError):
            runner.run()
        fix = [m for s, m in sc.sent if sc.label_of(s) == 'build'][-1]
        self.assertIn('install the rust toolchain', fix)
        self.assertIn('MUST build the project and run its tests', fix)
        self.assertIn('lines 40%', fix)            # evidence survives

    def test_findings_forbid_gaming_the_number(self) -> None:
        patch, _calls = self._gate(
            [_outcome(False, 2, 'short'), _outcome(True)]
        )
        runner, sc, _wt = self._build()
        with patch:
            runner.run()
        fix = [m for s, m in sc.sent if sc.label_of(s) == 'build'][1]
        self.assertIn('ADDING tests', fix)
        self.assertIn('may NOT modify, weaken, skip, or delete', fix)
        self.assertIn('hollow out production code', fix)

    # ── campaigns ─────────────────────────────────────────────────

    def test_a_chunk_is_gated_before_it_becomes_the_thread(self) -> None:
        # A module that cannot pass must never become the base every
        # later module is built on.
        text = _PER_MODULE.replace(
            'publish: local',
            'publish: local\nverify:\n  command: make check\n',
        )
        runner, _sc, wt = self._build(
            text,
            replies={'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
        )
        seen: list[int] = []

        def fake(**_kw):
            seen.append(len(wt.aliases))
            return _outcome(True)

        with mock.patch.object(
            R.verify, 'run_verification', side_effect=fake
        ):
            runner.run()
        # m0's gate ran with the campaign thread not yet advanced.
        self.assertEqual(seen[0], 0)
        self.assertEqual(len(seen), 2)  # once per module

    def test_a_failing_chunk_publishes_nothing(self) -> None:
        text = _PER_MODULE.replace(
            'publish: local',
            'publish: local\nverify:\n  command: make check\n',
        )
        patch, _calls = self._gate([_outcome(False, 1, 'nope')])
        runner, _sc, wt = self._build(
            text,
            replies={'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
        )
        with patch, self.assertRaises(R.PipelineRunError):
            runner.run()
        self.assertEqual(wt.publishes, [])
        self.assertEqual(wt.aliases, [])

    # ── the disk budget ───────────────────────────────────────────

    def test_the_gate_vm_counts_toward_the_disk_preflight(self) -> None:
        plain = R.max_concurrent_vms(self._cfg(_LINEAR))
        gated = R.max_concurrent_vms(self._cfg(_VERIFY))
        self.assertEqual(gated, plain + 1)


class TestTeardownPreservesAFailedRun(_Base):
    """The run directory is the only copy of what --resume reads, so a
    run that did not finish keeps it."""

    def _failing(self, **kw):
        cfg = self._cfg(_TDD)
        wt, sc = FakeWT(), FakeSC({'tests': 't'})
        sc.fail_labels = {'build'}
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, **kw,
        )
        with self.assertRaises(R.PipelineRunError):
            runner.run()
        return sc, wt

    def test_a_failed_run_keeps_its_directory(self) -> None:
        # THE live loss: a run died standing up one session, teardown
        # wiped the run dir, and a finished module's plan, tests and
        # both implementations went with it — none of it published.
        _sc, wt = self._failing()
        self.assertNotIn('r1', wt.disposed)

    def test_a_blocked_run_keeps_its_directory(self) -> None:
        # A blocked gate is the case you most want to resume after
        # fixing the cause.
        result, _sc, wt = self._run(
            _LINEAR,
            {'plan': 'P', 'build': 'b', 'review-sec': 'VERDICT: BLOCKING'},
            max_review_rounds=1,
        )
        self.assertEqual(result.status, 'blocked')
        self.assertNotIn('r1', wt.disposed)

    def test_a_failed_run_still_disposes_its_vms(self) -> None:
        # Only the DIRECTORY is precious; the microVMs are dead weight
        # and cost disk on every later run.
        sc, _wt = self._failing()
        self.assertEqual(sorted(sc.disposed), sorted(sc._label))

    def test_a_completed_run_removes_its_directory(self) -> None:
        # The contrast: its work is published, so nothing is preserved.
        _result, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        self.assertIn('r1', wt.disposed)

    def test_the_kept_path_and_resume_command_are_reported(self) -> None:
        with mock.patch.object(R.click, 'echo') as echo:
            self._failing()
        said = ' '.join(
            str(c.args[0]) for c in echo.call_args_list if c.args
        )
        self.assertIn('/wt/r1', said)      # names what it kept
        self.assertIn('--resume', said)    # …and how to use it

    def test_keep_preserves_everything(self) -> None:
        sc, wt = self._failing(keep=True)
        self.assertEqual(sc.disposed, [])
        self.assertNotIn('r1', wt.disposed)


class TestPartialWorkSalvage(_Base):
    """A failed turn must not take the agent's work down with it."""

    def _failing_writer(self):
        cfg = self._cfg(_TDD)
        wt, sc = FakeWT(), FakeSC({'tests': 't'})
        sc.fail_labels = {'build'}
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        )
        with self.assertRaises(R.PipelineRunError):
            runner.run()
        return wt

    def test_failed_writer_still_commits_its_tree(self) -> None:
        # THE live loss: a TDD writer produced a full test suite, the
        # turn timed out before the commit step, and the branch was
        # left empty while the work sat uncommitted on disk.
        wt = self._failing_writer()
        partials = [
            c for c in wt.commits if 'partial work' in c[1]
        ]
        self.assertEqual([c[0] for c in partials], ['build'])

    def test_partial_commit_is_attributed_to_the_node(self) -> None:
        # The NODE, never the agent — a judge reads this out of git log
        # (TASKS.md #33). This fixture's node and agent share a name, so
        # the discriminating case lives in TestJudgeCannotSeeTheAuthor.
        wt = self._failing_writer()
        partial = next(c for c in wt.commits if 'partial work' in c[1])
        self.assertIn('build@pipeline.local', partial[2])

    def test_a_failed_stage_is_never_marked_complete(self) -> None:
        # Otherwise a resume would skip the stage that failed.
        wt = self._failing_writer()
        self.assertNotIn('build', wt.states[-1]['completed'])

    def test_salvage_never_masks_the_original_failure(self) -> None:
        # The commit runs while an error is propagating; if it raises,
        # the caller would see the wrong error.
        cfg = self._cfg(_TDD)
        wt, sc = FakeWT(), FakeSC({'tests': 't'})
        sc.fail_labels = {'build'}
        wt.commit_raises = True
        with self.assertRaises(R.PipelineRunError) as ctx:
            R.PipelineRunner(
                cfg, session_client=sc, worktree_manager=wt, run_id='r1',
                agent_ids={n: f'ag-{n}' for n in cfg.agents},
                swap_age_s=lambda: 0.0,
            ).run()
        self.assertIn('failed', str(ctx.exception))


def _plan_with_decisions(*decisions: str, header: str | None = None) -> str:
    head = header or 'DECISIONS FOR LATER MODULES:'
    body = '\n'.join(f'- {d}' for d in decisions)
    return f'# Module design\n\nprose here\n\n{head}\n{body}\n'


class TestParseDecisions(unittest.TestCase):
    """A decision reached in one module's session reaches no other
    unless it is lifted out and carried forward."""

    def test_parses_items_in_order(self) -> None:
        text = _plan_with_decisions('use ULIDs', 'diff in the DB')
        self.assertEqual(
            R.parse_decisions(text), ['use ULIDs', 'diff in the DB']
        )

    def test_tolerates_a_decorated_header(self) -> None:
        # Models routinely wrap a requested header in ## or ** —
        # losing the whole block to a stray asterisk would silently
        # drop the decisions it names.
        for head in (
            '## DECISIONS FOR LATER MODULES:',
            '**DECISIONS FOR LATER MODULES:**',
            'decisions for later modules',
        ):
            with self.subTest(head=head):
                self.assertEqual(
                    R.parse_decisions(_plan_with_decisions('x', header=head)),
                    ['x'],
                )

    def test_blank_lines_tolerated_prose_ends_the_block(self) -> None:
        text = (
            'DECISIONS FOR LATER MODULES:\n'
            '- one\n'
            '\n'
            '- two\n'
            'Some closing prose.\n'
            '- not a decision\n'
        )
        self.assertEqual(R.parse_decisions(text), ['one', 'two'])

    def test_numbered_items_accepted(self) -> None:
        text = 'DECISIONS FOR LATER MODULES:\n1. first\n2) second\n'
        self.assertEqual(R.parse_decisions(text), ['first', 'second'])

    def test_last_header_wins(self) -> None:
        text = (
            'DECISIONS FOR LATER MODULES:\n- stale\n\nmore prose\n\n'
            'DECISIONS FOR LATER MODULES:\n- current\n'
        )
        self.assertEqual(R.parse_decisions(text), ['current'])

    def test_absent_or_empty_yields_nothing(self) -> None:
        # A module that settled nothing cross-cutting is the normal
        # case, not a failure.
        self.assertEqual(R.parse_decisions('just a plan'), [])
        self.assertEqual(R.parse_decisions(''), [])
        self.assertEqual(R.parse_decisions(None), [])
        self.assertEqual(
            R.parse_decisions('DECISIONS FOR LATER MODULES:\n\nprose'), []
        )


class TestCrossModuleContext(_Base):
    """What a per-module planner is told about the OTHER modules."""

    def _per_module(self, replies=None, **kw):
        return self._run(
            _PER_MODULE,
            replies or {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
            **kw,
        )

    def _runner(self, text=_PER_MODULE, **kw):
        cfg = self._cfg(text)
        wt, sc = FakeWT(), FakeSC({})
        runner = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0, **kw,
        )
        return runner, wt, sc

    # ── the module table ──────────────────────────────────────────

    def test_planner_sees_every_module_not_just_its_own(self) -> None:
        # THE live conflict: shown only its own row, module 0's planner
        # specified the storage layer and diffing that the human's table
        # assigns to module 1, and the test writer had to stop and ask.
        _, sc, _ = self._per_module()
        msg = sc.message_for_label('m0-plan')
        self.assertIn('[m0] contracts and core', msg)
        self.assertIn('[m1] storage and schema', msg)

    def test_the_active_module_is_marked(self) -> None:
        _, sc, _ = self._per_module()
        rows = {
            line.strip()[:2].strip(): line
            for line in sc.message_for_label('m1-plan').splitlines()
            if '[m0]' in line or '[m1]' in line
        }
        marked = [ln for ln in rows.values() if ln.lstrip().startswith('▶')]
        self.assertEqual(len(marked), 1)
        self.assertIn('[m1]', marked[0])

    def test_other_rows_are_marked_out_of_scope(self) -> None:
        _, sc, _ = self._per_module()
        self.assertIn('its OWN run', sc.message_for_label('m0-plan'))

    # ── the docs/plans pointer ────────────────────────────────────

    def test_only_later_modules_are_pointed_at_docs_plans(self) -> None:
        _, sc, _ = self._per_module()
        self.assertNotIn('docs/plans/', sc.message_for_label('m0-plan'))
        self.assertIn('docs/plans/', sc.message_for_label('m1-plan'))

    # ── the decisions ledger ──────────────────────────────────────

    def test_decisions_from_one_module_reach_the_next(self) -> None:
        _, sc, _ = self._per_module(
            {
                'm0-plan': _plan_with_decisions('ULIDs for every id'),
                'm1-plan': 'D1',
            }
        )
        m1 = sc.message_for_label('m1-plan')
        self.assertIn('ULIDs for every id', m1)
        self.assertIn('BINDING', m1)
        # …and the module that recorded it is named, so a later planner
        # can go read that module's plan for the reasoning.
        self.assertIn('[m0]', m1)

    def test_first_module_gets_no_ledger(self) -> None:
        _, sc, _ = self._per_module(
            {'m0-plan': _plan_with_decisions('x'), 'm1-plan': 'D1'}
        )
        self.assertNotIn('BINDING', sc.message_for_label('m0-plan'))

    def test_ledger_committed_on_each_published_module(self) -> None:
        _, _sc, wt = self._per_module(
            {
                'm0-plan': _plan_with_decisions('ULIDs everywhere'),
                'm1-plan': 'D1',
            }
        )
        docs = [
            (node, path, body)
            for node, path, body in wt.tracked_files
            if path.endswith('-decisions.md')
        ]
        self.assertEqual(
            [p for _n, p, _b in docs],
            ['docs/plans/mods-decisions.md'] * 2,  # threads to m1 too
        )
        self.assertIn('ULIDs everywhere', docs[0][2])
        self.assertIn(
            'docs: record decisions binding later modules',
            [m for _n, m, _a in wt.commits],
        )

    def test_no_decisions_commits_no_ledger(self) -> None:
        _, _sc, wt = self._per_module()
        self.assertEqual(
            [p for _n, p, _b in wt.tracked_files if 'decisions' in p], []
        )

    def test_a_repeated_decision_is_recorded_once(self) -> None:
        _, sc, _ = self._per_module(
            {
                'm0-plan': _plan_with_decisions('one rule'),
                'm1-plan': _plan_with_decisions('one rule'),
            }
        )
        m1 = sc.message_for_label('m1-plan')
        self.assertEqual(m1.count('one rule'), 1)

    def test_single_pass_never_records_decisions(self) -> None:
        # No modules, no later module to bind: the ledger is inert.
        _, _sc, wt = self._run(
            _LINEAR,
            {'plan': _plan_with_decisions('x'), 'build': 'b',
             'review-sec': 'VERDICT: APPROVED'},
        )
        self.assertEqual(
            [p for _n, p, _b in wt.tracked_files if 'decisions' in p], []
        )

    # ── the consolidation ask ─────────────────────────────────────

    def test_module_consolidation_asks_for_the_block(self) -> None:
        _, sc, _ = self._run(_PER_MODULE, {'m0-plan': 'D0', 'm1-plan': 'D1'})
        plan_sid = next(s for s, lb in sc._label.items() if lb == 'm0-plan')
        sends = [m for s, m in sc.sent if s == plan_sid]
        self.assertIn('DECISIONS FOR LATER MODULES:', sends[1])

    # ── resume ────────────────────────────────────────────────────

    def test_decisions_survive_a_crash(self) -> None:
        runner, _wt, _sc = self._runner()
        runner._decisions = [('m0', 'ULIDs'), ('m1', 'diff in the DB')]
        payload = runner._state_payload()
        self.assertEqual(
            payload['decisions'],
            [
                {'module': 'm0', 'text': 'ULIDs'},
                {'module': 'm1', 'text': 'diff in the DB'},
            ],
        )
        resumed, wt2, _sc2 = self._runner(resume=True)
        wt2.state_to_load = payload
        resumed._load_state()
        self.assertEqual(resumed._decisions, runner._decisions)

    def test_a_state_without_decisions_resumes_clean(self) -> None:
        # State written by an older build carries no ledger.
        runner, wt, _sc = self._runner(resume=True)
        payload = runner._state_payload()
        payload.pop('decisions')
        wt.state_to_load = payload
        runner._load_state()
        self.assertEqual(runner._decisions, [])


_SESSION_TURNS = [
    ('assistant', '# Draft\n```mermaid\ngraph TD\n  A-->B\n```'),
    ('user', 'Use ULIDs, not UUIDs.'),
    ('assistant', '# Revised\n```sql\nCREATE TABLE t (id TEXT);\n```'),
]


class TestRenderPrBody(unittest.TestCase):
    """A PR a reviewer can trust without reading the diff."""

    def _verified(self, *, demo_ok=True, out='test result: ok. 42 passed'):
        steps = [R.verify.StepOutcome('tests', 'cargo test', 0, out)]
        steps.append(
            R.verify.StepOutcome(
                'demo', './scripts/demo.sh', 0 if demo_ok else 1,
                'TLS1.3 X25519MLKEM768; wrote 17 rows',
            )
        )
        return R.verify.VerifyOutcome(
            ok=demo_ok, exit_code=0 if demo_ok else 1,
            output=out, steps=tuple(steps),
        )

    def _body(self, **kw):
        kw.setdefault('summary', '**[m1]** Storage & schema')
        kw.setdefault('outcome', self._verified())
        return R.render_pr_body(**kw)

    def test_the_evidence_is_the_captured_output(self) -> None:
        # Not "the tests passed" — the transcript of them passing.
        body = self._body()
        self.assertIn('test result: ok. 42 passed', body)
        self.assertIn('TLS1.3 X25519MLKEM768; wrote 17 rows', body)
        self.assertIn('cargo test', body)
        self.assertIn('Exit status **0**', body)

    def test_a_demonstration_is_shown_not_folded_away(self) -> None:
        # It is the thing a reviewer came to SEE; the test log is
        # corroboration and can be collapsed.
        body = self._body()
        demo_at = body.index('Proof it works')
        tests_at = body.index('How it was proven')
        self.assertLess(tests_at, demo_at)
        self.assertIn('<details><summary>Full output</summary>', body)
        self.assertNotIn('<details>', body[demo_at:])

    def test_it_links_the_design_and_the_reasoning(self) -> None:
        body = self._body(
            plan_doc='docs/plans/d-m1.md',
            session_doc='docs/plans/d-m1-session.md',
        )
        self.assertIn('`docs/plans/d-m1.md`', body)
        self.assertIn('`docs/plans/d-m1-session.md`', body)

    def test_no_gate_configured_still_renders(self) -> None:
        body = self._body(outcome=None)
        self.assertIn('[m1]', body)
        self.assertNotIn('How it was proven', body)

    def test_a_failing_step_is_labelled_as_such(self) -> None:
        body = self._body(outcome=self._verified(demo_ok=False))
        self.assertIn('Exit status **1** — FAILED.', body)

    def test_huge_output_is_capped_and_says_so(self) -> None:
        body = self._body(outcome=self._verified(out='x' * 50_000))
        self.assertLess(len(body), 40_000)
        self.assertIn('Truncated: showing the last', body)

    def test_a_fence_in_the_output_cannot_break_the_block(self) -> None:
        # Test output containing ``` would otherwise close the block
        # early and spill raw output into the prose.
        body = self._body(outcome=self._verified(out='before\n```\nafter'))
        self.assertIn('````', body)


class TestShortTitle(unittest.TestCase):
    def test_it_keeps_the_leading_clause(self) -> None:
        self.assertEqual(
            R.short_title('Storage & schema — Postgres DDL + migrations'),
            'Storage & schema',
        )

    def test_a_long_single_clause_is_truncated(self) -> None:
        out = R.short_title('x' * 200)
        self.assertLessEqual(len(out), 60)
        self.assertTrue(out.endswith('…'))

    def test_a_short_title_is_untouched(self) -> None:
        self.assertEqual(R.short_title('AWS provider'), 'AWS provider')


class TestPrEvidenceReachesTheRequest(_Base):
    """The gate's output has to survive as far as the PR."""

    _V = _LINEAR.replace(
        'publish: local',
        'publish: local\nverify:\n  command: make check\n'
        '  demo: ./scripts/demo.sh\n',
    )

    def _publish(self, **kw):
        cfg = self._cfg(self._V)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))
        outcome = R.verify.VerifyOutcome(
            ok=True, exit_code=0, output='ok',
            steps=(
                R.verify.StepOutcome('tests', 'make check', 0, '42 passed'),
                R.verify.StepOutcome(
                    'demo', './scripts/demo.sh', 0, 'connected over TLS1.3'
                ),
            ),
        )
        with mock.patch.object(
            R.verify, 'run_verification', return_value=outcome
        ) as ran:
            R.PipelineRunner(
                cfg, session_client=sc, worktree_manager=wt, run_id='r1',
                agent_ids={n: f'ag-{n}' for n in cfg.agents},
                swap_age_s=lambda: 0.0, **kw,
            ).run()
        return wt, ran

    def test_the_body_carries_both_steps(self) -> None:
        wt, _ran = self._publish()
        body = wt.pr_bodies[-1]
        self.assertIn('42 passed', body)
        self.assertIn('connected over TLS1.3', body)
        self.assertIn('docs/plans/demo.md', body)
        self.assertIn('docs/plans/demo-session.md', body)

    def test_the_demo_runs_in_the_same_sandbox(self) -> None:
        # Same clean checkout the gate just tested — otherwise the
        # demonstration proves something about a different tree.
        _wt, ran = self._publish()
        self.assertIn('./scripts/demo.sh', ran.call_args.kwargs['demo_script'])

    def test_a_module_pr_title_is_readable(self) -> None:
        _, _sc, wt = self._run(
            _PER_MODULE,
            {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
        )
        for title in wt.pr_titles:
            self.assertLess(len(title), 120)


class TestRenderPlanningSession(unittest.TestCase):
    """The design record: the conversation, not just its summary."""

    def _doc(self, turns=None):
        return R.render_planning_session(
            _SESSION_TURNS if turns is None else turns,
            title='Planning session — demo [m1]',
            plan_doc='docs/plans/demo-m1.md',
        )

    def test_no_turns_renders_nothing(self) -> None:
        self.assertEqual(self._doc([]), '')

    def test_artifacts_survive_verbatim(self) -> None:
        # THE point: the consolidated plan drops what it makes
        # redundant. Observed live — a draft carried a mermaid diagram
        # and nine SQL blocks; the revision that replaced it carried
        # four and no diagram, and that is what would have shipped.
        doc = self._doc()
        self.assertIn('```mermaid', doc)
        self.assertIn('graph TD', doc)
        self.assertIn('CREATE TABLE t (id TEXT);', doc)

    def test_the_humans_side_is_kept_too(self) -> None:
        # The answers are half the reasoning and exist nowhere else.
        self.assertIn('Use ULIDs, not UUIDs.', self._doc())

    def test_turns_are_numbered_and_attributed(self) -> None:
        doc = self._doc()
        self.assertIn('## 1. Planner', doc)
        self.assertIn('## 2. Human', doc)
        self.assertIn('## 3. Planner', doc)

    def test_it_points_at_the_plan_of_record(self) -> None:
        self.assertIn('demo-m1.md', self._doc())

    def test_an_unknown_role_is_not_dropped(self) -> None:
        self.assertIn('note', self._doc([('note', 'from the system')]))


class TestPlanningSessionCommitted(_Base):
    """The session record ships with the module it designed."""

    def test_per_module_commits_one_record_per_module(self) -> None:
        _, _sc, wt = self._run(
            _PER_MODULE,
            {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
        )
        paths = sorted(p for _n, p, _c in wt.tracked_files if 'session' in p)
        self.assertEqual(
            paths,
            ['docs/plans/mods-m0-session.md',
             'docs/plans/mods-m1-session.md'],
        )
        self.assertIn(
            'docs: add planning session record',
            [m for _n, m, _a in wt.commits],
        )

    def test_a_single_pass_run_commits_one_beside_its_plan(self) -> None:
        _, _sc, wt = self._run(_LINEAR, dict(_LINEAR_REPLIES))
        paths = [p for _n, p, _c in wt.tracked_files]
        self.assertIn('docs/plans/demo.md', paths)
        self.assertIn('docs/plans/demo-session.md', paths)

    def test_the_record_carries_both_sides_of_the_session(self) -> None:
        _, _sc, wt = self._run(
            _PER_MODULE,
            {'m0-plan': 'D0', 'm1-plan': 'D1'},
            interactive_plan=False,
            transcript={
                'm0-plan': [
                    ('assistant', 'draft with ```mermaid\ngraph TD\n```'),
                    ('user', 'my answer to its question'),
                ]
            },
        )
        doc = next(
            c for _n, p, c in wt.tracked_files
            if p == 'docs/plans/mods-m0-session.md'
        )
        self.assertIn('```mermaid', doc)      # the artifact survives
        self.assertIn('my answer to its question', doc)   # …and so do I

    def test_a_pipeline_without_a_planner_records_nothing(self) -> None:
        _, _sc, wt = self._run(_TDD, {'tests': 't', 'build': 'b'})
        self.assertEqual(
            [p for _n, p, _c in wt.tracked_files if 'session' in p], []
        )

    def test_an_unreadable_session_never_fails_the_publish(self) -> None:
        # A design record is worth having, not worth losing a publish.
        cfg = self._cfg(_LINEAR)
        wt, sc = FakeWT(), FakeSC(dict(_LINEAR_REPLIES))

        def boom(*_a, **_kw):
            raise SwarmSessionError('gone')

        sc.read_transcript = boom
        result = R.PipelineRunner(
            cfg, session_client=sc, worktree_manager=wt, run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            swap_age_s=lambda: 0.0,
        ).run()
        self.assertEqual(result.status, 'completed')
        self.assertIsNotNone(wt.published)


class TestParsersAndResolve(unittest.TestCase):
    def test_parse_verdict_last_wins(self) -> None:
        self.assertEqual(
            R.parse_verdict('VERDICT: BLOCKING\n...\nVERDICT: APPROVED'),
            'APPROVED',
        )
        self.assertIsNone(R.parse_verdict('no verdict here'))

    def test_parse_verdict_approve_synonyms(self) -> None:
        # Models paraphrase; a real reviewer ended with "VERDICT: PASS".
        for text in (
            'VERDICT: PASS',
            'VERDICT: passed',
            'VERDICT: Approve',
            'VERDICT: LGTM',
            'VERDICT: ok',
        ):
            self.assertEqual(R.parse_verdict(text), 'APPROVED', text)

    def test_parse_verdict_block_synonyms(self) -> None:
        for text in (
            'VERDICT: FAIL',
            'VERDICT: blocked',
            'VERDICT: Reject',
        ):
            self.assertEqual(R.parse_verdict(text), 'BLOCKING', text)

    def test_parse_verdict_unknown_token_is_none(self) -> None:
        self.assertIsNone(R.parse_verdict('VERDICT: maybe'))

    def test_parse_verdict_unknown_does_not_clobber(self) -> None:
        # A stray later 'verdict:' mention must not override a real one.
        self.assertEqual(
            R.parse_verdict('VERDICT: PASS\nmy earlier verdict: unsure'),
            'APPROVED',
        )

    def test_parse_select(self) -> None:
        self.assertEqual(R.parse_select('reasons\nSELECT: impl-b'), 'impl-b')
        self.assertIsNone(R.parse_select('undecided'))

    def test_parse_subtasks_well_formed(self) -> None:
        plan = (
            '# Design\n\nsome prose\n\n'
            'SUBTASKS:\n'
            '- [core] contracts and core skeleton\n'
            '- [ingest] ingestion pipeline\n'
            '- [query] query engine\n'
        )
        subs = R.parse_subtasks(plan)
        self.assertEqual([s.id for s in subs], ['core', 'ingest', 'query'])
        self.assertEqual(subs[0].title, 'contracts and core skeleton')

    def test_parse_subtasks_numbered_and_blank_lines(self) -> None:
        plan = (
            'SUBTASKS:\n'
            '1. [m0] first\n'
            '\n'
            '2. [m1] second\n'
        )
        self.assertEqual([s.id for s in R.parse_subtasks(plan)], ['m0', 'm1'])

    def test_parse_subtasks_stops_at_trailing_prose(self) -> None:
        plan = (
            'SUBTASKS:\n'
            '- [a] first\n'
            'That concludes the plan; ship them in order.\n'
            '- [b] not part of the list\n'
        )
        self.assertEqual([s.id for s in R.parse_subtasks(plan)], ['a'])

    def test_parse_subtasks_sanitizes_and_dedupes_ids(self) -> None:
        plan = (
            'SUBTASKS:\n'
            '- [Core Module!] one\n'
            '- [Core Module?] two\n'   # sanitizes to the same id
        )
        ids = [s.id for s in R.parse_subtasks(plan)]
        self.assertEqual(ids, ['core-module', 'core-module-2'])

    def test_parse_subtasks_last_header_wins(self) -> None:
        plan = (
            'SUBTASKS:\n- [old] stale\n\ndiscarded prose\n\n'
            'SUBTASKS:\n- [new] real\n'
        )
        self.assertEqual([s.id for s in R.parse_subtasks(plan)], ['new'])

    def test_parse_subtasks_empty_and_missing(self) -> None:
        self.assertEqual(R.parse_subtasks(None), [])
        self.assertEqual(R.parse_subtasks(''), [])
        self.assertEqual(R.parse_subtasks('# plan\nno subtasks block'), [])
        self.assertEqual(R.parse_subtasks('SUBTASKS:\nnot an item\n'), [])

    def test_consolidation_instruction_requests_subtasks(self) -> None:
        cfg = _make_min_cfg()
        runner = R.PipelineRunner(
            cfg, session_client=FakeSC({}), worktree_manager=FakeWT(),
            run_id='r1', agent_ids={n: n for n in cfg.agents},
        )
        instr = runner._plan_consolidation_instruction()
        self.assertIn('SUBTASKS:', instr)
        self.assertIn('[<short-id>]', instr)

    def test_resolve_agent_ids_missing(self) -> None:
        cfg = _make_min_cfg()
        with self.assertRaises(R.PipelineRunError):
            R.resolve_agent_ids(cfg, [])  # empty catalog

    def test_resolve_agent_ids_ok(self) -> None:
        cfg = _make_min_cfg()
        spec = pipeline.namespaced_agent_name(cfg.name, 'build')
        ids = R.resolve_agent_ids(cfg, [{'id': 'ag_x', 'name': spec}])
        self.assertEqual(ids['build'], 'ag_x')


def _make_min_cfg() -> pipeline.PipelineConfig:
    tmp = Path(tempfile.mkdtemp(prefix='rn-min-'))
    p = tmp / 'pipeline.yaml'
    p.write_text(
        'name: m\nrepo: ./p\nagents:\n  build: {template: coder}\n',
        encoding='utf-8',
    )
    return pipeline.load_pipeline(p)


class TestProvisionOnly(_Base):
    def _no_task(self) -> str:
        return _LINEAR.replace('task: |\n  add parse_ports\n', '')

    def test_provisions_and_binds_without_driving(self) -> None:
        result, sc, wt = self._run(self._no_task(), {})
        self.assertEqual(result.status, 'provisioned')
        # No turns driven, nothing committed, VMs kept up.
        self.assertEqual(sc.sent, [])
        self.assertEqual(wt.commits, [])
        self.assertNotIn('r1', wt.disposed)
        by_node = {b['node']: b for b in result.bindings}
        self.assertEqual(set(by_node), {'plan', 'build', 'review-sec'})
        self.assertEqual(by_node['build']['mode'], 'rw')
        self.assertEqual(by_node['plan']['mode'], 'ro')
        self.assertEqual(by_node['review-sec']['mode'], 'ro')

    def test_reviewer_provisions_on_writer_tree(self) -> None:
        result, _sc, _wt = self._run(self._no_task(), {})
        by_node = {b['node']: b for b in result.bindings}
        self.assertEqual(
            by_node['review-sec']['worktree'],
            by_node['build']['worktree'],
        )



_PUB_TMPL = """\
name: pubtest
repo: {repo}
publish:
  mode: {mode}
task: |
  do it
agents:
  build: {{template: coder, model: claude-sonnet-5}}
stages:
  - {{id: build, run: build, write: true}}
"""

_GH_URL = 'https://github.com/org/proj.git'


class TestPublishPreflight(_Base):
    """`mode: pr` needs a GitHub target, checked before provisioning."""

    def _build(self, repo, mode, **kw):
        cfg = self._cfg(_PUB_TMPL.format(repo=repo, mode=mode))
        return R.PipelineRunner(
            cfg,
            session_client=FakeSC({}),
            worktree_manager=FakeWT(),
            run_id='r1',
            agent_ids={n: f'ag-{n}' for n in cfg.agents},
            **kw,
        )

    def test_pr_mode_with_local_path_refuses_at_startup(self) -> None:
        with self.assertRaises(R.PipelineRunError) as ctx:
            self._build('./proj', 'pr')
        msg = str(ctx.exception)
        self.assertIn('./proj', msg)          # names the bad target
        self.assertIn('--publish-repo', msg)  # …and the fix

    def test_pr_mode_with_github_url_is_fine(self) -> None:
        self._build(_GH_URL, 'pr')

    def test_publish_repo_override_satisfies_pr_mode(self) -> None:
        # repo: stays local (fast clone); PRs go to GitHub.
        self._build('./proj', 'pr', publish_repo=_GH_URL)

    def test_local_mode_accepts_a_local_path(self) -> None:
        self._build('./proj', 'local')

    def test_none_mode_accepts_a_local_path(self) -> None:
        self._build('./proj', 'none')


class TestPublishTokenIsValidatedEarlyAndReadLate(unittest.TestCase):
    """
    Both properties at once, and they pull in opposite directions.

    A broken token command must still cost two seconds rather than a
    finished module — but the VALUE must not be captured at startup and
    carried for hours, because that is how a completed campaign lost
    its pull request to a token rotated mid-run (TASKS.md #43).
    """

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='tok-cli-'))
        self.cfg = self.root / 'pipeline.yaml'
        self.cfg.write_text(_LINEAR, encoding='utf-8')

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _invoke(self, *extra: str):
        with mock.patch.object(R, 'preflight_disk'), \
                mock.patch.object(R, 'preflight_codex_auth'), \
                mock.patch.object(R, '_drive') as drive:
            res = CliRunner().invoke(
                R.main,
                [
                    '-c', str(self.cfg),
                    '--canonical-root', str(self.root / 'c'),
                    '--worktree-root', str(self.root / 'w'),
                    '--skip-agy-check',
                    *extra,
                ],
            )
        return res, drive

    def test_a_broken_token_command_still_fails_fast(self) -> None:
        res, drive = self._invoke('--publish-token-command', 'false')
        self.assertNotEqual(res.exit_code, 0)
        drive.assert_not_called()  # nothing was provisioned

    def test_an_empty_token_still_fails_fast(self) -> None:
        res, drive = self._invoke('--publish-token-command', 'echo')
        self.assertNotEqual(res.exit_code, 0)
        drive.assert_not_called()

    def test_the_runner_is_handed_a_PROVIDER_not_a_captured_value(
        self,
    ) -> None:
        tok = self.root / 'tok'
        tok.write_text('first\n', encoding='utf-8')
        res, drive = self._invoke('--publish-token-file', str(tok))
        self.assertEqual(res.exit_code, 0, res.output)
        provider = drive.call_args.kwargs['publish_token']
        self.assertTrue(callable(provider))
        self.assertEqual(provider(), 'first')
        # The decisive assertion: rotating the credential AFTER the run
        # started changes what publish will use.
        tok.write_text('rotated\n', encoding='utf-8')
        self.assertEqual(provider(), 'rotated')

    def test_no_token_options_still_mean_host_credentials(self) -> None:
        res, drive = self._invoke()
        self.assertEqual(res.exit_code, 0, res.output)
        self.assertIsNone(drive.call_args.kwargs['publish_token']())


class TestResolvePublishToken(unittest.TestCase):
    """Token comes from a file or a command — never the command line."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix='tok-'))

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _file(self, text: str) -> str:
        p = self.root / 'tok'
        p.write_text(text, encoding='utf-8')
        return str(p)

    def test_neither_option_means_host_credentials(self) -> None:
        self.assertIsNone(R.resolve_publish_token(None, None))

    def test_from_file(self) -> None:
        tok = R.resolve_publish_token(self._file('secret\n'), None)
        self.assertEqual(tok, 'secret')  # trailing newline stripped

    def test_missing_file_errors(self) -> None:
        with self.assertRaises(R.PipelineRunError):
            R.resolve_publish_token(str(self.root / 'nope'), None)

    def test_empty_file_errors(self) -> None:
        with self.assertRaises(R.PipelineRunError):
            R.resolve_publish_token(self._file('  \n'), None)

    def test_from_command(self) -> None:
        tok = R.resolve_publish_token(None, 'echo secret')
        self.assertEqual(tok, 'secret')

    def test_command_is_argv_not_a_shell(self) -> None:
        # A shell would pipe; argv passes the bar through literally.
        tok = R.resolve_publish_token(None, 'echo a|b')
        self.assertEqual(tok, 'a|b')

    def test_failing_command_errors(self) -> None:
        with self.assertRaises(R.PipelineRunError):
            R.resolve_publish_token(None, 'false')

    def test_unknown_command_errors(self) -> None:
        with self.assertRaises(R.PipelineRunError):
            R.resolve_publish_token(None, 'sbx-no-such-binary-xyz')

    def test_command_with_empty_output_errors(self) -> None:
        with self.assertRaises(R.PipelineRunError):
            R.resolve_publish_token(None, 'echo')

    def test_both_options_error(self) -> None:
        with self.assertRaises(R.PipelineRunError):
            R.resolve_publish_token(self._file('s'), 'echo s')


if __name__ == '__main__':
    unittest.main()


class BlockedReviewForfeitsOneCandidate(unittest.TestCase):
    """
    A review that never reaches consensus withdraws ITS candidate.

    Two writers are run so a judge can choose between them on evidence.
    A blocked review used to raise out of the whole run, so a campaign
    could end with one candidate approved by every reviewer,
    gate-verified and unpublished, because the other could not converge.
    Seen twice in live campaigns.
    """

    def _stage(self, runner):
        review_a = R.pipeline.PipelineStage(
            id='review-a', run=['sec'], needs=['impl-a'], on_block='impl-a',
        )
        review_b = R.pipeline.PipelineStage(
            id='review-b', run=['sec'], needs=['impl-b'], on_block='impl-b',
        )
        parent = R.pipeline.PipelineStage(
            id='review', parallel=[review_a, review_b],
        )
        runner._stage_by_id['review-a'] = review_a
        runner._stage_by_id['review-b'] = review_b
        return parent

    def _runner(self):
        runner = R.PipelineRunner.__new__(R.PipelineRunner)
        runner._nodes = {}
        runner._stage_by_id = {}
        runner._forfeited = set()
        return runner

    def test_a_cleared_sibling_lets_the_run_continue(self) -> None:
        runner = self._runner()
        parent = self._stage(runner)
        # review-a cleared; review-b spent its budget.
        runner._nodes['review-a'] = R.NodeResult('review-a', 'review')
        runner._nodes['review-b'] = R.NodeResult(
            'review-b', 'review', verdict='BLOCKING',
        )

        proceed = runner._forfeit_blocked_reviews(
            parent, [R._Blocked('review-b', 4)],
        )

        self.assertTrue(proceed)
        # The blocked review AND the candidate it was vetting withdraw.
        self.assertEqual(runner._forfeited, {'review-b', 'impl-b'})

    def test_no_cleared_sibling_still_stops_the_run(self) -> None:
        # Nothing was vetted, so there is nothing to judge. The caller
        # re-raises on False.
        runner = self._runner()
        parent = self._stage(runner)
        for node_id in ('review-a', 'review-b'):
            runner._nodes[node_id] = R.NodeResult(
                node_id, 'review', verdict='BLOCKING',
            )

        proceed = runner._forfeit_blocked_reviews(
            parent,
            [R._Blocked('review-a', 4), R._Blocked('review-b', 4)],
        )

        self.assertFalse(proceed)
        self.assertEqual(runner._forfeited, set())

    def test_a_forfeited_candidate_is_not_judged(self) -> None:
        # Judging a forfeited branch would compare a vetted candidate
        # against an unvetted one and call the result a choice.
        runner = self._runner()
        for node_id in ('impl-a', 'impl-b'):
            runner._nodes[node_id] = R.NodeResult(
                node_id, 'writer', branch=f'pl/{node_id}',
            )
        pick = R.pipeline.PipelineStage(
            id='pick', run=['judge'], needs=['impl-a', 'impl-b'],
        )

        self.assertEqual(
            runner._judge_candidates(pick), ['impl-a', 'impl-b'],
        )

        runner._forfeited.add('impl-b')

        self.assertEqual(runner._judge_candidates(pick), ['impl-a'])
