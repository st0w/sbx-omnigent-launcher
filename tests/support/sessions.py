"""A scriptable session client for tests of the pipeline runner."""

from __future__ import annotations

from sbx_omnigent.swarm_session import (
    PlanDecision,
    SwarmRunnerUnavailable,
    SwarmSessionError,
    SwarmTurnResult,
    SwarmTurnTimeout,
)
from tests.support.plans import plan_text

#: The line a writer's reply must end with; spelled out here rather than
#: read from the runner, so the fake keeps working while it is absent.
WORK_COMPLETE = 'WORK COMPLETE'


def _is_planner_label(label: str) -> bool:
    """Whether *label* names a planner node (``plan``, ``m1-plan``)."""
    return label == 'plan' or label.endswith('-plan')


class FakeSC:
    """Scriptable session client. *replies* maps a stage label to a
    reply string or a list consumed per call."""

    def __init__(self, replies: dict[str, object]) -> None:
        self._replies = replies
        self.creates: list[dict] = []
        #: ``(session, level)`` per share, in order.
        self.shares: list[tuple[str, int]] = []
        self.sent: list[tuple[str, str]] = []
        self.sent_calls: list[dict] = []
        self.disposed: list[str] = []
        #: ('create'|'dispose', label) in call order — lets a test
        #: assert a VM was freed BEFORE another was booted, which
        #: counting alone cannot show.
        self.events: list[tuple[str, str]] = []
        self.warmed: list[str] = []
        self.approvals: list[str] = []
        #: The phrase each approval is sent with, in order; past the end
        #: of the list every approval is plain ``APPROVED`` (#49).
        self.approval_phrases: list[str] = []
        #: Notices posted to a session, as ``(label, text)``.
        self.notices: list[tuple[str, str]] = []
        #: Each notice's headline, which is all a collapsed card shows.
        self.headlines: list[tuple[str, str]] = []
        #: Turns and notices in the order they happened, as
        #: ``('send' | 'notice', label, text)``.
        self.timeline: list[tuple[str, str, str]] = []
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
        #: label -> outcomes for its next turns, one consumed per turn,
        #: after which turns succeed: 'lost' (failed, no error), 'drop'
        #: (the stream dropped), 'timeout', or 'error:<text>' (the
        #: agent reported an error).
        self.turn_outcomes: dict[str, list[str]] = {}
        #: Labels whose item store keeps GROWING between polls — a
        #: reviewer that is still working (installing a toolchain,
        #: running a build), as opposed to one that has gone quiet.
        self.busy_labels: set[str] = set()
        #: label -> successive item lists, one per read (the last
        #: repeats): a session's transcript as it grows, for tests of
        #: background work the agent ended its turn on.
        self.items_for_label: dict[str, list[list[dict]]] = {}
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
        #: Set to return writer replies verbatim. Otherwise a turn that
        #: asks for the finish line gets it, as from a writer that did
        #: what it was told; tests of the finish rule itself opt out.
        self.raw_writer_replies = False
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

    def wait_for_plan_decision(self, session, **kw) -> PlanDecision:
        plan = self.wait_for_plan_approval(session, **kw)
        phrase = (
            self.approval_phrases.pop(0) if self.approval_phrases
            else 'APPROVED'
        )
        return PlanDecision(plan=plan, phrase=phrase)

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
            return plan_text(2200, text)
        return text

    def read_items(self, session, *, tail=None):
        # Models the activity marker _newest_item_id reads. A label in
        # `busy_labels` gets a NEW id on every read, i.e. it is still
        # producing output — a reviewer mid-build, which must not be
        # mistaken for a silent one.
        label = self._label.get(session, '')
        seq = self.items_for_label.get(label)
        if seq:
            items = seq.pop(0) if len(seq) > 1 else seq[0]
            return list(items[-tail:]) if tail else list(items)
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

    def share(self, session, *, level) -> None:
        self.shares.append((session, level))

    def post_notice(self, session, headline, details='') -> bool:
        label = self._label.get(session, '')
        text = f'{headline}\n\n{details}' if details else headline
        self.notices.append((label, text))
        self.headlines.append((label, headline))
        self.timeline.append(('notice', label, text))
        return True

    def send_and_wait(self, session, message, *, timeout=1800.0, **kw):
        self.sent.append((session, message))
        self.timeline.append(
            ('send', self._label.get(session, ''), message)
        )
        if self._label.get(session, '') in self.interrupt_labels:
            raise KeyboardInterrupt
        if self._label.get(session, '') in self.raise_labels:
            raise SwarmTurnTimeout(
                f'turn on {session} did not complete within 3600s'
            )
        if self._label.get(session, '') in self.fail_labels:
            return SwarmTurnResult('failed', None, '')
        scripted = self.turn_outcomes.get(self._label.get(session, ''))
        if scripted:
            return self._scripted_outcome(session, scripted.pop(0))
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
        # not a design plan, and a real planner never answers 'P' — so a
        # fixture that did would only be testing the refusal. Tests of
        # the guard itself pass real text directly.
        if _is_planner_label(label) and not self.raw_plan_replies:
            reply = plan_text(2200, reply)
        if (
            WORK_COMPLETE in message
            and not self.raw_writer_replies
            and WORK_COMPLETE not in reply
        ):
            reply = f'{reply}\n\n{WORK_COMPLETE}'
        self._last_reply[session] = reply
        self._assistant.setdefault(session, []).append(reply)
        return SwarmTurnResult('idle', None, reply)

    @staticmethod
    def _scripted_outcome(session, outcome):
        """Raise or return what one scripted turn outcome stands for."""
        if outcome == 'drop':
            raise SwarmSessionError(f'stream for {session} closed')
        if outcome == 'unavailable':
            raise SwarmRunnerUnavailable(
                f'POST /v1/sessions/{session}/events returned 503: '
                'managed runner did not connect after launch'
            )
        if outcome == 'timeout':
            raise SwarmTurnTimeout(
                f'turn on {session} did not complete within 3600s'
            )
        if outcome.startswith('error:'):
            return SwarmTurnResult(
                'failed', outcome.removeprefix('error:'), ''
            )
        return SwarmTurnResult('failed', None, '')

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
