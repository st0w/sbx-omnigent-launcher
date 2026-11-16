"""The run-state record: what a resume restores, and what it refuses.

Run: python -m unittest tests.test_runner_state
"""

from __future__ import annotations

import unittest

from sbx_omnigent import pipeline
from sbx_omnigent.runner.errors import PipelineRunError
from sbx_omnigent.runner.parsing.dispositions import Disposition
from sbx_omnigent.runner.records.judging import JudgePick
from sbx_omnigent.runner.records.results import NodeResult
from sbx_omnigent.runner.records.reviews import ReviewRecord
from sbx_omnigent.runner.state import (
    RUN_STATE_VERSION,
    RunState,
    decode,
    encode,
)

_COMMIT = 'a' * 40


def _full_state() -> RunState:
    """A state with every field set, one entry each."""
    return RunState(
        run_id='r1',
        pipeline='demo',
        plan_of_record='# Plan',
        subtasks=[pipeline.Subtask(id='m1', title='Parser')],
        active_subtask='m1',
        active_is_first=True,
        completed={'plan', 'm1-impl'},
        sessions=['s-1'],
        decisions=[('m1', 'Use ULIDs')],
        referrals=[('m1', 'Ask the human about retention')],
        dispositions={'m1/x/bugs/r1#1': Disposition('filed', 'real')},
        blocking_claims={'m1/x/bugs/r1#B1': 'unpaginated'},
        harness_versions={
            'build': {'runs': 'claude', 'versions': {'claude': '2.1.325'}},
        },
        completed_chunks={'m0'},
        chunk_publishes={'m0': {'seed': _COMMIT, 'pr': 'https://pr/1'}},
        reviewed_ok={'m1-impl'},
        reviews=[
            ReviewRecord(
                chunk='m1', stage='m1-review', reviewer='bugs',
                round_no=1, verdict='APPROVED',
            ),
        ],
        reader_turns={'plan': (('user', 'Plan it'), ('assistant', 'Done'))},
        picks=[
            JudgePick(
                chunk='m1', stage='m1-judge',
                candidates=('m1-impl-a', 'm1-impl-b'),
                selected='m1-impl-a', stated='m1-impl-a',
            ),
        ],
        last_branch_node='m1-impl',
        published=['https://pr/1'],
        nodes={
            'm1-impl': NodeResult(
                'm1-impl', 'writer', branch='pl/r1/m1-impl',
                worktree='/w/m1-impl', output='built', verdict=None,
                selected=None,
            ),
        },
    )


def _encoded(**changes: object) -> dict:
    """A valid encoded state, with *changes* applied to the payload."""
    raw = encode(_full_state())
    raw.update(changes)
    return raw


class TestTheFormat(unittest.TestCase):
    """The file a resume reads must not change shape under a refactor:
    runs already in flight are resumed from it."""

    def test_every_field_is_written_as_before(self) -> None:
        self.assertEqual(
            encode(_full_state()),
            {
                'version': RUN_STATE_VERSION,
                'run_id': 'r1',
                'pipeline': 'demo',
                'plan_of_record': '# Plan',
                'subtasks': [{'id': 'm1', 'title': 'Parser'}],
                'active_subtask': 'm1',
                'active_is_first': True,
                'completed': ['m1-impl', 'plan'],
                'sessions': ['s-1'],
                'decisions': [{'module': 'm1', 'text': 'Use ULIDs'}],
                'referrals': [
                    {'module': 'm1', 'text': 'Ask the human about retention'},
                ],
                'dispositions': {
                    'm1/x/bugs/r1#1': {'verdict': 'filed', 'reason': 'real'},
                },
                'blocking_claims': {'m1/x/bugs/r1#B1': 'unpaginated'},
                'harness_versions': {
                    'build': {
                        'runs': 'claude', 'versions': {'claude': '2.1.325'},
                    },
                },
                'completed_chunks': ['m0'],
                'chunk_publishes': {
                    'm0': {'seed': _COMMIT, 'pr': 'https://pr/1'},
                },
                'reviewed_ok': ['m1-impl'],
                'reviews': [_full_state().reviews[0].as_dict()],
                'reader_turns': {
                    'plan': [['user', 'Plan it'], ['assistant', 'Done']],
                },
                'judge_picks': [_full_state().picks[0].as_dict()],
                'last_branch_node': 'm1-impl',
                'published': ['https://pr/1'],
                'nodes': {
                    'm1-impl': {
                        'kind': 'writer',
                        'branch': 'pl/r1/m1-impl',
                        'worktree': '/w/m1-impl',
                        'output': 'built',
                        'verdict': None,
                        'selected': None,
                    },
                },
            },
        )

    def test_what_is_written_is_what_is_read_back(self) -> None:
        self.assertEqual(decode(encode(_full_state()), 'r1'), _full_state())

    def test_an_empty_run_round_trips(self) -> None:
        empty = RunState(run_id='r1', pipeline='demo')
        self.assertEqual(decode(encode(empty), 'r1'), empty)


class TestARefusedResume(unittest.TestCase):
    """Better to refuse than to resume from something misread."""

    def test_no_state_is_refused_with_the_fix(self) -> None:
        with self.assertRaises(PipelineRunError) as caught:
            decode(None, 'r1')
        self.assertIn("'r1'", str(caught.exception))
        self.assertIn('fresh run id', str(caught.exception))

    def test_another_version_is_refused_naming_both(self) -> None:
        with self.assertRaises(PipelineRunError) as caught:
            decode(_encoded(version=RUN_STATE_VERSION + 1), 'r1')
        self.assertIn(repr(RUN_STATE_VERSION + 1), str(caught.exception))
        self.assertIn(str(RUN_STATE_VERSION), str(caught.exception))

    def test_a_missing_version_is_refused(self) -> None:
        raw = _encoded()
        del raw['version']
        with self.assertRaises(PipelineRunError):
            decode(raw, 'r1')

    def test_a_version_of_the_wrong_type_is_refused(self) -> None:
        with self.assertRaises(PipelineRunError):
            decode(_encoded(version=str(RUN_STATE_VERSION)), 'r1')


class TestOlderStateStillResumes(unittest.TestCase):
    """Keys added after a run started are absent from its state; each
    restores the old, empty behaviour instead of failing."""

    def test_only_the_version_is_required(self) -> None:
        restored = decode({'version': RUN_STATE_VERSION}, 'r1')
        self.assertEqual(restored, RunState(run_id='r1', pipeline=''))


class TestMalformedEntriesAreDropped(unittest.TestCase):
    """A resumed run must not die over one bad bookkeeping entry, nor
    trust it."""

    def test_a_node_that_did_not_complete_is_not_restored(self) -> None:
        # It must look like it never ran, so it is re-driven.
        raw = _encoded(completed=['plan'])
        self.assertEqual(decode(raw, 'r1').nodes, {})

    def test_a_node_that_is_not_a_mapping_is_skipped(self) -> None:
        raw = _encoded(nodes={'m1-impl': 'writer'})
        self.assertEqual(decode(raw, 'r1').nodes, {})

    def test_a_node_without_a_kind_is_a_reader(self) -> None:
        raw = _encoded(nodes={'m1-impl': {}})
        node = decode(raw, 'r1').nodes['m1-impl']
        self.assertEqual((node.kind, node.output), ('reader', ''))

    def test_a_session_that_is_not_a_string_is_dropped(self) -> None:
        raw = _encoded(sessions=['s-1', 7, None])
        self.assertEqual(decode(raw, 'r1').sessions, ['s-1'])

    def test_a_decision_without_text_is_dropped(self) -> None:
        raw = _encoded(
            decisions=[{'module': 'm1'}, 'x', {'module': 'm1', 'text': 'y'}],
        )
        self.assertEqual(decode(raw, 'r1').decisions, [('m1', 'y')])

    def test_a_referral_without_a_module_is_dropped(self) -> None:
        raw = _encoded(referrals=[{'text': 'y'}])
        self.assertEqual(decode(raw, 'r1').referrals, [])

    def test_a_disposition_that_is_not_a_mapping_is_dropped(self) -> None:
        raw = _encoded(dispositions={'a': 'filed', 'b': {}})
        self.assertEqual(
            decode(raw, 'r1').dispositions, {'b': Disposition('', '')}
        )

    def test_a_blocking_claim_that_is_not_text_is_dropped(self) -> None:
        raw = _encoded(blocking_claims={'a': 3, 'b': 'real'})
        self.assertEqual(decode(raw, 'r1').blocking_claims, {'b': 'real'})

    def test_a_reviewed_writer_that_is_not_a_string_is_dropped(self) -> None:
        raw = _encoded(reviewed_ok=['m1-impl', ['x']])
        self.assertEqual(decode(raw, 'r1').reviewed_ok, {'m1-impl'})

    def test_a_subtask_without_a_title_is_dropped(self) -> None:
        raw = _encoded(subtasks=[{'id': 'm1'}, {'id': 'm2', 'title': 'B'}])
        self.assertEqual(
            decode(raw, 'r1').subtasks,
            [pipeline.Subtask(id='m2', title='B')],
        )

    def test_a_malformed_reader_turn_is_dropped(self) -> None:
        raw = _encoded(reader_turns={
            'plan': [['user', 'hi'], ['only one'], 'x'],
            'bad': 'not a list',
        })
        self.assertEqual(
            decode(raw, 'r1').reader_turns, {'plan': (('user', 'hi'),)}
        )

    def test_a_malformed_review_or_pick_is_dropped(self) -> None:
        raw = _encoded(reviews=[{}, 'x'], judge_picks=[{}, 3])
        restored = decode(raw, 'r1')
        self.assertEqual((restored.reviews, restored.picks), ([], []))

    def test_a_seed_that_is_not_a_commit_is_blanked(self) -> None:
        # Only a git object id is let into a link in a PR description.
        raw = _encoded(chunk_publishes={
            'm0': {'seed': 'main; rm -rf', 'pr': 'https://pr/1'},
        })
        self.assertEqual(
            decode(raw, 'r1').chunk_publishes,
            {'m0': {'seed': '', 'pr': 'https://pr/1'}},
        )

    def test_a_malformed_chunk_publish_is_dropped_or_blanked(self) -> None:
        raw = _encoded(chunk_publishes={
            'm0': {'seed': 'abc1234', 'pr': 'https://x/pull/1'},
            'bad': 'nope',
            'worse': {'seed': 'not a sha', 'pr': 3},
        })
        self.assertEqual(
            decode(raw, 'r1').chunk_publishes,
            {'m0': {'seed': 'abc1234', 'pr': 'https://x/pull/1'},
             'worse': {'seed': '', 'pr': ''}},
        )

    def test_no_chunk_publishes_restores_none(self) -> None:
        raw = _encoded(chunk_publishes=None)
        self.assertEqual(decode(raw, 'r1').chunk_publishes, {})

    def test_an_unknown_harness_is_not_trusted(self) -> None:
        raw = _encoded(harness_versions={
            'build': {'runs': 'nope', 'versions': {'nope': '1'}},
            'judge': {'runs': 'claude', 'versions': {'claude': 5}},
        })
        self.assertEqual(decode(raw, 'r1').harness_versions, {})



class TestAValueOfTheWrongTypeIsDropped(unittest.TestCase):
    """A container of the wrong type is dropped like a malformed entry,
    rather than failing the resume with a TypeError."""

    _LISTS = ('subtasks', 'decisions', 'referrals', 'reviews', 'judge_picks')
    _MAPS = ('dispositions', 'reader_turns', 'nodes')

    def test_a_list_field_holding_a_number_restores_empty(self) -> None:
        for key in self._LISTS:
            with self.subTest(key=key):
                restored = decode(_encoded(**{key: 7}), 'r1')
                self.assertEqual(restored, decode(_encoded(**{key: []}), 'r1'))

    def test_a_mapping_field_holding_a_list_restores_empty(self) -> None:
        for key in self._MAPS:
            with self.subTest(key=key):
                restored = decode(_encoded(**{key: ['x']}), 'r1')
                self.assertEqual(restored, decode(_encoded(**{key: {}}), 'r1'))


class TestStringListsHoldOnlyStrings(unittest.TestCase):
    """Stage ids, module ids, session ids and PR URLs are strings. An
    entry that is not one is dropped: in ``completed`` it used to fail
    the resume, being unhashable."""

    _FIELDS = (
        'completed', 'completed_chunks', 'published', 'sessions',
        'reviewed_ok',
    )

    def test_an_entry_that_is_not_a_string_is_dropped(self) -> None:
        for key in self._FIELDS:
            with self.subTest(key=key):
                restored = decode(_encoded(**{key: ['a', {'x': 1}, 3]}), 'r1')
                self.assertEqual(sorted(getattr(restored, key)), ['a'])

    def test_a_string_is_not_read_as_a_list_of_letters(self) -> None:
        for key in self._FIELDS:
            with self.subTest(key=key):
                restored = decode(_encoded(**{key: 'abc'}), 'r1')
                self.assertEqual(list(getattr(restored, key)), [])

    def test_a_number_restores_empty(self) -> None:
        for key in self._FIELDS:
            with self.subTest(key=key):
                restored = decode(_encoded(**{key: 7}), 'r1')
                self.assertEqual(list(getattr(restored, key)), [])

    def test_blocking_claims_holding_a_list_restore_empty(self) -> None:
        restored = decode(_encoded(blocking_claims=['x']), 'r1')
        self.assertEqual(restored.blocking_claims, {})

if __name__ == '__main__':
    unittest.main()
