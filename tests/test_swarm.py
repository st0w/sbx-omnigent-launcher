"""Unit tests for :mod:`sbx_omnigent.swarm` (the A-prime orchestrator).

Deterministic logic only — the session client and worktree manager are
mocked, so no live server/sbx: sentinel construction, role addressing,
the start/teardown wiring (rw coder + N :ro reviewers, YOLO on all),
failure cleanup, registry round-trip, and last-assistant-text
extraction. Run:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import io
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

import click

from sbx_omnigent.swarm import (
    _PUBLISH_MODE_ENV,
    _YOLO_LAUNCH_ARGS,
    Reviewer,
    SwarmHandle,
    SwarmOrchestrator,
    SwarmRegistry,
    _launch_args_for,
    _parse_reviewer_specs,
    _read_message,
    _resolve_open_pr,
    _turn_note,
    credential_kind_for,
    handle_from_entry,
    handle_to_entry,
    last_assistant_text,
    mount_sentinel,
)
from sbx_omnigent.swarm_session import (
    SwarmSessionError,
    SwarmTurnResult,
    looks_like_rate_limit,
)


class TestParseReviewerSpecs(unittest.TestCase):
    def test_parses_role_equals_agent(self) -> None:
        got = _parse_reviewer_specs(
            ('security=ag_sec', ' bug-hunter = ag_bug ')
        )
        self.assertEqual(got, {'security': 'ag_sec', 'bug-hunter': 'ag_bug'})

    def test_empty_input_is_empty_map(self) -> None:
        self.assertEqual(_parse_reviewer_specs(()), {})

    def test_rejects_malformed(self) -> None:
        for bad in ('noequals', '=ag', 'role=', '  =  '):
            with self.assertRaises(click.UsageError):
                _parse_reviewer_specs((bad,))

    def test_rejects_duplicate_role(self) -> None:
        with self.assertRaises(click.UsageError):
            _parse_reviewer_specs(('security=a', 'security=b'))


class TestReadMessage(unittest.TestCase):
    """``_read_message`` source selection, incl. ``-`` → stdin."""

    def test_inline_message(self) -> None:
        self.assertEqual(_read_message('hi', None), 'hi')

    def test_file(self) -> None:
        with tempfile.NamedTemporaryFile(
            'w', suffix='.txt', delete=False
        ) as fh:
            fh.write('from file')
            path = fh.name
        try:
            self.assertEqual(_read_message(None, path), 'from file')
        finally:
            os.unlink(path)

    def test_dash_reads_stdin(self) -> None:
        # THE crash: a coordinator passing `--message-file -` for a long
        # multi-line turn must read stdin, not open('-').
        with mock.patch.object(
            sys, 'stdin', io.StringIO('piped turn\nline two')
        ):
            self.assertEqual(_read_message(None, '-'), 'piped turn\nline two')

    def test_both_rejected(self) -> None:
        with self.assertRaises(click.UsageError):
            _read_message('a', 'b')

    def test_neither_rejected(self) -> None:
        with self.assertRaises(click.UsageError):
            _read_message(None, None)


class TestRateLimitDetection(unittest.TestCase):
    def test_looks_like_rate_limit_matches_markers(self) -> None:
        for s in (
            'Error: usage limit reached, resets at 5pm',
            'HTTP 429 Too Many Requests',
            'anthropic: rate_limit_error',
            'the service is Overloaded, try again later',
        ):
            self.assertTrue(looks_like_rate_limit(s), s)

    def test_looks_like_rate_limit_ignores_normal_and_none(self) -> None:
        self.assertFalse(looks_like_rate_limit(None, '', 'VERDICT: APPROVED'))

    def test_turn_note_flags_rate_limit_on_idle_reply(self) -> None:
        # A status=idle turn whose reply mentions a limit is flagged.
        note = _turn_note(
            SwarmTurnResult('idle', None),
            'I have hit my usage limit; stopping.',
            None,
        )
        self.assertIsNotNone(note)
        self.assertIn('rate limit', note.lower())

    def test_turn_note_flags_last_task_error(self) -> None:
        note = _turn_note(
            SwarmTurnResult('failed', None),
            '',
            'rate_limit_error: quota exceeded',
        )
        self.assertIsNotNone(note)
        self.assertIn('rate limit', note.lower())

    def test_turn_note_flags_failed_without_limit(self) -> None:
        note = _turn_note(SwarmTurnResult('failed', 'boom'), 'partial', None)
        self.assertIsNotNone(note)
        self.assertIn('failed', note.lower())

    def test_turn_note_flags_empty_reply(self) -> None:
        note = _turn_note(SwarmTurnResult('idle', None), '   ', None)
        self.assertIsNotNone(note)

    def test_turn_note_none_on_clean_turn(self) -> None:
        note = _turn_note(
            SwarmTurnResult('idle', None), 'VERDICT: APPROVED', None
        )
        self.assertIsNone(note)


class TestResolveOpenPr(unittest.TestCase):
    def test_explicit_flag_wins_over_env(self) -> None:
        with mock.patch.dict(
            os.environ, {_PUBLISH_MODE_ENV: 'local'}, clear=False
        ):
            self.assertTrue(_resolve_open_pr(False))  # --pr
            self.assertFalse(_resolve_open_pr(True))  # --no-pr

    def test_env_local_defaults_to_no_pr(self) -> None:
        with mock.patch.dict(
            os.environ, {_PUBLISH_MODE_ENV: 'local'}, clear=False
        ):
            self.assertFalse(_resolve_open_pr(None))

    def test_env_unset_or_github_defaults_to_pr(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(_PUBLISH_MODE_ENV, None)
            self.assertTrue(_resolve_open_pr(None))
        with mock.patch.dict(
            os.environ, {_PUBLISH_MODE_ENV: 'github'}, clear=False
        ):
            self.assertTrue(_resolve_open_pr(None))


class TestMountSentinel(unittest.TestCase):
    def test_rw_and_ro(self) -> None:
        self.assertEqual(
            mount_sentinel('/srv/worktrees/a', 'rw'),
            'git@sbxmount:/srv/worktrees/a#rw',
        )
        self.assertEqual(
            mount_sentinel('/srv/worktrees/a', 'ro'),
            'git@sbxmount:/srv/worktrees/a#ro',
        )

    def test_bad_mode(self) -> None:
        with self.assertRaises(ValueError):
            mount_sentinel('/srv/worktrees/a', 'rwx')


class TestSwarmHandle(unittest.TestCase):
    def _handle(self) -> SwarmHandle:
        return SwarmHandle(
            swarm_id='s1',
            repo_url='https://github.com/o/r.git',
            worktree_path='/srv/worktrees/s1',
            coder_session='conv_coder',
            reviewers=(
                Reviewer(role='security', session='conv_sec'),
                Reviewer(role='tdd', session='conv_tdd'),
            ),
        )

    def test_session_for_coder_and_reviewers(self) -> None:
        handle = self._handle()
        self.assertEqual(handle.session_for('coder'), 'conv_coder')
        self.assertEqual(handle.session_for('security'), 'conv_sec')
        self.assertEqual(handle.session_for('tdd'), 'conv_tdd')

    def test_session_for_unknown_role_raises(self) -> None:
        with self.assertRaises(KeyError):
            self._handle().session_for('nope')

    def test_roles_lists_coder_first(self) -> None:
        self.assertEqual(self._handle().roles(), ['coder', 'security', 'tdd'])


class TestLastAssistantText(unittest.TestCase):
    def test_returns_last_assistant_message(self) -> None:
        client = mock.Mock()
        client.read_items.return_value = [
            {
                'type': 'message',
                'role': 'user',
                'content': [{'type': 'input_text', 'text': 'go'}],
            },
            {
                'type': 'message',
                'role': 'assistant',
                'content': [{'type': 'output_text', 'text': 'first'}],
            },
            {'type': 'function_call', 'role': 'assistant', 'content': []},
            {
                'type': 'message',
                'role': 'assistant',
                'content': [{'type': 'output_text', 'text': 'final'}],
            },
        ]
        self.assertEqual(last_assistant_text(client, 'conv_1'), 'final')

    def test_empty_when_no_assistant(self) -> None:
        client = mock.Mock()
        client.read_items.return_value = [
            {'type': 'message', 'role': 'user', 'content': []}
        ]
        self.assertEqual(last_assistant_text(client, 'conv_1'), '')


def _orch() -> tuple[SwarmOrchestrator, mock.Mock, mock.Mock]:
    sc = mock.Mock()
    wt = mock.Mock()
    orch = SwarmOrchestrator(
        session_client=sc,
        worktree_manager=wt,
        coder_agent_id='ag_coder',
        reviewer_agent_id='ag_reviewer',
    )
    return orch, sc, wt


class TestStartSwarm(unittest.TestCase):
    def test_default_single_reviewer_with_yolo(self) -> None:
        orch, sc, wt = _orch()
        wt.create_swarm_worktree.return_value = '/srv/worktrees/s1'
        sc.create.side_effect = ['conv_coder', 'conv_reviewer']

        handle = orch.start_swarm('s1', 'https://github.com/o/r.git')

        wt.create_swarm_worktree.assert_called_once_with(
            's1', 'https://github.com/o/r.git', None
        )
        self.assertEqual(sc.create.call_count, 2)
        coder_kw = sc.create.call_args_list[0].kwargs
        reviewer_kw = sc.create.call_args_list[1].kwargs
        self.assertEqual(
            coder_kw['workspace'], 'git@sbxmount:/srv/worktrees/s1#rw'
        )
        self.assertEqual(coder_kw['agent_id'], 'ag_coder')
        self.assertEqual(
            reviewer_kw['workspace'], 'git@sbxmount:/srv/worktrees/s1#ro'
        )
        self.assertEqual(reviewer_kw['agent_id'], 'ag_reviewer')
        # YOLO applied to BOTH roles.
        for kw in (coder_kw, reviewer_kw):
            self.assertEqual(
                kw['terminal_launch_args'], list(_YOLO_LAUNCH_ARGS)
            )
        self.assertEqual(handle.coder_session, 'conv_coder')
        self.assertEqual(handle.reviewers[0].role, 'reviewer')
        self.assertEqual(handle.reviewers[0].session, 'conv_reviewer')
        self.assertEqual(handle.worktree_path, '/srv/worktrees/s1')

    def test_multiple_named_reviewers(self) -> None:
        orch, sc, wt = _orch()
        wt.create_swarm_worktree.return_value = '/srv/worktrees/s1'
        sc.create.side_effect = ['conv_coder', 'conv_sec', 'conv_tdd']

        handle = orch.start_swarm(
            's1',
            'https://github.com/o/r.git',
            reviewer_roles=('security', 'tdd'),
        )

        # 1 coder + 2 reviewers, all :ro for reviewers.
        self.assertEqual(sc.create.call_count, 3)
        for kw in sc.create.call_args_list[1:]:
            self.assertTrue(
                kw.kwargs['workspace'].endswith('#ro'),
            )
        self.assertEqual(
            [r.role for r in handle.reviewers], ['security', 'tdd']
        )
        self.assertEqual(handle.session_for('security'), 'conv_sec')

    def test_per_role_reviewers_bind_distinct_agents(self) -> None:
        orch, sc, wt = _orch()
        wt.create_swarm_worktree.return_value = '/srv/worktrees/s1'
        sc.create.side_effect = ['conv_coder', 'conv_bug', 'conv_sec']

        handle = orch.start_swarm(
            's1',
            'g',
            reviewers={
                'bug-hunter': 'ag_bug',
                'security': 'ag_sec',
            },
        )

        # coder + one VM per role, each with ITS OWN agent id.
        self.assertEqual(sc.create.call_count, 3)
        by_role = {
            kw.kwargs['title'].split('/', 1)[1]: kw.kwargs['agent_id']
            for kw in sc.create.call_args_list[1:]
        }
        self.assertEqual(
            by_role, {'bug-hunter': 'ag_bug', 'security': 'ag_sec'}
        )
        self.assertEqual(
            [r.role for r in handle.reviewers], ['bug-hunter', 'security']
        )

    def test_per_role_map_overrides_reviewer_roles(self) -> None:
        # When both are passed, the explicit map wins.
        orch, sc, wt = _orch()
        wt.create_swarm_worktree.return_value = '/srv/worktrees/s1'
        sc.create.side_effect = ['conv_coder', 'conv_x']
        handle = orch.start_swarm(
            's1',
            'g',
            reviewer_roles=('ignored', 'also-ignored'),
            reviewers={'security': 'ag_sec'},
        )
        self.assertEqual([r.role for r in handle.reviewers], ['security'])

    def test_per_role_rejects_empty_agent_id(self) -> None:
        orch, _sc, wt = _orch()
        wt.create_swarm_worktree.return_value = '/srv/worktrees/s1'
        with self.assertRaises(click.ClickException):
            orch.start_swarm('s1', 'g', reviewers={'security': ''})

    def test_single_spec_path_needs_a_reviewer_agent(self) -> None:
        sc, wt = mock.Mock(), mock.Mock()
        # No default reviewer agent + no per-role map -> error.
        orch = SwarmOrchestrator(
            session_client=sc,
            worktree_manager=wt,
            coder_agent_id='ag_coder',
        )
        with self.assertRaises(click.ClickException):
            orch.start_swarm('s1', 'g', reviewer_roles=('security',))

    def test_rejects_duplicate_reviewer_role(self) -> None:
        orch, _sc, wt = _orch()
        wt.create_swarm_worktree.return_value = '/srv/worktrees/s1'
        with self.assertRaises(click.ClickException):
            orch.start_swarm('s1', 'g', reviewer_roles=('sec', 'sec'))

    def test_rejects_reserved_coder_role(self) -> None:
        orch, _sc, wt = _orch()
        wt.create_swarm_worktree.return_value = '/srv/worktrees/s1'
        with self.assertRaises(click.ClickException):
            orch.start_swarm('s1', 'g', reviewer_roles=('coder',))

    def test_cleans_up_all_created_when_a_reviewer_fails(self) -> None:
        orch, sc, wt = _orch()
        wt.create_swarm_worktree.return_value = '/srv/worktrees/s1'
        # coder + first reviewer created, second reviewer explodes.
        sc.create.side_effect = [
            'conv_coder',
            'conv_sec',
            SwarmSessionError('boom'),
        ]

        with self.assertRaises(SwarmSessionError):
            orch.start_swarm('s1', 'g', reviewer_roles=('sec', 'tdd'))

        # Both already-created sessions and the worktree are removed.
        sc.dispose.assert_any_call('conv_coder')
        sc.dispose.assert_any_call('conv_sec')
        wt.dispose_swarm.assert_called_once_with('s1')


class TestSendByRole(unittest.TestCase):
    def _wired(self) -> tuple[SwarmOrchestrator, mock.Mock, SwarmHandle]:
        orch, sc, _wt = _orch()
        handle = SwarmHandle(
            swarm_id='s1',
            repo_url='g',
            worktree_path='/w',
            coder_session='conv_coder',
            reviewers=(Reviewer(role='security', session='conv_sec'),),
        )
        return orch, sc, handle

    def test_send_routes_to_role_session(self) -> None:
        orch, sc, handle = self._wired()
        orch.send(handle, 'security', 'look at this', timeout=5)
        sc.send_and_wait.assert_called_once_with(
            'conv_sec', 'look at this', timeout=5
        )

    def test_run_coder_targets_coder(self) -> None:
        orch, sc, handle = self._wired()
        orch.run_coder(handle, 'implement', timeout=9)
        sc.send_and_wait.assert_called_once_with(
            'conv_coder', 'implement', timeout=9
        )


class TestCommit(unittest.TestCase):
    def test_commit_delegates_to_worktree_manager(self) -> None:
        orch, _sc, wt = _orch()
        wt.commit_worktree.return_value = True
        handle = SwarmHandle(
            swarm_id='s1',
            repo_url='g',
            worktree_path='/w',
            coder_session='conv_coder',
            reviewers=(),
        )
        made = orch.commit(handle, 'msg', author='coder <c@x>')
        self.assertTrue(made)
        wt.commit_worktree.assert_called_once_with(
            's1', message='msg', author='coder <c@x>'
        )


class TestTeardown(unittest.TestCase):
    def _handle(self) -> SwarmHandle:
        return SwarmHandle(
            swarm_id='s1',
            repo_url='https://github.com/o/r.git',
            worktree_path='/srv/worktrees/s1',
            coder_session='conv_coder',
            reviewers=(
                Reviewer(role='security', session='conv_sec'),
                Reviewer(role='tdd', session='conv_tdd'),
            ),
        )

    def test_disposes_every_session_and_worktree(self) -> None:
        orch, sc, wt = _orch()
        orch.teardown(self._handle())
        sc.dispose.assert_any_call('conv_coder')
        sc.dispose.assert_any_call('conv_sec')
        sc.dispose.assert_any_call('conv_tdd')
        wt.dispose_swarm.assert_called_once_with('s1')

    def test_dispose_failure_does_not_strand_others(self) -> None:
        orch, sc, wt = _orch()
        sc.dispose.side_effect = [SwarmSessionError('gone'), None, None]
        orch.teardown(self._handle(), dispose_worktree=False)
        # All three dispose calls attempted despite the first failing.
        self.assertEqual(sc.dispose.call_count, 3)
        wt.dispose_swarm.assert_not_called()


class TestRegistryRoundTrip(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.mkdtemp(prefix='swarm-reg-')
        self.registry = SwarmRegistry(self.root)
        self.handle = SwarmHandle(
            swarm_id='s1',
            repo_url='https://github.com/o/r.git',
            worktree_path='/srv/worktrees/s1',
            coder_session='conv_coder',
            reviewers=(Reviewer(role='security', session='conv_sec'),),
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def test_entry_serialization_is_reversible(self) -> None:
        entry = handle_to_entry(
            self.handle,
            server='http://x:6767',
            canonical_root='/c',
            worktree_root='/w',
        )
        rebuilt = handle_from_entry(entry)
        self.assertEqual(rebuilt, self.handle)

    def test_save_load_remove_and_list(self) -> None:
        entry = handle_to_entry(
            self.handle,
            server='http://x:6767',
            canonical_root='/c',
            worktree_root='/w',
        )
        self.registry.save(entry)
        self.assertEqual(self.registry.list_ids(), ['s1'])
        loaded = self.registry.load('s1')
        self.assertEqual(loaded['server'], 'http://x:6767')
        self.assertEqual(handle_from_entry(loaded), self.handle)
        self.registry.remove('s1')
        self.assertEqual(self.registry.list_ids(), [])

    def test_load_unknown_swarm_raises(self) -> None:
        with self.assertRaises(click.ClickException):
            self.registry.load('nope')

    def test_list_ids_on_missing_dir(self) -> None:
        empty = os.path.join(self.root, 'does-not-exist')
        self.assertEqual(SwarmRegistry(empty).list_ids(), [])


if __name__ == '__main__':
    unittest.main()


class TestCodexIsAThirdHarness(unittest.TestCase):
    """Additive: agy stays supported and unchanged. What made this more
    than config was that the launcher's mount sentinel carried a bare
    `agy: bool` — it now has to say WHICH credential a VM gets."""

    def test_each_harness_maps_to_its_own_credential(self) -> None:
        self.assertEqual(credential_kind_for('antigravity-native'), 'agy')
        self.assertEqual(credential_kind_for('codex-native'), 'codex')
        self.assertIsNone(credential_kind_for('claude-native'))
        self.assertIsNone(credential_kind_for(None))

    def test_the_sentinel_tags_the_credential_it_needs(self) -> None:
        self.assertTrue(
            mount_sentinel('/srv/w/a', 'rw', credential='codex')
            .endswith('#rw-codex')
        )
        self.assertTrue(
            mount_sentinel('/srv/w/a', 'ro', credential='agy')
            .endswith('#ro-agy')
        )
        self.assertTrue(
            mount_sentinel('/srv/w/a', 'rw').endswith('#rw')
        )

    def test_an_unknown_credential_is_refused(self) -> None:
        # A typo must not silently produce an untagged VM that then
        # fails its first turn on a missing credential.
        with self.assertRaises(ValueError):
            mount_sentinel('/srv/w/a', 'rw', credential='gemini')

    def test_codex_does_not_get_claudes_launch_flag(self) -> None:
        # THE bug this task existed to fix. codex rejects
        # `--permission-mode` with exit 2, so every Codex agent would
        # have died at launch. Its own help names this flag for an
        # externally sandboxed environment, which the microVM is.
        args = _launch_args_for('codex-native')
        self.assertEqual(args, ('--dangerously-bypass-approvals-and-sandbox',))
        self.assertNotIn('--permission-mode', args)

    def test_claude_uses_neither_retired_mode(self) -> None:
        """
        Both previous Claude modes were wrong, in opposite ways.

        `auto` was silently downgraded to MANUAL on Haiku 4.5 — its
        classifier is not implemented there — and the agent then
        blocked on an approval prompt for every tool call (TASKS.md
        #28). `dontAsk` replaced it and turned out not to auto-approve
        at all: it suppresses the prompt and DENIES, so every writer
        was refused while read-only reviewers looked healthy (TASKS.md
        #39). Omnigent's own value for claude-native is
        `bypassPermissions`.
        """
        for harness in ('claude-native', None):
            with self.subTest(harness=harness):
                args = _launch_args_for(harness)
                self.assertNotIn('auto', args)
                self.assertNotIn('dontAsk', args)
                self.assertEqual(
                    args, ('--permission-mode', 'bypassPermissions')
                )

    def test_the_other_two_harnesses_are_untouched(self) -> None:
        self.assertEqual(
            _launch_args_for('antigravity-native'),
            ('--dangerously-skip-permissions',),
        )
        self.assertEqual(
            _launch_args_for('claude-native'),
            ('--permission-mode', 'bypassPermissions'),
        )
        self.assertEqual(
            _launch_args_for(None),
            ('--permission-mode', 'bypassPermissions'),
        )

    def test_codex_carries_its_reasoning_effort_in_its_launch_args(
        self,
    ) -> None:
        # codex-native DROPS the session's reasoning_effort: the
        # value is persisted and reported back by the API, but the
        # turn runs at codex's default (omnigent#2800/#3536).
        # `-c` is the only channel that reaches it.
        self.assertEqual(
            _launch_args_for('codex-native', 'xhigh'),
            (
                '--dangerously-bypass-approvals-and-sandbox',
                '-c',
                'model_reasoning_effort="xhigh"',
            ),
        )

    def test_an_effort_codex_does_not_accept_is_dropped(self) -> None:
        # `max` is a REAL level for Anthropic but absent from codex's
        # ladder, so a cadre that pins max everywhere must not send it
        # here — the value is interpolated into a `-c key=value` config
        # expression, and codex rejects an unknown effort client-side.
        for effort in ('max', 'ultra', 'bogus', ''):
            with self.subTest(effort=effort):
                self.assertEqual(
                    _launch_args_for('codex-native', effort),
                    ('--dangerously-bypass-approvals-and-sandbox',),
                )

    def test_no_effort_leaves_codex_on_its_own_default(self) -> None:
        self.assertEqual(
            _launch_args_for('codex-native', None),
            ('--dangerously-bypass-approvals-and-sandbox',),
        )

    def test_effort_does_not_leak_into_the_other_harnesses(self) -> None:
        # Claude gets --effort from Omnigent's own launch path, and for
        # agy the effort IS the model id — neither takes it from here.
        self.assertEqual(
            _launch_args_for('claude-native', 'xhigh'),
            ('--permission-mode', 'bypassPermissions'),
        )
        self.assertEqual(
            _launch_args_for('antigravity-native', 'high'),
            ('--dangerously-skip-permissions',),
        )
        self.assertEqual(
            _launch_args_for(None, 'xhigh'),
            ('--permission-mode', 'bypassPermissions'),
        )
