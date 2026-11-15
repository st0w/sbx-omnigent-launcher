"""Agent text sent to GitHub notifies nobody (#124).

Reviewer and verifier text reaches issue titles, issue bodies and pull
request bodies unchanged, and GitHub notifies whoever is named after an
``@``: a reviewer writing about the npm package ``@types/node`` notified
the GitHub account ``types``. Every write to GitHub now goes through
:func:`neutralize_mentions`, which puts each mention in a code span,
where GitHub does not treat it as one.

Run: .venv/bin/python -m unittest tests.test_worktrees_mentions
"""

from __future__ import annotations

import json
import os
import stat
import unittest

import click

from sbx_omnigent.worktrees import (
    WorktreeManager,
    _pr_create_command,
    neutralize_mentions,
)


class TestMentionsAreNeutralized(unittest.TestCase):
    def test_a_scoped_package_name(self) -> None:
        self.assertEqual(
            neutralize_mentions('pin @types/node to 22'),
            'pin `@types/node` to 22',
        )

    def test_a_team_handle(self) -> None:
        self.assertEqual(
            neutralize_mentions('ask @org/security-team first'),
            'ask `@org/security-team` first',
        )

    def test_a_user_handle(self) -> None:
        self.assertEqual(
            neutralize_mentions('as @alice said'), 'as `@alice` said'
        )

    def test_at_the_start_of_a_line(self) -> None:
        self.assertEqual(
            neutralize_mentions('first\n@bob owns this'),
            'first\n`@bob` owns this',
        )

    def test_after_punctuation_and_in_parentheses(self) -> None:
        self.assertEqual(
            neutralize_mentions('(cc @carol), see:@dave; "@erin"'),
            '(cc `@carol`), see:`@dave`; "`@erin`"',
        )

    def test_trailing_punctuation_stays_outside(self) -> None:
        for text, want in (
            ('thanks @frank.', 'thanks `@frank`.'),
            ('ask @org/team.', 'ask `@org/team`.'),
            ('ask @org/team-.', 'ask `@org/team`-.'),
        ):
            with self.subTest(text=text):
                self.assertEqual(neutralize_mentions(text), want)

    def test_several_on_one_line(self) -> None:
        self.assertEqual(
            neutralize_mentions('@a and @b-c'), '`@a` and `@b-c`'
        )

    def test_a_list_item_and_a_heading(self) -> None:
        self.assertEqual(
            neutralize_mentions('- @g\n## @h'), '- `@g`\n## `@h`'
        )


class TestWhatIsLeftAlone(unittest.TestCase):
    def test_an_email_address(self) -> None:
        text = 'mail ops@example.com or a.b@x.io'
        self.assertEqual(neutralize_mentions(text), text)

    def test_an_inline_code_span(self) -> None:
        for text in ('run `npm i @types/node` now', 'a ``x `@y` z`` b'):
            with self.subTest(text=text):
                self.assertEqual(neutralize_mentions(text), text)

    def test_a_fenced_block(self) -> None:
        for fence in ('```', '~~~', '````'):
            with self.subTest(fence=fence):
                text = f'before\n{fence}\n@inside\n{fence}\nafter'
                self.assertEqual(neutralize_mentions(text), text)

    def test_a_mention_after_a_fenced_block_is_still_caught(self) -> None:
        self.assertEqual(
            neutralize_mentions('```\n@in\n```\n@out'),
            '```\n@in\n```\n`@out`',
        )

    def test_only_a_matching_fence_closes_one(self) -> None:
        # A tilde line, a shorter run, or a fence with an info string
        # inside a backtick block is content, not its end.
        for inner in ('~~~', '``', '```python'):
            with self.subTest(inner=inner):
                text = f'```\n{inner}\n@still-code\n```\n@out'
                self.assertEqual(
                    neutralize_mentions(text),
                    f'```\n{inner}\n@still-code\n```\n`@out`',
                )

    def test_a_shorter_run_does_not_close_a_longer_fence(self) -> None:
        text = '````\n```\n@still-code\n````\n@out'
        self.assertEqual(
            neutralize_mentions(text),
            '````\n```\n@still-code\n````\n`@out`',
        )

    def test_an_unclosed_fence_runs_to_the_end(self) -> None:
        text = 'text\n```\n@never-closed\nstill code'
        self.assertEqual(neutralize_mentions(text), text)

    def test_an_html_comment(self) -> None:
        # The finding-id marker lives in one, and dedup reads it back.
        for text in (
            'body\n<!-- note for @bob -->',
            'a <!-- multi\nline @carol --> b',
        ):
            with self.subTest(text=text):
                self.assertEqual(neutralize_mentions(text), text)

    def test_a_url(self) -> None:
        text = 'see https://social.example/@someone for that'
        self.assertEqual(neutralize_mentions(text), text)

    def test_a_bare_at_sign(self) -> None:
        for text in ('meet @ noon', 'x @ y', '@', '@-'):
            with self.subTest(text=text):
                self.assertEqual(neutralize_mentions(text), text)

    def test_running_it_twice_changes_nothing_more(self) -> None:
        once = neutralize_mentions('cc @alice and @types/node')
        self.assertEqual(neutralize_mentions(once), once)

    def test_text_without_mentions_is_unchanged(self) -> None:
        text = '## Findings\n\n- a `x` b\n\n```\ncode\n```\n'
        self.assertEqual(neutralize_mentions(text), text)


class TestEveryGitHubWriteGoesThroughIt(unittest.TestCase):
    """The three places text reaches GitHub, so no path can skip it."""

    def test_a_new_pull_request(self) -> None:
        argv = _pr_create_command(
            'org/repo', head='h', base='main',
            title='fix for @alice', body='thanks @types/node', draft=True,
        )
        self.assertEqual(argv[argv.index('--title') + 1], 'fix for `@alice`')
        self.assertEqual(
            argv[argv.index('--body') + 1], 'thanks `@types/node`'
        )

    def _mgr(self, calls: list[list[str]]) -> WorktreeManager:
        def rec(cmd, *, cwd=None, env=None):
            calls.append(cmd)
            return 'https://github.com/org/repo/issues/1\n'

        return WorktreeManager(
            canonical_root='/nonexistent/repos',
            worktree_root='/nonexistent/worktrees',
            default_branch='main',
            run=rec,
        )

    def test_a_new_issue(self) -> None:
        calls: list[list[str]] = []
        self._mgr(calls).create_issue(
            'https://github.com/org/repo', title='@bob broke it',
            body='per @org/team',
        )
        argv = calls[0]
        self.assertEqual(argv[argv.index('--title') + 1], '`@bob` broke it')
        self.assertEqual(argv[argv.index('--body') + 1], 'per `@org/team`')


class TestThePullRequestIsEditedOverRest(unittest.TestCase):
    """
    A PR description is replaced through the REST endpoint, which needs
    only the ``repo`` scope. ``gh pr edit`` also reads the PR's
    reviewers and teams over GraphQL, which a classic token needs
    ``read:org`` for: live, the publish token had only ``repo`` and
    ``workflow``, and every edit failed.
    """

    def _edit(self, *, fail: bool = False):
        seen: dict[str, object] = {}

        def run(cmd, *, cwd=None, env=None):
            seen['argv'] = cmd
            path = cmd[cmd.index('--input') + 1]
            seen['path'] = path
            with open(path, encoding='utf-8') as fh:
                seen['payload'] = json.load(fh)
            seen['mode'] = stat.S_IMODE(os.stat(path).st_mode)
            if fail:
                raise click.ClickException('HTTP 422')
            return '{}'

        mgr = WorktreeManager(
            canonical_root='/nonexistent/repos',
            worktree_root='/nonexistent/worktrees',
            run=run,
        )
        def call() -> None:
            mgr.edit_pr_body(
                'https://github.com/org/repo.git',
                'https://github.com/org/repo/pull/7',
                'raised by @carol',
            )

        if fail:
            with self.assertRaises(click.ClickException):
                call()
        else:
            call()
        return seen

    def test_it_patches_the_pull_over_rest(self) -> None:
        argv = self._edit()['argv']
        self.assertEqual(
            argv[:5],
            ['gh', 'api', '--method', 'PATCH', 'repos/org/repo/pulls/7'],
        )
        self.assertNotIn('pr', argv[1:2])

    def test_the_description_goes_in_a_json_file_not_argv(self) -> None:
        seen = self._edit()
        self.assertEqual(seen['payload'], {'body': 'raised by `@carol`'})
        self.assertFalse(
            any('carol' in str(arg) for arg in seen['argv'])
        )

    def test_the_file_is_private_and_removed(self) -> None:
        seen = self._edit()
        self.assertEqual(seen['mode'], 0o600)
        self.assertFalse(os.path.exists(str(seen['path'])))

    def test_the_file_is_removed_when_the_edit_fails(self) -> None:
        seen = self._edit(fail=True)
        self.assertFalse(os.path.exists(str(seen['path'])))

    def test_a_url_of_another_repo_is_refused(self) -> None:
        mgr = WorktreeManager(
            canonical_root='/nonexistent/repos',
            worktree_root='/nonexistent/worktrees',
            run=lambda *a, **k: self.fail('nothing may run'),
        )
        with self.assertRaises(click.ClickException):
            mgr.edit_pr_body(
                'https://github.com/org/repo.git',
                'https://github.com/other/repo/pull/7', 'x',
            )


class TestTheBaseMayAlreadyHoldACommit(unittest.TestCase):
    """GitHub's compare endpoint says whether the base branch already
    contains a commit, so a PR knows whether an earlier module's work
    is still in its diff."""

    def _ask(self, reply, *, commit='a' * 40, branch='main'):
        calls: list[list[str]] = []

        def run(cmd, *, cwd=None, env=None):
            calls.append(cmd)
            if isinstance(reply, Exception):
                raise reply
            return reply

        mgr = WorktreeManager(
            canonical_root='/nonexistent/repos',
            worktree_root='/nonexistent/worktrees',
            run=run,
        )
        answer = mgr.base_contains(
            'https://github.com/org/repo.git', branch=branch, commit=commit
        )
        return answer, calls

    def test_nothing_ahead_means_it_is_contained(self) -> None:
        answer, calls = self._ask('0\n')
        self.assertIs(answer, True)
        self.assertEqual(
            calls[0][:3],
            ['gh', 'api', f'repos/org/repo/compare/main...{"a" * 40}'],
        )
        self.assertIn('.ahead_by', calls[0])

    def test_commits_ahead_means_it_is_not(self) -> None:
        self.assertIs(self._ask('3\n')[0], False)

    def test_an_unreadable_answer_is_unknown(self) -> None:
        for reply in ('', 'null', 'x', '-1', click.ClickException('404')):
            with self.subTest(reply=reply):
                self.assertIsNone(self._ask(reply)[0])

    def test_a_commit_that_is_not_an_object_id_is_never_sent(self) -> None:
        for commit in ('main', '../x', 'a' * 6, ''):
            with self.subTest(commit=commit):
                answer, calls = self._ask('0', commit=commit)
                self.assertIsNone(answer)
                self.assertEqual(calls, [])

    def test_a_branch_with_a_slash_is_kept_in_the_path(self) -> None:
        _a, calls = self._ask('0', branch='release/1.0')
        self.assertTrue(
            calls[0][2].startswith('repos/org/repo/compare/release/1.0...')
        )

    def test_an_unsafe_branch_is_never_sent(self) -> None:
        for branch in ('a b', '../main', 'main?x=1', '-x', ''):
            with self.subTest(branch=branch):
                answer, calls = self._ask('0', branch=branch)
                self.assertIsNone(answer)
                self.assertEqual(calls, [])


if __name__ == '__main__':
    unittest.main()
