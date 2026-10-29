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

import unittest

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

    def test_an_edited_pull_request_body(self) -> None:
        calls: list[list[str]] = []
        self._mgr(calls).edit_pr_body(
            'https://github.com/org/repo.git',
            'https://github.com/org/repo/pull/7',
            'raised by @carol',
        )
        argv = calls[0]
        self.assertEqual(argv[argv.index('--body') + 1], 'raised by `@carol`')


if __name__ == '__main__':
    unittest.main()
