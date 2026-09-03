"""Unit + local-git tests for :mod:`sbx_omnigent.worktrees`.

Pure helpers and the ``WorktreeManager`` lifecycle. Git integration
runs against a throwaway LOCAL upstream (no network); publish is checked
with an injected command recorder (no real ``git push`` / ``gh``). Run:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest

import click

from sbx_omnigent.worktrees import (
    WorktreeManager,
    _pr_create_command,
    _repo_name,
    _validate_name,
    github_slug,
    looks_like_auth_failure,
)

_GIT_ENV = {
    **os.environ,
    'GIT_AUTHOR_NAME': 't',
    'GIT_AUTHOR_EMAIL': 't@t',
    'GIT_COMMITTER_NAME': 't',
    'GIT_COMMITTER_EMAIL': 't@t',
}


def _git(cwd: str, *args: str) -> str:
    proc = subprocess.run(
        ['git', '-C', cwd, *args],
        check=True,
        capture_output=True,
        text=True,
        env=_GIT_ENV,
    )
    return proc.stdout


def _make_upstream(parent: str) -> str:
    """Create a local ``main`` repo with a README (push target)."""
    up = os.path.join(parent, 'upstream')
    os.makedirs(up)
    _git(up, 'init', '-q')
    _git(up, 'symbolic-ref', 'HEAD', 'refs/heads/main')
    with open(os.path.join(up, 'README.txt'), 'w', encoding='utf-8') as fh:
        fh.write('upstream main\n')
    _git(up, 'add', '-A')
    _git(up, 'commit', '-qm', 'init')
    return up


class TestPureHelpers(unittest.TestCase):
    def test_validate_name_ok(self) -> None:
        self.assertEqual(
            _validate_name('swarm-a.1_x', 'swarm id'), 'swarm-a.1_x'
        )

    def test_validate_name_rejects_separators(self) -> None:
        for bad in ('a/b', '..', '.', 'a b', 'a$b', ''):
            with self.assertRaises(click.ClickException):
                _validate_name(bad, 'swarm id')

    def test_repo_name(self) -> None:
        self.assertEqual(_repo_name('https://github.com/org/repo.git'), 'repo')
        self.assertEqual(_repo_name('git@github.com:org/repo'), 'repo')

    def test_github_slug_forms(self) -> None:
        for url in (
            'https://github.com/org/repo.git',
            'https://github.com/org/repo',
            'git@github.com:org/repo.git',
            'ssh://git@github.com/org/repo.git',
        ):
            self.assertEqual(github_slug(url), 'org/repo')

    def test_github_slug_rejects_bad(self) -> None:
        for bad in ('not-a-url', 'https://github.com/orgonly'):
            with self.assertRaises(click.ClickException):
                github_slug(bad)

    def test_pr_command_draft_and_ready(self) -> None:
        draft = _pr_create_command(
            'org/repo',
            head='task/x',
            base='main',
            title='T',
            body='B',
            draft=True,
        )
        self.assertEqual(draft[:3], ['gh', 'pr', 'create'])
        self.assertIn('--draft', draft)
        self.assertEqual(draft[draft.index('-R') + 1], 'org/repo')
        ready = _pr_create_command(
            'org/repo',
            head='task/x',
            base='main',
            title='T',
            body='B',
            draft=False,
        )
        self.assertNotIn('--draft', ready)


class TestWorktreeLifecycle(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='wt-life-')
        self.up = _make_upstream(self.tmp)
        self.mgr = WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'repos'),
            worktree_root=os.path.join(self.tmp, 'worktrees'),
            default_branch='main',
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_ensure_canonical_clone_then_refresh(self) -> None:
        path = self.mgr.ensure_canonical(self.up)
        self.assertTrue(os.path.isdir(path))
        branches = _git(path, 'branch', '--list', 'main')
        self.assertIn('main', branches)
        # Second call refreshes (fetch) without error.
        self.assertEqual(self.mgr.ensure_canonical(self.up), path)

    def test_create_swarm_worktree(self) -> None:
        wt = self.mgr.create_swarm_worktree('swarm-a', self.up)
        self.assertTrue(os.path.isdir(wt))
        head = _git(wt, 'rev-parse', '--abbrev-ref', 'HEAD').strip()
        self.assertEqual(head, 'task/swarm-a')
        self.assertTrue(os.path.isfile(os.path.join(wt, 'README.txt')))

    def test_create_swarm_worktree_rejects_duplicate(self) -> None:
        self.mgr.create_swarm_worktree('swarm-a', self.up)
        with self.assertRaises(click.ClickException):
            self.mgr.create_swarm_worktree('swarm-a', self.up)

    def test_commit_worktree_commits_dirty_tree(self) -> None:
        wt = self.mgr.create_swarm_worktree('swarm-c', self.up)
        with open(os.path.join(wt, 'app.txt'), 'w', encoding='utf-8') as fh:
            fh.write('coder change\n')
        made = self.mgr.commit_worktree(
            'swarm-c',
            message='add app',
            author='swarm coder <coder@swarm.local>',
        )
        self.assertTrue(made)
        # The commit landed on the task branch with the coder as AUTHOR.
        log = _git(wt, 'log', '-1', '--pretty=%an <%ae>%n%s')
        self.assertIn('coder@swarm.local', log)
        self.assertIn('add app', log)
        self.assertEqual(_git(wt, 'status', '--porcelain').strip(), '')

    def test_commit_worktree_clean_tree_is_noop(self) -> None:
        self.mgr.create_swarm_worktree('swarm-d', self.up)
        self.assertFalse(
            self.mgr.commit_worktree('swarm-d', message='nothing')
        )

    def test_commit_worktree_missing_worktree_raises(self) -> None:
        with self.assertRaises(click.ClickException):
            self.mgr.commit_worktree('no-such', message='x')

    def test_dispose_swarm(self) -> None:
        wt = self.mgr.create_swarm_worktree('swarm-b', self.up)
        self.mgr.dispose_swarm('swarm-b')
        self.assertFalse(os.path.exists(wt))


class TestPublishRecorded(unittest.TestCase):
    """Publish sequence via an injected recorder (no push / gh)."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='wt-pub-')
        self.wt_root = os.path.join(self.tmp, 'worktrees')
        os.makedirs(os.path.join(self.wt_root, 'swarm-a'))
        self.calls: list[list[str]] = []

        def rec(cmd: list[str], *, cwd: str | None = None) -> str:
            self.calls.append(cmd)
            return 'https://github.com/org/repo/pull/7\n'

        self.mgr = WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'repos'),
            worktree_root=self.wt_root,
            default_branch='main',
            run=rec,
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_publish_pushes_task_branch_then_opens_draft(self) -> None:
        url = self.mgr.publish_swarm(
            'swarm-a',
            'https://github.com/org/repo.git',
            title='Add feature',
            body='why + how',
        )
        self.assertEqual(url, 'https://github.com/org/repo/pull/7')
        self.assertEqual(len(self.calls), 2)

        push = self.calls[0]
        self.assertEqual(push[:2], ['git', '-C'])
        self.assertIn('push', push)
        self.assertIn('https://github.com/org/repo.git', push)
        # Explicit src:dst — only the task branch moves, never base.
        self.assertIn('task/swarm-a:task/swarm-a', push)

        gh = self.calls[1]
        self.assertEqual(gh[:3], ['gh', 'pr', 'create'])
        self.assertIn('--draft', gh)
        self.assertEqual(gh[gh.index('-R') + 1], 'org/repo')
        self.assertEqual(gh[gh.index('--head') + 1], 'task/swarm-a')
        self.assertEqual(gh[gh.index('--base') + 1], 'main')

    def test_publish_local_mode_pushes_only_no_gh(self) -> None:
        result = self.mgr.publish_swarm(
            'swarm-a',
            '/path/to/local/repo',
            title='ignored in local mode',
            body='',
            open_pr=False,
        )
        # Exactly one command: the branch push. No gh, no slug parse.
        self.assertEqual(len(self.calls), 1)
        push = self.calls[0]
        self.assertIn('push', push)
        self.assertIn('/path/to/local/repo', push)
        self.assertIn('task/swarm-a:task/swarm-a', push)
        # Human-readable summary naming the branch + target, not a URL.
        self.assertIn('task/swarm-a', result)
        self.assertIn('/path/to/local/repo', result)

    def test_publish_missing_worktree_fails(self) -> None:
        with self.assertRaises(click.ClickException):
            self.mgr.publish_swarm(
                'no-such',
                'https://github.com/org/repo.git',
                title='T',
                body='B',
            )



class TestPublishIdentity(unittest.TestCase):
    """The publish token is scoped to the publish subprocess only."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='wt-tok-')
        self.wt_root = os.path.join(self.tmp, 'worktrees')
        os.makedirs(os.path.join(self.wt_root, 'swarm-a'))
        os.makedirs(os.path.join(self.wt_root, 'r1', 'repo'))
        #: (argv, env) for every command the manager ran.
        self.calls: list[tuple[list[str], dict | None]] = []

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mgr(self, token=None, fail=None) -> WorktreeManager:
        def rec(cmd, *, cwd=None, env=None):
            self.calls.append((cmd, env))
            if fail is not None:
                raise click.ClickException(fail)
            return 'https://github.com/org/repo/pull/7\n'

        return WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'repos'),
            worktree_root=self.wt_root,
            default_branch='main',
            run=rec,
            publish_token=token,
        )

    def _publish(self, mgr):
        return mgr.publish_swarm(
            'swarm-a',
            'https://github.com/org/repo.git',
            title='T',
            body='B',
        )

    def test_no_token_inherits_the_parent_environment(self) -> None:
        self._publish(self._mgr())
        self.assertEqual(len(self.calls), 2)  # push + gh
        # env=None means "inherit" — behavior is unchanged for hosts
        # that publish on their own credentials.
        self.assertTrue(all(env is None for _c, env in self.calls))

    def test_token_reaches_both_publish_commands(self) -> None:
        self._publish(self._mgr(token='tok-secret'))
        self.assertEqual(len(self.calls), 2)
        for cmd, env in self.calls:
            self.assertEqual(env['GH_TOKEN'], 'tok-secret')
            # …and NEVER on the command line, where ps would see it.
            self.assertNotIn('tok-secret', ' '.join(cmd))

    def test_token_env_keeps_the_rest_of_environ(self) -> None:
        self._publish(self._mgr(token='tok-secret'))
        _cmd, env = self.calls[0]
        for key in os.environ:
            self.assertIn(key, env)

    def test_failure_message_redacts_the_token(self) -> None:
        mgr = self._mgr(
            token='tok-secret', fail='fatal: bad creds tok-secret'
        )
        with self.assertRaises(click.ClickException) as ctx:
            self._publish(mgr)
        self.assertNotIn('tok-secret', str(ctx.exception))
        self.assertIn('***', str(ctx.exception))

    def test_pipeline_publish_node_is_scoped_too(self) -> None:
        mgr = self._mgr(token='tok-secret')
        mgr.publish_node(
            'r1',
            'refactor',
            'https://github.com/org/repo.git',
            title='T',
            body='B',
            remote_branch='pipeline/r1-m0',
        )
        self.assertEqual(len(self.calls), 2)
        for cmd, env in self.calls:
            self.assertEqual(env['GH_TOKEN'], 'tok-secret')
            self.assertNotIn('tok-secret', ' '.join(cmd))


class TestAuthFailureDetection(unittest.TestCase):
    """
    Only a REJECTED CREDENTIAL earns a re-read and a retry.

    A repository failure retried with a different token is a failing
    command run twice; the point of matching narrowly is that it stays
    one.
    """

    def test_the_live_failure_is_recognised(self) -> None:
        # Verbatim from the run that lost a module's pull request.
        self.assertTrue(
            looks_like_auth_failure(
                'remote: Invalid username or token. Password '
                'authentication is not supported for Git operations.\n'
                "fatal: Authentication failed for "
                "'https://github.com/org/repo/'"
            )
        )

    def test_gh_api_credential_failures_are_recognised(self) -> None:
        for text in ('HTTP 401: Bad credentials',
                     'gh: This API operation requires authentication',
                     'fatal: could not read Username for https://...'):
            with self.subTest(text=text):
                self.assertTrue(looks_like_auth_failure(text))

    def test_repository_failures_are_NOT_recognised(self) -> None:
        for text in ('error: failed to push some refs (non-fast-forward)',
                     'GraphQL: Resource not accessible by integration',
                     'remote: Repository not found.',
                     'protected branch hook declined',
                     ''):
            with self.subTest(text=text):
                self.assertFalse(looks_like_auth_failure(text))


class TestPublishTokenIsReadLate(unittest.TestCase):
    """
    Publish is the LAST thing a run does, hours after it started.

    A token captured at startup and held in memory can be rotated or
    expired by then, and the runner used to present exactly that: the
    whole campaign passed, then the push was refused with "Invalid
    username or token" (TASKS.md #43).
    """

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='wt-late-')
        self.wt_root = os.path.join(self.tmp, 'worktrees')
        os.makedirs(os.path.join(self.wt_root, 'swarm-a'))
        #: (argv, env) for every command run.
        self.calls: list[tuple[list[str], dict | None]] = []
        #: Tokens the provider handed out, in order.
        self.handed: list[str | None] = []

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mgr(self, provider, fail_first: str | None = None):
        def rec(cmd, *, cwd=None, env=None):
            self.calls.append((cmd, env))
            if fail_first is not None and len(self.calls) == 1:
                raise click.ClickException(fail_first)
            return 'https://github.com/org/repo/pull/7\n'

        def wrapped() -> str | None:
            token = provider()
            self.handed.append(token)
            return token

        return WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'repos'),
            worktree_root=self.wt_root,
            default_branch='main',
            run=rec,
            publish_token=wrapped,
        )

    def _publish(self, mgr):
        return mgr.publish_swarm(
            'swarm-a',
            'https://github.com/org/repo.git',
            title='T',
            body='B',
        )

    def test_the_provider_is_not_called_before_publish(self) -> None:
        # The whole point: construction must not capture the value.
        self._mgr(lambda: 'tok-a')
        self.assertEqual(self.handed, [])

    def test_the_provider_supplies_the_token_at_publish(self) -> None:
        self._publish(self._mgr(lambda: 'tok-a'))
        self.assertEqual(self.handed, ['tok-a'])
        for _cmd, env in self.calls:
            self.assertEqual(env['GH_TOKEN'], 'tok-a')

    def test_a_plain_string_still_works(self) -> None:
        # Backward compatible: callers passing a resolved token, and
        # every existing test, are unaffected.
        mgr = WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'repos'),
            worktree_root=self.wt_root,
            default_branch='main',
            run=lambda cmd, *, cwd=None, env=None: (
                self.calls.append((cmd, env)) or 'url\n'
            ),
            publish_token='tok-plain',
        )
        self._publish(mgr)
        self.assertEqual(self.calls[0][1]['GH_TOKEN'], 'tok-plain')

    def test_a_rejected_token_is_re_read_and_retried_once(self) -> None:
        # The observed failure, end to end: the process holds the
        # pre-rotation token, the store holds the new one.
        tokens = iter(['stale', 'rotated'])
        mgr = self._mgr(
            lambda: next(tokens),
            fail_first='fatal: Authentication failed for https://…',
        )
        self._publish(mgr)
        self.assertEqual(self.handed, ['stale', 'rotated'])
        # push retried with the NEW token, then the PR opened with it.
        self.assertEqual(
            [env['GH_TOKEN'] for _c, env in self.calls],
            ['stale', 'rotated', 'rotated'],
        )

    def test_a_repository_failure_is_not_retried(self) -> None:
        tokens = iter(['tok-a', 'tok-b'])
        mgr = self._mgr(
            lambda: next(tokens),
            fail_first='error: failed to push some refs',
        )
        with self.assertRaises(click.ClickException):
            self._publish(mgr)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.handed, ['tok-a'])

    def test_an_unchanged_token_is_not_retried_and_says_why(self) -> None:
        # The store holds the same value, so the token really is
        # expired or unauthorised — retrying would just fail twice.
        mgr = self._mgr(
            lambda: 'same',
            fail_first='remote: Invalid username or token.',
        )
        with self.assertRaises(click.ClickException) as ctx:
            self._publish(mgr)
        self.assertEqual(len(self.calls), 1)
        self.assertIn('still holds the same value', str(ctx.exception))

    def test_a_failing_re_read_reports_the_original_rejection(self) -> None:
        def boom() -> str | None:
            if self.handed:
                raise RuntimeError('keychain locked')
            return 'stale'

        mgr = self._mgr(boom, fail_first='fatal: Authentication failed')
        with self.assertRaises(click.ClickException) as ctx:
            self._publish(mgr)
        message = str(ctx.exception)
        self.assertIn('Authentication failed', message)
        self.assertIn('keychain locked', message)

    def test_the_retry_redacts_the_new_token_too(self) -> None:
        tokens = iter(['stale', 'rotated'])

        def rec(cmd, *, cwd=None, env=None):
            self.calls.append((cmd, env))
            raise click.ClickException(
                'fatal: Authentication failed for rotated'
                if len(self.calls) > 1
                else 'fatal: Authentication failed for stale'
            )

        mgr = WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'repos'),
            worktree_root=self.wt_root,
            default_branch='main',
            run=rec,
            publish_token=lambda: next(tokens),
        )
        with self.assertRaises(click.ClickException) as ctx:
            self._publish(mgr)
        self.assertNotIn('rotated', str(ctx.exception))
        self.assertIn('***', str(ctx.exception))


class TestWarmBuildCache(unittest.TestCase):
    """
    Carrying a build directory between nodes (TASKS.md #46, lever 2).

    Measured on gcp-custom-roles-1: every node compiled the workspace
    from clean, and that was the dominant cost of an increment — the
    whole toolchain install is 93 seconds, so the rest of a 12-minute
    gate stage is the compile.
    """

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='wt-cache-')
        self.can = os.path.join(self.tmp, 'can')
        self.mgr = WorktreeManager(
            canonical_root=self.can,
            worktree_root=os.path.join(self.tmp, 'wt'),
            build_cache=('target',),
            build_cache_key='proj',
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _node(self, name: str, built: bool = True) -> str:
        path = os.path.join(self.tmp, name)
        os.makedirs(path, exist_ok=True)
        if built:
            os.makedirs(
                os.path.join(path, 'target', 'debug'), exist_ok=True
            )
            with open(os.path.join(path, 'target', 'artifact'), 'w') as fh:
                fh.write('compiled')
        return path

    def test_a_finished_node_seeds_the_next_one(self) -> None:
        self.assertEqual(
            self.mgr.refresh_build_cache(self._node('a')), ['target']
        )
        fresh = self._node('b', built=False)
        self.assertEqual(self.mgr.seed_build_cache(fresh), ['target'])
        with open(os.path.join(fresh, 'target', 'artifact')) as fh:
            self.assertEqual(fh.read(), 'compiled')

    def test_it_never_overwrites_a_directory_already_there(self) -> None:
        # A checked-in directory of the same name belongs to the
        # repository, not to the cache.
        self.mgr.refresh_build_cache(self._node('a'))
        theirs = self._node('b')
        with open(os.path.join(theirs, 'target', 'artifact'), 'w') as fh:
            fh.write('theirs')
        self.assertEqual(self.mgr.seed_build_cache(theirs), [])
        with open(os.path.join(theirs, 'target', 'artifact')) as fh:
            self.assertEqual(fh.read(), 'theirs')

    def test_a_node_with_nothing_built_is_a_silent_no_op(self) -> None:
        # Readers and :ro reviewers build nothing; refreshing from one
        # must not empty the cache the writers filled.
        self.mgr.refresh_build_cache(self._node('a'))
        self.assertEqual(
            self.mgr.refresh_build_cache(self._node('reader', built=False)),
            [],
        )
        self.assertEqual(
            self.mgr.seed_build_cache(self._node('b', built=False)),
            ['target'],
        )

    def test_an_empty_cache_entry_is_not_seeded(self) -> None:
        os.makedirs(os.path.join(self.can, '_buildcache', 'proj', 'target'))
        self.assertEqual(
            self.mgr.seed_build_cache(self._node('b', built=False)), []
        )

    def test_it_is_off_unless_both_settings_are_given(self) -> None:
        for kwargs in (
            {},
            {'build_cache': ('target',)},          # no key
            {'build_cache_key': 'proj'},           # no names
        ):
            with self.subTest(kwargs=kwargs):
                mgr = WorktreeManager(
                    canonical_root=self.can,
                    worktree_root=os.path.join(self.tmp, 'wt'),
                    **kwargs,
                )
                self.assertEqual(
                    mgr.refresh_build_cache(self._node('x')), []
                )
                self.assertEqual(
                    mgr.seed_build_cache(self._node('y', built=False)), []
                )

    def test_two_repositories_never_share_a_cache(self) -> None:
        other = WorktreeManager(
            canonical_root=self.can,
            worktree_root=os.path.join(self.tmp, 'wt'),
            build_cache=('target',),
            build_cache_key='other-repo',
        )
        self.mgr.refresh_build_cache(self._node('a'))
        self.assertEqual(
            other.seed_build_cache(self._node('b', built=False)), []
        )

    def test_a_failed_copy_leaves_no_half_written_directory(self) -> None:
        # A torn build directory is precisely what a build tool cannot
        # revalidate its way out of, so a failed seed must leave the
        # worktree exactly as it was.
        self.mgr.refresh_build_cache(self._node('a'))
        fresh = self._node('b', built=False)

        def boom(cmd, **kw):
            raise click.ClickException('copy died midway')

        self.mgr._run = boom
        self.assertEqual(self.mgr.seed_build_cache(fresh), [])
        self.assertFalse(os.path.exists(os.path.join(fresh, 'target')))

    def test_a_failed_refresh_keeps_the_previous_cache_intact(self) -> None:
        self.mgr.refresh_build_cache(self._node('a'))

        def boom(cmd, **kw):
            raise click.ClickException('copy died midway')

        self.mgr._run = boom
        self.assertEqual(self.mgr.refresh_build_cache(self._node('c')), [])
        cache = os.path.join(self.can, '_buildcache', 'proj')
        self.assertEqual(sorted(os.listdir(cache)), ['target'])
        with open(os.path.join(cache, 'target', 'artifact')) as fh:
            self.assertEqual(fh.read(), 'compiled')


class TestDisposeNodeWorktrees(unittest.TestCase):
    """Reclaiming a chunk's clones: named only, never by prefix."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='wt-reclaim-')
        self.wt_root = os.path.join(self.tmp, 'worktrees')
        self.nodes = os.path.join(self.wt_root, 'r1', 'nodes')
        os.makedirs(self.nodes)
        self.mgr = WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'repos'),
            worktree_root=self.wt_root,
            default_branch='main',
            run=lambda cmd, **kw: '',
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _node(self, name: str) -> str:
        path = os.path.join(self.nodes, name)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, 'f.txt'), 'w') as fh:
            fh.write('x')
        return path

    def test_removes_only_the_named_clones(self) -> None:
        self._node('m0-build')
        keep = self._node('m1-build')
        self.assertEqual(
            self.mgr.dispose_node_worktrees('r1', ['m0-build']), 1
        )
        self.assertFalse(os.path.isdir(os.path.join(self.nodes, 'm0-build')))
        self.assertTrue(os.path.isdir(keep))

    def test_never_sweeps_by_prefix(self) -> None:
        # Chunk ids 'core' and 'core-extra' share a prefix; a prefix
        # sweep would delete a LATER chunk's live worktrees.
        self._node('core-build')
        live = self._node('core-extra-build')
        self.mgr.dispose_node_worktrees('r1', ['core-build'])
        self.assertTrue(os.path.isdir(live))

    def test_missing_names_are_skipped_not_fatal(self) -> None:
        self._node('m0-build')
        freed = self.mgr.dispose_node_worktrees(
            'r1', ['m0-build', 'never-existed', 'm0-build-verify']
        )
        self.assertEqual(freed, 1)

    def test_an_unsafe_name_is_refused(self) -> None:
        with self.assertRaises(click.ClickException):
            self.mgr.dispose_node_worktrees('r1', ['../../etc'])

    def test_a_run_with_no_nodes_dir_is_a_noop(self) -> None:
        self.assertEqual(
            self.mgr.dispose_node_worktrees('never-ran', ['x']), 0
        )


class TestRunState(unittest.TestCase):
    """Run bookkeeping: written atomically, never fails a run."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='wt-state-')
        self.mgr = WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'repos'),
            worktree_root=os.path.join(self.tmp, 'worktrees'),
            default_branch='main',
            run=lambda cmd, **kw: '',
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_round_trip(self) -> None:
        payload = {'version': 1, 'nodes': {'build': {'branch': 'pl/r1/build'}}}
        self.assertTrue(self.mgr.write_run_state('r1', payload))
        self.assertEqual(self.mgr.read_run_state('r1'), payload)

    def test_state_lives_beside_the_hub_not_inside_the_repo(self) -> None:
        # It is orchestrator bookkeeping: no node may commit it and no
        # agent may see it.
        self.mgr.write_run_state('r1', {'version': 1})
        path = self.mgr.run_state_path('r1')
        self.assertTrue(path.startswith(self.mgr.run_dir('r1')))
        self.assertNotIn(os.sep + 'repo' + os.sep, path)

    def test_rewrite_replaces_cleanly(self) -> None:
        self.mgr.write_run_state('r1', {'version': 1, 'n': 1})
        self.mgr.write_run_state('r1', {'version': 1, 'n': 2})
        self.assertEqual(self.mgr.read_run_state('r1')['n'], 2)
        # The temp file used for the atomic swap is never left behind.
        leftovers = [
            f for f in os.listdir(self.mgr.run_dir('r1'))
            if f.endswith('.tmp')
        ]
        self.assertEqual(leftovers, [])

    def test_missing_state_reads_as_none(self) -> None:
        self.assertIsNone(self.mgr.read_run_state('never-ran'))

    def test_corrupt_state_reads_as_none(self) -> None:
        # A half-written file from an older build must not be resumed
        # from — better to start clean than to trust a misread.
        self.mgr.write_run_state('r1', {'version': 1})
        with open(self.mgr.run_state_path('r1'), 'w', encoding='utf-8') as fh:
            fh.write('{not json')
        self.assertIsNone(self.mgr.read_run_state('r1'))

    def test_non_object_state_reads_as_none(self) -> None:
        self.mgr.write_run_state('r1', {'version': 1})
        with open(self.mgr.run_state_path('r1'), 'w', encoding='utf-8') as fh:
            fh.write('[1, 2, 3]')
        self.assertIsNone(self.mgr.read_run_state('r1'))

    def test_unserializable_payload_fails_quietly(self) -> None:
        # Bookkeeping must never take down a run that is succeeding.
        self.assertFalse(self.mgr.write_run_state('r1', {'x': {1, 2}}))

    def test_unwritable_location_fails_quietly(self) -> None:
        blocked = os.path.join(self.tmp, 'worktrees', 'r2')
        os.makedirs(os.path.dirname(blocked), exist_ok=True)
        open(blocked, 'w').close()  # a FILE where the run dir must go
        self.assertFalse(self.mgr.write_run_state('r2', {'version': 1}))


class TestStackedPullRequestBase(unittest.TestCase):
    """A module's request is based on the module below it, and that
    branch is routinely merged AND DELETED before the next one
    publishes — a request against a base that is gone fails outright,
    so the runner always names a way back."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='wt-stack-')
        os.makedirs(os.path.join(self.tmp, 'worktrees', 'r1', 'repo'))
        self.calls: list[list[str]] = []

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mgr(self, *, base_exists: bool, lookup_fails: bool = False):
        def rec(cmd, *, cwd=None, env=None):
            self.calls.append(cmd)
            if 'ls-remote' in cmd:
                if lookup_fails:
                    raise click.ClickException('could not read remote')
                return 'abc123\trefs/heads/x\n' if base_exists else ''
            return 'https://github.com/org/repo/pull/9\n'

        return WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'repos'),
            worktree_root=os.path.join(self.tmp, 'worktrees'),
            default_branch='main',
            run=rec,
        )

    def _publish(self, mgr, **kw):
        return mgr.publish_node(
            'r1', 'refactor', 'https://github.com/org/repo.git',
            title='T', body='B', remote_branch='pipeline/r1-m1', **kw,
        )

    def _pr_base(self) -> str:
        pr = next(c for c in self.calls if c[0] == 'gh')
        return pr[pr.index('--base') + 1]

    def test_a_live_base_is_used(self) -> None:
        self._publish(
            self._mgr(base_exists=True),
            base_branch='pipeline/r1-m0', base_fallback='main',
        )
        self.assertEqual(self._pr_base(), 'pipeline/r1-m0')

    def test_a_deleted_base_falls_back(self) -> None:
        self._publish(
            self._mgr(base_exists=False),
            base_branch='pipeline/r1-m0', base_fallback='main',
        )
        self.assertEqual(self._pr_base(), 'main')

    def test_an_unreadable_remote_falls_back_too(self) -> None:
        # Guessing the base is still there loses the publish; falling
        # back always yields a valid (if noisier) request.
        self._publish(
            self._mgr(base_exists=True, lookup_fails=True),
            base_branch='pipeline/r1-m0', base_fallback='main',
        )
        self.assertEqual(self._pr_base(), 'main')

    def test_the_default_base_is_never_looked_up(self) -> None:
        # The first module bases on the repo's own branch; probing the
        # remote for it would be a pointless round trip.
        self._publish(self._mgr(base_exists=False))
        self.assertFalse([c for c in self.calls if 'ls-remote' in c])
        self.assertEqual(self._pr_base(), 'main')

    def test_a_local_push_never_probes_the_remote(self) -> None:
        # No request is opened, so there is no base to validate.
        self._publish(
            self._mgr(base_exists=False),
            base_branch='pipeline/r1-m0', open_pr=False,
        )
        self.assertFalse([c for c in self.calls if 'ls-remote' in c])


class TestRunArtifact(unittest.TestCase):
    """Durable records that must outlive what produced them — a
    reviewer's report survives the disposal of its own microVM."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='wt-art-')
        self.mgr = WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'repos'),
            worktree_root=os.path.join(self.tmp, 'worktrees'),
            default_branch='main',
            run=lambda cmd, **kw: '',
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _read(self, run_id, relpath):
        with open(
            os.path.join(self.mgr.run_dir(run_id), relpath), encoding='utf-8'
        ) as fh:
            return fh.read()

    def test_nested_path_is_created(self) -> None:
        self.assertTrue(
            self.mgr.write_run_artifact('r1', 'reviews/m1-sec-r1.md', '# hi')
        )
        self.assertEqual(self._read('r1', 'reviews/m1-sec-r1.md'), '# hi')

    def test_it_lives_in_the_run_dir_not_the_repo(self) -> None:
        # Orchestrator record, not a tracked file: no node commits it.
        self.mgr.write_run_artifact('r1', 'reviews/a.md', 'x')
        self.assertFalse(
            os.path.exists(
                os.path.join(self.mgr.run_dir('r1'), 'repo', 'reviews')
            )
        )

    def test_rewrite_leaves_no_temp_file(self) -> None:
        self.mgr.write_run_artifact('r1', 'reviews/a.md', 'one')
        self.mgr.write_run_artifact('r1', 'reviews/a.md', 'two')
        self.assertEqual(self._read('r1', 'reviews/a.md'), 'two')
        left = os.listdir(os.path.join(self.mgr.run_dir('r1'), 'reviews'))
        self.assertEqual(left, ['a.md'])

    def test_an_escaping_path_is_refused(self) -> None:
        # The relpath is composed from pipeline ids; a hostile one must
        # not be able to write outside the run dir.
        for bad in ('../escaped.md', '/etc/x.md', 'a/../../out.md', ''):
            with self.subTest(bad=bad):
                self.assertFalse(self.mgr.write_run_artifact('r1', bad, 'x'))

    def test_an_unwritable_location_fails_quietly(self) -> None:
        # Recording something must never fail a run that is succeeding.
        blocked = os.path.join(self.tmp, 'worktrees', 'r3')
        os.makedirs(os.path.dirname(blocked), exist_ok=True)
        open(blocked, 'w').close()  # a FILE where the run dir must go
        self.assertFalse(self.mgr.write_run_artifact('r3', 'a.md', 'x'))


if __name__ == '__main__':
    unittest.main()
