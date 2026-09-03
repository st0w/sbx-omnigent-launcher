"""Real-git tests for the branch-as-artifact pipeline worktrees.

These exercise WorktreeManager's pipeline methods against actual git
repos (isolated per-writer worktrees, branch inheritance, judge
comparison trees, merge, publish). Run:

    .venv/bin/python -m unittest tests.test_worktrees_pipeline
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import click

from sbx_omnigent.worktrees import WorktreeManager


def _git(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ['git', '-C', str(cwd), *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout


def _init_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ['git', 'init', '-b', 'main', str(path)],
        check=True,
        capture_output=True,
    )
    _git(path, 'config', 'user.email', 't@t')
    _git(path, 'config', 'user.name', 't')
    (path / 'README.md').write_text('base\n', encoding='utf-8')
    _git(path, 'add', '-A')
    _git(path, 'commit', '-m', 'init')


def _files(path: str | Path) -> list[str]:
    return sorted(
        p.name for p in Path(path).iterdir() if p.name != '.git'
    )


def _head(path: str) -> str:
    return _git(Path(path), 'rev-parse', '--abbrev-ref', 'HEAD').strip()


class TestPipelineWorktrees(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix='wt-pl-'))
        self.src = self.tmp / 'src'
        _init_repo(self.src)
        self.mgr = WorktreeManager(
            canonical_root=str(self.tmp / 'canon'),
            worktree_root=str(self.tmp / 'wt'),
            default_branch='main',
        )
        self.repo = str(self.src)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_create_node_from_base(self) -> None:
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        self.assertTrue(Path(wt).is_dir())
        self.assertIn('README.md', _files(wt))
        self.assertEqual(_head(wt), 'pl/run1/build')

    def test_commit_node_clean_then_dirty(self) -> None:
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        self.assertFalse(
            self.mgr.commit_node('run1', 'build', message='m')
        )  # clean tree
        (Path(wt) / 'new.py').write_text('x', encoding='utf-8')
        made = self.mgr.commit_node(
            'run1', 'build', message='add', author='Coder <c@x>'
        )
        self.assertTrue(made)
        log = _git(Path(wt), 'log', '--format=%an|%s', '-1')
        self.assertEqual(log.strip(), 'Coder|add')

    def test_an_agents_own_commit_still_reaches_the_hub(self) -> None:
        # THE bug. Agents run `git commit` inside their own VM. When one
        # has already committed everything, the tree is clean here — and
        # the old early return skipped the push, so the work stayed in
        # the node's clone and the hub never saw it. Everything
        # downstream reads the HUB, so the branch looked untouched: the
        # no-op guard reported "changed 0 file(s)" on a real refactor
        # and failed the run three times over.
        self.mgr.create_run('run1', self.repo)
        wt = Path(self.mgr.create_node_worktree('run1', 'build'))
        seed = self.mgr.hub_branch_tip('run1', 'build')

        (wt / 'impl.py').write_text('real work\n', encoding='utf-8')
        _git(wt, 'add', '-A')
        _git(wt, '-c', 'user.name=agent', '-c', 'user.email=a@x',
             'commit', '-m', 'agent committed this itself')

        # Nothing left for the orchestrator to commit...
        self.assertEqual(_git(wt, 'status', '--porcelain').strip(), '')
        # ...but the hub must still end up with the work.
        self.assertTrue(self.mgr.commit_node('run1', 'build', message='m'))
        self.assertNotEqual(self.mgr.hub_branch_tip('run1', 'build'), seed)
        self.assertIn(
            'impl.py',
            self.mgr.node_diff_files('run1', 'build', against='main'),
        )

    def test_a_genuinely_untouched_branch_still_reads_as_untouched(self):
        # The guard must keep catching a writer that really did nothing;
        # pushing unconditionally must not manufacture a diff.
        self.mgr.create_run('run1', self.repo)
        self.mgr.create_node_worktree('run1', 'build')
        seed = self.mgr.hub_branch_tip('run1', 'build')
        self.assertFalse(self.mgr.commit_node('run1', 'build', message='m'))
        self.assertEqual(self.mgr.hub_branch_tip('run1', 'build'), seed)
        self.assertEqual(
            self.mgr.node_diff_files('run1', 'build', against='main'), []
        )

    def test_the_hub_tip_is_reported_not_the_clones(self) -> None:
        # The two answers differ exactly when work has not been pushed —
        # which is the state this whole bug lived in.
        self.mgr.create_run('run1', self.repo)
        wt = Path(self.mgr.create_node_worktree('run1', 'build'))
        (wt / 'x.py').write_text('x\n', encoding='utf-8')
        _git(wt, 'add', '-A')
        _git(wt, '-c', 'user.name=a', '-c', 'user.email=a@x',
             'commit', '-m', 'unpushed')
        local = _git(wt, 'rev-parse', 'HEAD').strip()
        self.assertNotEqual(self.mgr.hub_branch_tip('run1', 'build'), local)
        self.mgr.commit_node('run1', 'build', message='m')
        self.assertEqual(self.mgr.hub_branch_tip('run1', 'build'), local)

    def test_an_unknown_branch_has_no_hub_tip(self) -> None:
        self.mgr.create_run('run1', self.repo)
        self.assertIsNone(self.mgr.hub_branch_tip('run1', 'never-ran'))

    def test_write_ignored_file_hidden_from_git_and_commit(self) -> None:
        # A staged agy task file is readable by the agent but invisible
        # to git: absent from `git status` (so the settle-wait never
        # mistakes it for the agent's output) and never committed (so it
        # never lands on the branch or gets published).
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        self.mgr.write_ignored_file(wt, 'OMNI_TASK.md', 'do it\nline2\n')
        self.assertEqual(
            (Path(wt) / 'OMNI_TASK.md').read_text(encoding='utf-8'),
            'do it\nline2\n',
        )
        # git ignores it entirely: clean status, and a no-op commit.
        self.assertEqual(
            _git(Path(wt), 'status', '--porcelain').strip(), ''
        )
        self.assertFalse(self.mgr.commit_node('run1', 'build', message='m'))
        # a real edit still commits — without the task file.
        (Path(wt) / 'net.py').write_text('code\n', encoding='utf-8')
        self.assertTrue(self.mgr.commit_node('run1', 'build', message='impl'))
        tracked = _git(Path(wt), 'ls-files').split()
        self.assertIn('net.py', tracked)
        self.assertNotIn('OMNI_TASK.md', tracked)

    def test_write_ignored_file_idempotent_and_overwrites(self) -> None:
        # Re-staging updates the content but never duplicates the ignore
        # entry (a session can be driven more than once).
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        self.mgr.write_ignored_file(wt, 'OMNI_TASK.md', 'first')
        self.mgr.write_ignored_file(wt, 'OMNI_TASK.md', 'second')
        exclude = (
            Path(wt) / '.git' / 'info' / 'exclude'
        ).read_text(encoding='utf-8')
        self.assertEqual(exclude.count('OMNI_TASK.md'), 1)
        self.assertEqual(
            (Path(wt) / 'OMNI_TASK.md').read_text(encoding='utf-8'), 'second'
        )

    def test_write_ignored_file_rejects_non_bare_name(self) -> None:
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        for bad in ('../evil', 'sub/dir', '', '..'):
            with self.assertRaises(click.ClickException):
                self.mgr.write_ignored_file(wt, bad, 'x')

    def test_inheritance_from_upstream(self) -> None:
        self.mgr.create_run('run1', self.repo)
        tdd = self.mgr.create_node_worktree('run1', 'tests')
        (Path(tdd) / 'test_x.py').write_text('t\n', encoding='utf-8')
        self.mgr.commit_node('run1', 'tests', message='tests')
        build = self.mgr.create_node_worktree(
            'run1', 'build', from_node='tests'
        )
        # The downstream writer inherits the upstream node's work.
        self.assertIn('test_x.py', _files(build))
        self.assertEqual(_head(build), 'pl/run1/build')

    def test_reseed_picks_up_upstream_committed_after_prewarm(self) -> None:
        # Pre-warm order: a downstream writer's worktree is created (VM
        # boots) while its upstream is still at base; the upstream
        # commits later. reseed moves the downstream onto the upstream's
        # real tip so the early-booted VM sees its files before driving.
        self.mgr.create_run('run1', self.repo)
        tests = self.mgr.create_node_worktree('run1', 'tests')
        impl = self.mgr.create_node_worktree(
            'run1', 'impl-a', from_node='tests'
        )
        # impl seeded from tests@base — no test file yet.
        self.assertNotIn('test_x.py', _files(impl))
        # tests does its work and commits (its branch advances on hub).
        (Path(tests) / 'test_x.py').write_text('t\n', encoding='utf-8')
        self.mgr.commit_node('run1', 'tests', message='tests')
        # reseed impl onto the now-committed tests tip.
        self.mgr.reseed_node_worktree('run1', 'impl-a', 'tests')
        self.assertIn('test_x.py', _files(impl))
        self.assertEqual(_head(impl), 'pl/run1/impl-a')  # still its branch
        # impl's own commit now sits exactly 1 above tests (ff).
        (Path(impl) / 'net.py').write_text('code\n', encoding='utf-8')
        self.mgr.commit_node('run1', 'impl-a', message='impl')
        ahead = _git(
            Path(impl), 'rev-list', '--count', 'origin/pl/run1/tests..HEAD'
        ).strip()
        self.assertEqual(ahead, '1')

    def test_reseed_missing_worktree_raises(self) -> None:
        self.mgr.create_run('run1', self.repo)
        with self.assertRaises(click.ClickException):
            self.mgr.reseed_node_worktree('run1', 'nope', 'tests')

    def test_write_tracked_file_is_committed_at_nested_path(self) -> None:
        # A tracked plan artifact is written at a nested repo-relative
        # path (dirs created) and committed onto the node's branch.
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        self.mgr.write_tracked_file(
            wt, 'docs/plans/tdd-race.md', '# Plan\nbody\n'
        )
        self.assertEqual(
            (Path(wt) / 'docs' / 'plans' / 'tdd-race.md').read_text(
                encoding='utf-8'
            ),
            '# Plan\nbody\n',
        )
        self.assertTrue(
            self.mgr.commit_node('run1', 'build', message='docs: plan')
        )
        tracked = _git(Path(wt), 'ls-files').split()
        self.assertIn('docs/plans/tdd-race.md', tracked)

    def test_write_tracked_file_rejects_unsafe_path(self) -> None:
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        for bad in ('/etc/passwd', '../escape.md', 'docs/../../x', ''):
            with self.assertRaises(click.ClickException):
                self.mgr.write_tracked_file(wt, bad, 'x')

    def test_create_issue_argv_and_label_fallback(self) -> None:
        # A label the repo does not have makes `gh` refuse the WHOLE
        # call, which would lose the finding. Retry once without it.
        calls: list[list[str]] = []

        def run_publish(cmd):
            calls.append(cmd)
            if '--label' in cmd:
                raise click.ClickException('could not add label: finding')
            return 'https://github.com/org/proj/issues/7\n'

        self.mgr._run_publish = run_publish
        url = self.mgr.create_issue(
            'https://github.com/org/proj', title='t', body='b',
            label='finding',
        )
        self.assertEqual(url, 'https://github.com/org/proj/issues/7')
        self.assertEqual(len(calls), 2, 'it did not retry without label')
        self.assertIn('--label', calls[0])
        self.assertNotIn('--label', calls[1])
        self.assertEqual(calls[1][:5],
                         ['gh', 'issue', 'create', '-R', 'org/proj'])

    def test_issue_listing_includes_closed_and_reports_failure(
        self,
    ) -> None:
        # state=all matters: re-filing something a human already closed
        # is the one behaviour that would make the tracker worthless.
        seen: list[list[str]] = []
        self.mgr._run_publish = lambda cmd: (seen.append(cmd), 'body')[1]
        self.assertEqual(
            self.mgr.issue_bodies_text('https://github.com/org/proj'),
            'body',
        )
        self.assertIn('--state', seen[0])
        self.assertIn('all', seen[0])

        def boom(cmd):
            raise click.ClickException('offline')

        self.mgr._run_publish = boom
        # None, not '' — unreadable must not read as "no issues".
        self.assertIsNone(
            self.mgr.issue_bodies_text('https://github.com/org/proj')
        )

    def test_read_tracked_file_round_trips(self) -> None:
        # The findings ledger is append-only and human-edited, so the
        # runner has to read what is already on the branch rather than
        # regenerate it.
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        self.mgr.write_tracked_file(wt, 'docs/plans/x-findings.md', 'rows\n')
        self.assertEqual(
            self.mgr.read_tracked_file(wt, 'docs/plans/x-findings.md'),
            'rows\n',
        )

    def test_read_tracked_file_is_none_when_absent(self) -> None:
        # A first run has no ledger yet. That is the normal case, not an
        # error, and it must not fail a publish.
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        self.assertIsNone(
            self.mgr.read_tracked_file(wt, 'docs/plans/never-written.md')
        )

    def test_read_tracked_file_rejects_unsafe_path(self) -> None:
        # Same guard as the write side: a hostile pipeline value
        # must not be able to read arbitrary host files either.
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        for bad in ('/etc/passwd', '../escape.md', 'docs/../../x', ''):
            with self.subTest(path=bad), self.assertRaises(
                click.ClickException
            ):
                self.mgr.read_tracked_file(wt, bad)

    def test_isolated_writers_do_not_share_tree(self) -> None:
        self.mgr.create_run('run1', self.repo)
        a = self.mgr.create_node_worktree('run1', 'impl-a')
        b = self.mgr.create_node_worktree('run1', 'impl-b')
        (Path(a) / 'a.py').write_text('a', encoding='utf-8')
        self.mgr.commit_node('run1', 'impl-a', message='a')
        # b never saw a's uncommitted or committed file on its own tree.
        self.assertNotIn('a.py', _files(b))

    def test_judge_worktree(self) -> None:
        self.mgr.create_run('run1', self.repo)
        a = self.mgr.create_node_worktree('run1', 'impl-a')
        (Path(a) / 'a.py').write_text('a', encoding='utf-8')
        self.mgr.commit_node('run1', 'impl-a', message='a')
        b = self.mgr.create_node_worktree('run1', 'impl-b')
        (Path(b) / 'b.py').write_text('b', encoding='utf-8')
        self.mgr.commit_node('run1', 'impl-b', message='b')
        judge = self.mgr.create_judge_worktree(
            'run1', 'pick', ['impl-a', 'impl-b']
        )
        self.assertIn('a.py', _files(Path(judge) / 'impl-a'))
        self.assertIn('b.py', _files(Path(judge) / 'impl-b'))

    def test_alias_node_branch_lets_writer_seed_from_judge(self) -> None:
        # A judge publishes no branch of its own; aliasing its node
        # branch to the selected winner lets a downstream writer cut its
        # worktree from the judge node.
        self.mgr.create_run('run1', self.repo)
        a = self.mgr.create_node_worktree('run1', 'impl-a')
        (Path(a) / 'net.py').write_text('winner\n', encoding='utf-8')
        self.mgr.commit_node('run1', 'impl-a', message='impl')
        alias = self.mgr.alias_node_branch('run1', 'pick', 'impl-a')
        self.assertEqual(alias, 'pl/run1/pick')
        rf = self.mgr.create_node_worktree(
            'run1', 'refactor', from_node='pick'
        )
        self.assertEqual(
            (Path(rf) / 'net.py').read_text(encoding='utf-8'), 'winner\n'
        )
        self.assertEqual(_head(rf), 'pl/run1/refactor')

    def test_alias_missing_run_raises(self) -> None:
        with self.assertRaises(click.ClickException):
            self.mgr.alias_node_branch('nope', 'pick', 'impl-a')

    def test_merge_node_into(self) -> None:
        self.mgr.create_run('run1', self.repo)
        a = self.mgr.create_node_worktree('run1', 'a')
        (Path(a) / 'a.py').write_text('a', encoding='utf-8')
        self.mgr.commit_node('run1', 'a', message='a')
        b = self.mgr.create_node_worktree('run1', 'b')
        (Path(b) / 'b.py').write_text('b', encoding='utf-8')
        self.mgr.commit_node('run1', 'b', message='b')
        self.mgr.merge_node_into(
            'run1', source_node='a', target_node='b', message='merge a'
        )
        self.assertIn('a.py', _files(b))
        self.assertIn('b.py', _files(b))

    def test_publish_node_local(self) -> None:
        remote = self.tmp / 'remote.git'
        subprocess.run(
            ['git', 'init', '--bare', '-b', 'main', str(remote)],
            check=True,
            capture_output=True,
        )
        self.mgr.create_run('run1', self.repo)
        wt = self.mgr.create_node_worktree('run1', 'build')
        (Path(wt) / 'n.py').write_text('n', encoding='utf-8')
        self.mgr.commit_node('run1', 'build', message='n')
        out = self.mgr.publish_node(
            'run1', 'build', str(remote), title='t', body='b', open_pr=False
        )
        self.assertIn('pipeline/run1', out)
        branches = _git(remote, 'branch', '--list')
        self.assertIn('pipeline/run1', branches)

    def test_dispose_run(self) -> None:
        self.mgr.create_run('run1', self.repo)
        self.mgr.create_node_worktree('run1', 'build')
        rdir = self.mgr.run_dir('run1')
        self.assertTrue(Path(rdir).is_dir())
        self.mgr.dispose_run('run1')
        self.assertFalse(Path(rdir).exists())

    def test_invalid_run_id_rejected(self) -> None:
        with self.assertRaises(click.ClickException):
            self.mgr.create_run('bad id!', self.repo)

    def test_node_without_run_errors(self) -> None:
        with self.assertRaises(click.ClickException):
            self.mgr.create_node_worktree('nope', 'build')


if __name__ == '__main__':
    unittest.main()


class TestRetainingLosingBranches(unittest.TestCase):
    """Real-git checks for the archive of a non-selected implementation.

    Only the winner publishes, so the loser's branch lives on the run
    hub and dies with it at teardown. These exercise the actual git
    bundle, not a fake (TASKS.md #32).
    """

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix='wt-keep-'))
        self.src = self.tmp / 'src'
        _init_repo(self.src)
        self.mgr = WorktreeManager(
            canonical_root=str(self.tmp / 'canon'),
            worktree_root=str(self.tmp / 'wt'),
            default_branch='main',
        )
        self.mgr.create_run('run1', str(self.src))

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _writer(self, node: str, filename: str) -> None:
        wt = Path(self.mgr.create_node_worktree('run1', node))
        (wt / filename).write_text('work\n', encoding='utf-8')
        self.mgr.commit_node(
            'run1', node, message=f'{node}: implement',
            author=f'{node} <{node}@pipeline.local>',
        )

    def test_a_losing_branch_is_bundled(self) -> None:
        self._writer('impl-a', 'a.py')
        path = self.mgr.retain_node_bundle('run1', 'impl-a', against='main')
        self.assertIsNotNone(path)
        self.assertTrue(Path(path).is_file())

    def test_the_bundle_lives_outside_the_worktree_root(self) -> None:
        # Everything the launcher deletes goes through
        # _remove_under_root, which refuses anything outside
        # worktree_root — an archive inside it could be swept away.
        self._writer('impl-a', 'a.py')
        path = self.mgr.retain_node_bundle('run1', 'impl-a', against='main')
        root = str((self.tmp / 'wt').resolve())
        self.assertFalse(str(Path(path).resolve()).startswith(root + os.sep))

    def test_the_bundle_survives_disposing_the_run(self) -> None:
        self._writer('impl-a', 'a.py')
        path = self.mgr.retain_node_bundle('run1', 'impl-a', against='main')
        self.mgr.dispose_run('run1')
        self.assertTrue(Path(path).is_file())

    def test_the_bundle_restores_the_losing_work(self) -> None:
        # The whole point: a diff against the winner, later, from this.
        self._writer('impl-a', 'a.py')
        path = self.mgr.retain_node_bundle('run1', 'impl-a', against='main')
        dst = self.tmp / 'restored'
        _git(self.tmp, 'clone', '-q', str(self.src), str(dst))
        _git(dst, 'fetch', path, 'refs/heads/pl/run1/impl-a:refs/heads/loser')
        shown = _git(dst, 'show', '--name-only', '--format=', 'loser')
        self.assertIn('a.py', shown)

    def test_a_branch_with_nothing_new_is_not_archived(self) -> None:
        # git refuses an empty bundle; a writer that committed nothing
        # beyond the base does not deserve a file.
        self.mgr.create_node_worktree('run1', 'idle')
        self.assertIsNone(
            self.mgr.retain_node_bundle('run1', 'idle', against='main')
        )

    def test_a_missing_branch_raises_rather_than_lying(self) -> None:
        with self.assertRaises(click.ClickException):
            self.mgr.retain_node_bundle('run1', 'ghost', against='main')
