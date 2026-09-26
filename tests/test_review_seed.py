"""A reviewer's warm build cache (#37): scratch, sentinel and launcher.

Reviewers mount the tree under review read-only, and their read-write
primary used to be an empty temp directory, so every reviewer turn
compiled the workspace from clean. Each reviewer now gets its own
scratch directory beside the round's snapshot, cloned from the build
cache, and the launcher mounts it as that reviewer's primary.

Run: .venv/bin/python -m unittest tests.test_review_seed
"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from unittest import mock

import click

from sbx_omnigent.launcher import _MOUNT_SENTINEL_PREFIX, SbxLauncher
from sbx_omnigent.swarm import mount_sentinel
from sbx_omnigent.worktrees import WorktreeManager

_TOKEN = '0123456789ab'


class _Root(unittest.TestCase):
    """A worktree root holding one run's nodes dir and a snapshot."""

    def setUp(self) -> None:
        self.tmp = os.path.realpath(tempfile.mkdtemp(prefix='seed-'))
        self.root = os.path.join(self.tmp, 'wt')
        self.nodes = os.path.join(self.root, 'run1', 'nodes')
        self.snapshot = os.path.join(self.nodes, 'review-r1')
        os.makedirs(self.snapshot)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestTheScratchIsSeeded(_Root):
    def setUp(self) -> None:
        super().setUp()
        self.mgr = WorktreeManager(
            canonical_root=os.path.join(self.tmp, 'can'),
            worktree_root=self.root,
            build_cache=('target',),
            build_cache_key='proj',
        )
        cached = os.path.join(self.tmp, 'can', '_buildcache', 'proj', 'target')
        os.makedirs(cached)
        with open(os.path.join(cached, 'artifact'), 'w') as fh:
            fh.write('compiled')

    def _seed(self, token: str = _TOKEN) -> str:
        return self.mgr.seed_review_scratch('run1', self.snapshot, token)

    def test_it_sits_beside_the_snapshot(self) -> None:
        self.assertEqual(self._seed(), f'{self.snapshot}.seed-{_TOKEN}')

    def test_it_holds_a_copy_of_the_cache(self) -> None:
        path = self._seed()
        with open(os.path.join(path, 'target', 'artifact')) as fh:
            self.assertEqual(fh.read(), 'compiled')

    def test_each_seeding_starts_fresh(self) -> None:
        # A retry must never inherit what the last reviewer VM wrote.
        path = self._seed()
        with open(os.path.join(path, 'target', 'artifact'), 'w') as fh:
            fh.write('written by a reviewer')
        with open(os.path.join(path, 'stray'), 'w') as fh:
            fh.write('x')
        self._seed()
        with open(os.path.join(path, 'target', 'artifact')) as fh:
            self.assertEqual(fh.read(), 'compiled')
        self.assertFalse(os.path.exists(os.path.join(path, 'stray')))

    def test_a_planted_symlink_is_replaced_not_followed(self) -> None:
        # Seeding through a link would write the cache into, or clear,
        # whatever it points at.
        victim = os.path.join(self.nodes, 'build')
        os.makedirs(victim)
        with open(os.path.join(victim, 'keep'), 'w') as fh:
            fh.write('x')
        os.symlink(victim, f'{self.snapshot}.seed-{_TOKEN}')
        path = self._seed()
        self.assertFalse(os.path.islink(path))
        self.assertEqual(os.listdir(victim), ['keep'])

    def test_no_cache_yet_gives_an_empty_scratch(self) -> None:
        shutil.rmtree(os.path.join(self.tmp, 'can'))
        path = self._seed()
        self.assertEqual(os.listdir(path), [])

    def test_a_bad_token_is_refused(self) -> None:
        for token in ('', 'ABCDEF012345', '0123456789a', '../../escape',
                      '0123456789abc', '01234567-9ab'):
            with self.subTest(token=token):
                with self.assertRaises(click.ClickException):
                    self._seed(token)

    def test_a_path_outside_the_runs_nodes_dir_is_refused(self) -> None:
        elsewhere = os.path.join(self.tmp, 'elsewhere')
        os.makedirs(elsewhere)
        run_dir = os.path.join(self.root, 'run1')
        for mounted in (elsewhere, self.nodes, run_dir):
            with self.subTest(mounted=mounted):
                with self.assertRaises(click.ClickException):
                    self.mgr.seed_review_scratch('run1', mounted, _TOKEN)

    def test_the_rounds_seeds_are_disposed(self) -> None:
        first = self._seed()
        second = self._seed('ba9876543210')
        # Another round whose label shares a prefix is left alone.
        other_snapshot = os.path.join(self.nodes, 'review-r10')
        os.makedirs(other_snapshot)
        other = self.mgr.seed_review_scratch('run1', other_snapshot, _TOKEN)
        self.assertEqual(self.mgr.dispose_review_seeds('run1', 'review-r1'), 2)
        self.assertFalse(os.path.exists(first))
        self.assertFalse(os.path.exists(second))
        self.assertTrue(os.path.exists(other))
        self.assertTrue(os.path.exists(self.snapshot))


class TestTheSentinelCarriesTheToken(unittest.TestCase):
    def test_a_seeded_reviewer(self) -> None:
        self.assertEqual(
            mount_sentinel('/w/run1/nodes/review-r1', 'ro', seed=_TOKEN),
            f'git@sbxmount:/w/run1/nodes/review-r1#ro.{_TOKEN}',
        )

    def test_with_a_credential(self) -> None:
        self.assertEqual(
            mount_sentinel('/w/x', 'ro', credential='codex', seed=_TOKEN),
            f'git@sbxmount:/w/x#ro.{_TOKEN}-codex',
        )

    def test_no_seed_is_unchanged(self) -> None:
        self.assertEqual(mount_sentinel('/w/x', 'ro'), 'git@sbxmount:/w/x#ro')

    def test_a_writer_cannot_carry_one(self) -> None:
        with self.assertRaises(ValueError):
            mount_sentinel('/w/x', 'rw', seed=_TOKEN)

    def test_a_bad_token_is_refused(self) -> None:
        for token in ('', 'XYZ', '0123456789AB', '0123456789a/'):
            with self.subTest(token=token):
                with self.assertRaises(ValueError):
                    mount_sentinel('/w/x', 'ro', seed=token)


class TestTheLauncherMountsTheSeed(_Root):
    def _workspaces(self, fragment: str) -> list[str]:
        launcher = SbxLauncher(worktree_root=self.root)
        with (
            mock.patch.object(launcher, '_create_sandbox') as create,
            mock.patch.object(launcher, '_make_scratch', return_value='/tmpd'),
            mock.patch.object(launcher, '_apply_egress'),
            mock.patch.object(launcher, '_seed_claude_settings'),
            mock.patch.object(launcher, '_inject_codex_credentials'),
            mock.patch.object(launcher, '_launch_host'),
        ):
            launcher.start_host(
                'box', token='t', host_id='h', host_name='n',
                server_url='http://host.docker.internal:6767',
                repo_url=f'{_MOUNT_SENTINEL_PREFIX}{self.snapshot}',
                repo_branch=fragment,
            )
        return list(create.call_args.args[1])

    def _make_seed(self) -> str:
        seed = f'{self.snapshot}.seed-{_TOKEN}'
        os.makedirs(seed)
        return seed

    def test_the_seed_is_the_reviewers_primary(self) -> None:
        seed = self._make_seed()
        self.assertEqual(
            self._workspaces(f'ro.{_TOKEN}'), [seed, f'{self.snapshot}:ro']
        )

    def test_with_a_credential_too(self) -> None:
        seed = self._make_seed()
        self.assertEqual(
            self._workspaces(f'ro.{_TOKEN}-codex')[0], seed
        )

    def test_no_seed_keeps_the_empty_scratch(self) -> None:
        self.assertEqual(
            self._workspaces('ro'), ['/tmpd', f'{self.snapshot}:ro']
        )

    def test_a_missing_seed_is_refused(self) -> None:
        with self.assertRaises(click.ClickException):
            self._workspaces(f'ro.{_TOKEN}')

    def test_a_seed_that_is_a_symlink_is_refused(self) -> None:
        outside = os.path.join(self.tmp, 'outside')
        os.makedirs(outside)
        os.symlink(outside, f'{self.snapshot}.seed-{_TOKEN}')
        with self.assertRaises(click.ClickException):
            self._workspaces(f'ro.{_TOKEN}')

    def test_a_seed_linked_to_a_writers_clone_is_refused(self) -> None:
        # Inside the root, so only the exact-path check stands between
        # a reviewer and a writer's tree mounted read-write.
        writer = os.path.join(self.nodes, 'build')
        os.makedirs(writer)
        os.symlink(writer, f'{self.snapshot}.seed-{_TOKEN}')
        with self.assertRaises(click.ClickException):
            self._workspaces(f'ro.{_TOKEN}')

    def test_a_bad_token_is_refused(self) -> None:
        for fragment in (
            'ro.xyz', 'ro.0123456789ab0', 'ro.', 'ro.0123456789a',
        ):
            # The directory it names exists, so only the token check
            # stands in the way.
            os.makedirs(f'{self.snapshot}.seed-{fragment[3:]}')
            with self.subTest(fragment=fragment):
                with self.assertRaises(click.ClickException):
                    self._workspaces(fragment)

    def test_a_writer_with_a_token_is_refused(self) -> None:
        self._make_seed()
        with self.assertRaises(click.ClickException):
            self._workspaces(f'rw.{_TOKEN}')


if __name__ == '__main__':
    unittest.main()
