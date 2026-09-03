"""Tests for recording what a run actually costs on disk."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from sbx_omnigent import disk_metrics as dm


class _Proc:
    def __init__(self, stdout: str = '', returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode


def _du(stdout: str, code: int = 0):
    """A subprocess.run stand-in returning fixed `du` output."""
    def run(argv, **kwargs):
        run.argv = argv
        return _Proc(stdout, code)
    return run


class TestEnabled(unittest.TestCase):
    def test_it_is_off_unless_asked_for(self) -> None:
        # Measuring a 26 GB tree costs minutes; a routine run must not
        # pay for data nobody is going to read.
        self.assertFalse(dm.enabled({}))
        self.assertFalse(dm.enabled({dm.ENABLE_ENV_VAR: ''}))

    def test_any_non_empty_value_switches_it_on(self) -> None:
        for value in ('1', 'true', 'yes'):
            with self.subTest(value=value):
                self.assertTrue(dm.enabled({dm.ENABLE_ENV_VAR: value}))


class TestDirBytes(unittest.TestCase):
    def test_kilobytes_become_bytes(self) -> None:
        run = _du('2048\t/wt/r1/nodes/impl-a\n')
        got = dm.dir_bytes('/wt/r1/nodes/impl-a', run=run)
        self.assertEqual(got, 2048 * 1024)

    def test_it_measures_allocation_on_one_filesystem(self) -> None:
        # -k allocated blocks, not apparent size (a sparse guest disk
        # reads 21 GB apparent against 11 GB real); -x so a bind-mounted
        # guest disk cannot be counted into a host worktree.
        run = _du('1\t/x\n')
        dm.dir_bytes('/x', run=run)
        self.assertEqual(run.argv[:2], ['du', '-skx'])

    def test_a_failed_walk_is_not_a_number(self) -> None:
        self.assertIsNone(dm.dir_bytes('/nope', run=_du('', code=1)))

    def test_unparseable_output_is_not_a_number(self) -> None:
        self.assertIsNone(dm.dir_bytes('/x', run=_du('what\t/x\n')))

    def test_a_hung_or_missing_du_is_not_fatal(self) -> None:
        def hang(argv, **kw):
            raise subprocess.TimeoutExpired('du', 120)

        def boom(argv, **kw):
            raise OSError('no du')

        self.assertIsNone(dm.dir_bytes('/x', run=hang))
        self.assertIsNone(dm.dir_bytes('/x', run=boom))


class TestNodeDirs(unittest.TestCase):
    def test_it_reads_the_filesystem_not_the_state(self) -> None:
        # A crashed run can have state and reality disagree; what is
        # THERE is what costs disk.
        with tempfile.TemporaryDirectory() as tmp:
            nodes = Path(tmp) / 'nodes'
            (nodes / 'impl-a').mkdir(parents=True)
            (nodes / 'impl-b').mkdir()
            (nodes / 'stray.txt').write_text('x', encoding='utf-8')
            self.assertEqual(
                [d.name for d in dm.node_dirs(tmp)], ['impl-a', 'impl-b']
            )

    def test_a_run_with_no_nodes_yet_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(dm.node_dirs(tmp), [])


class TestSample(unittest.TestCase):
    def _run_dir(self, tmp: str, *names: str) -> str:
        for name in names:
            (Path(tmp) / 'nodes' / name).mkdir(parents=True)
        return tmp

    def test_every_node_is_measured_and_labelled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._run_dir(tmp, 'impl-a', 'pick')
            got = dm.sample(
                run_id='r1', event='chunk-peak', run_dir=tmp,
                kinds={'impl-a': 'writer', 'pick': 'judge'},
                measure=lambda p, **kw: 1_000,
                now=1.0,
            )
        trees = {r['node']: r for r in got if r['what'] == 'worktree'}
        self.assertEqual(trees['impl-a']['kind'], 'writer')
        self.assertEqual(trees['pick']['kind'], 'judge')
        self.assertEqual(trees['impl-a']['bytes'], 1_000)

    def test_the_event_is_stamped_because_the_series_needs_it(self) -> None:
        # A tally without a boundary label cannot show a PEAK, which is
        # the only thing the preflight is trying to predict.
        with tempfile.TemporaryDirectory() as tmp:
            self._run_dir(tmp, 'impl-a')
            got = dm.sample(
                run_id='r1', event='stage-complete:impl-a', run_dir=tmp,
                chunk='m6', measure=lambda p, **kw: 5, now=1.0,
            )
        self.assertEqual(got[0]['event'], 'stage-complete:impl-a')
        self.assertEqual(got[0]['chunk'], 'm6')
        self.assertEqual(got[0]['run'], 'r1')

    def test_the_guest_store_rides_along(self) -> None:
        # The same run answers whether per_vm_gb is right.
        with tempfile.TemporaryDirectory() as tmp:
            got = dm.sample(
                run_id='r1', event='e', run_dir=tmp,
                store_layers=(6, 30_000_000_000), now=1.0,
            )
        store = [r for r in got if r['what'] == 'sbx-store']
        self.assertEqual(store[0]['layers'], 6)
        self.assertEqual(store[0]['bytes'], 30_000_000_000)

    def test_host_free_space_is_recorded_for_comparison(self) -> None:
        class _U:
            free = 288_000_000_000

        with tempfile.TemporaryDirectory() as tmp:
            got = dm.sample(
                run_id='r1', event='e', run_dir=tmp, free_path='/',
                usage=lambda _p: _U(), now=1.0,
            )
        free = [r for r in got if r['what'] == 'host-free']
        self.assertEqual(free[0]['bytes'], 288_000_000_000)

    def test_a_node_that_cannot_be_measured_is_omitted_not_zeroed(self):
        # A zero would read as "this tree costs nothing", which is a
        # wrong answer rather than a missing one.
        with tempfile.TemporaryDirectory() as tmp:
            self._run_dir(tmp, 'impl-a')
            got = dm.sample(
                run_id='r1', event='e', run_dir=tmp,
                measure=lambda p, **kw: None, now=1.0,
            )
        self.assertEqual([r for r in got if r['what'] == 'worktree'], [])


class TestAppend(unittest.TestCase):
    def test_records_land_as_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / '_metrics' / 'r1.jsonl'
            self.assertTrue(dm.append(path, [{'a': 1}, {'a': 2}]))
            rows = [json.loads(x) for x in path.read_text().splitlines()]
            self.assertEqual([r['a'] for r in rows], [1, 2])

    def test_a_later_sample_appends_rather_than_truncates(self) -> None:
        # A killed run must keep every sample it already took — that is
        # the run whose disk behaviour is most worth reading.
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'r1.jsonl'
            dm.append(path, [{'a': 1}])
            dm.append(path, [{'a': 2}])
            self.assertEqual(len(path.read_text().splitlines()), 2)

    def test_nothing_to_write_writes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'r1.jsonl'
            self.assertFalse(dm.append(path, []))
            self.assertFalse(path.exists())

    def test_an_unwritable_path_is_not_fatal(self) -> None:
        # Instrumentation that can fail a run is worse than none.
        self.assertFalse(dm.append('/proc/nope/r1.jsonl', [{'a': 1}]))


if __name__ == '__main__':
    unittest.main()
