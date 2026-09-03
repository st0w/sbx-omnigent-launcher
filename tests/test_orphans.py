"""Tests for detecting guest disk images sbx has leaked."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from sbx_omnigent import orphans


def _store(tmp: Path, layers: int, blocks: int = 2048) -> Path:
    """A snapshots dir holding *layers* sparse rwlayer.img files."""
    root = tmp / 'snapshots'
    root.mkdir(parents=True, exist_ok=True)
    for i in range(layers):
        snap = root / str(i)
        snap.mkdir()
        (snap / 'rwlayer.img').write_bytes(b'\0' * (blocks * 512))
    return root


class _Proc:
    def __init__(self, stdout: str = '', returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode


def _sbx(stdout: str, code: int = 0):
    def run(argv, **kwargs):
        return _Proc(stdout, code)
    return run


class TestSnapshotRoot(unittest.TestCase):
    def test_the_env_override_wins(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                orphans.snapshot_root(env={orphans.ROOT_ENV_VAR: tmp}),
                Path(tmp),
            )

    def test_a_bad_override_is_not_used(self) -> None:
        self.assertIsNone(
            orphans.snapshot_root(env={orphans.ROOT_ENV_VAR: '/nope/x'})
        )

    def test_no_candidate_means_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(
                orphans.snapshot_root(home=Path(tmp), env={})
            )


class TestLayerBytes(unittest.TestCase):
    def test_it_counts_layers_and_allocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _store(Path(tmp), 3)
            count, total = orphans.layer_bytes(root)
            self.assertEqual(count, 3)
            self.assertGreater(total, 0)

    def test_a_missing_store_is_zero_not_an_error(self) -> None:
        self.assertEqual(orphans.layer_bytes(Path('/nope/x')), (0, 0))


class TestLiveSandboxes(unittest.TestCase):
    def test_the_header_row_is_not_a_sandbox(self) -> None:
        out = 'SANDBOX  AGENT  STATUS\nagy-auth-trusted  shell  stopped\n'
        self.assertEqual(orphans.live_sandboxes(run=_sbx(out)), 1)

    def test_an_empty_listing_is_zero(self) -> None:
        self.assertEqual(orphans.live_sandboxes(run=_sbx('SANDBOX\n')), 0)

    def test_sbx_failing_is_UNKNOWN_not_zero(self) -> None:
        # Zero would make every layer look orphaned and produce a
        # confident, wrong number in a refusal message.
        self.assertIsNone(orphans.live_sandboxes(run=_sbx('', code=1)))

    def test_sbx_missing_or_hanging_is_unknown(self) -> None:
        def boom(argv, **kw):
            raise OSError('no sbx')

        def hang(argv, **kw):
            raise subprocess.TimeoutExpired('sbx', 20)

        self.assertIsNone(orphans.live_sandboxes(run=boom))
        self.assertIsNone(orphans.live_sandboxes(run=hang))


class TestOrphanAdvice(unittest.TestCase):
    def test_more_layers_than_sandboxes_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _store(Path(tmp), 3)
            said = orphans.orphan_advice(
                root, run=_sbx('SANDBOX\nagy-auth-trusted\n')
            )
            self.assertIsNotNone(said)
            self.assertIn('2 leaked', said)
            self.assertIn('sbx daemon stop', said)

    def test_it_warns_against_deleting_by_hand(self) -> None:
        # Removing a snapshot dir leaves a dangling metadata.db row —
        # corruption, not cleanup.
        with tempfile.TemporaryDirectory() as tmp:
            said = orphans.orphan_advice(
                _store(Path(tmp), 2), run=_sbx('SANDBOX\n')
            ) or ''
            self.assertIn('Never delete', said)

    def test_a_matched_store_says_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _store(Path(tmp), 2)
            self.assertIsNone(
                orphans.orphan_advice(root, run=_sbx('SANDBOX\na\nb\n'))
            )

    def test_an_unknown_sandbox_count_says_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = _store(Path(tmp), 3)
            self.assertIsNone(
                orphans.orphan_advice(root, run=_sbx('', code=1))
            )

    def test_an_empty_store_says_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'snapshots'
            root.mkdir()
            self.assertIsNone(
                orphans.orphan_advice(root, run=_sbx('SANDBOX\n'))
            )


if __name__ == '__main__':
    unittest.main()
