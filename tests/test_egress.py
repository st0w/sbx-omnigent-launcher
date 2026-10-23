"""The server's published egress allowlist (#41).

The pipeline runner never saw the server's config, so the verify gate
always got the built-in default allowlist, whatever the operator had
set, including ``[]``. The server now publishes its resolved allowlist
beside the worktrees, and the runner reads it for the gate.

Run: .venv/bin/python -m unittest tests.test_egress
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
import unittest

from sbx_omnigent import egress


class TestThePublishedAllowlist(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='egress-')
        self.root = os.path.join(self.tmp, 'wt')

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_it_round_trips(self) -> None:
        egress.publish_policy(self.root, ('pypi.org', 'deb.debian.org:80'))
        self.assertEqual(
            egress.read_policy(self.root), ('pypi.org', 'deb.debian.org:80')
        )

    def test_an_empty_list_round_trips(self) -> None:
        egress.publish_policy(self.root, ())
        self.assertEqual(egress.read_policy(self.root), ())

    def test_only_the_owner_reads_it(self) -> None:
        egress.publish_policy(self.root, ('pypi.org',))
        path = os.path.join(self.root, egress.POLICY_FILE)
        self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)

    def test_republishing_replaces_it_and_leaves_nothing_behind(self) -> None:
        egress.publish_policy(self.root, ('pypi.org',))
        egress.publish_policy(self.root, ())
        self.assertEqual(egress.read_policy(self.root), ())
        self.assertEqual(os.listdir(self.root), [egress.POLICY_FILE])

    def test_the_file_holds_a_versioned_list(self) -> None:
        egress.publish_policy(self.root, ('pypi.org',))
        with open(os.path.join(self.root, egress.POLICY_FILE)) as fh:
            self.assertEqual(
                json.load(fh), {'version': 1, 'egress_allow': ['pypi.org']}
            )

    def test_publishing_refuses_an_entry_that_would_split(self) -> None:
        # The gate joins the list with commas for `sbx policy allow`.
        for bad in ('a,b', 'a b', ''):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    egress.publish_policy(self.root, (bad,))

    def test_a_missing_file_says_to_restart_the_server(self) -> None:
        with self.assertRaises(egress.PolicyError) as caught:
            egress.read_policy(self.root)
        self.assertIn('Restart the server', str(caught.exception))

    def _write(self, content: str) -> None:
        os.makedirs(self.root, exist_ok=True)
        with open(os.path.join(self.root, egress.POLICY_FILE), 'w') as fh:
            fh.write(content)

    def test_anything_malformed_is_refused(self) -> None:
        for content in (
            'not json', '[]', '{}', '{"version": 2, "egress_allow": []}',
            '{"version": 1}', '{"version": 1, "egress_allow": "pypi.org"}',
            '{"version": 1, "egress_allow": [""]}',
            '{"version": 1, "egress_allow": ["a,b"]}',
            '{"version": 1, "egress_allow": [7]}',
        ):
            with self.subTest(content=content):
                self._write(content)
                with self.assertRaises(egress.PolicyError):
                    egress.read_policy(self.root)


if __name__ == '__main__':
    unittest.main()
