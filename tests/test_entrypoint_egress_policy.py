"""The server publishes its resolved allowlist for the runner (#41).

The pipeline runner is a separate process and never saw the server's
config, so the verify gate got the built-in default allowlist whatever
the operator had set, including ``[]``.

Run: .venv/bin/python -m unittest tests.test_entrypoint_egress_policy
"""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from unittest import mock

from sbx_omnigent import egress
from sbx_omnigent.defaults import DEFAULT_EGRESS_ALLOW
from sbx_omnigent.entrypoint import _build_sbx_config

_SERVER_URL = 'http://host.docker.internal:6767'


class TestTheServerPublishesItsAllowlist(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='entry-egress-')
        self.root = os.path.join(self.tmp, 'wt')

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _build(self, **sbx: object) -> None:
        _build_sbx_config({
            'server_url': _SERVER_URL,
            'sbx': {'worktree_root': self.root, **sbx},
        })

    def test_unset_publishes_the_default(self) -> None:
        self._build()
        self.assertEqual(egress.read_policy(self.root), DEFAULT_EGRESS_ALLOW)

    def test_lockdown_publishes_an_empty_list(self) -> None:
        self._build(egress_allow=[])
        self.assertEqual(egress.read_policy(self.root), ())

    def test_a_list_is_published_as_is(self) -> None:
        self._build(egress_allow=['api.anthropic.com', 'crates.io'])
        self.assertEqual(
            egress.read_policy(self.root), ('api.anthropic.com', 'crates.io')
        )

    def test_without_a_worktree_root_nothing_is_published(self) -> None:
        with mock.patch.object(egress, 'publish_policy') as publish:
            _build_sbx_config({'server_url': _SERVER_URL, 'sbx': {}})
        publish.assert_not_called()

    def test_a_root_it_cannot_write_fails_startup(self) -> None:
        blocker = os.path.join(self.tmp, 'a-file')
        with open(blocker, 'w') as fh:
            fh.write('x')
        with self.assertRaises(ValueError) as caught:
            _build_sbx_config({
                'server_url': _SERVER_URL,
                'sbx': {'worktree_root': os.path.join(blocker, 'wt')},
            })
        self.assertIn('worktree_root', str(caught.exception))


if __name__ == '__main__':
    unittest.main()
