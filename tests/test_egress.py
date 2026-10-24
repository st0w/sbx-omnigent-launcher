"""The server's published egress allowlist (#41).

The pipeline runner never saw the server's config, so the verify gate
always got the built-in default allowlist, whatever the operator had
set, including ``[]``. The server now publishes its resolved allowlist
beside the worktrees, and the runner reads it for the gate. A pipeline's
declared ``egress:`` hosts are checked against it before any turn.

Run: .venv/bin/python -m unittest tests.test_egress
"""

from __future__ import annotations

import io
import json
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr

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


_BOX = 'egress-check-run-abc123'
_IMAGE = 'example.invalid/host:1'


def _verdict(target: str, allowed: bool) -> str:
    raw: dict[str, object] = {
        'allowed': allowed, 'context': f'sandbox:{_BOX}', 'target': target,
    }
    if not allowed:
        raw['reason'] = 'No matching allow rule (default deny)'
    return json.dumps(raw)


class _FakeSbx:
    """Records sbx argv; answers each check from ``allowed``."""

    def __init__(self, allowed: frozenset[str] = frozenset()) -> None:
        self.allowed = allowed
        self.calls: list[list[str]] = []
        self.fail: dict[str, subprocess.CompletedProcess[str]] = {}
        self.hang: set[str] = set()

    def __call__(
        self, argv: list[str], **_kw: object
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        verb = argv[1] if argv[1] != 'policy' else argv[2]
        if verb in self.hang:
            raise subprocess.TimeoutExpired(argv, 1)
        if verb in self.fail:
            return self.fail[verb]
        if verb == 'check':
            host = argv[-1]
            ok = host in self.allowed
            return subprocess.CompletedProcess(
                argv, 0 if ok else 1, _verdict(host, ok), ''
            )
        return subprocess.CompletedProcess(argv, 0, '', '')

    def verbs(self) -> list[str]:
        return [c[1] if c[1] != 'policy' else c[2] for c in self.calls]


class TestDeclaredHostsAreCheckedInASandbox(unittest.TestCase):
    def _check(self, sbx: _FakeSbx, hosts=('sh.rustup.rs',),
               allowlist=('pypi.org',)):
        return egress.check_declared_hosts(
            name=_BOX, hosts=tuple(hosts), allowlist=tuple(allowlist),
            image=_IMAGE, runner=sbx,
        )

    def test_it_creates_scopes_checks_and_removes_the_box(self) -> None:
        sbx = _FakeSbx(frozenset({'sh.rustup.rs', 'deb.debian.org:80'}))
        self._check(
            sbx, hosts=('sh.rustup.rs', 'deb.debian.org:80'),
            allowlist=('pypi.org', 'deb.debian.org:80'),
        )
        self.assertEqual(sbx.calls, [
            ['sbx', 'create', 'shell', '--template', _IMAGE,
             '--name', _BOX, '--quiet', '--cpus', '1'],
            ['sbx', 'policy', 'allow', 'network', '--sandbox', _BOX,
             'pypi.org,deb.debian.org:80'],
            ['sbx', 'policy', 'check', 'network', '--json',
             '--sandbox', _BOX, 'sh.rustup.rs'],
            ['sbx', 'policy', 'check', 'network', '--json',
             '--sandbox', _BOX, 'deb.debian.org:80'],
            ['sbx', 'rm', '--force', _BOX],
        ])

    def test_the_box_mounts_nothing(self) -> None:
        sbx = _FakeSbx()
        self._check(sbx)
        create = sbx.calls[0]
        self.assertEqual(create[:3], ['sbx', 'create', 'shell'])
        self.assertTrue(create[3].startswith('--'), create)

    def test_each_host_gets_a_verdict(self) -> None:
        verdicts = self._check(
            _FakeSbx(frozenset({'a.example'})),
            hosts=('a.example', 'b.example'),
        )
        self.assertEqual(verdicts, (
            egress.HostVerdict('a.example', True, ''),
            egress.HostVerdict(
                'b.example', False, 'No matching allow rule (default deny)'
            ),
        ))

    def test_a_lockdown_adds_no_rule(self) -> None:
        sbx = _FakeSbx()
        self._check(sbx, allowlist=())
        self.assertEqual(sbx.verbs(), ['create', 'check', 'rm'])

    def _raises(self, sbx: _FakeSbx) -> str:
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(egress.EgressCheckError) as caught:
                self._check(sbx)
        return str(caught.exception)

    def test_a_failed_create_raises_and_removes_nothing(self) -> None:
        sbx = _FakeSbx()
        sbx.fail['create'] = subprocess.CompletedProcess([], 1, '', 'boom')
        self.assertIn('boom', self._raises(sbx))
        self.assertEqual(sbx.verbs(), ['create'])

    def test_a_failed_allow_raises_and_removes_the_box(self) -> None:
        sbx = _FakeSbx()
        sbx.fail['allow'] = subprocess.CompletedProcess([], 1, '', 'nope')
        self.assertIn('nope', self._raises(sbx))
        self.assertEqual(sbx.verbs(), ['create', 'allow', 'rm'])

    def test_a_check_that_is_not_a_verdict_raises(self) -> None:
        # A 404 or a changed output format must not read as a pass.
        for out in ('', 'not json', '[]', '{}', '{"allowed": "yes"}'):
            with self.subTest(out=out):
                sbx = _FakeSbx()
                sbx.fail['check'] = subprocess.CompletedProcess(
                    [], 1, out, 'Error: 404 Not Found'
                )
                self.assertIn('sh.rustup.rs', self._raises(sbx))
                self.assertEqual(
                    sbx.verbs(), ['create', 'allow', 'check', 'rm']
                )

    def test_a_hung_call_raises_naming_the_daemon(self) -> None:
        for verb in ('create', 'allow', 'check'):
            with self.subTest(verb=verb):
                sbx = _FakeSbx()
                sbx.hang.add(verb)
                self.assertIn('not responding', self._raises(sbx))

    def test_a_box_it_cannot_remove_is_reported(self) -> None:
        sbx = _FakeSbx()
        sbx.fail['rm'] = subprocess.CompletedProcess([], 1, '', 'busy')
        err = io.StringIO()
        with redirect_stderr(err):
            self._check(sbx)
        self.assertIn(f'sbx rm --force {_BOX}', err.getvalue())

    def test_a_hung_remove_is_reported(self) -> None:
        sbx = _FakeSbx()
        sbx.hang.add('rm')
        err = io.StringIO()
        with redirect_stderr(err):
            self._check(sbx)
        self.assertIn(f'sbx rm --force {_BOX}', err.getvalue())


class TestTheCheckBoxName(unittest.TestCase):
    def test_it_is_prefixed_short_and_unique(self) -> None:
        a = egress.check_box_name('x' * 200)
        b = egress.check_box_name('x' * 200)
        self.assertTrue(a.startswith('egress-check-xxx'))
        self.assertLessEqual(len(a), 56)
        self.assertNotEqual(a, b)

    def test_it_is_a_safe_sandbox_name(self) -> None:
        name = egress.check_box_name('Run / with $pace')
        self.assertRegex(name, r'^[a-z0-9-]+$')


if __name__ == '__main__':
    unittest.main()
