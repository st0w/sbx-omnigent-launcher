"""The runner signs in to a server with accounts, and shares sessions.

Hermetic: the HTTP transport is a fake, the secret is a temporary file,
and the command's preflights are stubbed, so nothing opens a socket or
touches sbx.
"""

from __future__ import annotations

import contextlib
import json
import tempfile
import unittest
from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import click
from click.testing import CliRunner, Result

from sbx_omnigent.runner import cli, server_client
from sbx_omnigent.server_auth import MachineClient
from sbx_omnigent.swarm_session import READ_ACCESS, SwarmSessionClient

_SERVER = 'http://127.0.0.1:6767'


class _Transport:
    """Mints a token at the token endpoint; answers the rest empty."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, str], object]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        timeout: float,
    ) -> tuple[int, bytes]:
        if url.endswith('/oauth/token'):
            self.sent.append((method, url, dict(headers), body))
            return 200, b'{"access_token": "jwt-1", "expires_in": 3600}'
        decoded = json.loads(body) if body is not None else None
        self.sent.append((method, url, dict(headers), decoded))
        return 200, b'{"data": []}'

    def iter_lines(
        self, url: str, *, headers: dict[str, str], read_timeout: float
    ) -> Iterator[str]:
        return iter([])


class _WithSecret(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name).resolve()
        self.secret = self.tmp / 'machine-secret'
        self.secret.write_text('s3cr3t\n')
        self.secret.chmod(0o600)
        self.config = self.tmp / 'pipeline.yaml'
        self.config.write_text('name: p\n')


class TestSessionClient(_WithSecret):
    def setUp(self) -> None:
        super().setUp()
        self.transport = _Transport()
        patcher = mock.patch.object(
            server_client, 'UrllibTransport', lambda: self.transport
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def client(
        self, machine: MachineClient | None, share_with: str | None = None
    ) -> SwarmSessionClient:
        return server_client.session_client(
            _SERVER, machine=machine, share_with=share_with,
            auto_approve=True,
        )

    def test_it_signs_in_before_anything_else(self) -> None:
        self.client(MachineClient('pipeline', self.secret))
        self.assertEqual(len(self.transport.sent), 1)
        self.assertTrue(self.transport.sent[0][1].endswith('/oauth/token'))

    def test_requests_carry_the_machine_token(self) -> None:
        client = self.client(MachineClient('pipeline', self.secret))
        client.list_builtin_agents()
        _method, url, headers, _body = self.transport.sent[1]
        self.assertTrue(url.startswith(f'{_SERVER}/v1/agents'))
        self.assertEqual(headers['Authorization'], 'Bearer jwt-1')

    def test_without_a_machine_client_nothing_is_signed_in(self) -> None:
        self.client(None).list_builtin_agents()
        self.assertEqual(len(self.transport.sent), 1)
        self.assertNotIn('Authorization', self.transport.sent[0][2])

    def test_sessions_are_shared_with_the_named_person(self) -> None:
        self.client(None, share_with='alice').share(
            'conv_1', level=READ_ACCESS
        )
        self.assertEqual(
            self.transport.sent[0][3], {'user_id': 'alice', 'level': 1}
        )


def _option(name: str) -> click.Option:
    flags = [
        p for p in cli.main.params
        if isinstance(p, click.Option) and name in p.opts
    ]
    if len(flags) != 1:
        raise AssertionError(f'{name} is not offered')
    return flags[0]


class TestCommand(_WithSecret):
    def invoke(self, *args: str, **stubs: mock.Mock) -> Result:
        replaced: dict[str, mock.Mock] = {
            'pipeline': mock.DEFAULT,
            'preflight_sbx': mock.DEFAULT,
            'resolve_publish_token': mock.DEFAULT,
            **stubs,
        }
        with contextlib.ExitStack() as stack:
            self.patched = {
                name: stack.enter_context(mock.patch.object(cli, name, new))
                for name, new in replaced.items()
            }
            return CliRunner().invoke(cli.main, [
                '-c', str(self.config), '--server', _SERVER,
                '--canonical-root', str(self.tmp),
                '--worktree-root', str(self.tmp), *args,
            ])

    def test_each_option_reads_its_environment_variable(self) -> None:
        for name, envvar in (
            ('--share-with', 'OMNI_SHARE_WITH'),
            ('--machine-client-id', 'OMNI_MACHINE_CLIENT_ID'),
            ('--machine-secret-file', 'OMNI_MACHINE_SECRET_FILE'),
        ):
            with self.subTest(name=name):
                option = _option(name)
                self.assertEqual(option.envvar, envvar)
                self.assertIsNone(
                    option.get_default(click.Context(cli.main))
                )

    def test_a_bad_share_name_stops_before_any_preflight(self) -> None:
        result = self.invoke('--share-with', 'Alice')
        self.assertEqual(result.exit_code, 2, result.output)
        self.assertIn('--share-with', result.output)
        self.patched['preflight_sbx'].assert_not_called()

    def test_half_a_machine_client_stops_before_any_preflight(
        self,
    ) -> None:
        for args in (
            ('--machine-client-id', 'pipeline'),
            ('--machine-secret-file', str(self.secret)),
        ):
            with self.subTest(args=args):
                result = self.invoke(*args)
                self.assertEqual(result.exit_code, 2, result.output)
                self.assertIn('together', result.output)
                self.patched['preflight_sbx'].assert_not_called()

    def test_the_machine_client_reaches_the_session_client(self) -> None:
        make_client = mock.Mock(side_effect=click.Abort)
        self.invoke(
            '--machine-client-id', 'pipeline',
            '--machine-secret-file', str(self.secret),
            session_client=make_client,
        )
        machine = make_client.call_args.kwargs['machine']
        self.assertEqual(machine, MachineClient('pipeline', self.secret))

    def test_resume_and_the_run_use_the_one_client(self) -> None:
        client = mock.Mock(name='client')
        make_client = mock.Mock(return_value=client)
        result = self.invoke(
            '--share-with', 'alice', '--resume',
            '--skip-disk-check', '--skip-agy-check',
            session_client=make_client,
            read_operator_egress=mock.Mock(return_value=None),
            WorktreeManager=mock.DEFAULT,
            reclaim_for_resume=mock.DEFAULT,
            _resume_worktree_count=mock.Mock(return_value=0),
            preflight_codex_auth=mock.DEFAULT,
            preflight_codex_login=mock.DEFAULT,
            ensure_pipeline_image=mock.DEFAULT,
            _drive=mock.DEFAULT,
            agy=mock.DEFAULT,
        )
        self.assertEqual(result.exit_code, 0, result.output)
        make_client.assert_called_once_with(
            _SERVER, machine=None, share_with='alice', auto_approve=True
        )
        drive = self.patched['_drive'].call_args.kwargs
        reclaim = self.patched['reclaim_for_resume'].call_args.kwargs
        self.assertIs(reclaim['client'], client)
        self.assertIs(drive['client'], client)


if __name__ == '__main__':
    unittest.main()
