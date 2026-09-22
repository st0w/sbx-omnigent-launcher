"""Startup warnings for server config that parses but does nothing.

Two settings validate cleanly at startup and then fail silently inside
every guest: a ``providers`` entry that is the default for no model
family (#34), and an explicit ``sbx.egress_allow`` that drops the hosts
covering gaps in sbx's own bundles (#40). Both warn, never refuse.
"""

from __future__ import annotations

import contextlib
import io
import unittest
from typing import ClassVar

from sbx_omnigent import entrypoint
from sbx_omnigent.defaults import DEFAULT_EGRESS_ALLOW, SBX_BUNDLE_GAP_HOSTS
from sbx_omnigent.entrypoint import _build_sbx_config


def _stderr_of(fn, *args, **kwargs) -> str:
    """Run *fn* and return what it wrote to stderr."""
    buf = io.StringIO()
    with contextlib.redirect_stderr(buf):
        fn(*args, **kwargs)
    return buf.getvalue()


class TestAProviderThatIsDefaultForNothing(unittest.TestCase):
    """An entry with no ``default:`` is ignored by routing (#34)."""

    _RAW: ClassVar[dict[str, object]] = {
        'server_url': 'http://host.docker.internal:6767',
    }

    def _build(self, providers: dict[str, object]) -> str:
        return _stderr_of(
            _build_sbx_config,
            {**self._RAW, 'host_config': {'providers': providers}},
        )

    def test_an_entry_without_default_is_named(self) -> None:
        said = self._build(
            {'codex': {'kind': 'subscription', 'cli': 'codex'}}
        )
        self.assertIn('providers.codex', said)
        self.assertIn('default: true', said)

    def test_a_default_entry_is_silent(self) -> None:
        for default in (True, 'openai'):
            with self.subTest(default=default):
                said = self._build({
                    'codex': {
                        'kind': 'subscription',
                        'cli': 'codex',
                        'default': default,
                    }
                })
                self.assertEqual(said, '')

    def test_only_the_inert_entry_is_named(self) -> None:
        # A non-default entry beside a default one is legitimate, so
        # this warns and never refuses; it names only the inert one.
        said = self._build({
            'claude': {'kind': 'subscription', 'cli': 'claude',
                       'default': True},
            'codex': {'kind': 'subscription', 'cli': 'codex'},
        })
        self.assertIn('providers.codex', said)
        self.assertNotIn('providers.claude', said)

    def test_the_config_still_builds(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            cfg = _build_sbx_config({
                **self._RAW,
                'host_config': {
                    'providers': {
                        'codex': {'kind': 'subscription', 'cli': 'codex'}
                    }
                },
            })
        self.assertIsNotNone(cfg.host_config)

    def test_no_providers_block_is_silent(self) -> None:
        self.assertEqual(
            _stderr_of(_build_sbx_config, dict(self._RAW)), ''
        )
        self.assertEqual(
            _stderr_of(
                _build_sbx_config,
                {**self._RAW, 'host_config': {'other': 1}},
            ),
            '',
        )


class TestAnExplicitAllowlistThatDropsAGapHost(unittest.TestCase):
    """A custom list silently loses the sbx-bundle workarounds (#40)."""

    _RAW: ClassVar[dict[str, object]] = {
        'server_url': 'http://host.docker.internal:6767',
    }

    def _build(self, egress_allow: object) -> str:
        return _stderr_of(
            _build_sbx_config,
            {**self._RAW, 'sbx': {'egress_allow': egress_allow}},
        )

    def test_the_gap_hosts_are_in_the_default(self) -> None:
        # Nothing pinned these: removing one left every test green.
        for host in ('**.astral.sh', 'api.osv.dev', 'deb.debian.org:80'):
            with self.subTest(host=host):
                self.assertIn(host, SBX_BUNDLE_GAP_HOSTS)
                self.assertIn(host, DEFAULT_EGRESS_ALLOW)

    def test_every_gap_host_says_what_it_prevents(self) -> None:
        for host, why in SBX_BUNDLE_GAP_HOSTS.items():
            with self.subTest(host=host):
                self.assertTrue(why.strip())

    def test_a_list_missing_gap_hosts_names_each(self) -> None:
        said = self._build(['api.anthropic.com', 'pypi.org'])
        for host in SBX_BUNDLE_GAP_HOSTS:
            with self.subTest(host=host):
                self.assertIn(host, said)

    def test_a_list_carrying_them_over_is_silent(self) -> None:
        self.assertEqual(
            self._build(['api.anthropic.com', *SBX_BUNDLE_GAP_HOSTS]), ''
        )

    def test_only_the_missing_host_is_named(self) -> None:
        kept = [h for h in SBX_BUNDLE_GAP_HOSTS if h != 'api.osv.dev']
        said = self._build(['api.anthropic.com', *kept])
        self.assertIn('api.osv.dev', said)
        self.assertNotIn('**.astral.sh', said)

    def test_the_default_is_silent(self) -> None:
        self.assertEqual(self._build(None), '')

    def test_lockdown_is_silent(self) -> None:
        # `[]` blocks everything on purpose; nothing was dropped by
        # accident.
        self.assertEqual(self._build([]), '')

    def test_it_never_adds_a_host(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            hosts = entrypoint._as_egress_allow(['api.anthropic.com'])
        self.assertEqual(hosts, ('api.anthropic.com',))


if __name__ == '__main__':
    unittest.main()
