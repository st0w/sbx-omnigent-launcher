"""The ``sandbox:`` block when the provider is ``sbx`` (#54).

Omnigent parses ``sandbox.reaper`` and a ``providers:`` list outside the
provider entry. The wrapper that adds ``sbx`` builds its deployment from
the sbx block alone, so those settings were dropped without a word, or
refused with a message blaming the wrong thing. They are now refused at
startup, naming the setting, as Omnigent refuses a config it cannot
honour.

Run: .venv/bin/python -m unittest tests.test_sbx_sandbox_config
"""

from __future__ import annotations

import contextlib
import io
import unittest

from sbx_omnigent.entrypoint import _SbxParseSandboxConfig

_SBX: dict[str, object] = {
    'provider': 'sbx',
    'server_url': 'http://x:1',
    'sbx': {'worktree_root': '/tmp/wt'},
}


class _Original:
    """Stands in for Omnigent's parser and records what reached it."""

    def __init__(self) -> None:
        self.calls: list[object] = []

    def __call__(self, raw: object) -> None:
        self.calls.append(raw)


def _parse(raw: object) -> tuple[object, _Original]:
    original = _Original()
    with contextlib.redirect_stderr(io.StringIO()):
        result = _SbxParseSandboxConfig(original)(raw)
    return result, original


class TestAnSbxBlockStillParses(unittest.TestCase):
    def test_a_plain_sbx_block_builds_a_deployment(self) -> None:
        result, original = _parse(dict(_SBX))
        self.assertIsNotNone(result)
        self.assertEqual(original.calls, [])

    def test_other_providers_reach_omnigent_unchanged(self) -> None:
        raw = {'provider': 'modal', 'reaper': {'enabled': True}}
        _result, original = _parse(raw)
        self.assertEqual(original.calls, [raw])

    def test_a_providers_list_without_sbx_reaches_omnigent(self) -> None:
        raw = {'providers': [{'provider': 'modal'}, {'provider': 'e2b'}]}
        _result, original = _parse(raw)
        self.assertEqual(original.calls, [raw])

    def test_no_sandbox_block_reaches_omnigent(self) -> None:
        _result, original = _parse(None)
        self.assertEqual(original.calls, [None])


class TestAReaperBesideSbxIsRefused(unittest.TestCase):
    """It was dropped: the deployment kept Omnigent's defaults, and even
    an unknown key was accepted where Omnigent refuses to start."""

    def _refused(self, reaper: object) -> str:
        with self.assertRaises(ValueError) as caught:
            _parse({**_SBX, 'reaper': reaper})
        return str(caught.exception)

    def test_an_enabled_reaper_is_refused(self) -> None:
        message = self._refused(
            {'enabled': True, 'terminate_after_offline_days': 7}
        )
        self.assertIn("'sandbox.reaper'", message)
        self.assertIn("'sbx'", message)

    def test_an_unknown_reaper_key_is_refused(self) -> None:
        self._refused({'bogus': 1})

    def test_a_disabled_reaper_is_refused_too(self) -> None:
        # Nothing reads it, so it is not honoured whatever it says.
        self._refused({'enabled': False})

    def test_it_never_reaches_omnigent(self) -> None:
        original = _Original()
        with self.assertRaises(ValueError):
            _SbxParseSandboxConfig(original)({**_SBX, 'reaper': {}})
        self.assertEqual(original.calls, [])


class TestSbxInAProvidersListIsRefused(unittest.TestCase):
    """Omnigent's parser does not know `sbx`, so this was refused with a
    list of Omnigent's providers, as if `sbx` were a typo."""

    def test_sbx_in_the_list_is_refused_naming_sbx(self) -> None:
        raw = {'providers': [{'provider': 'modal'}, {'provider': 'sbx'}]}
        with self.assertRaises(ValueError) as caught:
            _parse(raw)
        message = str(caught.exception)
        self.assertIn("'sandbox.providers'", message)
        self.assertIn("'sbx'", message)
        self.assertIn('provider: sbx', message)

    def test_it_never_reaches_omnigent(self) -> None:
        original = _Original()
        with self.assertRaises(ValueError):
            _SbxParseSandboxConfig(original)(
                {'providers': [{'provider': 'sbx'}]}
            )
        self.assertEqual(original.calls, [])

    def test_provider_sbx_beside_a_providers_list_is_refused(self) -> None:
        # Omnigent refuses both keys at once; the wrapper used to take
        # `provider: sbx` and drop the list.
        with self.assertRaises(ValueError) as caught:
            _parse({**_SBX, 'providers': [{'provider': 'modal'}]})
        self.assertIn("'providers'", str(caught.exception))

    def test_a_malformed_list_is_left_to_omnigent(self) -> None:
        # Its own message for a list that is not a list of mappings.
        raw = {'providers': ['sbx', 3]}
        _result, original = _parse(raw)
        self.assertEqual(original.calls, [raw])


if __name__ == '__main__':
    unittest.main()
