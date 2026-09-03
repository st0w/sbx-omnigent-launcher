"""Stage-3 tests: agy agent-VM credential seed builders.

Exercises the pure builders and the rendered in-VM seeding program
(``sbx_omnigent.agy``) with no real ``sbx``. Run with:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from sbx_omnigent import agy


class TestSeedBuilders(unittest.TestCase):
    """The placeholder token / creds / onboarding objects."""

    def test_token_file_nested_placeholder(self) -> None:
        tok = agy.placeholder_token_file(enterprise=True)
        self.assertEqual(tok['token']['access_token'], agy.PLACEHOLDER_TOKEN)
        self.assertEqual(tok['token']['expiry'], agy.PLACEHOLDER_EXPIRY)
        self.assertTrue(tok['token']['refresh_token'])

    def test_token_access_is_non_empty_but_inert(self) -> None:
        # The gate needs a NON-EMPTY access token, but it must be inert.
        tok = agy.placeholder_token_file(enterprise=True)
        self.assertTrue(tok['token']['access_token'])
        self.assertIn('PLACEHOLDER', agy.PLACEHOLDER_TOKEN)

    def test_auth_method_tracks_account_type(self) -> None:
        # Enterprise (GCP) accounts need "gcp"; consumer needs "oauth".
        # A mismatch makes agy reject the token ("Please sign in").
        self.assertEqual(agy.auth_method_for(enterprise=True), 'gcp')
        self.assertEqual(agy.auth_method_for(enterprise=False), 'oauth')
        self.assertEqual(
            agy.placeholder_token_file(enterprise=True)['auth_method'], 'gcp'
        )
        self.assertEqual(
            agy.placeholder_token_file(enterprise=False)['auth_method'],
            'oauth',
        )

    def test_oauth_creds_is_flat(self) -> None:
        creds = agy.placeholder_oauth_creds(enterprise=True)
        self.assertEqual(creds['access_token'], agy.PLACEHOLDER_TOKEN)
        self.assertEqual(creds['auth_method'], 'gcp')
        self.assertNotIn('token', creds)

    def test_onboarding_enterprise_true(self) -> None:
        onb = agy.onboarding_json(enterprise=True)
        self.assertIs(onb['enterpriseOnboardingComplete'], True)
        self.assertIs(onb['consumerOnboardingComplete'], True)
        self.assertIs(onb['onboardingComplete'], True)

    def test_onboarding_enterprise_false(self) -> None:
        onb = agy.onboarding_json(enterprise=False)
        self.assertIs(onb['enterpriseOnboardingComplete'], False)


class TestSeedScript(unittest.TestCase):
    """The rendered in-VM ``python3 -c`` seeding program."""

    def test_compiles_both_variants(self) -> None:
        compile(agy.build_agent_seed_script(enterprise=True), '<s>', 'exec')
        compile(agy.build_agent_seed_script(enterprise=False), '<s>', 'exec')

    def test_embeds_marker_and_placeholder(self) -> None:
        script = agy.build_agent_seed_script(enterprise=False)
        self.assertIn(agy.SEED_OK_MARKER, script)
        self.assertIn(agy.PLACEHOLDER_TOKEN, script)

    def test_enterprise_flag_renders_into_script(self) -> None:
        self.assertIn(
            '"enterpriseOnboardingComplete": true',
            agy.build_agent_seed_script(enterprise=True),
        )
        self.assertIn(
            '"enterpriseOnboardingComplete": false',
            agy.build_agent_seed_script(enterprise=False),
        )

    def test_executing_seeds_three_files(self) -> None:
        home = os.path.join(
            os.path.dirname(__file__), '_agy_seed_home_probe'
        )
        script = agy.build_agent_seed_script(enterprise=True)
        seed_globals = {'__name__': '__seed__'}
        try:
            with mock.patch.dict(os.environ, {'HOME': home}):
                exec(compile(script, '<seed>', 'exec'), seed_globals)
            gem = os.path.join(home, '.gemini')
            acli = os.path.join(gem, 'antigravity-cli')
            creds_path = os.path.join(gem, 'oauth_creds.json')
            token_path = os.path.join(acli, 'antigravity-oauth-token')
            onb_path = os.path.join(acli, 'cache', 'onboarding.json')
            self.assertTrue(os.path.isfile(creds_path))
            self.assertTrue(os.path.isfile(token_path))
            self.assertTrue(os.path.isfile(onb_path))
            with open(token_path, encoding='utf-8') as fh:
                self.assertEqual(
                    json.load(fh)['token']['access_token'],
                    agy.PLACEHOLDER_TOKEN,
                )
            with open(onb_path, encoding='utf-8') as fh:
                self.assertIs(
                    json.load(fh)['enterpriseOnboardingComplete'], True
                )
        finally:
            shutil.rmtree(home, ignore_errors=True)


class TestSettingsAndBridge(unittest.TestCase):
    """GCP settings seed + the enterprise-onboarding bridge patch."""

    def test_settings_json_with_project(self) -> None:
        block = agy.settings_json(gcp_project='p-1', gcp_location='eu')
        self.assertEqual(block['gcp'], {'project': 'p-1', 'location': 'eu'})

    def test_settings_json_default_location(self) -> None:
        block = agy.settings_json(gcp_project='p-1')
        self.assertEqual(block['gcp']['location'], 'us')

    def test_settings_json_empty_without_project(self) -> None:
        self.assertEqual(agy.settings_json(gcp_project=None), {})

    def test_seed_script_includes_gcp(self) -> None:
        script = agy.build_agent_seed_script(
            enterprise=True, gcp_project='p-xyz'
        )
        self.assertIn('p-xyz', script)
        self.assertIn('settings.json', script)

    def test_seed_script_guards_empty_settings(self) -> None:
        script = agy.build_agent_seed_script(enterprise=True)
        self.assertIn('if _settings:', script)

    def test_bridge_patch_compiles_and_targets_both(self) -> None:
        patch = agy.build_bridge_patch_script(
            enterprise=True, seed_settings=True
        )
        compile(patch, '<patch>', 'exec')
        self.assertIn('enterpriseOnboardingComplete', patch)
        self.assertIn('_AGY_SEED_FILES', patch)
        self.assertIn('_do_ent = True', patch)
        self.assertIn('_do_settings = True', patch)
        self.assertIn(agy.BRIDGE_PATCH_OK_MARKER, patch)

    def test_bridge_patch_flags_reflect_args(self) -> None:
        patch = agy.build_bridge_patch_script(
            enterprise=False, seed_settings=True
        )
        self.assertIn('_do_ent = False', patch)
        self.assertIn('_do_settings = True', patch)
        compile(patch, '<patch>', 'exec')

    def test_paste_placeholder_flag_reflects_args(self) -> None:
        on = agy.build_bridge_patch_script(
            enterprise=False, seed_settings=False
        )
        self.assertIn('_do_paste = True', on)  # on by default
        off = agy.build_bridge_patch_script(
            enterprise=False, seed_settings=False, paste_placeholder=False
        )
        self.assertIn('_do_paste = False', off)
        compile(off, '<patch>', 'exec')


    def test_executing_seed_with_gcp_writes_settings(self) -> None:
        home = os.path.join(
            os.path.dirname(__file__), '_agy_gcp_home_probe'
        )
        script = agy.build_agent_seed_script(
            enterprise=True, gcp_project='p-1', gcp_location='eu'
        )
        try:
            with mock.patch.dict(os.environ, {'HOME': home}):
                exec(compile(script, '<seed>', 'exec'), {'__name__': '__s__'})
            sp = os.path.join(
                home, '.gemini', 'antigravity-cli', 'settings.json'
            )
            self.assertTrue(os.path.isfile(sp))
            with open(sp, encoding='utf-8') as fh:
                gcp = json.load(fh)['gcp']
            self.assertEqual(gcp, {'project': 'p-1', 'location': 'eu'})
        finally:
            shutil.rmtree(home, ignore_errors=True)

#: A faithful copy of the bridge's paste-commit path: the real composer
#: scoping (only the lines BETWEEN the last two separator rules), the
#: candidate/needle match, and the hard fail. A MULTI-LINE draft grows
#: agy's box past that rule pair, so the scoped window reads empty and
#: the needle is never found — the shape a human's reply to an
#: interactive planner hits. A long paste instead collapses to
#: "[Pasted text #N ... chars]". Both rendered; neither was seen.
_FAKE_BRIDGE = '''\
_SEP = "\\u2500"


def _agy_separator_line(line):
    stripped = line.strip()
    return stripped.count(_SEP) >= 8 and set(stripped) <= {_SEP}


def _agy_input_region(pane):
    lines = pane.splitlines()
    idx = [i for i, ln in enumerate(lines) if _agy_separator_line(ln)]
    if len(idx) >= 2:
        return "\\n".join(lines[idx[-2] + 1:idx[-1]])
    return "\\n".join(lines[-8:])


def _agy_draft_candidate_lines(region):
    out = []
    for raw_line in region.splitlines():
        line = raw_line.strip()
        if not line or line == ">":
            continue
        if line.startswith(">"):
            draft = line[1:].strip()
            if draft:
                out.append(draft)
            continue
        out.append(line)
    return out


def _draft_in_input_region(pane, needle, baseline_region):
    region = _agy_input_region(pane)
    if region == baseline_region:
        return False
    candidates = _agy_draft_candidate_lines(region)
    normalized_needle = needle.strip() if needle else ""
    if not normalized_needle:
        return bool(candidates)
    return any(
        line == normalized_needle
        or line.startswith(normalized_needle)
        or normalized_needle in line
        for line in candidates
    )


def _commit_paste(pane, needle, baseline_region, mid_turn=False, content=""):
    draft_seen = False
    last_commit_pane = pane
    if _draft_in_input_region(pane, needle, baseline_region):
        draft_seen = True
    if not draft_seen and not mid_turn:
        raise RuntimeError("agy did not render the pasted message")
    return True
'''

_RULE = '─' * 40
_NEEDLE = 'we will plan to eventually'

#: agy collapsed the paste into its own placeholder.
_COLLAPSED = (
    f'{_RULE}\n> [Pasted text #1 1099 chars]\n{_RULE}\n? for shortcuts'
)

#: A multi-line draft rendered ABOVE both trailing rules, so the scoped
#: composer window is empty (the live full-cadre planner failure).
_ESCAPED = (
    'people all the options.\n'
    '3. The team is free to choose standard stable crates.\n'
    f'{_NEEDLE} support vault solutions (Infisical, Vault),\n'
    'down the road\n'
    f'{_RULE}\n{_RULE}\n? for shortcuts'
)

#: The needle split across two wrapped rows inside the composer.
_WRAPPED = (
    f'{_RULE}\n> we will plan to\neventually support vault solutions\n'
    f'{_RULE}\n? for shortcuts'
)

#: A long reply whose FIRST line — the bridge's only needle source
#: (_submit_needle takes 24 chars of it) — has scrolled out of the pane
#: entirely, so NO needle match can succeed anywhere, not even against
#: the whole pane. The composer still plainly shows the message's TAIL,
#: cursor at the end. Observed live on a human's multi-paragraph reply
#: to the interactive planner.
_CONTENT_SCROLLED = (
    '1. Agree with option A, we want to keep the core trait generic.\n'
    '2. We really want to encourage WIF, so support WIF, ADC, SA\n'
    'impersonation, and SA keys out of the box.\n'
    '3. Option A.\n'
    '4. Yes, fully accepted.\n'
    'Please also ensure all encryption utilizes PQC wherever possible.'
)
#: What _submit_needle(content) yields: 24 chars of the FIRST line.
_NEEDLE_SCROLLED = _CONTENT_SCROLLED.splitlines()[0][:24]
_SCROLLED = (
    f'{_RULE}\n'
    '> impersonation, and SA keys out of the box.\n'
    '3. Option A.\n'
    '4. Yes, fully accepted.\n'
    'Please also ensure all encryption utilizes PQC wherever possible.\n'
    f'{_RULE}\n? for shortcuts'
)

#: Nothing rendered at all — must still fail after the patch.
_EMPTY = f'{_RULE}\n>\n{_RULE}\n? for shortcuts'


class TestBridgePastePatch(unittest.TestCase):
    """Run the rendered patch against a copy of the bridge's render
    check and assert the RESULTING BEHAVIOR, not just its text."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix='agy-bridge-')
        pkg = os.path.join(self.tmp, 'omnigent')
        os.makedirs(pkg)
        open(os.path.join(pkg, '__init__.py'), 'w').close()
        self.mod = os.path.join(pkg, 'antigravity_native_bridge.py')
        with open(self.mod, 'w', encoding='utf-8') as fh:
            fh.write(_FAKE_BRIDGE)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _apply_patch(self) -> str:
        """Run the in-VM patch program against the fake; return stdout.

        Uses AGY_BRIDGE_PATCH_TARGET so the script edits exactly the
        fake and imports NO omnigent — the host's editable omnigent
        (whose import finder outranks PYTHONPATH) can never be reached,
        so a test can never write the user's real checkout.
        """
        script = agy.build_bridge_patch_script(
            enterprise=False, seed_settings=False
        )
        env = {**os.environ, 'AGY_BRIDGE_PATCH_TARGET': self.mod}
        proc = subprocess.run(
            [sys.executable, '-c', script],
            capture_output=True, text=True, env=env,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc.stdout

    def _commit(self):
        """The (possibly patched) paste-commit path on disk."""
        ns: dict = {}
        with open(self.mod, encoding='utf-8') as fh:
            exec(fh.read(), ns)  # fixture module, not user input
        return ns['_commit_paste']

    def test_multiline_draft_is_refused_before_the_patch(self) -> None:
        # The live failure: the draft DID render, but above the rule
        # pair the check scopes to, so the needle is never found.
        with self.assertRaises(RuntimeError):
            self._commit()(_ESCAPED, _NEEDLE, '')

    def test_multiline_draft_is_accepted_after_the_patch(self) -> None:
        self.assertIn(agy.BRIDGE_PATCH_OK_MARKER, self._apply_patch())
        self.assertTrue(self._commit()(_ESCAPED, _NEEDLE, ''))

    def test_scrolled_draft_has_no_needle_anywhere_in_the_pane(self) -> None:
        # Guards the premise of the next test: this render defeats a
        # needle match even against the WHOLE pane, so accepting it
        # cannot come from the needle path.
        self.assertNotIn(_NEEDLE_SCROLLED, _SCROLLED)

    def test_scrolled_draft_is_refused_before_the_patch(self) -> None:
        with self.assertRaises(RuntimeError):
            self._commit()(
                _SCROLLED, _NEEDLE_SCROLLED, '', content=_CONTENT_SCROLLED
            )

    def test_scrolled_draft_is_accepted_after_the_patch(self) -> None:
        # THE live failure: a human's long reply to the interactive
        # planner scrolled past its own first line. The composer shows
        # the TAIL, so the tail slice is what rescues it.
        self.assertIn(agy.BRIDGE_PATCH_OK_MARKER, self._apply_patch())
        self.assertTrue(
            self._commit()(
                _SCROLLED, _NEEDLE_SCROLLED, '', content=_CONTENT_SCROLLED
            )
        )

    def test_tail_match_needs_a_real_render(self) -> None:
        # The tail slice must not rescue a pane that shows nothing: an
        # empty composer still fails even with content supplied.
        self.assertIn(agy.BRIDGE_PATCH_OK_MARKER, self._apply_patch())
        with self.assertRaises(RuntimeError):
            self._commit()(
                _EMPTY, _NEEDLE_SCROLLED, '', content=_CONTENT_SCROLLED
            )

    def test_collapsed_paste_accepted_after_the_patch(self) -> None:
        with self.assertRaises(RuntimeError):
            self._commit()(_COLLAPSED, _NEEDLE, '')
        self._apply_patch()
        self.assertTrue(self._commit()(_COLLAPSED, _NEEDLE, ''))

    def test_wrapped_needle_accepted_after_the_patch(self) -> None:
        # A needle split across wrapped rows matches once the pane is
        # whitespace-collapsed.
        with self.assertRaises(RuntimeError):
            self._commit()(_WRAPPED, _NEEDLE, '')
        self._apply_patch()
        self.assertTrue(self._commit()(_WRAPPED, _NEEDLE, ''))

    def test_patch_keeps_the_real_checks_intact(self) -> None:
        self._apply_patch()
        commit = self._commit()
        # A normally-rendered draft still submits.
        normal = f'{_RULE}\n> {_NEEDLE} and more\n{_RULE}'
        self.assertTrue(commit(normal, _NEEDLE, ''))
        # A pane where nothing rendered STILL fails.
        with self.assertRaises(RuntimeError):
            commit(_EMPTY, _NEEDLE, '')

    def test_patch_is_idempotent(self) -> None:
        first = self._apply_patch()
        second = self._apply_patch()
        self.assertIn('AGY_BRIDGE_PATCHED 1', first)
        self.assertIn('AGY_BRIDGE_PATCHED 0', second)  # nothing to do
        self.assertTrue(self._commit()(_ESCAPED, _NEEDLE, ''))

    def test_patched_module_still_compiles(self) -> None:
        self._apply_patch()
        with open(self.mod, encoding='utf-8') as fh:
            compile(fh.read(), self.mod, 'exec')

    def test_missing_module_is_a_safe_noop(self) -> None:
        # No target and omnigent unimportable → skip, never a crash
        # (version drift must not block a launch). Run with -S so site
        # is skipped and the host's editable-omnigent finder is NOT
        # registered — the import genuinely fails AND cannot reach the
        # user's real checkout.
        script = agy.build_bridge_patch_script(
            enterprise=False, seed_settings=False
        )
        proc = subprocess.run(
            [sys.executable, '-S', '-c', script],
            capture_output=True, text=True,
            env={**os.environ, 'PYTHONPATH': ''}, cwd=tempfile.gettempdir(),
        )
        self.assertEqual(proc.returncode, 0)
        self.assertIn(agy.BRIDGE_PATCH_OK_MARKER, proc.stdout)
        self.assertIn('skip', proc.stdout)  # took the import-failed branch

    def test_script_can_be_targeted_without_importing_omnigent(self) -> None:
        # Regression guard: the rendered script MUST honor an explicit
        # target path, so tests/probes never rely on import resolution
        # (which once leaked this patch into the user's real omnigent).
        script = agy.build_bridge_patch_script(
            enterprise=False, seed_settings=False
        )
        self.assertIn('AGY_BRIDGE_PATCH_TARGET', script)


if __name__ == '__main__':
    unittest.main()
