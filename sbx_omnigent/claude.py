"""
Claude Code launch-gate pre-acceptance for sbx microVMs.

The third harness module beside :mod:`sbx_omnigent.agy` and
:mod:`sbx_omnigent.codex`, and the smallest of the three: Claude
authenticates through the sbx proxy, so nothing here seeds a
credential. What it seeds is the one setting without which a headless
Claude cannot launch in ``bypassPermissions`` at all.

WHY THIS EXISTS
---------------
``--permission-mode bypassPermissions`` is Omnigent's OWN YOLO value for
claude-native — ``_derive_terminal_launch_args_from_spec`` in
``omnigent/server/routes/_sessions/helpers.py`` maps a spec's
``permission_mode`` onto ``--permission-mode`` and states "YOLO uses
``bypassPermissions``", and Omnigent's web permission-mode selector
sends exactly ``["--permission-mode", "bypassPermissions"]``.

But the first launch in that mode opens a full-screen dialog —
"WARNING: Claude Code running in Bypass Permissions mode ... 2. Yes, I
accept". Nobody is at the terminal in a swarm VM, so the turn sits
there until it times out.

Omnigent already pre-accepts Claude's OTHER two launch gates,
``hasCompletedOnboarding`` and the per-directory
``hasTrustDialogAccepted`` (``claude_native_bridge``'s
``ensure_claude_workspace_trusted``), and its docstring says
explicitly that it "deliberately does NOT skip per-tool permission
prompts". So this third gate is ours to clear.

Accepting the dialog once persists
``skipDangerousModePermissionPrompt: true`` into
``~/.claude/settings.json``, and seeding that key into a HOME that has
never accepted anything suppresses the dialog outright. Verified
2026-08-22 against a throwaway HOME carrying exactly Omnigent's own
seeding and nothing else: WITHOUT the key the launch stops on the
dialog; WITH it the footer reads "bypass permissions on" and
:func:`sbx_omnigent.readback.claude_permission_mode` returns
``bypassPermissions``.

The payload is a fixed boolean rather than a secret, so — like agy's
inert placeholder seed, and unlike codex's real token — it is passed as
a script literal instead of on stdin.
"""

from __future__ import annotations

#: Sentinel the in-VM seed prints on success, so a silent no-op cannot
#: pass for a successful seed.
SEED_OK_MARKER = '__omni_claude_seed_ok__'

#: The ``~/.claude/settings.json`` key that records "I have accepted the
#: bypass-permissions warning". Claude Code writes it itself when a
#: human answers the dialog; we write it ahead of time so the dialog
#: never renders.
SETTINGS_KEY = 'skipDangerousModePermissionPrompt'

#: Path of the settings file inside the guest, relative to ``$HOME``.
SETTINGS_REL_PATH = '.claude/settings.json'


def build_settings_seed_script() -> str:
    """
    The in-VM program that pre-accepts the bypass-permissions dialog.

    Merges :data:`SETTINGS_KEY` into the guest's
    ``~/.claude/settings.json`` rather than writing the file wholesale:
    the host image may already ship settings, and clobbering them would
    trade one silent launch failure for another. Idempotent — a file
    that already carries the key is left untouched, and the marker is
    printed either way.

    Fails loud on a settings file that is not a JSON object, matching
    Omnigent's own refusal to overwrite an unexpected user config
    rather than silently replacing it.

    :returns: A ``python3 -c`` program printing :data:`SEED_OK_MARKER`.
    """
    return (
        'import json, os, pathlib, sys\n'
        "home = pathlib.Path(os.path.expanduser('~'))\n"
        f'p = home / {SETTINGS_REL_PATH!r}\n'
        'data = {}\n'
        'if p.exists():\n'
        "    raw = p.read_text(encoding='utf-8').strip()\n"
        '    data = json.loads(raw) if raw else {}\n'
        '    if not isinstance(data, dict):\n'
        "        sys.exit('claude settings is not a JSON object: %s' % p)\n"
        f'if data.get({SETTINGS_KEY!r}) is not True:\n'
        f'    data[{SETTINGS_KEY!r}] = True\n'
        '    p.parent.mkdir(parents=True, exist_ok=True)\n'
        "    p.write_text(json.dumps(data, indent=2), encoding='utf-8')\n"
        '    os.chmod(p, 0o600)\n'
        f'print({SEED_OK_MARKER!r})\n'
    )
