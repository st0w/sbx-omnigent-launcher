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

import re

#: Sentinel the in-VM seed prints on success, so a silent no-op cannot
#: pass for a successful seed.
#: The claude-native harness ids, as Omnigent spells them
#: (``CLAUDE_GATEWAY_HARNESSES`` in ``omnigent/gateway_inference.py``).
CLAUDE_NATIVE_HARNESSES: frozenset[str] = frozenset(
    {'claude-native', 'native-claude'}
)

#: Largest agent instructions, in bytes, a claude-native agent is
#: allowed at pipeline load.
#:
#: Omnigent v0.13.0 starts Claude Code inside tmux and passes the
#: agent's instructions on that command line (``--append-system-prompt
#: <text>``). tmux 3.5a refuses a command over about 16,329 bytes
#: (measured in a guest), and the same command also carries an inline
#: MCP config, tool lists and the model and permission flags. Over the
#: limit the terminal never starts, and the run fails with
#: ``failed: None``. Observed: 13,584 bytes launched; 16,763 and 17,719
#: did not. This leaves about 2 KB for the rest of the command.
LAUNCH_INSTRUCTIONS_BUDGET = 14_000

SEED_OK_MARKER = '__omni_claude_seed_ok__'

#: The ``~/.claude/settings.json`` key that records "I have accepted the
#: bypass-permissions warning". Claude Code writes it itself when a
#: human answers the dialog; we write it ahead of time so the dialog
#: never renders.
SETTINGS_KEY = 'skipDangerousModePermissionPrompt'

#: Path of the settings file inside the guest, relative to ``$HOME``.
SETTINGS_REL_PATH = '.claude/settings.json'

#: Claude Code's switch for its background auto-updater, set through the
#: ``env`` block of ``settings.json`` (documented: code.claude.com
#: "Disable auto-updates"). It stops only the background check, which is
#: what moves a VM to a new version partway through a run.
AUTOUPDATER_ENV = 'DISABLE_AUTOUPDATER'

#: Printed by :func:`build_version_pin_script` once ``claude --version``
#: reports the pinned version.
PIN_OK_MARKER = '__omni_claude_pin_ok__'

#: The npm package the host image installs Claude Code from. Replacing
#: it in place keeps the ``claude`` Omnigent launches: the image puts it
#: at ``/usr/local/bin/claude``, while a native ``claude install`` lands
#: in ``~/.local/bin``, which is not on the VM's ``PATH``.
NPM_PACKAGE = '@anthropic-ai/claude-code'

_VERSION_RE = re.compile(r'[0-9]+\.[0-9]+\.[0-9]+')

#: Claude Code's reply when the API rejected the turn: one line,
#: optionally prefixed for a rejected credential. Seen live: "API Error:
#: 400 Claude Code 2.1.266 does not support this model; ..." and
#: "Failed to authenticate. API Error: 401 OAuth access token is
#: invalid.".
_API_ERROR_RE = re.compile(
    r'(?:Failed to authenticate\. )?API Error: [0-9]{3}\b[^\n]*'
)

#: The API's refusal of a model the installed Claude Code predates.
_TOO_OLD_RE = re.compile(
    r'Claude Code ([0-9]+\.[0-9]+\.[0-9]+) does not support this model; '
    r'version ([0-9]+\.[0-9]+\.[0-9]+) or newer is required'
)


def api_error_reply(reply: str | None) -> str | None:
    """
    The API error a turn's whole reply consists of, or ``None``.

    Claude Code answers a request the API rejected with that one line
    of text, and Omnigent forwards it as the turn's reply. The server
    does mark the turn failed, but the runner can see the turn finish
    first: a pipeline writer's four turns each came back as that line
    and were taken for success. Only a reply that is exactly the line
    counts, so an agent quoting one in real work is left alone.

    :param reply: The turn's reply.
    :returns: The error line, or ``None``.
    """
    text = (reply or '').strip()
    return text if _API_ERROR_RE.fullmatch(text) else None


def version_hint(error: str | None) -> str:
    """
    Name the fix when the API refused a model the VM's Claude predates.

    :param error: A turn's error text.
    :returns: A parenthesised note naming ``sandbox.sbx.claude_version``
        and the version required, or ``''``.
    """
    match = _TOO_OLD_RE.search(error or '')
    if match is None:
        return ''
    have, need = match.groups()
    return (
        f' (the VM runs Claude Code {have}, too old for this model: set '
        f'sandbox.sbx.claude_version to {need} or newer in the server '
        f'config, then restart the server)'
    )


def validate_claude_version(value: object) -> str:
    """
    Return *value* if it is an exact Claude Code version, else raise.

    The version is interpolated into a shell command in the VM, so only
    ``MAJOR.MINOR.PATCH`` digits are accepted: no channel names, no
    ranges, no whitespace.

    :param value: The configured version, e.g. ``"2.1.280"``.
    :returns: *value*, unchanged.
    :raises ValueError: If it is not a string of that exact form.
    """
    if not isinstance(value, str) or not _VERSION_RE.fullmatch(value):
        raise ValueError(
            f'a Claude Code version must be exact, like 2.1.280; '
            f'got {value!r}'
        )
    return value


def build_version_pin_script(version: str) -> str:
    """
    The in-VM shell program that makes ``claude`` report *version*.

    Installs the exact version with npm when ``claude --version``
    reports anything else, including a newer one, then checks again.
    Written for a minimal POSIX ``sh`` (the VM's is dash).

    :param version: The exact version to pin, e.g. ``"2.1.280"``.
    :returns: A ``sh -c`` program printing :data:`PIN_OK_MARKER` and
        the version on success. On failure it exits non-zero and prints
        why: npm's last lines, or the version still reported.
    :raises ValueError: If *version* is not exact.
    """
    want = validate_claude_version(version)
    return (
        f"want='{want}'\n"
        "have=$(claude --version 2>/dev/null | cut -d' ' -f1)\n"
        'if [ "$have" != "$want" ]; then\n'
        '  log=$(mktemp)\n'
        f'  if ! npm install -g --no-audit --no-fund "{NPM_PACKAGE}@$want" '
        '>"$log" 2>&1; then\n'
        '    echo "npm install of Claude Code $want failed:"\n'
        '    tail -n 20 "$log"\n'
        '    exit 3\n'
        '  fi\n'
        'fi\n'
        "now=$(claude --version 2>/dev/null | cut -d' ' -f1)\n"
        'if [ "$now" != "$want" ]; then\n'
        '  echo "claude --version reports ${now:-nothing}, not $want"\n'
        '  exit 4\n'
        'fi\n'
        f'echo "{PIN_OK_MARKER} $now"\n'
    )


def build_settings_seed_script(*, disable_autoupdater: bool = False) -> str:
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

    :param disable_autoupdater: Also set :data:`AUTOUPDATER_ENV` in the
        file's ``env`` block, so a VM pinned to a Claude Code version
        stays on it for the whole run. Existing ``env`` entries are
        kept; an ``env`` that is not a mapping fails loud.
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
        'changed = False\n'
        f'if data.get({SETTINGS_KEY!r}) is not True:\n'
        f'    data[{SETTINGS_KEY!r}] = True\n'
        '    changed = True\n'
        f'if {disable_autoupdater!r}:\n'
        "    env = data.setdefault('env', {})\n"
        '    if not isinstance(env, dict):\n'
        "        sys.exit('claude settings env is not a mapping: %s' % p)\n"
        f"    if env.get({AUTOUPDATER_ENV!r}) != '1':\n"
        f"        env[{AUTOUPDATER_ENV!r}] = '1'\n"
        '        changed = True\n'
        'if changed:\n'
        '    p.parent.mkdir(parents=True, exist_ok=True)\n'
        "    p.write_text(json.dumps(data, indent=2), encoding='utf-8')\n"
        '    os.chmod(p, 0o600)\n'
        f'print({SEED_OK_MARKER!r})\n'
    )
