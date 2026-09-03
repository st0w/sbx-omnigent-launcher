"""Read a native harness's TUI pane straight out of its microVM.

The launcher drives claude, codex and agy as REAL terminal UIs over
tmux. When one of those stops responding, every channel the launcher
normally listens on goes quiet at once — no ``thread.started`` event, no
assistant message, no error — and the turn dies at its timeout carrying
nothing but the timeout itself. The cause is on the screen the whole
time, and until this module existed nothing ever looked at it.

Three separate days were lost to that in 2026-08 alone (TASKS.md #26):

* a retired codex model left the TUI blocked on a migration picker
  ("Choose how you'd like Codex to proceed"), so every turn failed at
  exactly 30s. Four other hypotheses were chased first; three of them
  were real bugs and none of them was the blocker.
* agy sat in a first-run trust gate (#12), same silence.
* a Haiku reviewer ran in manual mode after ``--permission-mode auto``
  was silently discarded (#28) — the pane footer said ``manual mode on``
  outright, which is what eventually diagnosed it.

So this is not a codex fix or an agy fix. It is the launcher's missing
eyes, and it is deliberately harness-agnostic: every native terminal
Omnigent launches lives on a ``/tmp/omnigent-terminal-*/tmux.sock``
socket in a session named ``main``, so one capture serves all of them
and any harness added later.

It does NOT answer the prompt it finds. Same reasoning as #12: the
orchestrator must not choose on the human's behalf. The goal is a
legible diagnosis, not automation.

Verified live 2026-08-19 against a running claude-native microVM.
"""

from __future__ import annotations

import re
import subprocess

#: Emitted by the in-VM script when no native terminal directory exists
#: — an SDK harness, or a VM whose terminal never launched. Not an
#: error: there is simply no pane to show.
NO_TERMINAL_MARKER = '__omni_no_terminal__'

#: Emitted when the socket is there but tmux would not read it (the
#: server died, the pane is gone). Distinguished from the above so a
#: reader can tell "nothing to capture" from "should have worked".
UNREADABLE_MARKER = '__omni_pane_unreadable__'

#: Where Omnigent puts a native terminal's tmux socket, and the session
#: name inside it. Shared by claude-native, codex-native and
#: antigravity-native — confirmed against all three.
TERMINAL_DIR_GLOB = '/tmp/omnigent-terminal-*/'
TMUX_SESSION = 'main'

#: Scrollback lines captured above the visible pane. The blocking prompt
#: is almost always ON the visible pane; the history is what carries the
#: banner or notice that explains it (a deprecation notice, a mode
#: change). Bounded because this lands in a run artifact a human reads.
DEFAULT_SCROLLBACK_LINES = 120

#: Wall clock for the whole round-trip. Generous because ``sbx exec``
#: auto-starts a stopped box, but bounded: this runs on the failure
#: path, where something is already wrong and teardown is waiting.
DEFAULT_TIMEOUT_S = 30.0


#: A modal prompt's key legend. Every native harness draws one, and the
#: wording differs per CLI, so this matches the KEYS rather than any
#: prose: agy says "up/down Navigate · enter Select · esc Skip", codex
#: "Use up/down to move, press enter to confirm", Claude "Enter to
#: confirm · Esc to cancel". Deliberately never matched against the
#: question text — that is the agent's words, in any language.
_LEGEND_RE = re.compile(
    r'(?:\b(?:up/down|↑/↓|arrow keys)\b'
    r'|\benter\b[^\n]{0,24}\b(?:select|confirm|submit)\b'
    r'|\b(?:esc|escape)\b[^\n]{0,24}\b(?:skip|cancel|exit)\b)',
    re.I,
)

#: A selectable option line: ``1. …``, ``> 2. …``, and the same with
#: a pointer glyph. The
#: leading allowance is generous because a TUI centres its dialogs —
#: Claude's API-key prompt indents its options six columns, which a
#: tighter bound missed. Loose is safe here: this is only ever applied
#: to a pane that already showed a key legend.
_OPTION_RE = re.compile(r'^[^\w\n]{0,10}(\d+)[.)]\s+\S', re.M)

#: How many options make it a picker. Two — a yes/no gate is the most
#: common one and the most fatal, since it blocks at launch.
_MIN_OPTIONS = 2


def modal_prompt(pane: str | None) -> str | None:
    """
    The blocking prompt a pane is showing, or ``None``.

    A native harness sitting in a modal picker is NOT in its composer,
    so there is no input box for a pasted turn to land in: only
    keystrokes can answer it. The turn then fails with a message about
    paste delivery, or simply times out — and in both cases the thing a
    human needs to do is on the screen and nowhere else.

    Matched on the key legend plus numbered options, never on the
    question wording, so it holds for agy's question picker, codex's
    model-migration prompt and Claude's trust and API-key gates alike.

    :param pane: Captured pane text.
    :returns: The prompt's own lines, trimmed to the picker, or ``None``
        when the pane is not showing one.
    """
    if not pane:
        return None
    lines = pane.splitlines()
    legend = [i for i, ln in enumerate(lines) if _LEGEND_RE.search(ln)]
    if not legend:
        return None
    end = legend[-1]
    options = [
        i for i, ln in enumerate(lines[:end]) if _OPTION_RE.match(ln)
    ]
    if len(options) < _MIN_OPTIONS:
        return None
    # Start a couple of lines above the first option, so the question
    # itself comes along without dragging in the whole scrollback.
    start = max(0, options[0] - 2)
    block = [ln.rstrip() for ln in lines[start:end + 1]]
    while block and not block[0].strip():
        block.pop(0)
    return '\n'.join(block) or None


def blocked_on_prompt_message(label: str, prompt: str) -> str:
    """
    What to tell a human whose run is stuck on a modal prompt.

    Leads with the ACTION. The failure they see otherwise describes the
    paste mechanism and buries the thing to do, with the picker dumped
    raw at the end of a RuntimeError (TASKS.md #12).

    :param label: The node that is blocked.
    :param prompt: The prompt text from :func:`modal_prompt`.
    :returns: The message, ready to echo.
    """
    return (
        f'{label} is waiting on an interactive prompt in its TUI, which '
        f'a typed message CANNOT answer — while a picker is open the '
        f'agent is not in its composer, so there is no input box for a '
        f'pasted turn to land in.\n'
        "Open this agent's pane in the Omnigent UI and choose there "
        '(up/down then enter, or esc to skip). Nothing was answered on '
        'your behalf.\n'
        f'It is showing:\n\n{prompt}'
    )


def capture_script(lines: int = DEFAULT_SCROLLBACK_LINES) -> str:
    """
    The shell program that reads the pane, run inside the guest.

    Exits 0 in every case, including "no terminal here", so a missing
    pane is reported as text rather than as a failed command someone
    then has to interpret.

    :param lines: Scrollback lines to include above the visible pane.
    :returns: The shell source.
    """
    return (
        f'S=$(ls -d {TERMINAL_DIR_GLOB} 2>/dev/null | head -1); '
        f'if [ -z "$S" ]; then echo {NO_TERMINAL_MARKER}; exit 0; fi; '
        f'tmux -S "${{S}}tmux.sock" capture-pane -p -S -{int(lines)} '
        f'-t {TMUX_SESSION} 2>&1 || echo {UNREADABLE_MARKER}'
    )


def capture_command(
    sandbox: str, *, lines: int = DEFAULT_SCROLLBACK_LINES
) -> list[str]:
    """
    The ``sbx exec`` argv that captures *sandbox*'s pane.

    :param sandbox: The microVM name, e.g. ``"managed-cb683c32"``.
    :param lines: Scrollback lines to include.
    :returns: The argv.
    """
    return ['sbx', 'exec', sandbox, '--', 'sh', '-lc', capture_script(lines)]


def capture_pane(
    sandbox: str,
    *,
    lines: int = DEFAULT_SCROLLBACK_LINES,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    run: object = subprocess.run,
) -> str | None:
    """
    Capture *sandbox*'s TUI pane, or ``None`` when there is nothing.

    NEVER raises. This runs on a path where a turn has already failed
    and the run is seconds from teardown — a diagnostic that can itself
    fail the run, or hang it, is worse than no diagnostic. Every failure
    mode (no terminal, unreadable pane, sbx missing, sbx timing out)
    collapses to ``None``, and the caller simply omits the section.

    :param sandbox: The microVM name.
    :param lines: Scrollback lines to include above the visible pane.
    :param timeout_s: Whole round-trip budget.
    :param run: Subprocess runner (injected in tests).
    :returns: The pane text with trailing blank lines trimmed, or
        ``None`` when there is no pane to show.
    """
    try:
        proc = run(  # type: ignore[operator]
            capture_command(sandbox, lines=lines),
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout_s,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if getattr(proc, 'returncode', 1) != 0:
        return None
    out = (getattr(proc, 'stdout', '') or '').strip('\n')
    # A pane of nothing but whitespace is no pane. tmux happily returns
    # blank rows for a pane that exists but has rendered nothing, and a
    # run artifact full of spaces reads as a broken capture rather than
    # as "there was nothing to see".
    if not out.strip():
        return None
    if NO_TERMINAL_MARKER in out or UNREADABLE_MARKER in out:
        return None
    return out
