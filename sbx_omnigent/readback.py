"""Check what a harness ACTUALLY launched with, against what we asked.

Four separate failures in two days, all the same shape: the launcher
asks for something, the CLI accepts the flag, and quietly does something
else. Nothing in the launcher's own logs said so; every one was visible
on the agent's screen (TASKS.md #27, #28, #34, #35):

* ``--permission-mode auto`` on Haiku 4.5 → the session ran in MANUAL
  mode and blocked on an approval prompt for every tool call. Auto mode
  needs a model-side risk classifier Haiku does not implement, so the
  requested mode was discarded.
* ``reasoning_effort=xhigh`` on codex-native → the turn ran at codex's
  DEFAULT. The value was accepted, persisted, and returned by the API.
* ``--model gemini-3.7-flash-high`` on agy → Gemini 3.6 Flash served the
  turn. The flag was accepted without error.
* a retired ``--model`` on codex → the TUI sat in a migration picker and
  every turn died at the timeout.

So this module reads the answer back off the pane that #26 already
captures, and compares. It is deliberately CONSERVATIVE: a signal it
cannot find is reported as "could not verify", never as a mismatch. A
read-back that cries wolf gets switched off, and then the next silent
substitution costs another day.

Pure functions over pane text — no I/O — so every case below is a real
captured pane in the tests.
"""

from __future__ import annotations

import re

#: Claude's footer states the effective permission mode outright. The
#: indicator is the ONLY place the downgrade showed: argv, the settings
#: file and the API all still said ``auto``.
_CLAUDE_MODES = {
    'auto': re.compile(r'\bauto mode on\b', re.I),
    'manual': re.compile(r'\bmanual mode on\b', re.I),
    'dontAsk': re.compile(r"\bdon'?t ask on\b", re.I),
    'acceptEdits': re.compile(r'\baccept edits on\b', re.I),
    'plan': re.compile(r'\bplan mode on\b', re.I),
    'bypassPermissions': re.compile(r'\bbypass permissions on\b', re.I),
}

#: Codex prints ``model: <slug> <effort>`` in its session header and
#: repeats it in the status bar. ``default`` is codex's own word for
#: "no effort pinned", which is exactly the #34 symptom.
#: The leading run is anything-but-word-characters on purpose: the
#: header is inside a box-drawn frame, so the line really begins with
#: U+2502 (a BOX DRAWINGS LIGHT VERTICAL), not an ASCII pipe.
_CODEX_HEADER = re.compile(
    r'^[^\w\n]*model:\s*(?P<model>[\w.\-]+)'
    r'(?:[^\S\n]+(?P<effort>[\w-]+))?',
    re.I | re.M,
)

#: agy names the SERVED model in its banner and status bar, e.g.
#: ``Gemini 3.6 Flash (High)`` or ``Gemini 3.7 Flash · high``.
_AGY_MODEL = re.compile(
    r'\bGemini\s+([\d.]+)\s+(Flash|Pro)\b(?:\s*[(·]\s*(low|medium|high)\)?)?',
    re.I,
)


def _fold(text: str) -> str:
    """Fold a model id or display name to a comparable core.

    ``gemini-3.6-flash-high`` and ``Gemini 3.6 Flash (High)`` are the
    same model written two ways; comparing them means dropping
    everything that is presentation.

    :param text: A model id or display name.
    :returns: Lowercase alphanumerics only.
    """
    return re.sub(r'[^a-z0-9]', '', text.lower())


def claude_permission_mode(pane: str) -> str | None:
    """
    The permission mode Claude's footer reports, or ``None``.

    :param pane: Captured pane text.
    :returns: One of the mode names, or ``None`` when the footer is not
        in view (a pane captured before the TUI finished drawing).
    """
    for mode, rx in _CLAUDE_MODES.items():
        if rx.search(pane):
            return mode
    return None


def codex_model_effort(pane: str) -> tuple[str | None, str | None]:
    """
    The model and effort codex's header reports.

    :param pane: Captured pane text.
    :returns: ``(model, effort)``; either may be ``None``. An effort of
        ``'default'`` is codex's own word for "nothing pinned" and is
        returned verbatim, because that IS the finding.
    """
    m = _CODEX_HEADER.search(pane)
    if m is None:
        return None, None
    return m.group('model'), m.group('effort')


def agy_model(pane: str) -> str | None:
    """
    The model agy reports SERVING, folded for comparison.

    :param pane: Captured pane text.
    :returns: e.g. ``'gemini36flashhigh'``, or ``None`` when the banner
        and status bar are both out of view.
    """
    m = _AGY_MODEL.search(pane)
    if m is None:
        return None
    parts = [f'gemini{m.group(1)}', m.group(2)]
    if m.group(3):
        parts.append(m.group(3))
    return _fold(''.join(parts))


def launch_mismatches(
    harness: str | None,
    pane: str,
    *,
    model: str | None = None,
    effort: str | None = None,
    permission_mode: str | None = None,
) -> list[str]:
    """
    What the harness actually got, where it differs from the request.

    :param harness: The agent's harness id.
    :param pane: Captured pane text; an empty pane yields ``[]``.
    :param model: The model that was requested, if any.
    :param effort: The reasoning effort that was requested, if any.
    :param permission_mode: The permission mode that was requested.
    :returns: Human-readable mismatches. Empty means "nothing
        contradicted the request" — which includes the case where
        nothing could be read at all.
    """
    if not pane or not pane.strip():
        return []
    if harness in ('antigravity-native', 'antigravity', 'agy'):
        return _agy_mismatches(pane, model)
    if harness in ('codex-native', 'codex', 'native-codex'):
        return _codex_mismatches(pane, model, effort)
    return _claude_mismatches(pane, permission_mode)


def _agy_mismatches(pane: str, model: str | None) -> list[str]:
    """agy substitutes a nearby model in silence; compare the name."""
    if not model:
        return []
    served = agy_model(pane)
    if served is None or served == _fold(model):
        return []
    return [
        f'model: asked for {model!r}, but the pane reports a DIFFERENT '
        f'model serving this session. agy accepts an unavailable model '
        f'id without error and quietly serves a nearby one.'
    ]


def _codex_mismatches(
    pane: str, model: str | None, effort: str | None
) -> list[str]:
    """codex reports both in its header; the effort is what lies."""
    got_model, got_effort = codex_model_effort(pane)
    out: list[str] = []
    if model and got_model and _fold(got_model) != _fold(model):
        out.append(
            f'model: asked for {model!r}, pane reports {got_model!r}.'
        )
    if effort and got_effort and _fold(got_effort) != _fold(effort):
        out.append(
            f'reasoning effort: asked for {effort!r}, pane reports '
            f'{got_effort!r}.'
        )
    return out


def _claude_mismatches(pane: str, requested: str | None) -> list[str]:
    """Claude downgrades a mode its model cannot support, in silence."""
    if not requested:
        return []
    got = claude_permission_mode(pane)
    if got is None or got == requested:
        return []
    return [
        f'permission mode: asked for {requested!r}, pane reports '
        f'{got!r}. An unattended agent in a prompting mode blocks on '
        f'the first tool call and never returns.'
    ]
