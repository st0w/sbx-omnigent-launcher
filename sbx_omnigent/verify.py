"""
Mechanical verification of a branch, in a disposable microVM.

Every other gate in a pipeline is an agent's WORD — a reviewer's
``VERDICT:`` line, a coder's "the tests pass". None of them executes
anything, which is how an implementation that was never written
collected two independent approvals. This module is the one gate that
does not ask: it runs the project's own test/coverage command and
believes only the exit status.

WHERE it runs is the whole design. Running it on the host would execute
agent-authored code in the trusted plane, alongside the publish token —
a sandbox escape, and the reason the runner's no-op guard is limited to
git inspection. Running it in an agent's own VM would trust an
environment that agent had write access to for an hour (a shimmed
``cargo`` on ``PATH`` defeats the check). So verification gets its OWN
sandbox: created fresh from the branch that is about to publish, given
no credentials of any kind, and destroyed immediately afterwards.

The command is supplied by the pipeline (``verify.command``), so this
stays language-agnostic — the launcher never learns what a test or a
coverage report looks like, only what exit 0 means.
"""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable
from dataclasses import dataclass

import click

from sbx_omnigent.pipeline import _sanitize

#: Max length of a generated sandbox name (kept well under any sbx
#: limit; a long run/node id is truncated rather than rejected).
_NAME_MAX = 56

#: Extra seconds allowed for the ``sbx exec`` round-trip beyond the
#: command's own budget, so the in-VM command hits ITS deadline first
#: and we get its output rather than an opaque outer timeout.
_EXEC_MARGIN_S = 120.0

#: How much command output to carry back into a loop-back finding. A
#: coverage tool's per-file table is the useful part and it is the tail.
_OUTPUT_TAIL = 6000

#: Longest single line kept whole in captured output.
#:
#: The tail is the right thing to keep — a suite's summary and a
#: coverage table are both at the end — right up until ONE line eats
#: the entire budget. A build tool that fails to link prints the whole
#: failing compiler invocation as a single line of ten thousand
#: characters, so tail-capping kept that and threw away the diagnostic
#: explaining it. Measured on a live loop-back: 10,382 characters
#: handed to the writer, not one occurrence of a `file:line` pointer,
#: and the first several hundred characters pure library hashes. It was
#: told which target failed and never why.
#:
#: Clamping costs nothing legible: no human-readable diagnostic is one
#: line this long, and machine-generated ones are exactly what should
#: yield the budget to text a reader can act on.
_MAX_LINE_CHARS = 500

#: Echoed between the prologue and the project's command, so a prologue
#: that died can be told from a command that failed. Without it the two
#: are indistinguishable — both are just a non-zero exit — and a gate
#: whose OWN setup was broken got reported as the branch failing its
#: tests, which re-drove a writer three times over code that was fine.
_SETUP_MARKER = '__omni_verify_setup_ok__'

#: ANSI/VT escapes a test runner emits when it thinks it has a terminal.
#: Stripped because this output is embedded in a pull request, where
#: the raw escapes are unreadable noise.
_ANSI_RE = re.compile(r'\x1b\[[0-9;?]*[ -/]*[@-~]')

#: Best-effort redaction applied to captured output before it can reach
#: a pull request. A demonstration that PROVES a database or TLS
#: connection necessarily prints connection strings, and a throwaway
#: sandbox password is still a bad thing to publish into a repo. Not a
#: security boundary — the sandbox holds no real credentials — just
#: hygiene on the one path that leaves it.
_REDACT_RES = (
    (
        re.compile(
            r'(?i)\b(password|passwd|pwd|token|secret|api[_-]?key)\b'
            r'(\s*[=:]\s*)\S+'
        ),
        r'\1\2***',
    ),
    (re.compile(r'(?i)\bauthorization\s*:\s*\S+'), 'authorization: ***'),
    (re.compile(r'://[^:/@\s]+:[^@/\s]+@'), '://***:***@'),
)


def clamp_lines(text: str, limit: int = _MAX_LINE_CHARS) -> str:
    """
    Shorten individual over-long lines, saying how much was dropped.

    Applied BEFORE the tail cap so one machine-generated line cannot
    consume the whole budget. See :data:`_MAX_LINE_CHARS`.

    :param text: Captured output.
    :param limit: Longest single line kept whole.
    :returns: The text with over-long lines clamped.
    """
    out = []
    for line in text.splitlines():
        if len(line) > limit:
            line = f'{line[:limit]}… [+{len(line) - limit:,} chars]'
        out.append(line)
    return '\n'.join(out)


#: Lines worth rescuing from the middle of a long capture. A gate that
#: fails prints its verdict somewhere; a suite that stands up hermetic
#: Postgres clusters prints hundreds of server log lines around it, and
#: a positional tail keeps the chatter and drops the verdict. Measured
#: live: the whole 6000-character budget spent on "checkpoint
#: complete", "database system is shut down" and socket paths, with the
#: cargo failure nowhere in it — the writer was re-driven to "close the
#: gap" and never told what the gap was.
_SALIENT_RE = re.compile(
    r'(^error(\[[A-Z]\d+\])?:)'          # rustc / cargo
    r'|(^error: )'
    r'|(panicked at)'
    r'|(assertion .*failed)'
    r'|(^failures:)'
    r'|(^---- .* stdout ----)'
    r'|(test result: FAILED)'
    r'|(^FAILED)'
    r'|(error: test failed)'
    r'|(^\s*--> )',                       # rustc's file:line pointer
    re.I | re.M,
)


def salient_tail(text: str, limit: int) -> str:
    """
    Trim *text* to *limit* chars, keeping the lines that say WHY.

    A plain tail is right when the interesting part is at the end — a
    coverage table, a suite summary — and wrong when something noisy
    runs after the failure. So: take the tail as before, then check
    whether any :data:`_SALIENT_RE` line fell outside it, and if so
    spend up to half the budget lifting those lines back in. Nothing is
    dropped that a plain tail would have kept; the only change is that
    a verdict buried mid-capture survives.

    :param text: The captured output.
    :param limit: Character budget.
    :returns: The trimmed text, annotated when lines were lifted.
    """
    if len(text) <= limit:
        return text
    tail = text[-limit:]
    rescued = [
        line
        for line in text[: len(text) - limit].splitlines()
        if _SALIENT_RE.search(line)
    ]
    if not rescued:
        return tail
    head_budget = limit // 2
    kept: list[str] = []
    used = 0
    # Newest-first while filling, so the failure nearest the end wins
    # the budget, then restore reading order.
    for line in reversed(rescued):
        if used + len(line) + 1 > head_budget:
            break
        kept.append(line)
        used += len(line) + 1
    kept.reverse()
    if not kept:
        return tail
    dropped = len(rescued) - len(kept)
    note = (
        f'[{len(kept)} line(s) lifted from earlier in the output '
        f'because they name the failure'
        + (f'; {dropped} more not shown' if dropped else '')
        + ']'
    )
    return (
        note
        + '\n'
        + '\n'.join(kept)
        + '\n\n[end of output:]\n'
        + tail[-(limit - used - len(note) - 32) :]
    )


def _strip_marker(text: str) -> str:
    """
    Drop the setup marker line from output bound for a pull request.

    :param text: Captured output.
    :returns: The text without the marker line.
    """
    return '\n'.join(
        line for line in text.splitlines() if _SETUP_MARKER not in line
    ).strip()


def scrub(text: str) -> str:
    """
    Strip terminal escapes and secret-shaped fragments from output.

    :param text: Raw combined stdout/stderr.
    :returns: Text safe to embed in a document.
    """
    out = _ANSI_RE.sub('', text)
    for pattern, replacement in _REDACT_RES:
        out = pattern.sub(replacement, out)
    return out


@dataclass(frozen=True)
class StepOutcome:
    """
    One command run inside the verification sandbox.

    :param label: Which step this is (``'tests'`` / ``'demo'``).
    :param command: The command as run, for the record.
    :param exit_code: Its exit status (``-1`` on timeout).
    :param output: Tail of the combined stdout/stderr, ANSI stripped.
    :param timed_out: Whether it hit its own deadline.
    """

    label: str
    command: str
    exit_code: int
    output: str
    timed_out: bool = False
    #: The whole capture, line-clamped and scrubbed but NOT tail-cut.
    #: Written beside the run so a diagnostic that did not fit the
    #: finding is still recoverable.
    full_output: str = ''

    @property
    def ok(self) -> bool:
        """Whether the command exited 0."""
        return self.exit_code == 0


class VerifyError(Exception):
    """
    The verification could not be RUN (infrastructure, not a verdict).

    Distinct from a failing gate on purpose: a sandbox that would not
    start says nothing about the branch, so it must fail the run rather
    than loop back to a writer who cannot fix it.
    """


@dataclass(frozen=True)
class VerifyOutcome:
    """
    Result of one verification pass.

    :param ok: Whether the command exited 0.
    :param exit_code: The command's exit status (``-1`` on timeout).
    :param output: Tail of the combined stdout/stderr, for the human
        and for the loop-back finding.
    :param timed_out: Whether the command hit its own deadline.
    :param steps: Every command run, in order, with its own captured
        output — the evidence a pull-request body is built from.
    """

    ok: bool
    exit_code: int
    output: str
    timed_out: bool = False
    steps: tuple[StepOutcome, ...] = ()


def _run_default(
    command: list[str], *, timeout: float | None = None
) -> subprocess.CompletedProcess[str]:
    """Run *command*, capturing output; never raises on non-zero."""
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        timeout=timeout,
    )


def sandbox_name(run_id: str, node_id: str) -> str:
    """
    A collision-safe sbx name for a verification sandbox.

    :param run_id: Pipeline run id.
    :param node_id: The node whose branch is being verified.
    :returns: e.g. ``"verify-discover-m1-refactor"``, truncated.
    """
    return f'verify-{_sanitize(run_id)}-{_sanitize(node_id)}'[:_NAME_MAX]


def build_script(setup: str | None, command: str) -> str:
    """
    The shell program the verification sandbox runs.

    Prepends ``verify.setup`` so the fresh VM installs whatever the
    command needs — otherwise the gate fails for want of a compiler and
    blames the branch. This must be SHELL: the pipeline's top-level
    ``setup:`` is prose addressed to an agent and pasting it here
    produced ``sh: 2: This: not found``, an exit 127 that looked exactly
    like a failing test suite.

    A marker is echoed between the prologue and the command so the two
    failure modes stay distinguishable — see :data:`_SETUP_MARKER`.

    :param setup: The ``verify.setup`` shell, or ``None``.
    :param command: The rendered ``verify.command``.
    :returns: A ``sh -c`` program.
    """
    parts = ['set -e']
    if setup and setup.strip():
        parts.append(setup.strip())
    parts.append(f'echo {_SETUP_MARKER}')
    # The command runs WITHOUT `set -e` inheritance mattering: its own
    # exit status is the verdict, and a multi-line command should stop
    # at its first failure — which `set -e` above already gives it.
    parts.append(command.strip())
    return '\n'.join(parts) + '\n'


def create_command(
    name: str,
    workspace: str,
    *,
    image: str,
    cpus: int | None = None,
    memory: str | None = None,
) -> list[str]:
    """
    The ``sbx create`` argv for a verification sandbox.

    :param name: Sandbox name.
    :param workspace: Host path to mount read-write.
    :param image: Template image to boot.
    :param cpus: CPUs, or ``None`` for sbx's default.
    :param memory: Memory limit, or ``None`` for sbx's default.
    :returns: The argv.
    """
    argv = [
        'sbx', 'create', 'shell', workspace,
        '--template', image, '--name', name, '--quiet',
    ]
    if cpus is not None:
        argv += ['--cpus', str(cpus)]
    if memory:
        argv += ['--memory', memory]
    return argv


def run_verification(
    *,
    name: str,
    workspace: str,
    script: str,
    demo_script: str | None = None,
    image: str,
    egress: tuple[str, ...] = (),
    timeout_s: float = 1800.0,
    cpus: int | None = None,
    memory: str | None = None,
    run: Callable[..., subprocess.CompletedProcess[str]] = _run_default,
) -> VerifyOutcome:
    """
    Run *script* in a fresh sandbox on *workspace*, then destroy it.

    The sandbox is a plain shell box: no agy credential seeding, no
    Omnigent host, no publish token — nothing this process holds is
    passed to it. Its only powers are the mounted worktree and the
    scoped network allowlist needed to install a toolchain.

    Disposal runs in a ``finally`` and never raises: leaking a microVM
    costs disk on every subsequent module, so cleanup must survive the
    failure path it exists for.

    :param name: Sandbox name (see :func:`sandbox_name`).
    :param workspace: Host path to mount read-write.
    :param script: The gate program (see :func:`build_script`).
    :param demo_script: Optional demonstration run AFTER the gate
        passes, in the SAME sandbox — so what it exercises is the code
        the gate just tested, from the same clean checkout. Its output
        becomes the pull request's proof that the thing runs. A non-zero
        exit fails the gate: publishing "proof it works" beside a
        failing demonstration would be worse than not publishing.
    :param image: Template image to boot.
    :param egress: Hosts to allow outbound, scoped to this sandbox.
    :param timeout_s: Budget for the command itself.
    :param cpus: CPUs for the sandbox; ``None`` leaves sbx's default of
        every host CPU. This box is built HERE rather than by the
        server, so the server's per-sandbox limits never reach it —
        and unset, a build sees every host CPU while its memory is
        capped at half the host, which is how the gate's linker got
        killed on a branch that built fine everywhere else.
    :param memory: Memory limit for the sandbox (e.g. ``"8g"``);
        ``None`` leaves sbx's default.
    :param run: Command runner (injected in tests).
    :returns: The :class:`VerifyOutcome`.
    :raises VerifyError: If the sandbox could not be created or scoped.
    """
    created = run(
        create_command(
            name, workspace, image=image, cpus=cpus, memory=memory
        )
    )
    if created.returncode != 0:
        raise VerifyError(
            f'could not create the verification sandbox {name!r} '
            f'(rc={created.returncode}): {(created.stderr or "").strip()}'
        )
    try:
        if egress:
            scoped = run(
                [
                    'sbx', 'policy', 'allow', 'network',
                    '--sandbox', name, ','.join(egress),
                ]
            )
            if scoped.returncode != 0:
                raise VerifyError(
                    f'could not scope egress for {name!r} '
                    f'(rc={scoped.returncode}): '
                    f'{(scoped.stderr or "").strip()}'
                )
        def _step(label: str, program: str) -> StepOutcome:
            try:
                proc = run(
                    ['sbx', 'exec', name, '--', 'sh', '-c', program],
                    timeout=timeout_s + _EXEC_MARGIN_S,
                )
            except subprocess.TimeoutExpired:
                # A command that never terminates is the BRANCH's
                # problem, not infrastructure — report it as a failed
                # gate so the writer gets a chance to fix it.
                return StepOutcome(
                    label=label,
                    command=program,
                    exit_code=-1,
                    output=(
                        f'the {label} command did not finish within '
                        f'{timeout_s:.0f}s and was killed'
                    ),
                    timed_out=True,
                )
            combined = ((proc.stdout or '') + (proc.stderr or '')).strip()
            if _SETUP_MARKER not in combined:
                # The prologue never finished, so the project's command
                # never ran. That says nothing about the branch — fail
                # the RUN rather than loop a writer over someone else's
                # broken setup script.
                raise VerifyError(
                    f'the verification sandbox never reached the '
                    f'{label} command: verify.setup failed (exit '
                    f'{proc.returncode}) — the prologue the gate '
                    f'runs, not the branch. Its output:\n'
                    + salient_tail(
                        clamp_lines(scrub(combined)), _OUTPUT_TAIL
                    )
                )
            return StepOutcome(
                label=label,
                command=program,
                exit_code=proc.returncode,
                output=salient_tail(
                    clamp_lines(_strip_marker(scrub(combined))),
                    _OUTPUT_TAIL,
                ),
                full_output=clamp_lines(_strip_marker(scrub(combined))),
            )

        steps = [_step('tests', script)]
        if steps[0].ok and demo_script:
            # Only demonstrate a branch whose tests actually pass —
            # otherwise the "proof" documents broken behavior.
            steps.append(_step('demo', demo_script))
        failed = next((s for s in steps if not s.ok), None)
        reported = failed or steps[-1]
        return VerifyOutcome(
            ok=failed is None,
            exit_code=reported.exit_code,
            output=reported.output,
            timed_out=reported.timed_out,
            steps=tuple(steps),
        )
    finally:
        try:
            run(['sbx', 'rm', '--force', name])
        except (OSError, subprocess.SubprocessError, click.ClickException):
            pass
