"""Tests for the disposable-sandbox verification gate.

The gate is the one pipeline check that does not take an agent's word,
so what matters here is the sandbox LIFECYCLE: it is created fresh,
scoped, run, and destroyed — including on every failure path, because a
leaked microVM costs disk on every subsequent module. Run:

    .venv/bin/python -m unittest discover -s tests
"""

from __future__ import annotations

import subprocess
import unittest

from sbx_omnigent import verify
from sbx_omnigent.verify import (
    _OUTPUT_TAIL,
    _SETUP_MARKER,
    VerifyError,
    build_script,
    clamp_lines,
    create_command,
    run_verification,
    sandbox_name,
    scrub,
)


class FakeProc:
    """A stand-in for ``subprocess.CompletedProcess``."""

    def __init__(self, returncode=0, stdout='', stderr=''):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class FakeRunner:
    """
    Records argv per call and replays queued results (or an OK).

    An ``exec`` result gets the setup marker prepended, because the real
    script always echoes it once the prologue succeeds. Pass
    *emit_marker=False* to model a prologue that died before reaching
    the project's command.
    """

    def __init__(self, results=None, raises=None, *, emit_marker=True):
        self._results = list(results or [])
        self._raises = list(raises or [])
        self._emit_marker = emit_marker
        self.calls: list[list[str]] = []

    def __call__(self, command, *, timeout=None):
        self.calls.append(list(command))
        if self._raises:
            exc = self._raises.pop(0)
            if exc is not None:
                raise exc
        proc = self._results.pop(0) if self._results else FakeProc(0)
        if command[1] == 'exec' and self._emit_marker:
            return FakeProc(
                proc.returncode,
                f'{_SETUP_MARKER}\n{proc.stdout}',
                proc.stderr,
            )
        return proc

    def verbs(self) -> list[str]:
        """The sbx subcommand of each call, in order."""
        return [c[1] for c in self.calls]


def _run(runner, **kw):
    kw.setdefault('name', 'verify-r1-refactor')
    kw.setdefault('workspace', '/wt/r1/nodes/refactor-verify')
    kw.setdefault('script', 'make test')
    kw.setdefault('image', 'img:tag')
    return run_verification(run=runner, **kw)


class TestSandboxName(unittest.TestCase):
    def test_sanitizes_and_prefixes(self) -> None:
        self.assertEqual(
            sandbox_name('Discover', 'm1-refactor-verify'),
            'verify-discover-m1-refactor-verify',
        )

    def test_truncates_rather_than_rejecting(self) -> None:
        name = sandbox_name('r' * 80, 'n' * 80)
        self.assertLessEqual(len(name), 56)
        self.assertTrue(name.startswith('verify-'))


class TestBuildScript(unittest.TestCase):
    def test_setup_runs_before_the_command(self) -> None:
        script = build_script('install rust', 'cargo test')
        self.assertLess(
            script.index('install rust'), script.index('cargo test')
        )

    def test_exits_on_the_first_failure(self) -> None:
        # A multi-line command must stop at its first failing step,
        # or a later success would mask an earlier failure.
        self.assertTrue(build_script(None, 'a\nb').startswith('set -e'))

    def test_absent_setup_is_omitted(self) -> None:
        script = build_script(None, 'cargo test')
        self.assertEqual(script.strip().splitlines()[-1], 'cargo test')


class TestRunVerification(unittest.TestCase):
    """Lifecycle: create → scope → exec → ALWAYS destroy."""

    def test_passing_command_is_ok(self) -> None:
        # no egress -> calls are create, exec, rm (no policy).
        runner = FakeRunner([FakeProc(0), FakeProc(0, 'all green')])
        outcome = _run(runner)
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.exit_code, 0)
        self.assertIn('all green', outcome.output)

    def test_failing_command_is_not_ok_and_carries_output(self) -> None:
        runner = FakeRunner(
            [FakeProc(0), FakeProc(1, 'lines: 71%', 'below min')]
        )
        outcome = _run(runner)
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.exit_code, 1)
        self.assertIn('lines: 71%', outcome.output)
        self.assertIn('below min', outcome.output)

    def test_full_lifecycle_order(self) -> None:
        runner = FakeRunner()
        _run(runner, egress=('crates.io',))
        self.assertEqual(runner.verbs(), ['create', 'policy', 'exec', 'rm'])

    def test_no_egress_skips_the_policy_call(self) -> None:
        runner = FakeRunner()
        _run(runner)
        self.assertEqual(runner.verbs(), ['create', 'exec', 'rm'])

    def test_sandbox_is_destroyed_after_a_failing_command(self) -> None:
        runner = FakeRunner([FakeProc(0), FakeProc(3)])
        _run(runner)
        self.assertEqual(runner.calls[-1][:3], ['sbx', 'rm', '--force'])

    def test_sandbox_is_destroyed_after_a_timeout(self) -> None:
        # THE path that matters: a hung suite must not leak its VM.
        runner = FakeRunner(
            raises=[None, subprocess.TimeoutExpired(['sbx'], 1), None]
        )
        outcome = _run(runner)
        self.assertFalse(outcome.ok)
        self.assertTrue(outcome.timed_out)
        self.assertEqual(runner.verbs()[-1], 'rm')

    def test_a_failed_dispose_never_masks_the_outcome(self) -> None:
        runner = FakeRunner(
            raises=[None, None, OSError('daemon wedged')]
        )
        outcome = _run(runner)
        self.assertTrue(outcome.ok)  # the verdict still stands

    def test_create_failure_is_infrastructure_not_a_verdict(self) -> None:
        # It says nothing about the branch, so it must NOT look like a
        # failing gate (which would loop a writer back over it).
        runner = FakeRunner([FakeProc(1, '', 'no such template')])
        with self.assertRaises(VerifyError) as ctx:
            _run(runner)
        self.assertIn('no such template', str(ctx.exception))
        self.assertEqual(runner.verbs(), ['create'])  # never exec'd

    def test_scope_failure_still_destroys_the_sandbox(self) -> None:
        runner = FakeRunner([FakeProc(0), FakeProc(1, '', 'bad host')])
        with self.assertRaises(VerifyError):
            _run(runner, egress=('crates.io',))
        self.assertEqual(runner.verbs()[-1], 'rm')

    def test_the_command_runs_in_the_sandbox_not_the_host(self) -> None:
        runner = FakeRunner()
        _run(runner, name='verify-x', script='cargo test')
        exec_call = next(c for c in runner.calls if c[1] == 'exec')
        self.assertEqual(exec_call[:2], ['sbx', 'exec'])
        self.assertEqual(exec_call[2], 'verify-x')
        self.assertIn('cargo test', exec_call[-1])

    def test_output_is_tailed_not_unbounded(self) -> None:
        runner = FakeRunner([FakeProc(0), FakeProc(1, 'x' * 20000)])
        outcome = _run(runner)
        self.assertLessEqual(len(outcome.output), 6000)

    def test_exec_budget_exceeds_the_commands_own(self) -> None:
        # The in-VM command must hit ITS deadline first, so we get its
        # output instead of an opaque outer kill.
        seen = {}

        def rec(command, *, timeout=None):
            if command[1] == 'exec':
                seen['timeout'] = timeout
                return FakeProc(0, _SETUP_MARKER)
            return FakeProc(0)

        _run(rec, timeout_s=600.0)
        self.assertGreater(seen['timeout'], 600.0)


class TestDemonstrationStep(unittest.TestCase):
    """A passing suite shows the code satisfies its tests; it does not
    show a reviewer the thing running. The demo does."""

    def _run(self, results, **kw):
        runner = FakeRunner(results)
        outcome = run_verification(
            name='verify-x', workspace='/wt/x', script='make check',
            demo_script='./scripts/demo.sh', image='img', run=runner, **kw
        )
        return outcome, runner

    def test_both_steps_are_captured_separately(self) -> None:
        outcome, _r = self._run(
            [FakeProc(0), FakeProc(0, '42 passed'), FakeProc(0, 'TLS1.3 ok')]
        )
        self.assertTrue(outcome.ok)
        self.assertEqual([s.label for s in outcome.steps], ['tests', 'demo'])
        self.assertEqual(outcome.steps[0].output, '42 passed')
        self.assertEqual(outcome.steps[1].output, 'TLS1.3 ok')
        self.assertEqual(outcome.steps[1].command, './scripts/demo.sh')

    def test_it_runs_in_the_same_sandbox_as_the_gate(self) -> None:
        # A demonstration against a DIFFERENT checkout proves nothing
        # about the branch the gate just tested.
        _outcome, runner = self._run(
            [FakeProc(0), FakeProc(0), FakeProc(0)]
        )
        self.assertEqual(
            runner.verbs(), ['create', 'exec', 'exec', 'rm']
        )
        names = {c[2] for c in runner.calls if c[1] == 'exec'}
        self.assertEqual(names, {'verify-x'})

    def test_a_failing_gate_skips_the_demonstration(self) -> None:
        # Demonstrating a branch whose tests fail would document broken
        # behavior as if it were proof.
        outcome, runner = self._run([FakeProc(0), FakeProc(1, 'boom')])
        self.assertFalse(outcome.ok)
        self.assertEqual([s.label for s in outcome.steps], ['tests'])
        self.assertEqual(runner.verbs().count('exec'), 1)

    def test_a_failing_demonstration_fails_the_gate(self) -> None:
        # Publishing "proof it works" beside a failing demo is worse
        # than not publishing.
        outcome, _r = self._run(
            [FakeProc(0), FakeProc(0, 'passed'), FakeProc(3, 'refused')]
        )
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.exit_code, 3)
        self.assertIn('refused', outcome.output)

    def test_the_sandbox_is_destroyed_after_a_demo(self) -> None:
        _outcome, runner = self._run(
            [FakeProc(0), FakeProc(0), FakeProc(1, 'nope')]
        )
        self.assertEqual(runner.verbs()[-1], 'rm')

    def test_no_demo_configured_runs_one_step(self) -> None:
        runner = FakeRunner([FakeProc(0), FakeProc(0, 'passed')])
        outcome = run_verification(
            name='verify-x', workspace='/wt/x', script='make check',
            image='img', run=runner,
        )
        self.assertEqual([s.label for s in outcome.steps], ['tests'])
        self.assertEqual(runner.verbs().count('exec'), 1)


class TestBrokenPrologue(unittest.TestCase):
    """A gate whose OWN setup fails says nothing about the branch. It
    used to be indistinguishable from a failing suite: the pipeline's
    prose setup: was pasted into the script, died on its first word with
    exit 127, and the runner closed the "finding" by re-driving a writer
    three times before failing the run."""

    def test_a_prologue_failure_is_infrastructure_not_a_verdict(self) -> None:
        runner = FakeRunner(
            [FakeProc(0), FakeProc(127, '', 'sh: 2: This: not found')],
            emit_marker=False,
        )
        with self.assertRaises(VerifyError) as ctx:
            _run(runner)
        msg = str(ctx.exception)
        self.assertIn('never reached', msg)
        self.assertIn('not the branch', msg)
        self.assertIn('This: not found', msg)  # carries what broke

    def test_the_sandbox_is_still_destroyed(self) -> None:
        runner = FakeRunner([FakeProc(0), FakeProc(127)], emit_marker=False)
        with self.assertRaises(VerifyError):
            _run(runner)
        self.assertEqual(runner.verbs()[-1], 'rm')

    def test_the_marker_is_echoed_between_setup_and_command(self) -> None:
        script = build_script('apt-get install -y cc', 'make test')
        lines = [ln for ln in script.splitlines() if ln]
        self.assertLess(
            lines.index(f'echo {_SETUP_MARKER}'), lines.index('make test')
        )
        self.assertGreater(
            lines.index(f'echo {_SETUP_MARKER}'),
            lines.index('apt-get install -y cc'),
        )

    def test_no_setup_still_marks_the_boundary(self) -> None:
        # Even with no prologue the marker must be there, or every gate
        # would read as a prologue failure.
        self.assertIn(_SETUP_MARKER, build_script(None, 'make test'))

    def test_the_marker_never_reaches_the_pull_request(self) -> None:
        runner = FakeRunner([FakeProc(0), FakeProc(0, '42 passed')])
        outcome = _run(runner)
        self.assertNotIn(_SETUP_MARKER, outcome.output)
        self.assertEqual(outcome.steps[0].output, '42 passed')


def _link_failure_output() -> str:
    """
    A build that fails to LINK, in the shape a real one has.

    The diagnostic comes first and the failing compiler invocation —
    one enormous line — comes last, which is what made tail-capping
    throw away the only part a reader can act on.
    """
    libs = ','.join(f'lib{n}_crate-{n:016x}' for n in range(600))
    return '\n'.join(
        [f'   Compiling crate_{n} v1.0.0' for n in range(80)]
        + [
            'error[E0308]: mismatched types',
            '   --> providers/aws/src/lib.rs:412:19',
            '    |',
            '412 |         let out = res.groups();',
            '    |                   ^^^^^^^^^^^^ expected `Option`',
            '',
            'error: linking with `cc` failed: signal: 9 (SIGKILL)',
            f'  = note: rustc --edition=2021 --extern {libs}',
            'error: could not compile `discover-aws` (test "x") '
            'due to 1 previous error',
        ]
    )


class TestOversizedLines(unittest.TestCase):
    """One machine-generated line must not eat the whole output budget.
    Measured live: 10,382 characters of loop-back evidence, not one
    file:line pointer in it — the writer was told WHICH target failed
    and never why."""

    def test_a_short_line_is_untouched(self) -> None:
        text = 'error[E0308]: mismatched types\n  --> src/lib.rs:1:1'
        self.assertEqual(clamp_lines(text), text)

    def test_an_over_long_line_says_what_it_dropped(self) -> None:
        out = clamp_lines('x' * 9000)
        self.assertLess(len(out), 9000)
        self.assertIn('chars]', out)

    def test_only_the_offending_line_is_clamped(self) -> None:
        text = f'keep me\n{"x" * 9000}\nkeep me too'
        lines = clamp_lines(text).splitlines()
        self.assertEqual(lines[0], 'keep me')
        self.assertEqual(lines[2], 'keep me too')
        self.assertLess(len(lines[1]), 9000)

    def test_the_diagnostic_survives_a_link_failure(self) -> None:
        # THE regression, end to end: tail-capping raw output kept the
        # linker invocation and cut the error. Both must now survive.
        raw = _link_failure_output()
        self.assertGreater(len(raw), _OUTPUT_TAIL)  # capping is in play
        kept = clamp_lines(raw)[-_OUTPUT_TAIL:]
        self.assertIn('error[E0308]: mismatched types', kept)
        self.assertIn('providers/aws/src/lib.rs:412:19', kept)
        self.assertIn('could not compile', kept)

    def test_the_old_behaviour_really_did_lose_it(self) -> None:
        # Pins that this test proves something: tail-capping the RAW
        # output keeps the library hashes and drops the diagnostic.
        raw = _link_failure_output()
        self.assertNotIn(
            'error[E0308]', raw[-_OUTPUT_TAIL:]
        )

    def test_a_gate_failure_carries_the_diagnostic(self) -> None:
        runner = FakeRunner(
            [FakeProc(0), FakeProc(101, _link_failure_output())]
        )
        outcome = _run(runner)
        self.assertFalse(outcome.ok)
        self.assertIn('error[E0308]: mismatched types', outcome.output)
        self.assertIn('src/lib.rs:412:19', outcome.output)


class TestSandboxResources(unittest.TestCase):
    """The gate builds its own sandbox, so the SERVER's per-sandbox
    limits never reach it. Unset, sbx gives a guest every host CPU with
    memory capped at half the host — which killed the gate's linker on
    a branch that built and tested cleanly in a capped agent VM, and
    the runner read that as the BRANCH failing."""

    def test_limits_reach_the_create_call(self) -> None:
        argv = create_command(
            'v', '/ws', image='img', cpus=4, memory='8g'
        )
        self.assertIn('--cpus', argv)
        self.assertEqual(argv[argv.index('--cpus') + 1], '4')
        self.assertEqual(argv[argv.index('--memory') + 1], '8g')

    def test_unset_leaves_sbx_defaults_alone(self) -> None:
        argv = create_command('v', '/ws', image='img')
        self.assertNotIn('--cpus', argv)
        self.assertNotIn('--memory', argv)

    def test_they_reach_the_real_create(self) -> None:
        runner = FakeRunner()
        _run(runner, cpus=2, memory='6g')
        create = next(c for c in runner.calls if c[1] == 'create')
        self.assertEqual(create[create.index('--cpus') + 1], '2')
        self.assertEqual(create[create.index('--memory') + 1], '6g')


class TestScrub(unittest.TestCase):
    """Captured output ends up in a repository; a connection proof
    prints connection strings."""

    def test_terminal_escapes_are_stripped(self) -> None:
        self.assertEqual(scrub('\x1b[32mok\x1b[0m'), 'ok')

    def test_credentials_in_a_url_are_masked(self) -> None:
        self.assertEqual(
            scrub('postgres://bob:hunter2@db:5432/x'),
            'postgres://***:***@db:5432/x',
        )

    def test_secret_shaped_assignments_are_masked(self) -> None:
        for raw in ('password=hunter2', 'API_KEY: abc123', 'token = xyz'):
            with self.subTest(raw=raw):
                self.assertNotIn('hunter2', scrub(raw))
                self.assertIn('***', scrub(raw))

    def test_ordinary_output_is_untouched(self) -> None:
        text = 'test result: ok. 42 passed; 0 failed'
        self.assertEqual(scrub(text), text)

    def test_the_scrub_reaches_captured_output(self) -> None:
        runner = FakeRunner(
            [FakeProc(0), FakeProc(0, 'dsn=postgres://u:p@h/db')]
        )
        outcome = run_verification(
            name='v', workspace='/w', script='s', image='i', run=runner
        )
        self.assertIn('***', outcome.steps[0].output)
        self.assertNotIn(':p@', outcome.steps[0].output)



class TestSalientTail(unittest.TestCase):
    """
    A capped tail keeps whatever ran LAST, which on this project's
    suite is Postgres shutdown chatter — so the cargo verdict was
    dropped and a writer was re-driven to "close the gap" with no idea
    what the gap was (TASKS.md #42).
    """

    NOISE = '\n'.join(
        f'2026-08-23 23:00:0{i % 10} UTC [231{i:02d}] LOG:  checkpoint '
        f'complete: wrote 971 buffers (5.9%); 0 WAL file(s) added'
        for i in range(80)
    )
    VERDICT = (
        '---- storage::diff stdout ----\n'
        "thread 'diff' panicked at core/tests/storage_diff_test.rs:88:9:\n"
        'assertion `left == right` failed\n'
        'failures:\n'
        'test result: FAILED. 414 passed; 1 failed\n'
    )

    def test_it_rescues_the_verdict_a_plain_tail_would_drop(self) -> None:
        text = self.VERDICT + self.NOISE
        self.assertNotIn('panicked at', text[-900:])  # the old behaviour
        out = verify.salient_tail(text, 900)
        self.assertIn('panicked at', out)
        self.assertIn('storage_diff_test.rs:88:9', out)
        self.assertIn('test result: FAILED', out)

    def test_it_still_keeps_the_end_of_the_output(self) -> None:
        # The tail is right for a coverage table; rescuing must ADD to
        # it, never replace it.
        out = verify.salient_tail(self.VERDICT + self.NOISE, 900)
        self.assertIn('checkpoint', out)

    def test_it_respects_the_budget(self) -> None:
        out = verify.salient_tail(self.VERDICT + self.NOISE, 900)
        self.assertLessEqual(len(out), 900)

    def test_short_output_is_returned_whole(self) -> None:
        self.assertEqual(verify.salient_tail('boom', 900), 'boom')

    def test_no_salient_lines_degrades_to_a_plain_tail(self) -> None:
        out = verify.salient_tail(self.NOISE, 500)
        self.assertEqual(out, self.NOISE[-500:])

if __name__ == '__main__':
    unittest.main()
