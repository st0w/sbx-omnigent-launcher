# Codex smoke — a Codex writer with a Claude reviewer

The smallest pipeline that exercises the **`codex-native`** harness end to end.
Codex writes; Claude reviews and must approve; the branch is published.

```
build (codex-native) ──▶ review (claude-native, must APPROVE) ──▶ verify ──▶ publish
                         └── blocks? loop findings back to build ──┘
```

**Why Codex is the writer, not the reviewer:** writing is the path with the most
moving parts — the credential is seeded into the microVM over stdin, codex is
launched with its own approvals flag, a real terminal turn runs, files are
written, and the settle-and-commit has to catch them. A reviewer only reads. The
Claude half is the already-proven side, so it takes the easy job.

The task is trivial on purpose. This is a plumbing test, not a coding test.

## Setup

The top-level [README](../../README.md) foundation, plus **Codex credentials**:

1. Install the launcher into your Omnigent environment.
2. Add the `sandbox:` block to your server config (see
   [`../../config.sample.yaml`](../../config.sample.yaml)); set an absolute
   `sbx.worktree_root`.
3. The egress **network policy** (`sbx policy allow network …`), which must
   include `auth.openai.com` and `*.openai.com`.
4. `codex login` **on the host**. The launcher reads that credential and seeds
   it into each VM over stdin — never in argv, never in the environment. It
   refuses to start if the token is expired or expires within six hours, and
   names the exact re-auth command (`codex login --device-auth`).
5. **Claude** credentials for the reviewer, as in
   [`../quickstart/`](../quickstart/).

If your server config injects an `OPENAI_API_KEY` into the VM, add it to
`sbx.unset_env` — it shadows the subscription login and codex will use the key
instead, silently.

## The scratch repo

`repo:` wants a throwaway git repo with two files. Create it anywhere:

```bash
mkdir codex-smoke-src && cd codex-smoke-src && git init -b main

cat > durations.py <<'EOF'
"""Duration parsing helpers."""


def parse_duration(spec):
    """Parse a duration spec into whole seconds.

    Only a bare number of seconds is supported so far, e.g. "90" -> 90.
    """
    spec = spec.strip()
    if not spec:
        raise ValueError("empty duration")
    return int(spec)
EOF

cat > test_durations.py <<'EOF'
import unittest

from durations import parse_duration


class ParseDurationTest(unittest.TestCase):
    def test_bare_seconds(self):
        self.assertEqual(parse_duration("90"), 90)

    def test_surrounding_whitespace_is_ignored(self):
        self.assertEqual(parse_duration("  90  "), 90)

    def test_empty_is_an_error(self):
        with self.assertRaises(ValueError):
            parse_duration("   ")


if __name__ == "__main__":
    unittest.main()
EOF

git add -A && git commit -m "seed: parse_duration accepts bare seconds"
```

Then set `repo:` in [`pipeline.yaml`](./pipeline.yaml) to that directory.

## Run it

```bash
# 1. Start the server WITH the pipeline (registers build + reviewer).
omni-sbx server -c <your-config.yaml> --pipeline examples/codex-smoke/pipeline.yaml

# 2. Fire the run.
python -m sbx_omnigent.runner -c examples/codex-smoke/pipeline.yaml \
    --run-id codex-smoke-1
```

A pass leaves `pipeline/codex-smoke-1` on the scratch repo with `durations.py`
extended, new tests in `test_durations.py`, the three original tests untouched,
and the reviewer's write-up under `docs/plans/`.

## Two traps this example is shaped around

Both were found the hard way, and both are silent — nothing in the config layer
can see either coming.

**A deprecated Codex model wedges the run.** Model names age out. A retired one
is still accepted by `--model`, but the TUI interrupts startup with a migration
picker and waits for a keystroke that never comes, so the turn dies at the
timeout with no output at all. If every turn fails at exactly the timeout,
**read the agent's pane before investigating anything else** — the blocking menu
is sitting right there.

**A Haiku reviewer is not unattended.** Claude Code silently downgrades
`--permission-mode auto` to manual when the session model is Haiku 4.5: auto
mode runs a model-side risk and prompt-injection classifier that Haiku does not
implement, so the requested mode is discarded with no warning and no log line.
The agent then stops on an approval prompt for every tool call and waits for a
human. Sonnet is the cheapest model verified to hold auto mode, which is why the
reviewer here is pinned to it despite the task being trivial.
