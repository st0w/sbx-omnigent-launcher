# Harness CLI versions — what a wedged run gets compared against

The launcher drives three CLIs inside the microVM: `claude`, `codex` and `agy`.
None of them is version-pinned, and all three are free to change behaviour
between one run and the next. That is not theoretical — it has cost this project
three separate days:

| What changed | What it looked like |
| --- | --- |
| A codex model was retired | Every turn died at exactly the turn timeout, with no output. The TUI was sitting in a "switch to the new model?" migration picker waiting for a keystroke. |
| Claude Code gained a model-gated `auto` mode | `--permission-mode auto` was silently downgraded to **manual** on Haiku 4.5, and the reviewer blocked on an approval prompt for every tool call. |
| agy's `--model` grammar changed | `gemini-3.5-flash` — valid when the pipeline was written — became `invalid model selection … requires --effort`, killing every agy agent at launch. |

## Known-good set

Recorded from a live microVM on **2026-08-19**, image
`ghcr.io/omnigent-ai/omnigent-host:dev-adcf83cc`:

| CLI | Version |
| --- | --- |
| `@anthropic-ai/claude-code` | 2.1.235 |
| `@openai/codex` | 0.148.0 |
| `agy` (Antigravity CLI) | 1.1.15 |

Read the set out of any running agent VM with:

```bash
sbx exec <sandbox> -- sh -lc 'claude --version; codex --version; agy --version'
```

## Pinning the image is necessary but NOT sufficient

Omnigent's host image installs `@anthropic-ai/claude-code` and `@openai/codex`
unpinned, deliberately (`deploy/docker/Dockerfile`: *"Unpinned on purpose — the
official image is rebuilt by CI, so it tracks the same latest a laptop install
would get"*). Note the asymmetry: kiro-cli and agy in that same file **are**
pinned, with the rationale *"behaviorally coupled to a specific build"* — which
is exactly as true of codex and claude.

But pinning the image would not be enough, because **these CLIs update themselves
at runtime, inside the VM**:

- agy reported **1.0.10** in one VM and **1.1.15** in another, both built from the
  same image tag on the same day. The egress policy carries a global allow rule
  for `antigravity-cli-auto-updater-…run.app`, which is how.
- A Claude pane was observed showing `✔ Update installed · Restart to apply`
  mid-session; `claude --version` then reported 2.1.236 while the image's
  `node_modules` still held 2.1.235 (its updater installs to a user-local path).

So genuinely freezing a harness means BOTH pinning the package in the image
snapshot AND stopping the in-VM updater — removing the auto-updater host from the
egress policy, or setting whatever disable switch each CLI offers. That is a
deliberate trade: a frozen harness cannot pick up a security fix either. It has
NOT been done here; this file exists so the comparison is possible when a run
wedges.

## The operational rule

**A run that times out on the FIRST turn of a harness gets its pane read before
anything else is investigated.**

That rule alone would have saved a full day: three other hypotheses were chased —
all real bugs, none of them the blocker — while the migration picker sat on
screen the whole time. The launcher now captures the pane automatically on a turn
that does not report, to `<run>/turns/<label>.pane.txt`.

The launcher also reads back what a harness actually launched with, after its
first turn, and warns when it differs from what was asked (`[launch] …`). Both
checks exist because none of the three failures above was visible anywhere in the
launcher's own logs.
