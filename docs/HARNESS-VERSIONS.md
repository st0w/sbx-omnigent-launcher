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

Recorded from a fresh microVM on **2026-09-14**, image
`ghcr.io/omnigent-ai/omnigent-host:v0.13.0`, read immediately after creation —
before any in-VM self-update could run:

| CLI | Version |
| --- | --- |
| `@anthropic-ai/claude-code` | 2.1.266 |
| `@openai/codex` | 0.153.4 |
| `agy` (Antigravity CLI) | 1.1.16 |

Models confirmed on that image — one real turn each. The launch read-back checked
the codex row and found no mismatch. Read-back does not check a Claude model, so
the Claude row was confirmed by reading the captured pane's banner by hand:

| Harness | Model | Effort | Pane showed |
| --- | --- | --- | --- |
| `claude-native` | `claude-fable-5-1` | `xhigh` | `Fable 5.1 with xhigh effort` |
| `codex-native` | `gpt-6-astra` | `xhigh` | `model: gpt-6-astra xhigh` |

Read the set out of any running agent VM with:

```bash
sbx exec <sandbox> -- sh -lc '{ claude --version; codex --version; agy --version; } 2>/dev/null'
```

The `2>/dev/null` is not tidiness: codex prints a Node
`[UNDICI-EHPA] Warning: EnvHttpProxyAgent is experimental` line on stderr, and
without the redirect it lands where the version line should be.

### Read it from a guest, never from the host

The workstation's CLIs are not the guest's, and the difference is not cosmetic.
On 2026-09-14 the host had codex **0.148.0** while the v0.13.0 guest had
**0.153.4** — and only the guest's codex knew `gpt-6-astra`. Asking the host
reported a model as unavailable that every agent VM could run. That includes the
server's `/v1/hosts/<host-id>/harnesses/<harness>/model-options`, which proxies to
whichever host it names: point it at the workstation and it answers for the
workstation.

### Previous sets

Add a row whenever the image changes; never overwrite one. The difference between
rows is what a run that wedges right after an upgrade gets compared against.

| Recorded | Image | claude | codex | agy |
| --- | --- | --- | --- | --- |
| 2026-09-14 | `omnigent-host:v0.13.0` | 2.1.266 | 0.153.4 | 1.1.16 |
| 2026-08-19 | `omnigent-host:dev-adcf83cc` (local build) | 2.1.235 | 0.148.0 | 1.1.15 |

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
