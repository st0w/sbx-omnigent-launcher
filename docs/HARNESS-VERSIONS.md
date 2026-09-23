# Harness CLI versions — what a wedged run gets compared against

The launcher drives three CLIs inside the microVM: `claude`, `codex` and `agy`.
codex and agy are not version-pinned. Claude Code is only when
`sbx.claude_version` is set (see [Pinning Claude Code](#pinning-claude-code-sbxclaude_version)).
An unpinned CLI is free to change behaviour between one run and the next. That is
not theoretical — it has cost this project four separate days:

| What changed | What it looked like |
| --- | --- |
| A codex model was retired | Every turn died at exactly the turn timeout, with no output. The TUI was sitting in a "switch to the new model?" migration picker waiting for a keystroke. |
| Claude Code gained a model-gated `auto` mode | `--permission-mode auto` was silently downgraded to **manual** on Haiku 4.5, and the reviewer blocked on an approval prompt for every tool call. |
| agy's `--model` grammar changed | `gemini-3.5-flash` — valid when the pipeline was written — became `invalid model selection … requires --effort`, killing every agy agent at launch. |
| A model needed a newer Claude Code than the image | Every turn of an Opus 5.5 writer replied `API Error: 400 Claude Code 2.1.266 does not support this model; version 2.1.280 or newer is required`. The reply reached the runner before the server's failed status, so the runner took each turn for success and failed the stage 20 minutes later as a writer that "produced no implementation". The runner now fails such a turn at once and names the pin. |

## Known-good set

Recorded on **2026-09-23**: image `ghcr.io/omnigent-ai/omnigent-host:v0.13.0`
with `sbx.claude_version: "2.1.280"`. The image itself carries Claude Code
2.1.266; the pin moves every Claude VM to 2.1.280 before its host starts. codex
and agy come from the unchanged image: read from a fresh VM on 2026-09-14 and
again by a pipeline run on 2026-09-22.

| CLI | Version |
| --- | --- |
| `@anthropic-ai/claude-code` | 2.1.280 |
| `@openai/codex` | 0.153.4 |
| `agy` (Antigravity CLI) | 1.1.16 |

Models confirmed on that image — one real turn each:

| Harness | Model | Effort | How | Recorded |
| --- | --- | --- | --- | --- |
| `claude-native` | `claude-opus-5-5` | default | `claude -p` in a v0.13.0 VM after `npm install -g @anthropic-ai/claude-code@2.1.280` replied `ok`. Before the install, the same VM gave the 400 above. | 2026-09-23 |
| `claude-native` | `claude-fable-5-1` | `xhigh` | the pane banner read `Fable 5.1 with xhigh effort`, on Claude Code 2.1.266 | 2026-09-14 |
| `codex-native` | `gpt-6-astra` | `xhigh` | launch read-back: `model: gpt-6-astra xhigh` | 2026-09-14 |

The Opus 5.5 row has not yet been through a pipeline turn, whose launch read-back
checks the pane banner's model and effort. The Fable 5.1 row predates the pin.

Read the set out of any running agent VM with:

```bash
sbx exec <sandbox> -- sh -lc '{ claude --version; codex --version; agy --version; } 2>/dev/null'
```

The `2>/dev/null` is not tidiness: codex prints a Node
`[UNDICI-EHPA] Warning: EnvHttpProxyAgent is experimental` line on stderr, and
without the redirect it lands where the version line should be.

The pipeline runner reads the same set out of every session's VM after its first
turn, and again when a turn fails. It records the result under `harness_versions`
in the run state, keyed by session label. It prints a `[versions]` warning when the
CLI a session drives:

- is not the version in the known-good set above (once per CLI per run);
- differs from an earlier session in the same run that drives the same CLI;
- differs from the version that session had at its first turn.

A failed turn's `turns/<label>.pane.txt` also names the installed versions. It notes
when the pane shows an update waiting on a restart, in which case the running
process is older than the version that was read.

### Read it from a guest, never from the host

The workstation's CLIs are not the guest's, and the difference is not cosmetic.
On 2026-09-14 the host had codex **0.148.0** while the v0.13.0 guest had
**0.153.4** — and only the guest's codex knew `gpt-6-astra`. Asking the host
reported a model as unavailable that every agent VM could run. That includes the
server's `/v1/hosts/<host-id>/harnesses/<harness>/model-options`, which proxies to
whichever host it names: point it at the workstation and it answers for the
workstation.

### Previous sets

Add a row whenever the image changes; never overwrite one. When the known-good set
above changes, update `KNOWN_GOOD` and `KNOWN_GOOD_RECORDED` in
`sbx_omnigent/harness_versions.py` to match; a test fails until they agree. The difference between
rows is what a run that wedges right after an upgrade gets compared against.

| Recorded | Image | claude | codex | agy |
| --- | --- | --- | --- | --- |
| 2026-09-23 | `omnigent-host:v0.13.0` + `claude_version: 2.1.280` | 2.1.280 | 0.153.4 | 1.1.16 |
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
deliberate trade: a frozen harness cannot pick up a security fix either. For
codex and agy it has NOT been done here; this file exists so the comparison is
possible when a run wedges.

### Pinning Claude Code: `sbx.claude_version`

Claude Code can be pinned without waiting for a new image. A model can need a
newer Claude Code than the image carries: on the `v0.13.0` image, every turn of
an Opus 5.5 agent failed with `API Error: 400 Claude Code 2.1.266 does not
support this model; version 2.1.280 or newer is required`. That error came back
as the turn's reply, so the run looked like an agent that did nothing.

```yaml
sandbox:
  sbx:
    claude_version: "2.1.280"
```

With it set, every Claude VM runs `npm install -g @anthropic-ai/claude-code@<pin>`
before its host starts (skipped when the image already carries the pin), checks
that `claude --version` then reports it, and fails the VM loudly if not. It also
sets `DISABLE_AUTOUPDATER=1` in the VM's Claude settings, so no VM moves off the
pin mid-run. Codex and agy VMs are untouched.

- The install needs `registry.npmjs.org`, which the default egress allows. A
  custom `sbx.egress_allow` without it is refused at startup.
- Every Claude VM pays for the download.
- Only pipeline and swarm VMs are pinned. A session started from the Omnigent UI
  takes a different path and keeps the image's version.
- Bump the pin deliberately, after a run on the new version, and record it in the
  known-good set above.

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
