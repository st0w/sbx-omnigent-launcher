# Antigravity (`agy`) support — design + debugging record

**Status:** working end-to-end. A managed Antigravity agent runs in an isolated
`sbx` microVM, authenticates with **zero credential exposure**, edits code, and
returns its reply through the swarm — fully automatically, with **no changes to
your Omnigent install**.
**Audience:** a human or LLM picking this up cold. It captures not just *what*
was built but *why*, what was rejected, and — critically — **every non-obvious
thing it took to get a green turn**, so the reasoning survives a lost session.
**See also:** [`../README.md`](../README.md) (user-facing setup),
[`PIPELINES.md`](./PIPELINES.md) (the declarative-pipeline way to run agy
agents), and [`COLLABORATIVE_SWARM_DESIGN.md`](./COLLABORATIVE_SWARM_DESIGN.md)
(the swarm this plugs into).

---

## 1. Goal & the hard constraint

Run **Antigravity (`agy`)** — Google's Gemini-backed CLI coding agent — as a
swarm agent alongside Claude, inside the same `sbx` microVMs, **without a durable
Google credential ever living on a YOLO agent VM**, and **without modifying
Omnigent source** (only the runtime monkeypatches this package already uses, plus
per-VM setup the launcher performs).

`agy` is fundamentally different from the Claude path:

- **Auth is Gemini OAuth, not an API key.** There is no `agy login` subcommand;
  `agy` writes OAuth credentials to disk on its first interactive browser login.
  Its wire bearer (`access_token`) lives ~1h; refreshing it needs `agy`'s
  embedded `client_secret`, which is **not extractable** from the ~164 MB Go
  binary (assembled at runtime). So the host **cannot** mint or refresh tokens
  itself — only a running `agy` can.
- We want to avoid the atypical, friction-heavy step of exporting a
  service-account key just to run a coding agent, so the Vertex/SA path is out.

That leaves one viable path: **harvest / proxy-swap.**

---

## 2. Architecture — harvest / proxy-swap

Three planes, so the durable refresh credential never touches an untrusted agent:

```
   ┌─────────────────────────┐        ┌──────────────────────────────┐
   │  TRUSTED auth-agy box    │        │  agy AGENT VM (YOLO, untrusted)│
   │  (agy-auth-trusted)      │        │  managed Omnigent host         │
   │  • the ONLY real refresh │        │  • PLACEHOLDER token only      │
   │    token; self-refreshes │        │  • agy sends the placeholder   │
   │  • driven via `sbx exec` │        │    verbatim on the wire        │
   └───────────┬─────────────┘        └───────────────┬──────────────┘
               │ harvester reads the fresh             │ outbound HTTPS to
               │ access token off the box              │ **.googleapis.com
               ▼                                       ▼
   ┌─────────────────────────┐        ┌──────────────────────────────┐
   │  omni-sbx-agy harvest    │  sets  │  sbx host-side proxy (MITM)   │
   │  (host helper, ~30 min)  │───────▶│  swaps PLACEHOLDER → real     │
   │  writes token into the   │  value │  access token in the          │
   │  sbx custom secret VALUE │        │  Authorization: Bearer header │
   └─────────────────────────┘        └──────────────────────────────┘
```

1. **Agent VM** carries only a **placeholder** access token (looks like a real
   `ya29.…` token, far-future expiry, dummy refresh token), so `agy` emits it
   verbatim and never tries to refresh. Omnigent's `harness_is_configured` gate
   passes (it only checks for a non-empty token).
2. The **sbx proxy** MITMs the googleapis hosts and swaps the placeholder for the
   real token **in the outbound header, on the wire** — the agent never sees a
   real credential.
3. A **trusted auth-agy box** (a plain `sbx` sandbox, *not* an Omnigent host —
   driven only via `sbx exec`, no dial-back, empty workspace, no other agents)
   holds the *only* real refresh token and self-refreshes.
4. The **harvester** (`omni-sbx-agy harvest`) pokes the trusted box on a cadence,
   reads the freshly-minted access token, and writes it into the sbx custom
   secret's *value* — **on stdin**, so the token never appears in argv/`ps`/logs.

### Rejected alternatives

| Approach | Why not |
|---|---|
| Store a Gemini **API key** (`sbx secret set -g google`) | `agy` uses OAuth, not an API key; the built-in `google` secret injects the wrong header. |
| Extract `client_secret` and mint tokens host-side | Not extractable from the Go binary (no `GOCSPX`/`client_id` strings; assembled at runtime). |
| Service-account / Vertex key on each VM | We don't want to add an atypical, friction-heavy SA-key export step just to run a coding agent; and it *is* a durable credential on a YOLO box — the thing we're avoiding. |
| Interactive `agy /login` in every fresh VM | No TTY in a headless microVM; and it would put a durable refresh token on every agent box. |
| Refresh in-VM using the placeholder | The placeholder has a dummy refresh token + far-future expiry precisely so `agy` never refreshes; only the trusted box holds a real one. |

---

## 3. Component reference (where the code lives)

| Piece | File | What it does |
|---|---|---|
| Harvester + trusted-box bootstrap | `sbx_omnigent/agy.py` (`omni-sbx-agy`) | `harvest` loop (poke → read → write secret on stdin) and `bootstrap` (stand up + guide one-time login). Also the shared constants + agent-VM seed builders + the in-VM bridge-patch script. |
| Per-VM credential injection | `sbx_omnigent/launcher.py` (`SbxLauncher._inject_agy_credentials`) | Seeds an **agy** VM's `~/.gemini` + patches its in-VM bridge, **before the runner launches**. Scoped to agy VMs only: the swarm tags them with an `-agy` mount-sentinel suffix (`mount_sentinel(…, agy=True)`), which `_parse_mount_sentinel` reads — a Claude VM in the same server is never touched. |
| Config wiring | `sbx_omnigent/entrypoint.py` (`_build_sbx_config`) | Parses `sbx.agy_enabled` / `agy_enterprise` / `agy_gcp_project` / `agy_gcp_location`; installs the enterprise onboarding monkeypatch. |
| Harness-aware launch args + fail-loud gate | `sbx_omnigent/swarm.py` | agy agents get `--dangerously-skip-permissions`; `--agy` gate fails loud if an agy agent is bound without acknowledgment. |
| Turn-completion detection | `sbx_omnigent/swarm_session.py` (`_await_terminal`) | Recognizes agy's SSE completion (differs from Claude's). |
| Bundled agy agents | `agents/swarm-agy-coder/`, `agents/swarm-agy-reviewer-bug/` | `antigravity-native` siblings of the Claude specs. |

Config keys (all under `sandbox.sbx`, all ignored unless `agy_enabled`):

| Key | Default | Meaning |
|---|---|---|
| `agy_enabled` | `false` | Enable agy seeding (applied only to agy-tagged VMs — see the injection row above). |
| `agy_enterprise` | `false` | Business/enterprise (Vertex) account — sets the onboarding flag + patches the in-VM bridge to skip the first-run wizard. |
| `agy_gcp_project` | — | GCP project id (**required** for a Business/Vertex account). |
| `agy_gcp_location` | `us` | GCP location for the seeded project block. |

---

## 4. The debugging record — everything it took to get a green turn

This is the part worth preserving. Each item below **blocked a real turn** and
was found by inspecting the live VM (`sbx exec`, the tmux pane, agy's own logs,
Omnigent's runner log, and the SSE stream). They are ordered as encountered;
each is now handled automatically by the launcher/swarm code.

### Fix 1 — `auth_method` must match the account: `gcp`, not `oauth`

- **Symptom:** the agy terminal exited immediately; `agy models` in the VM said
  *"Please sign in to view available models."* despite a well-formed token file.
- **Root cause:** the placeholder token file's `auth_method` field is
  **load-bearing** — `agy` branches on it to pick its credential path. This
  account is Business/GCP, whose real token (on the trusted box) carries
  `auth_method: "gcp"`. The seed used `"oauth"`, so `agy` looked down the wrong
  path and rejected the token **locally**, before ever making a request — the
  wire swap never even engaged.
- **Fix:** `agy.auth_method_for(enterprise)` → `"gcp"` for enterprise, `"oauth"`
  for consumer; threaded into the seeded token file + flat creds. Proven: with
  `gcp`, `agy models` in the agent VM authenticates through the swap.
- **Note:** the gate (`onboarding/gemini_auth.py`) only checks for a non-empty
  `access_token`/`refresh_token`, so it passed regardless — masking this bug.

### Fix 2 — launch flag: `--dangerously-skip-permissions`, NOT `--permission-mode`

- **Symptom:** `runner_error: … the agy terminal is no longer running (the TUI
  exited)`. No agy process, no tmux pane, no agy log.
- **Root cause:** the swarm passes YOLO launch args as `terminal_launch_args`,
  which Omnigent appends verbatim to the agy argv. The default was Claude's
  `--permission-mode auto`. `agy` does **not** know that flag and exits on
  startup: `flags provided but not defined: -permission-mode` (rc=2). agy's
  equivalent auto-approve flag is `--dangerously-skip-permissions`.
- **Fix:** `swarm._launch_args_for(harness)` — agy harnesses get
  `('--dangerously-skip-permissions',)`, Claude/codex get `('--permission-mode',
  'auto')`. The orchestrator resolves each bound agent's harness from
  `GET /v1/agents` and picks per-agent args. **Client-side** (the `omni-sbx-swarm`
  CLI), so it needs no server restart.

### Fix 3 — enterprise onboarding wizard blocks turn delivery

- **Symptom:** with the flag fixed, agy launched and stayed alive, but the turn
  failed with *"agy did not render the pasted message in its input box"* — the
  tmux pane showed **"Welcome to Antigravity CLI! Choose your color scheme:"**
  followed by a Terms-of-Service screen.
- **Root cause:** agy 1.0.16 runs a first-run TUI wizard (color scheme → ToS)
  gated by `enterpriseOnboardingComplete`. On a Business account that flag must
  be `true` or the wizard holds the input box; a headless VM can't answer it.
  Completing the wizard interactively flips the flag `false → true` (and adds
  `enableTelemetry:false`).
- **Why the obvious fixes don't reach it:** the flag lives in the **per-session
  bridge dir** (`~/.omnigent/antigravity-native/<hash>/agy-home/.gemini`), which
  Omnigent's in-VM `seed_isolated_agy_home` re-seeds with a **hardcoded `false`**
  on every launch. That re-seed runs in the **in-VM runner process** — out of
  reach of our server-side monkeypatch — and one of the `false` literals is a
  **function-local inline dict** (not a patchable module constant), so an
  in-memory monkeypatch can't reach it either.
- **Fix:** the launcher edits the **installed** `antigravity_native_bridge.py`
  *inside the ephemeral VM*, **before the runner imports it**, flipping both
  `"enterpriseOnboardingComplete": False` occurrences to `True`
  (`agy.build_bridge_patch_script`, best-effort, gated on `agy_enterprise`). See
  §5 for why this is an in-VM file edit and not a wrapper monkeypatch.

### Fix 4 — GCP project id + a version-skew gotcha

- **Symptom:** onboarding cleared, the turn *ran*, but ended
  `agent executor error: invalid project ID: ""` (note `applyAuthResult …
  quotaProject=` empty). `agy models` still worked — because **listing** models
  doesn't need a project, but **running a cascade** does.
- **Root cause:** a Business/Vertex account needs a GCP project to run the agent.
  The trusted box carries it in `settings.json`:
  `"gcp": {"project": "<gcp-project-id>", "location": "us"}`. The agent VM had no
  such block.
- **The version-skew trap:** `settings.json` **is** in `_AGY_SEED_FILES` (copied
  HOME → bridge) in the *reference* Omnigent checkout — but the **deployed image**
  runs an **older** Omnigent whose `_AGY_SEED_FILES` **omits** `settings.json`
  (and has no `write_gemini_dir`; its seeder is `seed_isolated_agy_home`). So
  seeding HOME `settings.json` alone did nothing — the copy loop skipped it. **Do
  not trust the reference checkout for deployed behavior; inspect the VM's
  `/build/omnigent/…` instead.**
- **Fix:** the launcher seeds `~/.gemini/antigravity-cli/settings.json` with the
  `gcp` block **and** the in-VM bridge patch **adds `settings.json` to
  `_AGY_SEED_FILES`** on builds that omit it, so the copy carries the project to
  the bridge (Omnigent's trust/survey writers are merge-only, so they preserve
  it). Config: `agy_gcp_project` (+ `agy_gcp_location`, default `us`).

### Fix 5 — the fail-loud gate + config plumbing (supporting work)

- `sbx.agy_enabled` is **server-side** config; `omni-sbx-swarm start` is a
  separate client process that can't read it. So the swarm-side guard is an
  explicit `--agy` acknowledgment (default from `OMNI_SBX_AGY_ENABLED`); when
  unacknowledged, `start` does one best-effort `GET /v1/agents` and fails loud if
  a bound agent uses the `antigravity-native` harness — turning a cryptic in-VM
  failure into an actionable start-time error. The real enforcement remains the
  in-VM readiness gate.

### Fix 6 — reply-capture: agy's SSE completion differs from Claude's

- **Symptom:** the file edit landed and the session went `idle`, but the
  `send` CLI **timed out** — the coordinator never saw turn completion, so the
  review-loop couldn't advance. Worse, it depended on tool use: a plain-text
  reply returned in ~3 s, a **file-editing** turn hung.
- **Root cause (from capturing the raw SSE stream):**
  - Claude native completes with `session.status: idle` carrying a non-null
    `response_id`, and emits a *premature* settle-idle (`response_id=None`) at
    turn start. Our waiter gated on `idle + non-null response_id`.
  - agy's real completion-idle carries **`response_id=None`**, and for a **tool
    turn the idle arrives a beat BEFORE** agy mirrors the reply
    (`response.output_item.done`):
    `running → response.completed → idle(no id) → reply`. So neither
    "has response_id" nor "reply already seen" was true at the idle → timeout.
- **Fix:** `swarm_session._await_terminal` now treats a bare (id-less) idle as
  terminal once a **`response.completed`** frame has been seen (agy fires it when
  the model work finishes, *before* the idle — the clean discriminator from
  Claude's pre-work premature idle), then **grace-waits** `_IDLE_REPLY_GRACE_S`
  (30 s) for the lagging reply. Claude's premature idle (before any
  `response.completed`) is still skipped. Live-verified: a tool turn now returns
  its reply in ~5 s. **Client-side** — no server restart.

---

## 5. Why the enterprise/settings fixes are in-VM *file edits*, not monkeypatches

A natural instinct is to fix Fixes 3–4 with a runtime monkeypatch like
`install_sbx_provider` in `entrypoint.py`. That **cannot** work here, for two
independent reasons:

1. **Process boundary.** Our wrappers patch the **server** process. The code that
   seeds the bridge dir and copies seed files (`seed_isolated_agy_home`,
   `_AGY_SEED_FILES`, the onboarding seeder) runs in the **in-VM Omnigent runner**
   — a separate process, unreachable from the server-side patch.
2. **Function-local literal.** One `enterpriseOnboardingComplete: False` is an
   **inline dict inside a function**, not a module constant, so reassigning a
   module global (even in-VM) can't reach it.

So any fix must run **inside the VM**. The chosen mechanism (a deliberate
decision) is: the launcher runs an in-VM `python3 -c` script that **edits the
installed `/build/omnigent/antigravity_native_bridge.py` in place, before the
runner imports it.** Properties: runtime, per-VM, config-gated, best-effort
(a no-op if the literal moved), and it dies with the disposed sandbox. It is
**not** a change to this repo's source or to your Omnigent install — it mutates
only the throwaway VM's copy. (The considered alternative — an in-VM
`sitecustomize.py` that monkeypatches in memory and wraps the seeder function for
the inline case — is cleaner in principle but needs more moving parts, incl.
`PYTHONPATH` into both launch paths.)

---

## 6. Other non-obvious facts (cost hours; don't relearn them)

- **The sbx proxy MITMs a host only when a matching secret exists.** Without the
  swap secret, googleapis is `forward-bypass` (tunneled, not decrypted, not
  swapped). Adding the secret flips the host to `forward`. `agy` does **not**
  cert-pin, so the MITM is accepted. (`sbx policy log` shows the mode.)
- **Use `**.googleapis.com`, not `*.googleapis.com`.** Enterprise/GCP accounts
  route the model call to the **3-label** Vertex regional host
  `aiplatform.us.rep.googleapis.com`. A single-`*` wildcard (one label) misses
  it → `UNAUTHENTICATED (401)`. The swap secret is scoped to `**.googleapis.com`
  + `*.googleapis.com` + the explicit Vertex/CloudCode hosts.
- **Trusted-box refresh mechanics:** the poke is `agy models` (the lightest
  authenticated action, no inference). Force a deterministic refresh by setting
  the on-disk token's `expiry` to the past, then running `agy models`; agy
  re-mints via `oauth2` and rewrites the file. Token life ~1h → harvest ~30 min.
- **Trusted-box steady-state egress = two hosts:** `oauth2.googleapis.com` +
  `cloudcode-pa.googleapis.com`. The one-time interactive `/login` needs a wider
  set (userinfo/consent/assets) — `bootstrap` opens that, and you can tighten
  after.
- **`agy` auto-updates** (image ships 1.0.10; long-lived boxes self-update to
  1.0.16). The 1.0.16 first-run wizard (color scheme + ToS) is what Fix 3
  addresses.
- **Executor model:** the `antigravity-native` executor launches agy **headless
  in a tmux pane** (the pty is enough) and delivers turns over agy's
  **connect-RPC** port — it does **not** type into the TUI. "web-attended" only
  means a web client can *watch/drive prompts*; headless swarm turns work fine
  once agy stays alive and serves RPC. (An early wrong conclusion was that the
  harness was web-UI-only; it is not.)

---

## 7. Operational runbook

### One-time: stand up the trusted box

```bash
omni-sbx-agy bootstrap                 # creates agy-auth-trusted, scopes egress,
                                       # seeds the swap-secret placeholder
sbx exec -it agy-auth-trusted agy      # run '/login', complete the browser consent
```

### Always-on: keep the swap token fresh

```bash
omni-sbx-agy harvest                   # ~30-min force-refresh loop (run under a
                                       # process manager)
omni-sbx-agy harvest --once            # single cycle (cron / verification)
```

The token never appears in argv/logs — only a redacted `sha256:… len=`
fingerprint prints. If the trusted box's refresh token is revoked/expired,
`harvest` prints a loud `RE-LOGIN REQUIRED` and keeps probing; re-run the
interactive `/login` and it self-recovers on the next cycle.

### Enable agy agents (server config)

```yaml
sandbox:
  sbx:
    agy_enabled: true
    agy_enterprise: true               # Business/enterprise (Vertex) account
    agy_gcp_project: <gcp-project-id>   # required for that account
    agy_gcp_location: us
```

Restart the server so the launcher loads these. Then run an agy swarm agent like
any other, acknowledging support with `--agy`:

```bash
omni-sbx-swarm start --agy --swarm-id <id> --repo-url <repo> \
  --canonical-root <dir> --worktree-root <dir> --base-branch main \
  --coder-agent swarm-agy-coder \
  --reviewer-agent swarm-reviewer-security --reviewer-role security
omni-sbx-swarm send --swarm-id <id> --role coder --message "<task>"
```

### Verifying a fresh VM (what "working" looks like)

Inside the coder VM (`sbx exec <box> …`):

- `~/.gemini/antigravity-cli/antigravity-oauth-token` has `auth_method: "gcp"` +
  the placeholder access token.
- The **bridge** `…/agy-home/.gemini/antigravity-cli/cache/onboarding.json` shows
  `enterpriseOnboardingComplete: true`.
- The bridge `…/settings.json` carries the `gcp` block.
- `agy models` lists models (auth via the swap works).
- A `send` turn returns `{"status":"idle","reply":"…"}` and the worktree file is
  edited.

---

## 8. Known limitations / follow-ups

- **In-VM bridge patch is version-sensitive** (it matches string literals). It's
  best-effort — a no-op if a future Omnigent renames the literal or the seed
  list — so a silent Omnigent change could regress Fixes 3–4. Re-verify §7 after
  Omnigent image bumps.
- **`agy_gcp_project` is per-server config**, not per-agent. Fine while one
  account backs all agy agents; multi-account would need per-agent plumbing.
- **Trusted-box re-login is manual** (by design — the one human step). The
  harvester surfaces `RE-LOGIN REQUIRED` clearly.
- Per-agent egress tightening (agy agents scoped to just their model hosts) is a
  planned Tier-2 item.

---

## 9. Provenance

Everything above was proven on real managed VMs: a bundled `swarm-agy-coder`
authenticated via the harvest/proxy-swap and wrote functions to a worktree file
across several turns, each returning its reply. The full unit-test suite (incl.
coverage for every fix) is green and lint-clean.
</content>
