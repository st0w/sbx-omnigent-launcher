# Which Omnigent releases this launcher runs against

This package is installed into the same environment as `omnigent` and
imports it at runtime, so an Omnigent upgrade can break it without
anything here changing.

**The oldest supported release is v0.13.0**, for both the server and the host
image (`sandbox.sbx.image`). On an older server, `omni-sbx server` stops at
startup with a `ModuleNotFoundError` naming the module it couldn't import.

v0.13.0 is past Omnigent's **`refactor: group top-level omnigent modules into
subpackages (140 -> 45)`**. So the three modules that refactor moved
(`model_family_mismatch`, the codex effort ladder and the agy bridge) are
imported from their one home in `sbx_omnigent/_compat.py`. A host image older
than v0.13.0 keeps the agy bridge elsewhere; the in-VM patch then prints
`AGY_BRIDGE_PATCHED skip ModuleNotFoundError` and changes nothing.

## What still differs, and how each is handled

Omnigent's **multi-repo `start_host`** arrived after v0.13.0, so the launcher
still spans both sides of it:

| Omnigent surface | v0.13.0 | Later releases (v0.19.1, v0.20.0) | Handled by |
| --- | --- | --- | --- |
| `RepoWorkspace` | `omnigent.server.managed_hosts` | `omnigent.onboarding.sandboxes.types` | `_compat.repo_workspace` (resolved lazily) |
| `start_host` repositories | `repo_url` / `repo_branch` / `repo_name` | `repos: Sequence[RepoWorkspace]` | `SbxLauncher.start_host` accepts both; delegates in the shape the installed base declares |

## Checking a new release

Last checked against **v0.13.0**, **v0.19.1** and **v0.20.0**: the full unit suite
passes on all three, and mypy reports the same errors against each, none of them
in code that touches Omnigent. Pipeline runs also completed on v0.19.1 and
v0.20.0, each with a Claude writer, a Codex and an agy reviewer, and the verify
gate. v0.19.1 needed the one-line server fix described under [v0.19.0 and v0.19.1
need an upstream fix](#v0190-and-v0191-need-an-upstream-fix); v0.20.0 includes
it. The CLI versions those runs read are in `docs/HARNESS-VERSIONS.md`.

Unit tests can't exercise the server itself, so check a new release with a small
pipeline run too: one writer, a reviewer for each harness you use, and a verify
gate.

To check another release, install its published wheel into a throwaway virtual
environment and run the suite and mypy from the repository root, so the launcher
is imported from the working tree:

```bash
uv venv --python 3.14 /tmp/omni-check
uv pip install --python /tmp/omni-check/bin/python omnigent==X.Y.Z
/tmp/omni-check/bin/python -W default -m unittest discover -s tests
uvx mypy --python-executable /tmp/omni-check/bin/python sbx_omnigent/ tests/
```

Run mypy against a wheel install, not an editable one. mypy can't follow an
editable install's import hook, so against an editable Omnigent every import of it
is `import-not-found` and nothing that touches Omnigent is type-checked at all.
That is how a `start_host` override that no longer matched the base class, and
tests reading attributes the factory's declared type doesn't have, went unnoticed
until v0.19.1 was checked this way.

## The straddle is meant to be temporary

When the supported floor rises to a release whose `start_host` takes `repos=`
(#56):

- drop `omnigent.server.managed_hosts` from `RepoWorkspace`'s paths in
  `sbx_omnigent/_compat.py` (call sites do not change);
- drop the `repo_url` / `repo_branch` / `repo_name` keywords and
  `_base_repo_kwargs` from `SbxLauncher.start_host`, keeping only
  `repos`;
- `base_start_host_takes_repos()` in `sbx_omnigent/launcher.py` then has one answer and can go.

`tests/test_compat.py` and the delegation tests in
`tests/test_launcher.py` are what make that deletion safe to attempt.

## v0.19.0 and v0.19.1 need an upstream fix

On v0.19.0 and v0.19.1, every pipeline turn fails at once with `the turn failed:
None` and an empty pane, on a server with a single local user:

1. The runner sends a session's first message as soon as the server's stream is
   ready, before the VM's own runner has connected.
2. Since v0.19.0, the server saves such a message and delivers it once the runner
   connects (`omnigent/server/startup_inputs.py`).
3. On a single-user server every request is the reserved user `"local"`, and
   Omnigent stores no author for it. The saved message was authorized later as
   that stored author, so delivery found no user: `Unable to deliver saved
   message` and `OmnigentError: Authentication required` in the server log.

v0.13.0 doesn't have this code, so it isn't affected. Upstream commit `b9e80e9d3`
(`omnigent-ai/omnigent`, merged 2026-10-21) fixes it, and **v0.20.0 is the first
release that includes it**: install v0.20.0 or later rather than patching. The
fix changes one line in `omnigent/server/routes/sessions/routes_events.py`, inside
`_post_event_impl`:

```diff
-                session_id, body, user_id=created_by, headers=request.headers, first=False
+                session_id, body, user_id=user_id, headers=request.headers, first=False
```

To stay on v0.19.0 or v0.19.1, apply that change to the installed package; any
reinstall undoes it. To check whether a release contains the commit (expect
`behind` or `identical`):

```bash
gh api repos/omnigent-ai/omnigent/compare/<tag>...b9e80e9d3 --jq .status
```

## Two things this does NOT paper over

**The host image is pinned separately.** `sandbox.sbx.image` in the
server config names the guest's Omnigent, which is a different version
from the server's. Pin it to the server's release, and to v0.13.0 or
later. Upstream's public image tags are `sha-<short>`,
`latest-nightly`, `vX.Y.Z` and `latest` — a `dev-<short>` tag is a
locally built image and has to be rebuilt to follow a server upgrade.
Two VMs the launcher boots itself don't read it and default to `:latest`: the
verify gate (set `verify.image` in the pipeline) and the trusted agy box (pass
`omni-sbx-agy bootstrap --image`). See `docs/HARNESS-VERSIONS.md`.

**Omnigent now has a real provider plugin hook.** The
`omnigent.sandbox_providers` entry-point group
(`omnigent/onboarding/sandboxes/registry.py`) loads community
providers. Adopting it would retire the `parse_sandbox_config`
monkeypatch, and would bring `sandbox.reaper` and a `providers:` list
to sbx; the wrapper refuses both beside sbx today (#54). The cost is
moving the launcher class under the `omnigent.community.sandbox.*`
namespace the registry requires. Not done; the wrapper still works.
