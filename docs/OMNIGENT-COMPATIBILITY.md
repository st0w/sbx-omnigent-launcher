# Which Omnigent releases this launcher runs against

This package is installed into the same environment as `omnigent` and
imports it at runtime, so an Omnigent upgrade can break it without
anything here changing. It is written to span the releases either side
of Omnigent's **`refactor: group top-level omnigent modules into
subpackages (140 -> 45)`** and its **multi-repo `start_host`** change,
so the two can be pulled in any order and rolled back independently.

## What differs, and how each is handled

| Omnigent surface | Before | After | Handled by |
| --- | --- | --- | --- |
| `model_family_mismatch` | `omnigent.model_override` | `omnigent.models.model_override` | `_compat` |
| codex effort ladder | `omnigent.reasoning_effort` | `omnigent.util.reasoning_effort` | `_compat` |
| agy native bridge | `omnigent.antigravity_native_bridge` | `omnigent.harnesses.antigravity_native.bridge` | `_compat`, and the in-VM patch script in `agy.py` |
| `RepoWorkspace` | `omnigent.server.managed_hosts` | `omnigent.onboarding.sandboxes.types` | `_compat.repo_workspace` (resolved lazily) |
| `start_host` repositories | `repo_url` / `repo_branch` / `repo_name` | `repos: Sequence[RepoWorkspace]` | `SbxLauncher.start_host` accepts both; delegates in the shape the installed base declares |

None of the old paths were aliased upstream, so a plain import pins
this package to one side. Two of the three module moves would have
failed loudly at import — taking `omni-sbx server` with them, since
`entrypoint -> launcher -> swarm` is a top-level chain. The third was
inside an `except ImportError` and would have gone silent.

## The straddle is meant to be temporary

When the supported floor rises past both changes:

- drop the legacy entry from each tuple in `sbx_omnigent/_compat.py`
  (call sites do not change);
- drop the `repo_url` / `repo_branch` / `repo_name` keywords and
  `_base_repo_kwargs` from `SbxLauncher.start_host`, keeping only
  `repos`;
- `base_start_host_takes_repos()` then has one answer and can go.

`tests/test_compat.py` and the delegation tests in
`tests/test_launcher.py` are what make that deletion safe to attempt.

## Two things this does NOT paper over

**The host image is pinned separately.** `sandbox.sbx.image` in the
server config names the guest's Omnigent, which is a different version
from the server's. The in-VM agy bridge patch spans both layouts for
exactly this reason. Upstream's public image tags are `sha-<short>`,
`latest-nightly`, `vX.Y.Z` and `latest` — a `dev-<short>` tag is a
locally built image and has to be rebuilt to follow a server upgrade.
See `docs/HARNESS-VERSIONS.md`.

**Omnigent now has a real provider plugin hook.** The
`omnigent.sandbox_providers` entry-point group
(`omnigent/onboarding/sandboxes/registry.py`) does what
`entrypoint.py`'s docstring says does not exist. Adopting it would
retire the `parse_sandbox_config` monkeypatch, at the cost of moving
the launcher class under the `omnigent.community.sandbox.*` namespace
the registry requires. Not done; the wrapper still works.
