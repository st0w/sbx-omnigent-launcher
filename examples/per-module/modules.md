# Modules — one `- [<id>] <title>` line each, in DEPENDENCY ORDER.
#
# The runner builds these top-to-bottom, one full cadre per line. Each
# later module is planned and built against the FROZEN artifacts of the
# earlier ones (its worktree seeds from the accumulated tip). Keep <id>
# short — it namespaces that module's branches (pl/<run>/<id>-tests, …)
# and its published branch (pipeline/<run>-<id>). The <title> is the
# module's one-line scope handed to its planner; the planner fills in the
# detailed design and work breakdown.
#
# Lines that are not `- [id] title` items (like these comments) are
# ignored, so you can annotate freely.

- [core]  Contracts and core skeleton — shared domain types, the provider trait, capability flags, error taxonomy, config + logging, CLI shell, conformance harness
- [store] Storage and schema — migrations for the full data model, the thin persistence layer, snapshot lifecycle (atomic commit / partial-failure), tenant scoping, diffing
- [aws]   AWS provider — full enumeration behind the core trait, permission manifest, recorded fixtures (no live calls in CI), sample onboarding
- [gcp]   GCP provider — full enumeration behind the core trait, condition expressions preserved verbatim, manifest, fixtures, onboarding
- [cli]   CLI integration and packaging — end-to-end wiring and summary output, docs, reproducible signed build, SBOM, CI dependency audit
