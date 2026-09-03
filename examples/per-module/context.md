# Shared project context (the "system prompt")

This is the `context_file`: short, stable guidance baked into EVERY
agent's system prompt for every module — the things true of the whole
project regardless of which module is being built. Keep it small; the
per-module scope comes from `modules.md` and the design detail from each
module's planner.

Replace the placeholder text below.

- **Product** — one line on what the project is and who it serves.
- **Stack / conventions** — language(s), build tool, test runner, style,
  and any hard rules (e.g. "single static binary; minimal, pinned,
  vendored dependencies; code that runs in customer environments is
  auditable and signed").
- **Where the docs live** — point agents at the source-of-truth docs in
  the repo to read before going deep.
