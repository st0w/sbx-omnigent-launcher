# Project brief — shared design context for every module's planner

This is the `task_file`: the whole-component brief each module's planner
receives ALONGSIDE its own module row (from `modules.md`). It describes
the component as a whole and the rules every module must uphold — it does
**not** list the modules (that table lives in `modules.md`, and repeating
it here just invites drift). Replace the placeholder text below with your
component's real brief.

## What we are building

<One or two paragraphs: what the whole component does, what is in scope
for this body of work, and what is explicitly out of scope. Each module
is one slice of this; the planner uses this to design its slice so the
slices compose.>

## Invariants (enforced at every gate, in every module)

<The rules that hold across all modules — the reviewers check these on
every module, and a violation is a stage failure, not a review note.
E.g. "strictly read-only, minimum permissions", "no new dependency
without written justification", "structured logs free of secrets". List
them concretely and mechanically.>

1. ...
2. ...

## Data model / core contracts (planner starting point — refine, don't discard)

<The shared shapes the earliest module freezes and later modules build
against: the key types, the trait/interface every provider implements,
the schema. Later modules treat these as fixed contracts; a genuine need
to change one is a halt-and-escalate to the human.>

## Stage directives (apply within every module run)

<What each stage owes, framed for a single module. The runner already
frames each role, so keep this to the project-specific expectations.>

- **plan** — design THIS module (layout, contracts within it, error/retry
  strategy) plus a work breakdown with mechanically verifiable
  done-criteria the TDD writer can turn directly into tests. Surface real
  decisions to the human rather than guess.
- **tests** — a failing suite derived from the approved module plan: unit,
  integration against recorded fixtures (no live calls in CI), schema,
  and invariant-conformance tests. Every plan work-item maps to ≥1 test.
- **implement** — make the frozen suite green; may not modify/skip any
  test; no new dependency without approval.
- **review** — the security + bug checklists, citing specific files/lines.
- **pick / refactor / review-r** — judge selects; refactor is
  behavior-preserving; the final review ships it.
