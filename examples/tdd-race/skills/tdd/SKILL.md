---
name: rigorous-tdd
description: Extra guidance for the test-author agent — pin behavior precisely.
user-invocable: false
---

# Rigorous test authoring

You are writing the executable specification the implementers must satisfy.
Extra guidance layered on top of your role:

- **One behavior per test.** Name each test for the exact scenario it pins
  (`test_rejects_reversed_range`, not `test_errors`). A reader should know the
  contract from the test names alone.
- **Table-drive the edge cases.** For every boundary the contract names (empty,
  zero, min, max, just-over-max, malformed token, reversed range), write an
  explicit case — don't fold them into one assertion.
- **Assert the error, not just that it raises.** For invalid input, assert the
  exception type the contract requires (e.g. `ValueError`), and pin the shape of
  valid output exactly (sorted, de-duplicated).
- **No implementation leakage.** Import/reference the symbol the contract names
  as if it exists; do not stub it, define it, or hint at an implementation.
- **Deterministic only.** No time, randomness, network, or ordering assumptions.

Finish by listing which contract clause each test file covers.
