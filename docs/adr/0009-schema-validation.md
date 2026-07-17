# ADR 0009 — Layered, fixed-order per-page validation passes

*Mask → quality → contract → drift, per page, in a fixed order — because ordering here is a correctness property.*

- **Status:** Accepted (implemented) — the per-page pass sequence in `run_stream`, `crates/core/src/pipeline.rs`; passes in `masking/`, `quality/`, `contract/`, `drift.rs`.

## Context

Records must be protected and validated between source and sink: PII masked,
bad rows caught, an output contract enforced, and destination schema drift handled.
These are four distinct concerns, each with its own config surface, and each is
optional.

## Problem

Two questions: (1) should these be one monolithic validator or separate passes? and
(2) in what order do they run — and is the order the user's choice or the system's?
The order is not cosmetic: mask-after-write leaks PII; validate-before-mask lets a
check read raw PII; commit-before-contract-check writes breaching data.

## Decision

**Four separate passes, run per page, in a fixed, non-configurable order:**

```
mask → quality → contract → drift → write
```

- **Masking first, unconditionally** — so PII never reaches a sink, the DLQ, or a
  lineage sample. (Invariant I5.)
- **Quality** removes/aborts bad records so later passes see clean data.
- **Contract** enforces the versioned output promise; `on_breach: fail` aborts the
  page and **writes nothing** — breaching data must never be committed.
- **Drift** runs last, on exactly the records about to be written; `on_drift: fail`
  **defers** its abort until the page's individually-valid survivors are durable
  (mirroring the DLQ-budget/circuit-breaker deferral).

The passes are separate modules with independent config blocks, compiled fail-fast
at load time; the *order* is owned by the system, not exposed as a knob.

## Alternatives considered

- **One monolithic validator.** Rejected: couples four independent concerns, makes
  each harder to test and evolve, and buries the ordering guarantee.
- **User-configurable pass order.** Rejected: a footgun — an operator could reorder
  into a PII leak or a bad-row-written bug. The safe order is fixed.
- **Whole-dataset (not per-page) validation.** Rejected: would require buffering the
  whole dataset (defeating [streaming](./0001-stream-pages.md)); per-page keeps
  memory bounded. The known cost is that a cross-page aggregate check only sees one
  page at a time — documented, and handled by the SQL transform's `batch_size: 0`.

## Trade-offs

- **Per-page semantics** mean a schema/aggregate is only as complete as the page's
  sample; a column absent from every record on a page is invisible until it appears.
- **Contract-`fail` aborts vs drift-`fail` defers** is a deliberate asymmetry: a
  contract breach is about the *data's* validity (never commit it); drift is about
  *shape* mismatch on otherwise-valid rows (commit them, then stop).
- **Drift on top-level columns only** trades nested precision for a pure, total diff.

## Consequences

- **Positive:** each pass is independently testable and evolvable; the ordering
  guarantee is explicit and unbreakable by config; memory stays bounded.
- **Negative:** no cross-page validation without opting into a buffering transform;
  the contract/drift abort asymmetry is a subtlety reviewers must know.

## Future work

- Nested-column drift (needs a nested-aware `diff_schema`).
- Sharing one inferred schema between drift and the lineage facet.

## Related

- [Schema](../architecture/schema.md) · [quality](../architecture/quality.md) · [contracts](../architecture/contracts.md) · [masking](../architecture/masking.md)
- [Design invariants (I5, I6)](../architecture/invariants.md) · [ADR 0001 — Stream pages](./0001-stream-pages.md)
