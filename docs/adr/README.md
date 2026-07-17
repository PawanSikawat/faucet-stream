# Architecture Decision Records

*The log of major architectural decisions, each with the context and trade-offs that produced it.*

An **Architecture Decision Record** (ADR) captures one significant decision:
what we chose, why, what we rejected, and what we now live with. ADRs are
immutable once accepted — if a decision is later reversed, a *new* ADR
supersedes the old one and links back to it, rather than the old one being
edited. This preserves the reasoning trail for future maintainers who ask "why
is it done this way?".

ADRs describe decisions **already made and implemented**. Forward-looking
proposals live in [`../../rfcs/`](../../rfcs/README.md); an accepted-and-shipped
RFC often produces an ADR here.

## Format

Every ADR follows the same structure: Status · Context · Problem · Decision ·
Alternatives considered · Trade-offs · Consequences · Future work · Related.

## The records

| ADR | Decision | Anchors |
|-----|----------|---------|
| [0001](./0001-stream-pages.md) | Page-based streaming (`StreamPage`) to bound memory at `O(batch_size)` | [stream-pages](../architecture/stream-pages.md) |
| [0002](./0002-checkpoint-ordering.md) | **Write → flush → checkpoint** ordering — the data-integrity keystone | [invariants](../architecture/invariants.md), [recovery](../architecture/recovery.md) |
| [0003](./0003-builder-pattern.md) | Progressive builder APIs over object-safe traits with defaulted methods | [connector-sdk](../architecture/connector-sdk.md) |
| [0004](./0004-json-record-model.md) | `serde_json::Value` as the universal record model | [overview](../architecture/overview.md), [RFC 0002](../../rfcs/0002-arrow-support.md) |
| [0005](./0005-runtime-recovery.md) | Sink-anchored resume — commit tokens embed the resume bookmark | [recovery](../architecture/recovery.md) |
| [0006](./0006-state-management.md) | `StateStore` trait; light backends in core, heavy ones in their own crates | [state-management](../architecture/state-management.md) |
| [0007](./0007-retries.md) | Two-layer retries; never retry a non-idempotent write | [retries](../architecture/retries.md) |
| [0008](./0008-observability.md) | Automatic instrumentation via pipeline decorators | [observability](../architecture/observability.md) |
| [0009](./0009-schema-validation.md) | Fixed-order per-page passes: mask → quality → contract → drift | [schema](../architecture/schema.md) |
| [0010](./0010-pipeline-runtime.md) | Lean core library; all orchestration in the CLI layer | [execution](../architecture/execution.md) |

## Proposing a new ADR

1. Copy the structure of an existing ADR; take the next free number.
2. If the decision is still open, write an [RFC](../../rfcs/README.md) first and
   land the ADR once the decision is made and implemented.
3. Cross-link the architecture pages the decision touches, and add a row above.

## Related

- [Architecture overview](../architecture/README.md)
- [Design invariants](../architecture/invariants.md)
- [RFC process](../../rfcs/README.md)
- [Documentation home](../README.md)
