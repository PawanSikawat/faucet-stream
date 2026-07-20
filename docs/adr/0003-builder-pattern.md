# ADR 0003 — Progressive builder + object-safe traits

*Configure the pipeline with `with_*` builder methods and keep connector traits object-safe with defaulted methods.*

- **Status:** Accepted (implemented) — `Pipeline` / `RunStreamOptions` in `crates/core/src/pipeline.rs`; `Source`/`Sink` in `traits.rs`.

## Context

The pipeline has grown many optional cross-cutting concerns — state, DLQ, quality,
contract, masking, drift, resilience, delivery mode, cancellation, adaptive batching.
Connectors, meanwhile, must remain trivially implementable by third parties and
usable as trait objects (`Box<dyn Source>`), because the CLI registry stores
heterogeneous connectors behind one type.

## Problem

Two related API-shape questions:

1. How does a caller configure a pipeline with a growing set of optional features
   without a combinatorial explosion of constructors or a breaking change every
   time a feature is added?
2. How do the connector traits gain new capabilities (exactly-once, sharding,
   discovery, schema evolution) over time without breaking existing connectors or
   losing object-safety?

## Decision

- **Progressive builder for the pipeline.** `Pipeline::new(&source, &sink)` returns a
  value configured by chained `with_state_store`, `with_dlq`, `with_quality`,
  `with_delivery`, `with_cancel`, … Each returns `Self`; unspecified features default
  to off. `run_stream` takes the same shape as `RunStreamOptions` for direct callers.
- **Object-safe traits with defaulted methods.** `Source` / `Sink` have exactly one
  required method each (`fetch_with_context` / `write_batch`); every other method
  (streaming, resumability, exactly-once, sharding, discovery, evolution, probes) has
  a default. No associated types, no generic methods — so `Box<dyn Source>` works and
  adding a method is always backward-compatible.

```rust
let result = Pipeline::new(&source, &sink)
    .with_state_store(store)
    .with_delivery(DeliveryMode::ExactlyOnce)
    .with_dlq(dlq)          // opt in only what you need
    .run().await?;
```

## Alternatives considered

- **One giant config struct** passed to a single `run(config)`. Rejected: every new
  field is a struct change; optional-vs-required is unclear; poor discoverability
  compared to `.with_*()` autocomplete.
- **Generics / associated types on the traits** (e.g. `type Bookmark`). More precise,
  but breaks object-safety — the CLI could not store `Box<dyn Source>` — and forces
  monomorphisation across dozens of connectors. Rejected.
- **A separate trait per capability** (`Source: Resumable + Shardable + …`).
  Object-safety and dyn-upcasting friction make this awkward today; the defaulted-
  method pattern achieves the same evolvability more simply. Revisited in
  [RFC 0001](../../rfcs/0001-capability-traits.md).

## Trade-offs

- Defaulted booleans (`supports_idempotent_writes`) are stringly-checked at the CLI
  gate (allowlists in `registry.rs`) rather than statically — a typed capability
  model would remove the allowlists at the cost of trait complexity.
- The builder allows nonsensical combinations to be *expressed*; they are rejected at
  `run`/`expand` time rather than at compile time.

## Consequences

- **Positive:** connectors implement almost nothing; core adds capabilities without
  breaking anyone; callers opt into exactly what they need; the API is discoverable.
- **Negative:** capability checks are runtime, not compile-time; the builder's
  optional surface must be validated (gates in `expand`/`run_stream`).

## Future work

- Typed capability traits ([RFC 0001](../../rfcs/0001-capability-traits.md)). **Note —
  the capabilities themselves already ship** as defaulted methods on `Source`/`Sink`:
  discovery (`supports_discover`/`discover`), health (`check`), exactly-once
  (`supports_exactly_once`/`replay_guarantee`/`capture_resume_position`), and sharding
  (`is_shardable`/`enumerate_shards`). Only the *typed refactor* (splitting them into
  separate marker traits) is deferred, and only worth revisiting if a genuinely new
  capability arrives that does not fit the defaulted-method pattern — do not re-chase it
  as missing functionality.

## Related

- [Connector SDK](../architecture/connector-sdk.md) · [pipeline](../architecture/pipeline.md)
- [Standards: API design](../standards/api-design.md) · [API stability](../stability.md)
