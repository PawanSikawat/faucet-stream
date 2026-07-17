# ADR 0004 — `serde_json::Value` as the universal record model

*Every record is a `serde_json::Value` end to end — chosen for connector-author ergonomics, at a known allocation cost.*

- **Status:** Accepted (implemented) — the record type throughout `faucet-core` and every connector.

## Context

faucet-stream connects heterogeneous systems whose payloads have no common schema:
JSON APIs, SQL rows, protobuf messages, CSV lines, Avro, object-store blobs. The
core must carry records between any source and any sink, apply transforms, and let
third parties write connectors with minimal friction.

## Problem

The record type is the single most pervasive type in the codebase — it appears in
every trait method, every transform, every pass. It must be (a) universal enough to
represent any source's output, (b) trivial for a connector author to produce and
consume, and (c) compatible with `schemars`-based config introspection and the JSON
transform/quality/contract layers.

## Decision

Use **`serde_json::Value`** as the universal record. `faucet-core` re-exports
`serde_json` (and `Value`, `json!`) so a connector author's only dependency is
`faucet-core`. Every source yields `Vec<Value>`; every sink consumes `&[Value]`;
transforms, quality checks, contracts, masking, and drift all operate on `Value`.

## Alternatives considered

- **Apache Arrow `RecordBatch`.** Columnar, zero-copy, vectorised — ideal for
  throughput. But it forces a schema up front (many sources are schemaless or
  schema-late), complicates row-oriented transforms and CDC envelopes, and would
  make connector authoring far heavier. Rejected *for now*; the migration path is
  [RFC 0002](../../rfcs/0002-arrow-support.md).
- **A bespoke `enum Record` type.** Would let us optimise representation, but
  reinvents `serde_json::Value`, loses the vast `serde`/`schemars` ecosystem, and
  raises the connector-authoring bar. Rejected.
- **Raw bytes + per-connector codecs.** Maximal flexibility, but pushes
  parse/serialize into every transform and pass and defeats the JSON-native
  quality/contract/masking layers. Rejected.

## Trade-offs

- **Allocation cost.** `Value` allocates per field (heap `String` keys, boxed
  arrays/maps). For very high-throughput numeric workloads this is the dominant cost
  — see [performance](../architecture/performance.md).
- **No columnar batching.** Sinks that would prefer Arrow (Parquet, BigQuery) shovel
  JSON↔Arrow internally (e.g. the SQL transform, the Parquet sink) rather than the
  pipeline carrying columns.

## Consequences

- **Positive:** any payload flows without a declared schema; connector authoring is
  as simple as producing JSON; the transform/quality/contract/masking layers are
  uniform and JSON-native; config introspection via `schemars` is free.
- **Negative:** per-record allocation overhead; a JSON↔columnar boundary inside
  columnar sinks; no free vectorisation.

## Future work

- An Arrow-backed fast path behind the same `StreamPage` loop, opt-in for
  columnar-friendly source→sink pairs — [RFC 0002](../../rfcs/0002-arrow-support.md).

## Related

- [Overview](../architecture/overview.md) · [performance](../architecture/performance.md) · [connector SDK](../architecture/connector-sdk.md)
- [RFC 0002 — Arrow support](../../rfcs/0002-arrow-support.md)
