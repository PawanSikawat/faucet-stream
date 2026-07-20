# RFC 0002 — Optional Apache Arrow record path

*Add an optional columnar (Arrow) batch representation at the `StreamPage` boundary, additive to and coexisting with the `serde_json::Value` record model.*

| | |
|---|---|
| **RFC** | 0002 |
| **Title** | Optional Apache Arrow record path |
| **Status** | Draft (proposal) — **benchmark-justified**, see [Benchmark evidence](#benchmark-evidence-324) |
| **Authors** | faucet-stream maintainers |
| **Related issues** | epic #38 · #324 (benchmark + go/no-go) |
| **Related ADRs** | [0004 JSON record model](../docs/adr/0004-json-record-model.md), [0001 stream-pages](../docs/adr/0001-stream-pages.md) |

## Summary

faucet-stream represents every in-flight record as a `serde_json::Value` (see
[ADR 0004](../docs/adr/0004-json-record-model.md)). That choice buys unmatched
connector-author ergonomics but pays for it in per-record allocation and the
absence of vectorized processing. This RFC proposes an **optional, feature-gated
Arrow columnar batch** that a connector may produce or consume at the
`StreamPage` boundary, leaving the `Value` path as the default and always-present
representation.

## Motivation

The `Value` model is the right default and is not going away. But several facts
in the current codebase point to a columnar fast path being worth its weight:

- **We already cross the Arrow boundary internally.** `faucet-source-parquet` /
  `faucet-sink-parquet` use `parquet::arrow` async readers/writers; the SQL
  transform (`faucet-transform-sql`) shovels JSON↔Arrow via `arrow-json` to feed
  embedded DuckDB; Kafka schema-registry formats decode through Arrow-adjacent
  paths. Today these connectors convert Arrow → `Value` → Arrow across the
  pipeline boundary, paying double conversion for data that was already columnar.
- **Allocation cost.** Every field of every record is a heap-allocated `Value`
  node. For high-volume numeric/analytical workloads this dominates, and it
  cannot be vectorized. The [performance architecture](../docs/architecture/performance.md)
  doc names this as the known structural cost of the record model.
- **Ecosystem gravity.** Parquet, DuckDB, Polars, DataFusion, Flight, and most
  modern warehouse loaders speak Arrow natively. A columnar path lets those
  connectors move batches without ever materializing `Value`.

Doing nothing is a legitimate option — the `Value` path is correct and simple —
but leaves throughput on the table for exactly the analytical connectors most
likely to move large volumes.

## Benchmark evidence (#324)

Issue #324 (D) asked for this to be **measured before committed**, not assumed.
The benchmark `crates/transform-sql/benches/columnar_roundtrip.rs` times the
Arrow↔`Value` conversions a `parquet → transform-sql → parquet` chain performs
today (byte-identical to `src/shovel.rs`) against the irreducible columnar work
(Parquet encode/decode, DuckDB SQL) on a representative 6-column analytical page.
Medians (Apple silicon, criterion, one page = one row group):

| rows | Arrow→`Value` | `Value`→Arrow | **round-trip (tax / boundary)** | Parquet decode + encode |
|-----:|------:|------:|------:|------:|
| 1 000 | 514 µs | 151 µs | **698 µs** | 101 µs |
| 10 000 | 5.14 ms | 1.56 ms | **7.01 ms** | 0.93 ms |
| 50 000 | 28.3 ms | 8.65 ms | **41.7 ms** | 4.78 ms |

**Finding — the `Value` tax is material and dominant on columnar chains.** A
single Arrow→`Value`→Arrow round-trip costs **~7–9× the entire Parquet
encode+decode** for the same page, and a full `parquet → sql → parquet` run pays
that conversion at *every* pipeline boundary (source emit, transform in, transform
out, sink write) — work a fully Arrow-native path skips end to end. The
Arrow→`Value` direction dominates (serde parse of the JSON buffer), scaling
super-linearly with page size. This is exactly the avoidable cost this RFC
targets, on exactly the analytical workloads where throughput matters most.

**Go/no-go: GO** — the number justifies building the *opt-in, additive* Arrow
path below (never a `Value` replacement; the default path is unchanged). The
conversion cost on IO-bound row connectors (REST/Mongo/webhook) remains
negligible against the wire round-trip, so they stay on `Value`.

> Scope note for the benchmark: the DuckDB reference is capped at ≤1 000 rows
> because feeding a single page larger than DuckDB's standard vector size through
> the `vtab-arrow` bridge **aborts the process** — a separate large-page defect in
> the `sql` transform, tracked in its own issue, not part of this RFC.

## Guide-level explanation

`StreamPage` gains an optional columnar payload. A connector that can produce
columnar data does so; one that cannot keeps emitting `Value` rows exactly as
today. The pipeline carries whichever representation the source produced and only
converts when a downstream stage requires the other form.

Conceptually the page becomes "records, in one of two representations, plus the
bookmark". A sink advertises (via its [capabilities](./0001-capability-traits.md))
whether it accepts columnar batches directly; if not, the pipeline converts the
batch to `Value` rows transparently before calling `write_batch`. The
checkpoint-ordering invariant is unchanged — the representation of the records is
orthogonal to when the bookmark is persisted.

The feature is opt-in at build time (`arrow` feature). Builds without it are
byte-for-byte the `Value`-only pipeline of today.

## Reference-level explanation

- **Page shape.** Extend the page type so records are an enum:
  `Rows(Vec<Value>)` (today) or `Columnar(RecordBatch)` (Arrow). This is an
  additive change; `StreamPage { records, bookmark }` keeps working via a
  `Rows` constructor. All existing stages that expect `Vec<Value>` get a helper
  that converts a `Columnar` page to rows on demand.
- **Feature gating.** Arrow (`arrow`, `arrow-json`) is pulled only under a new
  `arrow` feature in `faucet-core`, mirroring how `transform-sql` already pins
  Arrow v58. Connector crates opt in with their own `arrow` feature per the
  [feature-isolation rule](../docs/contributing/common-mistakes.md).
- **Trait touchpoints.** Add defaulted methods so the traits stay object-safe
  and additive (see [ADR 0003](../docs/adr/0003-builder-pattern.md)):
  `Source::stream_pages` may yield columnar pages; `Sink` gains an optional
  `write_batch_columnar(&RecordBatch)` whose default converts to rows and calls
  `write_batch`. No connector is forced to implement the columnar side.
- **Conversion shims.** Centralize `RecordBatch ↔ Vec<Value>` in one core module
  reusing `arrow-json`, so validation/quality/contract/masking (which operate on
  `Value`) can run against a converted view when a page is columnar. The layered
  validation passes ([schema architecture](../docs/architecture/schema.md)) stay
  `Value`-shaped in v1 — a columnar page is converted for those passes and, if
  unmodified, passed through columnar to the sink.

## Drawbacks

- **Two code paths.** Every stage must handle both representations or rely on a
  conversion shim; that is real, permanent maintenance surface.
- **Schema rigidity.** Arrow batches require a fixed schema per batch. Sources
  with ragged/heterogeneous records (the natural home of `Value`) cannot go
  columnar without imposing a schema — so the fast path helps analytical
  connectors, not document-shaped ones.
- **Connector-author complexity.** The framework's headline simplicity is "a
  record is a JSON value". Introducing Arrow risks eroding that; it must remain
  strictly optional and off the beginner path.
- **Validation semantics.** Running the `Value`-shaped quality/contract/masking
  passes over a converted view, then writing columnar, needs care so the two
  representations never diverge (e.g. a masking rewrite must invalidate the
  columnar payload).

## Rationale and alternatives

- **Replace `Value` with Arrow outright.** Rejected — it would sacrifice the
  connector-author ergonomics that are a core project value and break the
  ragged-record use cases.
- **Keep Arrow purely internal to individual connectors (status quo).** Viable
  and zero-risk, but forces Arrow-native connectors to round-trip through
  `Value` at the pipeline boundary, which is exactly the cost this RFC targets.
- **A separate "columnar pipeline" type.** Rejected — bifurcates the runtime and
  duplicates the checkpoint/DLQ/observability machinery.

## Prior art

Arrow itself is the reference for the columnar batch and zero-copy semantics.
Polars/DataFusion show Arrow as an execution substrate; Kafka Connect's
`SchemaAndValue` and Singer's row model show the row-oriented alternative we keep
as default. dbt/warehouse loaders increasingly accept Arrow/Parquet directly.

## Unresolved questions

- Must resolve before Accepted: whether the layered validation passes gain
  native columnar implementations or always operate on a converted view in v1.
- During implementation: batch-size interaction with `MAX_BATCH_SIZE`; how
  columnar pages interact with adaptive batching (row reslicing assumes rows).

## Future possibilities

- Zero-copy Arrow Flight transport between faucet instances.
- Vectorized transforms operating directly on `RecordBatch`.
- A columnar-aware DLQ envelope for analytical sinks.

## Related

- [RFC process](./README.md) · [RFC 0001 capabilities](./0001-capability-traits.md) · [RFC 0004 streaming](./0004-streaming-improvements.md)
- [ADR 0004 JSON record model](../docs/adr/0004-json-record-model.md) · [Performance](../docs/architecture/performance.md) · [Batching](../docs/architecture/batching.md)
