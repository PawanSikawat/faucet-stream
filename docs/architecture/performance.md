# Performance

*Why performance is the reason this project exists, and the concrete disciplines every connector follows.*

## Why it exists

The Primary Goal in `CLAUDE.md` is unambiguous: *all sources and sinks must be as
fast, efficient, and reliable as possible* — it is the number-one input to every
architectural decision. faucet-stream aims to be the fastest way to move data
between two endpoints in Rust. Performance is therefore not a late optimization
pass; it is a set of standing disciplines encoded in the connector conventions
and enforced in review.

## Problem it solves

Naïve connectors leak throughput in predictable ways: recreating clients per
call, one INSERT per row, serial I/O, unbuffered writes. Rather than optimize
these case-by-case, the project fixes them as *conventions* so every connector —
built-in or third-party — starts fast.

## The disciplines (from `.claude/rules/connectors.md`)

| Discipline | Rule | Where |
|---|---|---|
| **Reuse clients/connections** | Build HTTP/S3/DB clients in `new()`, store in the struct; never per-call. | every connector's `stream.rs`/`sink.rs` |
| **Connection pooling** | Configurable `max_connections` (default 10 source / 5 sink). | SQL sources/sinks |
| **Multi-row INSERT** | `INSERT … VALUES (…),(…),…`, never one INSERT per record. | SQL sinks |
| **Transaction wrapping** | Batches in `BEGIN`/`COMMIT`. | SQLite sink |
| **Parallel I/O** | `buffer_unordered(concurrency)` for concurrent object reads/writes; semaphore-bounded HTTP fan-out. | S3/GCS/Parquet/HTTP |
| **Bulk APIs** | Elasticsearch `_bulk`, BigQuery `insertAll`, Mongo `insert_many`, Redis pipelines. | applicable sinks |
| **Buffered I/O** | Buffered writers; CSV uses `spawn_blocking` to keep the async runtime unblocked. | file sinks |
| **Configurable concurrency** | Expose `concurrency` / `max_connections` with sane defaults. | all |

These are the same rules the [connector SDK](./connector-sdk.md) and the
[performance standard](../standards/performance.md) hold authors to.

## The memory model

Throughput is bounded by the [batching](./batching.md) model: memory is
**O(batch_size)**, not O(total records), because only one [page](./stream-pages.md)
is resident at a time. This is what lets a connector be both fast *and* safe on
arbitrarily large datasets — the two are not in tension here.

## Where the cost actually is

The dominant per-record cost in the current design is the record model itself:
records are `serde_json::Value` (see [ADR 0004](../adr/0004-json-record-model.md)),
which allocates per field and per string. For I/O-bound connectors (the common
case) this is dwarfed by network/DB latency, and the paging + bulk-API disciplines
keep it off the hot path. For CPU-bound, high-fan-in transforms it is the ceiling.

The evidence-backed direction to raise that ceiling is a columnar record model —
an Arrow-native page — which would make transforms and (de)serialization
zero-copy for connectors that already speak Arrow (Parquet, Arrow Flight,
DuckDB). That is deliberately an [RFC](../../rfcs/0002-arrow-support.md), not an
in-place rewrite, because it touches the one type every connector shares.

## Invariants

- **No per-call client construction** — a reviewable, testable rule (offline
  `connect_lazy` pool tests exist precisely so this is checkable).
- **No unbounded buffering** — `validate_batch_size` enforces the cap.
- **Optimize with evidence.** The benchmark harness (`BENCHMARKS.md`, `scripts/`)
  exists so a claimed speedup is measured, not asserted; `CLAUDE.md` forbids
  optimizing code without evidence.

## Trade-offs

- **`serde_json::Value` costs allocations** but keeps the connector SDK trivial
  (one dependency, no schema ceremony) — a chosen trade in favour of ecosystem
  friendliness. See [extensibility](./extensibility.md).
- **Bulk/pooled paths add code** vs a naïve loop, justified by throughput on the
  hot path.

## Failure scenarios

- **A connector that recreates its client per page** silently caps throughput;
  caught in review against these conventions, and often visible as inflated
  `faucet_source_page_duration_seconds`.
- **A page sized beyond a sink's request limit** — a batching concern; see
  [batching](./batching.md).

## Future evolution

- Arrow-native pages / zero-copy execution ([RFC 0002](../../rfcs/0002-arrow-support.md)).
- A standing, tracked benchmark suite gating regressions in CI (see
  [roadmap](./roadmap.md)).

## Related

- [Batching](./batching.md) · [Stream pages](./stream-pages.md) · [Connector SDK](./connector-sdk.md)
- [Performance standard](../standards/performance.md) · [Contributing: performance](../contributing/performance.md)
- [ADR 0004 — JSON record model](../adr/0004-json-record-model.md) · [RFC 0002 — Arrow support](../../rfcs/0002-arrow-support.md)
- Benchmarks: [../../BENCHMARKS.md](../../BENCHMARKS.md)
