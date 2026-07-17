# ADR 0006 — `StateStore` abstraction with pluggable backends

*A three-method async trait over `Value`; light backends in core, heavy ones in their own crates.*

- **Status:** Accepted (implemented) — `crates/core/src/state.rs`; `faucet-state-redis`, `faucet-state-postgres`.

## Context

Bookmarks must be persisted durably for incremental replication and recovery. Where
and how they are stored varies by deployment: a local file for a single-node run, a
shared database for a cluster, an in-memory map for tests. The core must not care
which.

## Problem

Two competing pressures: (1) the pipeline needs *a* durable store to uphold the
[checkpoint-ordering invariant](./0002-checkpoint-ordering.md); (2) `faucet-core` is
the crate every connector depends on and must stay dependency-light — it cannot pull
a Redis or Postgres driver.

## Decision

Define a minimal async trait and split backends by weight:

```rust
trait StateStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError>;
    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError>;
    async fn delete(&self, key: &str) -> Result<(), FaucetError>;
}
```

- **In `faucet-core`:** `MemoryStateStore` and `FileStateStore` (one JSON file per
  key, atomic rename) — zero extra dependencies.
- **In their own crates:** `faucet-state-redis`, `faucet-state-postgres` — so their
  drivers never burden a connector author.
- **Key hygiene:** `validate_state_key` rejects empty/hidden/traversal-unsafe keys
  so a key is safe as a filename component.
- **Key scheme:** `{name}::{row_id}` (root), `{name}::{row_id}::{parent_key}`
  (child), plus subsystem-scoped keys (replication marker, backfill unit). See
  [state management](../architecture/state-management.md).
- **Exactly-once envelope:** the stored value is `wrap_state(bookmark, seq)` under
  exactly-once, still a plain `Value` from the store's perspective.

## Alternatives considered

- **A DB baked into core.** Rejected: forces a driver dependency on every connector
  and a deployment choice on every user.
- **A pure key→string KV interface** (no `Value`). Rejected: bookmarks are
  structured; JSON round-tripping keeps sources free to evolve their bookmark shape
  (invariant I8).
- **A richer trait** (range scans, TTLs, CAS). Rejected as premature: bookmarks are
  tiny and low-frequency; the three-method surface maximises backend portability.
  CAS is noted as future work.

## Trade-offs

- The `Value`-typed, three-method interface forgoes any backend-specific
  optimisation, but bookmark traffic is small and infrequent, so this is the right
  trade.
- `FileStateStore`'s one-file-per-key is simple and atomic but not ideal for
  thousands of matrix children — hence the Redis/Postgres backends.

## Consequences

- **Positive:** core stays dependency-light; deployments choose a backend without
  touching connectors; the store is fully generic over bookmark shape.
- **Negative:** no compare-and-swap fencing yet (concurrent writers to one key are
  the deployer's responsibility); heavier backends are separate crates to enable.

## Future work

- Optional CAS semantics for stores that support it, to fence concurrent writers.
- A typed bookmark trait exposing comparability for lag/SLA math.

## Related

- [State management](../architecture/state-management.md) · [recovery](../architecture/recovery.md)
- [Standards: state & durability](../standards/state.md)
- [ADR 0002 — Checkpoint ordering](./0002-checkpoint-ordering.md) · [ADR 0010 — Pipeline runtime](./0010-pipeline-runtime.md)
