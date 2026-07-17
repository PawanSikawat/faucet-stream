# Common mistakes

*The traps this codebase has actually hit — each with why it bites and the correct pattern.*

Every item below is grounded in a real rule, a real regression, or a real CI
gate in this repository. Read it before your first PR; it will save you a
review round-trip.

## 1. Retrying a non-idempotent write

**Mistake:** wrapping a plain `Sink::write_batch` in a retry.

**Why it bites:** `write_batch` makes no atomicity promise. If the request
commits server-side but the *response* is lost, a retry re-sends the batch and
**silently duplicates every row** — the worst bug class in a data-movement tool
(silent downstream corruption).

**Correct pattern:** the pipeline only retries writes when
`sink.supports_idempotent_writes()` is true (`run_stream`'s `with_retry_write!`
macro). If you're adding retry logic, gate it the same way, or route through
`write_batch_idempotent`, which is a no-op to replay. See the
[retries ADR](../adr/0007-retries.md).

## 2. Persisting a bookmark before the write is durable

**Mistake:** calling `StateStore::put` (or advancing any resume position) before
the sink has flushed.

**Why it bites:** a crash in that window leaves the checkpoint ahead of the data.
On resume the source skips records that were never written — silent data loss.

**Correct pattern:** never checkpoint yourself. The pipeline enforces
write → flush → `put` in that order across all three write paths. Your source
only needs `state_key` + `apply_start_bookmark`. See the
[checkpoint-ordering ADR](../adr/0002-checkpoint-ordering.md).

## 3. High-cardinality metric labels

**Mistake:** using a record id, URL, query string, or `parent_record_key` as a
Prometheus label.

**Why it bites:** cardinality explosion — the metric backend degrades or OOMs.

**Correct pattern:** labels are the fixed set `{pipeline, row, connector}` plus a
few enum-valued ones. High-cardinality values go on the tracing *span*
(`run_id`, `parent_record_key`), never a metric label. See
[observability](../architecture/observability.md).

## 4. Not declaring a crate's own dependency features

**Mistake:** using a dependency feature (e.g. `tokio`'s `macros` for
`tokio::select!`) without enabling it in *this crate's* `Cargo.toml`, relying on
another crate in the workspace to turn it on.

**Why it bites:** feature unification makes it compile locally and in a normal
`--all-features` build, but the `feature-check` CI matrix builds each connector
**in isolation** and it fails there.

**Correct pattern:** every crate enables in its own manifest every dependency
feature it uses. Build your crate alone (`cargo build -p faucet-source-foo`) to
check.

## 5. Asserting `serde_json::Map` key order

**Mistake:** a test asserting the *sequence* of keys in a `serde_json::Map`.

**Why it bites:** the `preserve_order` feature (on under `--all-features`)
switches the map from `BTreeMap` (sorted) to `IndexMap` (insertion order). The
test passes under `-p crate` and fails in CI.

**Correct pattern:** assert the *set* of keys/values, not their order.

## 6. Creating a crate at `0.x`

**Mistake:** scaffolding a new crate at `version = "0.1.0"`.

**Why it bites:** it violates a hard workspace rule — nothing we deploy ships a
`0.x` version — and breaks the coordinated-release assumptions.

**Correct pattern:** new crates start at **`version = "1.0.0"`**, in both the
crate `Cargo.toml` and its `[workspace.dependencies]` path entry.

## 7. Adding a connector without the registry entry

**Mistake:** wiring a new source/sink into the CLI but forgetting
[`connectors/registry.json`](../../connectors/registry.json).

**Why it bites:** the CLI `registry_index` test panics under `--all-features`
(`Test`/`Coverage` jobs) — and this isn't in the docs-sync trigger table, so it's
easy to miss.

**Correct pattern:** add a verified `registry.json` entry as part of the
[wiring checklist](./connector-authoring.md#wiring-checklist-ci-enforces-most-of-this).

## 8. Blindly updating a failing test

**Mistake:** changing an assertion so a test goes green after your code change.

**Why it bites:** the test may have been *correctly* catching a regression you
just introduced. Rewriting it to match hides the bug.

**Correct pattern:** investigate why the test broke first. Only change it once
you understand that the *new* behavior is the intended one.

## 9. Putting I/O or protocol logic in `config.rs`

**Mistake:** building a client, or encoding protocol details, inside the config
module.

**Why it bites:** `config.rs` is meant to be pure and trivially unit-testable,
and it's what `faucet schema`/`init` introspect. Mixing in I/O couples the two
and makes the config untestable offline.

**Correct pattern:** config structs and enums only in `config.rs`; all I/O in
`stream.rs`/`sink.rs`. See [connector-authoring](./connector-authoring.md).

## 10. Retrying a source read that isn't safe to replay under exactly-once

**Mistake:** enabling `delivery: exactly_once` with a query-based source.

**Why it bites:** a query re-executed on resume can return *different* content,
so skipping "already-committed" pages by sequence would drop or duplicate rows.

**Correct pattern:** the exactly-once gate in `cli/src/expand.rs` rejects
non-deterministic-replay sources up front. Only CDC/log sources qualify — or use
the keyed-upsert mechanism (`write_mode: upsert` + `key`). See
[recovery](../architecture/recovery.md).

## Related

- [Connector authoring](./connector-authoring.md)
- [Testing](./testing.md)
- [Debugging](./debugging.md)
- [Retries ADR](../adr/0007-retries.md)
- [Checkpoint-ordering ADR](../adr/0002-checkpoint-ordering.md)
