# faucet-sink-sqlite

[![Crates.io](https://img.shields.io/crates/v/faucet-sink-sqlite.svg)](https://crates.io/crates/faucet-sink-sqlite)
[![Docs.rs](https://docs.rs/faucet-sink-sqlite/badge.svg)](https://docs.rs/faucet-sink-sqlite)
[![MSRV](https://img.shields.io/crates/msrv/faucet-sink-sqlite.svg)](https://github.com/PawanSikawat/faucet-stream/blob/main/rust-toolchain.toml)
[![License](https://img.shields.io/crates/l/faucet-sink-sqlite.svg)](https://github.com/PawanSikawat/faucet-stream#license)

**SQLite** sink for the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem. Writes JSON records into a SQLite table — either as serialized JSON text in a single column, or with top-level JSON keys auto-mapped to real table columns.

Reach for it when you want a zero-dependency, embedded landing table on local disk (or in memory) — a fast, transactional destination for events, change-data-capture mirrors, dev/test fixtures, or any pipeline that just needs a queryable file. The whole batch commits atomically inside a `BEGIN`/`COMMIT` transaction, so a partial write never leaves the table half-populated.

## Feature highlights

- **Two write strategies** — store each record as a serialized JSON text value (`json` mode), or auto-map top-level JSON keys directly onto table columns (`auto_map` mode) discovered via `PRAGMA table_info`.
- **Transactional batches** — every chunk is one multi-row `INSERT` wrapped in a `BEGIN`/`COMMIT`, so a batch commits all-or-nothing.
- **High write throughput** — multi-row INSERTs, WAL journal mode, and a 5-second `busy_timeout` keep the single writer fast and lock-tolerant.
- **Parameter-limit aware** — in `auto_map` mode the sink splits each chunk so `rows × columns` never exceeds SQLite's `SQLITE_MAX_VARIABLE_NUMBER`, so wide tables never fail with "too many SQL variables".
- **Native-typed binds** — in `auto_map` mode strings bind as `TEXT`, JSON numbers as `INTEGER`/`REAL`, booleans as `INTEGER` 0/1; arrays/objects bind as JSON text — so column affinity round-trips correctly.
- **Write modes** — `append` (default), `upsert` (`INSERT … ON CONFLICT … DO UPDATE`), and `delete`, all keyed on a UNIQUE/PRIMARY KEY column set.
- **Exactly-once delivery** — pairs with a CDC source to commit records and a watermark token in one transaction.
- **Dead-letter queue** — per-row error reporting routes failing rows to a DLQ instead of failing the whole batch.

## Installation

```bash
# As a library:
cargo add faucet-sink-sqlite
cargo add tokio --features full

# In the CLI (opt-in connector feature):
cargo install faucet-cli --features sink-sqlite

# Or via the umbrella crate:
cargo add faucet-stream --features sink-sqlite
```

## Quick start

```yaml
# pipeline.yaml — faucet run pipeline.yaml
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      endpoint: /v1/events
  sink:
    type: sqlite
    config:
      database_url: /data/app.db
      table_name: events
      column_mapping: auto_map
```

```bash
faucet run pipeline.yaml
```

## Configuration reference

### Core

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `database_url` | string | — *(required)* | SQLite database URL. A file path (`/tmp/app.db`), a `sqlite:` URL, or `sqlite::memory:` for an in-memory database. The file (and parent dirs) is created if missing. |
| `table_name` | string | — *(required)* | Target table name. Must already exist with the appropriate columns. |
| `column_mapping` | `SqliteColumnMapping` | `{ json: { column: "data" } }` | How JSON records map to columns — see [Column mapping](#column-mapping). |

### Batching & pooling

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | int | `1000` | Maximum rows per multi-row INSERT. **`0` = no batching** (write the whole page in one transaction). See [Streaming & batching](#streaming--batching). |
| `max_connections` | int | `1` | Connections in the pool. SQLite serializes writers at the file level, so one writer is the safe default — a multi-connection pool against one file races for the write lock and risks `SQLITE_BUSY`. Connections open in WAL mode with a 5s `busy_timeout`, so raising this lets extra connections read concurrently with the single writer. |

### Write mode

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `write_mode` | `"append"` \| `"upsert"` \| `"delete"` | `"append"` | Write semantics. Upsert/delete require `column_mapping: auto_map` and a UNIQUE/PRIMARY KEY on `key`. See [Write modes](#write-modes-upsert--delete). |
| `key` | `[string]` | `[]` | Key column(s) for upsert/delete. Required and non-empty when `write_mode` is not `append`. |
| `delete_marker` | `{ field: string, values: [string] }` | absent | `upsert` only: rows whose `field` matches one of `values` are routed to `DELETE`; the marker field is stripped from upsert rows. |

### Column mapping (`SqliteColumnMapping`)

| Variant | YAML | Description |
|---------|------|-------------|
| `Json { column }` | `{ json: { column: "data" } }` | Insert each record as a serialized JSON text string in a single column. The column name defaults to `"data"`. |
| `AutoMap` | `auto_map` | Map top-level JSON keys directly to table columns, discovered via `PRAGMA table_info(table_name)`. Only keys matching existing columns are inserted; extra keys are silently ignored. Records with no matching keys are skipped with a warning. |

## Examples

### JSON-column mode — store records as serialized JSON text

```sql
CREATE TABLE raw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);
```

```yaml
sink:
  type: sqlite
  config:
    database_url: /data/analytics.db
    table_name: raw_events
    column_mapping:
      json:
        column: data
    batch_size: 1000
```

### AutoMap mode — map JSON keys to table columns

```sql
CREATE TABLE events (
    user_id TEXT,
    event TEXT,
    amount REAL,
    created_at TEXT DEFAULT (datetime('now'))
);
```

```yaml
sink:
  type: sqlite
  config:
    database_url: /data/analytics.db
    table_name: events
    column_mapping: auto_map
    batch_size: 1000
```

A record missing a column (e.g. `{"user_id": "u2", "event": "signup"}` with no `amount`) binds SQL `NULL` for that column.

### In-memory database for testing

```yaml
sink:
  type: sqlite
  config:
    database_url: "sqlite::memory:"
    table_name: test_table
    column_mapping: auto_map
```

## Streaming & batching

The SQLite sink re-chunks each incoming `StreamPage` to keep individual multi-row INSERT statements within SQLite's per-statement parameter limits and to amortise per-transaction overhead.

- **`batch_size > 0`** (default `1000`) — the sink slices the incoming records into `batch_size`-row chunks and issues one multi-row INSERT per chunk, each wrapped in its own `BEGIN`/`COMMIT` transaction. **`1000` is the recommended value**: large enough to amortise transaction overhead, small enough to stay well under SQLite's default `SQLITE_MAX_VARIABLE_NUMBER` (32766 since 3.32.0). In `auto_map` mode the sink also splits each chunk further so `rows × columns` never exceeds that limit, so a wide table never fails with "too many SQL variables" regardless of `batch_size`.
- **`batch_size = 0`** — the "no batching" sentinel. The entire upstream `StreamPage` is written in a single multi-row INSERT inside one transaction. Use this when the source already emits page sizes tuned for SQLite (e.g. a Postgres source with `batch_size: 1000`). Pages large enough to push the parameter count past SQLite's per-statement limit will fail at the prepare step.

`batch_size` is purely a chunk-size knob — transaction wrapping, identifier quoting, and per-record error reporting are unchanged.

## Write modes (upsert / delete)

By default the sink uses `write_mode: append` — every record is inserted as a new row. Two additional modes are available when `column_mapping: auto_map` is set and the target table has a `UNIQUE` or `PRIMARY KEY` constraint on the key column(s):

| Mode | Behaviour |
|------|-----------|
| `append` (default) | Insert every record unconditionally. |
| `upsert` | Insert-or-update by `key` (last-write-wins via `ON CONFLICT … DO UPDATE`). Optionally route delete-marked rows to `DELETE` via `delete_marker`. |
| `delete` | Delete every record by `key`. |

**Requirements:**

- `column_mapping` must be `auto_map` — key columns must be real table columns, not embedded inside a JSON blob.
- The table must have a `UNIQUE` or `PRIMARY KEY` constraint on the `key` column(s) so SQLite's `ON CONFLICT` clause can enforce uniqueness.
- Rows within a single batch are deduped by key (last-write-wins) before writing, so a batch never conflicts with itself.
- A row missing or null in a key column fails. With a `dlq:` block configured, the good rows are still written and only the missing/null-key rows are routed to the DLQ per-row; without a DLQ the whole batch fails.

### CDC mirror with upsert + delete marker

The standard CDC → mirror shape: a CDC source feeds change events, the `cdc_unwrap` transform normalizes them to a flat row plus a `__op` marker, and the sink upserts (or deletes flagged rows).

```yaml
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://user:pass@localhost/db
      slot_name: faucet_slot
      publication_name: faucet_pub
  sink:
    type: sqlite
    config:
      database_url: /data/warehouse.db
      table_name: users
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
      delete_marker:
        field: __op
        values: [d, delete]
  state:
    type: file
    config:
      path: ./state
```

## Exactly-once delivery

`SqliteSink` implements `Sink::supports_idempotent_writes` (returns `true`) and the two companion hooks:

- `write_batch_idempotent(records, scope, token)` — writes `records` and UPSERTs the `token` into a `_faucet_commit_token(scope TEXT, token TEXT)` watermark table inside the **same `BEGIN`/`COMMIT` transaction**, so both either commit together or neither does.
- `last_committed_token(scope)` — reads the current watermark so the pipeline skips already-committed pages on resume.

To use exactly-once delivery, set `delivery: exactly_once` and pair this sink with a CDC source (`postgres-cdc`, `mysql-cdc`, `mongodb-cdc`) plus a `state:` block. A DLQ is not permitted in exactly-once mode. All four requirements are validated at config-load time (`faucet validate`) before any run starts.

```yaml
pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
  sink:
    type: sqlite
    config:
      database_url: /data/warehouse.db
      table_name: change_events
      column_mapping: auto_map
  state:
    type: file
    config:
      path: ./state
delivery: exactly_once
```

See the [Exactly-once delivery cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/state.html#exactly-once-delivery) for full rationale and the supported source/sink set.

## Schema evolution

`SqliteSink` reports its live destination schema via `current_schema()` (read from `PRAGMA table_info`, including the `notnull` flag), so the pipeline-level `schema:` policy can detect drift between an incoming page's top-level shape and the real table. All five `on_drift` modes (`warn` / `ignore` / `quarantine` / `fail` / `evolve`) work against this sink.

Under `on_drift: evolve`, `SqliteSink::evolve_schema()` is **add-column only**, owing to SQLite's limited `ALTER TABLE` and dynamic typing:

- **New columns** → `ALTER TABLE … ADD COLUMN`. SQLite has no `ADD COLUMN IF NOT EXISTS`, so the current columns are read first and any already present is skipped (idempotent by pre-check).
- **Type widenings are a no-op** — under SQLite's dynamic typing a column already accepts a value of any type, so there is nothing to alter (logged once at `debug`).
- **Nullability relaxations are a no-op** — SQLite cannot drop a `NOT NULL` constraint in place (it would require a full table rebuild, out of scope here); the column is left as-is (logged once at `debug`).

Incompatible changes (narrowing / type swaps) are never auto-applied — they are routed by `on_incompatible` (`fail` or `quarantine`). See the [schema-drift cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/schema-drift.html).

## Dead-letter queue

The sink reports per-row outcomes via `write_batch_partial`, so when a `dlq:` block is configured, rows that fail (e.g. a missing/null key column in upsert/delete mode) are wrapped in a DLQ envelope and routed to the configured DLQ sink while the rest of the batch still commits. Without a DLQ, a failing row fails the whole batch. See the [DLQ cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/dlq.html).

## Config loading & schema introspection

Load from YAML/JSON or environment:

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_sink_sqlite::SqliteSinkConfig;

// From a JSON file
let config: SqliteSinkConfig = load_json("config.json")?;

// From an .env file with a prefix
let config: SqliteSinkConfig = load_env_file(".env", "SQLITE_SINK")?;
```

```env
SQLITE_SINK_DATABASE_URL=/data/analytics.db
SQLITE_SINK_TABLE_NAME=raw_events
SQLITE_SINK_COLUMN_MAPPING='{"json":{"column":"data"}}'
SQLITE_SINK_BATCH_SIZE=1000
SQLITE_SINK_MAX_CONNECTIONS=1
```

Inspect the full JSON Schema with:

```bash
faucet schema sink sqlite
```

## Library usage

```rust
use faucet_core::{Pipeline, Sink};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = SqliteSinkConfig::new("/data/app.db", "events")
    .column_mapping(SqliteColumnMapping::AutoMap)
    .with_batch_size(1000)
    .max_connections(1);

let sink = SqliteSink::new(config).await?;

let records = vec![
    json!({"user_id": "u1", "event": "purchase", "amount": 29.99}),
    json!({"user_id": "u2", "event": "signup"}),
];
let rows_written = sink.write_batch(&records).await?;
println!("Wrote {rows_written} rows");
# Ok(())
# }
```

Building the upsert `WriteSpec` directly:

```rust
use faucet_core::{WriteMode, WriteSpec};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let config = SqliteSinkConfig {
    database_url: "sqlite:///data/warehouse.db".into(),
    table_name: "users".into(),
    column_mapping: SqliteColumnMapping::AutoMap,
    batch_size: 1000,
    max_connections: 1,
    write: WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
    },
};
let sink = SqliteSink::new(config).await?;
# Ok(())
# }
```

## How it works

- A connection pool is created in `SqliteSink::new()` using `sqlx::SqlitePool` with the configured `max_connections` (default `1`). Each connection opens in WAL journal mode with a 5-second `busy_timeout` and `create_if_missing`, so a writer and readers proceed concurrently and lock contention waits-and-retries instead of failing immediately with `SQLITE_BUSY`. WAL on a `sqlite::memory:` database is a harmless no-op.
- `write_batch()` slices the input into `batch_size`-row chunks (or forwards the whole slice when `batch_size = 0`). Each chunk is inserted with a single multi-row INSERT wrapped in a `BEGIN`/`COMMIT` transaction.
- In **JSON mode**, each record is serialized to a JSON string and inserted as `INSERT INTO t (col) VALUES (?), (?), …`.
- In **AutoMap mode**, column names are discovered via `PRAGMA table_info(table_name)`. The INSERT column set is the **union** of record keys across the batch (in table order), so a field present only in a later record is still written; a row missing a column binds SQL `NULL`. Values bind as native SQLite types (`TEXT`/`INTEGER`/`REAL`; booleans as `0`/`1`; arrays/objects as JSON text).
- All identifiers (table and column names) are quoted via `quote_ident()` (double-quote escaping) to prevent SQL injection.
- Transaction wrapping guarantees per-batch atomicity: either all rows in a chunk commit, or none do.

## Lineage dataset URI

`sqlite://<path>?table=<table>` — e.g. `sqlite:///tmp/test.db?table=events`.

## Feature flags

This crate has no optional features of its own; enable it in the CLI/umbrella via the `sink-sqlite` feature.

## Troubleshooting / FAQ

| Symptom | Likely cause & fix |
|---------|--------------------|
| `SQLITE_BUSY` / "database is locked" | Multiple writers contending for the file. Keep `max_connections: 1` (the default); only raise it for read-heavy WAL workloads. Ensure no external process holds a long write lock. |
| "too many SQL variables" | In JSON/`batch_size: 0` mode a single page exceeds SQLite's per-statement parameter limit (32766). Lower `batch_size` (e.g. `1000`). AutoMap mode auto-splits, so this is JSON-mode/large-page-specific. |
| AutoMap inserts nothing / "skipped with no matching keys" warning | Record keys don't match any column. Confirm the table exists and column names match the JSON top-level keys (AutoMap silently drops unmatched keys). |
| Upsert fails with `ON CONFLICT` error | The table has no `UNIQUE`/`PRIMARY KEY` on the `key` column(s). Add a constraint, e.g. `CREATE UNIQUE INDEX ON users(id)`. |
| Upsert/delete rejected at config load | `column_mapping` is not `auto_map`, or `key` is empty. Upsert/delete require `auto_map` and a non-empty `key`. |
| Exactly-once rejected by `faucet validate` | One of the four requirements is unmet: a CDC source, this sink, a `state:` block, and **no** `dlq:` block. |
| A row with a missing/null key column fails the whole batch | Add a `dlq:` block to route only the offending rows to the DLQ while the rest commit. |
| Numbers stored as text in AutoMap | Pass JSON numbers (`29.99`), not strings (`"29.99"`); strings bind as `TEXT`. |

## See also

- [Choosing a connector](https://pawansikawat.github.io/faucet-stream/reference/choosing.html)
- [Connector capability matrix](https://pawansikawat.github.io/faucet-stream/reference/connectors.html)
- [Upsert cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/upsert.html)
- [State & exactly-once cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/state.html)
- [`faucet-source-sqlite`](https://crates.io/crates/faucet-source-sqlite) — the matching SQLite query source.
- [`faucet-sink-postgres`](https://crates.io/crates/faucet-sink-postgres) / [`faucet-sink-mysql`](https://crates.io/crates/faucet-sink-mysql) — server-database sinks with the same write-mode surface.

## License

Licensed under either of [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) or [MIT license](https://opensource.org/licenses/MIT) at your option.
