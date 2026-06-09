# Incremental replication & state

For pipelines that run repeatedly, you usually want to fetch only what's new.
That requires two things: an **incremental replication method** on the source and
a **state store** to persist the bookmark between runs.

## Replication methods

- `FullTable` — fetch everything every run.
- `Incremental` — track a high-water mark on a `cursor_field` (e.g. `updated_at`,
  an auto-increment id) and only emit records past the last seen value.

```yaml
source:
  type: rest
  config:
    # …
    replication_method:
      type: Incremental
      cursor_field: updated_at
    primary_keys: [id]
```

## State stores

Attach a `state:` block so the bookmark survives between runs:

```yaml
state:
  type: file          # built into faucet-core
  config:
    path: ./state
```

Available backends:

| Backend | Crate | Use when |
|---------|-------|----------|
| `memory` | `faucet-core` | tests, one-shot runs (not persistent) |
| `file` | `faucet-core` | single host; one JSON file per key, atomic writes |
| `redis` | `faucet-state-redis` | shared/ephemeral state across hosts |
| `postgres` | `faucet-state-postgres` | shared, durable, transactional state |

```yaml
# Redis
state:
  type: redis
  config:
    connection_url: redis://localhost:6379
    namespace: faucet

# Postgres
state:
  type: postgres
  config:
    connection_url: postgres://user:pass@localhost/faucet
```

## How bookmarks advance

The pipeline reads the bookmark before fetching, and persists a new one **only
after the sink confirms** the page. Most sources emit a bookmark on the final
page; CDC-style sources emit one per committed transaction and get
per-transaction durability automatically. Either way, a crash can never advance
the bookmark past data that wasn't written — the next run re-fetches from the
last confirmed point.

## State keys

Each invocation has a state key so concurrent matrix rows don't collide:
`{name}::{row_id}` for roots and `{name}::{row_id}::{parent_record_key}` for DAG
children. The CDC source uses `postgres-cdc:<slot>`.

## Exactly-once delivery

### The at-least-once crash window

By default (`delivery: at_least_once`) the pipeline persists the bookmark
*after* the sink confirms the write. A crash in the small window between
"sink durably wrote the page" and "state store persisted the bookmark" causes the
page to be re-delivered on the next run. For most workloads, duplicates in the
destination can be handled by upsert logic or deduplication downstream.

For CDC pipelines landing into SQL databases or Iceberg, faucet can close that
window entirely.

### How exactly-once closes the gap

When `delivery: exactly_once`, the pipeline issues a monotonic **commit token** for
every bookmark-carrying page. Instead of a plain `write_batch`, it calls
`write_batch_idempotent(records, scope, token)`. The sink commits both the
records and the token atomically inside its own transaction:

- **SQL sinks** (postgres, mysql, mssql, sqlite) — an in-transaction `UPSERT`
  into a `_faucet_commit_token(scope TEXT, token TEXT)` watermark table.
- **Iceberg sink** — the token is written as snapshot summary properties
  `faucet.commit-scope` and `faucet.commit-token` on the committed snapshot.

On the *next run*, before writing each page, the pipeline reads the sink's
`last_committed_token` for the current scope. If the stored token is greater
than or equal to the page's own token, the sink already durably committed that
page — the pipeline **skips the write** and advances the state store. Zero
duplicates result from a crash at any point in the sequence.

### Supported sources and sinks

Only certain connectors are allowed in an exactly-once pipeline:

| Role | Allowed connectors | Why others are excluded |
|------|--------------------|------------------------|
| Source | `postgres-cdc`, `mysql-cdc`, `mongodb-cdc` | The source must deterministically replay the same pages from a given bookmark. Non-CDC / batch sources (REST, SQL query, etc.) do not replay deterministically — a different page on resume would cause the pipeline to silently skip records it never wrote. |
| Sink | `sqlite`, `postgres`, `mysql`, `mssql`, `iceberg` | The sink must be able to commit data and a watermark token atomically in a single transaction or snapshot. Sinks without transaction support cannot provide this guarantee. |

A DLQ (`dlq:` block) is incompatible with `exactly_once` in this version.

### Hard gate at config-load time

The four requirements (CDC source, idempotent sink, state store, no DLQ) are
validated when the config is loaded — `faucet validate` will report a clear
`config error` naming the offending row before any run starts. There is no
runtime fallback.

### Example: PostgreSQL CDC → PostgreSQL sink

```yaml
version: 1
name: cdc_exactly_once

pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: postgres://faucet:faucet@localhost:5432/appdb
      slot_name: faucet_slot
      publication_name: faucet_pub
      create_slot_if_missing: true
      idle_timeout: 30
  sink:
    type: postgres
    config:
      connection_url: postgres://writer:pass@localhost:5432/warehouse
      table_name: change_events
      column_mapping: auto_map
      batch_size: 1000
  state:
    type: file
    config:
      path: ./state

delivery: exactly_once
```

Validate the config before the first run:

```bash
faucet validate pipeline.yaml
```

### Monitoring

The `faucet_pipeline_pages_skipped_total{pipeline,row}` counter increments
each time the pipeline skips a page on resume because the sink already
committed it. A non-zero value on the first run after a crash is expected; a
persistently non-zero value on steady-state runs may indicate a state-store
or sink connectivity issue worth investigating.
