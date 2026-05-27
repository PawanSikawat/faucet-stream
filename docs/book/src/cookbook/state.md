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
