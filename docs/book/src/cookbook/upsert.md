# Upsert / mirror tables

By default every sink **appends** — each record becomes a new row. That is the
right behaviour for event logs and immutable history, but it is wrong for a
*mirror*: a destination table that should stay an exact, up-to-date replica of a
source table, where an updated source row updates the mirror in place and a
deleted source row disappears from the mirror.

Upsert-capable sinks add two more write modes — `upsert` and `delete` — keyed by
a configurable `key`, so faucet can keep a destination in sync with a changing
source instead of only ever growing it.

## Write modes

Each upsert-capable sink config carries three flattened fields (they appear at
the top level of the sink's `config`, alongside `table_name` etc.):

| Field | Default | Purpose |
|-------|---------|---------|
| `write_mode` | `append` | `append`, `upsert`, or `delete` |
| `key` | `[]` | Key columns. **Required and non-empty** for `upsert`/`delete`; ignored for `append` |
| `delete_marker` | (none) | `upsert` only — `{ field: <name>, values: [<str>, …] }`; rows whose `field` matches one of `values` become deletes instead of upserts |

- **`append`** — insert every record (the default; today's behaviour).
- **`upsert`** — insert-or-update by `key`. If `delete_marker` is set, rows whose
  marker field matches are routed to deletes instead; the marker field is
  stripped from the upserted row before writing.
- **`delete`** — delete by `key` for every record in the batch.

## Supported sinks and their native primitives

Eight sinks support `upsert`/`delete`; every other sink is append-only.

| Sink | Requires | Native primitive |
|------|----------|------------------|
| `postgres` | `column_mapping: auto_map` + UNIQUE/PK on `key` | `INSERT … ON CONFLICT … DO UPDATE` |
| `sqlite` | `column_mapping: auto_map` + UNIQUE/PK on `key` | `INSERT … ON CONFLICT … DO UPDATE` |
| `mysql` | `column_mapping: auto_map` + a PRIMARY/UNIQUE index whose columns **exactly match** `key` | `INSERT … ON DUPLICATE KEY UPDATE` |
| `mssql` | `column_mapping: auto_columns` + UNIQUE/PK on `key` | `MERGE` |
| `mongodb` | — (schemaless) | `replace_one(upsert)` / `delete_one`, `key` → match filter |
| `elasticsearch` | — (schemaless) | `_bulk` `index` / `delete`, `key` → `_id` |
| `bigquery` | a defined table schema + `key` columns | in-place `MERGE … USING UNNEST(@payload)` (no staging table) |
| `spanner` | `key` must equal the table's primary-key columns | `InsertOrUpdate` / `Delete` mutations (mutations always address the PK) |

The **SQL sinks require column-mapping mode** — `column_mapping: auto_map`
(postgres/mysql/sqlite) or `auto_columns` (mssql). The single-JSONB-column blob
mode cannot upsert because there is no per-column conflict target. They also require a
**UNIQUE or PRIMARY KEY constraint on the `key` columns** — that constraint is
what the database's `ON CONFLICT` / `ON DUPLICATE KEY` / `MERGE` matches against;
without it the upsert silently degrades to plain inserts. faucet does not create
the constraint for you; create it on the destination table first.

> **MySQL validates the index match at startup.** MySQL's `ON DUPLICATE KEY
> UPDATE` resolves against *whichever* unique index a row collides with — not the
> columns you name in `key`. So a `key` that doesn't correspond to a real
> PRIMARY/UNIQUE index would silently upsert on the wrong index. The MySQL sink
> therefore checks at construction that the configured `key` **exactly matches**
> (order-insensitively) the columns of some PRIMARY or UNIQUE index on the target
> table, and fails fast with a typed error if it does not — catching the
> mismatch before any data is written rather than corrupting rows.

The **schemaless sinks** (MongoDB, Elasticsearch) have no such requirement: the
`key` columns are joined into a document filter / `_id`, so the same record both
inserts and replaces.

> **Not yet supported:** Iceberg is append-only today — Iceberg upsert is blocked
> on equality-delete writer support in `iceberg-rust` (#225).

## Last-write-wins within a batch

A single batch may contain several changes to the same key (common with CDC — an
insert and three updates of one row in one transaction). faucet **deduplicates by
`key` within the batch, last-write-wins**: only the final action for each key is
applied. If the last action is a delete, the row is deleted; if it is an upsert,
the row is upserted — regardless of what came before it in the batch. This keeps
the write minimal and the result deterministic.

## Missing or null keys

`upsert`/`delete` need a key value for every row. A record that is not a JSON
object, is missing a `key` column, or has a `null` value in a `key` column cannot
be keyed:

- **With a DLQ configured**, the offending rows are routed to the dead-letter
  queue per-row (the rest of the batch still writes).
- **Without a DLQ**, the whole batch fails with a typed error so the bad data is
  never silently dropped.

## CDC → mirror with `cdc_unwrap`

The most common use of upsert is mirroring a database table via change-data
capture. CDC sources emit change-event **envelopes** (`{op, before, after, …}`),
not bare rows, so a [`cdc_unwrap`](./transforms.md#cdc_unwrap--normalize-cdc-change-events-into-flat-rows)
transform sits between the source and the sink: it flattens the envelope into a
single row and stamps an `__op` marker (`"u"` for insert/update, `"d"` for
delete). The sink's `delete_marker` then routes the `"d"` rows to deletes.

This is the shipped example
[`cli/examples/postgres_cdc_to_postgres_upsert.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/postgres_cdc_to_postgres_upsert.yaml):

```yaml
version: 1
name: pg_cdc_mirror
delivery: exactly_once

pipeline:
  source:
    type: postgres-cdc
    config:
      connection_url: ${env:SOURCE_PG_URL}
      slot_name: faucet_mirror
      publication_name: faucet_pub
      create_slot_if_missing: true
      idle_timeout: 30

  transforms:
    - type: cdc_unwrap

  sink:
    type: postgres
    config:
      connection_url: ${env:DEST_PG_URL}
      table_name: users_mirror
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
      delete_marker: { field: __op, values: [d] }

  state:
    type: file
    config:
      path: ./state
```

The destination table needs a UNIQUE/PRIMARY KEY on the `key` columns before the
first run:

```sql
CREATE TABLE IF NOT EXISTS users_mirror (id int4 PRIMARY KEY, name text);
```

Validate it offline (no database connection required):

```bash
faucet validate cli/examples/postgres_cdc_to_postgres_upsert.yaml
```

## Composing with effectively-once delivery

A keyed upsert **is** an effectively-once mechanism in its own right: any
source feeding an upsert-capable sink with `write_mode: upsert` + `key` is
accepted under [`delivery: exactly_once`](./state.md#effectively-once-delivery)
and reported by `faucet validate` as `effectively-once (keyed upsert)` — the
replayed records converge on the same keyed rows instead of duplicating. No
state store or watermark is required for this mechanism (state is still
recommended so re-runs are incremental).

The **atomic-watermark** mechanism additionally composes with upsert on the
four SQL sinks (`postgres`, `mysql`, `mssql`, `sqlite`), **BigQuery**, and
**MongoDB** (replica set required): the sink commits the upserted/deleted rows
**and** the monotonic commit token in a single transaction, so a crash-and-resume
never re-applies or skips a batch — the mirror stays exactly consistent with
the source even across restarts. Its requirements, checked at config-load time:

1. a positional-replay source (`postgres-cdc` / `mysql-cdc` / `mongodb-cdc` / `kafka`),
2. an idempotent sink (`postgres` / `mysql` / `mssql` / `sqlite` / `bigquery` / `mongodb`),
3. a **durable** `state:` block (not `memory`), and
4. **no** `dlq:` block (incompatible with the atomic-watermark path in this version —
   a missing/null-key row therefore fails the batch rather than being routed aside).

For BigQuery, the whole page is merged as one `jobs.query` request (~10 MB limit);
keep the CDC source's `batch_size` modest (the default 1 000 rows is fine for most
schemas; lower it for very wide rows that approach the limit).

Elasticsearch supports upsert but not the atomic watermark (`_bulk` cannot
commit a watermark atomically) — an upsert mirror into Elasticsearch reaches
effectively-once via the keyed-upsert mechanism instead.
