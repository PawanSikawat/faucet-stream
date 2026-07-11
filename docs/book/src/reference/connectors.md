# Connector catalog

faucet-stream ships **24 sources** and **18 sinks**. Each is a Cargo feature
(`source-<name>` / `sink-<name>`) and an independently published crate. Full API
docs are on [docs.rs](https://docs.rs/faucet-stream).

Run `faucet list` to see what's compiled into your binary, and
`faucet schema source <name>` / `faucet schema sink <name>` for a connector's
exact config fields. Not sure which to pick? See
[Choosing a connector](./choosing.md).

Legend: ✓ supported · ✗ not applicable. Tier: T1 = passes the faucet-conformance battery in CI; T2 = not yet wired into the battery.

## Sources

| Connector | Tier¹¹ | Feature | Streams¹ | Resumable² | Effectively-once³ | Compression | Discover¹⁰ | Underlying primitive |
|-----------|:---:|---------|:---:|:---:|:---:|:---:|:---:|----------------------|
| REST | T1 ✅ | `source-rest` | ✓ | ✓ | ✗ | ✗ | ✗ | HTTP + 6 pagination styles, JSONPath extraction |
| GraphQL | T2 | `source-graphql` | ✓ | ✗ | ✗ | ✗ | ✗ | cursor pagination, variable injection |
| XML / SOAP | T2 | `source-xml` | ✓ | ✗ | ✗ | ✗ | ✗ | streaming XML→JSON, dot-path extraction |
| gRPC | T2 | `source-grpc` | ✓⁴ | ✗ | ✗ | ✗ | ✗ | dynamic protobuf; unary + server-streaming |
| PostgreSQL | T2 | `source-postgres` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL query, rows as JSON |
| PostgreSQL CDC | T2 | `source-postgres-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | logical replication (pgoutput), LSN bookmarks |
| MySQL | T2 | `source-mysql` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL query, rows as JSON |
| MySQL CDC | T2 | `source-mysql-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | binlog row events, file/pos or GTID bookmarks |
| Microsoft SQL Server | T2 | `source-mssql` | ✓ | ✓⁸ | ✗ | ✗ | ✓ | SQL query (tiberius), rows as JSON |
| SQLite | T1 ✅ | `source-sqlite` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL query, rows as JSON |
| AWS S3 | T2 | `source-s3` | ✓⁵ | ✗ | ✗ | ✓ | ✓ | object reader: JSONL, JSON array, raw text |
| Google Cloud Storage | T2 | `source-gcs` | ✓⁵ | ✗ | ✗ | ✓ | ✓ | object reader: JSONL, JSON array, raw text |
| MongoDB | T2 | `source-mongodb` | ✓ | ✗ | ✗ | ✗ | ✓ | `find()` with filter/projection/sort |
| MongoDB CDC | T2 | `source-mongodb-cdc` | ✓ | ✓ | **✓** | ✗ | ✗ | Change Streams, resumeToken bookmarks; `max_staged_records` buffer cap |
| Redis | T2 | `source-redis` | ✓ | ✗ | ✗ | ✗ | ✗ | streams, lists, key patterns |
| Webhook | T2 | `source-webhook` | ✗⁶ | ✗ | ✗ | ✗ | ✗ | temporary HTTP server collecting POSTs |
| WebSocket | T2 | `source-websocket` | ✓ | ✗ | ✗ | ✗ | ✗ | live push feed; subscribe frames, reconnect, ping keepalive |
| CSV | T1 ✅ | `source-csv` | ✓ | ✗ | ✗ | ✓ | ✗ | CSV files as JSON; strict field count by default (`flexible: true` to tolerate ragged rows) |
| Elasticsearch | T2 | `source-elasticsearch` | ✓ | ✗ | ✗ | ✗ | ✓ | search/scroll API |
| Apache Kafka | T2 | `source-kafka` | ✓ | ✓ | **✓** | ✗ | ✗ | consumer; idle/max-messages termination, offset bookmarks |
| AWS Kinesis | T2 | `source-kinesis` | ✓ | ✓ | ✗ | ✗ | ✗ | per-shard GetRecords workers; sequence-number bookmarks, idle/max-messages termination |
| Apache Parquet | T2 | `source-parquet` | ✓ | ✗ | ✗ | ✗ | ✗ | local/glob/S3, vectorized Arrow reader, projection |
| BigQuery | T2 | `source-bigquery` | ✓ | ✗ | ✗ | ✗ | ✓ | `jobs.query` + pageToken pagination |
| Snowflake | T2 | `source-snowflake` | ✓ | ✗ | ✗ | ✗ | ✓ | SQL REST API, server-side partitions |
| Singer bridge ⚠️ | T2 ⚠️ | `source-singer` | ✓ | ✓⁹ | ✗ | ✗ | ✗ | runs an external Singer tap; NDJSON over stdout, STATE→bookmark. **Tier-2 / experimental** |

¹⁰ **Discover** = enumerates the datasets behind the connection for
[`faucet discover`](../cookbook/discover.md) (tables / collections / indices /
prefixes with schemas + row estimates where the catalog provides them).
¹ **Streams** = yields records in bounded-memory batches rather than buffering the
whole result. ² **Resumable** = persists a bookmark to a [state store](../cookbook/state.md)
so re-runs continue where they left off (incremental replication / CDC / Kafka
offsets). ³ **Effectively-once** = the source emits a complete resume position on
every page and replaying from a bookmark continues the record stream at exactly
that position (immutable-log sources: CDC WAL/binlog/change streams, Kafka
partition offsets); required for the atomic-watermark mechanism behind
`delivery: exactly_once` — see
[Effectively-once delivery](../cookbook/state.md#effectively-once-delivery).
⁴ gRPC streams natively in *server-streaming* mode; unary buffers the
single response. ⁵ S3/GCS stream in JSONL and raw-text modes; JSON-array mode
buffers one object. ⁶ Webhook is buffer-shaped by nature (it collects POSTs over
a window). ⁸ MSSQL is resumable only in `replication: incremental` mode (it
persists a tracking-column bookmark); in `full` mode it is not.
⁹ The Singer bridge is resumable via the tap's `STATE` messages, but the
*granularity* of resume (and whether re-emitted rows overlap) depends on the
individual tap — pair it with a keyed/upsert sink for clean, effectively-once
(idempotent at-least-once) behavior.

> **Support tiers.** A connector is **Tier-1 (supported)** when it invokes and
> passes the `faucet-conformance` battery in CI (valid config schema,
> bounded-memory streaming, and the further checks as they land). That battery
> **is** the tiering mechanism — there is no separate scheme. Connectors marked
> **⚠️ Tier-2 / experimental** (currently the Singer bridge) are best-effort:
> correctness bugs are fixed, but breadth of testing and upstream-drift tracking
> are not guaranteed.

## Sinks

Every sink exposes a `batch_size` knob for write-side re-chunking. For the
file/append sinks (`jsonl`, `csv`, `stdout`) it's a no-op — they write per record.

| Connector | Tier¹¹ | Feature | `batch_size` | Compression | Upsert⁸ | Effectively-once⁷ | Write unit |
|-----------|:---:|---------|:---:|:---:|:---:|:---:|------------|
| BigQuery | T2 | `sink-bigquery` | ✓ | ✗ | **✓** | **✓** | `tabledata.insertAll` streaming; in-place `MERGE` for upsert + effectively-once |
| PostgreSQL | T2 | `sink-postgres` | ✓ | ✗ | **✓** | **✓** | multi-row `INSERT` (JSONB or mapped cols) |
| JSON Lines | T1 ✅ | `sink-jsonl` | no-op | ✓ | ✗ | ✗ | buffered file append |
| Snowflake | T2 | `sink-snowflake` | ✓ | ✗ | ✗ | **✓** | SQL REST API; multi-statement `BEGIN;INSERT;MERGE;COMMIT` transaction for effectively-once |
| MySQL | T2 | `sink-mysql` | ✓ | ✗ | **✓** | **✓** | multi-row `INSERT` |
| Microsoft SQL Server | T2 | `sink-mssql` | ✓ | ✗ | **✓** | **✓** | multi-row `INSERT` (2100-param auto-split, per-row DLQ) |
| SQLite | T1 ✅ | `sink-sqlite` | ✓ | ✗ | **✓** | **✓** | transaction-wrapped batch |
| AWS S3 | T2 | `sink-s3` | ✓ | ✓ | ✗ | ✗ | JSONL objects, parallel uploads |
| Google Cloud Storage | T2 | `sink-gcs` | ✓ | ✓ | ✗ | ✗ | JSONL objects |
| MongoDB | T2 | `sink-mongodb` | ✓ | ✗ | **✓** | **✓** | `insert_many`; multi-document transaction for effectively-once (replica set required) |
| Redis | T2 | `sink-redis` | ✓ | ✗ | ✗ | **✓** | streams, lists, key-value (pipelined); `MULTI`/`EXEC` transaction for effectively-once |
| CSV | T2 | `sink-csv` | no-op | ✓ | ✗ | ✗ | buffered file rows; column set frozen from first batch (`on_unknown_field: warn`/`error`) |
| Elasticsearch | T2 | `sink-elasticsearch` | ✓ | ✗ | **✓** | ✗ | `_bulk` NDJSON (per-row DLQ) |
| HTTP | T2 | `sink-http` | ✓ | ✗ | ✗ | ✗ | POST, concurrent under a semaphore |
| Stdout | T2 | `sink-stdout` | no-op | ✗ | ✗ | ✗ | JSON Lines / pretty JSON / TSV |
| Apache Kafka | T2 | `sink-kafka` | ✓ | ✗ | ✗ | **✓** | producer, batched sends, multi-topic routing; transactional producer + compacted watermark side-topic for effectively-once |
| AWS Kinesis | T2 | `sink-kinesis` | ✓ | ✗ | ✗ | ✗ | batched PutRecords; partition-key routing, per-entry partial-failure retry (DLQ-routable) |
| Apache Parquet | T2 | `sink-parquet` | ✓ | ✗⁶ | ✗ | ✗ | local/S3, schema inference (re-inferred per file on rollover), row/byte rollover |
| Apache Iceberg | T2 | `sink-iceberg` | ✓ | ✗⁶ | ✗ | **✓** | REST/Glue/SQL/HMS catalog, local + cloud (S3/GCS) warehouses, `fast_append` snapshot, Parquet data files |

⁶ Parquet and Iceberg both handle compression internally at the Parquet column
level, so the file-level `compression` feature doesn't apply to either.
⁷ **Effectively-once** = commits data and a watermark token atomically; required for
`delivery: exactly_once`. The BigQuery sink does this via a multi-statement
`MERGE` transaction (distinct from its default streaming `insertAll` path); the
Kafka sink uses a transactional producer that writes each page's records plus a
commit-token record into a compacted side-topic in one Kafka transaction; the
Snowflake sink runs one multi-statement `BEGIN;INSERT;MERGE;COMMIT` request; the
Redis sink wraps the page plus a `_faucet_commit_token:<scope>` key in one
`MULTI`/`EXEC`; the MongoDB sink commits the page plus a watermark document in
one multi-document transaction (replica set required). Sinks configured with
`write_mode: upsert` + `key` also reach effectively-once via keyed dedup, with
any source. See
[Effectively-once delivery](../cookbook/state.md#effectively-once-delivery).
⁸ **Upsert** = supports `write_mode: upsert` / `delete` (insert-or-update and
delete by `key`) in addition to plain `append`. The SQL sinks require
column-mapping mode (`auto_map`, or `auto_columns` for mssql) and a
UNIQUE/PRIMARY KEY on `key`; the
schemaless sinks (MongoDB, Elasticsearch) map `key` to a match filter / `_id`.
Iceberg upsert is not yet supported (a follow-up, blocked on `iceberg-rust`). See
[Upsert / mirror tables](../cookbook/upsert.md).

## Data-integrity notes

A few connectors enforce defaults that prevent silent data loss or corruption.
Inspect the exact fields with `faucet schema source <name>` / `faucet schema sink <name>`.

- **CSV source** — strict by default. A row whose field count differs from the
  header raises an error naming the offending line. Set `flexible: true` to
  tolerate ragged rows (the pre-1.x behaviour). *(Breaking default change.)*
- **CSV sink** — the column set is frozen from the first batch (the header cannot
  be rewritten in place). A field that first appears in a later page is dropped;
  `on_unknown_field: warn` (default) emits a one-shot warning naming the dropped
  field(s), while `on_unknown_field: error` aborts with a typed error.
- **Parquet sink** — the Arrow schema is re-inferred per output file on rollover,
  so a file written after the source widens picks up the new schema. A Parquet
  file's schema is immutable once opened, so a field appearing only later *within
  a single file* is dropped with a per-file one-shot warning.
- **MongoDB CDC source** — `max_staged_records` (default unbounded) caps the
  in-memory change-event buffer (including under `batch_size: 0`) and aborts with
  a typed error rather than risking OOM, mirroring `postgres-cdc` / `mysql-cdc`.

## Schema evolution

The pipeline-level [`schema:`](../cookbook/schema-drift.md) block detects when an
incoming page's top-level shape diverges from the sink's destination schema and
applies one policy (`warn` / `ignore` / `fail` / `quarantine` / `evolve`). Which
sinks can actually *act* on it varies:

| Sink | Schema evolution |
|------|------------------|
| `postgres`, `mysql`, `mssql`, `sqlite`, `bigquery` | **✓ evolve** — in-place additive/widening DDL |
| `elasticsearch` | **✓ evolve** — can add fields only (existing-field type change is incompatible) |
| `iceberg` | detect-only — `warn`/`ignore`/`fail`/`quarantine` work; `evolve` blocked on upstream `iceberg-rust` (#255) |
| `jsonl`, `csv`, `stdout`, `mongodb`, `redis`, `http`, `kafka`, `s3`, `gcs`, `snowflake`, `parquet` | — (schemaless; the `schema:` policy is inert) |

`on_drift: evolve` against a detect-only or schemaless sink is rejected at
config-load. See [Schema drift](../cookbook/schema-drift.md) for the per-sink
nuances (e.g. SQLite widening is a no-op; Elasticsearch can only add fields).

## Authentication at a glance

| Family | Auth options |
|--------|--------------|
| REST / GraphQL / XML | Bearer, Basic, ApiKey (header), ApiKeyQuery, OAuth2 (client-credentials), TokenEndpoint, Custom headers — see [Auth cookbook](../cookbook/auth.md) |
| BigQuery | service-account key (path or inline JSON), application-default credentials |
| Snowflake | JWT key-pair, OAuth |
| Kafka | SASL (PLAIN/SCRAM) + TLS |
| WebSocket | none, Bearer token, Custom headers |
| Elasticsearch | basic, API key, bearer, none |
| S3 / GCS | cloud SDK credential chains (env, profile, metadata) |
| SQL databases | connection URL (with embedded credentials / TLS params) |

Inspect any connector's exact auth shape with `faucet schema source <name>` /
`faucet schema sink <name>`.

## Batching

Default `batch_size` is 1000; max is 1,000,000. `batch_size: 0` means "no
batching" — the source emits the whole result set in one page and the sink writes
it in one request (good for small lookup tables or load-job-style sinks). See
[Performance tuning](../operations/tuning.md).

---

¹¹ **Tier** = conformance status. **T1 ✅** means the connector adds a
`tests/conformance.rs` that invokes the reusable `faucet-conformance` battery
against the real connector and passes it in CI (valid config schema,
bounded-memory streaming, honest capabilities, and the further checks as they
land) — that battery is the single source of truth for the tier. **T2** means
the connector is not yet wired into the battery; most still have their own
integration tests, so T2 does **not** mean low quality. See the
[Faucet Connector Protocol (FCP v0)](../spec/faucet-connector-spec-v0.md) for the
full contract.
