# Choosing a connector

Several connectors overlap. This page resolves the common "which one?" questions.
For the full feature grid see the [connector catalog](./connectors.md).

## PostgreSQL: query source vs. CDC

- **`source-postgres`** runs a SQL query and returns the rows. Use it for
  one-shot extracts, snapshots, or when you control an `updated_at` column and
  parameterize the query yourself. Simple, no special Postgres config.
- **`source-postgres-cdc`** streams every `INSERT`/`UPDATE`/`DELETE` from the
  write-ahead log via logical replication. Use it when you need **every change**
  (including deletes), low-latency capture, or resumability without a cursor
  column. Requires `wal_level = logical` and a publication, and retains WAL
  between runs. See the [CDC tutorial](../tutorials/postgres-cdc.md).

**Rule of thumb:** periodic snapshot → query source; continuous change feed → CDC.

## MySQL: query source vs. CDC

- **`source-mysql`** runs a SQL query and returns the rows — one-shot extracts,
  snapshots, or `updated_at`-driven incremental pulls you parameterize yourself.
  Simple, no special MySQL config.
- **`source-mysql-cdc`** streams every `INSERT`/`UPDATE`/`DELETE` from the binary
  log via row-based replication. Use it when you need **every change** (including
  deletes), low-latency capture, or resumability without a cursor column. Requires
  `binlog_format=ROW`, `binlog_row_image=FULL`, `binlog_row_metadata=FULL` (for
  column names), a unique `server_id`, and `REPLICATION SLAVE`/`REPLICATION CLIENT`
  grants; resumes from a `{file,pos}` (or GTID) bookmark. Targets transactional
  (InnoDB) tables. See the [connector reference](connectors.md).

**Rule of thumb (MySQL too):** periodic snapshot → query source; continuous change feed → CDC.

## MongoDB: query source vs. Change Streams (CDC)

- **`source-mongodb`** runs a `find()` with filter/projection/sort — snapshots and
  bounded extracts.
- **`source-mongodb-cdc`** tails MongoDB Change Streams for every document change,
  resumable via the opaque `resumeToken`. Requires a replica set or sharded
  cluster. See the [connector reference](connectors.md).

## Object storage: S3/GCS source vs. Parquet source

- **`source-s3` / `source-gcs`** read objects as JSONL, a JSON array, or raw
  text. Use them for line-delimited JSON, logs, or text dumps.
- **`source-parquet`** reads columnar Parquet (local, glob, or S3) with a
  vectorized Arrow reader and column projection. Use it for analytical datasets —
  it's far faster and can skip columns you don't need.

**Rule of thumb:** the file is `.parquet` → Parquet source; it's JSON/text →
S3/GCS source. (The Parquet source reads from S3 directly, so you don't need the
S3 source in front of it.)

## Live feeds: WebSocket vs. Webhook vs. Kafka/Redis

- **`source-websocket`** — connects *out* to a live push endpoint (`ws://`/`wss://`),
  optionally sends subscription frames, and streams each incoming message as a record.
  Use it for market data, chat feeds, telemetry, or any server that pushes over WebSocket.
  Live-only — no replay, no durable offset.
- **`source-webhook`** — opens a temporary HTTP server and *receives* inbound HTTP
  POSTs from external systems over a time window. Use it when the remote system pushes
  to you over HTTP rather than WebSocket.
- **`source-kafka`** / **`source-redis`** — broker-backed streaming with durable,
  replayable offsets and resumable bookmarks. Use these when you need guaranteed delivery
  and the ability to continue from where a previous run left off.

**Rule of thumb:** connecting out to a live WebSocket feed → `source-websocket`; receiving
inbound HTTP POST payloads → `source-webhook`; durable, replayable event stream →
`source-kafka` or `source-redis`.

## Streaming: Redis vs. Kafka vs. Kinesis

- **`source-redis`** reads streams, lists, or key patterns. Great when Redis is
  already in your stack and volumes are modest.
- **`source-kafka`** is a real consumer with consumer-group offsets and
  resumable bookmarks. Use it for high-throughput event pipelines and durable,
  replayable streams.
- **`source-kinesis`** consumes AWS Kinesis Data Streams shard-by-shard with
  resumable per-shard sequence checkpoints. Use it when your event stream is
  already on AWS — same termination knobs as the Kafka source.

**Rule of thumb:** durable, high-volume event stream → Kafka (self-managed /
Confluent) or Kinesis (AWS-native); lightweight queue/cache already on hand →
Redis.

## HTTP APIs: REST vs. GraphQL vs. XML vs. gRPC

- **`source-rest`** — JSON REST APIs. The most full-featured source: six
  pagination styles, seven auth strategies, incremental replication, partitions.
- **`source-graphql`** — GraphQL endpoints with cursor pagination and variable
  injection.
- **`source-xml`** — XML/SOAP APIs; converts XML to JSON with dot-path extraction.
- **`source-grpc`** — gRPC services via dynamic protobuf (`prost-reflect`), unary
  or server-streaming.

**Rule of thumb:** match the protocol the API speaks. For incremental/resumable
ingestion, REST has the richest support.

## Warehouses: when to read with BigQuery / Snowflake sources

Use **`source-bigquery`** / **`source-snowflake`** to *read out of* a warehouse
(e.g. to move a query result elsewhere). To *load into* one, use the matching
**sink**. To transform data already inside the warehouse, reach for
[dbt](https://www.getdbt.com/) — that's not faucet's job.

## Sinks: column-mapped vs. JSON blob (SQL databases)

The Postgres/MySQL/SQLite/SQL Server sinks can write either:

- **a single JSON/JSONB column** (`column_mapping: { type: jsonb, column: data }`)
  — schemaless, no DDL coupling, easiest to start with; or
- **auto-mapped columns** — one column per top-level field, for queryable
  relational tables.

**Rule of thumb:** exploratory / evolving schema → JSON column; stable schema you
query with SQL → mapped columns.

## File sinks: JSONL vs. CSV vs. Parquet vs. stdout

- **`sink-stdout`** — debugging and pipelines (`faucet preview` uses it).
- **`sink-jsonl`** — line-delimited JSON; lossless, streaming-friendly,
  gzip/zstd-capable.
- **`sink-csv`** — flat tabular output for spreadsheets/BI; nested fields flatten.
- **`sink-parquet`** — columnar analytical output with built-in compression and
  schema inference; best for large datasets consumed by analytics engines.

**Rule of thumb:** machine-to-machine JSON → JSONL; tabular for humans → CSV;
analytics at scale → Parquet.

## Parquet sink vs. Iceberg sink

Both write columnar Parquet files, but they serve different use cases:

- **`sink-parquet`** — writes raw Parquet files to a local path or S3 prefix.
  Simple, zero catalog dependency, compatible with any Parquet reader. Use it
  when you want portable files and don't need schema evolution, time-travel, or
  ACID snapshot isolation.
- **`sink-iceberg`** — writes Parquet data files and registers them in an Iceberg
  catalog (REST, AWS Glue, SQL-backed, or Hive Metastore). The catalog tracks
  schema, partitioning, and snapshot history, enabling time-travel queries,
  schema evolution, and atomic reads across concurrent writers. Requires a
  running catalog service.

**Rule of thumb:** portable raw files with no catalog → `sink-parquet`; managed
lakehouse table with snapshots, time-travel, and catalog-aware readers → `sink-iceberg`.

## Still unsure?

Run `faucet list` to see what's installed, `faucet schema source <name>` to
inspect a connector's config, and `faucet preview <config> --limit 10` to try a
source without writing anywhere.
