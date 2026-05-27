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

## Object storage: S3/GCS source vs. Parquet source

- **`source-s3` / `source-gcs`** read objects as JSONL, a JSON array, or raw
  text. Use them for line-delimited JSON, logs, or text dumps.
- **`source-parquet`** reads columnar Parquet (local, glob, or S3) with a
  vectorized Arrow reader and column projection. Use it for analytical datasets —
  it's far faster and can skip columns you don't need.

**Rule of thumb:** the file is `.parquet` → Parquet source; it's JSON/text →
S3/GCS source. (The Parquet source reads from S3 directly, so you don't need the
S3 source in front of it.)

## Streaming: Redis vs. Kafka

- **`source-redis`** reads streams, lists, or key patterns. Great when Redis is
  already in your stack and volumes are modest.
- **`source-kafka`** is a real consumer with consumer-group offsets and
  resumable bookmarks. Use it for high-throughput event pipelines and durable,
  replayable streams.

**Rule of thumb:** durable, high-volume event stream → Kafka; lightweight
queue/cache already on hand → Redis.

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

The Postgres/MySQL/SQLite sinks can write either:

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

## Still unsure?

Run `faucet list` to see what's installed, `faucet schema source <name>` to
inspect a connector's config, and `faucet preview <config> --limit 10` to try a
source without writing anywhere.
