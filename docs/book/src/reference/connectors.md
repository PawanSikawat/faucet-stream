# Connector catalog

faucet-stream ships **19 sources** and **16 sinks**. Each is a Cargo feature
(`source-<name>` / `sink-<name>`) and an independently published crate. Full API
docs for every connector are on [docs.rs](https://docs.rs/faucet-stream).

Run `faucet list` to see what's compiled into your binary, and
`faucet schema source <name>` / `faucet schema sink <name>` for a connector's
exact config fields.

## Sources

| Connector | Feature | Notes |
|-----------|---------|-------|
| REST | `source-rest` | auth, 6 pagination styles, JSONPath extraction, schema inference, incremental |
| GraphQL | `source-graphql` | cursor pagination, variable injection |
| XML / SOAP | `source-xml` | XML→JSON, dot-path extraction |
| gRPC | `source-grpc` | dynamic protobuf; unary + server-streaming |
| PostgreSQL | `source-postgres` | run SQL, rows as JSON |
| PostgreSQL CDC | `source-postgres-cdc` | logical replication, resumable |
| MySQL | `source-mysql` | run SQL, rows as JSON |
| SQLite | `source-sqlite` | run SQL, rows as JSON |
| AWS S3 | `source-s3` | JSONL, JSON array, raw text |
| Google Cloud Storage | `source-gcs` | JSONL, JSON array, raw text |
| MongoDB | `source-mongodb` | `find()` with filter/projection/sort |
| Redis | `source-redis` | streams, lists, key patterns |
| Webhook | `source-webhook` | temporary HTTP server collecting POSTs |
| CSV | `source-csv` | CSV files as JSON |
| Elasticsearch | `source-elasticsearch` | search/scroll API |
| Apache Kafka | `source-kafka` | consumer, idle/max-messages termination |
| Apache Parquet | `source-parquet` | local/glob/S3, vectorized Arrow reader, projection |
| BigQuery | `source-bigquery` | `jobs.query` + pageToken pagination |
| Snowflake | `source-snowflake` | SQL REST API, server-side partitions, JWT/OAuth |

## Sinks

| Connector | Feature | Notes |
|-----------|---------|-------|
| BigQuery | `sink-bigquery` | streaming inserts, per-row DLQ |
| PostgreSQL | `sink-postgres` | JSONB or auto-mapped columns, multi-row INSERT |
| JSON Lines | `sink-jsonl` | buffered file output |
| Snowflake | `sink-snowflake` | SQL REST API, JWT/OAuth |
| MySQL | `sink-mysql` | JSON column or auto-mapped columns |
| SQLite | `sink-sqlite` | transaction-wrapped batches |
| AWS S3 | `sink-s3` | JSONL files, parallel uploads |
| Google Cloud Storage | `sink-gcs` | JSONL files |
| MongoDB | `sink-mongodb` | `insert_many` |
| Redis | `sink-redis` | streams, lists, key-value |
| CSV | `sink-csv` | CSV rows, buffered |
| Elasticsearch | `sink-elasticsearch` | `_bulk` API, per-row DLQ |
| HTTP | `sink-http` | POST records, concurrent |
| Stdout | `sink-stdout` | JSON Lines, pretty JSON, or TSV |
| Apache Kafka | `sink-kafka` | producer, batched sends, multi-topic routing |
| Apache Parquet | `sink-parquet` | local/S3, schema inference, row/byte rollover |

## Streaming & batching

Most sources stream natively (bounded memory). Most sinks expose a `batch_size`
that controls their natural write unit. The default batch size is 1000;
`batch_size: 0` means "no batching" (one large request) — useful for small lookup
tables or load-job-style sinks. See [Performance tuning](../operations/tuning.md).

> A capability matrix with per-connector streaming/state/auth/compression columns
> is planned (WS-6 of the adoption roadmap).
