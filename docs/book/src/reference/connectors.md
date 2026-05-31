# Connector catalog

faucet-stream ships **21 sources** and **17 sinks**. Each is a Cargo feature
(`source-<name>` / `sink-<name>`) and an independently published crate. Full API
docs are on [docs.rs](https://docs.rs/faucet-stream).

Run `faucet list` to see what's compiled into your binary, and
`faucet schema source <name>` / `faucet schema sink <name>` for a connector's
exact config fields. Not sure which to pick? See
[Choosing a connector](./choosing.md).

Legend: ✓ supported · ✗ not applicable.

## Sources

| Connector | Feature | Streams¹ | Resumable² | Compression | Underlying primitive |
|-----------|---------|:---:|:---:|:---:|----------------------|
| REST | `source-rest` | ✓ | ✓ | ✗ | HTTP + 6 pagination styles, JSONPath extraction |
| GraphQL | `source-graphql` | ✓ | ✗ | ✗ | cursor pagination, variable injection |
| XML / SOAP | `source-xml` | ✓ | ✗ | ✗ | streaming XML→JSON, dot-path extraction |
| gRPC | `source-grpc` | ✓³ | ✗ | ✗ | dynamic protobuf; unary + server-streaming |
| PostgreSQL | `source-postgres` | ✓ | ✗ | ✗ | SQL query, rows as JSON |
| PostgreSQL CDC | `source-postgres-cdc` | ✓ | ✓ | ✗ | logical replication (pgoutput), LSN bookmarks |
| MySQL | `source-mysql` | ✓ | ✗ | ✗ | SQL query, rows as JSON |
| Microsoft SQL Server | `source-mssql` | ✓ | ✓⁷ | ✗ | SQL query (tiberius), rows as JSON |
| SQLite | `source-sqlite` | ✓ | ✗ | ✗ | SQL query, rows as JSON |
| AWS S3 | `source-s3` | ✓⁴ | ✗ | ✓ | object reader: JSONL, JSON array, raw text |
| Google Cloud Storage | `source-gcs` | ✓⁴ | ✗ | ✓ | object reader: JSONL, JSON array, raw text |
| MongoDB | `source-mongodb` | ✓ | ✗ | ✗ | `find()` with filter/projection/sort |
| Redis | `source-redis` | ✓ | ✗ | ✗ | streams, lists, key patterns |
| Webhook | `source-webhook` | ✗⁵ | ✗ | ✗ | temporary HTTP server collecting POSTs |
| WebSocket | `source-websocket` | ✓ | ✗ | ✗ | live push feed; subscribe frames, reconnect, ping keepalive |
| CSV | `source-csv` | ✓ | ✗ | ✓ | CSV files as JSON |
| Elasticsearch | `source-elasticsearch` | ✓ | ✗ | ✗ | search/scroll API |
| Apache Kafka | `source-kafka` | ✓ | ✓ | ✗ | consumer; idle/max-messages termination, offset bookmarks |
| Apache Parquet | `source-parquet` | ✓ | ✗ | ✗ | local/glob/S3, vectorized Arrow reader, projection |
| BigQuery | `source-bigquery` | ✓ | ✗ | ✗ | `jobs.query` + pageToken pagination |
| Snowflake | `source-snowflake` | ✓ | ✗ | ✗ | SQL REST API, server-side partitions |

¹ **Streams** = yields records in bounded-memory batches rather than buffering the
whole result. ² **Resumable** = persists a bookmark to a [state store](../cookbook/state.md)
so re-runs continue where they left off (incremental replication / CDC / Kafka
offsets). ³ gRPC streams natively in *server-streaming* mode; unary buffers the
single response. ⁴ S3/GCS stream in JSONL and raw-text modes; JSON-array mode
buffers one object. ⁵ Webhook is buffer-shaped by nature (it collects POSTs over
a window). ⁷ MSSQL is resumable only in `replication: incremental` mode (it
persists a tracking-column bookmark); in `full` mode it is not.

## Sinks

Every sink exposes a `batch_size` knob for write-side re-chunking. For the
file/append sinks (`jsonl`, `csv`, `stdout`) it's a no-op — they write per record.

| Connector | Feature | `batch_size` | Compression | Write unit |
|-----------|---------|:---:|:---:|------------|
| BigQuery | `sink-bigquery` | ✓ | ✗ | `tabledata.insertAll` (per-row DLQ) |
| PostgreSQL | `sink-postgres` | ✓ | ✗ | multi-row `INSERT` (JSONB or mapped cols) |
| JSON Lines | `sink-jsonl` | no-op | ✓ | buffered file append |
| Snowflake | `sink-snowflake` | ✓ | ✗ | SQL REST API |
| MySQL | `sink-mysql` | ✓ | ✗ | multi-row `INSERT` |
| Microsoft SQL Server | `sink-mssql` | ✓ | ✗ | multi-row `INSERT` (2100-param auto-split, per-row DLQ) |
| SQLite | `sink-sqlite` | ✓ | ✗ | transaction-wrapped batch |
| AWS S3 | `sink-s3` | ✓ | ✓ | JSONL objects, parallel uploads |
| Google Cloud Storage | `sink-gcs` | ✓ | ✓ | JSONL objects |
| MongoDB | `sink-mongodb` | ✓ | ✗ | `insert_many` |
| Redis | `sink-redis` | ✓ | ✗ | streams, lists, key-value (pipelined) |
| CSV | `sink-csv` | no-op | ✓ | buffered file rows |
| Elasticsearch | `sink-elasticsearch` | ✓ | ✗ | `_bulk` NDJSON (per-row DLQ) |
| HTTP | `sink-http` | ✓ | ✗ | POST, concurrent under a semaphore |
| Stdout | `sink-stdout` | no-op | ✗ | JSON Lines / pretty JSON / TSV |
| Apache Kafka | `sink-kafka` | ✓ | ✗ | producer, batched sends, multi-topic routing |
| Apache Parquet | `sink-parquet` | ✓ | ✗⁶ | local/S3, schema inference, row/byte rollover |

⁶ Parquet has internal columnar compression, so the file-level `compression`
feature doesn't apply.

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
