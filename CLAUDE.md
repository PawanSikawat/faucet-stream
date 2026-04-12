# CLAUDE.md

## Library Purpose

`faucet-stream` is a modular, config-driven data pipeline toolkit for Rust with pluggable **source** and **sink** connectors.

- **Sources** fetch data from external systems (e.g. REST APIs).
- **Sinks** write data to external systems (e.g. BigQuery).

Design goal: callers configure a source or sink once, call `fetch_all()` or `write_batch()`, and get/write all records — no manual pagination loop, no auth boilerplate.

This is a library workspace — there is no binary, no database, no migrations, no server.

## Workspace Structure

The project is a Cargo workspace with 27 crates:

| Crate | Path | Description |
|-------|------|-------------|
| `faucet-core` | `crates/core/` | Shared types, traits (`Source`, `Sink`), pipeline orchestration, transforms, error types |
| `faucet-source-rest` | `crates/source/rest/` | REST API source — auth, pagination, extraction, schema inference |
| `faucet-source-graphql` | `crates/source/graphql/` | GraphQL API source — cursor pagination, variable injection |
| `faucet-source-xml` | `crates/source/xml/` | XML/SOAP API source — XML-to-JSON conversion, dot-path extraction |
| `faucet-source-grpc` | `crates/source/grpc/` | gRPC source — dynamic protobuf via `prost-reflect` |
| `faucet-source-postgres` | `crates/source/postgres/` | PostgreSQL query source — run SQL, return rows as JSON |
| `faucet-source-mysql` | `crates/source/mysql/` | MySQL query source — run SQL, return rows as JSON |
| `faucet-source-sqlite` | `crates/source/sqlite/` | SQLite query source — run SQL, return rows as JSON |
| `faucet-source-s3` | `crates/source/s3/` | AWS S3 source — read objects as JSONL, JSON array, or raw text |
| `faucet-source-mongodb` | `crates/source/mongodb/` | MongoDB source — find() query with filter/projection/sort |
| `faucet-source-redis` | `crates/source/redis/` | Redis source — read from streams, lists, or key patterns |
| `faucet-source-webhook` | `crates/source/webhook/` | Webhook source — temporary HTTP server collecting POST payloads |
| `faucet-source-csv` | `crates/source/csv/` | CSV file source — read CSV rows as JSON objects |
| `faucet-source-elasticsearch` | `crates/source/elasticsearch/` | Elasticsearch source — search/scroll API pagination |
| `faucet-sink-bigquery` | `crates/sink/bigquery/` | Google BigQuery streaming insert sink |
| `faucet-sink-postgres` | `crates/sink/postgres/` | PostgreSQL sink — JSONB or auto-mapped columns |
| `faucet-sink-jsonl` | `crates/sink/jsonl/` | JSON Lines file sink |
| `faucet-sink-snowflake` | `crates/sink/snowflake/` | Snowflake SQL REST API sink |
| `faucet-sink-mysql` | `crates/sink/mysql/` | MySQL sink — JSON column or auto-mapped columns |
| `faucet-sink-sqlite` | `crates/sink/sqlite/` | SQLite sink — JSON column or auto-mapped columns |
| `faucet-sink-s3` | `crates/sink/s3/` | AWS S3 sink — write JSONL files to S3 bucket |
| `faucet-sink-mongodb` | `crates/sink/mongodb/` | MongoDB sink — insert_many documents |
| `faucet-sink-redis` | `crates/sink/redis/` | Redis sink — write to streams, lists, or key-value pairs |
| `faucet-sink-csv` | `crates/sink/csv/` | CSV file sink — write JSON records as CSV rows |
| `faucet-sink-elasticsearch` | `crates/sink/elasticsearch/` | Elasticsearch sink — bulk index API |
| `faucet-sink-http` | `crates/sink/http/` | HTTP POST sink — send records to HTTP endpoint |
| `faucet-stream` | `faucet-stream/` | Umbrella crate — feature-gated re-exports of all connectors |

### Crate Dependency Graph

```
faucet-core  <──  faucet-source-rest
             <──  faucet-source-graphql
             <──  faucet-source-xml
             <──  faucet-source-grpc
             <──  faucet-source-postgres
             <──  faucet-source-mysql
             <──  faucet-source-sqlite

             <──  faucet-source-s3
             <──  faucet-source-mongodb
             <──  faucet-source-redis
             <──  faucet-source-webhook
             <──  faucet-source-csv
             <──  faucet-source-elasticsearch
             <──  faucet-sink-bigquery
             <──  faucet-sink-postgres
             <──  faucet-sink-jsonl
             <──  faucet-sink-snowflake
             <──  faucet-sink-mysql
             <──  faucet-sink-sqlite

             <──  faucet-sink-s3
             <──  faucet-sink-mongodb
             <──  faucet-sink-redis
             <──  faucet-sink-csv
             <──  faucet-sink-elasticsearch
             <──  faucet-sink-http
             <──  faucet-stream (umbrella, all optional)
```

## Keeping This File Up to Date

**Whenever you change the project structure, add/remove crates, modify traits, add new patterns, or change any fundamental aspect of the codebase, update this CLAUDE.md file immediately to reflect those changes.** This file is the single source of truth for how the project works. Stale documentation wastes time on every future conversation.

Specifically, update CLAUDE.md when:
- Adding, removing, or renaming crates or modules
- Changing trait signatures (`Source`, `Sink`, `Pipeline`)
- Adding new re-exports to `faucet-core`
- Changing error variants in `FaucetError`
- Adding new workspace dependencies
- Changing feature flags
- Adding new architectural patterns or conventions

**Also update the README.md of any crate you modify.** Every crate under `crates/source/`, `crates/sink/`, `crates/core/`, and `faucet-stream/` has its own README. When you change a crate's config fields, add new methods, change defaults, add auth variants, or modify behavior, update that crate's README to reflect the change. The root `README.md` should also be updated if the change affects the overall project description or examples.

**When adding or removing a source/sink connector**, also update `.github/workflows/ci.yml` to add/remove the feature from the `feature-check` matrix so it gets tested in isolation.

## Primary Goal

**All sources and sinks must be as fast, efficient, and reliable as possible.** This is the number one priority for every decision — architecture, implementation, dependency choice, and API design. Performance and reliability are not afterthoughts; they are the reason this library exists. Every connector should be the fastest way to move data between its endpoints in Rust.

## Code Quality Standard

Every code change must be production-library quality:

- Correctness first — subtle bugs in pagination state or retry logic silently corrupt downstream consumers
- No shortcuts on error handling — every failure path must be explicit and typed in `FaucetError`
- Think about failure modes: what if the API returns an empty page mid-stream? If the Link header is malformed? If the JSONPath matches nothing?
- No hardcoded credentials, tokens, or service URLs — ever
- Public API must be self-documenting and stable

When reviewing or modifying any part of the codebase, **proactively fix** any issue that falls below this standard. If a change is large enough to warrant a conversation first, call it out clearly and explain the fix, but default to fixing rather than flagging.

## Third-Party Connector Friendliness

This crate is designed as a **marketplace ecosystem** — third-party developers should be able to build and publish their own `faucet-source-*` and `faucet-sink-*` crates with minimal friction. Every change must preserve and improve this experience:

- **`faucet-core` is the only required dependency** for connector authors. It re-exports `async_trait`, `serde_json` (`Value`, `json!`), and `schemars` (`JsonSchema`, `schema_for!`) so third-party crates don't need to add those separately. If a new common dependency is needed by connector authors, re-export it from `faucet-core` rather than requiring them to add it.
- **`Source` and `Sink` traits must stay simple and object-safe.** Don't add methods that require connector-specific types, complex generics, or associated types that break `Box<dyn Source>` / `Box<dyn Sink>`. New trait methods must have default implementations so existing connectors don't break.
- **`FaucetError` must accommodate third-party errors.** The `Custom(Box<dyn Error + Send + Sync>)` variant lets connector authors wrap their own error types without losing the chain. Don't remove it. If adding new error variants, consider whether third-party connectors would need them.
- **`Pipeline` must remain generic** over any `Source` + `Sink` combination. Don't introduce coupling to specific connectors in the pipeline or core crate.
- **Naming convention: `faucet-source-<name>` / `faucet-sink-<name>`** — all first-party crates follow this, and the README guides third-party authors to do the same.
- **Don't add mandatory dependencies to `faucet-core`** that connector authors wouldn't need (e.g. database drivers, cloud SDKs). Keep core lightweight — connector-specific deps belong in their own crates.

## Performance

All connectors must be optimised for throughput by default. When modifying or adding connectors, apply these principles:

- **Reuse clients/connections** — create S3 clients, MongoDB clients, Redis connections, HTTP clients in `new()` and store in the struct. Never recreate per-call.
- **Connection pooling** — all database connectors (PostgreSQL, MySQL, SQLite) must use configurable `max_connections` pools (default: 10 for sources, 5 for sinks).
- **Multi-row INSERT** — database sinks (PostgreSQL, MySQL, SQLite) must use multi-row `INSERT INTO ... VALUES (...), (...), ...` instead of one INSERT per record.
- **Transaction wrapping** — SQLite sink wraps batches in `BEGIN`/`COMMIT` transactions.
- **Parallel I/O** — S3 source/sink uses `buffer_unordered()` for concurrent object reads/writes. HTTP sink sends Individual-mode requests concurrently via semaphore. REST source processes partitions concurrently when `partition_concurrency` is set.
- **Bulk APIs** — prefer bulk/batch APIs when available (Elasticsearch bulk NDJSON, BigQuery insertAll, MongoDB insert_many, Redis pipelines + MGET).
- **Buffered I/O** — file sinks (JSONL, CSV) must use buffered writers. CSV uses `spawn_blocking` to avoid blocking the async runtime.
- **Configurable concurrency** — expose `concurrency` or `max_connections` fields on configs with sensible defaults so users can tune throughput.

## Commands

```bash
# Build (all crates)
cargo build --workspace

# Run all tests (no external dependencies required)
cargo test --workspace --all-features

# Format
cargo fmt --all

# Lint
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Lint and apply safe fixes automatically
cargo clippy --workspace --fix --allow-dirty --allow-staged

# Dry-run publish (verify all crates are publishable)
cargo publish --dry-run -p faucet-core
cargo publish --dry-run -p faucet-source-rest
cargo publish --dry-run -p faucet-sink-bigquery
cargo publish --dry-run -p faucet-stream
```

## Architecture

### faucet-core (`crates/core/`)

- **`src/lib.rs`** — crate root; re-exports `FaucetError`, `Source`, `Sink`, `Pipeline`, `PipelineResult`, `run_stream`, `RecordTransform`, `ReplicationMethod`, `SourceDAG`, `DagResult`, `DagNodeResult`, `DagNodeError`, `DagNode`. Also re-exports third-party crates for connector authors: `async_trait`, `serde_json` (+ `Value`, `json!`), `schemars` (+ `JsonSchema`, `schema_for!`)
- **`src/error.rs`** — `FaucetError` enum: `Http`, `HttpStatus`, `Json`, `JsonPath`, `Auth`, `RateLimited`, `Url`, `Transform`, `Config`, `Source`, `Sink`, `Custom(Box<dyn Error>)`
- **`src/config.rs`** — Config loading utilities: `load_json()` (from JSON file), `load_env()` (from env vars with prefix), `load_env_file()` (load `.env` then env vars). Also `duration_secs` and `duration_secs_option` serde helper modules for `Duration` fields
- **`src/util.rs`** — Shared utilities: `quote_ident()` (SQL injection prevention), `extract_records()` (JSONPath extraction), `check_http_response()` (HTTP status error handling), `substitute_context()` (placeholder substitution for URLs/paths — NOT safe for SQL or JSON), `substitute_context_bind_params()` (SQL-safe substitution using bind parameter markers), `substitute_context_json()` (JSON-safe substitution with proper escaping), `extract_context()` (JSONPath-based context extraction from parent records)
- **`src/traits.rs`** — `Source` and `Sink` async traits. `Source` uses `fetch_with_context()` as primary method (receives parent context); `fetch_all()` is a convenience wrapper. Both include `config_schema(&self) -> Value` method that returns a JSON Schema describing the connector's configuration (auto-generated via `schemars`)
- **`src/pipeline.rs`** — `Pipeline` struct (batch source→sink), `run_stream()` (streaming source→sink), `PipelineResult`
- **`src/transform.rs`** — `RecordTransform` enum + `CompiledTransform`: flatten, rename keys (regex), snake_case, custom closures; feature-gated built-ins
- **`src/replication.rs`** — `ReplicationMethod` enum, `filter_incremental()`, `max_replication_value()` for bookmark-based incremental replication
- **`src/schema.rs`** — `infer_schema()`: JSON Schema inference from record samples with type merging and nullable detection
- **`src/dag.rs`** — `SourceDAG` builder and executor: parent-child DAG of source-sink pairs with context passing, concurrent child execution, and non-fatal error collection

### faucet-source-rest (`crates/source/rest/`)

- **`src/lib.rs`** — crate root; re-exports core types + REST-specific types
- **`src/config.rs`** — `RestStreamConfig` struct with fluent builder (base_url, path, method, auth, headers, query_params, body, pagination, records_path, max_pages, request_delay, timeout, max_retries, retry_backoff, replication, transforms, partitions, schema, tolerated_http_errors). All config types derive `Serialize, Deserialize, JsonSchema`
- **`src/stream.rs`** — `RestStream`: the main executor; `new(config)`, `fetch_all()`, `fetch_all_as::<T>()`, `fetch_all_incremental()`, `infer_schema()`, `stream_pages()`; implements `faucet_core::Source` (incl. `config_schema()`)
- **`src/auth/`** — `Auth` enum + per-strategy impls: `bearer.rs`, `basic.rs`, `api_key.rs`, `custom.rs`, `oauth2.rs`, `token_endpoint.rs`
- **`src/pagination/`** — `PaginationStyle` enum + `PaginationState` + per-strategy impls: `cursor.rs`, `page.rs`, `offset.rs`, `link_header.rs`, `next_link_body.rs`
- **`src/extract/`** — `extract_records()`: JSONPath extraction from response bodies
- **`src/retry/`** — `execute_with_retry()`: generic exponential backoff retry executor
- **`src/serde_helpers.rs`** — `http_method` module: serialize/deserialize `reqwest::Method` as string

### faucet-source-graphql (`crates/source/graphql/`)

- **`src/lib.rs`** — crate root; re-exports config + stream types
- **`src/config.rs`** — `GraphqlStreamConfig`, `GraphqlAuth`, `GraphqlPagination` (Relay cursor)
- **`src/stream.rs`** — `GraphqlStream`: cursor pagination loop, JSONPath extraction, GraphQL error handling; implements `faucet_core::Source`

### faucet-source-xml (`crates/source/xml/`)

- **`src/lib.rs`** — crate root; re-exports config + stream types
- **`src/config.rs`** — `XmlStreamConfig`, `XmlAuth`, `XmlPagination` (page-number, offset)
- **`src/convert.rs`** — `xml_to_json()`: event-based XML-to-JSON conversion; `extract_at_path()`: dot-path record extraction
- **`src/stream.rs`** — `XmlStream`: pagination, XML-to-JSON conversion pipeline; implements `faucet_core::Source`
- **`src/serde_helpers.rs`** — `http_method` module: serialize/deserialize `reqwest::Method` as string

### faucet-source-grpc (`crates/source/grpc/`)

- **`src/lib.rs`** — crate root; re-exports config + stream types
- **`src/config.rs`** — `GrpcStreamConfig`, `GrpcAuth` (bearer, metadata)
- **`src/stream.rs`** — `GrpcStream`: dynamic protobuf via `prost-reflect`, custom `DynamicCodec` for tonic; implements `faucet_core::Source`

### faucet-sink-bigquery (`crates/sink/bigquery/`)

- **`src/lib.rs`** — crate root; re-exports core types + sink types
- **`src/config.rs`** — `BigQuerySinkConfig` with builder, `BigQueryCredentials` enum
- **`src/sink.rs`** — `BigQuerySink`: streaming insert executor; implements `faucet_core::Sink`

### faucet-sink-postgres (`crates/sink/postgres/`)

- **`src/lib.rs`** — crate root; re-exports config + sink types
- **`src/config.rs`** — `PostgresSinkConfig`, `PostgresColumnMapping` (Jsonb or AutoMap)
- **`src/sink.rs`** — `PostgresSink`: JSONB unnest inserts or auto-mapped column inserts; implements `faucet_core::Sink`

### faucet-sink-jsonl (`crates/sink/jsonl/`)

- **`src/lib.rs`** — crate root; re-exports config + sink types
- **`src/config.rs`** — `JsonlSinkConfig` with builder (path, append, pretty)
- **`src/sink.rs`** — `JsonlSink`: lazy file open, buffered async writes; implements `faucet_core::Sink`

### faucet-sink-snowflake (`crates/sink/snowflake/`)

- **`src/lib.rs`** — crate root; re-exports config + sink types
- **`src/config.rs`** — `SnowflakeSinkConfig`, `SnowflakeAuth` (KeyPair JWT, OAuth)
- **`src/sink.rs`** — `SnowflakeSink`: SQL REST API with JWT/OAuth auth, PARSE_JSON inserts; implements `faucet_core::Sink`

### faucet-source-postgres (`crates/source/postgres/`)

- **`src/config.rs`** — `PostgresSourceConfig` with connection_url, query, params
- **`src/stream.rs`** — `PostgresSource`: PgPool, row-to-JSON conversion; implements `faucet_core::Source`

### faucet-source-mysql (`crates/source/mysql/`)

- **`src/config.rs`** — `MysqlSourceConfig` with connection_url, query
- **`src/stream.rs`** — `MysqlSource`: MySqlPool, row-to-JSON conversion; implements `faucet_core::Source`

### faucet-source-sqlite (`crates/source/sqlite/`)

- **`src/config.rs`** — `SqliteSourceConfig` with database_url, query
- **`src/stream.rs`** — `SqliteSource`: SqlitePool, `sqlite_value_to_json()` type probing, row-to-JSON conversion; implements `faucet_core::Source`

### faucet-source-s3 (`crates/source/s3/`)

- **`src/config.rs`** — `S3SourceConfig`, `S3FileFormat` (JsonLines, JsonArray, RawText)
- **`src/stream.rs`** — `S3Source`: list + get objects, format-based parsing; implements `faucet_core::Source`

### faucet-source-mongodb (`crates/source/mongodb/`)

- **`src/config.rs`** — `MongoSourceConfig` with filter, projection, sort, limit
- **`src/stream.rs`** — `MongoSource`: find() with BSON conversion; implements `faucet_core::Source`

### faucet-source-redis (`crates/source/redis/`)

- **`src/config.rs`** — `RedisSourceConfig`, `RedisSourceType` (List, Stream, Keys)
- **`src/stream.rs`** — `RedisSource`: LRANGE/XREAD/SCAN+GET; implements `faucet_core::Source`

### faucet-source-webhook (`crates/source/webhook/`)

- **`src/config.rs`** — `WebhookSourceConfig` with listen_addr, path, timeout, max_payloads
- **`src/stream.rs`** — `WebhookSource`: temporary axum server collecting POSTs; implements `faucet_core::Source`

### faucet-source-csv (`crates/source/csv/`)

- **`src/config.rs`** — `CsvSourceConfig` with path, headers, delimiter, quote
- **`src/stream.rs`** — `CsvSource`: csv::Reader in spawn_blocking; implements `faucet_core::Source`

### faucet-source-elasticsearch (`crates/source/elasticsearch/`)

- **`src/config.rs`** — `ElasticsearchSourceConfig`, `ElasticsearchAuth` (None, Basic, Bearer, ApiKey)
- **`src/stream.rs`** — `ElasticsearchSource`: scroll API pagination; implements `faucet_core::Source`

### faucet-sink-mysql (`crates/sink/mysql/`)

- **`src/config.rs`** — `MysqlSinkConfig`, `MysqlColumnMapping` (Json, AutoMap)
- **`src/sink.rs`** — `MysqlSink`: backtick-quoted inserts; implements `faucet_core::Sink`

### faucet-sink-sqlite (`crates/sink/sqlite/`)

- **`src/config.rs`** — `SqliteSinkConfig`, `SqliteColumnMapping` (Json, AutoMap)
- **`src/sink.rs`** — `SqliteSink`: PRAGMA table_info column discovery; implements `faucet_core::Sink`

### faucet-sink-s3 (`crates/sink/s3/`)

- **`src/config.rs`** — `S3SinkConfig` with bucket, prefix, max_records_per_file
- **`src/sink.rs`** — `S3Sink`: UUID-keyed JSONL uploads; implements `faucet_core::Sink`

### faucet-sink-mongodb (`crates/sink/mongodb/`)

- **`src/config.rs`** — `MongoSinkConfig` with connection_uri, database, collection
- **`src/sink.rs`** — `MongoSink`: insert_many with BSON conversion; implements `faucet_core::Sink`

### faucet-sink-redis (`crates/sink/redis/`)

- **`src/config.rs`** — `RedisSinkConfig`, `RedisSinkType` (List, Stream, KeyValue)
- **`src/sink.rs`** — `RedisSink`: RPUSH/XADD/SET via pipeline; implements `faucet_core::Sink`

### faucet-sink-csv (`crates/sink/csv/`)

- **`src/config.rs`** — `CsvSinkConfig` with path, delimiter, headers, append
- **`src/sink.rs`** — `CsvSink`: csv::Writer in spawn_blocking; implements `faucet_core::Sink`

### faucet-sink-elasticsearch (`crates/sink/elasticsearch/`)

- **`src/config.rs`** — `ElasticsearchSinkConfig`, `ElasticsearchSinkAuth`
- **`src/sink.rs`** — `ElasticsearchSink`: NDJSON bulk API; implements `faucet_core::Sink`

### faucet-sink-http (`crates/sink/http/`)

- **`src/config.rs`** — `HttpSinkConfig`, `HttpSinkAuth`, `HttpBatchMode` (Individual, Array)
- **`src/sink.rs`** — `HttpSink`: POST records individually or as array; implements `faucet_core::Sink`
- **`src/serde_helpers.rs`** — `http_method` module: serialize/deserialize `reqwest::Method` as string

### faucet-stream (umbrella, `faucet-stream/`)

- **`src/lib.rs`** — feature-gated re-exports of all connectors; `pub use faucet_core::*` always available; backwards-compatible flat re-exports for existing users

## Feature Flags (umbrella crate)

| Feature | Default | Description |
|---------|---------|-------------|
| `source-rest` | yes | REST API source connector |
| `source-graphql` | no | GraphQL API source connector |
| `source-xml` | no | XML/SOAP API source connector |
| `source-grpc` | no | gRPC source connector |
| `source-postgres` | no | PostgreSQL query source |
| `source-mysql` | no | MySQL query source |
| `source-sqlite` | no | SQLite query source |
| `source-s3` | no | AWS S3 file source |
| `source-mongodb` | no | MongoDB query source |
| `source-redis` | no | Redis source (streams, lists, keys) |
| `source-webhook` | no | Webhook HTTP receiver source |
| `source-csv` | no | CSV file source |
| `source-elasticsearch` | no | Elasticsearch search/scroll source |
| `sink-bigquery` | no | Google BigQuery sink connector |
| `sink-postgres` | no | PostgreSQL sink connector |
| `sink-jsonl` | no | JSON Lines file sink connector |
| `sink-snowflake` | no | Snowflake sink connector |
| `sink-mysql` | no | MySQL sink |
| `sink-sqlite` | no | SQLite sink |
| `sink-s3` | no | AWS S3 file sink |
| `sink-mongodb` | no | MongoDB insert sink |
| `sink-redis` | no | Redis sink (streams, lists, key-value) |
| `sink-csv` | no | CSV file sink |
| `sink-elasticsearch` | no | Elasticsearch bulk index sink |
| `sink-http` | no | HTTP POST sink |
| `source` | no | All source connectors |
| `sink` | no | All sink connectors |
| `full` | no | Every connector |
| `transform-flatten` | yes (via source-rest) | Flatten nested objects |
| `transform-rename-keys` | yes (via source-rest) | Regex key renaming |
| `transform-snake-case` | yes (via source-rest) | Snake_case normalisation |

## Pagination Styles

| Style | Stops When |
|-------|-----------|
| `None` | After first page |
| `Cursor` | Next-token JSONPath is null or absent |
| `PageNumber` | Response returns zero records |
| `Offset` | Offset reaches total (via JSONPath) or response has fewer records than the limit |
| `LinkHeader` | No `rel="next"` in the `Link` response header |
| `NextLinkInBody` | Next-page URL in response body is absent, null, or empty |

`max_pages` acts as a hard cap across all pagination styles. All styles include loop detection — if the same cursor/link is returned twice in a row, pagination stops.

## Coding Principles

### Self-Learning

When the user points out something fundamental about how code in this library should be written — module structure, naming, patterns, error handling, etc. — **add it to this file immediately** under the relevant section. The goal is that the user never has to repeat the same guidance twice.

### Module Boundaries

#### faucet-core
- `src/traits.rs` — trait definitions only (`Source`, `Sink` with `config_schema()`). No HTTP or connector-specific logic.
- `src/config.rs` — config loading helpers (`load_json`, `load_env`, `load_env_file`) and serde Duration modules. No connector-specific logic.
- `src/pipeline.rs` — source→sink orchestration only. Depends only on `Source` and `Sink` traits. No connector-specific logic.
- `src/transform.rs` — record transform compilation and application only. No HTTP logic. Built-in transforms are feature-gated.
- `src/replication.rs` — incremental replication filtering and bookmark computation only. No HTTP logic.
- `src/schema.rs` — JSON Schema inference from `Vec<Value>` only. No HTTP logic.

#### faucet-source-rest
- `src/auth/` — auth strategies only. No HTTP logic here.
- `src/pagination/` — pagination parameter generation and state advancement only. No HTTP logic here. `advance()` accepts the response body and headers so each strategy can inspect whatever it needs.
- `src/extract/` — record extraction from JSON values only.
- `src/retry/` — retry/backoff logic only. Generic over the return type.
- `src/stream.rs` — the only place where HTTP requests are executed. Orchestrates all other modules.

#### faucet-source-graphql
- `src/config.rs` — configuration types only. No HTTP logic.
- `src/stream.rs` — HTTP requests, pagination loop, JSONPath extraction.

#### faucet-source-xml
- `src/config.rs` — configuration types only. No HTTP logic.
- `src/convert.rs` — XML parsing and JSON conversion only. No HTTP logic.
- `src/stream.rs` — HTTP requests, pagination loop, XML-to-JSON pipeline.

#### faucet-source-grpc
- `src/config.rs` — configuration types only. No gRPC logic.
- `src/stream.rs` — gRPC channel setup, protobuf encoding/decoding, RPC execution.

#### faucet-sink-bigquery
- `src/config.rs` — configuration and credential types only.
- `src/sink.rs` — BigQuery API calls and Sink trait impl.

#### faucet-sink-postgres
- `src/config.rs` — configuration types only. No SQL logic.
- `src/sink.rs` — PostgreSQL inserts (JSONB or auto-mapped) and Sink trait impl.

#### faucet-sink-jsonl
- `src/config.rs` — configuration types only. No I/O logic.
- `src/sink.rs` — File I/O and Sink trait impl.

#### faucet-sink-snowflake
- `src/config.rs` — configuration and auth types only. No HTTP logic.
- `src/sink.rs` — Snowflake SQL REST API calls, JWT generation, and Sink trait impl.

#### faucet-source-postgres / faucet-source-mysql / faucet-source-sqlite
- `src/config.rs` — configuration types only. No SQL logic.
- `src/stream.rs` — connection pool, query execution, row-to-JSON conversion, Source trait impl.


#### faucet-source-s3 / faucet-sink-s3
- `src/config.rs` — configuration types only. No AWS logic.
- `src/stream.rs` / `src/sink.rs` — S3 client creation, object listing/reading/writing.

#### faucet-source-mongodb / faucet-sink-mongodb
- `src/config.rs` — configuration types only. No MongoDB logic.
- `src/stream.rs` / `src/sink.rs` — MongoDB client, BSON conversion, find/insert operations.

#### faucet-source-redis / faucet-sink-redis
- `src/config.rs` — configuration types only. No Redis logic.
- `src/stream.rs` / `src/sink.rs` — Redis connection, command execution per source/sink type.

#### faucet-source-webhook
- `src/config.rs` — configuration types only. No HTTP server logic.
- `src/stream.rs` — axum server lifecycle, payload collection, timeout handling.

#### faucet-source-csv / faucet-sink-csv
- `src/config.rs` — configuration types only. No I/O logic.
- `src/stream.rs` / `src/sink.rs` — csv crate Reader/Writer in spawn_blocking.

#### faucet-source-elasticsearch / faucet-sink-elasticsearch
- `src/config.rs` — configuration types only. No HTTP logic.
- `src/stream.rs` / `src/sink.rs` — scroll/bulk API calls, auth application.

#### faucet-sink-mysql / faucet-sink-sqlite
- `src/config.rs` — configuration types only. No SQL logic.
- `src/sink.rs` — connection pool, column discovery, auto-mapped or JSON inserts.

#### faucet-sink-http
- `src/config.rs` — configuration types only. No HTTP logic.
- `src/sink.rs` — HTTP POST with auth, individual or batched mode.

### Config Loading

All connector config structs derive `Serialize` + `Deserialize` + `JsonSchema`, so they can be loaded from JSON files, environment variables, or `.env` files using the helpers in `faucet_core::config`:

- `load_json::<T>(path)` — deserialize from a JSON file
- `load_env::<T>(prefix)` — deserialize from environment variables (e.g. prefix `"BQ"` reads `BQ_PROJECT_ID`, `BQ_DATASET_ID`, etc.)
- `load_env_file::<T>(env_path, prefix)` — load a `.env` file first, then read env vars

Duration fields use `#[serde(with = "faucet_core::config::duration_secs")]` to serialize as `u64` seconds. Optional Durations use `duration_secs_option`.

Fields that can't be serialized (closures, `HeaderMap`, `reqwest::Method`) use:
- `#[serde(skip)]` / `#[serde(skip, default)]` for non-serializable fields
- `#[serde(with = "crate::serde_helpers::http_method")]` + `#[schemars(with = "String")]` for `reqwest::Method`

### Config Schema Introspection

Every `Source` and `Sink` has a `config_schema(&self) -> Value` method that returns a JSON Schema describing the config the connector accepts. This is auto-generated via `schemars::schema_for!()` from the config struct.

Usage:
```rust
let source = RestStream::new(config).await?;
let schema = source.config_schema(); // JSON Schema as serde_json::Value
println!("{}", serde_json::to_string_pretty(&schema)?);
```

When adding a new connector, always:
1. Derive `JsonSchema` on the config struct and all its sub-types (auth enums, etc.)
2. Add `#[schemars(with = "...")]` for fields with custom serde (Duration, Method)
3. Override `config_schema()` in the `Source`/`Sink` impl

### Error Handling

All errors must map to a `FaucetError` variant. Never use `.unwrap()` or `.expect()` on values that can fail at runtime. Use `.expect()` only for programmer errors (invariants validated at construction time). All error types use `thiserror` derive macros.

## Testing

Every non-trivial piece of logic must have tests. Untested public API surface is a liability.

### Tools
- **`wiremock`** — integration tests using a real in-process HTTP mock server. Use for end-to-end pagination and auth scenarios in `tests/`.
- **`#[cfg(test)]`** modules — unit tests inside each source file for logic that doesn't need HTTP (JSONPath extraction, pagination state transitions, auth header generation, Link header parsing, trait impls).

### Rules
- Unit tests go in `#[cfg(test)]` modules at the bottom of each source file.
- Integration tests go in `crates/source/rest/tests/` and use `wiremock`.
- When adding a new feature (auth method, pagination style, etc.), add unit tests in the source file and integration tests in `tests/`.
- Tests must assert the specific outcome, not just "no panic".
- **New code** — always write tests for new functions or behaviors. This is non-negotiable.
- **Modified code** — do NOT automatically rewrite or update existing tests. If a code change breaks existing tests, that is signal — investigate whether the behaviour change is intentional before touching the test. Silently updating tests to match new behaviour defeats the purpose of having tests and can hide regressions.

### Running tests

```bash
# All workspace tests
cargo test --workspace --all-features

# Single crate (examples)
cargo test -p faucet-core
cargo test -p faucet-source-rest

cargo test -p faucet-sink-mongodb
cargo test -p faucet-stream --features full
```

## Dependency Policy

Always use the **highest available stable version** for every crate, the Rust toolchain, and the Rust edition.

- Rust toolchain is pinned in `rust-toolchain.toml`. Update `channel` to the latest stable when upgrading.
- Cargo.toml `edition` should always be the latest stable Rust edition. As of 2026, that is `"2024"`. Update this when a newer edition is released.
- Before adding a new crate, check its latest stable release on crates.io and use that version.
- When upgrading existing crates, check with `cargo search <crate>` and update to the latest stable.
- Never use alpha, beta, or rc versions unless there is no stable alternative.
- Shared dependencies should be declared in the workspace `[workspace.dependencies]` table and referenced with `.workspace = true` in member crates.

### Key Workspace Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `serde` | 1 (derive) | Serialize/Deserialize for all config structs |
| `serde_json` | 1 | JSON Value type, used everywhere |
| `schemars` | 1.2 | JSON Schema generation from config structs via `JsonSchema` derive |
| `async-trait` | 0.1 | Async trait support for Source/Sink |
| `thiserror` | 2 | Derive macros for FaucetError |
| `reqwest` | 0.13 | HTTP client (REST, GraphQL, XML, HTTP sink, Elasticsearch, Snowflake) |
| `tokio` | 1 | Async runtime |
| `tracing` | 0.1 | Structured logging |
| `sqlx` | 0.8 | Database pool/queries (PostgreSQL, MySQL, SQLite) |
| `dotenvy` | 0.15 | `.env` file loading (in faucet-core) |
| `envy` | 0.4 | Env var → struct deserialization (in faucet-core) |

## Publishing

Crates must be published in dependency order with delays for crates.io index propagation:

1. `faucet-core`
2. All sources + sinks (after 30s): `faucet-source-rest`, `faucet-source-graphql`, `faucet-source-xml`, `faucet-source-grpc`, `faucet-source-postgres`, `faucet-source-mysql`, `faucet-source-sqlite`, `faucet-source-s3`, `faucet-source-mongodb`, `faucet-source-redis`, `faucet-source-webhook`, `faucet-source-csv`, `faucet-source-elasticsearch`, `faucet-sink-bigquery`, `faucet-sink-postgres`, `faucet-sink-jsonl`, `faucet-sink-snowflake`, `faucet-sink-mysql`, `faucet-sink-sqlite`, `faucet-sink-s3`, `faucet-sink-mongodb`, `faucet-sink-redis`, `faucet-sink-csv`, `faucet-sink-elasticsearch`, `faucet-sink-http`
3. `faucet-stream` (after 30s)

The `.github/workflows/publish.yml` handles this automatically on version tags (`v*.*.*`).

## Project Structure Sync

**When adding, removing, or moving files or directories, update the Project Structure section in README.md to reflect the change.**

## GitHub Auth Switching

This repo is pushed under the `PawanSikawat` GitHub account, but the default CLI auth is `pawan-dt`. Before any `git push`, `git pull`, or `gh` commands that interact with the remote:

1. Switch to the correct account: `gh auth switch --user PawanSikawat`
2. Perform the git/gh operation
3. Switch back to the default account: `gh auth switch --user pawan-dt`

Always revert back to `pawan-dt` once the operation is done.
