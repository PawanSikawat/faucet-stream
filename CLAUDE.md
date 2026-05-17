# CLAUDE.md

## Library Purpose

`faucet-stream` is a modular, config-driven data pipeline toolkit for Rust with pluggable **source** and **sink** connectors, plus a `faucet` CLI binary that runs pipelines declaratively from YAML/JSON — no Rust code required.

- **Sources** fetch data from external systems (e.g. REST APIs).
- **Sinks** write data to external systems (e.g. BigQuery).
- **`faucet` CLI** (`cli/`) wires source → transforms → sink together based on a config file.

Design goal: callers configure a source or sink once, call `fetch_all()` or `write_batch()`, and get/write all records — no manual pagination loop, no auth boilerplate. Rust users embed the library directly; everyone else runs the `faucet` binary.

This workspace produces both library crates (`faucet-core` + every connector and state backend) and the `faucet` CLI binary. There is no database, no migrations, and no server.

## Workspace Structure

The project is a Cargo workspace with 34 crates (33 libraries + the `faucet-cli` binary):

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
| `faucet-sink-stdout` | `crates/sink/stdout/` | Stdout/stderr sink — JSON Lines, pretty JSON, or TSV |
| `faucet-kafka-common` | `crates/kafka-common/` | Shared types for Kafka source/sink — auth, value formats, Schema Registry client |
| `faucet-source-kafka` | `crates/source/kafka/` | Apache Kafka consumer — subscribes to topics, drains with idle/max-messages termination |
| `faucet-sink-kafka` | `crates/sink/kafka/` | Apache Kafka producer — FuturesUnordered batched sends, QueueFull retry, multi-topic routing |
| `faucet-state-redis` | `crates/state/redis/` | Redis-backed `StateStore` for replication bookmarks |
| `faucet-state-postgres` | `crates/state/postgres/` | PostgreSQL-backed `StateStore` for replication bookmarks |
| `faucet-stream` | `faucet-stream/` | Umbrella crate — feature-gated re-exports of all connectors and state backends |
| `faucet-cli` | `cli/` | `faucet` binary — YAML/JSON config-driven pipeline runner (`run`, `validate`, `schema`, `list`, `preview`, `init`) |

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
             <──  faucet-sink-stdout
             <──  faucet-kafka-common
             <──  faucet-source-kafka  (depends on faucet-kafka-common)
             <──  faucet-sink-kafka  (depends on faucet-kafka-common)
             <──  faucet-state-redis
             <──  faucet-state-postgres
             <──  faucet-stream (umbrella, all optional)
             <──  faucet-cli (binary — depends on every connector + state crate via optional features)
```

## Capturing Feature Ideas as GitHub Issues

**Whenever a new feature, enhancement, or bug surfaces in conversation — whether the user asks for it directly, you propose it, or it emerges as a side observation while working on something else — file it as a GitHub issue immediately, before continuing the current task.** This is how future sessions inherit the full context without needing to re-derive it from chat history.

Rules:

- **File the issue right away.** Don't wait until the end of the session, and don't batch multiple ideas into one issue unless they are genuinely the same change. One issue per discrete piece of work.
- **Be exhaustively descriptive.** Each issue must stand alone — a fresh Claude session opening it cold should have everything needed to scope and implement the work. Include:
  - **Summary** — one-paragraph statement of the problem and the proposed change.
  - **Motivation** — why this matters (performance, correctness, ergonomics, third-party connector friendliness, etc.). Tie it back to the [Primary Goal](#primary-goal) or [Third-Party Connector Friendliness](#third-party-connector-friendliness) sections where relevant.
  - **Proposed design** — concrete API shape, config field names, trait method signatures, file paths to touch. Show example usage in Rust where useful.
  - **Affected crates / files** — explicit list of crates and modules that need changes, mirroring the [Architecture](#architecture) layout in this file.
  - **Edge cases & failure modes** — what could go wrong, what error variants apply, what tests are needed.
  - **Acceptance criteria** — bullet list of what "done" looks like (tests pass, docs updated, README updated, CI matrix updated if a new feature flag is added, etc.).
  - **Out of scope** — anything explicitly deferred so the scope doesn't drift.
  - **References** — links to related issues, PRs, upstream docs, or specific lines in this CLAUDE.md.
- **Apply two labels to every issue:**
  - **Type:** exactly one of `feature` (brand-new capability — a new connector, a new transform), `enhancement` (improvement to existing capability — new auth method on REST, new pagination style), or `bug` (incorrect behavior in existing code).
  - **Tier:** exactly one of `tier-1` (critical — correctness bug, broken API, blocks a core use case, regression), `tier-2` (important — significant improvement, frequently-requested connector, perf win on a hot path), or `tier-3` (nice-to-have — niche connector, minor ergonomic polish, speculative idea).
- **If a required label doesn't exist, create it first** with `gh label create <name> --description "<desc>" --color <hex>` before filing the issue. The repo uses GitHub's default `bug` and `enhancement` labels; `feature`, `tier-1`, `tier-2`, and `tier-3` may need to be created on first use.
- **Confirm the GitHub remote auth flow** before running `gh` commands — see the [GitHub Auth Switching](#github-auth-switching) section at the bottom of this file.
- **Don't file duplicates.** Search existing open issues (`gh issue list --search "<keywords>" --state open`) before creating a new one. If a related issue exists, comment on it instead.
- **Mention the issue number in any follow-up PR** so the work links back automatically.
- **Cross-link to the roadmap epic.** The repo maintains a single tracking epic — currently **[#38 Roadmap: faucet-stream connector & runtime coverage](https://github.com/PawanSikawat/faucet-stream/issues/38)** (labelled `epic`) — that organizes every feature/enhancement by tier and by area. When you file a new issue in scope of that roadmap, add a row to the relevant table in the epic (either by editing the epic body or by leaving a comment that links the new issue) so the epic stays a complete index. If the open `epic`-labelled issue has changed, follow the most recent one — find it with `gh issue list --label epic --state open`.

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

## Cleaning Up Stale Artifacts After PR Merge

**When the user asks for a cleanup after a PR merge, remove stale artifact files and any files that are no longer needed.** These are typically gitignored build outputs or generated trees that accumulate in the working tree and waste space — for example, rustdoc HTML in `doc/`, stray `*.rlib` files at the repo root, or other regenerable build products. They are not useful and should be deleted.

Rules:

- **Only act on the user's cleanup request.** Do not proactively delete files between tasks. Wait until the user explicitly asks to clean up (typically after a PR merge).
- **If a file is needed, don't touch it.** "Needed" means: tracked by git, referenced by build/config, contains in-progress work (e.g. recently modified design docs under `docs/superpowers/`), or is otherwise actively used. When unsure, leave it.
- **If a file is not needed, delete it.** Stale rustdoc output, stray compile artifacts (`*.rlib`, `*.dylib`, `*.so`), old log files, leftover temp files — all safe to remove if regenerable and gitignored.
- **Never delete `docs/` or anything under it.** The user keeps design docs and superpowers plans/specs there as reference, even when gitignored. Distinguish carefully between `doc/` (rustdoc output, deletable) and `docs/` (reference material, keep).
- **Report what was deleted and what was kept**, so the user can correct course if you removed something they wanted.

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

# Run all tests (no external services required; Kafka integration tests require Docker)
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
cargo publish --dry-run -p faucet-cli

# Build / install the CLI binary
cargo build -p faucet-cli                     # debug build → target/debug/faucet
cargo install --path cli                      # release install → ~/.cargo/bin/faucet
cargo install --path cli --no-default-features --features "source-rest,sink-jsonl,sink-stdout,transforms"  # slim build

# Drive the CLI on a config file
./target/debug/faucet list
./target/debug/faucet schema source rest
./target/debug/faucet validate cli/examples/csv_to_jsonl.yaml
./target/debug/faucet run cli/examples/csv_to_jsonl.yaml
```

## Architecture

### faucet-core (`crates/core/`)

- **`src/lib.rs`** — crate root; re-exports `FaucetError`, `Source`, `Sink`, `Pipeline`, `PipelineResult`, `run_stream`, `RecordTransform`, `ReplicationMethod`, `SourceDAG`, `DagResult`, `DagNodeResult`, `DagNodeError`, `DagNode`, `StateStore`, `MemoryStateStore`, `FileStateStore`. Also re-exports third-party crates for connector authors: `async_trait`, `serde_json` (+ `Value`, `json!`), `schemars` (+ `JsonSchema`, `schema_for!`)
- **`src/error.rs`** — `FaucetError` enum: `Http`, `HttpStatus`, `Json`, `JsonPath`, `Auth`, `RateLimited`, `Url`, `Transform`, `Config`, `Source`, `Sink`, `State`, `Custom(Box<dyn Error>)`
- **`src/config.rs`** — Config loading utilities: `load_json()` (from JSON file), `load_env()` (from env vars with prefix), `load_env_file()` (load `.env` then env vars). Also `duration_secs` and `duration_secs_option` serde helper modules for `Duration` fields
- **`src/util.rs`** — Shared utilities: `quote_ident()` (SQL injection prevention), `extract_records()` (JSONPath extraction), `check_http_response()` (HTTP status error handling), `substitute_context()` (placeholder substitution for URLs/paths — NOT safe for SQL or JSON), `substitute_context_bind_params()` (SQL-safe substitution using bind parameter markers), `substitute_context_json()` (JSON-safe substitution with proper escaping), `extract_context()` (JSONPath-based context extraction from parent records)
- **`src/traits.rs`** — `Source` and `Sink` async traits. `Source` uses `fetch_with_context()` as primary method (receives parent context); `fetch_all()` is a convenience wrapper. Both include `config_schema(&self) -> Value`. `Source` also exposes `state_key(&self) -> Option<String>` and `apply_start_bookmark(&self, bookmark)` for opting into resumable runs via a `StateStore` (both default no-ops, backwards-compatible).
- **`src/pipeline.rs`** — `Pipeline` struct (batch source→sink), `run_stream()` (streaming source→sink), `PipelineResult`. `Pipeline::with_state_store(Arc<dyn StateStore>)` wires in durable bookmarks — read before fetch, persisted only after sink confirms the batch.
- **`src/state.rs`** — `StateStore` async trait (`get` / `put` / `delete` over `serde_json::Value`), plus two built-in implementations: `MemoryStateStore` (in-process, for tests) and `FileStateStore` (one JSON file per key, written via atomic rename). Keys are validated by `validate_state_key`. Heavier backends (Redis, Postgres) live in their own crates to keep `faucet-core` dependency-light.
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

### faucet-sink-stdout (`crates/sink/stdout/`)

- **`src/config.rs`** — `StdoutSinkConfig`, `StdStream` (Stdout, Stderr), `StdoutFormat` (JsonLines, PrettyJson, Tsv)
- **`src/sink.rs`** — `StdoutSink`: writes encoded records to the chosen standard stream behind a `Mutex<Box<dyn AsyncWrite + Unpin + Send>>`. Treats `BrokenPipe` as clean termination. Honors `max_records` and `flush_per_record`. `StdoutSink::with_writer(...)` accepts a custom writer for tests and redirected output.

### faucet-kafka-common (`crates/kafka-common/`)

- **`src/lib.rs`** — crate root; re-exports all shared Kafka types (`KafkaAuth`, `KafkaValueFormat`, `SchemaRegistryConfig`, `KafkaCompression`, `KafkaTlsConfig`)
- **`src/auth.rs`** — `KafkaAuth` enum: `None`, `SaslPlain`, `SaslScram256`, `SaslScram512`, `Ssl`; maps to `rdkafka` `ClientConfig` entries
- **`src/format.rs`** — `KafkaValueFormat` enum: `Json`, `Avro`, `Protobuf`, `JsonSchema`, `RawBytes`; schema-registry-backed formats are gated on `kafka-schema-registry`
- **`src/registry.rs`** — `SchemaRegistryConfig` (url, credentials) + `SchemaRegistryClient`: fetches and caches schemas by subject/version; used by Avro/Protobuf/JsonSchema decoders
- **`src/compression.rs`** — `KafkaCompression` enum: `None`, `Gzip`, `Snappy`, `Lz4`, `Zstd`; serializes to the `compression.type` producer config string

### faucet-source-kafka (`crates/source/kafka/`)

- **`src/lib.rs`** — crate root; re-exports `Source`, `FaucetError`, and `Kafka*` config types from `faucet-kafka-common`
- **`src/config.rs`** — `KafkaSourceConfig` (brokers, topics, group_id, auth, formats, termination), `OffsetReset` enum (`Earliest`, `Latest`), `TerminationPolicy` (idle timeout + optional max-messages cap), validation
- **`src/decode.rs`** — value/key decoder dispatch on `KafkaValueFormat`; JSON decoded directly, Avro/Protobuf/JsonSchema require Schema Registry, raw bytes wrapped as base64 strings
- **`src/state.rs`** — `Bookmark` type (partition → offset map); `state_key()` generator derives a stable key from group_id + topics; `apply_start_bookmark()` seeks each partition to its stored offset on first poll
- **`src/stream.rs`** — `KafkaSource`: builds a `StreamConsumer` from config, drives `tokio::select!` over `recv`/`idle_timeout`/`ctrl_c`, sets `enable.auto.commit=false`, seeks to bookmark offsets before first message, collects into a `Vec<Value>` and implements `faucet_core::Source`

### faucet-sink-kafka (`crates/sink/kafka/`)

- **`src/lib.rs`** — crate root; re-exports `Sink`, `FaucetError`, and `Kafka*` config types from `faucet-kafka-common`
- **`src/config.rs`** — `KafkaSinkConfig` (brokers, default_topic, topic_field, auth, value_format, key_field, compression, acks, linger_ms, batch_size), validation
- **`src/encode.rs`** — value encoder dispatch on `KafkaValueFormat`; JSON serialized via `serde_json`, schema-registry formats gated on `kafka-schema-registry`
- **`src/sink.rs`** — `KafkaSink`: builds an `rdkafka::FutureProducer` in `new()`, sends records via `FuturesUnordered` for maximum parallelism, retries on `QueueFull` with exponential backoff, routes each record to the correct topic via `topic_field` override or `default_topic`, implements `faucet_core::Sink`

### faucet-state-redis (`crates/state/redis/`)

- **`src/store.rs`** — `RedisStateStore`: Redis-backed `StateStore`. Uses `redis::aio::MultiplexedConnection`, namespaces keys as `{namespace}:{key}`, exposes `connect(url, namespace)`, `from_connection(conn, namespace)`, and `ensure_table` is not needed (Redis is schemaless). Helper functions `build_redis_key`, `validate_namespace` are unit-tested.

### faucet-state-postgres (`crates/state/postgres/`)

- **`src/store.rs`** — `PostgresStateStore`: PostgreSQL-backed `StateStore`. Single table `faucet_state(key TEXT PRIMARY KEY, value JSONB, updated_at TIMESTAMPTZ)`. `connect`, `connect_with(url, max_connections, table)`, `from_pool(pool, table)`, and `ensure_table()` for schema bootstrap. Upsert via `ON CONFLICT (key) DO UPDATE`. SQL builders (`create_table_sql`, `select_sql`, `upsert_sql`, `delete_sql`) are free functions for unit testing.

### faucet-stream (umbrella, `faucet-stream/`)

- **`src/lib.rs`** — feature-gated re-exports of all connectors; `pub use faucet_core::*` always available; backwards-compatible flat re-exports for existing users

### faucet-cli (binary, `cli/`)

- **`src/main.rs`** — `tokio::main` entry point; installs `tracing-subscriber` against `--log-level` / `FAUCET_LOG`, then dispatches to `commands::*::run`. Reports `CliError` to stderr and exits 1 on failure.
- **`src/lib.rs`** — library half of the crate; re-exports `cli`, `commands`, `config`, `error`, `interpolate`, `registry`, `state`, `transforms` so tests (and downstream tooling) can drive the same code paths the binary does.
- **`src/cli.rs`** — `clap` argument types: `Cli`, `Command::{Run, Validate, Schema, List, Preview, Init}`, and per-subcommand arg structs (`RunArgs`, `ValidateArgs`, `SchemaArgs`, `PreviewArgs`, `InitArgs`).
- **`src/config.rs`** — `PipelineConfig` (top-level YAML/JSON schema) with `ConnectorSpec { kind, config }`, `TransformSpec`, `StateStoreSpec`. `from_path()` reads + interpolates + dispatches to the YAML or JSON parser based on the file extension. Rejects `version != 1`.
- **`src/interpolate.rs`** — substitutes `${env:VAR}`, `${file:PATH}`, `${secret:VAR}` (today an alias for `env`) in raw config text before parsing. `$${` is the escape for a literal `${`. Unclosed directives are left untouched.
- **`src/registry.rs`** — feature-gated `build_source` / `build_sink` async dispatchers, plus `source_schema` / `sink_schema` (via `schemars::schema_for!`) and `source_descriptions` / `sink_descriptions` for `faucet list`.
- **`src/state.rs`** — `build_state_store(&StateStoreSpec)` returns `Arc<dyn StateStore>`. Built-in `memory` and `file` backends are always available; `redis` / `postgres` are feature-gated.
- **`src/transforms.rs`** — `compile_transforms(&[TransformSpec])` turns YAML transform blocks into `RecordTransform` values. Only the built-in `flatten`, `rename_keys`, `snake_case` transforms are exposed via config; custom-closure transforms remain Rust-only.
- **`src/commands/run.rs`** — orchestrates a single `Pipeline` run, wrapping the source with `TransformingSource` when transforms are configured, and the sink with `LimitedSink` for `--limit` or `CountingSink` for `--dry-run`. Wires in a state store from `cfg.state` or `--state-path`.
- **`src/commands/validate.rs`** — parses the config and verifies the source/sink kinds, transform names, and state-store kind are compiled into the binary. Prints a one-line summary on success.
- **`src/commands/schema.rs`** — prints `source_schema()` / `sink_schema()` for the requested connector.
- **`src/commands/list.rs`** — two-column listing of every compiled-in source, sink, transform, and state-store backend.
- **`src/commands/preview.rs`** — runs only the source side, applies transforms, then writes the first `--limit` records to stdout via `faucet-sink-stdout`. Gated by the `sink-stdout` feature.
- **`src/commands/init.rs`** — scaffolds a starter `pipeline.yaml` (REST → JSONL with a file state store). Refuses to overwrite unless `--force`.

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
| `source-kafka` | no | Apache Kafka consumer source |
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
| `sink-stdout` | no | Stdout/stderr sink (JSON Lines, pretty JSON, TSV) |
| `sink-kafka` | no | Apache Kafka producer sink |
| `kafka-schema-registry` | no | Confluent Schema Registry (Avro / Protobuf / JSON Schema) support for the Kafka pair |
| `state-redis` | no | Redis-backed `StateStore` backend |
| `state-postgres` | no | PostgreSQL-backed `StateStore` backend |
| `source` | no | All source connectors |
| `sink` | no | All sink connectors |
| `state` | no | All state-store backends (file lives in `faucet-core`) |
| `full` | no | Every connector and state backend |
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
- `src/state.rs` — `StateStore` trait + in-memory and file-backed implementations only. No external service code (Redis / Postgres backends live in their own crates).

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

#### faucet-kafka-common
- `src/auth.rs` — auth mapping types only. No rdkafka client creation.
- `src/format.rs` — value format enum only. No encode/decode logic.
- `src/registry.rs` — Schema Registry HTTP client and schema cache only.
- `src/compression.rs` — compression enum + string serialization only.

#### faucet-source-kafka / faucet-sink-kafka
- `src/config.rs` — configuration types only. No rdkafka logic.
- `src/decode.rs` / `src/encode.rs` — format-specific encoding/decoding only. No consumer/producer logic.
- `src/state.rs` (source only) — bookmark type and state key derivation only.
- `src/stream.rs` / `src/sink.rs` — rdkafka consumer/producer creation, message loop, Source/Sink trait impl.

### Source/Sink Pair Config Sharing

When a connector ships both a `faucet-source-<name>` and a `faucet-sink-<name>` crate
for the same external system, shared configuration types (auth, value formats,
compression, TLS, etc.) live in a dedicated `faucet-<name>-common` crate. Both
the source and sink crates depend on the common crate and re-export the shared
types so end-user imports do not change. See `faucet-kafka-common` for the
reference implementation.

Existing pairs (`postgres`, `mysql`, `sqlite`, `redis`, `mongodb`, `s3`, `csv`,
`elasticsearch`) predate this convention and currently duplicate their tiny
shared config surface; backfilling them is tracked separately (#43). New pairs
must follow the convention from the start.

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
2. All sources + sinks (after 30s): `faucet-source-rest`, `faucet-source-graphql`, `faucet-source-xml`, `faucet-source-grpc`, `faucet-source-postgres`, `faucet-source-mysql`, `faucet-source-sqlite`, `faucet-source-s3`, `faucet-source-mongodb`, `faucet-source-redis`, `faucet-source-webhook`, `faucet-source-csv`, `faucet-source-elasticsearch`, `faucet-kafka-common`, `faucet-source-kafka`, `faucet-sink-bigquery`, `faucet-sink-postgres`, `faucet-sink-jsonl`, `faucet-sink-snowflake`, `faucet-sink-mysql`, `faucet-sink-sqlite`, `faucet-sink-s3`, `faucet-sink-mongodb`, `faucet-sink-redis`, `faucet-sink-csv`, `faucet-sink-elasticsearch`, `faucet-sink-http`, `faucet-sink-kafka`
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

## Merging Pull Requests

**Whenever the user asks to merge a PR, first verify that every CI check on that PR has passed before merging.** Never merge a PR with failing, pending, or skipped required checks — the failing job almost always represents a real defect that would land on `main` if merged.

The check:

```bash
gh pr checks <PR-number>
```

- If every line says `pass`, proceed with the merge.
- If any line says `fail` or `pending`, **stop and report the failing jobs to the user before merging.** Pull the job logs (`gh run view --log-failed --job <job-id>`) and surface the root cause so the user can decide whether to fix-then-merge or merge-anyway (rare — only if the failure is in an unrelated job the user explicitly tells you to ignore).
- If checks are still running, wait for them to finish before merging rather than racing.

This rule applies regardless of how the merge was requested — "merge it", "ship it", "land the PR", or anything similar. The verification is non-negotiable.
