# CLAUDE.md

## Library Purpose

`faucet-stream` is a modular, config-driven data pipeline toolkit for Rust with pluggable **source** and **sink** connectors.

- **Sources** fetch data from external systems (e.g. REST APIs).
- **Sinks** write data to external systems (e.g. BigQuery).

Design goal: callers configure a source or sink once, call `fetch_all()` or `write_batch()`, and get/write all records — no manual pagination loop, no auth boilerplate.

This is a library workspace — there is no binary, no database, no migrations, no server.

## Workspace Structure

The project is a Cargo workspace with ten crates:

| Crate | Path | Description |
|-------|------|-------------|
| `faucet-core` | `crates/core/` | Shared types, traits (`Source`, `Sink`), pipeline orchestration, transforms, error types |
| `faucet-source-rest` | `crates/source/rest/` | REST API source — auth, pagination, extraction, schema inference |
| `faucet-source-graphql` | `crates/source/graphql/` | GraphQL API source — cursor pagination, variable injection |
| `faucet-source-xml` | `crates/source/xml/` | XML/SOAP API source — XML-to-JSON conversion, dot-path extraction |
| `faucet-source-grpc` | `crates/source/grpc/` | gRPC source — dynamic protobuf via `prost-reflect` |
| `faucet-sink-bigquery` | `crates/sink/bigquery/` | Google BigQuery streaming insert sink |
| `faucet-sink-postgres` | `crates/sink/postgres/` | PostgreSQL sink — JSONB or auto-mapped columns |
| `faucet-sink-jsonl` | `crates/sink/jsonl/` | JSON Lines file sink |
| `faucet-sink-snowflake` | `crates/sink/snowflake/` | Snowflake SQL REST API sink |
| `faucet-stream` | `faucet-stream/` | Umbrella crate — feature-gated re-exports of all connectors |

### Crate Dependency Graph

```
faucet-core  <──  faucet-source-rest
             <──  faucet-source-graphql
             <──  faucet-source-xml
             <──  faucet-source-grpc
             <──  faucet-sink-bigquery
             <──  faucet-sink-postgres
             <──  faucet-sink-jsonl
             <──  faucet-sink-snowflake
             <──  faucet-stream (umbrella)
                    ├── optional dep: faucet-source-rest
                    ├── optional dep: faucet-source-graphql
                    ├── optional dep: faucet-source-xml
                    ├── optional dep: faucet-source-grpc
                    ├── optional dep: faucet-sink-bigquery
                    ├── optional dep: faucet-sink-postgres
                    ├── optional dep: faucet-sink-jsonl
                    └── optional dep: faucet-sink-snowflake
```

## Code Quality Standard

Every code change must be production-library quality:

- Correctness first — subtle bugs in pagination state or retry logic silently corrupt downstream consumers
- No shortcuts on error handling — every failure path must be explicit and typed in `FaucetError`
- Think about failure modes: what if the API returns an empty page mid-stream? If the Link header is malformed? If the JSONPath matches nothing?
- No hardcoded credentials, tokens, or service URLs — ever
- Public API must be self-documenting and stable

When reviewing or modifying any part of the codebase, **proactively fix** any issue that falls below this standard. If a change is large enough to warrant a conversation first, call it out clearly and explain the fix, but default to fixing rather than flagging.

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

- **`src/lib.rs`** — crate root; re-exports `FaucetError`, `Source`, `Sink`, `Pipeline`, `PipelineResult`, `run_stream`, `RecordTransform`, `ReplicationMethod`
- **`src/error.rs`** — `FaucetError` enum: `Http`, `HttpStatus`, `Json`, `JsonPath`, `Auth`, `RateLimited`, `Url`, `Transform`, `Config`, `Sink`
- **`src/util.rs`** — Shared utilities: `quote_ident()` (SQL injection prevention), `extract_records()` (JSONPath extraction), `check_http_response()` (HTTP status error handling)
- **`src/traits.rs`** — `Source` and `Sink` async traits (the core abstractions)
- **`src/pipeline.rs`** — `Pipeline` struct (batch source→sink), `run_stream()` (streaming source→sink), `PipelineResult`
- **`src/transform.rs`** — `RecordTransform` enum + `CompiledTransform`: flatten, rename keys (regex), snake_case, custom closures; feature-gated built-ins
- **`src/replication.rs`** — `ReplicationMethod` enum, `filter_incremental()`, `max_replication_value()` for bookmark-based incremental replication
- **`src/schema.rs`** — `infer_schema()`: JSON Schema inference from record samples with type merging and nullable detection

### faucet-source-rest (`crates/source/rest/`)

- **`src/lib.rs`** — crate root; re-exports core types + REST-specific types
- **`src/config.rs`** — `RestStreamConfig` struct with fluent builder (base_url, path, method, auth, headers, query_params, body, pagination, records_path, max_pages, request_delay, timeout, max_retries, retry_backoff, replication, transforms, partitions, schema, tolerated_http_errors)
- **`src/stream.rs`** — `RestStream`: the main executor; `new(config)`, `fetch_all()`, `fetch_all_as::<T>()`, `fetch_all_incremental()`, `infer_schema()`, `stream_pages()`; implements `faucet_core::Source`
- **`src/auth/`** — `Auth` enum + per-strategy impls: `bearer.rs`, `basic.rs`, `api_key.rs`, `custom.rs`, `oauth2.rs`, `token_endpoint.rs`
- **`src/pagination/`** — `PaginationStyle` enum + `PaginationState` + per-strategy impls: `cursor.rs`, `page.rs`, `offset.rs`, `link_header.rs`, `next_link_body.rs`
- **`src/extract/`** — `extract_records()`: JSONPath extraction from response bodies
- **`src/retry/`** — `execute_with_retry()`: generic exponential backoff retry executor

### faucet-source-graphql (`crates/source/graphql/`)

- **`src/lib.rs`** — crate root; re-exports config + stream types
- **`src/config.rs`** — `GraphqlStreamConfig`, `GraphqlAuth`, `GraphqlPagination` (Relay cursor)
- **`src/stream.rs`** — `GraphqlStream`: cursor pagination loop, JSONPath extraction, GraphQL error handling; implements `faucet_core::Source`

### faucet-source-xml (`crates/source/xml/`)

- **`src/lib.rs`** — crate root; re-exports config + stream types
- **`src/config.rs`** — `XmlStreamConfig`, `XmlAuth`, `XmlPagination` (page-number, offset)
- **`src/convert.rs`** — `xml_to_json()`: event-based XML-to-JSON conversion; `extract_at_path()`: dot-path record extraction
- **`src/stream.rs`** — `XmlStream`: pagination, XML-to-JSON conversion pipeline; implements `faucet_core::Source`

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

### faucet-stream (umbrella, `faucet-stream/`)

- **`src/lib.rs`** — feature-gated re-exports of all connectors; `pub use faucet_core::*` always available; backwards-compatible flat re-exports for existing users

## Feature Flags (umbrella crate)

| Feature | Default | Description |
|---------|---------|-------------|
| `source-rest` | yes | REST API source connector |
| `source-graphql` | no | GraphQL API source connector |
| `source-xml` | no | XML/SOAP API source connector |
| `source-grpc` | no | gRPC source connector |
| `sink-bigquery` | no | Google BigQuery sink connector |
| `sink-postgres` | no | PostgreSQL sink connector |
| `sink-jsonl` | no | JSON Lines file sink connector |
| `sink-snowflake` | no | Snowflake sink connector |
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
- `src/traits.rs` — trait definitions only. No HTTP or connector-specific logic.
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

# Single crate
cargo test -p faucet-core
cargo test -p faucet-source-rest
cargo test -p faucet-source-graphql
cargo test -p faucet-source-xml
cargo test -p faucet-source-grpc
cargo test -p faucet-sink-bigquery
cargo test -p faucet-sink-postgres
cargo test -p faucet-sink-jsonl
cargo test -p faucet-sink-snowflake
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

## Publishing

Crates must be published in dependency order with delays for crates.io index propagation:

1. `faucet-core`
2. All sources + sinks (after 30s): `faucet-source-rest`, `faucet-source-graphql`, `faucet-source-xml`, `faucet-source-grpc`, `faucet-sink-bigquery`, `faucet-sink-postgres`, `faucet-sink-jsonl`, `faucet-sink-snowflake`
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
