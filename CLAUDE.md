# CLAUDE.md

## Library Purpose

`faucet-stream` is a modular, config-driven data pipeline toolkit for Rust with pluggable **source** and **sink** connectors, plus a `faucet` CLI binary that runs pipelines declaratively from YAML/JSON — no Rust code required.

- **Sources** fetch data from external systems (e.g. REST APIs).
- **Sinks** write data to external systems (e.g. BigQuery).
- **`faucet` CLI** (`cli/`) wires source → transforms → sink together based on a config file.

Design goal: callers configure a source or sink once, call `fetch_all()` or `write_batch()`, and get/write all records — no manual pagination loop, no auth boilerplate. Rust users embed the library directly; everyone else runs the `faucet` binary.

This workspace produces both library crates (`faucet-core` + every connector and state backend) and the `faucet` CLI binary. There is no database, no migrations, and no server.

## Workspace Structure

Cargo workspace, 39 crates (38 libraries + the `faucet-cli` binary). All connector crates depend only on `faucet-core`; the umbrella `faucet-stream` and the `faucet-cli` binary depend on every connector + state crate via optional features.

| Crate | Path | Description |
|-------|------|-------------|
| `faucet-core` | `crates/core/` | Shared types, traits (`Source`, `Sink`), pipeline orchestration, transforms, error types |
| `faucet-source-rest` | `crates/source/rest/` | REST API source — auth, pagination, extraction, schema inference |
| `faucet-source-graphql` | `crates/source/graphql/` | GraphQL API source — cursor pagination, variable injection |
| `faucet-source-xml` | `crates/source/xml/` | XML/SOAP API source — XML-to-JSON conversion, dot-path extraction |
| `faucet-source-grpc` | `crates/source/grpc/` | gRPC source — dynamic protobuf via `prost-reflect`; unary and server-streaming RPCs |
| `faucet-source-postgres` | `crates/source/postgres/` | PostgreSQL query source — run SQL, return rows as JSON |
| `faucet-source-postgres-cdc` | `crates/source/postgres-cdc/` | PostgreSQL CDC (logical replication) source — pgoutput decoder, slot lifecycle, resumable via state store |
| `faucet-source-mysql` | `crates/source/mysql/` | MySQL query source |
| `faucet-source-sqlite` | `crates/source/sqlite/` | SQLite query source |
| `faucet-source-s3` | `crates/source/s3/` | AWS S3 source — JSONL, JSON array, or raw text |
| `faucet-source-gcs` | `crates/source/gcs/` | Google Cloud Storage source — JSONL, JSON array, or raw text |
| `faucet-source-mongodb` | `crates/source/mongodb/` | MongoDB source — find() with filter/projection/sort |
| `faucet-source-redis` | `crates/source/redis/` | Redis source — streams, lists, key patterns |
| `faucet-source-webhook` | `crates/source/webhook/` | Webhook source — temporary HTTP server collecting POSTs |
| `faucet-source-csv` | `crates/source/csv/` | CSV file source |
| `faucet-source-elasticsearch` | `crates/source/elasticsearch/` | Elasticsearch source — search/scroll API |
| `faucet-source-parquet` | `crates/source/parquet/` | Parquet source — local, glob, or S3; vectorized Arrow async reader, projection |
| `faucet-source-kafka` | `crates/source/kafka/` | Kafka consumer — subscribes to topics, drains with idle/max-messages termination |
| `faucet-source-bigquery` | `crates/source/bigquery/` | BigQuery query source — `jobs.query` + `jobs.getQueryResults`, pageToken pagination, type-aware row decoding |
| `faucet-source-snowflake` | `crates/source/snowflake/` | Snowflake query source — SQL REST API with server-side partition pagination, JWT / OAuth auth |
| `faucet-sink-bigquery` | `crates/sink/bigquery/` | BigQuery streaming insert sink |
| `faucet-sink-postgres` | `crates/sink/postgres/` | PostgreSQL sink — JSONB or auto-mapped columns |
| `faucet-sink-jsonl` | `crates/sink/jsonl/` | JSON Lines file sink |
| `faucet-sink-snowflake` | `crates/sink/snowflake/` | Snowflake SQL REST API sink |
| `faucet-sink-mysql` | `crates/sink/mysql/` | MySQL sink — JSON column or auto-mapped columns |
| `faucet-sink-sqlite` | `crates/sink/sqlite/` | SQLite sink — JSON column or auto-mapped columns |
| `faucet-sink-s3` | `crates/sink/s3/` | S3 sink — JSONL files |
| `faucet-sink-gcs` | `crates/sink/gcs/` | Google Cloud Storage sink — JSONL files |
| `faucet-sink-mongodb` | `crates/sink/mongodb/` | MongoDB sink — insert_many |
| `faucet-sink-redis` | `crates/sink/redis/` | Redis sink — streams, lists, key-value |
| `faucet-sink-csv` | `crates/sink/csv/` | CSV file sink |
| `faucet-sink-elasticsearch` | `crates/sink/elasticsearch/` | Elasticsearch sink — bulk index API |
| `faucet-sink-http` | `crates/sink/http/` | HTTP POST sink |
| `faucet-sink-stdout` | `crates/sink/stdout/` | Stdout/stderr sink — JSON Lines, pretty JSON, or TSV |
| `faucet-sink-parquet` | `crates/sink/parquet/` | Parquet sink — local or S3; schema inference, compression, row/byte rollover |
| `faucet-sink-kafka` | `crates/sink/kafka/` | Kafka producer — FuturesUnordered batched sends, QueueFull retry, multi-topic routing |
| `faucet-bigquery-common` | `crates/bigquery-common/` | Shared `BigQueryCredentials` enum and `build_client` for BigQuery source/sink |
| `faucet-elasticsearch-common` | `crates/elasticsearch-common/` | Shared `ElasticsearchAuth` enum for Elasticsearch source/sink |
| `faucet-gcs-common` | `crates/gcs-common/` | Shared types for GCS source/sink — credentials enum, Storage/StorageControl client builders |
| `faucet-kafka-common` | `crates/kafka-common/` | Shared types for Kafka source/sink — auth, value formats, Schema Registry client |
| `faucet-snowflake-common` | `crates/snowflake-common/` | Shared `SnowflakeAuth` enum + `authorization_header` / `snowflake_token_type` helpers for Snowflake source/sink |
| `faucet-state-redis` | `crates/state/redis/` | Redis-backed `StateStore` for replication bookmarks |
| `faucet-state-postgres` | `crates/state/postgres/` | PostgreSQL-backed `StateStore` for replication bookmarks |
| `faucet-stream` | `faucet-stream/` | Umbrella crate — feature-gated re-exports of all connectors and state backends |
| `faucet-cli` | `cli/` | `faucet` binary — YAML/JSON config-driven pipeline runner (`run`, `validate`, `schema`, `list`, `preview`, `init`) |

## Capturing Feature Ideas as GitHub Issues

**Whenever a new feature, enhancement, or bug surfaces in conversation — whether the user asks for it directly, you propose it, or it emerges as a side observation while working on something else — file it as a GitHub issue immediately, before continuing the current task.** This is how future sessions inherit the full context without needing to re-derive it from chat history.

Rules:

- **File the issue right away.** Don't wait until the end of the session, and don't batch multiple ideas into one issue unless they are genuinely the same change. One issue per discrete piece of work.
- **Be exhaustively descriptive.** Each issue must stand alone — a fresh Claude session opening it cold should have everything needed to scope and implement the work. Include:
  - **Summary** — one-paragraph statement of the problem and the proposed change.
  - **Motivation** — why this matters (performance, correctness, ergonomics, third-party connector friendliness, etc.). Tie it back to the [Primary Goal](#primary-goal) or [Third-Party Connector Friendliness](#third-party-connector-friendliness) sections where relevant.
  - **Proposed design** — concrete API shape, config field names, trait method signatures, file paths to touch. Show example usage in Rust where useful.
  - **Affected crates / files** — explicit list of crates and modules that need changes.
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

Concrete `target/` cleanup map (in order of safety, most-safe first — all gitignored and regenerable):

- `target/doc/` — rustdoc HTML output (regenerates via `cargo doc`).
- `target/package/` — old packaged builds from `cargo publish --dry-run` (e.g. `faucet-stream-0.1.x/`, `tmp-crate/`).
- `target/tmp/` — scratch dir, often empty.
- `target/flycheck0/` — rust-analyzer's separate check target; rebuilds on next analyzer session.
- `target/debug/incremental/` and `target/release/incremental/` — compiler incremental cache. Deleting does **not** force a full rebuild — `*.rlib`s in `deps/` still link; cargo just recomputes incremental state on the next build.
- `target/debug/examples/` and `target/release/examples/` — built example binaries; rebuild via `cargo build --examples`.
- `target/**/.DS_Store` — macOS Finder metadata.
- `cargo sweep --installed` — drops artifacts built against toolchains no longer installed by rustup (common after a `rust-toolchain.toml` bump). Use this before considering `cargo clean` — it's the safest way to reclaim multi-GB from `target/debug/deps/`.

**Do not delete these without an explicit user instruction** — they'll trigger an expensive full workspace rebuild or break the incremental graph:

- `target/debug/deps/` and `target/release/deps/` as a whole — the `.rlib`/`.rmeta` cargo links against. Cargo's mtime-on-use touches every file on every build, so `cargo sweep --time N` cannot identify per-hash duplicates as stale; only `cargo clean` (or per-crate `cargo clean -p`) clears them, which forces a full rebuild (~15–30 min on this workspace).
- `target/debug/build/` and `target/release/build/` — build-script outputs (`bindgen`, protobuf, etc.). Deleting invalidates the incremental graph for any crate with a `build.rs`.
- `target/debug/.fingerprint/` — cargo's per-artifact fingerprints; removing them desyncs cargo from the rlibs.
- Top-level `target/debug/*.rlib` and binaries (e.g. `target/debug/faucet`) — the workspace's own outputs.

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

This crate is designed as a **marketplace ecosystem** — third-party developers should be able to build and publish their own `faucet-source-*` and `faucet-sink-*` crates with minimal friction.

- **`faucet-core` is the only required dependency** for connector authors. It re-exports `async_trait`, `serde_json` (`Value`, `json!`), and `schemars` (`JsonSchema`, `schema_for!`). If a new common dependency is needed by connector authors, re-export it from `faucet-core` rather than requiring them to add it.
- **`Source` and `Sink` traits must stay simple and object-safe.** Don't add methods that require connector-specific types, complex generics, or associated types that break `Box<dyn Source>` / `Box<dyn Sink>`. New trait methods must have default implementations so existing connectors don't break.
- **`FaucetError` must accommodate third-party errors.** The `Custom(Box<dyn Error + Send + Sync>)` variant lets connector authors wrap their own error types without losing the chain. Don't remove it.
- **`Pipeline` must remain generic** over any `Source` + `Sink` combination. Don't introduce coupling to specific connectors in the pipeline or core crate.
- **Naming convention: `faucet-source-<name>` / `faucet-sink-<name>`.**
- **Don't add mandatory dependencies to `faucet-core`** that connector authors wouldn't need (e.g. database drivers, cloud SDKs). Keep core lightweight — connector-specific deps belong in their own crates.

## Performance

All connectors must be optimised for throughput by default. When modifying or adding connectors, apply these principles:

- **Reuse clients/connections** — create S3 clients, MongoDB clients, Redis connections, HTTP clients in `new()` and store in the struct. Never recreate per-call.
- **Connection pooling** — all database connectors must use configurable `max_connections` pools (default: 10 for sources, 5 for sinks).
- **Multi-row INSERT** — database sinks must use multi-row `INSERT INTO ... VALUES (...), (...), ...` instead of one INSERT per record.
- **Transaction wrapping** — SQLite sink wraps batches in `BEGIN`/`COMMIT` transactions.
- **Parallel I/O** — S3 source/sink uses `buffer_unordered()` for concurrent object reads/writes. HTTP sink sends Individual-mode requests concurrently via semaphore. REST source processes partitions concurrently when `partition_concurrency` is set.
- **Bulk APIs** — prefer bulk/batch APIs when available (Elasticsearch bulk NDJSON, BigQuery insertAll, MongoDB insert_many, Redis pipelines + MGET).
- **Buffered I/O** — file sinks (JSONL, CSV) must use buffered writers. CSV uses `spawn_blocking` to avoid blocking the async runtime.
- **Configurable concurrency** — expose `concurrency` or `max_connections` fields on configs with sensible defaults.

### Streaming and batching

Every `Pipeline::run` drives `Source::stream_pages(ctx, batch_size)` internally and writes each emitted `StreamPage` to the sink as it arrives. This bounds memory at O(batch_size) on both sides regardless of total record volume.

Sources that override `stream_pages` to stream natively from their underlying primitive: `rest`, `graphql`, `postgres`, `postgres-cdc`, `mysql`, `sqlite`, `mongodb`, `s3` (JSONL/RawText modes), `gcs` (JSONL/RawText modes), `parquet`, `csv`, `xml`, `elasticsearch` (scroll API), `kafka`, `redis` (all three modes), `grpc` (server-streaming mode only). Sources that intentionally keep the default chunk-the-buffer impl: `grpc` (unary mode — no native paging primitive), `webhook` (buffer-shaped by nature). Client-streaming and bidirectional-streaming gRPC remain out of scope.

Sinks that expose a `batch_size` config field for write-side re-chunking: every sink — `parquet`, `s3`, `gcs`, `bigquery`, `snowflake`, `postgres`, `mysql`, `sqlite`, `mongodb`, `redis`, `elasticsearch`, `http`, `kafka` (re-chunking is internally the natural unit for each — multi-row INSERTs, `_bulk` bodies, `tabledata.insertAll` requests, `insert_many` calls, Redis pipelines, etc.). The file/append sinks (`jsonl`, `csv`, `stdout`) carry the field for config parity but write per-record, so `batch_size` is a no-op for them.

**Contract:**
- `Source::stream_pages` returns `Stream<Item = Result<StreamPage, FaucetError>>` where `StreamPage { records, bookmark }`.
- The pipeline calls `Sink::write_batch` once per yielded page, then `Sink::flush` and `StateStore::put` whenever a page carries `Some(bookmark)`. Most sources emit `Some` only on the final page; CDC-style sources emit `Some` per committed transaction and get per-transaction durability automatically.
- `DEFAULT_BATCH_SIZE` is 1000; `MAX_BATCH_SIZE` is 1,000,000. Validate via `validate_batch_size` at config load time.
- **`batch_size = 0` is the "no batching" sentinel**: sources emit the entire result set in a single `StreamPage`, and sinks accept whatever upstream hands them without re-chunking. Use it for small lookup tables, or for sinks like SQL `COPY` / BigQuery load jobs that prefer one large request to many small ones.
- Per-source / per-sink `batch_size` config fields map onto this hint — see each crate's README for its native paging primitive and recommended values.
- On the source trait the `batch_size` argument to `stream_pages` is informational; every overriding source uses its config field as the authoritative knob (so a pipeline-supplied hint cannot silently override an explicit config value).

### Dead-letter queue (DLQ)

`Pipeline::with_dlq(DlqConfig)` and `RunStreamOptions::with_dlq(...)`
attach an optional DLQ sink. The streaming loop calls
`Sink::write_batch_partial` per page; rows that come back as `Err` get
wrapped in a fixed-shape envelope and dispatched to the DLQ sink before
the page bookmark advances. Sinks that don't override
`write_batch_partial` use the default impl (success → all-Ok; failure →
outer Err), and the router applies the configured `on_batch_error`
policy (`propagate` aborts the run like today; `dlq_all` routes every
row in the failed page to the DLQ). BigQuery and Elasticsearch override
`write_batch_partial` so their best-effort APIs don't duplicate rows in
the DLQ. The full design lives in
`docs/superpowers/specs/2026-05-24-dlq-design.md`.

## Commands

```bash
# Build / test / lint
cargo build --workspace
cargo test --workspace --all-features            # Kafka integration tests require Docker
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy --workspace --fix --allow-dirty --allow-staged

# Dry-run publish (sanity check)
cargo publish --dry-run -p faucet-core
cargo publish --dry-run -p faucet-stream
cargo publish --dry-run -p faucet-cli

# Build / install the CLI binary
cargo build -p faucet-cli                        # → target/debug/faucet
cargo install --path cli                         # → ~/.cargo/bin/faucet
cargo install --path cli --no-default-features --features "source-rest,sink-jsonl,sink-stdout,transforms"  # slim build

# Drive the CLI on a config file
./target/debug/faucet list
./target/debug/faucet schema source rest
./target/debug/faucet validate cli/examples/csv_to_jsonl.yaml
./target/debug/faucet run cli/examples/csv_to_jsonl.yaml
```

## Architecture

### faucet-core (`crates/core/`)

The only crate every connector depends on. Module layout:

- `error.rs` — `FaucetError` enum: `Http`, `HttpStatus`, `Json`, `JsonPath`, `Auth`, `RateLimited`, `Url`, `Transform`, `Config`, `Source`, `Sink`, `State`, `Custom(Box<dyn Error>)`.
- `traits.rs` — `Source` (primary: `fetch_with_context()`, convenience: `fetch_all()`, plus `state_key()` / `apply_start_bookmark()` for resumable runs) and `Sink` async traits. Both expose `config_schema(&self) -> Value` and `connector_name(&self) -> &'static str` (default returns stripped `type_name`; built-in connectors override with friendly labels like `"rest"`, `"jsonl"`). Object-safe — no associated types, no generics on trait methods.
- `pipeline.rs` — `Pipeline` (batch) and `run_stream()` (streaming). `Pipeline::with_state_store(Arc<dyn StateStore>)` wires durable bookmarks (read before fetch, persist only after sink confirms the batch).
- `config.rs` — config loading helpers (`load_json`, `load_env`, `load_env_file`) and `duration_secs` / `duration_secs_option` serde modules.
- `util.rs` — `quote_ident` (SQL injection prevention), `extract_records` (JSONPath), `check_http_response`, `substitute_context` (placeholder substitution for URLs/paths — NOT safe for SQL or JSON), `substitute_context_bind_params` (SQL-safe via bind markers), `substitute_context_json` (JSON-safe), `extract_context`.
- `transform.rs` — `RecordTransform` / `CompiledTransform`: flatten, rename keys (regex), snake_case, custom closures. Built-in transforms are feature-gated.
- `replication.rs` — `ReplicationMethod`, `filter_incremental`, `max_replication_value` for bookmark-based incremental replication.
- `retry.rs` — shared `execute_with_retry` (exponential backoff + jitter, gated on `FaucetError::is_retriable`) used by HTTP sources (XML, GraphQL). The REST source keeps its own retry module with extra 429/`Retry-After` handling.
- `schema.rs` — `infer_schema` from record samples with type merging and nullable detection.
- `state.rs` — `StateStore` async trait (`get` / `put` / `delete` over `Value`) + built-in `MemoryStateStore` and `FileStateStore` (one JSON file per key, atomic rename). Keys validated by `validate_state_key`. Heavier backends (Redis, Postgres) live in their own crates.
- `dlq.rs` — DLQ data types: `OnBatchError`, `DlqConfig`, `DlqStats`, `DlqReason`, `build_envelope`. The router itself lives in `pipeline.rs::run_stream`.

`lib.rs` re-exports the trait + types named above, plus third-party crates connector authors need: `async_trait`, `serde_json` (+ `Value`, `json!`), `schemars` (+ `JsonSchema`, `schema_for!`). It also re-exports DLQ types (`DlqConfig`, `DlqStats`, `OnBatchError`, `DlqReason`, `build_envelope`, `RowOutcome`) for connector authors who want to override `write_batch_partial`.

### Connector crate conventions

Every source/sink crate follows the same module layout. Stick to this when adding a new connector:

- `lib.rs` — re-exports config + the `Source`/`Sink` type.
- `config.rs` — config struct + auth/format/pagination sub-enums. Derives `Serialize + Deserialize + JsonSchema`. **No I/O or protocol logic here.**
- `stream.rs` (source) / `sink.rs` (sink) — the one place that performs I/O. Holds reusable clients/pools created in `new()`. Implements `faucet_core::Source` / `Sink` including `config_schema()` via `schemars::schema_for!`.
- Optional helper modules — `auth/`, `pagination/`, `extract/`, `retry/`, `convert.rs`, `schema.rs`, `decode.rs` / `encode.rs`, `state.rs` for bookmarks, `serde_helpers.rs` for non-serializable types (`reqwest::Method` etc.).

Some connectors have noteworthy specifics worth knowing without reading the source:

- **`faucet-source-rest`** — split into `auth/`, `pagination/`, `extract/`, `retry/`, `stream.rs`. `stream.rs` is the only place HTTP requests run; all other modules are pure logic. `pagination::advance()` accepts both response body and headers.
- **`faucet-source-postgres-cdc`** — `pgoutput/` (message types, decoder, relation registry, OID→JSON via `text_to_json`) is pure protocol; `replication.rs` is the `pgwire-replication` glue (replication connection, slot lifecycle via `sqlx`, CopyBoth duplex, Standby Status Updates). Transactions are buffered in memory and only emitted on Commit so partial transactions never leak; `max_staged_records` caps a single in-progress transaction's buffer and aborts with a typed error rather than risking OOM. **LSN-feedback durability:** the advertised `confirmed_flush_lsn` is advanced *only* from a durable bookmark (via `apply_start_bookmark`, i.e. after the pipeline persists), never from decoded WAL at commit time or a keepalive's `wal_end` — so Postgres is never told to recycle WAL for unpersisted changes (no data loss on crash). Tradeoff: WAL is retained for the duration of a fetch cycle and released when the next run resumes from the persisted bookmark. State key: `postgres-cdc:<slot>`.
- **`faucet-source-parquet` / `faucet-sink-parquet`** — both use `parquet::arrow` async reader/writer wired through `object_store` so local and S3 share one code path. Source projects columns via `ProjectionMask` and streams batches via `buffer_unordered(concurrency)`; multi-file schema mismatch surfaces as `FaucetError::Source` naming both files. Sink lazily opens an `AsyncArrowWriter` on first batch (so schema is inferred from real records), drops unknown fields with a one-shot `tracing::warn!`, rolls over on row/byte thresholds, and **requires `flush()` before drop or the multipart upload aborts**.
- **Kafka pair** — shared `faucet-kafka-common` holds `KafkaAuth`, `KafkaValueFormat`, `SchemaRegistryConfig`, `KafkaCompression`, `KafkaTlsConfig`. Schema-registry-backed formats (Avro / Protobuf / JsonSchema) are gated on the `kafka-schema-registry` feature. Source disables auto-commit and seeks to bookmark offsets before first message; sink uses `FuturesUnordered` with QueueFull retry.
- **State backends** — `faucet-state-redis` namespaces keys as `{namespace}:{key}`; `faucet-state-postgres` uses a single `faucet_state(key TEXT PRIMARY KEY, value JSONB, updated_at TIMESTAMPTZ)` table with `ON CONFLICT DO UPDATE`.

### faucet-cli (`cli/`)

Top-level YAML/JSON shape: `version: 1`, optional `name:`, required `pipeline: { source, sink, transforms, state }`, optional `matrix: [...]` (per-row overrides + parent/child fan-out), optional `execution: { max_concurrent, on_error }`. Each matrix row is deep-merged into `pipeline`; rows with `parent: <id>` run once per record produced by the parent, with `${parent_id.dotted.path}` tokens resolved per-record at runtime. Empty matrix = one anonymous invocation. State key is `{name}::{row_id}` for roots and `{name}::{row_id}::{parent_record_key}` for children. The full grammar lives in `cli/README.md`. Named source/sink templates under `pipeline.sources` / `pipeline.sinks` + a top-level `vars:` block are the recommended shape for multi-row configs; the legacy singular `pipeline.source` / `pipeline.sink` continues to work and registers as the `default` template.

- `main.rs` — `tokio::main` entry; installs `tracing-subscriber` against `--log-level` / `FAUCET_LOG`, dispatches to `commands::*::run`.
- `lib.rs` — re-exports every public module so integration tests and downstream crates can drive the same code paths as the binary.
- `cli.rs` — `clap` argument types: `Command::{Run, Validate, Schema, List, Preview, Init}`. `run`/`validate`/`preview` take an optional `config` path (auto-discovered if omitted) and the `--env-file` / `--no-env-file` pair.
- `config.rs` — `PipelineConfig { version, name, vars: Option<HashMap<String, Value>>, pipeline: PipelineSpec, matrix: Vec<MatrixRow>, execution: Option<ExecutionSpec> }` and `OnError { Continue, Stop }`. `PipelineSpec.source` / `sink` are `Option<ConnectorSpec>` (legacy singular shape); named templates live under `pipeline.sources` / `pipeline.sinks: HashMap<String, ConnectorSpec>`. `PartialConnector` gains `r#ref: Option<String>` for template selection per matrix row. `MatrixRow` carries `Option<PartialConnector>` overrides for source/sink. Rejects `version != 1` and the pre-#54 top-level `source:`/`sink:` shape (with a clear hint).
- `interpolate.rs` — three-stage: `interpolate(raw)` resolves `${env:VAR}` / `${file:PATH}` / `${secret:VAR}` at load time and leaves `${id.path}` tokens verbatim; `resolve_config_refs(cfg)` is the post-parse pass run automatically by `PipelineConfig::from_text` — it resolves `${vars.X}` against the top-level `vars:` block, then `${sources.NAME.PATH}` / `${sinks.NAME.PATH}` against post-vars template bodies; cycles in var or template references surface as `InterpolationCycle`; `${row_id.path}` tokens are left for runtime resolution; `interpolate_record(s, ctx)` resolves those deferred tokens at runtime against a context map of parent records.
- `merge.rs` — `merge_value(base, overlay)`: scalars replace, objects merge recursively, arrays replace wholesale. The single rule that defines matrix-row override semantics.
- `expand.rs` — `expand(&PipelineConfig) -> Vec<ExpandedNode>`. A private `Registry<'a>` builds the source/sink template catalog from singular (`pipeline.source` / `pipeline.sink`, registered as `default`) and named (`pipeline.sources.*` / `pipeline.sinks.*`) entries. Rows pick templates via `source.ref` / `sink.ref` and deep-merge `kind` / `config` overrides on top; a row without `ref` falls back to `default`. Validates ids (reserved-prefix / duplicate / unknown-parent / cycle / unknown-interpolation-id) and template names (`UnknownTemplate` / `MissingTemplate` / `DuplicateTemplate` carry actionable context). Emits nodes in BFS order so roots come before children.
- `executor.rs` — `run_expanded(nodes, ExecuteOptions)`. One bounded `Semaphore` shared across roots and per-parent-record child fan-outs. Both `on_error` modes run in parallel via `JoinSet`: `continue` lets siblings finish on failure (the failed subtree is skipped); `stop` calls `JoinSet::abort_all()` on first failure, which drops pending tasks before they acquire a permit and cancels in-flight tasks at their next `.await` (accepting partial-sink-state risk — caveat documented in `cli/README.md`). State-key collisions among siblings under a single parent surface as `CliError::DuplicateStateKey`. Wraps each invocation's source with a `StateKeyOverride` so per-row state keys override the source's natural one.
- `registry.rs` — feature-gated `build_source` / `build_sink` dispatchers, plus `source_schema` / `sink_schema` (via `schema_for!`) and descriptions for `faucet list`.
- `state.rs` — `build_state_store(&StateStoreSpec) -> Arc<dyn StateStore>`. Built-in `memory` / `file` always available; `redis` / `postgres` feature-gated.
- `env_loader.rs` — `resolve_env_file` (explicit / cwd `.env` / disabled) and `discover_config_path` (`faucet.yaml` → `.yml` → `.json` in cwd). Both honour `--env-file` / `--no-env-file`.
- `env_config.rs` — pure-env mode (`--from-env`): walks a `FAUCET_*` env-var snapshot and assembles a `PipelineConfig` with the matrix empty and the pipeline filled from `FAUCET_SOURCE_*` / `FAUCET_SINK_*` / `FAUCET_STATE_*` / `FAUCET_TRANSFORM_<N>_*`. `*_JSON` suffix handles nested/tagged-enum fields; scalar/json conflict errors name both vars; transform indices must be contiguous from 1.
- `transforms.rs` — `compile_transforms`: only `flatten`, `rename_keys`, `snake_case` are exposed via config; custom-closure transforms remain Rust-only.
- `commands/` — `run` calls `expand` + `run_expanded`; `validate` calls `expand` and reports one line per row; `preview` runs the first root only (children need parent records to resolve `${parent.path}`); `schema` / `list` are unchanged in shape. `init` is schema-driven: it accepts `--source` / `--sink` (and an optional positional `name`), pulls each kind's JSON Schema via `registry::source_schema` / `sink_schema`, and renders a comment-annotated YAML through `init_template::schema_to_yaml_template` — required fields are surfaced with `# REQUIRED` and a typed placeholder; optional fields are commented out so connector-level defaults stand. The `--interactive` mode is gated behind the `cli-interactive` Cargo feature and uses `inquire` for the TTY Select prompt.
- `init_template.rs` — pure JSON-Schema → YAML emitter behind `faucet init`. Walks `properties` (resolving `$ref`s against `$defs`), recurses into required nested objects, expands tagged-enum (`oneOf` with a `const`-discriminator) blocks inline, and truncates long descriptions to the first sentence (or 120 chars) so output stays readable when rustdoc comments contain paragraphs of context. Pure function, no I/O.

### Observability

Pipelines emit `tracing` spans and `metrics` counters/histograms automatically — every source, sink, transform, and state-store operation is wrapped by the pipeline-internal decorators in `crates/core/src/observability/`. No per-connector code is required for the universal metric set; connectors only override `connector_name()` (default: stripped `type_name`) when they want a friendly label.

**Common labels:** `pipeline`, `row` (matrix row id; `""` for non-matrix runs), `connector` (from `connector_name()`). `run_id` is a span attribute only (high cardinality; never a Prometheus label).

**Common metrics** (counters and histograms; see the design spec for the full list):
- `faucet_source_records_total{pipeline,row,connector}`, `faucet_source_errors_total{...,kind}`, `faucet_source_page_duration_seconds`, `faucet_source_in_flight`
- `faucet_sink_records_total{...}`, `faucet_sink_writes_total`, `faucet_sink_errors_total`, `faucet_sink_write_duration_seconds`, `faucet_sink_flush_duration_seconds`, `faucet_sink_in_flight`
- `faucet_transform_records_total{pipeline,row}`, `faucet_transform_duration_seconds`
- `faucet_state_{get,put,delete}_total{...,outcome=hit|miss for get}`, `faucet_state_errors_total{...,op,kind}`, `faucet_state_{get,put,delete}_duration_seconds`
- `faucet_pipeline_runs_total{pipeline,row,source,sink,status=ok|err,kind}` (`kind` present only on `status=err`), `faucet_pipeline_run_duration_seconds`, `faucet_pipeline_in_flight`, `faucet_pipeline_start_time_unix_seconds`, `faucet_pipeline_seconds_since_last_bookmark`, `faucet_pipeline_last_bookmark_unix_seconds`
- `faucet_build_info{version}` — set to `1` by `register_build_info()` (called automatically from `install_observability`). Standard Prometheus convention: `group_left` this onto any other metric to annotate dashboards with the running version.

**Reliability guarantees:** drop-guard timers (sample on cancellation), panic isolation (`Panic` error kind via `AssertUnwindSafe.catch_unwind()`), idempotent `install_observability` (already-installed recorder/subscriber warn rather than panic), typed `CliError::Observability` for port-in-use / malformed listen.

**Cardinality rules:**
- Never use high-cardinality values (record IDs, URLs, query strings) as metric labels.
- `parent_record_key` in a parent-child matrix is a *span attribute only*, never a metric label.
- Connector authors must return a non-empty `&'static str` from `connector_name()`; empty strings fall back to `"unknown"` in release builds (and `debug_assert!` in debug).

**Spec:** `docs/superpowers/specs/2026-05-23-observability-otel-prometheus-design.md`.

## Feature Flags (umbrella crate)

Default features: `source-rest`, `transform-flatten`, `transform-rename-keys`, `transform-snake-case`.

Each connector has its own feature: `source-<name>` / `sink-<name>` (`rest`, `graphql`, `xml`, `grpc`, `postgres`, `postgres-cdc`, `mysql`, `sqlite`, `s3`, `mongodb`, `redis`, `webhook`, `csv`, `elasticsearch`, `parquet`, `kafka` for sources; `bigquery`, `postgres`, `jsonl`, `snowflake`, `mysql`, `sqlite`, `s3`, `mongodb`, `redis`, `csv`, `elasticsearch`, `http`, `stdout`, `parquet`, `kafka` for sinks).

State backends: `state-redis`, `state-postgres` (file + memory live in `faucet-core`).

Aggregate features: `source` (all sources), `sink` (all sinks), `state` (all state backends), `full` (everything). Kafka-only: `kafka-schema-registry` enables Avro / Protobuf / JSON Schema via Confluent Schema Registry.

## Compression

`faucet-core` exposes a `compression` feature that pulls in `async-compression`
(async streaming wrappers), `flate2` (sync gzip), and `zstd` (sync zstd). It
surfaces a `faucet_core::compression` module with:

- `CompressionConfig` — user-facing enum (`None | Gzip | Zstd | Auto`),
  default `Auto`. Serializes as lowercase strings.
- `Compression` — post-resolution enum (no `Auto`).
- `CompressionConfig::resolve(path) -> Compression` — `Auto` consults the
  filename suffix (`.gz` → Gzip, `.zst` → Zstd, anything else → None);
  explicit variants pass through.
- `wrap_async_reader` / `wrap_async_writer` — for streaming paths.
- `wrap_sync_reader` / `wrap_sync_writer` — for `spawn_blocking` paths.
- `compress_buf(data, codec)` — one-shot in-memory compression used by
  S3/GCS sinks that upload a full `Vec<u8>` body.
- `warn_mismatch(path, codec)` — logs once per `(path, codec)` pair when
  the explicit codec disagrees with the filename suffix.

Seven connectors gain a `compression` config field gated on a crate-local
`compression` feature: `faucet-source-csv`, `faucet-source-s3`,
`faucet-source-gcs`, `faucet-sink-jsonl`, `faucet-sink-csv`, `faucet-sink-s3`,
`faucet-sink-gcs`. The `faucet-stream` umbrella exposes a `compression`
aggregate feature that activates compression on whichever of those connectors
the user has already opted into (it does **not** pull connectors itself —
use the optional-dep `?` forwarding syntax). The `full` aggregate now
includes compression. The `faucet-cli` binary mirrors the same shape, so
`cargo install faucet-cli --features compression` enables it for every
config-driven user.

Auto resolution runs at I/O time (per-call, per-key) so matrix-row path
interpolation works — the same source can read a mix of `.jsonl`,
`.jsonl.gz`, and `.jsonl.zst` objects in one run.

File sinks (`jsonl`, `csv`) finalise the encoder on `flush()`; subsequent
writes reopen the file in append mode (independent of `config.append`),
producing a multi-member compressed file that gzip / zstd decoders read
back transparently. S3 and GCS sinks do **not** set the `Content-Encoding`
header on uploads; consumers must decompress explicitly.

Parquet, HTTP, stdout, Kafka, and the database sinks are intentionally out
of scope (Parquet has internal columnar compression; the others have
native protocol-level compression options).

## Pagination Styles (REST source)

| Style | Stops When |
|-------|-----------|
| `None` | After first page |
| `Cursor` | Next-token JSONPath is null or absent |
| `PageNumber` | Response returns zero records |
| `Offset` | Offset reaches total (via JSONPath) or response has fewer records than the limit |
| `LinkHeader` | No `rel="next"` in the `Link` response header |
| `NextLinkInBody` | Next-page URL in response body is absent, null, or empty |

`max_pages` acts as a hard cap across all styles. All styles include loop detection — if the same cursor/link is returned twice in a row, pagination stops.

## Coding Principles

### Self-Learning

When the user points out something fundamental about how code in this library should be written — module structure, naming, patterns, error handling, etc. — **add it to this file immediately** under the relevant section. The goal is that the user never has to repeat the same guidance twice.

### Source/Sink pair shared config

When a connector ships both a `faucet-source-<name>` and a `faucet-sink-<name>` crate for the same external system, shared configuration types (auth, value formats, compression, TLS, etc.) live in a dedicated `faucet-<name>-common` crate. Both the source and sink crates depend on the common crate and re-export the shared types so end-user imports do not change. See `faucet-kafka-common` for the reference implementation.

Existing pairs that predate this convention (`postgres`, `mysql`, `sqlite`, `redis`, `mongodb`, `s3`, `csv`) duplicate a tiny shared surface — typically just `connection_url` / `batch_size` / `max_connections`. The Elasticsearch pair tripped the backfill trigger in 2026-05 (a duplicated 4-variant auth enum) and was migrated to `faucet-elasticsearch-common`. The Snowflake and BigQuery pairs were extracted to `faucet-snowflake-common` and `faucet-bigquery-common` when their respective sources landed (closing #29 and #30) — the sink already shipped a multi-variant auth/credentials enum each, so the new source picked up the shared types from the start. The remaining seven pairs stay duplicated for now: backfill an existing pair only once it gains a second shared type (e.g. TLS settings, a credential enum, a compression enum), since the cost of a near-empty `-common` crate exceeds the benefit until then. New pairs must follow the convention from the start.

### Config loading

All connector config structs derive `Serialize + Deserialize + JsonSchema`. Load from JSON files, env vars, or `.env` files via the helpers in `faucet_core::config` (`load_json`, `load_env`, `load_env_file`). `Duration` fields use `#[serde(with = "faucet_core::config::duration_secs")]` (or `duration_secs_option`). Non-serializable fields use `#[serde(skip)]` / `#[serde(skip, default)]`; `reqwest::Method` uses the per-crate `serde_helpers::http_method` module + `#[schemars(with = "String")]`.

### Config schema introspection

Every `Source` / `Sink` overrides `config_schema(&self) -> Value` to return `schema_for!(MyConfig)`. When adding a new connector: (1) derive `JsonSchema` on the config struct and all sub-types; (2) add `#[schemars(with = "...")]` for any custom-serde fields; (3) implement `config_schema()`.

### Error handling

All errors map to a `FaucetError` variant. Never `.unwrap()` / `.expect()` on values that can fail at runtime. Use `.expect()` only for programmer errors (invariants validated at construction time). All error types use `thiserror`.

## Testing

Every non-trivial piece of logic must have tests. Untested public API surface is a liability.

- **Unit tests** live in `#[cfg(test)]` modules at the bottom of each source file — for logic that doesn't need network I/O (JSONPath extraction, pagination state transitions, auth header generation, Link header parsing).
- **Integration tests** live in the crate's `tests/` directory and use `wiremock` for HTTP-based connectors.
- When adding a new feature (auth method, pagination style, etc.), add both unit and integration tests.
- Tests must assert the specific outcome, not just "no panic".
- **New code** — always write tests for new functions or behaviors. Non-negotiable.
- **Modified code** — do NOT automatically rewrite or update existing tests. If a code change breaks an existing test, investigate first; silently updating tests to match new behaviour hides regressions.

```bash
cargo test --workspace --all-features
cargo test -p faucet-core
cargo test -p faucet-source-rest
cargo test -p faucet-stream --features full
```

## Dependency Policy

Always use the **highest available stable version** for every crate, the Rust toolchain, and the Rust edition.

- Rust toolchain is pinned in `rust-toolchain.toml`. Update `channel` to latest stable when upgrading.
- `Cargo.toml` `edition` should always be the latest stable Rust edition (as of 2026: `"2024"`).
- Before adding a new crate, check its latest stable release on crates.io and use that version.
- When upgrading, check with `cargo search <crate>`.
- Never use alpha/beta/rc versions unless there is no stable alternative.
- Shared dependencies go in workspace `[workspace.dependencies]` and member crates reference them with `.workspace = true`.

Key workspace deps: `serde` 1, `serde_json` 1, `schemars` 1.2, `async-trait` 0.1, `thiserror` 2, `reqwest` 0.13, `tokio` 1, `tracing` 0.1, `sqlx` 0.8, `dotenvy` 0.15, `envy` 0.4.

## Publishing

Crates publish in dependency order with delays for crates.io index propagation: (1) `faucet-core`; (2) all connector + state crates (after 30s); (3) `faucet-stream` umbrella + `faucet-cli` (after another 30s). `.github/workflows/publish.yml` handles this automatically on version tags (`v*.*.*`).

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

```bash
gh pr checks <PR-number>
```

- If every line says `pass`, proceed with the merge.
- If any line says `fail` or `pending`, **stop and report the failing jobs to the user before merging.** Pull the job logs (`gh run view --log-failed --job <job-id>`) and surface the root cause so the user can decide whether to fix-then-merge or merge-anyway (rare — only if the failure is in an unrelated job the user explicitly tells you to ignore).
- If checks are still running, wait for them to finish rather than racing.

This rule applies regardless of how the merge was requested — "merge it", "ship it", "land the PR", or anything similar. The verification is non-negotiable.
