# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [Unreleased]

## `faucet-stream` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-stream-v1.0.0...faucet-stream-v1.0.1) - 2026-06-02

### Bug Fixes

- Correct stale crates.io READMEs + finish faucet-common-* umbrella rename

### Miscellaneous

- Release v1.0.1 ([#168](https://github.com/PawanSikawat/faucet-stream/pull/168))

## `faucet-sink-kafka` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-kafka-v1.0.0...faucet-sink-kafka-v1.0.1) - 2026-06-02

### Bug Fixes

- Correct stale crates.io READMEs + finish faucet-common-* umbrella rename

### Miscellaneous

- Release v1.0.1 ([#168](https://github.com/PawanSikawat/faucet-stream/pull/168))

### Other

- Restore rustfmt import ordering after the faucet-common-* rename

## `faucet-sink-elasticsearch` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-elasticsearch-v1.0.0...faucet-sink-elasticsearch-v1.0.1) - 2026-06-02

### Bug Fixes

- Correct stale crates.io READMEs + finish faucet-common-* umbrella rename

### Miscellaneous

- Release v1.0.1 ([#168](https://github.com/PawanSikawat/faucet-stream/pull/168))

## `faucet-sink-gcs` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-gcs-v1.0.0...faucet-sink-gcs-v1.0.1) - 2026-06-02

### Bug Fixes

- Correct stale crates.io READMEs + finish faucet-common-* umbrella rename

### Miscellaneous

- Release v1.0.1 ([#168](https://github.com/PawanSikawat/faucet-stream/pull/168))

### Other

- Restore rustfmt import ordering after the faucet-common-* rename

## `faucet-source-elasticsearch` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-elasticsearch-v1.0.0...faucet-source-elasticsearch-v1.0.1) - 2026-06-02

### Bug Fixes

- Correct stale crates.io READMEs + finish faucet-common-* umbrella rename

### Miscellaneous

- Release v1.0.1 ([#168](https://github.com/PawanSikawat/faucet-stream/pull/168))

## `faucet-source-gcs` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-gcs-v1.0.0...faucet-source-gcs-v1.0.1) - 2026-06-02

### Bug Fixes

- Correct stale crates.io READMEs + finish faucet-common-* umbrella rename

### Miscellaneous

- Release v1.0.1 ([#168](https://github.com/PawanSikawat/faucet-stream/pull/168))

### Other

- Restore rustfmt import ordering after the faucet-common-* rename

## `faucet-source-kafka` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-kafka-v1.0.0...faucet-source-kafka-v1.0.1) - 2026-06-02

### Bug Fixes

- Correct stale crates.io READMEs + finish faucet-common-* umbrella rename

### Miscellaneous

- Release v1.0.1 ([#168](https://github.com/PawanSikawat/faucet-stream/pull/168))

### Other

- Restore rustfmt import ordering after the faucet-common-* rename

## `faucet-common-mssql` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-common-mssql-v1.0.0...faucet-common-mssql-v1.0.1) - 2026-06-02

### Bug Fixes

- Correct stale crates.io READMEs + finish faucet-common-* umbrella rename

### Miscellaneous

- Release v1.0.1 ([#168](https://github.com/PawanSikawat/faucet-stream/pull/168))

## `faucet-cli` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-cli-v1.0.0...faucet-cli-v1.0.1) - 2026-06-02

### Miscellaneous

- Updated the following local packages: faucet-source-kafka, faucet-source-gcs, faucet-source-elasticsearch, faucet-sink-gcs, faucet-sink-elasticsearch, faucet-sink-kafka, faucet-source-mssql, faucet-sink-mssql

## `faucet-sink-mssql` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-sink-mssql-v1.0.0...faucet-sink-mssql-v1.0.1) - 2026-06-02

### Miscellaneous

- Updated the following local packages: faucet-common-mssql

## `faucet-source-mssql` — [1.0.1](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-mssql-v1.0.0...faucet-source-mssql-v1.0.1) - 2026-06-02

### Miscellaneous

- Updated the following local packages: faucet-common-mssql

### Bug Fixes

- Add multi-parent DAG validation and move futures to workspace deps
- Resolve SQL injection, JSON corruption, and semaphore deadlock
- Seek to bookmark in rebalance, eliminate restart duplicate (#50)
- Durable LSN feedback + transaction buffer cap (#78 findings #1, #2) (#81)

### CI & Build

- Backfill into release.yml ordering and project docs (#69)
- Fail fast if the derived publish list is incomplete (part of #78) (#90)

### Documentation

- Update CLAUDE.md and verify umbrella crate for SourceDAG
- Add runnable examples for pipeline, streaming, and DAG
- Cover the source × sink connector matrix
- Add 10 more popular source-sink combinations
- Exercise the full builder surface in every example
- Add cleanup-after-PR-merge rule to CLAUDE.md (#47)
- Condense CLAUDE.md, drop redundant per-crate enumeration (#51)
- Harden docs.rs rendering across all crates (WS-1 of #91) (#92)
- Positioning, comparison, architecture diagram + de-flake test (WS-2 & WS-3 of #91) (#93)
- MdBook site + runnable examples & local Docker stack (WS-4 & WS-5 of #91) (#94)
- Connector capability matrix, selection guide & community scaffolding (WS-6 & WS-7 of #91) (#95)

### Features

- Add substitute_context and extract_context utilities
- Make fetch_with_context the primary Source trait method
- Migrate all source crates to fetch_with_context
- Add SourceDAG data structures and builder
- Implement SourceDAG::run() execution engine
- Wire parent context into REST, DB, GraphQL, and S3 sources
- Wire parent context into remaining 7 source connectors
- Stdout/stderr sink + pluggable replication state stores
- Wire state-store resume into RestStream
- Add 'faucet' config-driven pipeline runner binary (#41)
- Apache Kafka source + sink + common crate (#46)
- Apache Parquet source + sink connectors (#48)
- PostgreSQL logical replication source (#49)
- --from-env pipeline mode (Closes #42) (#53)
- Pipeline+matrix config and cwd auto-discovery (#56)
- Streaming Pipeline::run with Source::stream_pages contract (#58)
- Observability — OTel-compatible tracing + Prometheus metrics (Closes #31) (#63)
- Dead-letter queue support for sinks (#65)
- GCS source + sink connectors (Closes #26, #27) (#66)
- Schema-driven faucet init --source X --sink Y scaffolder (#67)
- Faucet-elasticsearch-common shared auth crate (closes #43) (#68)
- Named source/sink templates + matrix `ref:` syntax (#73)
- Add Snowflake + BigQuery query source connectors (#75)
- Gzip/zstd compression for file connectors (closes #33) (#76)
- Add server-streaming RPC support (closes #34) (#77)

### Miscellaneous

- Increase publish wave wait to 15 minutes
- Ignore docs/superpowers/ AI workflow artifacts

### Other

- Pre-1.0 hardening: CRITICAL batch 1 (#78 findings #3, #4, #5, #6, #7, #8, #11) (#79)
- Pre-1.0 hardening: CRITICAL #9 + 10 fixes (#78 findings #9,#10,#14,#15,#16,#18,#20,#21,#22,#27,#28) (#86)
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) (#87)
- Pre-1.0 hardening: 17 MEDIUM-tier fixes (closes out #78) (#88)
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) (#89)

### Refactor

- Replace local resolve_path with faucet_core::util::substitute_context
- Struct variants for newtype auth enums (Closes #40) (#52)
- Remove unused SourceDAG executor (closes #62) (#74)

### Testing

- Add GitHub-style integration test for SourceDAG
- De-flake on_error=stop abort test (part of #78 finding #24) (#80)

## [0.2.0] - 2026-04-03

### Bug Fixes

- Fix security issues, error semantics, and code quality across workspace
- Fix CI: install libcurl-dev for rdkafka-sys build
- Fix release.toml: remove invalid publish-delay key
- Fix release workflow: publish in waves to respect crates.io rate limit

### Miscellaneous

- Bump version to 0.1.4
- Set faucet-stream version to 0.1.0

### Other

- Add Auth::TokenEndpoint with ResponseValidator for fetching credentials from APIs
- Restructure into multi-crate workspace with source/sink categories and add comprehensive test coverage
- Add Pipeline orchestration for source-to-sink data transfer
- Add 6 new connectors: GraphQL, XML, gRPC sources and Postgres, JSONL, Snowflake sinks
- Extract shared utilities into faucet-core::util module
- Add 18 new connectors: 9 sources + 9 sinks
- Remove Kafka source+sink (rdkafka C dependency too heavy for CI)
- Add SQLite source connector (faucet-source-sqlite)
- Improve third-party connector developer experience
- Optimize all connectors for throughput
- Add config loading from JSON files and env vars
- Add config_schema() to Source and Sink traits via schemars
- Update README with config loading, schema introspection, and version fixes
- Add extensive README for every source, sink, core, and umbrella crate
- Add automated release workflow with cargo-release
- Add crate README update rule to CLAUDE.md
- Remove old publish.yml, replaced by release.yml
- Restrict releases to main branch only
- Optimize CI: parallelize jobs and test all features in isolation
- {{crate_name}} v{{version}}

## [0.1.4] - 2026-03-25

### Other

- Add all features from reststream meltano
- Update readme and docs for all meltano features

## [0.1.3] - 2026-03-23

### Bug Fixes

- Reduce keywords to crates.io limit of 5

### Miscellaneous

- Bump version to 0.1.3

### Other

- Add wf for crates publish
- Add support for next link
- Add support for next link
- Automatic update of crate version

## [0.1.2] - 2026-03-23

### Other

- Initial code for faucet-stream
- Add CI workflow
- Add precommit
- Add precommit
- Precommit run
- Add support for docs
- Add stream pages support
- Add wf for crates publish
- Add wf for crates publish


