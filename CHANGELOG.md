# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(see the versioning policy in CONTRIBUTING.md — connector crates version
independently).
## [Unreleased]

## `faucet-cli` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-cli-v1.0.0) - 2026-06-01

### Bug Fixes

- ES resume-dup surfacing + Redis fetch_all drain + fan-out Arc<Value> ([#160](https://github.com/PawanSikawat/faucet-stream/pull/160)) ([#166](https://github.com/PawanSikawat/faucet-stream/pull/166))
- *(#146)* LOW/NIT backlog — serve/scheduler/observability + doctor probe + docs (#164)
- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)
- *(#146)* LOW/NIT backlog — secrets & security hardening (#162)
- Resolve the 11 verified LOW items from the #146 hardening audit ([#159](https://github.com/PawanSikawat/faucet-stream/pull/159))
- Resolve the final 3 HIGH release-blockers from #146 (H7/H9/H16) ([#158](https://github.com/PawanSikawat/faucet-stream/pull/158))
- *(cli)* Config-validation & interpolation hardening — M1/M2/M3/M17 ([#146](https://github.com/PawanSikawat/faucet-stream/pull/146)) ([#156](https://github.com/PawanSikawat/faucet-stream/pull/156))
- *(serve)* Idempotency & history correctness — M5/M6/M7/M8 ([#146](https://github.com/PawanSikawat/faucet-stream/pull/146)) ([#155](https://github.com/PawanSikawat/faucet-stream/pull/155))
- Resolve the 4 CRITICAL release-blockers from the #146 pre-1.0 audit (C1–C4) ([#147](https://github.com/PawanSikawat/faucet-stream/pull/147))

### Documentation

- *(readme)* Positioning, comparison, architecture diagram + de-flake test (WS-2 & WS-3 of #91) ([#93](https://github.com/PawanSikawat/faucet-stream/pull/93))
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- *(mssql)* Microsoft SQL Server source + sink connectors (#119, #120) ([#145](https://github.com/PawanSikawat/faucet-stream/pull/145))
- *(serve)* Complete `faucet serve` — SSE logs, persistent history, OpenAPI + docs (Phases 4–6 of #127) ([#144](https://github.com/PawanSikawat/faucet-stream/pull/144))
- *(serve)* Run lifecycle + idempotency + doctor_first (Phase 2+3 of #127) ([#143](https://github.com/PawanSikawat/faucet-stream/pull/143))
- *(core)* Adaptive AIMD batch sizing ([#128](https://github.com/PawanSikawat/faucet-stream/pull/128)) ([#142](https://github.com/PawanSikawat/faucet-stream/pull/142))
- *(serve)* Faucet serve — HTTP control-plane skeleton (Phase 1 of #127) ([#140](https://github.com/PawanSikawat/faucet-stream/pull/140))
- *(cli)* Faucet schedule cron scheduler + ${now.*} run-clock interpolation ([#139](https://github.com/PawanSikawat/faucet-stream/pull/139))
- Faucet doctor — preflight probes for every connector ([#126](https://github.com/PawanSikawat/faucet-stream/pull/126)) ([#137](https://github.com/PawanSikawat/faucet-stream/pull/137))
- *(cli)* Complete secrets interpolation — auth: catalog + vars: block ([#134](https://github.com/PawanSikawat/faucet-stream/pull/134)), Azure KV chain reuse ([#135](https://github.com/PawanSikawat/faucet-stream/pull/135)) ([#136](https://github.com/PawanSikawat/faucet-stream/pull/136))
- *(cli)* Secrets-manager interpolation (vault / aws-sm / gcp-sm / azure-kv) ([#133](https://github.com/PawanSikawat/faucet-stream/pull/133))
- Built-in data-quality checks (per-record + per-batch) with DLQ routing ([#132](https://github.com/PawanSikawat/faucet-stream/pull/132))
- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- *(source-websocket)* WebSocket streaming source connector ([#112](https://github.com/PawanSikawat/faucet-stream/pull/112))
- *(transforms)* Richer transform model — filter (1→0) and explode (1→N) ([#111](https://github.com/PawanSikawat/faucet-stream/pull/111))
- Library transforms wrapper + layered config ([#101](https://github.com/PawanSikawat/faucet-stream/pull/101)) ([#110](https://github.com/PawanSikawat/faucet-stream/pull/110))
- *(cli)* List/describe available transforms ([#100](https://github.com/PawanSikawat/faucet-stream/pull/100)) ([#109](https://github.com/PawanSikawat/faucet-stream/pull/109))
- *(transforms)* [**breaking**] Add 8 config-exposed transforms, replace snake_case with keys_case ([#107](https://github.com/PawanSikawat/faucet-stream/pull/107))
- Gzip/zstd compression for file connectors (closes #33) ([#76](https://github.com/PawanSikawat/faucet-stream/pull/76))
- *(sources)* Add Snowflake + BigQuery query source connectors ([#75](https://github.com/PawanSikawat/faucet-stream/pull/75))
- *(cli)* Named source/sink templates + matrix `ref:` syntax ([#73](https://github.com/PawanSikawat/faucet-stream/pull/73))
- *(cli)* Schema-driven faucet init --source X --sink Y scaffolder ([#67](https://github.com/PawanSikawat/faucet-stream/pull/67))
- GCS source + sink connectors (Closes #26, #27) ([#66](https://github.com/PawanSikawat/faucet-stream/pull/66))
- Dead-letter queue support for sinks ([#65](https://github.com/PawanSikawat/faucet-stream/pull/65))
- [**breaking**] Observability — OTel-compatible tracing + Prometheus metrics (Closes #31) ([#63](https://github.com/PawanSikawat/faucet-stream/pull/63))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))
- *(cli)* Pipeline+matrix config and cwd auto-discovery ([#56](https://github.com/PawanSikawat/faucet-stream/pull/56))
- *(cli)* --from-env pipeline mode (Closes #42) ([#53](https://github.com/PawanSikawat/faucet-stream/pull/53))
- *(source-postgres-cdc)* PostgreSQL logical replication source ([#49](https://github.com/PawanSikawat/faucet-stream/pull/49))
- *(parquet)* Apache Parquet source + sink connectors ([#48](https://github.com/PawanSikawat/faucet-stream/pull/48))
- *(kafka)* Apache Kafka source + sink + common crate ([#46](https://github.com/PawanSikawat/faucet-stream/pull/46))
- *(cli)* Add 'faucet' config-driven pipeline runner binary ([#41](https://github.com/PawanSikawat/faucet-stream/pull/41))

### Other

- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 17 MEDIUM-tier fixes (closes out #78) ([#88](https://github.com/PawanSikawat/faucet-stream/pull/88))
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) ([#87](https://github.com/PawanSikawat/faucet-stream/pull/87))
- Pre-1.0 hardening: CRITICAL #9 + 10 fixes (#78 findings #9,#10,#14,#15,#16,#18,#20,#21,#22,#27,#28) ([#86](https://github.com/PawanSikawat/faucet-stream/pull/86))

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/
- *(core)* Remove unused SourceDAG executor (closes #62) ([#74](https://github.com/PawanSikawat/faucet-stream/pull/74))
- *(auth)* [**breaking**] Struct variants for newtype auth enums (Closes #40) ([#52](https://github.com/PawanSikawat/faucet-stream/pull/52))

### Testing

- *(cli)* De-flake on_error=stop abort test (part of #78 finding #24) ([#80](https://github.com/PawanSikawat/faucet-stream/pull/80))

## `faucet-sink-kafka` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-sink-kafka-v1.0.0) - 2026-06-01

### Documentation

- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- Faucet doctor — preflight probes for every connector ([#126](https://github.com/PawanSikawat/faucet-stream/pull/126)) ([#137](https://github.com/PawanSikawat/faucet-stream/pull/137))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))
- *(kafka)* Apache Kafka source + sink + common crate ([#46](https://github.com/PawanSikawat/faucet-stream/pull/46))

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: CRITICAL #9 + 10 fixes (#78 findings #9,#10,#14,#15,#16,#18,#20,#21,#22,#27,#28) ([#86](https://github.com/PawanSikawat/faucet-stream/pull/86))

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/

## `faucet-sink-elasticsearch` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-sink-elasticsearch-v1.0.0) - 2026-06-01

### Bug Fixes

- ES resume-dup surfacing + Redis fetch_all drain + fan-out Arc<Value> ([#160](https://github.com/PawanSikawat/faucet-stream/pull/160)) ([#166](https://github.com/PawanSikawat/faucet-stream/pull/166))

### Documentation

- Bump dependency version examples to 1.0 across READMEs and docs site
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- Faucet doctor — preflight probes for every connector ([#126](https://github.com/PawanSikawat/faucet-stream/pull/126)) ([#137](https://github.com/PawanSikawat/faucet-stream/pull/137))
- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- Faucet-elasticsearch-common shared auth crate (closes #43) ([#68](https://github.com/PawanSikawat/faucet-stream/pull/68))
- Dead-letter queue support for sinks ([#65](https://github.com/PawanSikawat/faucet-stream/pull/65))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 17 MEDIUM-tier fixes (closes out #78) ([#88](https://github.com/PawanSikawat/faucet-stream/pull/88))
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) ([#87](https://github.com/PawanSikawat/faucet-stream/pull/87))
- {{crate_name}} v{{version}}
- Add extensive README for every source, sink, core, and umbrella crate
- Add config_schema() to Source and Sink traits via schemars
- Add config loading from JSON files and env vars
- Add 18 new connectors: 9 sources + 9 sinks

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/
- *(auth)* [**breaking**] Struct variants for newtype auth enums (Closes #40) ([#52](https://github.com/PawanSikawat/faucet-stream/pull/52))

## `faucet-sink-gcs` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-sink-gcs-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* Quality-DLQ record_index, warn_mismatch cap, GCS-sink batch_size (missed in earlier PRs) (#165)

### Documentation

- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- Faucet doctor — preflight probes for every connector ([#126](https://github.com/PawanSikawat/faucet-stream/pull/126)) ([#137](https://github.com/PawanSikawat/faucet-stream/pull/137))
- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- Gzip/zstd compression for file connectors (closes #33) ([#76](https://github.com/PawanSikawat/faucet-stream/pull/76))
- GCS source + sink connectors (Closes #26, #27) ([#66](https://github.com/PawanSikawat/faucet-stream/pull/66))

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 17 MEDIUM-tier fixes (closes out #78) ([#88](https://github.com/PawanSikawat/faucet-stream/pull/88))

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/

## `faucet-sink-mssql` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-sink-mssql-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)
- *(sinks)* SQL sink correctness — AutoMap union, MySQL param-split, MSSQL dup-on-retry (H1/H14/H6, #146) ([#148](https://github.com/PawanSikawat/faucet-stream/pull/148))

### Features

- *(mssql)* Microsoft SQL Server source + sink connectors (#119, #120) ([#145](https://github.com/PawanSikawat/faucet-stream/pull/145))

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/

## `faucet-sink-snowflake` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-sink-snowflake-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)
- Resolve the 4 CRITICAL release-blockers from the #146 pre-1.0 audit (C1–C4) ([#147](https://github.com/PawanSikawat/faucet-stream/pull/147))
- Fix security issues, error semantics, and code quality across workspace

### Documentation

- Bump dependency version examples to 1.0 across READMEs and docs site
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- Faucet doctor — preflight probes for every connector ([#126](https://github.com/PawanSikawat/faucet-stream/pull/126)) ([#137](https://github.com/PawanSikawat/faucet-stream/pull/137))
- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- *(sources)* Add Snowflake + BigQuery query source connectors ([#75](https://github.com/PawanSikawat/faucet-stream/pull/75))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 17 MEDIUM-tier fixes (closes out #78) ([#88](https://github.com/PawanSikawat/faucet-stream/pull/88))
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) ([#87](https://github.com/PawanSikawat/faucet-stream/pull/87))
- Pre-1.0 hardening: CRITICAL batch 1 (#78 findings #3, #4, #5, #6, #7, #8, #11) ([#79](https://github.com/PawanSikawat/faucet-stream/pull/79))
- {{crate_name}} v{{version}}
- Add extensive README for every source, sink, core, and umbrella crate
- Add config_schema() to Source and Sink traits via schemars
- Add config loading from JSON files and env vars
- Extract shared utilities into faucet-core::util module
- Add 6 new connectors: GraphQL, XML, gRPC sources and Postgres, JSONL, Snowflake sinks

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/

## `faucet-sink-bigquery` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-sink-bigquery-v1.0.0) - 2026-06-01

### Bug Fixes

- Resolve the 4 CRITICAL release-blockers from the #146 pre-1.0 audit (C1–C4) ([#147](https://github.com/PawanSikawat/faucet-stream/pull/147))
- Fix security issues, error semantics, and code quality across workspace

### Documentation

- Bump dependency version examples to 1.0 across READMEs and docs site
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- Faucet doctor — preflight probes for every connector ([#126](https://github.com/PawanSikawat/faucet-stream/pull/126)) ([#137](https://github.com/PawanSikawat/faucet-stream/pull/137))
- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- *(sources)* Add Snowflake + BigQuery query source connectors ([#75](https://github.com/PawanSikawat/faucet-stream/pull/75))
- Dead-letter queue support for sinks ([#65](https://github.com/PawanSikawat/faucet-stream/pull/65))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 17 MEDIUM-tier fixes (closes out #78) ([#88](https://github.com/PawanSikawat/faucet-stream/pull/88))
- {{crate_name}} v{{version}}
- Add extensive README for every source, sink, core, and umbrella crate
- Add config_schema() to Source and Sink traits via schemars
- Add config loading from JSON files and env vars
- Restructure into multi-crate workspace with source/sink categories and add comprehensive test coverage

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/

## `faucet-source-snowflake` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-source-snowflake-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — serve/scheduler/observability + doctor probe + docs (#164)
- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)
- Resolve the 11 verified LOW items from the #146 hardening audit ([#159](https://github.com/PawanSikawat/faucet-stream/pull/159))

### Documentation

- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- *(sources)* Add Snowflake + BigQuery query source connectors ([#75](https://github.com/PawanSikawat/faucet-stream/pull/75))

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 17 MEDIUM-tier fixes (closes out #78) ([#88](https://github.com/PawanSikawat/faucet-stream/pull/88))
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) ([#87](https://github.com/PawanSikawat/faucet-stream/pull/87))
- Pre-1.0 hardening: CRITICAL #9 + 10 fixes (#78 findings #9,#10,#14,#15,#16,#18,#20,#21,#22,#27,#28) ([#86](https://github.com/PawanSikawat/faucet-stream/pull/86))

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/

## `faucet-source-bigquery` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-source-bigquery-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — serve/scheduler/observability + doctor probe + docs (#164)
- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)

### Documentation

- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- *(sources)* Add Snowflake + BigQuery query source connectors ([#75](https://github.com/PawanSikawat/faucet-stream/pull/75))

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 17 MEDIUM-tier fixes (closes out #78) ([#88](https://github.com/PawanSikawat/faucet-stream/pull/88))
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) ([#87](https://github.com/PawanSikawat/faucet-stream/pull/87))

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/

## `faucet-source-elasticsearch` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-source-elasticsearch-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)
- Resolve SQL injection, JSON corruption, and semaphore deadlock

### Documentation

- Bump dependency version examples to 1.0 across READMEs and docs site
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- Faucet-elasticsearch-common shared auth crate (closes #43) ([#68](https://github.com/PawanSikawat/faucet-stream/pull/68))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))
- *(sources)* Wire parent context into remaining 7 source connectors
- *(sources)* Migrate all source crates to fetch_with_context

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 17 MEDIUM-tier fixes (closes out #78) ([#88](https://github.com/PawanSikawat/faucet-stream/pull/88))
- {{crate_name}} v{{version}}
- Add extensive README for every source, sink, core, and umbrella crate
- Add config_schema() to Source and Sink traits via schemars
- Add config loading from JSON files and env vars
- Add 18 new connectors: 9 sources + 9 sinks

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/
- *(auth)* [**breaking**] Struct variants for newtype auth enums (Closes #40) ([#52](https://github.com/PawanSikawat/faucet-stream/pull/52))

## `faucet-source-gcs` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-source-gcs-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)

### Documentation

- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- Gzip/zstd compression for file connectors (closes #33) ([#76](https://github.com/PawanSikawat/faucet-stream/pull/76))
- GCS source + sink connectors (Closes #26, #27) ([#66](https://github.com/PawanSikawat/faucet-stream/pull/66))

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) ([#87](https://github.com/PawanSikawat/faucet-stream/pull/87))

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/

## `faucet-source-mssql` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-source-mssql-v1.0.0) - 2026-06-01

### Features

- *(mssql)* Microsoft SQL Server source + sink connectors (#119, #120) ([#145](https://github.com/PawanSikawat/faucet-stream/pull/145))

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/

## `faucet-source-mysql` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-source-mysql-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)
- Resolve SQL injection, JSON corruption, and semaphore deadlock

### Documentation

- Bump dependency version examples to 1.0 across READMEs and docs site
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))
- *(sources)* Wire parent context into REST, DB, GraphQL, and S3 sources
- *(sources)* Migrate all source crates to fetch_with_context

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) ([#87](https://github.com/PawanSikawat/faucet-stream/pull/87))
- {{crate_name}} v{{version}}
- Add extensive README for every source, sink, core, and umbrella crate
- Add config_schema() to Source and Sink traits via schemars
- Add config loading from JSON files and env vars
- Optimize all connectors for throughput
- Add 18 new connectors: 9 sources + 9 sinks

## `faucet-source-postgres` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-postgres-v0.2.0...faucet-source-postgres-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)
- *(streaming/auth)* 429 retry, gRPC reconnect reset, auth timeout, PG param binding (H3/H8/H11/H12, #146) ([#150](https://github.com/PawanSikawat/faucet-stream/pull/150))
- Resolve SQL injection, JSON corruption, and semaphore deadlock

### Documentation

- Bump dependency version examples to 1.0 across READMEs and docs site
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))
- *(sources)* Wire parent context into REST, DB, GraphQL, and S3 sources
- *(sources)* Migrate all source crates to fetch_with_context

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) ([#87](https://github.com/PawanSikawat/faucet-stream/pull/87))

## `faucet-source-kafka` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-source-kafka-v1.0.0) - 2026-06-01

### Bug Fixes

- Resolve the 11 verified LOW items from the #146 hardening audit ([#159](https://github.com/PawanSikawat/faucet-stream/pull/159))
- Resolve the final 3 HIGH release-blockers from #146 (H7/H9/H16) ([#158](https://github.com/PawanSikawat/faucet-stream/pull/158))
- *(source-kafka)* Seek to bookmark in rebalance, eliminate restart duplicate ([#50](https://github.com/PawanSikawat/faucet-stream/pull/50))

### Documentation

- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- Faucet doctor — preflight probes for every connector ([#126](https://github.com/PawanSikawat/faucet-stream/pull/126)) ([#137](https://github.com/PawanSikawat/faucet-stream/pull/137))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))
- *(kafka)* Apache Kafka source + sink + common crate ([#46](https://github.com/PawanSikawat/faucet-stream/pull/46))

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))

### Refactor

- Rename faucet-*-common crates to faucet-common-* under crates/common/

## `faucet-source-grpc` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-grpc-v0.2.0...faucet-source-grpc-v1.0.0) - 2026-06-01

### Bug Fixes

- Resolve the 11 verified LOW items from the #146 hardening audit ([#159](https://github.com/PawanSikawat/faucet-stream/pull/159))
- *(streaming/auth)* 429 retry, gRPC reconnect reset, auth timeout, PG param binding (H3/H8/H11/H12, #146) ([#150](https://github.com/PawanSikawat/faucet-stream/pull/150))
- Resolve SQL injection, JSON corruption, and semaphore deadlock

### Documentation

- Bump dependency version examples to 1.0 across READMEs and docs site
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- *(source-grpc)* Add server-streaming RPC support (closes #34) ([#77](https://github.com/PawanSikawat/faucet-stream/pull/77))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))
- *(sources)* Wire parent context into remaining 7 source connectors
- *(sources)* Migrate all source crates to fetch_with_context

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: 10 HIGH-tier fixes (part of #78) ([#87](https://github.com/PawanSikawat/faucet-stream/pull/87))

### Refactor

- *(auth)* [**breaking**] Struct variants for newtype auth enums (Closes #40) ([#52](https://github.com/PawanSikawat/faucet-stream/pull/52))

## `faucet-source-xml` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-xml-v0.2.0...faucet-source-xml-v1.0.0) - 2026-06-01

### Bug Fixes

- *(sources)* CSV/XML/REST data-loss + pagination reliability (H2/H4/H5/H13/H15, #146) ([#149](https://github.com/PawanSikawat/faucet-stream/pull/149))

### Documentation

- Bump dependency version examples to 1.0 across READMEs and docs site
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))

### Features

- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))
- *(sources)* Wire parent context into remaining 7 source connectors
- *(sources)* Migrate all source crates to fetch_with_context

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: CRITICAL #9 + 10 fixes (#78 findings #9,#10,#14,#15,#16,#18,#20,#21,#22,#27,#28) ([#86](https://github.com/PawanSikawat/faucet-stream/pull/86))

### Refactor

- *(auth)* [**breaking**] Struct variants for newtype auth enums (Closes #40) ([#52](https://github.com/PawanSikawat/faucet-stream/pull/52))

## `faucet-source-graphql` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-graphql-v0.2.0...faucet-source-graphql-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)

### Documentation

- Bump dependency version examples to 1.0 across READMEs and docs site
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))
- *(examples)* Exercise the full builder surface in every example

### Features

- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))
- *(sources)* Wire parent context into REST, DB, GraphQL, and S3 sources
- *(sources)* Migrate all source crates to fetch_with_context

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: CRITICAL #9 + 10 fixes (#78 findings #9,#10,#14,#15,#16,#18,#20,#21,#22,#27,#28) ([#86](https://github.com/PawanSikawat/faucet-stream/pull/86))

### Refactor

- *(auth)* [**breaking**] Struct variants for newtype auth enums (Closes #40) ([#52](https://github.com/PawanSikawat/faucet-stream/pull/52))

## `faucet-source-rest` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/compare/faucet-source-rest-v0.2.0...faucet-source-rest-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — core & connector correctness (retry/backoff + batch_size + connector bugs) (#163)
- *(sources)* REST 204, WS idle reset, Parquet up-front schema check, CDC slot retry (M9/M10/M11/M12, #146) ([#154](https://github.com/PawanSikawat/faucet-stream/pull/154))
- *(sources)* CSV/XML/REST data-loss + pagination reliability (H2/H4/H5/H13/H15, #146) ([#149](https://github.com/PawanSikawat/faucet-stream/pull/149))
- Add multi-parent DAG validation and move futures to workspace deps

### Documentation

- Bump dependency version examples to 1.0 across READMEs and docs site
- Harden docs.rs rendering across all crates (WS-1 of #91) ([#92](https://github.com/PawanSikawat/faucet-stream/pull/92))
- *(examples)* Add runnable examples for pipeline, streaming, and DAG

### Features

- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))
- *(transforms)* Richer transform model — filter (1→0) and explode (1→N) ([#111](https://github.com/PawanSikawat/faucet-stream/pull/111))
- Library transforms wrapper + layered config ([#101](https://github.com/PawanSikawat/faucet-stream/pull/101)) ([#110](https://github.com/PawanSikawat/faucet-stream/pull/110))
- *(transforms)* [**breaking**] Add 8 config-exposed transforms, replace snake_case with keys_case ([#107](https://github.com/PawanSikawat/faucet-stream/pull/107))
- [**breaking**] Observability — OTel-compatible tracing + Prometheus metrics (Closes #31) ([#63](https://github.com/PawanSikawat/faucet-stream/pull/63))
- *(core)* [**breaking**] Streaming Pipeline::run with Source::stream_pages contract ([#58](https://github.com/PawanSikawat/faucet-stream/pull/58))
- *(source-rest)* Wire state-store resume into RestStream
- *(sources)* Wire parent context into REST, DB, GraphQL, and S3 sources
- *(sources)* Migrate all source crates to fetch_with_context

### Other

- Crates.io keyword tuning + launch kit (WS-10 of #91) ([#102](https://github.com/PawanSikawat/faucet-stream/pull/102))
- Pre-1.0 hardening: 15 LOW-tier fixes (fully closes out #78) ([#89](https://github.com/PawanSikawat/faucet-stream/pull/89))
- Pre-1.0 hardening: CRITICAL #9 + 10 fixes (#78 findings #9,#10,#14,#15,#16,#18,#20,#21,#22,#27,#28) ([#86](https://github.com/PawanSikawat/faucet-stream/pull/86))
- Pre-1.0 hardening: CRITICAL batch 1 (#78 findings #3, #4, #5, #6, #7, #8, #11) ([#79](https://github.com/PawanSikawat/faucet-stream/pull/79))

### Refactor

- *(auth)* [**breaking**] Struct variants for newtype auth enums (Closes #40) ([#52](https://github.com/PawanSikawat/faucet-stream/pull/52))
- Replace local resolve_path with faucet_core::util::substitute_context

## `faucet-auth` — [1.0.0](https://github.com/PawanSikawat/faucet-stream/releases/tag/faucet-auth-v1.0.0) - 2026-06-01

### Bug Fixes

- *(#146)* LOW/NIT backlog — secrets & security hardening (#162)
- *(auth)* TokenEndpoint force-refresh + expiry_ratio validation (M15/M16, #146) ([#157](https://github.com/PawanSikawat/faucet-stream/pull/157))
- *(streaming/auth)* 429 retry, gRPC reconnect reset, auth timeout, PG param binding (H3/H8/H11/H12, #146) ([#150](https://github.com/PawanSikawat/faucet-stream/pull/150))

### Features

- *(auth)* [**breaking**] Consistent { type, config } auth shape + shared providers (auth: { ref }) ([#130](https://github.com/PawanSikawat/faucet-stream/pull/130))

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


