<p align="center">
  <img src="https://raw.githubusercontent.com/PawanSikawat/faucet-stream/main/.github/assets/social-banner.png" alt="faucet-stream" width="640">
</p>

# faucet-stream

[![Crates.io](https://img.shields.io/crates/v/faucet-stream.svg)](https://crates.io/crates/faucet-stream)
[![Docs.rs](https://docs.rs/faucet-stream/badge.svg)](https://docs.rs/faucet-stream)
[![Guide](https://img.shields.io/badge/guide-pawansikawat.github.io-1f6feb)](https://pawansikawat.github.io/faucet-stream/)
[![CI](https://github.com/PawanSikawat/faucet-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/PawanSikawat/faucet-stream/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/PawanSikawat/faucet-stream/branch/main/graph/badge.svg)](https://codecov.io/gh/PawanSikawat/faucet-stream)
[![Downloads](https://img.shields.io/crates/d/faucet-stream.svg)](https://crates.io/crates/faucet-stream)
[![MSRV](https://img.shields.io/crates/msrv/faucet-stream.svg)](rust-toolchain.toml)
[![Dependencies](https://img.shields.io/badge/deps-cargo--deny-blue)](deny.toml)
[![License](https://img.shields.io/crates/l/faucet-stream.svg)](#license)
[![Changelog](https://img.shields.io/badge/changelog-keep%20a%20changelog-orange)](CHANGELOG.md)

**The fast, config-driven way to move data in Rust.**

faucet-stream is a complete **ETL** toolkit: **23 source** and **18 sink** connectors
(**41 in total**) plus in-flight transforms — including a page-level embedded-DuckDB `sql`
transform — wired together by a single `faucet` binary that runs pipelines declaratively
from a YAML/JSON file, no Rust code required. Or skip the binary and embed the same engine
in your own service through the typed `Source` / `Sink` traits. One toolkit, whether you
want a CLI you can drop on any box or a library you compile in.

A single native binary **and** an embeddable Rust library: config-driven pipelines with
no Python runtime, no platform to stand up, no daemon to babysit — plus a typed API when
you'd rather compile data movement straight into your own service.

```bash
cargo install faucet-cli          # the CLI
# — or —
cargo add faucet-stream           # the library
```

### Why faucet-stream

- **🚀 Built for throughput** — native streaming with bounded memory, connection
  pooling, multi-row inserts, bulk APIs, and parallel I/O. Throughput is a first-class
  design goal for every connector. (See [`BENCHMARKS.md`](BENCHMARKS.md) for a
  reproducible comparison — numbers, not adjectives.)
- **🧩 Config-driven _or_ embeddable** — run `faucet run pipeline.yaml`, or call
  `Pipeline::new(&source, &sink).run().await?` from Rust. Same orchestration either way.
- **⚙️ A runtime, not just connectors** — incremental + resumable replication, change-data-capture,
  effectively-once delivery (idempotent dedup-on-resume), upsert/delete write modes, data-quality checks, data contracts,
  freshness/volume SLA monitoring, dead-letter queues, automatic retries, adaptive batch sizing,
  secrets-manager interpolation,
  cron scheduling, an HTTP control plane with event-driven triggers, OpenLineage emission, and
  built-in Prometheus metrics + `tracing` spans — all with zero per-connector code.
- **📦 Pay only for what you use** — every connector is a Cargo feature, so a slim build can
  be just REST + JSONL, or pull in all 41 connectors with `--features full`.

**Documentation:** the [faucet-stream guide](https://pawansikawat.github.io/faucet-stream/)
(getting started, tutorials, cookbook, operations) · API reference on
[docs.rs](https://docs.rs/faucet-stream) · [`cli/README.md`](cli/README.md) for the full config grammar.

---

## Table of contents

- [Quickstart — the CLI](#quickstart--the-cli)
- [Quickstart — the library](#quickstart--the-library)
- [What's in the box](#whats-in-the-box)
- [Connectors](#connectors)
- [How it compares](#how-it-compares)
- [When to use faucet-stream](#when-to-use-faucet-stream)
- [Architecture](#architecture)
- [Performance](#performance)
- [Observability](#observability)
- [Feature flags](#feature-flags)
- [Using faucet-stream as a Rust library](#using-faucet-stream-as-a-rust-library)
- [Building custom connectors](#building-custom-connectors)
- [Project structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

---

## Quickstart — the CLI

> **Just want to poke at it?** From a clone, run `./scripts/try-local.sh` — it
> builds a light feature set, runs a no-infrastructure demo (transforms,
> quality, contracts, masking, lineage, catalog, DLQ replay) against generated
> data, then leaves the [web console](https://pawansikawat.github.io/faucet-stream/getting-started/try-it-locally.html)
> running so you can browse Runs, Datasets, and Lineage. No Docker or cloud
> accounts needed.

Move data without writing any Rust:

```bash
cargo install faucet-cli
faucet init my_pipeline --source postgres --sink bigquery   # scaffold pipeline.yaml from schemas
faucet validate pipeline.yaml                               # parse + resolve secrets, no run
faucet doctor pipeline.yaml                                 # preflight: probe auth/network/permissions
faucet test tests/*.yaml                                    # offline fixture tests for pipeline logic
faucet run pipeline.yaml                                    # one-shot run to completion
faucet schedule pipeline.yaml                               # run on a cron schedule (add a schedule: block)
faucet serve --no-auth                                      # HTTP control plane: submit/poll/cancel runs over REST
```

A minimal config — fetch open GitHub issues and write them to JSON Lines:

```yaml
# faucet.yaml — `faucet run` auto-discovers this file (and a sibling `.env`) in cwd
version: 1
pipeline:
  source:
    type: rest
    config:
      base_url: https://api.github.com
      path: /repos/PawanSikawat/faucet-stream/issues
      method: GET
      auth: { type: api_key, config: { header: Authorization, value: "Bearer ${env:GITHUB_TOKEN}" } }
      query_params: { state: open }
      pagination: { type: LinkHeader }
      max_retries: 3
  transforms:
    - type: keys_case          # re-case every key: snake / camel / pascal / kebab / screaming_snake
      config: { mode: snake }
  sink:
    type: jsonl
    config:
      path: ./out/issues.jsonl
```

Run many invocations from one config with a `matrix:` block (independent fan-out, a
parent/child DAG, **or** `depends_on:` completion ordering between rows), and bound
concurrency with `execution:`. See [`cli/README.md`](cli/README.md)
for the full grammar, [`cli/examples/rest_to_bigquery_matrix.yaml`](cli/examples/rest_to_bigquery_matrix.yaml)
for matrix fan-out, and [`cli/examples/rest_users_posts_dag.yaml`](cli/examples/rest_users_posts_dag.yaml)
for the DAG pattern. The [`cli/examples/`](cli/examples) directory has runnable configs for
every common source→sink combination.

## Quickstart — the library

Embed the same engine in a Rust service:

```bash
cargo add faucet-stream --features sink-jsonl   # default already has the REST source
cargo add tokio --features full
```

```rust
use faucet_stream::{Pipeline, RestStream, RestStreamConfig, PaginationStyle};
use faucet_stream::sink::jsonl::{JsonlSink, JsonlSinkConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/users")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.meta.next_cursor".into(),
                param_name: "cursor".into(),
            }),
    )?;
    let sink = JsonlSink::new(JsonlSinkConfig::new("./users.jsonl"));

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("Wrote {} records", result.records_written);
    Ok(())
}
```

More library recipes — pagination styles, OAuth2, streaming, incremental replication,
partitions, transforms, and custom connectors — are in
[Using faucet-stream as a Rust library](#using-faucet-stream-as-a-rust-library) below and the
[library tutorial](https://pawansikawat.github.io/faucet-stream/tutorials/library.html).

## What's in the box

faucet-stream is a full data-movement **runtime**, not just a bag of connectors. Every
capability below works across all connectors with zero per-connector code, and each is a
one-block addition to your YAML:

| Capability | What it does | Learn more |
|---|---|---|
| **Streaming, bounded memory** | Sources stream page-by-page; sinks write each page as it arrives — memory stays at one `batch_size` regardless of total volume. | [concepts](https://pawansikawat.github.io/faucet-stream/getting-started/concepts.html) |
| **Incremental + resumable** | Bookmark-based replication: only fetch what changed, resume mid-run from a durable state store (file / Redis / Postgres). | [state](https://pawansikawat.github.io/faucet-stream/cookbook/state.html) |
| **Change data capture** | Streaming row-level CDC for **PostgreSQL** (logical replication), **MySQL** (binlog), and **MongoDB** (change streams) — resumable. | [CDC guide](https://pawansikawat.github.io/faucet-stream/reference/connectors.html) |
| **Effectively-once delivery** | Monotonic per-page commit tokens committed atomically with the data (SQL sinks, Iceberg, BigQuery), so a resumed run re-delivers no duplicates. This is idempotent at-least-once (dedup on resume), not distributed-consensus exactly-once. | [state](https://pawansikawat.github.io/faucet-stream/cookbook/state.html) |
| **Upsert / delete write modes** | `write_mode: upsert \| delete` with a `key` + `delete_marker` — merge by key on Postgres / MySQL / SQL Server / SQLite / Mongo / Elasticsearch. | [upsert](https://pawansikawat.github.io/faucet-stream/cookbook/upsert.html) |
| **Data-quality checks** | 13 per-record and per-batch assertions (not-null, regex, ranges, uniqueness, row-count, JSON Schema, …) with quarantine routing or abort policies. | [quality](https://pawansikawat.github.io/faucet-stream/cookbook/quality.html) |
| **Data contracts** | A versioned promise about the output shape (types, nullability, enums, patterns, bounds) enforced per page — breaches fail, quarantine, or warn; export as JSON Schema / OpenLineage via `faucet contract`. | [contracts](https://pawansikawat.github.io/faucet-stream/cookbook/contracts.html) |
| **SLA monitoring** | Declared freshness (`max_staleness_secs`) + volume floors and learned-baseline anomaly detection (z-score / IQR) per pipeline — violations emit metrics + warnings and surface in `faucet doctor`, never failing the run. | [SLA](https://pawansikawat.github.io/faucet-stream/cookbook/sla.html) |
| **Dead-letter queue** | Route failed rows to any sink instead of aborting the run, with a fixed envelope and reason. | [DLQ](https://pawansikawat.github.io/faucet-stream/cookbook/dlq.html) |
| **Adaptive batch sizing** | Opt-in AIMD controller that tunes write batch size from observed sink latency and error rate. | [tuning](https://pawansikawat.github.io/faucet-stream/) |
| **Secrets-manager interpolation** | `${vault:…}`, `${aws-sm:…}`, `${gcp-sm:…}`, `${azure-kv:…}` resolved at load time, redacted from logs. | [secrets](https://pawansikawat.github.io/faucet-stream/cookbook/secrets.html) |
| **Cron scheduling** | `faucet schedule` — DST-correct cron, overlap policies, run timeouts, graceful drain. | [scheduling](https://pawansikawat.github.io/faucet-stream/) |
| **HTTP control plane** | `faucet serve` — submit / poll / cancel runs over REST, idempotency keys, run history, optional embedded web console (`serve-ui`), clustered execution. | [serve](https://pawansikawat.github.io/faucet-stream/) |
| **Event-driven triggers** | `faucet serve --triggers` — auto-enqueue runs on object-arrival (S3/GCS), webhook, or queue-depth (Redis/Kafka). | [triggers](https://pawansikawat.github.io/faucet-stream/reference/triggers.html) |
| **OpenLineage emission** | Emit START/RUNNING/COMPLETE/FAIL events with schema facets and column-level lineage over HTTP / file / Kafka. | [lineage](https://pawansikawat.github.io/faucet-stream/cookbook/lineage.html) |
| **Observability** | Automatic Prometheus metrics + `tracing` spans for every source, sink, transform, and state op — labelled by pipeline / row / connector. | [observability](cli/README.md#observability-prometheus--tracing) |
| **Transforms** | `flatten`, `rename_keys`, `keys_case`, `select`, `drop`, `set`, `rename_field`, `cast`, `redact`, `value_case`, `spell_symbols`, `cdc_unwrap`, and `sql` (embedded DuckDB, page-level). | [transforms](https://pawansikawat.github.io/faucet-stream/cookbook/transforms.html) |

## Connectors

All connector crates depend only on `faucet-core`, so any source pairs with any sink. See
the [connector capability matrix](https://pawansikawat.github.io/faucet-stream/reference/connectors.html)
(streaming, resumable state, compression, auth per connector) and the
[choosing-a-connector guide](https://pawansikawat.github.io/faucet-stream/reference/choosing.html)
for help picking between overlapping connectors (Postgres query vs CDC, S3 vs Parquet, Redis vs Kafka, …).

### Sources (23)

| Crate | Description |
|-------|-------------|
| [`faucet-source-rest`](crates/source/rest) | REST API — auth, pagination, extraction, schema inference |
| [`faucet-source-graphql`](crates/source/graphql) | GraphQL API — cursor-based pagination, variable injection |
| [`faucet-source-xml`](crates/source/xml) | XML/SOAP API — XML-to-JSON conversion, dot-path extraction |
| [`faucet-source-grpc`](crates/source/grpc) | gRPC — dynamic protobuf via `prost-reflect`, unary + server-streaming |
| [`faucet-source-postgres`](crates/source/postgres) | PostgreSQL — run SQL queries, return rows as JSON |
| [`faucet-source-postgres-cdc`](crates/source/postgres-cdc) | PostgreSQL CDC — logical replication via pgoutput, resumable |
| [`faucet-source-mysql`](crates/source/mysql) | MySQL — run SQL queries, return rows as JSON |
| [`faucet-source-mysql-cdc`](crates/source/mysql-cdc) | MySQL CDC — binlog row events, resumable via file/pos or GTID |
| [`faucet-source-mssql`](crates/source/mssql) | Microsoft SQL Server — streaming queries, incremental replication |
| [`faucet-source-sqlite`](crates/source/sqlite) | SQLite — run SQL queries, return rows as JSON |
| [`faucet-source-mongodb`](crates/source/mongodb) | MongoDB — find() with filter, projection, sort |
| [`faucet-source-mongodb-cdc`](crates/source/mongodb-cdc) | MongoDB CDC — Change Streams, resumable via resumeToken |
| [`faucet-source-redis`](crates/source/redis) | Redis — read from streams, lists, or key patterns |
| [`faucet-source-kafka`](crates/source/kafka) | Apache Kafka — consumer with idle/max-messages termination |
| [`faucet-source-s3`](crates/source/s3) | AWS S3 — read objects as JSONL, JSON array, or raw text |
| [`faucet-source-gcs`](crates/source/gcs) | Google Cloud Storage — read objects as JSONL, JSON array, or raw text |
| [`faucet-source-parquet`](crates/source/parquet) | Apache Parquet — local file, glob, or S3; vectorized Arrow reader, projection |
| [`faucet-source-elasticsearch`](crates/source/elasticsearch) | Elasticsearch — search/scroll API |
| [`faucet-source-bigquery`](crates/source/bigquery) | Google BigQuery — `jobs.query` + `getQueryResults`, type-aware decoding |
| [`faucet-source-snowflake`](crates/source/snowflake) | Snowflake — SQL REST API, server-side partition pagination, JWT / OAuth |
| [`faucet-source-webhook`](crates/source/webhook) | Webhook — temporary HTTP server collecting POST payloads |
| [`faucet-source-websocket`](crates/source/websocket) | WebSocket — live streaming feed; subscribe frames, reconnect, keepalive |
| [`faucet-source-csv`](crates/source/csv) | CSV — read CSV files as JSON objects |

### Sinks (18)

| Crate | Description |
|-------|-------------|
| [`faucet-sink-bigquery`](crates/sink/bigquery) | Google BigQuery — streaming inserts; effectively-once via MERGE |
| [`faucet-sink-iceberg`](crates/sink/iceberg) | Apache Iceberg — append snapshots via REST/Glue/SQL/HMS catalogs |
| [`faucet-sink-postgres`](crates/sink/postgres) | PostgreSQL — JSONB or auto-mapped columns; upsert/delete |
| [`faucet-sink-mysql`](crates/sink/mysql) | MySQL — JSON column or auto-mapped columns; upsert/delete |
| [`faucet-sink-mssql`](crates/sink/mssql) | Microsoft SQL Server — JSON or auto-mapped columns, 2100-param split |
| [`faucet-sink-sqlite`](crates/sink/sqlite) | SQLite — JSON column or auto-mapped columns; upsert/delete |
| [`faucet-sink-snowflake`](crates/sink/snowflake) | Snowflake — SQL REST API with JWT/OAuth |
| [`faucet-sink-mongodb`](crates/sink/mongodb) | MongoDB — insert_many; upsert/delete by key |
| [`faucet-sink-redis`](crates/sink/redis) | Redis — write to streams, lists, or key-value |
| [`faucet-sink-kafka`](crates/sink/kafka) | Apache Kafka — producer with batching, multi-topic routing |
| [`faucet-sink-elasticsearch`](crates/sink/elasticsearch) | Elasticsearch — bulk index API; upsert/delete by `_id` |
| [`faucet-sink-s3`](crates/sink/s3) | AWS S3 — write JSONL files to bucket |
| [`faucet-sink-gcs`](crates/sink/gcs) | Google Cloud Storage — write JSONL files to bucket |
| [`faucet-sink-parquet`](crates/sink/parquet) | Apache Parquet — local file or S3; schema inference, row/byte rollover |
| [`faucet-sink-jsonl`](crates/sink/jsonl) | JSON Lines — file output with append/truncate |
| [`faucet-sink-csv`](crates/sink/csv) | CSV — write JSON records as CSV rows |
| [`faucet-sink-http`](crates/sink/http) | HTTP — POST records to any endpoint |
| [`faucet-sink-stdout`](crates/sink/stdout) | Stdout/stderr — JSON Lines, pretty JSON, or TSV |

<details>
<summary><b>Supporting crates</b> — core, shared connector libraries, state stores, lineage, transforms, umbrella, CLI</summary>

| Crate | Description |
|-------|-------------|
| [`faucet-core`](crates/core) | Shared types, traits (`Source`, `Sink`, `AuthProvider`), pipeline orchestration, transforms, error types |
| [`faucet-auth`](crates/auth) | Shared single-flight auth providers (OAuth2, token-endpoint) for `auth: { ref }` |
| [`faucet-common-bigquery`](crates/common/bigquery) | Shared BigQuery types — `BigQueryCredentials` enum and `build_client` helper |
| [`faucet-common-elasticsearch`](crates/common/elasticsearch) | Shared `ElasticsearchAuth` enum for Elasticsearch source/sink |
| [`faucet-common-gcs`](crates/common/gcs) | Shared GCS types — credentials enum, Storage/StorageControl client builders |
| [`faucet-common-kafka`](crates/common/kafka) | Shared Kafka types — auth, value formats, Schema Registry client |
| [`faucet-common-snowflake`](crates/common/snowflake) | Shared Snowflake types — `SnowflakeAuth` enum + auth header helpers |
| [`faucet-common-mssql`](crates/common/mssql) | Shared MSSQL types — connection/TLS config, `tiberius`+`bb8` pool builder, identifier quoting |
| [`faucet-state-redis`](crates/state/redis) | Redis-backed `StateStore` for persistent bookmarks |
| [`faucet-state-postgres`](crates/state/postgres) | PostgreSQL-backed `StateStore` for persistent bookmarks |
| [`faucet-lineage`](crates/lineage) | OpenLineage event emission — HTTP/file/Kafka transports, schema facets, column-lineage analysis |
| [`faucet-transform-sql`](crates/transform-sql) | Embedded DuckDB SQL transform — run DuckDB SQL over each page (`batch` relation) |
| [`faucet-stream`](faucet-stream) | Umbrella crate — feature-gated re-exports of all connectors and state backends |
| [`faucet-cli`](cli) | `faucet` binary — YAML/JSON config-driven pipeline runner (`run`, `validate`, `schema`, `list`, `preview`, `init`, `doctor`, `test`, `schedule`, `serve`) |

</details>

### Install

```bash
# Everything (default includes the REST source)
cargo add faucet-stream

# All sources / all sinks / all connectors
cargo add faucet-stream --features source
cargo add faucet-stream --features sink
cargo add faucet-stream --features full

# Pick individual connectors
cargo add faucet-stream --features source-rest,sink-postgres,sink-s3

# Or depend on individual connector crates directly
cargo add faucet-source-rest
cargo add faucet-source-mongodb
```

## How it compares

There are many great data-movement tools. faucet-stream's niche is being **a single fast
native binary _and_ an embeddable Rust library** — config-driven, with no Python runtime, no
platform to operate, and a typed library API when you want to compile pipelines into your
own service.

| | **faucet-stream** | Meltano (Singer) | Airbyte | Benthos / Redpanda Connect | Vector | Fivetran |
|---|---|---|---|---|---|---|
| Runtime | Rust, native binary | Python | Java/Python on Docker | Go, native binary | Rust, native binary | Hosted SaaS |
| Single static binary | ✓ | ✗ | ✗ | ✓ | ✓ | n/a |
| Config-driven (YAML/JSON) | ✓ | ✓ | via UI/API | ✓ | ✓ | via UI |
| Embeddable as a library | ✓ (Rust) | ✗ | ✗ | ✓ (Go) | ✗ | ✗ |
| Connector count | 41, growing | 600+ taps | 350+ | dozens | dozens | 500+ |
| Change data capture | ✓ Postgres / MySQL / Mongo | partial¹ | ✓ | partial | ✗ | ✓ |
| Incremental + resumable state | ✓ | ✓ | ✓ | partial | n/a | ✓ |
| Effectively-once delivery³ | ✓ (SQL / Iceberg / BigQuery) | ✗ | partial | ✗ | ✗ | ✓ |
| Built-in data-quality checks | ✓ native | ✗ | paywalled add-on | ✗ | ✗ | paywalled add-on |
| Built-in metrics + tracing | ✓ Prometheus + `tracing` | partial | ✓ (platform) | ✓ | ✓ | ✓ (hosted) |
| Self-hosted, no daemon | ✓ run-to-completion | ✓ | ✗ needs platform | usually a service | agent | ✗ SaaS |
| License | MIT / Apache-2.0 | MIT | ELv2 + MIT | Apache-2.0 / source-available² | MPL-2.0 | Proprietary |

¹ Singer CDC depends on the individual tap. ² The original Benthos is Apache-2.0; Redpanda Connect's maintained build is source-available. ³ "Effectively-once" = idempotent at-least-once: per-page commit tokens are committed atomically with the data so a resumed run drops duplicates — not distributed-consensus exactly-once (see [delivery guarantees](https://pawansikawat.github.io/faucet-stream/cookbook/state.html)). *Comparison reflects the general shape of each tool as of 2026-05 — check each project for current details.*

For reference: **[Singer](https://www.singer.io/)** is a connector spec and **[Meltano](https://meltano.com/)**
is its most common runtime; both appear above. faucet-stream is a full **ETL** tool — it
transforms data *in flight* as it moves (13 record transforms plus a page-level
embedded-DuckDB `sql` transform for aggregation, joins, and filtering), not just
extract-and-load. **[dbt](https://www.getdbt.com/) is complementary, not a competitor:**
it models transformations *in the warehouse* on data already loaded (the "T" of ELT, at
warehouse scale) — pair the two when you need heavy in-warehouse modeling on top of what
faucet extracts, transforms, and loads.

## When to use faucet-stream

**Reach for it when:**

- You want **one fast static binary** (or a Rust library) to move data between APIs, databases, object stores, and warehouses — without standing up a platform, scheduler, or Python environment.
- You want **version-controlled, config-driven pipelines** you can run anywhere: locally, in CI, behind cron, or inside another service.
- You need **streaming with bounded memory, incremental/resumable replication, CDC, effectively-once delivery, data-quality assertions, retries, dead-letter queues, and metrics** without hand-writing that plumbing.
- You're **already in Rust** and want typed `Source`/`Sink` traits you can embed and extend.

**Look elsewhere (for now) when:**

- You need a connector faucet-stream **doesn't ship yet and can't write** — [Meltano](https://meltano.com/) (600+ Singer taps) and [Airbyte](https://airbyte.com/) (350+) have far broader catalogs today.
- You want a **fully-managed, hosted service** with a UI and a team operating it — Fivetran or Airbyte Cloud.
- Your job is **heavy in-warehouse transformation modeling** — a tested, version-controlled SQL DAG built on data already in the warehouse, at warehouse scale. That's dbt's domain; pair it with faucet-stream, which extracts, transforms in-flight, and loads.
- You need a **continuous record-by-record streaming processor** — [Benthos / Redpanda Connect](https://www.redpanda.com/connect) and [Vector](https://vector.dev/) are purpose-built for that. faucet-stream runs discrete pipelines to completion; even the long-running modes (`faucet schedule`, `faucet serve`) orchestrate complete runs rather than a never-ending stream.

## Architecture

A `Source` streams batches of records, optional `Transform`s reshape them, and the
`Pipeline` writes each batch to a `Sink` — bounding memory at one batch on both sides
regardless of total volume. The pipeline also drives the cross-cutting runtime (bookmarks,
dead-letter routing, quality checks, metrics) so connectors stay simple:

```mermaid
flowchart LR
    S["<b>Source</b><br/>REST · DB · CDC<br/>Kafka · S3 · Parquet"]
    T["<b>Transforms</b><br/>flatten · rename · keys_case<br/>select · drop · set · cast<br/>redact · value_case · spell_symbols<br/>sql (DuckDB, page-level)"]
    P{{"<b>Pipeline</b>"}}
    K["<b>Sink</b><br/>BigQuery · Postgres<br/>Parquet · Kafka · ..."]
    ST[("State store<br/>file · Redis · Postgres")]
    D[("Dead-letter<br/>queue")]
    O(["Prometheus<br/>+ tracing"])

    S -->|StreamPage batches| T --> P -->|write_batch| K
    P -.->|bookmark per page| ST
    ST -.->|resume from bookmark| S
    P -.->|failed rows| D
    P -.->|metrics + spans| O
```

faucet-stream is a Cargo workspace with **55 crates** — 23 sources, 18 sinks, 6 shared
connector libraries, the shared auth-provider library, 2 state-store backends, the lineage
crate, the SQL transform crate, the shared core, the umbrella crate, and the CLI binary. See
the [Connectors](#connectors) table above and the
[architecture guide](https://pawansikawat.github.io/faucet-stream/getting-started/concepts.html).

## Performance

> **Performance and reliability are why this library exists.** Every connector is optimised
> for throughput out of the box — there are no "slow defaults" to tune away.

| Technique | Where |
|-----------|-------|
| **Parallel I/O** | S3/GCS read/write objects concurrently (configurable `concurrency`); HTTP sink sends requests in parallel; REST source processes partitions concurrently |
| **Multi-row INSERT** | PostgreSQL, MySQL, SQLite, and SQL Server sinks batch records into single INSERT statements instead of one per row (MSSQL auto-splits at the 2100-parameter limit) |
| **Transaction wrapping** | SQLite sink wraps batches in `BEGIN`/`COMMIT` for a large write speedup |
| **Connection pooling** | All database connectors use connection pools with configurable `max_connections` |
| **Connection reuse** | S3, MongoDB, Redis, Elasticsearch, and HTTP connectors create clients once and reuse them across all operations |
| **Redis pipelining** | Redis sink batches commands with `pipe()`; Redis source uses `MGET` for bulk key reads |
| **Bulk APIs** | Elasticsearch uses the bulk NDJSON API; BigQuery uses `insertAll`; MongoDB uses `insert_many` |
| **Buffered I/O** | JSONL sink uses `BufWriter`; CSV uses buffered readers/writers in blocking threads |
| **Streaming pagination** | Sources stream pages one at a time via `stream_pages()` to bound memory |
| **Adaptive batch sizing** | Opt-in AIMD controller tunes the effective write batch size from observed sink latency and error rate |

## Observability

Every pipeline emits OTel-compatible `tracing` spans and Prometheus metrics automatically —
labelled by `pipeline`, `row` (matrix row id), and `connector`, with **zero per-connector
code**. The CLI exposes a `/metrics` endpoint via the optional `observability:` block in
`faucet.yaml`. See the [CLI README](cli/README.md#observability-prometheus--tracing) for the
YAML grammar and the OpenTelemetry bridge snippet.

## Feature flags

Default features: `source-rest`, `transform-flatten`, `transform-rename-keys`,
`transform-keys-case`. Every connector and capability is an opt-in Cargo feature.

<details>
<summary><b>Full feature-flag table (umbrella crate)</b></summary>

| Feature | Default | Description |
|---------|---------|-------------|
| `source-rest` | yes | REST API source |
| `source-graphql` | no | GraphQL API source |
| `source-xml` | no | XML/SOAP API source |
| `source-grpc` | no | gRPC source |
| `source-postgres` | no | PostgreSQL query source |
| `source-postgres-cdc` | no | PostgreSQL CDC source (logical replication) |
| `source-mysql` | no | MySQL query source |
| `source-mysql-cdc` | no | MySQL CDC source (binlog replication) |
| `source-mssql` | no | Microsoft SQL Server query source |
| `source-sqlite` | no | SQLite query source |
| `source-mongodb` | no | MongoDB query source |
| `source-mongodb-cdc` | no | MongoDB CDC source (Change Streams) |
| `source-redis` | no | Redis source |
| `source-kafka` | no | Apache Kafka consumer source |
| `source-s3` | no | AWS S3 file source |
| `source-gcs` | no | Google Cloud Storage file source |
| `source-parquet` | no | Apache Parquet file source (local, glob, S3) |
| `source-elasticsearch` | no | Elasticsearch source |
| `source-bigquery` | no | Google BigQuery query source |
| `source-snowflake` | no | Snowflake query source |
| `source-webhook` | no | Webhook HTTP receiver |
| `source-websocket` | no | WebSocket live streaming source |
| `source-csv` | no | CSV file source |
| `sink-bigquery` | no | Google BigQuery sink |
| `sink-iceberg` | no | Apache Iceberg sink (append, REST/Glue/SQL/HMS catalogs) |
| `sink-postgres` | no | PostgreSQL sink |
| `sink-mysql` | no | MySQL sink |
| `sink-mssql` | no | Microsoft SQL Server sink |
| `sink-sqlite` | no | SQLite sink |
| `sink-snowflake` | no | Snowflake sink |
| `sink-mongodb` | no | MongoDB sink |
| `sink-redis` | no | Redis sink |
| `sink-kafka` | no | Apache Kafka producer sink |
| `sink-elasticsearch` | no | Elasticsearch bulk index sink |
| `sink-s3` | no | AWS S3 file sink |
| `sink-gcs` | no | Google Cloud Storage file sink |
| `sink-parquet` | no | Apache Parquet file sink (local, S3) |
| `sink-jsonl` | no | JSON Lines file sink |
| `sink-csv` | no | CSV file sink |
| `sink-http` | no | HTTP POST sink |
| `sink-stdout` | no | Stdout/stderr sink (JSON Lines, pretty JSON, TSV) |
| `kafka-schema-registry` | no | Confluent Schema Registry support for the Kafka pair (Avro, Protobuf, JSON Schema) |
| `state-redis` | no | Redis-backed `StateStore` backend |
| `state-postgres` | no | PostgreSQL-backed `StateStore` backend |
| `source` / `sink` / `state` | no | All sources / all sinks / all state backends |
| `auth` | no | Shared OAuth2 / token-endpoint auth providers |
| `full` | no | Every connector, state backend, and capability |
| `transform-flatten` | yes | Flatten nested objects |
| `transform-rename-keys` | yes | Regex key renaming |
| `transform-keys-case` | yes | Re-case every key — snake / camel / pascal / kebab / screaming_snake |
| `transform-select` / `transform-drop` / `transform-set` | no | Keep / remove / add top-level fields |
| `transform-rename-field` | no | Exact-name field rename (single or batch) |
| `transform-cast` | no | Per-field type coercion with `on_error` policy |
| `transform-redact` | no | Replace listed field values with a mask |
| `transform-value-case` | no | Lowercase / uppercase / trim string field values |
| `transform-spell-symbols` | no | Spell out symbols in keys (`%` → `percent`, `#` → `number`, …) |
| `transform-cdc-unwrap` | no | Normalize a CDC envelope into a flat row + `__op` marker (pairs with upsert sinks) |
| `transforms` | no | All built-in transforms above |
| `transform-sql` | no | Embedded DuckDB SQL transform — run DuckDB SQL over each page (`batch` relation; `batch_size: 0` for global aggregation) |
| `compression` | no | gzip / zstd read+write on JSONL/CSV/S3/GCS source and sink connectors |

`RecordTransform::Custom` is always available regardless of feature flags. CLI-only features
(`schedule`, `serve`, `serve-ui`, `serve-history-*`, `triggers*`, `lineage`, `quality`,
`contract`, `secrets-*`) live in `faucet-cli`, not the umbrella crate — see
[`cli/README.md`](cli/README.md).

</details>

## Using faucet-stream as a Rust library

The CLI is just one consumer of the engine. Everything it does is available as a typed API.

<details>
<summary><b>Pagination styles</b> — cursor, page-number, offset, Link header, next-link-in-body</summary>

```rust
use faucet_stream::{RestStream, RestStreamConfig, Auth, PaginationStyle};

// Cursor-based pagination with Bearer auth
let stream = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v1/users")
        .auth(Auth::Bearer { token: "my-token".into() })
        .records_path("$.data[*]")
        .pagination(PaginationStyle::Cursor {
            next_token_path: "$.meta.next_cursor".into(),
            param_name: "cursor".into(),
        })
        .max_pages(50),
)?;
let users: Vec<serde_json::Value> = stream.fetch_all().await?;

// Page-number pagination with an API key
let stream = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/v2/orders")
        .auth(Auth::ApiKey { header: "X-Api-Key".into(), value: "secret".into() })
        .records_path("$.results[*]")
        .pagination(PaginationStyle::PageNumber {
            param_name: "page".into(),
            start_page: 1,
            page_size: Some(100),
            page_size_param: Some("per_page".into()),
        }),
)?;
```

| Style | Use when |
|-------|----------|
| `Cursor` | API returns a next-page token in the response body |
| `PageNumber` | API uses `?page=1&per_page=100` style |
| `Offset` | API uses `?offset=0&limit=50` style |
| `LinkHeader` | API returns pagination in the `Link` HTTP header (GitHub-style) |
| `NextLinkInBody` | API returns the full next-page URL in the response body |

Every pagination style has a termination/loop guard. `Cursor`, `LinkHeader`, and
`NextLinkInBody` stop when the same token/link repeats; `PageNumber` stops on a zero-record
page or a repeated page body (content-fingerprint detection); `Offset` stops when the offset
reaches `total` or a page returns fewer records than the limit. `max_pages` is a hard cap
across all styles.

</details>

<details>
<summary><b>Authentication</b> — Bearer, Basic, API key, OAuth2, token endpoint, custom</summary>

| Method | Description |
|--------|-------------|
| `Bearer` | `Authorization: Bearer <token>` header |
| `Basic` | `Authorization: Basic <base64>` header |
| `ApiKey` | Custom header (e.g. `X-Api-Key: secret`) |
| `ApiKeyQuery` | API key as a query parameter (e.g. `?api_key=secret`) |
| `OAuth2` | Client-credentials flow with automatic token caching and refresh |
| `TokenEndpoint` | Fetch a token from any HTTP API via JSONPath, with caching and refresh |
| `Custom` | Arbitrary headers |

```rust
use faucet_stream::{Auth, fetch_oauth2_token};

// OAuth2 client credentials
let token = fetch_oauth2_token(
    "https://auth.example.com/oauth/token",
    "client-id",
    "client-secret",
    &["read:data".into()],
).await?;
let config = RestStreamConfig::new("https://api.example.com", "/data")
    .auth(Auth::Bearer { token });
```

`Auth::TokenEndpoint` fetches a token from an external API (a login endpoint, secrets
manager, or custom auth service) via a JSONPath, caches it across pages, and refreshes it at
`expiry_ratio` of the reported lifetime (default 90%). See the
[auth cookbook](https://pawansikawat.github.io/faucet-stream/cookbook/auth.html) for the full
shape and `ResponseValidator` customization.

</details>

<details>
<summary><b>Streaming, incremental replication, partitions, and typed deserialization</b></summary>

```rust
use faucet_stream::{RestStream, RestStreamConfig, ReplicationMethod, PaginationStyle};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::json;

// Stream page-by-page (bounded memory)
let mut pages = stream.stream_pages();
while let Some(result) = pages.next().await {
    let records = result?;
    println!("processing page of {} records", records.len());
}

// Incremental replication — only fetch records newer than a stored bookmark
let stream = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/events")
        .records_path("$.data[*]")
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .start_replication_value(json!("2024-06-01T00:00:00Z")),
)?;
let (records, bookmark) = stream.fetch_all_incremental().await?;  // persist `bookmark` for next run

// Typed deserialization straight into your structs
#[derive(Debug, Deserialize)]
struct User { id: u64, name: String, email: String }
let users: Vec<User> = stream.fetch_all_as::<User>().await?;
```

`add_partition(...)` runs the same stream config across multiple contexts (e.g. per-org,
per-repo) and concatenates the results.

</details>

<details>
<summary><b>Transforms</b> — reshape records as they're extracted</summary>

Wrap any `Source` with `TransformingSource`. Built-in transforms are feature-gated (the
three case/flatten/rename ones are on by default):

```rust
use faucet_stream::{
    KeyCaseMode, Labels, RecordTransform, RestStream, RestStreamConfig, Source, TransformingSource,
};

let inner = RestStream::new(
    RestStreamConfig::new("https://api.example.com", "/data").records_path("$.results[*]"),
)?;
let stream = TransformingSource::new(
    Box::new(inner) as Box<dyn Source>,
    vec![
        RecordTransform::Flatten { separator: "__".into() },         // {"user":{"id":1}} -> {"user__id":1}
        RecordTransform::KeysCase { mode: KeyCaseMode::Snake },        // re-case every key
        RecordTransform::RenameKeys { pattern: r"^_sdc_".into(), replacement: "".into() },
        RecordTransform::custom(|mut record| {                         // arbitrary closure
            if let serde_json::Value::Object(ref mut map) = record {
                map.insert("_source".to_string(), serde_json::json!("my-api"));
            }
            record
        }),
    ],
    Labels::for_named("rest"),
)?;
```

</details>

<details>
<summary><b>Config loading and schema introspection</b></summary>

All connector configs load from JSON files, environment variables, or `.env` files, and can
describe themselves:

```rust
use faucet_core::config::{load_json, load_env, load_env_file};
use faucet_source_rest::RestStreamConfig;

let source: RestStreamConfig = load_json("source_config.json")?;       // from a JSON file
let source: RestStreamConfig = load_env("REST")?;                       // from REST_* env vars
let source: RestStreamConfig = load_env_file(".env", "REST")?;          // from a .env file

// Every source/sink can print a full JSON Schema of its config — always in sync with the code
let schema = RestStream::new(source)?.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

</details>

## Building custom connectors

faucet-stream is a **marketplace ecosystem** — third-party developers can publish their own
`faucet-source-*` / `faucet-sink-*` crates with minimal friction. The only dependency you
need is `faucet-core`; it re-exports everything required (`async_trait`, `serde_json`,
`Value`, `json!`, `JsonSchema`, `schema_for!`).

```rust
use faucet_core::{async_trait, FaucetError, Source, Sink, Value, json, JsonSchema, schema_for};

#[async_trait]
impl Source for MySource {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        Ok(vec![json!({"id": 1, "name": "example"})])
    }
    fn config_schema(&self) -> Value {
        serde_json::to_value(schema_for!(MySourceConfig)).expect("schema serialization")
    }
}

#[async_trait]
impl Sink for MySink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }
    fn config_schema(&self) -> Value {
        serde_json::to_value(schema_for!(MySinkConfig)).expect("schema serialization")
    }
}
```

Any `Source` works with any `Sink` via `Pipeline::new(&source, &sink).run().await?`. Map your
own failures to `FaucetError` variants (`Source` / `Sink` / `Config`, or `Custom(boxed_err)`
to wrap any `std::error::Error` without losing the chain). Publish under the naming
convention `faucet-source-<name>` / `faucet-sink-<name>`. Full walkthrough:
[authoring connectors](https://pawansikawat.github.io/faucet-stream/extending/authoring-connectors.html).

## Project structure

```
Cargo.toml                    — workspace manifest (55 crates)
crates/
  core/                       — faucet-core: shared types, traits, pipeline, transforms, config
  auth/                       — faucet-auth: shared OAuth2 / token-endpoint providers
  source/                     — 23 source connectors (rest, graphql, xml, grpc, *-cdc, kafka, s3, …)
  sink/                       — 18 sink connectors (bigquery, iceberg, postgres, parquet, kafka, …)
  common/                     — 6 shared connector libraries (bigquery, elasticsearch, gcs, kafka, snowflake, mssql)
  state/                      — Redis- and Postgres-backed StateStore backends
  lineage/                    — faucet-lineage: OpenLineage event emission
  transform-sql/              — faucet-transform-sql: embedded DuckDB SQL transform
faucet-stream/                — umbrella crate with feature-gated re-exports
cli/                          — faucet-cli: `faucet` binary, YAML/JSON pipeline runner
  examples/                   — ready-to-run pipeline YAMLs
  tests/                      — assert_cmd + wiremock integration tests
examples/                     — repo-level examples: docker-compose infra stack + run index
scripts/                      — helper scripts (try-local.sh interactive demo, cleanup-artifacts.sh)
docs/book/                    — mdBook documentation site (source under docs/book/src)
.github/workflows/            — ci.yml, release-plz.yml, docs.yml (mdBook → GitHub Pages)
.github/assets/               — brand assets: logo, wordmark, social-preview banner, favicon
```

## Contributing

Contributions — core changes and third-party connectors alike — are welcome. See
[CONTRIBUTING.md](CONTRIBUTING.md) for setup, the checks CI runs, and the add-a-connector
checklist, and the
[authoring guide](https://pawansikawat.github.io/faucet-stream/extending/authoring-connectors.html)
for building your own `faucet-source-*` / `faucet-sink-*` crate. Please review our
[Code of Conduct](CODE_OF_CONDUCT.md). To report a vulnerability, see [SECURITY.md](SECURITY.md).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above,
without any additional terms or conditions.
