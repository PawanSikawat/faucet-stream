# Moving data in Rust shouldn't require a platform — introducing faucet-stream

*Draft launch post. Edit voice/details before publishing, then post to your blog / dev.to / Medium and link it from the Show HN and Reddit threads (see `README.md` in this folder).*

---

Every data team eventually hits the same wall: you need to move records from an
API into a warehouse, from Postgres into Kafka, from S3 into Elasticsearch — and
your options are a Python framework with a plugin runtime to operate, a hosted
SaaS with per-row pricing, or a pile of one-off scripts that rot.

**faucet-stream** is a different answer: a single fast Rust binary (and an
embeddable library) that runs data pipelines from a YAML file.

```bash
cargo install faucet-cli
faucet run pipeline.yaml
```

```yaml
version: 1
pipeline:
  source: { type: postgres, config: { connection_url: "${env:PG_URL}", query: "select * from events" } }
  transforms: [{ type: keys_case, config: { mode: snake } }]
  sink: { type: bigquery, config: { project_id: my-proj, dataset_id: analytics, table_id: events } }
```

No daemon, no scheduler, no Python environment. Drop the binary on a box, point
it at a config, run it on cron. Or `cargo add faucet-stream` and build the same
pipeline from Rust with typed `Source`/`Sink` traits.

## What's in the box

- **49 connectors** across REST, GraphQL, gRPC, Kafka, Postgres/MySQL/SQLite,
  Postgres CDC, S3/GCS, Parquet, MongoDB, Redis, Elasticsearch, BigQuery, and
  Snowflake.
- **A real runtime, not just connectors:** native streaming with bounded memory,
  incremental + resumable replication, change-data-capture, dead-letter queues,
  automatic retries, and built-in Prometheus metrics + `tracing` spans — with
  zero per-connector code.
- **Config-driven *or* embeddable:** the CLI and the library are the same engine.
- **Pay only for what you compile:** every connector is a Cargo feature.

## Why Rust

Performance and reliability are the whole point. Every connector reuses
connections, pools, batches into multi-row inserts and bulk APIs, and streams
with bounded memory — so a pipeline is the fastest way to move its data in Rust,
not a thin wrapper that falls over at scale. And because it's a single static
binary, "deploying" is copying a file.

## Where it fits (and where it doesn't)

faucet-stream is for **moving** data — the EL of ELT. If you need a connector we
don't ship yet, mature ecosystems like Meltano (600+ Singer taps) or Airbyte
still have far broader catalogs today, and we're honest about that in the README.
If your job is in-warehouse transformation, that's dbt's job — pair them. And if
you need a long-running streaming *service*, Benthos/Redpanda Connect or Vector
are purpose-built for that; faucet runs pipelines to completion.

But if you want one fast, trustworthy binary to move data between the systems you
already run — without standing up a platform — that's exactly what we built.

## Try it

- Docs: <https://pawansikawat.github.io/faucet-stream/>
- Source: <https://github.com/PawanSikawat/faucet-stream>
- Crates: <https://crates.io/crates/faucet-stream>

It's pre-1.0 and moving fast. Connector requests and contributions welcome —
there's a [contributing guide](https://github.com/PawanSikawat/faucet-stream/blob/main/CONTRIBUTING.md)
and an authoring guide for building your own connector crates.
