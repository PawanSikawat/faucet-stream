# faucet-stream

**The fast, config-driven way to move data in Rust.**

faucet-stream wires **19 source** and **16 sink** connectors together with a single
`faucet` binary that runs pipelines declaratively from a YAML/JSON file — no Rust
code required. Or skip the binary and embed the same engine in your own service
through the typed `Source` / `Sink` traits.

```bash
cargo install faucet-cli
faucet init my_pipeline --source postgres --sink bigquery
faucet validate pipeline.yaml
faucet run pipeline.yaml
```

## What you get

- **Fast and reliable by default** — native streaming with bounded memory,
  connection pooling, multi-row inserts, bulk APIs, and parallel I/O.
- **Config-driven _or_ embeddable** — run `faucet run pipeline.yaml`, or call
  `Pipeline::new(&source, &sink).run().await?` from Rust.
- **A runtime, not just connectors** — incremental + resumable replication,
  PostgreSQL change-data-capture, dead-letter queues, automatic retries, and
  built-in Prometheus metrics + `tracing` spans, all with zero per-connector code.
- **Pay only for what you use** — every connector is a Cargo feature.

## How this book is organized

- **[Getting Started](./getting-started/installation.md)** — install, run your
  first pipeline in five minutes, and learn the core concepts.
- **[Tutorials](./tutorials/rest-to-bigquery.md)** — end-to-end walkthroughs of
  real pipelines (incremental REST → BigQuery, Postgres CDC, DAGs, embedding).
- **[Cookbook](./cookbook/pagination.md)** — short, task-oriented recipes for
  pagination, auth, state, dead-letter queues, and compression.
- **[Reference](./reference/connectors.md)** — the connector catalog, CLI
  commands, and config-file grammar.
- **[Operations](./operations/deploying.md)** — deploying, observability,
  performance tuning, and troubleshooting.
- **[Extending](./extending/authoring-connectors.md)** — author and publish your
  own `faucet-source-*` / `faucet-sink-*` crate.

## Where else to look

- **API docs:** every crate is on [docs.rs](https://docs.rs/faucet-stream),
  rendered with all features so optional connectors are visible.
- **Source & issues:** [github.com/PawanSikawat/faucet-stream](https://github.com/PawanSikawat/faucet-stream).
- **Runnable examples:** the [`cli/examples/`](https://github.com/PawanSikawat/faucet-stream/tree/main/cli/examples)
  directory ships a config for nearly every connector pair, and
  [`examples/`](https://github.com/PawanSikawat/faucet-stream/tree/main/examples)
  has a `docker-compose` stack so they run locally.
