<div class="fs-hero">
  <img src="assets/wordmark.svg" alt="faucet-stream" />
  <p class="fs-tagline">The fast, config-driven way to <strong>move data in Rust</strong>.</p>
  <div class="fs-cta">
    <a class="primary" href="getting-started/installation.html">Get started →</a>
    <a class="secondary" href="getting-started/learn.html">Learn the architecture</a>
    <a class="secondary" href="https://github.com/PawanSikawat/faucet-stream">GitHub</a>
  </div>
</div>

faucet-stream wires **37 source** and **29 sink** connectors together with a single
`faucet` binary that runs pipelines declaratively from a YAML/JSON file — no Rust
code required. Or skip the binary and embed the same engine in your own service
through the typed `Source` / `Sink` traits.

```bash
cargo install faucet-cli
faucet init my_pipeline --source postgres --sink bigquery
faucet validate pipeline.yaml
faucet run pipeline.yaml
```

## Why faucet-stream

<div class="fs-cards">
  <div class="fs-card">
    <h3>Fast &amp; reliable by default</h3>
    <p>Native streaming with bounded memory, connection pooling, multi-row inserts, bulk APIs, and parallel I/O — performance is the reason the library exists.</p>
  </div>
  <div class="fs-card">
    <h3>Config-driven <em>or</em> embeddable</h3>
    <p>Run <code>faucet run pipeline.yaml</code>, or call <code>Pipeline::new(&amp;source, &amp;sink).run().await?</code> from Rust. Same engine either way.</p>
  </div>
  <div class="fs-card">
    <h3>A runtime, not just connectors</h3>
    <p>Incremental + resumable replication, change-data-capture, exactly-once delivery, dead-letter queues, retries, quality checks, and built-in metrics + tracing — with zero per-connector code.</p>
  </div>
  <div class="fs-card">
    <h3>Pay only for what you use</h3>
    <p>Every connector is a Cargo feature. Build a slim binary with just the source and sink you need.</p>
  </div>
</div>

## How this book is organized

- **[Getting Started](./getting-started/installation.md)** — install, run your
  first pipeline in five minutes, and (if you like) **[learn the whole
  architecture as a story](./getting-started/learn.md)**.
- **[Tutorials](./tutorials/rest-to-bigquery.md)** — end-to-end walkthroughs of
  real pipelines (incremental REST → BigQuery, Postgres CDC, DAGs, embedding).
- **[Cookbook](./cookbook/pagination.md)** — short, task-oriented recipes for
  pagination, auth, state, upserts, dead-letter queues, secrets, and more.
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
