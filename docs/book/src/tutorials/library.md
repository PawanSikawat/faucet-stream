# Embedding faucet as a Rust library

The `faucet` CLI is a thin wrapper over the same library you can use directly.
Embedding gives you typed configs, compile-time connector selection, and the
ability to build a `Source` or `Sink` from your own code.

## Add the dependency

```toml
[dependencies]
faucet-stream = { version = "0.2", features = ["source-rest", "sink-bigquery"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Build and run a pipeline

```rust,ignore
use faucet_stream::source::rest::{RestStream, RestStreamConfig, Auth, PaginationStyle};
use faucet_stream::sink::bigquery::{BigQuerySink, BigQuerySinkConfig};
use faucet_stream::Pipeline;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(RestStreamConfig {
        base_url: "https://api.example.com".into(),
        path: "/v1/events".into(),
        auth: Auth::Bearer { token: std::env::var("API_TOKEN")? },
        ..Default::default()
    })?;

    let sink = BigQuerySink::new(/* BigQuerySinkConfig { .. } */).await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("moved {} records", result.records_written);
    Ok(())
}
```

> Exact field names and constructors are documented per crate on
> [docs.rs](https://docs.rs/faucet-stream) (rendered with all features, so every
> connector's API is visible). Treat the snippet above as the shape, not the
> literal field list.

## Durable state and streaming

Wire a state store for resumable runs, and use the streaming entry point when you
want to control batching explicitly:

```rust,ignore
use std::sync::Arc;
use faucet_stream::{Pipeline, FileStateStore};

let state = Arc::new(FileStateStore::new("./state")?);
let result = Pipeline::new(&source, &sink)
    .with_state_store(state)
    .run()
    .await?;
```

The pipeline reads the bookmark before fetching and persists a new one only after
the sink confirms each page — so a crash never loses unwritten data.

## Why embed instead of shelling out to the CLI?

- **Typed configs** — config structs implement `serde` + `JsonSchema`, so you get
  compile-time checking and can generate UIs/forms from the schema.
- **Custom connectors** — implement the `Source` / `Sink` traits for systems we
  don't ship, and run them through the same `Pipeline`. See
  [authoring a connector](../extending/authoring-connectors.md).
- **One process** — no subprocess, no temp config files; integrate pipelines into
  an existing service, job runner, or test harness.
