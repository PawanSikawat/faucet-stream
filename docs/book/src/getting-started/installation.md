# Installation

## The `faucet` CLI

The CLI is the fastest way to start. Install it from crates.io:

```bash
cargo install faucet-cli
```

This gives you a `faucet` binary with **every** first-party connector compiled in,
so it can run any of the published example configs out of the box.

### Slim builds

Every connector is a Cargo feature, so you can build a smaller binary with only
what you need:

```bash
cargo install faucet-cli --no-default-features \
  --features "source-rest,sink-jsonl,sink-stdout,transforms"
```

Run `faucet list` to see which sources and sinks are compiled into your binary.

## The library

To embed pipelines in your own Rust program, depend on the umbrella crate and
enable the connectors you need:

```toml
[dependencies]
# Default features include the REST source only.
faucet-stream = "0.2"

# Or enable specific connectors:
faucet-stream = { version = "0.2", features = ["source-rest", "sink-postgres", "sink-s3"] }

# Or everything:
faucet-stream = { version = "0.2", features = ["full"] }
```

Feature groups: `source` (all sources), `sink` (all sinks), `state` (all
state-store backends), `full` (everything), and `compression` (gzip/zstd on the
file-shaped connectors you've enabled).

You can also depend on individual connector crates directly
(`faucet-source-rest`, `faucet-sink-bigquery`, …) — each depends only on
`faucet-core`.

## Requirements

- A recent stable Rust toolchain (see the repo's `rust-toolchain.toml` for the
  current MSRV).
- Some connectors link native libraries — the Kafka connectors build
  `librdkafka` and need `cmake` and a C toolchain available at compile time.

Next: [run your first pipeline](./first-pipeline.md).
