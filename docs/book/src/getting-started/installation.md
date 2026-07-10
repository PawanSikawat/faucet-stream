# Installation

## The `faucet` CLI

### Prebuilt binaries (no Rust required)

Every `faucet-cli` release ships prebuilt binaries for macOS (Apple Silicon +
Intel) and Linux (x86_64 + aarch64), so you don't need a Rust toolchain to try
it.

**Homebrew (macOS / Linux):**

```bash
brew install PawanSikawat/faucet-stream/faucet-cli
```

(The formula is named after the `faucet-cli` package; it installs the `faucet`
binary.)

**Shell installer (macOS / Linux):**

```bash
curl -LsSf https://github.com/PawanSikawat/faucet-stream/releases/latest/download/faucet-cli-installer.sh | sh
```

**Direct download:** grab the archive for your platform from the latest
[`faucet-cli` GitHub Release](https://github.com/PawanSikawat/faucet-stream/releases?q=faucet-cli&expanded=true)
(e.g. `faucet-cli-aarch64-apple-darwin.tar.xz`), verify it against the
published `.sha256` checksum, and put `faucet` on your `PATH`.

The prebuilt binary includes the CLI **default** feature set (every first-party
connector, transforms, quality checks, contracts, masking, compression) plus
`serve` (with the embedded web console), `schedule`, and `lineage`. Not
included — build from source for these: `transform-sql` (embedded DuckDB),
`otel`, `triggers`, `catalog`, and the `serve-history-*` backends.

> **macOS Gatekeeper:** the binaries are not currently notarized. If macOS
> blocks the downloaded binary, clear the quarantine attribute:
> `xattr -d com.apple.quarantine $(which faucet)`. Homebrew installs are not
> affected.

### From source (crates.io)

For the full feature set, or any custom combination, install from crates.io:

```bash
cargo install faucet-cli                     # the default feature set
cargo install faucet-cli --features full     # everything (DuckDB, otel, triggers, …)
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
faucet-stream = "1.0"

# Or enable specific connectors:
faucet-stream = { version = "1.0", features = ["source-rest", "sink-postgres", "sink-s3"] }

# Or everything:
faucet-stream = { version = "1.0", features = ["full"] }
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
