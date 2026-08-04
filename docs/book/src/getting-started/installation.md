# Installation

## The `faucet` CLI

### Prebuilt binaries (no Rust required)

Every `faucet-cli` release ships prebuilt binaries for macOS (Apple Silicon +
Intel) and Linux (x86_64 + aarch64), so you don't need a Rust toolchain to try
it.

**Homebrew (macOS / Linux):**

```bash
brew install faucet-hq/faucet-stream/faucet-cli
```

(The formula is named after the `faucet-cli` package; it installs the `faucet`
binary.)

**Shell installer (macOS / Linux):**

```bash
curl -LsSf https://github.com/faucet-hq/faucet-stream/releases/latest/download/faucet-cli-installer.sh | sh
```

**Direct download:** grab the archive for your platform from the latest
[`faucet-cli` GitHub Release](https://github.com/faucet-hq/faucet-stream/releases?q=faucet-cli&expanded=true)
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

### Choose your build (feature flags)

Every connector and runtime capability is a **Cargo feature**, so you can build exactly the
binary you need. Connector features are named **`source-<name>`** and **`sink-<name>`**.

**Bare minimum** — the smallest useful binary (REST in, JSON Lines out):

```bash
cargo install faucet-cli --no-default-features --features "source-rest,sink-jsonl"
```

**Add a source or sink** — list the connectors you want (plus `transforms` if you need in-flight shaping):

```bash
cargo install faucet-cli --no-default-features \
  --features "source-postgres,sink-bigquery,transforms"
```

**Add a runtime capability** — compose any of `serve`, `serve-ui`, `schedule`, `lineage`,
`transform-sql` (embedded DuckDB), `triggers`, `catalog`, `otel`, `compression`, `quality`,
`contract`, `masking`:

```bash
cargo install faucet-cli --features "serve,schedule,transform-sql,lineage"
```

Run `faucet list` to see which sources, sinks, and transforms are compiled into your binary,
and the [connector catalog](../reference/connectors.md) for every feature name.

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
