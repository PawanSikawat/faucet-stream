# faucet-sink-stdout

Stdout/stderr sink for the [faucet-stream](https://crates.io/crates/faucet-stream) ecosystem. Writes each record to a standard stream in one of three formats — useful for debugging pipelines, building demos, and powering the `faucet preview` CLI workflow.

## Install

```toml
[dependencies]
faucet-core = "0.2"
faucet-sink-stdout = "0.2"
```

Or via the umbrella crate with the `sink-stdout` feature:

```toml
[dependencies]
faucet-stream = { version = "0.2", features = ["sink-stdout"] }
```

## Usage

```rust,no_run
use faucet_core::Sink;
use faucet_sink_stdout::{StdoutFormat, StdStream, StdoutSink, StdoutSinkConfig};
use serde_json::json;

# async fn run() -> Result<(), faucet_core::FaucetError> {
let sink = StdoutSink::new(
    StdoutSinkConfig::new()
        .destination(StdStream::Stdout)
        .format(StdoutFormat::JsonLines),
);

sink.write_batch(&[json!({"id": 1, "name": "Alice"})]).await?;
sink.flush().await?;
# Ok(())
# }
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `destination` | `stdout` \| `stderr` | `stdout` | Which standard stream to write to. |
| `format` | `json_lines` \| `pretty_json` \| `tsv` | `json_lines` | Output format. |
| `flush_per_record` | `bool` | `false` | Flush after every record (lower latency, slightly lower throughput). |
| `max_records` | `usize?` | `None` | Stop after `n` records. Useful for `faucet preview --limit=N`. |

### Formats

- **`json_lines`** — one compact JSON object per line. The de facto debug format.
- **`pretty_json`** — indented JSON, each record separated by a newline. Easier on the eyes.
- **`tsv`** — tab-separated values. Each record's keys are sorted alphabetically; scalar values are emitted raw (with control characters replaced by spaces in strings), and nested objects/arrays are emitted as compact JSON. Requires every record to be a JSON object.

## Behavior notes

- The underlying writer is opened eagerly in `new()`.
- A `BrokenPipe` from the consumer (e.g. piping into `head`) is treated as a clean termination — subsequent `write_batch` calls return `Ok(0)` rather than erroring.
- `flush()` flushes the underlying writer; the default `Sink` impl does not flush on `Drop`, so call it explicitly if you need durability of buffered output.
- `StdoutSink::with_writer(config, writer)` accepts any `Box<dyn AsyncWrite + Unpin + Send>`. Useful in tests and when redirecting into log files or in-memory buffers.

## License

Licensed under either of MIT or Apache-2.0 at your option.
