# Authoring a connector

faucet-stream is designed as an ecosystem: third parties can publish their own
`faucet-source-*` / `faucet-sink-*` crates with minimal friction. **`faucet-core`
is the only required dependency** — it re-exports everything a connector author
needs (`async_trait`, `serde_json`, `schemars`).

## The traits

Implement `Source` or `Sink`. Both are object-safe (`Box<dyn Source>` works) and
all newer methods have defaults, so a minimal connector is small.

```rust,ignore
use faucet_core::{async_trait, Source, Sink, FaucetError, Value};

struct MySource { /* reusable client/pool created in new() */ }

#[async_trait]
impl Source for MySource {
    // Primary entry point. (`fetch_all()` is a provided convenience.)
    async fn fetch_with_context(&self) -> Result<Vec<Value>, FaucetError> {
        todo!("fetch records from your system")
    }
}

struct MySink { /* reusable client/pool */ }

#[async_trait]
impl Sink for MySink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        todo!("write records to your system")
    }
}
```

Your connector now works with the `Pipeline` and every other connector:
`Pipeline::new(&MySource { .. }, &MySink { .. }).run().await?`.

## Crate layout

Follow the same module layout as the built-in connectors:

- **`lib.rs`** — re-export the config + the `Source`/`Sink` type. First line:
  `#![cfg_attr(docsrs, feature(doc_cfg))]` (see below).
- **`config.rs`** — the config struct + sub-enums, deriving
  `Serialize + Deserialize + JsonSchema`. **No I/O here.**
- **`stream.rs`** (source) / **`sink.rs`** (sink) — the one place that performs
  I/O. Create reusable clients/pools in `new()` and store them; never reconnect
  per call.

## Make it fast

Performance is the project's first principle. Reuse clients and connections,
pool database connections, use multi-row inserts and bulk APIs, and prefer
parallel I/O. Where it makes sense, override `stream_pages` to stream natively
from your source's paging primitive so memory stays bounded.

## Config schema introspection

Implement `config_schema()` so `faucet schema` and `faucet init` work:

```rust,ignore
fn config_schema(&self) -> Value {
    faucet_core::schema_for!(MyConfig).into()
}
```

Derive `JsonSchema` on the config struct and all sub-types, and add
`#[schemars(with = "String")]` for any custom-serde fields.

## Errors

Map every failure to a `FaucetError` variant. Third-party error types wrap into
`FaucetError::Custom(Box<dyn Error + Send + Sync>)` without losing the chain.
Never `.unwrap()` on anything that can fail at runtime.

## docs.rs setup

So docs.rs renders your full API with per-feature badges, add to `Cargo.toml`:

```toml
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

and make the first line of `lib.rs` `#![cfg_attr(docsrs, feature(doc_cfg))]`.

## Naming & publishing

Name crates `faucet-source-<name>` / `faucet-sink-<name>`. If you ship both a
source and a sink for the same system, put shared types (auth, formats) in a
`faucet-<name>-common` crate that both depend on and re-export.

> See any built-in connector (e.g. `faucet-source-rest`) for a reference
> implementation, and `CLAUDE.md` in the repo for the full conventions.
