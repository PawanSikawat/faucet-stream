# faucet-state-redis

Redis-backed [`StateStore`](https://docs.rs/faucet-core/latest/faucet_core/state/trait.StateStore.html) for the [faucet-stream](https://crates.io/crates/faucet-stream) ecosystem. Persists incremental-replication bookmarks across pipeline runs so a source can resume exactly where it left off after a crash, restart, or scheduled invocation.

## Install

```toml
[dependencies]
faucet-core = "1.0"
faucet-state-redis = "1.0"
```

## Usage

```rust,no_run
use std::sync::Arc;
use faucet_core::{Pipeline, state::StateStore};
use faucet_state_redis::RedisStateStore;

# async fn run(source: impl faucet_core::Source, sink: impl faucet_core::Sink) -> Result<(), faucet_core::FaucetError> {
let store: Arc<dyn StateStore> = Arc::new(
    RedisStateStore::connect("redis://127.0.0.1:6379", "faucet").await?,
);

Pipeline::new(&source, &sink)
    .with_state_store(store)
    .run()
    .await?;
# Ok(())
# }
```

## Storage layout

Each entry is a single Redis string:

| Redis key | Value |
|-----------|-------|
| `{namespace}:{state_key}` | UTF-8 JSON serialization of the bookmark value. |

The namespace is configured once in `connect()` and prefixed onto every key. Use it to keep multiple pipelines isolated within a shared Redis instance (e.g. `prod:`, `staging:`, `team-a:`).

## Operations

- `get(key)` — `GET {namespace}:{key}` → parse JSON, return `None` if the key is missing.
- `put(key, value)` — `SET {namespace}:{key} <serialized>`.
- `delete(key)` — `DEL {namespace}:{key}` (a missing key is not an error).

Connections use `redis::aio::MultiplexedConnection`, which is cheaply cloneable and safe to share across tasks.

## Key validation

State keys are validated by [`faucet_core::state::validate_state_key`] — ASCII alphanumerics plus `_ - : .`, max 256 characters, no leading dot. Namespaces additionally disallow `:` (since the namespace itself becomes a key prefix).

## License

Licensed under either of MIT or Apache-2.0 at your option.
