# faucet-state-postgres

PostgreSQL-backed [`StateStore`](https://docs.rs/faucet-core/latest/faucet_core/state/trait.StateStore.html) for the [faucet-stream](https://crates.io/crates/faucet-stream) ecosystem. Persists incremental-replication bookmarks across pipeline runs in a single JSONB-backed table, so sources can resume exactly where they left off after a crash, restart, or scheduled invocation.

## Install

```toml
[dependencies]
faucet-core = "1.0"
faucet-state-postgres = "1.0"
```

## Usage

```rust,no_run
use std::sync::Arc;
use faucet_core::{Pipeline, state::StateStore};
use faucet_state_postgres::PostgresStateStore;

# async fn run(source: impl faucet_core::Source, sink: impl faucet_core::Sink) -> Result<(), faucet_core::FaucetError> {
let store = PostgresStateStore::connect("postgres://user:pass@localhost/faucet").await?;
store.ensure_table().await?;
let store: Arc<dyn StateStore> = Arc::new(store);

Pipeline::new(&source, &sink)
    .with_state_store(store)
    .run()
    .await?;
# Ok(())
# }
```

## Storage layout

```sql
CREATE TABLE faucet_state (
    key        TEXT        PRIMARY KEY,
    value      JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

`ensure_table()` runs the `CREATE TABLE IF NOT EXISTS` for you. If you manage schema separately, skip it.

## Operations

- `get(key)` — `SELECT value FROM <table> WHERE key = $1`. Returns `None` when no row exists.
- `put(key, value)` — `INSERT ... ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`.
- `delete(key)` — `DELETE FROM <table> WHERE key = $1`. A missing row is not an error.

The pool is configured via `connect_with(url, max_connections, table)`; the default `connect(url)` uses 5 connections and the `faucet_state` table.

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `max_connections` | `5` | Size of the `sqlx::PgPool`. State writes are cheap; the default is intentionally small. |
| `table` | `faucet_state` | Override the table name. Validated as a Postgres identifier (letters/digits/underscore, ≤ 63 chars). |

## Validation

- State keys are validated by [`faucet_core::state::validate_state_key`] — ASCII alphanumerics plus `_ - : .`, max 256 characters.
- Table identifiers are validated against the Postgres rules to keep error messages clear and avoid SQL injection through misconfiguration. Identifiers are also double-quoted via `faucet_core::util::quote_ident`.

## License

Licensed under either of MIT or Apache-2.0 at your option.
