//! # faucet-state-postgres
//!
//! PostgreSQL-backed [`StateStore`](faucet_core::state::StateStore) for
//! faucet-stream incremental replication bookmarks. Stores each entry in a
//! single `faucet_state(key TEXT PRIMARY KEY, value JSONB, updated_at TIMESTAMPTZ)`
//! table.

pub mod store;

pub use store::PostgresStateStore;
