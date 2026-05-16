//! # faucet-state-redis
//!
//! Redis-backed [`StateStore`](faucet_core::state::StateStore) for faucet-stream
//! incremental replication bookmarks. Stores each entry as a single Redis
//! string under `{namespace}:{key}` containing a serialized JSON value.

pub mod store;

pub use store::RedisStateStore;
