//! Source sharding for clustered (Mode B) execution.
//!
//! A *shardable* source can split its work into independent [`ShardSpec`]s that
//! different cluster workers process concurrently — so one logical pipeline over
//! a single large source (a big S3 prefix, a billion-row table) scales
//! horizontally instead of being capped at one worker's throughput.
//!
//! The capability is exposed as **defaulted methods on the [`Source`] trait**
//! ([`enumerate_shards`](crate::Source::enumerate_shards),
//! [`apply_shard`](crate::Source::apply_shard),
//! [`is_shardable`](crate::Source::is_shardable)) rather than a separate trait,
//! so it composes with the existing `Box<dyn Source>` object model with no
//! downcast and stays fully backward compatible: a source that does not override
//! them reports `is_shardable() == false` and enumerates to a single
//! whole-dataset shard, i.e. it behaves exactly as today.
//!
//! ## Lifecycle
//!
//! 1. A coordinator calls [`enumerate_shards`](crate::Source::enumerate_shards)
//!    once per run (idempotently) to discover the shard set.
//! 2. Each shard is persisted and claimed by exactly one worker.
//! 3. The claiming worker calls [`apply_shard`](crate::Source::apply_shard) to
//!    narrow its source instance to that one shard before streaming.
//! 4. Each shard carries its own state-key suffix so resume is per-shard: a
//!    reassigned shard resumes where its dead owner left off.

use serde::{Deserialize, Serialize};

/// One independently-processable slice of a shardable source.
///
/// The [`descriptor`](ShardSpec::descriptor) is opaque to the coordinator — only
/// the producing/consuming connector interprets it (e.g. `{"pk_range":[0,1000]}`
/// for a key-range split, `{"prefix":"dt=2026-06-11/"}` for an object split). It
/// must round-trip through JSON unchanged so the coordinator can persist it and
/// hand it back to [`apply_shard`](crate::Source::apply_shard) on the worker that
/// claims the shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardSpec {
    /// Stable identifier, unique within a run's shard set. Also used as the
    /// per-shard state-key suffix (`{run}::{shard.id}`), so it must be a valid
    /// state-key segment (no `:` — see
    /// [`validate_state_key`](crate::state::validate_state_key)).
    pub id: String,

    /// Connector-specific, opaque shard descriptor. Persisted verbatim and
    /// passed back to [`apply_shard`](crate::Source::apply_shard).
    pub descriptor: serde_json::Value,

    /// Optional relative size estimate (rows / bytes / objects — the unit is the
    /// connector's choice, only comparisons within one run's set matter) used
    /// for skew-aware assignment. `None` when the source cannot cheaply
    /// estimate; the coordinator then falls back to count-based balancing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_estimate: Option<u64>,
}

impl ShardSpec {
    /// The single whole-dataset shard a non-shardable source enumerates to.
    ///
    /// Its [`id`](ShardSpec::id) is `"0"` and its descriptor is JSON `null`;
    /// [`apply_shard`](crate::Source::apply_shard)'s default ignores it, so the
    /// source streams its entire dataset unchanged.
    pub fn whole() -> Self {
        Self {
            id: "0".to_string(),
            descriptor: serde_json::Value::Null,
            size_estimate: None,
        }
    }

    /// A shard with the given id and descriptor and no size estimate.
    pub fn new(id: impl Into<String>, descriptor: serde_json::Value) -> Self {
        Self {
            id: id.into(),
            descriptor,
            size_estimate: None,
        }
    }

    /// Attach a relative size estimate (builder style).
    pub fn with_size(mut self, size: u64) -> Self {
        self.size_estimate = Some(size);
        self
    }

    /// Whether this is the whole-dataset shard (id `"0"`, null descriptor) — the
    /// no-op shard a non-shardable source produces.
    pub fn is_whole(&self) -> bool {
        self.id == "0" && self.descriptor.is_null()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn whole_is_the_no_op_shard() {
        let w = ShardSpec::whole();
        assert_eq!(w.id, "0");
        assert!(w.descriptor.is_null());
        assert_eq!(w.size_estimate, None);
        assert!(w.is_whole());
    }

    #[test]
    fn new_and_with_size() {
        let s = ShardSpec::new("3", json!({ "partition": 3 })).with_size(42);
        assert_eq!(s.id, "3");
        assert_eq!(s.descriptor, json!({ "partition": 3 }));
        assert_eq!(s.size_estimate, Some(42));
        assert!(!s.is_whole(), "a real shard is not the whole-dataset shard");
    }

    #[test]
    fn round_trips_through_json_with_size_omitted_when_none() {
        let s = ShardSpec::new("a", json!({ "prefix": "dt=2026-06-11/" }));
        let text = serde_json::to_string(&s).unwrap();
        // size_estimate is omitted from the wire form when None.
        assert!(
            !text.contains("size_estimate"),
            "None size is skipped: {text}"
        );
        let back: ShardSpec = serde_json::from_str(&text).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn round_trips_with_size_present() {
        let s = ShardSpec::new("b", json!({ "pk_range": [0, 1000] })).with_size(1000);
        let back: ShardSpec = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert_eq!(back, s);
        assert_eq!(back.size_estimate, Some(1000));
    }

    // A non-whole shard with id "0" but a non-null descriptor is NOT treated as
    // the whole-dataset shard (the descriptor disambiguates).
    #[test]
    fn id_zero_with_descriptor_is_not_whole() {
        let s = ShardSpec::new("0", json!({ "partition": 0 }));
        assert!(!s.is_whole());
    }
}
