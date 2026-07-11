//! Per-shard bookmark format: `{ "shards": { "<shard-id>": "<sequence>" } }`.
//!
//! Every emitted page carries the **cumulative** map, so any page's bookmark
//! is a valid resume point. On resume, a bookmarked shard restarts at
//! `AFTER_SEQUENCE_NUMBER(<persisted>)`; shards absent from the map use the
//! configured start position.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// The persisted bookmark value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardBookmarks {
    /// Shard id → last durably-emitted sequence number.
    #[serde(default)]
    pub shards: BTreeMap<String, String>,
}

impl ShardBookmarks {
    /// Record a shard's newest sequence number.
    pub fn advance(&mut self, shard_id: &str, sequence: &str) {
        self.shards
            .insert(shard_id.to_string(), sequence.to_string());
    }

    /// The persisted sequence for a shard, if any.
    pub fn get(&self, shard_id: &str) -> Option<&str> {
        self.shards.get(shard_id).map(String::as_str)
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// Parse a bookmark `Value`; a malformed value is treated as absent
    /// (never fails a run — worst case the shard re-reads from the start
    /// position, which is at-least-once-safe).
    pub fn from_value(v: &Value) -> Self {
        serde_json::from_value(v.clone()).unwrap_or_default()
    }
}

/// The source's stable state key.
pub fn state_key(stream_name: &str) -> String {
    format!("kinesis:{stream_name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bookmark_round_trips_and_advances() {
        let mut b = ShardBookmarks::default();
        b.advance("shardId-000000000000", "491");
        b.advance("shardId-000000000001", "530");
        b.advance("shardId-000000000000", "495"); // newer overwrites
        let v = b.to_value();
        assert_eq!(v["shards"]["shardId-000000000000"], "495");
        let back = ShardBookmarks::from_value(&v);
        assert_eq!(back, b);
        assert_eq!(back.get("shardId-000000000001"), Some("530"));
        assert_eq!(back.get("missing"), None);
    }

    #[test]
    fn malformed_bookmark_is_treated_as_fresh() {
        assert_eq!(
            ShardBookmarks::from_value(&json!("not-a-map")),
            ShardBookmarks::default()
        );
        assert_eq!(
            ShardBookmarks::from_value(&json!(null)),
            ShardBookmarks::default()
        );
    }

    #[test]
    fn state_key_shape_is_valid() {
        assert_eq!(state_key("events"), "kinesis:events");
        faucet_core::state::validate_state_key(&state_key("events")).unwrap();
    }
}
