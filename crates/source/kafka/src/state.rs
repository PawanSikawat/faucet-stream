//! Bookmark <-> JSON serialization for Kafka offset progress.
//!
//! Bookmark shape (round-trips through `serde_json::Value`):
//!
//! ```json
//! {
//!   "partition_offsets": [
//!     {"topic": "orders", "partition": 0, "offset": 1234},
//!     {"topic": "orders", "partition": 1, "offset":  987}
//!   ]
//! }
//! ```
//!
//! `offset` is the next offset to read (i.e. one past the highest offset
//! whose value has been written to the sink).

use faucet_core::FaucetError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Bookmark {
    #[serde(default)]
    pub partition_offsets: Vec<PartitionOffset>,
}

impl Bookmark {
    pub fn from_value(v: Value) -> Result<Self, FaucetError> {
        serde_json::from_value(v)
            .map_err(|e| FaucetError::State(format!("kafka bookmark parse: {e}")))
    }

    pub fn to_value(&self) -> Result<Value, FaucetError> {
        serde_json::to_value(self)
            .map_err(|e| FaucetError::State(format!("kafka bookmark serialize: {e}")))
    }

    pub fn from_map(map: std::collections::HashMap<(String, i32), i64>) -> Self {
        let mut entries: Vec<PartitionOffset> = map
            .into_iter()
            .map(|((topic, partition), offset)| PartitionOffset {
                topic,
                partition,
                offset,
            })
            .collect();
        // Deterministic order makes diffs in state-store files reviewable.
        entries
            .sort_by(|a, b| (a.topic.as_str(), a.partition).cmp(&(b.topic.as_str(), b.partition)));
        Self {
            partition_offsets: entries,
        }
    }
}

/// Generate the `state_key` for a `(group_id, topics)` pair.
///
/// Topics are sorted before joining so the key is stable regardless of
/// config ordering. Allowed characters per
/// [`faucet_core::state::validate_state_key`] are `[A-Za-z0-9_:.-]`, so we
/// replace `,` with `.` when joining.
pub fn state_key(group_id: &str, topics: &[String]) -> String {
    let mut sorted = topics.to_vec();
    sorted.sort();
    format!("kafka:{group_id}:{}", sorted.join("."))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn round_trip_via_value() {
        let bookmark = Bookmark {
            partition_offsets: vec![
                PartitionOffset {
                    topic: "t".into(),
                    partition: 0,
                    offset: 5,
                },
                PartitionOffset {
                    topic: "t".into(),
                    partition: 1,
                    offset: 9,
                },
            ],
        };
        let v = bookmark.to_value().unwrap();
        let parsed = Bookmark::from_value(v).unwrap();
        assert_eq!(parsed.partition_offsets, bookmark.partition_offsets);
    }

    #[test]
    fn from_map_is_deterministic() {
        let mut a: HashMap<(String, i32), i64> = HashMap::new();
        a.insert(("z".into(), 0), 1);
        a.insert(("a".into(), 1), 2);
        a.insert(("a".into(), 0), 3);
        let b = Bookmark::from_map(a);
        let topics: Vec<_> = b
            .partition_offsets
            .iter()
            .map(|p| (p.topic.as_str(), p.partition))
            .collect();
        assert_eq!(topics, vec![("a", 0), ("a", 1), ("z", 0)]);
    }

    #[test]
    fn state_key_sorts_topics() {
        assert_eq!(
            state_key("g1", &["beta".into(), "alpha".into()]),
            "kafka:g1:alpha.beta"
        );
    }

    #[test]
    fn state_key_single_topic() {
        assert_eq!(state_key("g1", &["only".into()]), "kafka:g1:only");
    }

    #[test]
    fn from_value_rejects_garbage() {
        let v = json!({"partition_offsets": "not an array"});
        assert!(Bookmark::from_value(v).is_err());
    }

    #[test]
    fn empty_bookmark_round_trips() {
        let b = Bookmark::default();
        let v = b.to_value().unwrap();
        let parsed = Bookmark::from_value(v).unwrap();
        assert!(parsed.partition_offsets.is_empty());
    }
}
