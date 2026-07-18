//! Bookmark shape + state key for the Pub/Sub source.
//!
//! Pub/Sub has **no client-side resume offset** — durability is the server
//! tracking acked messages on the subscription. So the emitted bookmark is
//! purely informational (a cumulative count + the last message id): it exists
//! so each durable page triggers a `flush` + `StateStore::put`, which is the
//! signal the streaming loop uses to ack the previous page. On resume the
//! subscription redelivers whatever was never acked, so the persisted
//! bookmark is not consulted to seek.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The persisted (informational) bookmark value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PubsubBookmark {
    /// Cumulative messages emitted this run.
    #[serde(default)]
    pub delivered: u64,
    /// The most recently emitted message id (for observability).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_message_id: Option<String>,
}

impl PubsubBookmark {
    /// Record one more delivered message.
    pub fn advance(&mut self, message_id: &str) {
        self.delivered += 1;
        self.last_message_id = Some(message_id.to_string());
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// Parse a bookmark `Value`; a malformed value is treated as fresh (never
    /// fails a run — Pub/Sub redelivers unacked messages regardless).
    pub fn from_value(v: &Value) -> Self {
        serde_json::from_value(v.clone()).unwrap_or_default()
    }
}

/// The source's stable state key.
pub fn state_key(subscription: &str) -> String {
    format!("pubsub:{subscription}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bookmark_advances_and_round_trips() {
        let mut b = PubsubBookmark::default();
        b.advance("m1");
        b.advance("m2");
        assert_eq!(b.delivered, 2);
        assert_eq!(b.last_message_id.as_deref(), Some("m2"));
        let back = PubsubBookmark::from_value(&b.to_value());
        assert_eq!(back, b);
    }

    #[test]
    fn malformed_bookmark_is_fresh() {
        assert_eq!(
            PubsubBookmark::from_value(&json!("nope")),
            PubsubBookmark::default()
        );
        assert_eq!(
            PubsubBookmark::from_value(&json!(null)),
            PubsubBookmark::default()
        );
    }

    #[test]
    fn state_key_shape_is_valid() {
        assert_eq!(state_key("orders-sub"), "pubsub:orders-sub");
        faucet_core::state::validate_state_key(&state_key("orders-sub")).unwrap();
    }
}
