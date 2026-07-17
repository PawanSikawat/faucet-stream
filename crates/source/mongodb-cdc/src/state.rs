//! State key derivation and resume-token bookmark serialization.
//!
//! Bookmark shape (round-trips through `serde_json::Value`):
//!
//! ```json
//! { "resume_token": { "_data": "8264..." } }
//! ```

use crate::config::Scope;
use faucet_core::FaucetError;
use mongodb::bson::{self, Bson};
use mongodb::change_stream::event::ResumeToken;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// State key for a given watch scope.
///
/// `cluster` → `mongodb-cdc:cluster`
/// `database` → `mongodb-cdc:db:<database>`
/// `collection` → `mongodb-cdc:coll:<database>.<collection>`
pub fn state_key(scope: &Scope) -> String {
    match scope {
        Scope::Cluster => "mongodb-cdc:cluster".to_string(),
        Scope::Database { database } => format!("mongodb-cdc:db:{database}"),
        Scope::Collection {
            database,
            collection,
        } => {
            format!("mongodb-cdc:coll:{database}.{collection}")
        }
    }
}

/// Durable bookmark wrapping a resume token.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Bookmark {
    /// The opaque resume token as relaxed extended JSON (e.g. `{"_data": "..."}`).
    pub resume_token: Value,
    /// True when this token was captured from an **Invalidate** change event
    /// (the collection was dropped/renamed, or a dropDatabase for db scope).
    /// MongoDB forbids `resumeAfter` on an invalidate token — only `startAfter`
    /// — so on resume the source must open the next stream with `start_after`,
    /// or every subsequent watch open fails and the CDC pipeline wedges
    /// permanently (audit #321 M3). Absent from older bookmarks (defaults
    /// `false`) and omitted from the serialized state when `false`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub invalidate: bool,
}

/// `skip_serializing_if` predicate keeping non-invalidate bookmarks byte-compatible
/// with the pre-#321 state format.
fn is_false(b: &bool) -> bool {
    !*b
}

impl Bookmark {
    /// Build a bookmark from a driver `ResumeToken`.
    pub fn from_token(token: &ResumeToken) -> Result<Self, FaucetError> {
        let bson = bson::to_bson(token)
            .map_err(|e| FaucetError::State(format!("mongodb-cdc resume token serialize: {e}")))?;
        Ok(Self {
            resume_token: bson.into_relaxed_extjson(),
            invalidate: false,
        })
    }

    /// Parse a bookmark previously emitted by `to_value`.
    pub fn from_value(v: Value) -> Result<Self, FaucetError> {
        serde_json::from_value(v)
            .map_err(|e| FaucetError::State(format!("mongodb-cdc bookmark parse: {e}")))
    }

    /// Serialize for the state store.
    pub fn to_value(&self) -> Result<Value, FaucetError> {
        serde_json::to_value(self)
            .map_err(|e| FaucetError::State(format!("mongodb-cdc bookmark serialize: {e}")))
    }

    /// Decode the stored resume token back into a driver `ResumeToken`.
    pub fn to_token(&self) -> Result<ResumeToken, FaucetError> {
        let bson: Bson = bson::to_bson(&self.resume_token)
            .map_err(|e| FaucetError::State(format!("mongodb-cdc resume token to bson: {e}")))?;
        bson::from_bson(bson)
            .map_err(|e| FaucetError::State(format!("mongodb-cdc resume token decode: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn state_key_cluster() {
        assert_eq!(state_key(&Scope::Cluster), "mongodb-cdc:cluster");
    }

    #[test]
    fn state_key_database() {
        assert_eq!(
            state_key(&Scope::Database {
                database: "app".into()
            }),
            "mongodb-cdc:db:app"
        );
    }

    #[test]
    fn state_key_collection() {
        assert_eq!(
            state_key(&Scope::Collection {
                database: "app".into(),
                collection: "users".into()
            }),
            "mongodb-cdc:coll:app.users"
        );
    }

    #[test]
    fn bookmark_value_round_trip() {
        let b = Bookmark {
            resume_token: json!({ "_data": "8264AB" }),
            ..Default::default()
        };
        let v = b.to_value().unwrap();
        let parsed = Bookmark::from_value(v).unwrap();
        assert_eq!(parsed, b);
    }

    #[test]
    fn bookmark_token_round_trip() {
        let b = Bookmark {
            resume_token: json!({ "_data": "8264AB00" }),
            ..Default::default()
        };
        let token = b.to_token().unwrap();
        let b2 = Bookmark::from_token(&token).unwrap();
        assert_eq!(b2.resume_token["_data"], json!("8264AB00"));
    }

    #[test]
    fn from_value_rejects_garbage() {
        assert!(Bookmark::from_value(json!({})).is_err());
        assert!(Bookmark::from_value(json!("bare")).is_err());
    }

    #[test]
    fn invalidate_flag_serde_is_backward_compatible() {
        // #321 M3: a legacy bookmark without the field parses as invalidate=false
        // and a non-invalidate bookmark omits the field entirely (compact state).
        let legacy = Bookmark::from_value(json!({ "resume_token": { "_data": "AA" } })).unwrap();
        assert!(!legacy.invalidate);
        let normal = Bookmark {
            resume_token: json!({ "_data": "AA" }),
            invalidate: false,
        };
        let v = normal.to_value().unwrap();
        assert!(v.get("invalidate").is_none(), "false flag is omitted: {v}");
        // An invalidate bookmark serializes the flag and round-trips.
        let inv = Bookmark {
            resume_token: json!({ "_data": "BB" }),
            invalidate: true,
        };
        let back = Bookmark::from_value(inv.to_value().unwrap()).unwrap();
        assert!(back.invalidate);
    }
}
