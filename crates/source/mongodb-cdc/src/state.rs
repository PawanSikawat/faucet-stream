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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Bookmark {
    /// The opaque resume token as relaxed extended JSON (e.g. `{"_data": "..."}`).
    pub resume_token: Value,
}

impl Bookmark {
    /// Build a bookmark from a driver `ResumeToken`.
    pub fn from_token(token: &ResumeToken) -> Result<Self, FaucetError> {
        let bson = bson::to_bson(token)
            .map_err(|e| FaucetError::State(format!("mongodb-cdc resume token serialize: {e}")))?;
        Ok(Self {
            resume_token: bson.into_relaxed_extjson(),
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
        };
        let v = b.to_value().unwrap();
        let parsed = Bookmark::from_value(v).unwrap();
        assert_eq!(parsed, b);
    }

    #[test]
    fn bookmark_token_round_trip() {
        let b = Bookmark {
            resume_token: json!({ "_data": "8264AB00" }),
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
}
