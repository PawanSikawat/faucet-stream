//! State key + bookmark serialization for MySQL binlog progress.
//!
//! Bookmark JSON shapes:
//! ```json
//! { "file": "binlog.000123", "pos": 4567 }
//! { "gtid_set": "uuid:1-1000" }
//! ```

use faucet_core::FaucetError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// State key for a given replica `server_id`.
pub fn state_key(server_id: u32) -> String {
    format!("mysql-cdc:{server_id}")
}

/// Durable bookmark: either a binlog file+position or an executed GTID set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Bookmark {
    /// Binlog coordinates.
    FilePos { file: String, pos: u64 },
    /// Executed GTID set (e.g. `uuid:1-1000`).
    GtidSet { gtid_set: String },
}

impl Bookmark {
    /// Parse a bookmark previously emitted by `to_value`.
    pub fn from_value(v: Value) -> Result<Self, FaucetError> {
        serde_json::from_value(v)
            .map_err(|e| FaucetError::State(format!("mysql-cdc bookmark parse: {e}")))
    }

    /// Serialize for the state store.
    pub fn to_value(&self) -> Result<Value, FaucetError> {
        serde_json::to_value(self)
            .map_err(|e| FaucetError::State(format!("mysql-cdc bookmark serialize: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn state_key_format() {
        assert_eq!(state_key(1001), "mysql-cdc:1001");
    }

    #[test]
    fn file_pos_round_trip() {
        let b = Bookmark::FilePos {
            file: "binlog.000123".into(),
            pos: 4567,
        };
        assert_eq!(Bookmark::from_value(b.to_value().unwrap()).unwrap(), b);
    }

    #[test]
    fn gtid_round_trip() {
        let b = Bookmark::GtidSet {
            gtid_set: "uuid:1-1000".into(),
        };
        assert_eq!(Bookmark::from_value(b.to_value().unwrap()).unwrap(), b);
    }

    #[test]
    fn file_pos_json_shape() {
        let v = Bookmark::FilePos {
            file: "b.1".into(),
            pos: 9,
        }
        .to_value()
        .unwrap();
        assert_eq!(v, json!({ "file": "b.1", "pos": 9 }));
    }

    #[test]
    fn rejects_garbage() {
        assert!(Bookmark::from_value(json!({ "nope": 1 })).is_err());
    }
}
