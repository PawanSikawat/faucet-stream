//! Bookmark <-> JSON serialization for replication slot LSN progress.
//!
//! Bookmark shape (round-trips through `serde_json::Value`):
//!
//! ```json
//! { "last_lsn": "0/16A4F88" }
//! ```
//!
//! `last_lsn` is the `commit_lsn` of the last transaction whose change
//! records have been written to the sink — i.e. the position to resume
//! *after* on the next `START_REPLICATION`.

use faucet_core::FaucetError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Durable bookmark for a `PostgresCdcSource`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Bookmark {
    /// LSN in Postgres' canonical hex form: `XXXXXXXX/XXXXXXXX`.
    pub last_lsn: String,
}

impl Bookmark {
    /// Parse a `Value` previously emitted by `to_value`.
    pub fn from_value(v: Value) -> Result<Self, FaucetError> {
        let b: Self = serde_json::from_value(v)
            .map_err(|e| FaucetError::State(format!("postgres-cdc bookmark parse: {e}")))?;
        parse_lsn(&b.last_lsn)?;
        Ok(b)
    }

    /// Serialize for the state store.
    pub fn to_value(&self) -> Result<Value, FaucetError> {
        serde_json::to_value(self)
            .map_err(|e| FaucetError::State(format!("postgres-cdc bookmark serialize: {e}")))
    }

    /// Build a Bookmark from a raw 64-bit LSN value.
    pub fn from_u64(lsn: u64) -> Self {
        Self {
            last_lsn: format_lsn(lsn),
        }
    }

    /// Parse `last_lsn` into a `u64` for wire-protocol use.
    pub fn as_u64(&self) -> Result<u64, FaucetError> {
        parse_lsn(&self.last_lsn)
    }
}

/// Format a `u64` LSN into Postgres' `XXXXXXXX/XXXXXXXX` text form, dropping
/// leading zeros from each half (matches `pg_lsn::out`).
pub fn format_lsn(lsn: u64) -> String {
    let hi = (lsn >> 32) as u32;
    let lo = lsn as u32;
    format!("{hi:X}/{lo:X}")
}

/// Parse `XXXX/XXXX` (case-insensitive hex, no leading zeros required) into a
/// `u64`. Returns `FaucetError::State` on any malformation.
pub fn parse_lsn(s: &str) -> Result<u64, FaucetError> {
    let (hi, lo) = s.split_once('/').ok_or_else(|| {
        FaucetError::State(format!("postgres-cdc invalid LSN '{s}': missing '/'"))
    })?;
    if hi.is_empty() || lo.is_empty() || hi.contains('/') || lo.contains('/') {
        return Err(FaucetError::State(format!(
            "postgres-cdc invalid LSN '{s}'"
        )));
    }
    let hi = u32::from_str_radix(hi, 16)
        .map_err(|e| FaucetError::State(format!("postgres-cdc LSN high half '{hi}': {e}")))?;
    let lo = u32::from_str_radix(lo, 16)
        .map_err(|e| FaucetError::State(format!("postgres-cdc LSN low half '{lo}': {e}")))?;
    Ok((u64::from(hi) << 32) | u64::from(lo))
}

/// Generate the `state_key` for a given replication slot.
///
/// Allowed characters per `faucet_core::state::validate_state_key` are
/// `[A-Za-z0-9_:.-]`. Postgres slot names are constrained to `[a-z0-9_]`, so
/// every key is valid by construction.
pub fn state_key(slot_name: &str) -> String {
    format!("postgres-cdc:{slot_name}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip_via_value() {
        let b = Bookmark {
            last_lsn: "0/16A4F88".into(),
        };
        let v = b.to_value().unwrap();
        let parsed = Bookmark::from_value(v).unwrap();
        assert_eq!(parsed.last_lsn, "0/16A4F88");
    }

    #[test]
    fn from_value_rejects_garbage() {
        assert!(Bookmark::from_value(json!({"last_lsn": 42})).is_err());
        assert!(Bookmark::from_value(json!({})).is_err());
        assert!(Bookmark::from_value(json!("bare string")).is_err());
    }

    #[test]
    fn from_value_rejects_malformed_lsn() {
        assert!(Bookmark::from_value(json!({"last_lsn": ""})).is_err());
        assert!(Bookmark::from_value(json!({"last_lsn": "not-an-lsn"})).is_err());
        assert!(Bookmark::from_value(json!({"last_lsn": "0/"})).is_err());
        assert!(Bookmark::from_value(json!({"last_lsn": "/0"})).is_err());
        assert!(Bookmark::from_value(json!({"last_lsn": "0/16A4F88/extra"})).is_err());
    }

    #[test]
    fn from_value_accepts_canonical_lsn() {
        let ok = Bookmark::from_value(json!({"last_lsn": "1A/BEEFCAFE"})).unwrap();
        assert_eq!(ok.last_lsn, "1A/BEEFCAFE");
    }

    #[test]
    fn lsn_u64_round_trip() {
        let b = Bookmark::from_u64(0x1A_BEEF_CAFE);
        // 0x1A_BEEF_CAFE = 0x1A_0000_0000 | 0xBEEF_CAFE → "1A/BEEFCAFE"
        // (the high 32 bits are the WAL segment, low 32 are the offset)
        assert_eq!(b.last_lsn, "1A/BEEFCAFE");
        assert_eq!(b.as_u64().unwrap(), 0x1A_BEEF_CAFE);
    }

    #[test]
    fn state_key_for_slot() {
        assert_eq!(state_key("faucet_slot"), "postgres-cdc:faucet_slot");
    }
}
