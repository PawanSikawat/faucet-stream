//! Configuration for `PostgresCdcSource`.

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

fn default_true() -> bool {
    true
}
fn default_proto_version() -> u32 {
    1
}
fn default_idle_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_status_update_interval() -> Duration {
    Duration::from_secs(10)
}
fn default_tcp_keepalive() -> Duration {
    Duration::from_secs(60)
}

/// Configuration for [`PostgresCdcSource`](crate::PostgresCdcSource).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct PostgresCdcSourceConfig {
    /// Connection URL pointing at the database whose WAL we want to read.
    /// The crate internally upgrades the connection to `replication=database`
    /// — callers do **not** need to add it themselves.
    pub connection_url: String,

    /// Logical replication slot name. Must match the Postgres naming rules:
    /// 1–63 chars, lowercase letters / digits / underscores only.
    pub slot_name: String,

    /// Publication name on the server. Must already exist (faucet does not
    /// create publications — they're a DBA-level concern that determines
    /// which tables are replicated).
    pub publication_name: String,

    /// If the slot does not exist, create it as a logical/`pgoutput` slot
    /// at connection time. Default: `true`.
    #[serde(default = "default_true")]
    pub create_slot_if_missing: bool,

    /// Optional starting LSN override (e.g. `"0/16A4F88"`). Ignored when a
    /// state-store-managed bookmark is present (that bookmark wins).
    /// When neither is set, replication starts from the slot's
    /// `confirmed_flush_lsn`.
    #[serde(default)]
    pub start_lsn: Option<String>,

    /// pgoutput protocol version. Only `1` is fully exercised in v1; `2` is
    /// accepted but streaming-transaction messages (S/E/c/A) are not yet
    /// decoded. Default: `1`.
    #[serde(default = "default_proto_version")]
    pub proto_version: u32,

    /// Maximum time to wait for new replication messages before returning
    /// the current batch. Default: 30 s.
    #[serde(
        default = "default_idle_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub idle_timeout: Duration,

    /// Optional cap on the number of change events drained per fetch call.
    /// Acts as a safety bound — `idle_timeout` is the primary terminator.
    #[serde(default)]
    pub max_messages: Option<usize>,

    /// Interval at which Standby Status Update keepalives are sent to the
    /// server. Must be shorter than `idle_timeout` and well under the
    /// server's `wal_sender_timeout` (default 60 s). Default: 10 s.
    #[serde(
        default = "default_status_update_interval",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub status_update_interval: Duration,

    /// TCP keepalive for the replication connection. Default: 60 s.
    #[serde(
        default = "default_tcp_keepalive",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub tcp_keepalive: Duration,
}

impl std::fmt::Debug for PostgresCdcSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresCdcSourceConfig")
            .field("connection_url", &"***")
            .field("slot_name", &self.slot_name)
            .field("publication_name", &self.publication_name)
            .field("create_slot_if_missing", &self.create_slot_if_missing)
            .field("start_lsn", &self.start_lsn)
            .field("proto_version", &self.proto_version)
            .field("idle_timeout", &self.idle_timeout)
            .field("max_messages", &self.max_messages)
            .field("status_update_interval", &self.status_update_interval)
            .field("tcp_keepalive", &self.tcp_keepalive)
            .finish()
    }
}

impl PostgresCdcSourceConfig {
    /// Validate fail-fast invariants. Called from `PostgresCdcSource::new`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.connection_url.trim().is_empty() {
            return Err(FaucetError::Config(
                "postgres-cdc: connection_url must not be empty".into(),
            ));
        }
        validate_slot_name(&self.slot_name)?;
        if self.publication_name.is_empty() {
            return Err(FaucetError::Config(
                "postgres-cdc: publication_name must not be empty".into(),
            ));
        }
        if self.proto_version != 1 {
            return Err(FaucetError::Config(format!(
                "postgres-cdc: proto_version must be 1 (v2 streaming-transaction \
                 support is not yet available via pgwire-replication), got {}",
                self.proto_version
            )));
        }
        if self.idle_timeout.is_zero() {
            return Err(FaucetError::Config(
                "postgres-cdc: idle_timeout must be > 0".into(),
            ));
        }
        if self.status_update_interval >= self.idle_timeout {
            return Err(FaucetError::Config(format!(
                "postgres-cdc: status_update_interval ({}s) must be \
                 strictly less than idle_timeout ({}s)",
                self.status_update_interval.as_secs(),
                self.idle_timeout.as_secs()
            )));
        }
        Ok(())
    }
}

fn validate_slot_name(name: &str) -> Result<(), FaucetError> {
    if name.is_empty() {
        return Err(FaucetError::Config(
            "postgres-cdc: slot_name must not be empty".into(),
        ));
    }
    if name.len() > 63 {
        return Err(FaucetError::Config(format!(
            "postgres-cdc: slot_name '{name}' exceeds Postgres' 63-char limit"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(FaucetError::Config(format!(
            "postgres-cdc: slot_name '{name}' must contain only \
             [a-z0-9_]"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> PostgresCdcSourceConfig {
        PostgresCdcSourceConfig {
            connection_url: "postgres://u:p@localhost/db".into(),
            slot_name: "faucet_slot".into(),
            publication_name: "faucet_pub".into(),
            create_slot_if_missing: true,
            start_lsn: None,
            proto_version: 1,
            idle_timeout: std::time::Duration::from_secs(30),
            max_messages: None,
            status_update_interval: std::time::Duration::from_secs(10),
            tcp_keepalive: std::time::Duration::from_secs(60),
        }
    }

    #[test]
    fn defaults_via_serde() {
        let value: PostgresCdcSourceConfig = serde_json::from_value(serde_json::json!({
            "connection_url": "postgres://u:p@localhost/db",
            "slot_name": "faucet_slot",
            "publication_name": "faucet_pub",
        }))
        .unwrap();
        assert!(value.create_slot_if_missing);
        assert_eq!(value.proto_version, 1);
        assert_eq!(value.idle_timeout.as_secs(), 30);
        assert_eq!(value.status_update_interval.as_secs(), 10);
        assert_eq!(value.tcp_keepalive.as_secs(), 60);
        assert!(value.start_lsn.is_none());
        assert!(value.max_messages.is_none());
    }

    #[test]
    fn rejects_empty_slot_name() {
        let mut c = minimal();
        c.slot_name = String::new();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_invalid_slot_name_chars() {
        let mut c = minimal();
        c.slot_name = "Faucet-Slot".into(); // uppercase + dash both disallowed
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_slot_name_over_63_chars() {
        let mut c = minimal();
        c.slot_name = "a".repeat(64);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_empty_publication_name() {
        let mut c = minimal();
        c.publication_name = String::new();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_zero_idle_timeout() {
        let mut c = minimal();
        c.idle_timeout = std::time::Duration::from_secs(0);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_status_update_interval_longer_than_idle_timeout() {
        // Keepalives must fire before idle_timeout would terminate the loop.
        let mut c = minimal();
        c.status_update_interval = std::time::Duration::from_secs(60);
        c.idle_timeout = std::time::Duration::from_secs(30);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_invalid_proto_version() {
        // 0, 2, and 3 are all rejected — only 1 is supported.
        let mut c = minimal();
        c.proto_version = 0;
        assert!(c.validate().is_err());
        c.proto_version = 2;
        assert!(c.validate().is_err());
        c.proto_version = 3;
        assert!(c.validate().is_err());
    }

    #[test]
    fn accepts_proto_version_one() {
        let mut c = minimal();
        c.proto_version = 1;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn rejects_empty_connection_url() {
        let mut c = minimal();
        c.connection_url = String::new();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_whitespace_connection_url() {
        let mut c = minimal();
        c.connection_url = "   ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn debug_redacts_connection_url() {
        let cfg = minimal();
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("connection_url: \"***\""));
        assert!(!dbg.contains("u:p@localhost"));
    }

    #[test]
    fn schema_for_config_includes_required_fields() {
        let schema = schemars::schema_for!(PostgresCdcSourceConfig);
        let json = serde_json::to_value(&schema).unwrap();
        let required = json["required"].as_array().expect("required array");
        let names: Vec<_> = required.iter().filter_map(|v| v.as_str()).collect();
        assert!(names.contains(&"connection_url"));
        assert!(names.contains(&"slot_name"));
        assert!(names.contains(&"publication_name"));
    }
}
