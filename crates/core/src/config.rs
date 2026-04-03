//! Configuration loading utilities.
//!
//! Helpers for loading any `Deserialize`-able config struct from JSON files
//! or environment variables.
//!
//! # JSON file
//!
//! ```rust,no_run
//! use faucet_core::config::load_json;
//! # #[derive(serde::Deserialize)] struct MyConfig { url: String }
//! let config: MyConfig = load_json("config.json").unwrap();
//! ```
//!
//! # Environment variables
//!
//! ```rust,no_run
//! use faucet_core::config::load_env;
//! # #[derive(serde::Deserialize)] struct MyConfig { url: String }
//! // Reads MYAPP_URL, MYAPP_BATCH_SIZE, etc.
//! let config: MyConfig = load_env("MYAPP").unwrap();
//! ```
//!
//! # `.env` file + environment variables
//!
//! ```rust,no_run
//! use faucet_core::config::load_env_file;
//! # #[derive(serde::Deserialize)] struct MyConfig { url: String }
//! // Loads .env file, then reads MYAPP_URL, MYAPP_BATCH_SIZE, etc.
//! let config: MyConfig = load_env_file(".env", "MYAPP").unwrap();
//! ```

use crate::error::FaucetError;
use serde::de::DeserializeOwned;
use std::path::Path;

/// Load a config struct from a JSON file.
///
/// The file contents are read and deserialized into `T`.
pub fn load_json<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<T, FaucetError> {
    let path = path.as_ref();
    let contents = std::fs::read_to_string(path).map_err(|e| {
        FaucetError::Config(format!(
            "failed to read config file '{}': {e}",
            path.display()
        ))
    })?;
    serde_json::from_str(&contents).map_err(|e| {
        FaucetError::Config(format!(
            "failed to parse JSON config from '{}': {e}",
            path.display()
        ))
    })
}

/// Load a config struct from environment variables with a prefix.
///
/// Environment variable names are formed by uppercasing the field name
/// and prepending the prefix with an underscore separator.
/// For example, with prefix `"BQ"` and a field `project_id`, the env
/// var `BQ_PROJECT_ID` is read.
///
/// Nested structs and enums are supported via `envy`'s deserialization.
pub fn load_env<T: DeserializeOwned>(prefix: &str) -> Result<T, FaucetError> {
    envy::prefixed(format!("{prefix}_"))
        .from_env()
        .map_err(|e| FaucetError::Config(format!("failed to load config from env vars: {e}")))
}

/// Load a `.env` file into the process environment, then deserialize
/// a config struct from environment variables with a prefix.
///
/// This combines `dotenvy` (for `.env` file loading) with `envy`
/// (for struct deserialization from env vars).
///
/// The `.env` file is loaded first, setting any variables that aren't
/// already set in the environment. Then [`load_env`] is called.
pub fn load_env_file<T: DeserializeOwned>(
    env_path: impl AsRef<Path>,
    prefix: &str,
) -> Result<T, FaucetError> {
    let env_path = env_path.as_ref();
    dotenvy::from_path(env_path).map_err(|e| {
        FaucetError::Config(format!(
            "failed to load .env file '{}': {e}",
            env_path.display()
        ))
    })?;
    load_env(prefix)
}

/// Serde helper module for serializing `Duration` as seconds (u64).
///
/// Use with `#[serde(with = "faucet_core::config::duration_secs")]` on
/// `std::time::Duration` fields.
pub mod duration_secs {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

/// Serde helper for optional Duration fields.
pub mod duration_secs_option {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(
        duration: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match duration {
            Some(d) => serializer.serialize_some(&d.as_secs()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        let opt = Option::<u64>::deserialize(deserializer)?;
        Ok(opt.map(Duration::from_secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::io::Write;

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestConfig {
        url: String,
        #[serde(default)]
        batch_size: Option<usize>,
    }

    #[test]
    fn load_json_works() {
        let dir = std::env::temp_dir();
        let path = dir.join("faucet_test_config.json");
        let mut f = std::fs::File::create(&path).unwrap();
        write!(f, r#"{{"url": "https://example.com", "batch_size": 100}}"#).unwrap();

        let config: TestConfig = load_json(&path).unwrap();
        assert_eq!(config.url, "https://example.com");
        assert_eq!(config.batch_size, Some(100));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn load_json_missing_file() {
        let result = load_json::<TestConfig>("/nonexistent/path.json");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("failed to read"));
    }

    #[test]
    fn load_json_invalid_json() {
        let dir = std::env::temp_dir();
        let path = dir.join("faucet_test_bad.json");
        std::fs::write(&path, "not json").unwrap();

        let result = load_json::<TestConfig>(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("failed to parse"));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn load_json_with_defaults() {
        let dir = std::env::temp_dir();
        let path = dir.join("faucet_test_defaults.json");
        std::fs::write(&path, r#"{"url": "https://example.com"}"#).unwrap();

        let config: TestConfig = load_json(&path).unwrap();
        assert_eq!(config.url, "https://example.com");
        assert_eq!(config.batch_size, None);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn duration_secs_roundtrip() {
        use std::time::Duration;

        #[derive(Debug, serde::Serialize, Deserialize, PartialEq)]
        struct D {
            #[serde(with = "super::duration_secs")]
            timeout: Duration,
        }

        let d = D {
            timeout: Duration::from_secs(30),
        };
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(json, r#"{"timeout":30}"#);

        let parsed: D = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, d);
    }
}
