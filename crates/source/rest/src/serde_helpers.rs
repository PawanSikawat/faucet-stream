//! Serde helper modules for types that don't implement Serialize/Deserialize natively.

/// Serialize/deserialize `reqwest::Method` as a string (e.g. `"GET"`, `"POST"`).
pub mod http_method {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(method: &reqwest::Method, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(method.as_str())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<reqwest::Method, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}
