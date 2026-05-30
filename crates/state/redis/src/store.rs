//! Redis-backed [`StateStore`].

use async_trait::async_trait;
use faucet_core::state::{DOCTOR_SENTINEL_KEY, StateStore, validate_state_key};
use faucet_core::{FaucetError, Value};
use redis::AsyncCommands;

/// A `StateStore` that persists each entry as a single Redis string under
/// `{namespace}:{key}`.
///
/// The connection is opened in [`RedisStateStore::connect`] and reused across
/// all calls — `redis::aio::MultiplexedConnection` is cheaply cloneable.
pub struct RedisStateStore {
    namespace: String,
    conn: redis::aio::MultiplexedConnection,
}

impl RedisStateStore {
    /// Connect to a Redis server and namespace all keys under `namespace:`.
    ///
    /// `url` follows the standard Redis URL form (`redis://[:pass@]host:port/db`).
    pub async fn connect(
        url: impl AsRef<str>,
        namespace: impl Into<String>,
    ) -> Result<Self, FaucetError> {
        let namespace = namespace.into();
        validate_namespace(&namespace)?;
        let client = redis::Client::open(url.as_ref())
            .map_err(|e| FaucetError::Config(format!("invalid Redis URL: {e}")))?;
        let conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| FaucetError::State(format!("Redis connection failed: {e}")))?;
        Ok(Self { namespace, conn })
    }

    /// Construct from an existing async connection. Useful for tests and for
    /// integrators who want to share a connection across multiple stores.
    pub fn from_connection(
        conn: redis::aio::MultiplexedConnection,
        namespace: impl Into<String>,
    ) -> Result<Self, FaucetError> {
        let namespace = namespace.into();
        validate_namespace(&namespace)?;
        Ok(Self { namespace, conn })
    }

    /// Returns the fully-qualified Redis key for a given state key.
    pub fn redis_key(&self, key: &str) -> String {
        build_redis_key(&self.namespace, key)
    }
}

/// Format the namespaced Redis key. Exposed as a free function so it can be
/// unit-tested without constructing a `RedisStateStore` (which needs a real
/// Redis connection).
pub(crate) fn build_redis_key(namespace: &str, key: &str) -> String {
    format!("{namespace}:{key}")
}

pub(crate) fn validate_namespace(namespace: &str) -> Result<(), FaucetError> {
    if namespace.is_empty() {
        return Err(FaucetError::Config(
            "Redis state namespace must not be empty".into(),
        ));
    }
    for (i, c) in namespace.char_indices() {
        let ok = c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.');
        if !ok {
            return Err(FaucetError::Config(format!(
                "Redis state namespace contains illegal character {c:?} at byte {i}"
            )));
        }
    }
    Ok(())
}

#[async_trait]
impl StateStore for RedisStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        validate_state_key(key)?;
        let mut conn = self.conn.clone();
        let raw: Option<String> = conn
            .get(self.redis_key(key))
            .await
            .map_err(|e| FaucetError::State(format!("Redis GET for key '{key}' failed: {e}")))?;
        match raw {
            None => Ok(None),
            Some(s) => {
                let value: Value = serde_json::from_str(&s).map_err(|e| {
                    FaucetError::State(format!(
                        "stored value for key '{key}' is not valid JSON: {e}"
                    ))
                })?;
                Ok(Some(value))
            }
        }
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        let serialized = serde_json::to_string(value).map_err(|e| {
            FaucetError::State(format!("failed to serialize state for key '{key}': {e}"))
        })?;
        let mut conn = self.conn.clone();
        let _: () = conn
            .set(self.redis_key(key), serialized)
            .await
            .map_err(|e| FaucetError::State(format!("Redis SET for key '{key}' failed: {e}")))?;
        tracing::debug!(key, namespace = %self.namespace, "state written to Redis");
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        let mut conn = self.conn.clone();
        let _: i64 = conn
            .del(self.redis_key(key))
            .await
            .map_err(|e| FaucetError::State(format!("Redis DEL for key '{key}' failed: {e}")))?;
        Ok(())
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        // Exercise the real put → get → delete cycle on a sentinel key. This
        // validates connectivity, auth, and read/write permissions through the
        // actual code path and leaves no residue.
        let start = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, self.sentinel_roundtrip()).await {
            Ok(Ok(())) => Probe::pass("sentinel", start.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "sentinel",
                start.elapsed(),
                e.to_string(),
                "verify the Redis server is reachable and the credentials grant read/write access",
            ),
            Err(_) => Probe::fail_hint(
                "sentinel",
                start.elapsed(),
                format!(
                    "round-trip timed out after {:?}; Redis did not respond",
                    ctx.timeout
                ),
                "verify the Redis server is reachable or raise the check timeout",
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

impl RedisStateStore {
    /// Write, read back, and delete a sentinel key — the body of the `check()`
    /// probe, factored out so the happy path stays linear. Reuses the store's
    /// own `put`/`get`/`delete`, which already namespace the key.
    async fn sentinel_roundtrip(&self) -> Result<(), FaucetError> {
        let probe = serde_json::json!({ "faucet_doctor": true });
        self.put(DOCTOR_SENTINEL_KEY, &probe).await?;
        let got = self.get(DOCTOR_SENTINEL_KEY).await?;
        // Best-effort cleanup regardless of the read result.
        let _ = self.delete(DOCTOR_SENTINEL_KEY).await;
        match got {
            Some(v) if v == probe => Ok(()),
            _ => Err(FaucetError::State(
                "sentinel readback did not match what was written".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_redis_key_namespaces_consistently() {
        assert_eq!(
            build_redis_key("faucet", "github_issues"),
            "faucet:github_issues"
        );
        assert_eq!(build_redis_key("a", "b"), "a:b");
    }

    #[test]
    fn validate_namespace_accepts_typical_values() {
        for ns in ["faucet", "team-1.prod", "a_b", "ABC.123"] {
            validate_namespace(ns).unwrap_or_else(|e| panic!("expected ok for {ns:?}: {e}"));
        }
    }

    #[test]
    fn validate_namespace_rejects_empty() {
        let err = validate_namespace("").unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn validate_namespace_rejects_illegal_chars() {
        for ns in ["a:b", "a/b", "a b", "hello world"] {
            let err = validate_namespace(ns).expect_err(&format!("expected error for {ns:?}"));
            assert!(matches!(err, FaucetError::Config(_)));
        }
    }

    #[tokio::test]
    async fn connect_rejects_invalid_url() {
        let result = RedisStateStore::connect("not a url", "faucet").await;
        match result {
            Err(FaucetError::Config(msg)) => assert!(msg.contains("invalid Redis URL")),
            Err(other) => panic!("expected Config error, got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[tokio::test]
    async fn connect_rejects_invalid_namespace() {
        let result = RedisStateStore::connect("redis://127.0.0.1:6379", "bad:namespace").await;
        match result {
            Err(FaucetError::Config(_)) => {}
            Err(other) => panic!("expected Config error, got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }
}
