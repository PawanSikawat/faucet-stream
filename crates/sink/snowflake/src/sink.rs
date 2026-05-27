//! Snowflake SQL REST API sink.

use crate::config::SnowflakeSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;
use faucet_snowflake_common::{authorization_header, snowflake_token_type};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};

/// A sink that writes JSON records to a Snowflake table using the
/// SQL REST API.
pub struct SnowflakeSink {
    config: SnowflakeSinkConfig,
    client: Client,
    /// Optional explicit endpoint override. When `None`, the URL is derived
    /// from `config.account`. Used by tests to point the sink at a mock
    /// server, and useful for proxies / private-link deployments.
    endpoint: Option<String>,
}

#[derive(Deserialize)]
struct SnowflakeResponse {
    message: Option<String>,
    #[serde(default)]
    code: Option<String>,
    /// Present on an HTTP 202 (asynchronous execution) response — the
    /// opaque handle used to poll the statement to completion.
    #[serde(rename = "statementHandle", default)]
    statement_handle: Option<String>,
}

/// Map a parsed statement response onto a success/error result. Code
/// `090001` is "Statement executed successfully"; any other non-null code
/// is a Snowflake-side error.
fn check_statement_code(sf_resp: &SnowflakeResponse) -> Result<(), FaucetError> {
    if let Some(code) = &sf_resp.code
        && code != "090001"
    {
        return Err(FaucetError::Sink(format!(
            "Snowflake error {}: {}",
            code,
            sf_resp.message.clone().unwrap_or_default()
        )));
    }
    Ok(())
}

impl SnowflakeSink {
    /// Create a new Snowflake sink.
    pub fn new(config: SnowflakeSinkConfig) -> Self {
        Self {
            config,
            client: Client::new(),
            endpoint: None,
        }
    }

    /// Override the API endpoint URL (full URL including
    /// `/api/v2/statements`). When set, this URL is used verbatim instead
    /// of the account-derived `https://{account}.snowflakecomputing.com/...`
    /// URL. Intended for tests (wiremock) and proxy / private-link setups.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Build the SQL REST API endpoint URL.
    fn api_url(&self) -> String {
        if let Some(endpoint) = &self.endpoint {
            return endpoint.clone();
        }
        format!(
            "https://{}.snowflakecomputing.com/api/v2/statements",
            self.config.account
        )
    }

    /// Get the authorization header value.
    fn auth_header(&self) -> Result<String, FaucetError> {
        authorization_header(&self.config.auth, &self.config.account)
    }

    /// Execute a SQL statement via the REST API, optionally with positional
    /// bindings (`{"1": {"type": "TEXT", "value": ...}}`).
    async fn execute_sql(&self, sql: &str, bindings: Option<Value>) -> Result<(), FaucetError> {
        let url = self.api_url();
        let auth = self.auth_header()?;
        let token_type = snowflake_token_type(&self.config.auth);

        let mut body = json!({
            "statement": sql,
            "timeout": 60,
            "database": self.config.database,
            "schema": self.config.schema,
            "warehouse": self.config.warehouse,
        });
        if let Some(bindings) = bindings {
            body["bindings"] = bindings;
        }

        let resp = self
            .client
            .post(&url)
            .header("Authorization", &auth)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("X-Snowflake-Authorization-Token-Type", token_type)
            .json(&body)
            .send()
            .await
            .map_err(|e| FaucetError::Sink(format!("Snowflake request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Sink(format!(
                "Snowflake SQL API returned HTTP {status}: {body_text}"
            )));
        }

        // HTTP 202 means Snowflake *accepted* the statement but has not yet
        // executed it. Treating that as success would report rows as written
        // before they are actually committed. Poll the returned handle until
        // the statement completes (#78/#17).
        let is_async = status.as_u16() == 202;

        let sf_resp: SnowflakeResponse = resp
            .json()
            .await
            .map_err(|e| FaucetError::Sink(format!("failed to parse Snowflake response: {e}")))?;

        if is_async {
            let handle = sf_resp.statement_handle.ok_or_else(|| {
                FaucetError::Sink(
                    "Snowflake returned HTTP 202 without a statementHandle to poll".into(),
                )
            })?;
            return self.poll_until_complete(&handle, &auth, token_type).await;
        }

        check_statement_code(&sf_resp)
    }

    /// Poll `GET /api/v2/statements/{handle}` until the statement finishes
    /// executing (HTTP 200 + code `090001`), bounded by `poll_timeout`.
    async fn poll_until_complete(
        &self,
        handle: &str,
        auth: &str,
        token_type: &'static str,
    ) -> Result<(), FaucetError> {
        let url = format!("{}/{}", self.api_url(), handle);
        let poll_timeout = self.config.poll_timeout;
        let started = std::time::Instant::now();
        loop {
            let resp = self
                .client
                .get(&url)
                .header("Authorization", auth)
                .header("Accept", "application/json")
                .header("X-Snowflake-Authorization-Token-Type", token_type)
                .send()
                .await
                .map_err(|e| FaucetError::Sink(format!("Snowflake poll request failed: {e}")))?;

            let status = resp.status();
            if status.as_u16() == 202 {
                // `poll_timeout == 0` disables the cap (poll forever).
                if !poll_timeout.is_zero() && started.elapsed() >= poll_timeout {
                    return Err(FaucetError::Sink(format!(
                        "Snowflake statement '{handle}' did not finish within poll_timeout ({}s); still HTTP 202",
                        poll_timeout.as_secs()
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
            if !status.is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(FaucetError::Sink(format!(
                    "Snowflake poll returned HTTP {status}: {body_text}"
                )));
            }
            let sf_resp: SnowflakeResponse = resp.json().await.map_err(|e| {
                FaucetError::Sink(format!("failed to parse Snowflake poll response: {e}"))
            })?;
            return check_statement_code(&sf_resp);
        }
    }

    /// Build an INSERT statement plus the JSON payload to bind to its single
    /// `PARSE_JSON(?)` parameter.
    ///
    /// The record array is passed as a bound `TEXT` parameter, never
    /// interpolated into a SQL string literal: interpolation was a
    /// SQL-injection vector and corrupted any value containing an apostrophe
    /// (#78/#5). Returns `(sql, json_payload)`.
    fn build_insert(&self, records: &[Value]) -> Result<(String, String), FaucetError> {
        for record in records {
            record.as_object().ok_or_else(|| {
                FaucetError::Sink("Snowflake sink requires JSON object records".into())
            })?;
        }

        let payload = Value::Array(records.to_vec()).to_string();
        let sql = format!(
            "INSERT INTO {}.{}.{} (SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON(?))))",
            quote_ident(&self.config.database),
            quote_ident(&self.config.schema),
            quote_ident(&self.config.table),
        );
        Ok((sql, payload))
    }
}

#[async_trait]
impl faucet_core::Sink for SnowflakeSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(SnowflakeSinkConfig))
            .expect("schema serialization")
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // `batch_size = 0` is the "no batching" sentinel: forward whatever
        // upstream handed us as a single INSERT, preserving `StreamPage`
        // framing. Otherwise re-chunk into `batch_size` slices so each
        // outbound REST request stays near Snowflake's documented sweet
        // spot (~1000 rows).
        let effective_chunk = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };

        let mut total = 0;
        for chunk in records.chunks(effective_chunk) {
            let (sql, payload) = self.build_insert(chunk)?;
            let bindings = json!({ "1": { "type": "TEXT", "value": payload } });
            self.execute_sql(&sql, Some(bindings)).await?;
            total += chunk.len();
        }

        tracing::info!(
            table = %format!(
                "{}.{}.{}",
                self.config.database, self.config.schema, self.config.table
            ),
            rows = total,
            "Snowflake write complete"
        );
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SnowflakeAuth;

    #[test]
    fn api_url_format() {
        let config = SnowflakeSinkConfig::new(
            "xy12345.us-east-1",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth {
                token: "tok".into(),
            },
        );
        let sink = SnowflakeSink::new(config);
        assert_eq!(
            sink.api_url(),
            "https://xy12345.us-east-1.snowflakecomputing.com/api/v2/statements"
        );
    }

    #[test]
    fn oauth_auth_header() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth {
                token: "my-token".into(),
            },
        );
        let sink = SnowflakeSink::new(config);
        let header = sink.auth_header().unwrap();
        assert_eq!(header, "Snowflake Token=\"my-token\"");
    }

    #[test]
    fn api_url_honours_endpoint_override() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink =
            SnowflakeSink::new(config).with_endpoint("http://127.0.0.1:1234/api/v2/statements");
        assert_eq!(sink.api_url(), "http://127.0.0.1:1234/api/v2/statements");
    }

    #[test]
    fn build_insert_uses_quoted_identifiers() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "MY_DB",
            "PUBLIC",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config);
        let records = vec![serde_json::json!({"id": 1})];
        let (sql, _payload) = sink.build_insert(&records).unwrap();
        assert!(sql.contains("\"MY_DB\".\"PUBLIC\".\"events\""));
    }

    #[test]
    fn build_insert_binds_payload_instead_of_interpolating() {
        // Regression for #78/#5. The record JSON must travel as a bound TEXT
        // parameter to PARSE_JSON(?), never interpolated into a SQL string
        // literal — interpolation is a SQL-injection vector and breaks on any
        // value containing an apostrophe.
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config);
        let records = vec![
            serde_json::json!({"name": "O'Brien"}),
            serde_json::json!({"note": "'); DROP TABLE events;--"}),
        ];
        let (sql, payload) = sink.build_insert(&records).unwrap();

        // SQL is a parameterised placeholder — no record data, no literal.
        assert!(sql.contains("PARSE_JSON(?)"), "sql: {sql}");
        assert!(
            !sql.contains('\''),
            "sql must not embed a quoted literal: {sql}"
        );
        assert!(!sql.contains("O'Brien"));
        assert!(!sql.contains("DROP TABLE"));

        // The payload is the JSON array, carrying the apostrophe data intact.
        let parsed: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed[0]["name"], "O'Brien");
        assert_eq!(parsed[1]["note"], "'); DROP TABLE events;--");
    }
}
