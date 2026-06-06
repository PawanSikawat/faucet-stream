//! Snowflake SQL REST API sink.

use crate::config::SnowflakeSinkConfig;
use async_trait::async_trait;
use faucet_common_snowflake::{
    SnowflakeAuth, authorization_header, credential_to_auth, snowflake_token_type,
};
use faucet_core::util::quote_ident;
use faucet_core::{AuthSpec, FaucetError, SharedAuthProvider};
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
    /// Optional shared auth provider. When set, takes precedence over inline
    /// auth; the provider yields a `Bearer` or `Token` credential mapped onto
    /// [`SnowflakeAuth::OAuth`]. Set via [`Self::with_auth_provider`].
    auth_provider: Option<SharedAuthProvider>,
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
    ///
    /// Returns [`FaucetError::Config`] if `batch_size` exceeds
    /// `MAX_BATCH_SIZE` (#78/#44).
    pub fn new(config: SnowflakeSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        Ok(Self {
            config,
            client: Client::new(),
            endpoint: None,
            auth_provider: None,
        })
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking
    /// precedence over inline auth), so several sinks can share one OAuth
    /// token with single-flight refresh. Used by the CLI to resolve
    /// `auth: { ref }`, and by library callers who inject a provider directly.
    ///
    /// The provider must yield a `Bearer` or `Token` credential, which maps
    /// onto [`SnowflakeAuth::OAuth`]. Key-pair JWT cannot be supplied via a
    /// provider (JWT is minted locally from the RSA key).
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
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

    /// Resolve the effective [`SnowflakeAuth`] for this request.
    ///
    /// Resolution order:
    /// 1. If a shared provider is attached, call it and map the credential.
    /// 2. Otherwise, use the inline auth from the config.
    /// 3. If the config holds an unresolved `Reference` with no provider,
    ///    return [`FaucetError::Auth`].
    async fn resolve_auth(&self) -> Result<SnowflakeAuth, FaucetError> {
        if let Some(p) = &self.auth_provider {
            return credential_to_auth(p.credential().await?);
        }
        match &self.config.auth {
            AuthSpec::Inline(a) => Ok(a.clone()),
            AuthSpec::Reference(r) => Err(FaucetError::Auth(format!(
                "auth references provider '{}' but no provider was supplied",
                r.name
            ))),
        }
    }

    /// Get the authorization header value.
    async fn auth_header(&self) -> Result<(String, &'static str), FaucetError> {
        let effective = self.resolve_auth().await?;
        let header = authorization_header(&effective, &self.config.account)?;
        let token_type = snowflake_token_type(&effective);
        Ok((header, token_type))
    }

    /// Execute a SQL statement via the REST API, optionally with positional
    /// bindings (`{"1": {"type": "TEXT", "value": ...}}`).
    async fn execute_sql(&self, sql: &str, bindings: Option<Value>) -> Result<(), FaucetError> {
        let url = self.api_url();
        let (auth, token_type) = self.auth_header().await?;

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
            return self.poll_until_complete(&handle).await;
        }

        check_statement_code(&sf_resp)
    }

    /// Poll `GET /api/v2/statements/{handle}` until the statement finishes
    /// executing (HTTP 200 + code `090001`), bounded by `poll_timeout`.
    async fn poll_until_complete(&self, handle: &str) -> Result<(), FaucetError> {
        let url = format!("{}/{}", self.api_url(), handle);
        let poll_timeout = self.config.poll_timeout;
        let started = std::time::Instant::now();
        loop {
            // Re-resolve auth every iteration: a long-running async statement can
            // outlive a short-lived OAuth token, so we re-ask the (single-flight,
            // cached) provider for a current token rather than reusing the one
            // minted at submit time — otherwise the poll 401s mid-run after a
            // rotation (#146).
            let (auth, token_type) = self.auth_header().await?;
            let resp = self
                .client
                .get(&url)
                .header("Authorization", &auth)
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
    /// The record array travels as one bound `TEXT` parameter to
    /// `PARSE_JSON(?)`, never interpolated into a SQL string literal:
    /// interpolation was a SQL-injection vector and corrupted any value
    /// containing an apostrophe (#78/#5). `FLATTEN` then yields one row per
    /// array element, and each record field is projected into its matching
    /// column.
    ///
    /// The projection is **per-column** — `value:"col"::string` for each key —
    /// not `SELECT *`. `SELECT *` over `FLATTEN` returns FLATTEN's fixed
    /// `SEQ, KEY, PATH, INDEX, VALUE, THIS` metadata columns, so the previous
    /// statement inserted that metadata instead of the record's fields and was
    /// non-functional for any normal table (audit #146 C2). The `::string` cast
    /// strips the VARIANT's JSON quotes and lets Snowflake coerce the scalar
    /// into the destination column's type on `INSERT` (text → number / boolean
    /// / timestamp, etc.). The column set is taken from the first non-empty
    /// record; a key missing from a later record projects to SQL `NULL`.
    ///
    /// Both the column identifiers and the JSON path keys are escaped via
    /// [`quote_ident`] (double-quote doubling), so record keys cannot inject
    /// SQL. Returns `(sql, json_payload)`.
    ///
    /// Note: a record key whose target column is semi-structured (`VARIANT` /
    /// `OBJECT` / `ARRAY`) is stringified by the `::string` cast rather than
    /// stored as structured JSON; this sink maps records to scalar columns.
    fn build_insert(&self, records: &[Value]) -> Result<(String, String), FaucetError> {
        // The column set is the keys of the first non-empty record (all rows in
        // one INSERT must share a column list). Every record must be an object.
        let mut columns: Option<Vec<String>> = None;
        for record in records {
            let obj = record.as_object().ok_or_else(|| {
                FaucetError::Sink("Snowflake sink requires JSON object records".into())
            })?;
            if columns.is_none() && !obj.is_empty() {
                columns = Some(obj.keys().cloned().collect());
            }
        }
        let columns = columns.ok_or_else(|| {
            FaucetError::Sink("Snowflake sink: records have no fields to insert".into())
        })?;

        // `quote_ident` produces a `"`-escaped quoted identifier, which is also
        // the correct (injection-safe) form for a FLATTEN path key: `value:"k"`.
        let col_list = columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let projection = columns
            .iter()
            .map(|c| format!("value:{}::string", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", ");

        let payload = Value::Array(records.to_vec()).to_string();
        let sql = format!(
            "INSERT INTO {}.{}.{} ({}) SELECT {} FROM TABLE(FLATTEN(input => PARSE_JSON(?)))",
            quote_ident(&self.config.database),
            quote_ident(&self.config.schema),
            quote_ident(&self.config.table),
            col_list,
            projection,
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

    fn dataset_uri(&self) -> String {
        format!("snowflake://{}/{}/{}?table={}",
            self.config.account, self.config.database, self.config.schema, self.config.table)
    }

    /// Preflight check (`faucet doctor`).
    ///
    /// Runs a single read-only `SELECT 1` through the existing SQL REST API
    /// request path (`execute_sql`), reusing the sink's
    /// configured account/warehouse/auth. This resolves the effective
    /// credential (inline or shared provider), builds the authorization
    /// header, and confirms Snowflake accepts the session — without writing
    /// any rows. Auth-resolution, network, and SQL-API errors surface as a
    /// `Fail` probe with a hint. Tokens are never placed in the reason/hint.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();

        let result = tokio::time::timeout(ctx.timeout, self.execute_sql("SELECT 1", None)).await;

        let probe = match result {
            Ok(Ok(())) => Probe::pass("auth", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                format!("Snowflake SELECT 1 failed: {e}"),
                "Verify the account identifier, warehouse, and credentials \
                 (OAuth token or key-pair JWT) and that the role can use the \
                 configured warehouse.",
            ),
            Err(_elapsed) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                format!("Snowflake SELECT 1 timed out after {:?}", ctx.timeout),
                "Check network reachability to the Snowflake SQL REST API \
                 endpoint and that the warehouse can resume within the timeout.",
            ),
        };

        Ok(CheckReport::single(probe))
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
    use faucet_core::Sink as _;

    #[test]
    fn dataset_uri_includes_account_db_schema_table() {
        let config = SnowflakeSinkConfig::new(
            "myacct.us-east-1",
            "wh",
            "mydb",
            "PUBLIC",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        assert_eq!(
            sink.dataset_uri(),
            "snowflake://myacct.us-east-1/mydb/PUBLIC?table=events"
        );
    }

    #[test]
    fn new_rejects_oversized_batch_size() {
        // Regression for #78/#44.
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth { token: "t".into() },
        )
        .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(SnowflakeSink::new(config).is_err());
    }

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
        let sink = SnowflakeSink::new(config).unwrap();
        assert_eq!(
            sink.api_url(),
            "https://xy12345.us-east-1.snowflakecomputing.com/api/v2/statements"
        );
    }

    #[tokio::test]
    async fn oauth_auth_header() {
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
        let sink = SnowflakeSink::new(config).unwrap();
        let (header, token_type) = sink.auth_header().await.unwrap();
        assert_eq!(header, "Snowflake Token=\"my-token\"");
        assert_eq!(token_type, "OAUTH");
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
        let sink = SnowflakeSink::new(config)
            .unwrap()
            .with_endpoint("http://127.0.0.1:1234/api/v2/statements");
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
        let sink = SnowflakeSink::new(config).unwrap();
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
        let sink = SnowflakeSink::new(config).unwrap();
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

    #[test]
    fn build_insert_maps_record_fields_to_columns_not_flatten_metadata() {
        // C2 regression (audit #146): the INSERT must project each record field
        // into its named column, NOT `SELECT *` over FLATTEN — `SELECT *` over
        // FLATTEN returns the fixed SEQ/KEY/PATH/INDEX/VALUE/THIS metadata
        // columns, so the old statement inserted metadata instead of the
        // record's own fields.
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let records = vec![serde_json::json!({"user_id": 1, "event": "click"})];
        let (sql, _payload) = sink.build_insert(&records).unwrap();

        // Named column list + per-column projection from the FLATTEN `value`.
        assert!(sql.contains("\"user_id\""), "sql: {sql}");
        assert!(sql.contains("\"event\""), "sql: {sql}");
        assert!(sql.contains("value:\"user_id\"::string"), "sql: {sql}");
        assert!(sql.contains("value:\"event\"::string"), "sql: {sql}");
        // Crucially, NOT a metadata-projecting `SELECT *`.
        assert!(
            !sql.contains("SELECT *"),
            "must not SELECT * over FLATTEN: {sql}"
        );
        assert!(
            sql.contains("FLATTEN(input => PARSE_JSON(?))"),
            "sql: {sql}"
        );
    }

    #[test]
    fn build_insert_escapes_record_keys_in_columns_and_paths() {
        // Record keys are user-controlled; a key containing a double quote must
        // be `"`-doubled in both the column list and the FLATTEN path so it
        // cannot break out of the identifier / path.
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let records = vec![serde_json::json!({"a\"b": 1})];
        let (sql, _payload) = sink.build_insert(&records).unwrap();
        // Column identifier and path key are both escaped as "a""b".
        assert!(sql.contains("\"a\"\"b\""), "sql: {sql}");
        assert!(sql.contains("value:\"a\"\"b\"::string"), "sql: {sql}");
    }

    #[test]
    fn build_insert_rejects_all_empty_records() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config).unwrap();
        let records = vec![serde_json::json!({})];
        assert!(sink.build_insert(&records).is_err());
    }
}
