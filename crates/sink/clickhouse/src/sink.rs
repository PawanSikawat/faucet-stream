//! The ClickHouse [`Sink`] implementation — HTTP client and batched
//! `INSERT … FORMAT JSONEachRow` writes.

use async_trait::async_trait;
use faucet_common_clickhouse::{apply_auth, build_client, build_json_each_row, query_params};
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use faucet_core::{FaucetError, Sink};
use serde_json::Value;

use crate::config::ClickHouseSinkConfig;

/// ClickHouse sink (HTTP interface, `INSERT … FORMAT JSONEachRow`).
pub struct ClickHouseSink {
    config: ClickHouseSinkConfig,
    client: reqwest::Client,
    /// Resolved once in [`ClickHouseSink::new`] so the hot path never re-parses.
    base_url: String,
    /// Per-sink run id for staged-object keys (avoids cross-run collisions).
    #[cfg(feature = "staging")]
    stage_run_id: String,
    /// Monotonic part counter for staged objects within this sink.
    #[cfg(feature = "staging")]
    stage_seq: std::sync::atomic::AtomicUsize,
}

/// Quote a (possibly schema-qualified) table name. Each `.`-separated segment
/// is quoted with [`faucet_core::util::quote_ident`] so `db.table` becomes
/// `"db"."table"` — safe against identifier injection while preserving the
/// database/table split.
fn quote_table(table: &str) -> String {
    table
        .split('.')
        .map(faucet_core::util::quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

/// Build the `INSERT … FORMAT JSONEachRow` statement (carried in the `query`
/// URL parameter; the row data travels in the request body).
fn insert_statement(table: &str) -> String {
    format!("INSERT INTO {} FORMAT JSONEachRow", quote_table(table))
}

/// Build the ordered query parameters for an insert request: the `database`,
/// any async-insert settings, and the `query` statement. Pure and
/// unit-testable.
fn insert_params(
    database: &str,
    statement: &str,
    async_insert: bool,
    wait_for_async_insert: bool,
) -> Vec<(String, String)> {
    let mut settings: Vec<(&str, &str)> = Vec::new();
    if async_insert {
        settings.push(("async_insert", "1"));
        settings.push((
            "wait_for_async_insert",
            if wait_for_async_insert { "1" } else { "0" },
        ));
    }
    settings.push(("query", statement));
    query_params(database, &settings)
}

impl ClickHouseSink {
    /// Validate the config and build the reusable HTTP client.
    pub fn new(config: ClickHouseSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let base_url = config.connection.base_url()?;
        let client = build_client(&config.connection)?;
        Ok(Self {
            config,
            client,
            base_url,
            #[cfg(feature = "staging")]
            stage_run_id: format!(
                "run-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            ),
            #[cfg(feature = "staging")]
            stage_seq: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Send a bare SQL statement (no row body) over the HTTP interface — used by
    /// the staged-load path's `INSERT … SELECT FROM s3(…)`.
    #[cfg(feature = "staging")]
    async fn send_query(&self, statement: &str) -> Result<(), FaucetError> {
        let params = query_params(&self.config.connection.database, &[("query", statement)]);
        let req = self.client.post(&self.base_url).query(&params);
        let req = apply_auth(req, &self.config.connection);
        let resp = req.send().await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        Ok(())
    }

    /// Staged bulk load (#528): upload the page to the object store, then have
    /// the ClickHouse server pull it with `s3()` / `gcs()`.
    #[cfg(feature = "staging")]
    async fn write_batch_staged(
        &self,
        records: &[Value],
        staging: &crate::config::ClickHouseStagingConfig,
    ) -> Result<usize, FaucetError> {
        use crate::staged::{clickhouse_stage_insert_sql, staged_https_url};
        use faucet_core::staging::{StageUploader, StagingFormat, StagingScheme};
        use std::sync::atomic::Ordering;

        // Restrict to what the s3()/gcs() path supports.
        let loc = staging.spec.validate(
            &[StagingScheme::S3, StagingScheme::Gcs],
            &[StagingFormat::Jsonl, StagingFormat::Csv],
        )?;
        let uploader = StageUploader::from_location(loc.clone())?;
        let seq = self.stage_seq.fetch_add(1, Ordering::Relaxed);
        let staged = uploader
            .stage_page(
                &staging.spec,
                &self.config.table,
                &self.stage_run_id,
                seq,
                records,
                None,
            )
            .await?;

        let url = staged_https_url(
            loc.scheme,
            &loc.bucket,
            &staged.key,
            staging.region.as_deref(),
            staging.endpoint.as_deref(),
        )?;
        let creds = staging
            .access_key
            .as_deref()
            .zip(staging.secret_key.as_deref());
        let sql = clickhouse_stage_insert_sql(
            &quote_table(&self.config.table),
            loc.scheme,
            &url,
            creds,
            staging.spec.format,
        )?;

        let result = self.send_query(&sql).await;
        uploader
            .cleanup(&[staged], staging.spec.cleanup, result.is_ok())
            .await;
        result?;
        tracing::debug!(records = records.len(), "ClickHouse staged load written");
        Ok(records.len())
    }

    /// Send one `INSERT … FORMAT JSONEachRow` request for a slice of records.
    async fn send_insert(&self, records: &[Value]) -> Result<(), FaucetError> {
        let body = build_json_each_row(records)?;
        let statement = insert_statement(&self.config.table);
        let params = insert_params(
            &self.config.connection.database,
            &statement,
            self.config.async_insert,
            self.config.wait_for_async_insert,
        );
        let req = self.client.post(&self.base_url).query(&params).body(body);
        let req = apply_auth(req, &self.config.connection);
        let resp = req.send().await?;
        check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for ClickHouseSink {
    /// Insert records via `INSERT … FORMAT JSONEachRow`.
    ///
    /// When `batch_size > 0` and the page is larger, it is split into
    /// `batch_size`-row chunks, each sent as its own request. `batch_size = 0`
    /// forwards the whole page as a single request.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Staged bulk load (#528): stage the whole page and let the server pull
        // it — no row body, no `batch_size` re-chunking.
        #[cfg(feature = "staging")]
        if let Some(staging) = &self.config.staging {
            return self.write_batch_staged(records, staging).await;
        }
        #[cfg(not(feature = "staging"))]
        if self.config.staging.is_some() {
            return Err(FaucetError::Config(
                "clickhouse: `staging:` is configured but this build lacks the `staging` \
                 feature — rebuild with `--features staging` (CLI: `sink-clickhouse-staging`)"
                    .into(),
            ));
        }

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut total = 0;
        for chunk in chunks {
            self.send_insert(chunk).await?;
            total += chunk.len();
            tracing::debug!(records = chunk.len(), "ClickHouse insert chunk written");
        }
        Ok(total)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(ClickHouseSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "clickhouse"
    }

    /// Staged bulk load is active only when a `staging:` block is configured
    /// and the `staging` feature is compiled in.
    fn supports_staged_load(&self) -> bool {
        cfg!(feature = "staging") && self.config.staging.is_some()
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}/{}",
            faucet_core::redact_uri_credentials(&self.base_url),
            self.config.table
        )
    }

    /// Non-mutating preflight probe (`connect`): runs `SELECT 1` over the HTTP
    /// interface. Deliberately does **not** touch the target table (no inserts,
    /// no residual rows).
    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let started = std::time::Instant::now();
        let hint = "check url / host / database / credentials / that the server is reachable";
        let params = query_params(&self.config.connection.database, &[]);
        let req = self
            .client
            .post(&self.base_url)
            .query(&params)
            .body("SELECT 1");
        let req = apply_auth(req, &self.config.connection);
        let probe = match tokio::time::timeout(ctx.timeout, req.send()).await {
            Ok(Ok(resp)) => match check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await {
                Ok(_) => Probe::pass("connect", started.elapsed()),
                Err(e) => Probe::fail_hint("connect", started.elapsed(), e.to_string(), hint),
            },
            Ok(Err(e)) => Probe::fail_hint("connect", started.elapsed(), e.to_string(), hint),
            Err(_) => Probe::fail_hint("connect", started.elapsed(), "timed out", hint),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sink() -> ClickHouseSink {
        ClickHouseSink::new(ClickHouseSinkConfig::new(
            "http://db.example.com:8123",
            "events",
        ))
        .unwrap()
    }

    #[test]
    fn quote_table_quotes_each_segment() {
        assert_eq!(quote_table("events"), "\"events\"");
        assert_eq!(quote_table("analytics.events"), "\"analytics\".\"events\"");
    }

    #[test]
    fn quote_table_escapes_hostile_identifier() {
        // A double quote in the identifier is doubled by quote_ident, so it
        // cannot break out of the quoting.
        let q = quote_table("we\"ird");
        assert_eq!(q, "\"we\"\"ird\"");
    }

    #[test]
    fn insert_statement_uses_json_each_row() {
        assert_eq!(
            insert_statement("events"),
            "INSERT INTO \"events\" FORMAT JSONEachRow"
        );
    }

    #[test]
    fn insert_params_without_async_insert() {
        let params = insert_params("analytics", "INSERT INTO x FORMAT JSONEachRow", false, true);
        assert_eq!(params[0], ("database".to_string(), "analytics".to_string()));
        // Only database + query when async insert is off.
        assert_eq!(params.len(), 2);
        assert_eq!(
            params[1],
            (
                "query".to_string(),
                "INSERT INTO x FORMAT JSONEachRow".to_string()
            )
        );
        assert!(!params.iter().any(|(k, _)| k == "async_insert"));
    }

    #[test]
    fn insert_params_with_async_insert_and_wait() {
        let params = insert_params("db", "INSERT INTO x FORMAT JSONEachRow", true, true);
        assert!(params.contains(&("async_insert".to_string(), "1".to_string())));
        assert!(params.contains(&("wait_for_async_insert".to_string(), "1".to_string())));
    }

    #[test]
    fn insert_params_async_insert_no_wait() {
        let params = insert_params("db", "INSERT INTO x FORMAT JSONEachRow", true, false);
        assert!(params.contains(&("wait_for_async_insert".to_string(), "0".to_string())));
    }

    #[test]
    fn build_body_produces_ndjson() {
        // The body builder is shared with the source's decoder; assert the
        // exact wire bytes the sink would POST.
        let page = vec![json!({"id": 1}), json!({"id": 2})];
        assert_eq!(
            build_json_each_row(&page).unwrap(),
            "{\"id\":1}\n{\"id\":2}\n"
        );
    }

    #[test]
    fn dataset_uri_combines_base_url_and_table() {
        assert_eq!(sink().dataset_uri(), "http://db.example.com:8123/events");
    }

    #[test]
    fn connector_name_is_clickhouse() {
        assert_eq!(sink().connector_name(), "clickhouse");
    }

    #[test]
    fn config_schema_is_object() {
        assert_eq!(sink().config_schema()["type"], "object");
    }

    #[test]
    fn append_is_the_only_write_mode() {
        // ClickHouse upsert is engine-dependent (ReplacingMergeTree) and is not
        // emulated by the sink — see the crate README.
        assert_eq!(
            sink().supported_write_modes(),
            &[faucet_core::WriteMode::Append]
        );
    }

    #[test]
    fn new_rejects_invalid_config() {
        assert!(ClickHouseSink::new(ClickHouseSinkConfig::new("http://h:8123", "")).is_err());
    }

    #[tokio::test]
    async fn write_batch_empty_is_noop() {
        assert_eq!(sink().write_batch(&[]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn check_fails_against_unreachable_server() {
        let sink =
            ClickHouseSink::new(ClickHouseSinkConfig::new("http://127.0.0.1:1", "t")).unwrap();
        let ctx = CheckContext {
            timeout: std::time::Duration::from_secs(2),
        };
        let report = sink.check(&ctx).await.unwrap();
        assert!(matches!(
            report.probes[0].status,
            faucet_core::check::ProbeStatus::Fail { .. }
        ));
    }
}
