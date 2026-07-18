//! Amazon Redshift sink implementation.
//!
//! Two load paths, selected by `write_strategy`:
//! - `copy` (default) — stage each page to S3 (JSONL or CSV) and bulk-load it
//!   with `COPY … FROM 's3://…' IAM_ROLE '…'`, then best-effort delete the
//!   staged object. This is Redshift's recommended, fastest load path.
//! - `insert` — multi-row `INSERT INTO … VALUES (…), (…)`. Portable, no S3, but
//!   slower for bulk data.
//!
//! Append-only (`supported_write_modes` = `[Append]`): Redshift has no
//! `ON CONFLICT`, and `COPY` cannot upsert.

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use faucet_core::FaucetError;
use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::config::{RedshiftCopyFormat, RedshiftSinkConfig, RedshiftWriteStrategy};
use crate::copy::{
    columns_present, copy_statement, insert_statement, qualified_table_ref, s3_uri, serialize_csv,
    serialize_jsonl,
};

/// Redshift caps bind parameters per statement; keep multi-row `INSERT`s under
/// this by sub-chunking.
const MAX_REDSHIFT_PARAMS: usize = 32_767;

/// A sink that loads JSON records into an Amazon Redshift table.
pub struct RedshiftSink {
    config: RedshiftSinkConfig,
    pool: PgPool,
    /// S3 client, built only for the `copy` strategy.
    s3: Option<S3Client>,
}

impl RedshiftSink {
    /// Create a new sink. Validates config, builds a lazily-connected pool (no
    /// DB I/O), and — for the `copy` strategy — an S3 client.
    pub async fn new(config: RedshiftSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let pool =
            faucet_common_redshift::build_pool_lazy(&config.connection, config.max_connections)?;
        let s3 = if config.write_strategy == RedshiftWriteStrategy::Copy {
            Some(Self::build_s3_client(&config).await)
        } else {
            None
        };
        Ok(Self { config, pool, s3 })
    }

    /// Build an S3 client honouring the optional region / endpoint overrides.
    async fn build_s3_client(config: &RedshiftSinkConfig) -> S3Client {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = &config.region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        if let Some(endpoint) = &config.endpoint_url {
            loader = loader.endpoint_url(endpoint);
        }
        let sdk_config = loader.load().await;
        S3Client::new(&sdk_config)
    }

    fn table_ref(&self) -> String {
        qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name)
    }

    /// Generate a unique staging object key for one page.
    fn staging_key(&self, ext: &str) -> String {
        let id = uuid::Uuid::new_v4();
        format!("{}{}.{}", self.config.staging_prefix, id, ext)
    }

    /// Discover the destination table's column names in ordinal order via
    /// `information_schema.columns`. Used by the `insert` path and the CSV
    /// `copy` path (both need the column set/order).
    async fn discover_columns(&self) -> Result<Vec<String>, FaucetError> {
        let rows = sqlx::query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = $1 AND ($2::text IS NULL OR table_schema = $2) \
             ORDER BY ordinal_position",
        )
        .bind(&self.config.table_name)
        .bind(self.config.schema.as_deref())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("redshift: column discovery failed: {e}")))?;

        let cols: Vec<String> = rows
            .iter()
            .map(|r| r.get::<String, _>("column_name"))
            .collect();
        if cols.is_empty() {
            return Err(FaucetError::Sink(format!(
                "redshift: table {} has no columns or does not exist",
                self.config.table_name
            )));
        }
        Ok(cols)
    }

    /// Load one chunk via the `COPY`-from-S3 fast path.
    async fn copy_chunk(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let s3 = self.s3.as_ref().ok_or_else(|| {
            FaucetError::Sink("redshift: S3 client not initialized for copy strategy".into())
        })?;
        let bucket = self.config.staging_bucket.as_deref().ok_or_else(|| {
            FaucetError::Sink("redshift: staging_bucket is required for copy strategy".into())
        })?;
        let iam_role = self.config.iam_role.as_deref().ok_or_else(|| {
            FaucetError::Sink("redshift: iam_role is required for copy strategy".into())
        })?;

        // Serialize the page and, for CSV, learn the destination column order.
        let (body, columns): (Vec<u8>, Option<Vec<String>>) = match self.config.copy_format {
            RedshiftCopyFormat::Jsonl => (serialize_jsonl(records)?, None),
            RedshiftCopyFormat::Csv => {
                let cols = self.discover_columns().await?;
                (serialize_csv(records, &cols)?, Some(cols))
            }
        };
        let ext = match self.config.copy_format {
            RedshiftCopyFormat::Jsonl => "jsonl",
            RedshiftCopyFormat::Csv => "csv",
        };
        let key = self.staging_key(ext);

        // 1. Upload the staged object.
        s3.put_object()
            .bucket(bucket)
            .key(&key)
            .body(body.into())
            .send()
            .await
            .map_err(|e| {
                FaucetError::Sink(format!("redshift: S3 upload failed for key '{key}': {e}"))
            })?;

        // 2. COPY it into the table.
        let sql = copy_statement(
            &self.table_ref(),
            columns.as_deref(),
            &s3_uri(bucket, &key),
            iam_role,
            self.config.region.as_deref(),
            self.config.copy_format,
        );
        let copy_result = sqlx::query(&sql).execute(&self.pool).await;

        // 3. Best-effort cleanup of the staged object (regardless of COPY
        //    outcome — a failed COPY still leaves the object behind).
        if let Err(e) = s3.delete_object().bucket(bucket).key(&key).send().await {
            tracing::warn!(key = %key, error = %e, "redshift: failed to delete staged S3 object (best-effort)");
        }

        copy_result.map_err(|e| FaucetError::Sink(format!("redshift: COPY failed: {e}")))?;
        Ok(records.len())
    }

    /// Load one chunk via multi-row `INSERT`, sub-chunked to respect the bind
    /// parameter cap.
    async fn insert_chunk(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let table_columns = self.discover_columns().await?;
        let present: Vec<String> = columns_present(records, &table_columns)
            .into_iter()
            .cloned()
            .collect();
        if present.is_empty() {
            tracing::warn!(
                table = %self.config.table_name,
                "redshift: no record keys match table columns; skipping insert"
            );
            return Ok(0);
        }

        let num_cols = present.len();
        let max_rows = (MAX_REDSHIFT_PARAMS / num_cols).max(1);
        let mut total = 0usize;

        for sub in records.chunks(max_rows) {
            let sql = insert_statement(&self.table_ref(), &present, sub.len());
            let mut q = sqlx::query(&sql);
            for record in sub {
                let obj = record.as_object().ok_or_else(|| {
                    FaucetError::Sink("redshift: insert requires JSON object records".into())
                })?;
                for col in &present {
                    q = bind_json(q, obj.get(col));
                }
            }
            q.execute(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("redshift: INSERT failed: {e}")))?;
            total += sub.len();
        }
        Ok(total)
    }
}

/// Bind one JSON value onto a `sqlx` query as a native scalar type (so it lands
/// in a typed Redshift column instead of being coerced from `jsonb`). Missing /
/// null binds SQL NULL.
fn bind_json<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    v: Option<&Value>,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match v {
        None | Some(Value::Null) => query.bind(None::<String>),
        Some(Value::String(s)) => query.bind(s.clone()),
        Some(Value::Bool(b)) => query.bind(*b),
        Some(Value::Number(n)) => {
            if n.is_i64() {
                query.bind(n.as_i64().unwrap())
            } else if n.is_u64() {
                query.bind(n.as_u64().unwrap() as i64)
            } else {
                query.bind(n.as_f64().unwrap_or(0.0))
            }
        }
        Some(other) => query.bind(other.to_string()),
    }
}

#[async_trait]
impl faucet_core::Sink for RedshiftSink {
    fn connector_name(&self) -> &'static str {
        "redshift"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(RedshiftSinkConfig))
            .expect("schema serialization")
    }

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        // Append-only: Redshift has no ON CONFLICT and COPY cannot upsert.
        &[faucet_core::WriteMode::Append]
    }

    fn dataset_uri(&self) -> String {
        let table = match &self.config.schema {
            Some(s) => format!("{}.{}", s, self.config.table_name),
            None => self.config.table_name.clone(),
        };
        format!(
            "redshift://{}:{}/{}?table={}",
            self.config.connection.host,
            self.config.connection.port,
            self.config.connection.database,
            table
        )
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut total = 0;
        for chunk in chunks {
            total += match self.config.write_strategy {
                RedshiftWriteStrategy::Copy => self.copy_chunk(chunk).await?,
                RedshiftWriteStrategy::Insert => self.insert_chunk(chunk).await?,
            };
        }

        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            strategy = self.config.write_strategy.as_str(),
            "Redshift write complete"
        );
        Ok(total)
    }

    /// Preflight connectivity probe (`faucet doctor`): acquire a connection and
    /// run `SELECT 1`. Non-mutating and idempotent.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe =
            match tokio::time::timeout(ctx.timeout, sqlx::query("SELECT 1").execute(&self.pool))
                .await
            {
                Ok(Ok(_)) => Probe::pass("auth", started.elapsed()),
                Ok(Err(e)) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    e.to_string(),
                    "check host/port/database/user/credentials and that the cluster is reachable",
                ),
                Err(_) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    "timed out",
                    "check host/port/database/user/credentials and that the cluster is reachable",
                ),
            };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RedshiftCopyFormat, RedshiftWriteStrategy};
    use faucet_common_redshift::RedshiftConnection;
    use faucet_core::Sink as _;
    use serde_json::json;

    fn insert_config() -> RedshiftSinkConfig {
        RedshiftSinkConfig {
            connection: RedshiftConnection::new("host", "db", "user", "pw"),
            table_name: "events".into(),
            schema: Some("public".into()),
            write_strategy: RedshiftWriteStrategy::Insert,
            copy_format: RedshiftCopyFormat::Jsonl,
            staging_bucket: None,
            staging_prefix: String::new(),
            iam_role: None,
            region: None,
            endpoint_url: None,
            batch_size: 1000,
            max_connections: 5,
        }
    }

    fn copy_config() -> RedshiftSinkConfig {
        RedshiftSinkConfig {
            write_strategy: RedshiftWriteStrategy::Copy,
            staging_bucket: Some("stage".into()),
            staging_prefix: "rs/".into(),
            iam_role: Some("arn:aws:iam::1:role/r".into()),
            // Explicit region so building the S3 client resolves without the
            // default region provider probing IMDS (hermetic, fast tests).
            region: Some("us-east-1".into()),
            ..insert_config()
        }
    }

    async fn sink(c: RedshiftSinkConfig) -> RedshiftSink {
        RedshiftSink::new(c).await.unwrap()
    }

    #[tokio::test]
    async fn new_insert_has_no_s3_client() {
        let s = sink(insert_config()).await;
        assert!(s.s3.is_none());
    }

    #[tokio::test]
    async fn new_copy_builds_s3_client() {
        let s = sink(copy_config()).await;
        assert!(s.s3.is_some());
    }

    #[tokio::test]
    async fn new_rejects_copy_without_bucket() {
        let mut c = copy_config();
        c.staging_bucket = None;
        assert!(matches!(
            RedshiftSink::new(c).await,
            Err(FaucetError::Config(_))
        ));
    }

    #[tokio::test]
    async fn new_surfaces_unsupported_credentials() {
        let mut c = insert_config();
        c.connection.credentials = faucet_common_redshift::RedshiftCredentials::RedshiftDataApi {
            region: None,
            cluster_identifier: None,
            workgroup_name: None,
            secret_arn: None,
            db_user: None,
        };
        assert!(matches!(
            RedshiftSink::new(c).await,
            Err(FaucetError::Config(_))
        ));
    }

    #[tokio::test]
    async fn connector_name_is_redshift() {
        assert_eq!(sink(insert_config()).await.connector_name(), "redshift");
    }

    #[tokio::test]
    async fn supported_write_modes_is_append_only() {
        let s = sink(insert_config()).await;
        assert_eq!(
            s.supported_write_modes(),
            [faucet_core::WriteMode::Append].as_slice()
        );
    }

    #[tokio::test]
    async fn dataset_uri_schema_qualified() {
        let s = sink(insert_config()).await;
        assert_eq!(
            s.dataset_uri(),
            "redshift://host:5439/db?table=public.events"
        );
    }

    #[tokio::test]
    async fn config_schema_reports_required_fields() {
        let s = sink(insert_config()).await;
        let schema = s.config_schema();
        assert!(schema["properties"]["table_name"].is_object());
        let required = schema["required"].as_array().expect("required array");
        assert!(required.iter().any(|v| v == "table_name"));
    }

    #[tokio::test]
    async fn write_batch_empty_is_zero() {
        let s = sink(insert_config()).await;
        assert_eq!(s.write_batch(&[]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn table_ref_and_staging_key() {
        let s = sink(copy_config()).await;
        assert_eq!(s.table_ref(), "\"public\".\"events\"");
        let key = s.staging_key("jsonl");
        assert!(key.starts_with("rs/"));
        assert!(key.ends_with(".jsonl"));
    }

    #[test]
    fn bind_json_covers_all_scalar_kinds() {
        // Smoke: binding must not panic for every JSON scalar kind. The actual
        // wire encoding is exercised by the live integration test.
        let q = sqlx::query("SELECT $1, $2, $3, $4, $5, $6");
        let q = bind_json(q, Some(&json!("s")));
        let q = bind_json(q, Some(&json!(7)));
        let q = bind_json(q, Some(&json!(7.5)));
        let q = bind_json(q, Some(&json!(true)));
        let q = bind_json(q, Some(&Value::Null));
        let _q = bind_json(q, None);
    }
}
