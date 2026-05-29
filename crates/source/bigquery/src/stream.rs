//! BigQuery query source.
//!
//! Submits the configured SQL statement via `jobs.query` and pages through
//! the result set via `jobs.getQueryResults`. The first response may carry
//! `jobComplete=false` (statement still running on the server side); the
//! source polls `getQueryResults` until BigQuery flips that flag, exactly
//! mirroring the behaviour of `gcp_bigquery_client::Client::job().query_all`
//! without giving up the row-level access we need for incremental
//! [`StreamPage`]s.

use crate::config::BigQuerySourceConfig;
use crate::convert::row_to_json;
use async_trait::async_trait;
use faucet_bigquery_common::build_client;
use faucet_core::util::substitute_context_bind_params;
use faucet_core::{FaucetError, Stream, StreamPage};
use gcp_bigquery_client::Client;
use gcp_bigquery_client::model::get_query_results_parameters::GetQueryResultsParameters;
use gcp_bigquery_client::model::query_parameter::QueryParameter;
use gcp_bigquery_client::model::query_parameter_type::QueryParameterType;
use gcp_bigquery_client::model::query_parameter_value::QueryParameterValue;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::QueryResponse;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use gcp_bigquery_client::model::table_row::TableRow;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

/// A source that runs a SQL query against BigQuery and yields rows as JSON.
pub struct BigQuerySource {
    config: BigQuerySourceConfig,
    client: Client,
}

impl BigQuerySource {
    /// Create a new BigQuery source from the given configuration.
    ///
    /// Initialises the underlying BigQuery client and exchanges credentials
    /// for an OAuth token. Returns [`FaucetError::Auth`] on credential
    /// failures.
    pub async fn new(config: BigQuerySourceConfig) -> Result<Self, FaucetError> {
        let client = build_client(&config.auth).await?;
        Ok(Self { config, client })
    }

    /// Construct a source from a pre-built BigQuery client.
    ///
    /// Low-level escape hatch for callers that build their own
    /// [`gcp_bigquery_client::Client`] — for example to target the
    /// [`bigquery-emulator`](https://github.com/goccy/bigquery-emulator) or
    /// drive a wiremock-backed test fixture. Production code should prefer
    /// [`BigQuerySource::new`], which handles credential loading.
    #[doc(hidden)]
    pub fn from_parts(config: BigQuerySourceConfig, client: Client) -> Self {
        Self { config, client }
    }

    /// Resolve the final SQL statement and ordered bind values for a given
    /// parent-record context.
    fn resolve_query(&self, context: &HashMap<String, Value>) -> (String, Vec<Value>) {
        let mut bindings = self.config.params.clone();
        let (rewritten, context_values) = if context.is_empty() {
            (self.config.query.clone(), Vec::new())
        } else {
            substitute_context_bind_params(&self.config.query, context, bindings.len() + 1, |_| {
                "?".to_string()
            })
        };
        bindings.extend(context_values);
        (rewritten, bindings)
    }

    fn build_query_request(&self, query: String, bindings: &[Value]) -> QueryRequest {
        build_query_request(&self.config, query, bindings)
    }
}

/// Free-standing version of [`BigQuerySource::build_query_request`] — kept
/// separate so unit tests can exercise it without spinning up a real
/// `gcp_bigquery_client::Client`.
fn build_query_request(
    cfg: &BigQuerySourceConfig,
    query: String,
    bindings: &[Value],
) -> QueryRequest {
    let mut req = QueryRequest::new(query);
    req.use_legacy_sql = cfg.use_legacy_sql;
    req.timeout_ms = Some(clamp_timeout_ms(cfg.statement_timeout));
    req.max_results = Some(cfg.max_results_per_page);
    if let Some(location) = &cfg.location {
        req.location = Some(location.clone());
    }

    if !bindings.is_empty() {
        req.parameter_mode = Some("POSITIONAL".to_string());
        req.query_parameters = Some(
            bindings
                .iter()
                .map(|v| QueryParameter {
                    name: None,
                    parameter_type: Some(QueryParameterType {
                        r#type: bq_param_type(v).to_string(),
                        array_type: None,
                        struct_types: None,
                    }),
                    parameter_value: Some(QueryParameterValue {
                        // BigQuery REST always carries the value as a string;
                        // the parameter_type tells the engine how to parse it.
                        // A JSON null becomes a typed NULL (value omitted).
                        value: match v {
                            Value::Null => None,
                            other => Some(stringify_param(other)),
                        },
                        array_values: None,
                        struct_values: None,
                    }),
                })
                .collect(),
        );
    }

    req
}

/// Infer the BigQuery positional-parameter type from the JSON value, so a
/// numeric or boolean bind compares correctly against a numeric/bool column
/// instead of being forced to STRING (#78/#34). Arrays / objects / null fall
/// back to STRING (stringified JSON).
fn bq_param_type(v: &Value) -> &'static str {
    match v {
        Value::Bool(_) => "BOOL",
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "INT64"
            } else {
                "FLOAT64"
            }
        }
        _ => "STRING",
    }
}

fn stringify_param(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn clamp_timeout_ms(timeout: Duration) -> i32 {
    let ms = timeout.as_millis();
    if ms > i32::MAX as u128 {
        i32::MAX
    } else {
        ms as i32
    }
}

fn schema_fields(qr: &QueryResponse) -> Vec<TableFieldSchema> {
    qr.schema
        .as_ref()
        .and_then(|s| s.fields.clone())
        .unwrap_or_default()
}

fn job_reference(qr: &QueryResponse) -> Result<(String, Option<String>), FaucetError> {
    let r = qr.job_reference.as_ref().ok_or_else(|| {
        FaucetError::Source("BigQuery query response missing jobReference".into())
    })?;
    let job_id = r
        .job_id
        .clone()
        .ok_or_else(|| FaucetError::Source("BigQuery jobReference missing jobId".into()))?;
    Ok((job_id, r.location.clone()))
}

#[async_trait]
impl faucet_core::Source for BigQuerySource {
    fn connector_name(&self) -> &'static str {
        "bigquery"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(BigQuerySourceConfig))
            .expect("schema serialization")
    }

    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (query, bindings) = self.resolve_query(context);
        let req = self.build_query_request(query, &bindings);

        let initial = self
            .client
            .job()
            .query(&self.config.project_id, req)
            .await
            .map_err(|e| FaucetError::Source(format!("BigQuery jobs.query failed: {e}")))?;

        let fields = schema_fields(&initial);
        let mut all_rows: Vec<Value> = rows_from_response(&initial, &fields);
        let mut page_token = initial.page_token.clone();
        let mut job_complete = initial.job_complete.unwrap_or(false);
        let (job_id, job_location) = job_reference(&initial)?;
        let mut fields = fields;
        let poll_timeout = self.config.poll_timeout;
        let poll_started = std::time::Instant::now();

        // Either keep polling until jobComplete, or keep paging until
        // pageToken vanishes. The two reasons we'd loop again share one
        // condition: we are not done.
        while !job_complete || page_token.is_some() {
            let params = GetQueryResultsParameters {
                page_token: page_token.clone(),
                max_results: Some(self.config.max_results_per_page),
                location: job_location.clone(),
                ..Default::default()
            };

            let resp = self
                .client
                .job()
                .get_query_results(&self.config.project_id, &job_id, params)
                .await
                .map_err(|e| {
                    FaucetError::Source(format!("BigQuery jobs.getQueryResults failed: {e}"))
                })?;

            job_complete = resp.job_complete.unwrap_or(false);
            if !job_complete {
                // `poll_timeout == 0` disables the cap (poll forever).
                if !poll_timeout.is_zero() && poll_started.elapsed() >= poll_timeout {
                    return Err(FaucetError::Source(format!(
                        "BigQuery job '{job_id}' did not complete within poll_timeout ({}s)",
                        poll_timeout.as_secs()
                    )));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }

            // Fill in the schema from the first complete page if `jobs.query`
            // returned 200 without one (happens when the statement timeout
            // fires before completion).
            if fields.is_empty()
                && let Some(s) = resp.schema.as_ref()
                && let Some(f) = s.fields.as_ref()
            {
                fields = f.clone();
            }

            for row in resp.rows.unwrap_or_default() {
                all_rows.push(row_to_json(&row, &fields));
            }
            page_token = resp.page_token;
            if page_token.is_none() {
                break;
            }
        }

        tracing::info!(
            rows = all_rows.len(),
            query = %self.config.query,
            "BigQuery source fetch complete",
        );
        Ok(all_rows)
    }

    /// Stream rows page-by-page via `jobs.getQueryResults` without
    /// buffering the full result set.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: all rows from all
    /// pages are concatenated and emitted as a single page. The source has
    /// no incremental-replication mode today, so every emitted page carries
    /// `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let (query, bindings) = self.resolve_query(context);
            let req = self.build_query_request(query, &bindings);

            let initial = self
                .client
                .job()
                .query(&self.config.project_id, req)
                .await
                .map_err(|e| FaucetError::Source(format!("BigQuery jobs.query failed: {e}")))?;

            let mut fields = schema_fields(&initial);
            let mut buffer: Vec<Value> = if batch_size == 0 {
                Vec::with_capacity(1024)
            } else {
                Vec::with_capacity(batch_size)
            };
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };

            for row in rows_from_response_owned(&initial, &fields) {
                buffer.push(row);
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(chunk));
                    yield StreamPage { records: page, bookmark: None };
                }
            }

            let mut job_complete = initial.job_complete.unwrap_or(false);
            let mut page_token = initial.page_token.clone();

            // If the first response was incomplete, we have to know the job id
            // to keep polling. If it was complete but had no further token,
            // we're done after emitting the first batch.
            let (job_id, job_location) = job_reference(&initial)?;
            let poll_timeout = self.config.poll_timeout;
            let poll_started = std::time::Instant::now();

            while !job_complete || page_token.is_some() {
                let params = GetQueryResultsParameters {
                    page_token: page_token.clone(),
                    max_results: Some(self.config.max_results_per_page),
                    location: job_location.clone(),
                    ..Default::default()
                };

                let resp = self
                    .client
                    .job()
                    .get_query_results(&self.config.project_id, &job_id, params)
                    .await
                    .map_err(|e| {
                        FaucetError::Source(format!("BigQuery jobs.getQueryResults failed: {e}"))
                    })?;

                job_complete = resp.job_complete.unwrap_or(false);
                if !job_complete {
                    // `poll_timeout == 0` disables the cap (poll forever).
                    if !poll_timeout.is_zero() && poll_started.elapsed() >= poll_timeout {
                        Err(FaucetError::Source(format!(
                            "BigQuery job '{job_id}' did not complete within poll_timeout ({}s)",
                            poll_timeout.as_secs()
                        )))?;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }

                if fields.is_empty()
                    && let Some(s) = resp.schema.as_ref()
                    && let Some(f) = s.fields.as_ref()
                {
                    fields = f.clone();
                }

                for row in resp.rows.unwrap_or_default() {
                    buffer.push(row_to_json(&row, &fields));
                    if buffer.len() >= chunk {
                        let page = std::mem::replace(&mut buffer, Vec::with_capacity(chunk));
                        yield StreamPage { records: page, bookmark: None };
                    }
                }
                page_token = resp.page_token;
                if page_token.is_none() {
                    break;
                }
            }

            if !buffer.is_empty() {
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                batch_size,
                query = %self.config.query,
                "BigQuery source stream complete",
            );
        })
    }
}

/// Borrow-based row extraction (used by `fetch_with_context`, which collects
/// into a `Vec` anyway).
fn rows_from_response(resp: &QueryResponse, fields: &[TableFieldSchema]) -> Vec<Value> {
    resp.rows
        .as_ref()
        .map(|rows| rows.iter().map(|r| row_to_json(r, fields)).collect())
        .unwrap_or_default()
}

/// Owned-iteration variant — clones each row out of the response so the
/// streaming loop above doesn't have to keep a borrow open across yields.
fn rows_from_response_owned(resp: &QueryResponse, fields: &[TableFieldSchema]) -> Vec<Value> {
    let rows: &Vec<TableRow> = match resp.rows.as_ref() {
        Some(r) => r,
        None => return Vec::new(),
    };
    rows.iter().map(|r| row_to_json(r, fields)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BigQueryCredentials;
    use serde_json::json;

    fn cfg() -> BigQuerySourceConfig {
        BigQuerySourceConfig::new(
            "my-project",
            BigQueryCredentials::ApplicationDefault,
            "SELECT id FROM events",
        )
    }

    #[test]
    fn stringify_param_passes_strings_unquoted() {
        assert_eq!(stringify_param(&json!("us-east")), "us-east");
        assert_eq!(stringify_param(&json!(42)), "42");
        assert_eq!(stringify_param(&json!(true)), "true");
    }

    #[test]
    fn clamp_timeout_ms_handles_overflow() {
        assert_eq!(clamp_timeout_ms(Duration::from_secs(1)), 1000);
        assert_eq!(clamp_timeout_ms(Duration::from_secs(u64::MAX)), i32::MAX);
    }

    #[test]
    fn build_request_no_params_omits_query_parameters() {
        let c = cfg();
        let req = build_query_request(&c, "SELECT id".to_string(), &[]);
        assert_eq!(req.query, "SELECT id");
        assert!(req.query_parameters.is_none());
        assert!(req.parameter_mode.is_none());
        assert!(!req.use_legacy_sql);
        assert_eq!(req.max_results, Some(1000));
    }

    #[test]
    fn build_request_with_params_uses_positional_string_binds() {
        let c = cfg().with_params(vec![json!("us-east"), json!(42)]);
        let req = build_query_request(&c, "SELECT * WHERE r = ? AND n > ?".to_string(), &c.params);
        assert_eq!(req.parameter_mode.as_deref(), Some("POSITIONAL"));
        let params = req.query_parameters.as_ref().unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].parameter_type.as_ref().unwrap().r#type, "STRING");
        assert_eq!(
            params[0].parameter_value.as_ref().unwrap().value.as_deref(),
            Some("us-east")
        );
        assert_eq!(
            params[1].parameter_value.as_ref().unwrap().value.as_deref(),
            Some("42")
        );
    }

    #[test]
    fn build_request_propagates_location_and_legacy_flag() {
        let c = cfg()
            .with_location("EU")
            .with_use_legacy_sql(true)
            .with_max_results_per_page(250);
        let req = build_query_request(&c, "SELECT 1".to_string(), &[]);
        assert!(req.use_legacy_sql);
        assert_eq!(req.location.as_deref(), Some("EU"));
        assert_eq!(req.max_results, Some(250));
    }

    #[test]
    fn resolve_query_substitutes_context_with_positional_markers() {
        // Test resolve_query without needing a Client by mimicking its core.
        let c = cfg();
        let mut bindings = c.params.clone();
        let mut ctx = HashMap::new();
        ctx.insert("parent.id".to_string(), json!(7));
        let (rewritten, extra) = substitute_context_bind_params(
            "SELECT * FROM t WHERE id = {parent.id}",
            &ctx,
            bindings.len() + 1,
            |_| "?".to_string(),
        );
        bindings.extend(extra);
        assert_eq!(rewritten, "SELECT * FROM t WHERE id = ?");
        assert_eq!(bindings, vec![json!(7)]);
    }
}
