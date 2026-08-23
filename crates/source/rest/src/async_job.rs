//! Async-job source pattern (#514): submit → poll → fetch result.
//!
//! Covers the "big-data export / bulk / report-run" class of APIs that a
//! paginated GET can't express (Salesforce Bulk, Stripe Reporting, warehouse
//! UNLOAD, …). Configured as an `async_job:` block on the REST source; the
//! fetched result is handed to the `decode:` pipeline (#515) or the normal
//! body parsing.
//!
//! ```yaml
//! async_job:
//!   submit: { method: POST, url: "/jobs", json: { query: "SELECT ..." } }
//!   job_id: "$.id"
//!   poll:   { url: "/jobs/${job_id}", interval_secs: 5, timeout_secs: 1800 }
//!   status: { path: "$.state", success: [JobComplete], failure: [Failed, Aborted] }
//!   fetch:  { url: "/jobs/${job_id}/result" }
//! decode:
//!   - parse: { format: csv }
//! ```

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

fn default_get() -> String {
    "GET".to_owned()
}
fn default_interval() -> u64 {
    5
}
fn default_timeout() -> u64 {
    1800
}

/// One HTTP request in a job lifecycle (submit / fetch).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobRequest {
    /// HTTP method (default `GET`; set `POST` for `submit`).
    #[serde(default = "default_get")]
    pub method: String,
    /// URL — absolute, or a `base_url`-relative path. `${job_id}` is substituted.
    ///
    /// Required for `submit`. For `fetch`, set **exactly one** of `url` (a fixed
    /// template) or [`url_from`](Self::url_from) (a JSONPath into the poll body).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// `fetch` only (#543): resolve the download URL from the **last poll
    /// response body** via JSONPath, instead of rendering [`url`](Self::url).
    /// For APIs that return a one-time signed download link in the poll body
    /// (e.g. a Stripe report run's `result.url`) rather than at a deterministic
    /// `/{job_id}` path. The matched value must be a string; an absolute URL is
    /// used verbatim, a relative one is resolved against `base_url`. Mutually
    /// exclusive with [`url`](Self::url).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url_from: Option<String>,
    /// Extra headers.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// Extra query params.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub query: HashMap<String, String>,
    /// JSON request body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json: Option<Value>,
    /// `fetch` only (#557): result-set continuation. Response header carrying a
    /// pagination locator (e.g. Salesforce Bulk `Sforce-Locator`). While present
    /// (and not empty / `"null"`), the fetch is repeated with the locator sent as
    /// [`locator_param`](Self::locator_param), appending records across pages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator_header: Option<String>,
    /// `fetch` only (#557): JSONPath into the fetch response **body** for the
    /// continuation locator, when it rides the body rather than a header.
    /// Alternative to [`locator_header`](Self::locator_header).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator_body: Option<String>,
    /// `fetch` only (#557): query-param name the locator is sent as on each
    /// continuation request. Required when a locator source is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator_param: Option<String>,
    /// `fetch` only (#557): JSONPath for extracting records from each fetch page,
    /// overriding the source-level `records_path`. Applies to a JSON result body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records_path: Option<String>,
}

/// The poll request + cadence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PollSpec {
    /// HTTP method (default `GET`).
    #[serde(default = "default_get")]
    pub method: String,
    /// Status URL — absolute or `base_url`-relative; `${job_id}` substituted.
    pub url: String,
    /// Extra headers.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// Extra query params.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub query: HashMap<String, String>,
    /// Seconds between polls (default `5`).
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    /// Give up after this many seconds (default `1800`).
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

/// How to read the job's terminal state from a poll response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobStatus {
    /// JSONPath to the status value in the poll response.
    pub path: String,
    /// Status values meaning "done — go fetch".
    pub success: Vec<String>,
    /// Status values meaning "failed — abort".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failure: Vec<String>,
}

/// Terminal classification of a poll status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOutcome {
    /// Ready to fetch.
    Success,
    /// Failed / aborted.
    Failure,
    /// Not terminal yet — keep polling.
    Pending,
}

impl JobStatus {
    /// Classify a poll's status value.
    pub fn classify(&self, status: &str) -> JobOutcome {
        if self.success.iter().any(|s| s == status) {
            JobOutcome::Success
        } else if self.failure.iter().any(|s| s == status) {
            JobOutcome::Failure
        } else {
            JobOutcome::Pending
        }
    }
}

/// The `async_job:` config block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AsyncJobConfig {
    /// Job-creation request.
    pub submit: JobRequest,
    /// JSONPath to the job id in the submit response.
    pub job_id: String,
    /// Status polling.
    pub poll: PollSpec,
    /// Terminal-state classification.
    pub status: JobStatus,
    /// Result-download request.
    pub fetch: JobRequest,
}

impl AsyncJobConfig {
    /// Validate the block at config-load time.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        // `submit` needs a fixed `url`; `url_from` is meaningless there (no poll
        // body exists yet).
        if self.submit.url_from.is_some() {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `submit.url_from` is not supported — `submit` needs a fixed `url`"
                    .into(),
            ));
        }
        if self.submit.url.as_deref().unwrap_or("").trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `submit.url` must not be empty".into(),
            ));
        }
        // `fetch` needs exactly one of `url` (templated) or `url_from` (JSONPath
        // into the poll body, #543).
        let fetch_url = self
            .fetch
            .url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let fetch_url_from = self
            .fetch
            .url_from
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        match (fetch_url, fetch_url_from) {
            (Some(_), Some(_)) => {
                return Err(faucet_core::FaucetError::Config(
                    "async_job: set exactly one of `fetch.url` or `fetch.url_from`, not both"
                        .into(),
                ));
            }
            (None, None) => {
                return Err(faucet_core::FaucetError::Config(
                    "async_job: `fetch` requires exactly one of `url` (templated) or `url_from` \
                     (a JSONPath into the poll response body)"
                        .into(),
                ));
            }
            _ => {}
        }
        if self.job_id.trim().is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `job_id` (JSONPath) must not be empty".into(),
            ));
        }
        if self.status.success.is_empty() {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `status.success` must list at least one terminal value".into(),
            ));
        }
        if self.poll.interval_secs == 0 && self.poll.timeout_secs == 0 {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `poll.timeout_secs` must be > 0".into(),
            ));
        }
        // #557: result-set continuation (locator paging) is a `fetch`-only
        // feature and needs a `locator_param` to request the next page.
        if self.submit.locator_header.is_some()
            || self.submit.locator_body.is_some()
            || self.submit.locator_param.is_some()
            || self.submit.records_path.is_some()
        {
            return Err(faucet_core::FaucetError::Config(
                "async_job: locator/`records_path` fields are `fetch`-only, not valid on `submit`"
                    .into(),
            ));
        }
        let has_locator_source =
            self.fetch.locator_header.is_some() || self.fetch.locator_body.is_some();
        if has_locator_source
            && self
                .fetch
                .locator_param
                .as_deref()
                .unwrap_or("")
                .trim()
                .is_empty()
        {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `fetch.locator_param` is required when a `locator_header` or \
                 `locator_body` is configured (it names the query param the locator is sent as)"
                    .into(),
            ));
        }
        if self.fetch.locator_param.is_some() && !has_locator_source {
            return Err(faucet_core::FaucetError::Config(
                "async_job: `fetch.locator_param` needs a `locator_header` or `locator_body` to \
                 read the locator from"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// Substitute `${job_id}` in a URL/template.
pub fn substitute_job_id(template: &str, job_id: &str) -> String {
    template.replace("${job_id}", job_id)
}

/// Resolve a possibly-relative URL against `base_url`.
pub fn resolve_url(base_url: &str, url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_string()
    } else {
        format!(
            "{}/{}",
            base_url.trim_end_matches('/'),
            url.trim_start_matches('/')
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classify_maps_status_to_outcome() {
        let s = JobStatus {
            path: "$.state".into(),
            success: vec!["JobComplete".into()],
            failure: vec!["Failed".into(), "Aborted".into()],
        };
        assert_eq!(s.classify("JobComplete"), JobOutcome::Success);
        assert_eq!(s.classify("Failed"), JobOutcome::Failure);
        assert_eq!(s.classify("Aborted"), JobOutcome::Failure);
        assert_eq!(s.classify("InProgress"), JobOutcome::Pending);
    }

    #[test]
    fn substitute_and_resolve_urls() {
        assert_eq!(substitute_job_id("/jobs/${job_id}/x", "42"), "/jobs/42/x");
        assert_eq!(resolve_url("https://h", "/jobs"), "https://h/jobs");
        assert_eq!(resolve_url("https://h/", "jobs"), "https://h/jobs");
        assert_eq!(
            resolve_url("https://h", "https://other/x"),
            "https://other/x"
        );
    }

    #[test]
    fn validate_rejects_empty_and_no_success() {
        let base: AsyncJobConfig = serde_json::from_value(json!({
            "submit": { "url": "/jobs" },
            "job_id": "$.id",
            "poll": { "url": "/jobs/${job_id}" },
            "status": { "path": "$.state", "success": ["Done"] },
            "fetch": { "url": "/jobs/${job_id}/result", "method": "GET" }
        }))
        .unwrap();
        assert!(base.validate().is_ok());

        let mut no_success = base.clone();
        no_success.status.success.clear();
        assert!(no_success.validate().is_err());

        let mut empty_id = base.clone();
        empty_id.job_id = " ".into();
        assert!(empty_id.validate().is_err());
    }

    #[test]
    fn validate_fetch_url_xor_url_from() {
        let make = |fetch: Value| -> AsyncJobConfig {
            serde_json::from_value(json!({
                "submit": { "method": "POST", "url": "/jobs" },
                "job_id": "$.id",
                "poll": { "url": "/jobs/${job_id}" },
                "status": { "path": "$.state", "success": ["Done"] },
                "fetch": fetch
            }))
            .unwrap()
        };

        // Exactly one → ok.
        assert!(
            make(json!({ "url": "/jobs/${job_id}/result" }))
                .validate()
                .is_ok()
        );
        assert!(
            make(json!({ "url_from": "$.result.url" }))
                .validate()
                .is_ok()
        );

        // Both → error.
        let both = make(json!({ "url": "/r", "url_from": "$.result.url" }));
        let err = both.validate().unwrap_err();
        assert!(
            matches!(err, faucet_core::FaucetError::Config(_)),
            "{err:?}"
        );
        assert!(err.to_string().contains("exactly one"), "{err}");

        // Neither → error.
        let neither = make(json!({}));
        assert!(neither.validate().is_err());

        // Empty strings count as unset → neither → error.
        let empty = make(json!({ "url": "  " }));
        assert!(empty.validate().is_err());
    }

    #[test]
    fn validate_rejects_url_from_on_submit() {
        let cfg: AsyncJobConfig = serde_json::from_value(json!({
            "submit": { "method": "POST", "url": "/jobs", "url_from": "$.x" },
            "job_id": "$.id",
            "poll": { "url": "/jobs/${job_id}" },
            "status": { "path": "$.state", "success": ["Done"] },
            "fetch": { "url_from": "$.result.url" }
        }))
        .unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("submit.url_from"), "{err}");
    }

    #[test]
    fn validate_locator_continuation_fields() {
        let make = |fetch: Value| -> AsyncJobConfig {
            serde_json::from_value(json!({
                "submit": { "method": "POST", "url": "/jobs" },
                "job_id": "$.id",
                "poll": { "url": "/jobs/${job_id}" },
                "status": { "path": "$.state", "success": ["Done"] },
                "fetch": fetch
            }))
            .unwrap()
        };

        // Header locator + param → ok.
        assert!(
            make(json!({
                "url": "/jobs/${job_id}/results",
                "locator_header": "Sforce-Locator",
                "locator_param": "locator",
                "records_path": "$.records[*]"
            }))
            .validate()
            .is_ok()
        );
        // Body locator + param → ok.
        assert!(
            make(json!({
                "url_from": "$.result.url",
                "locator_body": "$.next_locator",
                "locator_param": "locator"
            }))
            .validate()
            .is_ok()
        );
        // Locator source without param → error.
        let err = make(json!({
            "url": "/r",
            "locator_header": "Sforce-Locator"
        }))
        .validate()
        .unwrap_err();
        assert!(err.to_string().contains("locator_param"), "{err}");
        // Param without a source → error.
        assert!(
            make(json!({ "url": "/r", "locator_param": "locator" }))
                .validate()
                .is_err()
        );
    }

    #[test]
    fn validate_rejects_locator_on_submit() {
        let cfg: AsyncJobConfig = serde_json::from_value(json!({
            "submit": { "method": "POST", "url": "/jobs", "locator_header": "X" },
            "job_id": "$.id",
            "poll": { "url": "/jobs/${job_id}" },
            "status": { "path": "$.state", "success": ["Done"] },
            "fetch": { "url": "/r" }
        }))
        .unwrap();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn poll_defaults_apply() {
        let cfg: AsyncJobConfig = serde_json::from_value(json!({
            "submit": { "url": "/jobs" },
            "job_id": "$.id",
            "poll": { "url": "/jobs/${job_id}" },
            "status": { "path": "$.state", "success": ["Done"] },
            "fetch": { "url": "/r" }
        }))
        .unwrap();
        assert_eq!(cfg.poll.interval_secs, 5);
        assert_eq!(cfg.poll.timeout_secs, 1800);
        assert_eq!(cfg.poll.method, "GET");
        assert_eq!(cfg.submit.method, "GET"); // default; examples set POST explicitly
        assert_eq!(cfg.fetch.method, "GET");
    }
}
