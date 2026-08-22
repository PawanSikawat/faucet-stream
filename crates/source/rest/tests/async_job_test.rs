//! Integration test for the async-job source pattern (#514):
//! submit → poll (not-ready → ready) → fetch → decode.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_source_rest::{
    AsyncJobConfig, DecodeStep, ParseFormat, ParseSpec, RestStream, RestStreamConfig,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

/// Poll responds "InProgress" once, then "Complete".
struct PollThenComplete(Arc<AtomicUsize>);
impl Respond for PollThenComplete {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        let state = if n == 0 { "InProgress" } else { "Complete" };
        ResponseTemplate::new(200).set_body_json(json!({ "state": state }))
    }
}

#[tokio::test]
async fn async_job_submit_poll_fetch_decode() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "job-1" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1"))
        .respond_with(PollThenComplete(Arc::new(AtomicUsize::new(0))))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1/result"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id,name\n1,alice\n2,bob\n"))
        .mount(&server)
        .await;

    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs", "json": { "query": "SELECT 1" } },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": { "method": "GET", "url": "/jobs/${job_id}/result" }
    }))
    .unwrap();

    let mut cfg = RestStreamConfig::new(&server.uri(), "").decode(vec![DecodeStep::Parse {
        parse: ParseSpec {
            format: ParseFormat::Csv,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        },
    }]);
    cfg.async_job = Some(async_job);

    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], "1");
    assert_eq!(records[1]["name"], "bob");
}

/// Poll responds "pending" once, then "succeeded" carrying the download URL in
/// the body (the Stripe report-run shape).
struct StripeReportPoll {
    calls: Arc<AtomicUsize>,
    download_url: String,
}
impl Respond for StripeReportPoll {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let body = if n == 0 {
            json!({ "status": "pending" })
        } else {
            json!({ "status": "succeeded", "result": { "url": self.download_url } })
        };
        ResponseTemplate::new(200).set_body_json(body)
    }
}

#[tokio::test]
async fn async_job_fetch_url_from_poll_body() {
    let server = MockServer::start().await;
    // Absolute one-time download URL returned in the poll body.
    let download_url = format!("{}/files/report-abc.csv", server.uri());

    Mock::given(method("POST"))
        .and(path("/v1/reporting/report_runs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "frr_1" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/reporting/report_runs/frr_1"))
        .respond_with(StripeReportPoll {
            calls: Arc::new(AtomicUsize::new(0)),
            download_url: download_url.clone(),
        })
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/report-abc.csv"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id,name\n1,alice\n2,bob\n"))
        .mount(&server)
        .await;

    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/v1/reporting/report_runs" },
        "job_id": "$.id",
        "poll": {
            "url": "/v1/reporting/report_runs/${job_id}",
            "interval_secs": 0,
            "timeout_secs": 30
        },
        "status": { "path": "$.status", "success": ["succeeded"], "failure": ["failed"] },
        "fetch": { "url_from": "$.result.url" }
    }))
    .unwrap();

    let mut cfg = RestStreamConfig::new(&server.uri(), "").decode(vec![DecodeStep::Parse {
        parse: ParseSpec {
            format: ParseFormat::Csv,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        },
    }]);
    cfg.async_job = Some(async_job);

    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], "1");
    assert_eq!(records[1]["name"], "bob");
}

#[tokio::test]
async fn async_job_url_from_no_match_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "j" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j"))
        // Terminal, but no `result.url` in the body.
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "succeeded" })))
        .mount(&server)
        .await;

    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs" },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 5 },
        "status": { "path": "$.status", "success": ["succeeded"] },
        "fetch": { "url_from": "$.result.url" }
    }))
    .unwrap();
    let mut cfg = RestStreamConfig::new(&server.uri(), "");
    cfg.async_job = Some(async_job);
    let err = RestStream::new(cfg).unwrap().fetch_all().await.unwrap_err();
    assert!(err.to_string().contains("url_from"), "{err}");
}

#[tokio::test]
async fn async_job_failure_status_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "j" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "Failed" })))
        .mount(&server)
        .await;

    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs" },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 5 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": { "url": "/jobs/${job_id}/result" }
    }))
    .unwrap();
    let mut cfg = RestStreamConfig::new(&server.uri(), "");
    cfg.async_job = Some(async_job);
    let err = RestStream::new(cfg).unwrap().fetch_all().await.unwrap_err();
    assert!(err.to_string().contains("Failed"), "{err}");
}
