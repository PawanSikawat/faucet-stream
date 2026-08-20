//! `response_format: csv | excel` (#497) — download a file body from an authed
//! REST endpoint and parse it into records, reusing the REST source's auth.

use faucet_source_rest::{Auth, PaginationStyle, ResponseFormat, RestStream, RestStreamConfig};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CSV: &str = "id,name,city\n1,Alice,NYC\n2,Bob,LA\n";

fn cfg(server: &MockServer, p: &str) -> RestStreamConfig {
    let mut c = RestStreamConfig::new(&server.uri(), p);
    c.pagination = PaginationStyle::None;
    c.max_pages = Some(1);
    c
}

#[tokio::test]
async fn csv_body_with_bearer_auth() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/export.csv"))
        .and(header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CSV))
        .mount(&server)
        .await;

    let mut c = cfg(&server, "/export.csv");
    c.response_format = ResponseFormat::Csv;
    c.auth = faucet_core::AuthSpec::Inline(Auth::Bearer {
        token: "tok".into(),
    });
    let stream = RestStream::new(c).unwrap();
    let recs = stream.fetch_all().await.unwrap();

    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0]["name"], "Alice");
    assert_eq!(recs[0]["city"], "NYC");
    assert_eq!(recs[1]["name"], "Bob");
}

#[tokio::test]
async fn csv_custom_delimiter_no_headers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/d.csv"))
        .respond_with(ResponseTemplate::new(200).set_body_string("1;Alice\n2;Bob\n"))
        .mount(&server)
        .await;

    let mut c = cfg(&server, "/d.csv");
    c.response_format = ResponseFormat::Csv;
    c.csv_delimiter = b';';
    c.csv_has_headers = false;
    let recs = RestStream::new(c).unwrap().fetch_all().await.unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0]["column_0"], "1");
    assert_eq!(recs[0]["column_1"], "Alice");
}

#[tokio::test]
async fn csv_paginated_config_is_rejected() {
    let server = MockServer::start().await;
    let mut c = RestStreamConfig::new(&server.uri(), "/x.csv");
    c.response_format = ResponseFormat::Csv;
    c.pagination = PaginationStyle::PageNumber {
        param_name: "page".into(),
        start_page: 1,
        page_size: None,
        page_size_param: None,
    };
    let err = RestStream::new(c).map(|_| ()).unwrap_err();
    assert!(err.to_string().contains("does not paginate"), "{err}");
}

#[tokio::test]
async fn csv_records_path_is_rejected() {
    let server = MockServer::start().await;
    let mut c = cfg(&server, "/x.csv");
    c.response_format = ResponseFormat::Csv;
    c.records_path = Some("$.data".into());
    let err = RestStream::new(c).map(|_| ()).unwrap_err();
    assert!(err.to_string().contains("records_path"), "{err}");
}

#[cfg(feature = "excel")]
#[tokio::test]
async fn excel_body_named_and_default_sheet() {
    let xlsx = include_bytes!("fixtures/sample.xlsx");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/book.xlsx"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(xlsx.to_vec()))
        .mount(&server)
        .await;

    // Default (first) sheet.
    let mut c = cfg(&server, "/book.xlsx");
    c.response_format = ResponseFormat::Excel;
    let recs = RestStream::new(c).unwrap().fetch_all().await.unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0]["id"], 1.0); // Excel numbers are floats
    assert_eq!(recs[0]["name"], "Alice");
    assert_eq!(recs[0]["active"], true);
    assert_eq!(recs[0]["score"], 9.5);

    // Named second sheet.
    let mut c = cfg(&server, "/book.xlsx");
    c.response_format = ResponseFormat::Excel;
    c.excel_sheet = Some("Extra".into());
    let recs = RestStream::new(c).unwrap().fetch_all().await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["k"], "x");
    assert_eq!(recs[0]["v"], 42.0);
}

#[tokio::test]
async fn server_error_surfaces() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/x.csv"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;
    let mut c = cfg(&server, "/x.csv");
    c.response_format = ResponseFormat::Csv;
    let err = RestStream::new(c).unwrap().fetch_all().await.unwrap_err();
    assert!(
        matches!(err, faucet_core::FaucetError::HttpStatus { status, .. } if status == 500),
        "{err}"
    );
}
