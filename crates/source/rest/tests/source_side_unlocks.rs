//! Integration tests for the source-side unlocks: OData mode (#512),
//! server-side incremental push-down (#513), and flow-provider request-auth
//! placement / dynamic base-URL (#511).

use std::collections::BTreeMap;
use std::sync::Arc;

use faucet_core::{
    AuthProvider, Credential, CredentialPlacement, FaucetError, ReplicationBind, ReplicationMethod,
    RequestAuth, SharedAuthProvider, Source,
};
use faucet_source_rest::{ODataConfig, ODataVersion, RestStream, RestStreamConfig};
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ── #512 OData ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn odata_follows_nextlink_and_unwraps_value_envelope() {
    let server = MockServer::start().await;
    let page2 = format!("{}/Orders?page=2", server.uri());
    // Page 1: `$.value` records + a `@odata.nextLink` to page 2, and the derived
    // `$select` + `Prefer` should be present.
    Mock::given(method("GET"))
        .and(path("/Orders"))
        .and(query_param("$select", "DocEntry,DocDate"))
        .and(header("prefer", "odata.maxpagesize=2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [ {"DocEntry": 1}, {"DocEntry": 2} ],
            "@odata.nextLink": page2,
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/Orders"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "value": [ {"DocEntry": 3} ],
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(RestStreamConfig::new(&server.uri(), "").odata(ODataConfig {
        version: ODataVersion::V4,
        entity: Some("Orders".to_owned()),
        select: vec!["DocEntry".to_owned(), "DocDate".to_owned()],
        page_size: Some(2),
        ..Default::default()
    }))
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[2]["DocEntry"], 3);
}

#[tokio::test]
async fn odata_discover_parses_metadata() {
    let server = MockServer::start().await;
    let edmx = r#"<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
      <Schema Namespace="S">
        <EntityType Name="Order">
          <Key><PropertyRef Name="DocEntry"/></Key>
          <Property Name="DocEntry" Type="Edm.Int32" Nullable="false"/>
          <Property Name="DocDate" Type="Edm.DateTimeOffset"/>
        </EntityType>
        <EntityContainer Name="C">
          <EntitySet Name="Orders" EntityType="S.Order"/>
        </EntityContainer>
      </Schema>
    </edmx:Edmx>"#;
    Mock::given(method("GET"))
        .and(path("/$metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_string(edmx))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "").odata(ODataConfig {
            entity: Some("Orders".to_owned()),
            ..Default::default()
        }),
    )
    .unwrap();

    assert!(stream.supports_discover());
    let datasets = stream.discover().await.unwrap();
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].name, "Orders");
    assert_eq!(datasets[0].config_patch, json!({"odata": {"entity": "Orders"}}));
    assert_eq!(
        datasets[0].schema.as_ref().unwrap()["properties"]["DocEntry"]["type"],
        "integer"
    );
}

// ── #513 server-side incremental push-down ─────────────────────────────────────

#[tokio::test]
async fn bind_pushes_bookmark_into_query_and_advances() {
    let server = MockServer::start().await;
    // The mock only matches when the rendered bookmark is pushed down as
    // `?updated_after=gte|2024-06-01T00:00:00Z`.
    Mock::given(method("GET"))
        .and(path("/events"))
        .and(query_param("updated_after", "gte|2024-06-01T00:00:00Z"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [ {"id": 1, "updated_at": "2024-07-01T00:00:00Z"} ],
            "max_updated": "2024-07-15T00:00:00Z",
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/events")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-06-01"))
            .replication_bind(ReplicationBind {
                into: faucet_core::BindTarget::Query,
                name: "updated_after".to_owned(),
                template: "gte|${bookmark}".to_owned(),
                format: faucet_core::BindFormat::Iso8601,
                advance_from: Some("$.max_updated".to_owned()),
            }),
    )
    .unwrap();

    let ctx = std::collections::HashMap::new();
    let mut pages = Vec::new();
    {
        use futures::StreamExt;
        let mut s = Source::stream_pages(&stream, &ctx, 0);
        while let Some(p) = s.next().await {
            pages.push(p.unwrap());
        }
    }
    let records: Vec<_> = pages.iter().flat_map(|p| p.records.clone()).collect();
    assert_eq!(records.len(), 1);
    // advance_from pulled the next bookmark from `$.max_updated`, not max(record).
    let bookmark = pages.iter().rev().find_map(|p| p.bookmark.clone()).unwrap();
    assert_eq!(bookmark, json!("2024-07-15T00:00:00Z"));
}

// ── #511 flow-provider request-auth placement + dynamic base-URL ────────────────

#[derive(Debug)]
struct MockFlow {
    base_url: String,
}

#[async_trait::async_trait]
impl AuthProvider for MockFlow {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Err(FaucetError::Auth("query-placed; use request_auth".into()))
    }
    async fn request_auth(
        &self,
        _method: &str,
        _url: &str,
        _query: &BTreeMap<String, String>,
    ) -> Result<RequestAuth, FaucetError> {
        Ok(RequestAuth::new()
            .with_placement(CredentialPlacement::Query {
                name: "BhRestToken".to_owned(),
                value: "SESSION".to_owned(),
            })
            .with_base_url(self.base_url.clone()))
    }
    fn provider_name(&self) -> &'static str {
        "mock-flow"
    }
}

#[tokio::test]
async fn flow_provider_places_query_token_and_overrides_base_url() {
    // The data server is the base-URL the provider captures; the config points at
    // a decoy base-URL to prove the dynamic override wins.
    let data = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/orders"))
        .and(query_param("BhRestToken", "SESSION"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"value": [{"id": 7}]})))
        .mount(&data)
        .await;

    let provider: SharedAuthProvider = Arc::new(MockFlow {
        base_url: data.uri(),
    });
    let stream = RestStream::new(
        RestStreamConfig::new("https://decoy.invalid", "/orders").records_path("$.value[*]"),
    )
    .unwrap()
    .with_auth_provider(provider);

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 7);
}
