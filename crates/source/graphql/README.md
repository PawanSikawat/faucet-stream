# faucet-source-graphql

[![Crates.io](https://img.shields.io/crates/v/faucet-source-graphql.svg)](https://crates.io/crates/faucet-source-graphql)
[![Docs.rs](https://docs.rs/faucet-source-graphql/badge.svg)](https://docs.rs/faucet-source-graphql)

A config-driven GraphQL API source with cursor-based pagination, JSONPath record extraction, and pluggable authentication.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-graphql = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "0.2", features = ["source-graphql"] }
```

## Quick Start

```rust
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = GraphqlStreamConfig::new(
        "https://api.example.com/graphql",
        "query { users { id name email } }",
    )
    .records_path("$.data.users[*]");

    let stream = GraphqlStream::new(config);
    let records = stream.fetch_all().await?;

    for record in &records {
        println!("{}", record);
    }
    Ok(())
}
```

## Configuration

### GraphqlStreamConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | `String` | *(required)* | GraphQL endpoint URL |
| `query` | `String` | *(required)* | The GraphQL query string |
| `variables` | `Value` | `{}` | Variables to pass with the query |
| `auth` | `GraphqlAuth` | `GraphqlAuth::None` | Authentication method |
| `headers` | `HeaderMap` | empty | Additional request headers |
| `records_path` | `Option<String>` | `None` | JSONPath expression to extract records from the response. When `None`, the `data` field of the response is returned as a single record |
| `pagination` | `Option<GraphqlPagination>` | `None` | Pagination configuration. `None` for single-page queries |
| `max_pages` | `Option<usize>` | `None` | Maximum number of pages to fetch |

### Authentication (GraphqlAuth)

| Variant | Description |
|---------|-------------|
| `None` | No authentication |
| `Bearer(String)` | Bearer token in the `Authorization` header |
| `Custom(HeaderMap)` | Custom headers (e.g. API keys, cookies). Not serializable |

### Pagination (GraphqlPagination)

Cursor-based pagination following the Relay specification with `pageInfo { hasNextPage, endCursor }`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `has_next_page_path` | `String` | `"$.data.*.pageInfo.hasNextPage"` | JSONPath to the `hasNextPage` boolean in the response |
| `cursor_path` | `String` | `"$.data.*.pageInfo.endCursor"` | JSONPath to the `endCursor` string in the response |
| `cursor_variable` | `String` | `"after"` | Name of the cursor variable in the GraphQL query |
| `page_size` | `Option<usize>` | `None` | Optional page size, injected as a variable |
| `page_size_variable` | `String` | `"first"` | Name of the page size variable |

Pagination includes loop detection -- if the same cursor is returned twice in a row, pagination stops.

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_graphql::GraphqlStreamConfig;

let config: GraphqlStreamConfig = load_json("config.json")?;
let config: GraphqlStreamConfig = load_env_file(".env", "GRAPHQL")?;
```

### Example JSON config

```json
{
  "endpoint": "https://api.github.com/graphql",
  "query": "query($org: String!, $first: Int!, $after: String) { organization(login: $org) { repositories(first: $first, after: $after) { edges { node { name stargazerCount } } pageInfo { hasNextPage endCursor } } } }",
  "variables": {
    "org": "PawanSikawat"
  },
  "auth": {
    "type": "Bearer",
    "value": "ghp_xxxxxxxxxxxx"
  },
  "records_path": "$.data.organization.repositories.edges[*].node",
  "pagination": {
    "has_next_page_path": "$.data.organization.repositories.pageInfo.hasNextPage",
    "cursor_path": "$.data.organization.repositories.pageInfo.endCursor",
    "cursor_variable": "after",
    "page_size": 50,
    "page_size_variable": "first"
  },
  "max_pages": 10
}
```

### Example .env file

```env
GRAPHQL_ENDPOINT=https://api.github.com/graphql
GRAPHQL_QUERY=query { viewer { login } }
GRAPHQL_MAX_PAGES=10
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let stream = GraphqlStream::new(config);
let schema = stream.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### Simple single-page query

```rust
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use faucet_source_graphql::config::GraphqlAuth;
use serde_json::json;

let config = GraphqlStreamConfig::new(
    "https://api.example.com/graphql",
    "query($id: ID!) { user(id: $id) { name email } }",
)
.variables(json!({"id": "user-123"}))
.auth(GraphqlAuth::Bearer("your-token".into()));

let stream = GraphqlStream::new(config);
let records = stream.fetch_all().await?;
```

### Relay cursor pagination with record extraction

```rust
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use faucet_source_graphql::config::{GraphqlAuth, GraphqlPagination};
use serde_json::json;

let config = GraphqlStreamConfig::new(
    "https://api.example.com/graphql",
    r#"
    query($first: Int!, $after: String) {
        users(first: $first, after: $after) {
            edges {
                node { id name email createdAt }
            }
            pageInfo { hasNextPage endCursor }
        }
    }
    "#,
)
.auth(GraphqlAuth::Bearer("your-token".into()))
.records_path("$.data.users.edges[*].node")
.pagination(GraphqlPagination {
    has_next_page_path: "$.data.users.pageInfo.hasNextPage".into(),
    cursor_path: "$.data.users.pageInfo.endCursor".into(),
    cursor_variable: "after".into(),
    page_size: Some(100),
    page_size_variable: "first".into(),
})
.max_pages(20);

let stream = GraphqlStream::new(config);
let users = stream.fetch_all().await?;
println!("Fetched {} users", users.len());
```

### Using with a Pipeline

```rust
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use faucet_core::{Pipeline, Source, Sink};

let source = GraphqlStream::new(config);
let pipeline = Pipeline::new(Box::new(source), Box::new(my_sink));
let result = pipeline.run().await?;
println!("Transferred {} records", result.records_written);
```

## License

Licensed under MIT or Apache-2.0.
