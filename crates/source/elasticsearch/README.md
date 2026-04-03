# faucet-source-elasticsearch

[![Crates.io](https://img.shields.io/crates/v/faucet-source-elasticsearch.svg)](https://crates.io/crates/faucet-source-elasticsearch)
[![Docs.rs](https://docs.rs/faucet-source-elasticsearch/badge.svg)](https://docs.rs/faucet-source-elasticsearch)

An Elasticsearch source that reads documents using the search/scroll API with query DSL support, automatic scroll context management, and pluggable authentication.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-elasticsearch = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "0.2", features = ["source-elasticsearch"] }
```

## Quick Start

```rust
use faucet_source_elasticsearch::{ElasticsearchSource, ElasticsearchSourceConfig};
use faucet_core::Source;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ElasticsearchSourceConfig::new(
        "http://localhost:9200",
        "my_index",
    );

    let source = ElasticsearchSource::new(config);
    let records = source.fetch_all().await?;

    println!("Read {} documents", records.len());
    Ok(())
}
```

## How It Works

The source uses the Elasticsearch scroll API for efficient retrieval of large result sets:

1. Sends an initial `POST /{index}/_search?scroll={timeout}&size={size}` with the query
2. Extracts `_source` from each hit in `hits.hits[*]._source`
3. Continues scrolling with `POST /_search/scroll` using the `_scroll_id`
4. Stops when a scroll page returns zero hits, or `max_pages` is reached
5. Clears the scroll context (best-effort) to free server resources

## Configuration

### ElasticsearchSourceConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_url` | `String` | *(required)* | Base URL of the Elasticsearch cluster (e.g. `"http://localhost:9200"`). Trailing slash is trimmed |
| `index` | `String` | *(required)* | Index name to search |
| `query` | `Value` | `{"match_all": {}}` | Elasticsearch query DSL |
| `scroll_timeout` | `String` | `"1m"` | Scroll context timeout (e.g. `"1m"`, `"5m"`) |
| `scroll_size` | `usize` | `1000` | Number of documents per scroll page |
| `auth` | `ElasticsearchAuth` | `ElasticsearchAuth::None` | Authentication method |
| `max_pages` | `Option<usize>` | `None` | Maximum number of scroll pages to fetch. `None` means no limit |

### Authentication (ElasticsearchAuth)

| Variant | Fields | Description |
|---------|--------|-------------|
| `None` | -- | No authentication |
| `Basic { username, password }` | `String`, `String` | HTTP Basic authentication. Password is masked in debug output |
| `Bearer(String)` | token | Bearer token in the `Authorization` header. Masked in debug output |
| `ApiKey(String)` | key | API key sent as `ApiKey <key>` in the `Authorization` header. Masked in debug output |

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_elasticsearch::ElasticsearchSourceConfig;

let config: ElasticsearchSourceConfig = load_json("config.json")?;
let config: ElasticsearchSourceConfig = load_env_file(".env", "ES_SOURCE")?;
```

### Example JSON config

```json
{
  "base_url": "https://elasticsearch.example.com:9200",
  "index": "application-logs-2025.03",
  "query": {
    "bool": {
      "must": [
        { "match": { "level": "error" } },
        { "range": { "@timestamp": { "gte": "2025-03-01", "lt": "2025-04-01" } } }
      ]
    }
  },
  "scroll_timeout": "2m",
  "scroll_size": 5000,
  "auth": {
    "type": "Basic",
    "value": {
      "username": "elastic",
      "password": "changeme"
    }
  },
  "max_pages": 100
}
```

### Example .env file

```env
ES_SOURCE_BASE_URL=http://localhost:9200
ES_SOURCE_INDEX=my_index
ES_SOURCE_SCROLL_TIMEOUT=1m
ES_SOURCE_SCROLL_SIZE=1000
ES_SOURCE_MAX_PAGES=50
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let source = ElasticsearchSource::new(config);
let schema = source.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### Match-all query with defaults

```rust
use faucet_source_elasticsearch::{ElasticsearchSource, ElasticsearchSourceConfig};
use faucet_core::Source;

let config = ElasticsearchSourceConfig::new("http://localhost:9200", "products");
let source = ElasticsearchSource::new(config);
let products = source.fetch_all().await?;
```

### Filtered query with bearer auth

```rust
use faucet_source_elasticsearch::{
    ElasticsearchSource, ElasticsearchSourceConfig, ElasticsearchAuth,
};
use faucet_core::Source;
use serde_json::json;

let config = ElasticsearchSourceConfig::new(
    "https://es.production.example.com:9200",
    "orders-2025",
)
.query(json!({
    "bool": {
        "filter": [
            { "term": { "status": "completed" } },
            { "range": { "amount": { "gte": 100 } } }
        ]
    }
}))
.auth(ElasticsearchAuth::Bearer("your-token".into()))
.scroll_size(2000)
.scroll_timeout("5m")
.max_pages(50);

let source = ElasticsearchSource::new(config);
let orders = source.fetch_all().await?;
println!("Found {} matching orders", orders.len());
```

### Using API key authentication

```rust
use faucet_source_elasticsearch::{
    ElasticsearchSource, ElasticsearchSourceConfig, ElasticsearchAuth,
};
use faucet_core::Source;
use serde_json::json;

let config = ElasticsearchSourceConfig::new(
    "https://my-cluster.es.cloud.example.com:9243",
    "metrics-*",
)
.query(json!({
    "range": {
        "@timestamp": {
            "gte": "now-1h",
            "lt": "now"
        }
    }
}))
.auth(ElasticsearchAuth::ApiKey("base64-encoded-api-key".into()))
.scroll_size(5000);

let source = ElasticsearchSource::new(config);
let metrics = source.fetch_all().await?;
```

## License

Licensed under MIT or Apache-2.0.
