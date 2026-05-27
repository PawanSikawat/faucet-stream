# faucet-source-xml

[![Crates.io](https://img.shields.io/crates/v/faucet-source-xml.svg)](https://crates.io/crates/faucet-source-xml)
[![Docs.rs](https://docs.rs/faucet-source-xml/badge.svg)](https://docs.rs/faucet-source-xml)

A config-driven XML/SOAP API source with automatic XML-to-JSON conversion, dot-path record extraction, and pluggable authentication. Transient HTTP failures (5xx / connection resets) are retried with exponential backoff.

Part of the [faucet-stream](https://github.com/PawanSikawat/faucet-stream) ecosystem.

## Installation

```toml
[dependencies]
faucet-source-xml = "0.1"
tokio = { version = "1", features = ["full"] }
```

Or via the umbrella crate:
```toml
faucet-stream = { version = "0.2", features = ["source-xml"] }
```

## Quick Start

```rust
use faucet_source_xml::{XmlStream, XmlStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = XmlStreamConfig::new("https://api.example.com", "/users.xml")
        .records_element_path("Response.Users.User");

    let stream = XmlStream::new(config);
    let records = stream.fetch_all().await?;

    for record in &records {
        println!("{}", record);
    }
    Ok(())
}
```

## Configuration

### XmlStreamConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_url` | `String` | *(required)* | Base URL of the API |
| `path` | `String` | *(required)* | Request path appended to `base_url` |
| `method` | `Method` | `GET` | HTTP method (`GET` or `POST` for SOAP) |
| `auth` | `XmlAuth` | `XmlAuth::None` | Authentication method |
| `headers` | `HeaderMap` | empty | Additional request headers |
| `body` | `Option<String>` | `None` | Optional request body (e.g. SOAP envelope as raw XML string) |
| `records_element_path` | `Option<String>` | `None` | Dot-separated path to the repeating element in the XML response (e.g. `"Envelope.Body.GetUsersResponse.Users.User"`) |
| `pagination` | `Option<XmlPagination>` | `None` | Pagination configuration |
| `max_pages` | `Option<usize>` | `None` | Maximum number of pages to fetch |
| `query_params` | `HashMap<String, String>` | empty | Query parameters to include in every request |

### Authentication (XmlAuth)

| Variant | Fields | Description |
|---------|--------|-------------|
| `None` | -- | No authentication |
| `Bearer { token }` | `String` | Bearer token in the `Authorization` header |
| `Basic { username, password }` | `String`, `String` | HTTP Basic authentication |
| `Custom { headers }` | `HashMap<String, String>` | Custom headers (e.g. SOAP action headers, API keys) attached to every request |

### Pagination (XmlPagination)

| Variant | Fields | Stops When |
|---------|--------|------------|
| `PageNumber` | `param_name`, `start_page`, `page_size` (optional), `page_size_param` (optional) | Response returns zero records, or fewer records than `page_size` |
| `Offset` | `offset_param`, `limit_param`, `limit` | Fewer records returned than `limit`, or loop detected |

## Config Loading

```rust
use faucet_core::config::{load_json, load_env_file};
use faucet_source_xml::XmlStreamConfig;

let config: XmlStreamConfig = load_json("config.json")?;
let config: XmlStreamConfig = load_env_file(".env", "XML")?;
```

### Example JSON config

```json
{
  "base_url": "https://api.example.com",
  "path": "/soap/service",
  "method": "POST",
  "auth": {
    "type": "Basic",
    "username": "admin",
    "password": "secret"
  },
  "body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:web=\"http://example.com/webservice\"><soapenv:Body><web:GetUsers/></soapenv:Body></soapenv:Envelope>",
  "records_element_path": "Envelope.Body.GetUsersResponse.Users.User",
  "pagination": {
    "type": "PageNumber",
    "param_name": "page",
    "start_page": 1,
    "page_size": 100,
    "page_size_param": "pageSize"
  },
  "max_pages": 50,
  "query_params": {}
}
```

### Example .env file

```env
XML_BASE_URL=https://api.example.com
XML_PATH=/users.xml
XML_METHOD=GET
XML_MAX_PAGES=50
```

## Config Schema Introspection

```rust
use faucet_core::Source;

let stream = XmlStream::new(config);
let schema = stream.config_schema();
println!("{}", serde_json::to_string_pretty(&schema)?);
```

## Examples

### REST XML API with page-number pagination

```rust
use faucet_source_xml::{XmlStream, XmlStreamConfig, XmlPagination};

let config = XmlStreamConfig::new("https://api.example.com", "/api/products.xml")
    .records_element_path("Products.Product")
    .pagination(XmlPagination::PageNumber {
        param_name: "page".into(),
        start_page: 1,
        page_size: Some(50),
        page_size_param: Some("per_page".into()),
    })
    .max_pages(20);

let stream = XmlStream::new(config);
let products = stream.fetch_all().await?;
println!("Fetched {} products", products.len());
```

### SOAP API with custom headers

```rust
use faucet_source_xml::{XmlStream, XmlStreamConfig, XmlAuth};
use reqwest::Method;

let config = XmlStreamConfig::new("https://soap.example.com", "/ws")
    .method(Method::POST)
    .auth(XmlAuth::Basic {
        username: "admin".into(),
        password: "password".into(),
    })
    .body(r#"<?xml version="1.0"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
  <soapenv:Body>
    <GetOrders xmlns="http://example.com/orders"/>
  </soapenv:Body>
</soapenv:Envelope>"#)
    .records_element_path("Envelope.Body.GetOrdersResponse.Orders.Order");

let stream = XmlStream::new(config);
let orders = stream.fetch_all().await?;
```

### Offset-paginated XML feed

```rust
use faucet_source_xml::{XmlStream, XmlStreamConfig, XmlPagination};

let config = XmlStreamConfig::new("https://feeds.example.com", "/articles.xml")
    .records_element_path("Feed.Articles.Article")
    .pagination(XmlPagination::Offset {
        offset_param: "start".into(),
        limit_param: "count".into(),
        limit: 100,
    })
    .query_param("format", "xml");

let stream = XmlStream::new(config);
let articles = stream.fetch_all().await?;
```

## License

Licensed under MIT or Apache-2.0.
