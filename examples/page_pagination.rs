//! Example: Fetching data using page-number pagination with an API key.

use faucet_stream::{Auth, PaginationStyle, RestStream, RestStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let stream = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v2/orders")
            .auth(Auth::ApiKey {
                header: "X-Api-Key".into(),
                value: "your-api-key".into(),
            })
            .records_path("$.results[*]")
            .pagination(PaginationStyle::PageNumber {
                param_name: "page".into(),
                start_page: 1,
                page_size: Some(100),
                page_size_param: Some("per_page".into()),
            }),
    )?;

    let orders = stream.fetch_all().await?;
    println!("Fetched {} orders", orders.len());
    Ok(())
}
