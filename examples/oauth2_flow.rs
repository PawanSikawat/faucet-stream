//! Example: OAuth2 client credentials flow, then fetching data.

use faucet_stream::{Auth, PaginationStyle, RestStream, RestStreamConfig, fetch_oauth2_token};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let token = fetch_oauth2_token(
        "https://auth.example.com/oauth/token",
        "your-client-id",
        "your-client-secret",
        &["read:data".into()],
    )
    .await?;

    println!("Got OAuth2 token: {}...", &token[..20.min(token.len())]);

    let stream = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/resources")
            .auth(Auth::Bearer(token))
            .records_path("$.data[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.pagination.next".into(),
                param_name: "after".into(),
            }),
    )?;

    let resources = stream.fetch_all().await?;
    println!("Fetched {} resources", resources.len());
    Ok(())
}
