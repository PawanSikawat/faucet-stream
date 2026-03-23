//! Example: Fetching paginated data using cursor-based pagination.

use faucet_stream::{Auth, PaginationStyle, RestStream, RestStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let stream = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/users")
            .auth(Auth::Bearer("your-token-here".into()))
            .records_path("$.data[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.meta.next_cursor".into(),
                param_name: "cursor".into(),
            })
            .max_pages(10),
    )?;

    let users = stream.fetch_all().await?;
    println!("Fetched {} users", users.len());

    for user in &users[..users.len().min(5)] {
        println!("  - {}", user);
    }

    Ok(())
}
