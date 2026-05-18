//! Parent → child `SourceDAG` — full DAG knob set.
//!
//! Fetches users, then for each user fetches that user's posts from
//! `/users/{user_id}/posts`. Demonstrates:
//!
//! - both sources configured with auth, headers, pagination, retries
//! - `context_mapping` extracting a JSONPath from each parent record
//! - `inject_context = true` so the parent's `id` is merged into each child record as `user_id`
//! - `.concurrency(...)` to cap parallel child fetches per parent
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example dag_users_posts \
//!     --features "source-rest sink-jsonl"
//! ```

use std::collections::HashMap;
use std::time::Duration;

use faucet_stream::sink::jsonl::{JsonlSink, JsonlSinkConfig};
use faucet_stream::{Auth, PaginationStyle, RestStream, RestStreamConfig, SourceDAG};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let users_source = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/users")
            .name("users")
            .auth(Auth::Bearer {
                token: std::env::var("API_TOKEN")?,
            })
            .header("X-Client", "faucet-stream")
            .query("active", "true")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::PageNumber {
                param_name: "page".into(),
                start_page: 1,
                page_size: Some(100),
                page_size_param: Some("per_page".into()),
            })
            .max_pages(20)
            .timeout(Duration::from_secs(30))
            .max_retries(3)
            .primary_keys(vec!["id".into()]),
    )?;
    let users_sink = JsonlSink::new(JsonlSinkConfig::new("users.jsonl").append(false));

    let posts_source = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/users/{user_id}/posts")
            .name("posts")
            .auth(Auth::Bearer {
                token: std::env::var("API_TOKEN")?,
            })
            .records_path("$.data[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.meta.next_cursor".into(),
                param_name: "cursor".into(),
            })
            .max_pages(usize::MAX)
            .timeout(Duration::from_secs(20))
            .max_retries(3)
            .retry_backoff(Duration::from_millis(500))
            .request_delay(Duration::from_millis(50)),
    )?;
    let posts_sink = JsonlSink::new(JsonlSinkConfig::new("posts_by_user.jsonl").append(false));

    let mut context_mapping = HashMap::new();
    context_mapping.insert("user_id".to_string(), "$.id".to_string());

    let dag = SourceDAG::new()
        .add_root("users", Box::new(users_source), Box::new(users_sink))
        .add_child(
            "posts",
            "users",
            Box::new(posts_source),
            Box::new(posts_sink),
            context_mapping,
            true,
        )
        .concurrency(4);

    let result = dag.run().await?;
    for (name, node) in &result.node_results {
        println!(
            "{name}: {} records, {} parents processed, {} errors",
            node.records_written,
            node.parent_records_processed,
            node.errors.len(),
        );
    }
    Ok(())
}
