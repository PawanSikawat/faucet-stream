//! Parent-child source DAG.
//!
//! Fetches users from `jsonplaceholder.typicode.com`, then for each user
//! fetches that user's posts from `/users/{user_id}/posts`. The parent's
//! `id` is injected into every child record as `user_id`.
//!
//! Tree:
//! ```text
//! users  (root)         ──► /tmp/users.jsonl
//!   └── posts (child)   ──► /tmp/posts_by_user.jsonl   (one fetch per user, concurrent)
//! ```
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example dag_users_posts \
//!     --features "source-rest sink-jsonl"
//! ```

use std::collections::HashMap;

use faucet_stream::sink::jsonl::{JsonlSink, JsonlSinkConfig};
use faucet_stream::{RestStream, RestStreamConfig, SourceDAG};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let users_source = RestStream::new(RestStreamConfig::new(
        "https://jsonplaceholder.typicode.com",
        "/users",
    ))?;
    let users_sink = JsonlSink::new(JsonlSinkConfig::new("/tmp/users.jsonl"));

    let posts_source = RestStream::new(RestStreamConfig::new(
        "https://jsonplaceholder.typicode.com",
        "/users/{user_id}/posts",
    ))?;
    let posts_sink = JsonlSink::new(JsonlSinkConfig::new("/tmp/posts_by_user.jsonl"));

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
            "{name}: {} records written, {} parents processed, {} errors",
            node.records_written,
            node.parent_records_processed,
            node.errors.len(),
        );
    }
    Ok(())
}
