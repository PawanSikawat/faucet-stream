//! End-to-end integration: build `TransformingSource` directly, feed in a
//! known set of records, assert exact post-stage output for the
//! filter → explode → keys_case pipeline.

use async_trait::async_trait;
use faucet_cli::config::TransformSpec;
use faucet_cli::transforms::compile_transforms;
use faucet_core::observability::Labels;
use faucet_core::pipeline::StreamPage;
use faucet_core::{FaucetError, Source, TransformingSource};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;

struct OnePageSource {
    records: Vec<Value>,
}

#[async_trait]
impl Source for OnePageSource {
    async fn fetch_with_context(
        &self,
        _ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.records.clone())
    }
    fn stream_pages<'a>(
        &'a self,
        _ctx: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>
    {
        let page = StreamPage {
            records: self.records.clone(),
            bookmark: None,
        };
        Box::pin(async_stream::stream! { yield Ok(page); })
    }
}

#[tokio::test]
async fn end_to_end_filter_explode_keys_case() {
    let specs = vec![
        TransformSpec {
            kind: "filter".into(),
            config: json!({"path": "deleted", "op": "ne", "value": true}),
        },
        TransformSpec {
            kind: "explode".into(),
            config: json!({"path": "items", "prefix": "item"}),
        },
        TransformSpec {
            kind: "keys_case".into(),
            config: json!({"mode": "snake"}),
        },
    ];
    let stages = compile_transforms(&specs).expect("compile");

    let inner: Box<dyn Source> = Box::new(OnePageSource {
        records: vec![
            json!({"Id": 1, "deleted": false, "items": [{"Sku": "A", "Qty": 2}, {"Sku": "B", "Qty": 3}]}),
            json!({"Id": 2, "deleted": true, "items": [{"Sku": "X", "Qty": 1}]}),
            json!({"Id": 3, "deleted": false, "items": [{"Sku": "C", "Qty": 5}]}),
        ],
    });
    let wrapped = TransformingSource::new(inner, stages, Labels::for_named("test")).expect("wrap");

    let ctx: HashMap<String, Value> = HashMap::new();
    let mut stream = wrapped.stream_pages(&ctx, 100);
    let mut all: Vec<Value> = Vec::new();
    while let Some(p) = stream.next().await {
        all.extend(p.unwrap().records);
    }

    assert_eq!(all.len(), 3, "deleted row dropped; 2+1 children survive");
    // First child of first kept record
    assert_eq!(all[0]["id"], json!(1));
    assert_eq!(all[0]["item_sku"], json!("A"));
    assert_eq!(all[0]["item_qty"], json!(2));
    assert!(all[0].get("Id").is_none(), "keys_case applied");
}
