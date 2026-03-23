use faucet_stream::extract::extract_records;
use serde_json::json;

#[test]
fn extract_deeply_nested() {
    let body = json!({
        "response": {
            "results": {
                "items": [
                    {"id": 1},
                    {"id": 2},
                    {"id": 3},
                ]
            }
        }
    });
    let records = extract_records(&body, Some("$.response.results.items[*]")).unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[2]["id"], 3);
}

#[test]
fn extract_single_value() {
    let body = json!({"name": "faucet-stream", "version": "0.1.0"});
    let records = extract_records(&body, Some("$.name")).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], "faucet-stream");
}
