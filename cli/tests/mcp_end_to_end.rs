//! `faucet mcp` (stdio) end-to-end tests (issue #420).
//!
//! Drives the built binary as a real MCP client would: newline-delimited
//! JSON-RPC 2.0 on stdin, one response object per line on stdout.
#![cfg(all(feature = "mcp", feature = "source-csv", feature = "sink-jsonl"))]

use assert_cmd::Command;
use serde_json::Value;
use std::fs;
use tempfile::TempDir;

/// Run `faucet mcp [args]`, feeding `input` on stdin, returning parsed
/// per-line JSON-RPC responses from stdout.
fn mcp(args: &[&str], input: &str) -> Vec<Value> {
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .arg("mcp")
        .args(args)
        .write_stdin(input.to_string())
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    String::from_utf8(out)
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid JSON-RPC line"))
        .collect()
}

#[test]
fn initialize_and_tools_list_readonly() {
    let responses = mcp(
        &[],
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n\
         {\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n",
    );
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["result"]["protocolVersion"], "2024-11-05");
    assert_eq!(responses[0]["result"]["serverInfo"]["name"], "faucet");

    let names: Vec<String> = responses[1]["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"list_connectors".to_string()));
    assert!(names.contains(&"validate_config".to_string()));
    assert!(names.contains(&"preview".to_string()));
    // Mutating tool hidden without --allow-mutations.
    assert!(!names.contains(&"run_pipeline".to_string()));
}

#[test]
fn allow_mutations_exposes_run_pipeline() {
    let responses = mcp(
        &["--allow-mutations"],
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}\n",
    );
    let names: Vec<String> = responses[0]["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert!(names.contains(&"run_pipeline".to_string()));
}

#[test]
fn validate_and_run_pipeline_end_to_end() {
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("in.csv");
    let out = dir.path().join("out.jsonl");
    fs::write(&csv, "id,name\n1,alice\n2,bob\n").unwrap();

    let cfg = format!(
        "version: 1\nname: mcp_e2e\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
        csv.display(),
        out.display()
    );

    // Build JSON-RPC requests with the config embedded as a JSON string.
    let validate = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": { "name": "validate_config", "arguments": { "config": cfg } }
    });
    let run = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": { "name": "run_pipeline", "arguments": { "config": cfg } }
    });
    let input = format!("{validate}\n{run}\n");

    let responses = mcp(&["--allow-mutations"], &input);
    assert_eq!(responses.len(), 2);

    // validate_config
    assert_eq!(responses[0]["result"]["isError"], false);
    let vtext = responses[0]["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(vtext.contains("\"valid\": true"));

    // run_pipeline actually wrote the sink.
    assert_eq!(responses[1]["result"]["isError"], false);
    let rtext = responses[1]["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(rtext.contains("\"records_written\": 2"));
    assert_eq!(fs::read_to_string(&out).unwrap().lines().count(), 2);
}

#[test]
fn run_pipeline_rejected_without_allow_mutations() {
    // Even if a client names the tool, it is refused without the flag.
    let responses = mcp(
        &[],
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"run_pipeline\",\"arguments\":{\"config\":\"version: 1\"}}}\n",
    );
    assert_eq!(responses[0]["result"]["isError"], true);
    assert!(
        responses[0]["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("--allow-mutations")
    );
}

#[test]
fn get_connector_schema_returns_json_schema() {
    let responses = mcp(
        &[],
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"get_connector_schema\",\"arguments\":{\"kind\":\"source\",\"name\":\"csv\"}}}\n",
    );
    assert_eq!(responses[0]["result"]["isError"], false);
    let text = responses[0]["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(text.contains("properties"));
}
