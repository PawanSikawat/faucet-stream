//! JSON-RPC 2.0 + MCP protocol types (issue #420).
//!
//! A minimal, hand-rolled subset of the [Model Context Protocol](https://modelcontextprotocol.io)
//! — enough to serve `initialize`, `tools/list`, `tools/call`, `resources/list`,
//! `resources/read`, and `ping` over any byte transport. Kept dependency-free
//! (just `serde_json`) so the `mcp` feature adds no new crate tree.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// MCP protocol version this server implements/advertises.
pub const PROTOCOL_VERSION: &str = "2024-11-05";

/// JSON-RPC 2.0 standard error codes.
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;

/// An incoming JSON-RPC request or notification.
///
/// A message with no `id` is a *notification* (no response is sent).
#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    #[serde(default)]
    pub jsonrpc: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

impl JsonRpcRequest {
    /// A request carrying an `id` expects a response; a notification does not.
    pub fn is_notification(&self) -> bool {
        self.id.is_none()
    }
}

/// Build a success response envelope for `id` with `result`.
pub fn success(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// Build an error response envelope for `id`.
pub fn error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message.into() }
    })
}

/// An error whose `id` is unknown (e.g. an unparseable request): JSON-RPC
/// requires `id: null` in that case.
pub fn error_no_id(code: i64, message: impl Into<String>) -> Value {
    error(Value::Null, code, message)
}

/// A tool definition advertised via `tools/list`.
#[derive(Debug, Clone, Serialize)]
pub struct ToolDef {
    pub name: &'static str,
    pub description: &'static str,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

/// Wrap a tool's textual result in the MCP `tools/call` content shape.
pub fn tool_text(text: impl Into<String>) -> Value {
    json!({
        "content": [ { "type": "text", "text": text.into() } ],
        "isError": false
    })
}

/// Wrap a tool error in the MCP `tools/call` content shape (`isError: true`).
///
/// Per the MCP spec a *tool* failure is reported as a normal result with
/// `isError: true` (not a JSON-RPC protocol error), so the model can see and
/// react to it.
pub fn tool_error(text: impl Into<String>) -> Value {
    json!({
        "content": [ { "type": "text", "text": text.into() } ],
        "isError": true
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_request_with_id() {
        let r: JsonRpcRequest =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#).unwrap();
        assert_eq!(r.method, "ping");
        assert!(!r.is_notification());
        assert_eq!(r.id, Some(json!(1)));
    }

    #[test]
    fn parses_notification_without_id() {
        let r: JsonRpcRequest =
            serde_json::from_str(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
                .unwrap();
        assert!(r.is_notification());
    }

    #[test]
    fn success_envelope_shape() {
        let v = success(json!(7), json!({"ok": true}));
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 7);
        assert_eq!(v["result"]["ok"], true);
    }

    #[test]
    fn error_envelope_shape() {
        let v = error(json!(7), METHOD_NOT_FOUND, "nope");
        assert_eq!(v["error"]["code"], METHOD_NOT_FOUND);
        assert_eq!(v["error"]["message"], "nope");
        assert!(v.get("result").is_none());
    }

    #[test]
    fn tool_text_and_error_shapes() {
        let ok = tool_text("hi");
        assert_eq!(ok["isError"], false);
        assert_eq!(ok["content"][0]["type"], "text");
        assert_eq!(ok["content"][0]["text"], "hi");
        let err = tool_error("boom");
        assert_eq!(err["isError"], true);
    }
}
