//! Protobuf encode/decode wrapped in the Confluent wire envelope.
//!
//! Confluent's Protobuf format wraps the standard Confluent envelope around
//! `[message_indexes][protobuf bytes]`. `message_indexes` is a varint-encoded
//! array used when a `.proto` file contains multiple message types. For the
//! single-message case it is `[0]`, encoded as a single byte `0x00`.
//!
//! For v1 the codec is a placeholder: it ships the wire envelope path so the
//! type system is symmetric with Avro and JSON Schema, but returns a clear
//! configuration error if used. Real descriptor compilation needs either a
//! pre-built `FileDescriptorSet` or `protoc` at runtime — tracked as a
//! follow-up issue.

use crate::schema_registry::{client::SchemaRegistryClient, envelope};
use faucet_core::FaucetError;
use serde_json::Value;

/// Decode `bytes` (with Confluent envelope + message-index prefix) into JSON.
pub async fn decode(_client: &SchemaRegistryClient, bytes: &[u8]) -> Result<Value, FaucetError> {
    let (_schema_id, body) = envelope::decode(bytes)?;
    parse_message_indexes(body)?;
    Err(FaucetError::Config(
        "ConfluentProtobuf v1: descriptor pool not implemented. \
         Track in #21/#22 follow-up — descriptor compilation needs \
         protoc or a pre-built FileDescriptorSet."
            .into(),
    ))
}

/// Encode `value` as the named message under `subject`. Returns wire-envelope bytes.
pub async fn encode(
    _client: &SchemaRegistryClient,
    _subject: &str,
    _proto_source: &str,
    _message_full_name: &str,
    _value: &Value,
) -> Result<Vec<u8>, FaucetError> {
    Err(FaucetError::Config(
        "ConfluentProtobuf v1 requires the registry to be configured to return \
         FileDescriptorSet bytes. Raw .proto compilation is not yet supported \
         in faucet-kafka-common. See issue tracker for FileDescriptorSet support."
            .into(),
    ))
}

/// Strip the message_indexes prefix. v1 only supports the single-message case
/// (message_indexes = [0], encoded as a single 0x00 byte).
fn parse_message_indexes(body: &[u8]) -> Result<&[u8], FaucetError> {
    if body.is_empty() {
        return Err(FaucetError::Source(
            "protobuf payload is empty (missing message_indexes)".into(),
        ));
    }
    if body[0] != 0 {
        return Err(FaucetError::Source(
            "ConfluentProtobuf v1 only supports single-message schemas \
             (message_indexes = [0]). Multi-message support is a follow-up."
                .into(),
        ));
    }
    Ok(&body[1..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaRegistryConfig;
    use wiremock::MockServer;

    #[tokio::test]
    async fn encode_returns_error_for_raw_proto_text_in_v1() {
        let server = MockServer::start().await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let err = encode(
            &client,
            "users-value",
            "syntax = \"proto3\"; message User { int64 id = 1; }",
            "User",
            &serde_json::json!({"id": 1}),
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("FileDescriptorSet"));
    }

    #[test]
    fn parse_message_indexes_rejects_empty_payload() {
        assert!(parse_message_indexes(&[]).is_err());
    }

    #[test]
    fn parse_message_indexes_rejects_multi_message_marker() {
        // A nonzero first byte indicates message_indexes is something other than [0].
        assert!(parse_message_indexes(&[0x01]).is_err());
    }

    #[test]
    fn parse_message_indexes_strips_single_zero() {
        let body = &[0x00, 0xDE, 0xAD, 0xBE, 0xEF];
        let stripped = parse_message_indexes(body).unwrap();
        assert_eq!(stripped, &[0xDE, 0xAD, 0xBE, 0xEF]);
    }
}
