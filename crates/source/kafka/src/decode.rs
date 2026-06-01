//! Decode Kafka message bytes into JSON `Value`s, dispatching on
//! `KafkaValueFormat`.

use base64::Engine;
use faucet_core::FaucetError;
use faucet_common_kafka::KafkaValueFormat;
use serde_json::Value;

#[cfg(feature = "schema-registry")]
use faucet_common_kafka::schema_registry::{
    avro, client::SchemaRegistryClient, json_schema, protobuf,
};

/// Decode `bytes` per `format`. For Confluent SR formats, `sr_client` must be `Some`.
pub async fn decode(
    bytes: Option<&[u8]>,
    format: &KafkaValueFormat,
    #[cfg(feature = "schema-registry")] sr_client: Option<&SchemaRegistryClient>,
) -> Result<Value, FaucetError> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };
    match format {
        KafkaValueFormat::Json => serde_json::from_slice(bytes)
            .map_err(|e| FaucetError::Source(format!("kafka json decode: {e}"))),
        KafkaValueFormat::RawString => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| FaucetError::Source(format!("kafka raw_string decode: {e}")))?;
            Ok(Value::String(s.to_string()))
        }
        KafkaValueFormat::Bytes => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            Ok(Value::String(encoded))
        }
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentAvro { .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config(
                    "ConfluentAvro selected but no SchemaRegistryClient available".into(),
                )
            })?;
            avro::decode(client, bytes).await
        }
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentProtobuf { .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config(
                    "ConfluentProtobuf selected but no SchemaRegistryClient available".into(),
                )
            })?;
            protobuf::decode(client, bytes).await
        }
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentJsonSchema { validate, .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config(
                    "ConfluentJsonSchema selected but no SchemaRegistryClient available".into(),
                )
            })?;
            json_schema::decode(client, bytes, *validate).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn decode_json_object() {
        let bytes = br#"{"id": 7, "name": "x"}"#;
        let v = decode(
            Some(bytes),
            &KafkaValueFormat::Json,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap();
        assert_eq!(v["id"], 7);
    }

    #[tokio::test]
    async fn decode_raw_string_utf8() {
        let bytes = "hello".as_bytes();
        let v = decode(
            Some(bytes),
            &KafkaValueFormat::RawString,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap();
        assert_eq!(v, Value::String("hello".into()));
    }

    #[tokio::test]
    async fn decode_bytes_base64_encodes() {
        let bytes = &[0xDE, 0xAD, 0xBE, 0xEF];
        let v = decode(
            Some(bytes),
            &KafkaValueFormat::Bytes,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap();
        assert_eq!(v, Value::String("3q2+7w==".into()));
    }

    #[tokio::test]
    async fn decode_none_returns_null() {
        let v = decode(
            None,
            &KafkaValueFormat::Json,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap();
        assert_eq!(v, Value::Null);
    }

    #[tokio::test]
    async fn decode_invalid_json_errors() {
        let bytes = b"{not json";
        let err = decode(
            Some(bytes),
            &KafkaValueFormat::Json,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("json"));
    }

    #[tokio::test]
    async fn decode_raw_string_rejects_invalid_utf8() {
        let bytes = &[0xFF, 0xFE];
        let err = decode(
            Some(bytes),
            &KafkaValueFormat::RawString,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap_err();
        let m = format!("{err}").to_lowercase();
        assert!(m.contains("utf-8") || m.contains("utf"));
    }
}
