//! Encode Kafka message bytes from a serde_json::Value, dispatching on
//! KafkaValueFormat.

use base64::Engine;
use faucet_common_kafka::KafkaValueFormat;
use faucet_core::FaucetError;
use serde_json::Value;

#[cfg(feature = "schema-registry")]
use faucet_common_kafka::schema_registry::{
    avro, client::SchemaRegistryClient, json_schema, protobuf,
};

#[derive(Default, Clone)]
pub struct SchemaContext {
    /// Subject name used for ConfluentAvro/Protobuf/JsonSchema. Usually `{topic}-value`.
    pub subject: String,
    /// Schema text to register (Avro JSON, JSON Schema JSON, Protobuf source).
    /// Required for the Confluent variants on encode.
    pub schema_text: Option<String>,
}

pub async fn encode(
    value: &Value,
    format: &KafkaValueFormat,
    #[cfg(feature = "schema-registry")] sr_client: Option<&SchemaRegistryClient>,
    #[cfg(feature = "schema-registry")] schema_ctx: &SchemaContext,
) -> Result<Vec<u8>, FaucetError> {
    match format {
        KafkaValueFormat::Json => serde_json::to_vec(value)
            .map_err(|e| FaucetError::Sink(format!("kafka json encode: {e}"))),
        KafkaValueFormat::RawString => match value {
            Value::String(s) => Ok(s.as_bytes().to_vec()),
            other => Ok(other.to_string().into_bytes()),
        },
        KafkaValueFormat::Bytes => match value {
            Value::String(s) => base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(|e| FaucetError::Sink(format!("kafka bytes base64 decode: {e}"))),
            _ => Err(FaucetError::Sink(
                "kafka Bytes format requires the record value to be a base64-encoded string".into(),
            )),
        },
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentAvro { .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config("ConfluentAvro selected but no SchemaRegistryClient".into())
            })?;
            let schema_text = schema_ctx
                .schema_text
                .as_deref()
                .ok_or_else(|| FaucetError::Config("ConfluentAvro requires schema_text".into()))?;
            avro::encode(client, &schema_ctx.subject, schema_text, value).await
        }
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentProtobuf { .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config("ConfluentProtobuf selected but no SchemaRegistryClient".into())
            })?;
            let schema_text = schema_ctx.schema_text.as_deref().ok_or_else(|| {
                FaucetError::Config("ConfluentProtobuf requires schema_text".into())
            })?;
            protobuf::encode(client, &schema_ctx.subject, schema_text, value).await
        }
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentJsonSchema { .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config(
                    "ConfluentJsonSchema selected but no SchemaRegistryClient".into(),
                )
            })?;
            let schema_text = schema_ctx.schema_text.as_deref().ok_or_else(|| {
                FaucetError::Config("ConfluentJsonSchema requires schema_text".into())
            })?;
            json_schema::encode(client, &schema_ctx.subject, schema_text, value).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn encode_json_object() {
        let bytes = encode(
            &json!({"a": 1}),
            &KafkaValueFormat::Json,
            #[cfg(feature = "schema-registry")]
            None,
            #[cfg(feature = "schema-registry")]
            &SchemaContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(bytes, br#"{"a":1}"#);
    }

    #[tokio::test]
    async fn encode_raw_string_passes_through() {
        let bytes = encode(
            &Value::String("hello".into()),
            &KafkaValueFormat::RawString,
            #[cfg(feature = "schema-registry")]
            None,
            #[cfg(feature = "schema-registry")]
            &SchemaContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(bytes, b"hello");
    }

    #[tokio::test]
    async fn encode_raw_string_stringifies_non_string() {
        let bytes = encode(
            &json!(42),
            &KafkaValueFormat::RawString,
            #[cfg(feature = "schema-registry")]
            None,
            #[cfg(feature = "schema-registry")]
            &SchemaContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(bytes, b"42");
    }

    #[tokio::test]
    async fn encode_bytes_decodes_base64() {
        let bytes = encode(
            &Value::String("3q2+7w==".into()),
            &KafkaValueFormat::Bytes,
            #[cfg(feature = "schema-registry")]
            None,
            #[cfg(feature = "schema-registry")]
            &SchemaContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[tokio::test]
    async fn encode_bytes_errors_on_non_string() {
        let err = encode(
            &json!({"x": 1}),
            &KafkaValueFormat::Bytes,
            #[cfg(feature = "schema-registry")]
            None,
            #[cfg(feature = "schema-registry")]
            &SchemaContext::default(),
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("base64"));
    }
}
