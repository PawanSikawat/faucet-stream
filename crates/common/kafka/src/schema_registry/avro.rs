//! Avro encode/decode wrapped in the Confluent wire envelope.

use crate::schema_registry::{client::SchemaRegistryClient, envelope};
use apache_avro::{Schema, from_avro_datum, to_avro_datum, types::Value as AvroValue};
use faucet_core::FaucetError;
use serde_json::Value;

/// Decode `bytes` (with Confluent envelope) into a JSON value, using the
/// writer schema fetched from the registry by ID.
pub async fn decode(client: &SchemaRegistryClient, bytes: &[u8]) -> Result<Value, FaucetError> {
    let (schema_id, body) = envelope::decode(bytes)?;
    let registered = client.get_schema(schema_id).await?;
    let schema = Schema::parse_str(&registered.schema)
        .map_err(|e| FaucetError::Source(format!("avro schema parse: {e}")))?;
    let mut cursor = std::io::Cursor::new(body);
    let avro_value = from_avro_datum(&schema, &mut cursor, None)
        .map_err(|e| FaucetError::Source(format!("avro decode: {e}")))?;
    avro_to_json(avro_value)
}

/// Encode a JSON value as Avro under the named subject, registering or
/// reusing the schema. Returns the wire envelope bytes.
///
/// `subject` is typically `{topic}-value` (TopicNameStrategy). `schema_text`
/// is the writer schema as JSON; on first use it is registered with the
/// registry and the returned ID is cached for subsequent calls.
pub async fn encode(
    client: &SchemaRegistryClient,
    subject: &str,
    schema_text: &str,
    value: &Value,
) -> Result<Vec<u8>, FaucetError> {
    let schema = Schema::parse_str(schema_text)
        .map_err(|e| FaucetError::Config(format!("avro schema parse: {e}")))?;
    let id = client.register_schema(subject, "AVRO", schema_text).await?;
    let avro_value = json_to_avro(value, &schema)?;
    let payload = to_avro_datum(&schema, avro_value)
        .map_err(|e| FaucetError::Sink(format!("avro encode: {e}")))?;
    Ok(envelope::encode(id, &payload))
}

/// Convert an `AvroValue` to a `serde_json::Value`.
///
/// Uses the `TryFrom<AvroValue> for serde_json::Value` impl that is present in
/// apache-avro 0.21 — the plan's `v.clone().try_into()` pattern works exactly
/// as expected here.
fn avro_to_json(v: AvroValue) -> Result<Value, FaucetError> {
    v.try_into()
        .map_err(|e: apache_avro::Error| FaucetError::Source(format!("avro->json: {e}")))
}

/// Convert a `serde_json::Value` to an `AvroValue` and resolve it against
/// the writer schema.
///
/// apache-avro 0.21 provides `impl From<serde_json::Value> for AvroValue` so
/// the conversion is infallible; schema resolution (`.resolve()`) can fail if
/// the JSON shape doesn't match the Avro schema.
fn json_to_avro(v: &Value, schema: &Schema) -> Result<AvroValue, FaucetError> {
    // `From<serde_json::Value> for AvroValue` exists in apache-avro 0.21.
    let avro: AvroValue = AvroValue::from(v.clone())
        .resolve(schema)
        .map_err(|e| FaucetError::Sink(format!("json->avro resolve: {e}")))?;
    Ok(avro)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaRegistryConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn avro_round_trip_through_mock_registry() {
        let server = MockServer::start().await;
        let schema_text = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;

        Mock::given(method("POST"))
            .and(path("/subjects/users-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 1})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": schema_text,
                "schemaType": "AVRO",
            })))
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let record = serde_json::json!({"id": 42, "name": "alice"});
        let bytes = encode(&client, "users-value", schema_text, &record)
            .await
            .unwrap();
        let decoded = decode(&client, &bytes).await.unwrap();
        assert_eq!(decoded["id"], 42);
        assert_eq!(decoded["name"], "alice");
    }
}
