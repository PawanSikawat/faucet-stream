//! Snowflake SQL REST API sink.

use crate::config::{SnowflakeAuth, SnowflakeSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Value, json};

/// A sink that writes JSON records to a Snowflake table using the
/// SQL REST API.
pub struct SnowflakeSink {
    config: SnowflakeSinkConfig,
    client: Client,
}

#[derive(Deserialize)]
struct SnowflakeResponse {
    message: Option<String>,
    #[serde(default)]
    code: Option<String>,
}

impl SnowflakeSink {
    /// Create a new Snowflake sink.
    pub fn new(config: SnowflakeSinkConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }

    /// Build the SQL REST API endpoint URL.
    fn api_url(&self) -> String {
        format!(
            "https://{}.snowflakecomputing.com/api/v2/statements",
            self.config.account
        )
    }

    /// Get the authorization header value.
    fn auth_header(&self) -> Result<String, FaucetError> {
        match &self.config.auth {
            SnowflakeAuth::KeyPair {
                user,
                private_key_pem,
            } => {
                let account_upper = self.config.account.to_uppercase();
                let user_upper = user.to_uppercase();
                let qualified_user = format!("{account_upper}.{user_upper}");

                let now = jsonwebtoken::get_current_timestamp();
                let claims = serde_json::json!({
                    "iss": qualified_user,
                    "sub": qualified_user,
                    "iat": now,
                    "exp": now + 3600,
                });

                let key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key_pem.as_bytes())
                    .map_err(|e| FaucetError::Auth(format!("invalid RSA key: {e}")))?;

                let token = jsonwebtoken::encode(
                    &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
                    &claims,
                    &key,
                )
                .map_err(|e| FaucetError::Auth(format!("JWT generation failed: {e}")))?;

                Ok(format!("Bearer {token}"))
            }
            SnowflakeAuth::OAuth { token } => Ok(format!("Snowflake Token=\"{token}\"")),
        }
    }

    /// Execute a SQL statement via the REST API.
    async fn execute_sql(&self, sql: &str) -> Result<(), FaucetError> {
        let url = self.api_url();
        let auth = self.auth_header()?;

        let body = json!({
            "statement": sql,
            "timeout": 60,
            "database": self.config.database,
            "schema": self.config.schema,
            "warehouse": self.config.warehouse,
        });

        let resp = self
            .client
            .post(&url)
            .header("Authorization", &auth)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .json(&body)
            .send()
            .await
            .map_err(|e| FaucetError::Sink(format!("Snowflake request failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Sink(format!(
                "Snowflake SQL API returned HTTP {status}: {body_text}"
            )));
        }

        let sf_resp: SnowflakeResponse = resp
            .json()
            .await
            .map_err(|e| FaucetError::Sink(format!("failed to parse Snowflake response: {e}")))?;

        if let Some(code) = &sf_resp.code
            && code != "090001"
        {
            // 090001 = "Statement executed successfully"
            return Err(FaucetError::Sink(format!(
                "Snowflake error {}: {}",
                code,
                sf_resp.message.unwrap_or_default()
            )));
        }

        Ok(())
    }

    /// Build an INSERT statement for a batch of records using PARSE_JSON
    /// with parameterised identifiers.
    fn build_insert_sql(&self, records: &[Value]) -> Result<String, FaucetError> {
        for record in records {
            record.as_object().ok_or_else(|| {
                FaucetError::Sink("Snowflake sink requires JSON object records".into())
            })?;
        }

        // Serialize all records into a JSON array, then use FLATTEN to insert.
        let json_array: Vec<String> = records
            .iter()
            .map(|r| serde_json::to_string(r).unwrap_or_default())
            .collect();

        Ok(format!(
            "INSERT INTO {}.{}.{} (SELECT * FROM TABLE(FLATTEN(input => PARSE_JSON('[{}]'))))",
            quote_ident(&self.config.database),
            quote_ident(&self.config.schema),
            quote_ident(&self.config.table),
            json_array.join(",")
        ))
    }
}

#[async_trait]
impl faucet_core::Sink for SnowflakeSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(SnowflakeSinkConfig))
            .expect("schema serialization")
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut total = 0;
        for chunk in records.chunks(self.config.batch_size) {
            let sql = self.build_insert_sql(chunk)?;
            self.execute_sql(&sql).await?;
            total += chunk.len();
        }

        tracing::info!(
            table = %format!(
                "{}.{}.{}",
                self.config.database, self.config.schema, self.config.table
            ),
            rows = total,
            "Snowflake write complete"
        );
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_url_format() {
        let config = SnowflakeSinkConfig::new(
            "xy12345.us-east-1",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth {
                token: "tok".into(),
            },
        );
        let sink = SnowflakeSink::new(config);
        assert_eq!(
            sink.api_url(),
            "https://xy12345.us-east-1.snowflakecomputing.com/api/v2/statements"
        );
    }

    #[test]
    fn oauth_auth_header() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth {
                token: "my-token".into(),
            },
        );
        let sink = SnowflakeSink::new(config);
        let header = sink.auth_header().unwrap();
        assert_eq!(header, "Snowflake Token=\"my-token\"");
    }

    #[test]
    fn build_insert_sql_uses_quoted_identifiers() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "MY_DB",
            "PUBLIC",
            "events",
            SnowflakeAuth::OAuth { token: "t".into() },
        );
        let sink = SnowflakeSink::new(config);
        let records = vec![serde_json::json!({"id": 1})];
        let sql = sink.build_insert_sql(&records).unwrap();
        assert!(sql.contains("\"MY_DB\".\"PUBLIC\".\"events\""));
    }
}
