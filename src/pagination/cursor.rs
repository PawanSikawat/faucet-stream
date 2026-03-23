//! Cursor/token-based pagination.

use crate::error::FaucetError;
use jsonpath_rust::JsonPath;
use serde_json::Value;
use std::collections::HashMap;

pub fn apply_params(params: &mut HashMap<String, String>, param_name: &str, token: &Option<String>) {
    if let Some(t) = token {
        params.insert(param_name.to_string(), t.clone());
    }
}

pub fn advance(
    body: &Value,
    next_token_path: &str,
    next_token: &mut Option<String>,
) -> Result<bool, FaucetError> {
    let results = body
        .query(next_token_path)
        .map_err(|e| FaucetError::JsonPath(format!("{e}")))?;
    match results.first() {
        Some(v) => {
            if v.is_null() || (v.is_string() && v.as_str().unwrap().is_empty()) {
                *next_token = None;
                Ok(false)
            } else {
                *next_token = Some(v.as_str().unwrap_or(&v.to_string()).to_string());
                Ok(true)
            }
        }
        None => {
            *next_token = None;
            Ok(false)
        }
    }
}
