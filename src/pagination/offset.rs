//! Offset/limit pagination.

use crate::error::FaucetError;
use jsonpath_rust::JsonPath;
use serde_json::Value;
use std::collections::HashMap;

pub fn apply_params(
    params: &mut HashMap<String, String>,
    offset_param: &str,
    limit_param: &str,
    offset: usize,
    limit: usize,
) {
    params.insert(offset_param.to_string(), offset.to_string());
    params.insert(limit_param.to_string(), limit.to_string());
}

pub fn advance(
    body: &Value,
    offset: &mut usize,
    record_count: usize,
    limit: usize,
    total_path: Option<&str>,
) -> Result<bool, FaucetError> {
    *offset += record_count;
    if let Some(tp) = total_path {
        let results = body
            .query(tp)
            .map_err(|e| FaucetError::JsonPath(format!("{e}")))?;
        if let Some(total) = results.first() {
            let total = total.as_u64().unwrap_or(0) as usize;
            return Ok(*offset < total);
        }
    }
    Ok(record_count >= limit)
}
