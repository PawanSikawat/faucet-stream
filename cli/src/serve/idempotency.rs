//! Stable content fingerprint for idempotency replay-vs-conflict detection. A
//! key replayed with the *same* merged config returns the existing run; reused
//! with a *different* config is a 409. The hash is order-independent for object
//! keys (canonical JSON) and stable across process restarts (sha256), so the
//! Phase 5 SQL backends can store and compare it unchanged.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Fingerprint of a merged, resolved config plus its pipeline `name`.
pub fn fingerprint(merged: &Value, name: Option<&str>) -> String {
    let mut canon = String::new();
    write_canonical(merged, &mut canon);
    let mut hasher = Sha256::new();
    hasher.update(name.unwrap_or("").as_bytes());
    hasher.update([0u8]); // separator so name|config can't collide across the boundary
    hasher.update(canon.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Append a canonical (object keys sorted) JSON rendering of `v` to `out`.
fn write_canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(k).expect("string key serializes"));
                out.push(':');
                write_canonical(&map[*k], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&serde_json::to_string(other).expect("scalar serializes")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stable_and_order_independent() {
        let a = json!({ "b": 1, "a": [1, 2], "c": { "y": true, "x": null } });
        let b = json!({ "c": { "x": null, "y": true }, "a": [1, 2], "b": 1 });
        assert_eq!(fingerprint(&a, Some("p")), fingerprint(&b, Some("p")));
    }

    #[test]
    fn differs_on_value_change() {
        let a = json!({ "a": 1 });
        let b = json!({ "a": 2 });
        assert_ne!(fingerprint(&a, Some("p")), fingerprint(&b, Some("p")));
    }

    #[test]
    fn differs_on_name() {
        let v = json!({ "a": 1 });
        assert_ne!(fingerprint(&v, Some("p1")), fingerprint(&v, Some("p2")));
    }

    #[test]
    fn array_order_is_significant() {
        assert_ne!(
            fingerprint(&json!([1, 2]), None),
            fingerprint(&json!([2, 1]), None)
        );
    }
}
