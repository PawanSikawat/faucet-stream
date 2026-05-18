//! Deep-merge of `serde_json::Value` for matrix-row overrides.
//!
//! Rules:
//! - Two objects merge recursively, keys from `overlay` win on collision.
//! - Anything else (scalar, array, null) — `overlay` replaces `base` wholesale.
//! - Arrays do not element-merge or concatenate. This is deliberate: matrix
//!   rows that need to add to an inherited array should redeclare it in full.

use serde_json::Value;

/// In-place deep-merge `overlay` into `base`. See module docs for rules.
pub fn merge_value(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(b), Value::Object(o)) => {
            for (k, v) in o {
                match b.get_mut(&k) {
                    Some(existing) => merge_value(existing, v),
                    None => {
                        b.insert(k, v);
                    }
                }
            }
        }
        (slot, overlay) => *slot = overlay,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn scalar_overlay_replaces() {
        let mut base = json!({"a": 1, "b": 2});
        merge_value(&mut base, json!({"b": 99}));
        assert_eq!(base, json!({"a": 1, "b": 99}));
    }

    #[test]
    fn objects_merge_recursively() {
        let mut base = json!({"auth": {"type": "Bearer", "token": "x"}, "path": "/v1"});
        merge_value(&mut base, json!({"auth": {"token": "y"}}));
        assert_eq!(
            base,
            json!({"auth": {"type": "Bearer", "token": "y"}, "path": "/v1"})
        );
    }

    #[test]
    fn arrays_replace_wholesale() {
        let mut base = json!({"tags": ["a", "b"]});
        merge_value(&mut base, json!({"tags": ["c"]}));
        assert_eq!(base, json!({"tags": ["c"]}));
    }

    #[test]
    fn null_overlay_replaces() {
        let mut base = json!({"x": {"y": 1}});
        merge_value(&mut base, json!({"x": null}));
        assert_eq!(base, json!({"x": null}));
    }

    #[test]
    fn overlay_adds_new_keys() {
        let mut base = json!({"a": 1});
        merge_value(&mut base, json!({"b": 2}));
        assert_eq!(base, json!({"a": 1, "b": 2}));
    }

    #[test]
    fn object_into_non_object_replaces() {
        let mut base = json!({"path": "/v1"});
        merge_value(&mut base, json!({"path": {"foo": "bar"}}));
        assert_eq!(base, json!({"path": {"foo": "bar"}}));
    }

    #[test]
    fn non_object_into_object_replaces() {
        let mut base = json!({"path": {"foo": "bar"}});
        merge_value(&mut base, json!({"path": "/v1"}));
        assert_eq!(base, json!({"path": "/v1"}));
    }

    #[test]
    fn deeply_nested_merges() {
        let mut base = json!({"a": {"b": {"c": 1, "d": 2}, "x": 10}});
        merge_value(&mut base, json!({"a": {"b": {"c": 99, "e": 3}}}));
        assert_eq!(
            base,
            json!({"a": {"b": {"c": 99, "d": 2, "e": 3}, "x": 10}})
        );
    }
}
