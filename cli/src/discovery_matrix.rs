//! Pure planning logic for the discovery-driven request matrix (#501).
//!
//! A `discover:` row enumerates a value-set at runtime (build source → drain →
//! project `select` → dedup); a `for_each: [dims]` row fans out over the
//! **cartesian product** of those value-sets, one invocation per tuple.
//!
//! Everything here is pure (no I/O): projection, dedup, the cartesian product,
//! the per-tuple interpolation context, and the tuple state-key suffix — so the
//! fan-out semantics are unit-testable without a network. The only I/O (running
//! the discovery source) lives in the executor and feeds [`Dim::values`].

use serde_json::{Map, Value};
use std::collections::HashMap;

/// Hard ceiling on the number of invocations a single `for_each` row may expand
/// to. A cartesian product multiplies quickly; above this the run fails rather
/// than silently spawning an unbounded fleet.
pub const MAX_MATRIX_PRODUCT: usize = 10_000;

/// One resolved discovery dimension: the discovery row id, the alias its
/// projected value is exposed under, and the deduped value-set.
#[derive(Debug, Clone, PartialEq)]
pub struct Dim {
    /// Discovery row id (the `${<id>.<alias>}` reference target).
    pub id: String,
    /// Alias the projected value is exposed under.
    pub alias: String,
    /// Projected, deduped values (first-seen order).
    pub values: Vec<Value>,
}

/// Project `select` (a dot-path, optionally `$`-prefixed) from each record and
/// dedup the results in first-seen order. `null` / missing projections are
/// skipped. `$` (or `""`) selects the whole record. Pure.
pub fn project_dedup(records: &[Value], select: &str) -> Vec<Value> {
    let mut seen: Vec<Value> = Vec::new();
    for rec in records {
        let Some(v) = project_value(rec, select) else {
            continue;
        };
        if v.is_null() {
            continue;
        }
        if !seen.contains(&v) {
            seen.push(v);
        }
    }
    seen
}

/// Resolve a dot-path projection against one record. Supports a leading `$.` or
/// bare `$` (whole record), then `a.b.c` segment walking into objects. Returns
/// `None` if any segment is missing. Pure.
fn project_value(record: &Value, select: &str) -> Option<Value> {
    let path = select
        .strip_prefix("$.")
        .or_else(|| select.strip_prefix('$'))
        .unwrap_or(select);
    if path.is_empty() {
        return Some(record.clone());
    }
    let mut cur = record;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur.clone())
}

/// The number of invocations `dims` expands to (the product of the value-set
/// sizes). Returns `0` if any dimension is empty. Saturating, so it never
/// overflows; compare against [`MAX_MATRIX_PRODUCT`]. Pure.
pub fn product_size(dims: &[Dim]) -> usize {
    if dims.is_empty() {
        return 0;
    }
    dims.iter()
        .map(|d| d.values.len())
        .try_fold(1usize, |acc, n| acc.checked_mul(n))
        .unwrap_or(usize::MAX)
}

/// Build the cartesian product of the dimensions as per-tuple interpolation
/// contexts. Each context maps `dim_id -> { alias: value }`, so a token
/// `${dim_id.alias}` resolves to that tuple's value via the same
/// `interpolate_record` path the parent/child matrix uses. An empty dimension
/// yields no tuples. Pure.
pub fn cartesian(dims: &[Dim]) -> Vec<HashMap<String, Value>> {
    if dims.is_empty() || dims.iter().any(|d| d.values.is_empty()) {
        return Vec::new();
    }
    let mut out: Vec<HashMap<String, Value>> = vec![HashMap::new()];
    for dim in dims {
        let mut next = Vec::with_capacity(out.len() * dim.values.len());
        for base in &out {
            for v in &dim.values {
                let mut ctx = base.clone();
                let mut obj = Map::new();
                obj.insert(dim.alias.clone(), v.clone());
                ctx.insert(dim.id.clone(), Value::Object(obj));
                next.push(ctx);
            }
        }
        out = next;
    }
    out
}

/// Stable, collision-resistant state-key suffix for one product tuple, in
/// declared dimension order: `alias=value&alias=value…`. `dims` supplies the
/// order + aliases; `ctx` is one entry from [`cartesian`]. Pure.
pub fn tuple_state_key_suffix(dims: &[Dim], ctx: &HashMap<String, Value>) -> String {
    dims.iter()
        .map(|d| {
            let v = ctx
                .get(&d.id)
                .and_then(|o| o.get(&d.alias))
                .map(value_brief)
                .unwrap_or_else(|| "(missing)".to_string());
            format!("{}={}", d.alias, v)
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// Brief scalar rendering of a JSON value for a state-key segment.
fn value_brief(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn project_dedup_dotpath_and_dollar() {
        let recs = vec![
            json!({"id": 1, "name": "a"}),
            json!({"id": 2, "name": "b"}),
            json!({"id": 1, "name": "c"}), // dup id
        ];
        assert_eq!(project_dedup(&recs, "$.id"), vec![json!(1), json!(2)]);
        assert_eq!(project_dedup(&recs, "id"), vec![json!(1), json!(2)]);
        // `$` selects whole records (all distinct here).
        assert_eq!(project_dedup(&recs, "$").len(), 3);
    }

    #[test]
    fn project_dedup_skips_null_and_missing() {
        let recs = vec![
            json!({"id": 1}),
            json!({"id": null}),
            json!({"other": 9}), // missing `id`
            json!({"id": 2}),
        ];
        assert_eq!(project_dedup(&recs, "$.id"), vec![json!(1), json!(2)]);
    }

    #[test]
    fn project_nested_path() {
        let recs = vec![
            json!({"meta": {"code": "X"}}),
            json!({"meta": {"code": "Y"}}),
        ];
        assert_eq!(
            project_dedup(&recs, "$.meta.code"),
            vec![json!("X"), json!("Y")]
        );
    }

    fn dim(id: &str, alias: &str, vals: Vec<Value>) -> Dim {
        Dim {
            id: id.into(),
            alias: alias.into(),
            values: vals,
        }
    }

    #[test]
    fn cartesian_two_dims_is_product() {
        let dims = vec![
            dim("subs", "subsidiary_id", vec![json!(1), json!(2)]),
            dim(
                "fields",
                "field_id",
                vec![json!("a"), json!("b"), json!("c")],
            ),
        ];
        let ctxs = cartesian(&dims);
        assert_eq!(ctxs.len(), 6);
        // First tuple pairs the first value of each dim.
        assert_eq!(ctxs[0]["subs"]["subsidiary_id"], json!(1));
        assert_eq!(ctxs[0]["fields"]["field_id"], json!("a"));
        // Product covers every combination.
        let pairs: Vec<(Value, Value)> = ctxs
            .iter()
            .map(|c| {
                (
                    c["subs"]["subsidiary_id"].clone(),
                    c["fields"]["field_id"].clone(),
                )
            })
            .collect();
        assert!(pairs.contains(&(json!(2), json!("c"))));
    }

    #[test]
    fn cartesian_single_dim() {
        let dims = vec![dim("d", "v", vec![json!(1), json!(2)])];
        let ctxs = cartesian(&dims);
        assert_eq!(ctxs.len(), 2);
        assert_eq!(ctxs[1]["d"]["v"], json!(2));
    }

    #[test]
    fn cartesian_empty_dim_yields_nothing() {
        let dims = vec![dim("a", "x", vec![json!(1)]), dim("b", "y", vec![])];
        assert!(cartesian(&dims).is_empty());
        assert!(cartesian(&[]).is_empty());
    }

    #[test]
    fn product_size_math() {
        assert_eq!(product_size(&[]), 0);
        assert_eq!(
            product_size(&[
                dim("a", "x", vec![json!(1), json!(2)]),
                dim("b", "y", vec![json!(1)])
            ]),
            2
        );
        assert_eq!(
            product_size(&[dim("a", "x", vec![]), dim("b", "y", vec![json!(1)])]),
            0
        );
    }

    #[test]
    fn tuple_state_key_is_stable_and_ordered() {
        let dims = vec![
            dim("subs", "subsidiary_id", vec![json!(1)]),
            dim("fields", "field_id", vec![json!("a")]),
        ];
        let ctxs = cartesian(&dims);
        assert_eq!(
            tuple_state_key_suffix(&dims, &ctxs[0]),
            "subsidiary_id=1&field_id=a"
        );
    }
}
