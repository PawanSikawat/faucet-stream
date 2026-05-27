//! Record transformation pipeline.
//!
//! ## Built-in transforms (optional Cargo features)
//!
//! | Variant | Feature flag | Default |
//! |---------|-------------|---------|
//! | [`RecordTransform::Flatten`] | `transform-flatten` | enabled |
//! | [`RecordTransform::RenameKeys`] | `transform-rename-keys` | enabled |
//! | [`RecordTransform::KeysToSnakeCase`] | `transform-snake-case` | enabled |
//!
//! Disable a transform (and its dependencies) by opting out of its feature:
//!
//! ```toml
//! [dependencies]
//! faucet-stream = { version = "*", default-features = false,
//!                   features = ["transform-flatten"] }
//! ```
//!
//! ## Custom transforms
//!
//! [`RecordTransform::Custom`] is always available regardless of features.
//! Pass any closure or function pointer via [`RecordTransform::custom`].

use crate::error::FaucetError;
#[cfg(any(
    feature = "transform-flatten",
    feature = "transform-rename-keys",
    feature = "transform-snake-case"
))]
use serde_json::Map;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

#[cfg(any(feature = "transform-rename-keys", feature = "transform-snake-case"))]
use regex::Regex;

#[cfg(feature = "transform-snake-case")]
use std::sync::LazyLock;

// ── Public config-facing type ─────────────────────────────────────────────────

/// A transformation applied to every record fetched by a [`crate::stream::RestStream`].
///
/// Transforms are applied in the order they are added via
/// [`crate::config::RestStreamConfig::add_transform`].
///
/// The three built-in variants are each guarded by a Cargo feature flag
/// (all enabled by default — see module-level docs).
/// [`RecordTransform::Custom`] is always available and accepts any closure.
pub enum RecordTransform {
    /// Flatten nested JSON objects into a single-level map.
    ///
    /// Nested key paths are joined with `separator`.  Arrays are left as-is.
    ///
    /// _Requires feature `transform-flatten` (default)._
    ///
    /// # Example
    ///
    /// ```text
    /// {"user": {"id": 1, "addr": {"city": "NYC"}}}  →  (separator = "__")
    /// {"user__id": 1, "user__addr__city": "NYC"}
    /// ```
    #[cfg(feature = "transform-flatten")]
    Flatten { separator: String },

    /// Apply a single regex substitution to every key in the record.
    ///
    /// Keys in nested objects and objects inside arrays are also renamed
    /// recursively.  `pattern` is a Rust regex; `replacement` may reference
    /// capture groups with `$1`, `${name}`, etc.  Chain multiple `RenameKeys`
    /// transforms for multi-step pipelines.
    ///
    /// _Requires feature `transform-rename-keys` (default)._
    ///
    /// # Example
    ///
    /// ```text
    /// pattern = r"^_sdc_", replacement = ""   →   strip "_sdc_" prefix
    /// ```
    #[cfg(feature = "transform-rename-keys")]
    RenameKeys {
        pattern: String,
        replacement: String,
    },

    /// Convert all keys to `snake_case` using the same algorithm as Meltano's
    /// default key normaliser:
    ///
    /// 1. Strip characters that are neither alphanumeric nor whitespace.
    /// 2. Trim edges, then replace whitespace runs with `_`.
    /// 3. Collapse consecutive underscores.
    /// 4. Lowercase and trim leading/trailing underscores.
    ///
    /// _Requires feature `transform-snake-case` (default)._
    ///
    /// | Input key      | Output key     |
    /// |----------------|----------------|
    /// | `"First Name"` | `"first_name"` |
    /// | `"last-name"`  | `"lastname"`   |
    /// | `"price ($)"`  | `"price"`      |
    /// | `"ID"`         | `"id"`         |
    #[cfg(feature = "transform-snake-case")]
    KeysToSnakeCase,

    /// A user-supplied transformation function.
    ///
    /// The function receives each record as a [`Value`] and returns the
    /// (possibly modified) record.  Construct one with [`RecordTransform::custom`].
    ///
    /// Always available — not guarded by any feature flag.
    Custom(Arc<dyn Fn(Value) -> Value + Send + Sync>),
}

impl fmt::Debug for RecordTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "transform-flatten")]
            Self::Flatten { separator } => f
                .debug_struct("Flatten")
                .field("separator", separator)
                .finish(),
            #[cfg(feature = "transform-rename-keys")]
            Self::RenameKeys {
                pattern,
                replacement,
            } => f
                .debug_struct("RenameKeys")
                .field("pattern", pattern)
                .field("replacement", replacement)
                .finish(),
            #[cfg(feature = "transform-snake-case")]
            Self::KeysToSnakeCase => write!(f, "KeysToSnakeCase"),
            Self::Custom(_) => write!(f, "Custom(<fn>)"),
        }
    }
}

// Arc<dyn Fn> is Clone (bumps refcount) but #[derive(Clone)] can't see that,
// so we implement Clone manually.
impl Clone for RecordTransform {
    fn clone(&self) -> Self {
        match self {
            #[cfg(feature = "transform-flatten")]
            Self::Flatten { separator } => Self::Flatten {
                separator: separator.clone(),
            },
            #[cfg(feature = "transform-rename-keys")]
            Self::RenameKeys {
                pattern,
                replacement,
            } => Self::RenameKeys {
                pattern: pattern.clone(),
                replacement: replacement.clone(),
            },
            #[cfg(feature = "transform-snake-case")]
            Self::KeysToSnakeCase => Self::KeysToSnakeCase,
            Self::Custom(f) => Self::Custom(Arc::clone(f)),
        }
    }
}

impl RecordTransform {
    /// Create a custom transform from any function or closure.
    ///
    /// The closure receives each record as a [`Value`] and must return a
    /// [`Value`] (the transformed record).  It is called once per record and
    /// may perform any manipulation — adding fields, removing fields, renaming,
    /// type coercion, etc.
    ///
    /// Custom transforms are always available regardless of feature flags.
    ///
    /// # Example
    ///
    /// ```rust
    /// use faucet_core::RecordTransform;
    /// use serde_json::{Value, json};
    ///
    /// // Inject a constant "source" field into every record.
    /// let stamp = RecordTransform::custom(|mut record| {
    ///     if let Value::Object(ref mut map) = record {
    ///         map.insert("_source".to_string(), json!("my-api"));
    ///     }
    ///     record
    /// });
    /// ```
    pub fn custom<F>(f: F) -> Self
    where
        F: Fn(Value) -> Value + Send + Sync + 'static,
    {
        Self::Custom(Arc::new(f))
    }
}

// ── Internal compiled representation ─────────────────────────────────────────

/// Pre-compiled form of a [`RecordTransform`].
///
/// Stored inside [`crate::stream::RestStream`] so that regex patterns are
/// compiled exactly once (at [`crate::stream::RestStream::new`] time) rather
/// than once per record.
pub enum CompiledTransform {
    #[cfg(feature = "transform-flatten")]
    Flatten {
        separator: String,
    },
    #[cfg(feature = "transform-rename-keys")]
    RenameKeys {
        re: Regex,
        replacement: String,
    },
    #[cfg(feature = "transform-snake-case")]
    KeysToSnakeCase,
    Custom(Arc<dyn Fn(Value) -> Value + Send + Sync>),
}

/// Compile a [`RecordTransform`] into its [`CompiledTransform`] form.
///
/// Returns [`FaucetError::Transform`] if a regex pattern is invalid.
pub fn compile(t: &RecordTransform) -> Result<CompiledTransform, FaucetError> {
    match t {
        #[cfg(feature = "transform-flatten")]
        RecordTransform::Flatten { separator } => Ok(CompiledTransform::Flatten {
            separator: separator.clone(),
        }),
        #[cfg(feature = "transform-rename-keys")]
        RecordTransform::RenameKeys {
            pattern,
            replacement,
        } => {
            let re = Regex::new(pattern)
                .map_err(|e| FaucetError::Transform(format!("invalid regex '{pattern}': {e}")))?;
            Ok(CompiledTransform::RenameKeys {
                re,
                replacement: replacement.clone(),
            })
        }
        #[cfg(feature = "transform-snake-case")]
        RecordTransform::KeysToSnakeCase => Ok(CompiledTransform::KeysToSnakeCase),
        RecordTransform::Custom(f) => Ok(CompiledTransform::Custom(Arc::clone(f))),
    }
}

/// Apply a slice of pre-compiled transforms to a record, in order.
///
/// Returns [`FaucetError::Transform`] if a transform would silently lose data
/// — currently when `flatten` or `snake_case` collapse two distinct fields to
/// the same key (#78/#28).
pub fn apply_all(record: Value, transforms: &[CompiledTransform]) -> Result<Value, FaucetError> {
    let mut acc = record;
    for t in transforms {
        acc = apply_one(acc, t)?;
    }
    Ok(acc)
}

fn apply_one(value: Value, t: &CompiledTransform) -> Result<Value, FaucetError> {
    match t {
        #[cfg(feature = "transform-flatten")]
        CompiledTransform::Flatten { separator } => flatten(value, separator),
        #[cfg(feature = "transform-rename-keys")]
        CompiledTransform::RenameKeys { re, replacement } => {
            Ok(rename_keys(value, re, replacement))
        }
        #[cfg(feature = "transform-snake-case")]
        CompiledTransform::KeysToSnakeCase => keys_to_snake_case(value),
        CompiledTransform::Custom(f) => Ok(f(value)),
    }
}

// ── Flatten ───────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-flatten")]
fn flatten(value: Value, separator: &str) -> Result<Value, FaucetError> {
    match value {
        Value::Object(_) => {
            let mut out = Map::new();
            flatten_into(value, "", separator, &mut out)?;
            Ok(Value::Object(out))
        }
        other => Ok(other),
    }
}

#[cfg(feature = "transform-flatten")]
fn flatten_into(
    value: Value,
    prefix: &str,
    separator: &str,
    out: &mut Map<String, Value>,
) -> Result<(), FaucetError> {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let key = if prefix.is_empty() {
                    k
                } else {
                    format!("{prefix}{separator}{k}")
                };
                flatten_into(v, &key, separator, out)?;
            }
        }
        other => {
            // Erroring (rather than last-wins) avoids silently dropping a value
            // when a nested path and a literal key collide, e.g.
            // `{"a__b":1,"a":{"b":2}}` both map to `a__b` (#78/#28).
            if out.contains_key(prefix) {
                return Err(FaucetError::Transform(format!(
                    "flatten produced a duplicate key '{prefix}'; two distinct fields collapse \
                     to the same flattened key (separator '{separator}')"
                )));
            }
            out.insert(prefix.to_string(), other);
        }
    }
    Ok(())
}

// ── Rename keys ───────────────────────────────────────────────────────────────

#[cfg(feature = "transform-rename-keys")]
fn rename_keys(value: Value, re: &Regex, replacement: &str) -> Value {
    match value {
        Value::Object(map) => {
            let new_map: Map<String, Value> = map
                .into_iter()
                .map(|(k, v)| {
                    let new_k = re.replace_all(&k, replacement).into_owned();
                    (new_k, rename_keys(v, re, replacement))
                })
                .collect();
            Value::Object(new_map)
        }
        Value::Array(arr) => Value::Array(
            arr.into_iter()
                .map(|v| rename_keys(v, re, replacement))
                .collect(),
        ),
        other => other,
    }
}

// ── Keys to snake_case ────────────────────────────────────────────────────────

#[cfg(feature = "transform-snake-case")]
static RE_SPECIAL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[^a-zA-Z0-9\s]").expect("static regex"));

#[cfg(feature = "transform-snake-case")]
static RE_WHITESPACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").expect("static regex"));

#[cfg(feature = "transform-snake-case")]
static RE_MULTI_UNDERSCORE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"_+").expect("static regex"));

/// Convert a single key string to snake_case (mirrors Meltano's algorithm).
#[cfg(feature = "transform-snake-case")]
pub fn to_snake_case(key: &str) -> String {
    let s = RE_SPECIAL.replace_all(key, "");
    let s = RE_WHITESPACE.replace_all(s.trim(), "_");
    let s = RE_MULTI_UNDERSCORE.replace_all(&s, "_");
    s.to_lowercase().trim_matches('_').to_string()
}

#[cfg(feature = "transform-snake-case")]
fn keys_to_snake_case(value: Value) -> Result<Value, FaucetError> {
    match value {
        Value::Object(map) => {
            let mut new_map = Map::with_capacity(map.len());
            for (k, v) in map {
                // An all-special key like "!@#" snake_cases to "" — keep the
                // original key instead of producing an empty/blank key.
                let snaked = to_snake_case(&k);
                let new_k = if snaked.is_empty() { k } else { snaked };
                let new_v = keys_to_snake_case(v)?;
                // Erroring (rather than last-wins) avoids silently dropping a
                // value when two distinct keys snake_case to the same name,
                // e.g. "last-name" and "lastName" both → "last_name" (#78/#28).
                if new_map.contains_key(&new_k) {
                    return Err(FaucetError::Transform(format!(
                        "snake_case produced a duplicate key '{new_k}'; two distinct keys \
                         normalise to the same name"
                    )));
                }
                new_map.insert(new_k, new_v);
            }
            Ok(Value::Object(new_map))
        }
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(keys_to_snake_case(v)?);
            }
            Ok(Value::Array(out))
        }
        other => Ok(other),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Test-only wrapper that shadows [`super::apply_all`] and unwraps, so the
    /// many existing success-path tests need no changes now that `apply_all`
    /// returns `Result`. Collision tests call `super::apply_all` for the
    /// `Result` directly.
    fn apply_all(record: Value, transforms: &[CompiledTransform]) -> Value {
        super::apply_all(record, transforms).expect("transform should succeed in this test")
    }

    fn compiled(transforms: &[RecordTransform]) -> Vec<CompiledTransform> {
        transforms.iter().map(|t| compile(t).unwrap()).collect()
    }

    // ── Custom (always available) ─────────────────────────────────────────────

    #[test]
    fn test_custom_adds_field() {
        let record = json!({"id": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::custom(|mut v| {
                if let Value::Object(ref mut m) = v {
                    m.insert("added".to_string(), json!(true));
                }
                v
            })]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["added"], true);
    }

    #[test]
    fn test_custom_removes_field() {
        let record = json!({"id": 1, "secret": "drop_me"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::custom(|mut v| {
                if let Value::Object(ref mut m) = v {
                    m.remove("secret");
                }
                v
            })]),
        );
        assert_eq!(result["id"], 1);
        assert!(result.get("secret").is_none());
    }

    #[test]
    fn test_no_transforms_is_identity() {
        let record = json!({"id": 1, "name": "Alice"});
        let result = apply_all(record.clone(), &[]);
        assert_eq!(result, record);
    }

    // ── Flatten ───────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn test_flatten_nested_object() {
        let record = json!({"a": {"b": 1, "c": {"d": 2}}, "e": 3});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Flatten {
                separator: "__".into(),
            }]),
        );
        assert_eq!(result["a__b"], 1);
        assert_eq!(result["a__c__d"], 2);
        assert_eq!(result["e"], 3);
        assert!(result.get("a").is_none(), "nested key should be removed");
    }

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn test_flatten_leaves_arrays_intact() {
        let record = json!({"tags": ["rust", "api"], "meta": {"count": 2}});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Flatten {
                separator: ".".into(),
            }]),
        );
        assert_eq!(result["tags"], json!(["rust", "api"]));
        assert_eq!(result["meta.count"], 2);
    }

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn test_flatten_already_flat() {
        let record = json!({"id": 1, "name": "Alice"});
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Flatten {
                separator: "__".into(),
            }]),
        );
        assert_eq!(result, record);
    }

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn test_flatten_empty_separator() {
        let record = json!({"a": {"b": 1}});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Flatten {
                separator: "".into(),
            }]),
        );
        assert_eq!(result["ab"], 1);
    }

    // ── RenameKeys ────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-rename-keys")]
    #[test]
    fn test_rename_keys_strips_prefix() {
        let record = json!({"_prefix_id": 1, "_prefix_name": "Alice"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameKeys {
                pattern: r"^_prefix_".into(),
                replacement: "".into(),
            }]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["name"], "Alice");
    }

    #[cfg(feature = "transform-rename-keys")]
    #[test]
    fn test_rename_keys_uppercase_to_placeholder() {
        let record = json!({"OUTER": {"INNER": 42}});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameKeys {
                pattern: r"[A-Z]+".into(),
                replacement: "x".into(),
            }]),
        );
        assert_eq!(result["x"]["x"], 42);
    }

    #[cfg(feature = "transform-rename-keys")]
    #[test]
    fn test_rename_keys_in_array_elements() {
        let record = json!({"items": [{"KEY": 1}, {"KEY": 2}]});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameKeys {
                pattern: r"KEY".into(),
                replacement: "key".into(),
            }]),
        );
        assert_eq!(result["items"][0]["key"], 1);
        assert_eq!(result["items"][1]["key"], 2);
    }

    #[cfg(feature = "transform-rename-keys")]
    #[test]
    fn test_rename_keys_invalid_regex_errors_at_compile() {
        let err = compile(&RecordTransform::RenameKeys {
            pattern: "[invalid".into(),
            replacement: "".into(),
        });
        assert!(err.is_err());
        assert!(matches!(err, Err(FaucetError::Transform(_))));
    }

    #[cfg(feature = "transform-rename-keys")]
    #[test]
    fn test_rename_keys_chained() {
        let record = json!({"__camelCase__": 1});
        let result = apply_all(
            record,
            &compiled(&[
                RecordTransform::RenameKeys {
                    pattern: r"^_+|_+$".into(),
                    replacement: "".into(),
                },
                RecordTransform::RenameKeys {
                    pattern: r"[A-Z]".into(),
                    replacement: "_".into(),
                },
            ]),
        );
        let key = result.as_object().unwrap().keys().next().unwrap().clone();
        assert_eq!(key, "camel_ase");
    }

    // ── KeysToSnakeCase ───────────────────────────────────────────────────────

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn test_snake_case_spaces_to_underscores() {
        assert_eq!(to_snake_case("First Name"), "first_name");
        assert_eq!(to_snake_case("last name"), "last_name");
    }

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn test_snake_case_removes_hyphens_and_special_chars() {
        assert_eq!(to_snake_case("last-name"), "lastname");
        assert_eq!(to_snake_case("price ($)"), "price");
    }

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn test_snake_case_trims_edge_whitespace() {
        assert_eq!(to_snake_case("  id  "), "id");
        assert_eq!(to_snake_case("  first name  "), "first_name");
    }

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn test_snake_case_lowercases() {
        assert_eq!(to_snake_case("ID"), "id");
        assert_eq!(to_snake_case("UserName"), "username");
    }

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn test_snake_case_collapses_underscores_from_spaces() {
        assert_eq!(to_snake_case("foo   bar"), "foo_bar");
    }

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn test_snake_case_empty_after_stripping() {
        assert_eq!(to_snake_case("!@#"), "");
        assert_eq!(to_snake_case("---"), "");
    }

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn test_keys_to_snake_case_on_record() {
        let record = json!({
            "First Name": "Alice",
            "last-name": "Smith",
            "price ($)": 9.99,
            "  id  ": 1,
        });
        let result = apply_all(record, &compiled(&[RecordTransform::KeysToSnakeCase]));
        assert_eq!(result["first_name"], "Alice");
        assert_eq!(result["lastname"], "Smith");
        assert_eq!(result["price"], 9.99);
        assert_eq!(result["id"], 1);
    }

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn test_keys_to_snake_case_nested() {
        let record = json!({"Outer Key": {"Inner Key": 42}});
        let result = apply_all(record, &compiled(&[RecordTransform::KeysToSnakeCase]));
        assert_eq!(result["outer_key"]["inner_key"], 42);
    }

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn test_keys_to_snake_case_in_array() {
        let record = json!({"items": [{"MY KEY": 1}, {"MY KEY": 2}]});
        let result = apply_all(record, &compiled(&[RecordTransform::KeysToSnakeCase]));
        assert_eq!(result["items"][0]["my_key"], 1);
        assert_eq!(result["items"][1]["my_key"], 2);
    }

    // ── Chaining ──────────────────────────────────────────────────────────────

    #[cfg(all(feature = "transform-snake-case", feature = "transform-flatten"))]
    #[test]
    fn test_snake_case_then_flatten() {
        let record = json!({"User Info": {"First Name": "Alice", "Last Name": "Smith"}});
        let result = apply_all(
            record,
            &compiled(&[
                RecordTransform::KeysToSnakeCase,
                RecordTransform::Flatten {
                    separator: "_".into(),
                },
            ]),
        );
        assert_eq!(result["user_info_first_name"], "Alice");
        assert_eq!(result["user_info_last_name"], "Smith");
    }

    #[test]
    fn test_custom_chained_with_builtin() {
        // Custom runs before (or after) built-ins — ordering is preserved.
        let record = json!({"id": 1, "raw_value": 100});
        let result = apply_all(
            record,
            &compiled(&[
                // Step 1: custom — double raw_value
                RecordTransform::custom(|mut v| {
                    if let Some(n) = v.get("raw_value").and_then(|n| n.as_i64())
                        && let Value::Object(ref mut m) = v
                    {
                        m.insert("raw_value".to_string(), json!(n * 2));
                    }
                    v
                }),
                // Step 2: custom — rename raw_value → value
                RecordTransform::custom(|mut v| {
                    if let Value::Object(ref mut m) = v
                        && let Some(val) = m.remove("raw_value")
                    {
                        m.insert("value".to_string(), val);
                    }
                    v
                }),
            ]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["value"], 200);
        assert!(result.get("raw_value").is_none());
    }

    // ── #78/#28: collisions must error, not silently drop ──────────────────

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn flatten_key_collision_errors() {
        // `a__b` (literal) and `a.b` (nested) both flatten to `a__b`.
        let record = json!({"a__b": 1, "a": {"b": 2}});
        let err = super::apply_all(
            record,
            &compiled(&[RecordTransform::Flatten {
                separator: "__".into(),
            }]),
        )
        .expect_err("colliding flattened keys must error, not drop a value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("a__b"), "{err}");
    }

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn snake_case_key_collision_errors() {
        // "last-name" (dash stripped) and "lastname" both snake_case to "lastname".
        let record = json!({"last-name": "a", "lastname": "b"});
        let err = super::apply_all(record, &compiled(&[RecordTransform::KeysToSnakeCase]))
            .expect_err("colliding snake_case keys must error, not drop a value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("lastname"), "{err}");
    }

    #[cfg(feature = "transform-snake-case")]
    #[test]
    fn snake_case_empty_output_keeps_original_key() {
        // A key that strips to "" must not become a blank key — keep it as-is.
        let record = json!({"!@#": 1, "id": 2});
        let result = super::apply_all(record, &compiled(&[RecordTransform::KeysToSnakeCase]))
            .expect("no collision here");
        assert_eq!(
            result["!@#"], 1,
            "original key retained when snake_case is empty"
        );
        assert_eq!(result["id"], 2);
    }
}
