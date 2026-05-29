//! Record transformation pipeline.
//!
//! ## Built-in transforms (optional Cargo features)
//!
//! | Variant | Feature flag | Default |
//! |---------|-------------|---------|
//! | [`RecordTransform::Flatten`] | `transform-flatten` | enabled |
//! | [`RecordTransform::RenameKeys`] | `transform-rename-keys` | enabled |
//! | [`RecordTransform::KeysCase`] | `transform-keys-case` | enabled |
//! | [`RecordTransform::Select`] | `transform-select` | off |
//! | [`RecordTransform::Drop`] | `transform-drop` | off |
//! | [`RecordTransform::Set`] | `transform-set` | off |
//! | [`RecordTransform::RenameField`] | `transform-rename-field` | off |
//! | [`RecordTransform::Cast`] | `transform-cast` | off |
//! | [`RecordTransform::Redact`] | `transform-redact` | off |
//! | [`RecordTransform::ValueCase`] | `transform-value-case` | off |
//! | [`RecordTransform::SpellSymbols`] | `transform-spell-symbols` | off |
//!
//! The `transforms` aggregate feature pulls in every variant above.
//!
//! Disable a transform (and its dependencies) by opting out of its feature:
//!
//! ```toml
//! [dependencies]
//! faucet-stream = { version = "*", default-features = false,
//!                   features = ["transform-flatten"] }
//! ```
//!
//! ## Stage-level transforms (filter / explode)
//!
//! `filter` and `explode` are not `RecordTransform` variants — they live as
//! [`crate::stage::TransformStage::Filter`] / `TransformStage::Explode` because
//! they may emit 0 or N records per input. Their feature flags are
//! `transform-filter` and `transform-explode`. See the `stage` module for
//! details.
//!
//! ## Custom transforms
//!
//! [`RecordTransform::Custom`] is always available regardless of features.
//! Pass any closure or function pointer via [`RecordTransform::custom`].

use crate::error::FaucetError;
#[cfg(any(
    feature = "transform-flatten",
    feature = "transform-rename-keys",
    feature = "transform-keys-case",
    feature = "transform-set",
))]
use serde_json::Map;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

#[cfg(any(
    feature = "transform-cast",
    feature = "transform-rename-field",
    feature = "transform-value-case",
    feature = "transform-spell-symbols",
))]
use std::collections::HashMap;

#[cfg(feature = "transform-rename-keys")]
use regex::Regex;

// ── Support enums for the new transforms ──────────────────────────────────────

/// Target type for [`RecordTransform::Cast`].
///
/// Coerces a JSON value to the requested concrete type.  `Timestamp` parses
/// RFC 3339 / ISO 8601 strings and normalises them back to RFC 3339 (so
/// `"2026-05-28T00:00:00Z"` round-trips unchanged but `"2026-05-28T00:00:00+00:00"`
/// becomes the canonical form).
#[cfg(feature = "transform-cast")]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum CastType {
    /// 64-bit signed integer (`i64`).
    Int,
    /// 64-bit float (`f64`).
    Float,
    /// Boolean.  Accepts `true`/`false`/`1`/`0` (case-insensitive) when the
    /// source value is a string.
    Bool,
    /// String.  Numbers and booleans are stringified via `to_string()`.
    String,
    /// RFC 3339 timestamp, returned as a normalised RFC 3339 string.
    Timestamp,
}

/// Failure policy for [`RecordTransform::Cast`].  Default: `Error`.
#[cfg(feature = "transform-cast")]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    schemars::JsonSchema,
    Default,
)]
#[serde(rename_all = "lowercase")]
pub enum CastOnError {
    /// Return [`FaucetError::Transform`] when a value cannot be cast.
    #[default]
    Error,
    /// Replace the un-castable value with [`Value::Null`].
    Null,
    /// Leave the un-castable value unchanged in the record.
    Skip,
}

/// Output convention for [`RecordTransform::KeysCase`].
///
/// The transform tokenises each key on whitespace, `_`, `-`, dropped
/// punctuation, and lower→upper transitions, then re-joins the tokens in
/// the requested style.
#[cfg(feature = "transform-keys-case")]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum KeyCaseMode {
    /// `snake_case` — words separated by `_`, all lowercase.
    Snake,
    /// `camelCase` — first token lowercase, subsequent tokens capitalised,
    /// no separator.
    Camel,
    /// `PascalCase` — every token capitalised, no separator.
    Pascal,
    /// `kebab-case` — words separated by `-`, all lowercase.
    Kebab,
    /// `SCREAMING_SNAKE_CASE` — words separated by `_`, all uppercase.
    ScreamingSnake,
}

/// String-value casing mode for [`RecordTransform::ValueCase`].
#[cfg(feature = "transform-value-case")]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum ValueCaseMode {
    /// Lowercase the value.
    Lower,
    /// Uppercase the value.
    Upper,
    /// Trim leading/trailing whitespace from the value.
    Trim,
}

// ── Public config-facing type ─────────────────────────────────────────────────

/// A transformation applied to every record fetched by a source (e.g. the REST
/// source's `RestStream`).
///
/// Transforms are applied in the order they are added via the owning source's
/// configuration (e.g. `RestStreamConfig::add_transform`).
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

    /// Re-case every key in the record according to `mode`.
    ///
    /// Tokenises each key on whitespace, `_`, `-`, dropped punctuation, and
    /// lower→upper transitions, then re-joins in the requested convention.
    /// Walks recursively into nested objects and arrays.  Two distinct keys
    /// that re-case to the same name error rather than silently overwriting.
    ///
    /// _Requires feature `transform-keys-case` (default)._
    ///
    /// | Input          | `Snake`        | `Camel`       | `Pascal`     | `Kebab`        | `ScreamingSnake` |
    /// |----------------|----------------|---------------|--------------|----------------|------------------|
    /// | `"First Name"` | `"first_name"` | `"firstName"` | `"FirstName"`| `"first-name"` | `"FIRST_NAME"`   |
    /// | `"last-name"`  | `"last_name"`  | `"lastName"`  | `"LastName"` | `"last-name"`  | `"LAST_NAME"`    |
    /// | `"camelCase"`  | `"camel_case"` | `"camelCase"` | `"CamelCase"`| `"camel-case"` | `"CAMEL_CASE"`   |
    #[cfg(feature = "transform-keys-case")]
    KeysCase { mode: KeyCaseMode },

    /// Keep only the listed top-level fields on each record; remove the rest.
    ///
    /// Missing fields are silently skipped (they don't introduce `null`s).
    /// Non-object records pass through unchanged.
    ///
    /// _Requires feature `transform-select`._
    #[cfg(feature = "transform-select")]
    Select { fields: Vec<String> },

    /// Remove the listed top-level fields from each record.
    ///
    /// Missing fields are silently skipped. Non-object records pass through.
    ///
    /// _Requires feature `transform-drop`._
    #[cfg(feature = "transform-drop")]
    Drop { fields: Vec<String> },

    /// Insert or overwrite top-level fields on each record with constant values.
    ///
    /// Existing fields with the same name are overwritten. Non-object records
    /// pass through unchanged.
    ///
    /// _Requires feature `transform-set`._
    #[cfg(feature = "transform-set")]
    Set { values: Map<String, Value> },

    /// Exact-name rename of one or more top-level fields.
    ///
    /// Unlike [`RecordTransform::RenameKeys`] (regex, recursive), this only
    /// touches exact top-level keys. Missing source fields are silently skipped.
    /// If a target name already exists on the record, the rename errors rather
    /// than silently overwriting.
    ///
    /// _Requires feature `transform-rename-field`._
    #[cfg(feature = "transform-rename-field")]
    RenameField {
        /// Map of `old_name -> new_name`.
        fields: HashMap<String, String>,
    },

    /// Coerce per-field types on each record.
    ///
    /// Each named field is converted to the matching [`CastType`]. The
    /// [`CastOnError`] policy controls failure behaviour. Missing fields are
    /// silently skipped (no `null`s introduced).
    ///
    /// _Requires feature `transform-cast`._
    #[cfg(feature = "transform-cast")]
    Cast {
        fields: HashMap<String, CastType>,
        on_error: CastOnError,
    },

    /// Replace each listed field's value with a constant mask.
    ///
    /// Missing fields are silently skipped (no mask inserted). Default mask is
    /// `"***"` when constructed from CLI config.
    ///
    /// _Requires feature `transform-redact`._
    #[cfg(feature = "transform-redact")]
    Redact { fields: Vec<String>, mask: Value },

    /// Lowercase / uppercase / trim string values on listed fields.
    ///
    /// Non-string field values pass through unchanged. Missing fields are
    /// silently skipped.
    ///
    /// _Requires feature `transform-value-case`._
    #[cfg(feature = "transform-value-case")]
    ValueCase {
        fields: Vec<String>,
        mode: ValueCaseMode,
    },

    /// Recursively spell out symbols inside every key with their word
    /// equivalents (`%` → `percent`, `#` → `number`, `$` → `dollar`, …).
    ///
    /// Built-in defaults cover the common ASCII symbols (see
    /// [`default_symbol_map`]); `extra` adds or overrides entries.  Each
    /// replacement is surrounded by `separator` (default `" "`) so a chained
    /// [`RecordTransform::KeysCase`] picks up the word boundary.
    /// Keys are walked recursively into nested objects and arrays, mirroring
    /// the existing key-shape transforms.  Two distinct keys that collapse to
    /// the same name error rather than silently overwriting.
    ///
    /// _Requires feature `transform-spell-symbols`._
    ///
    /// # Example
    ///
    /// ```text
    /// {"% sold": 1, "C# courses": 2}
    ///   →  (defaults, separator=" ")
    /// {" percent  sold": 1, "C number  courses": 2}
    /// ```
    #[cfg(feature = "transform-spell-symbols")]
    SpellSymbols {
        /// Additional mappings (merged on top of [`default_symbol_map`];
        /// entries with the same `from` override the default).
        extra: HashMap<String, String>,
        /// Inserted around each replacement so word boundaries survive a
        /// downstream `keys_case` step. Default `" "`.
        separator: String,
    },

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
            #[cfg(feature = "transform-keys-case")]
            Self::KeysCase { mode } => f.debug_struct("KeysCase").field("mode", mode).finish(),
            #[cfg(feature = "transform-select")]
            Self::Select { fields } => f.debug_struct("Select").field("fields", fields).finish(),
            #[cfg(feature = "transform-drop")]
            Self::Drop { fields } => f.debug_struct("Drop").field("fields", fields).finish(),
            #[cfg(feature = "transform-set")]
            Self::Set { values } => f.debug_struct("Set").field("values", values).finish(),
            #[cfg(feature = "transform-rename-field")]
            Self::RenameField { fields } => f
                .debug_struct("RenameField")
                .field("fields", fields)
                .finish(),
            #[cfg(feature = "transform-cast")]
            Self::Cast { fields, on_error } => f
                .debug_struct("Cast")
                .field("fields", fields)
                .field("on_error", on_error)
                .finish(),
            #[cfg(feature = "transform-redact")]
            Self::Redact { fields, mask } => f
                .debug_struct("Redact")
                .field("fields", fields)
                .field("mask", mask)
                .finish(),
            #[cfg(feature = "transform-value-case")]
            Self::ValueCase { fields, mode } => f
                .debug_struct("ValueCase")
                .field("fields", fields)
                .field("mode", mode)
                .finish(),
            #[cfg(feature = "transform-spell-symbols")]
            Self::SpellSymbols { extra, separator } => f
                .debug_struct("SpellSymbols")
                .field("extra", extra)
                .field("separator", separator)
                .finish(),
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
            #[cfg(feature = "transform-keys-case")]
            Self::KeysCase { mode } => Self::KeysCase { mode: *mode },
            #[cfg(feature = "transform-select")]
            Self::Select { fields } => Self::Select {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-drop")]
            Self::Drop { fields } => Self::Drop {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-set")]
            Self::Set { values } => Self::Set {
                values: values.clone(),
            },
            #[cfg(feature = "transform-rename-field")]
            Self::RenameField { fields } => Self::RenameField {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-cast")]
            Self::Cast { fields, on_error } => Self::Cast {
                fields: fields.clone(),
                on_error: *on_error,
            },
            #[cfg(feature = "transform-redact")]
            Self::Redact { fields, mask } => Self::Redact {
                fields: fields.clone(),
                mask: mask.clone(),
            },
            #[cfg(feature = "transform-value-case")]
            Self::ValueCase { fields, mode } => Self::ValueCase {
                fields: fields.clone(),
                mode: *mode,
            },
            #[cfg(feature = "transform-spell-symbols")]
            Self::SpellSymbols { extra, separator } => Self::SpellSymbols {
                extra: extra.clone(),
                separator: separator.clone(),
            },
            Self::Custom(f) => Self::Custom(Arc::clone(f)),
        }
    }
}

// Arc<dyn Fn> is Clone (bumps refcount) but #[derive(Clone)] can't see that,
// so we implement Clone manually.
impl Clone for CompiledTransform {
    fn clone(&self) -> Self {
        match self {
            #[cfg(feature = "transform-flatten")]
            Self::Flatten { separator } => Self::Flatten {
                separator: separator.clone(),
            },
            #[cfg(feature = "transform-rename-keys")]
            Self::RenameKeys { re, replacement } => Self::RenameKeys {
                re: re.clone(),
                replacement: replacement.clone(),
            },
            #[cfg(feature = "transform-keys-case")]
            Self::KeysCase { mode } => Self::KeysCase { mode: *mode },
            #[cfg(feature = "transform-select")]
            Self::Select { fields } => Self::Select {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-drop")]
            Self::Drop { fields } => Self::Drop {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-set")]
            Self::Set { values } => Self::Set {
                values: values.clone(),
            },
            #[cfg(feature = "transform-rename-field")]
            Self::RenameField { fields } => Self::RenameField {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-cast")]
            Self::Cast { fields, on_error } => Self::Cast {
                fields: fields.clone(),
                on_error: *on_error,
            },
            #[cfg(feature = "transform-redact")]
            Self::Redact { fields, mask } => Self::Redact {
                fields: fields.clone(),
                mask: mask.clone(),
            },
            #[cfg(feature = "transform-value-case")]
            Self::ValueCase { fields, mode } => Self::ValueCase {
                fields: fields.clone(),
                mode: *mode,
            },
            #[cfg(feature = "transform-spell-symbols")]
            Self::SpellSymbols {
                replacements,
                separator,
            } => Self::SpellSymbols {
                replacements: replacements.clone(),
                separator: separator.clone(),
            },
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
/// Stored inside a source (e.g. the REST source's `RestStream`) so that regex
/// patterns are compiled exactly once (at construction time) rather than once
/// per record.
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
    #[cfg(feature = "transform-keys-case")]
    KeysCase {
        mode: KeyCaseMode,
    },
    #[cfg(feature = "transform-select")]
    Select {
        fields: Vec<String>,
    },
    #[cfg(feature = "transform-drop")]
    Drop {
        fields: Vec<String>,
    },
    #[cfg(feature = "transform-set")]
    Set {
        values: Map<String, Value>,
    },
    #[cfg(feature = "transform-rename-field")]
    RenameField {
        fields: HashMap<String, String>,
    },
    #[cfg(feature = "transform-cast")]
    Cast {
        fields: HashMap<String, CastType>,
        on_error: CastOnError,
    },
    #[cfg(feature = "transform-redact")]
    Redact {
        fields: Vec<String>,
        mask: Value,
    },
    #[cfg(feature = "transform-value-case")]
    ValueCase {
        fields: Vec<String>,
        mode: ValueCaseMode,
    },
    #[cfg(feature = "transform-spell-symbols")]
    SpellSymbols {
        /// `(from, to)` pairs sorted by descending `from.len()` so longer
        /// patterns win when prefixes overlap (e.g. `"<="` before `"<"`).
        replacements: Vec<(String, String)>,
        separator: String,
    },
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
        #[cfg(feature = "transform-keys-case")]
        RecordTransform::KeysCase { mode } => Ok(CompiledTransform::KeysCase { mode: *mode }),
        #[cfg(feature = "transform-select")]
        RecordTransform::Select { fields } => Ok(CompiledTransform::Select {
            fields: fields.clone(),
        }),
        #[cfg(feature = "transform-drop")]
        RecordTransform::Drop { fields } => Ok(CompiledTransform::Drop {
            fields: fields.clone(),
        }),
        #[cfg(feature = "transform-set")]
        RecordTransform::Set { values } => Ok(CompiledTransform::Set {
            values: values.clone(),
        }),
        #[cfg(feature = "transform-rename-field")]
        RecordTransform::RenameField { fields } => Ok(CompiledTransform::RenameField {
            fields: fields.clone(),
        }),
        #[cfg(feature = "transform-cast")]
        RecordTransform::Cast { fields, on_error } => Ok(CompiledTransform::Cast {
            fields: fields.clone(),
            on_error: *on_error,
        }),
        #[cfg(feature = "transform-redact")]
        RecordTransform::Redact { fields, mask } => Ok(CompiledTransform::Redact {
            fields: fields.clone(),
            mask: mask.clone(),
        }),
        #[cfg(feature = "transform-value-case")]
        RecordTransform::ValueCase { fields, mode } => Ok(CompiledTransform::ValueCase {
            fields: fields.clone(),
            mode: *mode,
        }),
        #[cfg(feature = "transform-spell-symbols")]
        RecordTransform::SpellSymbols { extra, separator } => {
            // Merge defaults + user overrides into a single ordered list,
            // sorted longest-first so `"<="` beats `"<"` etc.
            let mut merged = default_symbol_map();
            for (k, v) in extra {
                merged.insert(k.clone(), v.clone());
            }
            let mut replacements: Vec<(String, String)> = merged.into_iter().collect();
            replacements.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
            Ok(CompiledTransform::SpellSymbols {
                replacements,
                separator: separator.clone(),
            })
        }
        RecordTransform::Custom(f) => Ok(CompiledTransform::Custom(Arc::clone(f))),
    }
}

/// Apply a slice of pre-compiled transforms to a record, in order.
///
/// Returns [`FaucetError::Transform`] if a transform would silently lose data
/// — currently when `flatten`, `keys_case`, or `spell_symbols` collapse two
/// distinct fields to the same key (#78/#28).
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
        #[cfg(feature = "transform-keys-case")]
        CompiledTransform::KeysCase { mode } => keys_case(value, *mode),
        #[cfg(feature = "transform-select")]
        CompiledTransform::Select { fields } => Ok(select_fields(value, fields)),
        #[cfg(feature = "transform-drop")]
        CompiledTransform::Drop { fields } => Ok(drop_fields(value, fields)),
        #[cfg(feature = "transform-set")]
        CompiledTransform::Set { values } => Ok(set_fields(value, values)),
        #[cfg(feature = "transform-rename-field")]
        CompiledTransform::RenameField { fields } => rename_field(value, fields),
        #[cfg(feature = "transform-cast")]
        CompiledTransform::Cast { fields, on_error } => cast_fields(value, fields, *on_error),
        #[cfg(feature = "transform-redact")]
        CompiledTransform::Redact { fields, mask } => Ok(redact_fields(value, fields, mask)),
        #[cfg(feature = "transform-value-case")]
        CompiledTransform::ValueCase { fields, mode } => Ok(value_case(value, fields, *mode)),
        #[cfg(feature = "transform-spell-symbols")]
        CompiledTransform::SpellSymbols {
            replacements,
            separator,
        } => spell_symbols(value, replacements, separator),
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

// ── KeysCase ──────────────────────────────────────────────────────────────────

/// Recursively re-case every key in the record according to `mode`.
#[cfg(feature = "transform-keys-case")]
fn keys_case(value: Value, mode: KeyCaseMode) -> Result<Value, FaucetError> {
    match value {
        Value::Object(map) => {
            let mut new_map = Map::with_capacity(map.len());
            for (k, v) in map {
                let tokens = tokenize_key(&k);
                let recased = if tokens.is_empty() {
                    // An all-symbol key tokenises to nothing — keep the
                    // original key instead of producing a blank one.
                    k
                } else {
                    apply_key_case(tokens, mode)
                };
                let new_v = keys_case(v, mode)?;
                if new_map.contains_key(&recased) {
                    return Err(FaucetError::Transform(format!(
                        "keys_case produced a duplicate key '{recased}'; two distinct keys \
                         re-case to the same name under mode {mode:?}"
                    )));
                }
                new_map.insert(recased, new_v);
            }
            Ok(Value::Object(new_map))
        }
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(keys_case(v, mode)?);
            }
            Ok(Value::Array(out))
        }
        other => Ok(other),
    }
}

/// Split a key into word tokens.  Boundaries: whitespace, `_`, `-`, any
/// other non-alphanumeric char, and lower→upper transitions (so
/// `firstName` splits as `["first", "Name"]`).  Multi-char uppercase runs
/// are left as one token (`"XMLParser"` → `["XMLParser"]`); document the
/// limitation in the cookbook rather than complicating the tokeniser.
#[cfg(feature = "transform-keys-case")]
fn tokenize_key(key: &str) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut prev_was_lower = false;
    for ch in key.chars() {
        if ch.is_alphanumeric() {
            if prev_was_lower && ch.is_uppercase() && !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            current.push(ch);
            prev_was_lower = ch.is_lowercase();
        } else {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            prev_was_lower = false;
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

#[cfg(feature = "transform-keys-case")]
fn apply_key_case(tokens: Vec<String>, mode: KeyCaseMode) -> String {
    match mode {
        KeyCaseMode::Snake => tokens
            .iter()
            .map(|t| t.to_lowercase())
            .collect::<Vec<_>>()
            .join("_"),
        KeyCaseMode::ScreamingSnake => tokens
            .iter()
            .map(|t| t.to_uppercase())
            .collect::<Vec<_>>()
            .join("_"),
        KeyCaseMode::Kebab => tokens
            .iter()
            .map(|t| t.to_lowercase())
            .collect::<Vec<_>>()
            .join("-"),
        KeyCaseMode::Camel => {
            let mut iter = tokens.into_iter();
            match iter.next() {
                None => String::new(),
                Some(first) => {
                    let mut out = first.to_lowercase();
                    for t in iter {
                        out.push_str(&capitalize_token(&t));
                    }
                    out
                }
            }
        }
        KeyCaseMode::Pascal => tokens
            .into_iter()
            .map(|t| capitalize_token(&t))
            .collect::<String>(),
    }
}

/// Lowercase the input then uppercase the first char.
#[cfg(feature = "transform-keys-case")]
fn capitalize_token(s: &str) -> String {
    let lower = s.to_lowercase();
    let mut chars = lower.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

// ── Select ────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-select")]
fn select_fields(value: Value, fields: &[String]) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = Map::with_capacity(fields.len().min(map.len()));
            // Preserve `fields` order so downstream consumers get a stable layout.
            for f in fields {
                if let Some(v) = map.get(f) {
                    out.insert(f.clone(), v.clone());
                }
            }
            Value::Object(out)
        }
        other => other,
    }
}

// ── Drop ──────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-drop")]
fn drop_fields(value: Value, fields: &[String]) -> Value {
    match value {
        Value::Object(mut map) => {
            for f in fields {
                map.remove(f);
            }
            Value::Object(map)
        }
        other => other,
    }
}

// ── Set ───────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-set")]
fn set_fields(value: Value, values: &Map<String, Value>) -> Value {
    match value {
        Value::Object(mut map) => {
            for (k, v) in values {
                map.insert(k.clone(), v.clone());
            }
            Value::Object(map)
        }
        other => other,
    }
}

// ── RenameField ───────────────────────────────────────────────────────────────

#[cfg(feature = "transform-rename-field")]
fn rename_field(value: Value, fields: &HashMap<String, String>) -> Result<Value, FaucetError> {
    match value {
        Value::Object(mut map) => {
            for (from, to) in fields {
                if from == to {
                    continue;
                }
                if let Some(v) = map.remove(from) {
                    // Erroring (rather than overwriting) avoids silently dropping a
                    // value when the target name already exists on the record —
                    // mirrors the collision semantics in `flatten` / `keys_case`.
                    if map.contains_key(to) {
                        return Err(FaucetError::Transform(format!(
                            "rename_field: target key '{to}' already exists on the record \
                             (renaming from '{from}')"
                        )));
                    }
                    map.insert(to.clone(), v);
                }
            }
            Ok(Value::Object(map))
        }
        other => Ok(other),
    }
}

// ── Cast ──────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-cast")]
fn cast_fields(
    value: Value,
    fields: &HashMap<String, CastType>,
    on_error: CastOnError,
) -> Result<Value, FaucetError> {
    match value {
        Value::Object(mut map) => {
            for (field, target) in fields {
                let Some(current) = map.get(field) else {
                    continue;
                };
                match cast_value(current, *target) {
                    Ok(new_val) => {
                        map.insert(field.clone(), new_val);
                    }
                    Err(msg) => match on_error {
                        CastOnError::Error => {
                            return Err(FaucetError::Transform(format!(
                                "cast: field '{field}' to {target:?} failed: {msg}"
                            )));
                        }
                        CastOnError::Null => {
                            map.insert(field.clone(), Value::Null);
                        }
                        CastOnError::Skip => { /* leave as-is */ }
                    },
                }
            }
            Ok(Value::Object(map))
        }
        other => Ok(other),
    }
}

/// Try to coerce a single [`Value`] to `target`.  Returns a human-readable
/// reason string on failure (the caller wraps it in `FaucetError::Transform`).
#[cfg(feature = "transform-cast")]
fn cast_value(v: &Value, target: CastType) -> Result<Value, String> {
    match target {
        CastType::Int => match v {
            Value::Number(n) => n
                .as_i64()
                .map(|i| Value::Number(i.into()))
                .or_else(|| n.as_f64().map(|f| Value::Number((f as i64).into())))
                .ok_or_else(|| format!("number '{n}' is not representable as i64")),
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .map(|i| Value::Number(i.into()))
                .map_err(|e| format!("'{s}' is not an integer: {e}")),
            Value::Bool(b) => Ok(Value::Number(i64::from(*b).into())),
            Value::Null => Err("null cannot be cast to int".to_owned()),
            Value::Array(_) | Value::Object(_) => {
                Err("composite values cannot be cast to int".to_owned())
            }
        },
        CastType::Float => match v {
            Value::Number(n) => n
                .as_f64()
                .and_then(|f| serde_json::Number::from_f64(f).map(Value::Number))
                .ok_or_else(|| format!("number '{n}' is not representable as f64")),
            Value::String(s) => s
                .trim()
                .parse::<f64>()
                .ok()
                .and_then(|f| serde_json::Number::from_f64(f).map(Value::Number))
                .ok_or_else(|| format!("'{s}' is not a float")),
            Value::Bool(b) => serde_json::Number::from_f64(if *b { 1.0 } else { 0.0 })
                .map(Value::Number)
                .ok_or_else(|| "could not encode bool as f64".to_owned()),
            Value::Null => Err("null cannot be cast to float".to_owned()),
            Value::Array(_) | Value::Object(_) => {
                Err("composite values cannot be cast to float".to_owned())
            }
        },
        CastType::Bool => match v {
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    match i {
                        0 => Ok(Value::Bool(false)),
                        1 => Ok(Value::Bool(true)),
                        _ => Err(format!("integer {i} is not 0 or 1")),
                    }
                } else {
                    Err(format!("number '{n}' is not 0 or 1"))
                }
            }
            Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "y" => Ok(Value::Bool(true)),
                "false" | "0" | "no" | "n" => Ok(Value::Bool(false)),
                other => Err(format!("'{other}' is not a recognised boolean")),
            },
            Value::Null => Err("null cannot be cast to bool".to_owned()),
            Value::Array(_) | Value::Object(_) => {
                Err("composite values cannot be cast to bool".to_owned())
            }
        },
        CastType::String => match v {
            Value::String(s) => Ok(Value::String(s.clone())),
            Value::Number(n) => Ok(Value::String(n.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            Value::Null => Err("null cannot be cast to string".to_owned()),
            Value::Array(_) | Value::Object(_) => {
                Err("composite values cannot be cast to string".to_owned())
            }
        },
        CastType::Timestamp => match v {
            Value::String(s) => chrono::DateTime::parse_from_rfc3339(s)
                .map(|dt| Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)))
                .map_err(|e| format!("'{s}' is not a valid RFC 3339 timestamp: {e}")),
            other => Err(format!(
                "cannot cast {} to timestamp (expected RFC 3339 string)",
                value_type_name(other)
            )),
        },
    }
}

#[cfg(feature = "transform-cast")]
fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// ── Redact ────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-redact")]
fn redact_fields(value: Value, fields: &[String], mask: &Value) -> Value {
    match value {
        Value::Object(mut map) => {
            for f in fields {
                if map.contains_key(f) {
                    map.insert(f.clone(), mask.clone());
                }
            }
            Value::Object(map)
        }
        other => other,
    }
}

// ── ValueCase ─────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-value-case")]
fn value_case(value: Value, fields: &[String], mode: ValueCaseMode) -> Value {
    match value {
        Value::Object(mut map) => {
            for f in fields {
                if let Some(Value::String(s)) = map.get(f) {
                    let new_s = match mode {
                        ValueCaseMode::Lower => s.to_lowercase(),
                        ValueCaseMode::Upper => s.to_uppercase(),
                        ValueCaseMode::Trim => s.trim().to_owned(),
                    };
                    map.insert(f.clone(), Value::String(new_s));
                }
            }
            Value::Object(map)
        }
        other => other,
    }
}

// ── SpellSymbols ──────────────────────────────────────────────────────────────

/// Built-in symbol → word map used by [`RecordTransform::SpellSymbols`].
///
/// The defaults cover the common ASCII symbols that downstream identifier
/// rules (`snake_case`, SQL column naming, JSON pointer paths) typically
/// strip or reject.  Symbols that are already identifier-safe (`_`, `-`,
/// `.`) are intentionally left alone; symbols that `keys_case` strips
/// outright (`(`, `)`, `[`, `]`, `:`, `,` …) are also omitted — chain
/// `keys_case` after `spell_symbols` if you need them removed.
#[cfg(feature = "transform-spell-symbols")]
pub fn default_symbol_map() -> HashMap<String, String> {
    let pairs: &[(&str, &str)] = &[
        ("%", "percent"),
        ("#", "number"),
        ("$", "dollar"),
        ("&", "and"),
        ("@", "at"),
        ("+", "plus"),
        ("*", "star"),
        ("=", "equals"),
        ("<", "lt"),
        (">", "gt"),
        ("/", "slash"),
        ("\\", "backslash"),
        ("|", "pipe"),
        ("^", "caret"),
        ("~", "tilde"),
    ];
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[cfg(feature = "transform-spell-symbols")]
fn spell_symbols(
    value: Value,
    replacements: &[(String, String)],
    separator: &str,
) -> Result<Value, FaucetError> {
    match value {
        Value::Object(map) => {
            let mut new_map = Map::with_capacity(map.len());
            for (k, v) in map {
                let new_k = spell_symbols_in_key(&k, replacements, separator);
                let new_v = spell_symbols(v, replacements, separator)?;
                // Erroring (rather than last-wins) avoids silently dropping a
                // value when two distinct keys spell to the same name — same
                // contract as `flatten` / `keys_case` (#78/#28).
                if new_map.contains_key(&new_k) {
                    return Err(FaucetError::Transform(format!(
                        "spell_symbols produced a duplicate key '{new_k}'; two distinct keys \
                         expand to the same name"
                    )));
                }
                new_map.insert(new_k, new_v);
            }
            Ok(Value::Object(new_map))
        }
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(spell_symbols(v, replacements, separator)?);
            }
            Ok(Value::Array(out))
        }
        other => Ok(other),
    }
}

/// Apply the (longest-first) `replacements` to a single key string,
/// inserting `separator` around each substitution so word boundaries
/// survive a downstream `keys_case` step.
#[cfg(feature = "transform-spell-symbols")]
fn spell_symbols_in_key(key: &str, replacements: &[(String, String)], separator: &str) -> String {
    // Walk the input left-to-right; at each position try the longest
    // replacement first. This avoids `"<="` being split by the shorter
    // `"<"` substitution.
    let bytes = key.as_bytes();
    let mut out = String::with_capacity(key.len());
    let mut i = 0;
    while i < bytes.len() {
        let mut matched = false;
        for (from, to) in replacements {
            let f = from.as_bytes();
            if !f.is_empty() && bytes[i..].starts_with(f) {
                out.push_str(separator);
                out.push_str(to);
                out.push_str(separator);
                i += f.len();
                matched = true;
                break;
            }
        }
        if !matched {
            // Step by one UTF-8 char. We have to walk the &str slice (not
            // the byte buffer) to respect codepoint boundaries.
            let ch = key[i..]
                .chars()
                .next()
                .expect("non-empty slice yields at least one char");
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
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

    // ── Chaining ──────────────────────────────────────────────────────────────

    #[cfg(all(feature = "transform-keys-case", feature = "transform-flatten"))]
    #[test]
    fn test_keys_case_then_flatten() {
        let record = json!({"User Info": {"First Name": "Alice", "Last Name": "Smith"}});
        let result = apply_all(
            record,
            &compiled(&[
                RecordTransform::KeysCase {
                    mode: KeyCaseMode::Snake,
                },
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

    // ── Select ────────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-select")]
    #[test]
    fn select_keeps_only_listed_fields() {
        let record = json!({"id": 1, "name": "Alice", "secret": "drop"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Select {
                fields: vec!["id".into(), "name".into()],
            }]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["name"], "Alice");
        assert!(result.get("secret").is_none());
    }

    #[cfg(feature = "transform-select")]
    #[test]
    fn select_missing_field_is_no_op() {
        // Listed field is absent — must not introduce a null.
        let record = json!({"id": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Select {
                fields: vec!["id".into(), "missing".into()],
            }]),
        );
        assert_eq!(result["id"], 1);
        assert!(result.get("missing").is_none());
    }

    #[cfg(feature = "transform-select")]
    #[test]
    fn select_passes_through_non_object() {
        let record = json!([1, 2, 3]);
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Select {
                fields: vec!["id".into()],
            }]),
        );
        assert_eq!(result, record);
    }

    // ── Drop ──────────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-drop")]
    #[test]
    fn drop_removes_listed_fields() {
        let record = json!({"id": 1, "ssn": "111-22-3333", "name": "Alice"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Drop {
                fields: vec!["ssn".into()],
            }]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["name"], "Alice");
        assert!(result.get("ssn").is_none());
    }

    #[cfg(feature = "transform-drop")]
    #[test]
    fn drop_missing_field_is_no_op() {
        let record = json!({"id": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Drop {
                fields: vec!["missing".into()],
            }]),
        );
        assert_eq!(result["id"], 1);
    }

    // ── Set ───────────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-set")]
    #[test]
    fn set_inserts_new_fields() {
        let record = json!({"id": 1});
        let mut values = Map::new();
        values.insert("_source".into(), json!("api"));
        values.insert("ingested_at".into(), json!("2026-01-01"));
        let result = apply_all(record, &compiled(&[RecordTransform::Set { values }]));
        assert_eq!(result["id"], 1);
        assert_eq!(result["_source"], "api");
        assert_eq!(result["ingested_at"], "2026-01-01");
    }

    #[cfg(feature = "transform-set")]
    #[test]
    fn set_overwrites_existing_field() {
        let record = json!({"_source": "old", "id": 1});
        let mut values = Map::new();
        values.insert("_source".into(), json!("new"));
        let result = apply_all(record, &compiled(&[RecordTransform::Set { values }]));
        assert_eq!(result["_source"], "new");
        assert_eq!(result["id"], 1);
    }

    #[cfg(feature = "transform-set")]
    #[test]
    fn set_supports_any_json_value() {
        let record = json!({});
        let mut values = Map::new();
        values.insert("n".into(), json!(42));
        values.insert("b".into(), json!(true));
        values.insert("arr".into(), json!([1, 2]));
        values.insert("obj".into(), json!({"k": "v"}));
        values.insert("null".into(), Value::Null);
        let result = apply_all(record, &compiled(&[RecordTransform::Set { values }]));
        assert_eq!(result["n"], 42);
        assert_eq!(result["b"], true);
        assert_eq!(result["arr"], json!([1, 2]));
        assert_eq!(result["obj"]["k"], "v");
        assert_eq!(result["null"], Value::Null);
    }

    // ── RenameField ───────────────────────────────────────────────────────────

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_renames_exact_key() {
        let record = json!({"old_name": 1, "keep": 2});
        let mut fields = HashMap::new();
        fields.insert("old_name".to_owned(), "new_name".to_owned());
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameField { fields }]),
        );
        assert_eq!(result["new_name"], 1);
        assert_eq!(result["keep"], 2);
        assert!(result.get("old_name").is_none());
    }

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_missing_source_is_no_op() {
        let record = json!({"id": 1});
        let mut fields = HashMap::new();
        fields.insert("missing".to_owned(), "renamed".to_owned());
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameField { fields }]),
        );
        assert_eq!(result["id"], 1);
        assert!(result.get("renamed").is_none());
    }

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_target_collision_errors() {
        let record = json!({"a": 1, "b": 2});
        let mut fields = HashMap::new();
        fields.insert("a".to_owned(), "b".to_owned());
        let err = super::apply_all(
            record,
            &compiled(&[RecordTransform::RenameField { fields }]),
        )
        .expect_err("collision must error, not overwrite");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("'b'"), "{err}");
    }

    // ── Cast ──────────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-cast")]
    fn cast_specs(field: &str, ty: CastType, on_error: CastOnError) -> Vec<RecordTransform> {
        let mut fields = HashMap::new();
        fields.insert(field.to_owned(), ty);
        vec![RecordTransform::Cast { fields, on_error }]
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_string_to_int() {
        let record = json!({"age": "42"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("age", CastType::Int, CastOnError::Error)),
        );
        assert_eq!(result["age"], 42);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_string_to_float() {
        let record = json!({"price": "9.99"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("price", CastType::Float, CastOnError::Error)),
        );
        assert_eq!(result["price"], 9.99);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_string_to_bool() {
        for input in ["true", "TRUE", "1", "yes"] {
            let record = json!({"flag": input});
            let result = apply_all(
                record,
                &compiled(&cast_specs("flag", CastType::Bool, CastOnError::Error)),
            );
            assert_eq!(result["flag"], true, "input was {input:?}");
        }
        for input in ["false", "0", "no"] {
            let record = json!({"flag": input});
            let result = apply_all(
                record,
                &compiled(&cast_specs("flag", CastType::Bool, CastOnError::Error)),
            );
            assert_eq!(result["flag"], false, "input was {input:?}");
        }
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_number_to_string() {
        let record = json!({"id": 42});
        let result = apply_all(
            record,
            &compiled(&cast_specs("id", CastType::String, CastOnError::Error)),
        );
        assert_eq!(result["id"], "42");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_string_to_timestamp_normalises() {
        let record = json!({"ts": "2026-05-28T12:34:56+00:00"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("ts", CastType::Timestamp, CastOnError::Error)),
        );
        // `+00:00` normalises to `Z` via chrono's RFC 3339 emitter.
        assert_eq!(result["ts"], "2026-05-28T12:34:56Z");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_on_error_error_propagates() {
        let record = json!({"age": "not a number"});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("age", CastType::Int, CastOnError::Error)),
        )
        .expect_err("uncastable value must error under on_error=error");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("'age'"), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_on_error_null_replaces() {
        let record = json!({"age": "not a number"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("age", CastType::Int, CastOnError::Null)),
        );
        assert_eq!(result["age"], Value::Null);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_on_error_skip_leaves_value() {
        let record = json!({"age": "not a number"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("age", CastType::Int, CastOnError::Skip)),
        );
        assert_eq!(result["age"], "not a number");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_missing_field_is_no_op() {
        let record = json!({"id": 1});
        let result = apply_all(
            record,
            &compiled(&cast_specs("missing", CastType::Int, CastOnError::Error)),
        );
        assert_eq!(result["id"], 1);
        assert!(result.get("missing").is_none());
    }

    // ── Redact ────────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-redact")]
    #[test]
    fn redact_replaces_value_with_mask() {
        let record = json!({"id": 1, "ssn": "111-22-3333", "email": "x@y.z"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Redact {
                fields: vec!["ssn".into(), "email".into()],
                mask: json!("***"),
            }]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["ssn"], "***");
        assert_eq!(result["email"], "***");
    }

    #[cfg(feature = "transform-redact")]
    #[test]
    fn redact_missing_field_does_not_insert_mask() {
        let record = json!({"id": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Redact {
                fields: vec!["ssn".into()],
                mask: json!("***"),
            }]),
        );
        assert_eq!(result["id"], 1);
        assert!(result.get("ssn").is_none());
    }

    // ── ValueCase ─────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_lower() {
        let record = json!({"email": "User@Example.COM", "id": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["email".into()],
                mode: ValueCaseMode::Lower,
            }]),
        );
        assert_eq!(result["email"], "user@example.com");
        assert_eq!(result["id"], 1);
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_upper() {
        let record = json!({"code": "abc"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["code".into()],
                mode: ValueCaseMode::Upper,
            }]),
        );
        assert_eq!(result["code"], "ABC");
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_trim() {
        let record = json!({"name": "  Alice  "});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["name".into()],
                mode: ValueCaseMode::Trim,
            }]),
        );
        assert_eq!(result["name"], "Alice");
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_passes_non_string_through() {
        let record = json!({"id": 42});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["id".into()],
                mode: ValueCaseMode::Upper,
            }]),
        );
        assert_eq!(result["id"], 42);
    }

    // ── SpellSymbols ──────────────────────────────────────────────────────────

    #[cfg(feature = "transform-spell-symbols")]
    fn spell_default() -> Vec<RecordTransform> {
        vec![RecordTransform::SpellSymbols {
            extra: HashMap::new(),
            separator: " ".into(),
        }]
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_replaces_common_symbols() {
        let record = json!({"%sold": 1, "C#course": 2, "$amount": 3});
        let result = apply_all(record, &compiled(&spell_default()));
        // Defaults insert " " around each replacement so a downstream
        // snake_case picks up the word boundary.
        assert!(result.get(" percent sold").is_some());
        assert!(result.get("C number course").is_some());
        assert!(result.get(" dollar amount").is_some());
    }

    #[cfg(all(feature = "transform-spell-symbols", feature = "transform-keys-case"))]
    #[test]
    fn spell_symbols_then_keys_case_pipeline() {
        let record = json!({"% sold": 10, "C# courses": 20});
        let result = super::apply_all(
            record,
            &compiled(&[
                RecordTransform::SpellSymbols {
                    extra: HashMap::new(),
                    separator: " ".into(),
                },
                RecordTransform::KeysCase {
                    mode: KeyCaseMode::Snake,
                },
            ]),
        )
        .expect("pipeline must succeed");
        assert_eq!(result["percent_sold"], 10);
        assert_eq!(result["c_number_courses"], 20);
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_extra_overrides_defaults() {
        let mut extra = HashMap::new();
        extra.insert("#".to_owned(), "hash".to_owned());
        extra.insert("©".to_owned(), "copyright".to_owned());
        let record = json!({"#tag": 1, "©2026": 2});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::SpellSymbols {
                extra,
                separator: " ".into(),
            }]),
        );
        // `#` override beats the default `"number"`.
        assert!(result.get(" hash tag").is_some());
        // `©` is not in the default map but the user added it.
        assert!(result.get(" copyright 2026").is_some());
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_longest_match_wins() {
        // Without longest-first ordering, `"<"` would shadow `"<="`.
        let mut extra = HashMap::new();
        extra.insert("<=".to_owned(), "lte".to_owned());
        let record = json!({"a<=b": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::SpellSymbols {
                extra,
                separator: " ".into(),
            }]),
        );
        assert!(result.get("a lte b").is_some());
        // Confirm `<` alone was NOT applied separately.
        assert!(result.get("a lt = b").is_none());
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_recursive_into_objects_and_arrays() {
        let record = json!({"outer&": {"inner%": [{"deep#": 1}]}});
        let result = apply_all(record, &compiled(&spell_default()));
        let outer_key = result.as_object().unwrap().keys().next().unwrap().clone();
        assert!(outer_key.contains("and"), "outer key was {outer_key:?}");
        let inner = &result[&outer_key];
        let inner_key = inner.as_object().unwrap().keys().next().unwrap().clone();
        assert!(inner_key.contains("percent"), "inner key was {inner_key:?}");
        let deep = &inner[&inner_key][0];
        let deep_key = deep.as_object().unwrap().keys().next().unwrap().clone();
        assert!(deep_key.contains("number"), "deep key was {deep_key:?}");
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_key_collision_errors() {
        // With separator "" both keys collapse to "percent".
        let record = json!({"%": 1, "percent": 2});
        let err = super::apply_all(
            record,
            &compiled(&[RecordTransform::SpellSymbols {
                extra: HashMap::new(),
                separator: "".into(),
            }]),
        )
        .expect_err("colliding spelled keys must error, not drop a value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("percent"), "{err}");
    }

    // ── KeysCase ──────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-keys-case")]
    fn keys_case_specs(mode: KeyCaseMode) -> Vec<RecordTransform> {
        vec![RecordTransform::KeysCase { mode }]
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_snake() {
        let record = json!({"First Name": 1, "last-name": 2, "ID": 3});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)));
        assert_eq!(result["first_name"], 1);
        assert_eq!(result["last_name"], 2);
        assert_eq!(result["id"], 3);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_camel_from_various_inputs() {
        // snake → camel
        let record = json!({"first_name": 1, "User ID": 2, "kebab-case": 3, "PascalCase": 4});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Camel)));
        assert_eq!(result["firstName"], 1);
        assert_eq!(result["userId"], 2);
        assert_eq!(result["kebabCase"], 3);
        assert_eq!(result["pascalCase"], 4);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_pascal() {
        let record = json!({"first_name": 1, "second name": 2});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Pascal)));
        assert_eq!(result["FirstName"], 1);
        assert_eq!(result["SecondName"], 2);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_kebab() {
        let record = json!({"firstName": 1, "second_name": 2});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Kebab)));
        assert_eq!(result["first-name"], 1);
        assert_eq!(result["second-name"], 2);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_screaming_snake() {
        let record = json!({"firstName": 1, "second name": 2});
        let result = apply_all(
            record,
            &compiled(&keys_case_specs(KeyCaseMode::ScreamingSnake)),
        );
        assert_eq!(result["FIRST_NAME"], 1);
        assert_eq!(result["SECOND_NAME"], 2);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_recursive_into_nested() {
        let record = json!({"User Info": {"First Name": "Alice", "items": [{"Tag Name": "x"}]}});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)));
        assert_eq!(result["user_info"]["first_name"], "Alice");
        assert_eq!(result["user_info"]["items"][0]["tag_name"], "x");
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_collision_errors() {
        // "firstName" and "first_name" both snake_case to "first_name".
        let record = json!({"firstName": 1, "first_name": 2});
        let err = super::apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)))
            .expect_err("colliding re-cased keys must error, not drop a value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("first_name"), "{err}");
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_all_symbol_key_kept_as_is() {
        // A key that tokenises to nothing must keep its original form rather
        // than producing an empty-string key.
        let record = json!({"!@#": 1, "id": 2});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)));
        assert_eq!(result["!@#"], 1);
        assert_eq!(result["id"], 2);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_idempotent_in_target_mode() {
        // Re-running the transform should be a no-op once keys are already in
        // the target shape.
        let record = json!({"first_name": 1});
        let once = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)));
        let twice = apply_all(
            once.clone(),
            &compiled(&keys_case_specs(KeyCaseMode::Snake)),
        );
        assert_eq!(once, twice);
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_handles_unicode_keys() {
        // A non-ASCII char with a UTF-8 length > 1 must not corrupt the walk.
        let record = json!({"café%": 1});
        let result = apply_all(record, &compiled(&spell_default()));
        let key = result.as_object().unwrap().keys().next().unwrap().clone();
        assert!(key.contains("café"), "key was {key:?}");
        assert!(key.contains("percent"), "key was {key:?}");
    }
}
