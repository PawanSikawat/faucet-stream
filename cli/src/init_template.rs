//! Schema-driven YAML template emitter for `faucet init`.
//!
//! Given a [`schemars`]-emitted JSON Schema for a connector config struct,
//! [`schema_to_yaml_template`] walks the schema's `properties` and produces a
//! comment-annotated YAML block suitable for pasting under `config:` in a
//! generated pipeline file. Required fields are surfaced with placeholder
//! values and a `# REQUIRED` comment; optional fields are commented out so
//! users can opt in by uncommenting and editing.
//!
//! The emitter is intentionally a string builder rather than a `serde_yaml`
//! round-trip: comments and required/optional distinctions matter to the
//! reader and would be lost by any round-trip through a generic YAML value.

use serde_json::Value;

/// Render the `properties` of `schema` as a YAML block, indented by
/// `indent_spaces` columns. Returns a string that always ends with `\n`.
///
/// `schema` is the root config schema; `$ref`s are resolved against
/// `schema["$defs"]` when present.
pub fn schema_to_yaml_template(schema: &Value, indent_spaces: usize) -> String {
    let defs = schema.get("$defs");
    let mut out = String::new();
    emit_object_properties(schema, defs, indent_spaces, &mut out);
    if out.is_empty() {
        let pad = " ".repeat(indent_spaces);
        out.push_str(&format!("{pad}{{}}\n"));
    }
    out
}

fn emit_object_properties(schema: &Value, defs: Option<&Value>, indent: usize, out: &mut String) {
    let schema = resolve_ref(schema, defs);
    let Some(props) = schema.get("properties").and_then(|v| v.as_object()) else {
        return;
    };
    let required: Vec<&str> = schema
        .get("required")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    for (key, prop_schema) in props {
        let is_required = required.contains(&key.as_str());
        emit_property(key, prop_schema, is_required, defs, indent, out);
    }
}

fn emit_property(
    key: &str,
    schema: &Value,
    required: bool,
    defs: Option<&Value>,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    let resolved = resolve_ref(schema, defs);
    let description = resolved
        .get("description")
        .and_then(|v| v.as_str())
        .map(collapse_whitespace);

    // Tagged-enum (oneOf with `type` discriminator) — pick the first variant
    // and expand it inline when required, comment it flat when optional.
    if let Some(variants) = tagged_enum_variants(&resolved, defs) {
        emit_tagged_enum(key, &variants, required, &description, defs, indent, out);
        return;
    }

    // Nested object with its own `properties` — recurse when required, flatten
    // to a comment when optional.
    if is_object_with_properties(&resolved) {
        if required {
            out.push_str(&format!("{pad}{key}:\n"));
            emit_object_properties(&resolved, defs, indent + 2, out);
        } else {
            let line_comment = describe(&description, &resolved);
            let suffix = if line_comment.is_empty() {
                String::new()
            } else {
                format!("    # {line_comment}")
            };
            out.push_str(&format!("{pad}# {key}: {{ ... }}{suffix}\n"));
        }
        return;
    }

    let placeholder = type_placeholder(&resolved);
    let value = resolved
        .get("default")
        .map(render_default)
        .unwrap_or(placeholder);

    let mut comment_parts: Vec<String> = Vec::new();
    if required {
        comment_parts.push("REQUIRED".to_string());
    }
    if let Some(d) = description.as_ref()
        && !d.is_empty()
    {
        comment_parts.push(d.clone());
    }
    if let Some(values) = enum_string_values(&resolved) {
        comment_parts.push(format!("one of: {}", values.join(", ")));
    }
    let comment = if comment_parts.is_empty() {
        String::new()
    } else {
        format!("    # {}", comment_parts.join(" — "))
    };

    if required {
        out.push_str(&format!("{pad}{key}: {value}{comment}\n"));
    } else {
        out.push_str(&format!("{pad}# {key}: {value}{comment}\n"));
    }
}

fn emit_tagged_enum(
    key: &str,
    variants: &[TaggedVariant<'_>],
    required: bool,
    _description: &Option<String>,
    defs: Option<&Value>,
    indent: usize,
    out: &mut String,
) {
    let pad = " ".repeat(indent);
    let inner_pad = " ".repeat(indent + 2);
    let first = &variants[0];
    let all_tags: Vec<&str> = variants.iter().map(|v| v.tag).collect();

    if required {
        out.push_str(&format!("{pad}{key}:\n"));
        out.push_str(&format!(
            "{inner_pad}type: {tag}    # one of: {tags}\n",
            tag = first.tag,
            tags = all_tags.join(", "),
        ));
        // Emit first variant's required fields (besides the discriminator).
        for (field_key, field_schema, field_required) in &first.fields {
            if *field_key == first.discriminator {
                continue;
            }
            emit_property(
                field_key,
                field_schema,
                *field_required,
                defs,
                indent + 2,
                out,
            );
        }
    } else {
        out.push_str(&format!(
            "{pad}# {key}: {{ type: {tag} }}    # one of: {tags}\n",
            tag = first.tag,
            tags = all_tags.join(", "),
        ));
    }
}

struct TaggedVariant<'a> {
    tag: &'a str,
    discriminator: &'a str,
    fields: Vec<(&'a str, &'a Value, bool)>,
}

fn tagged_enum_variants<'a>(
    schema: &'a Value,
    defs: Option<&'a Value>,
) -> Option<Vec<TaggedVariant<'a>>> {
    let arr = schema.get("oneOf")?.as_array()?;
    if arr.is_empty() {
        return None;
    }
    // Discover the discriminator from the first variant.
    let first = resolve_ref_borrowed(&arr[0], defs);
    let props = first.get("properties")?.as_object()?;
    let (disc, _) = props
        .iter()
        .find(|(_, v)| v.get("const").and_then(|c| c.as_str()).is_some())?;
    let mut variants = Vec::new();
    for v in arr {
        let resolved = resolve_ref_borrowed(v, defs);
        let v_props = resolved.get("properties").and_then(|p| p.as_object())?;
        let tag = v_props
            .get(disc)
            .and_then(|t| t.get("const"))
            .and_then(|c| c.as_str())?;
        let required: Vec<&str> = resolved
            .get("required")
            .and_then(|r| r.as_array())
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let fields = v_props
            .iter()
            .map(|(k, s)| (k.as_str(), s, required.contains(&k.as_str())))
            .collect();
        variants.push(TaggedVariant {
            tag,
            discriminator: disc,
            fields,
        });
    }
    Some(variants)
}

fn is_object_with_properties(schema: &Value) -> bool {
    schema_type(schema) == Some("object") && schema.get("properties").is_some()
}

fn schema_type(schema: &Value) -> Option<&str> {
    match schema.get("type") {
        Some(Value::String(s)) => Some(s.as_str()),
        Some(Value::Array(arr)) => arr.iter().filter_map(|v| v.as_str()).find(|s| *s != "null"),
        _ => None,
    }
}

fn type_placeholder(schema: &Value) -> String {
    match schema_type(schema) {
        Some("string") => "\"\"".to_string(),
        Some("integer") | Some("number") => "0".to_string(),
        Some("boolean") => "false".to_string(),
        Some("array") => "[]".to_string(),
        Some("object") => "{}".to_string(),
        _ => "null".to_string(),
    }
}

fn render_default(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
        Value::Array(a) if a.is_empty() => "[]".to_string(),
        Value::Object(o) if o.is_empty() => "{}".to_string(),
        // Non-trivial composite defaults are rendered as compact JSON, which
        // happens to be a valid YAML flow-style literal.
        other => other.to_string(),
    }
}

fn enum_string_values(schema: &Value) -> Option<Vec<&str>> {
    let arr = schema.get("enum")?.as_array()?;
    let values: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
    if values.is_empty() {
        None
    } else {
        Some(values)
    }
}

fn describe(description: &Option<String>, schema: &Value) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(d) = description.as_ref()
        && !d.is_empty()
    {
        parts.push(d.clone());
    }
    if let Some(values) = enum_string_values(schema) {
        parts.push(format!("one of: {}", values.join(", ")));
    }
    parts.join(" — ")
}

/// Collapse runs of whitespace into single spaces and truncate to a
/// reader-friendly preview: the first sentence, or 120 chars, whichever comes
/// first. Long rustdoc paragraphs become illegible when inlined as a YAML
/// comment, and the full text is still available via `faucet schema`.
fn collapse_whitespace(s: &str) -> String {
    let collapsed: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    const MAX: usize = 120;
    if let Some(idx) = collapsed.find(". ") {
        let head = &collapsed[..idx + 1];
        return head.to_string();
    }
    if collapsed.chars().count() > MAX {
        let mut truncated: String = collapsed.chars().take(MAX).collect();
        truncated.push('…');
        return truncated;
    }
    collapsed
}

fn resolve_ref(schema: &Value, defs: Option<&Value>) -> Value {
    resolve_ref_borrowed(schema, defs).clone()
}

fn resolve_ref_borrowed<'a>(schema: &'a Value, defs: Option<&'a Value>) -> &'a Value {
    let Some(reference) = schema.get("$ref").and_then(|v| v.as_str()) else {
        return schema;
    };
    let Some(name) = reference.strip_prefix("#/$defs/") else {
        return schema;
    };
    defs.and_then(|d| d.get(name)).unwrap_or(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_object_schema_emits_brace_placeholder() {
        let schema = json!({ "type": "object" });
        let yaml = schema_to_yaml_template(&schema, 6);
        assert_eq!(yaml, "      {}\n");
    }

    #[test]
    fn required_string_field_gets_quoted_placeholder_and_required_marker() {
        let schema = json!({
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Path to the output file." }
            },
            "required": ["path"]
        });
        let yaml = schema_to_yaml_template(&schema, 6);
        assert!(yaml.contains("path: \"\""), "missing path key: {yaml}");
        assert!(
            yaml.contains("# REQUIRED"),
            "missing REQUIRED comment: {yaml}"
        );
        assert!(yaml.contains("Path to the output file."));
    }

    #[test]
    fn required_integer_field_gets_zero_placeholder() {
        let schema = json!({
            "type": "object",
            "properties": { "port": { "type": "integer" } },
            "required": ["port"]
        });
        let yaml = schema_to_yaml_template(&schema, 0);
        assert!(yaml.contains("port: 0"));
        assert!(yaml.contains("# REQUIRED"));
    }

    #[test]
    fn required_boolean_field_gets_false_placeholder() {
        let schema = json!({
            "type": "object",
            "properties": { "ssl": { "type": "boolean" } },
            "required": ["ssl"]
        });
        let yaml = schema_to_yaml_template(&schema, 0);
        assert!(yaml.contains("ssl: false"));
    }

    #[test]
    fn optional_field_with_default_is_commented_out_with_default_value() {
        let schema = json!({
            "type": "object",
            "properties": {
                "batch_size": { "type": "integer", "default": 1000, "description": "Batch size." }
            }
        });
        let yaml = schema_to_yaml_template(&schema, 0);
        assert!(yaml.contains("# batch_size: 1000"));
        assert!(yaml.contains("Batch size."));
    }

    #[test]
    fn optional_field_without_default_is_commented_out_with_placeholder() {
        let schema = json!({
            "type": "object",
            "properties": {
                "label": { "type": "string", "description": "Friendly label." }
            }
        });
        let yaml = schema_to_yaml_template(&schema, 0);
        assert!(yaml.contains("# label: \"\""));
        assert!(yaml.contains("Friendly label."));
    }

    #[test]
    fn enum_values_appear_in_comment() {
        let schema = json!({
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"],
                    "default": "GET",
                    "description": "HTTP method."
                }
            }
        });
        let yaml = schema_to_yaml_template(&schema, 0);
        assert!(yaml.contains("# method: \"GET\""), "yaml: {yaml}");
        assert!(
            yaml.contains("one of: GET, POST, PUT, PATCH, DELETE"),
            "yaml: {yaml}"
        );
    }

    #[test]
    fn required_nested_object_recurses() {
        let schema = json!({
            "type": "object",
            "properties": {
                "address": {
                    "type": "object",
                    "properties": {
                        "city": { "type": "string" }
                    },
                    "required": ["city"]
                }
            },
            "required": ["address"]
        });
        let yaml = schema_to_yaml_template(&schema, 0);
        assert!(yaml.contains("address:\n"), "yaml: {yaml}");
        assert!(yaml.contains("  city: \"\""), "yaml: {yaml}");
        assert!(yaml.contains("# REQUIRED"));
    }

    #[test]
    fn optional_nested_object_flattens_to_comment() {
        let schema = json!({
            "type": "object",
            "properties": {
                "tls": {
                    "type": "object",
                    "properties": { "ca_path": { "type": "string" } },
                    "description": "TLS settings."
                }
            }
        });
        let yaml = schema_to_yaml_template(&schema, 0);
        assert!(yaml.contains("# tls: { ... }"), "yaml: {yaml}");
        assert!(yaml.contains("TLS settings."));
    }

    #[test]
    fn tagged_enum_required_expands_first_variant_inline() {
        let schema = json!({
            "type": "object",
            "properties": {
                "auth": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": { "type": { "const": "none" } },
                            "required": ["type"]
                        },
                        {
                            "type": "object",
                            "properties": {
                                "type": { "const": "bearer" },
                                "token": { "type": "string" }
                            },
                            "required": ["type", "token"]
                        }
                    ]
                }
            },
            "required": ["auth"]
        });
        let yaml = schema_to_yaml_template(&schema, 0);
        assert!(yaml.contains("auth:\n"), "yaml: {yaml}");
        assert!(yaml.contains("type: none"), "yaml: {yaml}");
        assert!(yaml.contains("one of: none, bearer"), "yaml: {yaml}");
    }

    #[test]
    fn tagged_enum_optional_flattens_to_first_variant_comment() {
        let schema = json!({
            "type": "object",
            "properties": {
                "auth": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": { "type": { "const": "none" } },
                            "required": ["type"]
                        },
                        {
                            "type": "object",
                            "properties": {
                                "type": { "const": "bearer" },
                                "token": { "type": "string" }
                            },
                            "required": ["type", "token"]
                        }
                    ]
                }
            }
        });
        let yaml = schema_to_yaml_template(&schema, 0);
        assert!(yaml.contains("# auth: { type: none }"), "yaml: {yaml}");
        assert!(yaml.contains("one of: none, bearer"), "yaml: {yaml}");
    }

    #[test]
    fn ref_to_defs_is_resolved() {
        let schema = json!({
            "type": "object",
            "properties": {
                "creds": { "$ref": "#/$defs/Creds" }
            },
            "required": ["creds"],
            "$defs": {
                "Creds": {
                    "type": "object",
                    "properties": { "token": { "type": "string" } },
                    "required": ["token"]
                }
            }
        });
        let yaml = schema_to_yaml_template(&schema, 0);
        assert!(yaml.contains("creds:\n"), "yaml: {yaml}");
        assert!(yaml.contains("  token: \"\""), "yaml: {yaml}");
    }

    #[test]
    fn output_indent_matches_requested_column() {
        let schema = json!({
            "type": "object",
            "properties": { "name": { "type": "string" } },
            "required": ["name"]
        });
        let yaml = schema_to_yaml_template(&schema, 4);
        for line in yaml.lines() {
            if !line.trim().is_empty() {
                assert!(
                    line.starts_with("    "),
                    "line not indented 4 spaces: {line:?}"
                );
            }
        }
    }
}
