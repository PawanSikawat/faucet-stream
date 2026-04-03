//! XML to JSON conversion.
//!
//! Converts XML documents to `serde_json::Value` preserving the element
//! hierarchy. Attributes are prefixed with `@`, text content uses `#text`.

use faucet_core::FaucetError;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde_json::{Map, Value, json};

/// Convert an XML string to a JSON value.
///
/// Elements become objects, repeated elements become arrays, attributes
/// are stored with `@` prefix, and text content uses `#text`.
pub fn xml_to_json(xml: &str) -> Result<Value, FaucetError> {
    let mut reader = Reader::from_str(xml);
    let mut stack: Vec<(String, Map<String, Value>)> = vec![("$root".into(), Map::new())];

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                let mut obj = Map::new();

                // Collect attributes.
                for attr in e.attributes().flatten() {
                    let key = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let val = String::from_utf8_lossy(&attr.value).into_owned();
                    obj.insert(key, Value::String(val));
                }

                stack.push((name, obj));
            }
            Ok(Event::End(_)) => {
                let (name, obj) = stack.pop().ok_or_else(|| {
                    FaucetError::Transform("malformed XML: unexpected end tag".into())
                })?;

                let value = if obj.len() == 1 && obj.contains_key("#text") {
                    // Simplify: element with only text becomes a string.
                    obj.into_iter().next().unwrap().1
                } else {
                    Value::Object(obj)
                };

                let parent = stack.last_mut().ok_or_else(|| {
                    FaucetError::Transform("malformed XML: no parent element".into())
                })?;

                // If the key already exists, convert to array.
                match parent.1.get_mut(&name) {
                    Some(Value::Array(arr)) => arr.push(value),
                    Some(existing) => {
                        let prev = existing.clone();
                        *existing = Value::Array(vec![prev, value]);
                    }
                    None => {
                        parent.1.insert(name, value);
                    }
                }
            }
            Ok(Event::Text(e)) => {
                let text = e
                    .unescape()
                    .map_err(|err| FaucetError::Transform(format!("XML decode error: {err}")))?
                    .trim()
                    .to_string();

                if !text.is_empty()
                    && let Some(current) = stack.last_mut()
                {
                    match current.1.get_mut("#text") {
                        Some(Value::String(s)) => {
                            s.push(' ');
                            s.push_str(&text);
                        }
                        _ => {
                            current.1.insert("#text".into(), Value::String(text));
                        }
                    }
                }
            }
            Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                let mut obj = Map::new();
                for attr in e.attributes().flatten() {
                    let key = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let val = String::from_utf8_lossy(&attr.value).into_owned();
                    obj.insert(key, Value::String(val));
                }
                let value = if obj.is_empty() {
                    json!(null)
                } else {
                    Value::Object(obj)
                };

                if let Some(parent) = stack.last_mut() {
                    match parent.1.get_mut(&name) {
                        Some(Value::Array(arr)) => arr.push(value),
                        Some(existing) => {
                            let prev = existing.clone();
                            *existing = Value::Array(vec![prev, value]);
                        }
                        None => {
                            parent.1.insert(name, value);
                        }
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {} // Skip comments, processing instructions, etc.
            Err(e) => {
                return Err(FaucetError::Transform(format!("XML parse error: {e}")));
            }
        }
    }

    let (_, root) = stack
        .pop()
        .ok_or_else(|| FaucetError::Transform("empty XML document".into()))?;

    Ok(Value::Object(root))
}

/// Navigate into a JSON value using a dot-separated path and extract
/// matching records. If the final element is an array, its items are
/// returned individually.
pub fn extract_at_path(value: &Value, path: &str) -> Vec<Value> {
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = value.clone();

    for seg in &segments {
        current = match current {
            Value::Object(ref map) => match map.get(*seg) {
                Some(v) => v.clone(),
                None => return vec![],
            },
            _ => return vec![],
        };
    }

    match current {
        Value::Array(arr) => arr,
        other => vec![other],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_xml_to_json() {
        let xml = r#"<root><name>Alice</name><age>30</age></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["name"], "Alice");
        assert_eq!(json["root"]["age"], "30");
    }

    #[test]
    fn repeated_elements_become_array() {
        let xml = r#"<root><item>a</item><item>b</item><item>c</item></root>"#;
        let json = xml_to_json(xml).unwrap();
        let items = json["root"]["item"].as_array().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], "a");
        assert_eq!(items[1], "b");
    }

    #[test]
    fn attributes_prefixed() {
        let xml = r#"<user id="42"><name>Bob</name></user>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["user"]["@id"], "42");
        assert_eq!(json["user"]["name"], "Bob");
    }

    #[test]
    fn nested_elements() {
        let xml = r#"<root><user><address><city>NYC</city></address></user></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["user"]["address"]["city"], "NYC");
    }

    #[test]
    fn empty_elements() {
        let xml = r#"<root><flag/></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert!(json["root"]["flag"].is_null());
    }

    #[test]
    fn empty_element_with_attr() {
        let xml = r#"<root><flag enabled="true"/></root>"#;
        let json = xml_to_json(xml).unwrap();
        assert_eq!(json["root"]["flag"]["@enabled"], "true");
    }

    #[test]
    fn extract_at_path_nested() {
        let val = json!({"root": {"users": {"user": [{"id": 1}, {"id": 2}]}}});
        let records = extract_at_path(&val, "root.users.user");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
    }

    #[test]
    fn extract_at_path_single_element() {
        let val = json!({"root": {"user": {"id": 1}}});
        let records = extract_at_path(&val, "root.user");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["id"], 1);
    }

    #[test]
    fn extract_at_path_missing() {
        let val = json!({"root": {}});
        let records = extract_at_path(&val, "root.users.user");
        assert!(records.is_empty());
    }

    #[test]
    fn soap_envelope() {
        let xml = r#"
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <GetUsersResponse>
                    <User><Name>Alice</Name></User>
                    <User><Name>Bob</Name></User>
                </GetUsersResponse>
            </soap:Body>
        </soap:Envelope>"#;
        let json = xml_to_json(xml).unwrap();
        let users = extract_at_path(&json, "soap:Envelope.soap:Body.GetUsersResponse.User");
        assert_eq!(users.len(), 2);
    }
}
