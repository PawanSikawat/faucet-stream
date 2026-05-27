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

/// Walk an XML document with `quick_xml::Reader::read_event` and invoke
/// `on_record` once per element whose path matches the dot-separated
/// `records_element_path` selector. Records are materialised as JSON values
/// in the same shape `xml_to_json` would produce — attributes become `@key`
/// entries, repeated children become arrays, and a single `#text` child is
/// flattened to a bare string.
///
/// When `records_element_path` is `None` the entire document is emitted as
/// a single record (matches the eager `xml_to_json` behaviour).
///
/// The key difference from `xml_to_json` is that subtree JSON values are
/// only materialised while inside a matched element — surrounding elements
/// are observed via the event stream but never accumulated, which bounds
/// memory to one matched element + the path stack regardless of total
/// document size. Combined with batched yielding in
/// [`crate::stream::XmlStream`]'s `stream_pages`, this keeps client-side
/// memory at `O(batch_size * record_size)` even for multi-gigabyte
/// payloads.
pub fn stream_extract<F: FnMut(Value)>(
    xml: &str,
    records_element_path: Option<&str>,
    mut on_record: F,
) -> Result<(), FaucetError> {
    let target_segments: Option<Vec<&str>> = records_element_path.map(|p| p.split('.').collect());

    let mut reader = Reader::from_str(xml);

    // Current element path: outer-most → inner-most element name.
    let mut path: Vec<String> = Vec::new();

    // When `Some(start_depth)`, we are currently building a subtree rooted
    // at the element opened at `path[start_depth]`. The subtree stack
    // mirrors `xml_to_json`'s stack but is rooted at the matched element
    // rather than the document.
    let mut start_depth: Option<usize> = None;
    let mut subtree: Vec<(String, Map<String, Value>)> = Vec::new();

    // When `records_element_path` is None, we eagerly build the whole
    // document and emit it as one record on EOF. This preserves the
    // historical "no path = full doc" behaviour.
    let mut full_doc: Option<Vec<(String, Map<String, Value>)>> = if target_segments.is_none() {
        Some(vec![("$root".into(), Map::new())])
    } else {
        None
    };

    /// Returns true when the current open-element path matches the target
    /// dot-path selector exactly (i.e. the element just opened is the
    /// repeating record element).
    fn path_matches(path: &[String], target: &[&str]) -> bool {
        path.len() == target.len() && path.iter().zip(target).all(|(a, b)| a.as_str() == *b)
    }

    /// Append a child value under `name` to the topmost frame, converting to
    /// an array on repetition (mirrors `xml_to_json`).
    fn append_child(parent: &mut Map<String, Value>, name: String, value: Value) {
        match parent.get_mut(&name) {
            Some(Value::Array(arr)) => arr.push(value),
            Some(existing) => {
                let prev = existing.clone();
                *existing = Value::Array(vec![prev, value]);
            }
            None => {
                parent.insert(name, value);
            }
        }
    }

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                let mut obj = Map::new();
                for attr in e.attributes().flatten() {
                    let key = format!("@{}", String::from_utf8_lossy(attr.key.as_ref()));
                    let val = String::from_utf8_lossy(&attr.value).into_owned();
                    obj.insert(key, Value::String(val));
                }

                path.push(name.clone());

                if let Some(doc) = full_doc.as_mut() {
                    doc.push((name, obj));
                } else if let Some(target) = target_segments.as_deref() {
                    if start_depth.is_some() {
                        subtree.push((name, obj));
                    } else if path_matches(&path, target) {
                        // Opening the matched element itself — start a new
                        // subtree builder rooted at it.
                        start_depth = Some(path.len() - 1);
                        subtree.push((name, obj));
                    }
                    // Otherwise: outside any matched element — drop the
                    // event without materialising anything.
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

                // Treat self-closing tag as a transient open+close at the
                // current depth.
                path.push(name.clone());
                let matches_target = target_segments
                    .as_deref()
                    .map(|t| path_matches(&path, t))
                    .unwrap_or(false);
                path.pop();

                if let Some(doc) = full_doc.as_mut() {
                    if let Some(parent) = doc.last_mut() {
                        append_child(&mut parent.1, name, value);
                    }
                } else if matches_target && start_depth.is_none() {
                    // Self-closing matched element: emit immediately.
                    on_record(value);
                } else if start_depth.is_some()
                    && let Some(parent) = subtree.last_mut()
                {
                    append_child(&mut parent.1, name, value);
                }
            }
            Ok(Event::End(_)) => {
                let name = path.pop().ok_or_else(|| {
                    FaucetError::Transform("malformed XML: unexpected end tag".into())
                })?;

                if let Some(doc) = full_doc.as_mut() {
                    let (popped_name, obj) = doc.pop().ok_or_else(|| {
                        FaucetError::Transform("malformed XML: no element on stack".into())
                    })?;
                    debug_assert_eq!(popped_name, name);
                    let value = if obj.len() == 1 && obj.contains_key("#text") {
                        obj.into_iter().next().unwrap().1
                    } else {
                        Value::Object(obj)
                    };
                    let parent = doc.last_mut().ok_or_else(|| {
                        FaucetError::Transform("malformed XML: no parent element".into())
                    })?;
                    append_child(&mut parent.1, popped_name, value);
                } else if let Some(depth) = start_depth {
                    let (popped_name, obj) = subtree.pop().ok_or_else(|| {
                        FaucetError::Transform("malformed XML: no element on subtree stack".into())
                    })?;
                    debug_assert_eq!(popped_name, name);
                    let value = if obj.len() == 1 && obj.contains_key("#text") {
                        obj.into_iter().next().unwrap().1
                    } else {
                        Value::Object(obj)
                    };

                    if subtree.is_empty() {
                        // We just closed the matched element itself —
                        // emit and reset.
                        debug_assert_eq!(path.len(), depth);
                        start_depth = None;
                        on_record(value);
                    } else if let Some(parent) = subtree.last_mut() {
                        append_child(&mut parent.1, popped_name, value);
                    }
                }
                // Outside any matched element and no full-doc mode: drop.
            }
            Ok(Event::Text(e)) => {
                let text = e
                    .unescape()
                    .map_err(|err| FaucetError::Transform(format!("XML decode error: {err}")))?
                    .trim()
                    .to_string();
                if text.is_empty() {
                    continue;
                }

                if let Some(doc) = full_doc.as_mut() {
                    if let Some(current) = doc.last_mut() {
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
                } else if start_depth.is_some()
                    && let Some(current) = subtree.last_mut()
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
            Ok(Event::Eof) => break,
            Ok(_) => {} // Comments, PIs, CDATA boundaries, etc.
            Err(e) => {
                return Err(FaucetError::Transform(format!("XML parse error: {e}")));
            }
        }
    }

    if let Some(mut doc) = full_doc {
        let (_, root) = doc
            .pop()
            .ok_or_else(|| FaucetError::Transform("empty XML document".into()))?;
        on_record(Value::Object(root));
    }

    Ok(())
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

    fn collect_stream_extract(xml: &str, path: Option<&str>) -> Vec<Value> {
        let mut out = Vec::new();
        stream_extract(xml, path, |v| out.push(v)).unwrap();
        out
    }

    #[test]
    fn stream_extract_matches_eager_path_extraction() {
        let xml = r#"<root>
            <user id="1"><name>Alice</name><age>30</age></user>
            <user id="2"><name>Bob</name><age>25</age></user>
            <user id="3"><name>Carol</name><age>40</age></user>
        </root>"#;
        let streamed = collect_stream_extract(xml, Some("root.user"));
        let eager = extract_at_path(&xml_to_json(xml).unwrap(), "root.user");
        assert_eq!(streamed, eager);
        assert_eq!(streamed.len(), 3);
        assert_eq!(streamed[0]["@id"], "1");
        assert_eq!(streamed[0]["name"], "Alice");
        assert_eq!(streamed[2]["name"], "Carol");
    }

    #[test]
    fn stream_extract_handles_nested_children_and_attrs() {
        let xml = r#"<root>
            <order id="A"><line><sku>X</sku><qty>2</qty></line><line><sku>Y</sku><qty>5</qty></line></order>
            <order id="B"><line><sku>Z</sku><qty>1</qty></line></order>
        </root>"#;
        let streamed = collect_stream_extract(xml, Some("root.order"));
        let eager = extract_at_path(&xml_to_json(xml).unwrap(), "root.order");
        assert_eq!(streamed, eager);
        assert_eq!(streamed.len(), 2);
        let lines = streamed[0]["line"].as_array().expect("repeated children");
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1]["sku"], "Y");
    }

    #[test]
    fn stream_extract_no_path_returns_full_doc_once() {
        let xml = r#"<root><a>1</a><b>2</b></root>"#;
        let streamed = collect_stream_extract(xml, None);
        let eager = xml_to_json(xml).unwrap();
        assert_eq!(streamed.len(), 1);
        assert_eq!(streamed[0], eager);
    }

    #[test]
    fn stream_extract_no_matches_emits_nothing() {
        let xml = r#"<root><a>1</a></root>"#;
        let streamed = collect_stream_extract(xml, Some("root.missing"));
        assert!(streamed.is_empty());
    }

    #[test]
    fn stream_extract_self_closing_matched_element() {
        let xml = r#"<root><item id="1"/><item id="2"/><item id="3"/></root>"#;
        let streamed = collect_stream_extract(xml, Some("root.item"));
        assert_eq!(streamed.len(), 3);
        assert_eq!(streamed[0]["@id"], "1");
        assert_eq!(streamed[2]["@id"], "3");
    }

    #[test]
    fn stream_extract_preserves_soap_namespaces() {
        let xml = r#"
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <GetUsersResponse>
                    <User><Name>Alice</Name></User>
                    <User><Name>Bob</Name></User>
                </GetUsersResponse>
            </soap:Body>
        </soap:Envelope>"#;
        let streamed =
            collect_stream_extract(xml, Some("soap:Envelope.soap:Body.GetUsersResponse.User"));
        let eager = extract_at_path(
            &xml_to_json(xml).unwrap(),
            "soap:Envelope.soap:Body.GetUsersResponse.User",
        );
        assert_eq!(streamed, eager);
        assert_eq!(streamed.len(), 2);
        assert_eq!(streamed[1]["Name"], "Bob");
    }
}
