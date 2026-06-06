//! Convert a driver `ChangeStreamEvent` into the faucet CDC envelope.

use crate::state::Bookmark;
use faucet_core::FaucetError;
use mongodb::bson::{Bson, Document};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType};
use serde_json::{json, Value};

/// Map a driver `OperationType` to the envelope `op` string.
pub fn op_str(op: &OperationType) -> &'static str {
    match op {
        OperationType::Insert => "c",
        OperationType::Update => "u",
        OperationType::Replace => "r",
        OperationType::Delete => "d",
        OperationType::Drop
        | OperationType::Rename
        | OperationType::DropDatabase
        | OperationType::Invalidate => "ddl",
        _ => "ddl",
    }
}

fn doc_to_json(doc: &Document) -> Value {
    Bson::Document(doc.clone()).into_relaxed_extjson()
}

/// Build the CDC envelope for one change event. `bookmark` carries the event's
/// resume token serialized to JSON.
pub fn to_envelope(
    event: &ChangeStreamEvent<Document>,
    bookmark: &Bookmark,
) -> Result<Value, FaucetError> {
    let mut obj = serde_json::Map::new();
    obj.insert("op".into(), json!(op_str(&event.operation_type)));

    if let Some(ts) = &event.cluster_time {
        obj.insert("ts_ms".into(), json!(i64::from(ts.time) * 1000));
    } else {
        obj.insert("ts_ms".into(), Value::Null);
    }

    let mut ns = serde_json::Map::new();
    if let Some(n) = &event.ns {
        ns.insert("db".into(), json!(n.db));
        ns.insert("coll".into(), n.coll.clone().map(Value::String).unwrap_or(Value::Null));
    }
    obj.insert("namespace".into(), Value::Object(ns));

    obj.insert(
        "document_key".into(),
        event.document_key.as_ref().map(doc_to_json).unwrap_or(Value::Null),
    );
    obj.insert(
        "before".into(),
        event.full_document_before_change.as_ref().map(doc_to_json).unwrap_or(Value::Null),
    );
    obj.insert(
        "after".into(),
        event.full_document.as_ref().map(doc_to_json).unwrap_or(Value::Null),
    );

    if let Some(ud) = &event.update_description {
        obj.insert(
            "update_description".into(),
            json!({
                "updated_fields": doc_to_json(&ud.updated_fields),
                "removed_fields": ud.removed_fields,
            }),
        );
    }

    obj.insert("resume_token".into(), bookmark.resume_token.clone());
    Ok(Value::Object(obj))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson;
    use serde_json::json;

    fn event_from_json(v: Value) -> ChangeStreamEvent<Document> {
        let bson = bson::to_bson(&v).unwrap();
        bson::from_bson(bson).unwrap()
    }

    fn fixture(op: &str) -> Value {
        json!({
            "_id": { "_data": "8264AB00" },
            "operationType": op,
            "clusterTime": { "$timestamp": { "t": 1716700000_u32, "i": 1 } },
            "ns": { "db": "app", "coll": "users" },
            "documentKey": { "_id": 42 },
            "fullDocument": { "_id": 42, "name": "Alice" }
        })
    }

    fn bookmark() -> Bookmark {
        Bookmark { resume_token: json!({ "_data": "8264AB00" }) }
    }

    #[test]
    fn insert_maps_to_c() {
        let e = event_from_json(fixture("insert"));
        let env = to_envelope(&e, &bookmark()).unwrap();
        assert_eq!(env["op"], "c");
        assert_eq!(env["ts_ms"], json!(1716700000_i64 * 1000));
        assert_eq!(env["namespace"]["db"], "app");
        assert_eq!(env["namespace"]["coll"], "users");
        assert_eq!(env["after"]["name"], "Alice");
        assert_eq!(env["resume_token"]["_data"], "8264AB00");
    }

    #[test]
    fn update_maps_to_u_with_description() {
        let mut v = fixture("update");
        v["updateDescription"] = json!({
            "updatedFields": { "name": "Bob" },
            "removedFields": ["legacy"]
        });
        let e = event_from_json(v);
        let env = to_envelope(&e, &bookmark()).unwrap();
        assert_eq!(env["op"], "u");
        assert_eq!(env["update_description"]["updated_fields"]["name"], "Bob");
        assert_eq!(env["update_description"]["removed_fields"][0], "legacy");
    }

    #[test]
    fn replace_and_delete_map() {
        assert_eq!(to_envelope(&event_from_json(fixture("replace")), &bookmark()).unwrap()["op"], "r");
        let mut del = fixture("delete");
        del.as_object_mut().unwrap().remove("fullDocument");
        let env = to_envelope(&event_from_json(del), &bookmark()).unwrap();
        assert_eq!(env["op"], "d");
        assert_eq!(env["after"], Value::Null);
    }

    #[test]
    fn drop_maps_to_ddl() {
        let mut v = fixture("drop");
        v.as_object_mut().unwrap().remove("documentKey");
        v.as_object_mut().unwrap().remove("fullDocument");
        let env = to_envelope(&event_from_json(v), &bookmark()).unwrap();
        assert_eq!(env["op"], "ddl");
    }
}
