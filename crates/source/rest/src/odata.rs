//! OData `$metadata` (EDMX / CSDL) parsing → dataset discovery (#512).
//!
//! Pure, network-free: [`parse_edmx`] turns a `$metadata` XML document into the
//! entity types + sets it declares, and [`descriptors_from_edmx`] maps those
//! onto [`DatasetDescriptor`]s (one per entity set, each with a typed schema and
//! a `config_patch` that selects the entity).

use faucet_core::FaucetError;
use faucet_core::discover::{DatasetDescriptor, columns_to_schema, nullable_type};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use serde_json::{Value, json};
use std::collections::HashMap;

/// One EDM property (column) of an entity type.
#[derive(Debug, Clone, PartialEq)]
pub struct EdmProperty {
    /// Property name.
    pub name: String,
    /// EDM type (e.g. `Edm.String`, `Edm.Int32`).
    pub edm_type: String,
    /// Whether the property may be null (EDM default is `true`).
    pub nullable: bool,
}

/// An EDM entity type: its key columns + properties.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EdmEntityType {
    /// Type name (local, namespace-stripped).
    pub name: String,
    /// Key property names.
    pub keys: Vec<String>,
    /// Properties in declaration order.
    pub properties: Vec<EdmProperty>,
}

/// An EDM entity set: the queryable collection name + the type it holds.
#[derive(Debug, Clone, PartialEq)]
pub struct EdmEntitySet {
    /// Entity-set name (the path segment you query).
    pub name: String,
    /// Local name of the entity type backing this set.
    pub type_name: String,
}

/// Map an EDM primitive type to a JSON-Schema type fragment.
pub fn edm_type_to_json(edm_type: &str) -> Value {
    let t = edm_type.strip_prefix("Edm.").unwrap_or(edm_type);
    let ty = match t {
        "Boolean" => "boolean",
        "Byte" | "SByte" | "Int16" | "Int32" | "Int64" => "integer",
        "Decimal" | "Double" | "Single" => "number",
        // String, Guid, DateTimeOffset, Date, TimeOfDay, Duration, Binary,
        // Stream, Geography*, … all serialize as JSON strings.
        _ => "string",
    };
    json!({ "type": ty })
}

/// Namespace-strip a possibly-prefixed XML name (`edm:Property` → `Property`).
fn local_name(qname: &[u8]) -> String {
    let s = String::from_utf8_lossy(qname);
    s.rsplit(':').next().unwrap_or(&s).to_string()
}

/// Read one attribute of an element by (namespace-stripped) name.
fn attr(e: &BytesStart, key: &str) -> Option<String> {
    e.attributes()
        .flatten()
        .find(|a| local_name(a.key.as_ref()) == key)
        .and_then(|a| a.unescape_value().ok().map(|v| v.to_string()))
}

/// Parse an EDMX / CSDL `$metadata` document into its entity types + sets.
pub fn parse_edmx(xml: &str) -> Result<(Vec<EdmEntityType>, Vec<EdmEntitySet>), FaucetError> {
    let mut reader = Reader::from_str(xml);
    let mut types: Vec<EdmEntityType> = Vec::new();
    let mut sets: Vec<EdmEntitySet> = Vec::new();
    let mut current: Option<EdmEntityType> = None;
    let mut in_key = false;

    // Handle a start/empty element's attributes; `is_empty` self-closing tags
    // (`<Property .../>`, `<EntitySet .../>`) never get a matching `End`.
    let open = |e: &BytesStart,
                is_empty: bool,
                current: &mut Option<EdmEntityType>,
                in_key: &mut bool,
                types: &mut Vec<EdmEntityType>,
                sets: &mut Vec<EdmEntitySet>| {
        match local_name(e.name().as_ref()).as_str() {
            "EntityType" => {
                let t = EdmEntityType {
                    name: attr(e, "Name").unwrap_or_default(),
                    ..Default::default()
                };
                if is_empty {
                    types.push(t);
                } else {
                    *current = Some(t);
                }
            }
            "Key" => {
                if !is_empty {
                    *in_key = true;
                }
            }
            "PropertyRef" => {
                if *in_key && let (Some(cur), Some(n)) = (current.as_mut(), attr(e, "Name")) {
                    cur.keys.push(n);
                }
            }
            "Property" => {
                if let Some(cur) = current.as_mut() {
                    let name = attr(e, "Name").unwrap_or_default();
                    if !name.is_empty() {
                        cur.properties.push(EdmProperty {
                            name,
                            edm_type: attr(e, "Type").unwrap_or_else(|| "Edm.String".to_owned()),
                            // EDM `Nullable` defaults to true when absent.
                            nullable: attr(e, "Nullable").map(|v| v != "false").unwrap_or(true),
                        });
                    }
                }
            }
            "EntitySet" => {
                if let (Some(name), Some(ty)) = (attr(e, "Name"), attr(e, "EntityType")) {
                    let type_name = ty.rsplit('.').next().unwrap_or(&ty).to_owned();
                    sets.push(EdmEntitySet { name, type_name });
                }
            }
            _ => {}
        }
    };

    loop {
        match reader
            .read_event()
            .map_err(|e| FaucetError::Source(format!("odata: invalid $metadata XML: {e}")))?
        {
            Event::Eof => break,
            Event::Start(e) => open(&e, false, &mut current, &mut in_key, &mut types, &mut sets),
            Event::Empty(e) => open(&e, true, &mut current, &mut in_key, &mut types, &mut sets),
            Event::End(e) => match local_name(e.name().as_ref()).as_str() {
                "EntityType" => {
                    if let Some(t) = current.take() {
                        types.push(t);
                    }
                }
                "Key" => in_key = false,
                _ => {}
            },
            _ => {}
        }
    }
    Ok((types, sets))
}

/// Parse `$metadata` and produce one [`DatasetDescriptor`] per entity set,
/// each carrying a typed schema and a `config_patch` selecting the entity.
pub fn descriptors_from_edmx(xml: &str) -> Result<Vec<DatasetDescriptor>, FaucetError> {
    let (types, sets) = parse_edmx(xml)?;
    let by_name: HashMap<&str, &EdmEntityType> =
        types.iter().map(|t| (t.name.as_str(), t)).collect();
    let mut out = Vec::with_capacity(sets.len());
    for set in &sets {
        let schema = by_name.get(set.type_name.as_str()).map(|t| {
            let cols = t.properties.iter().map(|p| {
                let frag = edm_type_to_json(&p.edm_type);
                let frag = if p.nullable {
                    nullable_type(frag)
                } else {
                    frag
                };
                (p.name.clone(), frag)
            });
            columns_to_schema(cols)
        });
        let mut d = DatasetDescriptor::new(
            set.name.clone(),
            "entity",
            json!({ "odata": { "entity": set.name.clone() } }),
        );
        if let Some(s) = schema {
            d = d.with_schema(s);
        }
        out.push(d);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Sales" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Order">
        <Key><PropertyRef Name="DocEntry"/></Key>
        <Property Name="DocEntry" Type="Edm.Int32" Nullable="false"/>
        <Property Name="DocDate" Type="Edm.DateTimeOffset"/>
        <Property Name="Total" Type="Edm.Decimal" Nullable="false"/>
        <Property Name="Posted" Type="Edm.Boolean"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="Orders" EntityType="Sales.Order"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"#;

    #[test]
    fn edm_types_map_to_json_types() {
        assert_eq!(edm_type_to_json("Edm.Boolean"), json!({"type": "boolean"}));
        assert_eq!(edm_type_to_json("Edm.Int64"), json!({"type": "integer"}));
        assert_eq!(edm_type_to_json("Edm.Decimal"), json!({"type": "number"}));
        assert_eq!(edm_type_to_json("Edm.String"), json!({"type": "string"}));
        assert_eq!(
            edm_type_to_json("Edm.DateTimeOffset"),
            json!({"type": "string"})
        );
        assert_eq!(
            edm_type_to_json("Something.Custom"),
            json!({"type": "string"})
        );
    }

    #[test]
    fn parse_edmx_extracts_types_and_sets() {
        let (types, sets) = parse_edmx(SAMPLE).unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0].name, "Order");
        assert_eq!(types[0].keys, vec!["DocEntry".to_string()]);
        assert_eq!(types[0].properties.len(), 4);
        assert_eq!(types[0].properties[0].name, "DocEntry");
        assert!(!types[0].properties[0].nullable);
        assert!(types[0].properties[1].nullable); // DocDate has no Nullable attr
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0].name, "Orders");
        assert_eq!(sets[0].type_name, "Order");
    }

    #[test]
    fn descriptors_carry_schema_and_config_patch() {
        let ds = descriptors_from_edmx(SAMPLE).unwrap();
        assert_eq!(ds.len(), 1);
        let d = &ds[0];
        assert_eq!(d.name, "Orders");
        assert_eq!(d.kind, "entity");
        assert_eq!(d.config_patch, json!({ "odata": { "entity": "Orders" } }));
        let schema = d.schema.as_ref().unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["DocEntry"]["type"], "integer");
        assert_eq!(schema["properties"]["Total"]["type"], "number");
        assert_eq!(schema["properties"]["Posted"]["type"][0], "boolean");
        assert_eq!(schema["properties"]["Posted"]["type"][1], "null");
    }

    #[test]
    fn empty_entity_type_and_missing_type_are_tolerated() {
        let xml = r#"<Schema>
            <EntityType Name="Empty"/>
            <EntitySet Name="Ghosts" EntityType="ns.NotDeclared"/>
        </Schema>"#;
        let ds = descriptors_from_edmx(xml).unwrap();
        assert_eq!(ds.len(), 1);
        // No matching type → no schema, but the set is still discoverable.
        assert!(ds[0].schema.is_none());
        assert_eq!(ds[0].name, "Ghosts");
    }

    #[test]
    fn invalid_xml_errors() {
        assert!(parse_edmx("<Schema><EntityType Name=").is_err());
    }

    #[test]
    fn property_without_name_is_skipped() {
        // A `<Property>` with no `Name` is skipped (not added as an empty-named
        // column); an explicit `Nullable="true"` is honoured.
        let xml = r#"<Schema>
          <EntityType Name="T">
            <Property Type="Edm.String"/>
            <Property Name="ok" Type="Edm.String" Nullable="true"/>
          </EntityType>
        </Schema>"#;
        let (types, _sets) = parse_edmx(xml).unwrap();
        assert_eq!(types[0].properties.len(), 1);
        assert_eq!(types[0].properties[0].name, "ok");
        assert!(types[0].properties[0].nullable);
    }
}
