// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Builds self-contained `proto2` [`DescriptorProto`] trees (root and nested
//! `STRUCT`/`RANGE` messages) from a BigQuery [`TableSchema`].

use super::field::{check_column_names, field_label, field_proto_type, range_element_proto_type};
use super::{SchemaError, SchemaErrorKind};
use crate::model::table_field_schema::Type;
use crate::model::{TableFieldSchema, TableSchema};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use wkt::field_descriptor_proto::{Label, Type as ProtoType};
use wkt::{DescriptorProto, FieldDescriptorProto};

/// Converts a BigQuery [`TableSchema`] into a `proto2` [`DescriptorProto`].
///
/// The root descriptor is named `"root"` and carries one field per entry in
/// `schema.fields`, numbered sequentially from 1. Each field sets `json_name`
/// to the exact column name so `lowerCamelCase` JSON name conversions cannot
/// collide across columns such as `user_id` and `userId`.
///
/// Each `STRUCT` or `RANGE` column becomes a message field with type defined in
/// `root.nested_type`.
pub(crate) fn table_schema_to_descriptor(
    schema: &TableSchema,
) -> Result<DescriptorProto, SchemaError> {
    let mut nested = NestedTypes::new(&schema.fields);
    let fields = convert_fields(&schema.fields, &mut nested)?;
    Ok(DescriptorProto::new()
        .set_name("root")
        .set_field(fields)
        .set_nested_type(nested.messages))
}

/// Converts BigQuery fields into `proto2` field descriptors.
fn convert_fields(
    fields: &[TableFieldSchema],
    nested: &mut NestedTypes,
) -> Result<Vec<FieldDescriptorProto>, SchemaError> {
    check_column_names(fields)?;
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let proto_field = FieldDescriptorProto::new()
                .set_name(&field.name)
                .set_json_name(&field.name)
                // Protobuf field numbers are 1-based.
                .set_number(index as i32 + 1)
                .set_label(field_label(field)?)
                .set_type(field_proto_type(field)?);

            match field.r#type {
                Type::Struct => {
                    if field.fields.is_empty() {
                        return Err(SchemaErrorKind::EmptyStruct {
                            field: field.name.clone(),
                        }
                        .into());
                    }
                    let subfields = convert_fields(&field.fields, nested)
                        .map_err(|e| e.prepend_field(&field.name))?;
                    let type_name = nested.add_message(subfields);
                    Ok(proto_field.set_type_name(type_name))
                }
                Type::Range => {
                    let type_name = nested.add_message(range_fields(field)?);
                    Ok(proto_field.set_type_name(type_name))
                }
                _ => Ok(proto_field),
            }
        })
        .collect()
}

/// Builds the `start` and `end` [`FieldDescriptorProto`]s for a `RANGE` message.
///
/// BigQuery encodes a `RANGE` value as a message with two optional fields
/// (`start = 1`, `end = 2`). Leaving `start` or `end` unset marks that bound as
/// `UNBOUNDED`.
fn range_fields(field: &TableFieldSchema) -> Result<Vec<FieldDescriptorProto>, SchemaError> {
    let bound_type = range_element_proto_type(field)?;
    let bound = |name: &str, number: i32, r#type: ProtoType| {
        FieldDescriptorProto::new()
            .set_name(name)
            .set_json_name(name)
            .set_number(number)
            .set_label(Label::Optional)
            .set_type(r#type)
    };
    Ok(vec![
        bound("start", 1, bound_type.clone()),
        bound("end", 2, bound_type),
    ])
}

/// Collects helper `DescriptorProto` messages for `root.nested_type`.
///
/// Unlike `TableFieldSchema`, a `FieldDescriptorProto` cannot hold subfields
/// inline; it sets `type_name` to the name of a `DescriptorProto` in
/// `root.nested_type`. BigQuery ignores message names and matches columns by
/// field name, so we name these messages `m1`, `m2`, and so on, skipping any
/// name already used by a column in `root`. Columns that produce the same
/// protobuf fields share one nested message.
struct NestedTypes {
    messages: Vec<DescriptorProto>,
    by_fields: HashMap<MessageKey, String>,
    root_columns: HashSet<String>,
    next_id: usize,
}

impl NestedTypes {
    fn new(root_fields: &[TableFieldSchema]) -> Self {
        // BigQuery column names are case-insensitive, so reserve `M1` as well as `m1`.
        let root_columns = root_fields
            .iter()
            .map(|f| f.name.to_ascii_lowercase())
            .collect();
        Self {
            messages: Vec::new(),
            by_fields: HashMap::new(),
            root_columns,
            next_id: 0,
        }
    }

    fn add_message(&mut self, fields: Vec<FieldDescriptorProto>) -> String {
        self.by_fields
            .entry(MessageKey(fields))
            .or_insert_with_key(|key| {
                let name = loop {
                    self.next_id += 1;
                    let candidate = format!("m{}", self.next_id);
                    if !self.root_columns.contains(&candidate) {
                        break candidate;
                    }
                };
                self.messages.push(
                    DescriptorProto::new()
                        .set_name(&name)
                        .set_field(key.0.clone()),
                );
                name
            })
            .clone()
    }
}

/// Wraps a nested message's fields for `HashMap` lookup, since generated `wkt`
/// types only derive `PartialEq` (not `Eq` or `Hash`).
#[derive(PartialEq)]
struct MessageKey(Vec<FieldDescriptorProto>);

impl Eq for MessageKey {}

impl Hash for MessageKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the fields we populate. `json_name` is always equal to `name`.
        self.0.len().hash(state);
        for f in &self.0 {
            f.name.hash(state);
            f.number.hash(state);
            f.label.value().hash(state);
            f.r#type.value().hash(state);
            f.type_name.hash(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_helpers::{field, range_field, struct_field};
    use super::*;
    use crate::model::table_field_schema::Mode;
    use test_case::test_case;

    #[test]
    fn empty_schema() {
        let descriptor =
            table_schema_to_descriptor(&TableSchema::new()).expect("empty schema converts");
        assert_eq!(descriptor.name, "root");
        assert!(descriptor.field.is_empty());
    }

    #[test]
    fn descriptor_populates_numbers_labels_types_and_json_names() {
        let schema = TableSchema::new().set_fields([
            field("user_id", Type::String).set_mode(Mode::Required),
            field("userId", Type::Int64),
            field("tags", Type::String).set_mode(Mode::Repeated),
        ]);
        let descriptor = table_schema_to_descriptor(&schema).expect("should convert");

        let summary: Vec<_> = descriptor
            .field
            .iter()
            .map(|f| {
                (
                    f.name.as_str(),
                    f.json_name.as_str(),
                    f.number,
                    f.label.clone(),
                    f.r#type.clone(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            vec![
                ("user_id", "user_id", 1, Label::Optional, ProtoType::String),
                ("userId", "userId", 2, Label::Optional, ProtoType::Int64),
                ("tags", "tags", 3, Label::Repeated, ProtoType::String),
            ]
        );
    }

    #[test]
    fn error_propagates_from_later_field() {
        let schema = TableSchema::new()
            .set_fields([field("ok", Type::String), field("bad", Type::Unspecified)]);
        let err =
            table_schema_to_descriptor(&schema).expect_err("invalid field should fail conversion");
        assert_eq!(
            err.kind,
            SchemaErrorKind::InvalidFieldType {
                field: "bad".to_string(),
                field_type: Type::Unspecified,
            }
        );
    }

    #[test]
    fn nested_and_repeated_structs() {
        let geo = struct_field(
            "geo",
            [field("lat", Type::Double), field("lng", Type::Double)],
        );
        let schema = TableSchema::new().set_fields([
            field("m1", Type::String),
            struct_field("address", [field("city", Type::String), geo]),
            struct_field("tags", [field("key", Type::String)]).set_mode(Mode::Repeated),
        ]);
        let root = table_schema_to_descriptor(&schema).expect("should convert");

        // Column `m1` reserves `"m1"`, so nested messages start at `"m2"`.
        assert_eq!(root.field[1].r#type, ProtoType::Message);
        assert_eq!(root.field[1].type_name, "m3");
        assert_eq!(root.field[1].json_name, "address");
        assert_eq!(root.field[2].label, Label::Repeated);
        assert_eq!(root.field[2].type_name, "m4");

        let names: Vec<_> = root.nested_type.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, ["m2", "m3", "m4"]);
        assert_eq!(root.nested_type[1].field[1].type_name, "m2");
    }

    #[test_case(vec![], "STRUCT column 'a.b' has no fields")]
    #[test_case(vec![field("bad name", Type::String)], "column name 'a.b.bad name'")]
    #[test_case(vec![field("x", Type::Int64), field("X", Type::Int64)], "column name 'a.b.X'")]
    #[test_case(vec![field("ts", Type::Timestamp).set_timestamp_precision(12)], "for field 'a.b.ts'")]
    #[test_case(vec![field("u", Type::Unspecified)], "for field 'a.b.u'")]
    #[test_case(vec![field("m", Type::String).set_mode(Mode::from(9999))], "for field 'a.b.m'")]
    fn nested_struct_errors_include_full_path(subfields: Vec<TableFieldSchema>, want: &str) {
        let inner = field("b", Type::Struct).set_fields(subfields);
        let schema = TableSchema::new().set_fields([struct_field("a", [inner])]);
        let err = table_schema_to_descriptor(&schema).expect_err("should fail");
        assert!(err.to_string().contains(want), "got: {err}");
    }

    #[test]
    fn range_columns() {
        let schema = TableSchema::new().set_fields([
            range_field("dates", Type::Date),
            range_field("datetimes", Type::Datetime),
            range_field("timestamps", Type::Timestamp)
                .set_timestamp_precision(6)
                .set_mode(Mode::Repeated),
            struct_field("event", [range_field("window", Type::Date)]),
        ]);
        let root = table_schema_to_descriptor(&schema).expect("should convert");
        assert_eq!(root.field[2].label, Label::Repeated);

        let event_msg = root
            .nested_type
            .iter()
            .find(|m| m.name == root.field[3].type_name)
            .expect("event struct nested message");

        for (col, want_type) in [
            (&root.field[0], ProtoType::String),
            (&root.field[1], ProtoType::String),
            (&root.field[2], ProtoType::Int64),
            (&event_msg.field[0], ProtoType::String),
        ] {
            assert_eq!(col.r#type, ProtoType::Message);
            let msg = root
                .nested_type
                .iter()
                .find(|m| m.name == col.type_name)
                .expect("range nested message");
            let bounds: Vec<_> = msg
                .field
                .iter()
                .map(|f| {
                    (
                        f.name.as_str(),
                        f.json_name.as_str(),
                        f.number,
                        &f.label,
                        &f.r#type,
                    )
                })
                .collect();
            assert_eq!(
                bounds,
                [
                    ("start", "start", 1, &Label::Optional, &want_type),
                    ("end", "end", 2, &Label::Optional, &want_type),
                ]
            );
        }
    }

    #[test_case(
        field("r", Type::Range),
        SchemaErrorKind::InvalidRangeElementType {
            field: "a.r".to_string(),
            element_type: Type::Unspecified,
        },
        "invalid RANGE element type 'TYPE_UNSPECIFIED' for field 'a.r'"
    )]
    #[test_case(
        range_field("r", Type::Int64),
        SchemaErrorKind::InvalidRangeElementType {
            field: "a.r".to_string(),
            element_type: Type::Int64,
        },
        "invalid RANGE element type 'INT64' for field 'a.r'"
    )]
    #[test_case(
        range_field("r", Type::from(9999)),
        SchemaErrorKind::InvalidRangeElementType {
            field: "a.r".to_string(),
            element_type: Type::from(9999),
        },
        "invalid RANGE element type '9999' for field 'a.r'"
    )]
    #[test_case(
        range_field("r", Type::Timestamp).set_timestamp_precision(12),
        SchemaErrorKind::UnsupportedTimestampPrecision {
            field: "a.r".to_string(),
            precision: 12,
        },
        "unsupported timestamp precision 12 for field 'a.r'"
    )]
    fn invalid_range_columns_are_rejected(
        col: TableFieldSchema,
        want_kind: SchemaErrorKind,
        want_msg: &str,
    ) {
        let schema = TableSchema::new().set_fields([struct_field("a", [col])]);
        let err = table_schema_to_descriptor(&schema).expect_err("should fail");
        assert_eq!(err.kind, want_kind);
        assert_eq!(err.to_string(), want_msg);
    }

    #[test]
    fn deduplicates_identical_nested_types() {
        let addr = |name| {
            struct_field(
                name,
                [
                    field("city", Type::String),
                    range_field("valid_for", Type::Date),
                ],
            )
        };
        let schema = TableSchema::new().set_fields([
            addr("home"),
            addr("work"),
            struct_field(
                "other",
                [field("city", Type::String), field("country", Type::String)],
            ),
            struct_field(
                "repeated_city",
                [
                    field("city", Type::String).set_mode(Mode::Repeated),
                    field("country", Type::String),
                ],
            ),
            range_field("datetimes", Type::Datetime),
        ]);
        let root = table_schema_to_descriptor(&schema).expect("should convert");

        // `home.valid_for`, `work.valid_for`, and `datetimes` all share `"m1"`.
        // Because their inner ranges share `"m1"`, `home` and `work` also share `"m2"`.
        let assigned: Vec<_> = root
            .field
            .iter()
            .map(|f| (f.name.as_str(), f.type_name.as_str()))
            .collect();
        assert_eq!(
            assigned,
            [
                ("home", "m2"),
                ("work", "m2"),
                ("other", "m3"),
                ("repeated_city", "m4"),
                ("datetimes", "m1"),
            ]
        );

        let home_msg = root
            .nested_type
            .iter()
            .find(|m| m.name == "m2")
            .expect("home nested message");
        assert_eq!(home_msg.field[1].type_name, "m1");
        assert_eq!(root.nested_type.len(), 4);
    }
}
