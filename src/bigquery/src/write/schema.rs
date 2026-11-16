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

//! Converts BigQuery table schemas into protobuf descriptors.
//!
//! The Storage Write API accepts rows as encoded protobuf messages described by
//! a [`ProtoSchema`][crate::model::ProtoSchema]. Table schemas are only known at
//! runtime, so the descriptor is built dynamically rather than compiled by
//! `protoc`.

use crate::model::table_field_schema::{Mode, Type};
use crate::model::{TableFieldSchema, TableSchema};
use std::collections::HashSet;
use wkt::field_descriptor_proto::{Label, Type as ProtoType};
use wkt::{DescriptorProto, FieldDescriptorProto};

/// An error encountered when a BigQuery [`TableSchema`][crate::model::TableSchema]
/// cannot be converted for the JSON writer.
///
/// Use [`Display`][std::fmt::Display] to inspect the offending column path and
/// reason.
#[derive(thiserror::Error, Debug)]
#[cfg_attr(test, derive(Clone, PartialEq))]
#[error(transparent)]
pub(crate) struct SchemaError {
    #[from]
    pub(crate) kind: SchemaErrorKind,
}

#[derive(thiserror::Error, Debug)]
#[cfg_attr(test, derive(Clone, PartialEq))]
pub(crate) enum SchemaErrorKind {
    /// The field did not declare a supported BigQuery data type.
    #[error("invalid or unspecified type '{field_type}' for field '{field}'")]
    InvalidFieldType { field: String, field_type: Type },

    /// The field mode is not one this client version recognizes.
    #[error("unrecognized mode '{mode}' for field '{field}'")]
    UnknownMode { field: String, mode: Mode },

    /// The `TIMESTAMP` field declares a precision this client does not support.
    #[error("unsupported timestamp precision {precision} for field '{field}'")]
    UnsupportedTimestampPrecision { field: String, precision: i64 },

    /// The column name is not a valid protobuf field name.
    #[error("column name '{field}' is not a valid protobuf field name")]
    UnsupportedColumnName { field: String },

    /// Two columns in the same record have the same name, ignoring ASCII case.
    #[error("column name '{field}' is used more than once, ignoring case")]
    DuplicateColumnName { field: String },

    /// A `STRUCT` column has no fields.
    #[error("STRUCT column '{field}' has no fields")]
    EmptyStruct { field: String },
}

impl SchemaError {
    /// Prepends `parent` to the error's field name (for example, `"address.zip"`).
    fn prepend_field(mut self, parent: &str) -> Self {
        let field = match &mut self.kind {
            SchemaErrorKind::InvalidFieldType { field, .. }
            | SchemaErrorKind::UnknownMode { field, .. }
            | SchemaErrorKind::UnsupportedTimestampPrecision { field, .. }
            | SchemaErrorKind::UnsupportedColumnName { field }
            | SchemaErrorKind::DuplicateColumnName { field }
            | SchemaErrorKind::EmptyStruct { field } => field,
        };
        *field = format!("{parent}.{field}");
        self
    }
}

/// Maps the BigQuery type of `field` to its protobuf wire type.
///
/// `DATE`, `DATETIME`, `TIME`, `NUMERIC`, and `BIGNUMERIC` use `TYPE_STRING` so
/// BigQuery parses and range-checks their canonical text literals on the
/// server. `TIMESTAMP` uses its documented preferred `TYPE_INT64`
/// epoch-microsecond representation.
fn field_proto_type(field: &TableFieldSchema) -> Result<ProtoType, SchemaError> {
    match field.r#type {
        Type::Bool => Ok(ProtoType::Bool),
        Type::Int64 => Ok(ProtoType::Int64),
        Type::Timestamp => {
            check_timestamp_precision(field)?;
            Ok(ProtoType::Int64)
        }
        Type::Double => Ok(ProtoType::Double),
        Type::Bytes => Ok(ProtoType::Bytes),
        Type::Struct => Ok(ProtoType::Message),
        Type::String
        | Type::Date
        | Type::Time
        | Type::Datetime
        | Type::Geography
        | Type::Numeric
        | Type::Bignumeric
        | Type::Interval
        | Type::Json => Ok(ProtoType::String),
        // TODO(#6783): map Type::Range to ProtoType::Message.
        _ => Err(SchemaErrorKind::InvalidFieldType {
            field: field.name.clone(),
            field_type: field.r#type.clone(),
        }
        .into()),
    }
}

/// Validates that a `TIMESTAMP` column uses microsecond precision.
///
/// Unset, `0`, and `6` denote microsecond precision. Picosecond precision
/// (`12`) and unrecognized precisions are rejected in v1.
fn check_timestamp_precision(field: &TableFieldSchema) -> Result<(), SchemaError> {
    match field.timestamp_precision {
        None | Some(0) | Some(6) => Ok(()),
        Some(precision) => Err(SchemaErrorKind::UnsupportedTimestampPrecision {
            field: field.name.clone(),
            precision,
        }
        .into()),
    }
}

/// Maps the BigQuery [`Mode`] of `field` to its `proto2` [`Label`].
///
/// Both `NULLABLE` (including unset) and `REQUIRED` columns map to
/// `LABEL_OPTIONAL`: BigQuery enforces `NOT NULL` across all stream types with
/// per-row `RowError`s, and leaving omitted `REQUIRED` fields unset on the wire
/// stays compatible with SQL `DEFAULT` expressions.
fn field_label(field: &TableFieldSchema) -> Result<Label, SchemaError> {
    match field.mode {
        Mode::Repeated => Ok(Label::Repeated),
        Mode::Required | Mode::Nullable | Mode::Unspecified => Ok(Label::Optional),
        _ => Err(SchemaErrorKind::UnknownMode {
            field: field.name.clone(),
            mode: field.mode.clone(),
        }
        .into()),
    }
}

/// Converts a BigQuery [`TableSchema`] into a `proto2` [`DescriptorProto`].
///
/// The root descriptor is named `"root"` and carries one field per entry in
/// `schema.fields`, numbered sequentially from 1. Each field sets `json_name`
/// to the exact column name so `lowerCamelCase` JSON name conversions cannot
/// collide across columns such as `user_id` and `userId`.
///
/// Each `STRUCT` column becomes a message field with type defined in
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
                _ => Ok(proto_field),
            }
        })
        .collect()
}

/// Collects helper `DescriptorProto` messages for `root.nested_type`.
///
/// Unlike `TableFieldSchema`, a `FieldDescriptorProto` cannot hold subfields
/// inline; it sets `type_name` to the name of a `DescriptorProto` in
/// `root.nested_type`. BigQuery ignores message names and matches columns by
/// field name, so we name these messages `m1`, `m2`, and so on, skipping any
/// name already used by a column in `root`.
struct NestedTypes {
    messages: Vec<DescriptorProto>,
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
            root_columns,
            next_id: 0,
        }
    }

    fn add_message(&mut self, fields: Vec<FieldDescriptorProto>) -> String {
        let name = loop {
            self.next_id += 1;
            let candidate = format!("m{}", self.next_id);
            if !self.root_columns.contains(&candidate) {
                break candidate;
            }
        };
        self.messages
            .push(DescriptorProto::new().set_name(&name).set_field(fields));
        name
    }
}

/// Validates that every column name in `fields` is a legal protobuf field
/// identifier and unique within the record (ignoring ASCII case).
fn check_column_names(fields: &[TableFieldSchema]) -> Result<(), SchemaError> {
    let mut seen = HashSet::with_capacity(fields.len());
    for field in fields {
        if !is_proto_identifier(&field.name) {
            return Err(SchemaErrorKind::UnsupportedColumnName {
                field: field.name.clone(),
            }
            .into());
        }
        if !seen.insert(field.name.to_ascii_lowercase()) {
            return Err(SchemaErrorKind::DuplicateColumnName {
                field: field.name.clone(),
            }
            .into());
        }
    }
    Ok(())
}

/// Returns `true` if `name` matches the protobuf identifier grammar
/// `[A-Za-z_][A-Za-z0-9_]*`.
fn is_proto_identifier(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_impl_all;
    use test_case::test_case;

    assert_impl_all!(SchemaError: std::fmt::Debug, std::error::Error, Send, Sync);

    fn field(name: &str, r#type: Type) -> TableFieldSchema {
        TableFieldSchema::new().set_name(name).set_type(r#type)
    }

    fn record<const N: usize>(name: &str, fields: [TableFieldSchema; N]) -> TableFieldSchema {
        field(name, Type::Struct).set_fields(fields)
    }

    #[test_case(Type::Bool, ProtoType::Bool)]
    #[test_case(Type::Int64, ProtoType::Int64)]
    #[test_case(Type::Double, ProtoType::Double)]
    #[test_case(Type::Bytes, ProtoType::Bytes)]
    #[test_case(Type::Struct, ProtoType::Message)]
    #[test_case(Type::String, ProtoType::String)]
    #[test_case(Type::Geography, ProtoType::String)]
    #[test_case(Type::Json, ProtoType::String)]
    #[test_case(Type::Interval, ProtoType::String)]
    #[test_case(Type::Date, ProtoType::String)]
    #[test_case(Type::Datetime, ProtoType::String)]
    #[test_case(Type::Time, ProtoType::String)]
    #[test_case(Type::Numeric, ProtoType::String)]
    #[test_case(Type::Bignumeric, ProtoType::String)]
    #[test_case(Type::Timestamp, ProtoType::Int64)]
    fn scalar_types(bq_type: Type, want: ProtoType) {
        let f = field("f", bq_type.clone());
        let got =
            field_proto_type(&f).unwrap_or_else(|e| panic!("{bq_type:?} should convert: {e}"));
        assert_eq!(got, want);
    }

    #[test]
    fn invalid_and_unknown_field_types() {
        for bad_type in [Type::Unspecified, Type::from(9999)] {
            let f = field("bad_col", bad_type.clone());
            let err = field_proto_type(&f).expect_err("invalid type should be rejected");
            assert_eq!(
                err.kind,
                SchemaErrorKind::InvalidFieldType {
                    field: "bad_col".to_string(),
                    field_type: bad_type,
                }
            );
            assert!(err.to_string().contains("bad_col"), "{err}");
        }
    }

    #[test_case(Mode::Required, Label::Optional)]
    #[test_case(Mode::Nullable, Label::Optional)]
    #[test_case(Mode::Unspecified, Label::Optional)]
    #[test_case(Mode::Repeated, Label::Repeated)]
    fn modes(mode: Mode, want: Label) {
        let f = field("f", Type::String).set_mode(mode.clone());
        let got = field_label(&f).unwrap_or_else(|e| panic!("{mode:?} should convert: {e}"));
        assert_eq!(got, want);
    }

    #[test]
    fn unknown_mode_is_rejected() {
        let unknown = Mode::from(9999);
        let f = field("f", Type::String).set_mode(unknown.clone());
        let err = field_label(&f).expect_err("unknown mode should be rejected");
        assert_eq!(
            err.kind,
            SchemaErrorKind::UnknownMode {
                field: "f".to_string(),
                mode: unknown,
            }
        );
        assert_eq!(err.to_string(), "unrecognized mode '9999' for field 'f'");
    }

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

    #[test_case(None)]
    #[test_case(Some(0))]
    #[test_case(Some(6))]
    fn microsecond_timestamp_precisions(precision: Option<i64>) {
        let f = field("created_at", Type::Timestamp).set_or_clear_timestamp_precision(precision);
        assert_eq!(
            field_proto_type(&f).expect("should convert"),
            ProtoType::Int64
        );
    }

    #[test_case(12)]
    #[test_case(9)]
    fn unsupported_timestamp_precision_is_rejected(precision: i64) {
        let f = field("created_at", Type::Timestamp).set_timestamp_precision(precision);
        let err = field_proto_type(&f).expect_err("unsupported precision should be rejected");
        assert_eq!(
            err.kind,
            SchemaErrorKind::UnsupportedTimestampPrecision {
                field: "created_at".to_string(),
                precision,
            }
        );
        assert_eq!(
            err.to_string(),
            format!("unsupported timestamp precision {precision} for field 'created_at'")
        );
    }

    #[test_case("a")]
    #[test_case("_")]
    #[test_case("_a")]
    #[test_case("_1")]
    #[test_case("a1")]
    #[test_case("CamelCase")]
    #[test_case("snake_case")]
    fn valid_column_names(name: &str) {
        let schema = TableSchema::new().set_fields([field(name, Type::String)]);
        table_schema_to_descriptor(&schema)
            .unwrap_or_else(|e| panic!("{name} should convert: {e}"));
    }

    #[test_case("")]
    #[test_case("1st")]
    #[test_case("$id")]
    #[test_case("first name")]
    #[test_case("item-id")]
    #[test_case("a.b")]
    #[test_case("価格")]
    #[test_case("café")]
    fn unsupported_column_names_are_rejected(name: &str) {
        let schema = TableSchema::new().set_fields([field(name, Type::String)]);
        let err = table_schema_to_descriptor(&schema)
            .expect_err("non-identifier column name should fail");
        assert_eq!(
            err.kind,
            SchemaErrorKind::UnsupportedColumnName {
                field: name.to_string(),
            }
        );
        assert_eq!(
            err.to_string(),
            format!("column name '{name}' is not a valid protobuf field name")
        );
    }

    #[test_case("id", "id"; "exact match")]
    #[test_case("name", "NAME"; "ascii case insensitive")]
    fn duplicate_column_names_are_rejected(first: &str, duplicate: &str) {
        let schema = TableSchema::new().set_fields([
            field(first, Type::String),
            field("other", Type::Int64),
            field(duplicate, Type::Int64),
        ]);
        let err = table_schema_to_descriptor(&schema).expect_err("duplicate column must fail");
        assert_eq!(
            err.kind,
            SchemaErrorKind::DuplicateColumnName {
                field: duplicate.to_string(),
            }
        );
        assert_eq!(
            err.to_string(),
            format!("column name '{duplicate}' is used more than once, ignoring case")
        );
    }

    #[test]
    fn nested_and_repeated_structs() {
        let geo = record(
            "geo",
            [field("lat", Type::Double), field("lng", Type::Double)],
        );
        let schema = TableSchema::new().set_fields([
            field("m1", Type::String),
            record("address", [field("city", Type::String), geo]),
            record("tags", [field("key", Type::String)]).set_mode(Mode::Repeated),
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
        let schema = TableSchema::new().set_fields([record("a", [inner])]);
        let err = table_schema_to_descriptor(&schema).expect_err("should fail");
        assert!(err.to_string().contains(want), "got: {err}");
    }
}
