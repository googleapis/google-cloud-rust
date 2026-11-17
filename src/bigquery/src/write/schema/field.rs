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

//! Single-column validation and BigQuery-to-protobuf type/mode mapping rules.

use super::{SchemaError, SchemaErrorKind};
use crate::model::TableFieldSchema;
use crate::model::table_field_schema::{Mode, Type};
use std::collections::HashSet;
use wkt::field_descriptor_proto::{Label, Type as ProtoType};

/// Maps the BigQuery type of `field` to its protobuf wire type.
///
/// `DATE`, `DATETIME`, `TIME`, `NUMERIC`, and `BIGNUMERIC` use `TYPE_STRING` so
/// BigQuery parses and range-checks their canonical text literals on the
/// server. `TIMESTAMP` uses its documented preferred `TYPE_INT64`
/// epoch-microsecond representation.
pub(super) fn field_proto_type(field: &TableFieldSchema) -> Result<ProtoType, SchemaError> {
    match field.r#type {
        Type::Bool => Ok(ProtoType::Bool),
        Type::Int64 => Ok(ProtoType::Int64),
        Type::Timestamp => {
            check_timestamp_precision(field)?;
            Ok(ProtoType::Int64)
        }
        Type::Double => Ok(ProtoType::Double),
        Type::Bytes => Ok(ProtoType::Bytes),
        Type::Struct | Type::Range => Ok(ProtoType::Message),
        Type::String
        | Type::Date
        | Type::Time
        | Type::Datetime
        | Type::Geography
        | Type::Numeric
        | Type::Bignumeric
        | Type::Interval
        | Type::Json => Ok(ProtoType::String),
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
pub(super) fn field_label(field: &TableFieldSchema) -> Result<Label, SchemaError> {
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

/// Maps a `RANGE` field's element type to the [`ProtoType`] of its `start` and
/// `end` bounds.
pub(super) fn range_element_proto_type(field: &TableFieldSchema) -> Result<ProtoType, SchemaError> {
    let element_type = field
        .range_element_type
        .as_ref()
        .map_or(&Type::Unspecified, |e| &e.r#type);
    match element_type {
        Type::Date | Type::Datetime => Ok(ProtoType::String),
        Type::Timestamp => {
            check_timestamp_precision(field)?;
            Ok(ProtoType::Int64)
        }
        _ => Err(SchemaErrorKind::InvalidRangeElementType {
            field: field.name.clone(),
            element_type: element_type.clone(),
        }
        .into()),
    }
}

/// Validates that every column name in `fields` is a legal protobuf field
/// identifier and unique within the record (ignoring ASCII case).
pub(super) fn check_column_names(fields: &[TableFieldSchema]) -> Result<(), SchemaError> {
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
    use super::super::test_helpers::field;
    use super::*;
    use test_case::test_case;

    #[test_case(Type::Bool, ProtoType::Bool)]
    #[test_case(Type::Int64, ProtoType::Int64)]
    #[test_case(Type::Double, ProtoType::Double)]
    #[test_case(Type::Bytes, ProtoType::Bytes)]
    #[test_case(Type::Struct, ProtoType::Message)]
    #[test_case(Type::Range, ProtoType::Message)]
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
        check_column_names(&[field(name, Type::String)])
            .unwrap_or_else(|e| panic!("{name} should be valid: {e}"));
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
        let err = check_column_names(&[field(name, Type::String)])
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
        let err = check_column_names(&[
            field(first, Type::String),
            field("other", Type::Int64),
            field(duplicate, Type::Int64),
        ])
        .expect_err("duplicate column must fail");
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
}
