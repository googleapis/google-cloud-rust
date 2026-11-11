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

use crate::model::TableFieldSchema;
use crate::model::table_field_schema::Type;
use wkt::field_descriptor_proto::Type as ProtoType;

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
        Type::Int64 | Type::Timestamp => Ok(ProtoType::Int64),
        Type::Double => Ok(ProtoType::Double),
        Type::Bytes => Ok(ProtoType::Bytes),
        Type::String
        | Type::Date
        | Type::Time
        | Type::Datetime
        | Type::Geography
        | Type::Numeric
        | Type::Bignumeric
        | Type::Interval
        | Type::Json => Ok(ProtoType::String),
        // TODO(#6783): map Type::Struct and Type::Range to ProtoType::Message.
        _ => Err(SchemaErrorKind::InvalidFieldType {
            field: field.name.clone(),
            field_type: field.r#type.clone(),
        }
        .into()),
    }
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

    #[test_case(Type::Bool, ProtoType::Bool)]
    #[test_case(Type::Int64, ProtoType::Int64)]
    #[test_case(Type::Double, ProtoType::Double)]
    #[test_case(Type::Bytes, ProtoType::Bytes)]
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
}
