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

//! Error types returned when a BigQuery table schema cannot be converted.

use crate::model::table_field_schema::{Mode, Type};

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

    /// The `RANGE` field did not declare a supported element type (`DATE`,
    /// `DATETIME`, or `TIMESTAMP`).
    #[error("invalid RANGE element type '{element_type}' for field '{field}'")]
    InvalidRangeElementType { field: String, element_type: Type },

    /// The protobuf descriptor built from the schema is invalid.
    #[error("invalid protobuf descriptor: {detail}")]
    InvalidDescriptor { detail: String },
}

impl SchemaError {
    /// Prepends `parent` to the error's field name (for example, `"address.zip"`).
    pub(super) fn prepend_field(mut self, parent: &str) -> Self {
        match &mut self.kind {
            SchemaErrorKind::InvalidFieldType { field, .. }
            | SchemaErrorKind::UnknownMode { field, .. }
            | SchemaErrorKind::UnsupportedTimestampPrecision { field, .. }
            | SchemaErrorKind::UnsupportedColumnName { field }
            | SchemaErrorKind::DuplicateColumnName { field }
            | SchemaErrorKind::EmptyStruct { field }
            | SchemaErrorKind::InvalidRangeElementType { field, .. } => {
                *field = format!("{parent}.{field}");
            }
            // Whole-descriptor error; not tied to a single field.
            SchemaErrorKind::InvalidDescriptor { .. } => {}
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use static_assertions::assert_impl_all;

    assert_impl_all!(SchemaError: std::fmt::Debug, std::error::Error, Send, Sync);
}
