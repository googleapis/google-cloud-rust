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

mod descriptor;
mod error;
mod field;
mod reflect;

pub(crate) use error::{SchemaError, SchemaErrorKind};

/// Shared `TableFieldSchema` builders for `write::schema` unit tests.
#[cfg(test)]
mod test_helpers {
    use crate::model::TableFieldSchema;
    use crate::model::table_field_schema::{FieldElementType, Type};

    pub(super) fn field(name: &str, r#type: Type) -> TableFieldSchema {
        TableFieldSchema::new().set_name(name).set_type(r#type)
    }

    pub(super) fn struct_field<const N: usize>(
        name: &str,
        fields: [TableFieldSchema; N],
    ) -> TableFieldSchema {
        field(name, Type::Struct).set_fields(fields)
    }

    pub(super) fn range_field(name: &str, element: Type) -> TableFieldSchema {
        field(name, Type::Range).set_range_element_type(FieldElementType::new().set_type(element))
    }
}
