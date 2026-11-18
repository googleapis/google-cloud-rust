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

//! Case-insensitive column lookup for JSON row encoding.

use crate::model::TableFieldSchema;
use crate::model::table_field_schema::Type;
use prost_reflect::{FieldDescriptor, MessageDescriptor};
use std::collections::HashMap;

/// Pairs a column's protobuf [`FieldDescriptor`] with its BigQuery SQL [`Type`].
#[derive(Debug)]
struct Column {
    proto: FieldDescriptor,
    r#type: Type,
}

/// Case-insensitive lookup map from JSON object keys to BigQuery columns.
#[derive(Debug)]
struct ColumnMap {
    by_lower_name: HashMap<String, Column>,
}

impl ColumnMap {
    /// Builds a [`ColumnMap`] from a record `descriptor` and its validated `fields`.
    fn new(descriptor: &MessageDescriptor, fields: &[TableFieldSchema]) -> Self {
        let by_lower_name = fields
            .iter()
            .map(|field| {
                let proto = descriptor
                    .get_field_by_name(&field.name)
                    .expect("descriptor built from schema contains every field");
                (
                    field.name.to_ascii_lowercase(),
                    Column {
                        proto,
                        r#type: field.r#type.clone(),
                    },
                )
            })
            .collect();
        Self { by_lower_name }
    }

    /// Looks up a column by JSON object key, ignoring ASCII case.
    fn get(&self, key: &str) -> Option<&Column> {
        if key.bytes().any(|b| b.is_ascii_uppercase()) {
            self.by_lower_name.get(&key.to_ascii_lowercase())
        } else {
            self.by_lower_name.get(key)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TableSchema;
    use crate::write::schema::{build_message_descriptor, table_schema_to_descriptor};
    use prost_reflect::Kind;
    use static_assertions::assert_impl_all;

    assert_impl_all!(ColumnMap: std::fmt::Debug, Send, Sync);

    #[test]
    fn column_map_matches_keys_ignoring_ascii_case() {
        let col =
            |name: &str, r#type: Type| TableFieldSchema::new().set_name(name).set_type(r#type);
        let schema = TableSchema::new().set_fields([
            col("user_id", Type::String),
            col("UserId", Type::Int64),
            col("CreatedAt", Type::Timestamp),
        ]);
        let proto = table_schema_to_descriptor(&schema).expect("valid schema");
        let root = build_message_descriptor(proto).expect("valid descriptor");
        let map = ColumnMap::new(&root, &schema.fields);

        for (key, want) in [
            ("user_id", (1, "user_id", Kind::String, &Type::String)),
            ("USER_ID", (1, "user_id", Kind::String, &Type::String)),
            ("userid", (2, "UserId", Kind::Int64, &Type::Int64)),
            ("UserId", (2, "UserId", Kind::Int64, &Type::Int64)),
            ("USERID", (2, "UserId", Kind::Int64, &Type::Int64)),
            ("createdat", (3, "CreatedAt", Kind::Int64, &Type::Timestamp)),
            ("createdAt", (3, "CreatedAt", Kind::Int64, &Type::Timestamp)),
            ("CREATEDAT", (3, "CreatedAt", Kind::Int64, &Type::Timestamp)),
        ] {
            let c = map.get(key).expect(key);
            assert_eq!(
                (c.proto.number(), c.proto.name(), c.proto.kind(), &c.r#type),
                want
            );
        }

        for key in ["", "missing", "CAFÉ", "user_\u{0130}d", "\u{212A}"] {
            assert!(map.get(key).is_none(), "unexpected match for {key:?}");
        }
    }

    #[test]
    fn empty_schema() {
        let proto = table_schema_to_descriptor(&TableSchema::new()).expect("empty schema");
        let root = build_message_descriptor(proto).expect("valid descriptor");
        let map = ColumnMap::new(&root, &[]);
        assert!(map.get("").is_none());
        assert!(map.get("id").is_none());
    }
}
