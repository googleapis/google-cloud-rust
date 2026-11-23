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

//! Encodes JSON row objects into `proto2` [`prost_reflect::DynamicMessage`]s.
//!
//! [`ColumnMap`] resolves JSON keys ignoring ASCII case, [`row::RowEncoder`]
//! populates a row's [`prost_reflect::DynamicMessage`] from `{ "key": value }`,
//! and [`value::ValueEncoder`] converts each column value according to its
//! BigQuery [`Type`].

mod row;
mod value;

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
///
/// Stores each [`Column`] once in `columns`, with `by_exact` handling keys that
/// match the table schema's exact casing and `by_lower` handling case-insensitive
/// fallback lookups.
#[derive(Debug)]
struct ColumnMap {
    columns: Vec<Column>,
    by_exact: HashMap<String, usize>,
    by_lower: HashMap<String, usize>,
}

impl ColumnMap {
    /// Builds a [`ColumnMap`] from a record `descriptor` and its validated `fields`.
    fn new(descriptor: &MessageDescriptor, fields: &[TableFieldSchema]) -> Self {
        let mut columns = Vec::with_capacity(fields.len());
        let mut by_exact = HashMap::with_capacity(fields.len());
        let mut by_lower = HashMap::with_capacity(fields.len());
        for (index, field) in fields.iter().enumerate() {
            let proto = descriptor
                .get_field_by_name(&field.name)
                .expect("descriptor built from schema contains every field");
            by_exact.insert(field.name.clone(), index);
            by_lower.insert(field.name.to_ascii_lowercase(), index);
            columns.push(Column {
                proto,
                r#type: field.r#type.clone(),
            });
        }
        Self {
            columns,
            by_exact,
            by_lower,
        }
    }

    /// Looks up a column by JSON object key, checking exact schema casing first
    /// and falling back to ASCII-lowercased matching.
    fn get(&self, key: &str) -> Option<&Column> {
        let &index = self.by_exact.get(key).or_else(|| {
            if key.bytes().any(|b| b.is_ascii_uppercase()) {
                self.by_lower.get(&key.to_ascii_lowercase())
            } else {
                self.by_lower.get(key)
            }
        })?;
        self.columns.get(index)
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

        let want_user_id = (1, "user_id", Kind::String, &Type::String);
        let want_userid = (2, "UserId", Kind::Int64, &Type::Int64);
        let want_created_at = (3, "CreatedAt", Kind::Int64, &Type::Timestamp);
        for (key, want) in [
            ("user_id", &want_user_id),
            ("USER_ID", &want_user_id),
            ("userid", &want_userid),
            ("UserId", &want_userid),
            ("USERID", &want_userid),
            ("createdat", &want_created_at),
            ("createdAt", &want_created_at),
            ("CREATEDAT", &want_created_at),
        ] {
            let c = map.get(key).expect(key);
            assert_eq!(
                &(c.proto.number(), c.proto.name(), c.proto.kind(), &c.r#type),
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
