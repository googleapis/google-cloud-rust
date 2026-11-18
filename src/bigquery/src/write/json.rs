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

//! Encodes JSON row strings into `ProtoRows`.

use crate::model::TableFieldSchema;
use prost_reflect::{FieldDescriptor, MessageDescriptor};

/// Resolves a JSON object key to its protobuf [`FieldDescriptor`] and BigQuery
/// [`TableFieldSchema`], matching column names ignoring ASCII case.
fn lookup_column<'a>(
    descriptor: &MessageDescriptor,
    fields: &'a [TableFieldSchema],
    key: &str,
) -> Option<(FieldDescriptor, &'a TableFieldSchema)> {
    let proto_field = if key.bytes().all(|b| !b.is_ascii_uppercase()) {
        descriptor.get_field_by_json_name(key)?
    } else {
        descriptor.get_field_by_json_name(&key.to_ascii_lowercase())?
    };
    let column = &fields[(proto_field.number() - 1) as usize];
    Some((proto_field, column))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TableSchema;
    use crate::model::table_field_schema::Type;
    use crate::write::schema::{build_message_descriptor, table_schema_to_descriptor};

    #[test]
    fn lookup_column_matches_ignoring_ascii_case() {
        let col =
            |name: &str, r#type: Type| TableFieldSchema::new().set_name(name).set_type(r#type);
        let schema = TableSchema::new().set_fields([
            col("user_id", Type::String),
            col("UserId", Type::Int64),
            col("Address", Type::Struct).set_fields([col("City", Type::String)]),
        ]);
        let proto = table_schema_to_descriptor(&schema).expect("valid schema");
        let root = build_message_descriptor(proto).expect("valid descriptor");

        for key in ["userid", "UserId", "USERID"] {
            let (pf, bq) = lookup_column(&root, &schema.fields, key).expect("UserId match");
            assert_eq!(
                (pf.number(), bq.name.as_str(), &bq.r#type),
                (2, "UserId", &Type::Int64)
            );
        }
        for key in ["user_id", "USER_ID"] {
            let (pf, bq) = lookup_column(&root, &schema.fields, key).expect("user_id match");
            assert_eq!(
                (pf.number(), bq.name.as_str(), &bq.r#type),
                (1, "user_id", &Type::String)
            );
        }

        let (addr_pf, addr_col) = lookup_column(&root, &schema.fields, "ADDRESS").unwrap();
        let addr_desc = addr_pf.kind().as_message().unwrap().clone();
        for key in ["city", "City", "CITY"] {
            let (pf, bq) = lookup_column(&addr_desc, &addr_col.fields, key).expect("City match");
            assert_eq!((pf.number(), bq.name.as_str()), (1, "City"));
        }

        assert!(lookup_column(&root, &schema.fields, "missing").is_none());
        assert!(lookup_column(&root, &schema.fields, "MISSING").is_none());
        assert!(lookup_column(&addr_desc, &addr_col.fields, "zip").is_none());
    }
}
