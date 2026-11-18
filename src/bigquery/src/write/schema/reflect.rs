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

//! Loads a `wkt::DescriptorProto` into a `proto2` [`prost_reflect::MessageDescriptor`].

use super::{SchemaError, SchemaErrorKind};
use gaxi::prost::ToProto;
use prost_reflect::{DescriptorPool, MessageDescriptor};
use prost_types::FileDescriptorProto;
use wkt::DescriptorProto;

/// Builds a `proto2` [`MessageDescriptor`] for the `"root"` message in `descriptor`.
///
/// BigQuery treats omitted fields as SQL `NULL`, so `proto2` explicit presence
/// is required to keep zero values (`0`, `false`, `""`) on the wire.
pub(crate) fn build_message_descriptor(
    descriptor: DescriptorProto,
) -> Result<MessageDescriptor, SchemaError> {
    let prost_descriptor =
        descriptor
            .to_proto()
            .map_err(|e| SchemaErrorKind::InvalidDescriptor {
                detail: e.to_string(),
            })?;
    // In-memory wrapper for `DescriptorPool`; never sent to BigQuery.
    let file = FileDescriptorProto {
        name: Some("root.proto".into()),
        syntax: Some("proto2".into()),
        message_type: vec![prost_descriptor],
        ..Default::default()
    };
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_proto(file)
        .map_err(|e| SchemaErrorKind::InvalidDescriptor {
            detail: e.to_string(),
        })?;
    pool.get_message_by_name("root").ok_or_else(|| {
        SchemaErrorKind::InvalidDescriptor {
            detail: "descriptor pool missing 'root' message".to_string(),
        }
        .into()
    })
}

#[cfg(test)]
mod tests {
    use super::super::descriptor::table_schema_to_descriptor;
    use super::super::test_helpers::{field, range_field, struct_field};
    use super::*;
    use crate::model::TableSchema;
    use crate::model::table_field_schema::{Mode, Type};
    use prost::Message;
    use prost_reflect::{DynamicMessage, Syntax, Value};
    use test_case::test_case;
    use wkt::FieldDescriptorProto;
    use wkt::field_descriptor_proto::{Label, Type as ProtoType};

    #[test]
    fn build_message_descriptor_resolves_presence_and_nested_types() {
        let nested_message = |parent: &MessageDescriptor, field_name: &str| {
            parent
                .get_field_by_name(field_name)
                .and_then(|f| f.kind().as_message().cloned())
                .expect(field_name)
        };

        let schema = TableSchema::new().set_fields([
            field("b", Type::Bool).set_mode(Mode::Required),
            field("user_id", Type::String),
            field("userId", Type::String),
            field("id", Type::Int64),
            field("id_", Type::Int64),
            field("m1", Type::String),
            struct_field(
                "addr",
                [
                    field("city", Type::String),
                    range_field("valid_for", Type::Date),
                ],
            ),
            range_field("prev_valid_for", Type::Date),
            range_field("ts_window", Type::Timestamp).set_mode(Mode::Repeated),
        ]);
        let proto = table_schema_to_descriptor(&schema).expect("valid schema");
        let root = build_message_descriptor(proto).expect("valid descriptor");

        assert_eq!(root.name(), "root");
        assert_eq!(root.parent_file().syntax(), Syntax::Proto2);
        assert_eq!(root.fields().count(), 9);
        for f in root.fields() {
            assert_eq!(f.json_name(), f.name());
            assert_eq!(f.supports_presence(), !f.is_list(), "{}", f.name());
        }

        let addr = nested_message(&root, "addr");
        assert!(addr.get_field_by_name("city").unwrap().supports_presence());

        let addr_range = nested_message(&addr, "valid_for");
        assert_eq!(addr_range, nested_message(&root, "prev_valid_for"));
        assert_ne!(addr_range, nested_message(&root, "ts_window"));
        assert!(
            addr_range
                .get_field_by_name("start")
                .unwrap()
                .supports_presence()
        );
        assert!(
            addr_range
                .get_field_by_name("end")
                .unwrap()
                .supports_presence()
        );
    }

    #[test]
    fn proto2_preserves_zero_values_and_omits_unset() {
        let schema = TableSchema::new().set_fields([
            field("b", Type::Bool).set_mode(Mode::Required),
            field("i", Type::Int64),
            field("s", Type::String),
            field("opt", Type::Int64),
        ]);
        let proto = table_schema_to_descriptor(&schema).expect("valid schema");
        let root = build_message_descriptor(proto).expect("valid descriptor");

        let mut msg = DynamicMessage::new(root.clone());
        assert!(msg.encode_to_vec().is_empty());

        msg.set_field_by_name("b", Value::Bool(false));
        msg.set_field_by_name("i", Value::I64(0));
        msg.set_field_by_name("s", Value::String(String::new()));
        let wire = msg.encode_to_vec();
        assert!(!wire.is_empty());

        let mut decoded = DynamicMessage::decode(root, wire.as_slice()).unwrap();
        assert_eq!(decoded.take_field_by_name("b"), Some(Value::Bool(false)));
        assert_eq!(decoded.take_field_by_name("i"), Some(Value::I64(0)));
        assert_eq!(
            decoded.take_field_by_name("s"),
            Some(Value::String(String::new()))
        );
        assert_eq!(decoded.take_field_by_name("opt"), None);
    }

    #[test_case(DescriptorProto::new().set_name("other"); "missing_root_message")]
    #[test_case(
        DescriptorProto::new().set_name("root").set_field([FieldDescriptorProto::new()
            .set_name("x")
            .set_number(1)
            .set_type(ProtoType::from("UNKNOWN"))]);
        "unknown_field_type_enum"
    )]
    #[test_case(
        DescriptorProto::new().set_name("root").set_field([FieldDescriptorProto::new()
            .set_name("x")
            .set_number(1)
            .set_label(Label::Optional)
            .set_type(ProtoType::Message)
            .set_type_name("missing")]);
        "unresolved_nested_type"
    )]
    #[test_case(
        DescriptorProto::new().set_name("root").set_field([FieldDescriptorProto::new()
            .set_name("x")
            .set_number(19_000)
            .set_label(Label::Optional)
            .set_type(ProtoType::Int64)]);
        "reserved_field_number"
    )]
    fn build_message_descriptor_rejects_invalid_descriptors(bad: DescriptorProto) {
        let err = build_message_descriptor(bad).expect_err("invalid descriptor should fail");
        assert!(matches!(
            err.kind,
            SchemaErrorKind::InvalidDescriptor { .. }
        ));
        assert!(err.to_string().starts_with("invalid protobuf descriptor: "));
        assert_eq!(err.clone().prepend_field("parent"), err);
    }
}
