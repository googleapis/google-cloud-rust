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

//! Hand-written encoding of rows in the [protobuf wire format], and the
//! schema that describes them.
//!
//! A protobuf message is a sequence of fields. Each field is a *tag* (the
//! field number and a wire type), followed by the value. Field names never
//! appear on the wire; the schema maps field numbers to column names.
//!
//! [protobuf wire format]: https://protobuf.dev/programming-guides/encoding/

use wkt::FieldDescriptorProto;
use wkt::field_descriptor_proto::{Label, Type};

/// The wire type for length-delimited values, such as strings.
const LENGTH_DELIMITED: u32 = 2;

/// The number of low bits in a tag that hold the wire type.
const WIRE_TYPE_BITS: u32 = 3;

/// The number of payload bits in each varint byte.
const VARINT_PAYLOAD_BITS: u32 = 7;

/// Selects the payload bits of a varint byte.
const VARINT_PAYLOAD_MASK: u64 = 0x7F;

/// Marks every varint byte except the last one.
const VARINT_CONTINUATION: u8 = 0x80;

/// Appends `value` as a [varint]: 7 bits per byte, least significant first.
///
/// [varint]: https://protobuf.dev/programming-guides/encoding/#varints
fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    while value > VARINT_PAYLOAD_MASK {
        buf.push(((value & VARINT_PAYLOAD_MASK) as u8) | VARINT_CONTINUATION);
        value >>= VARINT_PAYLOAD_BITS;
    }
    buf.push(value as u8);
}

/// Appends a tag, which packs the field number and the wire type in a varint.
fn encode_tag(field_number: u32, wire_type: u32, buf: &mut Vec<u8>) {
    encode_varint(u64::from((field_number << WIRE_TYPE_BITS) | wire_type), buf);
}

/// Appends a `string` field: the tag, the length in bytes, and the UTF-8 bytes.
pub(super) fn encode_string(field_number: u32, value: &str, buf: &mut Vec<u8>) {
    encode_tag(field_number, LENGTH_DELIMITED, buf);
    encode_varint(value.len() as u64, buf);
    buf.extend_from_slice(value.as_bytes());
}

/// Describes a `string` field, which BigQuery maps to a `STRING` column.
pub(super) fn string_field(name: &str, field_number: u32) -> FieldDescriptorProto {
    FieldDescriptorProto::new()
        .set_name(name)
        .set_number(i32::try_from(field_number).expect("field numbers are at most 2^29 - 1"))
        // The default label and type are not valid values, always set them.
        .set_label(Label::Optional)
        .set_type(Type::String)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::cloud::bigquery::storage::v1;
    use crate::model::ProtoSchema;
    use gaxi::prost::ToProto;
    use prost::Message;
    use wkt::DescriptorProto;

    /// The name of the message that describes our row.
    const ROW: &str = "Row";

    /// The name of the only column in our row.
    const NAME_COLUMN: &str = "name";

    /// The field number of the `name` column in our one-column row.
    const NAME_FIELD: u32 = 1;

    /// A sample value for the `name` column.
    const NAME: &str = "alice";

    /// What `prost` generates for `message Row { string name = 1; }`.
    #[derive(Clone, PartialEq, Message)]
    struct ProstRow {
        #[prost(string, tag = "1")]
        name: String,
    }

    fn encode_row(name: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_string(NAME_FIELD, name, &mut buf);
        buf
    }

    #[test]
    fn encode_by_hand() {
        let mut want = vec![
            0x0A, // tag: (field number 1 << 3) | wire type 2 (length-delimited)
            0x05, // length: "alice" is 5 bytes
        ];
        want.extend_from_slice(NAME.as_bytes());
        assert_eq!(encode_row(NAME), want);
    }

    #[test]
    fn matches_prost() {
        let want = ProstRow {
            name: NAME.to_string(),
        }
        .encode_to_vec();
        assert_eq!(encode_row(NAME), want);
    }

    #[test]
    fn long_string_uses_multi_byte_length() {
        const LEN: usize = 300;
        let name = "a".repeat(LEN);
        let got = encode_row(&name);
        // 300 needs 9 bits, but a varint byte holds 7, so it takes two bytes:
        //   300 = 0b10_0101100
        //   0101100 + continuation bit 1 -> 0b1010_1100 = 0xAC
        //   0000010 + continuation bit 0 -> 0b0000_0010 = 0x02
        assert_eq!(got[..3], [0x0A, 0xAC, 0x02]);
        let want = ProstRow { name }.encode_to_vec();
        assert_eq!(got, want);
    }

    #[test]
    fn empty_string_is_encoded() {
        // We always write the field, even if the value is empty.
        assert_eq!(encode_row(""), [0x0A, 0x00]);
        // `prost` skips default values. BigQuery reads a missing field as
        // NULL, so relying on `prost` here would turn "" into NULL.
        let encoded = ProstRow {
            name: String::new(),
        }
        .encode_to_vec();
        assert!(encoded.is_empty(), "{encoded:?}");
    }

    #[test]
    fn default_label_and_type_are_invalid() -> anyhow::Result<()> {
        use prost_types::field_descriptor_proto;
        // A field described without a label or a type...
        let field = FieldDescriptorProto::new()
            .set_name(NAME_COLUMN)
            .set_number(i32::try_from(NAME_FIELD)?);
        let descriptor = DescriptorProto::new().set_name(ROW).set_field([field]);
        let schema = ProtoSchema::new().set_proto_descriptor(descriptor);
        let got: v1::ProtoSchema = schema.to_proto()?;
        let descriptor = got.proto_descriptor.expect("descriptor is set");
        let field = &descriptor.field[0];
        // ...is sent with 0 for both, and 0 is neither a valid label nor type.
        assert_eq!(field.label, Some(0));
        assert_eq!(field.r#type, Some(0));
        assert!(field_descriptor_proto::Label::try_from(0).is_err());
        assert!(field_descriptor_proto::Type::try_from(0).is_err());
        Ok(())
    }
}
