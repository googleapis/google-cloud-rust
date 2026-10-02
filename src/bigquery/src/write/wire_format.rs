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

/// The wire type for varints, such as `int64` values.
const VARINT: u32 = 0;

/// The wire type for 8-byte values, such as `double` values.
const FIXED64: u32 = 1;

/// The wire type for length-delimited values, such as strings.
const LENGTH_DELIMITED: u32 = 2;

/// The wire type for 4-byte values, such as `float` values.
const FIXED32: u32 = 5;

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

/// Appends a `bytes` field: the tag, the length, and the bytes.
pub(super) fn encode_bytes(field_number: u32, value: &[u8], buf: &mut Vec<u8>) {
    encode_tag(field_number, LENGTH_DELIMITED, buf);
    encode_varint(value.len() as u64, buf);
    buf.extend_from_slice(value);
}

/// Appends a `string` field: the tag, the length in bytes, and the UTF-8 bytes.
///
/// On the wire, a `string` is a `bytes` value that holds UTF-8.
pub(super) fn encode_string(field_number: u32, value: &str, buf: &mut Vec<u8>) {
    encode_bytes(field_number, value.as_bytes(), buf);
}

/// Appends an `int64` field: the tag, and the value as a varint.
///
/// Negative values are sign-extended to 64 bits (two's complement), so they
/// always take 10 bytes. This is how protobuf encodes `int64`.
pub(super) fn encode_int64(field_number: u32, value: i64, buf: &mut Vec<u8>) {
    encode_tag(field_number, VARINT, buf);
    encode_varint(value as u64, buf);
}

/// Appends a `bool` field: the tag, and 1 or 0 as a varint.
pub(super) fn encode_bool(field_number: u32, value: bool, buf: &mut Vec<u8>) {
    encode_tag(field_number, VARINT, buf);
    encode_varint(u64::from(value), buf);
}

/// Appends a `double` field: the tag, and the 8 bytes of the value, least
/// significant byte first.
pub(super) fn encode_double(field_number: u32, value: f64, buf: &mut Vec<u8>) {
    encode_tag(field_number, FIXED64, buf);
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Appends a `float` field: the tag, and the 4 bytes of the value, least
/// significant byte first.
pub(super) fn encode_float(field_number: u32, value: f32, buf: &mut Vec<u8>) {
    encode_tag(field_number, FIXED32, buf);
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Describes a `string` field, which BigQuery maps to a `STRING` column.
pub(super) fn string_field(name: &str, field_number: u32) -> FieldDescriptorProto {
    optional_field(name, field_number, Type::String)
}

/// Describes an `int64` field, which BigQuery maps to an `INT64` column.
pub(super) fn int64_field(name: &str, field_number: u32) -> FieldDescriptorProto {
    optional_field(name, field_number, Type::Int64)
}

/// Describes a `bool` field, which BigQuery maps to a `BOOL` column.
pub(super) fn bool_field(name: &str, field_number: u32) -> FieldDescriptorProto {
    optional_field(name, field_number, Type::Bool)
}

/// Describes a `double` field, which BigQuery maps to a `FLOAT64` column.
pub(super) fn double_field(name: &str, field_number: u32) -> FieldDescriptorProto {
    optional_field(name, field_number, Type::Double)
}

/// Describes a `float` field, which BigQuery maps to a `FLOAT64` column.
pub(super) fn float_field(name: &str, field_number: u32) -> FieldDescriptorProto {
    optional_field(name, field_number, Type::Float)
}

/// Describes a `bytes` field, which BigQuery maps to a `BYTES` column.
pub(super) fn bytes_field(name: &str, field_number: u32) -> FieldDescriptorProto {
    optional_field(name, field_number, Type::Bytes)
}

/// Makes `field` a repeated field, which BigQuery maps to an `ARRAY` column.
pub(super) fn repeated_field(field: FieldDescriptorProto) -> FieldDescriptorProto {
    field.set_label(Label::Repeated)
}

/// Describes an optional field with the given type.
fn optional_field(name: &str, field_number: u32, field_type: Type) -> FieldDescriptorProto {
    FieldDescriptorProto::new()
        .set_name(name)
        .set_number(i32::try_from(field_number).expect("field numbers are at most 2^29 - 1"))
        // The default label and type are not valid values, always set them.
        .set_label(Label::Optional)
        .set_type(field_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::cloud::bigquery::storage::v1;
    use crate::model::ProtoSchema;
    use gaxi::prost::ToProto;
    use prost::Message;
    use test_case::test_case;
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

    /// The field number of the `count` column, the second in our row.
    const COUNT_FIELD: u32 = 2;

    /// The number in the varint example of the protobuf encoding guide.
    const GUIDE_EXAMPLE: i64 = 150;

    /// What `prost` generates for `message Row { int64 count = 2; }`.
    #[derive(Clone, PartialEq, Message)]
    struct ProstCount {
        #[prost(int64, tag = "2")]
        count: i64,
    }

    fn encode_count(count: i64) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_int64(COUNT_FIELD, count, &mut buf);
        buf
    }

    #[test]
    fn encode_int64_by_hand() {
        // 150 needs 8 bits, so it takes two varint bytes:
        //   150 = 0b1_0010110
        //   0010110 + continuation bit 1 -> 0b1001_0110 = 0x96
        //   0000001 + continuation bit 0 -> 0b0000_0001 = 0x01
        let want = [
            0x10, // tag: (field number 2 << 3) | wire type 0 (varint)
            0x96, 0x01,
        ];
        assert_eq!(encode_count(GUIDE_EXAMPLE), want);
    }

    #[test_case(GUIDE_EXAMPLE; "guide example")]
    #[test_case(-1; "minus one")]
    #[test_case(i64::MAX; "max")]
    #[test_case(i64::MIN; "min")]
    fn int64_matches_prost(count: i64) {
        let want = ProstCount { count }.encode_to_vec();
        assert_eq!(encode_count(count), want);
    }

    #[test]
    fn negative_int64_takes_ten_bytes() {
        // -1 is 64 one bits in two's complement. At 7 bits per byte, that is
        // nine full bytes, and one more byte for the last bit.
        let mut want = vec![0x10]; // tag
        want.extend([0xFF; 9]); // 7 one bits + continuation bit 1
        want.push(0x01); // the last one bit + continuation bit 0
        assert_eq!(encode_count(-1), want);
    }

    #[test]
    fn zero_int64_is_encoded() {
        // We always write the field, even if the value is zero.
        assert_eq!(encode_count(0), [0x10, 0x00]);
        // `prost` skips default values, which BigQuery would read as NULL.
        let encoded = ProstCount { count: 0 }.encode_to_vec();
        assert!(encoded.is_empty(), "{encoded:?}");
    }

    /// The field number of the `flag` column in our `Scalars` message.
    const FLAG_FIELD: u32 = 1;

    /// The field number of the `ratio` column in our `Scalars` message.
    const RATIO_FIELD: u32 = 2;

    /// The field number of the `score` column in our `Scalars` message.
    const SCORE_FIELD: u32 = 3;

    /// The field number of the `payload` column in our `Scalars` message.
    const PAYLOAD_FIELD: u32 = 4;

    /// Sample bytes. They are not valid UTF-8, which `bytes` fields allow.
    const PAYLOAD: &[u8] = &[0x00, 0xFF];

    /// What `prost` generates for:
    ///
    /// `message Scalars { bool flag = 1; double ratio = 2; float score = 3;
    /// bytes payload = 4; }`
    #[derive(Clone, PartialEq, Message)]
    struct ProstScalars {
        #[prost(bool, tag = "1")]
        flag: bool,
        #[prost(double, tag = "2")]
        ratio: f64,
        #[prost(float, tag = "3")]
        score: f32,
        #[prost(bytes = "vec", tag = "4")]
        payload: Vec<u8>,
    }

    #[test_case(true, 0x01; "true")]
    #[test_case(false, 0x00; "false")]
    fn encode_bool_by_hand(flag: bool, value: u8) {
        let mut buf = Vec::new();
        encode_bool(FLAG_FIELD, flag, &mut buf);
        let want = [
            0x08, // tag: (field number 1 << 3) | wire type 0 (varint)
            value,
        ];
        assert_eq!(buf, want);
    }

    #[test]
    fn bool_matches_prost() {
        let mut buf = Vec::new();
        encode_bool(FLAG_FIELD, true, &mut buf);
        // `prost` skips `false`, so there is nothing to compare for it.
        let want = ProstScalars {
            flag: true,
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(buf, want);
    }

    // A `double` is 64 bits in IEEE 754: a sign bit, an 11-bit exponent, and a
    // 52-bit fraction. 1.0 has exponent 0x3FF (2^0), so its bits are
    // 0x3FF0_0000_0000_0000. -2.0 has the sign bit and exponent 0x400 (2^1),
    // so its bits are 0xC000_0000_0000_0000.
    #[test_case(1.0, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F]; "one")]
    #[test_case(-2.0, [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0]; "minus two")]
    fn encode_double_by_hand(ratio: f64, value: [u8; 8]) {
        let mut buf = Vec::new();
        encode_double(RATIO_FIELD, ratio, &mut buf);
        assert_eq!(buf[0], 0x11); // tag: (field number 2 << 3) | wire type 1 (64-bit)
        assert_eq!(buf[1..], value); // least significant byte first
    }

    // A `float` is 32 bits in IEEE 754: a sign bit, an 8-bit exponent, and a
    // 23-bit fraction. 1.0 has exponent 0x7F (2^0), so its bits are
    // 0x3F80_0000. -2.0 has the sign bit and exponent 0x80 (2^1), so its bits
    // are 0xC000_0000.
    #[test_case(1.0, [0x00, 0x00, 0x80, 0x3F]; "one")]
    #[test_case(-2.0, [0x00, 0x00, 0x00, 0xC0]; "minus two")]
    fn encode_float_by_hand(score: f32, value: [u8; 4]) {
        let mut buf = Vec::new();
        encode_float(SCORE_FIELD, score, &mut buf);
        assert_eq!(buf[0], 0x1D); // tag: (field number 3 << 3) | wire type 5 (32-bit)
        assert_eq!(buf[1..], value); // least significant byte first
    }

    #[test_case(1.5; "fraction")]
    #[test_case(-2.5; "negative")]
    #[test_case(f64::MAX; "max")]
    #[test_case(f64::MIN_POSITIVE; "smallest positive")]
    #[test_case(f64::INFINITY; "infinity")]
    #[test_case(f64::NAN; "not a number")]
    fn double_matches_prost(ratio: f64) {
        let mut buf = Vec::new();
        encode_double(RATIO_FIELD, ratio, &mut buf);
        let want = ProstScalars {
            ratio,
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(buf, want);
    }

    #[test_case(1.5; "fraction")]
    #[test_case(-2.5; "negative")]
    #[test_case(f32::MAX; "max")]
    #[test_case(f32::INFINITY; "infinity")]
    #[test_case(f32::NAN; "not a number")]
    fn float_matches_prost(score: f32) {
        let mut buf = Vec::new();
        encode_float(SCORE_FIELD, score, &mut buf);
        let want = ProstScalars {
            score,
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(buf, want);
    }

    #[test]
    fn encode_bytes_by_hand() {
        let mut buf = Vec::new();
        encode_bytes(PAYLOAD_FIELD, PAYLOAD, &mut buf);
        let mut want = vec![
            0x22, // tag: (field number 4 << 3) | wire type 2 (length-delimited)
            0x02, // length: 2 bytes
        ];
        want.extend_from_slice(PAYLOAD);
        assert_eq!(buf, want);
    }

    #[test]
    fn bytes_match_prost() {
        let mut buf = Vec::new();
        encode_bytes(PAYLOAD_FIELD, PAYLOAD, &mut buf);
        let want = ProstScalars {
            payload: PAYLOAD.to_vec(),
            ..Default::default()
        }
        .encode_to_vec();
        assert_eq!(buf, want);
    }
}
