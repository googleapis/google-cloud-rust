// Copyright 2025 Google LLC
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

//! Helper functions to convert from the well-known types to and from their
//! Prost versions.

use std::collections::BTreeMap;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ConvertError {
    #[error("enum {0} does not contain an integer value")]
    EnumNoIntegerValue(&'static str),
    #[error("Conversion unimplemented")]
    Unimplemented,
    #[error("Unexpected type URL: {0}")]
    UnexpectedTypeUrl(String),
    #[error("gax/prost conversion error: {0}")]
    Other(#[source] BoxError),
}

impl ConvertError {
    pub fn other<T>(e: T) -> Self
    where
        T: Into<BoxError>,
    {
        ConvertError::Other(e.into())
    }
}

type Result<T> = std::result::Result<T, ConvertError>;

/// Converts from `Self` into `T`, where `T` is expected to be a Protobuf-generated type.
pub trait ToProto<T>: Sized {
    type Output;
    fn to_proto(self) -> Result<Self::Output>;
}

/// Converts from `Self` into `T`, where `Self` is expected to be a Protobuf-generated type.
pub trait FromProto<T>: Sized {
    // By convention `from_*` functions do not consume a `self`. And we need
    // `self` so we can write generic code for repeated fields, maps, etc.
    fn cnv(self) -> Result<T>;
}

/// A helper for map conversions.
pub fn pair_transpose<K, V>(a: Result<K>, b: Result<V>) -> Result<(K, V)> {
    match (a, b) {
        (Ok(a), Ok(b)) => Ok((a, b)),
        (Err(e), _) => Err(e),
        (_, Err(e)) => Err(e),
    }
}

macro_rules! impl_primitive {
    ($t: ty) => {
        impl ToProto<$t> for $t {
            type Output = $t;
            fn to_proto(self) -> Result<$t> {
                Ok(self)
            }
        }

        impl FromProto<$t> for $t {
            fn cnv(self) -> Result<$t> {
                Ok(self)
            }
        }
    };
}

impl_primitive!(());
impl_primitive!(bool);
impl_primitive!(f32);
impl_primitive!(i32);
impl_primitive!(u32);
impl_primitive!(f64);
impl_primitive!(i64);
impl_primitive!(u64);
impl_primitive!(String);
impl_primitive!(bytes::Bytes);

impl FromProto<wkt::Duration> for prost_types::Duration {
    fn cnv(self) -> Result<wkt::Duration> {
        Ok(wkt::Duration::clamp(self.seconds, self.nanos))
    }
}

impl ToProto<prost_types::Duration> for wkt::Duration {
    type Output = prost_types::Duration;
    fn to_proto(self) -> Result<prost_types::Duration> {
        Ok(prost_types::Duration {
            seconds: self.seconds(),
            nanos: self.nanos(),
        })
    }
}

impl FromProto<wkt::FieldMask> for prost_types::FieldMask {
    fn cnv(self) -> Result<wkt::FieldMask> {
        Ok(wkt::FieldMask::default().set_paths(self.paths))
    }
}

impl ToProto<prost_types::FieldMask> for wkt::FieldMask {
    type Output = prost_types::FieldMask;
    fn to_proto(self) -> Result<prost_types::FieldMask> {
        Ok(prost_types::FieldMask { paths: self.paths })
    }
}

impl FromProto<wkt::Timestamp> for prost_types::Timestamp {
    fn cnv(self) -> Result<wkt::Timestamp> {
        Ok(wkt::Timestamp::clamp(self.seconds, self.nanos))
    }
}

impl ToProto<prost_types::Timestamp> for wkt::Timestamp {
    type Output = prost_types::Timestamp;
    fn to_proto(self) -> Result<prost_types::Timestamp> {
        Ok(prost_types::Timestamp {
            seconds: self.seconds(),
            nanos: self.nanos(),
        })
    }
}

impl FromProto<wkt::Struct> for prost_types::Struct {
    fn cnv(self) -> Result<wkt::Struct> {
        self.fields
            .into_iter()
            .map(|(k, v)| pair_transpose(k.cnv(), v.cnv()))
            .collect::<Result<serde_json::Map<_, _>>>()
    }
}

impl ToProto<prost_types::Struct> for wkt::Struct {
    type Output = prost_types::Struct;
    fn to_proto(self) -> Result<prost_types::Struct> {
        Ok(prost_types::Struct {
            fields: self
                .into_iter()
                .map(|(k, v)| pair_transpose(k.to_proto(), v.to_proto()))
                .collect::<Result<BTreeMap<_, _>>>()?,
        })
    }
}

impl FromProto<wkt::Value> for prost_types::Value {
    fn cnv(self) -> Result<wkt::Value> {
        use prost_types::value::Kind;
        let kind = match self.kind {
            None => wkt::Value::Null,
            Some(kind) => match kind {
                Kind::NullValue(_) => wkt::Value::Null,
                Kind::NumberValue(v) => {
                    let number =
                        serde_json::Number::from_f64(v).expect("JSON numbers cannot be NaN");
                    serde_json::Value::Number(number)
                }
                Kind::StringValue(v) => wkt::Value::String(v),
                Kind::BoolValue(v) => wkt::Value::Bool(v),
                Kind::StructValue(v) => wkt::Value::Object(v.cnv()?),
                Kind::ListValue(v) => wkt::Value::Array(v.cnv()?),
            },
        };
        Ok(kind)
    }
}

impl ToProto<prost_types::Value> for wkt::Value {
    type Output = prost_types::Value;
    fn to_proto(self) -> Result<prost_types::Value> {
        use prost_types::value::Kind;
        let kind = match self {
            serde_json::Value::Null => Kind::NullValue(0),
            serde_json::Value::Number(v) => Kind::NumberValue(v.as_f64().unwrap_or_default()),
            serde_json::Value::String(v) => Kind::StringValue(v),
            serde_json::Value::Bool(v) => Kind::BoolValue(v),
            serde_json::Value::Array(v) => Kind::ListValue(v.to_proto()?),
            serde_json::Value::Object(v) => Kind::StructValue(v.to_proto()?),
        };
        Ok(prost_types::Value { kind: Some(kind) })
    }
}

impl FromProto<wkt::ListValue> for prost_types::ListValue {
    fn cnv(self) -> Result<wkt::ListValue> {
        self.values
            .into_iter()
            .map(|v| v.cnv())
            .collect::<Result<Vec<_>>>()
    }
}

impl ToProto<prost_types::ListValue> for wkt::ListValue {
    type Output = prost_types::ListValue;
    fn to_proto(self) -> Result<Self::Output> {
        Ok(prost_types::ListValue {
            values: self
                .into_iter()
                .map(|v| v.to_proto())
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl ToProto<prost_types::NullValue> for wkt::NullValue {
    type Output = i32;
    fn to_proto(self) -> Result<Self::Output> {
        Ok(prost_types::NullValue::NullValue as i32)
    }
}

impl FromProto<wkt::NullValue> for prost_types::NullValue {
    fn cnv(self) -> Result<wkt::NullValue> {
        Ok(wkt::NullValue)
    }
}

/// A narrow, intentionally incomplete conversion for `DescriptorProto`.
///  
/// This conversion drops several AST fields and must not be used as
/// a general-purpose converter. It exists solely to bypass librarian
/// limitations when generating `ProtoSchema` in BigQuery.
// TODO(#6616): Generate this code.
impl FromProto<wkt::DescriptorProto> for prost_types::DescriptorProto {
    fn cnv(self) -> Result<wkt::DescriptorProto> {
        Ok(wkt::DescriptorProto::default()
            .set_name(self.name.unwrap_or_default())
            .set_field(
                self.field
                    .into_iter()
                    .map(|v| {
                        wkt::FieldDescriptorProto::default()
                            .set_name(v.name.unwrap_or_default())
                            .set_number(v.number.unwrap_or_default())
                            .set_label(wkt::field_descriptor_proto::Label::from(
                                v.label.unwrap_or(0),
                            ))
                            .set_type(wkt::field_descriptor_proto::Type::from(
                                v.r#type.unwrap_or(0),
                            ))
                            .set_type_name(v.type_name.unwrap_or_default())
                            .set_json_name(v.json_name.unwrap_or_default())
                            .set_default_value(v.default_value.unwrap_or_default())
                    })
                    .collect::<Vec<_>>(),
            )
            .set_nested_type(
                self.nested_type
                    .into_iter()
                    .map(|v| v.cnv())
                    .collect::<Result<Vec<_>>>()?,
            )
            .set_enum_type(
                self.enum_type
                    .into_iter()
                    .map(|v| {
                        wkt::EnumDescriptorProto::default()
                            .set_name(v.name.unwrap_or_default())
                            .set_value(
                                v.value
                                    .into_iter()
                                    .map(|ev| {
                                        wkt::EnumValueDescriptorProto::default()
                                            .set_name(ev.name.unwrap_or_default())
                                            .set_number(ev.number.unwrap_or_default())
                                    })
                                    .collect::<Vec<_>>(),
                            )
                    })
                    .collect::<Vec<_>>(),
            ))
    }
}

/// A narrow, intentionally incomplete conversion for `DescriptorProto`.
///  
/// This conversion drops several AST fields and must not be used as
/// a general-purpose converter. It exists solely to bypass librarian
/// limitations when generating `ProtoSchema` in BigQuery.
// TODO(#6616): Generate this code.
impl ToProto<prost_types::DescriptorProto> for wkt::DescriptorProto {
    type Output = prost_types::DescriptorProto;
    fn to_proto(self) -> Result<prost_types::DescriptorProto> {
        Ok(prost_types::DescriptorProto {
            name: Some(self.name),
            field: self
                .field
                .into_iter()
                .map(|v| prost_types::FieldDescriptorProto {
                    name: Some(v.name),
                    number: Some(v.number),
                    label: v.label.value(),
                    r#type: v.r#type.value(),
                    type_name: Some(v.type_name),
                    json_name: Some(v.json_name),
                    default_value: Some(v.default_value),
                    ..Default::default()
                })
                .collect(),
            nested_type: self
                .nested_type
                .into_iter()
                .map(|v| v.to_proto())
                .collect::<Result<Vec<_>>>()?,
            enum_type: self
                .enum_type
                .into_iter()
                .map(|v| prost_types::EnumDescriptorProto {
                    name: Some(v.name),
                    value: v
                        .value
                        .into_iter()
                        .map(|ev| prost_types::EnumValueDescriptorProto {
                            name: Some(ev.name),
                            number: Some(ev.number),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
    }
}

/// A placeholder for `google.protobuf.Empty`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Empty {}

impl ::prost::Name for Empty {
    const NAME: &'static str = "Empty";
    const PACKAGE: &'static str = "google.protobuf";
    fn full_name() -> ::prost::alloc::string::String {
        "google.protobuf.Empty".into()
    }
    fn type_url() -> ::prost::alloc::string::String {
        "type.googleapis.com/google.protobuf.Empty".into()
    }
}

impl ToProto<Empty> for wkt::Empty {
    type Output = Empty;
    fn to_proto(self) -> Result<Self::Output> {
        Ok(Empty {})
    }
}

impl FromProto<wkt::Empty> for Empty {
    fn cnv(self) -> Result<wkt::Empty> {
        Ok(wkt::Empty::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use test_case::test_case;

    #[test]
    fn fmt_convert_error() {
        let e = ConvertError::EnumNoIntegerValue("name123");
        let fmt = format!("{e}");
        assert!(
            fmt.contains("name123") && fmt.contains("does not contain an integer"),
            "{fmt}"
        );

        let e =
            ConvertError::UnexpectedTypeUrl("type.googleapis.com/my.custom.Message".to_string());
        let fmt = format!("{e}");
        assert!(
            fmt.contains("type.googleapis.com/my.custom.Message")
                && fmt.contains("Unexpected type"),
            "{fmt}"
        );

        let source = wkt::AnyError::TypeMismatch {
            has: "has.type".into(),
            want: "want.type".into(),
        };
        let e = ConvertError::other(source);
        let fmt = format!("{e}");
        ["gax/prost conversion error", "has.type", "want.type"]
            .into_iter()
            .for_each(|want| assert!(fmt.contains(want), "missing {want} in {fmt}"));
    }

    fn err() -> ConvertError {
        ConvertError::EnumNoIntegerValue("test")
    }

    #[test]
    fn pair_transpose_success() -> anyhow::Result<()> {
        let got = super::pair_transpose(Ok(1), Ok(2))?;
        assert_eq!(got, (1, 2));
        Ok(())
    }

    #[test_case(Err(err()), Ok(2))]
    #[test_case(Ok(1), Err(err()))]
    #[test_case(Err(err()), Err(err()))]
    fn pair_transpose_error(a: Result<i32>, b: Result<i32>) -> anyhow::Result<()> {
        let got = super::pair_transpose(a, b);
        assert!(got.is_err(), "{got:?}");
        Ok(())
    }

    #[test]
    fn primitive_unit() -> anyhow::Result<()> {
        ().cnv()?;
        ().to_proto()?;
        Ok(())
    }

    #[test]
    fn primitive_bool() -> anyhow::Result<()> {
        let input: bool = true;
        let got = input.cnv()?;
        assert_eq!(got, input);
        let input: bool = true;
        let got = input.to_proto()?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test_case(0 as f32)]
    #[test_case(0_i32)]
    #[test_case(0_u32)]
    #[test_case(0 as f64)]
    #[test_case(0_i64)]
    #[test_case(0_u64)]
    fn primitive_numeric_from_proto<T>(input: T) -> anyhow::Result<()>
    where
        T: std::fmt::Debug + Copy + PartialEq + FromProto<T>,
    {
        let got = input.cnv()?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test_case(0 as f32)]
    #[test_case(0_i32)]
    #[test_case(0_u32)]
    #[test_case(0 as f64)]
    #[test_case(0_i64)]
    #[test_case(0_u64)]
    fn primitive_numeric_to_proto<T>(input: T) -> anyhow::Result<()>
    where
        T: std::fmt::Debug + Copy + PartialEq + ToProto<T, Output = T>,
    {
        let got = input.to_proto()?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test]
    fn primitive_string() -> anyhow::Result<()> {
        let input = "abc".to_string();
        let got = input.cnv()?;
        assert_eq!(got, "abc");
        let input = "abc".to_string();
        let got = input.to_proto()?;
        assert_eq!(got, "abc");
        Ok(())
    }

    #[test]
    fn primitive_bytes() -> anyhow::Result<()> {
        let input = bytes::Bytes::from_static(b"abc");
        let got = input.clone().cnv()?;
        assert_eq!(got, input);
        let input = bytes::Bytes::from_static(b"abc");
        let got = input.clone().to_proto()?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test]
    fn from_proto_duration() -> anyhow::Result<()> {
        let input = prost_types::Duration {
            seconds: 123,
            nanos: 456,
        };
        let got = input.cnv()?;
        assert_eq!(got, wkt::Duration::clamp(123, 456));
        Ok(())
    }

    #[test]
    fn to_proto_duration() -> anyhow::Result<()> {
        let input = wkt::Duration::clamp(123, 456);
        let got = input.to_proto()?;
        assert_eq!(
            got,
            prost_types::Duration {
                seconds: 123,
                nanos: 456
            }
        );
        Ok(())
    }

    #[test]
    fn from_proto_field_mask() -> anyhow::Result<()> {
        let input = prost_types::FieldMask {
            paths: ["a", "b", "c"].map(str::to_string).to_vec(),
        };
        let got = input.cnv()?;
        assert_eq!(got, wkt::FieldMask::default().set_paths(["a", "b", "c"]));
        Ok(())
    }

    #[test]
    fn to_proto_field_mask() -> anyhow::Result<()> {
        let input = wkt::FieldMask::default().set_paths(["p1", "p2", "p3"]);
        let got = input.to_proto()?;
        assert_eq!(
            got,
            prost_types::FieldMask {
                paths: ["p1", "p2", "p3"].map(str::to_string).to_vec()
            }
        );
        Ok(())
    }

    #[test]
    fn from_proto_timestamp() -> anyhow::Result<()> {
        let input = prost_types::Timestamp {
            seconds: 123,
            nanos: 456,
        };
        let got = input.cnv()?;
        assert_eq!(got, wkt::Timestamp::clamp(123, 456));
        Ok(())
    }

    #[test]
    fn to_proto_timestamp() -> anyhow::Result<()> {
        let input = wkt::Timestamp::clamp(123, 456);
        let got = input.to_proto()?;
        assert_eq!(
            got,
            prost_types::Timestamp {
                seconds: 123,
                nanos: 456
            }
        );
        Ok(())
    }

    #[test_case(json!(null))]
    #[test_case(json!(1234.5))]
    #[test_case(json!("xyz"))]
    #[test_case(json!([true, 1234.5, "xyz", null, {"a": "b"}]))]
    #[test_case(json!({"a": true, "b": "xyz"}))]
    fn wkt_value_roundtrip(input: wkt::Value) -> anyhow::Result<()> {
        let convert = input.clone().to_proto()?;
        let got = convert.cnv()?;
        assert_eq!(got, input);
        Ok(())
    }

    #[test]
    fn to_proto_null_value() -> anyhow::Result<()> {
        let input = wkt::NullValue;
        let got: i32 = input.to_proto()?;
        assert_eq!(got, 0);
        Ok(())
    }

    #[test]
    fn from_prost_null_value() -> anyhow::Result<()> {
        let input = prost_types::NullValue::NullValue;
        let got = input.cnv()?;
        assert_eq!(got, wkt::NullValue);
        Ok(())
    }

    #[test]
    fn to_proto_empty() -> anyhow::Result<()> {
        let input = wkt::Empty::default();
        let got: super::Empty = input.to_proto()?;
        assert_eq!(got, super::Empty {});
        Ok(())
    }

    #[test]
    fn from_prost_empty() -> anyhow::Result<()> {
        let input = super::Empty {};
        let got = input.cnv()?;
        assert_eq!(got, wkt::Empty::default());
        Ok(())
    }

    #[test]
    fn prost_empty_and_any() -> anyhow::Result<()> {
        use prost::Name as _;
        let input = super::Empty {};
        let any = prost_types::Any::from_msg(&input)?;
        assert_eq!(any.type_url, super::Empty::type_url());
        let got = any.to_msg::<super::Empty>()?;
        assert_eq!(input, got);
        Ok(())
    }

    #[test]
    fn prost_empty_names() -> anyhow::Result<()> {
        use prost::Name as _;
        let full = super::Empty::full_name();
        let want = format!("{}.{}", super::Empty::PACKAGE, super::Empty::NAME);
        assert_eq!(full, want);
        let url = super::Empty::type_url();
        let want = format!("type.googleapis.com/{full}");
        assert_eq!(url, want);
        Ok(())
    }

    #[test]
    fn test_descriptor_proto_conversion() -> anyhow::Result<()> {
        let mut input = wkt::DescriptorProto::default().set_name("TestMessage".to_string());
        let field = wkt::FieldDescriptorProto::default()
            .set_name("test_field".to_string())
            .set_number(1)
            .set_label(wkt::field_descriptor_proto::Label::Optional)
            .set_type(wkt::field_descriptor_proto::Type::Int32)
            .set_type_name("int32".to_string())
            .set_json_name("testField".to_string())
            .set_default_value("42".to_string());
        input.field = vec![field];
        let prost_msg: prost_types::DescriptorProto = input.clone().to_proto()?;
        assert_eq!(prost_msg.name, Some("TestMessage".to_string()));
        assert_eq!(prost_msg.field.len(), 1);
        assert_eq!(prost_msg.field[0].name, Some("test_field".to_string()));
        assert_eq!(prost_msg.field[0].number, Some(1));
        assert_eq!(
            prost_msg.field[0].label(),
            prost_types::field_descriptor_proto::Label::Optional
        );
        assert_eq!(
            prost_msg.field[0].r#type(),
            prost_types::field_descriptor_proto::Type::Int32
        );
        assert_eq!(prost_msg.field[0].type_name, Some("int32".to_string()));
        assert_eq!(prost_msg.field[0].json_name, Some("testField".to_string()));
        assert_eq!(prost_msg.field[0].default_value, Some("42".to_string()));

        let back: wkt::DescriptorProto = prost_msg.cnv()?;
        assert_eq!(back.name, input.name);
        assert_eq!(back.field[0].name, input.field[0].name);
        assert_eq!(back.field[0].number, input.field[0].number);
        assert_eq!(back.field[0].label, input.field[0].label);
        assert_eq!(back.field[0].r#type, input.field[0].r#type);
        assert_eq!(back.field[0].type_name, input.field[0].type_name);
        assert_eq!(back.field[0].json_name, input.field[0].json_name);
        assert_eq!(back.field[0].default_value, input.field[0].default_value);
        Ok(())
    }
}
