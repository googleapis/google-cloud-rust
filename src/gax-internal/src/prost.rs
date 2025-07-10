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
        assert!(got.is_err());
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
}
