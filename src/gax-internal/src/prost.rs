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

/// Converts from `Self` into `T`.
pub trait Convert<T>: Sized {
    fn cnv(self) -> T;
}

macro_rules! impl_primitive {
    ($t: ty) => {
        impl Convert<$t> for $t {
            fn cnv(self) -> $t {
                self
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

impl Convert<wkt::Duration> for prost_types::Duration {
    fn cnv(self) -> wkt::Duration {
        wkt::Duration::clamp(self.seconds, self.nanos)
    }
}

impl Convert<prost_types::Duration> for wkt::Duration {
    fn cnv(self) -> prost_types::Duration {
        prost_types::Duration {
            seconds: self.seconds(),
            nanos: self.nanos(),
        }
    }
}

impl Convert<wkt::FieldMask> for prost_types::FieldMask {
    fn cnv(self) -> wkt::FieldMask {
        wkt::FieldMask::default().set_paths(self.paths)
    }
}

impl Convert<prost_types::FieldMask> for wkt::FieldMask {
    fn cnv(self) -> prost_types::FieldMask {
        prost_types::FieldMask { paths: self.paths }
    }
}

impl Convert<wkt::Timestamp> for prost_types::Timestamp {
    fn cnv(self) -> wkt::Timestamp {
        wkt::Timestamp::clamp(self.seconds, self.nanos)
    }
}

impl Convert<prost_types::Timestamp> for wkt::Timestamp {
    fn cnv(self) -> prost_types::Timestamp {
        prost_types::Timestamp {
            seconds: self.seconds(),
            nanos: self.nanos(),
        }
    }
}

impl Convert<wkt::Struct> for prost_types::Struct {
    fn cnv(self) -> wkt::Struct {
        self.fields
            .into_iter()
            .map(|(k, v)| (k.cnv(), v.cnv()))
            .collect()
    }
}

impl Convert<prost_types::Struct> for wkt::Struct {
    fn cnv(self) -> prost_types::Struct {
        prost_types::Struct {
            fields: self.into_iter().map(|(k, v)| (k.cnv(), v.cnv())).collect(),
        }
    }
}

impl Convert<wkt::Value> for prost_types::Value {
    fn cnv(self) -> wkt::Value {
        use prost_types::value::Kind;
        match self.kind {
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
                Kind::StructValue(v) => wkt::Value::Object(v.cnv()),
                Kind::ListValue(v) => wkt::Value::Array(v.cnv()),
            },
        }
    }
}

impl Convert<prost_types::Value> for wkt::Value {
    fn cnv(self) -> prost_types::Value {
        use prost_types::value::Kind;
        let kind = match self {
            serde_json::Value::Null => Kind::NullValue(0),
            serde_json::Value::Number(v) => Kind::NumberValue(v.as_f64().unwrap_or_default()),
            serde_json::Value::String(v) => Kind::StringValue(v),
            serde_json::Value::Bool(v) => Kind::BoolValue(v),
            serde_json::Value::Array(v) => Kind::ListValue(v.cnv()),
            serde_json::Value::Object(v) => Kind::StructValue(v.cnv()),
        };
        prost_types::Value { kind: Some(kind) }
    }
}

impl Convert<wkt::ListValue> for prost_types::ListValue {
    fn cnv(self) -> wkt::ListValue {
        self.values.into_iter().map(|v| v.cnv()).collect()
    }
}

impl Convert<prost_types::ListValue> for wkt::ListValue {
    fn cnv(self) -> prost_types::ListValue {
        prost_types::ListValue {
            values: self.into_iter().map(|v| v.cnv()).collect(),
        }
    }
}

impl Convert<i32> for wkt::NullValue {
    fn cnv(self) -> i32 {
        prost_types::NullValue::NullValue as i32
    }
}

impl Convert<wkt::NullValue> for prost_types::NullValue {
    fn cnv(self) -> wkt::NullValue {
        wkt::NullValue
    }
}

impl Convert<prost_types::NullValue> for wkt::NullValue {
    fn cnv(self) -> prost_types::NullValue {
        prost_types::NullValue::NullValue
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use test_case::test_case;

    #[test]
    fn primitive_unit() {
        assert_eq!((), ().cnv());
    }

    #[test]
    fn primitive_bool() {
        let input: bool = true;
        let got: bool = input.cnv();
        assert_eq!(got, input);
    }

    #[test_case(0 as f32)]
    #[test_case(0_i32)]
    #[test_case(0_u32)]
    #[test_case(0 as f64)]
    #[test_case(0_i64)]
    #[test_case(0_u64)]
    fn primitive_numeric<T>(input: T)
    where
        T: std::fmt::Debug + Copy + PartialEq + Convert<T>,
    {
        let got: T = input.cnv();
        assert_eq!(got, input);
    }

    #[test]
    fn primitive_string() {
        let input = "abc".to_string();
        let got: String = input.cnv();
        assert_eq!(&got, "abc");
    }

    #[test]
    fn primitive_bytes() {
        let input = bytes::Bytes::from_static(b"abc");
        let got: bytes::Bytes = input.clone().cnv();
        assert_eq!(got, input);
    }

    #[test]
    fn from_prost_duration() {
        let input = prost_types::Duration {
            seconds: 123,
            nanos: 456,
        };
        let got: wkt::Duration = input.cnv();
        assert_eq!(got, wkt::Duration::clamp(123, 456));
    }

    #[test]
    fn from_wkt_duration() {
        let input = wkt::Duration::clamp(123, 456);
        let got: prost_types::Duration = input.cnv();
        assert_eq!(
            got,
            prost_types::Duration {
                seconds: 123,
                nanos: 456
            }
        );
    }

    #[test]
    fn from_prost_field_mask() {
        let input = prost_types::FieldMask {
            paths: ["a", "b", "c"].map(str::to_string).to_vec(),
        };
        let got: wkt::FieldMask = input.cnv();
        assert_eq!(got, wkt::FieldMask::default().set_paths(["a", "b", "c"]));
    }

    #[test]
    fn from_wkt_field_mask() {
        let input = wkt::FieldMask::default().set_paths(["p1", "p2", "p3"]);
        let got: prost_types::FieldMask = input.cnv();
        assert_eq!(
            got,
            prost_types::FieldMask {
                paths: ["p1", "p2", "p3"].map(str::to_string).to_vec()
            }
        );
    }

    #[test]
    fn from_prost_timestamp() {
        let input = prost_types::Timestamp {
            seconds: 123,
            nanos: 456,
        };
        let got: wkt::Timestamp = input.cnv();
        assert_eq!(got, wkt::Timestamp::clamp(123, 456));
    }

    #[test]
    fn from_wkt_timestamp() {
        let input = wkt::Timestamp::clamp(123, 456);
        let got: prost_types::Timestamp = input.cnv();
        assert_eq!(
            got,
            prost_types::Timestamp {
                seconds: 123,
                nanos: 456
            }
        );
    }

    #[test_case(json!(null))]
    #[test_case(json!(1234.5))]
    #[test_case(json!("xyz"))]
    #[test_case(json!([true, 1234.5, "xyz", null, {"a": "b"}]))]
    #[test_case(json!({"a": true, "b": "xyz"}))]
    fn wkt_value_roundtrip(input: wkt::Value) {
        let convert: prost_types::Value = input.clone().cnv();
        let got: wkt::Value = convert.cnv();
        assert_eq!(got, input);
    }

    #[test]
    fn from_wkt_null_value() {
        let input = wkt::NullValue;
        let got: i32 = input.cnv();
        assert_eq!(got, 0);

        let input = wkt::NullValue;
        let got: prost_types::NullValue = input.cnv();
        assert_eq!(got, prost_types::NullValue::NullValue);
    }

    #[test]
    fn from_prost_null_value() {
        let input = prost_types::NullValue::NullValue;
        let got: wkt::NullValue = input.cnv();
        assert_eq!(got, wkt::NullValue);
    }
}
