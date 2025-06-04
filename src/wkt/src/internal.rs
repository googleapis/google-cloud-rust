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

//! Implementation details provided by the `google-cloud-sdk` crate.
//!
//! These types are intended for developers of the Google Cloud client libraries
//! for Rust. They are undocumented and may change at any time.

#[macro_use]
mod visitor_32;
mod int32;
pub use int32::I32;
mod uint32;
pub use uint32::U32;

#[macro_use]
mod visitor_64;
mod int64;
pub use int64::I64;
mod uint64;
pub use uint64::U64;

mod value;
pub use value::OptionalValue;

pub struct F32;
pub struct F64;

macro_rules! impl_serialize_as {
    ($t: ty, $ser_fn: ident) => {
        fn serialize_as<S>(value: &$t, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::ser::Serializer,
        {
            match value {
                x if x.is_nan() => serializer.serialize_str("NaN"),
                x if x.is_infinite() && x.is_sign_negative() => {
                    serializer.serialize_str("-Infinity")
                }
                x if x.is_infinite() => serializer.serialize_str("Infinity"),
                x => serializer.$ser_fn(*x),
            }
        }
    };
}

macro_rules! impl_visitor {
    ($name: ident, $t: ty, $msg: literal) => {
        struct $name;

        impl serde::de::Visitor<'_> for $name {
            type Value = $t;

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // Handle special strings, see https://protobuf.dev/programming-guides/json/.
                match value {
                    "NaN" => Ok(<$t>::NAN),
                    "Infinity" => Ok(<$t>::INFINITY),
                    "-Infinity" => Ok(<$t>::NEG_INFINITY),
                    _ => self.visit_f64(value.parse::<f64>().map_err(E::custom)?),
                }
            }

            // Floats and doubles in serde_json may be serialized as integers
            // if they do not have a fractional part.
            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // This is trivial for `f64`. For `f32`, casting f64 to f32 is guaranteed to produce the closest possible float value:
                // See https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-narrowing
                Ok(value as Self::Value)
            }

            // Floats and doubles in serde_json may be serialized as integers
            // if they do not have a fractional part.
            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // This is trivial for `f64`. For `f32`, casting f64 to f32 is guaranteed to produce the closest possible float value:
                // See https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-narrowing
                Ok(value as Self::Value)
            }

            // Floats and doubles in serde_json are f64.
            fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                // This is trivial for `f64`. For `f32`, casting f64 to f32
                // is guaranteed to produce the closest possible float
                // value:
                //     https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-narrowing
                match value {
                    _ if value < <$t>::MIN as f64 => Err(self::value_error(value, $msg)),
                    _ if value > <$t>::MAX as f64 => Err(self::value_error(value, $msg)),
                    _ => Ok(value as Self::Value),
                }
            }

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str($msg)
            }
        }
    };
}

fn value_error<E>(value: f64, msg: &str) -> E
where
    E: serde::de::Error,
{
    E::invalid_value(serde::de::Unexpected::Float(value), &msg)
}

impl_visitor!(FloatVisitor, f32, "a 32-bit float in ProtoJSON format");
impl_visitor!(DoubleVisitor, f64, "a 64-bit float in ProtoJSON format");

impl serde_with::SerializeAs<f32> for F32 {
    impl_serialize_as!(f32, serialize_f32);
}

impl<'de> serde_with::DeserializeAs<'de, f32> for F32 {
    fn deserialize_as<D>(deserializer: D) -> Result<f32, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(FloatVisitor)
    }
}

impl serde_with::SerializeAs<f64> for F64 {
    impl_serialize_as!(f64, serialize_f64);
}

impl<'de> serde_with::DeserializeAs<'de, f64> for F64 {
    fn deserialize_as<D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(DoubleVisitor)
    }
}

// For skipping serialization of default values of bool/numeric types.
pub fn is_default<T>(t: &T) -> bool
where
    T: Default + PartialEq,
{
    *t == T::default()
}

mod enums;
pub use enums::*;

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::{Value, json};
    use serde_with::{DeserializeAs, SerializeAs};
    use test_case::test_case;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test_case(9876.5, 9876.5)]
    #[test_case(0.0, 0.0)]
    #[test_case(1_f32, 1.0)]
    #[test_case(-2_f32, -2.0)]
    #[test_case(f32::NAN, "NaN")]
    #[test_case(-f32::NAN, "NaN")]
    #[test_case(f32::INFINITY, "Infinity")]
    #[test_case(2.0*f32::INFINITY, "Infinity")]
    #[test_case(f32::NEG_INFINITY, "-Infinity")]
    #[test_case(2.0*f32::NEG_INFINITY, "-Infinity")]
    #[test_case(f32::MAX, f32::MAX)]
    #[test_case(f32::MIN, f32::MIN)]
    #[test_case(f32::EPSILON, f32::EPSILON)]
    #[test_case(f32::MIN_POSITIVE, f32::MIN_POSITIVE)]
    #[test_case(-f32::MIN_POSITIVE, -f32::MIN_POSITIVE; "negative of MIN_POSITIVE")]
    fn roundtrip_f32<T>(input: f32, want: T) -> Result
    where
        T: std::fmt::Debug,
        serde_json::Value: PartialEq<T>,
    {
        let got = F32::serialize_as(&input, serde_json::value::Serializer)?;
        assert_eq!(got, want);
        let rt = F32::deserialize_as(got)?;
        assert_float_eq(input, rt);
        Ok(())
    }

    #[test_case("0", 0.0)]
    #[test_case("0.0", 0.0; "zero with trailing")]
    #[test_case("0.5", 0.5)]
    #[test_case("-0.75", -0.75)]
    #[test_case("123", 123.0)]
    #[test_case("-234", -234.0)]
    #[test_case(format!("{:.1}", f32::MAX), f32::MAX)]
    #[test_case(format!("{:.1}", f32::MIN), f32::MIN)]
    #[test_case(format!("{}", f32::EPSILON), f32::EPSILON)]
    #[test_case(format!("{}", f32::MIN_POSITIVE), f32::MIN_POSITIVE)]
    #[test_case(format!("{}", -f32::MIN_POSITIVE), -f32::MIN_POSITIVE; "negative of MIN_POSITIVE")]
    fn parse_string_f32<T>(input: T, want: f32) -> Result
    where
        T: Into<String> + std::fmt::Display + Clone,
    {
        let got = F32::deserialize_as(Value::String(input.clone().into()))?;
        assert_eq!(got, want, "{input}");
        Ok(())
    }

    #[test_case(-1_i32, -1.0)]
    #[test_case(-1_i64, -1.0)]
    #[test_case(2_i32, 2.0)]
    #[test_case(2_i64, 2.0)]
    fn deserialize_int_f32<T>(input: T, want: f32) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!(input);
        let got = F32::deserialize_as(value)?;
        assert_float_eq(got, want);
        Ok(())
    }

    #[test_case(9876.5_f64, 9876.5)]
    #[test_case(0.0, 0.0)]
    #[test_case(f64::NAN, "NaN")]
    #[test_case(-f64::NAN, "NaN")]
    #[test_case(f64::INFINITY, "Infinity")]
    #[test_case(2.0*f64::INFINITY, "Infinity")]
    #[test_case(f64::NEG_INFINITY, "-Infinity")]
    #[test_case(2.0*f64::NEG_INFINITY, "-Infinity")]
    #[test_case(f64::MAX, f64::MAX)]
    #[test_case(f64::MIN, f64::MIN)]
    #[test_case(f64::EPSILON, f64::EPSILON)]
    #[test_case(f64::MIN_POSITIVE, f64::MIN_POSITIVE)]
    #[test_case(-f64::MIN_POSITIVE, -f64::MIN_POSITIVE; "negative of MIN_POSITIVE")]
    fn roundtrip_f64<T>(input: f64, want: T) -> Result
    where
        T: std::fmt::Debug,
        serde_json::Value: PartialEq<T>,
    {
        let got = F64::serialize_as(&input, serde_json::value::Serializer)?;
        assert_eq!(got, want);
        let rt = F64::deserialize_as(got)?;
        assert_double_eq(input, rt);
        Ok(())
    }

    #[test_case("0", 0.0)]
    #[test_case("0.0", 0.0; "zero with trailing")]
    #[test_case("0.5", 0.5)]
    #[test_case("-0.75", -0.75)]
    #[test_case("123", 123.0)]
    #[test_case("-234", -234.0)]
    #[test_case(format!("{}", f64::MAX).as_str(), f64::MAX)]
    #[test_case(format!("{}", f64::MIN).as_str(), f64::MIN)]
    #[test_case(format!("{}", f64::EPSILON).as_str(), f64::EPSILON)]
    #[test_case(format!("{}", f64::MIN_POSITIVE).as_str(), f64::MIN_POSITIVE)]
    #[test_case(format!("{}", -f64::MIN_POSITIVE).as_str(), -f64::MIN_POSITIVE; "negative of MIN_POSITIVE")]
    fn parse_string_f64<T>(input: T, want: f64) -> Result
    where
        T: Into<String> + std::fmt::Display + Clone,
    {
        let got = F64::deserialize_as(Value::String(input.clone().into()))?;
        assert_eq!(got, want, "{input}");
        Ok(())
    }

    #[test_case(-1_i32, -1.0)]
    #[test_case(-1_i64, -1.0)]
    #[test_case(2_i32, 2.0)]
    #[test_case(2_i64, 2.0)]
    fn deserialize_int_f64<T>(input: T, want: f64) -> Result
    where
        T: serde::ser::Serialize,
    {
        let value = json!(input);
        let got = F64::deserialize_as(value)?;
        assert_double_eq(got, want);
        Ok(())
    }

    #[test_case(json!("some string"))]
    #[test_case(json!(true))]
    #[test_case(json!(f32::MAX as f64 * 2.0))]
    #[test_case(json!(f32::MIN as f64 * 2.0))]
    #[test_case(json!(-3.502823e+38); "range negative")] // Used in ProtoJSON conformance test
    #[test_case(json!(3.502823e+38); "range positive")] // Used in ProtoJSON conformance test
    fn deserialize_expect_err_32(input: Value) {
        let err = F32::deserialize_as(input).unwrap_err();
        assert!(err.is_data(), "{err:?}");
    }

    #[test_case(json!("some string"))]
    #[test_case(json!(true))]
    #[test_case(json!("-1.89769e+308"); "range negative")] // Used in ProtoJSON conformance test
    #[test_case(json!("1.89769e+308"); "range positive")] // Used in ProtoJSON conformance test
    fn deserialize_expect_err_64(input: Value) {
        let err = F64::deserialize_as(input).unwrap_err();
        assert!(err.is_data(), "{err:?}");
    }

    macro_rules! impl_assert_float_eq {
        ($fn: ident, $t: ty) => {
            fn $fn(left: $t, right: $t) {
                // Consider all NaN as equal.
                if left.is_nan() && right.is_nan() {
                    return;
                }
                // Consider all infinites floats of the same sign as equal.
                if left.is_infinite()
                    && right.is_infinite()
                    && left.is_sign_positive() == right.is_sign_positive()
                {
                    return;
                }
                assert_eq!(left, right);
            }
        };
    }
    impl_assert_float_eq!(assert_float_eq, f32);
    impl_assert_float_eq!(assert_double_eq, f64);
}
