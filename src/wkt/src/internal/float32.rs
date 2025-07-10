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

pub struct F32;

impl_visitor!(FloatVisitor, f32, "a 32-bit float in ProtoJSON format");

impl<'de> serde_with::DeserializeAs<'de, f32> for F32 {
    fn deserialize_as<D>(deserializer: D) -> Result<f32, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(FloatVisitor)
    }
}

impl serde_with::SerializeAs<f32> for F32 {
    impl_serialize_as!(f32, serialize_f32);
}

fn value_error<E>(value: f64, msg: &str) -> E
where
    E: serde::de::Error,
{
    E::invalid_value(serde::de::Unexpected::Float(value), &msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use serde_with::{DeserializeAs, SerializeAs};
    use test_case::test_case;
    type Result = anyhow::Result<()>;

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

    impl_assert_float_eq!(assert_float_eq, f32);
}
