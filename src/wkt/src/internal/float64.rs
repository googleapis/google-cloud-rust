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

pub struct F64;

impl_visitor!(DoubleVisitor, f64, "a 64-bit float in ProtoJSON format");

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
    #[test_case(json!("-1.89769e+308"); "range negative")] // Used in ProtoJSON conformance test
    #[test_case(json!("1.89769e+308"); "range positive")] // Used in ProtoJSON conformance test
    fn deserialize_expect_err_64(input: Value) {
        let err = F64::deserialize_as(input).unwrap_err();
        assert!(err.is_data(), "{err:?}");
    }

    impl_assert_float_eq!(assert_double_eq, f64);
}
