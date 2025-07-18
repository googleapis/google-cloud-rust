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

//! Implement custom serializers for `i32`.
//!
//! In ProtoJSON 32-bit integers can be serialized as either strings or numbers.

use serde::de::Unexpected::Other;

pub struct I32;

impl<'de> serde_with::DeserializeAs<'de, i32> for I32 {
    fn deserialize_as<D>(deserializer: D) -> Result<i32, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(I32Visitor)
    }
}

visitor_32!(I32Visitor, i32, "a 32-bit signed integer");

impl serde_with::SerializeAs<i32> for I32 {
    fn serialize_as<S>(source: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(*source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use serde_json::{Value, json};
    use serde_with::{DeserializeAs, SerializeAs};
    use test_case::test_case;

    #[test_case(0, 0)]
    #[test_case("0", 0; "zero string")]
    #[test_case("2.0", 2)]
    #[test_case(3e5, 300_000)]
    #[test_case(-4e4, -40_000)]
    #[test_case("5e4", 50_000)]
    #[test_case("-6e5", -600_000)]
    #[test_case(-42, -42)]
    #[test_case("-7", -7)]
    #[test_case(84, 84)]
    #[test_case(168.0, 168)]
    #[test_case("21", 21)]
    #[test_case(i32::MAX, i32::MAX; "max")]
    #[test_case(i32::MAX as f64, i32::MAX; "max as f64")]
    #[test_case(format!("{}", i32::MAX), i32::MAX; "max as string")]
    #[test_case(format!("{}.0", i32::MAX), i32::MAX; "max as f64 string")]
    #[test_case(i32::MIN, i32::MIN; "min")]
    #[test_case(i32::MIN as f64, i32::MIN; "min as f64")]
    #[test_case(format!("{}", i32::MIN), i32::MIN; "min as string")]
    #[test_case(format!("{}.0", i32::MIN), i32::MIN; "min as f64 string")]
    // Not quite a roundtrip test because we always serialize as numbers.
    fn deser_and_ser<T: serde::Serialize>(input: T, want: i32) -> Result<()> {
        let got = I32::deserialize_as(json!(input))?;
        assert_eq!(got, want);

        let serialized = I32::serialize_as(&got, serde_json::value::Serializer)?;
        assert_eq!(serialized, json!(got));
        Ok(())
    }

    #[test_case(json!(i64::MAX))]
    #[test_case(json!(i64::MIN))]
    #[test_case(json!(i32::MAX as i64 + 2))]
    #[test_case(json!(i32::MIN as i64 - 2))]
    #[test_case(json!(format!("{}", i64::MAX)))]
    #[test_case(json!(format!("{}", i64::MIN)))]
    #[test_case(json!("abc"))]
    #[test_case(json!(123.4))]
    #[test_case(json!("234.5"))]
    #[test_case(json!({}))]
    fn deser_error(input: Value) {
        let got = I32::deserialize_as(input).unwrap_err();
        assert!(got.is_data(), "{got:?}");
    }
}
