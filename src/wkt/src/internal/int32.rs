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

const ERRMSG: &str = "a 32-bit signed integer";

struct I32Visitor;

impl serde::de::Visitor<'_> for I32Visitor {
    type Value = i32;

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value.parse::<i32>().map_err(E::custom)
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match value {
            _ if value < i32::MIN as i64 => {
                Err(E::invalid_value(Other(&format!("{value}")), &ERRMSG))
            }
            _ if value > i32::MAX as i64 => {
                Err(E::invalid_value(Other(&format!("{value}")), &ERRMSG))
            }
            _ => Ok(value as i32),
        }
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match value {
            _ if value > i32::MAX as u64 => {
                Err(E::invalid_value(Other(&format!("{value}")), &ERRMSG))
            }
            _ => Ok(value as i32),
        }
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a 32-bit integer in ProtoJSON format")
    }
}

impl serde_with::SerializeAs<i32> for I32 {
    fn serialize_as<S>(source: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(*source)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use serde_json::{Value, json};
    use serde_with::{DeserializeAs, SerializeAs};
    use test_case::test_case;

    #[test_case(0, 0)]
    #[test_case("0", 0; "zero string")]
    #[test_case(-42, -42)]
    #[test_case("-7", -7)]
    #[test_case(84, 84)]
    #[test_case("21", 21)]
    #[test_case(i32::MAX, i32::MAX; "max")]
    #[test_case(format!("{}", i32::MAX), i32::MAX; "max as string")]
    #[test_case(i32::MIN, i32::MIN; "min")]
    #[test_case(format!("{}", i32::MIN), i32::MIN; "min as string")]
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
    #[test_case(json!({}))]
    fn deser_error(input: Value) {
        let got = I32::deserialize_as(input).unwrap_err();
        assert!(got.is_data(), "{got:?}");
    }
}
