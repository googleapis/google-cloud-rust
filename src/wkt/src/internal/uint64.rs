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

//! Implement custom serializers for `u64`.
//!
//! In ProtoJSON 64-bit integers can be serialized as either strings or numbers.

use serde::de::Unexpected::Other;

pub struct U64;

impl<'de> serde_with::DeserializeAs<'de, u64> for U64 {
    fn deserialize_as<D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(U64Visitor)
    }
}

visitor_64!(U64Visitor, u64, "a 64-bit unsigned integer");

impl serde_with::SerializeAs<u64> for U64 {
    fn serialize_as<S>(source: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(source)
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
    #[test_case("5e4", 50_000)]
    #[test_case(84, 84)]
    #[test_case(168.0, 168)]
    #[test_case("21", 21)]
    #[test_case(u64::MAX, u64::MAX; "max")]
    #[test_case(u64::MAX as f64, u64::MAX; "max as f64")]
    #[test_case(format!("{}", u64::MAX), u64::MAX; "max as string")]
    #[test_case(format!("{}.0", u64::MAX), u64::MAX; "max as f64 string")]
    #[test_case(u64::MIN, u64::MIN; "min")]
    #[test_case(u64::MIN as f64, u64::MIN; "min as f64")]
    #[test_case(format!("{}", u64::MIN), u64::MIN; "min as string")]
    #[test_case(format!("{}.0", u64::MIN), u64::MIN; "min as f64 string")]
    // Not quite a roundtrip test because we always serialize as strings.
    fn deser_and_ser<T: serde::Serialize>(input: T, want: u64) -> Result<()> {
        let got = U64::deserialize_as(json!(input))?;
        assert_eq!(got, want);

        let serialized = U64::serialize_as(&got, serde_json::value::Serializer)?;
        assert_eq!(serialized, json!(got.to_string()));
        Ok(())
    }

    #[test_case(json!(u64::MAX as f64 * 2.0))]
    #[test_case(json!(format!("{}", u64::MAX as f64 * 2.0)))]
    #[test_case(json!(format!("{}", u64::MAX as i128 + 1)); "MAX+1 as string")]
    #[test_case(json!(format!("{}", u64::MIN as i128 - 1)); "MIN-1 as string")]
    #[test_case(json!("abc"))]
    #[test_case(json!(123.4))]
    #[test_case(json!("234.5"))]
    #[test_case(json!({}))]
    fn deser_error(input: Value) {
        let got = U64::deserialize_as(input).unwrap_err();
        assert!(got.is_data(), "{got:?}");
    }
}
