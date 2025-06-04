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

use crate::Value;
use serde::Deserialize;

pub struct OptionalValue;

impl<'de> serde_with::DeserializeAs<'de, Option<Value>> for OptionalValue {
    fn deserialize_as<D>(deserializer: D) -> Result<Option<Value>, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        Ok(Option::<Value>::deserialize(deserializer)?.or(Some(Value::Null)))
    }
}

impl serde_with::SerializeAs<Option<Value>> for OptionalValue {
    fn serialize_as<S>(source: &Option<Value>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Serialize;
        match source {
            None => serializer.serialize_none(),
            Some(v) => v.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use serde_json::{Value, json};
    use serde_with::{DeserializeAs, SerializeAs};
    use test_case::test_case;

    #[test_case(json!(null))]
    #[test_case(json!("abc"))]
    #[test_case(json!(1))]
    #[test_case(json!([1, 2, "a"]))]
    #[test_case(json!({"a": [1, 2, "a"], "b": null}))]
    fn deser_and_ser(input: Value) -> Result<()> {
        let got = OptionalValue::deserialize_as(input.clone())?;
        assert_eq!(got, Some(input));

        let serialized = OptionalValue::serialize_as(&got, serde_json::value::Serializer)?;
        assert_eq!(serialized, json!(got));
        Ok(())
    }
}
