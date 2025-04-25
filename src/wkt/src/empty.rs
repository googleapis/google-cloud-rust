// Copyright 2024 Google LLC
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

use serde::{Deserialize, Deserializer, Serialize, de::IntoDeserializer};

/// A generic empty message that you can re-use to avoid defining duplicated
/// empty messages in your APIs. A typical example is to use it as the request
/// or the response type of an API method. For instance:
///
/// ```norust
/// service Foo {
///   rpc Bar(google.protobuf.Empty) returns (google.protobuf.Empty);
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Empty {}

impl<'de> Deserialize<'de> for Empty {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer).unwrap_or(String::default());
        if input.trim().is_empty() || input.trim().eq("null") {
            return Ok(Empty::default());
        }
        let string_deserializer = String::into_deserializer(input);
        Ok(Option::<Empty>::deserialize(string_deserializer)?.unwrap())
    }
}

impl crate::message::Message for Empty {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Empty"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn serialize() -> Result {
        let empty = Empty::default();
        let got = serde_json::to_value(empty)?;
        assert_eq!(json!({}), got);
        Ok(())
    }

    #[test]
    fn deserialize() -> Result {
        let got = serde_json::from_value(json!({}))?;
        assert_eq!(Empty::default(), got);
        Ok(())
    }

    #[test]
    fn deserialize_null() -> Result {
        let got = serde_json::from_value(json!(null))?;
        assert_eq!(Empty::default(), got);
        Ok(())
    }

    #[test]
    fn deserialize_empty() -> Result {
        let got = serde_json::from_str("")?;
        assert_eq!(Empty::default(), got);
        Ok(())
    }
}
