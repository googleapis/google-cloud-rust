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

/// A generic empty message that you can re-use to avoid defining duplicated
/// empty messages in your APIs. A typical example is to use it as the request
/// or the response type of an API method. For instance:
///
/// ```norust
/// service Foo {
///   rpc Bar(google.protobuf.Empty) returns (google.protobuf.Empty);
/// }
/// ```
///
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Empty {}

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
}
