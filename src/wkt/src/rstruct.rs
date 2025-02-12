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

/// Protobuf (and consequently the Google Cloud APIs) use `Struct` to represent
/// JSON objects. We need a type that might be referenced from the generated
/// code.
pub type Struct = serde_json::Map<String, serde_json::Value>;

/// Protobuf (and consequently the Google Cloud APIs) use `Value` to represent
/// JSON values. We need a type that might be referenced from the generated
/// code.
pub type Value = serde_json::Value;

/// Protobuf (and consequently the Google Cloud APIs) use `ListValue` to
/// represent a list of JSON values. We need a type that might be referenced
/// from the generated code.
pub type ListValue = Vec<serde_json::Value>;

impl crate::message::Message for Struct {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Struct"
    }
    fn to_map(&self) -> Result<crate::message::Map, crate::AnyError>
    where
        Self: serde::ser::Serialize + Sized,
    {
        let map: crate::message::Map = [
            ("@type", Value::String(Self::typename().to_string())),
            ("value", Value::Object(self.clone())),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        Ok(map)
    }
    fn from_map(map: &crate::message::Map) -> Result<Self, crate::AnyError>
    where
        Self: serde::de::DeserializeOwned,
    {
        map.get("value")
            .and_then(|v| v.as_object())
            .cloned()
            .ok_or_else(crate::message::missing_value_field)
    }
}

impl crate::message::Message for Value {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Value"
    }
    fn to_map(&self) -> Result<crate::message::Map, crate::AnyError>
    where
        Self: serde::ser::Serialize + Sized,
    {
        let map: crate::message::Map = [
            ("@type", Value::String(Self::typename().to_string())),
            ("value", self.clone()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        Ok(map)
    }
    fn from_map(map: &crate::message::Map) -> Result<Self, crate::AnyError>
    where
        Self: serde::de::DeserializeOwned,
    {
        map.get("value")
            .cloned()
            .ok_or_else(crate::message::missing_value_field)
    }
}

impl crate::message::Message for ListValue {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.ListValue"
    }
    fn to_map(&self) -> Result<crate::message::Map, crate::AnyError>
    where
        Self: serde::ser::Serialize + Sized,
    {
        let map: crate::message::Map = [
            ("@type", Value::String(Self::typename().to_string())),
            ("value", Value::Array(self.clone())),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        Ok(map)
    }
    fn from_map(map: &crate::message::Map) -> Result<Self, crate::AnyError>
    where
        Self: serde::de::DeserializeOwned,
    {
        map.get("value")
            .and_then(|v| v.as_array())
            .cloned()
            .ok_or_else(crate::message::missing_value_field)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Any;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn test_null_value() -> Result {
        let input = Value::Null;
        let any = Any::try_from(&input)?;
        let got = serde_json::to_value(&any)?;
        let want = serde_json::json!({
            "@type": "type.googleapis.com/google.protobuf.Value",
            "value": null
        });
        assert_eq!(got, want);
        let output = any.try_into_message::<Value>()?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test]
    fn test_bool_value() -> Result {
        let input = Value::Bool(true);
        let any = Any::try_from(&input)?;
        let got = serde_json::to_value(&any)?;
        let want = serde_json::json!({
            "@type": "type.googleapis.com/google.protobuf.Value",
            "value": true
        });
        assert_eq!(got, want);
        let output = any.try_into_message::<Value>()?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test]
    fn test_number_value() -> Result {
        let input = serde_json::json!(1234.5);
        let any = Any::try_from(&input)?;
        let got = serde_json::to_value(&any)?;
        let want = serde_json::json!({
            "@type": "type.googleapis.com/google.protobuf.Value",
            "value": 1234.5
        });
        assert_eq!(got, want);
        let output = any.try_into_message::<Value>()?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test]
    fn test_string_value() -> Result {
        let input = Value::String(String::from("abc123"));
        let any = Any::try_from(&input)?;
        let got = serde_json::to_value(&any)?;
        let want = serde_json::json!({
            "@type": "type.googleapis.com/google.protobuf.Value",
            "value": "abc123"
        });
        assert_eq!(got, want);
        let output = any.try_into_message::<Value>()?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test]
    fn test_struct_in_value() -> Result {
        let structz = serde_json::json!({
            "fieldA": "123",
            "fieldB": {
                "fieldC": ["a", "b", "c"]
            }
        })
        .as_object()
        .cloned()
        .unwrap();

        let input = Value::Object(structz);
        let any = Any::try_from(&input)?;
        let got = serde_json::to_value(&any)?;
        let want = serde_json::json!({
            "@type": "type.googleapis.com/google.protobuf.Value",
            "value": {
                "fieldA": "123",
                "fieldB": {
                    "fieldC": ["a", "b", "c"]
                }
            }
        });
        assert_eq!(got, want);
        let output = any.try_into_message::<Value>()?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test]
    fn test_list_value() -> Result {
        let input = serde_json::json!([1, 2, 3, 4, "abc"])
            .as_array()
            .cloned()
            .unwrap();
        let any = Any::try_from(&input)?;
        let got = serde_json::to_value(&any)?;
        let want = serde_json::json!({
            "@type": "type.googleapis.com/google.protobuf.ListValue",
            "value": [1, 2, 3, 4, "abc"],
        });
        assert_eq!(got, want);
        let output = any.try_into_message::<ListValue>()?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test]
    fn test_struct() -> Result {
        let input = serde_json::json!({
            "fieldA": "a_value",
            "fieldB": {
                "fieldC": [1, 2, 3, 4, "abc"],
            },
        })
        .as_object()
        .cloned()
        .unwrap();
        let any = Any::try_from(&input)?;
        let got = serde_json::to_value(&any)?;
        let want = serde_json::json!({
            "@type": "type.googleapis.com/google.protobuf.Struct",
            "value": {
                "fieldA": "a_value",
                "fieldB": {
                    "fieldC": [1, 2, 3, 4, "abc"],
                },
            },
        });
        assert_eq!(got, want);
        let output = any.try_into_message::<Struct>()?;
        assert_eq!(output, input);
        Ok(())
    }
}
