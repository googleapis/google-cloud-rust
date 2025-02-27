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

//! Implement the well-known types for structured, yet dynamically typed
//! messages. These types are a representation of JSON objects, lists, and
//! values as Protobuf messages. We have taken some (allowed) liberty in their
//! representation for Rust. We map them directly to the [serde_json] types,
//! except for `NullValue` where there is no corresponding type in serde.
//!
//! Services specified using Protobuf files may use `google.protobuf.Struct`,
//! `google.protobuf.Value`, `google.protobuf.ListValue`, and/or
//! `google.protobuf.NullValue` as part of their interface specification.

/// Protobuf (and consequently the Google Cloud APIs) use `Struct` to represent
/// JSON objects. We need a type that can be referenced from the generated code.
pub type Struct = serde_json::Map<String, serde_json::Value>;

/// Protobuf (and consequently the Google Cloud APIs) use `Value` to represent
/// JSON values. We need a type that can be referenced from the generated code.
pub type Value = serde_json::Value;

/// Protobuf (and consequently the Google Cloud APIs) use `ListValue` to
/// represent a list of JSON values. We need a type that can be referenced
/// from the generated code.
pub type ListValue = Vec<serde_json::Value>;

/// A message representing the `null` value. We need a type that can be
/// referenced from the generated code.
#[derive(Clone, Debug, PartialEq)]
pub struct NullValue;

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

impl crate::message::Message for NullValue {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Value"
    }
    fn to_map(&self) -> Result<crate::message::Map, crate::AnyError> {
        Value::from(self).to_map()
    }
    fn from_map(map: &crate::message::Map) -> Result<Self, crate::AnyError> {
        let value = Value::from_map(map)?;
        if !value.is_null() {
            return Err(crate::AnyError::deser("expected null value"));
        }
        Ok(Self)
    }
}

impl std::convert::From<NullValue> for serde_json::Value {
    fn from(_value: NullValue) -> Self {
        serde_json::Value::Null
    }
}

impl std::convert::From<&NullValue> for serde_json::Value {
    fn from(_value: &NullValue) -> Self {
        serde_json::Value::Null
    }
}

/// Implement [`serde`](::serde) serialization for [NullValue].
impl serde::ser::Serialize for NullValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serde_json::Value::Null.serialize(serializer)
    }
}

/// Implement [`serde`](::serde) deserialization for [NullValue].
impl<'de> serde::de::Deserialize<'de> for NullValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.is_null() {
            return Ok(NullValue);
        }
        Err(serde::de::Error::invalid_value(
            serde::de::Unexpected::Other(&value.to_string()),
            &"a null JSON object",
        ))
    }
}

// Verify the different value types work with `crate::Any`.
#[cfg(test)]
mod any_tests {
    use super::*;
    use crate::Any;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn test_wkt_null_value() -> Result {
        let input = NullValue;
        let any = Any::try_from(&input)?;
        let got = serde_json::to_value(&any)?;
        let want = serde_json::json!({
            "@type": "type.googleapis.com/google.protobuf.Value",
            "value": null
        });
        assert_eq!(got, want);
        let output = any.try_into_message::<NullValue>()?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test]
    fn test_wkt_null_value_bad_deser() -> Result {
        let input = serde_json::json!("a string");
        let any = Any::try_from(&input)?;
        let output = any.try_into_message::<NullValue>();
        assert!(output.is_err(), "{output:?}");
        Ok(())
    }

    #[test]
    fn test_serde_null_value() -> Result {
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

#[cfg(test)]
mod null_value_tests {
    use super::*;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn test_wkt_null_value_to_value() {
        let input = NullValue;
        let value = Value::from(input);
        assert!(value.is_null(), "{value:?}");

        let input = &NullValue;
        let value = Value::from(input);
        assert!(value.is_null(), "{value:?}");
    }

    #[test]
    fn test_null_value_serialize() -> Result {
        let input = NullValue;
        let got = serde_json::to_string(&input)?;
        assert_eq!(got, "null");
        Ok(())
    }

    #[test]
    fn test_null_value_deserialize() -> Result {
        let input = "null";
        let got = serde_json::from_str::<NullValue>(input)?;
        assert_eq!(got, NullValue);

        let input = "123";
        let got = serde_json::from_str::<NullValue>(input);
        assert!(got.is_err(), "{got:?}");

        let input = "\"123";
        let got = serde_json::from_str::<NullValue>(input);
        assert!(got.is_err(), "{got:?}");
        Ok(())
    }
}
