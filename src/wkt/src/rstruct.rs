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
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NullValue;

impl crate::message::Message for Struct {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Struct"
    }

    #[allow(private_interfaces)]
    fn serializer() -> impl crate::message::MessageSerializer<Self> {
        crate::message::ValueSerializer::<Self>::new()
    }
}

impl crate::message::Message for Value {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.Value"
    }

    #[allow(private_interfaces)]
    fn serializer() -> impl crate::message::MessageSerializer<Self> {
        crate::message::ValueSerializer::<Self>::new()
    }
}

impl crate::message::Message for ListValue {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.ListValue"
    }

    #[allow(private_interfaces)]
    fn serializer() -> impl crate::message::MessageSerializer<Self> {
        crate::message::ValueSerializer::<Self>::new()
    }
}

/// Protobuf represents `NullValue` as an enum. In some contexts, it is
/// useful to make it behave as if it was.
impl NullValue {
    /// Gets the value.
    pub fn value(&self) -> i32 {
        0
    }

    /// Gets the value as a string.
    pub fn as_str_name(&self) -> std::borrow::Cow<'static, str> {
        "NULL_VALUE".into()
    }

    /// Creates a value from the value name
    pub fn from_str_name(_name: &str) -> Option<Self> {
        Some(Self)
    }
}

impl From<i32> for NullValue {
    fn from(_value: i32) -> Self {
        Self
    }
}

// This is needed when `NullValue` is used in a Protobuf-generated message.
impl From<NullValue> for i32 {
    fn from(_value: NullValue) -> Self {
        Default::default()
    }
}

impl From<NullValue> for serde_json::Value {
    fn from(_value: NullValue) -> Self {
        Default::default()
    }
}

impl From<&NullValue> for serde_json::Value {
    fn from(_value: &NullValue) -> Self {
        Default::default()
    }
}

/// Implement [`serde`](::serde) serialization for [NullValue].
impl serde::ser::Serialize for NullValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
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
        let value = Value::deserialize(deserializer)?;
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
    fn test_null_value_interface() {
        let input = NullValue;
        assert_eq!(input.value(), NullValue.value());
        assert_eq!(input.as_str_name().as_ref(), "NULL_VALUE");
        assert_eq!(NullValue::from_str_name("NULL_VALUE"), Some(NullValue));
        assert_eq!(NullValue::from(0), NullValue);
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
    fn test_i32_from_null_value() {
        let got = i32::from(NullValue);
        assert_eq!(got, 0);
    }

    #[test]
    fn test_serde_from_null_value() {
        let got = serde_json::Value::from(NullValue);
        assert_eq!(got, serde_json::Value::Null);
        let got = serde_json::Value::from(&NullValue);
        assert_eq!(got, serde_json::Value::Null);
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
