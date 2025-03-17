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

/// Implements the `google.cloud.DoubleValue` well-known type.
///
/// In early versions of the `proto3` syntax optional primitive types were
/// represented by well-known messages, with a single field, that contained the
/// value. In Rust, we represent these with `Option` of the correct type. The
/// aliases are introduced here to simplify the code generator and resolve any
/// references in code or documentation.
///
/// The JSON representation for `DoubleValue` is JSON number.
pub type DoubleValue = f64;

/// Implements the `google.cloud.FloatValue` well-known type.
///
/// In early versions of the `proto3` syntax optional primitive types were
/// represented by well-known messages, with a single field, that contained the
/// value. In Rust, we represent these with `Option` of the correct type. The
/// aliases are introduced here to simplify the code generator and resolve any
/// references in code or documentation.
///
/// The JSON representation for `FloatValue` is JSON number.
pub type FloatValue = f32;

/// Implements the `google.cloud.Int64Value` well-known type.
///
/// In early versions of the `proto3` syntax optional primitive types were
/// represented by well-known messages, with a single field, that contained the
/// value. In Rust, we represent these with `Option` of the correct type. The
/// aliases are introduced here to simplify the code generator and resolve any
/// references in code or documentation.
///
/// The JSON representation for `Int64Value` is JSON string.
pub type Int64Value = i64;

/// Implements the `google.cloud.UInt64Value` well-known type.
///
/// In early versions of the `proto3` syntax optional primitive types were
/// represented by well-known messages, with a single field, that contained the
/// value. In Rust, we represent these with `Option` of the correct type. The
/// aliases are introduced here to simplify the code generator and resolve any
/// references in code or documentation.
///
/// The JSON representation for `UInt64Value` is JSON string.
pub type UInt64Value = u64;

/// Implements the `google.cloud.Int32Value` well-known type.
///
/// In early versions of the `proto3` syntax optional primitive types were
/// represented by well-known messages, with a single field, that contained the
/// value. In Rust, we represent these with `Option` of the correct type. The
/// aliases are introduced here to simplify the code generator and resolve any
/// references in code or documentation.
///
/// The JSON representation for `Int32Value` is JSON number.
pub type Int32Value = i32;

/// Implements the `google.cloud.UInt32Value` well-known type.
///
/// In early versions of the `proto3` syntax optional primitive types were
/// represented by well-known messages, with a single field, that contained the
/// value. In Rust, we represent these with `Option` of the correct type. The
/// aliases are introduced here to simplify the code generator and resolve any
/// references in code or documentation.
///
/// The JSON representation for `UInt32Value` is JSON number.
pub type UInt32Value = u32;

/// Implements the `google.cloud.BoolValue` well-known type.
///
/// In early versions of the `proto3` syntax optional primitive types were
/// represented by well-known messages, with a single field, that contained the
/// value. In Rust, we represent these with `Option` of the correct type. The
/// aliases are introduced here to simplify the code generator and resolve any
/// references in code or documentation.
///
/// The JSON representation for `BoolValue` is JSON `true` and `false`.
pub type BoolValue = bool;

/// Implements the `google.cloud.StringValue` well-known type.
///
/// In early versions of the `proto3` syntax optional primitive types were
/// represented by well-known messages, with a single field, that contained the
/// value. In Rust, we represent these with `Option` of the correct type. The
/// aliases are introduced here to simplify the code generator and resolve any
/// references in code or documentation.
///
/// The JSON representation for `StringValue` is JSON string.
pub type StringValue = String;

/// Implements the `google.cloud.BytesValue` well-known type.
///
/// In early versions of the `proto3` syntax optional primitive types were
/// represented by well-known messages, with a single field, that contained the
/// value. In Rust, we represent these with `Option` of the correct type. The
/// aliases are introduced here to simplify the code generator and resolve any
/// references in code or documentation.
///
/// The JSON representation for `BytesValue` is JSON string.
pub type BytesValue = bytes::Bytes;

impl crate::message::Message for BoolValue {
    fn typename() -> &'static str {
        "type.googleapis.com/google.protobuf.BoolValue"
    }
    fn to_map(&self) -> Result<crate::message::Map, crate::AnyError>
    where
        Self: serde::ser::Serialize + Sized,
    {
        let map: crate::message::Map = [
            (
                "@type",
                serde_json::Value::String(Self::typename().to_string()),
            ),
            ("value", serde_json::Value::Bool(*self)),
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
            .and_then(|v| v.as_bool())
            .ok_or_else(crate::message::missing_value_field)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::Any;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn test_bool_value() -> Result {
        let input: BoolValue = true;
        let any = Any::try_from(&input)?;
        let got = serde_json::to_value(&any)?;
        let want = serde_json::json!({
            "@type": "type.googleapis.com/google.protobuf.BoolValue",
            "value": true
        });
        assert_eq!(got, want);
        let output = any.try_into_message::<BoolValue>()?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test]
    fn test_bool_value_error() -> Result {
        let input = serde_json::Value::Bool(true);
        let any = Any::try_from(&input)?;
        assert!(any.try_into_message::<BoolValue>().is_err());
        Ok(())
    }
}
