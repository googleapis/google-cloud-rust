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

macro_rules! impl_message {
    ($t: ty) => {
        impl crate::message::Message for $t {
            fn typename() -> &'static str {
                concat!("type.googleapis.com/google.protobuf.", stringify!($t))
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
                    ("value", serde_json::json!(self)),
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
                crate::message::from_value::<Self>(map)
            }
        }
    };
}

impl_message!(DoubleValue);
impl_message!(FloatValue);
impl_message!(Int32Value);
impl_message!(UInt32Value);
impl_message!(BoolValue);
impl_message!(StringValue);

#[cfg(test)]
mod test {

    use super::*;
    use crate::Any;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;
    use test_case::test_case;

    #[test_case(1234.5 as DoubleValue, "DoubleValue")]
    #[test_case(9876.5 as FloatValue, "FloatValue")]
    #[test_case(-123 as Int32Value, "Int32Value")]
    #[test_case(123 as UInt32Value, "UInt32Value")]
    #[test_case(true as BoolValue, "BoolValue")]
    #[test_case(StringValue::from("I am a string"), "StringValue")]
    fn test_wrapper_in_any<T>(input: T, typename: &str) -> Result
    where
        T: crate::message::Message
            + std::fmt::Debug
            + PartialEq
            + serde::de::DeserializeOwned
            + serde::ser::Serialize,
    {
        let any = Any::try_from(&input)?;
        let got = serde_json::to_value(&any)?;
        let want = serde_json::json!({
            "@type": format!("type.googleapis.com/google.protobuf.{}", typename),
            "value": input,
        });
        assert_eq!(got, want);
        let output = any.try_into_message::<T>()?;
        assert_eq!(output, input);
        Ok(())
    }

    #[test_case(Int32Value::default(), DoubleValue::default())]
    #[test_case(Int32Value::default(), FloatValue::default())]
    #[test_case(DoubleValue::default(), Int32Value::default())]
    #[test_case(DoubleValue::default(), UInt32Value::default())]
    #[test_case(DoubleValue::default(), BoolValue::default())]
    #[test_case(DoubleValue::default(), StringValue::default())]
    fn test_wrapper_in_any_with_bad_types<T, U>(from: T, _into: U) -> Result
    where
        T: crate::message::Message + std::fmt::Debug + serde::ser::Serialize,
        U: crate::message::Message + std::fmt::Debug + serde::de::DeserializeOwned,
    {
        let any = Any::try_from(&from)?;
        assert!(any.try_into_message::<U>().is_err());
        Ok(())
    }
}
