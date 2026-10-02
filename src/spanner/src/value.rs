// Copyright 2026 Google LLC
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

pub(crate) const SPANNER_TIMESTAMP_FORMAT: &[time::format_description::FormatItem<'static>] = time::macros::format_description!(
    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z"
);
pub(crate) const SPANNER_DATE_FORMAT: &[time::format_description::FormatItem<'static>] =
    time::macros::format_description!("[year]-[month]-[day]");

pub use crate::from_value::{ConvertError, FromValue, SharedError};
pub use crate::to_value::ToValue;
pub use crate::types::{Type, TypeCode};
pub use google_cloud_type::model::Date;
pub use wkt::{Duration, Timestamp};

use prost_types::Value as ProtoValue;
use serde_json::Number as JsonNumber;
use serde_json::Value as JsonValue;

/// Kind indicates the type of the value.
///
/// This enum maps 1-to-1 with the frozen specification of JSON/Protobuf types
/// in `google.protobuf.Value`, and is guaranteed not to grow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(clippy::exhaustive_enums, reason = "Value kinds are frozen JSON types")]
pub enum Kind {
    /// Represents a null value of any data type.
    Null,
    /// Represents a floating point value.
    Number,
    /// Represents a UTF-8 string value or encoded representations of other data types,
    /// such as base64-encoded bytes, decimals, dates, timestamps, and integers.
    String,
    /// Represents a boolean value.
    Bool,
    /// Represents a structured object containing a collection of key-value pairs.
    Struct,
    /// Represents an ordered list of values.
    List,
}

/// Value is a transparent wrapper around a protobuf value.
/// It adds helper methods for accessing the underlying value.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Value(pub(crate) ProtoValue);

impl Value {
    /// Creates a null [Value].
    pub fn null() -> Self {
        Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NullValue(0)),
        })
    }

    /// Safely reinterprets a reference to the inner protobuf value as a reference to Value.
    /// Logical safety is guaranteed by #[repr(transparent)].
    pub(crate) fn from_ref(v: &ProtoValue) -> &Self {
        // Safety: Value is #[repr(transparent)] wrapper around ProtoValue.
        // This structure guarantees that Value has the exact same memory layout as ProtoValue.
        // This is the standard Rust pattern for safe zero-cost newtype references.
        unsafe { &*(v as *const ProtoValue as *const Value) }
    }

    /// Returns the kind of the value.
    pub fn kind(&self) -> Kind {
        match &self.0.kind {
            Some(prost_types::value::Kind::NullValue(_)) => Kind::Null,
            Some(prost_types::value::Kind::NumberValue(_)) => Kind::Number,
            Some(prost_types::value::Kind::StringValue(_)) => Kind::String,
            Some(prost_types::value::Kind::BoolValue(_)) => Kind::Bool,
            Some(prost_types::value::Kind::StructValue(_)) => Kind::Struct,
            Some(prost_types::value::Kind::ListValue(_)) => Kind::List,
            None => Kind::Null,
        }
    }

    /// Returns `true` if the value is null, or `false` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_spanner::value::Value;
    ///
    /// let value = Value::null();
    /// assert!(value.is_null());
    ///
    /// let not_null = Value::from("hello");
    /// assert!(!not_null.is_null());
    /// ```
    pub fn is_null(&self) -> bool {
        matches!(
            self.0.kind,
            Some(prost_types::value::Kind::NullValue(_)) | None
        )
    }

    /// Returns the underlying string slice if the value is a string, or `None` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_spanner::value::Value;
    ///
    /// let value = Value::from("hello");
    /// assert_eq!(value.as_str(), Some("hello"));
    ///
    /// let number_value = Value::from(42.5);
    /// assert_eq!(number_value.as_str(), None);
    /// ```
    pub fn as_str(&self) -> Option<&str> {
        match &self.0.kind {
            Some(prost_types::value::Kind::StringValue(string_value)) => Some(string_value),
            _ => None,
        }
    }

    /// Returns the underlying string slice if the value is a string, or `None` otherwise.
    ///
    /// # Deprecation
    ///
    /// Use [`as_str`](Value::as_str) instead.
    #[deprecated(note = "use `as_str` instead")]
    pub fn as_string(&self) -> Option<&str> {
        self.as_str()
    }

    /// Returns the underlying string value if the kind is String.
    ///
    /// # Deprecation
    ///
    /// Use [`as_str`](Value::as_str) instead.
    #[deprecated(note = "use `as_str` instead")]
    pub fn try_as_string(&self) -> Option<&str> {
        self.as_str()
    }

    /// Returns the underlying boolean value if the value is a boolean, or `None` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_spanner::value::Value;
    ///
    /// let value = Value::from(true);
    /// assert_eq!(value.as_bool(), Some(true));
    ///
    /// let string_value = Value::from("true");
    /// assert_eq!(string_value.as_bool(), None);
    /// ```
    pub fn as_bool(&self) -> Option<bool> {
        match &self.0.kind {
            Some(prost_types::value::Kind::BoolValue(bool_value)) => Some(*bool_value),
            _ => None,
        }
    }

    /// Returns the underlying bool value if the kind is Bool.
    ///
    /// # Deprecation
    ///
    /// Use [`as_bool`](Value::as_bool) instead.
    #[deprecated(note = "use `as_bool` instead")]
    pub fn try_as_bool(&self) -> Option<bool> {
        self.as_bool()
    }

    /// Returns the underlying number value as an `f64` if the value is a number, or `None` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_spanner::value::Value;
    ///
    /// let value = Value::from(42.5);
    /// assert_eq!(value.as_f64(), Some(42.5));
    ///
    /// let string_value = Value::from("42.5");
    /// assert_eq!(string_value.as_f64(), None);
    /// ```
    ///
    /// # Non-Finite Floats
    ///
    /// Spanner encodes non-finite IEEE 754 floats (`NaN`, `Infinity`, and `-Infinity`)
    /// as string values on the wire. Consequently, `as_f64()` returns `None` for non-finite
    /// floats. To decode floating point values with full IEEE 754 support including `NaN`
    /// and infinity, use [`Row::try_get`](crate::row::Row::try_get) or [`FromValue`].
    pub fn as_f64(&self) -> Option<f64> {
        match &self.0.kind {
            Some(prost_types::value::Kind::NumberValue(number_value)) => Some(*number_value),
            _ => None,
        }
    }

    /// Returns the underlying number value if the kind is Number.
    ///
    /// # Deprecation
    ///
    /// Use [`as_f64`](Value::as_f64) instead.
    #[deprecated(note = "use `as_f64` instead")]
    pub fn try_as_f64(&self) -> Option<f64> {
        self.as_f64()
    }

    /// Returns a reference to the underlying [`Struct`] if the value is a struct, or `None` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_spanner::value::Value;
    ///
    /// let value = Value::null();
    /// assert_eq!(value.as_struct(), None);
    /// ```
    pub fn as_struct(&self) -> Option<&Struct> {
        match &self.0.kind {
            Some(prost_types::value::Kind::StructValue(struct_value)) => {
                Some(Struct::from_ref(struct_value))
            }
            _ => None,
        }
    }

    /// Returns the underlying struct value as a map of Values if the kind is Struct.
    ///
    /// # Deprecation
    ///
    /// Use [`as_struct`](Value::as_struct) instead.
    #[deprecated(note = "use `as_struct` instead")]
    pub fn try_as_struct(&self) -> Option<&Struct> {
        self.as_struct()
    }

    /// Returns a reference to the underlying [`List`] if the value is a list, or `None` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_spanner::value::Value;
    ///
    /// let value = Value::from(vec![1i64, 2i64]);
    /// assert_eq!(value.as_list().map(|list| list.len()), Some(2));
    ///
    /// let null_value = Value::null();
    /// assert_eq!(null_value.as_list(), None);
    /// ```
    pub fn as_list(&self) -> Option<&List> {
        match &self.0.kind {
            Some(prost_types::value::Kind::ListValue(list_value)) => {
                Some(List::from_ref(list_value))
            }
            _ => None,
        }
    }

    /// Returns the underlying list value as a vector of Values if the kind is List.
    ///
    /// # Deprecation
    ///
    /// Use [`as_list`](Value::as_list) instead.
    #[deprecated(note = "use `as_list` instead")]
    pub fn try_as_list(&self) -> Option<&List> {
        self.as_list()
    }
}

impl Value {
    /// Converts a `prost_types::Value` to a `serde_json::Value`.
    /// This is needed because the generated gapic client uses `serde_json::Value` instead of `prost_types::Value`.
    /// It is converted back from `serde_json::Value` to `prost_types::Value` before hitting the wire.
    pub(crate) fn into_serde_value(self) -> JsonValue {
        match self.0.kind {
            Some(prost_types::value::Kind::NullValue(_)) => JsonValue::Null,
            Some(prost_types::value::Kind::NumberValue(n)) => {
                if let Some(number) = JsonNumber::from_f64(n) {
                    JsonValue::Number(number)
                } else if n.is_nan() {
                    JsonValue::String("NaN".to_string())
                } else if n.is_sign_positive() {
                    JsonValue::String("Infinity".to_string())
                } else {
                    JsonValue::String("-Infinity".to_string())
                }
            }
            Some(prost_types::value::Kind::StringValue(s)) => JsonValue::String(s),
            Some(prost_types::value::Kind::BoolValue(b)) => JsonValue::Bool(b),
            Some(prost_types::value::Kind::StructValue(s)) => JsonValue::Object(
                s.fields
                    .into_iter()
                    .map(|(k, v)| (k, Value(v).into_serde_value()))
                    .collect(),
            ),
            Some(prost_types::value::Kind::ListValue(l)) => JsonValue::Array(
                l.values
                    .into_iter()
                    .map(|v| Value(v).into_serde_value())
                    .collect(),
            ),
            None => JsonValue::Null,
        }
    }
}

/// A lightweight wrapper around a protobuf Struct.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Struct(pub(crate) prost_types::Struct);

impl Struct {
    /// Safely reinterprets a reference to the inner protobuf struct as a reference to Struct.
    pub(crate) fn from_ref(v: &prost_types::Struct) -> &Self {
        // Safety: Struct is #[repr(transparent)] wrapper around prost_types::Struct.
        unsafe { &*(v as *const prost_types::Struct as *const Struct) }
    }

    /// Returns the value for the given key, or `None` if the key is not present.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.fields.get(key).map(Value::from_ref)
    }

    /// Returns the number of fields in the struct.
    pub fn len(&self) -> usize {
        self.0.fields.len()
    }

    /// Returns `true` if the struct has no fields.
    pub fn is_empty(&self) -> bool {
        self.0.fields.is_empty()
    }

    /// Returns an iterator over the fields of the struct.
    pub fn fields(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.0.fields.iter().map(|(k, v)| (k, Value::from_ref(v)))
    }
}

/// A lightweight wrapper around a protobuf ListValue.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Default)]
pub struct List(pub(crate) prost_types::ListValue);

impl List {
    /// Safely reinterprets a reference to the inner protobuf list as a reference to List.
    pub(crate) fn from_ref(v: &prost_types::ListValue) -> &Self {
        // Safety: List is #[repr(transparent)] wrapper around prost_types::ListValue.
        unsafe { &*(v as *const prost_types::ListValue as *const List) }
    }

    /// Returns the value at the given index, or `None` if the index is out of bounds.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.0.values.get(index).map(Value::from_ref)
    }

    /// Returns the number of values in the list.
    pub fn len(&self) -> usize {
        self.0.values.len()
    }

    /// Returns `true` if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.0.values.is_empty()
    }

    /// Returns an iterator over the values in the list.
    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.0.values.iter().map(Value::from_ref)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map as JsonMap;
    use serde_json::Value as JsonValue;
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::hash::Hash;

    #[test]
    fn into_serde_value_non_finite_floats() {
        assert_eq!(
            Value::from(f64::NAN).into_serde_value(),
            JsonValue::String("NaN".to_string()),
            "f64::NAN must serialize as 'NaN' string"
        );
        assert_eq!(
            Value::from(f64::INFINITY).into_serde_value(),
            JsonValue::String("Infinity".to_string()),
            "f64::INFINITY must serialize as 'Infinity' string"
        );
        assert_eq!(
            Value::from(f64::NEG_INFINITY).into_serde_value(),
            JsonValue::String("-Infinity".to_string()),
            "f64::NEG_INFINITY must serialize as '-Infinity' string"
        );
        assert_eq!(
            Value::from(f32::NAN).into_serde_value(),
            JsonValue::String("NaN".to_string()),
            "f32::NAN must serialize as 'NaN' string"
        );
        assert_eq!(
            Value::from(f32::INFINITY).into_serde_value(),
            JsonValue::String("Infinity".to_string()),
            "f32::INFINITY must serialize as 'Infinity' string"
        );
        assert_eq!(
            Value::from(f32::NEG_INFINITY).into_serde_value(),
            JsonValue::String("-Infinity".to_string()),
            "f32::NEG_INFINITY must serialize as '-Infinity' string"
        );
    }

    #[test]
    fn into_serde_value_kinds() {
        // NullValue
        assert_eq!(
            Value::null().into_serde_value(),
            JsonValue::Null,
            "NullValue must serialize as JsonValue::Null"
        );

        // NumberValue with NaN / Infinity / -Infinity (defensive fallback when wire kind is NumberValue)
        let number_nan = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NumberValue(f64::NAN)),
        });
        assert_eq!(
            number_nan.into_serde_value(),
            JsonValue::String("NaN".to_string()),
            "NumberValue(NaN) must serialize as 'NaN' string"
        );

        let number_infinity = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NumberValue(f64::INFINITY)),
        });
        assert_eq!(
            number_infinity.into_serde_value(),
            JsonValue::String("Infinity".to_string()),
            "NumberValue(Infinity) must serialize as 'Infinity' string"
        );

        let number_negative_infinity = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NumberValue(f64::NEG_INFINITY)),
        });
        assert_eq!(
            number_negative_infinity.into_serde_value(),
            JsonValue::String("-Infinity".to_string()),
            "NumberValue(-Infinity) must serialize as '-Infinity' string"
        );

        // BoolValue
        assert_eq!(
            Value::from(true).into_serde_value(),
            JsonValue::Bool(true),
            "BoolValue must serialize as JsonValue::Bool"
        );

        // StructValue
        let mut fields = BTreeMap::new();
        fields.insert(
            "field_name".to_string(),
            ProtoValue {
                kind: Some(prost_types::value::Kind::StringValue(
                    "field_value".to_string(),
                )),
            },
        );
        let struct_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
                fields,
            })),
        });
        let mut expected_map = JsonMap::new();
        expected_map.insert(
            "field_name".to_string(),
            JsonValue::String("field_value".to_string()),
        );
        assert_eq!(
            struct_value.into_serde_value(),
            JsonValue::Object(expected_map),
            "StructValue must serialize as JsonValue::Object"
        );

        // None
        let empty_value = Value(ProtoValue { kind: None });
        assert_eq!(
            empty_value.into_serde_value(),
            JsonValue::Null,
            "None kind must serialize as JsonValue::Null"
        );
    }

    #[test]
    fn into_serde_value_float_arrays() {
        let f64_array = Value::from(vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 42.0]);
        assert_eq!(
            f64_array.into_serde_value(),
            JsonValue::Array(vec![
                JsonValue::String("NaN".to_string()),
                JsonValue::String("Infinity".to_string()),
                JsonValue::String("-Infinity".to_string()),
                JsonValue::Number(serde_json::Number::from_f64(42.0).expect("valid f64 number")),
            ]),
            "f64 array must serialize non-finite elements as strings and finite as numbers"
        );

        let f32_array = Value::from(vec![f32::NAN, f32::INFINITY, f32::NEG_INFINITY, 1.5f32]);
        assert_eq!(
            f32_array.into_serde_value(),
            JsonValue::Array(vec![
                JsonValue::String("NaN".to_string()),
                JsonValue::String("Infinity".to_string()),
                JsonValue::String("-Infinity".to_string()),
                JsonValue::Number(serde_json::Number::from_f64(1.5).expect("valid f32 number")),
            ]),
            "f32 array must serialize non-finite elements as strings and finite as numbers"
        );
    }

    #[test]
    fn value_non_finite_floats_wire_representation() {
        let nan_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue("NaN".to_string())),
        });
        assert_eq!(nan_value.kind(), Kind::String);
        assert_eq!(nan_value.as_str(), Some("NaN"));
        assert_eq!(
            nan_value.as_f64(),
            None,
            "StringValue('NaN') is represented as Kind::String in untyped Value; typed access is via FromValue"
        );

        let infinity_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(
                "Infinity".to_string(),
            )),
        });
        assert_eq!(infinity_value.kind(), Kind::String);
        assert_eq!(infinity_value.as_str(), Some("Infinity"));
        assert_eq!(
            infinity_value.as_f64(),
            None,
            "StringValue('Infinity') is represented as Kind::String in untyped Value; typed access is via FromValue"
        );

        let negative_infinity_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue(
                "-Infinity".to_string(),
            )),
        });
        assert_eq!(negative_infinity_value.kind(), Kind::String);
        assert_eq!(negative_infinity_value.as_str(), Some("-Infinity"));
        assert_eq!(
            negative_infinity_value.as_f64(),
            None,
            "StringValue('-Infinity') is represented as Kind::String in untyped Value; typed access is via FromValue"
        );
    }

    #[test]
    fn value_kind_and_accessors() {
        let null_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NullValue(0)),
        });
        assert_eq!(null_value.kind(), Kind::Null);
        assert!(null_value.is_null());
        assert_eq!(null_value.as_str(), None);
        assert_eq!(null_value.as_bool(), None);
        assert_eq!(null_value.as_f64(), None);
        assert_eq!(null_value.as_struct(), None);
        assert_eq!(null_value.as_list(), None);

        let string_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue("foo".to_string())),
        });
        assert_eq!(string_value.kind(), Kind::String);
        assert!(!string_value.is_null());
        assert_eq!(string_value.as_str(), Some("foo"));
        assert_eq!(string_value.as_bool(), None);
        assert_eq!(string_value.as_f64(), None);
        assert_eq!(string_value.as_struct(), None);
        assert_eq!(string_value.as_list(), None);

        let bool_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::BoolValue(true)),
        });
        assert_eq!(bool_value.kind(), Kind::Bool);
        assert!(!bool_value.is_null());
        assert_eq!(bool_value.as_bool(), Some(true));
        assert_eq!(bool_value.as_str(), None);
        assert_eq!(bool_value.as_f64(), None);
        assert_eq!(bool_value.as_struct(), None);
        assert_eq!(bool_value.as_list(), None);

        let number_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NumberValue(42.0)),
        });
        assert_eq!(number_value.kind(), Kind::Number);
        assert!(!number_value.is_null());
        assert_eq!(number_value.as_f64(), Some(42.0));
        assert_eq!(number_value.as_str(), None);
        assert_eq!(number_value.as_bool(), None);
        assert_eq!(number_value.as_struct(), None);
        assert_eq!(number_value.as_list(), None);

        let list_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![ProtoValue {
                        kind: Some(prost_types::value::Kind::NumberValue(1.0)),
                    }],
                },
            )),
        });
        assert_eq!(list_value.kind(), Kind::List);
        assert!(!list_value.is_null());
        assert_eq!(list_value.as_str(), None);
        assert_eq!(list_value.as_bool(), None);
        assert_eq!(list_value.as_f64(), None);
        assert_eq!(list_value.as_struct(), None);
        let list = list_value.as_list().expect("list must be Some");
        assert_eq!(list.len(), 1);
        assert_eq!(
            list.get(0).expect("element 0 must exist").as_f64(),
            Some(1.0)
        );

        let struct_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
                fields: BTreeMap::from([(
                    "a".to_string(),
                    ProtoValue {
                        kind: Some(prost_types::value::Kind::NumberValue(1.0)),
                    },
                )]),
            })),
        });
        assert_eq!(struct_value.kind(), Kind::Struct);
        assert!(!struct_value.is_null());
        assert_eq!(struct_value.as_str(), None);
        assert_eq!(struct_value.as_bool(), None);
        assert_eq!(struct_value.as_f64(), None);
        assert_eq!(struct_value.as_list(), None);
        let map = struct_value.as_struct().expect("struct must be Some");
        assert_eq!(map.len(), 1);
        assert_eq!(
            map.get("a").expect("field 'a' must exist").as_f64(),
            Some(1.0)
        );
    }

    #[test]
    fn is_null() {
        assert!(Value::null().is_null());
        assert!(Value(ProtoValue { kind: None }).is_null());
        assert!(
            Value(ProtoValue {
                kind: Some(prost_types::value::Kind::NullValue(0)),
            })
            .is_null()
        );
        assert!(!Value::from("hello").is_null());
        assert!(!Value::from(true).is_null());
        assert!(!Value::from(42.5).is_null());
    }

    #[test]
    #[allow(deprecated)]
    fn deprecated_try_as_accessors_match_as_accessors() {
        let string_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue("test".to_string())),
        });
        assert_eq!(string_value.try_as_string(), string_value.as_str());
        assert_eq!(string_value.as_string(), string_value.as_str());

        let bool_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::BoolValue(false)),
        });
        assert_eq!(bool_value.try_as_bool(), bool_value.as_bool());

        let number_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NumberValue(42.5)),
        });
        assert_eq!(number_value.try_as_f64(), number_value.as_f64());

        let list_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue::default(),
            )),
        });
        assert_eq!(list_value.try_as_list(), list_value.as_list());

        let struct_value = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StructValue(
                prost_types::Struct::default(),
            )),
        });
        assert_eq!(struct_value.try_as_struct(), struct_value.as_struct());
    }

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(Value: Send, Sync, Clone, Debug);
        static_assertions::assert_impl_all!(Struct: Send, Sync, Clone, Debug);
        static_assertions::assert_impl_all!(List: Send, Sync, Clone, Debug);
        static_assertions::assert_impl_all!(
            Kind: Send,
            Sync,
            Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
            Hash
        );
    }
}
