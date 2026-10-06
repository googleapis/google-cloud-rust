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

use prost_types::value::Kind as ProtoKind;
use prost_types::{ListValue as ProtoListValue, Value as ProtoValue};
use serde_json::Number as JsonNumber;
use serde_json::Value as JsonValue;

/// Kind indicates the data type of a [`Value`].
///
/// This enum corresponds to the possible variants of a Spanner value.
/// Spanner SQL `STRUCT`s and `ARRAY`s are represented as [`Kind::List`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(
    clippy::exhaustive_enums,
    reason = "Value kinds are frozen Spanner types"
)]
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
    /// Represents an ordered list of values.
    ///
    /// Spanner query results and parameters encode SQL `STRUCT`s and `ARRAY`s
    /// as positional lists.
    List,
    /// Represents an unknown or unexpected value kind.
    Unknown,
}

impl From<&Option<ProtoKind>> for Kind {
    fn from(kind: &Option<ProtoKind>) -> Self {
        match kind {
            Some(ProtoKind::NullValue(_)) | None => Kind::Null,
            Some(ProtoKind::NumberValue(_)) => Kind::Number,
            Some(ProtoKind::StringValue(_)) => Kind::String,
            Some(ProtoKind::BoolValue(_)) => Kind::Bool,
            Some(ProtoKind::ListValue(_)) => Kind::List,
            // Spanner never returns or accepts protobuf StructValue on the wire;
            // SQL STRUCT values are represented as Kind::List. Any unexpected kind
            // defaults to Kind::Unknown.
            _ => Kind::Unknown,
        }
    }
}

impl From<Option<ProtoKind>> for Kind {
    fn from(kind: Option<ProtoKind>) -> Self {
        Kind::from(&kind)
    }
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
            kind: Some(ProtoKind::NullValue(0)),
        })
    }

    /// Safely reinterprets a reference to the inner protobuf value as a reference to Value.
    /// Logical safety is guaranteed by #[repr(transparent)].
    pub(crate) fn from_ref(proto_value: &ProtoValue) -> &Self {
        // SAFETY: Value is #[repr(transparent)] wrapper around ProtoValue.
        // This structure guarantees that Value has the exact same memory layout as ProtoValue.
        // This is the standard Rust pattern for safe zero-cost newtype references.
        unsafe { &*(proto_value as *const ProtoValue as *const Value) }
    }

    /// Returns the kind of the value.
    pub fn kind(&self) -> Kind {
        Kind::from(&self.0.kind)
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
        self.kind() == Kind::Null
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
            Some(ProtoKind::StringValue(string_value)) => Some(string_value),
            _ => None,
        }
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
            Some(ProtoKind::BoolValue(bool_value)) => Some(*bool_value),
            _ => None,
        }
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
            Some(ProtoKind::NumberValue(number_value)) => Some(*number_value),
            _ => None,
        }
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
    ///
    /// # Spanner SQL STRUCT Representation
    ///
    /// Spanner transmits SQL `STRUCT` values on the wire as positional
    /// [`List`]s accompanied by schema metadata. Both `ARRAY` and `STRUCT` columns
    /// can be inspected via this method.
    pub fn as_list(&self) -> Option<&List> {
        match &self.0.kind {
            Some(ProtoKind::ListValue(list_value)) => Some(List::from_ref(list_value)),
            _ => None,
        }
    }
    /// Consumes the value and returns the underlying `String` if the value is a string, or `None` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_spanner::value::Value;
    ///
    /// let value = Value::from("hello".to_string());
    /// assert_eq!(value.into_string(), Some("hello".to_string()));
    ///
    /// let number_value = Value::from(42.5);
    /// assert_eq!(number_value.into_string(), None);
    /// ```
    pub fn into_string(self) -> Option<String> {
        match self.0.kind {
            Some(ProtoKind::StringValue(string_value)) => Some(string_value),
            _ => None,
        }
    }

    /// Consumes the value and returns the underlying [`List`] if the value is a list, or `None` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_spanner::value::{List, Value};
    ///
    /// let value = Value::from(vec![1i64, 2i64]);
    /// assert!(value.into_list().is_some());
    ///
    /// let null_value = Value::null();
    /// assert_eq!(null_value.into_list(), None);
    /// ```
    ///
    /// # Spanner SQL STRUCT Representation
    ///
    /// Spanner transmits SQL `STRUCT` values on the wire as positional
    /// [`List`]s accompanied by schema metadata. Both `ARRAY` and `STRUCT` columns
    /// can be extracted via this method.
    pub fn into_list(self) -> Option<List> {
        match self.0.kind {
            Some(ProtoKind::ListValue(list_value)) => Some(List(list_value)),
            _ => None,
        }
    }
}

impl Value {
    /// Converts a protobuf value to a `serde_json::Value`.
    /// This is needed because the generated gapic client uses `serde_json::Value` instead of `prost_types::Value`.
    /// It is converted back from `serde_json::Value` to `prost_types::Value` before hitting the wire.
    pub(crate) fn into_serde_value(self) -> JsonValue {
        match self.0.kind {
            Some(ProtoKind::NullValue(_)) => JsonValue::Null,
            Some(ProtoKind::NumberValue(number_value)) => {
                if let Some(number) = JsonNumber::from_f64(number_value) {
                    JsonValue::Number(number)
                } else if number_value.is_nan() {
                    JsonValue::String("NaN".to_string())
                } else if number_value.is_sign_positive() {
                    JsonValue::String("Infinity".to_string())
                } else {
                    JsonValue::String("-Infinity".to_string())
                }
            }
            Some(ProtoKind::StringValue(string_value)) => JsonValue::String(string_value),
            Some(ProtoKind::BoolValue(bool_value)) => JsonValue::Bool(bool_value),
            Some(ProtoKind::ListValue(list)) => JsonValue::Array(
                list.values
                    .into_iter()
                    .map(|value| Value(value).into_serde_value())
                    .collect(),
            ),
            // Spanner never returns or accepts protobuf StructValue on the wire;
            // SQL STRUCT values are encoded as ListValue. Any unexpected kind
            // or missing kind is treated as Null.
            _ => JsonValue::Null,
        }
    }
}

/// A lightweight wrapper around a protobuf ListValue.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Default)]
pub struct List(pub(crate) ProtoListValue);

impl List {
    /// Safely reinterprets a reference to the inner protobuf list as a reference to List.
    pub(crate) fn from_ref(proto_list: &ProtoListValue) -> &Self {
        // SAFETY: List is #[repr(transparent)] wrapper around ProtoListValue.
        unsafe { &*(proto_list as *const ProtoListValue as *const List) }
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
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_spanner::value::Value;
    ///
    /// let value = Value::from(vec![1.0, 2.0]);
    /// if let Some(list) = value.as_list() {
    ///     for element in list.iter() {
    ///         println!("{element:?}");
    ///     }
    /// }
    /// ```
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Value> + ExactSizeIterator {
        self.0.values.iter().map(Value::from_ref)
    }

    /// Consumes the list and returns its elements as a vector of [`Value`]s.
    ///
    /// # Example
    ///
    /// ```
    /// use google_cloud_spanner::value::Value;
    ///
    /// let value = Value::null();
    /// if let Some(list) = value.into_list() {
    ///     let values = list.into_values();
    ///     assert!(values.is_empty());
    /// }
    /// ```
    pub fn into_values(self) -> Vec<Value> {
        self.0.values.into_iter().map(Value).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value as JsonValue;
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
            kind: Some(ProtoKind::NumberValue(f64::NAN)),
        });
        assert_eq!(
            number_nan.into_serde_value(),
            JsonValue::String("NaN".to_string()),
            "NumberValue(NaN) must serialize as 'NaN' string"
        );

        let number_infinity = Value(ProtoValue {
            kind: Some(ProtoKind::NumberValue(f64::INFINITY)),
        });
        assert_eq!(
            number_infinity.into_serde_value(),
            JsonValue::String("Infinity".to_string()),
            "NumberValue(Infinity) must serialize as 'Infinity' string"
        );

        let number_negative_infinity = Value(ProtoValue {
            kind: Some(ProtoKind::NumberValue(f64::NEG_INFINITY)),
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
            kind: Some(ProtoKind::StringValue("NaN".to_string())),
        });
        assert_eq!(nan_value.kind(), Kind::String);
        assert_eq!(nan_value.as_str(), Some("NaN"));
        assert_eq!(
            nan_value.as_f64(),
            None,
            "StringValue('NaN') is represented as Kind::String in untyped Value; typed access is via FromValue"
        );

        let infinity_value = Value(ProtoValue {
            kind: Some(ProtoKind::StringValue("Infinity".to_string())),
        });
        assert_eq!(infinity_value.kind(), Kind::String);
        assert_eq!(infinity_value.as_str(), Some("Infinity"));
        assert_eq!(
            infinity_value.as_f64(),
            None,
            "StringValue('Infinity') is represented as Kind::String in untyped Value; typed access is via FromValue"
        );

        let negative_infinity_value = Value(ProtoValue {
            kind: Some(ProtoKind::StringValue("-Infinity".to_string())),
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
            kind: Some(ProtoKind::NullValue(0)),
        });
        assert_eq!(null_value.kind(), Kind::Null);
        assert!(null_value.is_null());
        assert_eq!(null_value.as_str(), None);
        assert_eq!(null_value.as_bool(), None);
        assert_eq!(null_value.as_f64(), None);
        assert_eq!(null_value.as_list(), None);

        let string_value = Value(ProtoValue {
            kind: Some(ProtoKind::StringValue("foo".to_string())),
        });
        assert_eq!(string_value.kind(), Kind::String);
        assert!(!string_value.is_null());
        assert_eq!(string_value.as_str(), Some("foo"));
        assert_eq!(string_value.as_bool(), None);
        assert_eq!(string_value.as_f64(), None);
        assert_eq!(string_value.as_list(), None);

        let bool_value = Value(ProtoValue {
            kind: Some(ProtoKind::BoolValue(true)),
        });
        assert_eq!(bool_value.kind(), Kind::Bool);
        assert!(!bool_value.is_null());
        assert_eq!(bool_value.as_bool(), Some(true));
        assert_eq!(bool_value.as_str(), None);
        assert_eq!(bool_value.as_f64(), None);
        assert_eq!(bool_value.as_list(), None);

        let number_value = Value(ProtoValue {
            kind: Some(ProtoKind::NumberValue(42.0)),
        });
        assert_eq!(number_value.kind(), Kind::Number);
        assert!(!number_value.is_null());
        assert_eq!(number_value.as_f64(), Some(42.0));
        assert_eq!(number_value.as_str(), None);
        assert_eq!(number_value.as_bool(), None);
        assert_eq!(number_value.as_list(), None);

        let list_value = Value(ProtoValue {
            kind: Some(ProtoKind::ListValue(ProtoListValue {
                values: vec![ProtoValue {
                    kind: Some(ProtoKind::NumberValue(1.0)),
                }],
            })),
        });
        assert_eq!(list_value.kind(), Kind::List);
        assert!(!list_value.is_null());
        assert_eq!(list_value.as_str(), None);
        assert_eq!(list_value.as_bool(), None);
        assert_eq!(list_value.as_f64(), None);
        let list = list_value.as_list().expect("list must be Some");
        assert_eq!(list.len(), 1);
        assert_eq!(
            list.get(0).expect("element 0 must exist").as_f64(),
            Some(1.0)
        );
    }

    #[test]
    fn is_null() {
        assert!(Value::null().is_null());
        assert!(Value(ProtoValue { kind: None }).is_null());
        assert!(
            Value(ProtoValue {
                kind: Some(ProtoKind::NullValue(0)),
            })
            .is_null()
        );
        assert!(!Value::from("hello").is_null());
        assert!(!Value::from(true).is_null());
        assert!(!Value::from(42.5).is_null());
        assert!(!Value::from(vec![1i64]).is_null());

        let unexpected_kind = Value(ProtoValue {
            kind: Some(ProtoKind::StructValue(Default::default())),
        });
        assert_eq!(
            unexpected_kind.kind(),
            Kind::Unknown,
            "unexpected proto kind must report Kind::Unknown"
        );
        assert!(
            !unexpected_kind.is_null(),
            "unexpected proto kind must report is_null == false"
        );
    }

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(Value: Send, Sync, Clone, Debug, Default);
        static_assertions::assert_impl_all!(List: Send, Sync, Clone, Debug, Default);
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

    #[test]
    fn value_null_and_kind() {
        let null_value = Value::null();
        assert!(
            null_value.is_null(),
            "null value must report is_null == true"
        );
        assert_eq!(
            null_value.kind(),
            Kind::Null,
            "null value kind must be Kind::Null"
        );

        let bool_value = Value::from(true);
        assert!(
            !bool_value.is_null(),
            "boolean value must not report is_null == true"
        );
        assert_eq!(
            bool_value.kind(),
            Kind::Bool,
            "boolean value kind must be Kind::Bool"
        );

        let string_value = Value::from("hello");
        assert!(
            !string_value.is_null(),
            "string value must not report is_null == true"
        );
        assert_eq!(
            string_value.kind(),
            Kind::String,
            "string value kind must be Kind::String"
        );

        let number_value = Value::from(42.5);
        assert!(
            !number_value.is_null(),
            "number value must not report is_null == true"
        );
        assert_eq!(
            number_value.kind(),
            Kind::Number,
            "number value kind must be Kind::Number"
        );

        let list_value = Value::from(vec![1i64, 2i64]);
        assert!(
            !list_value.is_null(),
            "list value must not report is_null == true"
        );
        assert_eq!(
            list_value.kind(),
            Kind::List,
            "list value kind must be Kind::List"
        );
    }

    #[test]
    fn value_owned_conversions() {
        let string_value = Value::from("hello".to_string());
        assert_eq!(
            string_value.into_string(),
            Some("hello".to_string()),
            "string value into_string should succeed"
        );

        let number_value = Value::from(42.5);
        assert_eq!(
            number_value.clone().into_string(),
            None,
            "number into_string must return None"
        );
        assert_eq!(
            number_value.into_list(),
            None,
            "number into_list must return None"
        );

        let list_value = Value::from(vec![1i64, 2i64]);
        let list = list_value.into_list().expect("list should exist");
        assert_eq!(list.len(), 2, "extracted list should have 2 elements");
    }

    #[test]
    fn list_iter_and_into_values() {
        let list_value = Value::from(vec![10i64, 20i64, 30i64]);
        let list = list_value.as_list().expect("list should exist");

        assert_eq!(list.len(), 3, "list length should be 3");
        assert!(!list.is_empty(), "list should not be empty");
        assert_eq!(
            list.get(0).and_then(|val| val.as_str()),
            Some("10"),
            "first element should be '10'"
        );
        assert_eq!(
            list.get(1).and_then(|val| val.as_str()),
            Some("20"),
            "second element should be '20'"
        );
        assert_eq!(
            list.get(2).and_then(|val| val.as_str()),
            Some("30"),
            "third element should be '30'"
        );
        assert_eq!(list.get(3), None, "out of bounds index should return None");

        // Verify iter() yielding &Value
        let elements: Vec<i64> = list
            .iter()
            .filter_map(|val| {
                val.as_str()
                    .and_then(|string_slice| string_slice.parse::<i64>().ok())
            })
            .collect();
        assert_eq!(
            elements,
            vec![10, 20, 30],
            "iter() should yield all elements"
        );

        // Verify DoubleEndedIterator and ExactSizeIterator capabilities
        {
            let mut iterator = list.iter();
            assert_eq!(
                iterator.len(),
                3,
                "iterator should report exact remaining length"
            );
            assert_eq!(
                iterator.next_back().and_then(|value| value.as_str()),
                Some("30"),
                "next_back() should yield the last element"
            );
            assert_eq!(
                iterator.len(),
                2,
                "iterator should decrement remaining length after next_back()"
            );
            assert_eq!(
                iterator.next().and_then(|value| value.as_str()),
                Some("10"),
                "next() should yield the first element"
            );
            assert_eq!(
                iterator.len(),
                1,
                "iterator should decrement remaining length after next()"
            );
            assert_eq!(
                iterator.next().and_then(|value| value.as_str()),
                Some("20"),
                "next() should yield the remaining element"
            );
            assert_eq!(
                iterator.len(),
                0,
                "iterator should report length 0 when exhausted"
            );
            assert_eq!(
                iterator.next(),
                None,
                "next() should return None when exhausted"
            );
            assert_eq!(
                iterator.next_back(),
                None,
                "next_back() should return None when exhausted"
            );

            let reversed_elements: Vec<i64> = list
                .iter()
                .rev()
                .filter_map(|value| {
                    value
                        .as_str()
                        .and_then(|string_slice| string_slice.parse::<i64>().ok())
                })
                .collect();
            assert_eq!(
                reversed_elements,
                vec![30, 20, 10],
                "rev() should yield elements in reverse order"
            );
        }

        // Verify into_values()
        let owned_list = list_value.into_list().expect("owned list should exist");
        let extracted_values = owned_list.into_values();
        assert_eq!(
            extracted_values.len(),
            3,
            "into_values() should return 3 elements"
        );
        assert_eq!(
            extracted_values[0].as_str(),
            Some("10"),
            "first extracted value should match '10'"
        );

        // Verify empty List behavior
        let empty_list = List::default();
        assert!(empty_list.is_empty(), "default list must be empty");
        assert_eq!(empty_list.len(), 0, "default list len must be 0");
        assert_eq!(empty_list.iter().count(), 0, "empty list count must be 0");
        assert_eq!(
            empty_list.into_values().len(),
            0,
            "empty into_values len must be 0"
        );
    }
}
