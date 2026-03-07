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

use prost_types::Value as ProtoValue;

/// Kind indicates the type of the value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Null,
    Number,
    String,
    Bool,
    Struct,
    List,
}

/// Value is a transparent wrapper around a protobuf value.
/// It adds helper methods for accessing the underlying value.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Value(pub(crate) ProtoValue);

impl Value {
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

    /// Returns the underlying string value if the kind is String.
    pub fn try_as_string(&self) -> Option<&str> {
        match &self.0.kind {
            Some(prost_types::value::Kind::StringValue(s)) => Some(s),
            _ => None,
        }
    }

    /// Returns the underlying string value. Panics if the kind is not String.
    pub fn as_string(&self) -> &str {
        self.try_as_string().expect("value is not a String")
    }

    /// Returns the underlying bool value if the kind is Bool.
    pub fn try_as_bool(&self) -> Option<bool> {
        match &self.0.kind {
            Some(prost_types::value::Kind::BoolValue(b)) => Some(*b),
            _ => None,
        }
    }

    /// Returns the underlying bool value. Panics if the kind is not Bool.
    pub fn as_bool(&self) -> bool {
        self.try_as_bool().expect("value is not a Bool")
    }

    /// Returns the underlying number value if the kind is Number.
    pub fn try_as_f64(&self) -> Option<f64> {
        match &self.0.kind {
            Some(prost_types::value::Kind::NumberValue(n)) => Some(*n),
            _ => None,
        }
    }

    /// Returns the underlying number value. Panics if the kind is not Number.
    pub fn as_f64(&self) -> f64 {
        self.try_as_f64().expect("value is not a Number")
    }

    /// Returns the underlying struct value as a map of Values if the kind is Struct.
    pub fn try_as_struct(&self) -> Option<&Struct> {
        match &self.0.kind {
            Some(prost_types::value::Kind::StructValue(s)) => Some(Struct::from_ref(s)),
            _ => None,
        }
    }

    /// Returns the underlying struct value. Panics if the kind is not Struct.
    pub fn as_struct(&self) -> &Struct {
        self.try_as_struct().expect("value is not a Struct")
    }

    /// Returns the underlying list value as a vector of Values if the kind is List.
    pub fn try_as_list(&self) -> Option<&List> {
        match &self.0.kind {
            Some(prost_types::value::Kind::ListValue(l)) => Some(List::from_ref(l)),
            _ => None,
        }
    }

    /// Returns the underlying list value. Panics if the kind is not List.
    pub fn as_list(&self) -> &List {
        self.try_as_list().expect("value is not a List")
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

    #[test]
    fn test_value_kind_and_accessors() {
        let v_null = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NullValue(0)),
        });
        assert_eq!(v_null.kind(), Kind::Null);
        assert!(v_null.try_as_string().is_none());

        let v_string = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StringValue("foo".to_string())),
        });
        assert_eq!(v_string.kind(), Kind::String);
        assert_eq!(v_string.try_as_string(), Some("foo"));
        assert_eq!(v_string.as_string(), "foo");
        assert!(v_string.try_as_bool().is_none());

        let v_bool = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::BoolValue(true)),
        });
        assert_eq!(v_bool.kind(), Kind::Bool);
        assert_eq!(v_bool.try_as_bool(), Some(true));
        assert!(v_bool.as_bool());

        let v_number = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::NumberValue(42.0)),
        });
        assert_eq!(v_number.kind(), Kind::Number);
        assert_eq!(v_number.try_as_f64(), Some(42.0));
        assert_eq!(v_number.as_f64(), 42.0);

        let v_list = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::ListValue(
                prost_types::ListValue {
                    values: vec![ProtoValue {
                        kind: Some(prost_types::value::Kind::NumberValue(1.0)),
                    }],
                },
            )),
        });
        assert_eq!(v_list.kind(), Kind::List);
        let list = v_list.try_as_list().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list.get(0).unwrap().try_as_f64(), Some(1.0));
        assert_eq!(v_list.as_list().len(), 1);

        let v_struct = Value(ProtoValue {
            kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct {
                fields: std::collections::BTreeMap::from([(
                    "a".to_string(),
                    ProtoValue {
                        kind: Some(prost_types::value::Kind::NumberValue(1.0)),
                    },
                )]),
            })),
        });
        assert_eq!(v_struct.kind(), Kind::Struct);
        let map = v_struct.try_as_struct().unwrap();
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("a").unwrap().try_as_f64(), Some(1.0));
        assert_eq!(v_struct.as_struct().len(), 1);
    }

    #[test]
    fn test_auto_traits() {
        static_assertions::assert_impl_all!(Value: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(Struct: Send, Sync, Clone, std::fmt::Debug);
        static_assertions::assert_impl_all!(List: Send, Sync, Clone, std::fmt::Debug);
    }
}
