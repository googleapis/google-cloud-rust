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

//! Datastore property values, value kinds, geographic points, and conversion traits.

use crate::entity::Entity;
use crate::error::ValueError;
use crate::key::{Key, PartitionId};
use bytes::Bytes;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

/// A geographic point represented by latitude and longitude in degrees (`google.type.LatLng`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct GeoPoint {
    /// Latitude in degrees (`[-90.0, 90.0]`).
    pub latitude: f64,
    /// Longitude in degrees (`[-180.0, 180.0]`).
    pub longitude: f64,
}

impl GeoPoint {
    /// Creates a new [GeoPoint] from `latitude` and `longitude` in degrees.
    pub const fn new(latitude: f64, longitude: f64) -> Self {
        Self {
            latitude,
            longitude,
        }
    }
}

/// The typed payload of a Datastore [Value], covering all 11 supported Datastore value types.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ValueKind {
    /// An explicit null value (`google.protobuf.NullValue`).
    Null,
    /// A boolean value (`bool`).
    Boolean(bool),
    /// A 64-bit signed integer value (`int64`).
    Integer(i64),
    /// A 64-bit IEEE 754 floating-point value (`double`).
    Double(f64),
    /// A UTC timestamp (`google.protobuf.Timestamp`, stored at microsecond precision).
    Timestamp(wkt::Timestamp),
    /// A reference to a complete Datastore [Key].
    Key(Key),
    /// A UTF-8 string value.
    String(String),
    /// A raw byte array (`bytes`).
    Bytes(Bytes),
    /// A geographic coordinate (`google.type.LatLng`).
    GeoPoint(GeoPoint),
    /// An embedded Datastore [Entity].
    Entity(Entity),
    /// An ordered list of [Value]s (`google.datastore.v1.ArrayValue`).
    ///
    /// Note: Datastore prohibits directly nested arrays.
    Array(Vec<Value>),
}

impl ValueKind {
    /// Returns a human-readable name for this value kind (used in error diagnostics).
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "Null",
            Self::Boolean(_) => "Boolean",
            Self::Integer(_) => "Integer",
            Self::Double(_) => "Double",
            Self::Timestamp(_) => "Timestamp",
            Self::Key(_) => "Key",
            Self::String(_) => "String",
            Self::Bytes(_) => "Bytes",
            Self::GeoPoint(_) => "GeoPoint",
            Self::Entity(_) => "Entity",
            Self::Array(_) => "Array",
        }
    }
}

/// A Datastore property value along with its indexing and meaning metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    pub(crate) kind: ValueKind,
    pub(crate) exclude_from_indexes: bool,
    pub(crate) meaning: i32,
}

impl Value {
    /// Creates an indexed [Value] from anything implementing [IntoValue].
    pub fn new(value: impl IntoValue) -> Self {
        value.into_value()
    }

    /// Creates a null [Value].
    pub const fn null() -> Self {
        Self {
            kind: ValueKind::Null,
            exclude_from_indexes: false,
            meaning: 0,
        }
    }

    /// Creates a [Value] directly from a [ValueKind].
    pub const fn from_kind(kind: ValueKind) -> Self {
        Self {
            kind,
            exclude_from_indexes: false,
            meaning: 0,
        }
    }

    /// Returns a reference to the underlying [ValueKind].
    pub fn kind(&self) -> &ValueKind {
        &self.kind
    }

    /// Consumes `self`, returning the underlying [ValueKind].
    pub fn into_kind(self) -> ValueKind {
        self.kind
    }

    /// Returns `true` if this value is [ValueKind::Null].
    pub fn is_null(&self) -> bool {
        matches!(self.kind, ValueKind::Null)
    }

    /// Returns `true` if this property value is excluded from Datastore indexes.
    pub fn exclude_from_indexes(&self) -> bool {
        self.exclude_from_indexes
    }

    /// Sets whether this property value is excluded from Datastore indexes.
    ///
    /// When `exclude` is `true` on a [ValueKind::Array], the flag is automatically
    /// propagated to each element of the array when serialized to the wire.
    pub fn with_exclude_from_indexes(mut self, exclude: bool) -> Self {
        self.exclude_from_indexes = exclude;
        if let ValueKind::Array(ref mut elems) = self.kind {
            for elem in elems {
                elem.exclude_from_indexes = exclude;
            }
        }
        self
    }

    /// Returns the internal Datastore `meaning` code (or `0` if unset).
    pub fn meaning(&self) -> i32 {
        self.meaning
    }

    /// Sets the internal Datastore `meaning` code.
    pub fn with_meaning(mut self, meaning: i32) -> Self {
        self.meaning = meaning;
        self
    }

    /// Converts this [Value] into its wire `crate::model::Value` representation,
    /// normalizing embedded keys against `default_partition`, enforcing the
    /// non-nested array invariant, and propagating `exclude_from_indexes` onto
    /// array elements rather than the outer `ArrayValue` wrapper.
    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> Result<crate::model::Value, ValueError> {
        use crate::model::value::ValueType;

        if let ValueKind::Array(elems) = &self.kind {
            let mut proto_elems = Vec::with_capacity(elems.len());
            for elem in elems {
                if matches!(elem.kind, ValueKind::Array(_)) {
                    return Err(ValueError::NestedArray);
                }
                let mut proto_elem = elem.to_proto(default_partition)?;
                if self.exclude_from_indexes {
                    proto_elem.exclude_from_indexes = true;
                }
                proto_elems.push(proto_elem);
            }
            return Ok(crate::model::Value::new()
                .set_array_value(crate::model::ArrayValue::new().set_values(proto_elems)));
        }

        let meaning = if self.meaning == 18 { 0 } else { self.meaning };
        let value_type = match &self.kind {
            ValueKind::Null => ValueType::NullValue(wkt::NullValue),
            ValueKind::Boolean(b) => ValueType::BooleanValue(*b),
            ValueKind::Integer(i) => ValueType::IntegerValue(*i),
            ValueKind::Double(d) => ValueType::DoubleValue(*d),
            ValueKind::Timestamp(ts) => ValueType::TimestampValue(Box::new(*ts)),
            ValueKind::Key(k) => {
                let normalized = k.normalize(default_partition)?;
                ValueType::KeyValue(Box::new(normalized.to_proto()))
            }
            ValueKind::String(s) => ValueType::StringValue(s.clone()),
            ValueKind::Bytes(b) => ValueType::BlobValue(b.clone()),
            ValueKind::GeoPoint(gp) => ValueType::GeoPointValue(Box::new(
                google_cloud_type::model::LatLng::new()
                    .set_latitude(gp.latitude)
                    .set_longitude(gp.longitude),
            )),
            ValueKind::Entity(ent) => {
                ValueType::EntityValue(Box::new(ent.to_proto(default_partition)?))
            }
            ValueKind::Array(_) => unreachable!("handled above"),
        };

        Ok(crate::model::Value::new()
            .set_meaning(meaning)
            .set_exclude_from_indexes(self.exclude_from_indexes)
            .set_value_type(value_type))
    }

    /// Converts a wire `crate::model::Value` into a [Value].
    pub(crate) fn from_proto(proto: crate::model::Value) -> Result<Self, ValueError> {
        use crate::model::value::ValueType;

        let mut exclude_from_indexes = proto.exclude_from_indexes;
        let meaning = proto.meaning;

        let kind = match proto.value_type {
            None | Some(ValueType::NullValue(_)) => ValueKind::Null,
            Some(ValueType::BooleanValue(b)) => ValueKind::Boolean(b),
            Some(ValueType::IntegerValue(i)) => ValueKind::Integer(i),
            Some(ValueType::DoubleValue(d)) => ValueKind::Double(d),
            Some(ValueType::TimestampValue(ts)) => ValueKind::Timestamp(*ts),
            Some(ValueType::KeyValue(k)) => ValueKind::Key(Key::from_proto(*k)?),
            Some(ValueType::StringValue(s)) => ValueKind::String(s),
            Some(ValueType::BlobValue(b)) => ValueKind::Bytes(b),
            Some(ValueType::GeoPointValue(gp)) => {
                ValueKind::GeoPoint(GeoPoint::new(gp.latitude, gp.longitude))
            }
            Some(ValueType::EntityValue(ent)) => ValueKind::Entity(Entity::from_proto(*ent)?),
            Some(ValueType::ArrayValue(arr)) => {
                let mut elems = Vec::with_capacity(arr.values.len());
                for v in arr.values {
                    let elem = Value::from_proto(v)?;
                    if matches!(elem.kind, ValueKind::Array(_)) {
                        return Err(ValueError::NestedArray);
                    }
                    elems.push(elem);
                }
                if !elems.is_empty() && elems.iter().all(|e| e.exclude_from_indexes) {
                    exclude_from_indexes = true;
                }
                ValueKind::Array(elems)
            }
        };

        Ok(Value {
            kind,
            exclude_from_indexes,
            meaning,
        })
    }
}

impl Default for Value {
    fn default() -> Self {
        Self::null()
    }
}

/// Converts a borrowed or owned Datastore [Value] into a Rust type `Self`.
pub trait FromValue: Sized {
    /// Converts a borrowed [Value] into `Self`.
    fn from_value(value: &Value) -> Result<Self, ValueError>;

    /// Converts an owned [Value] into `Self`.
    fn from_value_owned(value: Value) -> Result<Self, ValueError> {
        Self::from_value(&value)
    }

    /// Returns `Some(default)` when a property is absent from an [Entity]
    /// (implemented as `Some(None)` for `Option<T>`).
    fn from_missing() -> Option<Self> {
        None
    }
}

/// Converts a Rust value into a Datastore [Value].
pub trait IntoValue {
    /// Converts `self` into a Datastore [Value].
    fn into_value(self) -> Value;
}

impl FromValue for Value {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        Ok(value.clone())
    }

    fn from_value_owned(value: Value) -> Result<Self, ValueError> {
        Ok(value)
    }
}

impl IntoValue for Value {
    fn into_value(self) -> Value {
        self
    }
}

impl IntoValue for ValueKind {
    fn into_value(self) -> Value {
        Value::from_kind(self)
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::Boolean(b) => Ok(*b),
            other => Err(ValueError::UnexpectedType {
                expected: "Boolean",
                actual: other.type_name(),
            }),
        }
    }
}

impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Boolean(self))
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::Integer(i) => Ok(*i),
            other => Err(ValueError::UnexpectedType {
                expected: "Integer",
                actual: other.type_name(),
            }),
        }
    }
}

impl IntoValue for i64 {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Integer(self))
    }
}

macro_rules! impl_narrow_int {
    ($($t:ty),* $(,)?) => {
        $(
            impl FromValue for $t {
                fn from_value(value: &Value) -> Result<Self, ValueError> {
                    let v = i64::from_value(value)?;
                    <$t>::try_from(v).map_err(|_| ValueError::IntegerOutOfRange(v))
                }
            }

            impl IntoValue for $t {
                fn into_value(self) -> Value {
                    Value::from_kind(ValueKind::Integer(i64::from(self)))
                }
            }
        )*
    };
}

impl_narrow_int!(i32, i16, i8, u32, u16);

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::Double(d) => Ok(*d),
            other => Err(ValueError::UnexpectedType {
                expected: "Double",
                actual: other.type_name(),
            }),
        }
    }
}

impl IntoValue for f64 {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Double(self))
    }
}

impl FromValue for f32 {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        Ok(f64::from_value(value)? as f32)
    }
}

impl IntoValue for f32 {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Double(f64::from(self)))
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::String(s) => Ok(s.clone()),
            other => Err(ValueError::UnexpectedType {
                expected: "String",
                actual: other.type_name(),
            }),
        }
    }

    fn from_value_owned(value: Value) -> Result<Self, ValueError> {
        match value.kind {
            ValueKind::String(s) => Ok(s),
            other => Err(ValueError::UnexpectedType {
                expected: "String",
                actual: other.type_name(),
            }),
        }
    }
}

impl IntoValue for String {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::String(self))
    }
}

impl IntoValue for &str {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::String(self.to_string()))
    }
}

impl IntoValue for Cow<'_, str> {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::String(self.into_owned()))
    }
}

impl FromValue for Bytes {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::Bytes(b) => Ok(b.clone()),
            other => Err(ValueError::UnexpectedType {
                expected: "Bytes",
                actual: other.type_name(),
            }),
        }
    }

    fn from_value_owned(value: Value) -> Result<Self, ValueError> {
        match value.kind {
            ValueKind::Bytes(b) => Ok(b),
            other => Err(ValueError::UnexpectedType {
                expected: "Bytes",
                actual: other.type_name(),
            }),
        }
    }
}

impl IntoValue for Bytes {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Bytes(self))
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        Ok(Bytes::from_value(value)?.to_vec())
    }

    fn from_value_owned(value: Value) -> Result<Self, ValueError> {
        Ok(Bytes::from_value_owned(value)?.into())
    }
}

impl IntoValue for Vec<u8> {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Bytes(Bytes::from(self)))
    }
}

impl IntoValue for &[u8] {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Bytes(Bytes::copy_from_slice(self)))
    }
}

impl FromValue for wkt::Timestamp {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::Timestamp(ts) => Ok(*ts),
            // Datastore projection queries return indexed timestamps as integer microseconds with meaning = 18.
            ValueKind::Integer(micros) if value.meaning == 18 => {
                let secs = micros.div_euclid(1_000_000);
                let rem_micros = micros.rem_euclid(1_000_000);
                let nanos = (rem_micros * 1_000) as i32;
                wkt::Timestamp::new(secs, nanos)
                    .map_err(|e| ValueError::Custom(format!("invalid projected timestamp: {e}")))
            }
            other => Err(ValueError::UnexpectedType {
                expected: "Timestamp",
                actual: other.type_name(),
            }),
        }
    }
}

impl IntoValue for wkt::Timestamp {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Timestamp(self))
    }
}

impl FromValue for Key {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::Key(k) => Ok(k.clone()),
            other => Err(ValueError::UnexpectedType {
                expected: "Key",
                actual: other.type_name(),
            }),
        }
    }

    fn from_value_owned(value: Value) -> Result<Self, ValueError> {
        match value.kind {
            ValueKind::Key(k) => Ok(k),
            other => Err(ValueError::UnexpectedType {
                expected: "Key",
                actual: other.type_name(),
            }),
        }
    }
}

impl IntoValue for Key {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Key(self))
    }
}

impl FromValue for GeoPoint {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::GeoPoint(gp) => Ok(*gp),
            other => Err(ValueError::UnexpectedType {
                expected: "GeoPoint",
                actual: other.type_name(),
            }),
        }
    }
}

impl IntoValue for GeoPoint {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::GeoPoint(self))
    }
}

impl FromValue for Entity {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::Entity(e) => Ok(e.clone()),
            other => Err(ValueError::UnexpectedType {
                expected: "Entity",
                actual: other.type_name(),
            }),
        }
    }

    fn from_value_owned(value: Value) -> Result<Self, ValueError> {
        match value.kind {
            ValueKind::Entity(e) => Ok(e),
            other => Err(ValueError::UnexpectedType {
                expected: "Entity",
                actual: other.type_name(),
            }),
        }
    }
}

impl IntoValue for Entity {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Entity(self))
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        if value.is_null() {
            Ok(None)
        } else {
            Ok(Some(T::from_value(value)?))
        }
    }

    fn from_value_owned(value: Value) -> Result<Self, ValueError> {
        if value.is_null() {
            Ok(None)
        } else {
            Ok(Some(T::from_value_owned(value)?))
        }
    }

    fn from_missing() -> Option<Self> {
        Some(None)
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    fn into_value(self) -> Value {
        match self {
            Some(v) => v.into_value(),
            None => Value::null(),
        }
    }
}

impl<T: FromValue> FromValue for Vec<T> {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::Array(elems) => elems.iter().map(T::from_value).collect(),
            other => Err(ValueError::UnexpectedType {
                expected: "Array",
                actual: other.type_name(),
            }),
        }
    }

    fn from_value_owned(value: Value) -> Result<Self, ValueError> {
        match value.kind {
            ValueKind::Array(elems) => elems.into_iter().map(T::from_value_owned).collect(),
            other => Err(ValueError::UnexpectedType {
                expected: "Array",
                actual: other.type_name(),
            }),
        }
    }
}

impl<T: IntoValue> IntoValue for Vec<T> {
    fn into_value(self) -> Value {
        Value::from_kind(ValueKind::Array(
            self.into_iter().map(IntoValue::into_value).collect(),
        ))
    }
}

impl<T: FromValue> FromValue for BTreeMap<String, T> {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::Entity(ent) => ent
                .properties()
                .iter()
                .map(|(k, v)| Ok((k.clone(), T::from_value(v)?)))
                .collect(),
            other => Err(ValueError::UnexpectedType {
                expected: "Entity",
                actual: other.type_name(),
            }),
        }
    }
}

impl<T: IntoValue> IntoValue for BTreeMap<String, T> {
    fn into_value(self) -> Value {
        let mut ent = Entity::new();
        for (k, v) in self {
            ent.set(k, v);
        }
        Value::from_kind(ValueKind::Entity(ent))
    }
}

impl<T: FromValue> FromValue for HashMap<String, T> {
    fn from_value(value: &Value) -> Result<Self, ValueError> {
        match &value.kind {
            ValueKind::Entity(ent) => ent
                .properties()
                .iter()
                .map(|(k, v)| Ok((k.clone(), T::from_value(v)?)))
                .collect(),
            other => Err(ValueError::UnexpectedType {
                expected: "Entity",
                actual: other.type_name(),
            }),
        }
    }
}

impl<T: IntoValue> IntoValue for HashMap<String, T> {
    fn into_value(self) -> Value {
        let mut ent = Entity::new();
        for (k, v) in self {
            ent.set(k, v);
        }
        Value::from_kind(ValueKind::Entity(ent))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_value_types_roundtrip_and_convert() -> anyhow::Result<()> {
        let default_part = PartitionId::new("proj-1");

        let b = Value::new(true);
        assert!(bool::from_value(&b)?);

        let i = Value::new(42_i32);
        assert_eq!(i64::from_value(&i)?, 42);
        assert_eq!(i32::from_value(&i)?, 42);
        assert!(matches!(
            i8::from_value(&Value::new(1000_i64)),
            Err(ValueError::IntegerOutOfRange(1000))
        ));

        let d = Value::new(3.25_f64);
        assert_eq!(f64::from_value(&d)?, 3.25);

        let s = Value::new("hello");
        assert_eq!(String::from_value(&s)?, "hello");

        let raw = Value::new(vec![1_u8, 2, 3]);
        assert_eq!(Bytes::from_value(&raw)?, Bytes::from_static(&[1, 2, 3]));

        let gp = GeoPoint::new(37.422, -122.084);
        let gp_val = Value::new(gp);
        assert_eq!(GeoPoint::from_value(&gp_val)?, gp);

        let ts = wkt::Timestamp::new(1_700_000_000, 123_456_000)?;
        let ts_val = Value::new(ts);
        assert_eq!(wkt::Timestamp::from_value(&ts_val)?, ts);

        // Projected timestamp (meaning = 18 on integer microseconds)
        let projected_ts = Value::new(1_700_000_000_123_456_i64).with_meaning(18);
        assert_eq!(wkt::Timestamp::from_value(&projected_ts)?, ts);
        assert_eq!(i64::from_value(&projected_ts)?, 1_700_000_000_123_456);

        let key = Key::with_id("Task", 7)?;
        let key_val = Value::new(key.clone());
        let proto_key_val = key_val.to_proto(&default_part)?;
        let decoded_key_val = Value::from_proto(proto_key_val)?;
        assert_eq!(Key::from_value(&decoded_key_val)?.id_num(), Some(7));

        let opt_none: Option<String> = None;
        let null_val = Value::new(opt_none);
        assert!(null_val.is_null());
        assert_eq!(Option::<String>::from_value(&null_val)?, None);

        Ok(())
    }

    #[test]
    fn array_exclude_from_indexes_and_nested_array_rejection() -> anyhow::Result<()> {
        let default_part = PartitionId::new("proj-1");

        // Unindexed array propagates exclude_from_indexes = true to each element,
        // while keeping outer exclude_from_indexes = false on the wire.
        let arr = Value::new(vec!["a", "b"]).with_exclude_from_indexes(true);
        let proto = arr.to_proto(&default_part)?;
        assert!(!proto.exclude_from_indexes);
        let wire_elems = &proto.array_value().unwrap().values;
        assert_eq!(wire_elems.len(), 2);
        assert!(wire_elems[0].exclude_from_indexes);
        assert!(wire_elems[1].exclude_from_indexes);

        let roundtripped = Value::from_proto(proto)?;
        assert!(roundtripped.exclude_from_indexes());
        assert_eq!(Vec::<String>::from_value(&roundtripped)?, vec!["a", "b"]);

        // Nested arrays are rejected with ValueError::NestedArray
        let nested = Value::from_kind(ValueKind::Array(vec![Value::new(vec![1_i64])]));
        assert_eq!(nested.to_proto(&default_part), Err(ValueError::NestedArray));

        Ok(())
    }
}
