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
use crate::key::Key;

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
    Bytes(bytes::Bytes),
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
        Value {
            kind: self,
            exclude_from_indexes: false,
            meaning: 0,
        }
    }
}
