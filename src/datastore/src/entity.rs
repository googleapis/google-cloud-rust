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

//! Dynamic Datastore entity representation and entity conversion traits.

use crate::error::EntityError;
use crate::key::{IntoMutationKey, Key, MutationKey};
use crate::value::{FromValue, IntoValue, Value};
use std::collections::BTreeMap;

/// A Datastore entity consisting of an optional key, a map of named [Value]
/// properties, and an optional microsecond version timestamp.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Entity {
    pub(crate) key: Option<MutationKey>,
    pub(crate) properties: BTreeMap<String, Value>,
    pub(crate) version: Option<i64>,
}

impl Entity {
    /// Creates an empty key-less [Entity] (suitable for embedded entities or
    /// pairing with a key in `insert`/`upsert`/`update`).
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty [Entity] bound to the given [Key] or [IncompleteKey][crate::key::IncompleteKey].
    pub fn with_key(key: impl IntoMutationKey) -> Self {
        let _ = key;
        unimplemented!()
    }

    /// Returns the entity's complete [Key], if present and complete.
    pub fn key(&self) -> Option<&Key> {
        unimplemented!()
    }

    /// Returns the entity's [MutationKey] (complete or incomplete), if set.
    pub fn mutation_key(&self) -> Option<&MutationKey> {
        self.key.as_ref()
    }

    /// Sets the key of this entity.
    pub fn set_key(&mut self, key: impl IntoMutationKey) -> &mut Self {
        let _ = key;
        unimplemented!()
    }

    /// Returns the microsecond version timestamp of this entity, if populated by a read or query.
    pub fn version(&self) -> Option<i64> {
        self.version
    }

    /// Sets the microsecond version timestamp on this entity.
    pub fn with_version(mut self, version: i64) -> Self {
        self.version = Some(version);
        self
    }

    /// Borrows and converts the property `name` into `T` using [FromValue].
    pub fn get<T: FromValue>(&self, name: &str) -> Result<T, EntityError> {
        let _ = name;
        unimplemented!()
    }

    /// Removes and converts the property `name` into `T` by value using [FromValue].
    pub fn take<T: FromValue>(&mut self, name: &str) -> Result<T, EntityError> {
        let _ = name;
        unimplemented!()
    }

    /// Inserts or overwrites an indexed property `name` on this entity.
    pub fn set(&mut self, name: impl Into<String>, value: impl IntoValue) -> &mut Self {
        let _ = (name, value);
        unimplemented!()
    }

    /// Inserts or overwrites an unindexed property (`exclude_from_indexes = true`) on this entity.
    pub fn set_unindexed(&mut self, name: impl Into<String>, value: impl IntoValue) -> &mut Self {
        let _ = (name, value);
        unimplemented!()
    }

    /// Removes the raw [Value] for `name`, if present.
    pub fn remove(&mut self, name: &str) -> Option<Value> {
        self.properties.remove(name)
    }

    /// Returns `true` if this entity contains a property named `name`.
    pub fn contains_key(&self, name: &str) -> bool {
        self.properties.contains_key(name)
    }

    /// Returns the number of properties stored on this entity.
    pub fn len(&self) -> usize {
        self.properties.len()
    }

    /// Returns `true` if this entity has no properties.
    pub fn is_empty(&self) -> bool {
        self.properties.is_empty()
    }

    /// Returns a reference to the underlying property map.
    pub fn properties(&self) -> &BTreeMap<String, Value> {
        &self.properties
    }

    /// Returns a mutable reference to the underlying property map.
    pub fn properties_mut(&mut self) -> &mut BTreeMap<String, Value> {
        &mut self.properties
    }
}

/// Converts a Datastore [Entity] into a Rust domain type `Self`.
pub trait FromEntity: Sized {
    /// Converts a borrowed [Entity] into `Self`.
    fn from_entity(entity: &Entity) -> Result<Self, EntityError>;

    /// Converts an owned [Entity] into `Self`.
    fn from_entity_owned(entity: Entity) -> Result<Self, EntityError> {
        Self::from_entity(&entity)
    }
}

/// Converts a Rust domain type into a Datastore [Entity].
pub trait IntoEntity {
    /// Converts `self` into a Datastore [Entity].
    fn into_entity(self) -> Entity;
}

impl FromEntity for Entity {
    fn from_entity(entity: &Entity) -> Result<Self, EntityError> {
        Ok(entity.clone())
    }

    fn from_entity_owned(entity: Entity) -> Result<Self, EntityError> {
        Ok(entity)
    }
}

impl IntoEntity for Entity {
    fn into_entity(self) -> Entity {
        self
    }
}
