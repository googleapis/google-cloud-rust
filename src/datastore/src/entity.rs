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

use crate::error::{EntityError, ValueError};
use crate::key::{IntoMutationKey, Key, MutationKey, PartitionId};
use crate::value::{FromValue, IntoValue, Value};
use std::collections::{BTreeMap, HashMap};

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
        Self {
            key: Some(key.into_mutation_key()),
            properties: BTreeMap::new(),
            version: None,
        }
    }

    /// Returns the entity's complete [Key], if present and complete.
    pub fn key(&self) -> Option<&Key> {
        self.key.as_ref()?.as_complete()
    }

    /// Returns the entity's [MutationKey] (complete or incomplete), if set.
    pub fn mutation_key(&self) -> Option<&MutationKey> {
        self.key.as_ref()
    }

    /// Sets the key of this entity.
    pub fn set_key(&mut self, key: impl IntoMutationKey) -> &mut Self {
        self.key = Some(key.into_mutation_key());
        self
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
        match self.properties.get(name) {
            Some(v) => T::from_value(v).map_err(|source| EntityError::InvalidProperty {
                name: name.to_string(),
                source,
            }),
            None => T::from_missing().ok_or_else(|| EntityError::MissingProperty(name.to_string())),
        }
    }

    /// Removes and converts the property `name` into `T` by value using [FromValue].
    pub fn take<T: FromValue>(&mut self, name: &str) -> Result<T, EntityError> {
        match self.properties.remove(name) {
            Some(v) => T::from_value_owned(v).map_err(|source| EntityError::InvalidProperty {
                name: name.to_string(),
                source,
            }),
            None => T::from_missing().ok_or_else(|| EntityError::MissingProperty(name.to_string())),
        }
    }

    /// Inserts or overwrites an indexed property `name` on this entity.
    pub fn set(&mut self, name: impl Into<String>, value: impl IntoValue) -> &mut Self {
        self.properties.insert(name.into(), value.into_value());
        self
    }

    /// Inserts or overwrites an unindexed property (`exclude_from_indexes = true`) on this entity.
    pub fn set_unindexed(&mut self, name: impl Into<String>, value: impl IntoValue) -> &mut Self {
        self.properties.insert(
            name.into(),
            value.into_value().with_exclude_from_indexes(true),
        );
        self
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

    /// Converts this [Entity] into its wire `crate::model::Entity` representation.
    #[allow(dead_code)]
    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> Result<crate::model::Entity, ValueError> {
        let mut proto_props = HashMap::with_capacity(self.properties.len());
        for (name, val) in &self.properties {
            proto_props.insert(name.clone(), val.to_proto(default_partition)?);
        }
        let mut proto = crate::model::Entity::new().set_properties(proto_props);
        if let Some(key) = &self.key {
            let normalized = key.normalize(default_partition)?;
            proto = proto.set_key(normalized.to_proto());
        }
        Ok(proto)
    }

    /// Converts a wire `crate::model::Entity` into an [Entity].
    #[allow(dead_code)]
    pub(crate) fn from_proto(proto: crate::model::Entity) -> Result<Self, ValueError> {
        let key = match proto.key {
            Some(k) if !k.path.is_empty() => Some(MutationKey::from_proto(k)?),
            _ => None,
        };
        let mut properties = BTreeMap::new();
        for (name, proto_val) in proto.properties {
            properties.insert(name, Value::from_proto(proto_val)?);
        }
        Ok(Entity {
            key,
            properties,
            version: None,
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_properties_and_proto_roundtrip() -> anyhow::Result<()> {
        let key = Key::with_name("User", "alice")?;
        let mut entity = Entity::with_key(key.clone());
        entity
            .set("age", 30_i64)
            .set("tags", vec!["rust", "cloud"])
            .set_unindexed("bio", "large unindexed text");

        assert_eq!(entity.key(), Some(&key));
        assert_eq!(entity.get::<i64>("age")?, 30);
        assert_eq!(entity.get::<Vec<String>>("tags")?, vec!["rust", "cloud"]);
        assert_eq!(entity.get::<Option<String>>("missing_optional")?, None);
        assert_eq!(
            entity.get::<String>("missing_required"),
            Err(EntityError::MissingProperty("missing_required".to_string()))
        );
        assert!(matches!(
            entity.get::<bool>("age"),
            Err(EntityError::InvalidProperty { ref name, .. }) if name == "age"
        ));

        let default_part = PartitionId::new("proj-1");
        let proto = entity.to_proto(&default_part)?;
        let mut decoded = Entity::from_proto(proto)?;
        assert_eq!(decoded.key().unwrap().name(), Some("alice"));
        assert_eq!(decoded.take::<String>("bio")?, "large unindexed text");
        assert!(!decoded.contains_key("bio"));
        Ok(())
    }
}
