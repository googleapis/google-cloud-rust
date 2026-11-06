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

//! Datastore keys, incomplete keys, key factories, and URL-safe key encoding.

use crate::error::KeyError;

/// Identifies the logical partition (`project_id`, `database_id`, `namespace_id`)
/// to which a Datastore [Key], [IncompleteKey], or query belongs.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId {
    pub(crate) project_id: String,
    pub(crate) database_id: String,
    pub(crate) namespace_id: String,
}

impl PartitionId {
    /// Creates a new [PartitionId] for the given `project_id` in the default
    /// database (`""`) and default namespace (`""`).
    pub fn new(project_id: impl Into<String>) -> Self {
        let _ = project_id;
        unimplemented!()
    }

    /// Sets the database ID (`""` for the default database).
    pub fn with_database_id(self, database_id: impl Into<String>) -> Self {
        let _ = database_id;
        unimplemented!()
    }

    /// Sets the multi-tenant namespace ID (`""` for the default namespace).
    pub fn with_namespace_id(self, namespace_id: impl Into<String>) -> Self {
        let _ = namespace_id;
        unimplemented!()
    }

    /// Returns the Google Cloud project ID.
    pub fn project_id(&self) -> &str {
        unimplemented!()
    }

    /// Returns the Datastore database ID (`""` for the default database).
    pub fn database_id(&self) -> &str {
        unimplemented!()
    }

    /// Returns the multi-tenant namespace ID (`""` for the default namespace).
    pub fn namespace_id(&self) -> &str {
        unimplemented!()
    }
}

/// The unique identifier of a complete [PathElement] within its kind and parent scope.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum KeyId {
    /// A non-zero 64-bit signed integer ID.
    Id(i64),
    /// A non-empty UTF-8 string name.
    Name(String),
}

impl KeyId {
    /// Creates a validated numeric [KeyId::Id], returning [`KeyError::ZeroId`] if `id == 0`.
    pub fn id(id: i64) -> Result<Self, KeyError> {
        let _ = id;
        unimplemented!()
    }

    /// Creates a validated string [KeyId::Name], returning [`KeyError::EmptyName`] if `name` is empty.
    pub fn name(name: impl Into<String>) -> Result<Self, KeyError> {
        let _ = name;
        unimplemented!()
    }

    /// Returns the numeric ID if this is a [KeyId::Id].
    pub fn as_id(&self) -> Option<i64> {
        unimplemented!()
    }

    /// Returns the string name if this is a [KeyId::Name].
    pub fn as_name(&self) -> Option<&str> {
        unimplemented!()
    }
}

impl TryFrom<i64> for KeyId {
    type Error = KeyError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::id(value)
    }
}

impl TryFrom<&str> for KeyId {
    type Error = KeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::name(value)
    }
}

impl TryFrom<String> for KeyId {
    type Error = KeyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::name(value)
    }
}

/// A single complete element `(kind, id_or_name)` in a Datastore key path.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathElement {
    pub(crate) kind: String,
    pub(crate) id: KeyId,
}

impl PathElement {
    /// Creates a new complete [PathElement] from a non-empty `kind` and validated `id`.
    pub fn new(kind: impl Into<String>, id: KeyId) -> Result<Self, KeyError> {
        let _ = (kind, id);
        unimplemented!()
    }

    /// Creates a complete [PathElement] with a numeric `id`.
    pub fn with_id(kind: impl Into<String>, id: i64) -> Result<Self, KeyError> {
        let _ = (kind, id);
        unimplemented!()
    }

    /// Creates a complete [PathElement] with a string `name`.
    pub fn with_name(kind: impl Into<String>, name: impl Into<String>) -> Result<Self, KeyError> {
        let _ = (kind, name);
        unimplemented!()
    }

    /// Returns the entity kind of this path element.
    pub fn kind(&self) -> &str {
        unimplemented!()
    }

    /// Returns the identifier of this path element.
    pub fn id(&self) -> &KeyId {
        unimplemented!()
    }

    /// Returns the numeric ID if this element uses an integer ID.
    pub fn id_num(&self) -> Option<i64> {
        unimplemented!()
    }

    /// Returns the string name if this element uses a string name.
    pub fn name(&self) -> Option<&str> {
        unimplemented!()
    }
}

/// A guaranteed-complete Datastore key.
///
/// Every element in [`Key::path`] (both ancestors and the leaf element) has a
/// valid non-zero numeric ID or non-empty string name. Operations that require
/// complete keys (`get`, `get_multi`, `update`, `delete`, `reserve_ids`, and
/// `has_ancestor`) accept [Key] directly and reject [IncompleteKey] at compile time.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key {
    pub(crate) partition_id: PartitionId,
    pub(crate) path: Vec<PathElement>,
}

impl Key {
    /// Creates a single-element root [Key] with the given `kind` and `id`.
    pub fn new(kind: impl Into<String>, id: KeyId) -> Result<Self, KeyError> {
        let _ = (kind, id);
        unimplemented!()
    }

    /// Creates a root [Key] with a numeric `id`.
    pub fn with_id(kind: impl Into<String>, id: i64) -> Result<Self, KeyError> {
        let _ = (kind, id);
        unimplemented!()
    }

    /// Creates a root [Key] with a string `name`.
    pub fn with_name(kind: impl Into<String>, name: impl Into<String>) -> Result<Self, KeyError> {
        let _ = (kind, name);
        unimplemented!()
    }

    /// Creates a [Key] from a [PartitionId] and a non-empty sequence of [PathElement]s.
    pub fn from_path<I>(partition_id: PartitionId, path: I) -> Result<Self, KeyError>
    where
        I: IntoIterator<Item = PathElement>,
    {
        let _ = (partition_id, path);
        unimplemented!()
    }

    /// Replaces the [PartitionId] of this key.
    pub fn with_partition_id(self, partition_id: PartitionId) -> Self {
        let _ = partition_id;
        unimplemented!()
    }

    /// Returns the [PartitionId] of this key.
    pub fn partition_id(&self) -> &PartitionId {
        unimplemented!()
    }

    /// Returns the full ancestor + leaf path of this key (guaranteed non-empty).
    pub fn path(&self) -> &[PathElement] {
        unimplemented!()
    }

    /// Returns the leaf [PathElement] of this key.
    pub fn leaf(&self) -> &PathElement {
        unimplemented!()
    }

    /// Returns the entity kind of the leaf element.
    pub fn kind(&self) -> &str {
        unimplemented!()
    }

    /// Returns the [KeyId] of the leaf element.
    pub fn id(&self) -> &KeyId {
        unimplemented!()
    }

    /// Returns the numeric ID of the leaf element, if it uses a [KeyId::Id].
    pub fn id_num(&self) -> Option<i64> {
        unimplemented!()
    }

    /// Returns the string name of the leaf element, if it uses a [KeyId::Name].
    pub fn name(&self) -> Option<&str> {
        unimplemented!()
    }

    /// Returns the parent [Key] if this key has at least one ancestor (`path.len() > 1`).
    pub fn parent(&self) -> Option<Key> {
        unimplemented!()
    }

    /// Returns an iterator over all ancestor [Key]s from the root down to the immediate parent.
    pub fn ancestors(&self) -> Vec<Key> {
        unimplemented!()
    }

    /// Returns `true` if `ancestor` is a strict ancestor of `self` in the same partition.
    pub fn has_ancestor(&self, ancestor: &Key) -> bool {
        let _ = ancestor;
        unimplemented!()
    }

    /// Appends a complete child [PathElement] to this key, returning the child [Key].
    pub fn with_child(&self, kind: impl Into<String>, id: KeyId) -> Result<Key, KeyError> {
        let _ = (kind, id);
        unimplemented!()
    }

    /// Appends an incomplete child `kind` to this key, returning an [IncompleteKey].
    pub fn with_incomplete_child(
        &self,
        kind: impl Into<String>,
    ) -> Result<IncompleteKey, KeyError> {
        let _ = kind;
        unimplemented!()
    }
}

/// An incomplete Datastore key whose leaf element has a `kind` but no ID or name.
///
/// Used when allocating IDs via [`Datastore::allocate_ids`][crate::client::Datastore::allocate_ids]
/// or inserting new entities with server-allocated numeric IDs.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IncompleteKey {
    pub(crate) partition_id: PartitionId,
    pub(crate) ancestors: Vec<PathElement>,
    pub(crate) kind: String,
}

impl IncompleteKey {
    /// Creates a root [IncompleteKey] for the given `kind`.
    pub fn new(kind: impl Into<String>) -> Result<Self, KeyError> {
        let _ = kind;
        unimplemented!()
    }

    /// Creates an [IncompleteKey] with the given [PartitionId], complete `ancestors`, and leaf `kind`.
    pub fn from_ancestors<I>(
        partition_id: PartitionId,
        ancestors: I,
        kind: impl Into<String>,
    ) -> Result<Self, KeyError>
    where
        I: IntoIterator<Item = PathElement>,
    {
        let _ = (partition_id, ancestors, kind);
        unimplemented!()
    }

    /// Replaces the [PartitionId] of this incomplete key.
    pub fn with_partition_id(self, partition_id: PartitionId) -> Self {
        let _ = partition_id;
        unimplemented!()
    }

    /// Returns the [PartitionId] of this incomplete key.
    pub fn partition_id(&self) -> &PartitionId {
        unimplemented!()
    }

    /// Returns the complete ancestor path elements preceding the incomplete leaf.
    pub fn ancestors(&self) -> &[PathElement] {
        unimplemented!()
    }

    /// Returns the entity kind of the incomplete leaf element.
    pub fn kind(&self) -> &str {
        unimplemented!()
    }

    /// Returns the parent [Key] if this incomplete key has at least one ancestor.
    pub fn parent(&self) -> Option<Key> {
        unimplemented!()
    }

    /// Completes this key by assigning `id` to its leaf element.
    pub fn with_id(&self, id: KeyId) -> Key {
        let _ = id;
        unimplemented!()
    }
}

/// Either a complete [Key] or an [IncompleteKey], used by mutations (`insert`, `upsert`)
/// and [Entity][crate::entity::Entity] keys.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum MutationKey {
    /// A complete [Key].
    Complete(Key),
    /// An [IncompleteKey] whose numeric ID will be allocated on `Commit`.
    Incomplete(IncompleteKey),
}

impl MutationKey {
    /// Returns `Some(&Key)` if this key is complete.
    pub fn as_complete(&self) -> Option<&Key> {
        unimplemented!()
    }

    /// Returns `Some(&IncompleteKey)` if this key is incomplete.
    pub fn as_incomplete(&self) -> Option<&IncompleteKey> {
        unimplemented!()
    }
}

mod private {
    pub trait Sealed {}
    impl Sealed for super::Key {}
    impl Sealed for &super::Key {}
    impl Sealed for super::IncompleteKey {}
    impl Sealed for &super::IncompleteKey {}
    impl Sealed for super::MutationKey {}
}

/// Sealed trait implemented by [Key], [IncompleteKey], and [MutationKey] for
/// operations (`insert`, `upsert`, `Entity::with_key`) that accept either key state.
pub trait IntoMutationKey: private::Sealed {
    /// Converts `self` into a [MutationKey].
    fn into_mutation_key(self) -> MutationKey;
}

impl IntoMutationKey for Key {
    fn into_mutation_key(self) -> MutationKey {
        MutationKey::Complete(self)
    }
}

impl IntoMutationKey for &Key {
    fn into_mutation_key(self) -> MutationKey {
        MutationKey::Complete(self.clone())
    }
}

impl IntoMutationKey for IncompleteKey {
    fn into_mutation_key(self) -> MutationKey {
        MutationKey::Incomplete(self)
    }
}

impl IntoMutationKey for &IncompleteKey {
    fn into_mutation_key(self) -> MutationKey {
        MutationKey::Incomplete(self.clone())
    }
}

impl IntoMutationKey for MutationKey {
    fn into_mutation_key(self) -> MutationKey {
        self
    }
}

/// A factory bound to a client's [PartitionId] and an entity `kind` for
/// constructing [Key]s and [IncompleteKey]s without repeating project, database,
/// or namespace identifiers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyFactory {
    pub(crate) partition_id: PartitionId,
    pub(crate) ancestors: Vec<PathElement>,
    pub(crate) kind: String,
}

impl KeyFactory {
    /// Creates a new [KeyFactory] for `partition_id` and `kind`.
    pub fn new(partition_id: PartitionId, kind: impl Into<String>) -> Self {
        let _ = (partition_id, kind);
        unimplemented!()
    }

    /// Overrides the namespace ID for keys created by this factory.
    pub fn with_namespace_id(self, namespace_id: impl Into<String>) -> Self {
        let _ = namespace_id;
        unimplemented!()
    }

    /// Sets the parent [Key] for keys created by this factory.
    pub fn with_parent(self, parent: &Key) -> Self {
        let _ = parent;
        unimplemented!()
    }

    /// Builds an [IncompleteKey] for the configured partition, parent, and kind.
    pub fn incomplete(&self) -> Result<IncompleteKey, KeyError> {
        unimplemented!()
    }

    /// Builds a complete [Key] with the given numeric `id`.
    pub fn with_id(&self, id: i64) -> Result<Key, KeyError> {
        let _ = id;
        unimplemented!()
    }

    /// Builds a complete [Key] with the given string `name`.
    pub fn with_name(&self, name: impl Into<String>) -> Result<Key, KeyError> {
        let _ = name;
        unimplemented!()
    }
}
