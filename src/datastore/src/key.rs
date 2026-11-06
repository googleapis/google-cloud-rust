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
use base64::Engine;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use prost::Message as _;

fn validate_kind(kind: String) -> Result<String, KeyError> {
    if kind.is_empty() {
        Err(KeyError::EmptyKind)
    } else {
        Ok(kind)
    }
}

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
        Self {
            project_id: project_id.into(),
            database_id: String::new(),
            namespace_id: String::new(),
        }
    }

    /// Sets the database ID (`""` for the default database).
    pub fn with_database_id(mut self, database_id: impl Into<String>) -> Self {
        self.database_id = database_id.into();
        self
    }

    /// Sets the multi-tenant namespace ID (`""` for the default namespace).
    pub fn with_namespace_id(mut self, namespace_id: impl Into<String>) -> Self {
        self.namespace_id = namespace_id.into();
        self
    }

    /// Returns the Google Cloud project ID.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// Returns the Datastore database ID (`""` for the default database).
    pub fn database_id(&self) -> &str {
        &self.database_id
    }

    /// Returns the multi-tenant namespace ID (`""` for the default namespace).
    pub fn namespace_id(&self) -> &str {
        &self.namespace_id
    }

    /// Normalizes this partition ID against a client's default `PartitionId`:
    /// - Fills in empty `project_id`, `database_id`, or `namespace_id` from `default`.
    /// - Rejects keys whose non-empty `project_id` or `database_id` differs from `default`.
    pub(crate) fn normalize(&self, default: &PartitionId) -> Result<PartitionId, KeyError> {
        let project_id = if self.project_id.is_empty() {
            default.project_id.clone()
        } else if self.project_id != default.project_id {
            return Err(KeyError::PartitionMismatch {
                expected_project: default.project_id.clone(),
                actual_project: self.project_id.clone(),
                expected_database: default.database_id.clone(),
                actual_database: self.database_id.clone(),
            });
        } else {
            self.project_id.clone()
        };

        let database_id = if self.database_id.is_empty() {
            default.database_id.clone()
        } else if self.database_id != default.database_id {
            return Err(KeyError::PartitionMismatch {
                expected_project: default.project_id.clone(),
                actual_project: project_id,
                expected_database: default.database_id.clone(),
                actual_database: self.database_id.clone(),
            });
        } else {
            self.database_id.clone()
        };

        let namespace_id = if self.namespace_id.is_empty() {
            default.namespace_id.clone()
        } else {
            self.namespace_id.clone()
        };

        Ok(PartitionId {
            project_id,
            database_id,
            namespace_id,
        })
    }

    pub(crate) fn to_proto(&self) -> crate::model::PartitionId {
        crate::model::PartitionId::new()
            .set_project_id(self.project_id.clone())
            .set_database_id(self.database_id.clone())
            .set_namespace_id(self.namespace_id.clone())
    }

    pub(crate) fn from_proto(proto: crate::model::PartitionId) -> Self {
        Self {
            project_id: proto.project_id,
            database_id: proto.database_id,
            namespace_id: proto.namespace_id,
        }
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
        if id == 0 {
            Err(KeyError::ZeroId)
        } else {
            Ok(Self::Id(id))
        }
    }

    /// Creates a validated string [KeyId::Name], returning [`KeyError::EmptyName`] if `name` is empty.
    pub fn name(name: impl Into<String>) -> Result<Self, KeyError> {
        let name = name.into();
        if name.is_empty() {
            Err(KeyError::EmptyName)
        } else {
            Ok(Self::Name(name))
        }
    }

    /// Returns the numeric ID if this is a [KeyId::Id].
    pub fn as_id(&self) -> Option<i64> {
        match self {
            Self::Id(id) => Some(*id),
            Self::Name(_) => None,
        }
    }

    /// Returns the string name if this is a [KeyId::Name].
    pub fn as_name(&self) -> Option<&str> {
        match self {
            Self::Id(_) => None,
            Self::Name(name) => Some(name.as_str()),
        }
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
        let kind = validate_kind(kind.into())?;
        match &id {
            KeyId::Id(0) => return Err(KeyError::ZeroId),
            KeyId::Name(s) if s.is_empty() => return Err(KeyError::EmptyName),
            _ => {}
        }
        Ok(Self { kind, id })
    }

    /// Creates a complete [PathElement] with a numeric `id`.
    pub fn with_id(kind: impl Into<String>, id: i64) -> Result<Self, KeyError> {
        Self::new(kind, KeyId::id(id)?)
    }

    /// Creates a complete [PathElement] with a string `name`.
    pub fn with_name(kind: impl Into<String>, name: impl Into<String>) -> Result<Self, KeyError> {
        Self::new(kind, KeyId::name(name)?)
    }

    /// Returns the entity kind of this path element.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Returns the identifier of this path element.
    pub fn id(&self) -> &KeyId {
        &self.id
    }

    /// Returns the numeric ID if this element uses an integer ID.
    pub fn id_num(&self) -> Option<i64> {
        self.id.as_id()
    }

    /// Returns the string name if this element uses a string name.
    pub fn name(&self) -> Option<&str> {
        self.id.as_name()
    }

    pub(crate) fn to_proto(&self) -> crate::model::key::PathElement {
        let elem = crate::model::key::PathElement::new().set_kind(self.kind.clone());
        match &self.id {
            KeyId::Id(id) => elem.set_id(*id),
            KeyId::Name(name) => elem.set_name(name.clone()),
        }
    }

    pub(crate) fn from_proto(
        proto: crate::model::key::PathElement,
        is_ancestor: bool,
    ) -> Result<Self, KeyError> {
        let kind = validate_kind(proto.kind)?;
        match proto.id_type {
            Some(crate::model::key::path_element::IdType::Id(id)) => {
                Ok(Self::new(kind, KeyId::id(id)?)?)
            }
            Some(crate::model::key::path_element::IdType::Name(name)) => {
                Ok(Self::new(kind, KeyId::name(name)?)?)
            }
            None => {
                if is_ancestor {
                    Err(KeyError::IncompleteAncestor(kind))
                } else {
                    Err(KeyError::IncompleteKey(kind))
                }
            }
        }
    }
}

/// Wire-compatible protobuf definition for `google.datastore.v1.PartitionId` (used for URL-safe encoding).
#[derive(Clone, PartialEq, prost::Message)]
struct PartitionIdWire {
    #[prost(string, tag = "2")]
    project_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(string, tag = "4")]
    namespace_id: String,
}

/// Wire-compatible protobuf oneof for `google.datastore.v1.Key.PathElement.id_type`.
#[derive(Clone, PartialEq, prost::Oneof)]
enum IdTypeWire {
    #[prost(int64, tag = "2")]
    Id(i64),
    #[prost(string, tag = "3")]
    Name(String),
}

/// Wire-compatible protobuf definition for `google.datastore.v1.Key.PathElement`.
#[derive(Clone, PartialEq, prost::Message)]
struct PathElementWire {
    #[prost(string, tag = "1")]
    kind: String,
    #[prost(oneof = "IdTypeWire", tags = "2, 3")]
    id_type: Option<IdTypeWire>,
}

/// Wire-compatible protobuf definition for `google.datastore.v1.Key`.
#[derive(Clone, PartialEq, prost::Message)]
struct KeyWire {
    #[prost(message, optional, tag = "1")]
    partition_id: Option<PartitionIdWire>,
    #[prost(message, repeated, tag = "2")]
    path: Vec<PathElementWire>,
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
        let elem = PathElement::new(kind, id)?;
        Ok(Self {
            partition_id: PartitionId::default(),
            path: vec![elem],
        })
    }

    /// Creates a root [Key] with a numeric `id`.
    pub fn with_id(kind: impl Into<String>, id: i64) -> Result<Self, KeyError> {
        Self::new(kind, KeyId::id(id)?)
    }

    /// Creates a root [Key] with a string `name`.
    pub fn with_name(kind: impl Into<String>, name: impl Into<String>) -> Result<Self, KeyError> {
        Self::new(kind, KeyId::name(name)?)
    }

    /// Creates a [Key] from a [PartitionId] and a non-empty sequence of [PathElement]s.
    pub fn from_path<I>(partition_id: PartitionId, path: I) -> Result<Self, KeyError>
    where
        I: IntoIterator<Item = PathElement>,
    {
        let path: Vec<PathElement> = path.into_iter().collect();
        if path.is_empty() {
            return Err(KeyError::EmptyPath);
        }
        Ok(Self { partition_id, path })
    }

    /// Replaces the [PartitionId] of this key.
    pub fn with_partition_id(mut self, partition_id: PartitionId) -> Self {
        self.partition_id = partition_id;
        self
    }

    /// Returns the [PartitionId] of this key.
    pub fn partition_id(&self) -> &PartitionId {
        &self.partition_id
    }

    /// Returns the full ancestor + leaf path of this key (guaranteed non-empty).
    pub fn path(&self) -> &[PathElement] {
        &self.path
    }

    /// Returns the leaf [PathElement] of this key.
    pub fn leaf(&self) -> &PathElement {
        self.path
            .last()
            .expect("Key::path is guaranteed to be non-empty")
    }

    /// Returns the entity kind of the leaf element.
    pub fn kind(&self) -> &str {
        self.leaf().kind()
    }

    /// Returns the [KeyId] of the leaf element.
    pub fn id(&self) -> &KeyId {
        self.leaf().id()
    }

    /// Returns the numeric ID of the leaf element, if it uses a [KeyId::Id].
    pub fn id_num(&self) -> Option<i64> {
        self.leaf().id_num()
    }

    /// Returns the string name of the leaf element, if it uses a [KeyId::Name].
    pub fn name(&self) -> Option<&str> {
        self.leaf().name()
    }

    /// Returns the parent [Key] if this key has at least one ancestor (`path.len() > 1`).
    pub fn parent(&self) -> Option<Key> {
        if self.path.len() <= 1 {
            None
        } else {
            Some(Key {
                partition_id: self.partition_id.clone(),
                path: self.path[..self.path.len() - 1].to_vec(),
            })
        }
    }

    /// Returns all ancestor [Key]s from the root down to the immediate parent.
    pub fn ancestors(&self) -> Vec<Key> {
        (1..self.path.len())
            .map(|len| Key {
                partition_id: self.partition_id.clone(),
                path: self.path[..len].to_vec(),
            })
            .collect()
    }

    /// Returns `true` if `ancestor` is in the same partition and `ancestor.path()` is a prefix of `self.path()`.
    pub fn has_ancestor(&self, ancestor: &Key) -> bool {
        self.partition_id == ancestor.partition_id && self.path.starts_with(&ancestor.path)
    }

    /// Appends a complete child [PathElement] to this key, returning the child [Key].
    pub fn with_child(&self, kind: impl Into<String>, id: KeyId) -> Result<Key, KeyError> {
        let elem = PathElement::new(kind, id)?;
        let mut path = self.path.clone();
        path.push(elem);
        Ok(Key {
            partition_id: self.partition_id.clone(),
            path,
        })
    }

    /// Appends an incomplete child `kind` to this key, returning an [IncompleteKey].
    pub fn with_incomplete_child(
        &self,
        kind: impl Into<String>,
    ) -> Result<IncompleteKey, KeyError> {
        IncompleteKey::from_ancestors(self.partition_id.clone(), self.path.clone(), kind)
    }

    /// Serializes this key as a `google.datastore.v1.Key` protobuf and encodes it
    /// using URL-safe Base64 without padding.
    pub fn to_url_safe(&self) -> String {
        let partition_id = if self.partition_id == PartitionId::default() {
            None
        } else {
            Some(PartitionIdWire {
                project_id: self.partition_id.project_id.clone(),
                database_id: self.partition_id.database_id.clone(),
                namespace_id: self.partition_id.namespace_id.clone(),
            })
        };
        let path = self
            .path
            .iter()
            .map(|elem| PathElementWire {
                kind: elem.kind.clone(),
                id_type: Some(match &elem.id {
                    KeyId::Id(id) => IdTypeWire::Id(*id),
                    KeyId::Name(name) => IdTypeWire::Name(name.clone()),
                }),
            })
            .collect();
        let wire = KeyWire { partition_id, path };
        URL_SAFE_NO_PAD.encode(wire.encode_to_vec())
    }

    /// Decodes a URL-safe Base64 string (padded or unpadded) into a validated [Key].
    pub fn from_url_safe(encoded: &str) -> Result<Self, KeyError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .or_else(|_| URL_SAFE.decode(encoded))
            .map_err(|e| KeyError::InvalidUrlSafeEncoding(e.to_string()))?;
        let wire = KeyWire::decode(bytes.as_slice())
            .map_err(|e| KeyError::InvalidUrlSafeEncoding(e.to_string()))?;
        let partition_id = match wire.partition_id {
            Some(p) => PartitionId {
                project_id: p.project_id,
                database_id: p.database_id,
                namespace_id: p.namespace_id,
            },
            None => PartitionId::default(),
        };
        if wire.path.is_empty() {
            return Err(KeyError::EmptyPath);
        }
        let len = wire.path.len();
        let mut path = Vec::with_capacity(len);
        for (idx, elem) in wire.path.into_iter().enumerate() {
            let is_ancestor = idx + 1 < len;
            let kind = validate_kind(elem.kind)?;
            let id = match elem.id_type {
                Some(IdTypeWire::Id(id)) => KeyId::id(id)?,
                Some(IdTypeWire::Name(name)) => KeyId::name(name)?,
                None => {
                    return if is_ancestor {
                        Err(KeyError::IncompleteAncestor(kind))
                    } else {
                        Err(KeyError::IncompleteKey(kind))
                    };
                }
            };
            path.push(PathElement { kind, id });
        }
        Ok(Self { partition_id, path })
    }

    pub(crate) fn normalize(&self, default: &PartitionId) -> Result<Key, KeyError> {
        let partition_id = self.partition_id.normalize(default)?;
        Ok(Key {
            partition_id,
            path: self.path.clone(),
        })
    }

    pub(crate) fn to_proto(&self) -> crate::model::Key {
        crate::model::Key::new()
            .set_partition_id(self.partition_id.to_proto())
            .set_path(self.path.iter().map(PathElement::to_proto))
    }

    pub(crate) fn from_proto(proto: crate::model::Key) -> Result<Self, KeyError> {
        if proto.path.is_empty() {
            return Err(KeyError::EmptyPath);
        }
        let partition_id = proto
            .partition_id
            .map(PartitionId::from_proto)
            .unwrap_or_default();
        let len = proto.path.len();
        let mut path = Vec::with_capacity(len);
        for (idx, elem) in proto.path.into_iter().enumerate() {
            path.push(PathElement::from_proto(elem, idx + 1 < len)?);
        }
        Ok(Self { partition_id, path })
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
        let kind = validate_kind(kind.into())?;
        Ok(Self {
            partition_id: PartitionId::default(),
            ancestors: Vec::new(),
            kind,
        })
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
        let kind = validate_kind(kind.into())?;
        Ok(Self {
            partition_id,
            ancestors: ancestors.into_iter().collect(),
            kind,
        })
    }

    /// Replaces the [PartitionId] of this incomplete key.
    pub fn with_partition_id(mut self, partition_id: PartitionId) -> Self {
        self.partition_id = partition_id;
        self
    }

    /// Returns the [PartitionId] of this incomplete key.
    pub fn partition_id(&self) -> &PartitionId {
        &self.partition_id
    }

    /// Returns the complete ancestor path elements preceding the incomplete leaf.
    pub fn ancestors(&self) -> &[PathElement] {
        &self.ancestors
    }

    /// Returns the entity kind of the incomplete leaf element.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Returns the parent [Key] if this incomplete key has at least one ancestor.
    pub fn parent(&self) -> Option<Key> {
        if self.ancestors.is_empty() {
            None
        } else {
            Some(Key {
                partition_id: self.partition_id.clone(),
                path: self.ancestors.clone(),
            })
        }
    }

    /// Completes this key by assigning `id` to its leaf element.
    pub fn with_id(&self, id: KeyId) -> Key {
        let mut path = self.ancestors.clone();
        path.push(PathElement {
            kind: self.kind.clone(),
            id,
        });
        Key {
            partition_id: self.partition_id.clone(),
            path,
        }
    }

    pub(crate) fn normalize(&self, default: &PartitionId) -> Result<IncompleteKey, KeyError> {
        let partition_id = self.partition_id.normalize(default)?;
        Ok(IncompleteKey {
            partition_id,
            ancestors: self.ancestors.clone(),
            kind: self.kind.clone(),
        })
    }

    pub(crate) fn to_proto(&self) -> crate::model::Key {
        let mut path: Vec<crate::model::key::PathElement> =
            self.ancestors.iter().map(PathElement::to_proto).collect();
        path.push(crate::model::key::PathElement::new().set_kind(self.kind.clone()));
        crate::model::Key::new()
            .set_partition_id(self.partition_id.to_proto())
            .set_path(path)
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
        match self {
            Self::Complete(k) => Some(k),
            Self::Incomplete(_) => None,
        }
    }

    /// Returns `Some(&IncompleteKey)` if this key is incomplete.
    pub fn as_incomplete(&self) -> Option<&IncompleteKey> {
        match self {
            Self::Complete(_) => None,
            Self::Incomplete(k) => Some(k),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn normalize(&self, default: &PartitionId) -> Result<MutationKey, KeyError> {
        match self {
            Self::Complete(k) => Ok(Self::Complete(k.normalize(default)?)),
            Self::Incomplete(k) => Ok(Self::Incomplete(k.normalize(default)?)),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn to_proto(&self) -> crate::model::Key {
        match self {
            Self::Complete(k) => k.to_proto(),
            Self::Incomplete(k) => k.to_proto(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn from_proto(proto: crate::model::Key) -> Result<Self, KeyError> {
        if proto.path.is_empty() {
            return Err(KeyError::EmptyPath);
        }
        let partition_id = proto
            .partition_id
            .map(PartitionId::from_proto)
            .unwrap_or_default();
        let len = proto.path.len();
        let mut ancestors = Vec::with_capacity(len);
        let mut iter = proto.path.into_iter();
        for _ in 0..(len - 1) {
            let elem = iter.next().unwrap();
            ancestors.push(PathElement::from_proto(elem, true)?);
        }
        let leaf = iter.next().unwrap();
        if leaf.id_type.is_none() {
            let kind = validate_kind(leaf.kind)?;
            Ok(Self::Incomplete(IncompleteKey {
                partition_id,
                ancestors,
                kind,
            }))
        } else {
            ancestors.push(PathElement::from_proto(leaf, false)?);
            Ok(Self::Complete(Key {
                partition_id,
                path: ancestors,
            }))
        }
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
        Self {
            partition_id,
            ancestors: Vec::new(),
            kind: kind.into(),
        }
    }

    /// Overrides the namespace ID for keys created by this factory.
    pub fn with_namespace_id(mut self, namespace_id: impl Into<String>) -> Self {
        self.partition_id.namespace_id = namespace_id.into();
        self
    }

    /// Sets the parent [Key] for keys created by this factory.
    pub fn with_parent(mut self, parent: &Key) -> Self {
        self.ancestors = parent.path.clone();
        self
    }

    /// Builds an [IncompleteKey] for the configured partition, parent, and kind.
    pub fn incomplete(&self) -> Result<IncompleteKey, KeyError> {
        IncompleteKey::from_ancestors(
            self.partition_id.clone(),
            self.ancestors.clone(),
            self.kind.clone(),
        )
    }

    /// Builds a complete [Key] with the given numeric `id`.
    pub fn with_id(&self, id: i64) -> Result<Key, KeyError> {
        let incomplete = self.incomplete()?;
        Ok(incomplete.with_id(KeyId::id(id)?))
    }

    /// Builds a complete [Key] with the given string `name`.
    pub fn with_name(&self, name: impl Into<String>) -> Result<Key, KeyError> {
        let incomplete = self.incomplete()?;
        Ok(incomplete.with_id(KeyId::name(name)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_id_and_element_validation() {
        assert_eq!(KeyId::id(0), Err(KeyError::ZeroId));
        assert_eq!(KeyId::name(""), Err(KeyError::EmptyName));
        assert_eq!(PathElement::with_id("", 1), Err(KeyError::EmptyKind));
        assert_eq!(PathElement::with_id("Task", 0), Err(KeyError::ZeroId));
        assert_eq!(PathElement::with_name("Task", ""), Err(KeyError::EmptyName));
        assert_eq!(
            Key::from_path(PartitionId::default(), []),
            Err(KeyError::EmptyPath)
        );
        assert_eq!(IncompleteKey::new(""), Err(KeyError::EmptyKind));
    }

    #[test]
    fn key_hierarchy_and_ancestors() -> Result<(), KeyError> {
        let p = PartitionId::new("proj-1")
            .with_database_id("db-1")
            .with_namespace_id("ns-1");
        let org = Key::with_name("Org", "acme")?.with_partition_id(p.clone());
        let team = org.with_child("Team", KeyId::name("eng")?)?;
        let user = team.with_child("User", KeyId::id(42)?)?;

        assert_eq!(user.kind(), "User");
        assert_eq!(user.id_num(), Some(42));
        assert_eq!(user.name(), None);
        assert_eq!(user.parent().as_ref(), Some(&team));
        assert_eq!(team.parent().as_ref(), Some(&org));
        assert_eq!(org.parent(), None);

        assert_eq!(user.ancestors(), vec![org.clone(), team.clone()]);
        assert!(user.has_ancestor(&org));
        assert!(user.has_ancestor(&team));
        assert!(user.has_ancestor(&user));
        assert!(!org.has_ancestor(&user));

        let inc_task = user.with_incomplete_child("Task")?;
        assert_eq!(inc_task.kind(), "Task");
        assert_eq!(inc_task.parent().as_ref(), Some(&user));
        let task = inc_task.with_id(KeyId::id(99)?);
        assert_eq!(task.parent().as_ref(), Some(&user));
        assert_eq!(task.id_num(), Some(99));
        Ok(())
    }

    #[test]
    fn url_safe_key_roundtrip() -> Result<(), KeyError> {
        let p = PartitionId::new("my-project")
            .with_database_id("my-db")
            .with_namespace_id("tenant-a");
        let root = Key::with_name("Org", "acme")?.with_partition_id(p);
        let child = root.with_child("User", KeyId::id(123456789)?)?;

        let token = child.to_url_safe();
        assert!(!token.contains('='));
        assert!(!token.contains('+'));
        assert!(!token.contains('/'));

        let decoded = Key::from_url_safe(&token)?;
        assert_eq!(decoded, child);

        // Also accept padded base64 tokens
        let padded = URL_SAFE.encode(URL_SAFE_NO_PAD.decode(&token).unwrap());
        assert_eq!(Key::from_url_safe(&padded)?, child);

        // Invalid base64 or incomplete key wire bytes should return errors
        assert!(matches!(
            Key::from_url_safe("!!!not-base64!!!"),
            Err(KeyError::InvalidUrlSafeEncoding(_))
        ));
        let incomplete_wire = KeyWire {
            partition_id: None,
            path: vec![PathElementWire {
                kind: "Task".to_string(),
                id_type: None,
            }],
        };
        let incomplete_token = URL_SAFE_NO_PAD.encode(incomplete_wire.encode_to_vec());
        assert_eq!(
            Key::from_url_safe(&incomplete_token),
            Err(KeyError::IncompleteKey("Task".to_string()))
        );
        Ok(())
    }

    #[test]
    fn partition_normalization_and_key_factory() -> Result<(), KeyError> {
        let default_part = PartitionId::new("proj-a")
            .with_database_id("db-a")
            .with_namespace_id("ns-default");

        let bare_key = Key::with_id("Task", 1)?;
        let normalized = bare_key.normalize(&default_part)?;
        assert_eq!(normalized.partition_id(), &default_part);

        let mismatched = Key::with_id("Task", 1)?.with_partition_id(PartitionId::new("proj-b"));
        assert!(matches!(
            mismatched.normalize(&default_part),
            Err(KeyError::PartitionMismatch { .. })
        ));

        let factory = KeyFactory::new(default_part.clone(), "Comment")
            .with_namespace_id("ns-override")
            .with_parent(&normalized);
        let comment_key = factory.with_name("c-1")?;
        assert_eq!(comment_key.partition_id().namespace_id(), "ns-override");
        assert_eq!(comment_key.path().len(), 2);
        assert_eq!(comment_key.kind(), "Comment");
        assert_eq!(comment_key.name(), Some("c-1"));
        Ok(())
    }
}
