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

//! Mutations, server-side property transforms, property masks, and non-transactional writes.

use crate::Result;
use crate::client::Datastore;
use crate::entity::Entity;
use crate::key::{IntoMutationKey, Key};
use crate::value::{IntoValue, Value};

/// The operation performed by a [`Mutation`].
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum MutationKind {
    /// Inserts a new entity (fails with `ALREADY_EXISTS` if the key already exists).
    Insert(Entity),
    /// Updates an existing entity (fails with `NOT_FOUND` if the key does not exist).
    Update(Entity),
    /// Inserts or replaces an entity regardless of whether it already exists.
    Upsert(Entity),
    /// Deletes the entity at the given complete [`Key`].
    Delete(Key),
}

/// Server-computed value for [`PropertyTransform::set_to_server_value`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerValue {
    /// Sets the property to the time the server receives the commit request.
    RequestTime,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TransformOp {
    SetToServerValue(ServerValue),
    Increment(Value),
    Maximum(Value),
    Minimum(Value),
    AppendMissingElements(Vec<Value>),
    RemoveAllFromArray(Vec<Value>),
}

/// An atomic server-side transformation applied to a single property during a commit.
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub struct PropertyTransform {
    pub(crate) property: String,
    pub(crate) op: TransformOp,
}

impl PropertyTransform {
    /// Sets `property` to a server-computed value (such as [`ServerValue::RequestTime`]).
    pub fn set_to_server_value(property: impl Into<String>, value: ServerValue) -> Self {
        Self {
            property: property.into(),
            op: TransformOp::SetToServerValue(value),
        }
    }

    /// Atomically adds a numeric `amount` (`Integer` or `Double`) to `property`.
    pub fn increment(property: impl Into<String>, amount: impl IntoValue) -> Self {
        Self {
            property: property.into(),
            op: TransformOp::Increment(amount.into_value()),
        }
    }

    /// Atomically sets `property` to the maximum of its current value and `value`.
    pub fn maximum(property: impl Into<String>, value: impl IntoValue) -> Self {
        Self {
            property: property.into(),
            op: TransformOp::Maximum(value.into_value()),
        }
    }

    /// Atomically sets `property` to the minimum of its current value and `value`.
    pub fn minimum(property: impl Into<String>, value: impl IntoValue) -> Self {
        Self {
            property: property.into(),
            op: TransformOp::Minimum(value.into_value()),
        }
    }

    /// Atomically appends any elements in `values` that are not already present in the array `property`.
    pub fn append_missing_elements<I, V>(property: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: IntoValue,
    {
        Self {
            property: property.into(),
            op: TransformOp::AppendMissingElements(
                values.into_iter().map(IntoValue::into_value).collect(),
            ),
        }
    }

    /// Atomically removes all occurrences of `values` from the array `property`.
    pub fn remove_all_from_array<I, V>(property: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: IntoValue,
    {
        Self {
            property: property.into(),
            op: TransformOp::RemoveAllFromArray(
                values.into_iter().map(IntoValue::into_value).collect(),
            ),
        }
    }

    /// Returns the target property path of this transform.
    pub fn property(&self) -> &str {
        &self.property
    }
}

/// A mask of property paths restricting which properties an `Update` or `Upsert` mutation modifies.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PropertyMask {
    pub(crate) paths: Vec<String>,
}

impl PropertyMask {
    /// Creates a [`PropertyMask`] from an iterator of property paths.
    pub fn new<I, S>(paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            paths: paths.into_iter().map(Into::into).collect(),
        }
    }

    /// Creates an empty [`PropertyMask`] (used for transform-only updates that preserve existing properties).
    pub fn empty() -> Self {
        Self { paths: Vec::new() }
    }

    /// Returns the property paths in this mask.
    pub fn paths(&self) -> &[String] {
        &self.paths
    }
}

/// A single entity write operation (`Insert`, `Update`, `Upsert`, or `Delete`).
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
pub struct Mutation {
    pub(crate) kind: MutationKind,
    pub(crate) base_version: Option<i64>,
    pub(crate) property_mask: Option<PropertyMask>,
    pub(crate) property_transforms: Vec<PropertyTransform>,
}

impl Mutation {
    /// Creates an `Insert` mutation for `key` and `entity` (accepts either a [`Key`] or [`IncompleteKey`][crate::key::IncompleteKey]).
    pub fn insert(key: impl IntoMutationKey, mut entity: Entity) -> Self {
        entity.key = Some(key.into_mutation_key());
        Self {
            kind: MutationKind::Insert(entity),
            base_version: None,
            property_mask: None,
            property_transforms: Vec::new(),
        }
    }

    /// Creates an `Upsert` mutation for `key` and `entity` (accepts either a [`Key`] or [`IncompleteKey`][crate::key::IncompleteKey]).
    pub fn upsert(key: impl IntoMutationKey, mut entity: Entity) -> Self {
        entity.key = Some(key.into_mutation_key());
        Self {
            kind: MutationKind::Upsert(entity),
            base_version: None,
            property_mask: None,
            property_transforms: Vec::new(),
        }
    }

    /// Creates an `Update` mutation for a complete [`Key`] and `entity`.
    pub fn update(key: Key, mut entity: Entity) -> Self {
        entity.key = Some(key.into_mutation_key());
        Self {
            kind: MutationKind::Update(entity),
            base_version: None,
            property_mask: None,
            property_transforms: Vec::new(),
        }
    }

    /// Creates a `Delete` mutation for a complete [`Key`].
    pub fn delete(key: Key) -> Self {
        Self {
            kind: MutationKind::Delete(key),
            base_version: None,
            property_mask: None,
            property_transforms: Vec::new(),
        }
    }

    /// Sets an optimistic concurrency `base_version` check on this mutation.
    pub fn with_base_version(mut self, base_version: i64) -> Self {
        self.base_version = Some(base_version);
        self
    }

    /// Restricts the modified properties using a [`PropertyMask`].
    pub fn with_property_mask(mut self, mask: PropertyMask) -> Self {
        self.property_mask = Some(mask);
        self
    }

    /// Attaches server-side [`PropertyTransform`]s to this mutation.
    pub fn with_property_transforms<I>(mut self, transforms: I) -> Self
    where
        I: IntoIterator<Item = PropertyTransform>,
    {
        self.property_transforms = transforms.into_iter().collect();
        self
    }

    /// Appends a single server-side [`PropertyTransform`] to this mutation.
    pub fn add_property_transform(mut self, transform: PropertyTransform) -> Self {
        self.property_transforms.push(transform);
        self
    }

    /// Returns the [`MutationKind`] of this mutation.
    pub fn kind(&self) -> &MutationKind {
        &self.kind
    }
}

/// The server result for a single [`Mutation`] within a [`CommitResponse`].
#[derive(Clone, Debug, PartialEq)]
pub struct MutationResult {
    /// The complete [`Key`] of the entity (populated when an [`IncompleteKey`][crate::key::IncompleteKey] was allocated, or resolved from the request).
    pub key: Option<Key>,
    /// The version of the entity on the server after processing the mutation.
    pub version: i64,
    /// The creation timestamp of the entity, if reported by the server.
    pub create_time: Option<wkt::Timestamp>,
    /// The update timestamp of the entity after processing the mutation.
    pub update_time: Option<wkt::Timestamp>,
    /// Whether a conflict was detected for this mutation.
    pub conflict_detected: bool,
    /// The results of any [`PropertyTransform`]s applied in the mutation, in request order.
    pub transform_results: Vec<Value>,
}

/// The response returned by committing one or more mutations.
#[derive(Clone, Debug, PartialEq)]
pub struct CommitResponse {
    /// Results corresponding to each committed mutation.
    pub mutation_results: Vec<MutationResult>,
    /// Total number of index entries updated during the commit.
    pub index_updates: i32,
    /// The timestamp at which the commit was finalized by the server.
    pub commit_time: Option<wkt::Timestamp>,
}

/// Request builder for [`Datastore::insert`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct InsertBuilder {
    pub(crate) client: Datastore,
    pub(crate) mutation: Mutation,
    pub(crate) options: crate::RequestOptions,
}

impl InsertBuilder {
    /// Sets an optimistic concurrency `base_version` check.
    pub fn with_base_version(mut self, base_version: i64) -> Self {
        self.mutation = self.mutation.with_base_version(base_version);
        self
    }

    /// Appends a server-side [`PropertyTransform`] to this insert.
    pub fn add_property_transform(mut self, transform: PropertyTransform) -> Self {
        self.mutation = self.mutation.add_property_transform(transform);
        self
    }

    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Commits the insert in `NON_TRANSACTIONAL` mode and returns the complete [`Key`]
    /// (either newly allocated by the server or the input complete key).
    pub async fn send(self) -> Result<Key> {
        unimplemented!()
    }
}

/// Request builder for [`Datastore::upsert`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct UpsertBuilder {
    pub(crate) client: Datastore,
    pub(crate) mutation: Mutation,
    pub(crate) options: crate::RequestOptions,
}

impl UpsertBuilder {
    /// Sets an optimistic concurrency `base_version` check.
    pub fn with_base_version(mut self, base_version: i64) -> Self {
        self.mutation = self.mutation.with_base_version(base_version);
        self
    }

    /// Restricts the modified properties using a [`PropertyMask`].
    pub fn with_property_mask(mut self, mask: PropertyMask) -> Self {
        self.mutation = self.mutation.with_property_mask(mask);
        self
    }

    /// Appends a server-side [`PropertyTransform`] to this upsert.
    pub fn add_property_transform(mut self, transform: PropertyTransform) -> Self {
        self.mutation = self.mutation.add_property_transform(transform);
        self
    }

    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Commits the upsert in `NON_TRANSACTIONAL` mode and returns the complete [`Key`]
    /// (either newly allocated by the server or the input complete key).
    pub async fn send(self) -> Result<Key> {
        unimplemented!()
    }
}

/// Request builder for [`Datastore::update`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct UpdateBuilder {
    pub(crate) client: Datastore,
    pub(crate) mutation: Mutation,
    pub(crate) options: crate::RequestOptions,
}

impl UpdateBuilder {
    /// Sets an optimistic concurrency `base_version` check.
    pub fn with_base_version(mut self, base_version: i64) -> Self {
        self.mutation = self.mutation.with_base_version(base_version);
        self
    }

    /// Restricts the modified properties using a [`PropertyMask`].
    pub fn with_property_mask(mut self, mask: PropertyMask) -> Self {
        self.mutation = self.mutation.with_property_mask(mask);
        self
    }

    /// Appends a server-side [`PropertyTransform`] to this update.
    pub fn add_property_transform(mut self, transform: PropertyTransform) -> Self {
        self.mutation = self.mutation.add_property_transform(transform);
        self
    }

    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Commits the update in `NON_TRANSACTIONAL` mode and returns the [`MutationResult`].
    pub async fn send(self) -> Result<MutationResult> {
        unimplemented!()
    }
}

/// Request builder for [`Datastore::delete`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct DeleteBuilder {
    pub(crate) client: Datastore,
    pub(crate) mutation: Mutation,
    pub(crate) options: crate::RequestOptions,
}

impl DeleteBuilder {
    /// Sets an optimistic concurrency `base_version` check.
    pub fn with_base_version(mut self, base_version: i64) -> Self {
        self.mutation = self.mutation.with_base_version(base_version);
        self
    }

    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Commits the delete in `NON_TRANSACTIONAL` mode.
    pub async fn send(self) -> Result<()> {
        unimplemented!()
    }
}

/// Request builder for [`Datastore::mutate`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct MutateBuilder {
    pub(crate) client: Datastore,
    pub(crate) mutations: Vec<Mutation>,
    pub(crate) options: crate::RequestOptions,
}

impl MutateBuilder {
    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Commits the batch of mutations in `NON_TRANSACTIONAL` mode (deduplicating duplicate `Delete` keys).
    pub async fn send(self) -> Result<CommitResponse> {
        unimplemented!()
    }
}
