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

use crate::client::Datastore;
use crate::entity::Entity;
use crate::error::{KeyError, ValueError};
use crate::key::{IntoMutationKey, Key, MutationKey, PartitionId};
use crate::value::{IntoValue, Value};
use crate::{Error, Result};
use bytes::Bytes;
use std::collections::HashMap;

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

    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> std::result::Result<crate::model::PropertyTransform, ValueError> {
        let base = crate::model::PropertyTransform::new().set_property(self.property.clone());
        let proto = match &self.op {
            TransformOp::SetToServerValue(ServerValue::RequestTime) => base
                .set_set_to_server_value(
                    crate::model::property_transform::ServerValue::RequestTime,
                ),
            TransformOp::Increment(v) => base.set_increment(v.to_proto(default_partition)?),
            TransformOp::Maximum(v) => base.set_maximum(v.to_proto(default_partition)?),
            TransformOp::Minimum(v) => base.set_minimum(v.to_proto(default_partition)?),
            TransformOp::AppendMissingElements(values) => {
                let mut proto_vals = Vec::with_capacity(values.len());
                for v in values {
                    proto_vals.push(v.to_proto(default_partition)?);
                }
                base.set_append_missing_elements(
                    crate::model::ArrayValue::new().set_values(proto_vals),
                )
            }
            TransformOp::RemoveAllFromArray(values) => {
                let mut proto_vals = Vec::with_capacity(values.len());
                for v in values {
                    proto_vals.push(v.to_proto(default_partition)?);
                }
                base.set_remove_all_from_array(
                    crate::model::ArrayValue::new().set_values(proto_vals),
                )
            }
        };
        Ok(proto)
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

    pub(crate) fn to_proto(&self) -> crate::model::PropertyMask {
        crate::model::PropertyMask::new().set_paths(self.paths.clone())
    }
}

/// A single entity write operation (`Insert`, `Update`, `Upsert`, or `Delete`).
#[derive(Clone, Debug, PartialEq)]
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

    pub(crate) fn to_proto(
        &self,
        default_partition: &PartitionId,
    ) -> std::result::Result<(crate::model::Mutation, Option<Key>), ValueError> {
        let mut proto = crate::model::Mutation::new();
        let known_complete_key = match &self.kind {
            MutationKind::Insert(entity) => {
                let complete = Self::extract_normalized_complete_key(entity, default_partition)?;
                proto = proto.set_insert(entity.to_proto(default_partition)?);
                complete
            }
            MutationKind::Update(entity) => {
                let complete = Self::extract_normalized_complete_key(entity, default_partition)?;
                proto = proto.set_update(entity.to_proto(default_partition)?);
                complete
            }
            MutationKind::Upsert(entity) => {
                let complete = Self::extract_normalized_complete_key(entity, default_partition)?;
                proto = proto.set_upsert(entity.to_proto(default_partition)?);
                complete
            }
            MutationKind::Delete(key) => {
                let normalized = key.normalize(default_partition)?;
                proto = proto.set_delete(normalized.to_proto());
                Some(normalized)
            }
        };

        if let Some(base_version) = self.base_version {
            proto = proto.set_base_version(base_version);
        }
        if let Some(ref mask) = self.property_mask {
            proto = proto.set_property_mask(mask.to_proto());
        }
        if !self.property_transforms.is_empty() {
            let mut transforms = Vec::with_capacity(self.property_transforms.len());
            for t in &self.property_transforms {
                transforms.push(t.to_proto(default_partition)?);
            }
            proto = proto.set_property_transforms(transforms);
        }

        Ok((proto, known_complete_key))
    }

    fn extract_normalized_complete_key(
        entity: &Entity,
        default_partition: &PartitionId,
    ) -> std::result::Result<Option<Key>, KeyError> {
        match &entity.key {
            Some(MutationKey::Complete(k)) => Ok(Some(k.normalize(default_partition)?)),
            Some(MutationKey::Incomplete(_)) | None => Ok(None),
        }
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

impl MutationResult {
    pub(crate) fn from_proto(
        proto: crate::model::MutationResult,
        fallback_key: Option<Key>,
    ) -> std::result::Result<Self, ValueError> {
        let key = match proto.key {
            Some(pk) => Some(Key::from_proto(pk)?),
            None => fallback_key,
        };
        let mut transform_results = Vec::with_capacity(proto.transform_results.len());
        for v in proto.transform_results {
            transform_results.push(Value::from_proto(v)?);
        }
        Ok(Self {
            key,
            version: proto.version,
            create_time: proto.create_time,
            update_time: proto.update_time,
            conflict_detected: proto.conflict_detected,
            transform_results,
        })
    }
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

#[derive(Clone, Debug)]
pub(crate) enum CommitMode {
    NonTransactional,
    Transaction(Bytes),
    SingleUseTransaction(crate::model::TransactionOptions),
}

pub(crate) async fn execute_commit(
    client: &Datastore,
    mutations: &[Mutation],
    mode: CommitMode,
    options: crate::RequestOptions,
) -> Result<CommitResponse> {
    let default_partition = client.partition_id();
    let mut wire_mutations: Vec<crate::model::Mutation> = Vec::with_capacity(mutations.len());
    let mut orig_mapping: Vec<(usize, Option<Key>)> = Vec::with_capacity(mutations.len());
    let mut seen_deletes: HashMap<(Key, Option<i64>), usize> = HashMap::new();

    for mutation in mutations {
        let (proto_mut, complete_key) = mutation
            .to_proto(&default_partition)
            .map_err(|e| Error::ser(Box::new(e)))?;

        if let (MutationKind::Delete(_), Some(norm_key)) = (&mutation.kind, &complete_key) {
            let delete_dedup_key = (norm_key.clone(), mutation.base_version);
            if let Some(&existing_wire_idx) = seen_deletes.get(&delete_dedup_key) {
                orig_mapping.push((existing_wire_idx, complete_key));
                continue;
            }
            let wire_idx = wire_mutations.len();
            seen_deletes.insert(delete_dedup_key, wire_idx);
            wire_mutations.push(proto_mut);
            orig_mapping.push((wire_idx, complete_key));
            continue;
        }

        let wire_idx = wire_mutations.len();
        wire_mutations.push(proto_mut);
        orig_mapping.push((wire_idx, complete_key));
    }

    let mut req = client
        .inner
        .gapic
        .commit()
        .set_project_id(client.project_id())
        .set_database_id(client.database_id())
        .set_mutations(wire_mutations)
        .with_options(options);

    req = match mode {
        CommitMode::NonTransactional => {
            req.set_mode(crate::model::commit_request::Mode::NonTransactional)
        }
        CommitMode::Transaction(tx_id) => req
            .set_mode(crate::model::commit_request::Mode::Transactional)
            .set_transaction(tx_id),
        CommitMode::SingleUseTransaction(tx_opts) => req
            .set_mode(crate::model::commit_request::Mode::Transactional)
            .set_single_use_transaction(tx_opts),
    };

    let response = req.send().await?;
    let wire_results = response.mutation_results;

    let mut mutation_results = Vec::with_capacity(orig_mapping.len());
    for (wire_idx, fallback_key) in orig_mapping {
        let proto_res = wire_results.get(wire_idx).cloned().unwrap_or_default();
        let res = MutationResult::from_proto(proto_res, fallback_key)
            .map_err(|e| Error::deser(Box::new(e)))?;
        mutation_results.push(res);
    }

    Ok(CommitResponse {
        mutation_results,
        index_updates: response.index_updates,
        commit_time: response.commit_time,
    })
}

/// Request builder for [`Datastore::insert`].
#[derive(Clone, Debug)]
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
        let mut resp = execute_commit(
            &self.client,
            std::slice::from_ref(&self.mutation),
            CommitMode::NonTransactional,
            self.options,
        )
        .await?;
        resp.mutation_results
            .pop()
            .and_then(|r| r.key)
            .ok_or_else(|| Error::deser("CommitResponse did not contain a key for insert mutation"))
    }
}

/// Request builder for [`Datastore::upsert`].
#[derive(Clone, Debug)]
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
        let mut resp = execute_commit(
            &self.client,
            std::slice::from_ref(&self.mutation),
            CommitMode::NonTransactional,
            self.options,
        )
        .await?;
        resp.mutation_results
            .pop()
            .and_then(|r| r.key)
            .ok_or_else(|| Error::deser("CommitResponse did not contain a key for upsert mutation"))
    }
}

/// Request builder for [`Datastore::update`].
#[derive(Clone, Debug)]
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
        let mut resp = execute_commit(
            &self.client,
            std::slice::from_ref(&self.mutation),
            CommitMode::NonTransactional,
            self.options,
        )
        .await?;
        resp.mutation_results
            .pop()
            .ok_or_else(|| Error::deser("CommitResponse did not contain a MutationResult"))
    }
}

/// Request builder for [`Datastore::delete`].
#[derive(Clone, Debug)]
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
        let _ = execute_commit(
            &self.client,
            std::slice::from_ref(&self.mutation),
            CommitMode::NonTransactional,
            self.options,
        )
        .await?;
        Ok(())
    }
}

/// Request builder for [`Datastore::mutate`].
#[derive(Clone, Debug)]
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
        if self.mutations.is_empty() {
            return Ok(CommitResponse {
                mutation_results: Vec::new(),
                index_updates: 0,
                commit_time: None,
            });
        }
        execute_commit(
            &self.client,
            &self.mutations,
            CommitMode::NonTransactional,
            self.options,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::gapic_dataplane::stub::Datastore as DatastoreStub;
    use crate::model::{
        CommitRequest, CommitResponse as ProtoCommitResponse, MutationResult as ProtoMutationResult,
    };
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::response::Response;

    mockall::mock! {
        #[derive(Debug)]
        DatastoreMock {}
        impl DatastoreStub for DatastoreMock {
            async fn commit(
                &self,
                req: CommitRequest,
                options: RequestOptions,
            ) -> Result<Response<ProtoCommitResponse>>;
        }
    }

    fn test_client(mock: MockDatastoreMock) -> Datastore {
        Datastore::from_stub("test-proj", "test-db", "test-ns", mock)
    }

    #[tokio::test]
    async fn insert_with_incomplete_and_complete_keys() {
        let mut mock = MockDatastoreMock::new();

        // 1. Incomplete key insert -> server returns allocated key
        mock.expect_commit()
            .times(1)
            .withf(|req, _| {
                req.project_id == "test-proj"
                    && req.database_id == "test-db"
                    && req.mode == crate::model::commit_request::Mode::NonTransactional
                    && req.mutations.len() == 1
                    && req.mutations[0].insert().is_some()
            })
            .returning(|_, _| {
                let allocated_key = Key::with_id("Task", 98765)
                    .expect("valid key")
                    .with_partition_id(
                        PartitionId::new("test-proj")
                            .with_database_id("test-db")
                            .with_namespace_id("test-ns"),
                    );
                let resp = ProtoCommitResponse::new()
                    .set_mutation_results([ProtoMutationResult::new()
                        .set_key(allocated_key.to_proto())
                        .set_version(1)])
                    .set_index_updates(3);
                Ok(Response::from(resp))
            });

        // 2. Complete key insert -> server returns key: None, client returns normalized input key
        mock.expect_commit()
            .times(1)
            .withf(|req, _| req.mutations.len() == 1 && req.mutations[0].insert().is_some())
            .returning(|_, _| {
                let resp = ProtoCommitResponse::new()
                    .set_mutation_results([ProtoMutationResult::new().set_version(2)]);
                Ok(Response::from(resp))
            });

        let client = test_client(mock);
        let kf = client.key_factory("Task");

        let mut entity = Entity::new();
        entity.set("title", "Write tests");
        let allocated = client
            .insert(
                kf.incomplete().expect("valid incomplete key"),
                entity.clone(),
            )
            .send()
            .await
            .expect("insert incomplete succeeds");
        assert_eq!(allocated.id_num(), Some(98765));
        assert_eq!(allocated.partition_id().project_id(), "test-proj");

        let named_key = Key::with_name("Task", "explicit-id").expect("valid key");
        let returned_key = client
            .insert(named_key, entity)
            .send()
            .await
            .expect("insert complete succeeds");
        assert_eq!(returned_key.name(), Some("explicit-id"));
        assert_eq!(returned_key.partition_id().project_id(), "test-proj");
        assert_eq!(returned_key.partition_id().database_id(), "test-db");
        assert_eq!(returned_key.partition_id().namespace_id(), "test-ns");
    }

    #[tokio::test]
    async fn update_with_mask_base_version_and_property_transforms() {
        let mut mock = MockDatastoreMock::new();

        mock.expect_commit()
            .times(1)
            .withf(|req, _| {
                if req.mutations.len() != 1 {
                    return false;
                }
                let m = &req.mutations[0];
                let mask_ok = m
                    .property_mask
                    .as_ref()
                    .is_some_and(|pm| pm.paths == vec!["title".to_string()]);
                let version_ok = m.base_version() == Some(&5);
                let transforms_ok = m.property_transforms.len() == 3
                    && m.property_transforms[0].property == "updated_at"
                    && m.property_transforms[0].set_to_server_value()
                        == Some(&crate::model::property_transform::ServerValue::RequestTime)
                    && m.property_transforms[1].property == "views"
                    && m.property_transforms[1].increment().is_some()
                    && m.property_transforms[2].property == "tags"
                    && m.property_transforms[2].append_missing_elements().is_some();
                m.update().is_some() && mask_ok && version_ok && transforms_ok
            })
            .returning(|_, _| {
                let ts = wkt::Timestamp::clamp(1_700_000_123, 0);
                let resp =
                    ProtoCommitResponse::new().set_mutation_results([ProtoMutationResult::new()
                        .set_version(6)
                        .set_update_time(ts)
                        .set_transform_results([
                            ts.into_value().to_proto(&PartitionId::default()).unwrap(),
                            11_i64
                                .into_value()
                                .to_proto(&PartitionId::default())
                                .unwrap(),
                            Value::null().to_proto(&PartitionId::default()).unwrap(),
                        ])]);
                Ok(Response::from(resp))
            });

        let client = test_client(mock);
        let key = client.key_factory("Post").with_id(42).expect("valid key");
        let mut entity = Entity::new();
        entity.set("title", "Updated Title");

        let result = client
            .update(key.clone(), entity)
            .with_base_version(5)
            .with_property_mask(PropertyMask::new(["title"]))
            .add_property_transform(PropertyTransform::set_to_server_value(
                "updated_at",
                ServerValue::RequestTime,
            ))
            .add_property_transform(PropertyTransform::increment("views", 1_i64))
            .add_property_transform(PropertyTransform::append_missing_elements(
                "tags",
                ["rust", "datastore"],
            ))
            .send()
            .await
            .expect("update succeeds");

        assert_eq!(result.key, Some(key));
        assert_eq!(result.version, 6);
        assert_eq!(result.transform_results.len(), 3);
        assert_eq!(
            crate::value::FromValue::from_value(&result.transform_results[1]).ok(),
            Some(11_i64)
        );
    }

    #[tokio::test]
    async fn mutate_deduplicates_duplicate_deletes_and_maps_results() {
        let mut mock = MockDatastoreMock::new();

        // Caller passes 3 mutations: [Delete(k1), Upsert(k2), Delete(k1)]
        // Only 2 mutations should be sent on the wire: [Delete(k1), Upsert(k2)].
        mock.expect_commit()
            .times(1)
            .withf(|req, _| {
                req.mutations.len() == 2
                    && req.mutations[0].delete().is_some()
                    && req.mutations[1].upsert().is_some()
            })
            .returning(|_, _| {
                let resp = ProtoCommitResponse::new()
                    .set_mutation_results([
                        ProtoMutationResult::new().set_version(10),
                        ProtoMutationResult::new().set_version(20),
                    ])
                    .set_index_updates(2);
                Ok(Response::from(resp))
            });

        let client = test_client(mock);
        let kf = client.key_factory("Item");
        let k1 = kf.with_name("item-1").expect("valid key");
        let k2 = kf.with_name("item-2").expect("valid key");
        let mut e2 = Entity::new();
        e2.set("qty", 5_i64);

        let resp = client
            .mutate([
                Mutation::delete(k1.clone()),
                Mutation::upsert(k2.clone(), e2),
                Mutation::delete(k1.clone()),
            ])
            .send()
            .await
            .expect("mutate succeeds");

        assert_eq!(resp.mutation_results.len(), 3);
        assert_eq!(resp.mutation_results[0].version, 10);
        assert_eq!(resp.mutation_results[0].key, Some(k1.clone()));
        assert_eq!(resp.mutation_results[1].version, 20);
        assert_eq!(resp.mutation_results[1].key, Some(k2));
        assert_eq!(resp.mutation_results[2].version, 10);
        assert_eq!(resp.mutation_results[2].key, Some(k1));
    }
}
