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

//! The Cloud Datastore data-plane client.

use crate::ClientBuilderResult;
use crate::Result;
use crate::aggregation::{
    AggregationQuery, RunAggregationGqlQueryBuilder, RunAggregationQueryBuilder,
};
use crate::builder::{AllocateIdsBuilder, ClientBuilder, ReserveIdsBuilder};
use crate::entity::{FromEntity, IntoEntity};
use crate::error::KeyError;
use crate::generated::gapic_dataplane;
use crate::gql::{GqlQuery, RunGqlQueryBuilder};
use crate::key::{IncompleteKey, IntoMutationKey, Key, KeyFactory, PartitionId};
use crate::mutation::{
    DeleteBuilder, InsertBuilder, MutateBuilder, Mutation, UpdateBuilder, UpsertBuilder,
};
use crate::query::{Query, RunQueryBuilder};
use crate::read::{GetBuilder, GetMultiBuilder, ReadOptions};
use crate::transaction::{ReadOnlyTransactionBuilder, TransactionRunnerBuilder};
use std::sync::Arc;

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct DatastoreInner {
    pub(crate) project_id: String,
    pub(crate) database_id: String,
    pub(crate) namespace_id: String,
    pub(crate) gapic: gapic_dataplane::client::Datastore,
}

/// Client for interacting with Google Cloud Datastore (`google.datastore.v1`).
///
/// `Datastore` holds an internal connection pool and is cheap to `Clone`.
#[derive(Clone, Debug)]
pub struct Datastore {
    pub(crate) inner: Arc<DatastoreInner>,
}

impl Datastore {
    /// Returns a [`ClientBuilder`] for constructing a [`Datastore`] client bound to `project_id`.
    pub fn builder(project_id: impl Into<String>) -> ClientBuilder {
        ClientBuilder::new(project_id)
    }

    pub(crate) async fn from_builder(builder: ClientBuilder) -> ClientBuilderResult<Self> {
        let gapic = gapic_dataplane::client::Datastore::new(builder.config).await?;
        Ok(Self {
            inner: Arc::new(DatastoreInner {
                project_id: builder.project_id,
                database_id: builder.database_id,
                namespace_id: builder.namespace_id,
                gapic,
            }),
        })
    }

    /// Creates a [`Datastore`] client backed by a custom stub implementation (used in unit tests).
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn from_stub<T>(
        project_id: impl Into<String>,
        database_id: impl Into<String>,
        namespace_id: impl Into<String>,
        stub: T,
    ) -> Self
    where
        T: gapic_dataplane::stub::Datastore + 'static,
    {
        Self {
            inner: Arc::new(DatastoreInner {
                project_id: project_id.into(),
                database_id: database_id.into(),
                namespace_id: namespace_id.into(),
                gapic: gapic_dataplane::client::Datastore::from_stub(stub),
            }),
        }
    }

    /// Returns the Google Cloud project ID bound to this client.
    pub fn project_id(&self) -> &str {
        &self.inner.project_id
    }

    /// Returns the Datastore database ID bound to this client (`""` for the default database).
    pub fn database_id(&self) -> &str {
        &self.inner.database_id
    }

    /// Returns the default multi-tenant namespace ID bound to this client (`""` for default).
    pub fn namespace_id(&self) -> &str {
        &self.inner.namespace_id
    }

    /// Returns the default [PartitionId] configured on this client.
    pub fn partition_id(&self) -> PartitionId {
        PartitionId {
            project_id: self.inner.project_id.clone(),
            database_id: self.inner.database_id.clone(),
            namespace_id: self.inner.namespace_id.clone(),
        }
    }

    /// Returns a [KeyFactory] pre-configured with this client's partition context.
    pub fn key_factory(&self, kind: impl Into<String>) -> KeyFactory {
        KeyFactory::new(self.partition_id(), kind)
    }

    /// Allocates unique numeric IDs from the server for one or more [IncompleteKey]s.
    pub fn allocate_ids<I>(&self, keys: I) -> AllocateIdsBuilder
    where
        I: IntoIterator<Item = IncompleteKey>,
    {
        AllocateIdsBuilder {
            client: self.clone(),
            keys: keys.into_iter().collect(),
            options: crate::RequestOptions::default(),
        }
    }

    /// Reserves the IDs of the given complete [Key]s so Datastore's auto-ID generator
    /// will not assign them to future entities.
    pub fn reserve_ids<I>(&self, keys: I) -> ReserveIdsBuilder
    where
        I: IntoIterator<Item = Key>,
    {
        ReserveIdsBuilder {
            client: self.clone(),
            keys: keys.into_iter().collect(),
            options: crate::RequestOptions::default(),
        }
    }

    /// Reserves `count` sequential numeric IDs starting at `start_key`'s integer ID.
    pub fn reserve_ids_sequential(
        &self,
        start_key: Key,
        count: usize,
    ) -> std::result::Result<ReserveIdsBuilder, KeyError> {
        let _ = (start_key, count);
        unimplemented!()
    }

    /// Creates a [`GetBuilder`] to look up a single [`Entity`][crate::entity::Entity] by [`Key`].
    pub fn get(&self, key: impl Into<Key>) -> GetBuilder {
        GetBuilder {
            client: self.clone(),
            key: key.into(),
            read_options: ReadOptions::default(),
            options: crate::RequestOptions::default(),
        }
    }

    /// Looks up a single entity by [`Key`] and deserializes it into `T` via [`FromEntity`].
    pub async fn get_as<T: FromEntity>(&self, key: impl Into<Key>) -> Result<Option<T>> {
        self.get(key).send_as::<T>().await
    }

    /// Creates a [`GetMultiBuilder`] to look up multiple entities by [`Key`] in a single batch,
    /// preserving input key order and automatically retrying any server-reported `deferred` keys.
    pub fn get_multi<I, K>(&self, keys: I) -> GetMultiBuilder
    where
        I: IntoIterator<Item = K>,
        K: Into<Key>,
    {
        GetMultiBuilder {
            client: self.clone(),
            keys: keys.into_iter().map(Into::into).collect(),
            read_options: ReadOptions::default(),
            options: crate::RequestOptions::default(),
        }
    }

    /// Looks up multiple entities by [`Key`] and deserializes each found entity into `T` via [`FromEntity`].
    pub async fn get_multi_as<T, I, K>(&self, keys: I) -> Result<Vec<Option<T>>>
    where
        T: FromEntity,
        I: IntoIterator<Item = K>,
        K: Into<Key>,
    {
        self.get_multi(keys).send_as::<T>().await
    }

    /// Creates an [`InsertBuilder`] to insert a new entity at `key` (accepting either a [`Key`] or [`IncompleteKey`]).
    pub fn insert(&self, key: impl IntoMutationKey, entity: impl IntoEntity) -> InsertBuilder {
        InsertBuilder {
            client: self.clone(),
            mutation: Mutation::insert(key, entity.into_entity()),
            options: crate::RequestOptions::default(),
        }
    }

    /// Creates an [`UpsertBuilder`] to insert or replace an entity at `key` (accepting either a [`Key`] or [`IncompleteKey`]).
    pub fn upsert(&self, key: impl IntoMutationKey, entity: impl IntoEntity) -> UpsertBuilder {
        UpsertBuilder {
            client: self.clone(),
            mutation: Mutation::upsert(key, entity.into_entity()),
            options: crate::RequestOptions::default(),
        }
    }

    /// Creates an [`UpdateBuilder`] to update an existing entity at a complete [`Key`].
    pub fn update(&self, key: impl Into<Key>, entity: impl IntoEntity) -> UpdateBuilder {
        UpdateBuilder {
            client: self.clone(),
            mutation: Mutation::update(key.into(), entity.into_entity()),
            options: crate::RequestOptions::default(),
        }
    }

    /// Creates a [`DeleteBuilder`] to delete the entity at a complete [`Key`].
    pub fn delete(&self, key: impl Into<Key>) -> DeleteBuilder {
        DeleteBuilder {
            client: self.clone(),
            mutation: Mutation::delete(key.into()),
            options: crate::RequestOptions::default(),
        }
    }

    /// Creates a [`MutateBuilder`] to commit a batch of [`Mutation`]s in `NON_TRANSACTIONAL` mode.
    pub fn mutate<I>(&self, mutations: I) -> MutateBuilder
    where
        I: IntoIterator<Item = Mutation>,
    {
        MutateBuilder {
            client: self.clone(),
            mutations: mutations.into_iter().collect(),
            options: crate::RequestOptions::default(),
        }
    }

    /// Returns a [`TransactionRunnerBuilder`] for executing a retryable read-write transaction.
    pub fn transaction(&self) -> TransactionRunnerBuilder {
        TransactionRunnerBuilder {
            client: self.clone(),
            max_attempts: 5,
            backoff_policy: None,
            options: crate::RequestOptions::default(),
        }
    }

    /// Returns a [`ReadOnlyTransactionBuilder`] for executing a consistent read-only snapshot transaction.
    pub fn read_only_transaction(&self) -> ReadOnlyTransactionBuilder {
        ReadOnlyTransactionBuilder {
            client: self.clone(),
            read_time: None,
            options: crate::RequestOptions::default(),
        }
    }

    /// Creates a [`RunQueryBuilder`] to execute a structured [`Query`].
    pub fn run_query(&self, query: Query) -> RunQueryBuilder {
        RunQueryBuilder {
            client: self.clone(),
            query,
            namespace_id: None,
            read_options: ReadOptions::default(),
            transaction_id: None,
            explain_options: None,
            options: crate::RequestOptions::default(),
        }
    }

    /// Creates a [`RunGqlQueryBuilder`] to execute a [`GqlQuery`].
    pub fn run_gql_query(&self, gql_query: GqlQuery) -> RunGqlQueryBuilder {
        RunGqlQueryBuilder {
            client: self.clone(),
            gql_query,
            namespace_id: None,
            read_options: ReadOptions::default(),
            transaction_id: None,
            explain_options: None,
            options: crate::RequestOptions::default(),
        }
    }

    /// Creates a [`RunAggregationQueryBuilder`] to execute an [`AggregationQuery`].
    pub fn run_aggregation_query(&self, query: AggregationQuery) -> RunAggregationQueryBuilder {
        RunAggregationQueryBuilder {
            client: self.clone(),
            query,
            namespace_id: None,
            read_options: ReadOptions::default(),
            transaction_id: None,
            explain_options: None,
            options: crate::RequestOptions::default(),
        }
    }

    /// Creates a [`RunAggregationGqlQueryBuilder`] to execute a GQL aggregation query.
    pub fn run_aggregation_gql_query(&self, gql_query: GqlQuery) -> RunAggregationGqlQueryBuilder {
        RunAggregationGqlQueryBuilder {
            client: self.clone(),
            gql_query,
            namespace_id: None,
            read_options: ReadOptions::default(),
            transaction_id: None,
            explain_options: None,
            options: crate::RequestOptions::default(),
        }
    }
}
