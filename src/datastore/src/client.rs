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
pub(crate) struct DatastoreInner {
    pub(crate) project_id: String,
    pub(crate) database_id: String,
    pub(crate) namespace_id: String,
    pub(crate) gapic: gapic_dataplane::client::Datastore,
}

pub(crate) fn apply_emulator_env(config: &mut gaxi::options::ClientConfig) {
    if let Ok(emulator_host) = std::env::var("DATASTORE_EMULATOR_HOST") {
        let emulator_host = emulator_host.trim();
        if !emulator_host.is_empty() {
            if config.endpoint.is_none() {
                let endpoint = if emulator_host.starts_with("http://")
                    || emulator_host.starts_with("https://")
                {
                    emulator_host.to_string()
                } else {
                    format!("http://{emulator_host}")
                };
                config.endpoint = Some(endpoint);
            }
            if config.cred.is_none() {
                config.cred =
                    Some(google_cloud_auth::credentials::anonymous::Builder::new().build());
            }
        }
    }
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

    pub(crate) async fn from_builder(mut builder: ClientBuilder) -> ClientBuilderResult<Self> {
        apply_emulator_env(&mut builder.config);
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
        let Some(start_id) = start_key.id_num() else {
            return Err(KeyError::ZeroId);
        };
        let ancestors = &start_key.path()[..start_key.path().len() - 1];
        let kind = start_key.kind();
        let partition_id = start_key.partition_id().clone();
        let mut keys = Vec::with_capacity(count);
        for offset in 0..count {
            let offset_i64 = i64::try_from(offset).map_err(|_| KeyError::ZeroId)?;
            let next_id = start_id.checked_add(offset_i64).ok_or(KeyError::ZeroId)?;
            let mut path = ancestors.to_vec();
            path.push(crate::key::PathElement::with_id(kind, next_id)?);
            keys.push(Key::from_path(partition_id.clone(), path)?);
        }
        Ok(self.reserve_ids(keys))
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Response;
    use crate::model::{
        AllocateIdsRequest, AllocateIdsResponse, BeginTransactionRequest, BeginTransactionResponse,
        CommitRequest, CommitResponse, LookupRequest, LookupResponse, ReserveIdsRequest,
        ReserveIdsResponse, RollbackRequest, RollbackResponse, RunAggregationQueryRequest,
        RunAggregationQueryResponse, RunQueryRequest, RunQueryResponse,
    };

    mockall::mock! {
        #[derive(Debug)]
        pub Datastore {}

        impl gapic_dataplane::stub::Datastore for Datastore {
            async fn lookup(
                &self,
                req: LookupRequest,
                options: crate::RequestOptions,
            ) -> Result<Response<LookupResponse>>;

            async fn run_query(
                &self,
                req: RunQueryRequest,
                options: crate::RequestOptions,
            ) -> Result<Response<RunQueryResponse>>;

            async fn run_aggregation_query(
                &self,
                req: RunAggregationQueryRequest,
                options: crate::RequestOptions,
            ) -> Result<Response<RunAggregationQueryResponse>>;

            async fn begin_transaction(
                &self,
                req: BeginTransactionRequest,
                options: crate::RequestOptions,
            ) -> Result<Response<BeginTransactionResponse>>;

            async fn commit(
                &self,
                req: CommitRequest,
                options: crate::RequestOptions,
            ) -> Result<Response<CommitResponse>>;

            async fn rollback(
                &self,
                req: RollbackRequest,
                options: crate::RequestOptions,
            ) -> Result<Response<RollbackResponse>>;

            async fn allocate_ids(
                &self,
                req: AllocateIdsRequest,
                options: crate::RequestOptions,
            ) -> Result<Response<AllocateIdsResponse>>;

            async fn reserve_ids(
                &self,
                req: ReserveIdsRequest,
                options: crate::RequestOptions,
            ) -> Result<Response<ReserveIdsResponse>>;
        }
    }

    #[tokio::test]
    async fn allocate_ids_normalizes_and_converts() -> anyhow::Result<()> {
        let mut mock = MockDatastore::new();
        mock.expect_allocate_ids()
            .times(1)
            .withf(|req, _| {
                req.project_id == "proj-1"
                    && req.database_id == "db-1"
                    && req.keys.len() == 2
                    && req.keys[0]
                        .partition_id
                        .as_ref()
                        .is_some_and(|p| p.namespace_id == "ns-1")
            })
            .returning(|_, _| {
                let p = crate::model::PartitionId::new()
                    .set_project_id("proj-1")
                    .set_database_id("db-1")
                    .set_namespace_id("ns-1");
                let k1 = crate::model::Key::new()
                    .set_partition_id(p.clone())
                    .set_path([crate::model::key::PathElement::new()
                        .set_kind("Task")
                        .set_id(101)]);
                let k2 = crate::model::Key::new().set_partition_id(p).set_path([
                    crate::model::key::PathElement::new()
                        .set_kind("Task")
                        .set_id(102),
                ]);
                Ok(Response::from(
                    AllocateIdsResponse::new().set_keys([k1, k2]),
                ))
            });

        let client = Datastore::from_stub("proj-1", "db-1", "ns-1", mock);
        let inc = IncompleteKey::new("Task")?;
        let allocated = client.allocate_ids([inc.clone(), inc]).send().await?;
        assert_eq!(allocated.len(), 2);
        assert_eq!(allocated[0].id_num(), Some(101));
        assert_eq!(allocated[1].id_num(), Some(102));
        assert_eq!(allocated[0].partition_id().namespace_id(), "ns-1");
        Ok(())
    }

    #[tokio::test]
    async fn reserve_ids_and_sequential() -> anyhow::Result<()> {
        let mut mock = MockDatastore::new();
        mock.expect_reserve_ids()
            .times(1)
            .withf(|req, _| {
                req.project_id == "proj-1"
                    && req.keys.len() == 3
                    && req.keys[0].path[0].id() == Some(&500)
                    && req.keys[1].path[0].id() == Some(&501)
                    && req.keys[2].path[0].id() == Some(&502)
            })
            .returning(|_, _| Ok(Response::from(ReserveIdsResponse::new())));

        let client = Datastore::from_stub("proj-1", "", "", mock);
        let start = Key::with_id("Task", 500)?;
        client.reserve_ids_sequential(start, 3)?.send().await?;
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn datastore_emulator_host_configures_endpoint_and_anonymous_credentials()
    -> anyhow::Result<()> {
        let _env = scoped_env::ScopedEnv::set("DATASTORE_EMULATOR_HOST", "localhost:8081");

        let mut builder = Datastore::builder("emulator-proj");
        apply_emulator_env(&mut builder.config);
        assert_eq!(
            builder.config.endpoint.as_deref(),
            Some("http://localhost:8081")
        );
        assert!(builder.config.cred.is_some());

        // Building without explicit credentials succeeds when DATASTORE_EMULATOR_HOST is set.
        let client = Datastore::builder("emulator-proj").build().await?;
        assert_eq!(client.project_id(), "emulator-proj");

        // Explicit endpoint is preserved.
        let mut custom_builder =
            Datastore::builder("emulator-proj").with_endpoint("http://127.0.0.1:9999");
        apply_emulator_env(&mut custom_builder.config);
        assert_eq!(
            custom_builder.config.endpoint.as_deref(),
            Some("http://127.0.0.1:9999")
        );
        Ok(())
    }
}
