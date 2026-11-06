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

//! Closure-based read-write and read-only transactions (`TransactionRunner`, `ReadWriteTransaction`, `ReadOnlyTransaction`, `PendingKey`).

use crate::Error;
use crate::Result;
use crate::aggregation::{AggregationQuery, AggregationResults};
use crate::client::Datastore;
use crate::entity::{Entity, FromEntity};
use crate::gql::GqlQuery;
use crate::key::{IncompleteKey, IntoMutationKey, Key};
use crate::mutation::{CommitResponse, Mutation};
use crate::query::{Query, QueryStream};
use bytes::Bytes;
use google_cloud_gax::backoff_policy::{BackoffPolicy, BackoffPolicyArg};
use std::future::Future;
use std::sync::{Arc, Mutex};

/// A placeholder token returned when an [`IncompleteKey`] is buffered via
/// [`ReadWriteTransaction::insert`] or [`ReadWriteTransaction::upsert`].
///
/// Once the transaction commits, pass this token to [`TransactionResult::resolve_key`]
/// to retrieve the server-allocated complete [`Key`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PendingKey {
    pub(crate) mutation_index: usize,
}

/// The outcome of a committed [`ReadWriteTransaction`].
#[derive(Clone, Debug, PartialEq)]
pub struct TransactionResult<T> {
    /// The value returned by the user's transaction closure.
    pub value: T,
    /// The [`CommitResponse`] returned by the `Commit` RPC (`None` if no mutations and no transaction were started).
    pub commit_response: Option<CommitResponse>,
}

impl<T> TransactionResult<T> {
    /// Resolves a [`PendingKey`] into the server-allocated complete [`Key`] from the commit response.
    pub fn resolve_key(&self, pending: &PendingKey) -> Option<&Key> {
        self.commit_response
            .as_ref()?
            .mutation_results
            .get(pending.mutation_index)?
            .key
            .as_ref()
    }
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub(crate) struct ReadWriteTxState {
    pub(crate) transaction_id: Option<Bytes>,
    pub(crate) previous_transaction: Option<Bytes>,
    pub(crate) mutations: Vec<Mutation>,
}

/// Handle to an active read-write transaction passed to [`TransactionRunnerBuilder::run`].
///
/// Reads and queries executed through `ReadWriteTransaction` automatically attach the
/// transaction context (lazily starting the transaction via `ReadOptions.new_transaction`
/// on the first read). Mutations (`insert`, `upsert`, `update`, `delete`, `mutate`) are
/// buffered in memory and sent atomically when the closure returns `Ok`.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ReadWriteTransaction {
    pub(crate) client: Datastore,
    pub(crate) state: Arc<Mutex<ReadWriteTxState>>,
    pub(crate) options: crate::RequestOptions,
}

impl ReadWriteTransaction {
    /// Returns the server-assigned transaction ID if the transaction has been started on the server.
    pub fn transaction_id(&self) -> Option<Bytes> {
        self.state.lock().unwrap().transaction_id.clone()
    }

    /// Reads a single [`Entity`] by [`Key`] within this transaction.
    pub async fn get(&self, _key: &Key) -> Result<Option<Entity>> {
        unimplemented!()
    }

    /// Reads and deserializes a single entity into `T` via [`FromEntity`] within this transaction.
    pub async fn get_as<T: FromEntity>(&self, _key: &Key) -> Result<Option<T>> {
        unimplemented!()
    }

    /// Reads multiple entities by [`Key`] within this transaction, preserving input key order.
    pub async fn get_multi<I, K>(&self, _keys: I) -> Result<Vec<Option<Entity>>>
    where
        I: IntoIterator<Item = K>,
        K: Into<Key>,
    {
        unimplemented!()
    }

    /// Reads and deserializes multiple entities into `T` via [`FromEntity`] within this transaction.
    pub async fn get_multi_as<T, I, K>(&self, _keys: I) -> Result<Vec<Option<T>>>
    where
        T: FromEntity,
        I: IntoIterator<Item = K>,
        K: Into<Key>,
    {
        unimplemented!()
    }

    /// Executes a structured [`Query`] within this transaction.
    pub async fn run_query(&self, _query: Query) -> Result<QueryStream> {
        unimplemented!()
    }

    /// Executes a [`GqlQuery`] within this transaction.
    pub async fn run_gql_query(&self, _query: GqlQuery) -> Result<QueryStream> {
        unimplemented!()
    }

    /// Executes an [`AggregationQuery`] within this transaction.
    pub async fn run_aggregation_query(
        &self,
        _query: AggregationQuery,
    ) -> Result<AggregationResults> {
        unimplemented!()
    }

    /// Executes a GQL aggregation query within this transaction.
    pub async fn run_aggregation_gql_query(&self, _query: GqlQuery) -> Result<AggregationResults> {
        unimplemented!()
    }

    /// Buffers an `Insert` mutation with an [`IncompleteKey`] and returns a [`PendingKey`]
    /// that can be resolved via [`TransactionResult::resolve_key`] after commit.
    pub fn insert_incomplete(&self, key: IncompleteKey, entity: Entity) -> PendingKey {
        let mut state = self.state.lock().unwrap();
        let mutation_index = state.mutations.len();
        state.mutations.push(Mutation::insert(key, entity));
        PendingKey { mutation_index }
    }

    /// Buffers an `Insert` mutation (accepting either a [`Key`] or [`IncompleteKey`]) and returns a [`PendingKey`].
    pub fn insert(&self, key: impl IntoMutationKey, entity: Entity) -> PendingKey {
        let mut state = self.state.lock().unwrap();
        let mutation_index = state.mutations.len();
        state.mutations.push(Mutation::insert(key, entity));
        PendingKey { mutation_index }
    }

    /// Buffers an `Upsert` mutation (accepting either a [`Key`] or [`IncompleteKey`]) and returns a [`PendingKey`].
    pub fn upsert(&self, key: impl IntoMutationKey, entity: Entity) -> PendingKey {
        let mut state = self.state.lock().unwrap();
        let mutation_index = state.mutations.len();
        state.mutations.push(Mutation::upsert(key, entity));
        PendingKey { mutation_index }
    }

    /// Buffers an `Update` mutation for a complete [`Key`].
    pub fn update(&self, key: Key, entity: Entity) {
        let mut state = self.state.lock().unwrap();
        state.mutations.push(Mutation::update(key, entity));
    }

    /// Buffers a `Delete` mutation for a complete [`Key`].
    pub fn delete(&self, key: Key) {
        let mut state = self.state.lock().unwrap();
        state.mutations.push(Mutation::delete(key));
    }

    /// Buffers a custom [`Mutation`] (such as one with [`PropertyTransform`][crate::mutation::PropertyTransform] or [`PropertyMask`][crate::mutation::PropertyMask]).
    pub fn mutate(&self, mutation: Mutation) -> PendingKey {
        let mut state = self.state.lock().unwrap();
        let mutation_index = state.mutations.len();
        state.mutations.push(mutation);
        PendingKey { mutation_index }
    }
}

/// Builder for configuring and running a retryable read-write transaction via [`Datastore::transaction`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct TransactionRunnerBuilder {
    pub(crate) client: Datastore,
    pub(crate) max_attempts: u32,
    pub(crate) backoff_policy: Option<Arc<dyn BackoffPolicy>>,
    pub(crate) options: crate::RequestOptions,
}

impl TransactionRunnerBuilder {
    /// Sets the maximum number of transaction attempts before returning the last `ABORTED` error (defaults to `5`).
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    /// Configures a custom backoff policy between `ABORTED` transaction retries.
    pub fn with_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.backoff_policy = Some(policy.into().into());
        self
    }

    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Executes `f` inside a read-write transaction, automatically retrying on `ABORTED`
    /// conflicts (passing `previous_transaction` on retry) and rolling back on error.
    pub async fn run<F, Fut, T, E>(self, _f: F) -> std::result::Result<TransactionResult<T>, E>
    where
        F: FnMut(ReadWriteTransaction) -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        E: From<Error>,
    {
        unimplemented!()
    }
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub(crate) struct ReadOnlyTxState {
    pub(crate) transaction_id: Option<Bytes>,
    pub(crate) read_time: Option<wkt::Timestamp>,
}

/// Handle to a read-only snapshot transaction passed to [`ReadOnlyTransactionBuilder::run`].
///
/// Unlike [`ReadWriteTransaction`], `ReadOnlyTransaction` exposes no mutation methods,
/// preventing accidental writes inside a read-only transaction at compile time.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ReadOnlyTransaction {
    pub(crate) client: Datastore,
    pub(crate) state: Arc<Mutex<ReadOnlyTxState>>,
    pub(crate) options: crate::RequestOptions,
}

impl ReadOnlyTransaction {
    /// Returns the server-assigned transaction ID if the transaction has been started on the server.
    pub fn transaction_id(&self) -> Option<Bytes> {
        self.state.lock().unwrap().transaction_id.clone()
    }

    /// Reads a single [`Entity`] by [`Key`] within this read-only transaction.
    pub async fn get(&self, _key: &Key) -> Result<Option<Entity>> {
        unimplemented!()
    }

    /// Reads and deserializes a single entity into `T` via [`FromEntity`] within this read-only transaction.
    pub async fn get_as<T: FromEntity>(&self, _key: &Key) -> Result<Option<T>> {
        unimplemented!()
    }

    /// Reads multiple entities by [`Key`] within this read-only transaction, preserving input key order.
    pub async fn get_multi<I, K>(&self, _keys: I) -> Result<Vec<Option<Entity>>>
    where
        I: IntoIterator<Item = K>,
        K: Into<Key>,
    {
        unimplemented!()
    }

    /// Reads and deserializes multiple entities into `T` via [`FromEntity`] within this read-only transaction.
    pub async fn get_multi_as<T, I, K>(&self, _keys: I) -> Result<Vec<Option<T>>>
    where
        T: FromEntity,
        I: IntoIterator<Item = K>,
        K: Into<Key>,
    {
        unimplemented!()
    }

    /// Executes a structured [`Query`] within this read-only transaction.
    pub async fn run_query(&self, _query: Query) -> Result<QueryStream> {
        unimplemented!()
    }

    /// Executes a [`GqlQuery`] within this read-only transaction.
    pub async fn run_gql_query(&self, _query: GqlQuery) -> Result<QueryStream> {
        unimplemented!()
    }

    /// Executes an [`AggregationQuery`] within this read-only transaction.
    pub async fn run_aggregation_query(
        &self,
        _query: AggregationQuery,
    ) -> Result<AggregationResults> {
        unimplemented!()
    }

    /// Executes a GQL aggregation query within this read-only transaction.
    pub async fn run_aggregation_gql_query(&self, _query: GqlQuery) -> Result<AggregationResults> {
        unimplemented!()
    }
}

/// Builder for configuring and running a read-only transaction via [`Datastore::read_only_transaction`].
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct ReadOnlyTransactionBuilder {
    pub(crate) client: Datastore,
    pub(crate) read_time: Option<wkt::Timestamp>,
    pub(crate) options: crate::RequestOptions,
}

impl ReadOnlyTransactionBuilder {
    /// Configures an exact historical snapshot `read_time` for the read-only transaction.
    pub fn with_read_time(mut self, read_time: wkt::Timestamp) -> Self {
        self.read_time = Some(read_time);
        self
    }

    /// Overrides the per-request [RequestOptions][google_cloud_gax::options::RequestOptions].
    pub fn with_options(mut self, options: impl Into<crate::RequestOptions>) -> Self {
        self.options = options.into();
        self
    }

    /// Executes `f` inside a read-only snapshot transaction.
    pub async fn run<F, Fut, T, E>(self, _f: F) -> std::result::Result<T, E>
    where
        F: FnOnce(ReadOnlyTransaction) -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        E: From<Error>,
    {
        unimplemented!()
    }
}
