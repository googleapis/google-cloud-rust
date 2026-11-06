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

use crate::aggregation::{AggregationQuery, AggregationResults};
use crate::client::Datastore;
use crate::entity::{Entity, FromEntity};
use crate::gql::GqlQuery;
use crate::key::{IncompleteKey, IntoMutationKey, Key};
use crate::mutation::{CommitMode, CommitResponse, Mutation, execute_commit};
use crate::query::{Query, QueryStream};
use crate::read::{LookupReadMode, execute_lookup};
use crate::{Error, Result};
use bytes::Bytes;
use google_cloud_gax::backoff_policy::{BackoffPolicy, BackoffPolicyArg};
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::retry_state::RetryState;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

fn is_aborted(err: &Error) -> bool {
    err.status().is_some_and(|s| s.code == Code::Aborted)
}

fn read_write_tx_options(previous_transaction: Option<Bytes>) -> crate::model::TransactionOptions {
    let mut rw = crate::model::transaction_options::ReadWrite::new();
    if let Some(prev) = previous_transaction {
        rw = rw.set_previous_transaction(prev);
    }
    crate::model::TransactionOptions::new().set_read_write(rw)
}

fn read_only_tx_options(read_time: Option<wkt::Timestamp>) -> crate::model::TransactionOptions {
    let mut ro = crate::model::transaction_options::ReadOnly::new();
    if let Some(ts) = read_time {
        ro = ro.set_read_time(ts);
    }
    crate::model::TransactionOptions::new().set_read_only(ro)
}

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
pub(crate) struct ReadWriteTxState {
    pub(crate) transaction_id: Option<Bytes>,
    pub(crate) previous_transaction: Option<Bytes>,
    pub(crate) mutations: Vec<Mutation>,
    pub(crate) aborted: bool,
}

/// Handle to an active read-write transaction passed to [`TransactionRunnerBuilder::run`].
///
/// Reads and queries executed through `ReadWriteTransaction` automatically attach the
/// transaction context (lazily starting the transaction via `ReadOptions.new_transaction`
/// on the first read). Mutations (`insert`, `upsert`, `update`, `delete`, `mutate`) are
/// buffered in memory and sent atomically when the closure returns `Ok`.
#[derive(Clone, Debug)]
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

    pub(crate) fn read_mode(&self) -> LookupReadMode {
        let state = self.state.lock().unwrap();
        if let Some(ref tx_id) = state.transaction_id {
            LookupReadMode::Transaction(tx_id.clone())
        } else {
            LookupReadMode::NewTransaction(read_write_tx_options(
                state.previous_transaction.clone(),
            ))
        }
    }

    pub(crate) fn record_read_outcome<R>(
        &self,
        res: Result<R>,
        started_tx: Option<Bytes>,
    ) -> Result<R> {
        let mut state = self.state.lock().unwrap();
        if let Some(tx_id) = started_tx
            && !tx_id.is_empty()
        {
            state.transaction_id = Some(tx_id);
        }
        if let Err(ref err) = res
            && is_aborted(err)
        {
            state.aborted = true;
        }
        res
    }

    /// Reads a single [`Entity`] by [`Key`] within this transaction.
    pub async fn get(&self, key: &Key) -> Result<Option<Entity>> {
        let mode = self.read_mode();
        let res = execute_lookup(
            &self.client,
            std::slice::from_ref(key),
            mode,
            self.options.clone(),
        )
        .await;
        match res {
            Ok((mut entities, started_tx)) => {
                self.record_read_outcome(Ok(entities.pop().flatten()), started_tx)
            }
            Err(err) => self.record_read_outcome(Err(err), None),
        }
    }

    /// Reads and deserializes a single entity into `T` via [`FromEntity`] within this transaction.
    pub async fn get_as<T: FromEntity>(&self, key: &Key) -> Result<Option<T>> {
        match self.get(key).await? {
            Some(entity) => T::from_entity(&entity)
                .map(Some)
                .map_err(|e| Error::deser(Box::new(e))),
            None => Ok(None),
        }
    }

    /// Reads multiple entities by [`Key`] within this transaction, preserving input key order.
    pub async fn get_multi<I, K>(&self, keys: I) -> Result<Vec<Option<Entity>>>
    where
        I: IntoIterator<Item = K>,
        K: Into<Key>,
    {
        let keys_vec: Vec<Key> = keys.into_iter().map(Into::into).collect();
        let mode = self.read_mode();
        let res = execute_lookup(&self.client, &keys_vec, mode, self.options.clone()).await;
        match res {
            Ok((entities, started_tx)) => self.record_read_outcome(Ok(entities), started_tx),
            Err(err) => self.record_read_outcome(Err(err), None),
        }
    }

    /// Reads and deserializes multiple entities into `T` via [`FromEntity`] within this transaction.
    pub async fn get_multi_as<T, I, K>(&self, keys: I) -> Result<Vec<Option<T>>>
    where
        T: FromEntity,
        I: IntoIterator<Item = K>,
        K: Into<Key>,
    {
        let entities = self.get_multi(keys).await?;
        entities
            .into_iter()
            .map(|opt| match opt {
                Some(entity) => T::from_entity(&entity)
                    .map(Some)
                    .map_err(|e| Error::deser(Box::new(e))),
                None => Ok(None),
            })
            .collect()
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

    async fn best_effort_rollback(&self, transaction_id: Bytes) {
        let _ = self
            .client
            .inner
            .gapic
            .rollback()
            .set_project_id(self.client.project_id())
            .set_database_id(self.client.database_id())
            .set_transaction(transaction_id)
            .with_options(self.options.clone())
            .send()
            .await;
    }

    /// Executes `f` inside a read-write transaction, automatically retrying on `ABORTED`
    /// conflicts (passing `previous_transaction` on retry) and rolling back on error.
    pub async fn run<F, Fut, T, E>(self, mut f: F) -> std::result::Result<TransactionResult<T>, E>
    where
        F: FnMut(ReadWriteTransaction) -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        E: From<Error>,
    {
        let max_attempts = self.max_attempts.max(1);
        let default_backoff = crate::retry_policy::default_exponential_backoff();
        let backoff: &dyn BackoffPolicy = match &self.backoff_policy {
            Some(p) => p.as_ref(),
            None => &default_backoff,
        };
        let loop_start = Instant::now();
        let mut previous_transaction: Option<Bytes> = None;
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;
            let state = Arc::new(Mutex::new(ReadWriteTxState {
                transaction_id: None,
                previous_transaction: previous_transaction.clone(),
                mutations: Vec::new(),
                aborted: false,
            }));
            let tx = ReadWriteTransaction {
                client: self.client.clone(),
                state: state.clone(),
                options: self.options.clone(),
            };

            match f(tx).await {
                Ok(value) => {
                    let (tx_id, mutations) = {
                        let mut guard = state.lock().unwrap();
                        (
                            guard.transaction_id.take(),
                            std::mem::take(&mut guard.mutations),
                        )
                    };

                    if tx_id.is_none() && mutations.is_empty() {
                        return Ok(TransactionResult {
                            value,
                            commit_response: None,
                        });
                    }

                    let commit_mode = match tx_id.clone() {
                        Some(id) => CommitMode::Transaction(id),
                        None => CommitMode::SingleUseTransaction(read_write_tx_options(
                            previous_transaction.clone(),
                        )),
                    };

                    match execute_commit(
                        &self.client,
                        &mutations,
                        commit_mode,
                        self.options.clone(),
                    )
                    .await
                    {
                        Ok(commit_response) => {
                            return Ok(TransactionResult {
                                value,
                                commit_response: Some(commit_response),
                            });
                        }
                        Err(commit_err) => {
                            if is_aborted(&commit_err) && attempt < max_attempts {
                                if let Some(id) = tx_id {
                                    previous_transaction = Some(id);
                                }
                                let delay = backoff.on_failure(
                                    &RetryState::new(true)
                                        .set_start(loop_start)
                                        .set_attempt_count(attempt),
                                );
                                tokio::time::sleep(delay).await;
                                continue;
                            }
                            return Err(E::from(commit_err));
                        }
                    }
                }
                Err(user_err) => {
                    let (tx_id, aborted) = {
                        let mut guard = state.lock().unwrap();
                        (guard.transaction_id.take(), guard.aborted)
                    };
                    if aborted && attempt < max_attempts {
                        if let Some(id) = tx_id {
                            previous_transaction = Some(id);
                        }
                        let delay = backoff.on_failure(
                            &RetryState::new(true)
                                .set_start(loop_start)
                                .set_attempt_count(attempt),
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    if !aborted && let Some(id) = tx_id {
                        self.best_effort_rollback(id).await;
                    }
                    return Err(user_err);
                }
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReadOnlyTxState {
    pub(crate) transaction_id: Option<Bytes>,
    pub(crate) read_time: Option<wkt::Timestamp>,
}

/// Handle to a read-only snapshot transaction passed to [`ReadOnlyTransactionBuilder::run`].
///
/// Unlike [`ReadWriteTransaction`], `ReadOnlyTransaction` exposes no mutation methods,
/// preventing accidental writes inside a read-only transaction at compile time.
#[derive(Clone, Debug)]
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

    pub(crate) fn read_mode(&self) -> LookupReadMode {
        let state = self.state.lock().unwrap();
        if let Some(ref tx_id) = state.transaction_id {
            LookupReadMode::Transaction(tx_id.clone())
        } else {
            LookupReadMode::NewTransaction(read_only_tx_options(state.read_time))
        }
    }

    pub(crate) fn record_started_tx(&self, started_tx: Option<Bytes>) {
        if let Some(tx_id) = started_tx
            && !tx_id.is_empty()
        {
            self.state.lock().unwrap().transaction_id = Some(tx_id);
        }
    }

    /// Reads a single [`Entity`] by [`Key`] within this read-only transaction.
    pub async fn get(&self, key: &Key) -> Result<Option<Entity>> {
        let mode = self.read_mode();
        let (mut entities, started_tx) = execute_lookup(
            &self.client,
            std::slice::from_ref(key),
            mode,
            self.options.clone(),
        )
        .await?;
        self.record_started_tx(started_tx);
        Ok(entities.pop().flatten())
    }

    /// Reads and deserializes a single entity into `T` via [`FromEntity`] within this read-only transaction.
    pub async fn get_as<T: FromEntity>(&self, key: &Key) -> Result<Option<T>> {
        match self.get(key).await? {
            Some(entity) => T::from_entity(&entity)
                .map(Some)
                .map_err(|e| Error::deser(Box::new(e))),
            None => Ok(None),
        }
    }

    /// Reads multiple entities by [`Key`] within this read-only transaction, preserving input key order.
    pub async fn get_multi<I, K>(&self, keys: I) -> Result<Vec<Option<Entity>>>
    where
        I: IntoIterator<Item = K>,
        K: Into<Key>,
    {
        let keys_vec: Vec<Key> = keys.into_iter().map(Into::into).collect();
        let mode = self.read_mode();
        let (entities, started_tx) =
            execute_lookup(&self.client, &keys_vec, mode, self.options.clone()).await?;
        self.record_started_tx(started_tx);
        Ok(entities)
    }

    /// Reads and deserializes multiple entities into `T` via [`FromEntity`] within this read-only transaction.
    pub async fn get_multi_as<T, I, K>(&self, keys: I) -> Result<Vec<Option<T>>>
    where
        T: FromEntity,
        I: IntoIterator<Item = K>,
        K: Into<Key>,
    {
        let entities = self.get_multi(keys).await?;
        entities
            .into_iter()
            .map(|opt| match opt {
                Some(entity) => T::from_entity(&entity)
                    .map(Some)
                    .map_err(|e| Error::deser(Box::new(e))),
                None => Ok(None),
            })
            .collect()
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
    pub async fn run<F, Fut, T, E>(self, f: F) -> std::result::Result<T, E>
    where
        F: FnOnce(ReadOnlyTransaction) -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        E: From<Error>,
    {
        let state = Arc::new(Mutex::new(ReadOnlyTxState {
            transaction_id: None,
            read_time: self.read_time,
        }));
        let tx = ReadOnlyTransaction {
            client: self.client.clone(),
            state: state.clone(),
            options: self.options.clone(),
        };
        let res = f(tx).await;
        if res.is_err() {
            let tx_id = state.lock().unwrap().transaction_id.take();
            if let Some(tx_id) = tx_id {
                let _ = self
                    .client
                    .inner
                    .gapic
                    .rollback()
                    .set_project_id(self.client.project_id())
                    .set_database_id(self.client.database_id())
                    .set_transaction(tx_id)
                    .with_options(self.options)
                    .send()
                    .await;
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::gapic_dataplane::stub::Datastore as DatastoreStub;
    use crate::key::PartitionId;
    use crate::model::{
        CommitRequest, CommitResponse as ProtoCommitResponse, EntityResult, LookupRequest,
        LookupResponse, MutationResult as ProtoMutationResult, RollbackRequest, RollbackResponse,
    };
    use google_cloud_gax::error::rpc::Status;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::response::Response;

    mockall::mock! {
        #[derive(Debug)]
        DatastoreMock {}
        impl DatastoreStub for DatastoreMock {
            async fn lookup(
                &self,
                req: LookupRequest,
                options: RequestOptions,
            ) -> Result<Response<LookupResponse>>;

            async fn commit(
                &self,
                req: CommitRequest,
                options: RequestOptions,
            ) -> Result<Response<ProtoCommitResponse>>;

            async fn rollback(
                &self,
                req: RollbackRequest,
                options: RequestOptions,
            ) -> Result<Response<RollbackResponse>>;
        }
    }

    fn test_client(mock: MockDatastoreMock) -> Datastore {
        Datastore::from_stub("test-proj", "test-db", "test-ns", mock)
    }

    fn make_entity_result(key: &Key, balance: i64) -> EntityResult {
        let mut entity = Entity::with_key(key.clone());
        entity.set("balance", balance);
        EntityResult::new()
            .set_entity(entity.to_proto(key.partition_id()).expect("valid entity"))
            .set_version(1)
    }

    #[tokio::test]
    async fn read_write_tx_lazy_new_transaction_and_aborted_retry() {
        let mut mock = MockDatastoreMock::new();

        // Attempt 1: Lookup starts transaction "tx-1" via new_transaction(previous_transaction = "")
        mock.expect_lookup()
            .times(1)
            .withf(|req, _| {
                req.read_options
                    .as_ref()
                    .and_then(|ro| ro.new_transaction())
                    .and_then(|nt| nt.read_write())
                    .is_some_and(|rw| rw.previous_transaction.is_empty())
            })
            .returning(|req, _| {
                let key = Key::from_proto(req.keys[0].clone()).unwrap();
                Ok(Response::from(
                    LookupResponse::new()
                        .set_transaction(Bytes::from_static(b"tx-1"))
                        .set_found([make_entity_result(&key, 100)]),
                ))
            });

        // Attempt 1: Commit with transaction = "tx-1" fails with Code::Aborted
        mock.expect_commit()
            .times(1)
            .withf(|req, _| req.transaction() == Some(&Bytes::from_static(b"tx-1")))
            .returning(|_, _| {
                Err(Error::service(
                    Status::default()
                        .set_code(Code::Aborted)
                        .set_message("contention"),
                ))
            });

        // Attempt 2: Lookup starts transaction "tx-2" via new_transaction(previous_transaction = "tx-1")
        mock.expect_lookup()
            .times(1)
            .withf(|req, _| {
                req.read_options
                    .as_ref()
                    .and_then(|ro| ro.new_transaction())
                    .and_then(|nt| nt.read_write())
                    .is_some_and(|rw| rw.previous_transaction == Bytes::from_static(b"tx-1"))
            })
            .returning(|req, _| {
                let key = Key::from_proto(req.keys[0].clone()).unwrap();
                Ok(Response::from(
                    LookupResponse::new()
                        .set_transaction(Bytes::from_static(b"tx-2"))
                        .set_found([make_entity_result(&key, 150)]),
                ))
            });

        // Attempt 2: Commit with transaction = "tx-2" succeeds
        mock.expect_commit()
            .times(1)
            .withf(|req, _| {
                req.transaction() == Some(&Bytes::from_static(b"tx-2")) && req.mutations.len() == 1
            })
            .returning(|_, _| {
                Ok(Response::from(
                    ProtoCommitResponse::new()
                        .set_mutation_results([ProtoMutationResult::new().set_version(2)]),
                ))
            });

        let client = test_client(mock);
        let acct_key = client
            .key_factory("Account")
            .with_name("acc-1")
            .expect("valid key");

        let outcome = client
            .transaction()
            .run(|tx| {
                let key = acct_key.clone();
                async move {
                    let mut acct = tx.get(&key).await?.expect("account exists");
                    let bal: i64 = acct.get("balance").expect("balance");
                    acct.set("balance", bal + 50);
                    tx.update(key, acct);
                    Ok::<i64, Error>(bal + 50)
                }
            })
            .await
            .expect("transaction succeeds on retry");

        assert_eq!(outcome.value, 200);
        assert!(outcome.commit_response.is_some());
    }

    #[tokio::test]
    async fn write_only_tx_uses_single_use_transaction_and_resolves_pending_key() {
        let mut mock = MockDatastoreMock::new();
        mock.expect_lookup().never();

        mock.expect_commit()
            .times(1)
            .withf(|req, _| {
                req.mode == crate::model::commit_request::Mode::Transactional
                    && req.single_use_transaction().is_some()
                    && req.mutations.len() == 1
            })
            .returning(|_, _| {
                let allocated = Key::with_id("Task", 777)
                    .unwrap()
                    .with_partition_id(PartitionId::new("test-proj"));
                Ok(Response::from(
                    ProtoCommitResponse::new().set_mutation_results([ProtoMutationResult::new()
                        .set_key(allocated.to_proto())
                        .set_version(1)]),
                ))
            });

        let client = test_client(mock);
        let inc_key = client.key_factory("Task").incomplete().unwrap();

        let outcome = client
            .transaction()
            .run(|tx| {
                let ik = inc_key.clone();
                async move {
                    let mut entity = Entity::new();
                    entity.set("done", false);
                    let pending = tx.insert_incomplete(ik, entity);
                    Ok::<PendingKey, Error>(pending)
                }
            })
            .await
            .expect("write-only tx succeeds");

        let resolved = outcome
            .resolve_key(&outcome.value)
            .expect("pending key resolved");
        assert_eq!(resolved.id_num(), Some(777));
    }

    #[tokio::test]
    async fn tx_rolls_back_when_closure_returns_error() {
        let mut mock = MockDatastoreMock::new();

        mock.expect_lookup().times(1).returning(|_, _| {
            Ok(Response::from(
                LookupResponse::new().set_transaction(Bytes::from_static(b"tx-rollback")),
            ))
        });

        mock.expect_rollback()
            .times(1)
            .withf(|req, _| req.transaction == Bytes::from_static(b"tx-rollback"))
            .returning(|_, _| Ok(Response::from(RollbackResponse::new())));

        let client = test_client(mock);
        let key = client.key_factory("Task").with_id(1).unwrap();

        let res: std::result::Result<TransactionResult<()>, Error> = client
            .transaction()
            .run(|tx| {
                let k = key.clone();
                async move {
                    let _ = tx.get(&k).await?;
                    Err(Error::deser("business logic failure"))
                }
            })
            .await;

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn read_only_tx_starts_once_and_reuses_transaction_id() {
        let mut mock = MockDatastoreMock::new();
        let snapshot_ts = wkt::Timestamp::clamp(1_700_000_500, 0);

        // First read starts read-only transaction with read_time
        mock.expect_lookup()
            .times(1)
            .withf(move |req, _| {
                req.read_options
                    .as_ref()
                    .and_then(|ro| ro.new_transaction())
                    .and_then(|nt| nt.read_only())
                    .is_some_and(|ro| ro.read_time == Some(snapshot_ts))
            })
            .returning(|req, _| {
                let key = Key::from_proto(req.keys[0].clone()).unwrap();
                Ok(Response::from(
                    LookupResponse::new()
                        .set_transaction(Bytes::from_static(b"ro-tx-1"))
                        .set_found([make_entity_result(&key, 50)]),
                ))
            });

        // Second read reuses transaction = "ro-tx-1"
        mock.expect_lookup()
            .times(1)
            .withf(|req, _| {
                req.read_options.as_ref().and_then(|ro| ro.transaction())
                    == Some(&Bytes::from_static(b"ro-tx-1"))
            })
            .returning(|req, _| {
                let key = Key::from_proto(req.keys[0].clone()).unwrap();
                Ok(Response::from(
                    LookupResponse::new().set_found([make_entity_result(&key, 75)]),
                ))
            });

        let client = test_client(mock);
        let kf = client.key_factory("Account");
        let k1 = kf.with_name("a1").unwrap();
        let k2 = kf.with_name("a2").unwrap();

        let sum = client
            .read_only_transaction()
            .with_read_time(snapshot_ts)
            .run(|tx| async move {
                let e1 = tx.get(&k1).await?.expect("a1");
                let e2 = tx.get(&k2).await?.expect("a2");
                let b1: i64 = e1.get("balance").unwrap();
                let b2: i64 = e2.get("balance").unwrap();
                Ok::<i64, Error>(b1 + b2)
            })
            .await
            .expect("read-only tx succeeds");

        assert_eq!(sum, 125);
    }
}
