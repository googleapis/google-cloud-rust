// Copyright 2026 Google LLC
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

use crate::client::{DatabaseClient, Mutation, amend_request_options_for_lar};
use crate::model::request_options::Priority;
use crate::model::transaction_options::ReadWrite;
use crate::model::{
    BeginTransactionRequest, CommitRequest, CommitResponse, MultiplexedSessionPrecommitToken,
    Mutation as ProtoMutation, RequestOptions, TransactionOptions,
};
use crate::transaction_retry_policy::{
    BasicTransactionRetryPolicy, TransactionRetryPolicy, retry_aborted,
};
use bytes::Bytes;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::options::RequestOptions as GaxRequestOptions;
use google_cloud_gax::retry_policy::RetryPolicyArg;
use std::sync::{Arc, Mutex};
use wkt::Duration;

/// A builder for [WriteOnlyTransaction].
pub struct WriteOnlyTransactionBuilder {
    client: DatabaseClient,
    transaction_tag: Option<String>,
    max_commit_delay: Option<Duration>,
    retry_policy: Box<dyn TransactionRetryPolicy>,
    exclude_txn_from_change_streams: bool,
    return_commit_stats: bool,
    commit_priority: Priority,
    begin_gax_options: GaxRequestOptions,
    commit_gax_options: GaxRequestOptions,
}

impl WriteOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            transaction_tag: None,
            max_commit_delay: None,
            retry_policy: Box::new(BasicTransactionRetryPolicy::default()),
            exclude_txn_from_change_streams: false,
            return_commit_stats: false,
            commit_priority: Priority::Unspecified,
            begin_gax_options: GaxRequestOptions::default(),
            commit_gax_options: GaxRequestOptions::default(),
        }
    }

    /// Sets a transaction tag to be used for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction()
    ///     .with_transaction_tag("my-tag")
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See also: [Troubleshooting with tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
    pub fn with_transaction_tag(mut self, tag: impl Into<String>) -> Self {
        self.transaction_tag = Some(tag.into());
        self
    }

    /// Sets the RPC priority to use for the commit of this transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::model::request_options::Priority;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction()
    ///     .with_commit_priority(Priority::Low)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_commit_priority(mut self, priority: Priority) -> Self {
        self.commit_priority = priority;
        self
    }

    /// Sets the maximum commit delay for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use wkt::Duration;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction()
    ///     .with_max_commit_delay(Duration::try_from("0.1s").unwrap())
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This option allows you to specify the maximum amount of time Spanner can
    /// adjust the commit timestamp of the transaction to allow for commit batching.
    /// Increasing this value can increase throughput at the expense of latency.
    /// The value must be between 0 and 500 milliseconds. If not set, or set to 0,
    /// Spanner does not delay the commit.
    pub fn with_max_commit_delay(mut self, delay: Duration) -> Self {
        self.max_commit_delay = Some(delay);
        self
    }

    /// Sets whether to exclude the transaction from change streams.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction()
    ///     .with_exclude_txn_from_change_streams(true)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// When set to `true`, it prevents modifications from this transaction from being tracked in change streams.
    /// Note that this only affects change streams that have been created with the DDL option `allow_txn_exclusion = true`.
    /// If `allow_txn_exclusion` is not set or set to `false` for a change stream, updates made within this transaction
    /// are recorded in that change stream regardless of this setting.
    ///
    /// When set to `false` or not specified, modifications from this transaction are recorded in all change streams
    /// tracking columns modified by this transaction.
    pub fn with_exclude_txn_from_change_streams(mut self, exclude: bool) -> Self {
        self.exclude_txn_from_change_streams = exclude;
        self
    }

    /// Sets whether to return commit stats for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Mutation, Spanner};
    /// # async fn test_doc() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Spanner::builder().build().await?;
    /// # let db = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let mutation = Mutation::new_insert_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .build();
    ///
    /// let response = db.write_only_transaction()
    ///     .with_return_commit_stats(true)
    ///     .build()
    ///     .write(vec![mutation])
    ///     .await?;
    ///
    /// if let Some(stats) = response.commit_stats {
    ///     println!("Mutation count: {}", stats.mutation_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See also: <https://docs.cloud.google.com/spanner/docs/commit-statistics>
    pub fn with_return_commit_stats(mut self, return_stats: bool) -> Self {
        self.return_commit_stats = return_stats;
        self
    }

    /// Sets the retry policy for the transaction.
    ///
    /// # Example
    /// ```
    /// # use std::time::Duration;
    /// # use google_cloud_spanner::client::{BasicTransactionRetryPolicy, Spanner};
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    ///
    /// let retry_policy = BasicTransactionRetryPolicy::new()
    ///     .with_max_attempts(5)
    ///     .with_total_timeout(Duration::from_secs(60));
    ///
    /// let transaction = db_client.write_only_transaction()
    ///     .with_retry_policy(retry_policy)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The client will retry the transaction if it is aborted by Spanner.
    /// This policy can be used to customize whether a transaction should be retried
    /// or not. The default is to retry indefinitely until the transaction succeeds.
    pub fn with_retry_policy<P: TransactionRetryPolicy + 'static>(mut self, policy: P) -> Self {
        self.retry_policy = Box::new(policy);
        self
    }

    /// Sets the per-attempt timeout for the BeginTransaction RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use std::time::Duration;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction()
    ///     .with_begin_attempt_timeout(Duration::from_secs(5))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_begin_attempt_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.begin_gax_options.set_attempt_timeout(timeout);
        self
    }

    /// Sets the retry policy for the BeginTransaction RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::retry_policy::NeverRetry;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction()
    ///     .with_begin_retry_policy(NeverRetry)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_begin_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.begin_gax_options.set_retry_policy(policy);
        self
    }

    /// Sets the backoff policy for the BeginTransaction RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction()
    ///     .with_begin_backoff_policy(ExponentialBackoff::default())
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_begin_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.begin_gax_options.set_backoff_policy(policy);
        self
    }

    /// Sets the per-attempt timeout for the Commit RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use std::time::Duration;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction()
    ///     .with_commit_attempt_timeout(Duration::from_secs(5))
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_commit_attempt_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.commit_gax_options.set_attempt_timeout(timeout);
        self
    }

    /// Sets the retry policy for the Commit RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::retry_policy::NeverRetry;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction()
    ///     .with_commit_retry_policy(NeverRetry)
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_commit_retry_policy(mut self, policy: impl Into<RetryPolicyArg>) -> Self {
        self.commit_gax_options.set_retry_policy(policy);
        self
    }

    /// Sets the backoff policy for the Commit RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// # async fn sample(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction()
    ///     .with_commit_backoff_policy(ExponentialBackoff::default())
    ///     .build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_commit_backoff_policy(mut self, policy: impl Into<BackoffPolicyArg>) -> Self {
        self.commit_gax_options.set_backoff_policy(policy);
        self
    }

    /// Builds the [WriteOnlyTransaction].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let transaction = db_client.write_only_transaction().build();
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(self) -> WriteOnlyTransaction {
        let session_name = self.client.session_name();
        WriteOnlyTransaction {
            session_name,
            client: self.client,
            transaction_tag: self.transaction_tag,
            max_commit_delay: self.max_commit_delay,
            retry_policy: self.retry_policy,
            exclude_txn_from_change_streams: self.exclude_txn_from_change_streams,
            return_commit_stats: self.return_commit_stats,
            commit_priority: self.commit_priority,
            begin_gax_options: self.begin_gax_options,
            commit_gax_options: self.commit_gax_options,
        }
    }
}

/// A write-only transaction.
///
/// A write-only transaction can be used to execute blind writes.
pub struct WriteOnlyTransaction {
    pub(crate) session_name: String,
    client: DatabaseClient,
    transaction_tag: Option<String>,
    max_commit_delay: Option<Duration>,
    retry_policy: Box<dyn TransactionRetryPolicy>,
    exclude_txn_from_change_streams: bool,
    return_commit_stats: bool,
    commit_priority: Priority,
    begin_gax_options: GaxRequestOptions,
    commit_gax_options: GaxRequestOptions,
}

impl WriteOnlyTransaction {
    /// Writes a set of mutations atomically to Spanner.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Mutation, Spanner};
    /// # async fn test_doc() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Spanner::builder().build().await?;
    /// let db = client.database_client("projects/p/instances/i/databases/d").build().await?;
    ///
    /// let mutation = Mutation::new_insert_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .set("UserName").to(&"Alice")
    ///     .build();
    ///
    /// let response = db.write_only_transaction()
    ///     .with_transaction_tag("my-tag")
    ///     .build()
    ///     .write(vec![mutation])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This method uses retries and replay protection internally, which means that the mutations
    /// are applied exactly once on success, or not at all if an error is returned, regardless of
    /// any failures in the underlying network. Note that if the call is cancelled or reaches
    /// deadline, it is not possible to know whether the mutations were applied without performing
    /// a subsequent database operation, but the mutations will have been applied at most once.
    pub async fn write<I>(self, mutations: I) -> crate::Result<CommitResponse>
    where
        I: IntoIterator<Item = Mutation>,
    {
        let begin_gax_options = self.begin_gax_options();
        let commit_gax_options = self.commit_gax_options();
        let req_options = RequestOptions::default()
            .set_transaction_tag(self.transaction_tag.unwrap_or_default())
            .set_priority(self.commit_priority.clone());

        let mutations_proto: Vec<_> = mutations.into_iter().map(|m| m.build_proto()).collect();
        let mutation_key = Mutation::select_mutation_key(&mutations_proto);
        let client = self.client;
        let session_name = self.session_name.clone();
        let previous_transaction_id = Arc::new(Mutex::new(Bytes::new()));
        let channel_hint = client.spanner.next_channel_hint();

        let max_commit_delay = self.max_commit_delay;
        let return_commit_stats = self.return_commit_stats;

        retry_aborted(&*self.retry_policy, || {
            let client = client.clone();
            let session_name = session_name.clone();
            let req_options = req_options.clone();
            let mutations_proto = mutations_proto.clone();
            let mutation_key = mutation_key.clone();
            let previous_transaction_id = previous_transaction_id.clone();
            let begin_gax_options = begin_gax_options.clone();
            let commit_gax_options = commit_gax_options.clone();

            async move {
                let previous_id: Bytes = previous_transaction_id.lock().unwrap().clone();

                let begin_req = BeginTransactionRequest::default()
                    .set_session(session_name.clone())
                    .set_options(
                        TransactionOptions::default()
                            .set_read_write(Box::new(
                                ReadWrite::default()
                                    .set_multiplexed_session_previous_transaction_id(previous_id),
                            ))
                            .set_exclude_txn_from_change_streams(
                                self.exclude_txn_from_change_streams,
                            ),
                    )
                    .set_request_options(req_options.clone())
                    .set_or_clear_mutation_key(mutation_key.clone());

                let tx = client
                    .spanner
                    .begin_transaction(begin_req, begin_gax_options, channel_hint)
                    .await?;
                *previous_transaction_id.lock().unwrap() = tx.id.clone();

                let commit_req = create_commit_request(
                    session_name.clone(),
                    tx.id.clone(),
                    mutations_proto,
                    tx.precommit_token,
                    Some(req_options.clone()),
                    max_commit_delay,
                    return_commit_stats,
                );

                let response = client
                    .spanner
                    .commit(commit_req, commit_gax_options.clone(), channel_hint)
                    .await?;

                // If a commit_response with a precommit_token is returned, then we need to
                // retry the commit with the new precommit_token and without any mutations.
                if let Some(new_token) = response.precommit_token().map(|b| *b.clone()) {
                    let retry_commit_req = create_commit_request(
                        session_name.clone(),
                        tx.id,
                        Vec::new(),
                        Some(new_token),
                        Some(req_options),
                        max_commit_delay,
                        return_commit_stats,
                    );
                    client
                        .spanner
                        .commit(retry_commit_req, commit_gax_options, channel_hint)
                        .await
                } else {
                    Ok(response)
                }
            }
        })
        .await
    }

    /// Writes a set of mutations at least once using a single Commit RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Mutation, Spanner};
    /// # async fn test_doc() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = Spanner::builder().build().await?;
    /// let db = client.database_client("projects/p/instances/i/databases/d").build().await?;
    ///
    /// let mutation = Mutation::new_insert_or_update_builder("Users")
    ///     .set("UserId").to(&1)
    ///     .set("UserName").to(&"Alice")
    ///     .build();
    ///
    /// let response = db.write_only_transaction()
    ///     .with_transaction_tag("my-tag")
    ///     .build()
    ///     .write_at_least_once(vec![mutation])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Since this method does not feature replay protection, it may attempt to apply the provided
    /// mutations more than once. If the mutations are not idempotent, this may lead to a failure
    /// being reported even if the mutation was applied successfully the first time. For example,
    /// an insert may fail with an `AlreadyExists` error even though the row did not exist before
    /// this method was called. For this reason, most users of the library will prefer to use write
    /// transactions with replay protection instead.
    /// However, `write_at_least_once` requires only a single RPC, whereas replay-protected
    /// writes require two RPCs. Thus, this method may be appropriate for latency sensitive
    /// and/or high throughput blind writing.
    pub async fn write_at_least_once<I>(self, mutations: I) -> crate::Result<CommitResponse>
    where
        I: IntoIterator<Item = Mutation>,
    {
        let commit_gax_options = self.commit_gax_options();
        let single_use = TransactionOptions::new()
            .set_read_write(Box::new(ReadWrite::new()))
            .set_exclude_txn_from_change_streams(self.exclude_txn_from_change_streams);
        let req_options = RequestOptions::default()
            .set_transaction_tag(self.transaction_tag.unwrap_or_default())
            .set_priority(self.commit_priority.clone());
        let request = CommitRequest::new()
            .set_session(self.session_name.clone())
            .set_mutations(mutations.into_iter().map(|m| m.build_proto()))
            .set_single_use_transaction(Box::new(single_use))
            .set_request_options(req_options)
            .set_or_clear_max_commit_delay(self.max_commit_delay)
            .set_return_commit_stats(self.return_commit_stats);
        let client = self.client;
        let channel_hint = client.spanner.next_channel_hint();

        retry_aborted(&*self.retry_policy, || {
            let client = client.clone();
            let request = request.clone();
            let commit_gax_options = commit_gax_options.clone();

            async move {
                client
                    .spanner
                    .commit(request, commit_gax_options, channel_hint)
                    .await
            }
        })
        .await
    }

    fn begin_gax_options(&self) -> GaxRequestOptions {
        amend_request_options_for_lar(
            self.client.leader_aware_routing_enabled,
            self.begin_gax_options.clone(),
        )
    }

    fn commit_gax_options(&self) -> GaxRequestOptions {
        amend_request_options_for_lar(
            self.client.leader_aware_routing_enabled,
            self.commit_gax_options.clone(),
        )
    }
}

pub(crate) fn create_commit_request(
    session_name: String,
    transaction_id: bytes::Bytes,
    mutations: Vec<ProtoMutation>,
    precommit_token: Option<MultiplexedSessionPrecommitToken>,
    request_options: Option<RequestOptions>,
    max_commit_delay: Option<Duration>,
    return_commit_stats: bool,
) -> CommitRequest {
    CommitRequest::default()
        .set_session(session_name)
        .set_transaction_id(transaction_id)
        .set_mutations(mutations)
        .set_or_clear_precommit_token(precommit_token)
        .set_or_clear_request_options(request_options)
        .set_or_clear_max_commit_delay(max_commit_delay)
        .set_return_commit_stats(return_commit_stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Spanner;
    use crate::transaction_retry_policy::tests::create_aborted_status;
    use gaxi::grpc::tonic::Response;
    use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    use google_cloud_gax::retry_policy::NeverRetry;
    use google_cloud_test_macros::tokio_test_no_panics;
    use prost_types::Duration as ProstDuration;
    use prost_types::Timestamp;
    use spanner_grpc_mock::google::spanner::v1::CommitResponse;
    use spanner_grpc_mock::google::spanner::v1::Session;
    use spanner_grpc_mock::google::spanner::v1::Transaction;
    use spanner_grpc_mock::google::spanner::v1::commit_response::CommitStats;
    use spanner_grpc_mock::google::spanner::v1::transaction_options::Mode;
    use std::time::Duration as StdDuration;
    use wkt::Duration;

    pub(crate) async fn setup_db_client(
        mock: spanner_grpc_mock::MockSpanner,
    ) -> (DatabaseClient, tokio::task::JoinHandle<()>) {
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        let (address, server) = spanner_grpc_mock::start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        (db_client, server)
    }

    #[tokio_test_no_panics]
    async fn write_at_least_once() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                    ..Default::default()
                },
            ))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/123");

            // Validate the custom request options contain the transaction tag and priority
            assert!(req.request_options.is_some());
            let req_opts = req.request_options.as_ref().expect("request_options should be present");
            assert_eq!(req_opts.transaction_tag, "my_tag");
            assert_eq!(Priority::from(req_opts.priority), Priority::High);

            assert!(req.mutations.len() == 1);

            // Validate it's a single-use transaction configured correctly
            match req.transaction {
                Some(spanner_grpc_mock::google::spanner::v1::commit_request::Transaction::SingleUseTransaction(opts)) => {
                    assert!(opts.mode.is_some());
                }
                _ => panic!("Expected SingleUseTransaction"),
            }

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::CommitResponse {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 1234,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            ))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .with_transaction_tag("my_tag")
            .with_commit_priority(Priority::High)
            .build()
            .write_at_least_once(vec![mutation])
            .await;

        assert!(res.is_ok());
        let res = res.expect("write_at_least_once should succeed");
        assert!(res.commit_timestamp.is_some());
        assert_eq!(
            res.commit_timestamp
                .expect("commit_timestamp should be present")
                .seconds(),
            1234
        );
    }

    #[tokio_test_no_panics]
    async fn write() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                    ..Default::default()
                },
            ))
        });

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert!(req.options.is_some());
            assert!(req.mutation_key.is_some());

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Transaction {
                    id: vec![42],
                    precommit_token: Some(
                        spanner_grpc_mock::google::spanner::v1::MultiplexedSessionPrecommitToken {
                            precommit_token: vec![1, 2, 3],
                            seq_num: 1,
                        },
                    ),
                    ..Default::default()
                },
            ))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/123");
            assert_eq!(
                req.precommit_token.expect("precommit_token required").precommit_token,
                vec![1, 2, 3]
            );

            // Validate that we pass down the transaction ID from BeginTransaction.
            match req.transaction {
                Some(spanner_grpc_mock::google::spanner::v1::commit_request::Transaction::TransactionId(tid)) => {
                    assert_eq!(tid, vec![42]);
                }
                _ => panic!("Expected TransactionId"),
            }

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::CommitResponse {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 5678,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            ))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await;

        assert!(res.is_ok());
        let res = res.expect("write should succeed");
        assert!(res.commit_timestamp.is_some());
        assert_eq!(
            res.commit_timestamp
                .expect("commit_timestamp should be present")
                .seconds(),
            5678
        );
    }

    #[tokio_test_no_panics]
    async fn write_at_least_once_with_commit_stats() -> anyhow::Result<()> {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.return_commit_stats);

            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1234,
                    nanos: 0,
                }),
                commit_stats: Some(CommitStats { mutation_count: 5 }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .with_return_commit_stats(true)
            .build()
            .write_at_least_once(vec![mutation])
            .await?;

        let stats = res.commit_stats.expect("Commit stats should be present");
        assert_eq!(stats.mutation_count, 5);
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn write_with_commit_stats() -> anyhow::Result<()> {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(Response::new(Transaction {
                id: vec![42],
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.return_commit_stats);

            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 5678,
                    nanos: 0,
                }),
                commit_stats: Some(CommitStats { mutation_count: 10 }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .with_return_commit_stats(true)
            .build()
            .write(vec![mutation])
            .await?;

        let stats = res.commit_stats.expect("Commit stats should be present");
        assert_eq!(stats.mutation_count, 10);
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn write_at_least_once_with_exclude_txn_from_change_streams() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                    ..Default::default()
                },
            ))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            match req.transaction {
                Some(spanner_grpc_mock::google::spanner::v1::commit_request::Transaction::SingleUseTransaction(opts)) => {
                    assert!(opts.exclude_txn_from_change_streams);
                }
                _ => panic!("Expected SingleUseTransaction"),
            }

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::CommitResponse {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 1234,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            ))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .with_exclude_txn_from_change_streams(true)
            .build()
            .write_at_least_once(vec![mutation])
            .await;

        assert!(res.is_ok());
    }

    #[tokio_test_no_panics]
    async fn write_with_exclude_txn_from_change_streams() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                    ..Default::default()
                },
            ))
        });

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            let options = req.options.expect("Missing transaction options");
            assert!(options.exclude_txn_from_change_streams);

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                },
            ))
        });

        mock.expect_commit().once().returning(|_req| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::CommitResponse {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 5678,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            ))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .with_exclude_txn_from_change_streams(true)
            .build()
            .write(vec![mutation])
            .await;

        assert!(res.is_ok());
    }

    #[tokio_test_no_panics]
    async fn write_with_commit_retry() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                    ..Default::default()
                },
            ))
        });

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.mutation_key.is_some());

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                },
            ))
        });

        let commit_call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        mock.expect_commit().times(2).returning(move |req| {
            let count = commit_call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let req = req.into_inner();
            assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/123");

            if count == 0 {
                assert!(!req.mutations.is_empty());
                Ok(gaxi::grpc::tonic::Response::new(
                    spanner_grpc_mock::google::spanner::v1::CommitResponse {
                        multiplexed_session_retry: Some(
                            spanner_grpc_mock::google::spanner::v1::commit_response::MultiplexedSessionRetry::PrecommitToken(
                                spanner_grpc_mock::google::spanner::v1::MultiplexedSessionPrecommitToken {
                                    precommit_token: vec![4, 5, 6],
                                    seq_num: 2,
                                }
                            )
                        ),
                        ..Default::default()
                    },
                ))
            } else {
                assert!(req.mutations.is_empty());
                assert_eq!(
                    req.precommit_token.expect("precommit_token required").precommit_token,
                    vec![4, 5, 6]
                );
                Ok(gaxi::grpc::tonic::Response::new(
                    spanner_grpc_mock::google::spanner::v1::CommitResponse {
                        commit_timestamp: Some(prost_types::Timestamp {
                            seconds: 9999,
                            nanos: 0,
                        }),
                        ..Default::default()
                    },
                ))
            }
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await;

        assert!(res.is_ok());
        let res = res.expect("write should succeed");
        assert!(res.commit_timestamp.is_some());
        assert_eq!(
            res.commit_timestamp
                .expect("commit_timestamp should be present")
                .seconds(),
            9999
        );
    }

    #[tokio_test_no_panics]
    async fn write_with_commit_retry_preserves_options() -> anyhow::Result<()> {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                    ..Default::default()
                },
            ))
        });

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.mutation_key.is_some());

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                },
            ))
        });

        let expected_delay = prost_types::Duration {
            seconds: 0,
            nanos: 200_000_000,
        };

        let expected_delay_clone = expected_delay;
        let commit_call_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        mock.expect_commit().times(2).returning(move |req| {
            let count = commit_call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let req = req.into_inner();
            assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/123");

            // Verify options are present in both attempts
            assert!(req.return_commit_stats, "Expected return_commit_stats to be true");
            assert_eq!(req.max_commit_delay.as_ref(), Some(&expected_delay_clone), "Expected max_commit_delay to be 200ms");

            if count == 0 {
                assert!(!req.mutations.is_empty());
                Ok(gaxi::grpc::tonic::Response::new(
                    spanner_grpc_mock::google::spanner::v1::CommitResponse {
                        multiplexed_session_retry: Some(
                            spanner_grpc_mock::google::spanner::v1::commit_response::MultiplexedSessionRetry::PrecommitToken(
                                spanner_grpc_mock::google::spanner::v1::MultiplexedSessionPrecommitToken {
                                    precommit_token: vec![4, 5, 6],
                                    seq_num: 2,
                                }
                            )
                        ),
                        ..Default::default()
                    },
                ))
            } else {
                assert!(req.mutations.is_empty(), "Expected mutations to be empty on retry");
                assert_eq!(
                    req.precommit_token.expect("precommit_token required").precommit_token,
                    vec![4, 5, 6]
                );
                Ok(gaxi::grpc::tonic::Response::new(
                    spanner_grpc_mock::google::spanner::v1::CommitResponse {
                        commit_timestamp: Some(prost_types::Timestamp {
                            seconds: 9999,
                            nanos: 0,
                        }),
                        commit_stats: Some(CommitStats { mutation_count: 12 }),
                        ..Default::default()
                    },
                ))
            }
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .with_return_commit_stats(true)
            .with_max_commit_delay(Duration::new(0, 200_000_000).expect("valid duration"))
            .build()
            .write(vec![mutation])
            .await?;

        let stats = res.commit_stats.expect("Expected commit stats in response");
        assert_eq!(stats.mutation_count, 12);
        assert_eq!(
            res.commit_timestamp
                .expect("timestamp should be present")
                .seconds(),
            9999
        );

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn write_with_commit_aborted_retry() -> anyhow::Result<()> {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let mut seq = mockall::Sequence::new();

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                assert!(req.mutation_key.is_some());

                Ok(Response::new(Transaction {
                    id: vec![42],
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_req| Err(create_aborted_status(std::time::Duration::from_nanos(1))));

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                assert!(req.mutation_key.is_some());

                let options = req.options.as_ref().expect("options required on retry");
                let read_write = options.mode.as_ref().expect("mode required on retry");
                match read_write {
                    Mode::ReadWrite(rw) => {
                        assert_eq!(rw.multiplexed_session_previous_transaction_id, vec![42], "previous_transaction_id should be set to the ID of the aborted transaction");
                    }
                    _ => panic!("Expected ReadWrite mode"),
                }

                Ok(Response::new(Transaction {
                    id: vec![42],
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_req| {
                Ok(Response::new(CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 8888,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .build()
            .write(vec![mutation])
            .await;

        let res = res.expect("write should succeed");
        assert_eq!(
            res.commit_timestamp
                .expect("commit_timestamp should be present")
                .seconds(),
            8888,
            "expected commit timestamp to match"
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn write_at_least_once_with_max_commit_delay() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(
                req.max_commit_delay,
                Some(ProstDuration {
                    seconds: 0,
                    nanos: 100_000_000, // 100ms
                })
            );

            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 1234,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .with_max_commit_delay(Duration::try_from("0.1s").unwrap())
            .build()
            .write_at_least_once(vec![mutation])
            .await;

        assert!(res.is_ok());
    }

    #[tokio_test_no_panics]
    async fn leader_aware_routing_enabled_by_default() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            assert_eq!(
                req.metadata()
                    .get("x-goog-spanner-route-to-leader")
                    .expect("header required")
                    .to_str()
                    .unwrap(),
                "true"
            );
            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(Timestamp {
                    seconds: 1234,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();
        let res = db_client
            .write_only_transaction()
            .build()
            .write_at_least_once(vec![mutation])
            .await;
        assert!(res.is_ok());
    }

    #[tokio_test_no_panics]
    async fn write_only_transaction_builder_sets_gax_options() -> anyhow::Result<()> {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "session".to_string(),
                ..Default::default()
            }))
        });
        let (db_client, _server) = setup_db_client(mock).await;

        let builder = db_client
            .write_only_transaction()
            .with_begin_attempt_timeout(StdDuration::from_secs(5))
            .with_begin_retry_policy(NeverRetry)
            .with_begin_backoff_policy(ExponentialBackoff::default())
            .with_commit_attempt_timeout(StdDuration::from_secs(10))
            .with_commit_retry_policy(NeverRetry)
            .with_commit_backoff_policy(ExponentialBackoff::default());

        let begin_gax = &builder.begin_gax_options;
        assert_eq!(
            *begin_gax.attempt_timeout(),
            Some(StdDuration::from_secs(5))
        );
        assert!(begin_gax.retry_policy().is_some());
        assert!(begin_gax.backoff_policy().is_some());

        let commit_gax = &builder.commit_gax_options;
        assert_eq!(
            *commit_gax.attempt_timeout(),
            Some(StdDuration::from_secs(10))
        );
        assert!(commit_gax.retry_policy().is_some());
        assert!(commit_gax.backoff_policy().is_some());

        Ok(())
    }

    fn parse_grpc_timeout(metadata: &gaxi::grpc::tonic::MetadataMap) -> Option<StdDuration> {
        let timeout_header = metadata.get("grpc-timeout")?.to_str().ok()?;
        let numeric_part: String = timeout_header
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        let value = numeric_part.parse::<u64>().ok()?;
        let unit = timeout_header.trim_start_matches(&numeric_part);
        let duration = match unit {
            "u" => StdDuration::from_micros(value),
            "m" => StdDuration::from_millis(value),
            "S" => StdDuration::from_secs(value),
            "M" => StdDuration::from_secs(value * 60),
            "H" => StdDuration::from_secs(value * 3600),
            _ => return None,
        };
        Some(duration)
    }

    #[tokio_test_no_panics]
    async fn write_only_transaction_with_custom_options() -> anyhow::Result<()> {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let mut seq = mockall::Sequence::new();

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut seq)
            .withf(|req| {
                let duration =
                    parse_grpc_timeout(req.metadata()).expect("valid grpc-timeout header");
                assert_eq!(duration, StdDuration::from_secs(5));
                true
            })
            .returning(|_| {
                Ok(Response::new(Transaction {
                    id: vec![42],
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .in_sequence(&mut seq)
            .withf(|req| {
                let duration =
                    parse_grpc_timeout(req.metadata()).expect("valid grpc-timeout header");
                assert_eq!(duration, StdDuration::from_secs(10));
                true
            })
            .returning(|_| {
                Ok(Response::new(CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 8888,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .with_begin_attempt_timeout(StdDuration::from_secs(5))
            .with_commit_attempt_timeout(StdDuration::from_secs(10))
            .build()
            .write(vec![mutation])
            .await?;

        assert_eq!(
            res.commit_timestamp
                .expect("commit_timestamp should be present")
                .seconds(),
            8888
        );
        Ok(())
    }

    #[tokio_test_no_panics]
    async fn write_at_least_once_with_custom_commit_options() -> anyhow::Result<()> {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_begin_transaction().never();

        mock.expect_commit()
            .once()
            .withf(|req| {
                let duration =
                    parse_grpc_timeout(req.metadata()).expect("valid grpc-timeout header");
                assert_eq!(duration, StdDuration::from_secs(7));
                true
            })
            .returning(|_| {
                Ok(Response::new(CommitResponse {
                    commit_timestamp: Some(Timestamp {
                        seconds: 7777,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let mutation = Mutation::new_insert_or_update_builder("Users")
            .set("UserId")
            .to(&1)
            .build();

        let res = db_client
            .write_only_transaction()
            .with_commit_attempt_timeout(StdDuration::from_secs(7))
            .build()
            .write_at_least_once(vec![mutation])
            .await?;

        assert_eq!(
            res.commit_timestamp
                .expect("commit_timestamp should be present")
                .seconds(),
            7777
        );
        Ok(())
    }
}
