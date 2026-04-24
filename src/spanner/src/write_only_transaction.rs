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

use crate::client::{DatabaseClient, Mutation};
use crate::model::request_options::Priority;
use crate::model::transaction_options::ReadWrite;
use crate::model::{
    BeginTransactionRequest, CommitRequest, CommitResponse, RequestOptions, TransactionOptions,
};
use crate::transaction_retry_policy::{
    BasicTransactionRetryPolicy, TransactionRetryPolicy, retry_aborted,
};
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use wkt::Duration;

/// A builder for [WriteOnlyTransaction].
pub struct WriteOnlyTransactionBuilder {
    client: DatabaseClient,
    transaction_tag: Option<String>,
    max_commit_delay: Option<Duration>,
    retry_policy: Box<dyn TransactionRetryPolicy>,
    exclude_txn_from_change_streams: bool,
    commit_priority: Priority,
}

impl WriteOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            transaction_tag: None,
            max_commit_delay: None,
            retry_policy: Box::new(BasicTransactionRetryPolicy::default()),
            exclude_txn_from_change_streams: false,
            commit_priority: Priority::Unspecified,
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

    /// Sets the retry policy for the transaction.
    ///
    /// # Example
    /// ```
    /// # use std::time::Duration;
    /// # use google_cloud_spanner::client::{BasicTransactionRetryPolicy, Spanner};
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    ///
    /// let retry_policy = BasicTransactionRetryPolicy {
    ///     max_attempts: 5,
    ///     total_timeout: Duration::from_secs(60),
    /// };
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
            commit_priority: self.commit_priority,
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
    commit_priority: Priority,
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
        let req_options = RequestOptions::default()
            .set_transaction_tag(self.transaction_tag.unwrap_or_default())
            .set_priority(self.commit_priority.clone());

        let mutations_proto: Vec<_> = mutations.into_iter().map(|m| m.build_proto()).collect();
        let mutation_key = Mutation::select_mutation_key(&mutations_proto);
        let client = self.client;
        let session_name = self.session_name.clone();
        let previous_transaction_id = Arc::new(Mutex::new(Bytes::new()));

        retry_aborted(&*self.retry_policy, || {
            let client = client.clone();
            let session_name = session_name.clone();
            let req_options = req_options.clone();
            let mutations_proto = mutations_proto.clone();
            let mutation_key = mutation_key.clone();
            let previous_transaction_id = previous_transaction_id.clone();

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
                    .begin_transaction(begin_req, crate::RequestOptions::default())
                    .await?;
                *previous_transaction_id.lock().unwrap() = tx.id.clone();

                let commit_req = CommitRequest::default()
                    .set_session(session_name.clone())
                    .set_mutations(mutations_proto)
                    .set_transaction_id(tx.id.clone())
                    .set_request_options(req_options.clone())
                    .set_or_clear_precommit_token(tx.precommit_token)
                    .set_or_clear_max_commit_delay(self.max_commit_delay);

                let response = client
                    .spanner
                    .commit(commit_req, crate::RequestOptions::default())
                    .await?;

                // If a commit_response with a precommit_token is returned, then we need to
                // retry the commit with the new precommit_token and without any mutations.
                if let Some(new_token) = response.precommit_token().map(|b| *b.clone()) {
                    let retry_commit_req = CommitRequest::default()
                        .set_session(session_name.clone())
                        .set_transaction_id(tx.id)
                        .set_request_options(req_options)
                        .set_precommit_token(new_token);
                    client
                        .spanner
                        .commit(retry_commit_req, crate::RequestOptions::default())
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
            .set_or_clear_max_commit_delay(self.max_commit_delay);
        let client = self.client;

        retry_aborted(&*self.retry_policy, || {
            let client = client.clone();
            let request = request.clone();

            async move {
                client
                    .spanner
                    .commit(request, crate::RequestOptions::default())
                    .await
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Spanner;
    use crate::transaction_retry_policy::tests::create_aborted_status;
    use gaxi::grpc::tonic::Response;
    use prost_types::Duration as ProstDuration;
    use prost_types::Timestamp;
    use spanner_grpc_mock::google::spanner::v1::CommitResponse;
    use spanner_grpc_mock::google::spanner::v1::Session;
    use spanner_grpc_mock::google::spanner::v1::Transaction;
    use spanner_grpc_mock::google::spanner::v1::transaction_options::Mode;
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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
}
