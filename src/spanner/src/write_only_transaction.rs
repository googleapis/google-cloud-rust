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
use crate::model::{
    BeginTransactionRequest, CommitRequest, CommitResponse, RequestOptions, TransactionOptions,
    transaction_options::ReadWrite,
};
use crate::transaction_retry_policy::{
    BasicTransactionRetryPolicy, TransactionRetryPolicy, retry_aborted,
};

/// A builder for [WriteOnlyTransaction].
pub struct WriteOnlyTransactionBuilder {
    client: DatabaseClient,
    transaction_tag: Option<String>,
    retry_policy: Box<dyn TransactionRetryPolicy>,
}

impl WriteOnlyTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            transaction_tag: None,
            retry_policy: Box::new(BasicTransactionRetryPolicy::default()),
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
        WriteOnlyTransaction {
            client: self.client,
            transaction_tag: self.transaction_tag,
            retry_policy: self.retry_policy,
        }
    }
}

/// A write-only transaction.
///
/// A write-only transaction can be used to execute blind writes.
pub struct WriteOnlyTransaction {
    client: DatabaseClient,
    transaction_tag: Option<String>,
    retry_policy: Box<dyn TransactionRetryPolicy>,
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
        let req_options =
            RequestOptions::default().set_transaction_tag(self.transaction_tag.unwrap_or_default());

        let mutations_proto: Vec<_> = mutations.into_iter().map(|m| m.build_proto()).collect();
        let client = self.client;

        retry_aborted(&*self.retry_policy, || {
            let client = client.clone();
            let req_options = req_options.clone();
            let mutations_proto = mutations_proto.clone();

            async move {
                let begin_req = BeginTransactionRequest::default()
                    .set_session(client.session.name.clone())
                    .set_options(
                        TransactionOptions::default()
                            .set_read_write(Box::new(ReadWrite::default())),
                    )
                    .set_request_options(req_options.clone());

                let tx = client
                    .spanner
                    .begin_transaction(begin_req, crate::RequestOptions::default())
                    .await?;

                let commit_req = CommitRequest::default()
                    .set_session(client.session.name.clone())
                    .set_mutations(mutations_proto)
                    .set_transaction_id(tx.id)
                    .set_request_options(req_options);

                client
                    .spanner
                    .commit(commit_req, crate::RequestOptions::default())
                    .await
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
        let single_use = TransactionOptions::new().set_read_write(Box::new(ReadWrite::new()));
        let req_options =
            RequestOptions::new().set_transaction_tag(self.transaction_tag.unwrap_or_default());

        let request = CommitRequest::new()
            .set_session(self.client.session.name.clone())
            .set_mutations(mutations.into_iter().map(|m| m.build_proto()))
            .set_single_use_transaction(Box::new(single_use))
            .set_request_options(req_options);
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

            // Validate the custom request options contain the transaction tag
            assert!(req.request_options.is_some());
            assert_eq!(req.request_options.as_ref().expect("request_options should be present").transaction_tag, "my_tag");

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

            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                },
            ))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/123");

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
}
