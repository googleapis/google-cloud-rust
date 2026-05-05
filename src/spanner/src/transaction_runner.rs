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

use crate::database_client::DatabaseClient;
use crate::model::request_options::Priority;
use crate::model::transaction_options::IsolationLevel;
use crate::model::transaction_options::read_write::ReadLockMode;
use crate::read_write_transaction::{ReadWriteTransaction, ReadWriteTransactionBuilder};
use crate::transaction_retry_policy::{
    BasicTransactionRetryPolicy, TransactionRetryPolicy, backoff_if_aborted, is_aborted,
};
use wkt::Duration;

/// A builder for a [TransactionRunner] for a read/write transaction.
///
/// # Example
/// ```
/// # use google_cloud_spanner::client::Spanner;
/// # use google_cloud_spanner::client::Statement;
/// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
/// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
/// let runner = db_client.read_write_transaction().build().await?;
///
/// let result = runner.run(async |transaction| {
///     let statement = Statement::builder("UPDATE MyTable SET MyColumn = 'MyValue' WHERE Id = 1").build();
///     transaction.execute_update(statement).await?;
///     Ok(42)
/// }).await?;
/// # Ok(())
/// # }
/// ```
///
/// Spanner can abort any read/write transaction at any time. A [TransactionRunner]
/// automatically retries aborted transactions according to the configured retry policy.
pub struct TransactionRunnerBuilder {
    builder: ReadWriteTransactionBuilder,
    retry_policy: Box<dyn TransactionRetryPolicy>,
}

impl TransactionRunnerBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            builder: ReadWriteTransactionBuilder::new(client),
            retry_policy: Box::new(BasicTransactionRetryPolicy::default()),
        }
    }

    /// Sets the isolation level for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::model::transaction_options::IsolationLevel;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .with_isolation_level(IsolationLevel::Serializable)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See also: <https://docs.cloud.google.com/spanner/docs/isolation-levels>
    pub fn with_isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.builder = self.builder.with_isolation_level(isolation_level);
        self
    }

    /// Sets the read lock mode for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::model::transaction_options::read_write::ReadLockMode;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .with_read_lock_mode(ReadLockMode::Pessimistic)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See also: <https://docs.cloud.google.com/spanner/docs/concurrency-control>
    pub fn with_read_lock_mode(mut self, read_lock_mode: ReadLockMode) -> Self {
        self.builder = self.builder.with_read_lock_mode(read_lock_mode);
        self
    }

    /// Sets the transaction tag for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .with_transaction_tag("my-tag")
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The tag is applied to all statements executed within the transaction.
    ///
    /// See also: [Troubleshooting with tags](https://docs.cloud.google.com/spanner/docs/introspection/troubleshooting-with-tags)
    pub fn with_transaction_tag(mut self, tag: impl Into<String>) -> Self {
        self.builder = self.builder.with_transaction_tag(tag);
        self
    }

    /// Sets the RPC priority to use for the commit of this transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::model::request_options::Priority;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .with_commit_priority(Priority::Low)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_commit_priority(mut self, priority: Priority) -> Self {
        self.builder = self.builder.with_commit_priority(priority);
        self
    }

    /// Sets the maximum commit delay for the transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use wkt::Duration;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .with_max_commit_delay(Duration::try_from("0.2s").unwrap())
    ///     .build()
    ///     .await?;
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
        self.builder = self.builder.with_max_commit_delay(delay);
        self
    }

    /// Sets whether to exclude the transaction from change streams.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn build_tx(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction()
    ///     .with_exclude_txn_from_change_streams(true)
    ///     .build()
    ///     .await?;
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
        self.builder = self.builder.with_exclude_txn_from_change_streams(exclude);
        self
    }

    /// Sets the retry policy for the transaction.
    ///
    /// # Example
    /// ```
    /// # use std::time::Duration;
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::BasicTransactionRetryPolicy;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    ///
    /// let retry_policy = BasicTransactionRetryPolicy {
    ///     max_attempts: 5,
    ///     total_timeout: Duration::from_secs(60),
    /// };
    ///
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .with_retry_policy(retry_policy)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_retry_policy<P: TransactionRetryPolicy + 'static>(mut self, policy: P) -> Self {
        self.retry_policy = Box::new(policy);
        self
    }

    /// Builds a [TransactionRunner] for a read/write transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::Statement;
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction().build().await?;
    ///
    /// let result = runner.run(async |transaction| {
    ///     let statement = Statement::builder("UPDATE MyTable SET MyColumn = 'MyValue' WHERE Id = 1").build();
    ///     transaction.execute_update(statement).await?;
    ///     Ok(42)
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn build(self) -> crate::Result<TransactionRunner> {
        Ok(TransactionRunner {
            builder: self.builder,
            retry_policy: self.retry_policy,
        })
    }
}

/// A runner for read/write transactions. Aborted transactions are automatically retried.
pub struct TransactionRunner {
    builder: ReadWriteTransactionBuilder,
    retry_policy: Box<dyn TransactionRetryPolicy>,
}

impl TransactionRunner {
    /// Runs the provided closure within the context of a read/write transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # use google_cloud_spanner::client::Statement;
    /// # async fn run_tx(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction().build().await?;
    ///
    /// let result = runner.run(async |transaction| {
    ///     let statement = Statement::builder("UPDATE MyTable SET MyColumn = 'MyValue' WHERE Id = 1").build();
    ///     transaction.execute_update(statement).await?;
    ///     Ok(42) // This will be returned by runner.run()
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// If the transaction is aborted by Spanner, the closure will be retried
    /// automatically according to the configured `TransactionRetryPolicy`.
    ///
    /// The transaction is automatically committed if the closure returns `Ok`.
    /// If the closure returns `Err`, the transaction will be rolled back and
    /// the error will be propagated.
    pub async fn run<T, F>(mut self, mut work: F) -> crate::Result<T>
    where
        F: std::ops::AsyncFnMut(ReadWriteTransaction) -> crate::Result<T>,
    {
        let start_time = tokio::time::Instant::now();
        let mut attempts: u32 = 0;
        let backoff = crate::transaction_retry_policy::default_retry_backoff();

        loop {
            attempts += 1;

            let mut current_tx_id = None;
            let attempt_result = async {
                let transaction = self.builder.begin_transaction().await?;
                current_tx_id = transaction.transaction_id().ok();

                let result = match work(transaction.clone()).await {
                    Ok(res) => res,
                    Err(e) => {
                        // Rollback if the closure failed and it was not an Aborted error.
                        if !is_aborted(&e) {
                            let _ = transaction.rollback().await;
                        }
                        return Err(e);
                    }
                };

                transaction.commit().await?;
                Ok::<T, crate::Error>(result)
            }
            .await;

            match attempt_result {
                Ok(res) => return Ok(res),
                Err(e) => {
                    if is_aborted(&e) {
                        self.builder = self.builder.with_previous_transaction_id(current_tx_id);
                    }

                    backoff_if_aborted(
                        e,
                        attempts,
                        start_time.elapsed(),
                        self.retry_policy.as_ref(),
                        &backoff,
                    )
                    .await?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use crate::transaction_retry_policy::tests::create_aborted_status;
    use gaxi::grpc::tonic;
    use spanner_grpc_mock::google::spanner::v1;
    use spanner_grpc_mock::google::spanner::v1::transaction_options::Mode;

    fn expect_begin_transaction(
        mock: &mut spanner_grpc_mock::MockSpanner,
        times: usize,
        transaction_id: Vec<u8>,
    ) {
        mock.expect_begin_transaction()
            .times(times)
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(
                    req.session,
                    "projects/p/instances/i/databases/d/sessions/123"
                );
                Ok(tonic::Response::new(v1::Transaction {
                    id: transaction_id.clone(),
                    ..Default::default()
                }))
            });
    }

    async fn execute_test_runner(
        mock: spanner_grpc_mock::MockSpanner,
    ) -> Result<i64, crate::Error> {
        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .build()
            .await
            .unwrap();
        runner
            .run(async |tx| {
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                Ok(count)
            })
            .await
    }

    fn commit_response() -> Result<tonic::Response<v1::CommitResponse>, tonic::Status> {
        Ok(tonic::Response::new(v1::CommitResponse {
            commit_timestamp: Some(prost_types::Timestamp {
                seconds: 123456789,
                nanos: 0,
            }),
            ..Default::default()
        }))
    }

    fn row_count_exact_response(
        count: i64,
    ) -> Result<tonic::Response<v1::ResultSet>, tonic::Status> {
        Ok(tonic::Response::new(v1::ResultSet {
            stats: Some(v1::ResultSetStats {
                row_count: Some(v1::result_set_stats::RowCount::RowCountExact(count)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(TransactionRunnerBuilder: Send, Sync);
        static_assertions::assert_impl_all!(TransactionRunner: Send, Sync);
    }

    #[tokio::test]
    async fn run_success() {
        let mut mock = create_session_mock();

        expect_begin_transaction(&mut mock, 1, vec![1, 2, 3]);

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true");
            assert_eq!(req.seqno, 1);
            row_count_exact_response(1)
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    1, 2, 3
                ]))
            );
            commit_response()
        });

        let res = execute_test_runner(mock).await.unwrap();
        assert_eq!(res, 1);
    }

    #[tokio::test]
    async fn run_with_aborted_retry() -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(
                    req.session,
                    "projects/p/instances/i/databases/d/sessions/123"
                );
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![9, 9, 9],
                    ..Default::default()
                }))
            });

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_req| Err(create_aborted_status(std::time::Duration::from_nanos(1))));

        mock.expect_begin_transaction()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(req.session, "projects/p/instances/i/databases/d/sessions/123");

                let options = req.options.as_ref().expect("options required on retry");
                let read_write = options.mode.as_ref().expect("mode required on retry");
                match read_write {
                    Mode::ReadWrite(rw) => {
                        assert_eq!(rw.multiplexed_session_previous_transaction_id, vec![9, 9, 9], "previous_transaction_id should be set to the ID of the aborted transaction");
                    }
                    _ => panic!("Expected ReadWrite mode"),
                }

                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![8, 8, 8],
                    ..Default::default()
                }))
            });

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_req| row_count_exact_response(5));

        mock.expect_commit()
            .once()
            .returning(|_req| commit_response());

        let res = execute_test_runner(mock)
            .await
            .expect("runner should succeed");
        assert_eq!(res, 5);
        Ok(())
    }

    #[tokio::test]
    async fn run_with_non_aborted_error() {
        let mut mock = create_session_mock();

        expect_begin_transaction(&mut mock, 1, vec![9, 9, 9]);

        // Let execute_sql return an error to trigger a rollback.
        mock.expect_execute_sql().once().returning(move |_req| {
            Err(tonic::Status::new(
                tonic::Code::PermissionDenied,
                "permission denied",
            ))
        });

        // Must explicitly trigger rollback
        mock.expect_rollback()
            .once()
            .returning(|_req| Ok(tonic::Response::new(())));

        let res = execute_test_runner(mock).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        if let Some(status) = err.status() {
            assert_eq!(
                status.code,
                google_cloud_gax::error::rpc::Code::PermissionDenied
            );
        } else {
            panic!("Expected GRPC error");
        }
    }

    #[tokio::test]
    async fn run_with_non_aborted_error_and_rollback_fails() {
        let mut mock = create_session_mock();

        expect_begin_transaction(&mut mock, 1, vec![9, 9, 9]);

        // Let execute_sql return an error to trigger a rollback.
        mock.expect_execute_sql().once().returning(move |_req| {
            Err(tonic::Status::new(
                tonic::Code::PermissionDenied,
                "permission denied",
            ))
        });

        // Force the rollback itself to fail as well
        mock.expect_rollback()
            .once()
            .returning(|_req| Err(tonic::Status::new(tonic::Code::Internal, "rollback failed")));

        let res = execute_test_runner(mock).await;

        // Verify the user unequivocally receives the PRIMARY original error
        assert!(res.is_err());
        let err = res.unwrap_err();
        if let Some(status) = err.status() {
            assert_eq!(
                status.code,
                google_cloud_gax::error::rpc::Code::PermissionDenied
            );
        } else {
            panic!("Expected GRPC error");
        }
    }

    #[tokio::test]
    async fn run_commit_aborted_retry() {
        let mut mock = create_session_mock();

        expect_begin_transaction(&mut mock, 2, vec![9, 9, 9]);

        mock.expect_execute_sql()
            .times(2)
            .returning(|_req| row_count_exact_response(5));

        let mut attempt = 0;
        mock.expect_commit().times(2).returning(move |_req| {
            attempt += 1;
            if attempt == 1 {
                Err(create_aborted_status(std::time::Duration::from_nanos(1)))
            } else {
                commit_response()
            }
        });

        let res = execute_test_runner(mock).await.unwrap();
        assert_eq!(res, 5);
    }

    #[tokio::test]
    async fn run_begin_transaction_fails() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction()
            .once()
            .returning(|_req| Err(tonic::Status::new(tonic::Code::Internal, "internal error")));

        let res = execute_test_runner(mock).await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        if let Some(status) = err.status() {
            assert_eq!(status.code, google_cloud_gax::error::rpc::Code::Internal);
        } else {
            panic!("Expected GRPC error");
        }
    }

    #[tokio::test]
    async fn builder_options() {
        use crate::transaction_retry_policy::BasicTransactionRetryPolicy;

        let mock = create_session_mock();
        let (db_client, _server) = setup_db_client(mock).await;

        let retry_policy = BasicTransactionRetryPolicy {
            max_attempts: 1,
            total_timeout: std::time::Duration::from_secs(10),
        };

        // Validate builder chaining safely accepts and compiles options dynamically
        let _runner = TransactionRunnerBuilder::new(db_client)
            .with_isolation_level(IsolationLevel::Serializable)
            .with_read_lock_mode(ReadLockMode::Pessimistic)
            .with_retry_policy(retry_policy)
            .build()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn run_batch_dml_aborted_retry() {
        use crate::batch_dml::BatchDml;
        use crate::statement::Statement;
        use gaxi::grpc::tonic::Code;
        use spanner_grpc_mock::google::rpc::Status;
        use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;

        let mut mock = create_session_mock();

        expect_begin_transaction(&mut mock, 2, vec![9, 9, 9]);

        let mut seq = mockall::Sequence::new();
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_req| {
                // Return a successful response but with an embedded aborted status.
                let status = Status {
                    code: Code::Aborted as i32,
                    message: "transaction aborted".to_string(),
                    ..Default::default()
                };

                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    status: Some(status),
                    ..Default::default()
                }))
            });
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_req| {
                // Return success after the retry.
                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(RowCount::RowCountExact(5)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }))
            });

        mock.expect_commit()
            .once()
            .returning(move |_| commit_response());

        let (db_client, _) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .build()
            .await
            .expect("failed to build TransactionRunner");

        let mut attempt_counter = 0;

        // TransactionRunner retries the closure on transaction aborts
        let res = runner
            .run(async |tx| {
                attempt_counter += 1;
                let stmt = Statement::builder("UPDATE t SET c = 1").build();
                let batch = BatchDml::builder().add_statement(stmt).build();
                let counts = tx.execute_batch_update(batch).await?;
                Ok(counts)
            })
            .await
            .expect("transaction failed");

        assert_eq!(res, vec![5]);
        assert_eq!(attempt_counter, 2);
    }

    #[tokio::test]
    async fn run_with_transaction_tag() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            // Check if the transaction tag is correctly propagated.
            assert_eq!(
                req.request_options
                    .expect("Missing request_options")
                    .transaction_tag,
                "my-test-tag"
            );

            Ok(tonic::Response::new(v1::Transaction {
                id: vec![9, 9, 9],
                ..Default::default()
            }))
        });

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.request_options
                    .expect("Missing request_options")
                    .transaction_tag,
                "my-test-tag"
            );
            row_count_exact_response(5)
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.request_options
                    .expect("Missing request_options")
                    .transaction_tag,
                "my-test-tag"
            );
            commit_response()
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = TransactionRunnerBuilder::new(db_client)
            .with_transaction_tag("my-test-tag")
            .build()
            .await?;

        let res = runner
            .run(async |tx| {
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                Ok(count)
            })
            .await?;

        assert_eq!(res, 5);

        Ok(())
    }

    #[tokio::test]
    async fn run_with_exclude_txn_from_change_streams() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            let options = req.options.expect("Missing transaction options");
            assert!(options.exclude_txn_from_change_streams);

            Ok(tonic::Response::new(v1::Transaction {
                id: vec![9, 9, 9],
                ..Default::default()
            }))
        });

        mock.expect_execute_sql()
            .once()
            .returning(|_req| row_count_exact_response(5));
        mock.expect_commit()
            .once()
            .returning(|_req| commit_response());

        let (db_client, _server) = setup_db_client(mock).await;

        let runner = TransactionRunnerBuilder::new(db_client)
            .with_exclude_txn_from_change_streams(true)
            .build()
            .await?;

        let res = runner
            .run(async |tx| {
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                Ok(count)
            })
            .await?;

        assert_eq!(res, 5);

        Ok(())
    }

    #[tokio::test]
    async fn run_with_max_commit_delay() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        expect_begin_transaction(&mut mock, 1, vec![1, 2, 3]);

        mock.expect_execute_sql()
            .once()
            .returning(|_| row_count_exact_response(1));

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.max_commit_delay,
                Some(::prost_types::Duration {
                    seconds: 0,
                    nanos: 200_000_000, // 200ms
                })
            );
            commit_response()
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_max_commit_delay(Duration::try_from("0.2s").unwrap())
            .build()
            .await?;

        let res = runner
            .run(async |tx| {
                let count = tx.execute_update("UPDATE Users SET active = true").await?;
                Ok(count)
            })
            .await?;
        assert_eq!(res, 1);
        Ok(())
    }
}
