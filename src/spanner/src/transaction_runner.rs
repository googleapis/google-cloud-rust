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
use crate::model::transaction_options::IsolationLevel;
use crate::model::transaction_options::read_write::ReadLockMode;
use crate::read_only_transaction::BeginTransactionOption;
use crate::read_write_transaction::{ReadWriteTransaction, ReadWriteTransactionBuilder};
use crate::transaction_retry_policy::{
    BasicTransactionRetryPolicy, TransactionRetryPolicy, backoff_if_aborted, is_aborted,
};
use std::sync::Arc;

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
    /// # use google_cloud_spanner::client::IsolationLevel;
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
    /// # use google_cloud_spanner::client::ReadLockMode;
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

    /// Sets the option for how to start a transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, BeginTransactionOption};
    /// # async fn run(client: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = client.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client
    ///     .read_write_transaction()
    ///     .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// By default, the Spanner client will inline the `BeginTransaction` call with the first query
    /// or DML statement in the transaction. This reduces the number of round-trips to Spanner that
    /// are needed for a transaction. Setting this option to `ExplicitBegin` can be beneficial for specific
    /// transaction shapes:
    ///
    /// 1. When the transaction executes multiple parallel queries at the start of the transaction.
    ///    Only one query can include a `BeginTransaction` option, and all other queries must wait for
    ///    the first query to return the first result before they can proceed to execute. A
    ///    `BeginTransaction` RPC will quickly return a transaction ID and allow all queries to start
    ///    execution in parallel once the transaction ID has been returned.
    /// 2. When the first statement in the transaction could fail. If the statement fails, then it
    ///    will also not start a transaction and return a transaction ID. The transaction will then
    ///    fall back to executing a `BeginTransaction` RPC and retry the first statement.
    ///
    /// Default is `BeginTransactionOption::InlineBegin`.
    pub fn with_begin_transaction_option(mut self, option: BeginTransactionOption) -> Self {
        self.builder = self.builder.with_begin_transaction_option(option);
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

            let shared_tx_id = Arc::new(std::sync::Mutex::new(None));
            let attempt_result = async {
                let transaction = self.builder.build().await?;

                let result = match work(transaction.clone()).await {
                    Ok(res) => res,
                    Err(error) => {
                        let id = transaction.context.transaction_selector.get_id_no_wait();
                        // Rollback if the closure failed and it was not an Aborted error.
                        if !is_aborted(&error) {
                            let _ = transaction.rollback().await;
                        }
                        *shared_tx_id.lock().unwrap() = id;
                        return Err(error);
                    }
                };

                let id = transaction.context.transaction_selector.get_id_no_wait();
                *shared_tx_id.lock().unwrap() = id;
                transaction.commit().await?;
                Ok(result)
            }
            .await;

            match attempt_result {
                Ok(res) => return Ok(res),
                Err(error) => {
                    if is_aborted(&error) {
                        let current_tx_id = shared_tx_id.lock().unwrap().clone();
                        self.builder = self.builder.with_previous_transaction_id(current_tx_id);
                    }

                    backoff_if_aborted(
                        error,
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
        begin_transaction_option: BeginTransactionOption,
    ) -> crate::Result<i64> {
        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;
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
    async fn execute_run_success_explicit() -> anyhow::Result<()> {
        run_success(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio::test]
    async fn execute_run_success_inline() -> anyhow::Result<()> {
        run_success(BeginTransactionOption::InlineBegin).await
    }

    async fn run_success(begin_transaction_option: BeginTransactionOption) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 1, vec![1, 2, 3]);
        }

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET active = true");
            assert_eq!(req.seqno, 1);

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(
                    selector,
                    v1::transaction_selector::Selector::Begin(_)
                ));
            }

            let mut metadata = v1::ResultSetMetadata {
                ..Default::default()
            };
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                metadata.transaction = Some(v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                });
            }

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
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

        let res = execute_test_runner(mock, begin_transaction_option).await?;
        assert_eq!(res, 1);
        Ok(())
    }

    #[tokio::test]
    async fn execute_run_with_aborted_retry_explicit() -> anyhow::Result<()> {
        run_with_aborted_retry(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio::test]
    async fn execute_run_with_aborted_retry_inline() -> anyhow::Result<()> {
        run_with_aborted_retry(BeginTransactionOption::InlineBegin).await
    }

    async fn run_with_aborted_retry(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
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
        }

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            // Attempt 1: execute_sql fails with Aborted
            mock.expect_execute_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    let transaction = req
                        .transaction
                        .as_ref()
                        .expect("transaction options required for inline begin");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));

                    Err(create_aborted_status(std::time::Duration::from_nanos(1)))
                });
        } else {
            mock.expect_execute_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_req| {
                    Err(create_aborted_status(std::time::Duration::from_nanos(1)))
                });
        }

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
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
        }

        // Attempt 2 (retry of closure)
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let req = req.into_inner();
                    let transaction = req.transaction.as_ref().expect("transaction options required for inline begin");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    assert!(matches!(selector, v1::transaction_selector::Selector::Begin(_)));

                    let options = match selector {
                        v1::transaction_selector::Selector::Begin(o) => o,
                        _ => panic!("Expected Begin"),
                    };
                    let read_write = options.mode.as_ref().expect("mode required");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert!(rw.multiplexed_session_previous_transaction_id.is_empty(), "previous_transaction_id should NOT be set because the first attempt failed before getting an ID");
                        }
                        _ => panic!("Expected ReadWrite"),
                    }

                    let mut metadata = v1::ResultSetMetadata { ..Default::default() };
                    metadata.transaction = Some(v1::Transaction { id: vec![8, 8, 8], ..Default::default() });
                    return Ok(tonic::Response::new(v1::ResultSet {
                        metadata: Some(metadata),
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountExact(5)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }));
                }
                row_count_exact_response(5)
            });

        mock.expect_commit()
            .once()
            .returning(|_req| commit_response());

        let res = execute_test_runner(mock, begin_transaction_option)
            .await
            .expect("runner should succeed");
        assert_eq!(res, 5);
        Ok(())
    }

    #[tokio::test]
    async fn execute_run_query_with_aborted_retry_explicit() -> anyhow::Result<()> {
        run_query_with_aborted_retry(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio::test]
    async fn execute_run_query_with_aborted_retry_inline() -> anyhow::Result<()> {
        run_query_with_aborted_retry(BeginTransactionOption::InlineBegin).await
    }

    async fn run_query_with_aborted_retry(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_| {
                    Ok(tonic::Response::new(v1::Transaction {
                        id: vec![9, 9, 9],
                        ..Default::default()
                    }))
                });
        }

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            // Attempt 1: execute_streaming_sql fails with Aborted
            mock.expect_execute_streaming_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    let transaction = req
                        .transaction
                        .as_ref()
                        .expect("transaction options required for inline begin");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));

                    Err(tonic::Status::new(tonic::Code::Aborted, "aborted"))
                });
        } else {
            mock.expect_execute_streaming_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_req| Err(tonic::Status::new(tonic::Code::Aborted, "aborted")));
        }

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    let options = req.options.as_ref().expect("options required on retry");
                    let read_write = options.mode.as_ref().expect("mode required on retry");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert_eq!(
                                rw.multiplexed_session_previous_transaction_id,
                                vec![9, 9, 9]
                            );
                        }
                        _ => panic!("Expected ReadWrite mode"),
                    }

                    Ok(tonic::Response::new(v1::Transaction {
                        id: vec![8, 8, 8],
                        ..Default::default()
                    }))
                });
        }

        // Attempt 2 (retry of closure)
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let req = req.into_inner();
                    let transaction = req
                        .transaction
                        .as_ref()
                        .expect("transaction options required for inline begin");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));

                    let options = match selector {
                        v1::transaction_selector::Selector::Begin(o) => o,
                        _ => panic!("Expected Begin"),
                    };
                    let read_write = options.mode.as_ref().expect("mode required");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert!(rw.multiplexed_session_previous_transaction_id.is_empty());
                        }
                        _ => panic!("Expected ReadWrite"),
                    }
                }

                let mut rs = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("1".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    rs.metadata.as_mut().unwrap().transaction = Some(v1::Transaction {
                        id: vec![8, 8, 8],
                        ..Default::default()
                    });
                }

                Ok(tonic::Response::new(Box::pin(tokio_stream::iter(vec![
                    Ok(rs),
                ]))))
            });

        mock.expect_commit()
            .once()
            .returning(|_req| commit_response());

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;

        let mut attempt_counter = 0;
        let res = runner
            .run(async |tx| {
                attempt_counter += 1;
                let mut rs = tx.execute_query("SELECT 1").await?;
                let row = rs.next().await.expect("has row").expect("has valid row");
                Ok(row.raw_values()[0].as_string().to_string())
            })
            .await?;

        assert_eq!(res, "1");
        assert_eq!(attempt_counter, 2);
        Ok(())
    }

    #[tokio::test]
    async fn execute_run_query_stream_with_aborted_retry_explicit() -> anyhow::Result<()> {
        run_query_stream_with_aborted_retry(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio::test]
    async fn execute_run_query_stream_with_aborted_retry_inline() -> anyhow::Result<()> {
        run_query_stream_with_aborted_retry(BeginTransactionOption::InlineBegin).await
    }

    async fn run_query_stream_with_aborted_retry(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        let tx_id_1 = vec![9, 9, 9];
        let tx_id_2 = vec![8, 8, 8];

        let tx_id_1_c1 = tx_id_1.clone();
        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_| {
                    Ok(tonic::Response::new(v1::Transaction {
                        id: tx_id_1_c1.clone(),
                        ..Default::default()
                    }))
                });
        }

        let tx_id_1_c2 = tx_id_1.clone();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let transaction = req
                        .transaction
                        .as_ref()
                        .expect("transaction options required for inline begin");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));
                }

                let mut rs = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("1".to_string())),
                    }],
                    resume_token: b"token1".to_vec(),
                    ..Default::default()
                };

                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    rs.metadata.as_mut().unwrap().transaction = Some(v1::Transaction {
                        id: tx_id_1_c2.clone(),
                        ..Default::default()
                    });
                }

                let stream = tokio_stream::iter(vec![
                    Ok(rs),
                    Err(tonic::Status::new(tonic::Code::Aborted, "aborted")),
                ]);
                Ok(tonic::Response::new(Box::pin(stream)))
            });

        let tx_id_1_c3 = tx_id_1.clone();
        let tx_id_2_c3 = tx_id_2.clone();
        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    let options = req.options.as_ref().expect("options required on retry");
                    let read_write = options.mode.as_ref().expect("mode required on retry");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert_eq!(rw.multiplexed_session_previous_transaction_id, tx_id_1_c3);
                        }
                        _ => panic!("Expected ReadWrite mode"),
                    }

                    Ok(tonic::Response::new(v1::Transaction {
                        id: tx_id_2_c3.clone(),
                        ..Default::default()
                    }))
                });
        }

        let tx_id_1_c4 = tx_id_1.clone();
        let tx_id_2_c4 = tx_id_2.clone();
        mock.expect_execute_streaming_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let transaction = req
                        .transaction
                        .as_ref()
                        .expect("transaction options required for inline begin");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    let options = match selector {
                        v1::transaction_selector::Selector::Begin(o) => o,
                        _ => panic!("Expected Begin"),
                    };
                    let read_write = options.mode.as_ref().expect("mode required");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert_eq!(rw.multiplexed_session_previous_transaction_id, tx_id_1_c4);
                        }
                        _ => panic!("Expected ReadWrite"),
                    }
                }

                let mut rs = v1::PartialResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        row_type: Some(v1::StructType {
                            fields: vec![Default::default()],
                        }),
                        ..Default::default()
                    }),
                    values: vec![prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue("2".to_string())),
                    }],
                    last: true,
                    ..Default::default()
                };

                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    rs.metadata.as_mut().unwrap().transaction = Some(v1::Transaction {
                        id: tx_id_2_c4.clone(),
                        ..Default::default()
                    });
                }

                Ok(tonic::Response::new(Box::pin(tokio_stream::iter(vec![
                    Ok(rs),
                ]))))
            });

        mock.expect_commit()
            .once()
            .returning(|_req| commit_response());

        let (db_client, _server) = setup_db_client(mock).await;
        let runner = TransactionRunnerBuilder::new(db_client)
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;

        let mut attempt_counter = 0;
        let res = runner
            .run(async |tx| {
                attempt_counter += 1;
                let mut rs = tx.execute_query("SELECT 1").await?;
                let mut rows = vec![];
                while let Some(row_res) = rs.next().await {
                    rows.push(row_res?);
                }
                Ok(rows)
            })
            .await?;

        assert_eq!(attempt_counter, 2);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].raw_values()[0].as_string(), "2");
        Ok(())
    }

    #[tokio::test]
    async fn execute_run_with_non_aborted_error_explicit() {
        run_with_non_aborted_error(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio::test]
    async fn execute_run_with_non_aborted_error_inline() {
        run_with_non_aborted_error(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_with_non_aborted_error(begin_transaction_option: BeginTransactionOption) {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 1, vec![9, 9, 9]);
        }

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            // First execute_sql fails
            mock.expect_execute_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    let transaction = req
                        .transaction
                        .as_ref()
                        .expect("transaction options required for inline begin");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));

                    Err(tonic::Status::new(
                        tonic::Code::PermissionDenied,
                        "permission denied",
                    ))
                });

            // Falls back to begin_transaction
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_| {
                    Ok(tonic::Response::new(v1::Transaction {
                        id: vec![9, 9, 9],
                        ..Default::default()
                    }))
                });

            // Retries execute_sql and fails again
            mock.expect_execute_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    let transaction = req.transaction.as_ref().expect("transaction required");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    assert!(matches!(selector, v1::transaction_selector::Selector::Id(id) if id == &vec![9_u8, 9, 9]));

                    Err(tonic::Status::new(
                        tonic::Code::PermissionDenied,
                        "permission denied",
                    ))
                });
        } else {
            // Let execute_sql return an error to trigger a rollback.
            mock.expect_execute_sql().once().returning(move |_req| {
                Err(tonic::Status::new(
                    tonic::Code::PermissionDenied,
                    "permission denied",
                ))
            });
        }

        // Must explicitly trigger rollback
        mock.expect_rollback()
            .once()
            .returning(|_req| Ok(tonic::Response::new(())));

        let res = execute_test_runner(mock, begin_transaction_option).await;

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
    async fn execute_run_with_non_aborted_error_and_rollback_fails_explicit() {
        run_with_non_aborted_error_and_rollback_fails(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio::test]
    async fn execute_run_with_non_aborted_error_and_rollback_fails_inline() {
        run_with_non_aborted_error_and_rollback_fails(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_with_non_aborted_error_and_rollback_fails(
        begin_transaction_option: BeginTransactionOption,
    ) {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 1, vec![9, 9, 9]);
        }

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            // First execute_sql fails
            mock.expect_execute_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    let transaction = req
                        .transaction
                        .as_ref()
                        .expect("transaction options required for inline begin");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));

                    Err(tonic::Status::new(
                        tonic::Code::PermissionDenied,
                        "permission denied",
                    ))
                });

            // Falls back to begin_transaction
            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(move |_| {
                    Ok(tonic::Response::new(v1::Transaction {
                        id: vec![9, 9, 9],
                        ..Default::default()
                    }))
                });

            // Retries execute_sql and fails again
            mock.expect_execute_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    let transaction = req.transaction.as_ref().expect("transaction required");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    assert!(matches!(selector, v1::transaction_selector::Selector::Id(id) if id == &vec![9_u8, 9, 9]));

                    Err(tonic::Status::new(
                        tonic::Code::PermissionDenied,
                        "permission denied",
                    ))
                });
        } else {
            // Let execute_sql return an error to trigger a rollback.
            mock.expect_execute_sql().once().returning(move |_req| {
                Err(tonic::Status::new(
                    tonic::Code::PermissionDenied,
                    "permission denied",
                ))
            });
        }

        // Force the rollback itself to fail as well
        mock.expect_rollback()
            .once()
            .returning(|_req| Err(tonic::Status::new(tonic::Code::Internal, "rollback failed")));

        let res = execute_test_runner(mock, begin_transaction_option).await;

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
    async fn execute_run_commit_aborted_retry_explicit() -> anyhow::Result<()> {
        run_commit_aborted_retry(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio::test]
    async fn execute_run_commit_aborted_retry_inline() -> anyhow::Result<()> {
        run_commit_aborted_retry(BeginTransactionOption::InlineBegin).await
    }

    async fn run_commit_aborted_retry(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 2, vec![9, 9, 9]);
        }

        let mut attempt = 0;
        mock.expect_execute_sql().times(2).returning(move |req| {
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let req = req.into_inner();
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(
                    selector,
                    v1::transaction_selector::Selector::Begin(_)
                ));

                attempt += 1;
                if attempt == 2 {
                    let options = match selector {
                        v1::transaction_selector::Selector::Begin(o) => o,
                        _ => panic!("Expected Begin"),
                    };
                    let read_write = options.mode.as_ref().expect("mode required");
                    match read_write {
                        Mode::ReadWrite(rw) => {
                            assert_eq!(
                                rw.multiplexed_session_previous_transaction_id,
                                vec![9, 9, 9]
                            );
                        }
                        _ => panic!("Expected ReadWrite"),
                    }
                }

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };
                metadata.transaction = Some(v1::Transaction {
                    id: vec![9, 9, 9],
                    ..Default::default()
                });

                return Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(metadata),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(5)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }));
            }
            row_count_exact_response(5)
        });

        let mut commit_attempt = 0;
        mock.expect_commit().times(2).returning(move |_req| {
            commit_attempt += 1;
            if commit_attempt == 1 {
                Err(create_aborted_status(std::time::Duration::from_nanos(1)))
            } else {
                commit_response()
            }
        });

        let res = execute_test_runner(mock, begin_transaction_option).await?;
        assert_eq!(res, 5);
        Ok(())
    }

    #[tokio::test]
    async fn execute_run_begin_transaction_fails_explicit() {
        run_begin_transaction_fails(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio::test]
    async fn execute_run_begin_transaction_fails_inline() {
        run_begin_transaction_fails(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_begin_transaction_fails(begin_transaction_option: BeginTransactionOption) {
        let mut mock = create_session_mock();
        let mut seq = mockall::Sequence::new();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction()
                .once()
                .returning(|_req| Err(tonic::Status::new(tonic::Code::Internal, "internal error")));
        } else {
            mock.expect_execute_sql()
                .once()
                .in_sequence(&mut seq)
                .returning(move |req| {
                    let req = req.into_inner();
                    let transaction = req
                        .transaction
                        .as_ref()
                        .expect("transaction options required for inline begin");
                    let selector = transaction.selector.as_ref().expect("selector required");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));

                    Err(tonic::Status::new(tonic::Code::Internal, "internal error"))
                });

            mock.expect_begin_transaction()
                .once()
                .in_sequence(&mut seq)
                .returning(|_req| Err(tonic::Status::new(tonic::Code::Internal, "internal error")));
        }

        let res = execute_test_runner(mock, begin_transaction_option).await;

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
    async fn execute_run_batch_dml_aborted_retry_explicit() {
        run_batch_dml_aborted_retry(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio::test]
    async fn execute_run_batch_dml_aborted_retry_inline() {
        run_batch_dml_aborted_retry(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_batch_dml_aborted_retry(begin_transaction_option: BeginTransactionOption) {
        use crate::batch_dml::BatchDml;
        use crate::statement::Statement;
        use gaxi::grpc::tonic::Code;
        use spanner_grpc_mock::google::rpc::Status;
        use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;

        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            expect_begin_transaction(&mut mock, 2, vec![9, 9, 9]);
        }

        let mut seq = mockall::Sequence::new();
        mock.expect_execute_batch_dml()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let req = req.into_inner();
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));
                }

                // Return a successful response but with an embedded aborted status.
                let status = Status {
                    code: Code::Aborted as i32,
                    message: "transaction aborted".to_string(),
                    ..Default::default()
                };

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![9, 9, 9],
                        ..Default::default()
                    });
                }

                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        metadata: Some(metadata),
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
            .returning(move |req| {
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let req = req.into_inner();
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    assert!(matches!(
                        selector,
                        v1::transaction_selector::Selector::Begin(_)
                    ));
                }

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![9, 9, 9],
                        ..Default::default()
                    });
                }

                // Return success after the retry.
                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![v1::ResultSet {
                        metadata: Some(metadata),
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
            .with_begin_transaction_option(begin_transaction_option)
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
    async fn execute_run_with_transaction_tag_explicit() -> anyhow::Result<()> {
        run_with_transaction_tag(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio::test]
    async fn execute_run_with_transaction_tag_inline() -> anyhow::Result<()> {
        run_with_transaction_tag(BeginTransactionOption::InlineBegin).await
    }

    async fn run_with_transaction_tag(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
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
        }

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(
                req.request_options
                    .expect("Missing request_options")
                    .transaction_tag,
                "my-test-tag"
            );

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(
                    selector,
                    v1::transaction_selector::Selector::Begin(_)
                ));
            }

            let mut metadata = v1::ResultSetMetadata {
                ..Default::default()
            };
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                metadata.transaction = Some(v1::Transaction {
                    id: vec![9, 9, 9],
                    ..Default::default()
                });
            }

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(5)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
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
            .with_begin_transaction_option(begin_transaction_option)
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
}
