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

use crate::BatchDml;
use crate::RequestOptions;
use crate::database_client::DatabaseClient;
use crate::error::internal_error;
use crate::model::BeginTransactionRequest;
use crate::model::CommitRequest;
use crate::model::ExecuteBatchDmlRequest;
use crate::model::RollbackRequest;
use crate::model::TransactionOptions;
use crate::model::TransactionSelector;
use crate::model::execute_batch_dml_request::Statement as ExecuteBatchDmlStatement;
use crate::model::request_options::Priority;
use crate::model::result_set_stats::RowCount;
use crate::model::transaction_options::IsolationLevel;
use crate::model::transaction_options::Mode;
use crate::model::transaction_options::ReadWrite;
use crate::model::transaction_options::read_write::ReadLockMode;
use crate::model::transaction_selector::Selector;
use crate::precommit::PrecommitTokenTracker;
use crate::read_only_transaction::ReadContext;
use crate::result_set::ResultSet;
use crate::statement::Statement;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use wkt::Duration;

/// A builder for [ReadWriteTransaction].
#[derive(Clone, Debug)]
pub(crate) struct ReadWriteTransactionBuilder {
    client: DatabaseClient,
    options: TransactionOptions,
    transaction_tag: Option<String>,
    max_commit_delay: Option<Duration>,
    pub(crate) session_name: String,
    commit_priority: Priority,
}

impl ReadWriteTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        let session_name = client.session_name();
        Self {
            client,
            options: TransactionOptions::default().set_read_write(ReadWrite::default()),
            transaction_tag: None,
            max_commit_delay: None,
            session_name,
            commit_priority: Priority::Unspecified,
        }
    }

    pub(crate) fn with_isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.options = self.options.set_isolation_level(isolation_level);
        self
    }

    pub(crate) fn with_read_lock_mode(mut self, read_lock_mode: ReadLockMode) -> Self {
        if let Some(Mode::ReadWrite(rw)) = self.options.mode.take() {
            self.options = self
                .options
                .set_read_write(rw.set_read_lock_mode(read_lock_mode));
        }
        self
    }

    pub(crate) fn with_previous_transaction_id(mut self, id: Option<bytes::Bytes>) -> Self {
        if let Some(id) = id {
            if let Some(Mode::ReadWrite(rw)) = self.options.mode.take() {
                self.options = self
                    .options
                    .set_read_write(rw.set_multiplexed_session_previous_transaction_id(id));
            }
        }
        self
    }

    pub(crate) fn with_transaction_tag(mut self, tag: impl Into<String>) -> Self {
        self.transaction_tag = Some(tag.into());
        self
    }

    pub(crate) fn with_commit_priority(mut self, priority: Priority) -> Self {
        self.commit_priority = priority;
        self
    }

    pub(crate) fn with_max_commit_delay(mut self, delay: Duration) -> Self {
        self.max_commit_delay = Some(delay);
        self
    }

    pub(crate) fn with_exclude_txn_from_change_streams(mut self, exclude: bool) -> Self {
        self.options = self.options.set_exclude_txn_from_change_streams(exclude);
        self
    }

    pub(crate) async fn begin_transaction(&self) -> crate::Result<ReadWriteTransaction> {
        let session_name = self.session_name.clone();
        let mut request = BeginTransactionRequest::default()
            .set_session(session_name.clone())
            .set_options(self.options.clone());
        if let Some(tag) = &self.transaction_tag {
            request = request.set_request_options(
                crate::model::RequestOptions::default().set_transaction_tag(tag.clone()),
            );
        }

        // TODO(#4972): make request options configurable
        let response = self
            .client
            .spanner
            .begin_transaction(request, RequestOptions::default())
            .await?;

        let transaction_selector =
            crate::read_only_transaction::ReadContextTransactionSelector::Fixed(
                TransactionSelector::default().set_id(response.id),
                None,
            );
        Ok(ReadWriteTransaction {
            context: ReadContext {
                session_name,
                client: self.client.clone(),
                transaction_selector,
                precommit_token_tracker: PrecommitTokenTracker::new(),
                transaction_tag: self.transaction_tag.clone(),
            },
            seqno: Arc::new(AtomicI64::new(1)),
            max_commit_delay: self.max_commit_delay,
            commit_priority: self.commit_priority.clone(),
        })
    }
}

/// A read-write transaction.
#[derive(Clone, Debug)]
pub struct ReadWriteTransaction {
    pub(crate) context: ReadContext,
    seqno: Arc<AtomicI64>,
    max_commit_delay: Option<Duration>,
    commit_priority: Priority,
}

impl ReadWriteTransaction {
    /// Executes a query using this transaction.
    pub async fn execute_query<T: Into<Statement>>(
        &self,
        statement: T,
    ) -> crate::Result<ResultSet> {
        self.context.execute_query(statement).await
    }

    /// Reads rows from the database using key lookups and scans, as a simple key/value style alternative to execute_query.
    pub async fn execute_read<T: Into<crate::read::ReadRequest>>(
        &self,
        read: T,
    ) -> crate::Result<ResultSet> {
        self.context.execute_read(read).await
    }

    /// Executes an update using this transaction.
    pub async fn execute_update<T: Into<Statement>>(&self, statement: T) -> crate::Result<i64> {
        let seqno = self.seqno.fetch_add(1, Ordering::SeqCst);
        let statement = statement.into();
        let gax_options = statement.gax_options().clone();
        let mut request = statement
            .into_request()
            .set_session(self.context.session_name.clone())
            .set_transaction(self.context.transaction_selector.selector())
            .set_seqno(seqno);
        request.request_options = self.context.amend_request_options(request.request_options);

        let response = self
            .context
            .client
            .spanner
            .execute_sql(request, gax_options)
            .await?;
        self.context
            .precommit_token_tracker
            .update(response.precommit_token);

        let stats = response
            .stats
            .ok_or_else(|| internal_error("No stats returned"))?;
        match stats.row_count {
            Some(RowCount::RowCountExact(c)) => Ok(c),
            _ => Err(internal_error(
                "ExecuteSql returned an invalid or missing row count type for a read/write transaction",
            )),
        }
    }

    /// Executes a batch of DML statements using this transaction.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # use google_cloud_spanner::batch_dml::BatchDml;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// let runner = db_client.read_write_transaction().build().await?;
    /// let result = runner.run(async |transaction| {
    ///     let statement1 = Statement::builder("UPDATE users SET active = true WHERE id = @id")
    ///         .add_param("id", &1)
    ///         .build();
    ///     let statement2 = Statement::builder("UPDATE users SET active = true WHERE id = @id")
    ///         .add_param("id", &2)
    ///         .build();
    ///     let batch = BatchDml::builder()
    ///         .add_statement(statement1)
    ///         .add_statement(statement2)
    ///         .build();
    ///     let update_counts = transaction.execute_batch_update(batch).await?;
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// If a `BatchDml` request fails halfway through execution, `execute_batch_update` will return a
    /// `BatchUpdateError` indicating exactly which statements succeeded (and their respective update counts)
    /// before the batch execution failed.
    ///
    /// # Error Handling Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, Statement};
    /// # use google_cloud_spanner::batch_dml::BatchDml;
    /// # use google_cloud_spanner::BatchUpdateError;
    /// # async fn build(spanner: Spanner) -> Result<(), google_cloud_spanner::Error> {
    /// # let db_client = spanner.database_client("projects/p/instances/i/databases/d").build().await?;
    /// # let runner = db_client.read_write_transaction().build().await?;
    /// # let result = runner.run(async |transaction| {
    /// let statement1 = Statement::builder("UPDATE users SET active = true WHERE id = 1").build();
    /// let statement2 = Statement::builder("UPDATE non_existent_table SET active = true WHERE id = 2").build();
    ///
    /// let batch = BatchDml::builder()
    ///     .add_statement(statement1)
    ///     .add_statement(statement2)
    ///     .build();
    ///
    /// match transaction.execute_batch_update(batch).await {
    ///     Ok(update_counts) => {
    ///         println!("All statements succeeded. Update counts: {:?}", update_counts);
    ///     }
    ///     Err(e) => {
    ///         if let Some(batch_error) = BatchUpdateError::extract(&e) {
    ///             println!("Batch execution failed. Successful update counts: {:?}", batch_error.update_counts);
    ///         } else {
    ///             println!("RPC failed or internal error occurred: {}", e);
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_batch_update(&self, batch: BatchDml) -> crate::Result<Vec<i64>> {
        let seqno = self.seqno.fetch_add(1, Ordering::SeqCst);

        let statements: Vec<ExecuteBatchDmlStatement> = batch
            .statements
            .into_iter()
            .map(|stmt: crate::statement::Statement| stmt.into_batch_statement())
            .collect();

        let request = ExecuteBatchDmlRequest::default()
            .set_session(self.context.session_name.clone())
            .set_transaction(self.context.transaction_selector.selector())
            .set_seqno(seqno)
            .set_statements(statements)
            .set_or_clear_request_options(
                self.context.amend_request_options(batch.request_options),
            );

        let response_result = self
            .context
            .client
            .spanner
            .execute_batch_dml(request, batch.gax_options)
            .await;

        match response_result {
            Ok(response) => {
                self.context
                    .precommit_token_tracker
                    .update(response.precommit_token.clone());
                crate::batch_dml::process_response(response)
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) fn transaction_id(&self) -> crate::Result<bytes::Bytes> {
        match &self.context.transaction_selector.selector().selector {
            Some(Selector::Id(id)) => Ok(id.clone()),
            _ => Err(internal_error("Transaction ID is missing")),
        }
    }

    fn commit_request_options(&self) -> Option<crate::model::RequestOptions> {
        let mut options = self.context.amend_request_options(None);
        if self.commit_priority != Priority::Unspecified {
            options
                .get_or_insert_with(crate::model::RequestOptions::default)
                .priority = self.commit_priority.clone();
        }
        options
    }

    /// Commits the transaction.
    pub(crate) async fn commit(self) -> crate::Result<wkt::Timestamp> {
        let transaction_id = self.transaction_id()?;
        let precommit_token = self.context.precommit_token_tracker.get();
        let request = CommitRequest::default()
            .set_session(self.context.session_name.clone())
            .set_transaction_id(transaction_id.clone())
            .set_or_clear_precommit_token(precommit_token)
            .set_or_clear_request_options(self.commit_request_options())
            .set_or_clear_max_commit_delay(self.max_commit_delay);

        let response = self
            .context
            .client
            .spanner
            .commit(request, RequestOptions::default())
            .await?;

        let response =
            if let Some(new_precommit_token) = response.precommit_token().map(|b| (*b).clone()) {
                let retry_commit_req = CommitRequest::default()
                    .set_session(self.context.session_name.clone())
                    .set_transaction_id(transaction_id)
                    .set_precommit_token(*new_precommit_token)
                    .set_or_clear_request_options(self.commit_request_options());

                self.context
                    .client
                    .spanner
                    .commit(retry_commit_req, RequestOptions::default())
                    .await?
            } else {
                response
            };

        let timestamp = response
            .commit_timestamp
            .ok_or_else(|| internal_error("No commit timestamp returned"))?;
        Ok(timestamp)
    }

    /// Rolls back the transaction.
    pub(crate) async fn rollback(self) -> crate::Result<()> {
        let transaction_id = self.transaction_id()?;

        let request = RollbackRequest::default()
            .set_session(self.context.session_name.clone())
            .set_transaction_id(transaction_id);

        self.context
            .client
            .spanner
            .rollback(request, RequestOptions::default())
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BatchUpdateError;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use gaxi::grpc::tonic;
    use spanner_grpc_mock::google::spanner::v1;
    use std::fmt::Debug;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(ReadWriteTransactionBuilder: Send, Sync, Clone, Debug);
        static_assertions::assert_impl_all!(ReadWriteTransaction: Send, Sync, Debug);
    }

    #[tokio::test]
    async fn read_write_transaction_commit_retry() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![0, 0, 7],
                ..Default::default()
            }))
        });

        // execute_update returns a precommit token.
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Bob' WHERE Id = 1");
            Ok(tonic::Response::new(v1::ResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![101],
                    seq_num: 1,
                }),
                ..Default::default()
            }))
        });

        // Simulate that commit returns a precommit token in the response.
        // This would normally not happen, but we test it here to verify
        // that the commit is retried.
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.precommit_token,
                Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![101],
                    seq_num: 1,
                })
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                multiplexed_session_retry: Some(
                    v1::commit_response::MultiplexedSessionRetry::PrecommitToken(
                        v1::MultiplexedSessionPrecommitToken {
                            precommit_token: vec![202],
                            seq_num: 2,
                        },
                    ),
                ),
                ..Default::default()
            }))
        });

        // Second commit retry is automatically issued with the new token
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.precommit_token,
                Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![202],
                    seq_num: 2,
                })
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1001,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .begin_transaction()
            .await
            .expect("Failed to build transaction");

        let count = tx
            .execute_update("UPDATE Users SET Name = 'Bob' WHERE Id = 1")
            .await
            .unwrap();
        assert_eq!(count, 1);

        let timestamp = tx.commit().await.unwrap();
        assert_eq!(timestamp.seconds(), 1001);
    }

    #[tokio::test]
    async fn read_write_transaction_execute_update() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
            assert_eq!(req.seqno, 1);
            Ok(tonic::Response::new(v1::ResultSet {
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
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(
                req.transaction,
                Some(v1::commit_request::Transaction::TransactionId(vec![
                    1, 2, 3
                ]))
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .begin_transaction()
            .await
            .expect("Failed to build transaction");
        let count = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await
            .expect("Failed to execute update");
        assert_eq!(count, 1);

        let ts = tx.commit().await.expect("Failed to commit");
        assert_eq!(ts.seconds(), 123456789);
    }

    #[tokio::test]
    async fn read_write_transaction_execute_update_invalid_stats() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });

        mock.expect_execute_sql().once().returning(|_| {
            Ok(tonic::Response::new(v1::ResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountLowerBound(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .begin_transaction()
            .await
            .expect("Failed to build transaction");

        let result = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await;

        let err = result.expect_err("Expected an error for invalid row count stats");
        assert!(
            format!("{:?}", err).contains("invalid or missing row count type"),
            "Error did not contain expected message: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn read_write_transaction_rollback() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
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

        mock.expect_rollback().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.transaction_id, vec![9, 9, 9]);
            Ok(tonic::Response::new(()))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .begin_transaction()
            .await
            .expect("Failed to build transaction");

        tx.rollback().await.expect("Failed to rollback");
    }

    #[tokio::test]
    async fn read_write_transaction_execute_batch_update() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![4, 5, 6],
                ..Default::default()
            }))
        });

        mock.expect_execute_batch_dml().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.statements.len(), 2);
            assert_eq!(
                req.statements[0].sql,
                "UPDATE Users SET Name = 'Alice' WHERE Id = 1"
            );
            assert_eq!(
                req.statements[1].sql,
                "UPDATE Users SET Name = 'Bob' WHERE Id = 2"
            );

            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![
                    v1::ResultSet {
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    v1::ResultSet {
                        stats: Some(v1::ResultSetStats {
                            row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client)
            .begin_transaction()
            .await?;

        let batch = BatchDml::builder()
            .add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .add_statement("UPDATE Users SET Name = 'Bob' WHERE Id = 2");

        let counts = tx.execute_batch_update(batch.build()).await?;

        assert_eq!(counts, vec![1, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_execute_batch_update_partial_failure() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![7, 8, 9],
                ..Default::default()
            }))
        });

        mock.expect_execute_batch_dml().once().returning(|_| {
            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: gaxi::grpc::tonic::Code::AlreadyExists as i32,
                    message: "row already exists".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client)
            .begin_transaction()
            .await?;

        let batch = BatchDml::builder()
            .add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .add_statement("INSERT INTO Users (Id) VALUES (2)"); // assuming this fails

        let res = tx.execute_batch_update(batch.build()).await;

        let err = res.expect_err("expected error");
        use std::error::Error;
        let batch_err = err
            .source()
            .and_then(|e| e.downcast_ref::<BatchUpdateError>())
            .expect("should be BatchUpdateError");
        assert_eq!(batch_err.update_counts, vec![1]);
        assert_eq!(
            batch_err.status.status().expect("status").code,
            (gaxi::grpc::tonic::Code::AlreadyExists as i32).into()
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_execute_multiple_updates() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![4, 5, 6],
                ..Default::default()
            }))
        });

        let counter = Arc::new(AtomicI64::new(1));
        mock.expect_execute_sql().times(3).returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
            let c = counter.fetch_add(1, Ordering::SeqCst);
            assert_eq!(req.seqno, c);

            Ok(tonic::Response::new(v1::ResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .begin_transaction()
            .await
            .expect("Failed to build transaction");

        for i in 1..=3 {
            let count = tx
                .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                .await
                .unwrap_or_else(|_| panic!("Failed to execute update {}", i));
            assert_eq!(count, 1);
        }
    }

    #[tokio::test]
    async fn read_write_transaction_execute_query() {
        use crate::client::Statement;
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![7, 8, 9],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT 1");
            // Queries do not need to include a sequence number.
            assert_eq!(req.seqno, 0);

            assert_eq!(
                req.transaction,
                Some(v1::TransactionSelector {
                    selector: Some(v1::transaction_selector::Selector::Id(vec![7, 8, 9]))
                })
            );

            let (_, rx) = tokio::sync::mpsc::channel(1);
            Ok(tonic::Response::from(rx))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .begin_transaction()
            .await
            .expect("Failed to build transaction");

        let mut rs = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await
            .expect("Failed to execute query");

        let result = rs.next().await;
        assert!(result.is_none(), "expected None, got empty stream");
    }

    #[tokio::test]
    async fn read_write_transaction_with_options() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );

            let options = req.options.expect("missing transaction options");
            let mode = options.mode.expect("missing mode");
            match mode {
                v1::transaction_options::Mode::ReadWrite(rw) => {
                    assert_eq!(
                        rw.read_lock_mode,
                        v1::transaction_options::read_write::ReadLockMode::Pessimistic as i32
                    );
                }
                _ => panic!("Expected ReadWrite transaction mode"),
            }
            // Ensure isolation level is passed through
            assert_eq!(
                options.isolation_level,
                v1::transaction_options::IsolationLevel::Serializable as i32
            );

            Ok(tonic::Response::new(v1::Transaction {
                id: vec![9, 9, 9],
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let _tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .with_isolation_level(IsolationLevel::Serializable)
            .with_read_lock_mode(ReadLockMode::Pessimistic)
            .begin_transaction()
            .await
            .expect("Failed to build transaction");
    }

    #[tokio::test]
    async fn read_write_transaction_with_exclude_txn_from_change_streams() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|req| {
            let req = req.into_inner();
            let options = req.options.expect("missing transaction options");
            assert!(options.exclude_txn_from_change_streams);

            Ok(tonic::Response::new(v1::Transaction {
                id: vec![9, 9, 9],
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let _tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .with_exclude_txn_from_change_streams(true)
            .begin_transaction()
            .await
            .expect("Failed to build transaction");
    }

    #[tokio::test]
    async fn read_write_transaction_tracks_highest_precommit_token() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![4, 2],
                ..Default::default()
            }))
        });

        // 3 sequential updates returning tokens [seq 2, seq 5, seq 3]
        let tokens_iter = vec![2, 5, 3].into_iter();
        let counter_mutex = std::sync::Mutex::new(tokens_iter);

        mock.expect_execute_sql().times(3).returning(move |_req| {
            let seq = counter_mutex
                .lock()
                .expect("Failed to lock mutex")
                .next()
                .expect("Failed to get next token");
            Ok(tonic::Response::new(v1::ResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![seq as u8],
                    seq_num: seq,
                }),
                ..Default::default()
            }))
        });

        // Commit should only use the highest token (seq 5)
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.precommit_token,
                Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![5],
                    seq_num: 5,
                })
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 12345,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .begin_transaction()
            .await
            .expect("Failed to build transaction");

        for _ in 0..3 {
            tx.execute_update("UPDATE Y")
                .await
                .expect("Failed to execute update");
        }
        let ts = tx.commit().await.expect("Failed to commit transaction");
        assert_eq!(ts.seconds(), 12345);
    }

    #[tokio::test]
    async fn read_write_transaction_commit_retry_exactly_once() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![7, 7],
                ..Default::default()
            }))
        });

        // Initial commit returns a retry token (seq 2)
        mock.expect_commit().once().returning(|_| {
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1000,
                    nanos: 0,
                }),
                multiplexed_session_retry: Some(
                    v1::commit_response::MultiplexedSessionRetry::PrecommitToken(
                        v1::MultiplexedSessionPrecommitToken {
                            precommit_token: vec![2],
                            seq_num: 2,
                        },
                    ),
                ),
                ..Default::default()
            }))
        });

        // Retry commit returns another retry token (seq 3).
        // The library should not retry multiple times.
        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.precommit_token
                    .as_ref()
                    .expect("Missing precommit token in retry req")
                    .seq_num,
                2
            );

            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 9999,
                    nanos: 0,
                }),
                multiplexed_session_retry: Some(
                    v1::commit_response::MultiplexedSessionRetry::PrecommitToken(
                        v1::MultiplexedSessionPrecommitToken {
                            precommit_token: vec![3],
                            seq_num: 3,
                        },
                    ),
                ),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;
        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .begin_transaction()
            .await
            .expect("Failed to build transaction");

        let ts = tx.commit().await.expect("Failed to commit transaction");
        assert_eq!(ts.seconds(), 9999);
    }

    #[tokio::test]
    async fn read_write_transaction_commit_with_max_commit_delay() {
        let mut mock = create_session_mock();

        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(
                req.max_commit_delay,
                Some(::prost_types::Duration {
                    seconds: 0,
                    nanos: 200_000_000, // 200ms
                })
            );
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .with_max_commit_delay(Duration::new(0, 200_000_000).unwrap())
            .begin_transaction()
            .await
            .expect("Failed to build transaction");

        let ts = tx.commit().await.expect("Failed to commit");
        assert_eq!(ts.seconds(), 123456789);
    }
}
