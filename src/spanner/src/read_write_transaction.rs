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
use crate::model::CommitRequest;
use crate::model::ExecuteBatchDmlRequest;
use crate::model::RollbackRequest;
use crate::model::TransactionOptions;
use crate::model::execute_batch_dml_request::Statement as ExecuteBatchDmlStatement;
use crate::model::result_set_stats::RowCount;
use crate::model::transaction_options::IsolationLevel;
use crate::model::transaction_options::Mode;
use crate::model::transaction_options::ReadWrite;
use crate::model::transaction_options::read_write::ReadLockMode;
use crate::model::transaction_selector::Selector;
use crate::precommit::PrecommitTokenTracker;
use crate::read_only_transaction::{BeginTransactionOption, ReadContext};
use crate::result_set::ResultSet;
use crate::statement::Statement;
use crate::transaction_retry_policy::is_aborted;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

/// A builder for [ReadWriteTransaction].
#[derive(Clone, Debug)]
pub(crate) struct ReadWriteTransactionBuilder {
    client: DatabaseClient,
    options: TransactionOptions,
    transaction_tag: Option<String>,
    begin_transaction_option: BeginTransactionOption,
}

impl ReadWriteTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            options: TransactionOptions::default().set_read_write(ReadWrite::default()),
            transaction_tag: None,
            begin_transaction_option: BeginTransactionOption::InlineBegin,
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

    /// Sets the option for how to start a transaction.
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
    pub(crate) fn with_begin_transaction_option(mut self, option: BeginTransactionOption) -> Self {
        self.begin_transaction_option = option;
        self
    }

    async fn begin(
        &self,
    ) -> crate::Result<crate::read_only_transaction::ReadContextTransactionSelector> {
        let response = crate::read_only_transaction::execute_begin_transaction(
            &self.client,
            self.options.clone(),
            self.transaction_tag.clone(),
        )
        .await?;

        Ok(
            crate::read_only_transaction::ReadContextTransactionSelector::Fixed(
                crate::model::TransactionSelector::default().set_id(response.id),
                None,
            ),
        )
    }

    pub(crate) async fn build(&self) -> crate::Result<ReadWriteTransaction> {
        let transaction_selector = match self.begin_transaction_option {
            BeginTransactionOption::ExplicitBegin => self.begin().await?,
            BeginTransactionOption::InlineBegin => {
                crate::read_only_transaction::ReadContextTransactionSelector::Lazy(Arc::new(
                    Mutex::new(crate::read_only_transaction::TransactionState::NotStarted(
                        self.options.clone(),
                    )),
                ))
            }
        };

        Ok(ReadWriteTransaction {
            context: ReadContext {
                client: self.client.clone(),
                transaction_selector,
                precommit_token_tracker: PrecommitTokenTracker::new(),
                transaction_tag: self.transaction_tag.clone(),
            },
            seqno: Arc::new(AtomicI64::new(1)),
        })
    }
}

/// A read-write transaction.
#[derive(Clone, Debug)]
pub struct ReadWriteTransaction {
    pub(crate) context: ReadContext,
    seqno: Arc<AtomicI64>,
}

/// Helper macro to execute a DML or BatchDML RPC with retry logic if the
/// request included a BeginTransaction option.
macro_rules! execute_with_retry {
    ($self:expr, $request:ident, $rpc_method:ident, $extract_id:expr) => {{
        let is_starting = matches!(
            $request
                .transaction
                .as_ref()
                .and_then(|t| t.selector.as_ref()),
            Some(Selector::Begin(_))
        );

        let response_result = $self
            .context
            .client
            .spanner
            .$rpc_method($request.clone(), RequestOptions::default())
            .await;

        let response = match response_result {
            Ok(response) => {
                if is_starting {
                    let id = $extract_id(&response).ok_or_else(|| {
                        crate::error::internal_error("Transaction ID was not returned by Spanner")
                    })?;
                    $self.context.transaction_selector.update(id, None)?;
                }
                response
            }
            Err(error) => {
                if !is_starting {
                    return Err(error);
                }
                if is_aborted(&error) {
                    return Err(error);
                }

                $self
                    .context
                    .transaction_selector
                    .begin_explicitly(&$self.context.client)
                    .await?;

                $request.transaction = Some($self.context.transaction_selector.selector().await?);

                $self
                    .context
                    .client
                    .spanner
                    .$rpc_method($request.clone(), RequestOptions::default())
                    .await?
            }
        };

        response
    }};
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
        let mut request = statement
            .into()
            .into_request()
            .set_session(self.context.client.session.name.clone())
            .set_transaction(self.context.transaction_selector.selector().await?)
            .set_seqno(seqno);
        request.request_options = self.context.amend_request_options(request.request_options);

        let response = execute_with_retry!(
            self,
            request,
            execute_sql,
            |response: &crate::model::ResultSet| {
                response
                    .metadata
                    .as_ref()
                    .and_then(|md| md.transaction.as_ref())
                    .map(|t| t.id.clone())
            }
        );

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

        let BatchDml {
            statements,
            request_options,
        } = batch;
        let statements: Vec<ExecuteBatchDmlStatement> = statements
            .into_iter()
            .map(|stmt: crate::statement::Statement| stmt.into_batch_statement())
            .collect();

        let mut request = ExecuteBatchDmlRequest::default()
            .set_session(self.context.client.session.name.clone())
            .set_transaction(self.context.transaction_selector.selector().await?)
            .set_seqno(seqno)
            .set_statements(statements)
            .set_or_clear_request_options(self.context.amend_request_options(request_options));

        let response = execute_with_retry!(
            self,
            request,
            execute_batch_dml,
            |response: &crate::model::ExecuteBatchDmlResponse| {
                response
                    .result_sets
                    .first()
                    .and_then(|rs| rs.metadata.as_ref())
                    .and_then(|md| md.transaction.as_ref())
                    .map(|t| t.id.clone())
            }
        );
        self.context
            .precommit_token_tracker
            .update(response.precommit_token.clone());
        crate::batch_dml::process_response(response)
    }

    pub(crate) async fn transaction_id(&self) -> crate::Result<bytes::Bytes> {
        match &self.context.transaction_selector.selector().await?.selector {
            Some(Selector::Id(id)) => Ok(id.clone()),
            _ => Err(internal_error("Transaction ID is missing")),
        }
    }

    /// Commits the transaction.
    pub(crate) async fn commit(self) -> crate::Result<wkt::Timestamp> {
        let transaction_id = self.transaction_id().await?;
        let precommit_token = self.context.precommit_token_tracker.get();
        let request = CommitRequest::default()
            .set_session(self.context.client.session.name.clone())
            .set_transaction_id(transaction_id.clone())
            .set_or_clear_precommit_token(precommit_token)
            .set_or_clear_request_options(self.context.amend_request_options(None));

        let response = self
            .context
            .client
            .spanner
            .commit(request, RequestOptions::default())
            .await?;

        let response =
            if let Some(new_precommit_token) = response.precommit_token().map(|b| (*b).clone()) {
                let retry_commit_req = CommitRequest::default()
                    .set_session(self.context.client.session.name.clone())
                    .set_transaction_id(transaction_id)
                    .set_precommit_token(*new_precommit_token)
                    .set_or_clear_request_options(self.context.amend_request_options(None));

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
        let transaction_id = self.transaction_id().await?;

        let request = RollbackRequest::default()
            .set_session(self.context.client.session.name.clone())
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
    use v1::result_set_stats::RowCount;
    use v1::transaction_options::Mode;
    use v1::transaction_selector::Selector;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(ReadWriteTransactionBuilder: Send, Sync, Clone, Debug);
        static_assertions::assert_impl_all!(ReadWriteTransaction: Send, Sync, Debug);
    }

    #[tokio::test]
    async fn read_write_transaction_commit_retry_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_commit_retry(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio::test]
    async fn read_write_transaction_commit_retry_inline() -> anyhow::Result<()> {
        run_read_write_transaction_commit_retry(BeginTransactionOption::InlineBegin).await
    }

    async fn run_read_write_transaction_commit_retry(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
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
        }

        // execute_update returns a precommit token.
        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Bob' WHERE Id = 1");

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(selector, Selector::Begin(_)));
            }

            let mut metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType { fields: vec![] }),
                ..Default::default()
            };
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                metadata.transaction = Some(v1::Transaction {
                    id: vec![0, 0, 7],
                    ..Default::default()
                });
            }

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(metadata),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                    precommit_token: vec![101],
                    seq_num: 1,
                }),
                ..Default::default()
            }))
        });

        let mut seq = mockall::Sequence::new();

        // Simulate that commit returns a precommit token in the response.
        // This would normally not happen, but we test it here to verify
        // that the commit is retried.
        mock.expect_commit()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
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
        mock.expect_commit()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
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
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;

        let count = tx
            .execute_update("UPDATE Users SET Name = 'Bob' WHERE Id = 1")
            .await?;
        assert_eq!(count, 1);

        let timestamp = tx.commit().await?;
        assert_eq!(timestamp.seconds(), 1001);
        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_execute_update_explicit() {
        run_read_write_transaction_execute_update(BeginTransactionOption::ExplicitBegin).await;
    }

    #[tokio::test]
    async fn read_write_transaction_execute_update_inline() {
        run_read_write_transaction_execute_update(BeginTransactionOption::InlineBegin).await;
    }

    async fn run_read_write_transaction_execute_update(
        begin_transaction_option: BeginTransactionOption,
    ) {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
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
        }

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
            assert_eq!(req.seqno, 1);

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(selector, Selector::Begin(_)));
            }

            let mut metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType { fields: vec![] }),
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
                    row_count: Some(RowCount::RowCountExact(1)),
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
            .with_begin_transaction_option(begin_transaction_option)
            .build()
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
    async fn read_write_transaction_execute_update_invalid_stats_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_execute_update_invalid_stats(
            BeginTransactionOption::ExplicitBegin,
        )
        .await
    }

    #[tokio::test]
    async fn read_write_transaction_execute_update_invalid_stats_inline() -> anyhow::Result<()> {
        run_read_write_transaction_execute_update_invalid_stats(BeginTransactionOption::InlineBegin)
            .await
    }

    async fn run_read_write_transaction_execute_update_invalid_stats(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![1, 2, 3],
                    ..Default::default()
                }))
            });
        }

        mock.expect_execute_sql().once().returning(move |req| {
            let req = req.into_inner();
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(selector, Selector::Begin(_)));
            }

            let mut metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType { fields: vec![] }),
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
                    row_count: Some(RowCount::RowCountLowerBound(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;

        let result = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await;

        let err = result.expect_err("Expected an error for invalid row count stats");
        assert!(
            format!("{:?}", err).contains("invalid or missing row count type"),
            "Error did not contain expected message: {:?}",
            err
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_rollback_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_rollback(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio::test]
    async fn read_write_transaction_rollback_inline() -> anyhow::Result<()> {
        run_read_write_transaction_rollback(BeginTransactionOption::InlineBegin).await
    }

    async fn run_read_write_transaction_rollback(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        let transaction_id = vec![9, 9, 9];

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            let id = transaction_id.clone();
            mock.expect_begin_transaction().once().returning(move |_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: id.clone(),
                    ..Default::default()
                }))
            });
        } else {
            let id = transaction_id.clone();
            mock.expect_execute_sql().once().returning(move |req| {
                let req = req.into_inner();
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(selector, Selector::Begin(_)));

                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: id.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });
        }

        let id = transaction_id.clone();
        mock.expect_rollback().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(
                req.session,
                "projects/p/instances/i/databases/d/sessions/123"
            );
            assert_eq!(req.transaction_id, id);
            Ok(tonic::Response::new(()))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            tx.execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                .await
                .expect("Failed to execute update");
        }

        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_execute_batch_update_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_execute_batch_update(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio::test]
    async fn read_write_transaction_execute_batch_update_inline() -> anyhow::Result<()> {
        run_read_write_transaction_execute_batch_update(BeginTransactionOption::InlineBegin).await
    }

    async fn run_read_write_transaction_execute_batch_update(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![4, 5, 6],
                    ..Default::default()
                }))
            });
        }

        mock.expect_execute_batch_dml()
            .once()
            .returning(move |req| {
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

                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    assert!(matches!(selector, Selector::Begin(_)));
                }

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![4, 5, 6],
                        ..Default::default()
                    });
                }

                Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                    result_sets: vec![
                        v1::ResultSet {
                            metadata: Some(metadata),
                            stats: Some(v1::ResultSetStats {
                                row_count: Some(RowCount::RowCountExact(1)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        v1::ResultSet {
                            stats: Some(v1::ResultSetStats {
                                row_count: Some(RowCount::RowCountExact(1)),
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
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;

        let batch = BatchDml::builder()
            .add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .add_statement("UPDATE Users SET Name = 'Bob' WHERE Id = 2");

        let counts = tx.execute_batch_update(batch.build()).await?;

        assert_eq!(counts, vec![1, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_execute_batch_update_partial_failure_explicit()
    -> anyhow::Result<()> {
        run_read_write_transaction_execute_batch_update_partial_failure(
            BeginTransactionOption::ExplicitBegin,
        )
        .await
    }

    #[tokio::test]
    async fn read_write_transaction_execute_batch_update_partial_failure_inline()
    -> anyhow::Result<()> {
        run_read_write_transaction_execute_batch_update_partial_failure(
            BeginTransactionOption::InlineBegin,
        )
        .await
    }

    async fn run_read_write_transaction_execute_batch_update_partial_failure(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![7, 8, 9],
                    ..Default::default()
                }))
            });
        }

        mock.expect_execute_batch_dml()
            .once()
            .returning(move |req| {
                let req = req.into_inner();
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    assert!(matches!(selector, Selector::Begin(_)));
                }

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };
                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![7, 8, 9],
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
            .with_begin_transaction_option(begin_transaction_option)
            .build()
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
    async fn read_write_transaction_execute_multiple_updates_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_execute_multiple_updates(BeginTransactionOption::ExplicitBegin)
            .await
    }

    #[tokio::test]
    async fn read_write_transaction_execute_multiple_updates_inline() -> anyhow::Result<()> {
        run_read_write_transaction_execute_multiple_updates(BeginTransactionOption::InlineBegin)
            .await
    }

    async fn run_read_write_transaction_execute_multiple_updates(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
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
        }

        let mut seq = mockall::Sequence::new();

        // First update
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
                assert_eq!(req.seqno, 1);

                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };

                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    assert!(matches!(selector, Selector::Begin(_)));
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![4, 5, 6],
                        ..Default::default()
                    });
                } else {
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    match selector {
                        Selector::Id(id) => {
                            assert_eq!(id, vec![4, 5, 6]);
                        }
                        _ => panic!("Expected Selector::Id"),
                    }
                }

                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(metadata),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        // Second update
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
                assert_eq!(req.seqno, 2);

                let selector = req
                    .transaction
                    .expect("missing transaction selector")
                    .selector
                    .expect("missing selector");
                match selector {
                    Selector::Id(id) => {
                        assert_eq!(id, vec![4, 5, 6]);
                    }
                    _ => panic!("Expected Selector::Id"),
                }

                Ok(tonic::Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        // Third update
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
                assert_eq!(req.seqno, 3);

                let selector = req
                    .transaction
                    .expect("missing transaction selector")
                    .selector
                    .expect("missing selector");
                match selector {
                    Selector::Id(id) => {
                        assert_eq!(id, vec![4, 5, 6]);
                    }
                    _ => panic!("Expected Selector::Id"),
                }

                Ok(tonic::Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;

        for i in 1..=3 {
            let count = tx
                .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                .await
                .map_err(|e| anyhow::anyhow!("Failed to execute update {}: {:?}", i, e))?;
            assert_eq!(count, 1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_execute_query_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_execute_query(BeginTransactionOption::ExplicitBegin).await
    }

    #[tokio::test]
    async fn read_write_transaction_execute_query_inline() -> anyhow::Result<()> {
        run_read_write_transaction_execute_query(BeginTransactionOption::InlineBegin).await
    }

    async fn run_read_write_transaction_execute_query(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        use crate::client::Statement;
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
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
        }

        mock.expect_execute_streaming_sql().once().returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "SELECT 1");
            // Queries do not need to include a sequence number.
            assert_eq!(req.seqno, 0);

            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                let transaction = req.transaction.as_ref().expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(selector, Selector::Begin(_)));
            } else {
                assert_eq!(
                    req.transaction,
                    Some(v1::TransactionSelector {
                        selector: Some(Selector::Id(vec![7, 8, 9]))
                    })
                );
            }

            type StreamType = <spanner_grpc_mock::MockSpanner as v1::spanner_server::Spanner>::ExecuteStreamingSqlStream;

            let mut metadata = v1::ResultSetMetadata {
                row_type: Some(v1::StructType { fields: vec![] }),
                ..Default::default()
            };
            if begin_transaction_option == BeginTransactionOption::InlineBegin {
                metadata.transaction = Some(v1::Transaction {
                    id: vec![7, 8, 9],
                    ..Default::default()
                });
            }

            let first_response = v1::PartialResultSet {
                metadata: Some(metadata),
                ..Default::default()
            };

            let stream = tokio_stream::iter(vec![Ok(first_response)]);
            Ok(tonic::Response::new(Box::pin(stream) as StreamType))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;

        let mut rs = tx
            .execute_query(Statement::builder("SELECT 1").build())
            .await
            .expect("Failed to execute query");

        let result = rs.next().await;
        assert!(result.is_none(), "expected None, got empty stream");
        Ok(())
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
                Mode::ReadWrite(rw) => {
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
            .build()
            .await
            .expect("Failed to build transaction");
    }

    #[tokio::test]
    async fn read_write_transaction_tracks_highest_precommit_token_explicit() -> anyhow::Result<()>
    {
        run_read_write_transaction_tracks_highest_precommit_token(
            BeginTransactionOption::ExplicitBegin,
        )
        .await
    }

    #[tokio::test]
    async fn read_write_transaction_tracks_highest_precommit_token_inline() -> anyhow::Result<()> {
        run_read_write_transaction_tracks_highest_precommit_token(
            BeginTransactionOption::InlineBegin,
        )
        .await
    }

    async fn run_read_write_transaction_tracks_highest_precommit_token(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            mock.expect_begin_transaction().once().returning(|_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: vec![4, 2],
                    ..Default::default()
                }))
            });
        }

        let mut seq = mockall::Sequence::new();

        // First update
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                let mut metadata = v1::ResultSetMetadata {
                    ..Default::default()
                };

                if begin_transaction_option == BeginTransactionOption::InlineBegin {
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    assert!(matches!(selector, Selector::Begin(_)));
                    metadata.transaction = Some(v1::Transaction {
                        id: vec![4, 2],
                        ..Default::default()
                    });
                } else {
                    let selector = req
                        .transaction
                        .expect("missing transaction selector")
                        .selector
                        .expect("missing selector");
                    match selector {
                        Selector::Id(id) => {
                            assert_eq!(id, vec![4, 2]);
                        }
                        _ => panic!("Expected Selector::Id"),
                    }
                }

                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(metadata),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![2],
                        seq_num: 2,
                    }),
                    ..Default::default()
                }))
            });

        // Second update
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                let selector = req
                    .transaction
                    .expect("missing transaction selector")
                    .selector
                    .expect("missing selector");
                match selector {
                    Selector::Id(id) => {
                        assert_eq!(id, vec![4, 2]);
                    }
                    _ => panic!("Expected Selector::Id"),
                }

                Ok(tonic::Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![5],
                        seq_num: 5,
                    }),
                    ..Default::default()
                }))
            });

        // Third update
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let req = req.into_inner();
                let selector = req
                    .transaction
                    .expect("missing transaction selector")
                    .selector
                    .expect("missing selector");
                match selector {
                    Selector::Id(id) => {
                        assert_eq!(id, vec![4, 2]);
                    }
                    _ => panic!("Expected Selector::Id"),
                }

                Ok(tonic::Response::new(v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    precommit_token: Some(v1::MultiplexedSessionPrecommitToken {
                        precommit_token: vec![3],
                        seq_num: 3,
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
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;

        for _ in 0..3 {
            tx.execute_update("UPDATE Y")
                .await
                .expect("Failed to execute update");
        }
        let ts = tx.commit().await.expect("Failed to commit transaction");
        assert_eq!(ts.seconds(), 12345);
        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_commit_retry_exactly_once_explicit() -> anyhow::Result<()> {
        run_read_write_transaction_commit_retry_exactly_once(BeginTransactionOption::ExplicitBegin)
            .await
    }

    #[tokio::test]
    async fn read_write_transaction_commit_retry_exactly_once_inline() -> anyhow::Result<()> {
        run_read_write_transaction_commit_retry_exactly_once(BeginTransactionOption::InlineBegin)
            .await
    }

    async fn run_read_write_transaction_commit_retry_exactly_once(
        begin_transaction_option: BeginTransactionOption,
    ) -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        let transaction_id = vec![7, 7];

        if begin_transaction_option == BeginTransactionOption::ExplicitBegin {
            let id = transaction_id.clone();
            mock.expect_begin_transaction().once().returning(move |_| {
                Ok(tonic::Response::new(v1::Transaction {
                    id: id.clone(),
                    ..Default::default()
                }))
            });
        } else {
            let id = transaction_id.clone();
            mock.expect_execute_sql().once().returning(move |req| {
                let req = req.into_inner();
                let transaction = req
                    .transaction
                    .as_ref()
                    .expect("transaction options required for inline begin");
                let selector = transaction.selector.as_ref().expect("selector required");
                assert!(matches!(selector, Selector::Begin(_)));

                Ok(tonic::Response::new(v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: id.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });
        }

        let mut seq = mockall::Sequence::new();

        // Initial commit returns a retry token (seq 2)
        mock.expect_commit()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
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
        mock.expect_commit()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
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
            .with_begin_transaction_option(begin_transaction_option)
            .build()
            .await?;

        if begin_transaction_option == BeginTransactionOption::InlineBegin {
            tx.execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
                .await?;
        }

        let ts = tx.commit().await.expect("Failed to commit transaction");
        assert_eq!(ts.seconds(), 9999);
        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_execute_update_inline_begin() {
        let mut mock = create_session_mock();

        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
            assert_eq!(req.seqno, 1);

            let selector = req
                .transaction
                .expect("missing transaction selector")
                .selector
                .expect("missing selector");
            match selector {
                Selector::Begin(options) => {
                    assert!(options.mode.is_some());
                }
                _ => panic!("Expected Selector::Begin"),
            }

            Ok(tonic::Response::new(v1::ResultSet {
                metadata: Some(v1::ResultSetMetadata {
                    transaction: Some(v1::Transaction {
                        id: vec![7, 8, 9],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                stats: Some(v1::ResultSetStats {
                    row_count: Some(RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            match req.transaction.expect("missing transaction") {
                v1::commit_request::Transaction::TransactionId(id) => {
                    assert_eq!(id, vec![7, 8, 9]);
                }
                _ => panic!("Expected TransactionId"),
            }
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
            .build()
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
    async fn read_write_transaction_execute_batch_update_inline_begin() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        mock.expect_execute_batch_dml().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.statements.len(), 1);

            let selector = req
                .transaction
                .expect("missing transaction selector")
                .selector
                .expect("missing selector");
            match selector {
                Selector::Begin(options) => {
                    assert!(options.mode.is_some());
                }
                _ => panic!("Expected Selector::Begin"),
            }

            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
                    metadata: Some(v1::ResultSetMetadata {
                        transaction: Some(v1::Transaction {
                            id: vec![4, 5, 6],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            match req.transaction.expect("missing transaction") {
                v1::commit_request::Transaction::TransactionId(id) => {
                    assert_eq!(id, vec![4, 5, 6]);
                }
                _ => panic!("Expected TransactionId"),
            }
            Ok(tonic::Response::new(v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 123456789,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client).build().await?;

        let batch =
            BatchDml::builder().add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1");

        let counts = tx.execute_batch_update(batch.build()).await?;

        assert_eq!(counts, vec![1]);

        let ts = tx.commit().await?;
        assert_eq!(ts.seconds(), 123456789);

        Ok(())
    }

    #[tokio::test]
    async fn read_write_transaction_execute_update_fallback() {
        let mut mock = create_session_mock();

        // 1. First DML attempt fails!
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");

            let selector = req
                .transaction
                .expect("missing transaction selector")
                .selector
                .expect("missing selector");
            match selector {
                Selector::Begin(_) => {}
                _ => panic!("Expected Selector::Begin"),
            }

            Err(tonic::Status::new(tonic::Code::Internal, "internal error"))
        });

        // 2. Client falls back to explicit BeginTransaction!
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![7, 8, 9],
                ..Default::default()
            }))
        });

        // 3. Client retries DML with new ID!
        mock.expect_execute_sql().once().returning(|req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");

            let selector = req
                .transaction
                .expect("missing transaction selector")
                .selector
                .expect("missing selector");
            match selector {
                Selector::Id(id) => {
                    assert_eq!(id, vec![7, 8, 9]);
                }
                _ => panic!("Expected Selector::Id"),
            }

            Ok(tonic::Response::new(v1::ResultSet {
                stats: Some(v1::ResultSetStats {
                    row_count: Some(RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client.clone())
            .build()
            .await
            .expect("Failed to build transaction");

        let count = tx
            .execute_update("UPDATE Users SET Name = 'Alice' WHERE Id = 1")
            .await
            .expect("Failed to execute update after fallback");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn read_write_transaction_execute_batch_update_fallback() -> anyhow::Result<()> {
        let mut mock = create_session_mock();

        // 1. First Batch DML attempt fails!
        mock.expect_execute_batch_dml().once().returning(|req| {
            let req = req.into_inner();
            let selector = req
                .transaction
                .expect("missing transaction selector")
                .selector
                .expect("missing selector");
            match selector {
                Selector::Begin(_) => {}
                _ => panic!("Expected Selector::Begin"),
            }

            Err(tonic::Status::new(tonic::Code::Internal, "internal error"))
        });

        // 2. Client falls back to explicit BeginTransaction!
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(tonic::Response::new(v1::Transaction {
                id: vec![4, 5, 6],
                ..Default::default()
            }))
        });

        // 3. Client retries Batch DML with new ID!
        mock.expect_execute_batch_dml().once().returning(|req| {
            let req = req.into_inner();
            let selector = req
                .transaction
                .expect("missing transaction selector")
                .selector
                .expect("missing selector");
            match selector {
                Selector::Id(id) => {
                    assert_eq!(id, vec![4, 5, 6]);
                }
                _ => panic!("Expected Selector::Id"),
            }

            Ok(tonic::Response::new(v1::ExecuteBatchDmlResponse {
                result_sets: vec![v1::ResultSet {
                    stats: Some(v1::ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                status: Some(spanner_grpc_mock::google::rpc::Status {
                    code: 0,
                    message: "OK".into(),
                    details: vec![],
                }),
                ..Default::default()
            }))
        });

        let (db_client, _server) = setup_db_client(mock).await;

        let tx = ReadWriteTransactionBuilder::new(db_client).build().await?;

        let batch =
            BatchDml::builder().add_statement("UPDATE Users SET Name = 'Alice' WHERE Id = 1");

        let counts = tx.execute_batch_update(batch.build()).await?;

        assert_eq!(counts, vec![1]);

        Ok(())
    }
}
