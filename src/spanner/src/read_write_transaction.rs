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
use crate::model::BeginTransactionRequest;
use crate::model::CommitRequest;
use crate::model::ExecuteSqlRequest;
use crate::model::RollbackRequest;
use crate::model::TransactionOptions;
use crate::model::TransactionSelector;
use crate::model::result_set_stats::RowCount;
use crate::model::transaction_options::IsolationLevel;
use crate::model::transaction_options::Mode;
use crate::model::transaction_options::ReadWrite;
use crate::model::transaction_options::read_write::ReadLockMode;
use crate::model::transaction_selector::Selector;
use crate::read_only_transaction::ReadContext;
use crate::result_set::ResultSet;
use crate::statement::Statement;

/// A builder for [ReadWriteTransaction].
pub(crate) struct ReadWriteTransactionBuilder {
    client: DatabaseClient,
    options: TransactionOptions,
}

impl ReadWriteTransactionBuilder {
    pub(crate) fn new(client: DatabaseClient) -> Self {
        Self {
            client,
            options: TransactionOptions::default().set_read_write(ReadWrite::default()),
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

    pub(crate) async fn begin_transaction(&self) -> crate::Result<ReadWriteTransaction> {
        let request = BeginTransactionRequest::default()
            .set_session(self.client.session.name.clone())
            .set_options(self.options.clone());

        // TODO(#4972): make request options configurable
        let response = self
            .client
            .spanner
            .begin_transaction(request, crate::RequestOptions::default())
            .await?;

        let transaction_selector = TransactionSelector::default().set_id(response.id);
        Ok(ReadWriteTransaction {
            context: ReadContext::new(self.client.clone(), transaction_selector),
            seqno: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(1)),
        })
    }
}

/// A read-write transaction.
#[derive(Clone, Debug)]
pub struct ReadWriteTransaction {
    pub(crate) context: ReadContext,
    seqno: std::sync::Arc<std::sync::atomic::AtomicI64>,
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
        let seqno = self.seqno.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let statement = statement.into();
        let request = ExecuteSqlRequest::default()
            .set_session(self.context.client.session.name.clone())
            .set_transaction(self.context.transaction_selector.clone())
            .set_seqno(seqno)
            .set_or_clear_params(statement.get_params())
            .set_param_types(statement.get_param_types())
            .set_sql(statement.sql);

        let response = self
            .context
            .client
            .spanner
            .execute_sql(request, crate::RequestOptions::default())
            .await?;

        let stats = response
            .stats
            .ok_or_else(|| crate::error::internal_error("No stats returned"))?;
        match stats.row_count {
            Some(RowCount::RowCountExact(c)) => Ok(c),
            _ => Err(crate::error::internal_error(
                "ExecuteSql returned an invalid or missing row count type for a read/write transaction",
            )),
        }
    }

    fn transaction_id(&self) -> crate::Result<Vec<u8>> {
        match &self.context.transaction_selector.selector {
            Some(Selector::Id(id)) => Ok(id.to_vec()),
            _ => Err(crate::error::internal_error("Transaction ID is missing")),
        }
    }

    /// Commits the transaction.
    pub(crate) async fn commit(self) -> crate::Result<wkt::Timestamp> {
        let transaction_id = self.transaction_id()?;
        let request = CommitRequest::default()
            .set_session(self.context.client.session.name.clone())
            .set_transaction_id(transaction_id);

        let response = self
            .context
            .client
            .spanner
            .commit(request, crate::RequestOptions::default())
            .await?;

        let timestamp = response
            .commit_timestamp
            .ok_or_else(|| crate::error::internal_error("No commit timestamp returned"))?;
        Ok(timestamp)
    }

    /// Rolls back the transaction.
    pub(crate) async fn rollback(self) -> crate::Result<()> {
        let transaction_id = self.transaction_id()?;

        let request = RollbackRequest::default()
            .set_session(self.context.client.session.name.clone())
            .set_transaction_id(transaction_id);

        self.context
            .client
            .spanner
            .rollback(request, crate::RequestOptions::default())
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_only_transaction::tests::{create_session_mock, setup_db_client};
    use gaxi::grpc::tonic;
    use spanner_grpc_mock::google::spanner::v1;

    #[test]
    fn auto_traits() {
        static_assertions::assert_impl_all!(ReadWriteTransactionBuilder: Send, Sync);
        static_assertions::assert_impl_all!(ReadWriteTransaction: Send, Sync, std::fmt::Debug);
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

        let counter = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(1));
        mock.expect_execute_sql().times(3).returning(move |req| {
            let req = req.into_inner();
            assert_eq!(req.sql, "UPDATE Users SET Name = 'Alice' WHERE Id = 1");
            let c = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

            type StreamType = <spanner_grpc_mock::MockSpanner as v1::spanner_server::Spanner>::ExecuteStreamingSqlStream;
            let stream: tokio_stream::Empty<Result<v1::PartialResultSet, tonic::Status>> = tokio_stream::empty();
            Ok(tonic::Response::new(Box::pin(stream) as StreamType))
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
}
