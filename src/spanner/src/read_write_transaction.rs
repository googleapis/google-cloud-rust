use crate::client::Spanner;
use crate::model::Session;
use std::sync::Arc;
use google_cloud_gax::error::rpc::Code;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum TransactionState {
    Initialized = 0,
    Committed = 1,
    RolledBack = 2,
}

impl From<u8> for TransactionState {
    fn from(v: u8) -> Self {
        match v {
            0 => TransactionState::Initialized,
            1 => TransactionState::Committed,
            2 => TransactionState::RolledBack,
            _ => TransactionState::Initialized, // Should not happen with correct usage
        }
    }
}

#[derive(Clone)]
pub struct ReadWriteTransactionBuilder {
    pub(crate) multi_use_transaction_builder: crate::read_context::MultiUseTransactionBuilder,
}

impl ReadWriteTransactionBuilder {
    pub(crate) async fn build_transaction(self) -> Result<ReadWriteTransaction, crate::Error> {
        let transaction = self.multi_use_transaction_builder.build().await?;
        Ok(ReadWriteTransaction {
            transaction,
            state: AtomicU8::new(TransactionState::Initialized as u8),
        })
    }

    pub(crate) fn new(client: Arc<Spanner>, session: Arc<Session>) -> Self {
        Self {
            multi_use_transaction_builder: crate::read_context::MultiUseTransactionBuilder::new(
                client,
                session,
                crate::generated::gapic_dataplane::model::TransactionOptions {
                    mode: Some(crate::generated::gapic_dataplane::model::transaction_options::Mode::ReadWrite(Box::new(
                        crate::generated::gapic_dataplane::model::transaction_options::ReadWrite::default(),
                    ))),
                    ..Default::default()
                },
            ),
        }
    }

    pub fn with_explicit_begin_transaction(mut self, explicit: bool) -> Self {
        self.multi_use_transaction_builder = self.multi_use_transaction_builder.with_explicit_begin_transaction(explicit);
        self
    }

    pub fn read_lock_mode(
        mut self,
        mode: crate::generated::gapic_dataplane::model::transaction_options::read_write::ReadLockMode,
    ) -> Self {
        let rw = match self.multi_use_transaction_builder.transaction_builder.options.mode {
            Some(crate::generated::gapic_dataplane::model::transaction_options::Mode::ReadWrite(ref mut rw)) => rw,
            _ => unreachable!("ReadWriteTransactionBuilder must be in ReadWrite mode"),
        };
        rw.read_lock_mode = mode;
        self
    }

    pub fn transaction_tag(mut self, tag: impl Into<String>) -> Self {
        self.multi_use_transaction_builder.transaction_builder.transaction_tag = Some(tag.into());
        self
    }

    pub fn isolation_level(mut self, level: crate::generated::gapic_dataplane::model::transaction_options::IsolationLevel) -> Self {
        self.multi_use_transaction_builder.transaction_builder.options.isolation_level = level;
        self
    }

    pub async fn build(self) -> Result<TransactionRunner, crate::Error> {
        Ok(TransactionRunner { builder: self })
    }
}

pub struct TransactionRunner {
    builder: ReadWriteTransactionBuilder,
}

impl TransactionRunner {
    pub async fn run<F, T>(&mut self, mut f: F) -> Result<(T, crate::model::CommitResponse), crate::Error>
    where
        F: for<'a> FnMut(&'a mut ReadWriteTransaction) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, crate::Error>> + Send + 'a>>,
    {
        loop {
            let mut tx = self.builder.clone().build_transaction().await?;
            let res = f(&mut tx).await;
            return match res {
                Ok(val) => {
                    match tx.commit().await {
                        Ok(commit_response) => Ok((val, commit_response)),
                        Err(e) => {
                            let code = e.status().map(|s| s.code);
                            if code == Some(Code::Aborted) {
                                continue;
                            }
                            Err(e)
                        }
                    }
                }
                Err(e) => {
                    let code = e.status().map(|s| s.code);
                    if code == Some(Code::Aborted) {
                        continue;
                    }
                    let _ = tx.rollback().await;
                    Err(e)
                }
            }
        }
    }
}

pub struct ReadWriteTransaction {
    pub(crate) transaction: crate::read_context::MultiUseTransaction,
    state: AtomicU8,
}

impl ReadWriteTransaction {
    pub async fn execute_query(
        &self,
        statement: impl Into<crate::statement::Statement>,
    ) -> Result<crate::result_set::ResultSet, crate::Error> {
        self.transaction.execute_query(statement).await
    }

    pub(crate) async fn commit(&self) -> Result<crate::model::CommitResponse, crate::Error> {
        self.transition_state(TransactionState::Committed)?;

        if !self.transaction.is_begun() {
            return Ok(crate::model::CommitResponse::default());
        }

        let mut request = crate::model::CommitRequest::new();
        request.session = self.transaction.context.session.name.clone();
        request = request.set_transaction_id(self.transaction.context.get_transaction_id().await?);
        request.mutations = vec![]; // TODO: Buffer mutations
        self.transaction.context.client.commit(request).await
    }

    pub(crate) async fn rollback(&self) -> Result<(), crate::Error> {
        self.transition_state(TransactionState::RolledBack)?;

        if !self.transaction.is_begun() {
            return Ok(());
        }

        let mut request = crate::model::RollbackRequest::new();
        request.session = self.transaction.context.session.name.clone();
        request = request.set_transaction_id(self.transaction.context.get_transaction_id().await?);
        self.transaction.context.client.rollback(request).await
    }

    fn transition_state(&self, target: TransactionState) -> Result<(), crate::Error> {
        let current = self.state.compare_exchange(
            TransactionState::Initialized as u8,
            target as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );

        match current {
            Ok(_) => Ok(()),
            Err(prev) => {
                let state: TransactionState = prev.into();
                let msg = match state {
                    TransactionState::Committed => "Transaction already committed",
                    TransactionState::RolledBack => "Transaction already rolled back",
                    _ => "Transaction in invalid state",
                };
                Err(crate::Error::service(
                    google_cloud_gax::error::rpc::Status::default()
                        .set_code(Code::FailedPrecondition)
                        .set_message(msg),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spanner_grpc_mock::{MockSpanner, start};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;

    fn create_mock_stream() -> <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::ExecuteStreamingSqlStream{
        let stream = tokio_stream::iter(vec![Ok(
            spanner_grpc_mock::google::spanner::v1::PartialResultSet {
                metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                    row_type: None,
                    transaction: Some(spanner_grpc_mock::google::spanner::v1::Transaction {
                        id: vec![1, 2, 3],
                        ..Default::default()
                    }),
                    undeclared_parameters: None,
                }),
                values: vec![],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: true,
            },
        )]);
        Box::pin(stream) as <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::ExecuteStreamingSqlStream
    }

    #[tokio::test]
    async fn test_read_write_transaction_builder() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_req| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let (address, _server) = start("0.0.0.0:0", mock).await.expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner.database_client("projects/test-project/instances/test-instance/databases/test-db").await.expect("Failed to create DatabaseClient");

        let builder = db_client.read_write_transaction()
            .read_lock_mode(crate::generated::gapic_dataplane::model::transaction_options::read_write::ReadLockMode::Pessimistic);

        let _runner = builder.build().await.expect("Failed to build transaction");
    }

    #[tokio::test]
    async fn test_read_write_transaction_execute_query() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let req = req.into_inner();
            assert!(req.transaction.is_some());
            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        mock.expect_commit().once().returning(|req| {
            let req = req.into_inner();
            match req.transaction {
                Some(spanner_grpc_mock::google::spanner::v1::commit_request::Transaction::TransactionId(id)) => {
                    assert_eq!(id, vec![1, 2, 3]);
                }
                _ => panic!("Expected TransactionId"),
            }
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::CommitResponse {
                ..Default::default()
            }))
        });

        let (address, _server) = start("0.0.0.0:0", mock).await.expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner.database_client("projects/test-project/instances/test-instance/databases/test-db").await.expect("Failed to create DatabaseClient");

        let mut runner = db_client.read_write_transaction()
            .build()
            .await
            .expect("Failed to build transaction");

        let res = runner.run(|tx| Box::pin(async move {
            let mut rs = tx.execute_query("SELECT 1").await?;
            let row = rs.next().await.map_err(|e| {
                crate::Error::service(
                    google_cloud_gax::error::rpc::Status::default()
                        .set_code(e.code() as i32)
                        .set_message(e.message().to_string()),
                )
            })?;
            Ok(row)
        })).await.expect("Failed to run transaction");
        
        let (row, _) = res;
        assert!(row.is_none());
    }


    #[tokio::test]
    async fn test_read_write_transaction_retry_aborted() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().times(1).returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        // First attempt: Commit fails with Aborted
        mock.expect_execute_streaming_sql().times(1).returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });
        mock.expect_commit().times(1).returning(|_| {
            Err(gaxi::grpc::tonic::Status::new(gaxi::grpc::tonic::Code::Aborted, "Transaction aborted"))
        });

        // Second attempt: Success
        mock.expect_execute_streaming_sql().times(1).returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });
        mock.expect_commit().times(1).returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::CommitResponse {
                ..Default::default()
            }))
        });

        let (address, _server) = start("0.0.0.0:0", mock).await.expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner.database_client("projects/test-project/instances/test-instance/databases/test-db").await.expect("Failed to create DatabaseClient");

        let mut runner = db_client.read_write_transaction()
            .build()
            .await
            .expect("Failed to build transaction");

        let res = runner.run(|tx| Box::pin(async move {
            let mut rs = tx.execute_query("SELECT 1").await?;
            // We must read the result set to ensure the transaction ID is extracted from metadata
            let _ = rs.next().await;
            // Determine result based on whether commit will succeed or fail is managed by mock
            // We just return Ok to trigger commit
            Ok(())
        })).await.expect("Failed to run transaction");

        assert_eq!(res.0, ());
    }

    #[tokio::test]
    async fn test_read_write_transaction_state_management() {
        let mut mock = MockSpanner::new();
        // create_session is called 3 times (once for each sub-test case)
        mock.expect_create_session().times(3).returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        // 1. Unstarted transaction commit (no RPC expected)
        let (address, _server) = start("0.0.0.0:0", mock).await.expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner.database_client("projects/test-project/instances/test-instance/databases/test-db").await.expect("Failed to create DatabaseClient");

        // Case 1: Unstarted commit
        {
            let mut runner = db_client.read_write_transaction().build().await.expect("Failed to build runner");
            // Empty transaction should commit without RPC
            let _ = runner.run(|_tx| Box::pin(async move { Ok(()) })).await.expect("Failed to run empty transaction");
        }

        // Case 2: Double commit
        {
            let tx = db_client.read_write_transaction().build_transaction().await.expect("Failed to build tx");
            // First commit (unstarted) should succeed (noop)
            tx.commit().await.expect("First commit should succeed (noop)");
            // Second commit should fail due to state check
            let err = tx.commit().await.expect_err("Second commit should fail");
            // The code is FailedPrecondition mapped from our error
            assert_eq!(err.status().map(|s| s.code), Some(Code::FailedPrecondition));
        }

        // Case 3: Double rollback
        {
            let tx = db_client.read_write_transaction().build_transaction().await.expect("Failed to build tx");
            // First rollback (unstarted) should succeed (noop)
            tx.rollback().await.expect("First rollback should succeed (noop)");
            // Second rollback should fail due to state check
            let err = tx.rollback().await.expect_err("Second rollback should fail");
            assert_eq!(err.status().map(|s| s.code), Some(Code::FailedPrecondition));
        }
    }

    #[tokio::test]
    async fn test_read_write_transaction_options() {
        let mut mock = MockSpanner::new();
        mock.expect_create_session().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        // Expect ExecuteStreamingSql with transaction_tag
        mock.expect_execute_streaming_sql().withf(|req| {
            if let Some(opts) = &req.get_ref().request_options {
                opts.transaction_tag == "test-tag"
            } else {
                false
            }
        }).returning(|_| {
             Ok(gaxi::grpc::tonic::Response::new(create_mock_stream()))
        });

        // Expect Commit
        mock.expect_commit().returning(|_| {
             Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::CommitResponse::default()))
        });

        let (address, _server) = start("0.0.0.0:0", mock).await.expect("Failed to start mock server");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let db_client = spanner.database_client("projects/test-project/instances/test-instance/databases/test-db").await.expect("Failed to create DatabaseClient");

        let mut runner = db_client.read_write_transaction()
            .transaction_tag("test-tag")
            .isolation_level(crate::generated::gapic_dataplane::model::transaction_options::IsolationLevel::Serializable)
            .build()
            .await
            .expect("Failed to build runner");

        runner.run(|tx| Box::pin(async move {
            let mut rs = tx.execute_query("SELECT 1").await?;
            let _ = rs.next().await;
            Ok(())
        })).await.expect("Failed to run transaction");
    }
}
