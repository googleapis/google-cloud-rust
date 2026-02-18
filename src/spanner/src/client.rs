pub mod builder;
pub mod stream;

use crate::generated::gapic_dataplane::client::Spanner as GapicSpanner;
use gaxi::options::{ClientConfig, Credentials};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Spanner {
    inner: Arc<GapicSpanner>,
    grpc_client: gaxi::grpc::Client,
}

pub struct Factory;

impl crate::ClientFactory for Factory {
    type Client = Spanner;
    type Credentials = Credentials;

    async fn build(self, config: ClientConfig) -> crate::ClientBuilderResult<Self::Client> {
        let transport =
            crate::generated::gapic_dataplane::transport::Spanner::new(config.clone()).await?;
        let grpc_client = transport.inner.clone();

        let inner = if gaxi::options::tracing_enabled(&config) {
            GapicSpanner::from_stub(crate::generated::gapic_dataplane::tracing::Spanner::new(
                transport,
            ))
        } else {
            GapicSpanner::from_stub(transport)
        };
        Ok(Spanner {
            inner: Arc::new(inner),
            grpc_client,
        })
    }
}

/// A builder for the Spanner client.
pub type ClientBuilder = crate::ClientBuilder<Factory, Credentials>;

impl Spanner {
    pub fn builder() -> ClientBuilder {
        crate::new_client_builder(Factory)
    }

    pub async fn create_session(
        &self,
        request: crate::model::CreateSessionRequest,
    ) -> Result<crate::model::Session, crate::Error> {
        self.inner
            .create_session()
            .with_request(request)
            .send()
            .await
    }

    pub async fn execute_sql(
        &self,
        request: crate::model::ExecuteSqlRequest,
    ) -> Result<crate::model::ResultSet, crate::Error> {
        self.inner.execute_sql().with_request(request).send().await
    }

    pub async fn execute_batch_dml(
        &self,
        request: crate::model::ExecuteBatchDmlRequest,
    ) -> Result<crate::model::ExecuteBatchDmlResponse, crate::Error> {
        self.inner
            .execute_batch_dml()
            .with_request(request)
            .send()
            .await
    }

    pub fn batch_write(&self, request: crate::model::BatchWriteRequest) -> builder::BatchWrite {
        builder::BatchWrite::new(self.grpc_client.clone()).with_request(request)
    }

    pub async fn read(
        &self,
        request: crate::model::ReadRequest,
    ) -> Result<crate::model::ResultSet, crate::Error> {
        self.inner.read().with_request(request).send().await
    }

    pub async fn begin_transaction(
        &self,
        request: crate::model::BeginTransactionRequest,
    ) -> Result<crate::model::Transaction, crate::Error> {
        self.inner
            .begin_transaction()
            .with_request(request)
            .send()
            .await
    }

    pub async fn commit(
        &self,
        request: crate::model::CommitRequest,
    ) -> Result<crate::model::CommitResponse, crate::Error> {
        self.inner.commit().with_request(request).send().await
    }

    pub async fn rollback(
        &self,
        request: crate::model::RollbackRequest,
    ) -> Result<(), crate::Error> {
        self.inner.rollback().with_request(request).send().await
    }

    /// Executes an SQL statement, returning a stream of results.
    ///
    /// This is a custom streaming implementation over the underlying Spanner gRPC
    /// transport, since streaming responses are not yet auto-generated here.
    pub fn execute_streaming_sql(
        &self,
        request: crate::model::ExecuteSqlRequest,
    ) -> builder::ExecuteStreamingSql {
        builder::ExecuteStreamingSql::new(self.grpc_client.clone()).with_request(request)
    }

    /// Reads rows from the database, returning a stream of results.
    ///
    /// This is a custom streaming implementation over the underlying Spanner gRPC
    /// transport, since streaming responses are not yet auto-generated here.
    pub fn streaming_read(&self, request: crate::model::ReadRequest) -> builder::StreamingRead {
        builder::StreamingRead::new(self.grpc_client.clone()).with_request(request)
    }

    /// Returns a new `DatabaseClient` for interacting with a specific database.
    /// This automatically creates and manages a single multiplexed session.
    pub async fn database_client(
        &self,
        database: impl Into<String>,
    ) -> Result<crate::database_client::DatabaseClient, crate::Error> {
        let mut request = crate::model::CreateSessionRequest::new();
        request.database = database.into();

        let mut session_template = crate::model::Session::new();
        session_template.multiplexed = true;
        request.session = Some(session_template);

        let session = self.create_session(request).await?;

        Ok(crate::database_client::DatabaseClient {
            client: std::sync::Arc::new(self.clone()),
            session: std::sync::Arc::new(session),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::CreateSessionRequest;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use spanner_grpc_mock::{MockSpanner, start};

    #[tokio::test]
    async fn test_create_session() {
        // 1. Setup Mock Server
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        // 2. Start mock server
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        // 3. Configure Client to use mock endpoint
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        // 4. Call CreateSession
        let mut req = CreateSessionRequest::new();
        req.database =
            "projects/test-project/instances/test-instance/databases/test-db".to_string();

        let session = client
            .create_session(req)
            .await
            .expect("Failed to call create_session");

        // 5. Verify Response
        assert_eq!(
            session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
        );
    }

    #[tokio::test]
    async fn test_create_session_retry() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // 1. Setup Mock Server
        let mut mock = MockSpanner::new();
        let counter = Arc::new(AtomicUsize::new(0));

        mock.expect_create_session().times(2).returning(move |_| {
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(gaxi::grpc::tonic::Status::unavailable("server is unavailable"))
            } else {
                Ok(gaxi::grpc::tonic::Response::new(spanner_grpc_mock::google::spanner::v1::Session {
                    name: "projects/test-project/instances/test-instance/databases/test-db/sessions/456".to_string(),
                    ..Default::default()
                }))
            }
        });

        // 2. Start mock server
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        // 3. Configure Client to use mock endpoint
        // NOTE: Default retry policy is assigned automatically for GAPIC methods.
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        // 4. Call CreateSession with intentional retry configurations
        let mut req = CreateSessionRequest::new();
        req.database =
            "projects/test-project/instances/test-instance/databases/test-db".to_string();

        use google_cloud_gax::options::RequestOptionsBuilder;
        use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};

        let session = client
            .inner
            .create_session()
            .with_request(req)
            .with_idempotency(true)
            .with_retry_policy(Aip194Strict.with_attempt_limit(3))
            .send()
            .await
            .expect("Failed to call create_session");

        // 5. Verify Response
        assert_eq!(
            session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/456"
        );
    }

    #[tokio::test]
    async fn test_execute_sql() {
        use crate::model::ExecuteSqlRequest;

        let mut mock = MockSpanner::new();
        mock.expect_execute_sql().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::ResultSet {
                    metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                        row_type: Some(spanner_grpc_mock::google::spanner::v1::StructType {
                            fields: vec![],
                        }),
                        transaction: None,
                        undeclared_parameters: None,
                    }),
                    rows: vec![],
                    stats: None,
                    precommit_token: None,
                    cache_update: None,
                },
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let mut req = ExecuteSqlRequest::new();
        req.sql = "SELECT 1".to_string();

        let result_set = client
            .execute_sql(req)
            .await
            .expect("Failed to call execute_sql");
        assert!(result_set.metadata.is_some());
    }

    #[tokio::test]
    async fn test_execute_batch_dml() {
        use crate::model::ExecuteBatchDmlRequest;

        let mut mock = MockSpanner::new();
        mock.expect_execute_batch_dml().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::ExecuteBatchDmlResponse {
                    result_sets: vec![],
                    status: Some(spanner_grpc_mock::google::rpc::Status {
                        code: 0,
                        message: "OK".to_string(),
                        details: vec![],
                    }),
                    precommit_token: None,
                },
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let mut req = ExecuteBatchDmlRequest::new();
        req.session = "test_session".to_string();

        let response = client
            .execute_batch_dml(req)
            .await
            .expect("Failed to call execute_batch_dml");
        assert!(response.status.is_some());
    }

    #[tokio::test]
    async fn test_batch_write() {
        use crate::model::BatchWriteRequest;

        let mut mock = MockSpanner::new();
        mock.expect_batch_write().once().returning(|_| {
            let stream = tokio_stream::iter(vec![
                Ok(spanner_grpc_mock::google::spanner::v1::BatchWriteResponse {
                    indexes: vec![1, 2],
                    status: Some(spanner_grpc_mock::google::rpc::Status {
                        code: 0,
                        message: "OK".to_string(),
                        details: vec![],
                    }),
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 12345,
                        nanos: 0,
                    }),
                })
            ]);
            Ok(gaxi::grpc::tonic::Response::new(
                Box::pin(stream) as <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::BatchWriteStream
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let mut req = BatchWriteRequest::new();
        req.session = "test_session".to_string();

        let mut stream = client
            .batch_write(req)
            .send()
            .await
            .expect("Failed to call batch_write");

        let chunk1 = stream
            .next_message()
            .await
            .expect("Failed to get first stream message")
            .expect("First stream message should exist");
        assert!(chunk1.status.is_some());

        let chunk2 = stream
            .next_message()
            .await
            .expect("Stream shouldn't return error");
        assert!(chunk2.is_none(), "Stream should be exhausted");
    }

    #[tokio::test]
    async fn test_execute_streaming_sql() {
        use crate::model::ExecuteSqlRequest;

        let mut mock = MockSpanner::new();
        mock.expect_execute_streaming_sql().once().returning(|_| {
            let stream = tokio_stream::iter(vec![
                Ok(spanner_grpc_mock::google::spanner::v1::PartialResultSet {
                    metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                        row_type: Some(spanner_grpc_mock::google::spanner::v1::StructType { fields: vec![] }),
                        ..Default::default()
                    }),
                    values: vec![],
                    chunked_value: false,
                    resume_token: vec![],
                    stats: None,
                    precommit_token: None,
                    cache_update: None,
                    last: false,
                }),
                Ok(spanner_grpc_mock::google::spanner::v1::PartialResultSet {
                    metadata: None,
                    values: vec![],
                    chunked_value: false,
                    resume_token: vec![],
                    stats: None,
                    precommit_token: None,
                    cache_update: None,
                    last: true,
                })
            ]);
            Ok(gaxi::grpc::tonic::Response::new(
                Box::pin(stream) as <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::ExecuteStreamingSqlStream
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let mut req = ExecuteSqlRequest::new();
        req.sql = "SELECT 1".to_string();

        // Use the handwritten streaming API
        let mut stream = client
            .execute_streaming_sql(req)
            .send()
            .await
            .expect("Failed to call execute_streaming_sql");

        let chunk1 = stream
            .next_message()
            .await
            .expect("Failed to get first stream message")
            .expect("First stream message should exist");
        assert!(chunk1.metadata.is_some());

        let chunk2 = stream
            .next_message()
            .await
            .expect("Failed to get second stream message")
            .expect("Second stream message should exist");
        assert!(chunk2.metadata.is_none());

        let chunk3 = stream
            .next_message()
            .await
            .expect("Stream shouldn't return error");
        assert!(chunk3.is_none(), "Stream should be exhausted");
    }

    #[tokio::test]
    async fn test_read() {
        use crate::model::ReadRequest;

        let mut mock = MockSpanner::new();
        mock.expect_read().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::ResultSet {
                    metadata: None,
                    rows: vec![],
                    stats: None,
                    precommit_token: None,
                    cache_update: None,
                },
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let mut req = ReadRequest::new();
        req.table = "test_table".to_string();

        let result_set = client.read(req).await.expect("Failed to call read");
        assert!(result_set.metadata.is_none());
    }

    #[tokio::test]
    async fn test_streaming_read() {
        use crate::model::ReadRequest;

        let mut mock = MockSpanner::new();
        mock.expect_streaming_read().once().returning(|_| {
            let stream = tokio_stream::iter(vec![
                Ok(spanner_grpc_mock::google::spanner::v1::PartialResultSet {
                    metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                        row_type: Some(spanner_grpc_mock::google::spanner::v1::StructType { fields: vec![] }),
                        ..Default::default()
                    }),
                    values: vec![],
                    chunked_value: false,
                    resume_token: vec![],
                    stats: None,
                    precommit_token: None,
                    cache_update: None,
                    last: false,
                }),
                Ok(spanner_grpc_mock::google::spanner::v1::PartialResultSet {
                    metadata: None,
                    values: vec![],
                    chunked_value: false,
                    resume_token: vec![],
                    stats: None,
                    precommit_token: None,
                    cache_update: None,
                    last: true,
                })
            ]);
            Ok(gaxi::grpc::tonic::Response::new(
                Box::pin(stream) as <MockSpanner as spanner_grpc_mock::google::spanner::v1::spanner_server::Spanner>::StreamingReadStream
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let mut req = ReadRequest::new();
        req.table = "test_table".to_string();

        let mut stream = client
            .streaming_read(req)
            .send()
            .await
            .expect("Failed to call streaming_read");

        let chunk1 = stream
            .next_message()
            .await
            .expect("Failed to get first stream message")
            .expect("First stream message should exist");
        assert!(chunk1.metadata.is_some());

        let chunk2 = stream
            .next_message()
            .await
            .expect("Failed to get second stream message")
            .expect("Second stream message should exist");
        assert!(chunk2.metadata.is_none());

        let chunk3 = stream
            .next_message()
            .await
            .expect("Stream shouldn't return error");
        assert!(chunk3.is_none(), "Stream should be exhausted");
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        use crate::model::BeginTransactionRequest;

        let mut mock = MockSpanner::new();
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::Transaction {
                    id: vec![1, 2, 3],
                    read_timestamp: None,
                    precommit_token: None,
                },
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let mut req = BeginTransactionRequest::new();
        req.session = "test_session".to_string();

        let tx = client
            .begin_transaction(req)
            .await
            .expect("Failed to call begin_transaction");
        assert_eq!(tx.id, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_commit() {
        use crate::model::CommitRequest;

        let mut mock = MockSpanner::new();
        mock.expect_commit().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(
                spanner_grpc_mock::google::spanner::v1::CommitResponse {
                    commit_timestamp: Some(prost_types::Timestamp {
                        seconds: 12345,
                        nanos: 0,
                    }),
                    commit_stats: None,
                    multiplexed_session_retry: None,
                    snapshot_timestamp: None,
                },
            ))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let mut req = CommitRequest::new();
        req.session = "test_session".to_string();

        let response = client.commit(req).await.expect("Failed to call commit");
        assert!(response.commit_timestamp.is_some());
    }

    #[tokio::test]
    async fn test_rollback() {
        use crate::model::RollbackRequest;

        let mut mock = MockSpanner::new();
        mock.expect_rollback()
            .once()
            .returning(|_| Ok(gaxi::grpc::tonic::Response::new(())));

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let mut req = RollbackRequest::new();
        req.session = "test_session".to_string();

        client.rollback(req).await.expect("Failed to call rollback");
    }
}
