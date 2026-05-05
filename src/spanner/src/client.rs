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

use crate::generated::gapic_dataplane::client::Spanner as GapicSpanner;
use crate::model::{
    BeginTransactionRequest, CommitRequest, CommitResponse, CreateSessionRequest,
    ExecuteBatchDmlRequest, ExecuteBatchDmlResponse, ExecuteSqlRequest, PartitionQueryRequest,
    PartitionReadRequest, PartitionResponse, RollbackRequest, Session, Transaction,
};
use crate::server_streaming::builder;
use gaxi::options::{ClientConfig, Credentials};

pub use crate::database_client::DatabaseClient;
pub use crate::error::SpannerInternalError;
pub use crate::from_value::{ConvertError, FromValue};
pub use crate::key::{Key, KeyRange, KeySet, KeySetBuilder};
pub use crate::mutation::{Mutation, ValueBinder, WriteBuilder};
pub use crate::read::ConfiguredReadRequestBuilder;
pub use crate::read::ReadRequest;
pub use crate::read::ReadRequestBuilder;
pub use crate::read_only_transaction::MultiUseReadOnlyTransaction;
pub use crate::read_only_transaction::MultiUseReadOnlyTransactionBuilder;
pub use crate::read_only_transaction::SingleUseReadOnlyTransaction;
pub use crate::read_only_transaction::SingleUseReadOnlyTransactionBuilder;
pub use crate::read_write_transaction::ReadWriteTransaction;
pub use crate::result_set::ResultSet;
pub use crate::result_set::ResultSetError;
pub use crate::result_set_metadata::ResultSetMetadata;
pub use crate::row::Row;
pub use crate::statement::Statement;
pub use crate::timestamp_bound::TimestampBound;
pub use crate::to_value::ToValue;
pub use crate::transaction_retry_policy::BasicTransactionRetryPolicy;
pub use crate::transaction_runner::TransactionRunner;
pub use crate::transaction_runner::TransactionRunnerBuilder;
pub use crate::types::{Type, TypeCode};
pub use crate::value::{Kind, Value};
pub use wkt::{DurationError, TimestampError};

/// A client for the [Spanner] API.
///
/// Use this client to interact with the Spanner service.
///
/// [Spanner]: https://docs.cloud.google.com/spanner/docs
#[derive(Clone, Debug)]
pub struct Spanner {
    inner: GapicSpanner,
    grpc_client: Option<gaxi::grpc::Client>,
}

pub struct Factory;

impl google_cloud_gax::client_builder::internal::ClientFactory for Factory {
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
            inner,
            grpc_client: Some(grpc_client),
        })
    }
}

/// A builder for the Spanner client.
pub type ClientBuilder = google_cloud_gax::client_builder::ClientBuilder<Factory, Credentials>;

fn parse_emulator_endpoint(endpoint: &str) -> String {
    match url::Url::parse(endpoint) {
        Ok(url) if url.has_host() => endpoint.to_string(),
        _ => format!("http://{}", endpoint),
    }
}

macro_rules! define_idempotent_rpc {
    ($method:ident, $request_type:ty, $response_type:ty) => {
        pub(crate) async fn $method(
            &self,
            request: $request_type,
            options: crate::RequestOptions,
        ) -> crate::Result<$response_type> {
            self.inner
                .$method()
                .with_request(request)
                .with_options(with_default_idempotency(options))
                .send()
                .await
        }
    };
}

fn with_default_idempotency(mut options: crate::RequestOptions) -> crate::RequestOptions {
    if options.idempotent().is_none() {
        options.set_idempotency(true);
    }
    options
}

#[allow(dead_code)]
impl Spanner {
    pub fn builder() -> ClientBuilder {
        let builder = google_cloud_gax::client_builder::internal::new_builder(Factory);
        // The Spanner client should automatically use the Spanner emulator if the
        // SPANNER_EMULATOR_HOST environment variable is set.
        let Some(endpoint) = std::env::var("SPANNER_EMULATOR_HOST")
            .ok()
            .filter(|s| !s.is_empty())
        else {
            return builder;
        };

        // Determine if we need to prefix the endpoint with a scheme
        let full_endpoint = parse_emulator_endpoint(&endpoint);

        builder
            .with_endpoint(full_endpoint)
            .with_credentials(google_cloud_auth::credentials::anonymous::Builder::new().build())
    }

    /// Returns a new [DatabaseClientBuilder](crate::database_client::DatabaseClientBuilder) for
    /// interacting with a specific database.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    ///     let spanner = Spanner::builder().build().await?;
    ///     let database_client = spanner
    ///         .database_client("projects/my-project/instances/my-instance/databases/my-db")
    ///         .build()
    ///         .await?;
    ///     # Ok(())
    /// # }
    /// ```
    ///
    /// The returned `DatabaseClient` is intended to be a long-lived object and should be reused
    /// for all operations on the database.
    pub fn database_client(
        &self,
        database: impl Into<String>,
    ) -> crate::builder::DatabaseClientBuilder {
        crate::builder::DatabaseClientBuilder::new(self.clone(), database.into())
    }

    /// Creates a new client from the provided stub.
    ///
    /// The most common case for calling this function is in tests mocking the
    /// client's behavior.
    pub fn from_stub<T>(stub: T) -> Self
    where
        T: crate::generated::gapic_dataplane::stub::Spanner + 'static,
    {
        // This method is primarily for testing and doesn't fully initialize grpc_client.
        // For production use, prefer `Spanner::builder().build()`.
        Self {
            inner: GapicSpanner::from_stub(stub),
            grpc_client: None,
        }
    }

    define_idempotent_rpc!(create_session, CreateSessionRequest, Session);
    define_idempotent_rpc!(execute_sql, ExecuteSqlRequest, crate::model::ResultSet);
    define_idempotent_rpc!(
        execute_batch_dml,
        ExecuteBatchDmlRequest,
        ExecuteBatchDmlResponse
    );
    define_idempotent_rpc!(read, crate::model::ReadRequest, crate::model::ResultSet);
    define_idempotent_rpc!(begin_transaction, BeginTransactionRequest, Transaction);
    define_idempotent_rpc!(commit, CommitRequest, CommitResponse);
    define_idempotent_rpc!(rollback, RollbackRequest, ());
    define_idempotent_rpc!(partition_query, PartitionQueryRequest, PartitionResponse);
    define_idempotent_rpc!(partition_read, PartitionReadRequest, PartitionResponse);

    /// Executes an SQL statement, returning a stream of results.
    ///
    /// This is a custom streaming implementation over the underlying Spanner gRPC
    /// transport, since streaming responses are not yet auto-generated here.
    pub(crate) fn execute_streaming_sql(
        &self,
        request: crate::model::ExecuteSqlRequest,
        options: crate::RequestOptions,
    ) -> builder::ExecuteStreamingSql {
        let grpc = self
            .grpc_client
            .as_ref()
            .expect("Streaming RPCs are not supported when using a stub client");
        builder::ExecuteStreamingSql::new(grpc.clone())
            .with_request(request)
            .with_options(options)
    }

    /// Reads rows from the database, returning a stream of results.
    ///
    /// This is a custom streaming implementation over the underlying Spanner gRPC
    /// transport, since streaming responses are not yet auto-generated here.
    pub(crate) fn streaming_read(
        &self,
        request: crate::model::ReadRequest,
        options: crate::RequestOptions,
    ) -> builder::StreamingRead {
        let grpc = self
            .grpc_client
            .as_ref()
            .expect("Streaming RPCs are not supported when using a stub client");
        builder::StreamingRead::new(grpc.clone())
            .with_request(request)
            .with_options(options)
    }

    pub(crate) fn batch_write(
        &self,
        request: crate::model::BatchWriteRequest,
        options: crate::RequestOptions,
    ) -> builder::BatchWrite {
        let grpc = self
            .grpc_client
            .as_ref()
            .expect("Streaming RPCs are not supported when using a stub client");
        builder::BatchWrite::new(grpc.clone())
            .with_request(request)
            .with_options(options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::CreateSessionRequest;
    use crate::result_set::tests::adapt;
    use gaxi::grpc::tonic::{Code as GrpcCode, Response, Status};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::retry_state::RetryState;
    use google_cloud_test_macros::tokio_test_no_panics;
    use spanner_grpc_mock::google::rpc as mock_rpc;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;
    use spanner_grpc_mock::google::spanner::v1::Session;
    use spanner_grpc_mock::{MockSpanner, start};
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use std::time::Duration;

    mockall::mock! {
        #[derive(Debug)]
        BackoffPolicy {}
        impl BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, state: &RetryState) -> Duration;
        }
    }

    #[test]
    fn auto_traits() {
        assert_impl_all!(Spanner: std::fmt::Debug, Clone, Send, Sync);
        assert_not_impl_any!(Spanner: std::panic::RefUnwindSafe, std::panic::UnwindSafe);
    }

    #[tokio::test]
    async fn test_create_session() {
        // 1. Setup Mock Server
        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(mock_v1::Session {
                name:
                    "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
                        .to_string(),
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
            .create_session(req, crate::RequestOptions::default())
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
        use google_cloud_gax::options::RequestOptionsBuilder;
        use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};

        // 1. Setup Mock Server
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                Err(gaxi::grpc::tonic::Status::unavailable(
                    "server is unavailable",
                ))
            });
        mock.expect_create_session().once().in_sequence(&mut seq).returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(mock_v1::Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/456".to_string(),
                ..Default::default()
            }))
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
            Ok(gaxi::grpc::tonic::Response::new(mock_v1::ResultSet {
                metadata: Some(mock_v1::ResultSetMetadata {
                    row_type: Some(mock_v1::StructType { fields: vec![] }),
                    transaction: None,
                    undeclared_parameters: None,
                }),
                rows: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
            }))
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
            .execute_sql(req, crate::RequestOptions::default())
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
                mock_v1::ExecuteBatchDmlResponse {
                    result_sets: vec![],
                    status: Some(mock_rpc::Status {
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
            .execute_batch_dml(req, crate::RequestOptions::default())
            .await
            .expect("Failed to call execute_batch_dml");
        assert!(response.status.is_some());
    }

    #[tokio::test]
    async fn test_read() {
        use crate::model::ReadRequest;

        let mut mock = MockSpanner::new();
        mock.expect_read().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(mock_v1::ResultSet {
                metadata: None,
                rows: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
            }))
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

        let result_set = client
            .read(req, crate::RequestOptions::default())
            .await
            .expect("Failed to call read");
        assert!(result_set.metadata.is_none());
    }

    #[tokio::test]
    async fn test_begin_transaction() {
        use crate::model::BeginTransactionRequest;

        let mut mock = MockSpanner::new();
        mock.expect_begin_transaction().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(mock_v1::Transaction {
                id: vec![1, 2, 3],
                read_timestamp: None,
                precommit_token: None,
                ..Default::default()
            }))
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
            .begin_transaction(req, crate::RequestOptions::default())
            .await
            .expect("Failed to call begin_transaction");
        assert_eq!(tx.id, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_commit() {
        use crate::model::CommitRequest;

        let mut mock = MockSpanner::new();
        mock.expect_commit().once().returning(|_| {
            Ok(gaxi::grpc::tonic::Response::new(mock_v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 12345,
                    nanos: 0,
                }),
                commit_stats: None,
                multiplexed_session_retry: None,
                snapshot_timestamp: None,
                ..Default::default()
            }))
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

        let response = client
            .commit(req, crate::RequestOptions::default())
            .await
            .expect("Failed to call commit");
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

        client
            .rollback(req, crate::RequestOptions::default())
            .await
            .expect("Failed to call rollback");
    }

    #[tokio::test]
    async fn test_execute_streaming_sql() {
        use crate::model::ExecuteSqlRequest;

        let mut mock = MockSpanner::new();
        mock.expect_execute_streaming_sql().once().returning(|_| {
            let result_set = mock_v1::PartialResultSet {
                metadata: Some(mock_v1::ResultSetMetadata {
                    row_type: Some(mock_v1::StructType { fields: vec![] }),
                    transaction: None,
                    undeclared_parameters: None,
                }),
                values: vec![],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            };
            Ok(gaxi::grpc::tonic::Response::new(adapt([Ok(result_set)])))
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

        let mut stream = client
            .execute_streaming_sql(req, crate::RequestOptions::default())
            .send()
            .await
            .expect("Failed to call execute_streaming_sql");

        let result = stream.next_message().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_streaming_read() {
        use crate::model::ReadRequest;

        let mut mock = MockSpanner::new();
        mock.expect_streaming_read().once().returning(|_| {
            let result_set = mock_v1::PartialResultSet {
                metadata: Some(mock_v1::ResultSetMetadata {
                    row_type: Some(mock_v1::StructType { fields: vec![] }),
                    transaction: None,
                    undeclared_parameters: None,
                }),
                values: vec![],
                chunked_value: false,
                resume_token: vec![],
                stats: None,
                precommit_token: None,
                cache_update: None,
                last: false,
            };
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(result_set)])))
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
        req.columns = vec!["col1".to_string()];

        let mut stream = client
            .streaming_read(req, crate::RequestOptions::default())
            .send()
            .await
            .expect("Failed to call streaming_read");

        let result = stream.next_message().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_batch_write() {
        use crate::model::BatchWriteRequest;

        let mut mock = MockSpanner::new();
        mock.expect_batch_write().once().returning(|_| {
            let response = mock_v1::BatchWriteResponse {
                indexes: vec![],
                status: None,
                commit_timestamp: None,
            };
            Ok(gaxi::grpc::tonic::Response::from(adapt([Ok(response)])))
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
            .batch_write(req, crate::RequestOptions::default())
            .send()
            .await
            .expect("Failed to call batch_write");

        let result = stream.next_message().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_execute_streaming_sql_error() {
        use crate::model::ExecuteSqlRequest;

        let mut mock = MockSpanner::new();
        mock.expect_execute_streaming_sql().once().returning(|_| {
            let stream = adapt([Err(gaxi::grpc::tonic::Status::internal(
                "unexpected internal error",
            ))]);
            Ok(gaxi::grpc::tonic::Response::from(stream))
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

        let mut stream = client
            .execute_streaming_sql(req, crate::RequestOptions::default())
            .send()
            .await
            .expect("Failed to call execute_streaming_sql");

        let result = stream.next_message().await;
        assert!(result.is_some());
        let err = result.unwrap().expect_err("expected error");
        assert_eq!(
            err.status().unwrap().code,
            google_cloud_gax::error::rpc::Code::Internal
        );
    }

    #[tokio::test]
    async fn default_retry_respected() -> anyhow::Result<()> {
        use crate::model::CreateSessionRequest;

        // 1. Setup Mock Server
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::unavailable("server is unavailable")));
        mock.expect_create_session().once().in_sequence(&mut seq).returning(|_| {
            Ok(Response::new(Session {
                name: "projects/test-project/instances/test-instance/databases/test-db/sessions/456".to_string(),
                ..Default::default()
            }))
        });

        // 2. Start mock server
        let (address, _server) = start("0.0.0.0:0", mock).await?;

        // 3. Configure Client
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        // 4. Call CreateSession using the hand-written wrapper
        let mut req = CreateSessionRequest::new();
        req.database =
            "projects/test-project/instances/test-instance/databases/test-db".to_string();

        let session = client
            .create_session(req, crate::RequestOptions::default())
            .await
            .expect("Failed to call create_session");

        // 5. Verify Response
        assert_eq!(
            session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/456"
        );

        Ok(())
    }

    #[tokio::test]
    async fn override_idempotency_to_false() -> anyhow::Result<()> {
        use crate::model::CreateSessionRequest;

        // 1. Setup Mock Server to fail with UNAVAILABLE
        let mut mock = MockSpanner::new();
        mock.expect_create_session()
            .once()
            .returning(|_| Err(Status::unavailable("server is unavailable")));

        // 2. Start mock server
        let (address, _server) = start("0.0.0.0:0", mock).await?;

        // 3. Configure Client
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        // 4. Call CreateSession with explicit idempotency = false
        let mut req = CreateSessionRequest::new();
        req.database =
            "projects/test-project/instances/test-instance/databases/test-db".to_string();

        let mut options = crate::RequestOptions::default();
        options.set_idempotency(false);

        let result = client.create_session(req, options).await;

        // 5. Verify that it failed and did not retry
        assert!(result.is_err(), "Expected error, got {:?}", result);
        let err = result.unwrap_err();
        assert_eq!(err.status().map(|s| s.code), Some(Code::Unavailable));

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn timeout_respected() -> anyhow::Result<()> {
        use crate::batch_dml::BatchDml;
        use std::time::Duration;

        // 1. Setup Mock Server
        let mut mock = MockSpanner::new();

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_begin_transaction().returning(|_| {
            Ok(Response::new(mock_v1::Transaction {
                id: vec![42],
                ..Default::default()
            }))
        });

        mock.expect_execute_streaming_sql().once().returning(|req| {
            let metadata = req.metadata();
            let timeout = metadata.get("grpc-timeout");
            assert!(
                timeout.is_some(),
                "grpc-timeout header should be present for query"
            );

            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(Response::new(rx))
        });

        mock.expect_streaming_read().once().returning(|req| {
            let metadata = req.metadata();
            let timeout = metadata.get("grpc-timeout");
            assert!(
                timeout.is_some(),
                "grpc-timeout header should be present for read"
            );

            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Ok(Response::new(rx))
        });

        mock.expect_execute_sql().once().returning(|req| {
            let metadata = req.metadata();
            let timeout = metadata.get("grpc-timeout");
            assert!(
                timeout.is_some(),
                "grpc-timeout header should be present for single DML"
            );

            Ok(Response::new(mock_v1::ResultSet {
                stats: Some(mock_v1::ResultSetStats {
                    row_count: Some(mock_v1::result_set_stats::RowCount::RowCountExact(1)),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });

        mock.expect_execute_batch_dml().once().returning(|req| {
            let metadata = req.metadata();
            let timeout = metadata.get("grpc-timeout");
            assert!(
                timeout.is_some(),
                "grpc-timeout header should be present for batch dml"
            );

            Ok(Response::new(mock_v1::ExecuteBatchDmlResponse {
                result_sets: vec![mock_v1::ResultSet {
                    stats: Some(mock_v1::ResultSetStats {
                        row_count: Some(mock_v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }))
        });

        mock.expect_commit().returning(|_| {
            Ok(Response::new(mock_v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1234,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        // 2. Start mock server
        let (address, _server) = start("0.0.0.0:0", mock).await?;

        // 3. Configure Client
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db = client
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await?;
        let runner = db.read_write_transaction().build().await?;

        // 4. Run transaction
        runner
            .run(async |tx| {
                // Query
                let stmt = Statement::builder("SELECT 1")
                    .with_attempt_timeout(Duration::from_secs(10))
                    .build();
                let _ = tx.execute_query(stmt).await?;

                // Read
                let req = ReadRequest::builder("Table", vec!["Col"])
                    .with_keys(crate::key::KeySet::all())
                    .with_attempt_timeout(Duration::from_secs(5))
                    .build();
                let _ = tx.execute_read(req).await?;

                // Single DML
                let dml = Statement::builder("UPDATE t SET c = 1")
                    .with_attempt_timeout(Duration::from_secs(7))
                    .build();
                let _ = tx.execute_update(dml).await?;

                // Batch DML
                let batch = BatchDml::builder()
                    .add_statement("UPDATE t SET c = 2")
                    .with_attempt_timeout(Duration::from_secs(8))
                    .build();
                let _ = tx.execute_batch_update(batch).await?;

                Ok(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn retry_policy_respected() -> anyhow::Result<()> {
        use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};

        // Extend the default retry policy to also retry on ResourceExhausted.
        let retry_policy = Aip194Strict.continue_on_too_many_requests();

        // 1. Setup Mock Server
        let mut mock = MockSpanner::new();

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_begin_transaction().returning(|_| {
            Ok(Response::new(mock_v1::Transaction {
                id: vec![42],
                ..Default::default()
            }))
        });

        // Mock ExecuteSql to first return RESOURCE_EXHAUSTED and then succeed.
        let mut seq = mockall::Sequence::new();

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Err(Status::new(GrpcCode::ResourceExhausted, "quota exceeded")));

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(Response::new(mock_v1::ResultSet {
                    stats: Some(mock_v1::ResultSetStats {
                        row_count: Some(mock_v1::result_set_stats::RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
            });

        mock.expect_commit().returning(|_| {
            Ok(Response::new(mock_v1::CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 1234,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        // 2. Start mock server
        let (address, _server) = start("0.0.0.0:0", mock).await?;

        // 3. Configure Client
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let db = client
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await?;
        let runner = db.read_write_transaction().build().await?;

        // 4. Call execute_update with custom retry and backoff
        let mut mock_backoff = MockBackoffPolicy::new();
        mock_backoff
            .expect_on_failure()
            .once()
            .returning(|_| Duration::from_nanos(1));

        let stmt = Statement::builder("UPDATE t SET c = 1")
            .with_retry_policy(retry_policy)
            .with_backoff_policy(mock_backoff)
            .build();

        let result = runner
            .run(async |tx| {
                let count = tx.execute_update(stmt.clone()).await?;
                Ok(count)
            })
            .await?;

        // 5. Verify success after retry
        assert_eq!(result, 1);

        Ok(())
    }

    #[test]
    fn test_parse_emulator_endpoint() {
        assert_eq!(
            super::parse_emulator_endpoint("localhost:9010"),
            "http://localhost:9010"
        );
        assert_eq!(
            super::parse_emulator_endpoint("spanner-emulator:9010"),
            "http://spanner-emulator:9010"
        );
        assert_eq!(
            super::parse_emulator_endpoint("http://localhost:9010"),
            "http://localhost:9010"
        );
        assert_eq!(
            super::parse_emulator_endpoint("https://localhost:9010"),
            "https://localhost:9010"
        );
        assert_eq!(
            super::parse_emulator_endpoint("grpc://localhost:9010"),
            "grpc://localhost:9010"
        );
        assert_eq!(
            super::parse_emulator_endpoint("http_localhost:9010"),
            "http://http_localhost:9010"
        );
    }
}
