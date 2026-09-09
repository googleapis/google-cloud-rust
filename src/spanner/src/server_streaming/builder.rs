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

use crate::Error;
use crate::RequestBuilder;
use crate::RequestOptions;
use crate::Result;
use crate::model::BatchWriteRequest;
use crate::model::ExecuteSqlRequest;
use crate::model::FetchCacheUpdateRequest;
use crate::model::ReadRequest;
use crate::server_streaming::stream::BatchWriteStream;
use crate::server_streaming::stream::CacheUpdateStream;
use crate::server_streaming::stream::PartialResultSetStream;
use crate::server_streaming::stream::SpannerServerStream;
use crate::server_streaming::stream::StreamLifetimeGuard;
use crate::server_streaming::stream::TransactionIdCallback;
use gaxi::grpc::tonic::Extensions;
use gaxi::grpc::tonic::GrpcMethod;
use gaxi::prost::ToProto;
use prost::Message;
use std::sync::LazyLock;

/// The request builder for [SpannerImpl::execute_streaming_sql][crate::client::SpannerImpl::execute_streaming_sql] calls.
#[derive(Debug)]
pub(crate) struct ExecuteStreamingSql {
    grpc_client: gaxi::grpc::Client,
    request: ExecuteSqlRequest,
    options: RequestOptions,
    lifetime_guard: Option<StreamLifetimeGuard>,
    on_first_transaction_id: Option<TransactionIdCallback>,
}

impl ExecuteStreamingSql {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: ExecuteSqlRequest::default(),
            options: RequestOptions::default(),
            lifetime_guard: None,
            on_first_transaction_id: None,
        }
    }

    /// Attaches an opaque RAII lifetime guard that remains alive for the duration of the stream.
    #[allow(dead_code)]
    pub(crate) fn with_lifetime_guard(mut self, guard: StreamLifetimeGuard) -> Self {
        self.lifetime_guard = Some(guard);
        self
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<ExecuteSqlRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Attaches a callback to invoke when the first non-empty transaction ID arrives in the stream.
    pub(crate) fn with_transaction_id_callback<C: Into<Option<TransactionIdCallback>>>(
        mut self,
        callback: C,
    ) -> Self {
        self.on_first_transaction_id = callback.into();
        self
    }

    /// Returns a reference to the request options.
    #[allow(dead_code)]
    pub(crate) fn options(&self) -> &RequestOptions {
        &self.options
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<PartialResultSetStream> {
        let request_params = format!("session={}", self.request.session);
        let request = self.request.to_proto().map_err(Error::deser)?;
        let stream = make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "ExecuteStreamingSql",
            "/google.spanner.v1.Spanner/ExecuteStreamingSql",
            &request_params,
            self.lifetime_guard,
        )
        .await?;
        Ok(stream.with_transaction_id_callback(self.on_first_transaction_id))
    }
}

impl RequestBuilder for ExecuteStreamingSql {
    fn request_options(&mut self) -> &mut RequestOptions {
        &mut self.options
    }
}

/// The request builder for [SpannerImpl::streaming_read][crate::client::SpannerImpl::streaming_read] calls.
#[derive(Debug)]
pub(crate) struct StreamingRead {
    grpc_client: gaxi::grpc::Client,
    request: ReadRequest,
    options: RequestOptions,
    lifetime_guard: Option<StreamLifetimeGuard>,
    on_first_transaction_id: Option<TransactionIdCallback>,
}

impl StreamingRead {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: ReadRequest::default(),
            options: RequestOptions::default(),
            lifetime_guard: None,
            on_first_transaction_id: None,
        }
    }

    /// Attaches an opaque RAII lifetime guard that remains alive for the duration of the stream.
    #[allow(dead_code)]
    pub(crate) fn with_lifetime_guard(mut self, guard: StreamLifetimeGuard) -> Self {
        self.lifetime_guard = Some(guard);
        self
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<ReadRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Attaches a callback to invoke when the first non-empty transaction ID arrives in the stream.
    pub(crate) fn with_transaction_id_callback<C: Into<Option<TransactionIdCallback>>>(
        mut self,
        callback: C,
    ) -> Self {
        self.on_first_transaction_id = callback.into();
        self
    }

    /// Returns a reference to the request options.
    #[allow(dead_code)]
    pub(crate) fn options(&self) -> &RequestOptions {
        &self.options
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<PartialResultSetStream> {
        let request_params = format!("session={}", self.request.session);
        let request = self.request.to_proto().map_err(Error::deser)?;
        let stream = make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "StreamingRead",
            "/google.spanner.v1.Spanner/StreamingRead",
            &request_params,
            self.lifetime_guard,
        )
        .await?;
        Ok(stream.with_transaction_id_callback(self.on_first_transaction_id))
    }
}

impl RequestBuilder for StreamingRead {
    fn request_options(&mut self) -> &mut RequestOptions {
        &mut self.options
    }
}

/// The request builder for [SpannerImpl::batch_write][crate::client::SpannerImpl::batch_write] calls.
#[derive(Clone, Debug)]
pub(crate) struct BatchWrite {
    grpc_client: gaxi::grpc::Client,
    request: BatchWriteRequest,
    options: RequestOptions,
    lifetime_guard: Option<StreamLifetimeGuard>,
}

impl BatchWrite {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: BatchWriteRequest::default(),
            options: RequestOptions::default(),
            lifetime_guard: None,
        }
    }

    /// Attaches an opaque RAII lifetime guard that remains alive for the duration of the stream.
    #[allow(dead_code)]
    pub(crate) fn with_lifetime_guard(mut self, guard: StreamLifetimeGuard) -> Self {
        self.lifetime_guard = Some(guard);
        self
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<BatchWriteRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<BatchWriteStream> {
        let request_params = format!("session={}", self.request.session);
        let request = self.request.to_proto().map_err(Error::deser)?;
        make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "BatchWrite",
            "/google.spanner.v1.Spanner/BatchWrite",
            &request_params,
            self.lifetime_guard,
        )
        .await
    }
}

impl RequestBuilder for BatchWrite {
    fn request_options(&mut self) -> &mut RequestOptions {
        &mut self.options
    }
}

/// The request builder for `FetchCacheUpdate` calls.
#[derive(Clone, Debug)]
pub(crate) struct FetchCacheUpdate {
    grpc_client: gaxi::grpc::Client,
    request: FetchCacheUpdateRequest,
    options: RequestOptions,
    lifetime_guard: Option<StreamLifetimeGuard>,
}

impl FetchCacheUpdate {
    pub(crate) fn new(grpc_client: gaxi::grpc::Client) -> Self {
        Self {
            grpc_client,
            request: FetchCacheUpdateRequest::default(),
            options: RequestOptions::default(),
            lifetime_guard: None,
        }
    }

    /// Attaches an opaque RAII lifetime guard that remains alive for the duration of the stream.
    #[allow(dead_code)]
    pub(crate) fn with_lifetime_guard(mut self, guard: StreamLifetimeGuard) -> Self {
        self.lifetime_guard = Some(guard);
        self
    }

    /// Sets the full request, replacing any prior values.
    pub(crate) fn with_request<V: Into<FetchCacheUpdateRequest>>(mut self, v: V) -> Self {
        self.request = v.into();
        self
    }

    /// Sets all the options, replacing any prior values.
    pub(crate) fn with_options<V: Into<RequestOptions>>(mut self, v: V) -> Self {
        self.options = v.into();
        self
    }

    /// Start the server streaming request and receive the stream.
    pub(crate) async fn send(self) -> Result<CacheUpdateStream> {
        let request_params = format!("database={}", self.request.database);
        let request = self.request.to_proto().map_err(Error::deser)?;
        make_server_streaming_request(
            &self.grpc_client,
            request,
            self.options,
            "FetchCacheUpdate",
            "/google.spanner.v1.Spanner/FetchCacheUpdate",
            &request_params,
            self.lifetime_guard,
        )
        .await
    }
}

impl RequestBuilder for FetchCacheUpdate {
    fn request_options(&mut self) -> &mut RequestOptions {
        &mut self.options
    }
}

static X_GOOG_API_CLIENT_HEADER: LazyLock<String> = LazyLock::new(|| {
    let ac = gaxi::api_header::XGoogApiClient {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
        library_type: gaxi::api_header::GCCL,
    };
    ac.grpc_header_value()
});

async fn make_server_streaming_request<Req, Res>(
    grpc_client: &gaxi::grpc::Client,
    request: Req,
    options: RequestOptions,
    method_name: &'static str,
    path_str: &'static str,
    x_goog_request_params: &str,
    lifetime_guard: Option<StreamLifetimeGuard>,
) -> Result<SpannerServerStream<Res>>
where
    Req: Message + Default + Clone + 'static,
    Res: Message + Default + 'static,
{
    let options = google_cloud_gax::options::internal::set_default_idempotency(options, false);
    let extensions = {
        let mut extensions = Extensions::new();
        extensions.insert(GrpcMethod::new("google.spanner.v1.Spanner", method_name));
        extensions
    };
    let path = http::uri::PathAndQuery::from_static(path_str);

    let response = match grpc_client
        .server_streaming(
            extensions,
            path,
            request,
            options,
            &X_GOOG_API_CLIENT_HEADER,
            x_goog_request_params,
        )
        .await
    {
        Ok(response) => response,
        Err(err) => {
            if let Some(guard) = lifetime_guard
                && let Some(status) = err.status()
            {
                guard.record_error_code(status.code);
            }
            return Err(err);
        }
    };
    let (metadata, stream, _) = response.into_parts();
    let headers = metadata.into_headers();
    Ok(SpannerServerStream::new(stream, headers, lifetime_guard))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Spanner;
    use crate::server_streaming::stream::StreamGuard;
    use gaxi::grpc::tonic::Status;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_test_macros::tokio_test_no_panics;
    use std::fmt::Debug;
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::task::JoinHandle;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(ExecuteStreamingSql: Debug, Send, Sync);
        static_assertions::assert_impl_all!(StreamingRead: Debug, Send, Sync);
        static_assertions::assert_impl_all!(BatchWrite: Clone, Debug, Send, Sync);
        static_assertions::assert_impl_all!(FetchCacheUpdate: Clone, Debug, Send, Sync);
    }

    #[derive(Debug)]
    struct TestLifetimeGuard {
        recorded_code: Arc<Mutex<Option<Code>>>,
    }

    impl StreamGuard for TestLifetimeGuard {
        fn record_error_code(&self, code: Code) {
            *self.recorded_code.lock().expect("lock poisoned") = Some(code);
        }
    }

    async fn create_test_grpc_client_with_mock(
        mock: spanner_grpc_mock::MockSpanner,
    ) -> (gaxi::grpc::Client, JoinHandle<()>) {
        let (address, server) = spanner_grpc_mock::start("0.0.0.0:0", mock)
            .await
            .expect("mock server should start");
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("spanner client should build");

        let grpc_client = spanner.channels[0]
            .grpc_client
            .clone()
            .expect("grpc client should exist");
        (grpc_client, server)
    }

    async fn create_test_grpc_client() -> (gaxi::grpc::Client, JoinHandle<()>) {
        create_test_grpc_client_with_mock(spanner_grpc_mock::MockSpanner::new()).await
    }

    #[tokio_test_no_panics]
    async fn execute_streaming_sql_builder_configuration() {
        let (grpc_client, _server) = create_test_grpc_client().await;
        let guard = Arc::new(TestLifetimeGuard {
            recorded_code: Arc::new(Mutex::new(None)),
        });

        let mut options = RequestOptions::default();
        options.set_idempotency(true);

        let mut builder = ExecuteStreamingSql::new(grpc_client)
            .with_request(
                ExecuteSqlRequest::default()
                    .set_session("projects/p/instances/i/databases/d/sessions/s")
                    .set_sql("SELECT 1"),
            )
            .with_options(options)
            .with_lifetime_guard(guard);

        assert_eq!(
            builder.options().idempotent(),
            Some(true),
            "builder options should reflect configured options"
        );
        let request_options = builder.request_options();
        request_options.set_idempotency(false);
        assert_eq!(
            builder.options().idempotent(),
            Some(false),
            "builder options should reflect mutated options"
        );
        assert_eq!(
            builder.request.session, "projects/p/instances/i/databases/d/sessions/s",
            "session should match configured value"
        );
        assert_eq!(
            builder.request.sql, "SELECT 1",
            "sql should match configured value"
        );
    }

    #[tokio_test_no_panics]
    async fn streaming_read_builder_configuration() {
        let (grpc_client, _server) = create_test_grpc_client().await;
        let guard = Arc::new(TestLifetimeGuard {
            recorded_code: Arc::new(Mutex::new(None)),
        });

        let mut options = RequestOptions::default();
        options.set_idempotency(true);

        let mut builder = StreamingRead::new(grpc_client)
            .with_request(
                ReadRequest::default()
                    .set_session("projects/p/instances/i/databases/d/sessions/s")
                    .set_table("MyTable"),
            )
            .with_options(options)
            .with_lifetime_guard(guard);

        assert_eq!(
            builder.options().idempotent(),
            Some(true),
            "builder options should reflect configured options"
        );
        let request_options = builder.request_options();
        request_options.set_idempotency(false);
        assert_eq!(
            builder.options().idempotent(),
            Some(false),
            "builder options should reflect mutated options"
        );
        assert_eq!(
            builder.request.session, "projects/p/instances/i/databases/d/sessions/s",
            "session should match configured value"
        );
        assert_eq!(
            builder.request.table, "MyTable",
            "table should match configured value"
        );
    }

    #[tokio_test_no_panics]
    async fn batch_write_builder_configuration() {
        let (grpc_client, _server) = create_test_grpc_client().await;
        let guard = Arc::new(TestLifetimeGuard {
            recorded_code: Arc::new(Mutex::new(None)),
        });

        let mut builder = BatchWrite::new(grpc_client)
            .with_request(
                BatchWriteRequest::default()
                    .set_session("projects/p/instances/i/databases/d/sessions/s"),
            )
            .with_options(RequestOptions::default())
            .with_lifetime_guard(guard);

        let _ = builder.request_options();
        assert_eq!(
            builder.request.session, "projects/p/instances/i/databases/d/sessions/s",
            "session should match configured value"
        );
    }

    #[tokio_test_no_panics]
    async fn fetch_cache_update_builder_configuration() {
        let (grpc_client, _server) = create_test_grpc_client().await;
        let guard = Arc::new(TestLifetimeGuard {
            recorded_code: Arc::new(Mutex::new(None)),
        });

        let mut builder = FetchCacheUpdate::new(grpc_client)
            .with_request(
                FetchCacheUpdateRequest::default()
                    .set_database("projects/p/instances/i/databases/d"),
            )
            .with_options(RequestOptions::default())
            .with_lifetime_guard(guard);

        let _ = builder.request_options();
        assert_eq!(
            builder.request.database, "projects/p/instances/i/databases/d",
            "database should match configured value"
        );
    }

    #[tokio_test_no_panics]
    async fn make_server_streaming_request_records_error_code_on_initial_failure() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_| Err(Status::unavailable("backend unavailable")));

        let (grpc_client, _server) = create_test_grpc_client_with_mock(mock).await;

        let recorded_code = Arc::new(Mutex::new(None));
        let guard = Arc::new(TestLifetimeGuard {
            recorded_code: Arc::clone(&recorded_code),
        });

        let builder = ExecuteStreamingSql::new(grpc_client)
            .with_lifetime_guard(guard)
            .with_request(
                ExecuteSqlRequest::default()
                    .set_session("projects/p/instances/i/databases/d/sessions/s")
                    .set_sql("SELECT 1"),
            );

        let result = builder.send().await;
        assert!(result.is_err(), "Initial handshake failure must return Err");
        assert_eq!(
            *recorded_code.lock().expect("lock poisoned"),
            Some(Code::Unavailable),
            "Initial handshake failure must record Code::Unavailable on lifetime guard"
        );
    }

    #[tokio_test_no_panics]
    async fn streaming_read_records_error_code_on_initial_failure() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_streaming_read()
            .once()
            .returning(|_| Err(Status::unavailable("backend unavailable")));

        let (grpc_client, _server) = create_test_grpc_client_with_mock(mock).await;

        let recorded_code = Arc::new(Mutex::new(None));
        let guard = Arc::new(TestLifetimeGuard {
            recorded_code: Arc::clone(&recorded_code),
        });

        let builder = StreamingRead::new(grpc_client)
            .with_lifetime_guard(guard)
            .with_request(
                ReadRequest::default()
                    .set_session("projects/p/instances/i/databases/d/sessions/s")
                    .set_table("MyTable"),
            );

        let result = builder.send().await;
        assert!(result.is_err(), "Initial handshake failure must return Err");
        assert_eq!(
            *recorded_code.lock().expect("lock poisoned"),
            Some(Code::Unavailable),
            "Initial handshake failure must record Code::Unavailable on lifetime guard"
        );
    }

    #[tokio_test_no_panics]
    async fn batch_write_records_error_code_on_initial_failure() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_batch_write()
            .once()
            .returning(|_| Err(Status::unavailable("backend unavailable")));

        let (grpc_client, _server) = create_test_grpc_client_with_mock(mock).await;

        let recorded_code = Arc::new(Mutex::new(None));
        let guard = Arc::new(TestLifetimeGuard {
            recorded_code: Arc::clone(&recorded_code),
        });

        let builder = BatchWrite::new(grpc_client)
            .with_lifetime_guard(guard)
            .with_request(
                BatchWriteRequest::default()
                    .set_session("projects/p/instances/i/databases/d/sessions/s"),
            );

        let result = builder.send().await;
        assert!(result.is_err(), "Initial handshake failure must return Err");
        assert_eq!(
            *recorded_code.lock().expect("lock poisoned"),
            Some(Code::Unavailable),
            "Initial handshake failure must record Code::Unavailable on lifetime guard"
        );
    }

    #[tokio_test_no_panics]
    async fn fetch_cache_update_records_error_code_on_initial_failure() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_fetch_cache_update()
            .once()
            .returning(|_| Err(Status::unavailable("backend unavailable")));

        let (grpc_client, _server) = create_test_grpc_client_with_mock(mock).await;

        let recorded_code = Arc::new(Mutex::new(None));
        let guard = Arc::new(TestLifetimeGuard {
            recorded_code: Arc::clone(&recorded_code),
        });

        let builder = FetchCacheUpdate::new(grpc_client)
            .with_lifetime_guard(guard)
            .with_request(
                FetchCacheUpdateRequest::default()
                    .set_database("projects/p/instances/i/databases/d"),
            );

        let result = builder.send().await;
        assert!(result.is_err(), "Initial handshake failure must return Err");
        assert_eq!(
            *recorded_code.lock().expect("lock poisoned"),
            Some(Code::Unavailable),
            "Initial handshake failure must record Code::Unavailable on lifetime guard"
        );
    }

    #[tokio_test_no_panics]
    async fn make_server_streaming_request_without_guard_returns_error() {
        let mut mock = spanner_grpc_mock::MockSpanner::new();
        mock.expect_execute_streaming_sql()
            .once()
            .returning(|_| Err(Status::unavailable("backend unavailable")));

        let (grpc_client, _server) = create_test_grpc_client_with_mock(mock).await;

        let builder = ExecuteStreamingSql::new(grpc_client).with_request(
            ExecuteSqlRequest::default()
                .set_session("projects/p/instances/i/databases/d/sessions/s")
                .set_sql("SELECT 1"),
        );

        let result = builder.send().await;
        assert!(
            result.is_err(),
            "Initial handshake failure without lifetime guard must return Err"
        );
    }
}
