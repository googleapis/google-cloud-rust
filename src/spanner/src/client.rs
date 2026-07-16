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
use google_cloud_auth::credentials::anonymous;
use google_cloud_gax::backoff_policy::BackoffPolicyArg;
use google_cloud_gax::client_builder::ClientBuilder as GaxClientBuilder;
use google_cloud_gax::options::{
    RequestOptions as GaxRequestOptions, internal::RequestOptionsExt as _,
};
use google_cloud_gax::retry_policy::RetryPolicyArg;
use google_cloud_gax::retry_throttler::RetryThrottlerArg;
use google_cloud_spanner_admin_database_v1::builder::database_admin::ClientBuilder as DatabaseAdminBuilder;
use google_cloud_spanner_admin_instance_v1::builder::instance_admin::ClientBuilder as InstanceAdminBuilder;
use http::{
    HeaderMap,
    header::{HeaderName, HeaderValue},
};
use std::sync::{
    LazyLock,
    atomic::{AtomicUsize, Ordering},
};

pub use crate::database_client::DatabaseClient;
pub use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;
pub use google_cloud_spanner_admin_instance_v1::client::InstanceAdmin;

/// A client for the [Spanner] API.
///
/// Use this client to interact with the Spanner service.
///
/// [Spanner]: https://docs.cloud.google.com/spanner/docs
#[derive(Clone, Debug)]
pub struct Spanner {
    pub(crate) channels: Vec<Channel>,
    pub(crate) counter: std::sync::Arc<AtomicUsize>,
    pub(crate) config: ClientConfig,
    pub(crate) is_emulator: bool,
}

/// A builder for the Spanner client.
///
/// Obtain one with [`Spanner::builder`]. The builder is pre-configured with
/// standard defaults; the setters below override individual behaviors.
#[derive(Clone, Debug)]
pub struct ClientBuilder {
    config: ClientConfig,
    num_channels: Option<usize>,
}

impl ClientBuilder {
    fn new() -> Self {
        Self {
            config: ClientConfig::default(),
            num_channels: None,
        }
    }

    /// Creates the `Spanner` client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let spanner = Spanner::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub async fn build(self) -> crate::ClientBuilderResult<Spanner> {
        let ClientBuilder {
            mut config,
            num_channels,
        } = self;

        let mut is_emulator = false;
        if let Some(endpoint) = std::env::var("SPANNER_EMULATOR_HOST")
            .ok()
            .filter(|s| !s.is_empty())
        {
            is_emulator = true;
            if config.endpoint.is_none() {
                config.endpoint = Some(parse_emulator_endpoint(&endpoint));
            }
            if config.cred.is_none() {
                config.cred = Some(anonymous::Builder::new().build());
            }
        }

        let num_channels = num_channels
            .or_else(|| {
                std::env::var("SPANNER_NUM_CHANNELS")
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
            })
            .unwrap_or(4);

        let mut channels = Vec::with_capacity(num_channels);
        for _ in 0..num_channels {
            channels.push(Channel::create(&config).await?);
        }

        Ok(Spanner {
            channels,
            counter: std::sync::Arc::new(AtomicUsize::new(0)),
            config,
            is_emulator,
        })
    }

    /// Sets the size of the gRPC channel pool.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let spanner = Spanner::builder().with_num_channels(8).build().await?;
    /// # Ok(()) }
    /// ```
    ///
    /// The Spanner client demultiplexes requests over a pool of gRPC channels.
    /// A larger pool can improve throughput for highly concurrent workloads at
    /// the cost of more open connections.
    ///
    /// When unset, the pool size is read from the `SPANNER_NUM_CHANNELS`
    /// environment variable, and defaults to `4` if that is unset or invalid.
    /// An explicit value set here takes precedence over the environment
    /// variable.
    pub fn with_num_channels(mut self, v: usize) -> Self {
        self.num_channels = Some(v);
        self
    }

    /// Sets the endpoint.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let spanner = Spanner::builder()
    ///     .with_endpoint("https://private.googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_endpoint<V: Into<String>>(mut self, v: V) -> Self {
        self.config.endpoint = Some(v.into());
        self
    }

    /// Enables tracing.
    ///
    /// The client libraries can be dynamically instrumented with the Tokio
    /// [tracing] framework. Setting this flag enables this instrumentation.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let spanner = Spanner::builder()
    ///     .with_tracing()
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [tracing]: https://docs.rs/tracing/latest/tracing/
    pub fn with_tracing(mut self) -> Self {
        self.config.tracing = true;
        self
    }

    /// Configures the authentication credentials.
    ///
    /// Most Google Cloud services require authentication, though some services
    /// allow for anonymous access, and some services provide emulators where
    /// no authentication is required. More information about valid credentials
    /// types can be found in the [google-cloud-auth] crate documentation.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_auth::credentials::mds;
    /// let spanner = Spanner::builder()
    ///     .with_credentials(
    ///         mds::Builder::default()
    ///             .with_scopes(["https://www.googleapis.com/auth/cloud-platform.read-only"])
    ///             .build()?)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [google-cloud-auth]: https://docs.rs/google-cloud-auth
    pub fn with_credentials<T: Into<Credentials>>(mut self, v: T) -> Self {
        self.config.cred = Some(v.into());
        self
    }

    /// Configures the universe domain.
    ///
    /// Most applications do not need to set this field, it is only required
    /// when connecting to services in a non-default [universe domain]. When
    /// unset the client uses the default universe domain
    /// (`googleapis.com`).
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let spanner = Spanner::builder()
    ///     .with_universe_domain("my-universe.example.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [universe domain]: https://cloud.google.com/iam/docs/federated-identity-supported-services
    pub fn with_universe_domain<V: Into<String>>(mut self, v: V) -> Self {
        self.config.universe_domain = Some(v.into());
        self
    }

    /// Configures the retry policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// retry policy controls what errors are considered retryable, sets limits
    /// on the number of attempts or the time trying to make attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::retry_policy::RetryPolicyExt;
    /// use google_cloud_spanner::retry_policy::SpannerRetryPolicy;
    /// let spanner = Spanner::builder()
    ///     .with_retry_policy(SpannerRetryPolicy::new().with_attempt_limit(3))
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_policy<V: Into<RetryPolicyArg>>(mut self, v: V) -> Self {
        self.config.retry_policy = Some(v.into().into());
        self
    }

    /// Configures the retry backoff policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// backoff policy controls how long to wait in between retry attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// let spanner = Spanner::builder()
    ///     .with_backoff_policy(ExponentialBackoff::default())
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_backoff_policy<V: Into<BackoffPolicyArg>>(mut self, v: V) -> Self {
        self.config.backoff_policy = Some(v.into().into());
        self
    }

    /// Configures the per-attempt timeout used as the client default.
    ///
    /// When using a retry policy, this timeout applies to each individual
    /// attempt rather than the overall operation. It is used as the default for
    /// requests that do not override the timeout in their own request options.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use std::time::Duration;
    /// let spanner = Spanner::builder()
    ///     .with_attempt_timeout(Duration::from_secs(5))
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_attempt_timeout<V: Into<std::time::Duration>>(mut self, v: V) -> Self {
        self.config.attempt_timeout = Some(v.into());
        self
    }

    /// Configures the retry throttler.
    ///
    /// Advanced applications may want to configure a retry throttler to
    /// [Address Cascading Failures] and when [Handling Overload] conditions.
    /// The client libraries throttle their retry loop, using a policy to
    /// control the throttling algorithm. Use this method to fine tune or
    /// customize the default retry throttler.
    ///
    /// [Handling Overload]: https://sre.google/sre-book/handling-overload/
    /// [Address Cascading Failures]: https://sre.google/sre-book/addressing-cascading-failures/
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_gax::retry_throttler::AdaptiveThrottler;
    /// let spanner = Spanner::builder()
    ///     .with_retry_throttler(AdaptiveThrottler::default())
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_throttler<V: Into<RetryThrottlerArg>>(mut self, v: V) -> Self {
        self.config.retry_throttler = v.into().into();
        self
    }
}

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
            channel_hint: usize,
        ) -> crate::Result<$response_type> {
            self.get_channel(channel_hint)
                .inner
                .$method()
                .with_request(request)
                .with_options(apply_request_defaults(options))
                .send()
                .await
        }
    };
}

fn apply_request_defaults(mut options: crate::RequestOptions) -> crate::RequestOptions {
    if options.idempotent().is_none() {
        options.set_idempotency(true);
    }
    if options.retry_policy().is_none() {
        options.set_retry_policy(crate::retry_policy::SpannerRetryPolicy::new());
    }
    options
}

pub(crate) static LAR_HEADER_MAP: LazyLock<HeaderMap> = LazyLock::new(|| {
    let mut map = HeaderMap::new();
    map.insert(
        HeaderName::from_static("x-goog-spanner-route-to-leader"),
        HeaderValue::from_static("true"),
    );
    map
});

pub(crate) fn amend_request_options_for_lar(
    leader_aware_routing_enabled: bool,
    mut options: GaxRequestOptions,
) -> GaxRequestOptions {
    if leader_aware_routing_enabled {
        let mut headers = options
            .get_extension::<HeaderMap>()
            .cloned()
            .unwrap_or_default();
        headers.extend((*LAR_HEADER_MAP).clone());
        options = options.insert_extension(headers);
    }
    options
}

fn map_emulator_admin_endpoint(endpoint: &str, is_emulator: bool) -> String {
    let mut ep = endpoint.trim_end_matches('/').to_string();
    if is_emulator && ep.ends_with(":9010") {
        ep = ep.replace(":9010", ":9020");
    }
    ep
}

impl Spanner {
    /// Returns a builder for the `Spanner` client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let spanner = Spanner::builder().build().await?;
    ///
    /// let db_client = spanner
    ///     .database_client("projects/my-project/instances/my-instance/databases/my-db")
    ///     .build()
    ///     .await?;
    ///
    /// let tx = db_client.single_use().build();
    /// let mut rs = tx.execute_query("SELECT 1").await?;
    ///
    /// while let Some(row) = rs.next().await {
    ///     let row = row?;
    ///     let val: i64 = row.get(0);
    ///     assert_eq!(val, 1);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The returned builder is pre-configured with standard defaults. It automatically
    /// detects and connects to the Spanner emulator if the `SPANNER_EMULATOR_HOST`
    /// environment variable is set.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Returns a builder for the [DatabaseAdmin] client.
    ///
    /// This builder is automatically pre-configured with the same endpoints, credentials,
    /// and routing configurations as this `Spanner` instance.
    /// If configured to use the Emulator (via `SPANNER_EMULATOR_HOST`), it maps the gRPC endpoint port
    /// (`9010`) to the REST admin port (`9020`).
    pub fn database_admin_builder(&self) -> DatabaseAdminBuilder {
        self.configure_admin_builder(DatabaseAdmin::builder())
    }

    /// Returns a builder for the [InstanceAdmin] client.
    ///
    /// This builder is automatically pre-configured with the same endpoints, credentials,
    /// and routing configurations as this `Spanner` instance.
    /// If configured to use the Emulator (via `SPANNER_EMULATOR_HOST`), it maps the gRPC endpoint port
    /// (`9010`) to the REST admin port (`9020`).
    pub fn instance_admin_builder(&self) -> InstanceAdminBuilder {
        self.configure_admin_builder(InstanceAdmin::builder())
    }

    fn configure_admin_builder<F, C>(
        &self,
        mut builder: GaxClientBuilder<F, C>,
    ) -> GaxClientBuilder<F, C>
    where
        C: Clone + From<Credentials>,
    {
        if let Some(ref endpoint) = self.config.endpoint {
            let ep = map_emulator_admin_endpoint(endpoint, self.is_emulator);
            builder = builder.with_endpoint(ep);
        }
        if let Some(ref cred) = self.config.cred {
            builder = builder.with_credentials(cred.clone());
        }
        if let Some(ref ud) = self.config.universe_domain {
            builder = builder.with_universe_domain(ud.clone());
        }
        builder
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
            channels: vec![Channel {
                inner: GapicSpanner::from_stub(stub),
                grpc_client: None,
            }],
            counter: std::sync::Arc::new(AtomicUsize::new(0)),
            config: ClientConfig::default(),
            is_emulator: false,
        }
    }

    pub(crate) fn is_emulator(&self) -> bool {
        self.is_emulator
    }

    pub(crate) fn get_channel(&self, hint: usize) -> &Channel {
        let idx = hint % self.channels.len();
        &self.channels[idx]
    }

    pub(crate) fn next_channel_hint(&self) -> usize {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    define_idempotent_rpc!(create_session, CreateSessionRequest, Session);
    define_idempotent_rpc!(execute_sql, ExecuteSqlRequest, crate::model::ResultSet);
    define_idempotent_rpc!(
        execute_batch_dml,
        ExecuteBatchDmlRequest,
        ExecuteBatchDmlResponse
    );
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
        channel_hint: usize,
    ) -> builder::ExecuteStreamingSql {
        let channel = self.get_channel(channel_hint);
        let grpc = channel
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
        channel_hint: usize,
    ) -> builder::StreamingRead {
        let channel = self.get_channel(channel_hint);
        let grpc = channel
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
        channel_hint: usize,
    ) -> builder::BatchWrite {
        let channel = self.get_channel(channel_hint);
        let grpc = channel
            .grpc_client
            .as_ref()
            .expect("Streaming RPCs are not supported when using a stub client");
        builder::BatchWrite::new(grpc.clone())
            .with_request(request)
            .with_options(options)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Channel {
    pub(crate) inner: GapicSpanner,
    pub(crate) grpc_client: Option<gaxi::grpc::Client>,
}

impl Channel {
    pub(crate) async fn create(config: &ClientConfig) -> crate::ClientBuilderResult<Self> {
        let transport =
            crate::generated::gapic_dataplane::transport::Spanner::new(config.clone()).await?;
        let grpc_client = transport.inner.clone();

        let inner = if gaxi::options::tracing_enabled(config) {
            GapicSpanner::from_stub(crate::generated::gapic_dataplane::tracing::Spanner::new(
                transport,
            ))
        } else {
            GapicSpanner::from_stub(transport)
        };
        Ok(Self {
            inner,
            grpc_client: Some(grpc_client),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::CreateSessionRequest;
    use crate::read::ReadRequest;
    use crate::result_set::tests::adapt;
    use crate::statement::Statement;
    use gaxi::grpc::tonic::MetadataMap;
    use gaxi::grpc::tonic::{Code as GrpcCode, Response, Status};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::backoff_policy::BackoffPolicy;
    use google_cloud_gax::error::rpc::Code;
    use google_cloud_gax::retry_state::RetryState;
    use google_cloud_test_macros::tokio_test_no_panics;
    use spanner_grpc_mock::google::rpc as mock_rpc;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;
    use spanner_grpc_mock::google::spanner::v1::CommitResponse;
    use spanner_grpc_mock::google::spanner::v1::ResultSet;
    use spanner_grpc_mock::google::spanner::v1::ResultSetStats;
    use spanner_grpc_mock::google::spanner::v1::Session;
    use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;
    use spanner_grpc_mock::{MockSpanner, start};
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
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

    #[tokio_test_no_panics]
    async fn channel_pool_default_size() {
        let mock = MockSpanner::new();
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        assert_eq!(client.channels.len(), 4);
    }

    #[tokio_test_no_panics]
    async fn channel_pool_builder_override() {
        let mock = MockSpanner::new();
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .with_num_channels(2)
            .build()
            .await
            .expect("Failed to build client");

        assert_eq!(client.channels.len(), 2);
    }

    #[test]
    fn test_map_emulator_admin_endpoint() {
        // 1. Test normal endpoint without emulator (should remain unchanged)
        assert_eq!(
            map_emulator_admin_endpoint("https://spanner.googleapis.com", false),
            "https://spanner.googleapis.com"
        );

        // 2. Test emulator endpoint mapping (9010 -> 9020)
        assert_eq!(
            map_emulator_admin_endpoint("http://localhost:9010", true),
            "http://localhost:9020"
        );

        // 3. Test emulator endpoint with trailing slash (should be trimmed and mapped)
        assert_eq!(
            map_emulator_admin_endpoint("http://127.0.0.1:9010/", true),
            "http://127.0.0.1:9020"
        );

        // 4. Test emulator endpoint without is_emulator active (should remain unchanged)
        assert_eq!(
            map_emulator_admin_endpoint("http://localhost:9010", false),
            "http://localhost:9010"
        );
    }

    #[tokio_test_no_panics]
    async fn channel_selection() {
        let mock = MockSpanner::new();
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await
            .expect("Failed to build client");

        let hint0 = client.next_channel_hint();
        let hint1 = client.next_channel_hint();
        let hint2 = client.next_channel_hint();
        let hint3 = client.next_channel_hint();
        let hint4 = client.next_channel_hint();

        assert_eq!(hint0 % 4, 0);
        assert_eq!(hint1 % 4, 1);
        assert_eq!(hint2 % 4, 2);
        assert_eq!(hint3 % 4, 3);
        assert_eq!(hint4 % 4, 0);
    }

    #[tokio_test_no_panics]
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
            .create_session(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .await
            .expect("Failed to call create_session");

        // 5. Verify Response
        assert_eq!(
            session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
        );
    }

    #[tokio_test_no_panics]
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
            .get_channel(client.next_channel_hint())
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

    #[tokio_test_no_panics]
    async fn test_create_session_transport_retry() {
        // 1. Setup Mock Server
        let mut mock = MockSpanner::new();
        let mut seq = mockall::Sequence::new();
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                let mut status = Status::unavailable("connection reset");
                let mut headers = std::mem::take(status.metadata_mut()).into_headers();
                headers.insert("content-type", http::HeaderValue::from_static("text/html"));
                *status.metadata_mut() = MetadataMap::from_headers(headers);
                Err(status)
            });
        mock.expect_create_session()
            .once()
            .in_sequence(&mut seq)
            .returning(|_| {
                Ok(gaxi::grpc::tonic::Response::new(mock_v1::Session {
                    name: "projects/test-project/instances/test-instance/databases/test-db/sessions/789".to_string(),
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
            .create_session(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .await
            .expect("Failed to call create_session after transport error retry");

        // 5. Verify Response
        assert_eq!(
            session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/789",
            "Expected session name to match the second successful response after transport retry"
        );
    }

    #[tokio_test_no_panics]
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
            .execute_sql(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .await
            .expect("Failed to call execute_sql");
        assert!(result_set.metadata.is_some());
    }

    #[tokio_test_no_panics]
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
            .execute_batch_dml(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .await
            .expect("Failed to call execute_batch_dml");
        assert!(response.status.is_some());
    }

    #[tokio_test_no_panics]
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
            .begin_transaction(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .await
            .expect("Failed to call begin_transaction");
        assert_eq!(tx.id, vec![1, 2, 3]);
    }

    #[tokio_test_no_panics]
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
            .commit(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .await
            .expect("Failed to call commit");
        assert!(response.commit_timestamp.is_some());
    }

    #[tokio_test_no_panics]
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
            .rollback(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .await
            .expect("Failed to call rollback");
    }

    #[tokio_test_no_panics]
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
            .execute_streaming_sql(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .send()
            .await
            .expect("Failed to call execute_streaming_sql");

        let result = stream.next_message().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
    }

    #[tokio_test_no_panics]
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
            .streaming_read(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .send()
            .await
            .expect("Failed to call streaming_read");

        let result = stream.next_message().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
    }

    #[tokio_test_no_panics]
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
            .batch_write(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .send()
            .await
            .expect("Failed to call batch_write");

        let result = stream.next_message().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
    }

    #[tokio_test_no_panics]
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
            .execute_streaming_sql(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
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

    #[tokio_test_no_panics]
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
            .create_session(
                req,
                crate::RequestOptions::default(),
                client.next_channel_hint(),
            )
            .await
            .expect("Failed to call create_session");

        // 5. Verify Response
        assert_eq!(
            session.name,
            "projects/test-project/instances/test-instance/databases/test-db/sessions/456"
        );

        Ok(())
    }

    #[tokio_test_no_panics]
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

        let result = client
            .create_session(req, options, client.next_channel_hint())
            .await;

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

            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let metadata = mock_v1::ResultSetMetadata {
                transaction: Some(mock_v1::Transaction {
                    id: vec![42],
                    ..Default::default()
                }),
                ..Default::default()
            };
            let prs = mock_v1::PartialResultSet {
                metadata: Some(metadata),
                ..Default::default()
            };
            tx.try_send(Ok(prs)).unwrap();
            Ok(Response::new(rx))
        });

        mock.expect_streaming_read().once().returning(|req| {
            let metadata = req.metadata();
            let timeout = metadata.get("grpc-timeout");
            assert!(
                timeout.is_some(),
                "grpc-timeout header should be present for read"
            );

            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let metadata = mock_v1::ResultSetMetadata {
                transaction: None,
                ..Default::default()
            };
            let prs = mock_v1::PartialResultSet {
                metadata: Some(metadata),
                ..Default::default()
            };
            tx.try_send(Ok(prs)).unwrap();
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
                metadata: Some(mock_v1::ResultSetMetadata {
                    transaction: Some(mock_v1::Transaction {
                        id: vec![42],
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
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
                // TODO(#5673): ensure that transaction ID is processed even if ResultSet is dropped
                let _rs = tx.execute_query(stmt).await?;

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

    #[tokio_test_no_panics]
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
                    metadata: Some(mock_v1::ResultSetMetadata {
                        transaction: Some(mock_v1::Transaction {
                            id: vec![42],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
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
        assert_eq!(result.result, 1);

        Ok(())
    }

    fn parse_timeout(metadata: &MetadataMap) -> u64 {
        let timeout = metadata
            .get("grpc-timeout")
            .expect("grpc-timeout header should be present");
        let timeout_str = timeout
            .to_str()
            .expect("grpc-timeout should be a valid string");
        if timeout_str.ends_with('u') {
            timeout_str
                .trim_end_matches('u')
                .parse()
                .expect("valid u64")
        } else if timeout_str.ends_with('m') {
            timeout_str
                .trim_end_matches('m')
                .parse::<u64>()
                .expect("valid u64")
                * 1000
        } else if timeout_str.ends_with('n') {
            timeout_str
                .trim_end_matches('n')
                .parse::<u64>()
                .expect("valid u64")
                / 1000
        } else {
            panic!("Unknown timeout unit in {}", timeout_str);
        }
    }

    #[tokio_test_no_panics]
    async fn transaction_timeout_respected() -> anyhow::Result<()> {
        use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
        use spanner_grpc_mock::google::spanner::v1::Transaction;

        // 1. Setup Mock Server
        let mut mock = MockSpanner::new();

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        mock.expect_begin_transaction().returning(|_| {
            Ok(Response::new(Transaction {
                id: vec![1, 2, 3],
                ..Default::default()
            }))
        });

        mock.expect_commit().once().returning(|_| {
            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 12345,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        // Mock execute_sql to first fail and then succeed, checking timeout header on both
        let mut seq = mockall::Sequence::new();

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                let timeout_val = parse_timeout(req.metadata());
                assert!(
                    timeout_val <= 100000,
                    "Expected timeout to be <= 100ms, got {}",
                    timeout_val
                );
                Err(Status::new(GrpcCode::ResourceExhausted, "quota exceeded"))
            });

        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(|req| {
                let timeout_val = parse_timeout(req.metadata());
                assert!(
                    timeout_val <= 100000,
                    "Expected timeout to be <= 100ms, got {}",
                    timeout_val
                );

                let res = ResultSet {
                    metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                        transaction: Some(Transaction {
                            id: vec![1, 2, 3],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                Ok(Response::new(res))
            });

        // 2. Initialize Client
        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let db = client
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await?;

        // 3. Setup Transaction Runner with 100ms timeout
        let runner = db
            .read_write_transaction()
            .with_transaction_timeout(Duration::from_millis(100))
            .build()
            .await?;

        // 4. Run transaction and expect success after retry
        let result = runner
            .run(async |tx| {
                let mut mock_backoff = MockBackoffPolicy::new();
                mock_backoff
                    .expect_on_failure()
                    .times(1)
                    .returning(|_| Duration::from_nanos(1));

                let retry_policy = Aip194Strict.continue_on_too_many_requests();

                let stmt = Statement::builder("SELECT 1")
                    .with_retry_policy(retry_policy)
                    .with_backoff_policy(mock_backoff)
                    .build();
                tx.execute_update(stmt).await?;
                Ok(())
            })
            .await;

        result.expect("Transaction should have succeeded");

        Ok(())
    }

    #[tokio_test_no_panics]
    async fn transaction_timeout_ticks_down() -> anyhow::Result<()> {
        use spanner_grpc_mock::google::spanner::v1::Transaction;

        let mut mock = MockSpanner::new();

        mock.expect_create_session().returning(|_| {
            Ok(Response::new(Session {
                name: "projects/p/instances/i/databases/d/sessions/123".to_string(),
                ..Default::default()
            }))
        });

        let mut seq = mockall::Sequence::new();

        let previous_timeout = Arc::new(AtomicU64::new(0));
        let prev_clone1 = previous_timeout.clone();
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let timeout_val = parse_timeout(req.metadata());
                assert!(
                    timeout_val <= 500000,
                    "Expected timeout to be <= 500ms, got {}",
                    timeout_val
                );
                prev_clone1.store(timeout_val, Ordering::SeqCst);
                Err(Status::new(GrpcCode::Aborted, "Aborted"))
            });

        // Second attempt: Checks that timeout is <= previous

        let prev_clone2 = previous_timeout.clone();
        mock.expect_execute_sql()
            .once()
            .in_sequence(&mut seq)
            .returning(move |req| {
                let timeout_val = parse_timeout(req.metadata());
                let prev = prev_clone2.load(Ordering::SeqCst);
                assert!(
                    timeout_val <= prev,
                    "Timeout should tick down between attempts or be equal, got {} and {}",
                    timeout_val,
                    prev
                );
                prev_clone2.store(timeout_val, Ordering::SeqCst); // store for next check

                let res = ResultSet {
                    metadata: Some(spanner_grpc_mock::google::spanner::v1::ResultSetMetadata {
                        transaction: Some(Transaction {
                            id: vec![2],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    stats: Some(ResultSetStats {
                        row_count: Some(RowCount::RowCountExact(1)),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                Ok(Response::new(res))
            });

        let prev_clone3 = previous_timeout.clone();
        mock.expect_commit().once().returning(move |req| {
            let timeout_val = parse_timeout(req.metadata());
            let prev = prev_clone3.load(Ordering::SeqCst);
            assert!(
                timeout_val < prev,
                "Timeout should be smaller for commit, got {} and {}",
                timeout_val,
                prev
            );

            Ok(Response::new(CommitResponse {
                commit_timestamp: Some(prost_types::Timestamp {
                    seconds: 12345,
                    nanos: 0,
                }),
                ..Default::default()
            }))
        });

        let (address, _server) = start("127.0.0.1:0", mock).await?;
        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let db = client
            .database_client("projects/p/instances/i/databases/d")
            .build()
            .await?;

        let runner = db
            .read_write_transaction()
            .with_transaction_timeout(Duration::from_millis(500))
            .build()
            .await?;

        let result = runner
            .run(async |tx| {
                let stmt = Statement::builder("SELECT 1").build();
                tx.execute_update(stmt).await?;
                Ok(())
            })
            .await;

        result.expect("Transaction should have succeeded");

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
