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

use crate::ClientBuilderResult;
use crate::RequestOptions;
use crate::Result;
use crate::channel_pool::{
    ChannelLease, ChannelPool, ChannelPoolConfig, DynamicChannelPoolConfig,
    StaticChannelPoolConfig, TransactionAffinity,
};
use crate::generated::gapic_dataplane::client::Spanner as GapicSpanner;
use crate::model::{
    BatchWriteRequest, BeginTransactionRequest, CommitRequest, CommitResponse,
    CreateSessionRequest, ExecuteBatchDmlRequest, ExecuteBatchDmlResponse, ExecuteSqlRequest,
    FetchCacheUpdateRequest, PartitionQueryRequest, PartitionReadRequest, PartitionResponse,
    ReadRequest, RollbackRequest, Session, Transaction,
};
use crate::observability::Observability;
#[cfg(feature = "metrics")]
use crate::observability::metrics::SpannerMetricsInterceptor;
use crate::omni::{InstanceType, TlsConfig, TlsError, is_plaintext_endpoint};
use crate::request_id::RequestIdCreator;
use crate::request_id_interceptor::{REQUEST_ID_HEADER, SpannerRequestIdInterceptor};
use crate::server_streaming::builder;
use gaxi::attempt_interceptor::AttemptInterceptor;
use gaxi::options::{ClientConfig, Credentials};
use google_cloud_auth::credentials::anonymous;
use google_cloud_gax::client_builder::ClientBuilder as GaxClientBuilder;
use google_cloud_gax::client_builder::Error as BuilderError;
use google_cloud_gax::client_builder::internal::new_builder;
use google_cloud_gax::options::{
    RequestOptions as GaxRequestOptions, internal::RequestOptionsExt as _,
};
use google_cloud_spanner_admin_database_v1::builder::database_admin::ClientBuilder as DatabaseAdminBuilder;
use google_cloud_spanner_admin_instance_v1::builder::instance_admin::ClientBuilder as InstanceAdminBuilder;
use http::{
    HeaderMap,
    header::{HeaderName, HeaderValue},
};
use std::env;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::task::JoinSet;

pub use crate::database_client::DatabaseClient;
#[cfg(feature = "metrics")]
use crate::observability::SharedMeterProvider;
#[cfg(feature = "metrics")]
use google_cloud_gax::client_builder::Extensions;
pub use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;
pub use google_cloud_spanner_admin_instance_v1::client::InstanceAdmin;
#[cfg(feature = "metrics")]
use opentelemetry::metrics::MeterProvider;

/// A client for the [Spanner] API.
///
/// Use this client to interact with the Spanner service.
///
/// [Spanner]: https://docs.cloud.google.com/spanner/docs
#[derive(Clone, Debug)]
pub struct Spanner {
    pub(crate) channel_pool: ChannelPool,
    pub(crate) channels: Vec<Channel>,
    pub(crate) counter: Arc<AtomicUsize>,
    pub(crate) config: ClientConfig,
    pub(crate) is_emulator: bool,
    pub(crate) instance_type: InstanceType,
    pub(crate) request_id_creator: Arc<RequestIdCreator>,
    #[cfg(feature = "builtin-metrics")]
    pub(crate) export_builtin_metrics_to_cloud_monitoring: Option<bool>,
    #[cfg(feature = "metrics")]
    pub(crate) export_builtin_metrics_to_custom_provider: Option<bool>,
    #[cfg(feature = "metrics")]
    pub(crate) meter_provider: Option<SharedMeterProvider>,
}

/// A factory for constructing `Spanner` clients.
pub struct Factory;

impl google_cloud_gax::client_builder::internal::ClientFactory for Factory {
    type Client = Spanner;
    type Credentials = Credentials;

    async fn build(self, mut config: ClientConfig) -> ClientBuilderResult<Self::Client> {
        let is_emulator = detect_and_configure_emulator(&mut config);

        let is_plaintext = config
            .endpoint
            .as_deref()
            .is_some_and(is_plaintext_endpoint);

        if let Some(tls_config) = config.extensions.get::<TlsConfig>() {
            tls_config.validate().map_err(BuilderError::transport)?;
            if is_plaintext && tls_config.has_custom_certificates() {
                return Err(BuilderError::transport(TlsError::PlaintextWithTls));
            }
            if let Some(tonic_tls) = tls_config.to_tonic_client_tls_config() {
                config.extensions.insert(tonic_tls);
            }
        }

        let instance_type = config
            .extensions
            .get::<InstanceType>()
            .copied()
            .unwrap_or_default();

        if (instance_type == InstanceType::Omni || is_plaintext) && config.cred.is_none() {
            config.cred = Some(anonymous::Builder::new().build());
        }

        let pool_config = resolve_pool_config(&mut config, is_emulator)?;
        let (channel_pool, channels) = create_channel_pool(&config, pool_config).await?;

        #[cfg(feature = "builtin-metrics")]
        let export_builtin_metrics_to_cloud_monitoring = config
            .extensions
            .get::<ExportBuiltinMetricsToCloudMonitoring>()
            .map(|config| config.0);

        #[cfg(feature = "metrics")]
        let (export_builtin_metrics_to_custom_provider, meter_provider) =
            extract_metrics_config(&config.extensions);

        Ok(Spanner {
            channel_pool,
            channels,
            counter: Arc::new(AtomicUsize::new(0)),
            config,
            is_emulator,
            instance_type,
            request_id_creator: Arc::new(RequestIdCreator::new()),
            #[cfg(feature = "builtin-metrics")]
            export_builtin_metrics_to_cloud_monitoring,
            #[cfg(feature = "metrics")]
            export_builtin_metrics_to_custom_provider,
            #[cfg(feature = "metrics")]
            meter_provider,
        })
    }
}

/// A builder for the Spanner client.
pub type ClientBuilder = google_cloud_gax::client_builder::ClientBuilder<Factory, Credentials>;

/// Extension trait for [`ClientBuilder`] (also exported as `SpannerBuilder`) to configure Spanner-specific options.
pub trait SpannerBuilderExt {
    /// Configures the gRPC channel pool for the Spanner client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, SpannerBuilderExt};
    /// # use google_cloud_spanner::channel_pool::DynamicChannelPoolConfig;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Spanner::builder()
    ///     .with_channel_pool(DynamicChannelPoolConfig::new())
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Accepts any configuration convertible into [`ChannelPoolConfig`], such as
    /// [`StaticChannelPoolConfig`] or [`DynamicChannelPoolConfig`].
    fn with_channel_pool<C: Into<ChannelPoolConfig>>(self, pool_config: C) -> Self;

    /// Sets the target [`InstanceType`] (`Cloud` vs `Omni`) for the Spanner client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, SpannerBuilderExt};
    /// # use google_cloud_spanner::omni::InstanceType;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Spanner::builder()
    ///     .with_instance_type(InstanceType::Omni)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    fn with_instance_type(self, instance_type: InstanceType) -> Self;

    /// Configures custom TLS or mutual TLS (mTLS) settings for Spanner Omni.
    ///
    /// # Example
    /// ```no_run
    /// # use google_cloud_spanner::client::{Spanner, SpannerBuilderExt};
    /// # use google_cloud_spanner::omni::TlsConfig;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let tls_config = TlsConfig::new()
    ///     .with_root_certificate_file("path/to/ca.pem")?;
    /// let client = Spanner::builder()
    ///     .with_omni_tls(tls_config)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Calling this method automatically configures the client instance type as `InstanceType::Omni`.
    fn with_omni_tls(self, tls_config: TlsConfig) -> Self;

    /// Configures a custom OpenTelemetry [`MeterProvider`] for recording Client metrics.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use opentelemetry_sdk::metrics::SdkMeterProvider;
    /// # use google_cloud_spanner::client::{Spanner, SpannerBuilderExt};
    /// # async fn sample() -> anyhow::Result<()> {
    /// let provider = Arc::new(SdkMeterProvider::builder().build());
    /// let spanner = Spanner::builder()
    ///     .with_meter_provider(provider)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// The caller owns the lifecycle of this provider; Spanner will not shut it down.
    ///
    /// # Client Metrics
    ///
    /// When configured, Client metrics (such as gRPC channel pool metrics) are recorded
    /// directly to this provider.
    ///
    /// # Built-in Metrics Export (Secondary)
    ///
    /// In addition to Client metrics, this provider can optionally receive Spanner
    /// built-in request and attempt latency metrics:
    ///
    /// - **Cloud Spanner**: Built-in metrics are exported directly to Google Cloud
    ///   Monitoring free of charge and are **not** duplicated to this provider by default
    ///   to protect users from unexpected third-party monitoring costs. To export built-in
    ///   metrics to this provider as well, call
    ///   [`with_export_builtin_metrics_to_custom_provider(true)`](Self::with_export_builtin_metrics_to_custom_provider).
    ///   Note that this opt-in is required on Cloud Spanner even when the `builtin-metrics`
    ///   Cargo feature is compiled out.
    ///
    /// - **Spanner Omni**: Because Cloud Monitoring is not active for Omni, built-in metrics
    ///   are exported to this provider by default.
    #[cfg(feature = "metrics")]
    fn with_meter_provider(self, provider: Arc<dyn MeterProvider + Send + Sync>) -> Self;

    /// Configures whether built-in request and attempt latency metrics should be
    /// exported to the custom [`MeterProvider`] configured via
    /// [`with_meter_provider`](Self::with_meter_provider).
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use opentelemetry_sdk::metrics::SdkMeterProvider;
    /// # use google_cloud_spanner::client::{Spanner, SpannerBuilderExt};
    /// # async fn sample() -> anyhow::Result<()> {
    /// let provider = Arc::new(SdkMeterProvider::builder().build());
    /// let client = Spanner::builder()
    ///     .with_meter_provider(provider)
    ///     .with_export_builtin_metrics_to_custom_provider(true)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Background & Cost Considerations
    ///
    /// Cloud Spanner collects curated client metrics (such as `operation_latencies`,
    /// `attempt_latencies`, and `attempt_count`).
    ///
    /// - **Cloud Spanner**: Built-in metrics are exported directly to Google Cloud
    ///   Monitoring (GCM) free of charge under the `spanner.googleapis.com/internal/client/`
    ///   namespace.
    ///   If these high-frequency histograms were automatically duplicated to a customer's
    ///   custom `MeterProvider` (which may export to Datadog, New Relic, Dynatrace, or
    ///   custom GCM ingestion pipelines), the customer could incur substantial unexpected
    ///   monitoring and ingestion costs.
    ///   Therefore, on Cloud Spanner, exporting built-in metrics to the custom `MeterProvider`
    ///   **defaults to `false`** (even when the `builtin-metrics` Cargo feature is compiled out).
    ///   Callers must explicitly opt in by calling
    ///   `with_export_builtin_metrics_to_custom_provider(true)` if they want built-in metrics
    ///   sent to their custom OpenTelemetry sink.
    ///
    /// - **Spanner Omni**: Spanner Omni instances run outside GCP where Cloud Monitoring is
    ///   not active. Omni customers providing a custom `MeterProvider` rely on it as their
    ///   primary metrics sink.
    ///   Therefore, on Spanner Omni, exporting built-in metrics to the custom `MeterProvider`
    ///   **defaults to `true`**.
    ///
    /// - **Emulator**: When connecting to the Spanner emulator, all built-in metrics collection
    ///   and export are disabled.
    #[cfg(feature = "metrics")]
    fn with_export_builtin_metrics_to_custom_provider(self, export: bool) -> Self;

    /// Configures whether built-in request and attempt latency metrics should be
    /// exported to Google Cloud Monitoring.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::{Spanner, SpannerBuilderExt};
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Spanner::builder()
    ///     .with_export_builtin_metrics_to_cloud_monitoring(false)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Default & Environment Overrides
    ///
    /// - **Cloud Spanner**: Defaults to `true`. Export to Google Cloud Monitoring can also
    ///   be disabled by setting the environment variable `SPANNER_DISABLE_BUILTIN_METRICS=true`.
    ///   When explicitly configured via this method, the programmatic setting takes precedence.
    ///
    /// - **Spanner Omni & Emulator**: Defaults to `false`. Cloud Monitoring export is never
    ///   enabled for Spanner Omni instances or when connecting to the Spanner emulator.
    ///
    /// # Independence from Custom Provider Export
    ///
    /// Disabling Cloud Monitoring export does **not** affect built-in metrics exported to a
    /// custom [`MeterProvider`] configured via
    /// [`with_meter_provider`](Self::with_meter_provider) and
    /// [`with_export_builtin_metrics_to_custom_provider`](Self::with_export_builtin_metrics_to_custom_provider).
    #[cfg(feature = "builtin-metrics")]
    fn with_export_builtin_metrics_to_cloud_monitoring(self, export: bool) -> Self;
}

#[cfg(feature = "builtin-metrics")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExportBuiltinMetricsToCloudMonitoring(bool);

#[cfg(feature = "metrics")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExportBuiltinMetricsToCustomProvider(bool);

impl SpannerBuilderExt for ClientBuilder {
    fn with_channel_pool<C: Into<ChannelPoolConfig>>(self, pool_config: C) -> Self {
        self.with_extension(pool_config.into())
    }

    fn with_instance_type(self, instance_type: InstanceType) -> Self {
        self.with_extension(instance_type)
    }

    fn with_omni_tls(self, tls_config: TlsConfig) -> Self {
        self.with_extension(InstanceType::Omni)
            .with_extension(tls_config)
    }

    #[cfg(feature = "metrics")]
    fn with_meter_provider(self, provider: Arc<dyn MeterProvider + Send + Sync>) -> Self {
        self.with_extension(SharedMeterProvider::from(provider))
    }

    #[cfg(feature = "metrics")]
    fn with_export_builtin_metrics_to_custom_provider(self, export: bool) -> Self {
        self.with_extension(ExportBuiltinMetricsToCustomProvider(export))
    }

    #[cfg(feature = "builtin-metrics")]
    fn with_export_builtin_metrics_to_cloud_monitoring(self, export: bool) -> Self {
        self.with_extension(ExportBuiltinMetricsToCloudMonitoring(export))
    }
}

fn parse_emulator_endpoint(endpoint: &str) -> String {
    match url::Url::parse(endpoint) {
        Ok(url) if url.has_host() => endpoint.to_string(),
        _ => format!("http://{}", endpoint),
    }
}

fn detect_and_configure_emulator(config: &mut ClientConfig) -> bool {
    let Ok(endpoint) = env::var("SPANNER_EMULATOR_HOST") else {
        return false;
    };
    detect_and_configure_emulator_from_host(config, &endpoint)
}

fn detect_and_configure_emulator_from_host(config: &mut ClientConfig, emulator_host: &str) -> bool {
    if emulator_host.is_empty() {
        return false;
    }

    let emulator_endpoint = parse_emulator_endpoint(emulator_host);
    let is_emulator = match config.endpoint.as_deref() {
        // If no endpoint was explicitly set on the client config, adopt the emulator host.
        None => {
            config.endpoint = Some(emulator_endpoint);
            true
        }
        // If an explicit endpoint was specified, only treat the client as connecting to the
        // emulator if that endpoint actually points to the emulator host (either raw or parsed URL).
        Some(configured_endpoint)
            if configured_endpoint == emulator_host || configured_endpoint == emulator_endpoint =>
        {
            true
        }
        // An explicit endpoint pointing to another host (e.g. a mock server, Omni, or Cloud Spanner)
        // is not considered an emulator connection.
        Some(_) => false,
    };

    // The emulator does not require authentication; default to anonymous credentials
    // if none were provided.
    if is_emulator && config.cred.is_none() {
        config.cred = Some(anonymous::Builder::new().build());
    }

    is_emulator
}

fn resolve_pool_config(
    config: &mut ClientConfig,
    is_emulator: bool,
) -> ClientBuilderResult<ChannelPoolConfig> {
    resolve_pool_config_with(
        config,
        is_emulator,
        || env::var("SPANNER_NUM_CHANNELS").ok(),
        || env::var("SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL").ok(),
    )
}

fn is_truthy(val: &str) -> bool {
    matches!(
        val.trim().to_ascii_lowercase().as_str(),
        "true" | "1" | "yes" | "on" | "t"
    )
}

fn resolve_pool_config_with(
    config: &mut ClientConfig,
    is_emulator: bool,
    num_channels_lookup: impl FnOnce() -> Option<String>,
    dynamic_pool_lookup: impl FnOnce() -> Option<String>,
) -> ClientBuilderResult<ChannelPoolConfig> {
    if let Some(pool_config) = config.extensions.remove::<ChannelPoolConfig>() {
        let pool_config = Arc::unwrap_or_clone(pool_config);
        pool_config.validate().map_err(BuilderError::transport)?;
        return Ok(pool_config);
    }
    if let Some(arc_config) = config.extensions.remove::<Arc<ChannelPoolConfig>>() {
        let arc_config = Arc::unwrap_or_clone(arc_config);
        let pool_config = Arc::unwrap_or_clone(arc_config);
        pool_config.validate().map_err(BuilderError::transport)?;
        return Ok(pool_config);
    }
    if let Some(static_config) = config.extensions.remove::<StaticChannelPoolConfig>() {
        let static_config = Arc::unwrap_or_clone(static_config);
        static_config.validate().map_err(BuilderError::transport)?;
        return Ok(ChannelPoolConfig::Static(static_config));
    }
    if let Some(dynamic_config) = config.extensions.remove::<DynamicChannelPoolConfig>() {
        let dynamic_config = Arc::unwrap_or_clone(dynamic_config);
        dynamic_config.validate().map_err(BuilderError::transport)?;
        return Ok(ChannelPoolConfig::Dynamic(dynamic_config));
    }
    let enable_dynamic = dynamic_pool_lookup()
        .as_deref()
        .map(is_truthy)
        .unwrap_or(false);

    let num_channels = match num_channels_lookup() {
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.parse::<usize>().map_err(BuilderError::transport)?)
            }
        }
        None => None,
    };

    if enable_dynamic {
        let mut dynamic_config = DynamicChannelPoolConfig::default();
        if let Some(channels) = num_channels {
            dynamic_config = dynamic_config
                .with_initial_channels(channels)
                .with_min_channels(channels);
        }
        dynamic_config.validate().map_err(BuilderError::transport)?;
        return Ok(ChannelPoolConfig::Dynamic(dynamic_config));
    }

    if let Some(channels) = num_channels {
        let static_config = StaticChannelPoolConfig::new(channels);
        static_config.validate().map_err(BuilderError::transport)?;
        return Ok(ChannelPoolConfig::Static(static_config));
    }
    if is_emulator {
        return Ok(ChannelPoolConfig::Static(StaticChannelPoolConfig::new(1)));
    }
    Ok(ChannelPoolConfig::Static(StaticChannelPoolConfig::default()))
}

async fn create_channel_pool(
    config: &ClientConfig,
    pool_config: ChannelPoolConfig,
) -> ClientBuilderResult<(ChannelPool, Vec<Channel>)> {
    let num_initial = match &pool_config {
        ChannelPoolConfig::Static(static_config) => static_config.num_channels,
        ChannelPoolConfig::Dynamic(dynamic_config) => dynamic_config.initial_channels,
    };

    let mut join_set = JoinSet::new();
    for index in 0..num_initial {
        let config_clone = config.clone();
        join_set.spawn(async move { Channel::create(config_clone, index + 1).await });
    }

    let mut channels = Vec::with_capacity(num_initial);
    while let Some(join_result) = join_set.join_next().await {
        let channel_result = join_result.map_err(BuilderError::transport)?;
        channels.push(channel_result?);
    }
    channels.sort_by_key(|channel| channel.channel_id);

    let pool = match pool_config {
        ChannelPoolConfig::Static(static_config) => {
            ChannelPool::new_static(channels.clone(), static_config, config.clone())
        }
        ChannelPoolConfig::Dynamic(dynamic_config) => {
            ChannelPool::new_dynamic(channels.clone(), dynamic_config, config.clone())
        }
    };
    Ok((pool, channels))
}

#[cfg(feature = "metrics")]
fn extract_metrics_config(extensions: &Extensions) -> (Option<bool>, Option<SharedMeterProvider>) {
    let export_builtin_metrics_to_custom_provider = extensions
        .get::<ExportBuiltinMetricsToCustomProvider>()
        .map(|config| config.0);
    let meter_provider = extensions
        .get::<SharedMeterProvider>()
        .cloned()
        .or_else(|| {
            extensions
                .get::<Arc<dyn MeterProvider + Send + Sync>>()
                .map(|provider| SharedMeterProvider::from(Arc::clone(provider)))
        });
    (export_builtin_metrics_to_custom_provider, meter_provider)
}

macro_rules! define_idempotent_rpc {
    ($method:ident, $request_type:ty, $response_type:ty, $canonical_name:expr) => {
        pub(crate) async fn $method(
            &self,
            request: $request_type,
            options: RequestOptions,
            channel: &Channel,
            o11y: &Arc<Observability>,
        ) -> Result<$response_type> {
            let options = self.attach_request_id(options, channel.channel_id);
            #[cfg(feature = "metrics")]
            let options = options.insert_extension(Arc::clone(o11y));
            o11y.trace_operation(
                $canonical_name,
                channel
                    .inner
                    .$method()
                    .with_request(request)
                    .with_options(apply_request_defaults(options))
                    .send(),
            )
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

static ROUTE_TO_LEADER_HEADER: HeaderName =
    HeaderName::from_static("x-goog-spanner-route-to-leader");
static ROUTE_TO_LEADER_VALUE: HeaderValue = HeaderValue::from_static("true");

pub(crate) fn amend_request_options_for_lar(
    leader_aware_routing_enabled: bool,
    mut options: GaxRequestOptions,
) -> GaxRequestOptions {
    if leader_aware_routing_enabled {
        options.get_extension_or_default_mut::<HeaderMap>().insert(
            ROUTE_TO_LEADER_HEADER.clone(),
            ROUTE_TO_LEADER_VALUE.clone(),
        );
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
        new_builder(Factory)
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

    /// Returns the number of currently active channels in the client's channel pool.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_spanner::client::Spanner;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Spanner::builder().build().await?;
    /// let active_channels = client.active_channel_count();
    /// # Ok(()) }
    /// ```
    pub fn active_channel_count(&self) -> usize {
        self.channel_pool.active_channel_count()
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
        let channel = Channel {
            inner: GapicSpanner::from_stub(stub),
            grpc_client: None,
            channel_id: 1,
        };
        let channel_pool = ChannelPool::new_static(
            vec![channel.clone()],
            StaticChannelPoolConfig::new(1),
            ClientConfig::default(),
        );
        Self {
            channel_pool,
            channels: vec![channel],
            counter: Arc::new(AtomicUsize::new(0)),
            config: ClientConfig::default(),
            is_emulator: false,
            instance_type: InstanceType::Cloud,
            request_id_creator: Arc::new(RequestIdCreator::new()),
            #[cfg(feature = "builtin-metrics")]
            export_builtin_metrics_to_cloud_monitoring: None,
            #[cfg(feature = "metrics")]
            export_builtin_metrics_to_custom_provider: None,
            #[cfg(feature = "metrics")]
            meter_provider: None,
        }
    }

    #[cfg(feature = "builtin-metrics")]
    pub(crate) fn export_builtin_metrics_to_cloud_monitoring(&self) -> Option<bool> {
        self.export_builtin_metrics_to_cloud_monitoring
    }

    #[cfg(all(feature = "metrics", not(feature = "builtin-metrics")))]
    pub(crate) fn export_builtin_metrics_to_cloud_monitoring(&self) -> Option<bool> {
        None
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn export_builtin_metrics_to_custom_provider(&self) -> Option<bool> {
        self.export_builtin_metrics_to_custom_provider
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn meter_provider(&self) -> Option<SharedMeterProvider> {
        self.meter_provider.clone()
    }

    pub(crate) fn is_emulator(&self) -> bool {
        self.is_emulator
    }

    pub(crate) fn instance_type(&self) -> InstanceType {
        self.instance_type
    }

    #[allow(dead_code)]
    pub(crate) fn channel_pool(&self) -> &ChannelPool {
        &self.channel_pool
    }

    pub(crate) fn pick_channel(&self) -> ChannelLease {
        self.channel_pool
            .pick_channel()
            .expect("channel pool must have active channels")
    }

    #[allow(dead_code)]
    pub(crate) fn resolve_affinity(&self, affinity: &TransactionAffinity) -> ChannelLease {
        self.channel_pool
            .resolve_affinity(affinity)
            .expect("channel pool must have active channels")
    }

    pub(crate) fn get_channel(&self, hint: usize) -> &Channel {
        let idx = hint % self.channels.len();
        &self.channels[idx]
    }

    pub(crate) fn next_channel_hint(&self) -> usize {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn attach_request_id(
        &self,
        mut options: RequestOptions,
        channel_id: usize,
    ) -> RequestOptions {
        if options
            .get_extension::<HeaderMap>()
            .is_some_and(|headers| headers.contains_key(&REQUEST_ID_HEADER))
        {
            return options;
        }

        let header_val_str = self.request_id_creator.next_id_prefix(channel_id);
        let Ok(val) = HeaderValue::from_str(&header_val_str) else {
            return options;
        };

        options
            .get_extension_or_default_mut::<HeaderMap>()
            .insert(REQUEST_ID_HEADER.clone(), val);
        options
    }

    define_idempotent_rpc!(
        create_session,
        CreateSessionRequest,
        Session,
        "google.spanner.v1.Spanner/CreateSession"
    );
    define_idempotent_rpc!(
        execute_sql,
        ExecuteSqlRequest,
        crate::model::ResultSet,
        "google.spanner.v1.Spanner/ExecuteSql"
    );
    define_idempotent_rpc!(
        execute_batch_dml,
        ExecuteBatchDmlRequest,
        ExecuteBatchDmlResponse,
        "google.spanner.v1.Spanner/ExecuteBatchDml"
    );
    define_idempotent_rpc!(
        begin_transaction,
        BeginTransactionRequest,
        Transaction,
        "google.spanner.v1.Spanner/BeginTransaction"
    );
    define_idempotent_rpc!(
        commit,
        CommitRequest,
        CommitResponse,
        "google.spanner.v1.Spanner/Commit"
    );
    define_idempotent_rpc!(
        rollback,
        RollbackRequest,
        (),
        "google.spanner.v1.Spanner/Rollback"
    );
    define_idempotent_rpc!(
        partition_query,
        PartitionQueryRequest,
        PartitionResponse,
        "google.spanner.v1.Spanner/PartitionQuery"
    );
    define_idempotent_rpc!(
        partition_read,
        PartitionReadRequest,
        PartitionResponse,
        "google.spanner.v1.Spanner/PartitionRead"
    );

    /// Executes an SQL statement, returning a stream of results.
    ///
    /// This is a custom streaming implementation over the underlying Spanner gRPC
    /// transport, since streaming responses are not yet auto-generated here.
    pub(crate) fn execute_streaming_sql(
        &self,
        request: ExecuteSqlRequest,
        options: RequestOptions,
        channel: &Channel,
    ) -> builder::ExecuteStreamingSql {
        let grpc = channel
            .grpc_client
            .as_ref()
            .expect("Streaming RPCs are not supported when using a stub client");
        builder::ExecuteStreamingSql::new(grpc.clone())
            .with_request(request)
            .with_options(self.attach_request_id(options, channel.channel_id))
    }

    /// Reads rows from the database, returning a stream of results.
    ///
    /// This is a custom streaming implementation over the underlying Spanner gRPC
    /// transport, since streaming responses are not yet auto-generated here.
    pub(crate) fn streaming_read(
        &self,
        request: ReadRequest,
        options: RequestOptions,
        channel: &Channel,
    ) -> builder::StreamingRead {
        let grpc = channel
            .grpc_client
            .as_ref()
            .expect("Streaming RPCs are not supported when using a stub client");
        builder::StreamingRead::new(grpc.clone())
            .with_request(request)
            .with_options(self.attach_request_id(options, channel.channel_id))
    }

    pub(crate) fn batch_write(
        &self,
        request: BatchWriteRequest,
        options: RequestOptions,
        channel: &Channel,
    ) -> builder::BatchWrite {
        let grpc = channel
            .grpc_client
            .as_ref()
            .expect("Streaming RPCs are not supported when using a stub client");
        builder::BatchWrite::new(grpc.clone())
            .with_request(request)
            .with_options(self.attach_request_id(options, channel.channel_id))
    }

    pub(crate) fn fetch_cache_update(
        &self,
        request: FetchCacheUpdateRequest,
        options: RequestOptions,
        channel: &Channel,
    ) -> builder::FetchCacheUpdate {
        let grpc = channel
            .grpc_client
            .as_ref()
            .expect("Streaming RPCs are not supported when using a stub client");
        builder::FetchCacheUpdate::new(grpc.clone())
            .with_request(request)
            .with_options(self.attach_request_id(options, channel.channel_id))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Channel {
    pub(crate) inner: GapicSpanner,
    pub(crate) grpc_client: Option<gaxi::grpc::Client>,
    pub(crate) channel_id: usize,
}

impl Channel {
    pub(crate) async fn create(
        config: ClientConfig,
        channel_id: usize,
    ) -> crate::ClientBuilderResult<Self> {
        let tracing_enabled = gaxi::options::tracing_enabled(&config);
        let mut transport =
            crate::generated::gapic_dataplane::transport::Spanner::new(config).await?;
        let request_id_interceptor: Arc<dyn AttemptInterceptor> =
            Arc::new(SpannerRequestIdInterceptor);

        #[cfg(feature = "metrics")]
        let interceptor: Arc<dyn AttemptInterceptor> = Arc::new(vec![
            request_id_interceptor,
            Arc::new(SpannerMetricsInterceptor),
        ]);

        #[cfg(not(feature = "metrics"))]
        let interceptor: Arc<dyn AttemptInterceptor> = request_id_interceptor;

        transport.inner.set_attempt_interceptor(interceptor);
        let grpc_client = transport.inner.clone();

        let inner = if tracing_enabled {
            GapicSpanner::from_stub(crate::generated::gapic_dataplane::tracing::Spanner::new(
                transport,
            ))
        } else {
            GapicSpanner::from_stub(transport)
        };
        Ok(Self {
            inner,
            grpc_client: Some(grpc_client),
            channel_id,
        })
    }
}

#[cfg(test)]
impl Channel {
    pub(crate) fn new_for_test<T>(stub: T) -> Self
    where
        T: crate::generated::gapic_dataplane::stub::Spanner + 'static,
    {
        Self {
            inner: GapicSpanner::from_stub(stub),
            grpc_client: None,
            channel_id: 0,
        }
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
    use serial_test::serial;
    use spanner_grpc_mock::google::rpc as mock_rpc;
    use spanner_grpc_mock::google::spanner::v1 as mock_v1;
    use spanner_grpc_mock::google::spanner::v1::CommitResponse;
    use spanner_grpc_mock::google::spanner::v1::ResultSet;
    use spanner_grpc_mock::google::spanner::v1::ResultSetStats;
    use spanner_grpc_mock::google::spanner::v1::Session;
    use spanner_grpc_mock::google::spanner::v1::result_set_stats::RowCount;
    use spanner_grpc_mock::{MockSpanner, start};
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use std::fmt::Debug;
    use std::panic::{RefUnwindSafe, UnwindSafe};
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
        assert_impl_all!(Spanner: Debug, Clone, Send, Sync);
        assert_not_impl_any!(Spanner: RefUnwindSafe, UnwindSafe);
    }

    #[tokio_test_no_panics]
    #[serial]
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

        let expected_channels = if client.is_emulator() { 1 } else { 4 };
        assert_eq!(client.active_channel_count(), expected_channels);
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
                &client.pick_channel(),
                &Observability::disabled_arc(),
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
                &client.pick_channel(),
                &Observability::disabled_arc(),
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
                &client.pick_channel(),
                &Observability::disabled_arc(),
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
                &client.pick_channel(),
                &Observability::disabled_arc(),
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
                &client.pick_channel(),
                &Observability::disabled_arc(),
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
                &client.pick_channel(),
                &Observability::disabled_arc(),
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
                &client.pick_channel(),
                &Observability::disabled_arc(),
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
                &client.pick_channel(),
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
                &client.pick_channel(),
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
                &client.pick_channel(),
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
                &client.pick_channel(),
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
                &client.pick_channel(),
                &Observability::disabled_arc(),
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
            .create_session(
                req,
                options,
                &client.pick_channel(),
                &Observability::disabled_arc(),
            )
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

    #[tokio_test_no_panics]
    async fn attach_request_id_adds_header() {
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

        let channel = client.pick_channel();
        let options = crate::RequestOptions::default();
        let options = client.attach_request_id(options, channel.channel_id);
        let headers = options
            .get_extension::<HeaderMap>()
            .expect("HeaderMap should be present");
        let val = headers
            .get(&REQUEST_ID_HEADER)
            .expect("x-goog-spanner-request-id should be present")
            .to_str()
            .expect("should be valid ASCII");
        assert!(
            val.starts_with("1."),
            "Request ID prefix should start with protocol version 1, got {val}"
        );
        assert!(
            val.ends_with('.'),
            "Request ID prefix should end with a dot ready for AttemptInterceptor, got {val}"
        );
    }

    #[tokio_test_no_panics]
    async fn attach_request_id_channel_id_in_range() {
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

        assert_eq!(
            client.active_channel_count(),
            4,
            "default pool size should be 4 channels"
        );

        // Test with a channel_hint that is larger than the pool size (e.g., hint = 7).
        // get_channel(7) maps to channel at index (7 % 4 = 3), which has 1-based channel_id 4.
        let channel = client.get_channel(7);
        let options = crate::RequestOptions::default();
        let options = client.attach_request_id(options, channel.channel_id);
        let headers = options
            .get_extension::<HeaderMap>()
            .expect("HeaderMap should be present");
        let val = headers
            .get(&REQUEST_ID_HEADER)
            .expect("x-goog-spanner-request-id should be present")
            .to_str()
            .expect("should be valid ASCII");

        // With 4 channels and hint = 7: (7 % 4) + 1 = 3 + 1 = 4.
        // So the prefix should contain ".4." for channel ID 4.
        assert!(
            val.contains(".4."),
            "Request ID should contain channel ID 4 for hint 7 with pool size 4, got {val}"
        );
    }

    #[tokio_test_no_panics]
    async fn attach_request_id_idempotent_no_duplicate_headers() {
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

        let channel = client.get_channel(0);
        let mut options = crate::RequestOptions::default();
        options = client.attach_request_id(options, channel.channel_id);
        let first_headers = options
            .get_extension::<HeaderMap>()
            .expect("HeaderMap should be present")
            .clone();
        let first_val = first_headers
            .get(&REQUEST_ID_HEADER)
            .expect("request id should be present")
            .clone();

        // Calling attach_request_id a second time must NOT change the value or add duplicate headers
        options = client.attach_request_id(options, channel.channel_id);
        let second_headers = options
            .get_extension::<HeaderMap>()
            .expect("HeaderMap should be present");

        assert_eq!(
            second_headers.get_all(&REQUEST_ID_HEADER).iter().count(),
            1,
            "REQUEST_ID_HEADER should only be present once"
        );
        assert_eq!(
            second_headers.get(&REQUEST_ID_HEADER),
            Some(&first_val),
            "Second call must preserve original Request ID"
        );
    }

    #[test]
    fn amend_request_options_for_lar_idempotent_no_duplicate_headers() {
        let options = crate::RequestOptions::default();
        let options = super::amend_request_options_for_lar(true, options);
        let options = super::amend_request_options_for_lar(true, options);

        let headers = options
            .get_extension::<HeaderMap>()
            .expect("HeaderMap should be present");

        assert_eq!(
            headers
                .get_all(&super::ROUTE_TO_LEADER_HEADER)
                .iter()
                .count(),
            1,
            "LAR header should only be present once even after multiple amend calls"
        );
        assert_eq!(
            headers.get(&super::ROUTE_TO_LEADER_HEADER),
            Some(&super::ROUTE_TO_LEADER_VALUE),
            "LAR header should match ROUTE_TO_LEADER_VALUE"
        );
    }

    #[cfg(feature = "builtin-metrics")]
    #[tokio_test_no_panics]
    async fn spanner_builder_with_meter_provider_cloud_spanner_default_does_not_export_builtin_metrics()
     {
        use opentelemetry::metrics::MeterProvider;
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        };

        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            assert!(session.multiplexed, "session should be multiplexed");

            Ok(Response::new(Session {
                name:
                    "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
                        .to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter).build();
        let provider: Arc<dyn MeterProvider + Send + Sync> =
            Arc::new(SdkMeterProvider::builder().with_reader(reader).build());

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .with_meter_provider(Arc::clone(&provider))
            .build()
            .await
            .expect("Failed to build client");

        assert!(
            spanner.meter_provider().is_some(),
            "provider should be stored on spanner"
        );
        assert_eq!(
            spanner.export_builtin_metrics_to_custom_provider(),
            None,
            "export_builtin_metrics_to_custom_provider should default to None"
        );

        let db_client = spanner
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        assert!(
            !db_client.o11y.is_enabled(),
            "Observability should be disabled for built-in metrics on Cloud Spanner by default"
        );
        assert!(
            db_client.o11y.caller_meter_provider.is_some(),
            "caller_meter_provider should be preserved on the client"
        );
        assert_eq!(
            db_client.o11y.metrics.len(),
            0,
            "Cloud Spanner default does not duplicate built-in metrics to caller provider"
        );
    }

    #[cfg(feature = "builtin-metrics")]
    #[tokio_test_no_panics]
    async fn spanner_builder_with_export_builtin_metrics_to_custom_provider() {
        use opentelemetry::metrics::MeterProvider;
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        };

        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            assert!(session.multiplexed, "session should be multiplexed");

            Ok(Response::new(Session {
                name:
                    "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
                        .to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter).build();
        let provider: Arc<dyn MeterProvider + Send + Sync> =
            Arc::new(SdkMeterProvider::builder().with_reader(reader).build());

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .with_meter_provider(Arc::clone(&provider))
            .with_export_builtin_metrics_to_custom_provider(true)
            .build()
            .await
            .expect("Failed to build client");

        assert_eq!(
            spanner.export_builtin_metrics_to_custom_provider(),
            Some(true),
            "export flag should be Some(true)"
        );

        let db_client = spanner
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        assert!(
            db_client.o11y.is_enabled(),
            "Observability should be enabled when opting in to export built-in metrics"
        );
        assert_eq!(
            db_client.o11y.metrics.len(),
            1,
            "expected 1 metrics sink for the custom provider"
        );
    }

    #[cfg(feature = "builtin-metrics")]
    #[tokio_test_no_panics]
    async fn spanner_builder_omni_with_meter_provider_defaults_to_export_builtin_metrics() {
        use opentelemetry::metrics::MeterProvider;
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        };

        let mut mock = MockSpanner::new();
        mock.expect_create_session().once().returning(|req| {
            let req = req.into_inner();
            let session = req.session.expect("session present in request");
            assert!(session.multiplexed, "session should be multiplexed");

            Ok(Response::new(Session {
                name:
                    "projects/test-project/instances/test-instance/databases/test-db/sessions/123"
                        .to_string(),
                multiplexed: true,
                ..Default::default()
            }))
        });

        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter).build();
        let provider: Arc<dyn MeterProvider + Send + Sync> =
            Arc::new(SdkMeterProvider::builder().with_reader(reader).build());

        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_instance_type(InstanceType::Omni)
            .with_meter_provider(Arc::clone(&provider))
            .build()
            .await
            .expect("Failed to build client");

        assert_eq!(
            spanner.export_builtin_metrics_to_custom_provider(),
            None,
            "export flag defaults to None"
        );

        let db_client = spanner
            .database_client("projects/test-project/instances/test-instance/databases/test-db")
            .build()
            .await
            .expect("Failed to create DatabaseClient");

        assert!(
            db_client.o11y.is_enabled(),
            "Observability should be enabled on Omni by default when provider is supplied"
        );
        assert_eq!(
            db_client.o11y.metrics.len(),
            1,
            "expected 1 metrics sink for Omni custom provider"
        );
    }

    #[cfg(feature = "builtin-metrics")]
    #[tokio_test_no_panics]
    async fn spanner_builder_with_export_builtin_metrics_to_cloud_monitoring() {
        let spanner = Spanner::builder()
            .with_endpoint("http://127.0.0.1:1")
            .with_credentials(Anonymous::new().build())
            .with_export_builtin_metrics_to_cloud_monitoring(false)
            .build()
            .await
            .expect("Failed to build client");

        assert_eq!(
            spanner.export_builtin_metrics_to_cloud_monitoring(),
            Some(false),
            "export_builtin_metrics_to_cloud_monitoring should be Some(false)"
        );

        let spanner_enabled = Spanner::builder()
            .with_endpoint("http://127.0.0.1:1")
            .with_credentials(Anonymous::new().build())
            .with_export_builtin_metrics_to_cloud_monitoring(true)
            .build()
            .await
            .expect("Failed to build client");

        assert_eq!(
            spanner_enabled.export_builtin_metrics_to_cloud_monitoring(),
            Some(true),
            "export_builtin_metrics_to_cloud_monitoring should be Some(true)"
        );
    }

    #[cfg(feature = "builtin-metrics")]
    #[tokio_test_no_panics]
    async fn spanner_builder_with_raw_meter_provider_extension() {
        use opentelemetry::metrics::MeterProvider;
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        };

        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter).build();
        let provider: Arc<dyn MeterProvider + Send + Sync> =
            Arc::new(SdkMeterProvider::builder().with_reader(reader).build());

        let spanner = Spanner::builder()
            .with_endpoint("http://127.0.0.1:1")
            .with_credentials(Anonymous::new().build())
            .with_extension(Arc::clone(&provider))
            .build()
            .await
            .expect("Failed to build client");

        assert!(
            spanner.meter_provider().is_some(),
            "raw Arc<dyn MeterProvider> extension should be recognized"
        );
    }

    #[test]
    fn detect_and_configure_emulator_empty_host() {
        let mut config = ClientConfig::default();
        let is_emulator = super::detect_and_configure_emulator_from_host(&mut config, "");
        assert!(
            !is_emulator,
            "empty emulator host should not be detected as emulator"
        );
        assert!(config.endpoint.is_none(), "endpoint should remain None");
        assert!(config.cred.is_none(), "credentials should remain None");
    }

    #[test]
    fn detect_and_configure_emulator_default_endpoint_and_credentials() {
        let mut config = ClientConfig::default();
        let is_emulator =
            super::detect_and_configure_emulator_from_host(&mut config, "localhost:9010");
        assert!(is_emulator, "emulator host should be detected as emulator");
        assert_eq!(
            config.endpoint.as_deref(),
            Some("http://localhost:9010"),
            "endpoint should be populated with emulator URL"
        );
        assert!(
            config.cred.is_some(),
            "anonymous credentials should be automatically configured"
        );
    }

    #[test]
    fn detect_and_configure_emulator_distinct_endpoint_not_emulator() {
        let mut config = ClientConfig::default();
        config.endpoint = Some("0.0.0.0:12345".to_string());
        let is_emulator =
            super::detect_and_configure_emulator_from_host(&mut config, "localhost:9010");
        assert!(
            !is_emulator,
            "distinct endpoint should not be marked as emulator"
        );
        assert_eq!(
            config.endpoint.as_deref(),
            Some("0.0.0.0:12345"),
            "distinct endpoint should not be overwritten"
        );
        assert!(
            config.cred.is_none(),
            "credentials should not be set for non-emulator endpoint"
        );
    }

    #[test]
    fn detect_and_configure_emulator_matching_explicit_endpoint() {
        let mut config = ClientConfig::default();
        config.endpoint = Some("localhost:9010".to_string());
        let is_emulator =
            super::detect_and_configure_emulator_from_host(&mut config, "localhost:9010");
        assert!(
            is_emulator,
            "matching explicit endpoint should be marked as emulator"
        );
        assert_eq!(
            config.endpoint.as_deref(),
            Some("localhost:9010"),
            "explicit endpoint should be preserved"
        );
        assert!(
            config.cred.is_some(),
            "anonymous credentials should be configured"
        );
    }

    #[tokio_test_no_panics]
    async fn builder_with_static_channel_pool_config() {
        let mock = MockSpanner::new();
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .with_channel_pool(StaticChannelPoolConfig::new(2))
            .build()
            .await
            .expect("Failed to build client");

        assert_eq!(
            client.active_channel_count(),
            2,
            "Client should have exactly 2 channels configured"
        );
        assert!(
            client.channel_pool().default_channel().is_some(),
            "Client should have a default channel"
        );
        match client.channel_pool().config() {
            ChannelPoolConfig::Static(config) => {
                assert_eq!(
                    config.num_channels, 2,
                    "Configured static channel count should match"
                );
            }
            ChannelPoolConfig::Dynamic(_) => {
                panic!("Expected static pool config, got dynamic");
            }
        }
    }

    #[tokio_test_no_panics]
    async fn builder_with_dynamic_channel_pool_config() {
        let mock = MockSpanner::new();
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let dynamic_config = DynamicChannelPoolConfig::new()
            .with_initial_channels(3)
            .with_min_channels(2)
            .with_max_channels(8);

        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .with_channel_pool(dynamic_config)
            .build()
            .await
            .expect("Failed to build client");

        assert_eq!(
            client.active_channel_count(),
            3,
            "Client should have 3 initial channels configured"
        );
        match client.channel_pool().config() {
            ChannelPoolConfig::Dynamic(config) => {
                assert_eq!(config.initial_channels, 3, "Initial channels should match");
                assert_eq!(config.min_channels, 2, "Min channels should match");
                assert_eq!(config.max_channels, 8, "Max channels should match");
            }
            ChannelPoolConfig::Static(_) => {
                panic!("Expected dynamic pool config, got static");
            }
        }
    }

    #[tokio_test_no_panics]
    async fn builder_invalid_channel_pool_config_propagates_error() {
        // Case 1: Invalid static pool config
        let result = Spanner::builder()
            .with_endpoint("http://localhost:9010")
            .with_credentials(Anonymous::new().build())
            .with_channel_pool(StaticChannelPoolConfig { num_channels: 0 })
            .build()
            .await;

        assert!(
            result.is_err(),
            "Builder must propagate error when static channel pool validation fails"
        );

        // Case 2: Invalid dynamic pool config
        let invalid_dynamic = DynamicChannelPoolConfig {
            initial_channels: 0,
            ..Default::default()
        };
        let result = Spanner::builder()
            .with_endpoint("http://localhost:9010")
            .with_credentials(Anonymous::new().build())
            .with_channel_pool(invalid_dynamic)
            .build()
            .await;

        assert!(
            result.is_err(),
            "Builder must propagate error when dynamic channel pool validation fails"
        );

        // Case 3: Channel creation failure propagates error (e.g. invalid URI format)
        let result = Spanner::builder()
            .with_endpoint(":::invalid-uri")
            .with_credentials(Anonymous::new().build())
            .with_channel_pool(StaticChannelPoolConfig { num_channels: 1 })
            .build()
            .await;

        assert!(
            result.is_err(),
            "Builder must propagate error when channel creation fails"
        );
    }

    #[tokio_test_no_panics]
    async fn from_stub_creates_single_channel_static_pool() {
        use crate::stub::Spanner as SpannerStub;

        #[derive(Debug)]
        struct DummyStub;
        impl SpannerStub for DummyStub {}

        let client = Spanner::from_stub(DummyStub);

        assert_eq!(
            client.active_channel_count(),
            1,
            "Client from stub should have exactly 1 channel"
        );
        assert!(
            client.channel_pool().default_channel().is_some(),
            "Client from stub should have a default channel"
        );
        match client.channel_pool().config() {
            ChannelPoolConfig::Static(config) => {
                assert_eq!(
                    config.num_channels, 1,
                    "From stub should configure exactly 1 static channel"
                );
            }
            ChannelPoolConfig::Dynamic(_) => {
                panic!("Expected static pool config from stub");
            }
        }
    }

    #[tokio_test_no_panics]
    async fn resolve_affinity_binds_to_same_channel() {
        let mock = MockSpanner::new();
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        let client = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .with_channel_pool(StaticChannelPoolConfig::new(4))
            .build()
            .await
            .expect("Failed to build client");

        let affinity = TransactionAffinity::new_read_write();
        let lease1 = client.resolve_affinity(&affinity);
        let channel_id_1 = lease1.channel_id;
        drop(lease1);

        let lease2 = client.resolve_affinity(&affinity);
        let channel_id_2 = lease2.channel_id;
        drop(lease2);

        assert_eq!(
            channel_id_1, channel_id_2,
            "resolve_affinity should return the same channel for the same affinity"
        );
    }

    #[test]
    fn resolve_pool_config() {
        // Case 1: Default when no env var and no override
        let mut config = ClientConfig::default();
        let pool_config = resolve_pool_config_with(&mut config, false, || None, || None)
            .expect("default pool config should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig { num_channels: 4 }),
            "default static pool has 4 channels"
        );

        // Case 2: Emulator defaults to 1 channel when SPANNER_NUM_CHANNELS is not set
        let mut config = ClientConfig::default();
        let pool_config = resolve_pool_config_with(&mut config, true, || None, || None)
            .expect("emulator pool config should resolve to default 1 channel");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig { num_channels: 1 }),
            "emulator should default to 1 channel"
        );

        // Case 2b: SPANNER_NUM_CHANNELS overrides emulator default
        let mut config = ClientConfig::default();
        let pool_config =
            resolve_pool_config_with(&mut config, true, || Some("8".to_string()), || None)
                .expect("emulator pool config with env var override should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig { num_channels: 8 }),
            "SPANNER_NUM_CHANNELS should override emulator default"
        );

        // Case 3: SPANNER_NUM_CHANNELS valid integer
        let mut config = ClientConfig::default();
        let pool_config =
            resolve_pool_config_with(&mut config, false, || Some("2".to_string()), || None)
                .expect("pool config with SPANNER_NUM_CHANNELS=2 should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig { num_channels: 2 }),
            "configured static channels should be 2"
        );

        // Case 4: SPANNER_NUM_CHANNELS unparsable integer string
        let mut config = ClientConfig::default();
        let error = resolve_pool_config_with(
            &mut config,
            false,
            || Some("not_a_number".to_string()),
            || None,
        )
        .expect_err("should fail when SPANNER_NUM_CHANNELS is not a valid integer");
        let debug_error = format!("{error:?}");
        assert!(
            debug_error.contains("InvalidDigit"),
            "error should indicate invalid digit: {debug_error}"
        );

        // Case 5: SPANNER_NUM_CHANNELS zero (validation failure)
        let mut config = ClientConfig::default();
        let error = resolve_pool_config_with(&mut config, false, || Some("0".to_string()), || None)
            .expect_err("should fail when SPANNER_NUM_CHANNELS is 0");
        let debug_error = format!("{error:?}");
        assert!(
            debug_error.contains("num_channels must be at least 1"),
            "error should indicate num_channels must be at least 1: {debug_error}"
        );

        // Case 6: Extension override takes precedence over SPANNER_NUM_CHANNELS
        let mut config = ClientConfig::default();
        config
            .extensions
            .insert(ChannelPoolConfig::Static(StaticChannelPoolConfig {
                num_channels: 10,
            }));
        let pool_config =
            resolve_pool_config_with(&mut config, false, || Some("2".to_string()), || None)
                .expect("extension override should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig { num_channels: 10 }),
            "extension override takes precedence over env var"
        );

        // Case 7: Extension override takes precedence even if emulator is true
        let mut config = ClientConfig::default();
        config
            .extensions
            .insert(ChannelPoolConfig::Static(StaticChannelPoolConfig {
                num_channels: 8,
            }));
        let pool_config =
            resolve_pool_config_with(&mut config, true, || Some("2".to_string()), || None)
                .expect("extension override should resolve even on emulator");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig { num_channels: 8 }),
            "extension override takes precedence over emulator default"
        );

        // Case 8: SPANNER_NUM_CHANNELS empty or whitespace string falls back to default
        let mut config = ClientConfig::default();
        let pool_config =
            resolve_pool_config_with(&mut config, false, || Some("   ".to_string()), || None)
                .expect("whitespace SPANNER_NUM_CHANNELS should resolve to default");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig { num_channels: 4 }),
            "whitespace SPANNER_NUM_CHANNELS should default to 4 channels"
        );

        // Case 9: Extension override with dynamic channel pool configuration
        let mut config = ClientConfig::default();
        let dynamic_config = DynamicChannelPoolConfig::default();
        config
            .extensions
            .insert(ChannelPoolConfig::Dynamic(dynamic_config.clone()));
        let pool_config =
            resolve_pool_config_with(&mut config, false, || Some("2".to_string()), || None)
                .expect("dynamic extension override should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Dynamic(dynamic_config),
            "dynamic extension override takes precedence over env var"
        );

        // Case 10: Extension override with StaticChannelPoolConfig directly
        let mut config = ClientConfig::default();
        config
            .extensions
            .insert(StaticChannelPoolConfig { num_channels: 6 });
        let pool_config =
            resolve_pool_config_with(&mut config, false, || Some("2".to_string()), || None)
                .expect("static struct extension override should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig { num_channels: 6 }),
            "static struct extension override takes precedence over env var"
        );

        // Case 11: Extension override with DynamicChannelPoolConfig directly
        let mut config = ClientConfig::default();
        let dynamic_config = DynamicChannelPoolConfig::default();
        config.extensions.insert(dynamic_config.clone());
        let pool_config =
            resolve_pool_config_with(&mut config, false, || Some("2".to_string()), || None)
                .expect("dynamic struct extension override should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Dynamic(dynamic_config),
            "dynamic struct extension override takes precedence over env var"
        );

        // Case 12: Extension override with Arc<ChannelPoolConfig>
        let mut config = ClientConfig::default();
        config.extensions.insert(Arc::new(ChannelPoolConfig::Static(
            StaticChannelPoolConfig { num_channels: 7 },
        )));
        let pool_config =
            resolve_pool_config_with(&mut config, false, || Some("2".to_string()), || None)
                .expect("Arc<ChannelPoolConfig> extension override should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig { num_channels: 7 }),
            "Arc<ChannelPoolConfig> extension override takes precedence over env var"
        );

        // Case 13: Invalid StaticChannelPoolConfig in extensions fails validation
        let mut config = ClientConfig::default();
        config
            .extensions
            .insert(StaticChannelPoolConfig { num_channels: 0 });
        let error = resolve_pool_config_with(&mut config, false, || None, || None)
            .expect_err("should fail when StaticChannelPoolConfig in extensions is invalid");
        let debug_error = format!("{error:?}");
        assert!(
            debug_error.contains("num_channels must be at least 1"),
            "error should indicate invalid channels: {debug_error}"
        );

        // Case 14: Invalid DynamicChannelPoolConfig in extensions fails validation
        let mut config = ClientConfig::default();
        let invalid_dynamic = DynamicChannelPoolConfig {
            initial_channels: 0,
            ..Default::default()
        };
        config.extensions.insert(invalid_dynamic);
        let error = resolve_pool_config_with(&mut config, false, || None, || None)
            .expect_err("should fail when DynamicChannelPoolConfig in extensions is invalid");
        let debug_error = format!("{error:?}");
        assert!(
            debug_error.contains("initial_channels must be between min_channels and max_channels"),
            "error should indicate invalid initial_channels: {debug_error}"
        );

        // Case 15: Invalid Arc<ChannelPoolConfig> in extensions fails validation
        let mut config = ClientConfig::default();
        config.extensions.insert(Arc::new(ChannelPoolConfig::Static(
            StaticChannelPoolConfig { num_channels: 0 },
        )));
        let error = resolve_pool_config_with(&mut config, false, || None, || None)
            .expect_err("should fail when Arc<ChannelPoolConfig> in extensions is invalid");
        let debug_error = format!("{error:?}");
        assert!(
            debug_error.contains("num_channels must be at least 1"),
            "error should indicate invalid channels: {debug_error}"
        );

        // Case 16: SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=true resolves to default dynamic pool
        let mut config = ClientConfig::default();
        let pool_config =
            resolve_pool_config_with(&mut config, false, || None, || Some("true".to_string()))
                .expect("SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=true should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
            "SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=true should produce default dynamic pool"
        );

        // Case 17: SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL accepts truthy values ("1", "yes", "on", "t")
        for truthy in ["1", "yes", "on", "t", "TRUE", "Yes "] {
            let mut config = ClientConfig::default();
            let pool_config =
                resolve_pool_config_with(&mut config, false, || None, || Some(truthy.to_string()))
                    .expect("truthy value should resolve");
            assert_eq!(
                pool_config,
                ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
                "'{truthy}' must enable dynamic channel pool"
            );
        }

        // Case 18: SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=false falls back to default static pool
        let mut config = ClientConfig::default();
        let pool_config =
            resolve_pool_config_with(&mut config, false, || None, || Some("false".to_string()))
                .expect("SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=false should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig::default()),
            "SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=false should fall back to static pool"
        );

        // Case 19: Programmatic static configuration takes precedence over SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=true
        let mut config = ClientConfig::default();
        config.extensions.insert(StaticChannelPoolConfig::new(2));
        let pool_config =
            resolve_pool_config_with(&mut config, false, || None, || Some("true".to_string()))
                .expect("programmatic override should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Static(StaticChannelPoolConfig { num_channels: 2 }),
            "programmatic static config takes precedence over SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL"
        );

        // Case 20: Programmatic dynamic configuration takes precedence over SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=false
        let mut config = ClientConfig::default();
        let custom_dynamic = DynamicChannelPoolConfig::new().with_min_channels(2);
        config.extensions.insert(custom_dynamic.clone());
        let pool_config =
            resolve_pool_config_with(&mut config, false, || None, || Some("false".to_string()))
                .expect("programmatic dynamic override should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Dynamic(custom_dynamic),
            "programmatic dynamic config takes precedence over SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL"
        );

        // Case 21: SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=true with SPANNER_NUM_CHANNELS=8
        let mut config = ClientConfig::default();
        let pool_config = resolve_pool_config_with(
            &mut config,
            false,
            || Some("8".to_string()),
            || Some("true".to_string()),
        )
        .expect("dynamic pool with custom channels should resolve");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Dynamic(
                DynamicChannelPoolConfig::default()
                    .with_initial_channels(8)
                    .with_min_channels(8)
            ),
            "SPANNER_NUM_CHANNELS sets initial and min channels when dynamic pool is enabled"
        );

        // Case 22: SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=true with SPANNER_NUM_CHANNELS > max_channels (e.g. 300) fails validation
        let mut config = ClientConfig::default();
        let error = resolve_pool_config_with(
            &mut config,
            false,
            || Some("300".to_string()),
            || Some("true".to_string()),
        )
        .expect_err("should fail when SPANNER_NUM_CHANNELS exceeds maximum supported limit");
        let debug_error = format!("{error:?}");
        assert!(
            debug_error.contains("max_channels cannot exceed maximum supported limit")
                || debug_error.contains("min_channels"),
            "error should indicate max_channels limit exceeded: {debug_error}"
        );

        // Case 23: SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=true with invalid SPANNER_NUM_CHANNELS format
        let mut config = ClientConfig::default();
        let error = resolve_pool_config_with(
            &mut config,
            false,
            || Some("invalid_number".to_string()),
            || Some("true".to_string()),
        )
        .expect_err("should fail when SPANNER_NUM_CHANNELS is not a valid integer");
        let debug_error = format!("{error:?}");
        assert!(
            debug_error.contains("InvalidDigit"),
            "error should indicate invalid integer format: {debug_error}"
        );

        // Case 24: SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=true with whitespace SPANNER_NUM_CHANNELS
        let mut config = ClientConfig::default();
        let pool_config = resolve_pool_config_with(
            &mut config,
            false,
            || Some("   ".to_string()),
            || Some("true".to_string()),
        )
        .expect("whitespace SPANNER_NUM_CHANNELS should be ignored");
        assert_eq!(
            pool_config,
            ChannelPoolConfig::Dynamic(DynamicChannelPoolConfig::default()),
            "whitespace SPANNER_NUM_CHANNELS must fall back to default dynamic pool"
        );

        // Case 25: SPANNER_ENABLE_DYNAMIC_CHANNEL_POOL=true with SPANNER_NUM_CHANNELS=0 fails validation
        let mut config = ClientConfig::default();
        let error = resolve_pool_config_with(
            &mut config,
            false,
            || Some("0".to_string()),
            || Some("true".to_string()),
        )
        .expect_err("should fail when dynamic channels configured as 0");
        let debug_error = format!("{error:?}");
        assert!(
            debug_error.contains("min_channels must be at least 1"),
            "error should indicate invalid channels for dynamic pool: {debug_error}"
        );
    }

    #[tokio_test_no_panics]
    async fn client_builder_with_extension_configures_channel_pool() {
        let mock = MockSpanner::new();
        let (address, _server) = start("0.0.0.0:0", mock)
            .await
            .expect("Failed to start mock server");

        // Case A: using with_extension directly with ChannelPoolConfig
        let spanner = Spanner::builder()
            .with_endpoint(address.clone())
            .with_credentials(Anonymous::new().build())
            .with_extension(ChannelPoolConfig::Static(StaticChannelPoolConfig {
                num_channels: 3,
            }))
            .build()
            .await
            .expect("build client with with_extension override");

        assert_eq!(
            spanner.channel_pool.active_channel_count(),
            3,
            "Channel pool must have 3 channels from with_extension override"
        );

        // Case B: using with_channel_pool helper
        let spanner = Spanner::builder()
            .with_endpoint(address.clone())
            .with_credentials(Anonymous::new().build())
            .with_channel_pool(StaticChannelPoolConfig { num_channels: 2 })
            .build()
            .await
            .expect("build client with with_channel_pool override");

        assert_eq!(
            spanner.channel_pool.active_channel_count(),
            2,
            "Channel pool must have 2 channels from with_channel_pool override"
        );

        // Case C: using with_extension with StaticChannelPoolConfig directly
        let spanner = Spanner::builder()
            .with_endpoint(address.clone())
            .with_credentials(Anonymous::new().build())
            .with_extension(StaticChannelPoolConfig { num_channels: 3 })
            .build()
            .await
            .expect("build client with StaticChannelPoolConfig extension");

        assert_eq!(
            spanner.channel_pool.active_channel_count(),
            3,
            "Channel pool must have 3 channels from StaticChannelPoolConfig extension"
        );

        // Case D: using with_extension with DynamicChannelPoolConfig directly
        let dynamic_config = DynamicChannelPoolConfig::new()
            .with_initial_channels(2)
            .with_min_channels(2)
            .with_max_channels(4);
        let spanner = Spanner::builder()
            .with_endpoint(address.clone())
            .with_credentials(Anonymous::new().build())
            .with_extension(dynamic_config)
            .build()
            .await
            .expect("build client with DynamicChannelPoolConfig extension");

        assert_eq!(
            spanner.channel_pool.active_channel_count(),
            2,
            "Channel pool must have 2 channels from DynamicChannelPoolConfig extension"
        );

        // Case E: using with_extension with Arc<ChannelPoolConfig>
        let spanner = Spanner::builder()
            .with_endpoint(address)
            .with_credentials(Anonymous::new().build())
            .with_extension(Arc::new(ChannelPoolConfig::Static(
                StaticChannelPoolConfig { num_channels: 3 },
            )))
            .build()
            .await
            .expect("build client with Arc<ChannelPoolConfig> extension");

        assert_eq!(
            spanner.channel_pool.active_channel_count(),
            3,
            "Channel pool must have 3 channels from Arc<ChannelPoolConfig> extension"
        );
    }
}
