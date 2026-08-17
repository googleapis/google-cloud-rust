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

use crate::omni::InstanceType;
use gaxi::options::ClientConfig;
use google_cloud_gax::error::Error;
use http::HeaderMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "_experimental-builtin-metrics")]
use {
    crate::observability::exporter::GcpMonitoringExporter,
    gaxi::attempt_interceptor::AttemptInterceptor,
    google_cloud_gax::options::RequestOptions,
    google_cloud_monitoring_v3::client::MetricService,
    http::header::{HeaderName, HeaderValue},
    opentelemetry::KeyValue,
    opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider},
    opentelemetry_sdk::{
        Resource,
        error::OTelSdkError,
        metrics::{PeriodicReader, SdkMeterProvider},
    },
    std::borrow::Cow,
    std::sync::LazyLock,
    std::time::Instant,
};

#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) const DEFAULT_EXPORT_INTERVAL: Duration = Duration::from_secs(60);

#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) const BUCKET_BOUNDARIES: [f64; 50] = [
    0.0, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0,
    16.0, 17.0, 18.0, 19.0, 20.0, 25.0, 30.0, 40.0, 50.0, 65.0, 80.0, 100.0, 130.0, 160.0, 200.0,
    250.0, 300.0, 400.0, 500.0, 650.0, 800.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0, 50000.0,
    100000.0, 200000.0, 400000.0, 800000.0, 1600000.0, 3200000.0,
];

#[cfg(feature = "_experimental-builtin-metrics")]
#[derive(Debug)]
pub(crate) struct SpannerMetrics {
    pub(crate) operation_latencies: Histogram<f64>,
    pub(crate) attempt_latencies: Histogram<f64>,
    pub(crate) gfe_latencies: Histogram<f64>,
    pub(crate) afe_latencies: Histogram<f64>,
    pub(crate) operation_count: Counter<u64>,
    pub(crate) attempt_count: Counter<u64>,
    pub(crate) gfe_connectivity_error_count: Counter<u64>,
    #[allow(dead_code)]
    pub(crate) afe_connectivity_error_count: Counter<u64>,
}

#[cfg(feature = "_experimental-builtin-metrics")]
impl SpannerMetrics {
    pub(crate) fn new(meter: Meter) -> Self {
        Self {
            operation_latencies: meter
                .f64_histogram("spanner.googleapis.com/internal/client/operation_latencies")
                .with_unit("ms")
                .with_boundaries(BUCKET_BOUNDARIES.to_vec())
                .build(),
            attempt_latencies: meter
                .f64_histogram("spanner.googleapis.com/internal/client/attempt_latencies")
                .with_unit("ms")
                .with_boundaries(BUCKET_BOUNDARIES.to_vec())
                .build(),
            gfe_latencies: meter
                .f64_histogram("spanner.googleapis.com/internal/client/gfe_latencies")
                .with_unit("ms")
                .with_boundaries(BUCKET_BOUNDARIES.to_vec())
                .build(),
            afe_latencies: meter
                .f64_histogram("spanner.googleapis.com/internal/client/afe_latencies")
                .with_unit("ms")
                .with_boundaries(BUCKET_BOUNDARIES.to_vec())
                .build(),
            operation_count: meter
                .u64_counter("spanner.googleapis.com/internal/client/operation_count")
                .build(),
            attempt_count: meter
                .u64_counter("spanner.googleapis.com/internal/client/attempt_count")
                .build(),
            gfe_connectivity_error_count: meter
                .u64_counter("spanner.googleapis.com/internal/client/gfe_connectivity_error_count")
                .build(),
            afe_connectivity_error_count: meter
                .u64_counter("spanner.googleapis.com/internal/client/afe_connectivity_error_count")
                .build(),
        }
    }
}

/// Parses `projects/{project}/instances/{instance}/databases/{database}` into its
/// `(project_id, instance_id, database_id)` components.
#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) fn parse_database_name(database_name: &str) -> Option<(&str, &str, &str)> {
    let mut parts = database_name.split('/');
    if parts.next() != Some("projects") {
        return None;
    }
    let project = parts.next()?;
    if parts.next() != Some("instances") {
        return None;
    }
    let instance = parts.next()?;
    if parts.next() != Some("databases") {
        return None;
    }
    let database = parts.next()?;
    if parts.next().is_some() || project.is_empty() || instance.is_empty() || database.is_empty() {
        return None;
    }
    Some((project, instance, database))
}

/// Generates a unique identifier for the `client_uid` metric attribute in the format
/// `UUID@PID@hostname`.
#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) fn generate_client_uid() -> String {
    let uuid = uuid::Uuid::new_v4().to_string();
    let pid = std::process::id();
    let hostname = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "localhost".to_string());
    format!("{uuid}@{pid}@{hostname}")
}

/// Generates a 6-character zero-padded lowercase hexadecimal hash for the `client_hash`
/// resource label using the 24 least significant bits of an FNV-1a 64-bit hash of `client_uid`.
#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) fn generate_client_hash(client_uid: &str) -> String {
    if client_uid.is_empty() {
        return "000000".to_string();
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in client_uid.as_bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let hash_24 = hash & 0xff_ffff;
    format!("{hash_24:06x}")
}

/// Returns the library client identification string (`"spanner-rust/<VERSION>"`).
#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) fn client_name() -> &'static str {
    concat!("spanner-rust/", env!("CARGO_PKG_VERSION"))
}

#[cfg(feature = "_experimental-builtin-metrics")]
#[derive(Clone, Debug)]
pub(crate) struct Observability {
    pub(crate) metrics: Option<Arc<SpannerMetrics>>,
    pub(crate) common_attributes: [KeyValue; 3],
    pub(crate) meter_provider: Option<Arc<SdkMeterProvider>>,
}

#[cfg(feature = "_experimental-builtin-metrics")]
impl Observability {
    pub(crate) fn disabled() -> Self {
        Self {
            metrics: None,
            common_attributes: [
                KeyValue::new("client_uid", ""),
                KeyValue::new("client_name", ""),
                KeyValue::new("database", ""),
            ],
            meter_provider: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn disabled_arc() -> Arc<Self> {
        Arc::new(Self::disabled())
    }

    pub(crate) async fn init(
        config: &ClientConfig,
        instance_type: InstanceType,
        database_name: &str,
        is_emulator: bool,
    ) -> Self {
        let disable_builtin_metrics = std::env::var("SPANNER_DISABLE_BUILTIN_METRICS")
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
            .unwrap_or(false);
        let is_plaintext = config
            .endpoint
            .as_ref()
            .is_some_and(|ep| crate::omni::is_plaintext_endpoint(ep));
        if disable_builtin_metrics
            || instance_type == InstanceType::Omni
            || is_emulator
            || is_plaintext
        {
            return Self::disabled();
        }

        let (project_id, instance_id, database_id) = match parse_database_name(database_name) {
            Some(parts) => parts,
            None => return Self::disabled(),
        };

        // Create the Google Cloud Monitoring client using the same config
        let mut builder = MetricService::builder();

        if let Some(ref cred) = config.cred {
            builder = builder.with_credentials(cred.clone());
        }
        if let Some(ref universe_domain) = config.universe_domain {
            builder = builder.with_universe_domain(universe_domain.clone());
        }

        let monitoring_client = match builder.build().await {
            Ok(monitoring_client) => monitoring_client,
            Err(error) => {
                tracing::warn!(
                    "Failed to initialize Google Cloud Monitoring client for Spanner metrics: {:?}",
                    error
                );
                return Self::disabled();
            }
        };

        let exporter = GcpMonitoringExporter::new(monitoring_client, project_id);

        let client_uid = generate_client_uid();
        let client_hash = generate_client_hash(&client_uid);
        let client_name = client_name();

        let resource = Resource::builder()
            .with_attributes([
                KeyValue::new("project_id", project_id.to_string()),
                KeyValue::new("instance_id", instance_id.to_string()),
                KeyValue::new("location", "global"),
                KeyValue::new("instance_config", "unknown"),
                KeyValue::new("client_hash", client_hash),
            ])
            .build();

        // Set up PeriodicReader with a 60-second export interval.
        let reader = PeriodicReader::builder(exporter)
            .with_interval(DEFAULT_EXPORT_INTERVAL)
            .build();

        let meter_provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();

        let meter = meter_provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);

        let common_attributes = [
            KeyValue::new("client_uid", client_uid),
            KeyValue::new("client_name", client_name),
            KeyValue::new("database", database_id.to_string()),
        ];

        Self {
            metrics: Some(Arc::new(metrics)),
            common_attributes,
            meter_provider: Some(Arc::new(meter_provider)),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(metrics: SpannerMetrics, meter_provider: SdkMeterProvider) -> Self {
        Self {
            metrics: Some(Arc::new(metrics)),
            common_attributes: [
                KeyValue::new("client_uid", "test-uid"),
                KeyValue::new("client_name", "test-name"),
                KeyValue::new("database", "test-db"),
            ],
            meter_provider: Some(Arc::new(meter_provider)),
        }
    }

    /// Traces a client operation and records operation metrics.
    pub(crate) async fn trace_operation<Fut, T>(
        &self,
        method: &'static str,
        fut: Fut,
    ) -> crate::Result<T>
    where
        Fut: Future<Output = crate::Result<T>>,
    {
        if self.metrics.is_none() {
            return fut.await;
        }
        let start_time = Instant::now();
        let result = fut.await;
        let elapsed = start_time.elapsed();
        self.record_operation(method, elapsed, result.as_ref().err());
        result
    }

    pub(crate) fn record_operation(
        &self,
        method: &'static str,
        duration: Duration,
        error: Option<&Error>,
    ) {
        let Some(ref metrics) = self.metrics else {
            return;
        };

        let status = error_to_status_str(error);
        let method_name = normalize_method_name(method);
        let attributes = [
            KeyValue::new("method", method_name),
            KeyValue::new("status", status),
            KeyValue::new("directpath_enabled", "false"),
            self.common_attributes[0].clone(),
            self.common_attributes[1].clone(),
            self.common_attributes[2].clone(),
        ];

        metrics
            .operation_latencies
            .record(duration.as_secs_f64() * 1000.0, &attributes);
        metrics.operation_count.add(1, &attributes);
    }

    /// Records metrics for a single RPC attempt, including attempt latency, attempt count,
    /// and server-timing metrics (GFE and AFE latency / connectivity errors) extracted from headers.
    pub(crate) fn record_attempt(
        &self,
        method: &str,
        duration: Duration,
        error: Option<&Error>,
        headers: Option<&HeaderMap>,
    ) {
        let Some(ref metrics) = self.metrics else {
            return;
        };

        let timings = headers.map_or_else(ServerTimings::default, parse_server_timing_from_headers);
        let status = error_to_status_str(error);
        let method_name = normalize_method_name(method);
        let attributes = [
            KeyValue::new("method", method_name),
            KeyValue::new("status", status),
            KeyValue::new("directpath_enabled", "false"),
            KeyValue::new("directpath_used", "false"),
            self.common_attributes[0].clone(),
            self.common_attributes[1].clone(),
            self.common_attributes[2].clone(),
        ];

        metrics
            .attempt_latencies
            .record(duration.as_secs_f64() * 1000.0, &attributes);
        metrics.attempt_count.add(1, &attributes);

        // DirectPath is not used; record GFE latency or connectivity error counter
        if let Some(gfe) = timings.gfe_latency {
            metrics.gfe_latencies.record(gfe, &attributes);
        } else {
            metrics.gfe_connectivity_error_count.add(1, &attributes);
        }
        if let Some(afe) = timings.afe_latency {
            metrics.afe_latencies.record(afe, &attributes);
        }
    }

    pub(crate) fn shutdown(&self) {
        if let Some(ref provider) = self.meter_provider
            && let Err(err) = provider.shutdown()
            && !matches!(err, OTelSdkError::AlreadyShutdown)
        {
            tracing::warn!(
                "Error shutting down OpenTelemetry SdkMeterProvider: {:?}",
                err
            );
        }
    }
}

#[cfg(feature = "_experimental-builtin-metrics")]
impl Drop for Observability {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) const AFE_SERVER_TIMING_HEADER: &str = "x-goog-spanner-enable-afe-server-timing";

#[cfg(feature = "_experimental-builtin-metrics")]
static AFE_SERVER_TIMING_ENABLED: LazyLock<bool> = LazyLock::new(|| {
    !std::env::var("SPANNER_DISABLE_AFE_SERVER_TIMING")
        .map(|val| val.eq_ignore_ascii_case("true") || val == "1")
        .unwrap_or(false)
});

#[cfg(feature = "_experimental-builtin-metrics")]
#[inline]
fn is_afe_server_timing_enabled() -> bool {
    *AFE_SERVER_TIMING_ENABLED
}

#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) fn normalize_method_name(method: &str) -> Cow<'static, str> {
    let trimmed = method.trim_start_matches('/');
    let clean = if let Some(suffix) = trimmed.strip_prefix("google.spanner.v1.") {
        suffix
    } else if let Some(suffix) = trimmed.strip_prefix("Spanner.") {
        suffix
    } else {
        trimmed
    };

    match clean {
        "CreateSession" | "Spanner/CreateSession" => Cow::Borrowed("Spanner.CreateSession"),
        "BatchCreateSessions" | "Spanner/BatchCreateSessions" => {
            Cow::Borrowed("Spanner.BatchCreateSessions")
        }
        "GetSession" | "Spanner/GetSession" => Cow::Borrowed("Spanner.GetSession"),
        "ListSessions" | "Spanner/ListSessions" => Cow::Borrowed("Spanner.ListSessions"),
        "DeleteSession" | "Spanner/DeleteSession" => Cow::Borrowed("Spanner.DeleteSession"),
        "ExecuteSql" | "Spanner/ExecuteSql" => Cow::Borrowed("Spanner.ExecuteSql"),
        "ExecuteStreamingSql" | "Spanner/ExecuteStreamingSql" => {
            Cow::Borrowed("Spanner.ExecuteStreamingSql")
        }
        "ExecuteBatchDml" | "Spanner/ExecuteBatchDml" => Cow::Borrowed("Spanner.ExecuteBatchDml"),
        "Read" | "Spanner/Read" => Cow::Borrowed("Spanner.Read"),
        "StreamingRead" | "Spanner/StreamingRead" => Cow::Borrowed("Spanner.StreamingRead"),
        "BeginTransaction" | "Spanner/BeginTransaction" => {
            Cow::Borrowed("Spanner.BeginTransaction")
        }
        "Commit" | "Spanner/Commit" => Cow::Borrowed("Spanner.Commit"),
        "Rollback" | "Spanner/Rollback" => Cow::Borrowed("Spanner.Rollback"),
        "PartitionQuery" | "Spanner/PartitionQuery" => Cow::Borrowed("Spanner.PartitionQuery"),
        "PartitionRead" | "Spanner/PartitionRead" => Cow::Borrowed("Spanner.PartitionRead"),
        "BatchWrite" | "Spanner/BatchWrite" => Cow::Borrowed("Spanner.BatchWrite"),
        _ => {
            let suffix = clean.strip_prefix("Spanner/").unwrap_or(clean);
            Cow::Owned(format!("Spanner.{}", suffix.replace('/', ".")))
        }
    }
}

#[cfg(feature = "_experimental-builtin-metrics")]
fn error_to_status_str(error: Option<&Error>) -> &'static str {
    error.map_or("OK", |e| {
        e.status().map_or("UNKNOWN", |status| status.code.name())
    })
}

#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) fn parse_server_timing_from_headers(headers: &HeaderMap) -> ServerTimings {
    let mut timings = ServerTimings::default();
    for header_value in headers.get_all("server-timing") {
        let Ok(header_str) = header_value.to_str() else {
            continue;
        };
        let parsed = parse_server_timing(header_str);
        timings.gfe_latency = timings.gfe_latency.or(parsed.gfe_latency);
        timings.afe_latency = timings.afe_latency.or(parsed.afe_latency);
    }
    timings
}

#[derive(Debug, Default, Clone)]
#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) struct SpannerMetricsInterceptor;

#[cfg(feature = "_experimental-builtin-metrics")]
impl AttemptInterceptor for SpannerMetricsInterceptor {
    fn intercept(&self, headers: &mut HeaderMap, _attempt: u32) {
        if is_afe_server_timing_enabled() {
            headers.insert(
                HeaderName::from_static(AFE_SERVER_TIMING_HEADER),
                HeaderValue::from_static("true"),
            );
        }
    }

    fn on_attempt_complete(
        &self,
        method: &str,
        _attempt: u32,
        start_time: Instant,
        response_headers: Option<&HeaderMap>,
        error: Option<&Error>,
        options: &RequestOptions,
    ) {
        use google_cloud_gax::options::internal::RequestOptionsExt as _;
        if let Some(o11y) = options.get_extension::<Arc<Observability>>() {
            let duration = start_time.elapsed();
            o11y.record_attempt(method, duration, error, response_headers);
        }
    }
}

#[cfg(feature = "_experimental-builtin-metrics")]
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ServerTimings {
    pub(crate) gfe_latency: Option<f64>,
    pub(crate) afe_latency: Option<f64>,
}

#[cfg(feature = "_experimental-builtin-metrics")]
pub(crate) fn parse_server_timing(header_val: &str) -> ServerTimings {
    let mut timings = ServerTimings::default();
    for part in header_val.split(',') {
        let mut subparts = part.split(';');
        let Some(name) = subparts.next().map(str::trim) else {
            continue;
        };
        let is_gfe = name.eq_ignore_ascii_case("gfet4t7");
        let is_afe = name.eq_ignore_ascii_case("afe");
        if !is_gfe && !is_afe {
            continue;
        }
        if let Some(duration) = subparts.find_map(parse_duration_param) {
            if is_gfe {
                timings.gfe_latency = timings.gfe_latency.or(Some(duration));
            }
            if is_afe {
                timings.afe_latency = timings.afe_latency.or(Some(duration));
            }
        }
    }
    timings
}

#[cfg(feature = "_experimental-builtin-metrics")]
fn parse_duration_param(param: &str) -> Option<f64> {
    let (key, value) = param.split_once('=')?;
    if !key.trim().eq_ignore_ascii_case("dur") {
        return None;
    }
    value
        .trim()
        .trim_matches('"')
        .parse::<f64>()
        .ok()
        .filter(|duration| *duration >= 0.0 && duration.is_finite())
}

#[cfg(not(feature = "_experimental-builtin-metrics"))]
#[derive(Clone, Debug, Default)]
pub(crate) struct Observability;

#[cfg(not(feature = "_experimental-builtin-metrics"))]
impl Observability {
    pub(crate) fn disabled() -> Self {
        Self
    }

    #[allow(dead_code)]
    pub(crate) fn disabled_arc() -> Arc<Self> {
        Arc::new(Self::disabled())
    }

    pub(crate) async fn init(
        _config: &ClientConfig,
        _instance_type: InstanceType,
        _database_name: &str,
        _is_emulator: bool,
    ) -> Self {
        Self
    }

    #[inline(always)]
    pub(crate) async fn trace_operation<Fut, T>(
        &self,
        _method: &'static str,
        fut: Fut,
    ) -> crate::Result<T>
    where
        Fut: Future<Output = crate::Result<T>>,
    {
        fut.await
    }

    /// No-op stub implementation when the `_experimental-builtin-metrics` feature is disabled.
    ///
    /// This allows interceptors to call `record_attempt` unconditionally without sprinkling
    /// `#[cfg(feature = "_experimental-builtin-metrics")]` across call sites.
    #[inline(always)]
    pub(crate) fn record_attempt(
        &self,
        _method: &str,
        _duration: Duration,
        _error: Option<&Error>,
        _headers: Option<&HeaderMap>,
    ) {
    }

    /// No-op stub implementation when the `_experimental-builtin-metrics` feature is disabled.
    ///
    /// This allows client operations to call `record_operation` unconditionally without sprinkling
    /// `#[cfg(feature = "_experimental-builtin-metrics")]` across call sites.
    #[inline(always)]
    pub(crate) fn record_operation(
        &self,
        _method: &'static str,
        _duration: Duration,
        _error: Option<&Error>,
    ) {
    }

    #[allow(dead_code)]
    pub(crate) fn shutdown(&self) {}
}

#[cfg(not(feature = "_experimental-builtin-metrics"))]
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub(crate) struct SpannerMetricsInterceptor;

#[cfg(not(feature = "_experimental-builtin-metrics"))]
impl gaxi::attempt_interceptor::AttemptInterceptor for SpannerMetricsInterceptor {}

#[cfg(all(test, not(feature = "_experimental-builtin-metrics")))]
mod disabled_tests {
    use super::*;

    #[tokio::test]
    async fn disabled_stubs_exercise() {
        let o11y = Observability::disabled();
        let _o11y_arc = Observability::disabled_arc();
        let initialized = Observability::init(
            &ClientConfig::default(),
            InstanceType::Cloud,
            "projects/p/instances/i/databases/d",
            false,
        )
        .await;
        initialized.record_attempt("ExecuteSql", Duration::from_millis(10), None, None);
        initialized.record_operation("ExecuteSql", Duration::from_millis(10), None);
        let res = initialized
            .trace_operation("ExecuteSql", async { Ok::<_, crate::Error>(42) })
            .await
            .expect("trace_operation should succeed");
        assert_eq!(res, 42);
        o11y.shutdown();
    }
}

#[cfg(all(test, feature = "_experimental-builtin-metrics"))]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::{Code, Status};
    use http::HeaderValue;
    use opentelemetry_sdk::metrics::InMemoryMetricExporter;
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use std::collections::HashMap;
    use std::fmt::Debug;

    #[test]
    fn traits() {
        static_assertions::assert_impl_all!(Observability: Send, Sync, Debug, Clone);
        static_assertions::assert_impl_all!(SpannerMetrics: Send, Sync, Debug);
        static_assertions::assert_impl_all!(ServerTimings: Send, Sync, Debug, PartialEq, Default);
        static_assertions::assert_impl_all!(SpannerMetricsInterceptor: Send, Sync, Debug, Clone, Default);
    }

    #[test]
    fn normalize_method_names() {
        assert_eq!(
            normalize_method_name("/google.spanner.v1.Spanner/CreateSession"),
            "Spanner.CreateSession"
        );
        assert_eq!(
            normalize_method_name("google.spanner.v1.Spanner/BatchCreateSessions"),
            "Spanner.BatchCreateSessions"
        );
        assert_eq!(
            normalize_method_name("google.spanner.v1.Spanner/ExecuteBatchDml"),
            "Spanner.ExecuteBatchDml"
        );
        assert_eq!(
            normalize_method_name("Spanner.BeginTransaction"),
            "Spanner.BeginTransaction"
        );
        assert_eq!(normalize_method_name("Commit"), "Spanner.Commit");
        assert_eq!(normalize_method_name("Rollback"), "Spanner.Rollback");
        assert_eq!(
            normalize_method_name("PartitionQuery"),
            "Spanner.PartitionQuery"
        );
        assert_eq!(
            normalize_method_name("PartitionRead"),
            "Spanner.PartitionRead"
        );
        assert_eq!(normalize_method_name("BatchWrite"), "Spanner.BatchWrite");
        assert_eq!(
            normalize_method_name("ExecuteStreamingSql"),
            "Spanner.ExecuteStreamingSql"
        );
        assert_eq!(normalize_method_name("Read"), "Spanner.Read");
        assert_eq!(
            normalize_method_name("StreamingRead"),
            "Spanner.StreamingRead"
        );
        assert_eq!(
            normalize_method_name("DeleteSession"),
            "Spanner.DeleteSession"
        );
        assert_eq!(normalize_method_name("GetSession"), "Spanner.GetSession");
        assert_eq!(
            normalize_method_name("ListSessions"),
            "Spanner.ListSessions"
        );
        assert_eq!(
            normalize_method_name("CustomOperation"),
            "Spanner.CustomOperation"
        );
        assert_eq!(
            normalize_method_name("Spanner/CustomOp"),
            "Spanner.CustomOp"
        );
        assert_eq!(
            normalize_method_name("Spanner/Sub/Method"),
            "Spanner.Sub.Method"
        );
        assert_eq!(
            normalize_method_name("/google.spanner.v1.Spanner/CustomOp"),
            "Spanner.CustomOp"
        );
        assert_eq!(
            normalize_method_name("google.spanner.v1.Spanner/CustomOp"),
            "Spanner.CustomOp"
        );
        assert_eq!(
            normalize_method_name("Spanner.CustomOp"),
            "Spanner.CustomOp"
        );
        assert_eq!(
            normalize_method_name("Spanner.Sub/Method"),
            "Spanner.Sub.Method"
        );
        assert_eq!(
            normalize_method_name("google.spanner.v1.Spanner/Sub/Method"),
            "Spanner.Sub.Method"
        );
    }

    #[test]
    fn observability_disabled() {
        let o11y = Observability::disabled();
        assert!(o11y.metrics.is_none());
        let o11y_arc = Observability::disabled_arc();
        assert!(o11y_arc.metrics.is_none());

        o11y.record_operation("ExecuteSql", Duration::from_millis(10), None);
        o11y.record_attempt("ExecuteSql", Duration::from_millis(10), None, None);
        o11y.shutdown();
    }

    #[test]
    fn spanner_metrics_interceptor_intercept_sets_header() {
        let interceptor = SpannerMetricsInterceptor;
        let mut headers = HeaderMap::new();
        interceptor.intercept(&mut headers, 1);
        assert_eq!(
            headers
                .get(AFE_SERVER_TIMING_HEADER)
                .map(|v| v.to_str().expect("valid ascii")),
            Some("true")
        );
    }

    #[test]
    fn error_to_status_str_conversions() {
        assert_eq!(error_to_status_str(None), "OK");

        let status_pd = Status::default().set_code(Code::PermissionDenied);
        let err_pd = Error::service(status_pd);
        assert_eq!(error_to_status_str(Some(&err_pd)), "PERMISSION_DENIED");

        let status_nf = Status::default().set_code(Code::NotFound);
        let err_nf = Error::service(status_nf);
        assert_eq!(error_to_status_str(Some(&err_nf)), "NOT_FOUND");

        let err_timeout = Error::timeout("simulated timeout");
        assert_eq!(error_to_status_str(Some(&err_timeout)), "UNKNOWN");
    }

    #[test]
    fn spanner_metrics_record_operation_and_attempt() {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);
        let o11y = Observability {
            metrics: Some(Arc::new(metrics)),
            common_attributes: [
                KeyValue::new("client_uid", ""),
                KeyValue::new("client_name", ""),
                KeyValue::new("database", ""),
            ],
            meter_provider: Some(Arc::new(provider.clone())),
        };

        o11y.record_operation("ExecuteSql", Duration::from_millis(50), None);
        let mut headers = HeaderMap::new();
        headers.insert(
            "server-timing",
            HeaderValue::from_static("gfet4t7;dur=12.5,afe;dur=5.0"),
        );
        o11y.record_attempt(
            "ExecuteSql",
            Duration::from_millis(40),
            None,
            Some(&headers),
        );

        provider.force_flush().expect("force_flush failed");

        let finished = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics");
        assert!(!finished.is_empty());
    }

    #[tokio::test]
    async fn trace_operation_success() {
        let o11y = Observability::disabled();
        let result = o11y
            .trace_operation("ExecuteSql", async { Ok::<i32, crate::Error>(42) })
            .await;
        assert_eq!(result.expect("trace_operation result"), 42);
    }

    #[test]
    fn parse_database_name_valid() {
        let parsed = parse_database_name("projects/proj-123/instances/inst-456/databases/db-789");
        assert_eq!(parsed, Some(("proj-123", "inst-456", "db-789")));
    }

    #[test]
    fn parse_database_name_invalid() {
        assert_eq!(parse_database_name("projects/proj/instances/inst"), None);
        assert_eq!(
            parse_database_name("projects/proj/instances/inst/databases"),
            None
        );
        assert_eq!(
            parse_database_name("projects/proj/instances/inst/databases/db/extra"),
            None
        );
        assert_eq!(
            parse_database_name("projects//instances/inst/databases/db"),
            None
        );
        assert_eq!(
            parse_database_name("projects/proj/instances//databases/db"),
            None
        );
        assert_eq!(
            parse_database_name("projects/proj/instances/inst/databases/"),
            None
        );
        assert_eq!(parse_database_name("invalid/string"), None);
    }

    #[test]
    fn generate_client_hash_known_values() {
        assert_eq!(generate_client_hash(""), "000000");

        let hash1 = generate_client_hash("test-client-uid");
        assert_eq!(hash1, "416874");
        assert_eq!(hash1.len(), 6);
        assert!(
            hash1
                .chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
            "hash must be 6 lowercase hex characters, got {hash1}"
        );

        let hash2 = generate_client_hash("test-client-uid");
        assert_eq!(hash1, hash2, "client hash must be deterministic");

        assert_eq!(generate_client_hash("spanner"), "727a8e");
    }

    #[test]
    fn generate_client_uid_format() {
        let uid1 = generate_client_uid();
        let parts: Vec<&str> = uid1.split('@').collect();
        assert_eq!(
            parts.len(),
            3,
            "expected UUID@PID@hostname format, got {uid1}"
        );
        assert_eq!(parts[0].len(), 36, "expected 36-char UUID prefix");

        let uid2 = generate_client_uid();
        assert_ne!(
            uid1, uid2,
            "each generated client_uid must have a unique UUID"
        );
    }

    #[test]
    fn client_name_format() {
        let name = client_name();
        assert!(
            name.starts_with("spanner-rust/"),
            "expected prefix 'spanner-rust/', got {name}"
        );
        assert!(name.len() > "spanner-rust/".len());
    }

    #[test]
    fn parse_server_timing_header_values() {
        assert_eq!(
            parse_server_timing("gfet4t7;dur=12.5"),
            ServerTimings {
                gfe_latency: Some(12.5),
                afe_latency: None,
            }
        );
        assert_eq!(
            parse_server_timing("gfet4t7;desc=\"test\";dur=12.5,afe;dur=5;desc=\"other\""),
            ServerTimings {
                gfe_latency: Some(12.5),
                afe_latency: Some(5.0),
            }
        );
        assert_eq!(
            parse_server_timing("afe;dur=8.2,gfet4t7;dur=10.0"),
            ServerTimings {
                gfe_latency: Some(10.0),
                afe_latency: Some(8.2),
            }
        );
        assert_eq!(
            parse_server_timing("other;dur=1.0"),
            ServerTimings {
                gfe_latency: None,
                afe_latency: None,
            }
        );
        assert_eq!(
            parse_server_timing("gfet4t7;dur=\"invalid\""),
            ServerTimings {
                gfe_latency: None,
                afe_latency: None,
            }
        );
        assert_eq!(
            parse_server_timing("gfet4t7;dur=-5.0"),
            ServerTimings {
                gfe_latency: None,
                afe_latency: None,
            }
        );
    }

    #[test]
    fn parse_server_timing_from_headers_multiple_headers() {
        let mut headers = HeaderMap::new();
        headers.append(
            "server-timing",
            HeaderValue::from_static("gfet4t7;dur=15.0"),
        );
        headers.append("server-timing", HeaderValue::from_static("afe;dur=7.5"));

        let timings = parse_server_timing_from_headers(&headers);
        assert_eq!(
            timings,
            ServerTimings {
                gfe_latency: Some(15.0),
                afe_latency: Some(7.5),
            }
        );
    }

    #[tokio::test]
    async fn observability_init_disabled_env() {
        let _env = scoped_env::ScopedEnv::set("SPANNER_DISABLE_BUILTIN_METRICS", "true");
        let o11y = Observability::init(
            &ClientConfig::default(),
            InstanceType::Cloud,
            "projects/p/instances/i/databases/d",
            false,
        )
        .await;
        assert!(o11y.metrics.is_none());
    }

    #[tokio::test]
    async fn observability_init_plaintext_endpoint() {
        let mut config = ClientConfig::default();
        config.endpoint = Some("http://localhost:9010".to_string());
        let o11y = Observability::init(
            &config,
            InstanceType::Cloud,
            "projects/p/instances/i/databases/d",
            false,
        )
        .await;
        assert!(o11y.metrics.is_none());
    }

    #[tokio::test]
    async fn observability_init_emulator() {
        let o11y = Observability::init(
            &ClientConfig::default(),
            InstanceType::Cloud,
            "projects/p/instances/i/databases/d",
            true,
        )
        .await;
        assert!(o11y.metrics.is_none());
    }

    #[tokio::test]
    async fn observability_init_omni() {
        let o11y = Observability::init(
            &ClientConfig::default(),
            InstanceType::Omni,
            "projects/p/instances/i/databases/d",
            false,
        )
        .await;
        assert!(o11y.metrics.is_none());
    }

    #[test]
    fn retry_attempts_and_operation_recorded() {
        use google_cloud_gax::options::internal::RequestOptionsExt as _;

        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);
        let o11y = Arc::new(Observability::for_test(metrics, provider.clone()));
        let interceptor = SpannerMetricsInterceptor;

        let options = crate::RequestOptions::default().insert_extension(Arc::clone(&o11y));

        let start_time_1 = Instant::now();
        // Attempt 1 fails with UNAVAILABLE
        let status_unavail = Status::default().set_code(Code::Unavailable);
        let err_unavail = Error::service(status_unavail);
        interceptor.on_attempt_complete(
            "/google.spanner.v1.Spanner/ExecuteSql",
            1,
            start_time_1,
            None,
            Some(&err_unavail),
            &options,
        );

        let start_time_2 = Instant::now();
        // Attempt 2 succeeds with server-timing
        let mut res_headers = HeaderMap::new();
        res_headers.insert(
            "server-timing",
            HeaderValue::from_static("gfet4t7;dur=12.5,afe;dur=3.2"),
        );
        interceptor.on_attempt_complete(
            "/google.spanner.v1.Spanner/ExecuteSql",
            2,
            start_time_2,
            Some(&res_headers),
            None,
            &options,
        );

        // Record total operation completion
        o11y.record_operation(
            "google.spanner.v1.Spanner/ExecuteSql",
            Duration::from_millis(50),
            None,
        );

        provider.force_flush().expect("force_flush should succeed");
        let finished = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics should succeed");

        let attempt_count_attrs = extract_all_attributes(
            &finished,
            "spanner.googleapis.com/internal/client/attempt_count",
        );
        assert_eq!(attempt_count_attrs.len(), 2, "should record 2 attempts");
        let statuses: Vec<&str> = attempt_count_attrs
            .iter()
            .map(|attribute_map| {
                attribute_map
                    .get("status")
                    .map(String::as_str)
                    .unwrap_or("")
            })
            .collect();
        assert!(
            statuses.contains(&"UNAVAILABLE"),
            "should contain UNAVAILABLE attempt"
        );
        assert!(statuses.contains(&"OK"), "should contain OK attempt");

        for attr in &attempt_count_attrs {
            assert_eq!(
                attr.get("method").map(String::as_str),
                Some("Spanner.ExecuteSql"),
                "method attribute should be normalized across attempts"
            );
        }

        let op_count_attrs = extract_all_attributes(
            &finished,
            "spanner.googleapis.com/internal/client/operation_count",
        );
        assert_eq!(op_count_attrs.len(), 1, "should record 1 operation");
        assert_eq!(
            op_count_attrs[0].get("method").map(String::as_str),
            Some("Spanner.ExecuteSql"),
            "operation method attribute should be normalized"
        );
        assert_eq!(
            op_count_attrs[0].get("status").map(String::as_str),
            Some("OK")
        );
    }

    #[test]
    fn transport_error_status_unknown() {
        use google_cloud_gax::options::internal::RequestOptionsExt as _;

        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);
        let o11y = Arc::new(Observability::for_test(metrics, provider.clone()));
        let interceptor = SpannerMetricsInterceptor;

        let options = crate::RequestOptions::default().insert_extension(o11y);
        let start_time = Instant::now();

        // Non-gRPC transport error (e.g. IO failure without gRPC Status)
        let transport_error = Error::timeout("simulated timeout");
        interceptor.on_attempt_complete(
            "/google.spanner.v1.Spanner/ExecuteSql",
            1,
            start_time,
            None,
            Some(&transport_error),
            &options,
        );

        provider.force_flush().expect("force_flush should succeed");
        let finished = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics should succeed");

        let attempt_attrs = extract_all_attributes(
            &finished,
            "spanner.googleapis.com/internal/client/attempt_count",
        );
        assert_eq!(attempt_attrs.len(), 1);
        assert_eq!(
            attempt_attrs[0].get("status").map(String::as_str),
            Some("UNKNOWN"),
            "non-gRPC transport errors must record status = UNKNOWN"
        );
    }

    #[test]
    fn missing_server_timing_increments_gfe_connectivity_error_counter() {
        use google_cloud_gax::options::internal::RequestOptionsExt as _;

        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);
        let o11y = Arc::new(Observability::for_test(metrics, provider.clone()));
        let interceptor = SpannerMetricsInterceptor;

        let options = crate::RequestOptions::default().insert_extension(o11y);
        let start_time = Instant::now();

        // Attempt completes with headers missing server-timing
        let empty_headers = HeaderMap::new();
        interceptor.on_attempt_complete(
            "/google.spanner.v1.Spanner/CreateSession",
            1,
            start_time,
            Some(&empty_headers),
            None,
            &options,
        );

        provider.force_flush().expect("force_flush should succeed");
        let finished = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics should succeed");

        let metric_names: Vec<&str> = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .map(|m| m.name())
            .collect();

        assert!(
            metric_names
                .contains(&"spanner.googleapis.com/internal/client/gfe_connectivity_error_count"),
            "missing server-timing must increment gfe_connectivity_error_count"
        );
        assert!(
            !metric_names
                .contains(&"spanner.googleapis.com/internal/client/afe_connectivity_error_count"),
            "non-DirectPath requests must NOT increment afe_connectivity_error_count when AFE timing is absent"
        );
    }

    fn extract_all_attributes(
        finished: &[ResourceMetrics],
        metric_name: &str,
    ) -> Vec<HashMap<String, String>> {
        let mut result = Vec::new();
        for resource_metrics in finished {
            for scope_metrics in resource_metrics.scope_metrics() {
                for metric in scope_metrics.metrics() {
                    if metric.name() == metric_name {
                        match metric.data() {
                            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                                for data_point in sum.data_points() {
                                    result.push(
                                        data_point
                                            .attributes()
                                            .map(|key_value| {
                                                (
                                                    key_value.key.to_string(),
                                                    key_value.value.to_string(),
                                                )
                                            })
                                            .collect(),
                                    );
                                }
                            }
                            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                                for data_point in histogram.data_points() {
                                    result.push(
                                        data_point
                                            .attributes()
                                            .map(|key_value| {
                                                (
                                                    key_value.key.to_string(),
                                                    key_value.value.to_string(),
                                                )
                                            })
                                            .collect(),
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        result
    }
}
