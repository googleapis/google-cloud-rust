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
#[cfg(feature = "_experimental-builtin-metrics")]
use std::sync::Arc;
#[cfg(feature = "_experimental-builtin-metrics")]
use std::time::Duration;
#[cfg(feature = "_experimental-builtin-metrics")]
use std::time::Instant;

#[cfg(feature = "_experimental-builtin-metrics")]
use {
    crate::observability::exporter::GcpMonitoringExporter,
    gaxi::options::ClientConfig,
    google_cloud_monitoring_v3::client::MetricService,
    opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider},
    opentelemetry_sdk::{
        error::OTelSdkError,
        metrics::{PeriodicReader, SdkMeterProvider},
    },
};

#[cfg(not(feature = "_experimental-builtin-metrics"))]
use gaxi::options::ClientConfig;

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
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct SpannerMetrics {
    pub(crate) operation_latencies: Histogram<f64>,
    pub(crate) attempt_latencies: Histogram<f64>,
    pub(crate) gfe_latencies: Histogram<f64>,
    pub(crate) afe_latencies: Histogram<f64>,
    pub(crate) operation_count: Counter<u64>,
    pub(crate) attempt_count: Counter<u64>,
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
        }
    }
}

#[cfg(feature = "_experimental-builtin-metrics")]
#[derive(Clone, Debug)]
pub(crate) struct Observability {
    pub(crate) metrics: Option<Arc<SpannerMetrics>>,
    meter_provider: Option<Arc<SdkMeterProvider>>,
}

#[cfg(feature = "_experimental-builtin-metrics")]
impl Observability {
    pub(crate) fn disabled() -> Self {
        Self {
            metrics: None,
            meter_provider: None,
        }
    }

    pub(crate) async fn init(
        config: &ClientConfig,
        instance_type: InstanceType,
        is_emulator: bool,
        project_id: Option<&str>,
    ) -> Self {
        let disable_builtin_metrics = std::env::var("SPANNER_DISABLE_BUILTIN_METRICS")
            .map(|s| s.eq_ignore_ascii_case("true") || s == "1")
            .unwrap_or(false);
        if disable_builtin_metrics || instance_type == InstanceType::Omni || is_emulator {
            return Self::disabled();
        }

        let project_id = match project_id {
            Some(id) if !id.is_empty() => id,
            _ => return Self::disabled(),
        };

        // Create the Google Cloud Monitoring client using the same config
        let mut builder = MetricService::builder();

        if let Some(ref cred) = config.cred {
            builder = builder.with_credentials(cred.clone());
        }
        if let Some(ref ud) = config.universe_domain {
            builder = builder.with_universe_domain(ud.clone());
        }

        let monitoring_client = match builder.build().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    "Failed to initialize Google Cloud Monitoring client for Spanner metrics: {:?}",
                    e
                );
                return Self::disabled();
            }
        };

        let exporter = GcpMonitoringExporter::new(monitoring_client, project_id);

        // Set up PeriodicReader with a 60-second export interval.
        let reader = PeriodicReader::builder(exporter)
            .with_interval(DEFAULT_EXPORT_INTERVAL)
            .build();

        let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();

        let meter = meter_provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);

        Self {
            metrics: Some(Arc::new(metrics)),
            meter_provider: Some(Arc::new(meter_provider)),
        }
    }

    pub(crate) async fn trace_operation<Fut, T>(
        &self,
        method: &'static str,
        fut: Fut,
    ) -> crate::Result<T>
    where
        Fut: std::future::Future<Output = crate::Result<T>>,
    {
        if self.metrics.is_none() {
            return fut.await;
        }
        let start_time = Instant::now();
        let result = fut.await;
        let elapsed = start_time.elapsed();
        self.record_operation(method, elapsed, &result);
        result
    }

    #[allow(dead_code)]
    pub(crate) async fn trace_attempt<F, Fut, T>(
        &self,
        method: &'static str,
        f: F,
    ) -> crate::Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = crate::Result<T>>,
    {
        let start_time = Instant::now();
        let result = f().await;
        let elapsed = start_time.elapsed();
        self.record_attempt(method, elapsed, &result, None, None);
        result
    }

    #[allow(dead_code)]
    pub(crate) fn record_attempt<T>(
        &self,
        method: &'static str,
        duration: Duration,
        result: &crate::Result<T>,
        gfe_latency: Option<f64>,
        afe_latency: Option<f64>,
    ) {
        let Some(ref metrics) = self.metrics else {
            return;
        };

        let status = result_to_status_str(result);
        let attributes = [
            opentelemetry::KeyValue::new("method", method),
            opentelemetry::KeyValue::new("status", status),
        ];

        metrics
            .attempt_latencies
            .record(duration.as_secs_f64() * 1000.0, &attributes);
        metrics.attempt_count.add(1, &attributes);

        if let Some(gfe) = gfe_latency {
            metrics.gfe_latencies.record(gfe, &attributes);
        }
        if let Some(afe) = afe_latency {
            metrics.afe_latencies.record(afe, &attributes);
        }
    }

    pub(crate) fn record_operation<T>(
        &self,
        method: &'static str,
        duration: Duration,
        result: &crate::Result<T>,
    ) {
        let Some(ref metrics) = self.metrics else {
            return;
        };

        let status = result_to_status_str(result);
        let attributes = [
            opentelemetry::KeyValue::new("method", method),
            opentelemetry::KeyValue::new("status", status),
        ];

        metrics
            .operation_latencies
            .record(duration.as_secs_f64() * 1000.0, &attributes);
        metrics.operation_count.add(1, &attributes);
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
fn result_to_status_str<T>(result: &crate::Result<T>) -> &'static str {
    match result {
        Ok(_) => "OK",
        Err(e) => e.status().map_or("UNKNOWN", |status| status.code.name()),
    }
}

#[cfg(feature = "_experimental-builtin-metrics")]
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ServerTimings {
    pub(crate) gfe_latency: Option<f64>,
    pub(crate) afe_latency: Option<f64>,
}

#[cfg(feature = "_experimental-builtin-metrics")]
#[allow(dead_code)]
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
                timings.gfe_latency = Some(duration);
            }
            if is_afe {
                timings.afe_latency = Some(duration);
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
#[derive(Clone, Debug)]
pub(crate) struct Observability;

#[cfg(not(feature = "_experimental-builtin-metrics"))]
impl Observability {
    #[allow(dead_code)]
    pub(crate) fn disabled() -> Self {
        Self
    }

    pub(crate) async fn init(
        _config: &ClientConfig,
        _instance_type: InstanceType,
        _is_emulator: bool,
        _project_id: Option<&str>,
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
        Fut: std::future::Future<Output = crate::Result<T>>,
    {
        fut.await
    }

    #[allow(dead_code)]
    pub(crate) fn shutdown(&self) {}
}

#[cfg(all(test, feature = "_experimental-builtin-metrics"))]
mod tests {
    use super::*;
    use opentelemetry_sdk::metrics::InMemoryMetricExporter;
    #[test]
    fn test_observability_disabled() {
        let o11y = Observability::disabled();
        assert!(o11y.metrics.is_none());
    }

    #[test]
    fn test_result_to_status_str() {
        let ok_res: crate::Result<()> = Ok(());
        assert_eq!(result_to_status_str(&ok_res), "OK");

        let status_pd = google_cloud_gax::error::rpc::Status::default()
            .set_code(google_cloud_gax::error::rpc::Code::PermissionDenied);
        let err_pd: crate::Result<()> = Err(crate::Error::service(status_pd));
        assert_eq!(result_to_status_str(&err_pd), "PERMISSION_DENIED");
    }

    #[test]
    fn test_spanner_metrics_record_operation_and_attempt() {
        let exporter = InMemoryMetricExporter::default();
        let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);
        let o11y = Observability {
            metrics: Some(Arc::new(metrics)),
            meter_provider: Some(Arc::new(provider.clone())),
        };

        let ok_res: crate::Result<()> = Ok(());
        o11y.record_operation("ExecuteSql", Duration::from_millis(50), &ok_res);
        o11y.record_attempt(
            "ExecuteSql",
            Duration::from_millis(40),
            &ok_res,
            Some(12.5),
            Some(5.0),
        );

        provider.force_flush().expect("force_flush failed");

        let finished = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics");
        assert!(!finished.is_empty());
    }

    #[tokio::test]
    async fn test_trace_operation_success() {
        let o11y = Observability::disabled();
        let result = o11y
            .trace_operation("ExecuteSql", async { Ok::<i32, crate::Error>(42) })
            .await;
        assert_eq!(result.expect("trace_operation result"), 42);
    }

    #[test]
    fn parse_server_timing() {
        assert_eq!(
            super::parse_server_timing("gfet4t7;dur=12.5"),
            ServerTimings {
                gfe_latency: Some(12.5),
                afe_latency: None,
            }
        );
        assert_eq!(
            super::parse_server_timing("gfet4t7;desc=\"test\";dur=12.5,afe;dur=5;desc=\"other\""),
            ServerTimings {
                gfe_latency: Some(12.5),
                afe_latency: Some(5.0),
            }
        );
        assert_eq!(
            super::parse_server_timing("afe;dur=3,some-other;dur=10"),
            ServerTimings {
                gfe_latency: None,
                afe_latency: Some(3.0),
            }
        );
        assert_eq!(
            super::parse_server_timing("invalid_format"),
            ServerTimings::default()
        );
        assert_eq!(
            super::parse_server_timing("gfet4t7;dur=\"12.5\",afe;dur=\"5.0\""),
            ServerTimings {
                gfe_latency: Some(12.5),
                afe_latency: Some(5.0),
            }
        );
        assert_eq!(
            super::parse_server_timing("GFET4T7; dur=12.5, Afe; dur=5.0"),
            ServerTimings {
                gfe_latency: Some(12.5),
                afe_latency: Some(5.0),
            }
        );
        assert_eq!(
            super::parse_server_timing("gfet4t7;dur=-5.0,afe;dur=NaN"),
            ServerTimings::default()
        );
        assert_eq!(
            super::parse_server_timing(",,,gfet4t7;dur=10.0,,,"),
            ServerTimings {
                gfe_latency: Some(10.0),
                afe_latency: None,
            }
        );
        assert_eq!(
            super::parse_server_timing("gfet4t7;dur=inf,afe;dur=Infinity"),
            ServerTimings::default()
        );
        assert_eq!(
            super::parse_server_timing("gfet4t7;dur=foo;dur=12.5"),
            ServerTimings {
                gfe_latency: Some(12.5),
                afe_latency: None,
            }
        );
        assert_eq!(
            super::parse_server_timing("gfet4t7; dur = 12.5 , afe; dur = \"5.0\" "),
            ServerTimings {
                gfe_latency: Some(12.5),
                afe_latency: Some(5.0),
            }
        );
        assert_eq!(
            super::parse_server_timing("gfet4t7;dur=10.0, gfet4t7;dur=20.0"),
            ServerTimings {
                gfe_latency: Some(20.0),
                afe_latency: None,
            }
        );
        assert_eq!(
            super::parse_server_timing("foo;dur=1.0,gfet4t7;dur=12.5,bar;dur=2.0,afe;dur=5.0"),
            ServerTimings {
                gfe_latency: Some(12.5),
                afe_latency: Some(5.0),
            }
        );
    }

    #[test]
    fn test_default_export_interval() {
        assert_eq!(DEFAULT_EXPORT_INTERVAL, Duration::from_secs(60));
    }

    #[test]
    fn test_bucket_boundaries_len_and_values() {
        assert_eq!(BUCKET_BOUNDARIES.len(), 50);
        assert_eq!(BUCKET_BOUNDARIES[0], 0.0);
        assert_eq!(
            *BUCKET_BOUNDARIES
                .last()
                .expect("BUCKET_BOUNDARIES should not be empty"),
            3200000.0
        );
    }

    #[tokio::test]
    async fn test_observability_disabled_paths() {
        let o11y = Observability::disabled();
        assert!(
            o11y.metrics.is_none(),
            "disabled observability should have no metrics"
        );

        let config = ClientConfig::default();
        let o11y_emulator =
            Observability::init(&config, InstanceType::Cloud, true, Some("my-project")).await;
        assert!(
            o11y_emulator.metrics.is_none(),
            "emulator client should have disabled metrics"
        );

        let o11y_omni =
            Observability::init(&config, InstanceType::Omni, false, Some("my-project")).await;
        assert!(
            o11y_omni.metrics.is_none(),
            "omni client should have disabled metrics"
        );
    }

    #[test]
    fn test_spanner_metrics_initialization_and_recording() {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(DEFAULT_EXPORT_INTERVAL)
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);

        let attributes = [
            opentelemetry::KeyValue::new("method", "ExecuteSql"),
            opentelemetry::KeyValue::new("status", "OK"),
        ];

        metrics.operation_latencies.record(12.5, &attributes);
        metrics.attempt_latencies.record(10.0, &attributes);
        metrics.gfe_latencies.record(1.5, &attributes);
        metrics.afe_latencies.record(2.0, &attributes);
        metrics.operation_count.add(1, &attributes);
        metrics.attempt_count.add(1, &attributes);

        provider.force_flush().expect("force_flush should succeed");

        let finished_metrics = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics should succeed");
        assert!(
            !finished_metrics.is_empty(),
            "exported metrics should not be empty"
        );

        let mut metric_names = Vec::new();
        for resource_metrics in &finished_metrics {
            for scope_metrics in resource_metrics.scope_metrics() {
                for m in scope_metrics.metrics() {
                    metric_names.push(m.name().to_string());
                }
            }
        }

        assert!(
            metric_names.contains(
                &"spanner.googleapis.com/internal/client/operation_latencies".to_string()
            )
        );
        assert!(
            metric_names
                .contains(&"spanner.googleapis.com/internal/client/attempt_latencies".to_string())
        );
        assert!(
            metric_names
                .contains(&"spanner.googleapis.com/internal/client/gfe_latencies".to_string())
        );
        assert!(
            metric_names
                .contains(&"spanner.googleapis.com/internal/client/afe_latencies".to_string())
        );
        assert!(
            metric_names
                .contains(&"spanner.googleapis.com/internal/client/operation_count".to_string())
        );
        assert!(
            metric_names
                .contains(&"spanner.googleapis.com/internal/client/attempt_count".to_string())
        );
    }

    #[test]
    fn test_observability_recording_methods() {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(DEFAULT_EXPORT_INTERVAL)
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);

        let o11y = Observability {
            metrics: Some(Arc::new(metrics)),
            meter_provider: Some(Arc::new(provider.clone())),
        };

        o11y.record_operation("test_op", Duration::from_millis(15), &Ok(()));
        o11y.record_attempt(
            "test_op",
            Duration::from_millis(10),
            &Ok(()),
            Some(3.0),
            Some(2.0),
        );

        if let Some(ref provider) = o11y.meter_provider {
            provider.force_flush().expect("force_flush should succeed");
        }

        let finished = exporter
            .get_finished_metrics()
            .expect("get_finished_metrics should succeed");
        assert!(
            !finished.is_empty(),
            "exported metrics should not be empty after recording methods"
        );
    }

    #[test]
    fn observability_disabled_shutdown_and_drop() {
        let o11y = Observability::disabled();
        o11y.shutdown();
        o11y.shutdown();
        // Dropping o11y should invoke Drop and shutdown without panic.
    }

    #[cfg(feature = "_experimental-builtin-metrics")]
    #[test]
    fn observability_double_shutdown_ignores_already_shutdown() {
        let meter_provider = SdkMeterProvider::builder().build();
        let o11y = Observability {
            metrics: None,
            meter_provider: Some(Arc::new(meter_provider)),
        };
        // Calling shutdown twice should cleanly handle AlreadyShutdown on the second call.
        o11y.shutdown();
        o11y.shutdown();
        // Dropping o11y invokes Drop and calls shutdown a third time without panic or warning.
    }
}
