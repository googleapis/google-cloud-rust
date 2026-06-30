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

#[cfg(feature = "experimental-builtin-metrics")]
use std::time::Duration;
#[cfg(feature = "experimental-builtin-metrics")]
use std::time::Instant;

#[cfg(feature = "experimental-builtin-metrics")]
use {
    crate::observability::exporter::GcpMonitoringExporter,
    gaxi::options::ClientConfig,
    google_cloud_monitoring_v3::client::MetricService,
    opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider},
    opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider},
};

#[cfg(not(feature = "experimental-builtin-metrics"))]
use gaxi::options::ClientConfig;

#[cfg(feature = "experimental-builtin-metrics")]
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

#[cfg(feature = "experimental-builtin-metrics")]
impl SpannerMetrics {
    pub(crate) fn new(meter: Meter) -> Self {
        Self {
            operation_latencies: meter
                .f64_histogram("spanner.googleapis.com/internal/client/operation_latencies")
                .with_unit("ms")
                .build(),
            attempt_latencies: meter
                .f64_histogram("spanner.googleapis.com/internal/client/attempt_latencies")
                .with_unit("ms")
                .build(),
            gfe_latencies: meter
                .f64_histogram("spanner.googleapis.com/internal/client/gfe_latencies")
                .with_unit("ms")
                .build(),
            afe_latencies: meter
                .f64_histogram("spanner.googleapis.com/internal/client/afe_latencies")
                .with_unit("ms")
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

#[cfg(feature = "experimental-builtin-metrics")]
#[derive(Debug)]
pub(crate) struct Observability {
    pub(crate) metrics: Option<SpannerMetrics>,
    _meter_provider: Option<SdkMeterProvider>,
}

#[cfg(feature = "experimental-builtin-metrics")]
impl Observability {
    pub(crate) fn disabled() -> Self {
        Self {
            metrics: None,
            _meter_provider: None,
        }
    }

    pub(crate) async fn init(config: &ClientConfig, project_id: Option<&str>) -> Self {
        let disable_builtin_metrics = std::env::var("SPANNER_DISABLE_BUILTIN_METRICS")
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if disable_builtin_metrics {
            return Self::disabled();
        }

        let project_id = match project_id {
            Some(id) => id,
            None => return Self::disabled(),
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

        // Set up PeriodicReader
        let reader = PeriodicReader::builder(exporter).build();

        let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();

        let meter = meter_provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);

        Self {
            metrics: Some(metrics),
            _meter_provider: Some(meter_provider),
        }
    }

    pub(crate) async fn trace_operation<F, Fut, T>(
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
}

#[cfg(feature = "experimental-builtin-metrics")]
fn result_to_status_str<T>(result: &crate::Result<T>) -> &'static str {
    match result {
        Ok(_) => "OK",
        Err(e) => {
            if let Some(status) = e.status() {
                status.code.name()
            } else {
                "UNKNOWN"
            }
        }
    }
}

#[cfg(feature = "experimental-builtin-metrics")]
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ServerTimings {
    pub(crate) gfe_latency: Option<f64>,
    pub(crate) afe_latency: Option<f64>,
}

#[cfg(feature = "experimental-builtin-metrics")]
#[allow(dead_code)]
pub(crate) fn parse_server_timing(header_val: &str) -> ServerTimings {
    let mut timings = ServerTimings::default();
    for part in header_val.split(',') {
        let mut subparts = part.split(';');
        if let Some(name) = subparts.next().map(|s| s.trim()) {
            for param in subparts {
                let mut kv = param.split('=');
                let dur_opt = match (kv.next(), kv.next()) {
                    (Some(k), Some(v)) if k.trim() == "dur" => v.trim().parse::<f64>().ok(),
                    _ => None,
                };
                if let Some(dur) = dur_opt {
                    match name {
                        "gfet4t7" => timings.gfe_latency = Some(dur),
                        "afe" => timings.afe_latency = Some(dur),
                        _ => {}
                    }
                }
            }
        }
    }
    timings
}

#[cfg(not(feature = "experimental-builtin-metrics"))]
#[derive(Debug)]
pub(crate) struct Observability;

#[cfg(not(feature = "experimental-builtin-metrics"))]
impl Observability {
    #[allow(dead_code)]
    pub(crate) fn disabled() -> Self {
        Self
    }

    pub(crate) async fn init(_config: &ClientConfig, _project_id: Option<&str>) -> Self {
        Self
    }

    pub(crate) async fn trace_operation<F, Fut, T>(
        &self,
        _method: &'static str,
        f: F,
    ) -> crate::Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = crate::Result<T>>,
    {
        f().await
    }
}

#[cfg(all(test, feature = "experimental-builtin-metrics"))]
mod tests {
    use super::*;

    #[test]
    fn test_parse_server_timing() {
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
            parse_server_timing("afe;dur=3,some-other;dur=10"),
            ServerTimings {
                gfe_latency: None,
                afe_latency: Some(3.0),
            }
        );
        assert_eq!(
            parse_server_timing("invalid_format"),
            ServerTimings::default()
        );
    }
}
