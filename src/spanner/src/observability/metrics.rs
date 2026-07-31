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
use std::time::Duration;
#[cfg(feature = "_experimental-builtin-metrics")]
use std::time::Instant;

#[cfg(feature = "_experimental-builtin-metrics")]
use {
    crate::observability::exporter::GcpMonitoringExporter,
    gaxi::options::ClientConfig,
    google_cloud_monitoring_v3::client::MetricService,
    opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider},
    opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider},
};

#[cfg(not(feature = "_experimental-builtin-metrics"))]
use gaxi::options::ClientConfig;

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

/// Parses `projects/{project}/instances/{instance}/databases/{database}` into its
/// `(project_id, instance_id, database_id)` components.
#[cfg(feature = "_experimental-builtin-metrics")]
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
pub(crate) fn client_name() -> &'static str {
    concat!("spanner-rust/", env!("CARGO_PKG_VERSION"))
}

#[cfg(feature = "_experimental-builtin-metrics")]
#[derive(Debug)]
pub(crate) struct Observability {
    pub(crate) metrics: Option<SpannerMetrics>,
    common_attributes: [opentelemetry::KeyValue; 3],
    _meter_provider: Option<SdkMeterProvider>,
}

#[cfg(feature = "_experimental-builtin-metrics")]
impl Observability {
    pub(crate) fn disabled() -> Self {
        Self {
            metrics: None,
            common_attributes: [
                opentelemetry::KeyValue::new("client_uid", ""),
                opentelemetry::KeyValue::new("client_name", ""),
                opentelemetry::KeyValue::new("database", ""),
            ],
            _meter_provider: None,
        }
    }

    pub(crate) async fn init(
        config: &ClientConfig,
        instance_type: InstanceType,
        database_name: &str,
    ) -> Self {
        let disable_builtin_metrics = std::env::var("SPANNER_DISABLE_BUILTIN_METRICS")
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if disable_builtin_metrics || instance_type == InstanceType::Omni {
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

        let client_uid = generate_client_uid();
        let client_hash = generate_client_hash(&client_uid);
        let client_name = client_name();

        let resource = opentelemetry_sdk::Resource::builder()
            .with_attributes([
                opentelemetry::KeyValue::new("project_id", project_id.to_string()),
                opentelemetry::KeyValue::new("instance_id", instance_id.to_string()),
                opentelemetry::KeyValue::new("location", "global"),
                opentelemetry::KeyValue::new("instance_config", "unknown"),
                opentelemetry::KeyValue::new("client_hash", client_hash),
            ])
            .build();

        // Set up PeriodicReader
        let reader = PeriodicReader::builder(exporter).build();

        let meter_provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build();

        let meter = meter_provider.meter("cloud.google.com/rust");
        let metrics = SpannerMetrics::new(meter);

        let common_attributes = [
            opentelemetry::KeyValue::new("client_uid", client_uid),
            opentelemetry::KeyValue::new("client_name", client_name),
            opentelemetry::KeyValue::new("database", database_id.to_string()),
        ];

        Self {
            metrics: Some(metrics),
            common_attributes,
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
            self.common_attributes[0].clone(),
            self.common_attributes[1].clone(),
            self.common_attributes[2].clone(),
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
            self.common_attributes[0].clone(),
            self.common_attributes[1].clone(),
            self.common_attributes[2].clone(),
        ];

        metrics
            .operation_latencies
            .record(duration.as_secs_f64() * 1000.0, &attributes);
        metrics.operation_count.add(1, &attributes);
    }
}

#[cfg(feature = "_experimental-builtin-metrics")]
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

#[cfg(not(feature = "_experimental-builtin-metrics"))]
#[derive(Debug)]
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
        _database_name: &str,
    ) -> Self {
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

#[cfg(all(test, feature = "_experimental-builtin-metrics"))]
mod tests {
    use super::*;

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
    }
}
