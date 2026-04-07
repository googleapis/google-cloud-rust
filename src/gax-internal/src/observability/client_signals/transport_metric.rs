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

use crate::observability::RequestRecorder;
use crate::observability::attributes::keys::{
    GCP_CLIENT_ARTIFACT, GCP_CLIENT_REPO, GCP_CLIENT_SERVICE, GCP_CLIENT_VERSION, GCP_SCHEMA_URL,
    HTTP_RESPONSE_STATUS_CODE, RPC_METHOD, RPC_RESPONSE_STATUS_CODE, RPC_SYSTEM_NAME,
};
use crate::observability::attributes::{GCP_CLIENT_REPO_GOOGLEAPIS, SCHEMA_URL_VALUE};
use crate::observability::errors::ErrorType;
use crate::options::InstrumentationClientInfo;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use opentelemetry::metrics::{Histogram, MeterProvider};
use opentelemetry::{InstrumentationScope, KeyValue, Value};
use opentelemetry_semantic_conventions::attribute::{URL_DOMAIN, URL_TEMPLATE};
use opentelemetry_semantic_conventions::trace::{
    ERROR_TYPE, HTTP_REQUEST_RESEND_COUNT, HTTP_RESPONSE_BODY_SIZE, SERVER_ADDRESS, SERVER_PORT,
    URL_SCHEME,
};
use std::sync::Arc;

pub const BOUNDARIES: [f64; 16] = [
    0.0, 0.0001, 0.0005, 0.0010, 0.005, 0.010, 0.050, 0.100, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0,
    900.0, 3600.0,
];

const METRIC_NAME: &str = "gcp.client.attempt.duration";
const METRIC_UNIT: &str = "s";

/// Simplify the creation of transport attempt duration metrics.
#[derive(Clone, Debug)]
pub struct TransportMetric(Option<Histogram<f64>>);

impl TransportMetric {
    /// Creates a new instance based on the instrumentation info.
    pub fn new(info: Option<&InstrumentationClientInfo>) -> Self {
        let provider = opentelemetry::global::meter_provider();
        Self::new_with_provider(info, provider)
    }

    /// Used in the unit tests to avoid a global meter provider.
    pub(crate) fn new_with_provider(
        info: Option<&InstrumentationClientInfo>,
        provider: Arc<dyn MeterProvider + Send + Sync>,
    ) -> Self {
        let Some(info) = info else {
            return Self(None);
        };
        let scope = InstrumentationScope::builder(info.client_artifact)
            .with_version(info.client_version)
            .with_schema_url(SCHEMA_URL_VALUE)
            .with_attributes([
                KeyValue::new(GCP_CLIENT_ARTIFACT, info.client_artifact),
                KeyValue::new(GCP_CLIENT_SERVICE, info.service_name),
                KeyValue::new(GCP_CLIENT_REPO, GCP_CLIENT_REPO_GOOGLEAPIS),
            ])
            .build();
        let meter = provider.meter_with_scope(scope);
        let histogram = meter
            .f64_histogram(METRIC_NAME)
            .with_unit(METRIC_UNIT)
            .with_boundaries(BOUNDARIES.to_vec())
            .build();
        Self(Some(histogram))
    }

    /// Records the latency for a successful attempt.
    pub(crate) fn with_recorder_ok(&self, attempt_count: u32) {
        let Some(histogram) = &self.0 else {
            return;
        };
        let Some(snapshot) = RequestRecorder::current().map(|r| r.client_snapshot()) else {
            return;
        };
        let Some(duration) = snapshot.transport_duration() else {
            return;
        };

        let attributes: [(&str, Option<Value>); 16] = [
            (RPC_SYSTEM_NAME, snapshot.rpc_system().map(|v| v.into())),
            (RPC_METHOD, snapshot.rpc_method().map(|v| v.into())),
            (URL_DOMAIN, Some(snapshot.default_host().into())),
            (URL_TEMPLATE, snapshot.url_template().map(|v| v.into())),
            (RPC_RESPONSE_STATUS_CODE, Some(Code::Ok.name().into())),
            (
                HTTP_RESPONSE_STATUS_CODE,
                snapshot.http_status_code().map(|v| (v as i64).into()),
            ),
            (SERVER_ADDRESS, Some(snapshot.server_address().into())),
            (SERVER_PORT, Some((snapshot.server_port() as i64).into())),
            (GCP_CLIENT_SERVICE, Some(snapshot.service_name().into())),
            (GCP_CLIENT_VERSION, Some(snapshot.client_version().into())),
            (GCP_CLIENT_REPO, Some(GCP_CLIENT_REPO_GOOGLEAPIS.into())),
            (GCP_CLIENT_ARTIFACT, Some(snapshot.client_artifact().into())),
            (GCP_SCHEMA_URL, Some(SCHEMA_URL_VALUE.into())),
            (
                URL_SCHEME,
                snapshot.url_scheme().map(|v| v.to_string().into()),
            ),
            (
                HTTP_RESPONSE_BODY_SIZE,
                snapshot.http_response_body_size().map(|v| v.into()),
            ),
            (
                HTTP_REQUEST_RESEND_COUNT,
                Some((attempt_count.saturating_sub(1) as i64).into()),
            ),
        ];
        let attributes = attributes
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| KeyValue::new(k, v)))
            .collect::<Vec<_>>();

        histogram.record(duration.as_secs_f64(), attributes.as_slice());
    }

    /// Records the latency for a failed attempt.
    pub(crate) fn with_recorder_error(&self, error: &Error, attempt_count: u32) {
        let Some(histogram) = &self.0 else {
            return;
        };
        let Some(snapshot) = RequestRecorder::current().map(|r| r.client_snapshot()) else {
            return;
        };
        let Some(duration) = snapshot.transport_duration() else {
            return;
        };
        let error_type = ErrorType::from_gax_error(error);

        let attributes: [(&str, Option<Value>); 17] = [
            (RPC_SYSTEM_NAME, snapshot.rpc_system().map(|v| v.into())),
            (RPC_METHOD, snapshot.rpc_method().map(|v| v.into())),
            (URL_DOMAIN, Some(snapshot.default_host().into())),
            (URL_TEMPLATE, snapshot.url_template().map(|v| v.into())),
            (ERROR_TYPE, Some(error_type.as_str().into())),
            (
                RPC_RESPONSE_STATUS_CODE,
                error.status().map(|s| s.code.name().into()),
            ),
            (
                HTTP_RESPONSE_STATUS_CODE,
                snapshot.http_status_code().map(|v| (v as i64).into()),
            ),
            (SERVER_ADDRESS, Some(snapshot.server_address().into())),
            (SERVER_PORT, Some((snapshot.server_port() as i64).into())),
            (GCP_CLIENT_SERVICE, Some(snapshot.service_name().into())),
            (GCP_CLIENT_VERSION, Some(snapshot.client_version().into())),
            (GCP_CLIENT_REPO, Some(GCP_CLIENT_REPO_GOOGLEAPIS.into())),
            (GCP_CLIENT_ARTIFACT, Some(snapshot.client_artifact().into())),
            (GCP_SCHEMA_URL, Some(SCHEMA_URL_VALUE.into())),
            (
                URL_SCHEME,
                snapshot.url_scheme().map(|v| v.to_string().into()),
            ),
            (
                HTTP_RESPONSE_BODY_SIZE,
                snapshot.http_response_body_size().map(|v| v.into()),
            ),
            (
                HTTP_REQUEST_RESEND_COUNT,
                Some((attempt_count.saturating_sub(1) as i64).into()),
            ),
        ];
        let attributes = attributes
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| KeyValue::new(k, v)))
            .collect::<Vec<_>>();

        histogram.record(duration.as_secs_f64(), attributes.as_slice());
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{check_metric_data, check_metric_scope};
    use super::*;
    use crate::observability::ClientRequestAttributes;
    use crate::observability::attributes::SCHEMA_URL_VALUE;
    use crate::options::InstrumentationClientInfo;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use std::sync::Arc;
    use std::time::Duration;

    const TEST_METHOD: &str = "test.method";
    const TEST_INFO: InstrumentationClientInfo = InstrumentationClientInfo {
        client_artifact: "test-artifact",
        client_version: "1.2.3",
        service_name: "test-service",
        default_host: "example.com",
    };
    const DELAY: Duration = crate::observability::client_signals::tests::TEST_REQUEST_DURATION;

    #[tokio::test(start_paused = true)]
    async fn record_ok() -> anyhow::Result<()> {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let metric =
            TransportMetric::new_with_provider(Some(&TEST_INFO), Arc::new(provider.clone()));
        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_url_template("https://example.com/v1/projects/{project}/topics/{topic}")
                .set_rpc_method(TEST_METHOD),
        );

        let request = reqwest::Request::new(
            reqwest::Method::GET,
            "https://example.com/v1/projects/p/topics/t".parse()?,
        );

        recorder.on_http_request(&request);
        tokio::time::sleep(DELAY).await;

        let _ = recorder
            .scope(async {
                metric.with_recorder_ok(2); // attempt_count = 2
                Ok(())
            })
            .await;

        provider.force_flush()?;
        let metrics = exporter.get_finished_metrics()?;

        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            "gcp.client.attempt.duration",
            1_u64..=1_u64,
            &[
                (SERVER_ADDRESS, "example.com"),
                (SERVER_PORT, "443"),
                (URL_DOMAIN, "example.com"),
                (
                    URL_TEMPLATE,
                    "https://example.com/v1/projects/{project}/topics/{topic}",
                ),
                (RPC_METHOD, TEST_METHOD),
                (RPC_SYSTEM_NAME, "http"),
                (RPC_RESPONSE_STATUS_CODE, "OK"),
                (GCP_CLIENT_SERVICE, "test-service"),
                (GCP_CLIENT_VERSION, "1.2.3"),
                (GCP_CLIENT_REPO, "googleapis/google-cloud-rust"),
                (GCP_CLIENT_ARTIFACT, "test-artifact"),
                (GCP_SCHEMA_URL, SCHEMA_URL_VALUE),
                (URL_SCHEME, "https"),
                (HTTP_REQUEST_RESEND_COUNT, "1"),
            ],
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn record_error() -> anyhow::Result<()> {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let metric =
            TransportMetric::new_with_provider(Some(&TEST_INFO), Arc::new(provider.clone()));
        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_url_template("https://example.com/v1/projects/{project}/topics/{topic}")
                .set_rpc_method(TEST_METHOD),
        );

        let request = reqwest::Request::new(
            reqwest::Method::GET,
            "https://example.com/v1/projects/p/topics/t".parse()?,
        );

        recorder.on_http_request(&request);
        tokio::time::sleep(DELAY).await;

        let err =
            google_cloud_gax::error::Error::http(404, http::HeaderMap::new(), bytes::Bytes::new());

        let _ = recorder
            .scope(async {
                metric.with_recorder_error(&err, 2); // attempt_count = 2
                Ok(())
            })
            .await;

        provider.force_flush()?;
        let metrics = exporter.get_finished_metrics()?;

        check_metric_scope(&metrics);
        let found = metrics
            .iter()
            .flat_map(|s| s.scope_metrics())
            .flat_map(|r| r.metrics())
            .find(|m| m.name() == "gcp.client.attempt.duration");
        assert!(found.is_some());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn record_without_recorder() -> anyhow::Result<()> {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let metric =
            TransportMetric::new_with_provider(Some(&TEST_INFO), Arc::new(provider.clone()));

        // No recorder in scope
        metric.with_recorder_ok(1);

        provider.force_flush()?;
        let metrics = exporter.get_finished_metrics()?;

        let found = metrics
            .iter()
            .flat_map(|s| s.scope_metrics())
            .flat_map(|r| r.metrics())
            .any(|m| m.name() == "gcp.client.attempt.duration");
        assert!(!found);

        Ok(())
    }
}
