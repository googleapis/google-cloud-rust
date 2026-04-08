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
use opentelemetry_semantic_conventions::trace::{ERROR_TYPE, SERVER_ADDRESS, SERVER_PORT};
use std::sync::Arc;

pub const BOUNDARIES: [f64; 16] = [
    0.0, 0.0001, 0.0005, 0.0010, 0.005, 0.010, 0.050, 0.100, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0,
    900.0, 3600.0,
];
// TODO(#4772) - use the real name once the attributes are all working.
const METRIC_NAME: &str = "gcp.client.request.duration";
// This is seconds in SI units.
const METRIC_UNIT: &str = "s";

/// Simplify the creation of client request duration metrics.
///
/// The generated and hand-crafted client libraries will need to capture
/// per-request duration metrics in a histogram metric. The code to initialize
/// these histograms and record new values is shared by all the clients, so we
/// can refactor the code to this struct.
///
/// Typically client libraries will use this as:
///
/// ```ignore
/// #[derive(Clone, Debug)]
/// struct TracingLayer<T> {
///     inner: T /* where T implements the client trait */
///     request_duration: DurationMetric
/// }
///
/// impl<T> TracingLayer<T> {
///     pub fn new(inner: T) -> Self {
///         Self {
///             inner,
///             request_duration: DurationMetric::new(&info::INSTRUMENTATION_CLIENT_INFO),
///         }
///     }
/// }
/// ```
///
/// The client can use this metric in the implementation of request methods.
#[derive(Clone, Debug)]
pub struct DurationMetric(Histogram<f64>);

impl DurationMetric {
    /// Creates a new instance based on the instrumentation info.
    ///
    /// The instrumentation info is used to initialize the OpenTelemetry
    /// [InstrumentationScope]. Which provides information about the client
    /// library and target service.
    pub fn new(info: &InstrumentationClientInfo) -> Self {
        let provider = opentelemetry::global::meter_provider();
        Self::new_with_provider(info, provider)
    }

    /// Used in the unit tests to avoid a global meter provider.
    pub(crate) fn new_with_provider(
        info: &InstrumentationClientInfo,
        provider: Arc<dyn MeterProvider + Send + Sync>,
    ) -> Self {
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
        Self(histogram)
    }

    /// Records the latency for a successful request.
    ///
    /// Uses `RequestRecorder` to retrieve the request attributes.
    pub(crate) fn with_recorder_ok(&self) {
        let Some(snapshot) = RequestRecorder::current().map(|r| r.client_snapshot()) else {
            return;
        };
        let attributes: [(&str, Option<Value>); 13] = [
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
        ];
        let attributes = attributes
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| KeyValue::new(k, v)))
            .collect::<Vec<_>>();
        self.0.record(
            snapshot.client_duration().as_secs_f64(),
            attributes.as_slice(),
        );
    }

    /// Records the latency for a successful request.
    ///
    /// Uses `RequestRecorder` to retrieve the request attributes.
    pub(crate) fn with_recorder_error(&self, error: &Error) {
        let Some(snapshot) = RequestRecorder::current().map(|r| r.client_snapshot()) else {
            return;
        };
        let error_type = ErrorType::from_gax_error(error);
        let attributes: [(&str, Option<Value>); 14] = [
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
        ];
        let attributes = attributes
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| KeyValue::new(k, v)))
            .collect::<Vec<_>>();
        self.0.record(
            snapshot.client_duration().as_secs_f64(),
            attributes.as_slice(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        TEST_INFO, TEST_METHOD, TEST_URL_TEMPLATE, check_metric_data, check_metric_scope,
    };
    use super::*;
    use crate::observability::ClientRequestAttributes;
    use google_cloud_gax::error::rpc::Status;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use std::sync::Arc;
    use std::time::Duration;

    // This is in the middle of the [0.5, 1.0) bucket defined in `boundaries`.
    const DELAY: Duration = Duration::from_millis(750);

    #[tokio::test(start_paused = true)]
    async fn global_record_error() -> anyhow::Result<()> {
        let metric = DurationMetric::new(&TEST_INFO);
        let error = Error::http(408, http::HeaderMap::new(), bytes::Bytes::new());
        metric.with_recorder_error(&error);
        // We can make no assertions other than "this test does not crash" because it must use a
        // global variable (`opentelemetry::global::meter_provider()`) and any other test in the
        // same crate may set or use that variable too.
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn record_ok() -> anyhow::Result<()> {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let metric = DurationMetric::new_with_provider(&TEST_INFO, Arc::new(provider.clone()));
        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_url_template(TEST_URL_TEMPLATE)
                .set_rpc_method(TEST_METHOD),
        );
        // Use a long pause so it gets recorded as such.
        tokio::time::sleep(DELAY).await;
        let _ = recorder
            .scope(async {
                metric.with_recorder_ok();
                Ok(())
            })
            .await;
        provider.force_flush()?;
        let metrics = exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            "gcp.client.request.duration",
            1_u64..=1_u64,
            &[
                // We are not simulating the HTTP layer so these are not populated.
                // ("rpc.system.name", "http"),
                // ("http.response.status_code", "200"),
                // The server.address and server.port get default values:
                ("server.address", "example.com"),
                ("server.port", "443"),
                // And here are the interesting attributes:
                ("url.domain", "example.com"),
                ("url.template", TEST_URL_TEMPLATE),
                ("rpc.method", TEST_METHOD),
                ("rpc.response.status_code", "OK"),
                ("gcp.client.service", "test-service"),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.schema.url", SCHEMA_URL_VALUE),
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
        let metric = DurationMetric::new_with_provider(&TEST_INFO, Arc::new(provider.clone()));
        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_url_template(TEST_URL_TEMPLATE)
                .set_rpc_method(TEST_METHOD),
        );
        // Use a long pause so it gets recorded as such.
        tokio::time::sleep(DELAY).await;
        let error = Error::service(
            Status::default()
                .set_code(Code::NotFound)
                .set_message("NOT FOUND"),
        );
        let _ = recorder
            .scope(async {
                metric.with_recorder_error(&error);
                Ok(())
            })
            .await;
        provider.force_flush()?;
        let metrics = exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            "gcp.client.request.duration",
            1_u64..=1_u64,
            &[
                // We are not simulating the HTTP layer so these are not populated.
                // ("rpc.system.name", "http"),
                // ("http.response.status_code", "404"),
                // The server.address and server.port get default values:
                ("server.address", "example.com"),
                ("server.port", "443"),
                // And here are the interesting attributes:
                ("url.domain", "example.com"),
                ("url.template", TEST_URL_TEMPLATE),
                ("rpc.method", TEST_METHOD),
                ("error.type", "NOT_FOUND"),
                ("rpc.response.status_code", "NOT_FOUND"),
                ("gcp.client.service", "test-service"),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.schema.url", SCHEMA_URL_VALUE),
            ],
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn record_http_error() -> anyhow::Result<()> {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let metric = DurationMetric::new_with_provider(&TEST_INFO, Arc::new(provider.clone()));
        let recorder = RequestRecorder::new(TEST_INFO);
        recorder.on_client_request(
            ClientRequestAttributes::default()
                .set_url_template(TEST_URL_TEMPLATE)
                .set_rpc_method(TEST_METHOD),
        );
        // Use a long pause so it gets recorded as such.
        tokio::time::sleep(DELAY).await;
        let error = Error::http(429, http::HeaderMap::new(), bytes::Bytes::new());
        let _ = recorder
            .scope(async {
                metric.with_recorder_error(&error);
                Ok(())
            })
            .await;
        provider.force_flush()?;
        let metrics = exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            "gcp.client.request.duration",
            1_u64..=1_u64,
            &[
                // We are not simulating the HTTP layer so these are not populated.
                // ("rpc.system.name", "http"),
                // ("http.response.status_code", "200"),
                // The server.address and server.port get default values:
                ("server.address", "example.com"),
                ("server.port", "443"),
                // And here are the interesting attributes:
                ("url.domain", "example.com"),
                ("url.template", TEST_URL_TEMPLATE),
                ("rpc.method", TEST_METHOD),
                ("error.type", "429"),
                ("gcp.client.service", "test-service"),
                ("gcp.client.version", "1.2.3"),
                ("gcp.client.repo", "googleapis/google-cloud-rust"),
                ("gcp.client.artifact", "test-artifact"),
                ("gcp.schema.url", SCHEMA_URL_VALUE),
            ],
        );
        Ok(())
    }
}
