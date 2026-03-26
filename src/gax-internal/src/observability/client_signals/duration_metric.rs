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

use super::RequestStart;
use crate::observability::RequestRecorder;
use crate::observability::attributes::keys::{
    GCP_CLIENT_ARTIFACT, GCP_CLIENT_REPO, GCP_CLIENT_SERVICE, GCP_CLIENT_VERSION,
    HTTP_RESPONSE_STATUS_CODE, RPC_METHOD, RPC_RESPONSE_STATUS_CODE, RPC_SYSTEM_NAME,
};
use crate::observability::attributes::{GCP_CLIENT_REPO_GOOGLEAPIS, RPC_SYSTEM_HTTP};
use crate::observability::errors::ErrorType;
use crate::options::InstrumentationClientInfo;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;
use opentelemetry::metrics::{Histogram, MeterProvider};
use opentelemetry::{InstrumentationScope, KeyValue, Value};
use opentelemetry_semantic_conventions::attribute::{self, URL_DOMAIN, URL_TEMPLATE};
use opentelemetry_semantic_conventions::trace::{ERROR_TYPE, SERVER_ADDRESS, SERVER_PORT};
use std::sync::Arc;

pub const BOUNDARIES: [f64; 16] = [
    0.0, 0.0001, 0.0005, 0.0010, 0.005, 0.010, 0.050, 0.100, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0,
    900.0, 3600.0,
];
// TODO(#4772) - use the real name once the attributes are all working.
const METRIC_NAME: &str = "test.client.duration";
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
            .with_attributes([
                KeyValue::new(GCP_CLIENT_ARTIFACT, info.client_artifact),
                KeyValue::new(GCP_CLIENT_VERSION, info.client_version),
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
        let attributes: [(&str, Option<Value>); _] = [
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
    /// Uses `start` to compute the duration and the method attributes.
    pub(crate) fn record_ok(&self, start: &RequestStart) {
        let elapsed = start.elapsed();
        self.0.record(
            elapsed.as_secs_f64(),
            &[
                KeyValue::new(RPC_SYSTEM_NAME, RPC_SYSTEM_HTTP),
                KeyValue::new(attribute::URL_DOMAIN, start.info().default_host),
                KeyValue::new(attribute::URL_TEMPLATE, start.url_template()),
                KeyValue::new(attribute::RPC_METHOD, start.method()),
                KeyValue::new(RPC_RESPONSE_STATUS_CODE, Code::Ok.name()),
                KeyValue::new(attribute::HTTP_RESPONSE_STATUS_CODE, 200_i64),
            ],
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
        let attributes: [(&str, Option<Value>); _] = [
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

    /// Records the latency for a request that completed with an error.
    ///
    /// Uses `start` to compute the duration and most of the method attributes,
    /// `error` is summarized in some key parameters, including any status
    /// codes.
    pub(crate) fn record_error(&self, start: &RequestStart, error: &Error) {
        let elapsed = start.elapsed();
        // Use a `Vec` to omit HTTP_RESPONSE_STATUS_CODE. This extra allocation
        // occurs only on error paths, which should be rare.
        let mut attributes = Vec::from_iter([
            KeyValue::new(RPC_SYSTEM_NAME, RPC_SYSTEM_HTTP),
            KeyValue::new(attribute::URL_DOMAIN, start.info().default_host),
            KeyValue::new(attribute::URL_TEMPLATE, start.url_template()),
            KeyValue::new(attribute::RPC_METHOD, start.method()),
            KeyValue::new(
                RPC_RESPONSE_STATUS_CODE,
                error
                    .status()
                    .map(|s| s.code.name())
                    .unwrap_or(Code::Unknown.name()),
            ),
        ]);
        if let Some(code) = error.http_status_code() {
            attributes.push(KeyValue::new(
                attribute::HTTP_RESPONSE_STATUS_CODE,
                code as i64,
            ));
        }
        self.0.record(elapsed.as_secs_f64(), &attributes);
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{
        TEST_INFO, TEST_METHOD, TEST_URL_TEMPLATE, check_metric_data, check_metric_scope,
    };
    use super::*;
    use google_cloud_gax::error::rpc::Status;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use std::sync::Arc;
    use std::time::Duration;

    // This is in the middle of the [0.5, 1.0) bucket defined in `boundaries`.
    const DELAY: Duration = Duration::from_millis(750);

    #[tokio::test(start_paused = true)]
    async fn global_record_error() -> anyhow::Result<()> {
        let metric = DurationMetric::new(&TEST_INFO);
        let options = RequestOptions::default().insert_extension(PathTemplate(TEST_URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, TEST_METHOD);
        let error = Error::http(408, http::HeaderMap::new(), bytes::Bytes::new());
        metric.record_error(&start, &error);
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
        let options = RequestOptions::default().insert_extension(PathTemplate(TEST_URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, TEST_METHOD);
        // Use a long pause so it gets recorded as such.
        tokio::time::sleep(DELAY).await;
        metric.record_ok(&start);
        provider.force_flush()?;
        let metrics = exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            1_u64..=1_u64,
            &[
                ("rpc.system.name", "http"),
                ("url.domain", "example.com"),
                ("url.template", TEST_URL_TEMPLATE),
                ("rpc.method", TEST_METHOD),
                ("rpc.response.status_code", "OK"),
                ("http.response.status_code", "200"),
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
        let options = RequestOptions::default().insert_extension(PathTemplate(TEST_URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, TEST_METHOD);
        // Use a long pause so it gets recorded as such.
        tokio::time::sleep(DELAY).await;
        let error = Error::service(
            Status::default()
                .set_code(Code::NotFound)
                .set_message("NOT FOUND"),
        );
        metric.record_error(&start, &error);
        provider.force_flush()?;
        let metrics = exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            1_u64..=1_u64,
            &[
                ("rpc.system.name", "http"),
                ("url.domain", "example.com"),
                ("url.template", TEST_URL_TEMPLATE),
                ("rpc.method", TEST_METHOD),
                ("rpc.response.status_code", "NOT_FOUND"),
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
        let options = RequestOptions::default().insert_extension(PathTemplate(TEST_URL_TEMPLATE));
        let start = RequestStart::new(&TEST_INFO, &options, TEST_METHOD);
        // Use a long pause so it gets recorded as such.
        tokio::time::sleep(DELAY).await;
        let error = Error::http(429, http::HeaderMap::new(), bytes::Bytes::new());
        metric.record_error(&start, &error);
        provider.force_flush()?;
        let metrics = exporter.get_finished_metrics()?;
        check_metric_scope(&metrics);
        check_metric_data(
            &metrics,
            1_u64..=1_u64,
            &[
                ("rpc.system.name", "http"),
                ("url.domain", "example.com"),
                ("url.template", TEST_URL_TEMPLATE),
                ("rpc.method", TEST_METHOD),
                ("rpc.response.status_code", "UNKNOWN"),
                ("http.response.status_code", "429"),
            ],
        );
        Ok(())
    }
}
