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

//! This module contains types to export OpenTelemetry metrics to Google Cloud Monitoring.
//!
//! # Example
//! ```
//! use integration_tests_o11y::otlp::metrics::Builder;
//! use opentelemetry_sdk::metrics::SdkMeterProvider;
//! use opentelemetry::{global, KeyValue};
//! # async fn example() -> anyhow::Result<()> {
//! let provider: SdkMeterProvider = Builder::new("my-project", "my-service")
//!     .build()
//!     .await?;
//! // Make the provider available to the libraries and application.
//! global::set_meter_provider(provider.clone());
//! // Use the provider.
//! let meter = opentelemetry::global::meter("my-component");
//! let counter = meter.u64_counter("my_counter").build();
//! counter.add(1, &[KeyValue::new("my.key", "my.value")]);
//! # Ok(()) }
//! ```

use super::Error;
use super::Uri;
use super::{OTEL_KEY_GCP_PROJECT_ID, OTEL_KEY_SERVICE_NAME};
use crate::auth::CloudTelemetryAuthInterceptor;
use google_cloud_auth::credentials::{Builder as AdcBuilder, Credentials};
use opentelemetry::KeyValue;
use opentelemetry_otlp::tonic_types::transport::ClientTlsConfig;
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::metrics::{
    Aggregation, Instrument, InstrumentKind, SdkMeterProvider, Stream,
};
use opentelemetry_sdk::resource::ResourceDetector;

/// Creates a `SdkMeterProvider` optimized for Google Cloud Monitoring.
///
/// This builder creates a `SdkMeterProvider` configured to export metrics via
/// the Google Cloud Telemetry API (`telemetry.googleapis.com`) using the OTLP
/// gRPC protocol. It automatically handles authentication by injecting OAuth2
/// tokens into every request.
///
/// The resulting provider is configured with:
/// - **Transport:** gRPC via `tonic` with TLS enabled (using system roots).
/// - **Endpoint:** `https://telemetry.googleapis.com`.
/// - **Authentication:** by default, use Application Default Credentials (ADC).
///   The application can override the default using credentials from the
///   `google-cloud-auth` crate.
/// - **Resource Attributes:** sets `gcp.project_id` and `service.name` as
///   required by Cloud Monitoring.
///
/// # Example
/// ```
/// use opentelemetry_sdk::resource::Resource;
/// use integration_tests_o11y::otlp::metrics::Builder;
/// # async fn example() -> anyhow::Result<()> {
/// let provider = Builder::new("my-project", "my-service")
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
pub struct Builder {
    project_id: String,
    service_name: String,
    credentials: Option<Credentials>,
    endpoint: Uri,
    detector: Option<Box<dyn ResourceDetector>>,
}

impl Builder {
    /// Creates a new builder with the required Google Cloud project ID and service name.
    ///
    /// # Parameters
    /// * `project_id` - The Google Cloud project ID. This is attached as the `gcp.project_id`
    ///   resource attribute, which is required by Cloud Trace.
    /// * `service_name` - The logical name of the service. Attached as `service.name` resource
    ///   attribute, used by Cloud Trace to group and identify services.
    pub fn new<P, S>(project_id: P, service_name: S) -> Self
    where
        P: Into<String>,
        S: Into<String>,
    {
        let uri = http::Uri::from_static(super::GCP_OTLP_ENDPOINT);
        Self {
            project_id: project_id.into(),
            service_name: service_name.into(),
            credentials: None,
            endpoint: uri,
            detector: None,
        }
    }

    /// Sets the credentials used for authentication.
    /// If not provided, Application Default Credentials (ADC) will be loaded.
    pub fn with_credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Sets a custom OTLP endpoint. Useful for testing or VPC-SC.
    /// Defaults to `https://telemetry.googleapis.com`.
    pub fn with_endpoint(mut self, uri: Uri) -> Self {
        self.endpoint = uri;
        self
    }

    /// Sets the resource detector.
    pub fn with_detector<D>(mut self, detector: D) -> Self
    where
        D: ResourceDetector + 'static,
    {
        self.detector = Some(Box::new(detector));
        self
    }

    /// Builds and initializes the `SdkTracerProvider`.
    pub async fn build(self) -> Result<SdkMeterProvider, Error> {
        let resource = opentelemetry_sdk::Resource::builder()
            .with_attributes(vec![
                KeyValue::new(OTEL_KEY_GCP_PROJECT_ID, self.project_id),
                KeyValue::new(OTEL_KEY_SERVICE_NAME, self.service_name),
            ])
            .with_detectors(&Vec::from_iter(self.detector.into_iter()))
            .build();
        let credentials = match self.credentials {
            Some(c) => c,
            None => AdcBuilder::default().build().map_err(Error::credentials)?,
        };
        let interceptor = CloudTelemetryAuthInterceptor::new(credentials).await;

        let exporter_builder = {
            let builder = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(self.endpoint.to_string())
                .with_interceptor(interceptor);
            // Only enable TLS if the endpoint is HTTPS.
            // This allows using http://localhost for testing.
            if self
                .endpoint
                .scheme()
                .is_none_or(|s| s != &http::uri::Scheme::HTTPS)
            {
                builder
            } else {
                let domain = self
                    .endpoint
                    .authority()
                    .ok_or_else(|| Error::invalid_uri(self.endpoint.clone()))?;
                let config = ClientTlsConfig::new()
                    .with_enabled_roots()
                    .domain_name(domain.host());
                builder.with_tls_config(config)
            }
        };

        let exporter = exporter_builder.build().map_err(Error::create_exporter)?;
        let view = move |ins: &Instrument| {
            let name = if Self::name_missing_prefix(ins.name()) {
                format!("workload.googleapis.com/{}", ins.name())
            } else {
                ins.name().to_string()
            };
            let builder = Stream::builder().with_name(name);
            let builder = if ins.kind() != InstrumentKind::Histogram {
                builder
            } else {
                builder.with_aggregation(Aggregation::Base2ExponentialHistogram {
                    max_size: 32,
                    max_scale: 20,
                    record_min_max: true,
                })
            };
            builder.build().expect("stream should be valid").into()
        };
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter)
            .with_resource(resource)
            .with_view(view)
            .build();

        Ok(provider)
    }

    /// Returns true if the metric name needs a `workload.googleapis.com` prefix.
    ///
    /// Google Cloud Monitoring only accepts metric names in these formats:
    ///     custom.googleapis.com/{name}
    ///     workload.googleapis.com/{name}
    ///     project/{projectId}/metricDescriptors/custom.googleapis.com/{name}
    ///     project/{projectId}/metricDescriptors/workload.googleapis.com/{name}
    fn name_missing_prefix(name: &str) -> bool {
        const W: &str = "workload.googleapis.com";
        const C: &str = "custom.googleapis.com";
        const P: &str = "projects";
        const D: &str = "metricDescriptors";
        let mut s = name.split('/');
        !matches!(
            (s.next(), s.next(), s.next(), s.next(), s.next()),
            (Some(P), Some(_), Some(D), Some(W), Some(_))
                | (Some(P), Some(_), Some(D), Some(C), Some(_))
                | (Some(W), Some(_), _, _, _)
                | (Some(C), Some(_), _, _, _)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::TestTokenProvider;
    use super::*;
    use crate::mock_collector::MockCollector;
    use opentelemetry::KeyValue;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_proto::tonic::common::v1::any_value;
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    use std::collections::BTreeMap;
    use std::str::FromStr;
    use test_case::test_case;

    /// Tests that the provider sends authenticated results to a mock
    /// collector.
    #[tokio::test(flavor = "multi_thread")]
    async fn export_with_mock_collector() -> anyhow::Result<()> {
        // 1. Create a mock collector to receive the metrics.
        let mock_collector = MockCollector::default();
        let endpoint = mock_collector.start().await;

        // 2. Initialize the `SdkMeterProvider` with a known credential headers,
        //    known resources (project and service). Later we will verify the
        //    values are included in the request.
        let credentials = Credentials::from(TestTokenProvider);
        let uri = Uri::from_str(&endpoint)?;
        let provider = Builder::new("test-project", "test-service")
            .with_credentials(credentials)
            .with_endpoint(uri)
            .build()
            .await
            .expect("failed to build provider");

        // 3. Create a counter and record a value, later we will verify the
        //    request includes this counter.
        const NAME: &str = "gcp.client.request.counter";
        let mut want = [
            ("gcp.client.service", "storage"),
            ("gcp.client.version", "1.2.3"),
            ("gcp.client.repo", "googleapis/google-cloud-rust"),
            ("gcp.client.artifact", "google-cloud-storage"),
            ("rpc.system.name", "GRPC"),
            (
                "gcp.method",
                "google.cloud.storage.v2.Storage/delete_bucket",
            ),
        ];
        want.sort_by(|a, b| a.0.cmp(b.0));
        let attributes = want.map(|(k, v)| KeyValue::new(k, v));
        let meter = provider.meter("test-meter");
        let counter = meter.u64_counter(NAME).build();
        counter.add(123, &attributes);

        // 4. Force flush to ensure the metrics are sent.
        provider.force_flush()?;

        // 5. Read any requests received by the mock collector.
        let (metadata, _extensions, request) = mock_collector
            .metrics
            .lock()
            .expect("never poisoned")
            .pop()
            .expect("mock collector should have received requests")
            .into_parts();

        // 6. Verify the headers include the known auth values.
        let headers = metadata.into_headers();
        assert_eq!(
            headers.get("x-goog-user-project").map(|v| v.as_bytes()),
            Some("test-project".as_bytes()),
            "{headers:?}"
        );
        assert!(
            headers
                .get("authorization")
                .is_some_and(|v| v.as_bytes().starts_with(b"Bearer")),
            "{headers:?}"
        );

        // 7. Verify there is a single resource and it includes the expected
        //    attributes.
        let rm = match &request.resource_metrics[..] {
            [rm] => rm,
            _ => panic!("expected exactly one resource, got {request:#?}"),
        };
        let resource = rm
            .resource
            .as_ref()
            .expect("the resource metrics should have a resource: {rm:?}");
        let got = resource
            .attributes
            .iter()
            .find(|kv| kv.key == OTEL_KEY_GCP_PROJECT_ID)
            .and_then(|kv| kv.value.as_ref())
            .and_then(|av| av.value.as_ref());
        assert!(
            matches!(got, Some(any_value::Value::StringValue(s)) if s == "test-project"),
            "{got:?}\n{resource:?}"
        );
        let got = resource
            .attributes
            .iter()
            .find(|kv| kv.key == OTEL_KEY_SERVICE_NAME)
            .and_then(|kv| kv.value.as_ref())
            .and_then(|av| av.value.as_ref());
        assert!(
            matches!(got, Some(any_value::Value::StringValue(s)) if s == "test-service"),
            "{got:?}\n{resource:?}"
        );

        // 8. Find the counter data point value and verify it has the expected
        //    attributes.
        let point = rm
            .scope_metrics
            .iter()
            // All the metrics for a single scope are grouped in a vector.
            .flat_map(|m| m.metrics.iter())
            .filter(|m| m.name.ends_with(NAME))
            // Then all the data points for each metric.
            .filter_map(|m| m.data.as_ref())
            // We only want the counters.
            .filter_map(|d| if let Data::Sum(h) = d { Some(h) } else { None })
            // There may be multiple data points for each counter, find the
            // one we want.
            .flat_map(|s| s.data_points.iter())
            .find(|point| point.value == Some(Value::AsInt(123_i64)))
            .unwrap_or_else(|| {
                panic!("cannot find data point for metric {NAME} in captured request: {request:#?}")
            });
        // Sort the expectations so the errors are easier to grok.
        let got = BTreeMap::from_iter(
            point
                .attributes
                .iter()
                // The "value" is wrapped in a `Option<>` remove the entries where the value is `None`:
                .filter_map(|kv| kv.value.as_ref().map(|v| (kv.key.as_str(), v))),
        );
        let missing = want
            .iter()
            .filter(|(k, v)| {
                got.get(*k)
                    // The horrors of protobuf, anyvalue.value is an
                    //  `Option<any_value::Value>`, which is an enum
                    .and_then(|anyvalue| anyvalue.value.as_ref())
                    .is_none_or(|g| !matches!(g, any_value::Value::StringValue(s) if s == *v))
            })
            .collect::<Vec<_>>();

        assert!(
            missing.is_empty(),
            "missing = {missing:?}\ngot  = {got:?}\nwant = {want:?}"
        );

        Ok(())
    }

    #[test_case("plain", true)]
    #[test_case("custom.googleapis.com/plain", false)]
    #[test_case("workload.googleapis.com/plain", false)]
    #[test_case("workload.googleapis.com/with/complex/name", false)]
    #[test_case("projects/p/metricDescriptors/custom.googleapis.com/plain", false)]
    #[test_case("projects/p/metricDescriptors/workload.googleapis.com/plain", false)]
    #[test_case("projects/p/metricDescriptors/workload.googleapis.com/a/b/c", false)]
    #[test_case("projects/p/custom.googleapis.com/plain", true)]
    #[test_case("projects/p/workload.googleapis.com/plain", true)]
    #[test_case("projects/p/D/custom.googleapis.com/plain", true)]
    #[test_case("projects/p/D/workload.googleapis.com/plain", true)]
    fn prefix(name: &str, want: bool) {
        let got = Builder::name_missing_prefix(name);
        assert_eq!(got, want, "{name}");
    }
}
