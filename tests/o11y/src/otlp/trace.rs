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

//! This module contains types to export OpenTelemetry traces to Google Cloud Trace.
//!
//! # Examples
//! ```
//! use integration_tests_o11y::otlp::trace::Builder;
//! use opentelemetry_sdk::trace::SdkTracerProvider;
//! # async fn example() -> anyhow::Result<()> {
//! let provider: SdkTracerProvider = Builder::new("my-project", "my-service")
//!     .build()
//!     .await?;
//! // Make the provider available to the libraries and application.
//! opentelemetry::global::set_tracer_provider(provider.clone());
//! # Ok(()) }
//! ```

use super::{OTEL_KEY_GCP_PROJECT_ID, OTEL_KEY_SERVICE_NAME};
use crate::auth::CloudTelemetryAuthInterceptor;
use google_cloud_auth::credentials::{Builder as AdcBuilder, Credentials};
use opentelemetry_otlp::tonic_types::transport::ClientTlsConfig;
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::resource::ResourceDetector;
use opentelemetry_sdk::trace::{SdkTracerProvider, TraceError};

const GCP_OTLP_DOMAIN_NAME: &str = "telemetry.googleapis.com";

/// Builder for configuring an OpenTelemetry `SdkTracerProvider` optimized for Google Cloud Trace.
///
/// This builder creates a `SdkTracerProvider` configured to export traces to the Google Cloud
/// Telemetry API (`telemetry.googleapis.com`) using the OTLP gRPC protocol. It automatically
/// handles authentication by injecting OAuth2 tokens into every request.
///
/// The resulting provider is configured with:
/// - **Transport:** gRPC via `tonic` with TLS enabled (using system roots).
/// - **Endpoint:** `https://telemetry.googleapis.com`.
/// - **Authentication:** Application Default Credentials (ADC) or provided credentials,
///   refreshed automatically.
/// - **Resource Attributes:** Sets `gcp.project_id` and `service.name` as required by Cloud Trace.
///
/// # Examples
/// ```
/// use opentelemetry_sdk::trace::SdkTracerProvider;
/// # async fn example() -> anyhow::Result<()> {
/// # use integration_tests_o11y::otlp::trace::Builder;
/// let provider: SdkTracerProvider  = Builder::new("my-project", "my-service")
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct Builder {
    project_id: String,
    service_name: String,
    credentials: Option<Credentials>,
    endpoint: String,
    domain_name: String,
    detector: Option<Box<dyn ResourceDetector>>,
}

impl Builder {
    /// Creates a new builder with the required Google Cloud project ID and service name.
    ///
    /// * `project_id` - The Google Cloud project ID. This is attached as the `gcp.project_id`
    ///   resource attribute, which is required by Cloud Trace.
    /// * `service_name` - The logical name of the service. Attached as `service.name` resource
    ///   attribute, used by Cloud Trace to group and identify services.
    pub fn new(project_id: impl Into<String>, service_name: impl Into<String>) -> Self {
        Self {
            project_id: project_id.into(),
            service_name: service_name.into(),
            credentials: None,
            endpoint: format!("https://{}", GCP_OTLP_DOMAIN_NAME),
            domain_name: GCP_OTLP_DOMAIN_NAME.to_string(),
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
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Sets the domain name for TLS verification.
    /// Defaults to `telemetry.googleapis.com`.
    pub fn with_domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.domain_name = domain_name.into();
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
    pub async fn build(self) -> Result<SdkTracerProvider, TraceError> {
        let credentials = match self.credentials {
            Some(c) => c,
            None => AdcBuilder::default()
                .build()
                .map_err(|e| TraceError::Other(e.into()))?,
        };
        let interceptor = CloudTelemetryAuthInterceptor::new(credentials).await;

        let resource = opentelemetry_sdk::Resource::builder()
            .with_attributes(vec![
                opentelemetry::KeyValue::new(OTEL_KEY_GCP_PROJECT_ID, self.project_id),
                opentelemetry::KeyValue::new(OTEL_KEY_SERVICE_NAME, self.service_name),
            ])
            .with_detectors(&Vec::from_iter(self.detector.into_iter()))
            .build();

        let is_https = self.endpoint.starts_with("https");
        let mut exporter_builder = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(self.endpoint)
            .with_interceptor(interceptor);

        // Only enable TLS if the endpoint is HTTPS.
        // This allows using http://localhost for testing.
        if is_https {
            let tls_config = ClientTlsConfig::new()
                .with_enabled_roots()
                .domain_name(self.domain_name);
            exporter_builder = exporter_builder.with_tls_config(tls_config);
        }

        let exporter = exporter_builder
            .build()
            .map_err(|e| TraceError::Other(e.into()))?;

        let provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build();

        Ok(provider)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::TestTokenProvider;
    use super::*;
    use crate::mock_collector::MockCollector;
    use google_cloud_auth::credentials::testing::error_credentials;
    use opentelemetry::trace::{Tracer, TracerProvider as _};

    #[tokio::test]
    async fn test_builder_configuration() {
        let credentials = error_credentials(false);
        let project_id = "builder-project-id";
        let service_name = "builder-service-name";
        let endpoint = "https://custom-endpoint.example.com";

        let provider = Builder::new(project_id, service_name)
            .with_credentials(credentials)
            .with_endpoint(endpoint)
            .build()
            .await
            .expect("failed to build provider");

        let debug_output = format!("{:?}", provider);
        assert!(debug_output.contains(project_id));
        assert!(debug_output.contains(service_name));
    }

    /// Tests that the provider sends authenticated results to a mock
    /// collector.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_otlp_export_with_mock_collector() {
        let mock_collector = MockCollector::default();
        let endpoint = mock_collector.start().await;

        let credentials = Credentials::from(TestTokenProvider);

        let provider = Builder::new("test-project", "test-service")
            .with_credentials(credentials)
            .with_endpoint(endpoint)
            .build()
            .await
            .expect("failed to build provider");

        let tracer = provider.tracer("test-tracer");
        tracer.start("test-span");

        // Wait for credentials to be refreshed
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Force flush to ensure spans are sent
        let _ = provider.force_flush();

        let (metadata, _extensions, request) = mock_collector
            .traces
            .lock()
            .expect("never poisoned")
            .pop()
            .expect("mock collector should have received requests")
            .into_parts();

        let resource_spans = &request.resource_spans;
        assert!(!resource_spans.is_empty(), "{request:?}");
        let scope_spans = &resource_spans[0].scope_spans;
        assert!(!scope_spans.is_empty(), "{request:?}");
        let spans = &scope_spans[0].spans;
        assert!(!spans.is_empty(), "{request:?}");
        assert_eq!(spans[0].name, "test-span");

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
    }
}
