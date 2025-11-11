// Copyright 2025 Google LLC
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

use super::auth::CloudTelemetryAuthInterceptor;
use auth::credentials::{Builder, Credentials};
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::trace::{SdkTracerProvider, TraceError};
use tonic::transport::ClientTlsConfig;

const GCP_OTLP_DOMAIN_NAME: &str = "telemetry.googleapis.com";
const OTEL_KEY_GCP_PROJECT_ID: &str = "gcp.project_id";
const OTEL_KEY_SERVICE_NAME: &str = "service.name";

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
///
/// ```no_run
/// use integration_tests::observability::otlp::CloudTelemetryTracerProviderBuilder;
///
/// # async fn example() -> anyhow::Result<()> {
/// let provider = CloudTelemetryTracerProviderBuilder::new("my-project", "my-service")
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct CloudTelemetryTracerProviderBuilder {
    project_id: String,
    service_name: String,
    credentials: Option<Credentials>,
    endpoint: String,
    domain_name: String,
}

impl CloudTelemetryTracerProviderBuilder {
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

    /// Builds and initializes the `SdkTracerProvider`.
    pub async fn build(self) -> Result<SdkTracerProvider, TraceError> {
        let credentials = match self.credentials {
            Some(c) => c,
            None => Builder::default()
                .build()
                .map_err(|e| TraceError::Other(e.into()))?,
        };
        let interceptor = CloudTelemetryAuthInterceptor::new(credentials);

        let resource = opentelemetry_sdk::Resource::builder_empty()
            .with_attributes(vec![
                opentelemetry::KeyValue::new(OTEL_KEY_GCP_PROJECT_ID, self.project_id),
                opentelemetry::KeyValue::new(OTEL_KEY_SERVICE_NAME, self.service_name),
            ])
            .build();

        let mut exporter_builder = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(self.endpoint)
            .with_interceptor(interceptor);

        let tls_config = ClientTlsConfig::new()
            .with_enabled_roots()
            .domain_name(self.domain_name);
        exporter_builder = exporter_builder.with_tls_config(tls_config);

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
    use super::*;

    #[tokio::test]
    async fn test_builder_configuration() {
        let credentials = auth::credentials::testing::error_credentials(true);
        let project_id = "builder-project-id";
        let service_name = "builder-service-name";
        let endpoint = "https://custom-endpoint.example.com";

        let provider = CloudTelemetryTracerProviderBuilder::new(project_id, service_name)
            .with_credentials(credentials)
            .with_endpoint(endpoint)
            .build()
            .await
            .expect("failed to build provider");

        let debug_output = format!("{:?}", provider);
        assert!(debug_output.contains(project_id));
        assert!(debug_output.contains(service_name));
    }
}
