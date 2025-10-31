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

// OpenTelemetry Semantic Convention Keys
// See https://opentelemetry.io/docs/specs/semconv/http/http-spans/

/// Span Kind for OpenTelemetry interop.
///
/// Always "Client" for a span representing an outbound HTTP request.
pub const KEY_OTEL_KIND: &str = "otel.kind";
/// Span Name for OpenTelemetry interop.
///
/// If `url.template` is available use "{http.request.method} {url.template}", otherwise use "{http.request.method}".
pub const KEY_OTEL_NAME: &str = "otel.name";
/// Span Status for OpenTelemetry interop.
///
/// Use "Error" for unrecoverable errors like network issues or 5xx status codes.
/// Otherwise, leave "Unset" (including for 4xx codes on CLIENT spans).
pub const KEY_OTEL_STATUS: &str = "otel.status";

/// The string representation of the gRPC status code.
pub const KEY_GRPC_STATUS: &str = "grpc.status";

// Custom GCP Attributes
/// The Google Cloud service name.
///
/// Examples: appengine, run, firestore
pub const KEY_GCP_CLIENT_SERVICE: &str = "gcp.client.service";
/// The client library version.
///
/// Example: v1.0.2
pub const KEY_GCP_CLIENT_VERSION: &str = "gcp.client.version";
/// The client library repository.
///
/// Always "googleapis/google-cloud-rust".
pub const KEY_GCP_CLIENT_REPO: &str = "gcp.client.repo";
/// The client library crate name.
///
/// Example: google-cloud-storage
pub const KEY_GCP_CLIENT_ARTIFACT: &str = "gcp.client.artifact";

/// Values for the OpenTelemetry `error.type` attribute.
/// See [https://opentelemetry.io/docs/specs/semconv/attributes-registry/error/]
pub mod error_type_values {
    /// A client-configured timeout was reached.
    pub const CLIENT_TIMEOUT: &str = "CLIENT_TIMEOUT";
    /// Failure to establish the network connection (DNS, TCP, TLS).
    pub const CLIENT_CONNECTION_ERROR: &str = "CLIENT_CONNECTION_ERROR";
    /// Client-side issue forming or sending the request.
    pub const CLIENT_REQUEST_ERROR: &str = "CLIENT_REQUEST_ERROR";
    /// Client-side error decoding the response body.
    pub const CLIENT_RESPONSE_DECODE_ERROR: &str = "CLIENT_RESPONSE_DECODE_ERROR";
    /// Error during credential acquisition or application.
    pub const CLIENT_AUTHENTICATION_ERROR: &str = "CLIENT_AUTHENTICATION_ERROR";
    /// Resource exhausted (e.g. retry limit reached).
    pub const CLIENT_RETRY_EXHAUSTED: &str = "CLIENT_RETRY_EXHAUSTED";
    /// Unexpected issue within the client library's own logic.
    pub const INTERNAL: &str = "INTERNAL";
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OtelStatus {
    Unset,
    Ok,
    Error,
}

impl OtelStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            OtelStatus::Unset => "Unset",
            OtelStatus::Ok => "Ok",
            OtelStatus::Error => "Error",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otel_status_as_str() {
        assert_eq!(OtelStatus::Unset.as_str(), "Unset");
        assert_eq!(OtelStatus::Ok.as_str(), "Ok");
        assert_eq!(OtelStatus::Error.as_str(), "Error");
    }
}
