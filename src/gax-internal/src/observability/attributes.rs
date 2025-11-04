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

pub mod keys {
    /// Span Kind for OpenTelemetry interop.
    ///
    /// Always "Client" for a span representing an outbound HTTP request.
    pub const OTEL_KIND: &str = "otel.kind";
    /// Span Name for OpenTelemetry interop.
    ///
    /// If `url.template` is available use "{http.request.method} {url.template}", otherwise use "{http.request.method}".
    pub const OTEL_NAME: &str = "otel.name";
    /// Span Status Code for OpenTelemetry interop.
    ///
    /// Must be one of "UNSET", "OK", or "ERROR".
    pub const OTEL_STATUS_CODE: &str = "otel.status_code";
    /// Span Status Description for OpenTelemetry interop.
    ///
    /// A human-readable description of the status, used when status_code is "ERROR".
    pub const OTEL_STATUS_DESCRIPTION: &str = "otel.status_description";

    /// The string representation of the gRPC status code.
    pub const GRPC_STATUS: &str = "grpc.status";

    // Custom GCP Attributes
    /// The Google Cloud service name.
    ///
    /// Examples: appengine, run, firestore
    pub const GCP_CLIENT_SERVICE: &str = "gcp.client.service";
    /// The client library version.
    ///
    /// Example: v1.0.2
    pub const GCP_CLIENT_VERSION: &str = "gcp.client.version";
    /// The client library repository.
    ///
    /// Always "googleapis/google-cloud-rust".
    pub const GCP_CLIENT_REPO: &str = "gcp.client.repo";
    /// The client library crate name.
    ///
    /// Example: google-cloud-storage
    pub const GCP_CLIENT_ARTIFACT: &str = "gcp.client.artifact";
    /// The client library language.
    ///
    /// Always "rust".
    pub const GCP_CLIENT_LANGUAGE: &str = "gcp.client.language";
    /// The Google Cloud resource name.
    ///
    /// Example: //pubsub.googleapis.com/projects/my-project/topics/my-topic
    pub const GCP_RESOURCE_NAME: &str = "gcp.resource.name";
}

/// Value for [keys::OTEL_KIND].
pub const OTEL_KIND_CLIENT: &str = "Client";
/// Value for `rpc.system`.
pub const RPC_SYSTEM_HTTP: &str = "http";
/// Value for [keys::GCP_CLIENT_REPO].
pub const GCP_CLIENT_REPO_GOOGLEAPIS: &str = "googleapis/google-cloud-rust";
/// Value for [keys::GCP_CLIENT_LANGUAGE].
pub const GCP_CLIENT_LANGUAGE_RUST: &str = "rust";

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
    /// Unknown error type.
    pub const UNKNOWN: &str = "UNKNOWN";
}

/// Values for the OpenTelemetry `otel.status_code` attribute.
pub mod otel_status_codes {
    /// The operation has been validated by an Application developer or Operator to have completed successfully.
    pub const OK: &str = "OK";
    /// The operation contains an error.
    pub const ERROR: &str = "ERROR";
    /// The default status.
    pub const UNSET: &str = "UNSET";
}
