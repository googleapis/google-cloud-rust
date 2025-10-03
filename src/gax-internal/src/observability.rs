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

#![allow(dead_code)] // TODO(#3239): Remove once used in http.rs

use crate::options::InstrumentationClientInfo;
use gax::options::RequestOptions;
use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
use tracing::{Span, field};

// OpenTelemetry Semantic Convention Keys
// See https://opentelemetry.io/docs/specs/semconv/http/http-spans/

/// Span Kind for OpenTelemetry interop.
///
/// Always "Client" for a span representing an outbound HTTP request.
const KEY_OTEL_KIND: &str = "otel.kind";
/// Span Name for OpenTelemetry interop.
///
/// If `url.template` is available use "{http.request.method} {url.template}", otherwise use "{http.request.method}".
const KEY_OTEL_NAME: &str = "otel.name";
/// Span Status for OpenTelemetry interop.
///
/// Use "Error" for unrecoverable errors like network issues or 5xx status codes.
/// Otherwise, leave "Unset" (including for 4xx codes on CLIENT spans).
const KEY_OTEL_STATUS: &str = "otel.status";

// Custom GCP Attributes
/// The Google Cloud service name.
///
/// Examples: appengine, run, firestore
const KEY_GCP_CLIENT_SERVICE: &str = "gcp.client.service";
/// The client library version.
///
/// Example: v1.0.2
const KEY_GCP_CLIENT_VERSION: &str = "gcp.client.version";
/// The client library repository.
///
/// Always "googleapis/google-cloud-rust".
const KEY_GCP_CLIENT_REPO: &str = "gcp.client.repo";
/// The client library crate name.
///
/// Example: google-cloud-storage
const KEY_GCP_CLIENT_ARTIFACT: &str = "gcp.client.artifact";

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

/// Populate attributes of tracing spans for HTTP requests.
///
/// OpenTelemetry recommends a number of semantic conventions for
/// tracing HTTP requests. This type holds the information extracted
/// from HTTP requests and responses, formatted to align with these
/// OpenTelemetry semantic conventions.
/// See [OpenTelemetry Semantic Conventions for HTTP](https://opentelemetry.io/docs/specs/semconv/http/http-spans/).
#[derive(Debug, Clone)]
pub(crate) struct HttpSpanInfo {
    // Attributes for OpenTelemetry SDK interop
    /// The span kind for OpenTelemetry interop.
    ///
    /// Always "Client" for a span representing an outbound HTTP request.
    otel_kind: String,
    /// The span name for OpenTelemetry interop.
    ///
    /// If `url.template` is available use "{http.request.method} {url.template}", otherwise use "{http.request.method}".
    otel_name: String,
    /// The span status for OpenTelemetry interop.
    ///
    /// Use "Error" for unrecoverable errors like network issues or 5xx status codes.
    /// Otherwise, leave "Unset" (including for 4xx codes on CLIENT spans).
    otel_status: OtelStatus,

    // OpenTelemetry Semantic Conventions
    /// The RPC system used.
    ///
    /// Always "http" for REST calls.
    rpc_system: String,
    /// The HTTP request method.
    ///
    /// Examples: GET, POST, HEAD.
    http_request_method: String,
    /// The destination host name or IP address.
    ///
    /// Examples: myservice.googleapis.com, myservice-staging.sandbox.googleapis.com, 10.0.0.1
    server_address: String,
    /// The destination port number.
    ///
    /// Examples: 443, 8080
    server_port: i64,
    /// The absolute URL of the request.
    ///
    /// Example: https://www.foo.bar/search?q=OpenTelemetry
    url_full: String,
    /// The URI scheme component.
    ///
    /// Examples: http, https
    url_scheme: Option<String>,
    /// The low-cardinality template of the absolute path.
    ///
    /// Example: /v2/locations/{location}/projects/{project}/
    url_template: Option<&'static str>,
    /// The nominal domain from the original URL.
    ///
    /// Example: myservice.googleapis.com
    url_domain: Option<String>,

    /// The numeric HTTP response status code.
    ///
    /// Examples: 200, 404, 500
    http_response_status_code: Option<i64>,
    /// A low-cardinality classification of the error.
    ///
    /// For HTTP status codes >= 400, this is the status code as a string.
    /// For network errors, use a short identifier like TIMEOUT, CONNECTION_ERROR.
    error_type: Option<String>,
    /// The ordinal number of times this request has been resent.
    ///
    /// None for the first attempt.
    http_request_resend_count: Option<i64>,

    // Custom GCP Attributes
    /// The Google Cloud service name.
    ///
    /// Examples: appengine, run, firestore
    gcp_client_service: Option<String>,
    /// The client library version.
    ///
    /// Example: v1.0.2
    gcp_client_version: Option<String>,
    /// The client library repository.
    ///
    /// Always "googleapis/google-cloud-rust".
    gcp_client_repo: String,
    /// The client library crate name.
    ///
    /// Example: google-cloud-storage
    gcp_client_artifact: Option<String>,
}

impl HttpSpanInfo {
    pub(crate) fn from_request(
        request: &reqwest::Request,
        options: &RequestOptions,
        instrumentation: Option<&'static InstrumentationClientInfo>,
        prior_attempt_count: u32,
    ) -> Self {
        let url = request.url();
        let method = request.method();

        let url_template = gax::options::internal::get_path_template(options);
        let otel_name = url_template.map_or_else(
            || method.to_string(),
            |template| format!("{} {}", method, template),
        );

        let http_request_resend_count = if prior_attempt_count > 0 {
            Some(prior_attempt_count as i64)
        } else {
            None
        };

        let (gcp_client_service, gcp_client_version, gcp_client_artifact, url_domain) =
            instrumentation.map_or((None, None, None, None), |info| {
                (
                    Some(info.service_name.to_string()),
                    Some(info.client_version.to_string()),
                    Some(info.client_artifact.to_string()),
                    Some(info.default_host.to_string()),
                )
            });

        Self {
            rpc_system: "http".to_string(),
            otel_kind: "Client".to_string(),
            otel_name,
            otel_status: OtelStatus::Unset,
            http_request_method: method.to_string(),
            server_address: url.host_str().map(String::from).unwrap_or_default(),
            server_port: url.port_or_known_default().map(|p| p as i64).unwrap_or(0),
            url_full: url.to_string(),
            url_scheme: Some(url.scheme().to_string()),
            url_template,
            url_domain,
            http_response_status_code: None,
            error_type: None,
            http_request_resend_count,
            gcp_client_service,
            gcp_client_version,
            gcp_client_repo: "googleapis/google-cloud-rust".to_string(),
            gcp_client_artifact,
        }
    }

    /// Updates the span info based on the outcome of the HTTP request.
    ///
    /// This method should be called after the request has completed, it will fill in any parts of
    /// the span that depend on the result of the request.
    pub(crate) fn update_from_response(
        &mut self,
        result: &Result<reqwest::Response, reqwest::Error>,
    ) {
        match result {
            Ok(response) => {
                self.http_response_status_code = Some(response.status().as_u16() as i64);
                if response.status().is_success() {
                    self.otel_status = OtelStatus::Ok;
                } else {
                    self.otel_status = OtelStatus::Error;
                    self.error_type = Some(response.status().as_u16().to_string());
                }
            }
            Err(err) => {
                self.otel_status = OtelStatus::Error;
                let name = match err {
                    e if e.is_timeout() => "TIMEOUT",
                    e if e.is_connect() => "CONNECTION_ERROR",
                    e if e.is_request() => "REQUEST_ERROR",
                    _ => "UNKNOWN",
                };
                self.error_type = Some(name.to_string());
            }
        }
    }

    /// Creates a new tracing span with attributes populated from the request.
    pub(crate) fn create_span(&self) -> Span {
        tracing::info_span!(
            "http_request",
            { KEY_OTEL_NAME } = self.otel_name,
            { KEY_OTEL_KIND } = self.otel_kind,
            { otel_trace::RPC_SYSTEM } = self.rpc_system,
            { otel_trace::HTTP_REQUEST_METHOD } = self.http_request_method,
            { otel_trace::SERVER_ADDRESS } = self.server_address,
            { otel_trace::SERVER_PORT } = self.server_port,
            { otel_trace::URL_FULL } = self.url_full,
            { otel_trace::URL_SCHEME } = self.url_scheme,
            { otel_attr::URL_TEMPLATE } = self.url_template,
            { otel_attr::URL_DOMAIN } = self.url_domain,
            { KEY_GCP_CLIENT_SERVICE } = self.gcp_client_service,
            { KEY_GCP_CLIENT_VERSION } = self.gcp_client_version,
            { KEY_GCP_CLIENT_REPO } = self.gcp_client_repo,
            { KEY_GCP_CLIENT_ARTIFACT } = self.gcp_client_artifact,
            { otel_trace::HTTP_REQUEST_RESEND_COUNT } = self.http_request_resend_count,
            // Fields to be recorded later
            { KEY_OTEL_STATUS } = self.otel_status.as_str(), // Initial state
            { otel_trace::HTTP_RESPONSE_STATUS_CODE } = field::Empty,
            { otel_trace::ERROR_TYPE } = field::Empty,
        )
    }

    /// Records additional attributes to the span based on the response outcome.
    pub(crate) fn record_response_attributes(&self, span: &Span) {
        span.record(KEY_OTEL_STATUS, self.otel_status.as_str());
        span.record(
            otel_trace::HTTP_RESPONSE_STATUS_CODE,
            self.http_response_status_code,
        );
        span.record(otel_trace::ERROR_TYPE, self.error_type.as_deref());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::InstrumentationClientInfo;
    use gax::options::RequestOptions;
    use google_cloud_test_utils::test_layer::TestLayer;
    use http::Method;
    use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
    use reqwest;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_create_span_attributes() {
        const TEST_ID: &str = "test_create_span_attributes";

        let _guard = TestLayer::initialize(TEST_ID);
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = gax::options::internal::set_path_template(RequestOptions::default(), "/test");
        const INFO: InstrumentationClientInfo = InstrumentationClientInfo {
            service_name: "test.service",
            client_version: "1.2.3",
            client_artifact: "google-cloud-test",
            default_host: "example.com",
        };
        let span_info = HttpSpanInfo::from_request(&request, &options, Some(&INFO), 1);
        let _span = span_info.create_span();

        let expected_attributes: HashMap<String, String> = [
            (KEY_OTEL_NAME, "GET /test"),
            (KEY_OTEL_KIND, "Client"),
            (otel_trace::RPC_SYSTEM, "http"),
            (otel_trace::HTTP_REQUEST_METHOD, "GET"),
            (otel_trace::SERVER_ADDRESS, "example.com"),
            (otel_trace::SERVER_PORT, "443"),
            (otel_trace::URL_FULL, "https://example.com/test"),
            (otel_trace::URL_SCHEME, "https"),
            (otel_attr::URL_TEMPLATE, "/test"),
            (otel_attr::URL_DOMAIN, "example.com"),
            (KEY_GCP_CLIENT_SERVICE, "test.service"),
            (KEY_GCP_CLIENT_VERSION, "1.2.3"),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust"),
            (KEY_GCP_CLIENT_ARTIFACT, "google-cloud-test"),
            (otel_trace::HTTP_REQUEST_RESEND_COUNT, "1"),
            (KEY_OTEL_STATUS, "Unset"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let captured = TestLayer::capture(TEST_ID);
        assert_eq!(captured.len(), 1);
        let attributes = &captured[0].attributes;
        assert_eq!(*attributes, expected_attributes);
    }

    #[tokio::test]
    async fn test_create_span_attributes_optional() {
        const TEST_ID: &str = "test_create_span_attributes_optional";

        let _guard = TestLayer::initialize(TEST_ID);
        let request =
            reqwest::Request::new(Method::POST, "http://localhost:8080/".parse().unwrap());
        let options = RequestOptions::default(); // No path template
        // No InstrumentationClientInfo
        let span_info = HttpSpanInfo::from_request(&request, &options, None, 0);
        let _span = span_info.create_span();

        let expected_attributes: HashMap<String, String> = [
            (KEY_OTEL_NAME, "POST"),
            (KEY_OTEL_KIND, "Client"),
            (otel_trace::RPC_SYSTEM, "http"),
            (otel_trace::HTTP_REQUEST_METHOD, "POST"),
            (otel_trace::SERVER_ADDRESS, "localhost"),
            (otel_trace::SERVER_PORT, "8080"),
            (otel_trace::URL_FULL, "http://localhost:8080/"),
            (otel_trace::URL_SCHEME, "http"),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust"),
            (KEY_OTEL_STATUS, "Unset"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let captured = TestLayer::capture(TEST_ID);
        assert_eq!(captured.len(), 1);
        let attributes = &captured[0].attributes;
        assert_eq!(*attributes, expected_attributes);
    }

    #[tokio::test]
    async fn test_record_response_attributes_ok() {
        const TEST_ID: &str = "test_record_response_attributes_ok";

        let _guard = TestLayer::initialize(TEST_ID);
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let mut span_info =
            HttpSpanInfo::from_request(&request, &RequestOptions::default(), None, 0);

        let span = span_info.create_span();
        let _enter = span.enter();
        span_info.update_from_response(&Ok(reqwest::Response::from(
            http::Response::builder().status(200).body("").unwrap(),
        )));
        span_info.record_response_attributes(&span);

        let expected_attributes: HashMap<String, String> = [
            (KEY_OTEL_NAME, "GET"),
            (KEY_OTEL_KIND, "Client"),
            (otel_trace::RPC_SYSTEM, "http"),
            (otel_trace::HTTP_REQUEST_METHOD, "GET"),
            (otel_trace::SERVER_ADDRESS, "example.com"),
            (otel_trace::SERVER_PORT, "443"),
            (otel_trace::URL_FULL, "https://example.com/test"),
            (otel_trace::URL_SCHEME, "https"),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust"),
            (KEY_OTEL_STATUS, "Ok"), // Updated
            (otel_trace::HTTP_RESPONSE_STATUS_CODE, "200"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let captured = TestLayer::capture(TEST_ID);
        assert_eq!(captured.len(), 1);
        let attributes = &captured[0].attributes;
        assert_eq!(*attributes, expected_attributes);
    }

    #[tokio::test]
    async fn test_record_response_attributes_error() {
        const TEST_ID: &str = "test_record_response_attributes_error";

        let _guard = TestLayer::initialize(TEST_ID);
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let mut span_info =
            HttpSpanInfo::from_request(&request, &RequestOptions::default(), None, 0);

        let span = span_info.create_span();
        let _enter = span.enter();
        // Simulate a timeout error
        span_info.otel_status = OtelStatus::Error;
        span_info.error_type = Some("TIMEOUT".to_string());
        span_info.record_response_attributes(&span);

        let expected_attributes: HashMap<String, String> = [
            (KEY_OTEL_NAME, "GET"),
            (KEY_OTEL_KIND, "Client"),
            (otel_trace::RPC_SYSTEM, "http"),
            (otel_trace::HTTP_REQUEST_METHOD, "GET"),
            (otel_trace::SERVER_ADDRESS, "example.com"),
            (otel_trace::SERVER_PORT, "443"),
            (otel_trace::URL_FULL, "https://example.com/test"),
            (otel_trace::URL_SCHEME, "https"),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust"),
            (KEY_OTEL_STATUS, "Error"), // Updated
            (otel_trace::ERROR_TYPE, "TIMEOUT"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let captured = TestLayer::capture(TEST_ID);
        assert_eq!(captured.len(), 1);
        let attributes = &captured[0].attributes;
        assert_eq!(*attributes, expected_attributes);
    }

    #[tokio::test]
    async fn test_http_span_info_from_request_basic() {
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();

        let span_info = HttpSpanInfo::from_request(&request, &options, None, 0);

        assert_eq!(span_info.rpc_system, "http");
        assert_eq!(span_info.otel_kind, "Client");
        assert_eq!(span_info.otel_name, "GET");
        assert_eq!(span_info.otel_status, OtelStatus::Unset);
        assert_eq!(span_info.http_request_method, "GET");
        assert_eq!(span_info.server_address, "example.com".to_string());
        assert_eq!(span_info.server_port, 443);
        assert_eq!(span_info.url_full, "https://example.com/test");
        assert_eq!(span_info.url_scheme, Some("https".to_string()));
        assert_eq!(span_info.url_template, None);
        assert_eq!(span_info.url_domain, None);
        assert_eq!(span_info.http_response_status_code, None);
        assert_eq!(span_info.error_type, None);
        assert_eq!(span_info.http_request_resend_count, None);
        assert_eq!(span_info.gcp_client_service, None);
        assert_eq!(span_info.gcp_client_version, None);
        assert_eq!(span_info.gcp_client_repo, "googleapis/google-cloud-rust");
        assert_eq!(span_info.gcp_client_artifact, None);
    }

    #[tokio::test]
    async fn test_http_span_info_from_request_with_instrumentation() {
        let request = reqwest::Request::new(
            Method::POST,
            "https://test.service.dev:443/v1/items".parse().unwrap(),
        );
        let options = RequestOptions::default();
        const INFO: InstrumentationClientInfo = InstrumentationClientInfo {
            service_name: "test.service",
            client_version: "1.2.3",
            client_artifact: "google-cloud-test",
            default_host: "test.service.dev",
        };

        let span_info = HttpSpanInfo::from_request(&request, &options, Some(&INFO), 0);

        assert_eq!(
            span_info.gcp_client_service,
            Some("test.service".to_string())
        );
        assert_eq!(span_info.gcp_client_version, Some("1.2.3".to_string()));
        assert_eq!(
            span_info.gcp_client_artifact,
            Some("google-cloud-test".to_string())
        );
        assert_eq!(span_info.url_domain, Some("test.service.dev".to_string()));
        assert_eq!(span_info.server_address, "test.service.dev".to_string());
        assert_eq!(span_info.server_port, 443);
    }

    #[tokio::test]
    async fn test_http_span_info_from_request_with_path_template() {
        let request = reqwest::Request::new(
            Method::GET,
            "https://example.com/items/123".parse().unwrap(),
        );
        let options = gax::options::internal::set_path_template(
            RequestOptions::default(),
            "/items/{item_id}",
        );

        let span_info = HttpSpanInfo::from_request(&request, &options, None, 0);

        assert_eq!(span_info.url_template, Some("/items/{item_id}"));
        assert_eq!(span_info.otel_name, "GET /items/{item_id}");
    }

    #[tokio::test]
    async fn test_http_span_info_from_request_with_prior_attempt_count() {
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();

        // prior_attempt_count is 0 for the first try
        let span_info = HttpSpanInfo::from_request(&request, &options, None, 0);
        assert_eq!(span_info.http_request_resend_count, None);

        // prior_attempt_count is 1 for the second try (first retry)
        let span_info = HttpSpanInfo::from_request(&request, &options, None, 1);
        assert_eq!(span_info.http_request_resend_count, Some(1));

        let span_info = HttpSpanInfo::from_request(&request, &options, None, 5);
        assert_eq!(span_info.http_request_resend_count, Some(5));
    }

    #[tokio::test]
    async fn test_update_from_response_success() {
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let mut span_info =
            HttpSpanInfo::from_request(&request, &RequestOptions::default(), None, 0);

        let response =
            reqwest::Response::from(http::Response::builder().status(200).body("").unwrap());
        span_info.update_from_response(&Ok(response));

        assert_eq!(span_info.otel_status, OtelStatus::Ok);
        assert_eq!(span_info.http_response_status_code, Some(200));
        assert_eq!(span_info.error_type, None);
    }

    #[tokio::test]
    async fn test_update_from_response_http_error() {
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let mut span_info =
            HttpSpanInfo::from_request(&request, &RequestOptions::default(), None, 0);

        let response =
            reqwest::Response::from(http::Response::builder().status(404).body("").unwrap());
        span_info.update_from_response(&Ok(response));

        assert_eq!(span_info.otel_status, OtelStatus::Error);
        assert_eq!(span_info.http_response_status_code, Some(404));
        assert_eq!(span_info.error_type, Some("404".to_string()));
    }

    #[test]
    fn test_otel_status_as_str() {
        assert_eq!(OtelStatus::Unset.as_str(), "Unset");
        assert_eq!(OtelStatus::Ok.as_str(), "Ok");
        assert_eq!(OtelStatus::Error.as_str(), "Error");
    }
}
