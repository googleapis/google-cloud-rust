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

use crate::observability::attributes::*;
use crate::observability::errors::ErrorType;
use crate::options::InstrumentationClientInfo;
use gax::options::RequestOptions;
use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
use tracing::{Span, field};

/// Creates a new tracing span for an HTTP request attempt.
///
/// Populates the span with attributes available before the request is sent,
/// adhering to OpenTelemetry semantic conventions.
pub(crate) fn create_http_attempt_span(
    request: &reqwest::Request,
    options: &RequestOptions,
    instrumentation: Option<&'static InstrumentationClientInfo>,
    prior_attempt_count: u32,
) -> Span {
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

    let (gcp_client_service, gcp_client_version, gcp_client_artifact, url_domain) = instrumentation
        .map_or((None, None, None, None), |info| {
            (
                Some(info.service_name),
                Some(info.client_version),
                Some(info.client_artifact),
                Some(info.default_host),
            )
        });

    tracing::info_span!(
        "http_request",
        { KEY_OTEL_NAME } = otel_name,
        { KEY_OTEL_KIND } = "Client",
        { otel_trace::RPC_SYSTEM } = "http",
        { otel_trace::HTTP_REQUEST_METHOD } = method.as_str(),
        { otel_trace::SERVER_ADDRESS } = url
            .host_str()
            .map(|h| h.trim_start_matches('[').trim_end_matches(']'))
            .unwrap_or(""),
        { otel_trace::SERVER_PORT } = url.port_or_known_default().map(|p| p as i64).unwrap_or(0),
        { otel_trace::URL_FULL } = url.as_str(),
        { otel_trace::URL_SCHEME } = url.scheme(),
        { otel_attr::URL_TEMPLATE } = url_template,
        { otel_attr::URL_DOMAIN } = url_domain,
        { KEY_GCP_CLIENT_SERVICE } = gcp_client_service,
        { KEY_GCP_CLIENT_VERSION } = gcp_client_version,
        { KEY_GCP_CLIENT_REPO } = "googleapis/google-cloud-rust",
        { KEY_GCP_CLIENT_ARTIFACT } = gcp_client_artifact,
        { otel_trace::HTTP_REQUEST_RESEND_COUNT } = http_request_resend_count,
        // Fields to be recorded later
        { KEY_OTEL_STATUS } = OtelStatus::Unset.as_str(), // Initial state
        { otel_trace::HTTP_RESPONSE_STATUS_CODE } = field::Empty,
        { otel_trace::ERROR_TYPE } = field::Empty,
        { otel_attr::RPC_GRPC_STATUS_CODE } = field::Empty,
        { KEY_GRPC_STATUS } = field::Empty,
    )
}

/// Records additional attributes to the span based on the response outcome.
pub(crate) fn record_http_response_attributes(
    span: &Span,
    result: &Result<reqwest::Response, reqwest::Error>,
) {
    match result {
        Ok(response) => {
            let status = response.status();
            span.record(
                otel_trace::HTTP_RESPONSE_STATUS_CODE,
                status.as_u16() as i64,
            );
            if status.is_success() {
                span.record(KEY_OTEL_STATUS, OtelStatus::Ok.as_str());
            } else {
                span.record(KEY_OTEL_STATUS, OtelStatus::Error.as_str());
                // TODO(#3239): Extract reason from response headers/body if available
                let error_type = ErrorType::HttpError {
                    code: status,
                    reason: None,
                };
                span.record(otel_trace::ERROR_TYPE, error_type.as_str());
                span.record(
                    otel_attr::RPC_GRPC_STATUS_CODE,
                    error_type.grpc_code() as i64,
                );
                span.record(KEY_GRPC_STATUS, error_type.grpc_status());
            }
        }
        Err(err) => {
            span.record(KEY_OTEL_STATUS, OtelStatus::Error.as_str());
            let error_type = ErrorType::from_reqwest_error(err);
            span.record(otel_trace::ERROR_TYPE, error_type.as_str());
            span.record(
                otel_attr::RPC_GRPC_STATUS_CODE,
                error_type.grpc_code() as i64,
            );
            span.record(KEY_GRPC_STATUS, error_type.grpc_status());
        }
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
        let guard = TestLayer::initialize();
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = gax::options::internal::set_path_template(RequestOptions::default(), "/test");
        const INFO: InstrumentationClientInfo = InstrumentationClientInfo {
            service_name: "test.service",
            client_version: "1.2.3",
            client_artifact: "google-cloud-test",
            default_host: "example.com",
        };
        let _span = create_http_attempt_span(&request, &options, Some(&INFO), 1);

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

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;
        assert_eq!(
            *attributes, expected_attributes,
            "captured spans: {:?}",
            captured
        );
    }

    #[tokio::test]
    async fn test_create_span_attributes_optional() {
        let guard = TestLayer::initialize();
        let request =
            reqwest::Request::new(Method::POST, "http://localhost:8080/".parse().unwrap());
        let options = RequestOptions::default(); // No path template
        // No InstrumentationClientInfo
        let _span = create_http_attempt_span(&request, &options, None, 0);

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

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;
        assert_eq!(
            *attributes, expected_attributes,
            "captured spans: {:?}",
            captured
        );
    }

    #[tokio::test]
    async fn test_record_response_attributes_ok() {
        let guard = TestLayer::initialize();
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();
        let span = create_http_attempt_span(&request, &options, None, 0);
        let _enter = span.enter();

        let result = Ok(reqwest::Response::from(
            http::Response::builder().status(200).body("").unwrap(),
        ));
        record_http_response_attributes(&span, &result);

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

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;
        assert_eq!(
            *attributes, expected_attributes,
            "captured spans: {:?}",
            captured
        );
    }
    #[tokio::test]
    async fn test_record_response_attributes_error() {
        let guard = TestLayer::initialize();
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();
        let span = create_http_attempt_span(&request, &options, None, 0);
        let _enter = span.enter();

        // Simulate a timeout error
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(1))
            .build()
            .unwrap();
        let error_result = client.execute(request).await;
        assert!(error_result.is_err(), "error_result: {:?}", error_result);
        record_http_response_attributes(&span, &error_result);

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
            (otel_trace::ERROR_TYPE, "CLIENT_TIMEOUT"),
            (otel_attr::RPC_GRPC_STATUS_CODE, "4"), // DEADLINE_EXCEEDED
            (KEY_GRPC_STATUS, "DEADLINE_EXCEEDED"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;
        assert_eq!(
            *attributes, expected_attributes,
            "captured spans: {:?}",
            captured
        );
    }

    #[tokio::test]
    async fn test_record_response_attributes_http_error() {
        let guard = TestLayer::initialize();
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();
        let span = create_http_attempt_span(&request, &options, None, 0);
        let _enter = span.enter();

        let result = Ok(reqwest::Response::from(
            http::Response::builder().status(404).body("").unwrap(),
        ));
        record_http_response_attributes(&span, &result);

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
            (otel_trace::HTTP_RESPONSE_STATUS_CODE, "404"),
            (otel_trace::ERROR_TYPE, "404"),
            (otel_attr::RPC_GRPC_STATUS_CODE, "5"), // NOT_FOUND
            (KEY_GRPC_STATUS, "NOT_FOUND"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;
        assert_eq!(
            *attributes, expected_attributes,
            "captured spans: {:?}",
            captured
        );
    }

    #[tokio::test]
    async fn test_retry_attribute() {
        let guard = TestLayer::initialize();
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();

        // First attempt
        let _span1 = create_http_attempt_span(&request, &options, None, 0);
        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        assert!(
            !captured[0]
                .attributes
                .contains_key(otel_trace::HTTP_REQUEST_RESEND_COUNT),
            "captured spans: {:?}",
            captured
        );

        // First retry
        let _span2 = create_http_attempt_span(&request, &options, None, 1);
        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        assert_eq!(
            captured[0]
                .attributes
                .get(otel_trace::HTTP_REQUEST_RESEND_COUNT)
                .unwrap(),
            "1",
            "captured spans: {:?}",
            captured
        );

        // Second retry
        let _span3 = create_http_attempt_span(&request, &options, None, 2);
        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        assert_eq!(
            captured[0]
                .attributes
                .get(otel_trace::HTTP_REQUEST_RESEND_COUNT)
                .unwrap(),
            "2",
            "captured spans: {:?}",
            captured
        );
    }
}
