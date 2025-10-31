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
        { otel_trace::HTTP_RESPONSE_BODY_SIZE } = field::Empty,
        { otel_trace::ERROR_TYPE } = field::Empty,
        { otel_attr::RPC_GRPC_STATUS_CODE } = field::Empty,
        { KEY_GRPC_STATUS } = field::Empty,
    )
}

/// Records additional attributes to the span based on the intermediate gax::Result.
/// This should be called *before* the response body is downloaded and decoded.
pub(crate) fn record_http_response_attributes(
    span: &Span,
    result: &gax::Result<reqwest::Response>,
) {
    match result {
        Ok(response) => {
            span.record(
                otel_trace::HTTP_RESPONSE_STATUS_CODE,
                response.status().as_u16() as i64,
            );

            if response.status().is_success() {
                span.record(KEY_OTEL_STATUS, OtelStatus::Ok.as_str());
            } else {
                span.record(KEY_OTEL_STATUS, OtelStatus::Error.as_str());
                // For HTTP errors (4xx, 5xx) that haven't been converted to gax::Error yet,
                // we use the status code as a fallback error.type.
                // Real error parsing happens later, but we want to end the span now.
                span.record(otel_trace::ERROR_TYPE, response.status().as_str());
            }

            if let Some(content_length) = response.headers().get(http::header::CONTENT_LENGTH) {
                if let Ok(content_length_str) = content_length.to_str() {
                    if let Ok(size) = content_length_str.parse::<i64>() {
                        span.record(otel_trace::HTTP_RESPONSE_BODY_SIZE, size);
                    }
                }
            }
        }
        Err(err) => {
            span.record(KEY_OTEL_STATUS, OtelStatus::Error.as_str());
            // If we have a gax::Error here, it means something failed *before* we got a response
            // (e.g., connection error, timeout during request send).
            if let Some(status) = err.http_status_code() {
                span.record(otel_trace::HTTP_RESPONSE_STATUS_CODE, status as i64);
            }
            let error_type = ErrorType::from_gax_error(err);
            span.record(otel_trace::ERROR_TYPE, error_type.as_str());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::InstrumentationClientInfo;
    use gax::options::RequestOptions;
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer};
    use http::Method;
    use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
    use reqwest::{self, StatusCode};
    use std::collections::HashMap;
    use test_case::test_case;

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

        let expected_attributes: HashMap<String, AttributeValue> = [
            (KEY_OTEL_NAME, "GET /test".into()),
            (KEY_OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "GET".into()),
            (otel_trace::SERVER_ADDRESS, "example.com".into()),
            (otel_trace::SERVER_PORT, 443_i64.into()),
            (otel_trace::URL_FULL, "https://example.com/test".into()),
            (otel_trace::URL_SCHEME, "https".into()),
            (otel_attr::URL_TEMPLATE, "/test".into()),
            (otel_attr::URL_DOMAIN, "example.com".into()),
            (KEY_GCP_CLIENT_SERVICE, "test.service".into()),
            (KEY_GCP_CLIENT_VERSION, "1.2.3".into()),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (KEY_GCP_CLIENT_ARTIFACT, "google-cloud-test".into()),
            (otel_trace::HTTP_REQUEST_RESEND_COUNT, 1_i64.into()),
            (KEY_OTEL_STATUS, "Unset".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
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

        let expected_attributes: HashMap<String, AttributeValue> = [
            (KEY_OTEL_NAME, "POST".into()),
            (KEY_OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "POST".into()),
            (otel_trace::SERVER_ADDRESS, "localhost".into()),
            (otel_trace::SERVER_PORT, 8080_i64.into()),
            (otel_trace::URL_FULL, "http://localhost:8080/".into()),
            (otel_trace::URL_SCHEME, "http".into()),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (KEY_OTEL_STATUS, "Unset".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
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

    #[test_case(StatusCode::OK; "OK")]
    #[test_case(StatusCode::CREATED; "Created")]
    #[tokio::test]
    async fn test_record_response_attributes_ok(status_code: StatusCode) {
        let guard = TestLayer::initialize();
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();
        let span = create_http_attempt_span(&request, &options, None, 0);
        let _enter = span.enter();

        let response = http::Response::builder()
            .status(status_code)
            .body("")
            .unwrap();
        // We are now passing gax::Result<reqwest::Response>
        let result: gax::Result<reqwest::Response> = Ok(response.into());
        record_http_response_attributes(&span, &result);

        let expected_attributes: HashMap<String, AttributeValue> = [
            (KEY_OTEL_NAME, "GET".into()),
            (KEY_OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "GET".into()),
            (otel_trace::SERVER_ADDRESS, "example.com".into()),
            (otel_trace::SERVER_PORT, 443_i64.into()),
            (otel_trace::URL_FULL, "https://example.com/test".into()),
            (otel_trace::URL_SCHEME, "https".into()),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (KEY_OTEL_STATUS, "Ok".into()),
            (
                otel_trace::HTTP_RESPONSE_STATUS_CODE,
                (status_code.as_u16() as i64).into(),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
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

        // Simulate a timeout error as a gax::Error
        let error_result: gax::Result<reqwest::Response> =
            Err(gax::error::Error::timeout("test timeout"));
        record_http_response_attributes(&span, &error_result);

        let expected_attributes: HashMap<String, AttributeValue> = [
            (KEY_OTEL_NAME, "GET".into()),
            (KEY_OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "GET".into()),
            (otel_trace::SERVER_ADDRESS, "example.com".into()),
            (otel_trace::SERVER_PORT, 443_i64.into()),
            (otel_trace::URL_FULL, "https://example.com/test".into()),
            (otel_trace::URL_SCHEME, "https".into()),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (KEY_OTEL_STATUS, "Error".into()),
            (otel_trace::ERROR_TYPE, "CLIENT_TIMEOUT".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
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

    #[test_case(StatusCode::BAD_REQUEST, "400"; "Bad Request")]
    #[test_case(StatusCode::UNAUTHORIZED, "401"; "Unauthorized")]
    #[test_case(StatusCode::FORBIDDEN, "403"; "Forbidden")]
    #[test_case(StatusCode::NOT_FOUND, "404"; "Not Found")]
    #[test_case(StatusCode::INTERNAL_SERVER_ERROR, "500"; "Internal Server Error")]
    #[test_case(StatusCode::SERVICE_UNAVAILABLE, "503"; "Service Unavailable")]
    #[tokio::test]
    async fn test_record_response_attributes_http_error(
        status_code: StatusCode,
        expected_error_type: &str,
    ) {
        let guard = TestLayer::initialize();
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();
        let span = create_http_attempt_span(&request, &options, None, 0);
        let _enter = span.enter();

        // In the new model, a 4xx/5xx response is still an Ok(reqwest::Response)
        // at the point we record attributes.
        let response = http::Response::builder()
            .status(status_code)
            .body("")
            .unwrap();
        let result: gax::Result<reqwest::Response> = Ok(response.into());

        record_http_response_attributes(&span, &result);

        let expected_attributes: HashMap<String, AttributeValue> = [
            (KEY_OTEL_NAME, "GET".into()),
            (KEY_OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "GET".into()),
            (otel_trace::SERVER_ADDRESS, "example.com".into()),
            (otel_trace::SERVER_PORT, 443_i64.into()),
            (otel_trace::URL_FULL, "https://example.com/test".into()),
            (otel_trace::URL_SCHEME, "https".into()),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (KEY_OTEL_STATUS, "Error".into()),
            (
                otel_trace::HTTP_RESPONSE_STATUS_CODE,
                (status_code.as_u16() as i64).into(),
            ),
            // We expect the status code string as error type here because it hasn't been
            // converted to a full gax::Error with potentially richer details yet.
            (otel_trace::ERROR_TYPE, expected_error_type.into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
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
                .get(otel_trace::HTTP_REQUEST_RESEND_COUNT),
            Some(&1_i64.into()),
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
                .get(otel_trace::HTTP_REQUEST_RESEND_COUNT),
            Some(&2_i64.into()),
            "captured spans: {:?}",
            captured
        );
    }
}
