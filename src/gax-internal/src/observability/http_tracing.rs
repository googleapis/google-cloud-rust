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

use crate::observability::attributes::keys::*;
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
        { OTEL_NAME } = otel_name,
        { OTEL_KIND } = OTEL_KIND_CLIENT,
        { otel_trace::RPC_SYSTEM } = RPC_SYSTEM_HTTP,
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
        { GCP_CLIENT_SERVICE } = gcp_client_service,
        { GCP_CLIENT_VERSION } = gcp_client_version,
        { GCP_CLIENT_REPO } = GCP_CLIENT_REPO_GOOGLEAPIS,
        { GCP_CLIENT_ARTIFACT } = gcp_client_artifact,
        { GCP_CLIENT_LANGUAGE } = GCP_CLIENT_LANGUAGE_RUST,
        { otel_trace::HTTP_REQUEST_RESEND_COUNT } = http_request_resend_count,
        // Fields to be recorded later
        { OTEL_STATUS_CODE } = otel_status_codes::UNSET, // Initial state
        { OTEL_STATUS_DESCRIPTION } = field::Empty,
        { otel_trace::HTTP_RESPONSE_STATUS_CODE } = field::Empty,
        { otel_trace::HTTP_RESPONSE_BODY_SIZE } = field::Empty,
        { otel_trace::ERROR_TYPE } = field::Empty,
        { otel_attr::RPC_GRPC_STATUS_CODE } = field::Empty,
        { GRPC_STATUS } = field::Empty,
    )
}

/// Records additional attributes to the span based on the intermediate gax::Result.
/// This should be called *before* the response body is downloaded and decoded and
/// *after* any errors are processed.
pub(crate) fn record_http_response_attributes(
    span: &Span,
    result: Result<&reqwest::Response, &gax::error::Error>,
) {
    match result {
        Ok(response) => {
            span.record(
                otel_trace::HTTP_RESPONSE_STATUS_CODE,
                response.status().as_u16() as i64,
            );

            if let Some(content_length) = response.headers().get(http::header::CONTENT_LENGTH) {
                if let Ok(content_length_str) = content_length.to_str() {
                    if let Ok(size) = content_length_str.parse::<i64>() {
                        span.record(otel_trace::HTTP_RESPONSE_BODY_SIZE, size);
                    }
                }
            }
        }
        Err(err) => {
            span.record(OTEL_STATUS_CODE, otel_status_codes::ERROR);
            if let Some(status) = err.http_status_code() {
                span.record(otel_trace::HTTP_RESPONSE_STATUS_CODE, status as i64);
            }
            let error_type = ErrorType::from_gax_error(err);
            span.record(otel_trace::ERROR_TYPE, error_type.as_str());
            // TODO(#3239): clean up error messages
            span.record(OTEL_STATUS_DESCRIPTION, err.to_string());
        }
    }
}

/// Records HTTP transport attributes on the current span.
///
/// This function is used to enrich the Client Request Span (T3) with attributes
/// from the transport attempt that are only available before the response is consumed.
pub(crate) fn record_intermediate_client_request(
    result: Result<&reqwest::Response, &gax::error::Error>,
    prior_attempt_count: u32,
) {
    let span = Span::current();
    if span.is_disabled() {
        return;
    }

    // Only enrich spans that are explicitly marked as GAX client spans.
    // This prevents accidental enrichment of user-provided spans that happen to have the same fields.
    if let Some(metadata) = span.metadata() {
        if metadata.fields().field("gax.client.span").is_none() {
            return;
        }
    } else {
        return;
    }

    match result {
        Ok(response) => {
            let url = response.url();
            span.record(
                otel_trace::SERVER_ADDRESS,
                url.host_str()
                    .map(|h| h.trim_start_matches('[').trim_end_matches(']'))
                    .unwrap_or(""),
            );
            span.record(
                otel_trace::SERVER_PORT,
                url.port_or_known_default().map(|p| p as i64).unwrap_or(0),
            );
            span.record(otel_trace::URL_FULL, url.as_str());
            span.record(
                otel_trace::HTTP_RESPONSE_STATUS_CODE,
                response.status().as_u16() as i64,
            );
        }
        Err(err) => {
            if let Some(status) = err.http_status_code() {
                span.record(otel_trace::HTTP_RESPONSE_STATUS_CODE, status as i64);
            }
            // For errors, we might not have the final URL if the request failed before sending.
            // We rely on the initial URL set on the T3 span.
        }
    }

    if prior_attempt_count > 0 {
        span.record(
            otel_trace::HTTP_REQUEST_RESEND_COUNT,
            prior_attempt_count as i64,
        );
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
            (OTEL_NAME, "GET /test".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "GET".into()),
            (otel_trace::SERVER_ADDRESS, "example.com".into()),
            (otel_trace::SERVER_PORT, 443_i64.into()),
            (otel_trace::URL_FULL, "https://example.com/test".into()),
            (otel_trace::URL_SCHEME, "https".into()),
            (otel_attr::URL_TEMPLATE, "/test".into()),
            (otel_attr::URL_DOMAIN, "example.com".into()),
            (GCP_CLIENT_SERVICE, "test.service".into()),
            (GCP_CLIENT_VERSION, "1.2.3".into()),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (GCP_CLIENT_ARTIFACT, "google-cloud-test".into()),
            (GCP_CLIENT_LANGUAGE, "rust".into()),
            (otel_trace::HTTP_REQUEST_RESEND_COUNT, 1_i64.into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;
        assert_eq!(*attributes, expected_attributes);
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
            (OTEL_NAME, "POST".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "POST".into()),
            (otel_trace::SERVER_ADDRESS, "localhost".into()),
            (otel_trace::SERVER_PORT, 8080_i64.into()),
            (otel_trace::URL_FULL, "http://localhost:8080/".into()),
            (otel_trace::URL_SCHEME, "http".into()),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (GCP_CLIENT_LANGUAGE, "rust".into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;
        assert_eq!(*attributes, expected_attributes);
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
        let reqwest_response: reqwest::Response = response.into();
        record_http_response_attributes(&span, Ok(&reqwest_response));

        let expected_attributes: HashMap<String, AttributeValue> = [
            (OTEL_NAME, "GET".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "GET".into()),
            (otel_trace::SERVER_ADDRESS, "example.com".into()),
            (otel_trace::SERVER_PORT, 443_i64.into()),
            (otel_trace::URL_FULL, "https://example.com/test".into()),
            (otel_trace::URL_SCHEME, "https".into()),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (GCP_CLIENT_LANGUAGE, "rust".into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
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
        assert_eq!(*attributes, expected_attributes);
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
        let error = gax::error::Error::timeout("test timeout");
        record_http_response_attributes(&span, Err(&error));

        let expected_attributes: HashMap<String, AttributeValue> = [
            (OTEL_NAME, "GET".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "GET".into()),
            (otel_trace::SERVER_ADDRESS, "example.com".into()),
            (otel_trace::SERVER_PORT, 443_i64.into()),
            (otel_trace::URL_FULL, "https://example.com/test".into()),
            (otel_trace::URL_SCHEME, "https".into()),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (GCP_CLIENT_LANGUAGE, "rust".into()),
            (OTEL_STATUS_CODE, "ERROR".into()),
            (otel_trace::ERROR_TYPE, "CLIENT_TIMEOUT".into()),
            (
                OTEL_STATUS_DESCRIPTION,
                "the request exceeded the request deadline test timeout".into(),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;
        assert_eq!(*attributes, expected_attributes);
    }

    #[test_case(StatusCode::BAD_REQUEST, "400", "the HTTP transport reports a [400] error: "; "Bad Request")]
    #[test_case(StatusCode::UNAUTHORIZED, "401", "the HTTP transport reports a [401] error: "; "Unauthorized")]
    #[test_case(StatusCode::FORBIDDEN, "403", "the HTTP transport reports a [403] error: "; "Forbidden")]
    #[test_case(StatusCode::NOT_FOUND, "404", "the HTTP transport reports a [404] error: "; "Not Found")]
    #[test_case(StatusCode::INTERNAL_SERVER_ERROR, "500", "the HTTP transport reports a [500] error: "; "Internal Server Error")]
    #[test_case(StatusCode::SERVICE_UNAVAILABLE, "503", "the HTTP transport reports a [503] error: "; "Service Unavailable")]
    #[tokio::test]
    async fn test_record_response_attributes_http_error(
        status_code: StatusCode,
        expected_error_type: &'static str,
        expected_description_prefix: &'static str,
    ) {
        let guard = TestLayer::initialize();
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();
        let span = create_http_attempt_span(&request, &options, None, 0);
        let _enter = span.enter();

        // Simulate what to_http_error would return: a gax::Error with HTTP metadata
        let error = gax::error::Error::http(
            status_code.as_u16(),
            http::HeaderMap::new(),
            bytes::Bytes::new(),
        );

        record_http_response_attributes(&span, Err(&error));

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;

        assert_eq!(
            attributes.get(OTEL_STATUS_CODE),
            Some(&"ERROR".into()),
            "{} mismatch, attrs: {:?}",
            OTEL_STATUS_CODE,
            attributes
        );
        assert_eq!(
            attributes.get(otel_trace::HTTP_RESPONSE_STATUS_CODE),
            Some(&(status_code.as_u16() as i64).into()),
            "{} mismatch, attrs: {:?}",
            otel_trace::HTTP_RESPONSE_STATUS_CODE,
            attributes
        );
        assert_eq!(
            attributes.get(otel_trace::ERROR_TYPE),
            Some(&expected_error_type.into()),
            "{} mismatch, attrs: {:?}",
            otel_trace::ERROR_TYPE,
            attributes
        );
        let description = attributes
            .get(OTEL_STATUS_DESCRIPTION)
            .unwrap_or_else(|| panic!("{} missing", OTEL_STATUS_DESCRIPTION))
            .as_string()
            .unwrap_or_else(|| panic!("{} not a string", OTEL_STATUS_DESCRIPTION));
        assert!(
            description.starts_with(expected_description_prefix),
            "{} '{}' does not start with '{}', attrs: {:?}",
            OTEL_STATUS_DESCRIPTION,
            description,
            expected_description_prefix,
            attributes
        );
    }

    #[tokio::test]
    async fn test_record_response_attributes_error_info() {
        let guard = TestLayer::initialize();
        let request =
            reqwest::Request::new(Method::GET, "https://example.com/test".parse().unwrap());
        let options = RequestOptions::default();
        let span = create_http_attempt_span(&request, &options, None, 0);
        let _enter = span.enter();

        let error_info = rpc::model::ErrorInfo::default()
            .set_reason("API_KEY_INVALID")
            .set_domain("googleapis.com");
        let status = gax::error::rpc::Status::default()
            .set_code(gax::error::rpc::Code::InvalidArgument)
            .set_message("Invalid API Key")
            .set_details(vec![gax::error::rpc::StatusDetails::ErrorInfo(error_info)]);
        let error = gax::error::Error::service(status);

        record_http_response_attributes(&span, Err(&error));

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;

        assert_eq!(
            attributes.get(OTEL_STATUS_CODE),
            Some(&"ERROR".into()),
            "{} mismatch, attrs: {:?}",
            OTEL_STATUS_CODE,
            attributes
        );
        assert_eq!(
            attributes.get(otel_trace::ERROR_TYPE),
            Some(&"API_KEY_INVALID".into()),
            "{} mismatch, attrs: {:?}",
            otel_trace::ERROR_TYPE,
            attributes
        );
        let description = attributes
            .get(OTEL_STATUS_DESCRIPTION)
            .unwrap_or_else(|| panic!("{} missing", OTEL_STATUS_DESCRIPTION))
            .as_string()
            .unwrap_or_else(|| panic!("{} not a string", OTEL_STATUS_DESCRIPTION));
        assert!(
            description.contains("Invalid API Key"),
            "{} '{}' does not contain 'Invalid API Key', attrs: {:?}",
            OTEL_STATUS_DESCRIPTION,
            description,
            attributes
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

    #[tokio::test]
    async fn test_record_intermediate_client_request() {
        let guard = TestLayer::initialize();
        let span = tracing::info_span!(
            "test_span",
            { otel_trace::SERVER_ADDRESS } = field::Empty,
            { otel_trace::SERVER_PORT } = field::Empty,
            { otel_trace::URL_FULL } = field::Empty,
            { otel_trace::HTTP_RESPONSE_STATUS_CODE } = field::Empty,
            { otel_trace::HTTP_REQUEST_RESEND_COUNT } = field::Empty,
            "gax.client.span" = field::Empty, // Add marker field
        );
        let _enter = span.enter();

        let response = http::Response::builder()
            .status(StatusCode::OK)
            .body("")
            .unwrap();
        let reqwest_response: reqwest::Response = response.into();
        record_intermediate_client_request(Ok(&reqwest_response), 1);

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;

        assert_eq!(
            attributes.get(otel_trace::HTTP_RESPONSE_STATUS_CODE),
            Some(&(200_i64).into())
        );
        assert_eq!(
            attributes.get(otel_trace::HTTP_REQUEST_RESEND_COUNT),
            Some(&(1_i64).into())
        );
    }

    #[tokio::test]
    async fn test_record_intermediate_client_request_no_marker() {
        let guard = TestLayer::initialize();
        let span = tracing::info_span!(
            "test_span",
            { otel_trace::HTTP_RESPONSE_STATUS_CODE } = field::Empty,
            { otel_trace::HTTP_REQUEST_RESEND_COUNT } = field::Empty,
            // Missing "gax.client.span" marker field
        );
        let _enter = span.enter();

        let response = http::Response::builder()
            .status(StatusCode::OK)
            .body("")
            .unwrap();
        let reqwest_response: reqwest::Response = response.into();
        record_intermediate_client_request(Ok(&reqwest_response), 1);

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;

        // Should NOT be recorded
        assert!(!attributes.contains_key(otel_trace::HTTP_RESPONSE_STATUS_CODE));
        assert!(!attributes.contains_key(otel_trace::HTTP_REQUEST_RESEND_COUNT));
    }

    #[tokio::test]
    async fn test_record_intermediate_client_request_error() {
        let guard = TestLayer::initialize();
        let span = tracing::info_span!(
            "test_span",
            { otel_trace::HTTP_RESPONSE_STATUS_CODE } = field::Empty,
            { otel_trace::HTTP_REQUEST_RESEND_COUNT } = field::Empty,
            "gax.client.span" = field::Empty, // Add marker field
        );
        let _enter = span.enter();

        // Simulate a 404 error
        let error = gax::error::Error::http(404, http::HeaderMap::new(), bytes::Bytes::new());
        record_intermediate_client_request(Err(&error), 1);

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "captured spans: {:?}", captured);
        let attributes = &captured[0].attributes;

        assert_eq!(
            attributes.get(otel_trace::HTTP_RESPONSE_STATUS_CODE),
            Some(&(404_i64).into())
        );
        assert_eq!(
            attributes.get(otel_trace::HTTP_REQUEST_RESEND_COUNT),
            Some(&(1_i64).into())
        );
    }
}
