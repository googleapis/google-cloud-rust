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

#[cfg(all(test, feature = "_internal-http-client", google_cloud_unstable_tracing))]
mod tests {
    use gax::options::RequestOptions;
    use gax::response::Response;
    use google_cloud_gax_internal::http::{NoBody, ReqwestClient};
    use google_cloud_gax_internal::observability::attributes::keys::*;
    use google_cloud_gax_internal::options::{ClientConfig, InstrumentationClientInfo};
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer};
    use http::{Method, StatusCode};
    use httptest::matchers::request::{body, method, path};
    use httptest::{Expectation, Server, all_of, responders::*};
    use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
    use serde::Deserialize;
    use std::collections::HashMap;
    use test_case::test_case;
    use tracing::{Instrument, field};

    #[derive(Debug, Deserialize, Default, PartialEq)]
    struct TestResponse {
        hello: String,
    }

    const TEST_SERVICE: &str = "test.service";
    const TEST_VERSION: &str = "1.2.3";
    const TEST_ARTIFACT: &str = "google-cloud-test";
    const TEST_HOST: &str = "test.googleapis.com";

    lazy_static::lazy_static! {
        static ref TEST_INSTRUMENTATION_INFO: InstrumentationClientInfo = {
            let mut info = InstrumentationClientInfo::default();
            info.service_name = TEST_SERVICE;
            info.client_version = TEST_VERSION;
            info.client_artifact = TEST_ARTIFACT;
            info.default_host = TEST_HOST;
            info
        };
    }

    async fn create_client(tracing_enabled: bool, endpoint: String) -> ReqwestClient {
        let mut config = ClientConfig::default();
        config.tracing = tracing_enabled;
        config.endpoint = Some(endpoint.clone());
        config.cred = Some(auth::credentials::anonymous::Builder::new().build());
        let client = ReqwestClient::new(config, &endpoint).await.unwrap();
        if tracing_enabled {
            client.with_instrumentation(&TEST_INSTRUMENTATION_INFO)
        } else {
            client
        }
    }

    #[tokio::test]
    async fn success_with_tracing_on() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(all_of![method("GET"), path("/test"),])
                .respond_with(status_code(200).body("{\"hello\": \"world\"}")),
        );

        let client = create_client(true, server_url.clone()).await;
        let guard = TestLayer::initialize();

        let options = gax::options::internal::set_path_template(RequestOptions::default(), "/test");
        let request = client.builder(Method::GET, "/test".to_string());
        let _response: gax::Result<Response<TestResponse>> =
            client.execute(request, None::<NoBody>, options).await;

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "Should capture one span: {:?}", captured);

        let span = &captured[0];
        let attrs = &span.attributes;

        let expected_attributes: HashMap<String, AttributeValue> = [
            (OTEL_NAME, "GET /test".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "GET".into()),
            (otel_trace::URL_SCHEME, "http".into()),
            (otel_attr::URL_TEMPLATE, "/test".into()),
            (otel_attr::URL_DOMAIN, TEST_HOST.into()),
            (otel_trace::HTTP_RESPONSE_STATUS_CODE, 200_i64.into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
            (GCP_CLIENT_SERVICE, TEST_SERVICE.into()),
            (GCP_CLIENT_VERSION, TEST_VERSION.into()),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (GCP_CLIENT_ARTIFACT, TEST_ARTIFACT.into()),
            (GCP_CLIENT_LANGUAGE, "rust".into()),
            (otel_trace::HTTP_RESPONSE_BODY_SIZE, 18_i64.into()), // {"hello": "world"} is 18 bytes
            (
                otel_trace::SERVER_ADDRESS,
                server_addr.ip().to_string().into(),
            ),
            (otel_trace::SERVER_PORT, (server_addr.port() as i64).into()),
            (otel_trace::URL_FULL, format!("{}/test", server_url).into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, &expected_attributes);
    }

    #[tokio::test]
    async fn success_with_tracing_off() {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![method("GET"), path("/test"),])
                .respond_with(status_code(200).body("{\"hello\": \"world\"}")),
        );

        let client = create_client(false, format!("http://{}", server.addr())).await;
        let guard = TestLayer::initialize();

        let request = client.builder(Method::GET, "/test".to_string());
        let _response: gax::Result<Response<TestResponse>> = client
            .execute(request, None::<NoBody>, RequestOptions::default())
            .await;

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 0, "Should capture no spans: {:?}", captured);
    }

    #[test_case(StatusCode::BAD_REQUEST, "400", "the HTTP transport reports a [400] error: error"; "400 Bad Request")]
    #[test_case(StatusCode::UNAUTHORIZED, "401", "the HTTP transport reports a [401] error: error"; "401 Unauthorized")]
    #[test_case(StatusCode::FORBIDDEN, "403", "the HTTP transport reports a [403] error: error"; "403 Forbidden")]
    #[test_case(StatusCode::NOT_FOUND, "404", "the HTTP transport reports a [404] error: error"; "404 Not Found")]
    #[test_case(StatusCode::INTERNAL_SERVER_ERROR, "500", "the HTTP transport reports a [500] error: error"; "500 Internal Server Error")]
    #[test_case(StatusCode::SERVICE_UNAVAILABLE, "503", "the HTTP transport reports a [503] error: error"; "503 Service Unavailable")]
    #[tokio::test]
    async fn test_error_responses(
        http_status_code: StatusCode,
        expected_error_type: &'static str,
        expected_description: &'static str,
    ) {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(all_of![method("GET"), path("/error"),])
                .respond_with(status_code(http_status_code.as_u16()).body("error")),
        );

        let client = create_client(true, server_url.clone()).await;
        let guard = TestLayer::initialize();

        let options =
            gax::options::internal::set_path_template(RequestOptions::default(), "/error");
        let request = client.builder(Method::GET, "/error".to_string());
        let _response: gax::Result<Response<TestResponse>> =
            client.execute(request, None::<NoBody>, options).await;

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "Should capture one span: {:?}", captured);

        let span = &captured[0];

        let attrs = &span.attributes;

        assert_eq!(
            attrs.get(otel_trace::HTTP_RESPONSE_STATUS_CODE),
            Some(&(http_status_code.as_u16() as i64).into()),
            "http.response.status_code mismatch, attrs: {:?}",
            attrs
        );

        assert_eq!(
            attrs.get(otel_trace::ERROR_TYPE),
            Some(&expected_error_type.into()),
            "error.type mismatch, attrs: {:?}",
            attrs
        );

        assert_eq!(
            attrs.get(OTEL_STATUS_CODE),
            Some(&"ERROR".into()),
            "otel.status_code mismatch, attrs: {:?}",
            attrs
        );

        assert_eq!(
            attrs.get(OTEL_STATUS_DESCRIPTION),
            Some(&expected_description.into()),
            "otel.status_description mismatch, attrs: {:?}",
            attrs
        );
    }

    #[tokio::test]
    async fn post_with_body() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(all_of![
                method("POST"),
                path("/test"),
                body("{\"name\":\"test\"}"),
            ])
            .respond_with(status_code(201).body("{\"status\": \"created\"}")),
        );

        let client = create_client(true, server_url.clone()).await;
        let guard = TestLayer::initialize();

        let options = gax::options::internal::set_path_template(RequestOptions::default(), "/test");
        let request = client.builder(Method::POST, "/test".to_string());
        let body = serde_json::json!({"name": "test"});
        let _response: gax::Result<Response<TestResponse>> =
            client.execute(request, Some(body), options).await;

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "Should capture one span: {:?}", captured);

        let span = &captured[0];
        let attrs = &span.attributes;

        let expected_attributes: HashMap<String, AttributeValue> = [
            (OTEL_NAME, "POST /test".into()),
            (OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "POST".into()),
            (otel_trace::URL_SCHEME, "http".into()),
            (otel_attr::URL_TEMPLATE, "/test".into()),
            (otel_attr::URL_DOMAIN, TEST_HOST.into()),
            (otel_trace::HTTP_RESPONSE_STATUS_CODE, 201_i64.into()),
            (OTEL_STATUS_CODE, "UNSET".into()),
            (GCP_CLIENT_SERVICE, TEST_SERVICE.into()),
            (GCP_CLIENT_VERSION, TEST_VERSION.into()),
            (GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (GCP_CLIENT_ARTIFACT, TEST_ARTIFACT.into()),
            (GCP_CLIENT_LANGUAGE, "rust".into()),
            (otel_trace::HTTP_RESPONSE_BODY_SIZE, 21_i64.into()), // {"status": "created"} is 21 bytes
            (
                otel_trace::SERVER_ADDRESS,
                server_addr.ip().to_string().into(),
            ),
            (otel_trace::SERVER_PORT, (server_addr.port() as i64).into()),
            (otel_trace::URL_FULL, format!("{}/test", server_url).into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(attrs, &expected_attributes);
    }

    #[tokio::test]
    async fn test_error_info_parsing() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);

        let error_body = serde_json::json!({
            "error": {
                "code": 400,
                "message": "Invalid API Key",
                "status": "INVALID_ARGUMENT",
                "details": [
                    {
                        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                        "reason": "API_KEY_INVALID",
                        "domain": "googleapis.com",
                        "metadata": {
                            "service": "test.googleapis.com"
                        }
                    }
                ]
            }
        });

        server.expect(
            Expectation::matching(all_of![method("GET"), path("/error-info"),])
                .respond_with(status_code(400).body(error_body.to_string())),
        );

        let client = create_client(true, server_url.clone()).await;
        let guard = TestLayer::initialize();

        let options =
            gax::options::internal::set_path_template(RequestOptions::default(), "/error-info");
        let request = client.builder(Method::GET, "/error-info".to_string());
        let result: gax::Result<Response<TestResponse>> =
            client.execute(request, None::<NoBody>, options).await;

        assert!(result.is_err());

        let captured = TestLayer::capture(&guard);
        assert_eq!(captured.len(), 1, "Should capture one span: {:?}", captured);

        let span = &captured[0];
        let attrs = &span.attributes;

        assert_eq!(
            attrs.get(otel_trace::HTTP_RESPONSE_STATUS_CODE),
            Some(&400_i64.into()),
            "http.response.status_code mismatch, attrs: {:?}",
            attrs
        );

        assert_eq!(
            attrs.get(otel_trace::ERROR_TYPE),
            Some(&"API_KEY_INVALID".into()),
            "error.type should be parsed from ErrorInfo, attrs: {:?}",
            attrs
        );

        assert_eq!(
            attrs.get(OTEL_STATUS_CODE),
            Some(&"ERROR".into()),
            "otel.status_code should be ERROR, attrs: {:?}",
            attrs
        );

        let description = attrs
            .get(OTEL_STATUS_DESCRIPTION)
            .unwrap_or_else(|| panic!("{} missing, attrs: {:?}", OTEL_STATUS_DESCRIPTION, attrs))
            .as_string()
            .unwrap_or_else(|| {
                panic!(
                    "{} not a string, attrs: {:?}",
                    OTEL_STATUS_DESCRIPTION, attrs
                )
            });
        assert!(
            description.contains("Invalid API Key"),
            "{} '{}' does not contain 'Invalid API Key', attrs: {:?}",
            OTEL_STATUS_DESCRIPTION,
            description,
            attrs
        );
    }

    #[tokio::test]
    async fn test_t3_span_enrichment_hierarchy() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(all_of![method("GET"), path("/test"),])
                .respond_with(status_code(200).body("{\"hello\": \"world\"}")),
        );

        let client = create_client(true, server_url.clone()).await;
        let guard = TestLayer::initialize();

        let options = gax::options::internal::set_path_template(RequestOptions::default(), "/test");
        let request = client.builder(Method::GET, "/test".to_string());

        // Create a parent span (T3) with the marker field
        let t3_span = tracing::info_span!(
            "t3_span",
            { { otel_trace::HTTP_RESPONSE_STATUS_CODE } } = field::Empty,
            { { otel_trace::HTTP_REQUEST_RESEND_COUNT } } = field::Empty,
            "gax.client.span" = field::Empty,
        );

        // Execute the request within the T3 span
        let _response: gax::Result<Response<TestResponse>> = client
            .execute(request, None::<NoBody>, options)
            .instrument(t3_span.clone())
            .await;

        let captured = TestLayer::capture(&guard);
        // We expect t3_span to be captured, and the internal http_request span (T4).
        // t3_span should have the attributes.
        let t3_captured = captured
            .iter()
            .find(|s| s.name == "t3_span")
            .expect("t3_span not found");
        assert_eq!(
            t3_captured
                .attributes
                .get(otel_trace::HTTP_RESPONSE_STATUS_CODE),
            Some(&(200_i64).into())
        );
        // Resend count is only set if > 0
        assert!(
            !t3_captured
                .attributes
                .contains_key(otel_trace::HTTP_REQUEST_RESEND_COUNT)
        );

        let t4_captured = captured
            .iter()
            .find(|s| s.name == "http_request")
            .expect("http_request span not found");
        // T4 span should also have the status code (it's set in record_http_response_attributes)
        assert_eq!(
            t4_captured
                .attributes
                .get(otel_trace::HTTP_RESPONSE_STATUS_CODE),
            Some(&(200_i64).into())
        );
    }

    #[tokio::test]
    async fn test_t3_span_enrichment_user_span_with_fields() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(all_of![method("GET"), path("/test"),])
                .respond_with(status_code(200).body("{\"hello\": \"world\"}")),
        );

        let client = create_client(true, server_url.clone()).await;
        let guard = TestLayer::initialize();

        let options = gax::options::internal::set_path_template(RequestOptions::default(), "/test");
        let request = client.builder(Method::GET, "/test".to_string());

        // Create a user span that happens to have the same fields, but NO marker
        let user_span = tracing::info_span!(
            "user_span",
            { { otel_trace::HTTP_RESPONSE_STATUS_CODE } } = field::Empty,
            { { otel_trace::HTTP_REQUEST_RESEND_COUNT } } = field::Empty,
        );

        // Execute the request within the user span
        let _response: gax::Result<Response<TestResponse>> = client
            .execute(request, None::<NoBody>, options)
            .instrument(user_span.clone())
            .await;

        let captured = TestLayer::capture(&guard);
        let user_captured = captured
            .iter()
            .find(|s| s.name == "user_span")
            .expect("user_span not found");

        // Ensure the user span was NOT enriched because it lacked the marker
        assert!(
            !user_captured
                .attributes
                .contains_key(otel_trace::HTTP_RESPONSE_STATUS_CODE)
        );
    }
}
