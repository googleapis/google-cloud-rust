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
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_gax::Result;
    use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::options::internal::{PathTemplate, RequestOptionsExt};
    use google_cloud_gax::response::Response;
    use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
    use google_cloud_gax_internal::attempt_info::AttemptInfo;
    use google_cloud_gax_internal::http::{NoBody, ReqwestClient};
    use google_cloud_gax_internal::observability::SCHEMA_URL_VALUE;
    use google_cloud_gax_internal::observability::{
        ClientRequestAttributes, DurationMetric, RequestRecorder,
    };
    use google_cloud_gax_internal::options::{ClientConfig, InstrumentationClientInfo};
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer, TestLayerGuard};
    use http::{Method, StatusCode};
    use httptest::matchers::request::{body, headers, method, method_path, path};
    use httptest::{Expectation, Server, all_of, responders::*};
    use opentelemetry_semantic_conventions::trace as otel_trace;
    use pretty_assertions::assert_eq;
    use serde::Deserialize;
    use std::collections::BTreeMap;
    use std::sync::{Arc, LazyLock};
    use std::time::Duration;
    use test_case::test_case;
    use tracing::{Instrument, field};
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Debug, Deserialize, Default, PartialEq)]
    struct TestResponse {
        hello: String,
    }

    const TEST_SERVICE: &str = "test.service";
    const TEST_VERSION: &str = "1.2.3";
    const TEST_ARTIFACT: &str = "google-cloud-test";
    const TEST_HOST: &str = "test.googleapis.com";
    const TEST_RPC_METHOD: &str = "test.Service/method";
    const TEST_URL_TEMPLATE: &str = "/v42/{parent}:method";
    const TEST_RESOURCE: &str = "//test.googleapis.com/projects/p/locations/l/widgets/w";

    static TEST_INSTRUMENTATION_INFO: LazyLock<InstrumentationClientInfo> = LazyLock::new(|| {
        let mut info = InstrumentationClientInfo::default();
        info.service_name = TEST_SERVICE;
        info.client_version = TEST_VERSION;
        info.client_artifact = TEST_ARTIFACT;
        info.default_host = TEST_HOST;
        info
    });

    async fn create_client(tracing_enabled: bool, endpoint: String) -> ReqwestClient {
        let mut config = ClientConfig::default();
        config.tracing = tracing_enabled;
        config.endpoint = Some(endpoint.clone());
        config.cred = Some(Anonymous::new().build());
        let client = ReqwestClient::new(config, &endpoint).await.unwrap();
        if tracing_enabled {
            client.with_instrumentation(&TEST_INSTRUMENTATION_INFO)
        } else {
            client
        }
    }

    #[track_caller]
    fn http_request_attributes(guard: &TestLayerGuard) -> BTreeMap<String, AttributeValue> {
        let captured = TestLayer::capture(guard);
        let http_spans = captured
            .iter()
            .filter(|s| s.name == "http_request")
            .collect::<Vec<_>>();
        let span = match http_spans[..] {
            [span] => span,
            _ => panic!("should capture one `http_request` span, captured: {captured:?}"),
        };
        BTreeMap::from_iter(span.attributes.clone())
    }

    #[track_caller]
    fn client_request_attributes(guard: &TestLayerGuard) -> BTreeMap<String, AttributeValue> {
        let captured = TestLayer::capture(guard);
        let http_spans = captured
            .iter()
            .filter(|s| s.name == "client_request")
            .collect::<Vec<_>>();
        let span = match http_spans[..] {
            [span] => span,
            _ => panic!("should capture one `client_request` span, captured: {captured:?}"),
        };
        BTreeMap::from_iter(span.attributes.clone())
    }

    #[tokio::test]
    async fn success_with_tracing_on() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(method_path("GET", "/test"))
                .respond_with(status_code(200).body("{\"hello\": \"world\"}")),
        );

        let client = create_client(true, server_url.clone()).await;
        let guard = TestLayer::initialize();
        let _response = generated_tracing_stub(&client).await;

        let got = http_request_attributes(&guard);
        let want: BTreeMap<String, AttributeValue> = [
            ("otel.name", format!("GET {TEST_URL_TEMPLATE}").into()),
            ("otel.kind", "Client".into()),
            ("rpc.system.name", "http".into()),
            ("http.request.method", "GET".into()),
            ("url.scheme", "http".into()),
            ("url.template", TEST_URL_TEMPLATE.into()),
            ("url.domain", TEST_HOST.into()),
            ("http.response.status_code", 200_i64.into()),
            ("otel.status_code", "UNSET".into()),
            ("gcp.client.service", TEST_SERVICE.into()),
            ("gcp.client.version", TEST_VERSION.into()),
            ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
            ("gcp.client.artifact", TEST_ARTIFACT.into()),
            ("gcp.client.language", "rust".into()), // TODO(#....) - remove
            ("gcp.resource.name", TEST_RESOURCE.into()),
            ("http.response.body.size", 18_i64.into()), // {"hello": "world"} is 18 bytes
            ("gcp.schema.url", SCHEMA_URL_VALUE.into()),
            ("server.address", server_addr.ip().to_string().into()),
            ("server.port", (server_addr.port() as i64).into()),
            ("url.full", format!("{}/test", server_url).into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(got, want);
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
        let _response: Result<Response<TestResponse>> = client
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
    async fn error_responses(
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

        let options = RequestOptions::default().insert_extension(PathTemplate("/error"));
        let request = client.builder(Method::GET, "/error".to_string());
        let _response: Result<Response<TestResponse>> =
            client.execute(request, None::<NoBody>, options).await;

        let got = http_request_attributes(&guard);
        let want: BTreeMap<String, AttributeValue> = [
            (
                "http.response.status_code",
                (http_status_code.as_u16() as i64).into(),
            ),
            ("error.type", expected_error_type.into()),
            ("otel.status_code", "ERROR".into()),
            ("otel.status_description", expected_description.into()),
            // Boilerplate
            ("otel.name", "GET /error".into()),
            ("otel.kind", "Client".into()),
            ("rpc.system.name", "http".into()),
            ("http.request.method", "GET".into()),
            ("url.scheme", "http".into()),
            ("url.template", "/error".into()),
            ("url.domain", TEST_HOST.into()),
            ("gcp.client.service", TEST_SERVICE.into()),
            ("gcp.client.version", TEST_VERSION.into()),
            ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
            ("gcp.client.artifact", TEST_ARTIFACT.into()),
            ("gcp.client.language", "rust".into()), // TODO(#....) - remove
            ("server.address", server_addr.ip().to_string().into()),
            ("server.port", (server_addr.port() as i64).into()),
            ("url.full", format!("{}/error", server_url).into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(got, want);
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

        let options = RequestOptions::default().insert_extension(PathTemplate("/test"));
        let request = client.builder(Method::POST, "/test".to_string());
        let body = serde_json::json!({"name": "test"});
        let _response: Result<Response<TestResponse>> =
            client.execute(request, Some(body), options).await;

        let got = http_request_attributes(&guard);
        let want: BTreeMap<String, AttributeValue> = [
            ("http.response.status_code", 201_i64.into()),
            ("otel.status_code", "UNSET".into()),
            ("otel.name", "POST /test".into()),
            ("otel.kind", "Client".into()),
            ("rpc.system.name", "http".into()),
            ("http.request.method", "POST".into()),
            ("url.scheme", "http".into()),
            ("url.template", "/test".into()),
            ("url.domain", TEST_HOST.into()),
            ("gcp.client.service", TEST_SERVICE.into()),
            ("gcp.client.version", TEST_VERSION.into()),
            ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
            ("gcp.client.artifact", TEST_ARTIFACT.into()),
            ("gcp.client.language", "rust".into()), // TODO(#....) - remove
            ("gcp.schema.url", SCHEMA_URL_VALUE.into()),
            ("server.address", server_addr.ip().to_string().into()),
            ("server.port", (server_addr.port() as i64).into()),
            ("url.full", format!("{}/test", server_url).into()),
            ("http.response.body.size", 21_i64.into()), // {"status": "created"} is 21 bytes
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(got, want);
    }

    #[tokio::test]
    async fn error_info_parsing() {
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

        let options = RequestOptions::default().insert_extension(PathTemplate("/error-info"));
        let request = client.builder(Method::GET, "/error-info".to_string());
        let result: Result<Response<TestResponse>> =
            client.execute(request, None::<NoBody>, options).await;

        assert!(result.is_err(), "{result:?}");

        let mut got = http_request_attributes(&guard);
        let description = got.remove("otel.status_description");
        let want: BTreeMap<String, AttributeValue> = [
            ("http.response.status_code", 400_i64.into()),
            ("otel.status_code", "ERROR".into()),
            ("error.type", "API_KEY_INVALID".into()),
            ("otel.name", "GET /error-info".into()),
            ("otel.kind", "Client".into()),
            ("rpc.system.name", "http".into()),
            ("http.request.method", "GET".into()),
            ("url.scheme", "http".into()),
            ("url.template", "/error-info".into()),
            ("url.domain", TEST_HOST.into()),
            ("gcp.client.service", TEST_SERVICE.into()),
            ("gcp.client.version", TEST_VERSION.into()),
            ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
            ("gcp.client.artifact", TEST_ARTIFACT.into()),
            ("gcp.client.language", "rust".into()), // TODO(#....) - remove
            ("server.address", server_addr.ip().to_string().into()),
            ("server.port", (server_addr.port() as i64).into()),
            ("url.full", format!("{}/error-info", server_url).into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(got, want);

        assert!(
            description
                .as_ref()
                .and_then(|d| d.as_string())
                .is_some_and(|d| d.contains("Invalid API Key")),
            "otel.status_description mismatched: {description:?}"
        );
    }

    #[tokio::test]
    async fn test_t3_span_enrichment_hierarchy() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(method_path("GET", "/test"))
                .respond_with(status_code(200).body("{\"hello\": \"world\"}")),
        );

        let client = create_client(true, server_url.clone()).await;
        let guard = TestLayer::initialize();
        let result = generated_tracing_stub(&client).await;
        assert!(result.is_ok(), "{result:?}");

        let captured = TestLayer::capture(&guard);
        // We expect t3_span to be captured, and the internal http_request span (T4).
        // t3_span should have the attributes.
        let t3_captured = captured
            .iter()
            .find(|s| s.name == "client_request")
            .unwrap_or_else(|| {
                panic!("cannot find `client_request` span in capture: {captured:#?}")
            });
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

        let options = RequestOptions::default().insert_extension(PathTemplate("/test"));
        let request = client.builder(Method::GET, "/test".to_string());

        // Create a user span that happens to have the same fields, but NO marker
        let user_span = tracing::info_span!(
            "user_span",
            { { otel_trace::HTTP_RESPONSE_STATUS_CODE } } = field::Empty,
            { { otel_trace::HTTP_REQUEST_RESEND_COUNT } } = field::Empty,
        );

        // Execute the request within the user span
        let _response: Result<Response<TestResponse>> = client
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

    // Verify the path starting from execute() records the request and responses.
    //
    // The verification is indirect, we examine the contents of the T3 span and infer the values were
    // recorder.
    #[tokio::test]
    async fn execute_records_request_and_response() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(method_path("GET", "/test"))
                .respond_with(status_code(200).body("{\"hello\": \"world\"}")),
        );

        let client = create_client(true, server_url.clone()).await;
        let guard = TestLayer::initialize();

        let _response = generated_tracing_stub(&client).await;

        let got = client_request_attributes(&guard);
        let want: BTreeMap<String, AttributeValue> = [
            (
                "otel.name",
                concat!(env!("CARGO_CRATE_NAME"), "::", "generated_tracing_stub").into(),
            ),
            ("otel.kind", "Internal".into()),
            ("rpc.system.name", "http".into()),
            ("rpc.method", TEST_RPC_METHOD.into()),
            ("gcp.client.service", TEST_SERVICE.into()),
            ("gcp.client.version", TEST_VERSION.into()),
            ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
            ("gcp.client.artifact", TEST_ARTIFACT.into()),
            ("gcp.schema.url", SCHEMA_URL_VALUE.into()),
            ("http.response.status_code", 200_i64.into()),
            ("otel.status_code", "UNSET".into()),
            ("http.request.method", "GET".into()),
            ("server.address", server_addr.ip().to_string().into()),
            ("server.port", (server_addr.port() as i64).into()),
            ("url.full", format!("{}/test", server_url).into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(got, want);
    }

    // Verify the path starting from `execute_http()` records the request and send errors.
    //
    // The verification is indirect, we examine the contents of the T3 span and infer the values were
    // recorder.
    #[tokio::test]
    async fn execute_records_send_error() -> anyhow::Result<()> {
        // All requests will fail with a send error because there is nothing listening on this endpoint.
        const ENDPOINT: &str = "https://127.0.0.1:1";
        let mut config = ClientConfig::default();
        config.tracing = true;
        config.endpoint = Some(ENDPOINT.to_string());
        config.cred = Some(Anonymous::new().build());
        config.retry_policy = Some(Arc::new(
            Aip194Strict
                .with_time_limit(Duration::from_secs(5))
                .with_attempt_limit(5),
        ));
        config.backoff_policy = Some(Arc::new(
            ExponentialBackoffBuilder::default()
                .with_initial_delay(Duration::from_millis(1))
                .with_maximum_delay(Duration::from_millis(1))
                .build()?,
        ));
        let client = ReqwestClient::new(config, ENDPOINT)
            .await?
            .with_instrumentation(&TEST_INSTRUMENTATION_INFO);
        let guard = TestLayer::initialize();

        let _response = generated_tracing_stub(&client).await;

        let mut got = client_request_attributes(&guard);
        // Must exist, but we do not care about its value.
        assert!(got.remove("otel.status_description").is_some(), "{got:?}");
        assert!(got.remove("error.type").is_some(), "{got:?}");
        assert!(got.remove("http.request.resend_count").is_some(), "{got:?}");

        let want: BTreeMap<String, AttributeValue> = [
            (
                "otel.name",
                concat!(env!("CARGO_CRATE_NAME"), "::", "generated_tracing_stub").into(),
            ),
            ("otel.kind", "Internal".into()),
            ("rpc.system.name", "http".into()),
            ("rpc.method", TEST_RPC_METHOD.into()),
            ("gcp.client.service", TEST_SERVICE.into()),
            ("gcp.client.version", TEST_VERSION.into()),
            ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
            ("gcp.client.artifact", TEST_ARTIFACT.into()),
            ("gcp.schema.url", SCHEMA_URL_VALUE.into()),
            ("server.address", "127.0.0.1".into()),
            ("server.port", (1_i64).into()),
            ("url.full", "https://127.0.0.1:1/test".into()),
            ("http.request.method", "GET".into()),
            ("otel.status_code", "ERROR".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(got, want);
        Ok(())
    }

    // Verify the path starting from `execute_http()` records the request and responses.
    //
    // The verification is indirect, we examine the contents of the T3 span and infer the values were
    // recorder.
    #[tokio::test]
    async fn execute_http_records_request_and_response() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(method_path("GET", "/test"))
                .respond_with(status_code(200).body("{\"hello\": \"world\"}")),
        );

        let client = create_client(true, server_url.clone()).await;
        let guard = TestLayer::initialize();

        let _response = execute_http_tracing_stub(&client).await;

        let got = client_request_attributes(&guard);
        let want: BTreeMap<String, AttributeValue> = [
            (
                "otel.name",
                concat!(env!("CARGO_CRATE_NAME"), "::", "execute_http_tracing_stub").into(),
            ),
            ("otel.kind", "Internal".into()),
            ("rpc.system.name", "http".into()),
            ("rpc.method", TEST_RPC_METHOD.into()),
            ("gcp.client.service", TEST_SERVICE.into()),
            ("gcp.client.version", TEST_VERSION.into()),
            ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
            ("gcp.client.artifact", TEST_ARTIFACT.into()),
            ("gcp.schema.url", SCHEMA_URL_VALUE.into()),
            ("http.response.status_code", 200_i64.into()),
            ("server.address", server_addr.ip().to_string().into()),
            ("server.port", (server_addr.port() as i64).into()),
            ("url.full", format!("{}/test", server_url).into()),
            ("http.request.method", "GET".into()),
            ("otel.status_code", "UNSET".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(got, want);
    }

    // Verify the path starting from `execute_http()` records the request and send errors.
    //
    // The verification is indirect, we examine the contents of the T3 span and infer the values were
    // recorder.
    #[tokio::test]
    async fn execute_http_recorder_send_error() {
        // All requests will fail with a send error because there is nothing listening on this endpoint.
        let client = create_client(true, "https://127.0.0.1:1".to_string()).await;
        let guard = TestLayer::initialize();

        let _response = execute_http_tracing_stub(&client).await;

        let mut got = client_request_attributes(&guard);
        // Must exist, but we do not care about its value.
        assert!(got.remove("otel.status_description").is_some(), "{got:?}");

        let want: BTreeMap<String, AttributeValue> = [
            (
                "otel.name",
                concat!(env!("CARGO_CRATE_NAME"), "::", "execute_http_tracing_stub").into(),
            ),
            ("otel.kind", "Internal".into()),
            ("rpc.system.name", "http".into()),
            ("rpc.method", TEST_RPC_METHOD.into()),
            ("gcp.client.service", TEST_SERVICE.into()),
            ("gcp.client.version", TEST_VERSION.into()),
            ("gcp.client.repo", "googleapis/google-cloud-rust".into()),
            ("gcp.client.artifact", TEST_ARTIFACT.into()),
            ("gcp.schema.url", SCHEMA_URL_VALUE.into()),
            ("server.address", "127.0.0.1".into()),
            ("server.port", (1_i64).into()),
            ("url.full", "https://127.0.0.1:1/test".into()),
            ("http.request.method", "GET".into()),
            ("otel.status_code", "ERROR".into()),
            ("error.type", "CLIENT_CONNECTION_ERROR".into()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(got, want);
    }

    #[tokio::test]
    async fn generated_propagate_trace_context() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(all_of![
                method_path("GET", "/test"),
                headers(httptest::matchers::contains((
                    "traceparent",
                    httptest::matchers::any()
                ))),
            ])
            .respond_with(status_code(200).body("{\"hello\": \"world\"}")),
        );

        let client = create_client(true, server_url.clone()).await;

        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = opentelemetry::trace::TracerProvider::tracer(&tracer_provider, "test");
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = tracing_subscriber::registry().with(telemetry);
        let _guard = tracing::subscriber::set_default(subscriber);

        let _ = tracing::info_span!("parent_span").entered();
        let result = generated_tracing_stub(&client).await;
        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn execute_http_propagate_trace_context() {
        let server = Server::run();
        let server_addr = server.addr();
        let server_url = format!("http://{}", server_addr);
        server.expect(
            Expectation::matching(all_of![
                method_path("GET", "/test"),
                headers(httptest::matchers::contains((
                    "traceparent",
                    httptest::matchers::any()
                ))),
            ])
            .respond_with(status_code(200).body("{\"hello\": \"world\"}")),
        );

        let client = create_client(true, server_url.clone()).await;

        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = opentelemetry::trace::TracerProvider::tracer(&tracer_provider, "test");
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = tracing_subscriber::registry().with(telemetry);
        let _guard = tracing::subscriber::set_default(subscriber);

        let _ = tracing::info_span!("parent_span").entered();
        let result = execute_http_tracing_stub(&client).await;
        assert!(result.is_ok(), "{result:?}");
    }

    // Simulate a generated client tracing stub.
    async fn generated_tracing_stub(client: &ReqwestClient) -> Result<Response<TestResponse>> {
        let metric = DurationMetric::new(&TEST_INSTRUMENTATION_INFO);
        let (_span, pending) = google_cloud_gax_internal::client_request_signals!(
            metric: metric,
            info: *TEST_INSTRUMENTATION_INFO,
            method: "generated_tracing_stub",
            generated_transport_stub(client));
        pending.await
    }

    // Simulate a generated client transport stub.
    async fn generated_transport_stub(client: &ReqwestClient) -> Result<Response<TestResponse>> {
        let options = RequestOptions::default();
        let request = client.builder(Method::GET, "/test".to_string());
        if let Some(recorder) = RequestRecorder::current() {
            recorder.on_client_request(
                ClientRequestAttributes::default()
                    .set_rpc_method(TEST_RPC_METHOD)
                    .set_url_template(TEST_URL_TEMPLATE)
                    .set_resource_name(TEST_RESOURCE.to_string()),
            );
        }
        client.execute(request, None::<NoBody>, options).await
    }

    // Simulate a hand-crafted client (Storage at this time) using the `execute_http()` path.
    async fn execute_http_transport_stub(client: &ReqwestClient) -> Result<reqwest::Response> {
        let options = RequestOptions::default();
        if let Some(recorder) = RequestRecorder::current() {
            recorder.on_client_request(
                ClientRequestAttributes::default()
                    .set_rpc_method(TEST_RPC_METHOD)
                    .set_url_template(TEST_URL_TEMPLATE)
                    .set_resource_name(TEST_RESOURCE.to_string()),
            );
        }
        let builder = client.http_builder(reqwest::Method::GET, "/test");
        let attempt_info = AttemptInfo::new(0);
        builder.send(options, attempt_info).await
    }

    // Simulate a hand-crafted client (Storage at this time) using the `execute_http()` path.
    async fn execute_http_tracing_stub(client: &ReqwestClient) -> Result<reqwest::Response> {
        let metric = DurationMetric::new(&TEST_INSTRUMENTATION_INFO);
        let (_span, pending) = google_cloud_gax_internal::client_request_signals!(
            metric: metric,
            info: *TEST_INSTRUMENTATION_INFO,
            method: "execute_http_tracing_stub",
            execute_http_transport_stub(client));
        pending.await
    }
}
