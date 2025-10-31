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
    use google_cloud_gax_internal::observability::attributes::*;
    use google_cloud_gax_internal::options::{ClientConfig, InstrumentationClientInfo};
    use google_cloud_test_utils::test_layer::{AttributeValue, TestLayer};
    use http::{Method, StatusCode};
    use httptest::matchers::request::{body, method, path};
    use httptest::{Expectation, Server, all_of, responders::*};
    use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
    use serde::Deserialize;
    use std::collections::HashMap;
    use test_case::test_case;

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
            (KEY_OTEL_NAME, "GET /test".into()),
            (KEY_OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "GET".into()),
            (otel_trace::URL_SCHEME, "http".into()),
            (otel_attr::URL_TEMPLATE, "/test".into()),
            (otel_attr::URL_DOMAIN, TEST_HOST.into()),
            (otel_trace::HTTP_RESPONSE_STATUS_CODE, 200_i64.into()),
            (KEY_OTEL_STATUS, "Ok".into()),
            (KEY_GCP_CLIENT_SERVICE, TEST_SERVICE.into()),
            (KEY_GCP_CLIENT_VERSION, TEST_VERSION.into()),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (KEY_GCP_CLIENT_ARTIFACT, TEST_ARTIFACT.into()),
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

    #[test_case(StatusCode::BAD_REQUEST, "400"; "400 Bad Request")]
    #[test_case(StatusCode::UNAUTHORIZED, "401"; "401 Unauthorized")]
    #[test_case(StatusCode::FORBIDDEN, "403"; "403 Forbidden")]
    #[test_case(StatusCode::NOT_FOUND, "404"; "404 Not Found")]
    #[test_case(StatusCode::INTERNAL_SERVER_ERROR, "500"; "500 Internal Server Error")]
    #[test_case(StatusCode::SERVICE_UNAVAILABLE, "503"; "503 Service Unavailable")]
    #[tokio::test]
    async fn test_error_responses(http_status_code: StatusCode, expected_error_type: &str) {
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
            attrs.get(KEY_OTEL_STATUS),
            Some(&"Error".into()),
            "otel.status mismatch, attrs: {:?}",
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
            .respond_with(status_code(201).body("{\"hello\": \"world\"}")),
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
            (KEY_OTEL_NAME, "POST /test".into()),
            (KEY_OTEL_KIND, "Client".into()),
            (otel_trace::RPC_SYSTEM, "http".into()),
            (otel_trace::HTTP_REQUEST_METHOD, "POST".into()),
            (otel_trace::URL_SCHEME, "http".into()),
            (otel_attr::URL_TEMPLATE, "/test".into()),
            (otel_attr::URL_DOMAIN, TEST_HOST.into()),
            (otel_trace::HTTP_RESPONSE_STATUS_CODE, 201_i64.into()),
            (KEY_OTEL_STATUS, "Ok".into()),
            (KEY_GCP_CLIENT_SERVICE, TEST_SERVICE.into()),
            (KEY_GCP_CLIENT_VERSION, TEST_VERSION.into()),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust".into()),
            (KEY_GCP_CLIENT_ARTIFACT, TEST_ARTIFACT.into()),
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
}
