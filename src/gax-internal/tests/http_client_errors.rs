// Copyright 2024 Google LLC
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

#[cfg(all(test, feature = "_internal-http-client"))]
mod tests {
    use gax::options::*;
    use serde_json::json;

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    fn test_credentials() -> auth::credentials::Credentials {
        auth::credentials::anonymous::Builder::new().build()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_error_with_status() -> Result<()> {
        use serde_json::Value;
        let (endpoint, _server) = echo_server::start().await?;

        let client = echo_server::builder(endpoint)
            .with_credentials(test_credentials())
            .build()
            .await?;

        let builder = client.builder(reqwest::Method::GET, "/error".into());
        let body = json!({});
        let response = client
            .execute::<Value, Value>(builder, Some(body), RequestOptions::default())
            .await;

        match response {
            Ok(v) => panic!("expected an error got={v:?}"),
            Err(e) => {
                assert!(e.http_headers().is_some(), "missing headers in {e:?}");
                let headers = e.http_headers().unwrap();
                assert!(!headers.is_empty(), "empty headers in {e:?}");
                let got = e.status();
                let want = echo_server::make_status()?;
                assert_eq!(got, Some(&want));
            }
        }

        Ok(())
    }

    #[cfg(all(test, google_cloud_unstable_tracing, feature = "_internal-http-client"))]
    mod tracing_tests {
        use super::*;
        use google_cloud_gax_internal::http::ReqwestClient;
        use google_cloud_gax_internal::observability::attributes::error_type_values::CLIENT_CONNECTION_ERROR;
        use google_cloud_gax_internal::options::ClientConfig;
        use google_cloud_test_utils::test_layer::TestLayer;
        use opentelemetry_semantic_conventions::trace as semconv;

        #[tokio::test]
        async fn test_connection_error_with_tracing_on() -> Result<()> {
            use serde_json::Value;
            let endpoint = "http://localhost:1"; // Non-existent port
            let mut config = ClientConfig::default();
            config.tracing = true;
            config.cred = Some(test_credentials());
            let client = ReqwestClient::new(config, endpoint).await?;

            let guard = TestLayer::initialize();

            let builder = client.builder(reqwest::Method::GET, "/".into());
            let result = client
                .execute::<Value, Value>(builder, Option::<Value>::None, RequestOptions::default())
                .await;

            assert!(result.is_err(), "Expected connection error");

            let spans = TestLayer::capture(&guard);
            assert_eq!(spans.len(), 1, "Should capture one span: {:?}", spans);

            let span = &spans[0];
            let attributes = &span.attributes;
            assert_eq!(
                span.name, "http_request",
                "Span name mismatch: {:?}, all attributes: {:?}",
                span, attributes
            );

            let expected_error_type = CLIENT_CONNECTION_ERROR.to_string();
            assert_eq!(
                attributes.get(semconv::ERROR_TYPE),
                Some(&expected_error_type),
                "Span 0: '{}' mismatch, all attributes: {:?}",
                semconv::ERROR_TYPE,
                attributes
            );
            assert!(
                !attributes.contains_key(semconv::HTTP_RESPONSE_STATUS_CODE),
                "Span 0: '{}' should not be present on connection error, all attributes: {:?}",
                semconv::HTTP_RESPONSE_STATUS_CODE,
                attributes
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_redirect_error_with_tracing_on() -> Result<()> {
            use google_cloud_gax_internal::observability::attributes::error_type_values::CLIENT_REDIRECT_ERROR;
            use httptest::{Expectation, ServerPool, matchers::*, responders::*};
            use serde_json::Value;

            let server_pool = ServerPool::new(1);
            let server = server_pool.get_server();
            let endpoint = server.url("").to_string(); // Base endpoint
            let redirect_url = server.url("/loop").to_string();

            server.expect(
                Expectation::matching(request::method_path("GET", "/loop"))
                    .times(0..100)
                    .respond_with(
                        status_code(302).insert_header("location", redirect_url.as_str()),
                    ),
            );

            let mut config = ClientConfig::default();
            config.tracing = true;
            config.cred = Some(test_credentials());
            let client = ReqwestClient::new(config, &endpoint).await?;

            let guard = TestLayer::initialize();

            let builder = client.builder(reqwest::Method::GET, "loop".into());
            let result = client
                .execute::<Value, Value>(builder, Option::<Value>::None, RequestOptions::default())
                .await;

            assert!(result.is_err(), "Expected redirect error");

            let spans = TestLayer::capture(&guard);
            assert_eq!(spans.len(), 1, "Should capture one span: {:?}", spans);

            let span = &spans[0];
            let attributes = &span.attributes;
            assert_eq!(
                span.name, "http_request",
                "Span name mismatch: {:?}, all attributes: {:?}",
                span, attributes
            );

            let expected_error_type = CLIENT_REDIRECT_ERROR.to_string();
            assert_eq!(
                attributes.get(semconv::ERROR_TYPE),
                Some(&expected_error_type),
                "Span 0: {} mismatch, all attributes: {:?}",
                semconv::ERROR_TYPE,
                attributes
            );
            assert!(
                !attributes.contains_key(semconv::HTTP_RESPONSE_STATUS_CODE),
                "Span 0: {} should not be present on redirect error, all attributes: {:?}",
                semconv::HTTP_RESPONSE_STATUS_CODE,
                attributes
            );

            Ok(())
        }
    }
}
