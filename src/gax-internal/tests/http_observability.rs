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
    use google_cloud_gax_internal::observability::attributes::{
        KEY_GCP_CLIENT_ARTIFACT, KEY_GCP_CLIENT_REPO, KEY_GCP_CLIENT_SERVICE,
        KEY_GCP_CLIENT_VERSION, KEY_OTEL_KIND, KEY_OTEL_NAME, KEY_OTEL_STATUS,
    };
    use google_cloud_gax_internal::options::{ClientConfig, InstrumentationClientInfo};
    use google_cloud_test_utils::test_layer::TestLayer;
    use http::Method;
    use httptest::matchers::request::{method, path};
    use httptest::{Expectation, Server, all_of, responders::*};
    use opentelemetry_semantic_conventions::{attribute as otel_attr, trace as otel_trace};
    use serde::Deserialize;
    use std::collections::HashMap;

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
        assert_eq!(captured.len(), 1, "Should capture one span");

        let span = &captured[0];
        let attrs = &span.attributes;

        let mut expected_attributes: HashMap<String, String> = [
            (KEY_OTEL_NAME, "GET /test"),
            (KEY_OTEL_KIND, "Client"),
            (otel_trace::RPC_SYSTEM, "http"),
            (otel_trace::HTTP_REQUEST_METHOD, "GET"),
            (otel_trace::URL_SCHEME, "http"),
            (otel_attr::URL_TEMPLATE, "/test"),
            (otel_attr::URL_DOMAIN, TEST_HOST),
            (otel_trace::HTTP_RESPONSE_STATUS_CODE, "200"),
            (KEY_OTEL_STATUS, "Ok"),
            (KEY_GCP_CLIENT_SERVICE, TEST_SERVICE),
            (KEY_GCP_CLIENT_VERSION, TEST_VERSION),
            (KEY_GCP_CLIENT_REPO, "googleapis/google-cloud-rust"),
            (KEY_GCP_CLIENT_ARTIFACT, TEST_ARTIFACT),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        expected_attributes.insert(
            otel_trace::SERVER_ADDRESS.to_string(),
            server_addr.ip().to_string(),
        );
        expected_attributes.insert(
            otel_trace::SERVER_PORT.to_string(),
            server_addr.port().to_string(),
        );
        expected_attributes.insert(
            otel_trace::URL_FULL.to_string(),
            format!("{}/test", server_url),
        );

        assert_eq!(attrs, &expected_attributes, "Attribute mismatch");
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
        assert_eq!(captured.len(), 0, "Should capture no spans");
    }
}
