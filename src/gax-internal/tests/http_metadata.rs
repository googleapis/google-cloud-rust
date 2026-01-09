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

#[cfg(all(test, feature = "_internal-http-client"))]
mod tests {
    use gax::options::RequestOptions;
    use google_cloud_gax_internal::http::ReqwestClient;
    use google_cloud_gax_internal::options::ClientConfig;
    use http::HeaderValue;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use serde_json::json;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capture_headers() -> anyhow::Result<()> {
        let server = start();
        let endpoint = format!("http://{}", server.addr());

        let client = ReqwestClient::new(test_config(), &endpoint).await?;
        let builder = client.builder(reqwest::Method::GET, "/hello".into());
        let body = json!({});

        let response = client
            .execute::<serde_json::Value, serde_json::Value>(
                builder,
                Some(body),
                RequestOptions::default(),
            )
            .await;
        let response = response?;
        let (parts, body) = response.into_parts();
        assert_eq!(body, json!({"greeting": "Hello World!"}));
        assert_eq!(
            parts.headers.get("x-test-header"),
            Some(&HeaderValue::from_static("test-only"))
        );
        Ok(())
    }

    pub fn start() -> Server {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/hello")).respond_with(
                json_encoded(json!({"greeting": "Hello World!"}))
                    .insert_header("x-test-header", "test-only"),
            ),
        );
        server
    }

    fn test_config() -> ClientConfig {
        use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
        let mut config = ClientConfig::default();
        config.cred = Anonymous::new().build().into();
        config
    }
}
