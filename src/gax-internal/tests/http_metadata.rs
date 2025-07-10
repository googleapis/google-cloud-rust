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
    use axum::http::{HeaderName, HeaderValue, StatusCode};
    use gax::options::RequestOptions;
    use google_cloud_gax_internal::http::ReqwestClient;
    use google_cloud_gax_internal::options::ClientConfig;
    use serde_json::json;
    use tokio::task::JoinHandle;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn capture_headers() -> anyhow::Result<()> {
        let (endpoint, _server) = start().await?;

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

    pub async fn start() -> anyhow::Result<(String, JoinHandle<()>)> {
        let app = axum::Router::new().route("/hello", axum::routing::get(hello));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        Ok((format!("http://{}:{}", addr.ip(), addr.port()), server))
    }

    async fn hello() -> impl axum::response::IntoResponse {
        use axum::response::Json;
        (
            StatusCode::OK,
            [(
                HeaderName::from_static("x-test-header"),
                HeaderValue::from_static("test-only"),
            )],
            Json(json!({"greeting": "Hello World!"})),
        )
    }

    fn test_config() -> ClientConfig {
        ClientConfig {
            cred: auth::credentials::testing::test_credentials().into(),
            ..Default::default()
        }
    }
}
