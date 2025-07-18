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

//! Verify generated clients correctly send POST requests with empty bodies.

#[cfg(test)]
mod requests {
    use axum::extract::Path;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::Json;
    use serde_json::{Value, json};
    use std::time::Duration;
    use tokio::task::JoinHandle;

    #[tokio::test(flavor = "multi_thread")]
    async fn post_with_empty_body() -> anyhow::Result<()> {
        let (endpoint, _server) = start().await?;
        let client = aiplatform::client::PredictionService::builder()
            .with_endpoint(&endpoint)
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        client
            .cancel_operation()
            .set_name("projects/test-project/locations/test-locations/operations/test-001")
            .send()
            .await?;
        Ok(())
    }

    pub async fn start() -> anyhow::Result<(String, JoinHandle<()>)> {
        let app = axum::Router::new()
            .route("/", axum::routing::get(root))
            .route("/ui/{*path}", axum::routing::post(cancel_operation));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{}:{}", addr.ip(), addr.port());

        const ATTEMPTS: i32 = 5;
        for _ in 0..ATTEMPTS {
            match reqwest::get(&url).await {
                Ok(_) => {
                    return Ok((url, server));
                }
                Err(e) => {
                    eprintln!("error starting server {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };
        }
        Err(anyhow::Error::msg(format!(
            "cannot connect to server after {ATTEMPTS} attempts"
        )))
    }

    async fn root() -> Json<Value> {
        Json(json!({"status": "ready"}))
    }

    async fn cancel_operation(
        Path(_operation): Path<String>,
        headers: HeaderMap,
    ) -> (StatusCode, Json<Value>) {
        if headers.get("content-length").is_none() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json! {"missing content-length"}),
            );
        }
        (StatusCode::OK, Json(json!({})))
    }
}
