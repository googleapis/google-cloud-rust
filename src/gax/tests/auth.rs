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

use gax::http_client::ReqwestClient;
use gax::options::*;
use gcp_sdk_gax as gax;
use serde_json::json;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_auth_headers() -> Result<()> {
    let (endpoint, _server) = echo_server::start().await?;

    // TODO(#593) - We should use mock credentials instead of fake credentials, because
    // 1. we can test that multiple headers are included in the request
    // 2. it gives us extra confidence that our interfaces are called
    let config =
        ClientConfig::default().set_credential(auth::credentials::testing::test_credentials());
    let client = ReqwestClient::new(config, &endpoint).await?;

    let builder = client.builder(reqwest::Method::GET, "/echo".into());
    let body = json!({});
    let response: serde_json::Value = client
        .execute(builder, Some(body), RequestOptions::default())
        .await?;
    assert_eq!(
        get_header_value(&response, "authorization"),
        Some("Bearer: test-only-token".to_string())
    );
    Ok(())
}

fn get_header_value(response: &serde_json::Value, name: &str) -> Option<String> {
    response
        .as_object()
        .map(|o| o.get("headers"))
        .flatten()
        .map(|h| h.get(name))
        .flatten()
        .map(|v| v.as_str())
        .flatten()
        .map(str::to_string)
}
