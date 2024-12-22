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

use gax::http_client::*;
use gax::options::*;
use gcp_sdk_gax as gax;
use serde_json::json;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

mod echo_server;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_user_agent() -> Result<()> {
    let (endpoint, _server) = echo_server::start().await?;

    let config = ClientConfig::default().set_credential(auth::Credential::test_credentials());
    let client = ReqwestClient::new(config, &endpoint).await?;

    let builder = client.builder(reqwest::Method::GET, "/echo".into());
    let body = json!({});
    let response: serde_json::Value = client
        .execute(builder, Some(body), RequestOptions::default())
        .await?;
    let got = get_header_value(&response, "user-agent");
    assert_eq!(got, None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_user_agent_with_prefix() -> Result<()> {
    let (endpoint, _server) = echo_server::start().await?;

    let config = ClientConfig::default().set_credential(auth::Credential::test_credentials());
    let client = ReqwestClient::new(config, &endpoint).await?;

    let builder = client.builder(reqwest::Method::GET, "/echo".into());
    let body = json!({});
    let prefix = "test-prefix/1.2.3";
    let options = {
        let mut o = RequestOptions::default();
        o.set_user_agent(prefix);
        o
    };
    let response: serde_json::Value = client.execute(builder, Some(body), options).await?;
    let got = get_header_value(&response, "user-agent");
    assert_eq!(got.as_deref(), Some(prefix));
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
