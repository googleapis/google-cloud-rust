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

use gax::options::*;
use google_cloud_http_client::ReqwestClient;
use serde_json::json;
use std::time::Duration;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(start_paused = true)]
async fn test_no_timeout() -> Result<()> {
    let (endpoint, server) = echo_server::start().await?;
    let config =
        ClientConfig::default().set_credential(auth::credentials::testing::test_credentials());
    let client = ReqwestClient::new(config, &endpoint).await?;

    let delay = Duration::from_millis(200);
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    let builder = client
        .builder(reqwest::Method::GET, "/echo".into())
        .query(&[("delay_ms", format!("{}", delay.as_millis()))]);
    let response = client.execute::<serde_json::Value, serde_json::Value>(
        builder,
        Some(json!({})),
        RequestOptions::default(),
    );

    tokio::pin!(server);
    tokio::pin!(response);
    loop {
        tokio::select! {
            _ = &mut server => { },
            r = &mut response => {
                let response = r?;
                assert_eq!(
                    get_query_value(&response, "delay_ms"),
                    Some("200".to_string())
                );
                break;
            },
            _ = interval.tick() => { },
        }
    }

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_timeout_does_not_expire() -> Result<()> {
    let (endpoint, server) = echo_server::start().await?;
    let config =
        ClientConfig::default().set_credential(auth::credentials::testing::test_credentials());
    let client = ReqwestClient::new(config, &endpoint).await?;

    let delay = Duration::from_millis(200);
    let timeout = Duration::from_millis(2000);
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    let builder = client
        .builder(reqwest::Method::GET, "/echo".into())
        .query(&[("delay_ms", format!("{}", delay.as_millis()))]);
    let response = client.execute::<serde_json::Value, serde_json::Value>(
        builder,
        Some(json!({})),
        test_options(&timeout),
    );

    tokio::pin!(server);
    tokio::pin!(response);
    loop {
        tokio::select! {
            _ = &mut server => {  },
            r = &mut response => {
                let response = r?;
                assert_eq!(
                    get_query_value(&response, "delay_ms"),
                    Some("200".to_string())
                );
                break;
            },
            _ = interval.tick() => { },
        }
    }

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn test_timeout_expires() -> Result<()> {
    let (endpoint, server) = echo_server::start().await?;
    let config =
        ClientConfig::default().set_credential(auth::credentials::testing::test_credentials());
    let client = ReqwestClient::new(config, &endpoint).await?;

    let delay = Duration::from_millis(200);
    let timeout = Duration::from_millis(150);
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    let builder = client
        .builder(reqwest::Method::GET, "/echo".into())
        .query(&[("delay_ms", format!("{}", delay.as_millis()))]);
    let response = client.execute::<serde_json::Value, serde_json::Value>(
        builder,
        Some(json!({})),
        test_options(&timeout),
    );

    tokio::pin!(server);
    tokio::pin!(response);
    loop {
        tokio::select! {
            _ = &mut server => {  },
            r = &mut response => {
                use gax::error::ErrorKind;
                assert!(
                    r.is_err(),
                    "expected an error when timeout={}, got={:?}",
                    timeout.as_millis(),
                    r
                );
                let err = r.err().unwrap();
                assert_eq!(err.kind(), ErrorKind::Io);
                break;
            },
            _ = interval.tick() => { },
        }
    }

    Ok(())
}

fn test_options(timeout: &std::time::Duration) -> RequestOptions {
    let mut options = RequestOptions::default();
    options.set_attempt_timeout(timeout.clone());
    options
}

fn get_query_value(response: &serde_json::Value, name: &str) -> Option<String> {
    response
        .as_object()
        .map(|o| o.get("query"))
        .flatten()
        .map(|h| h.get(name))
        .flatten()
        .map(|v| v.as_str())
        .flatten()
        .map(str::to_string)
}
