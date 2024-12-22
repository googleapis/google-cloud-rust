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

//! Defines helpers functions to run ReqwestClient integration tests.
//!
//! Setting up integration tests is a bit complicated. So we refactor that code
//! to some helper functions.

use axum::{
    extract::Query,
    http::{HeaderMap, StatusCode},
};
use serde_json::json;
use std::collections::HashMap;
use tokio::task::JoinHandle;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn start() -> Result<(String, JoinHandle<()>)> {
    let app = axum::Router::new().route("/echo", axum::routing::get(echo));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async {
        axum::serve(listener, app).await.unwrap();
    });

    Ok((format!("http://{}:{}", addr.ip(), addr.port()), server))
}

#[allow(dead_code)]
pub fn get_query_value(response: &serde_json::Value, name: &str) -> Option<String> {
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

async fn echo(
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> (http::StatusCode, String) {
    let response = echo_impl(query, headers).await;
    match response {
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")),
        Ok(s) => (StatusCode::OK, s),
    }
}

async fn echo_impl(query: HashMap<String, String>, headers: HeaderMap) -> Result<String> {
    let query = serde_json::Value::Object(
        query
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect(),
    );
    let headers = headers_to_json(headers)?;
    let object = json!({
        "headers": headers,
        "query": query,
    });
    let body = serde_json::to_string(&object)?;
    Ok(body)
}

fn headers_to_json(headers: HeaderMap) -> Result<serde_json::Value> {
    let to_dyn = |e| -> Box<dyn std::error::Error + 'static> { Box::new(e) };
    let headers = headers
        .into_iter()
        .map(|(k, v)| {
            (
                k.map(|h| h.to_string()).unwrap_or("__status__".to_string()),
                v.to_str().map(|s| serde_json::Value::String(s.to_string())),
            )
        })
        .map(|(k, v)| v.map(|s| (k, s)))
        .map(|r| r.map_err(to_dyn))
        .collect::<Result<Vec<_>>>()?;

    Ok(serde_json::Value::Object(headers.into_iter().collect()))
}
