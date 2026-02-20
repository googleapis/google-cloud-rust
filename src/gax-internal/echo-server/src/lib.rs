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
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::client_builder::internal::{ClientFactory, new_builder};
use google_cloud_gax::client_builder::{ClientBuilder, Result as ClientBuilderResult};
use google_cloud_gax::error::rpc::{Code, Status, StatusDetails};
use google_cloud_rpc::model::{BadRequest, bad_request::FieldViolation};
use serde_json::json;
use std::collections::HashMap;
use tokio::task::JoinHandle;

pub async fn start() -> anyhow::Result<(String, JoinHandle<()>)> {
    let app = axum::Router::new()
        .route("/echo", axum::routing::get(echo))
        .route("/error", axum::routing::get(error));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async {
        axum::serve(listener, app).await.unwrap();
    });

    Ok((format!("http://{}:{}", addr.ip(), addr.port()), server))
}

pub fn builder(endpoint: impl Into<String>) -> ClientBuilder<Factory, Credentials> {
    new_builder(Factory(endpoint.into()))
}

pub struct Factory(String);
impl ClientFactory for Factory {
    type Client = gaxi::http::ReqwestClient;
    type Credentials = Credentials;
    async fn build(self, config: gaxi::options::ClientConfig) -> ClientBuilderResult<Self::Client> {
        Self::Client::new(config, &self.0).await
    }
}

pub fn make_status() -> anyhow::Result<Status> {
    let value = make_status_value()?;
    let payload = bytes::Bytes::from_owner(value.to_string());
    let status = Status::try_from(&payload)?;
    Ok(status)
}

async fn echo(
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> (StatusCode, String) {
    let response = echo_impl(query, headers).await;
    match response {
        Err(e) => internal_error(e),
        Ok(s) => (StatusCode::OK, s),
    }
}

async fn echo_impl(query: HashMap<String, String>, headers: HeaderMap) -> anyhow::Result<String> {
    if let Some(delay) = query
        .get("delay_ms")
        .map(|s| s.parse::<u64>())
        .transpose()?
        .map(tokio::time::Duration::from_millis)
    {
        tokio::time::sleep(delay).await;
    }
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

async fn error(
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> (StatusCode, String) {
    let response = error_impl(query, headers).await;
    match response {
        Err(e) => internal_error(e),
        Ok(r) => r,
    }
}

async fn error_impl(
    _query: HashMap<String, String>,
    _headers: HeaderMap,
) -> anyhow::Result<(StatusCode, String)> {
    let status = make_status_value()?;
    Ok((StatusCode::BAD_REQUEST, status.to_string()))
}

fn make_status_value() -> anyhow::Result<serde_json::Value> {
    let details = StatusDetails::BadRequest(BadRequest::default().set_field_violations(
        vec![FieldViolation::default()
            .set_field( "field" )
            .set_description( "desc" )
        ],
    ));
    let details = serde_json::to_value(&details)?;
    let status = json!({"error": {
        "code": StatusCode::BAD_REQUEST.as_u16(),
        "status": Code::InvalidArgument.name(),
        "message": "this path always returns an error",
        "details": [details],
    }});
    Ok(status)
}

fn headers_to_json(headers: HeaderMap) -> anyhow::Result<serde_json::Value> {
    let headers = headers
        .into_iter()
        .map(|(k, v)| {
            (
                k.map(|h| h.to_string()).unwrap_or("__status__".to_string()),
                v.to_str().map(|s| serde_json::Value::String(s.to_string())),
            )
        })
        .map(|(k, v)| v.map(|s| (k, s)))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(serde_json::Value::Object(headers.into_iter().collect()))
}

fn internal_error<E>(e: E) -> (StatusCode, String)
where
    E: std::fmt::Debug,
{
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:?}"))
}
