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
use serde_json::json;
use std::collections::HashMap;
use tokio::task::JoinHandle;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub async fn start() -> Result<(String, JoinHandle<()>)> {
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

pub fn builder(
    endpoint: impl Into<String>,
) -> gax::client_builder::ClientBuilder<Factory, Credentials> {
    gax::client_builder::internal::new_builder(Factory(endpoint.into()))
}

pub struct Factory(String);
impl gax::client_builder::internal::ClientFactory for Factory {
    type Client = gaxi::http::ReqwestClient;
    type Credentials = Credentials;
    async fn build(
        self,
        config: gaxi::options::ClientConfig,
    ) -> gax::client_builder::Result<Self::Client> {
        Self::Client::new(config, &self.0).await
    }
}

pub fn make_status() -> Result<gax::error::rpc::Status> {
    let value = make_status_value()?;
    let payload = bytes::Bytes::from_owner(value.to_string());
    let status = gax::error::rpc::Status::try_from(&payload)?;
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

async fn echo_impl(query: HashMap<String, String>, headers: HeaderMap) -> Result<String> {
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
) -> Result<(StatusCode, String)> {
    let status = make_status_value()?;
    Ok((StatusCode::BAD_REQUEST, status.to_string()))
}

fn make_status_value() -> Result<serde_json::Value> {
    use gax::error::rpc::StatusDetails;
    use rpc::model::BadRequest;
    use rpc::model::bad_request::FieldViolation;
    let details = StatusDetails::BadRequest(BadRequest::default().set_field_violations(
        vec![FieldViolation::default()
            .set_field( "field" )
            .set_description( "desc" )
        ],
    ));
    let details = serde_json::to_value(&details)?;
    let status = json!({"error": {
        "code": StatusCode::BAD_REQUEST.as_u16(),
        "status": gax::error::rpc::Code::InvalidArgument.name(),
        "message": "this path always returns an error",
        "details": [details],
    }});
    Ok(status)
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

fn internal_error(e: Box<dyn std::error::Error>) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))
}
