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

use axum::extract::Query;
use axum::http::StatusCode;
use gcp_sdk_gax::http_client::*;
use gcp_sdk_gax::paginator::{ItemPaginator, PageableResponse, Paginator};
use std::collections::HashMap;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_paginator() -> Result<()> {
    // Create a small server to satisfy the request.
    let app = axum::Router::new().route("/pagination", axum::routing::get(handle_page));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let endpoint = format!("http://{}:{}", addr.ip(), addr.port());
    println!("endpoint = {endpoint}");

    let _server = tokio::spawn(async { axum::serve(listener, app).await });
    let client = Client::new(&endpoint).await?;

    let mut page_token = String::default();
    let mut items = Vec::new();
    loop {
        let response = client.list(ListFoosRequest { page_token }).await?;
        response.items.into_iter().for_each(|s| items.push(s.name));
        page_token = response.next_page_token;
        if page_token.is_empty() {
            break;
        }
    }
    assert_eq!(items, ["f1", "f2", "f3", "f4"].map(str::to_string).to_vec());

    let mut items = Vec::new();
    let mut stream = client.list_stream(ListFoosRequest::default());
    while let Some(response) = stream.next().await {
        let response = response?;
        response.items.into_iter().for_each(|s| items.push(s.name));
    }
    assert_eq!(items, ["f1", "f2", "f3", "f4"].map(str::to_string).to_vec());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_item_paginator() -> Result<()> {
    // Create a small server to satisfy the request.
    let app = axum::Router::new().route("/pagination", axum::routing::get(handle_page));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;
    let endpoint = format!("http://{}:{}", addr.ip(), addr.port());
    println!("endpoint = {endpoint}");

    let _server = tokio::spawn(async { axum::serve(listener, app).await });
    let client = Client::new(&endpoint).await?;

    let mut items = Vec::new();
    let mut stream = client.list_stream_items(ListFoosRequest::default());
    while let Some(response) = stream.next().await {
        let response = response?;
        items.push(response.name);
    }
    assert_eq!(items, ["f1", "f2", "f3", "f4"].map(str::to_string).to_vec());

    Ok(())
}

struct Client {
    inner: ReqwestClient,
}

impl PageableResponse for ListFoosResponse {
    type PageItem = Foo;

    fn items(self) -> Vec<Self::PageItem> {
        self.items
    }
    fn next_page_token(&self) -> String {
        self.next_page_token.clone()
    }
}

impl Client {
    pub async fn new(default_endpoint: &str) -> Result<Self> {
        let config = ClientConfig::default().set_credential(auth::Credential::test_credentials());
        let inner = ReqwestClient::new(config, default_endpoint).await?;
        Ok(Self { inner })
    }

    pub fn list_stream(
        &self,
        req: ListFoosRequest,
    ) -> Paginator<ListFoosResponse, gcp_sdk_gax::error::Error> {
        let inner = self.inner.clone();
        let execute = move |page_token| {
            let mut request = req.clone();
            request.page_token = page_token;
            let client = Self {
                inner: inner.clone(),
            };
            client.list_impl(request)
        };
        Paginator::new(String::new(), execute)
    }

    pub fn list_stream_items(
        &self,
        req: ListFoosRequest,
    ) -> ItemPaginator<ListFoosResponse, gcp_sdk_gax::error::Error> {
        self.list_stream(req).items()
    }

    async fn list_impl(
        self,
        req: ListFoosRequest,
    ) -> std::result::Result<ListFoosResponse, gcp_sdk_gax::error::Error> {
        let mut builder = self
            .inner
            .builder(reqwest::Method::GET, "/pagination".to_string())
            .query(&[("alt", "json")]);
        if !req.page_token.is_empty() {
            builder = builder.query(&[("pageToken", req.page_token)]);
        }
        self.inner
            .execute(
                builder,
                None::<NoBody>,
                gcp_sdk_gax::options::RequestOptions::default(),
            )
            .await
    }

    pub async fn list(
        &self,
        req: ListFoosRequest,
    ) -> std::result::Result<ListFoosResponse, gcp_sdk_gax::error::Error> {
        let client = Self {
            inner: self.inner.clone(),
        };
        client.list_impl(req).await
    }
}

async fn handle_page(Query(params): Query<HashMap<String, String>>) -> (StatusCode, String) {
    let to_items = |v: &[&str]| {
        v.iter()
            .map(|s| Foo {
                name: s.to_string(),
            })
            .collect::<Vec<_>>()
    };

    let page_token = params.get("pageToken").map(String::as_str).unwrap_or("");
    let response = if page_token.is_empty() {
        ListFoosResponse {
            items: to_items(&["f1", "f2"]),
            next_page_token: "abc123".to_string(),
        }
    } else {
        ListFoosResponse {
            items: to_items(&["f3", "f4"]),
            next_page_token: String::default(),
        }
    };
    let response = serde_json::to_string(&response);

    match response {
        Ok(s) => (StatusCode::OK, s),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")),
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
struct ListFoosRequest {
    pub page_token: String,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
struct ListFoosResponse {
    pub items: Vec<Foo>,
    pub next_page_token: String,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
struct Foo {
    pub name: String,
}
