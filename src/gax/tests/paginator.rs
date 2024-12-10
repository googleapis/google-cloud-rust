
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

use gcp_sdk_gax::http_client::*;
use std::collections::HashMap;
use warp::http::Response;
use warp::Filter;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_paginator() -> Result<()> {
    // Create a small server to satisfy the request.

    let page = warp::get()
        .and(warp::path("pagination"))
        .and(warp::query::<HashMap<String, String>>())
        .map(|p| handle_page(p));
    let (socket, server) = warp::serve(page).bind_ephemeral(([127, 0, 0, 1], 0));

    let _server = tokio::spawn(async move { server.await });

    println!("{socket:?}");

    let socket = format!("http://{socket}");
    let client = Client::new(&socket).await?;

    let mut page_token = String::default();
    let mut items = Vec::new(); 
    loop {
        let response = client.list(ListFoosRequest { page_token: page_token }).await?;
        response.items.into_iter().for_each(|s| items.push(s.name));
        page_token = response.next_page_token;
        if page_token == "" {
            break;
        }
    }
    assert_eq!(items, ["f1", "f2", "f3", "f4"].map(str::to_string).to_vec());

    Ok(())
}

struct Client {
    inner: ReqwestClient,
}

impl Client {
    pub async fn new(default_endpoint: &str) -> Result<Self> {
        let config =  ClientConfig::default().set_credential(auth::Credential::test_credentials());
        let inner = ReqwestClient::new(config, default_endpoint).await?;
        Ok(Self { inner })
    }

    pub async fn list(&self, req: ListFoosRequest) -> std::result::Result<ListFoosResponse, gcp_sdk_gax::error::Error> {
        let mut builder = self.inner.builder(
            reqwest::Method::GET, "/pagination".to_string())
            .query(&[("alt", "json")]);
        if "" != req.page_token {
            builder = builder.query(&[("pageToken", req.page_token)]);
        }
        self.inner.execute(builder, None::<NoBody>).await
    }
}

fn handle_page(query: HashMap<String, String>) -> warp::http::Result<Response<String>> {
    let to_items = |v: &[&str]| { v.iter().map(|s| Foo { name: s.to_string() }).collect::<Vec<_>>() };

    let page_token = query.get("pageToken").map(String::as_str).unwrap_or("");
    let response = if page_token == "" {
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

    let response = match response {
        Ok(s) => Response::builder()
            .header("content-type", "application/json")
            .body(s),
        Err(e) => Response::builder()
            .status(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
            .body(format!("{e}")),
    };
    let response = response?;
    Ok(response)
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
