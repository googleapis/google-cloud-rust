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

use gcp_sdk_gax::error::rpc::Status;
use gcp_sdk_gax::error::Error;
use gcp_sdk_gax::error::HttpError;
use std::collections::HashMap;

#[derive(Debug, Default)]
struct LeafError {}

impl LeafError {
    fn hey(&self) -> &'static str {
        "hey"
    }
}

impl std::fmt::Display for LeafError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "other error")
    }
}

impl std::error::Error for LeafError {}

#[derive(Debug)]
struct MiddleError {
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for MiddleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "middle error")
    }
}

impl std::error::Error for MiddleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.source {
            Some(e) => Some(e.as_ref()),
            None => None,
        }
    }
}

#[test]
fn downcast() -> Result<(), Box<dyn std::error::Error>> {
    let leaf_err = LeafError::default();
    let middle_err = MiddleError {
        source: Some(Box::new(leaf_err)),
    };
    let root_err = Error::other(middle_err);
    let msg = root_err.as_inner::<LeafError>().unwrap().hey();
    assert_eq!(msg, "hey");

    let root_err = Error::other(MiddleError { source: None });
    let inner_err = root_err.as_inner::<LeafError>();
    assert!(inner_err.is_none());
    Ok(())
}

#[tokio::test]
async fn client_http_error() -> Result<(), Box<dyn std::error::Error>> {
    let http_resp = http::Response::builder()
        .header("Content-Type", "application/json")
        .status(400)
        .body(r#"{"error": "bad request"}"#)?;

    // Into reqwest response, like our clients use.
    let resp: reqwest::Response = http_resp.into();

    assert!(resp.status().is_client_error());

    let status = resp.status().as_u16();
    let headers = gcp_sdk_gax::error::convert_headers(resp.headers());
    let body = resp.bytes().await?;

    let http_err = HttpError::new(status, headers, Some(body));
    assert!(http_err.status_code() == 400);
    assert!(http_err.headers()["content-type"] == "application/json");
    assert!(http_err.payload().unwrap() == r#"{"error": "bad request"}"#.as_bytes());
    Ok(())
}

#[test]
fn http_error_to_status() -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::json!({
        "code": 9,
        "message": "msg",
        "details": [
            {"violations": [{"type": "type", "subject": "subject", "description": "desc"}]},
        ]
    });
    let json = serde_json::json!({"error": json});
    let http_err = HttpError::new(
        400,
        HashMap::from_iter([("content-type".to_string(), "application/json".to_string())]),
        Some(json.to_string().into()),
    );

    let status: Status = http_err.try_into()?;
    assert_eq!(status.code, 9);
    assert_eq!(status.message, "msg");
    assert_eq!(status.details.len(), 1);

    let html = r#"<!DOCTYPE html>
<html lang=en>
<meta charset=utf-8>
<title>Error 500!!!</title>"#
        .as_bytes();
    let http_err = HttpError::new(
        500,
        HashMap::from_iter([("content-type".to_string(), "text/html".to_string())]),
        Some(html.into()),
    );

    let status: Result<Status, Error> = http_err.try_into();
    assert!(status.is_err());

    Ok(())
}
