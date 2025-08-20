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

//! Verify the client library correctly sets preconditions on uploads.

use super::*;
use crate::storage::streaming_source::BytesSource;
use gax::retry_policy::{Aip194Strict, RetryPolicyExt};
use httptest::{Expectation, Server, matchers::*, responders::*};
use serde_json::{Value, json};

const VEXING: &str = "how vexingly quick daft zebras jump";

#[tokio::test]
async fn buffered_single_shot() -> Result {
    let server = single_shot_server();
    let object = start_single_shot(&server).await?.send_buffered().await?;
    assert_eq!(object.name, "test-object");
    Ok(())
}

#[tokio::test]
async fn buffered_resumable() -> Result {
    let server = resumable_server();
    let object = start_resumable(&server).await?.send_buffered().await?;
    assert_eq!(object.name, "test-object");
    Ok(())
}

#[tokio::test]
async fn unbuffered_single_shot() -> Result {
    let server = single_shot_server();
    let object = start_single_shot(&server).await?.send_unbuffered().await?;
    assert_eq!(object.name, "test-object");
    Ok(())
}

#[tokio::test]
async fn unbuffered_resumable() -> Result {
    let server = resumable_server();
    let object = start_resumable(&server).await?.send_unbuffered().await?;
    assert_eq!(object.name, "test-object");
    Ok(())
}

async fn start_resumable(server: &Server) -> anyhow::Result<WriteObject<BytesSource>> {
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .build()
        .await?;
    Ok(client
        .write_object("projects/_/buckets/test-bucket", "test-object", VEXING)
        .with_resumable_upload_threshold(0_usize)
        .with_retry_policy(Aip194Strict.with_attempt_limit(3))
        .set_if_generation_match(0)
        .set_if_generation_not_match(1)
        .set_if_metageneration_match(2)
        .set_if_metageneration_not_match(3))
}

async fn start_single_shot(server: &Server) -> anyhow::Result<WriteObject<BytesSource>> {
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .build()
        .await?;
    Ok(client
        .write_object("projects/_/buckets/test-bucket", "test-object", VEXING)
        .with_resumable_upload_threshold(1024 * 1024_usize)
        .with_retry_policy(Aip194Strict.with_attempt_limit(3))
        .set_if_generation_match(0)
        .set_if_generation_not_match(1)
        .set_if_metageneration_match(2)
        .set_if_metageneration_not_match(3))
}

fn single_shot_server() -> Server {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(all_of![
                contains(("name", "test-object")),
                contains(("uploadType", "multipart")),
                contains(("ifGenerationMatch", "0")),
                contains(("ifGenerationNotMatch", "1")),
                contains(("ifMetagenerationMatch", "2")),
                contains(("ifMetagenerationNotMatch", "3")),
            ])),
        ])
        .respond_with(status_code(200).body(body().to_string())),
    );
    server
}

fn resumable_server() -> Server {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    let path = session.path().to_string();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(all_of![
                contains(("name", "test-object")),
                contains(("uploadType", "resumable")),
                contains(("ifGenerationMatch", "0")),
                contains(("ifGenerationNotMatch", "1")),
                contains(("ifMetagenerationMatch", "2")),
                contains(("ifMetagenerationNotMatch", "3")),
            ])),
        ])
        .respond_with(status_code(200).append_header("location", session.to_string())),
    );
    let len = VEXING.len();
    assert_ne!(len, 0);
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains((
                "content-range",
                format!("bytes 0-{}/{len}", len - 1)
            )))
        ])
        .respond_with(
            status_code(200)
                .append_header("content-type", "application/json")
                .body(body().to_string()),
        ),
    );
    server
}

fn body() -> Value {
    json!({
        "bucket": "/projects/_/buckets/test-bucket",
        "name": "test-object",
        "size": 35,
    })
}
