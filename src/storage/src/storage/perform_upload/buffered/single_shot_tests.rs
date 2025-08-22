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

use super::{RESUMABLE_UPLOAD_QUANTUM, SizeHint};
use crate::client::Storage;
use httptest::{Expectation, Server, matchers::*, responders::status_code};

type Result = anyhow::Result<()>;

// We rely on the tests from `unbuffered.rs` for coverage of other
// single-shot upload features. Here we just want to verify the right upload
// type is selected depending on the with_resumable_upload_threshold()
// option.
#[tokio::test]
async fn upload_object_buffered() -> Result {
    let payload = serde_json::json!({
        "name": "test-object",
        "bucket": "test-bucket",
        "metadata": {
            "is-test-object": "true",
        }
    })
    .to_string();
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
        .respond_with(
            status_code(200)
                .append_header("content-type", "application/json")
                .body(payload),
        ),
    );

    let client = Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::testing::test_credentials())
        .with_resumable_upload_threshold(4 * RESUMABLE_UPLOAD_QUANTUM)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .send_buffered()
        .await?;
    assert_eq!(response.name, "test-object");
    assert_eq!(response.bucket, "projects/_/buckets/test-bucket");
    assert_eq!(
        response.metadata.get("is-test-object").map(String::as_str),
        Some("true")
    );

    Ok(())
}

#[tokio::test]
async fn single_shot_source_error() -> Result {
    let server = Server::run();

    let client = Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    use crate::streaming_source::tests::MockSimpleSource;
    use std::io::{Error as IoError, ErrorKind};
    let mut source = MockSimpleSource::new();
    source
        .expect_next()
        .once()
        .returning(|| Some(Err(IoError::new(ErrorKind::ConnectionAborted, "test-only"))));
    source
        .expect_size_hint()
        .once()
        .returning(|| Ok(SizeHint::with_exact(1024)));
    let err = client
        .write_object("projects/_/buckets/test-bucket", "test-object", source)
        .send_buffered()
        .await
        .expect_err("expected a serialization error");
    assert!(err.is_serialization(), "{err:?}");

    Ok(())
}
