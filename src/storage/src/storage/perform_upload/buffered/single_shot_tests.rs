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
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use httptest::{Expectation, Server, matchers::*};

type Result = anyhow::Result<()>;

struct Capture(std::sync::Arc<std::sync::Mutex<Option<(String, bytes::Bytes)>>>);

impl httptest::responders::Responder for Capture {
    fn respond<'a>(
        &mut self,
        req: &'a http::Request<bytes::Bytes>,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = http::Response<bytes::Bytes>> + std::marker::Send + 'a>,
    > {
        let ct = req
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        *self.0.lock().unwrap() = Some((ct, req.body().clone()));
        let res = http::Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(
                serde_json::json!({
                    "name": "test-object",
                    "bucket": "test-bucket",
                    "metadata": {
                        "is-test-object": "true",
                    }
                })
                .to_string()
                .into(),
            )
            .unwrap();
        Box::pin(async move { res })
    }
}

// We rely on the tests from `unbuffered.rs` for coverage of other
// single-shot upload features. Here we verify the right upload type is selected
// and that buffered uploads compute CRC32C upfront in Part 1 without trailing metadata.
#[tokio::test]
async fn upload_object_buffered() -> Result {
    let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
        .respond_with(Capture(captured.clone())),
    );

    let client = Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .with_resumable_upload_threshold(4 * RESUMABLE_UPLOAD_QUANTUM)
        .build()
        .await?;

    const PAYLOAD: &str = "how vexingly quick daft zebras jump";
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", PAYLOAD)
        .send_buffered()
        .await?;
    assert_eq!(response.name, "test-object");
    assert_eq!(response.bucket, "projects/_/buckets/test-bucket");
    assert_eq!(
        response.metadata.get("is-test-object").map(String::as_str),
        Some("true")
    );

    let (content_type, body) = captured.lock().unwrap().take().expect("captured request");
    let boundary = content_type
        .strip_prefix("multipart/related; boundary=")
        .expect("content-type must specify multipart boundary")
        .to_string();

    let stream = futures::stream::once(async move { Ok::<_, std::io::Error>(body) });
    let mut multipart = multer::Multipart::new(stream, boundary);

    // Part 1: Metadata containing upfront computed crc32c
    let m = multipart
        .next_field()
        .await?
        .expect("missing metadata field");
    let metadata_json: serde_json::Value = serde_json::from_slice(&m.bytes().await?)?;
    assert_eq!(
        metadata_json.get("crc32c"),
        Some(&serde_json::json!("9esWHQ=="))
    );

    // Part 2: Media payload bytes
    let p = multipart
        .next_field()
        .await?
        .expect("missing payload field");
    assert_eq!(p.bytes().await?, PAYLOAD);

    // Part 3: Must NOT be present
    assert!(
        multipart.next_field().await?.is_none(),
        "buffered single-shot must not append Part 3 trailing metadata"
    );

    Ok(())
}

#[tokio::test]
async fn single_shot_source_error() -> Result {
    let server = Server::run();

    let client = Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
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
