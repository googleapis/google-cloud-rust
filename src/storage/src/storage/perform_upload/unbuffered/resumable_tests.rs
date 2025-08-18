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

//! Unit tests for resumable uploads and unbuffered uploads.
//!
//! A separate module eases navigation and provides some structure to explain
//! the testing strategy. When the upload source implements [Seek] the client
//! library does not need to buffer any data to retry or resume uploads. In the
//! case of resumable uploads, on a recoverable error the client library can
//! rewind the source to the last byte successfully received by the service and
//! send the data from that point.
//!
//! In general the algorithm for a resumable upload is:
//! 1. Try to create a resumable upload session.
//!    - If that fails with a non-retryable error, return immediately.
//!    - If that fails with a retryable error, try again until the retry
//!      policy is exhausted.
//! 2. Start a PUT request to send all the data using the upload session.
//!    - If the request succeeds with a 200 status code, return the object.
//!    - If the request fails with 308, go to step 3.
//!    - If the request fails with a non-retryable error return immediately.
//!    - If the request fails with a retryable error and the retry policy is
//!      **not** exhausted, go to step 3.
//!    - If the request fails with a retryable error and the retry policy
//!      **is** exhausted, return immediately.
//! 3. Query the resumable upload session to find the persisted size.
//!    - If that succeeds, go to 4.
//!    - If that fails with a non-retryable error, return immediately.
//!    - If that fails with a retryable error, try step 3 again until the
//!      retry policy is exhausted.
//! 4. Rewind the data source to the new offset.
//! 5. Go to step 2.
//!
//! When the size of the data is not known the upload may require two PUT
//! requests. The first PUT request finalizes the upload **IF** the upload is
//! not a multiple of the upload quantum (256 KiB). In other cases the upload
//! will return a 308, the client library queries the status of the upload and
//! sends a second PUT for 0 bytes, which does finish the upload.
//!
//! We need tests to verify the client library correctly handles multiple
//! scenarios, in particular:
//!
//! - A successful upload where the size of the data is known.
//! - A successful upload where the size of the data is not known.
//! - A successful upload with CSEK encryption.
//! - An upload where the source returns errors on seek().
//! - An upload where the source returns errors on next().
//! - An upload that fails due to a permanent error while creating the upload
//!   session.
//! - An upload that fails due to too many transients creating the upload
//!   session.
//! - An upload that fails due to a permanent error while sending data.
//! - An upload that fails due to too many transients while sending data.
//! - An upload that fails due to a permanent error while querying the
//!   upload status.
//! - An upload that fails due to too many transients while querying the
//!   upload status.
//!
//! - An upload that partially fails: the PUT request returns an error, querying
//!   the status reveals that only part of the data was uploaded, the upload
//!   should continue with the remaining data.
//! - An upload that succeeds despite a PUT error. The data may arrive to the
//!   service but the PUT request fails to read the response or otherwise fails.
//!   The next query returns a finalized upload status.
//!
//! [Seek]: crate::streaming_source::Seek

use crate::model_ext::{KeyAes256, tests::create_key_helper};
use crate::storage::client::{Storage, tests::test_builder};
use crate::streaming_source::{BytesSource, SizeHint, tests::UnknownSize};
use gax::retry_policy::RetryPolicyExt;
use httptest::{Expectation, Server, matchers::*, responders::*};
use serde_json::{Value, json};

type Result = anyhow::Result<()>;

fn response_body() -> Value {
    json!({
        "name": "test-object",
        "bucket": "test-bucket",
        "metadata": {
            "is-test-object": "true",
        }
    })
}

#[tokio::test]
async fn resumable_empty_success() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    let path = session.path().to_string();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(200).append_header("location", session.to_string()),
        ]),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains(("content-range", "bytes */0")))
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(200)
                .append_header("content-type", "application/json")
                .body(response_body().to_string())
        ]),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains(("content-range", "bytes */*"))),
            request::headers(contains(("content-length", "0"))),
        ])
        .times(1)
        .respond_with(status_code(308)),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_if_generation_match(0_i64)
        .send_unbuffered()
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
async fn resumable_empty_unknown() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    let path = session.path().to_string();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(200).append_header("location", session.to_string()),
        ]),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains(("content-range", "bytes 0-*/*")))
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(200)
                .append_header("content-type", "application/json")
                .body(response_body().to_string())
        ]),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains(("content-range", "bytes */*"))),
            request::headers(contains(("content-length", "0"))),
        ])
        .times(1)
        .respond_with(status_code(308)),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object(
            "projects/_/buckets/test-bucket",
            "test-object",
            UnknownSize::new(BytesSource::new(bytes::Bytes::from_static(b""))),
        )
        .with_if_generation_match(0_i64)
        .send_unbuffered()
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
async fn resumable_empty_csek() -> Result {
    let (key, key_base64, _, key_sha256_base64) = create_key_helper();

    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    let path = session.path().to_string();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
            request::headers(contains(("x-goog-encryption-algorithm", "AES256"))),
            request::headers(contains(("x-goog-encryption-key", key_base64.clone()))),
            request::headers(contains((
                "x-goog-encryption-key-sha256",
                key_sha256_base64.clone()
            ))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(200).append_header("location", session.to_string()),
        ]),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains(("content-range", "bytes */0"))),
            request::headers(contains(("x-goog-encryption-algorithm", "AES256"))),
            request::headers(contains(("x-goog-encryption-key", key_base64.clone()))),
            request::headers(contains((
                "x-goog-encryption-key-sha256",
                key_sha256_base64.clone()
            ))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(200)
                .append_header("content-type", "application/json")
                .body(response_body().to_string())
        ]),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains(("content-range", "bytes */*"))),
            request::headers(contains(("content-length", "0"))),
        ])
        .times(1)
        .respond_with(status_code(308)),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_if_generation_match(0_i64)
        .with_key(KeyAes256::new(&key)?)
        .send_unbuffered()
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
async fn source_seek_error() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .times(1)
        .respond_with(cycle![
            status_code(200).append_header("location", session.to_string()),
        ]),
    );

    let client = Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    use crate::streaming_source::tests::MockSeekSource;
    use std::io::{Error as IoError, ErrorKind};
    let mut source = MockSeekSource::new();
    source.expect_next().never();
    source
        .expect_seek()
        .once()
        .returning(|_| Err(IoError::new(ErrorKind::ConnectionAborted, "test-only")));
    source
        .expect_size_hint()
        .once()
        .returning(|| Ok(SizeHint::with_exact(1024)));
    let err = client
        .write_object("projects/_/buckets/test-bucket", "test-object", source)
        .with_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await
        .expect_err("expected a serialization error");
    assert!(err.is_serialization(), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn source_next_error() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .times(1)
        .respond_with(cycle![
            status_code(200).append_header("location", session.to_string()),
        ]),
    );

    let client = Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    use crate::streaming_source::tests::MockSeekSource;
    use std::io::{Error as IoError, ErrorKind};
    let mut source = MockSeekSource::new();
    source
        .expect_next()
        .once()
        .returning(|| Some(Err(IoError::new(ErrorKind::ConnectionAborted, "test-only"))));
    source.expect_seek().times(1..).returning(|_| Ok(()));
    source
        .expect_size_hint()
        .returning(|| Ok(SizeHint::with_exact(1024)));
    let err = client
        .write_object("projects/_/buckets/test-bucket", "test-object", source)
        .with_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send_unbuffered()
        .await
        .expect_err("expected a serialization error");
    assert!(err.is_serialization(), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn resumable_start_permanent_error() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(403).body("uh-oh"),
        ]),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_if_generation_match(0_i64)
        .send_unbuffered()
        .await
        .expect_err("request should fail");
    assert_eq!(response.http_status_code(), Some(403), "{response:?}");

    Ok(())
}

#[tokio::test]
async fn resumable_start_too_many_transients() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .times(3)
        .respond_with(status_code(429).body("try-again")),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
        .with_if_generation_match(0_i64)
        .send_unbuffered()
        .await
        .expect_err("request should fail");
    assert_eq!(response.http_status_code(), Some(429), "{response:?}");

    Ok(())
}

#[tokio::test]
async fn resumable_query_permanent_error() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    let path = session.path().to_string();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .respond_with(status_code(200).append_header("location", session.to_string())),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains(("content-range", "bytes */0")))
        ])
        .respond_with(status_code(429).body("try-again")),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains(("content-range", "bytes */*"))),
            request::headers(contains(("content-length", "0"))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(404).body("not found"),
        ]),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_if_generation_match(0_i64)
        .send_unbuffered()
        .await
        .expect_err("request should fail");
    assert_eq!(response.http_status_code(), Some(404), "{response:?}");

    Ok(())
}

#[tokio::test]
async fn resumable_query_too_many_transients() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .respond_with(status_code(200).append_header("location", session.to_string())),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes */0")))
        ])
        .respond_with(status_code(429).body("try-again")),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes */*"))),
            request::headers(contains(("content-length", "0"))),
        ])
        .times(2)
        .respond_with(status_code(429).body("try-again")),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
        .with_if_generation_match(0_i64)
        .send_unbuffered()
        .await
        .expect_err("request should fail");
    assert_eq!(response.http_status_code(), Some(429), "{response:?}");

    Ok(())
}

#[tokio::test]
async fn resumable_put_permanent_error() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    let path = session.path().to_string();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .respond_with(status_code(200).append_header("location", session.to_string())),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains(("content-range", "bytes */0")))
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(412).body("precondition"),
        ]),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", path.clone()),
            request::headers(contains(("content-range", "bytes */*"))),
            request::headers(contains(("content-length", "0"))),
        ])
        .times(1)
        .respond_with(status_code(308)),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_if_generation_match(0_i64)
        .send_unbuffered()
        .await
        .expect_err("request should fail");
    assert_eq!(response.http_status_code(), Some(412), "{response:?}");

    Ok(())
}

#[tokio::test]
async fn resumable_put_too_many_transients() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .respond_with(status_code(200).append_header("location", session.to_string())),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes */0")))
        ])
        .times(3)
        .respond_with(status_code(429).body("try-again")),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes */*"))),
            request::headers(contains(("content-length", "0"))),
        ])
        .times(2)
        .respond_with(status_code(308)),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
        .with_if_generation_match(0_i64)
        .send_unbuffered()
        .await
        .expect_err("request should fail");
    assert_eq!(response.http_status_code(), Some(429), "{response:?}");

    Ok(())
}

#[tokio::test]
async fn resumable_put_partial_and_recover_unknown_size() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .respond_with(status_code(200).append_header("location", session.to_string())),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes 0-*/*")))
        ])
        .respond_with(status_code(429).body("try-again")),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes 256-*/*")))
        ])
        .respond_with(status_code(429).body("try-again")),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes 512-*/*")))
        ])
        .respond_with(status_code(200).body(response_body().to_string())),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes */*"))),
            request::headers(contains(("content-length", "0"))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(308).append_header("range", "bytes=0-255"),
            status_code(308).append_header("range", "bytes=0-511"),
        ]),
    );

    let payload = bytes::Bytes::from_owner(vec![0_u8; 1_000]);
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object(
            "projects/_/buckets/test-bucket",
            "test-object",
            UnknownSize::new(BytesSource::new(payload)),
        )
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
        .with_if_generation_match(0_i64)
        .send_unbuffered()
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
async fn resumable_put_partial_and_recover_known_size() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .respond_with(status_code(200).append_header("location", session.to_string())),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes 0-999/1000")))
        ])
        .respond_with(status_code(429).body("try-again")),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes 256-999/1000")))
        ])
        .respond_with(status_code(429).body("try-again")),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes 512-999/1000")))
        ])
        .respond_with(status_code(200).body(response_body().to_string())),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes */*"))),
            request::headers(contains(("content-length", "0"))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(308).append_header("range", "bytes=0-255"),
            status_code(308).append_header("range", "bytes=0-511"),
        ]),
    );

    let payload = bytes::Bytes::from_owner(vec![0_u8; 1_000]);
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", payload)
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
        .with_if_generation_match(0_i64)
        .send_unbuffered()
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
async fn resumable_put_error_and_finalized() -> Result {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("ifGenerationMatch", "0")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .respond_with(status_code(200).append_header("location", session.to_string())),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes 0-999/1000")))
        ])
        .respond_with(status_code(429).body("try-again")),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes */*"))),
            request::headers(contains(("content-length", "0"))),
        ])
        .times(1)
        .respond_with(cycle![status_code(200).body(response_body().to_string()),]),
    );

    let payload = bytes::Bytes::from_owner(vec![0_u8; 1_000]);
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", payload)
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
        .with_if_generation_match(0_i64)
        .send_unbuffered()
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
async fn resumable_upload_handle_response_success() -> Result {
    let response = http::Response::builder()
        .status(200)
        .body(response_body().to_string())?;
    let response = reqwest::Response::from(response);
    let object = super::handle_object_response(response).await?;
    assert_eq!(object.name, "test-object");
    assert_eq!(object.bucket, "projects/_/buckets/test-bucket");
    assert_eq!(
        object.metadata.get("is-test-object").map(String::as_str),
        Some("true")
    );
    Ok(())
}

#[tokio::test]
async fn resumable_upload_handle_response_http_error() -> Result {
    let response = http::Response::builder().status(429).body("try-again")?;
    let response = reqwest::Response::from(response);
    let err = super::handle_object_response(response)
        .await
        .expect_err("HTTP error should return errors");
    assert_eq!(err.http_status_code(), Some(429), "{err:?}");
    Ok(())
}

#[tokio::test]
async fn resumable_upload_handle_response_deser() -> Result {
    let response = http::Response::builder()
        .status(200)
        .body("a string is not an object")?;
    let response = reqwest::Response::from(response);
    let err = super::handle_object_response(response)
        .await
        .expect_err("bad format should return errors");
    assert!(err.is_deserialization(), "{err:?}");
    Ok(())
}
