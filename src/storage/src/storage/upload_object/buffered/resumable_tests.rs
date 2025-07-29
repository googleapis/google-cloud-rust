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

//! Unit tests for resumable and buffered uploads.
//!
//! A separate module eases navigation and provides some structure to explain
//! the testing strategy.
//!
//! When the upload source does **not** implement [Seek] single-shot uploads are
//! only suitable for relatively small sources. To handle failures in the upload
//! all the data needs to be buffered in memory, as the source offers no
//! "rewind" feature.
//!
//! Resumable uploads can be used for arbitrarily large uploads (up to the
//! service limit), but we need to perform some amount of buffering. Recall that
//! resumable uploads consist of a POST request to create the resumable upload
//! session, followed by a number of PUT requests with the data.
//!
//! The POST request may fail. We use the normal retry loop to handle the
//! failure.
//!
//! The PUT requests may fail, in which case the client library needs to resend
//! any portion of the data not persisted by the service.
//!
//! If the client keeps the last PUT request in a buffer it can handle failures
//! gracefully. After a failure the client can query the status of the upload,
//! discard any data successfully received (potentially all the data) and then
//! continue sending data from that point.
//!
//! The upload quantum adds a bit of complication as the client may pull more
//! data than desired from the upload source. Any extra data needs to be kept
//! until the next `PUT` request.
//!
//! In general the algorithm for a resumable upload is:
//!
//! 1. Try to create a resumable upload session.
//!    - If that fails with a non-retryable error, return immediately.
//!    - If that fails with a retryable error, try again until the retry
//!      policy is exhausted.
//! 2. Collect enough data from the source to fill a `PUT` request of the
//!    desired size.
//!    - If the data pulled from the source is too large, save it for the next
//!      `PUT` request.
//!    - If the data source is done, update the expected size of the upload to
//!      finalize the upload.
//! 3. Start a PUT request to send the buffer data using the upload session.
//!    - If the request succeeds with a 200 status code, return the object.
//!    - If the request fails with 308, go to step 4.
//!    - If the request fails with a non-retryable error return immediately.
//!    - If the request fails with a retryable error and the retry policy is
//!      **not** exhausted, go to step 5.
//!    - If the request fails with a retryable error and the retry policy
//!      **is** exhausted, return immediately.
//! 4. A 308 response indicates that all or at least part of the `PUT` request
//!    was successful.
//!    - Discard the portion of the buffer successfully persisted.
//!    - Add the remaining portion of the buffer to any data saved in step 2.
//!    - Go back to step 2.
//! 5. Query the resumable upload session to find the persisted size.
//!    - If that succeeds, go to step 4.
//!    - If that fails with a non-retryable error, return immediately.
//!    - If that fails with a retryable error, try step 3 again until the
//!      retry policy is exhausted.
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
//! [Seek]: crate::upload_source::Seek

use super::upload_source::{BytesSource, tests::UnknownSize};
use super::{KeyAes256, RESUMABLE_UPLOAD_QUANTUM};
use crate::storage::client::tests::{
    MockBackoffPolicy, MockRetryPolicy, MockRetryThrottler, create_key_helper, test_builder,
};
use gax::retry_policy::RetryPolicyExt;
use gax::retry_result::RetryResult;
use httptest::{Expectation, Server, matchers::*, responders::*};
use serde_json::{Value, json};
use std::time::Duration;

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
async fn empty_success() -> Result {
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
            request::headers(contains(("content-range", "bytes */*")))
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
        .upload_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_if_generation_match(0_i64)
        .send()
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
            request::headers(contains(("content-range", "bytes */*")))
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
        .upload_object(
            "projects/_/buckets/test-bucket",
            "test-object",
            UnknownSize::new(BytesSource::new(bytes::Bytes::from_static(b""))),
        )
        .with_if_generation_match(0_i64)
        .send()
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
async fn empty_csek() -> Result {
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
            request::headers(contains(("content-range", "bytes */*")))
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
        .upload_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_if_generation_match(0_i64)
        .with_key(KeyAes256::new(&key)?)
        .send()
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

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    use crate::upload_source::tests::MockSimpleSource;
    use std::io::{Error as IoError, ErrorKind};
    let mut source = MockSimpleSource::new();
    source
        .expect_next()
        .once()
        .returning(|| Some(Err(IoError::new(ErrorKind::ConnectionAborted, "test-only"))));
    source
        .expect_size_hint()
        .once()
        .returning(|| Ok((1024_u64, Some(1024_u64))));
    let err = client
        .upload_object("projects/_/buckets/test-bucket", "test-object", source)
        .with_if_generation_match(0)
        .with_resumable_upload_threshold(0_usize)
        .send()
        .await
        .expect_err("expected a serialization error");
    assert!(err.is_serialization(), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn start_permanent_error() -> Result {
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
        .upload_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_if_generation_match(0_i64)
        .send()
        .await
        .expect_err("request should fail");
    assert_eq!(response.http_status_code(), Some(403), "{response:?}");

    Ok(())
}

#[tokio::test]
async fn start_too_many_transients() -> Result {
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
        .upload_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
        .with_if_generation_match(0_i64)
        .send()
        .await
        .expect_err("request should fail");
    assert_eq!(response.http_status_code(), Some(429), "{response:?}");

    Ok(())
}

#[tokio::test]
async fn put_permanent_error() -> Result {
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
            request::headers(contains(("content-range", "bytes */*")))
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
        .upload_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_if_generation_match(0_i64)
        .send()
        .await
        .expect_err("request should fail");
    assert_eq!(response.http_status_code(), Some(412), "{response:?}");

    Ok(())
}

#[tokio::test]
async fn put_too_many_transients() -> Result {
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
            request::headers(contains(("content-range", "bytes */*")))
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
        .upload_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
        .with_if_generation_match(0_i64)
        .send()
        .await
        .expect_err("request should fail");
    assert_eq!(response.http_status_code(), Some(429), "{response:?}");

    Ok(())
}

#[tokio::test]
async fn put_partial_and_recover() -> Result {
    // Test with a buffer size that allows partial progress and multiple attempts.
    const QUANTUM: usize = RESUMABLE_UPLOAD_QUANTUM;
    const TARGET: usize = 2 * QUANTUM;
    const FULL: usize = 3 * QUANTUM + QUANTUM / 2;

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
            request::headers(contains((
                "content-range",
                format!("bytes 0-{}/*", TARGET - 1)
            )))
        ])
        .respond_with(status_code(429).body("try-again")),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains((
                "content-range",
                format!("bytes {QUANTUM}-{}/*", QUANTUM + TARGET - 1)
            )))
        ])
        .respond_with(
            status_code(308).append_header("range", format!("bytes=0-{}", 3 * QUANTUM - 1)),
        ),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains((
                "content-range",
                format!("bytes {}-{}/{FULL}", QUANTUM + TARGET, FULL - 1)
            )))
        ])
        .respond_with(status_code(200).body(response_body().to_string())),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes */*")))
        ])
        .respond_with(status_code(308).append_header("range", format!("bytes=0-{}", QUANTUM - 1))),
    );

    let payload = bytes::Bytes::from_owner(vec![0_u8; FULL]);
    let payload = UnknownSize::new(BytesSource::new(payload));
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .with_resumable_upload_buffer_size(TARGET)
        .build()
        .await?;
    let mut upload = client
        .upload_object("projects/_/buckets/test-bucket", "test-object", payload)
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
        .with_if_generation_match(0_i64);
    upload.options.resumable_upload_buffer_size = TARGET;
    let response = upload.send().await;
    assert!(response.is_ok(), "{response:?}");
    let response = response?;
    assert_eq!(response.name, "test-object");
    assert_eq!(response.bucket, "projects/_/buckets/test-bucket");
    assert_eq!(
        response.metadata.get("is-test-object").map(String::as_str),
        Some("true")
    );

    Ok(())
}

#[tokio::test]
async fn put_error_and_finalized() -> Result {
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
            request::headers(contains(("content-range", "bytes */*")))
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
        .upload_object("projects/_/buckets/test-bucket", "test-object", payload)
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
        .with_if_generation_match(0_i64)
        .send()
        .await?;
    assert_eq!(response.name, "test-object");
    assert_eq!(response.bucket, "projects/_/buckets/test-bucket");
    assert_eq!(
        response.metadata.get("is-test-object").map(String::as_str),
        Some("true")
    );

    Ok(())
}

// Verify the retry options are used and that exhausted policies result in
// errors.
#[tokio::test]
async fn start_resumable_upload_request_retry_options() -> Result {
    let server = Server::run();
    let matching = || {
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/bucket/o"),
            request::query(url_decoded(contains(("name", "object")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
    };
    server.expect(
        matching()
            .times(3)
            .respond_with(status_code(503).body("try-again")),
    );

    let mut retry = MockRetryPolicy::new();
    retry
        .expect_on_error()
        .times(1..)
        .returning(|_, _, _, e| RetryResult::Continue(e));

    let mut backoff = MockBackoffPolicy::new();
    backoff
        .expect_on_failure()
        .times(1..)
        .return_const(Duration::from_micros(1));

    let mut throttler = MockRetryThrottler::new();
    throttler
        .expect_throttle_retry_attempt()
        .times(1..)
        .return_const(false);
    throttler
        .expect_on_retry_failure()
        .times(1..)
        .return_const(());
    throttler.expect_on_success().never().return_const(());

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let err = client
        .upload_object("projects/_/buckets/bucket", "object", "hello")
        .with_retry_policy(retry.with_attempt_limit(3))
        .with_backoff_policy(backoff)
        .with_retry_throttler(throttler)
        .build()
        .send_buffered_resumable((0, None))
        .await
        .expect_err("request should fail after 3 retry attempts");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");

    Ok(())
}

// Verify the client retry options are used and that exhausted policies
// result in errors.
#[tokio::test]
async fn start_resumable_upload_client_retry_options() -> Result {
    use gax::retry_policy::RetryPolicyExt;
    let server = Server::run();
    let matching = || {
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/bucket/o"),
            request::query(url_decoded(contains(("name", "object")))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
    };
    server.expect(
        matching()
            .times(3)
            .respond_with(status_code(503).body("try-again")),
    );

    let mut retry = MockRetryPolicy::new();
    retry
        .expect_on_error()
        .times(1..)
        .returning(|_, _, _, e| RetryResult::Continue(e));

    let mut backoff = MockBackoffPolicy::new();
    backoff
        .expect_on_failure()
        .times(1..)
        .return_const(Duration::from_micros(1));

    let mut throttler = MockRetryThrottler::new();
    throttler
        .expect_throttle_retry_attempt()
        .times(1..)
        .return_const(false);
    throttler
        .expect_on_retry_failure()
        .times(1..)
        .return_const(());
    throttler.expect_on_success().never().return_const(());

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_retry_policy(retry.with_attempt_limit(3))
        .with_backoff_policy(backoff)
        .with_retry_throttler(throttler)
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let err = client
        .upload_object("projects/_/buckets/bucket", "object", "hello")
        .send()
        .await
        .expect_err("request should fail after 3 retry attempts");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");

    Ok(())
}
