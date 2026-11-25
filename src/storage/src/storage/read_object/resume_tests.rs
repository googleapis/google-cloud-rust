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

//! Verify read_object() retries reads.
//!
//! Read requests may be rejected by the service. Even after they start
//! successfully, they may be interrupted. The client library should
//! automatically retry reads that fail to start, and automatically resume
//! reads that are interrupted.
//!
//! This module contains tests to verify the library performs these functions.
//!
//! To simulate transient reads errors we use `206 - PARTIAL_CONTENT` responses
//! that return less data than promised.
//!
//! We have not found a way to create HTTP transfer errors with `httptest`, and
//! it is impossible to create a mismatched content length value for 200 - OK
//! responses.
//!
//! In addition to the common retry tests [^1] we need to test scenarios
//! specific to resuming interrupted reads. Specifically:
//!
//! - An interrupted read of the full object is resumed starting with the last
//!   received byte and the received generation.
//! - An interrupted read of a range is resumed starting with the last received
//!   byte and the received generation.
//! - Permanent errors (such as NOT_FOUND) detected while resuming cause the
//!   read to stop.
//! - Resuming a read uses the retry policies configured in the request.
//! - If there are no retry policies in the request, resuming a read uses
//!   the retry policies configured in the client.
//!
//! [^1]: verify that (1) a transient error followed by a successful request
//! works, (2) a transient error followed by a permanent error fails, (3) too
//! many transients results in an error, (4) the policies set in the
//! request options are used for the retry loop, and (5) if there are no
//! policies in the request, the policies set in the client are used for the
//! retry loop.

use crate::model_ext::ReadRange;
use crate::{
    read_resume_policy::{ReadResumePolicyExt, Recommended},
    storage::client::tests::{
        MockBackoffPolicy, MockReadResumePolicy, MockRetryPolicy, MockRetryThrottler, test_builder,
    },
};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::retry_policy::RetryPolicyExt;
use google_cloud_gax::retry_result::RetryResult;
use httptest::{Expectation, Server, matchers::*, responders::*};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Result = anyhow::Result<()>;

#[tokio::test]
async fn start_retry_normal() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(200)
                .append_header("x-goog-generation", 123456)
                .body("hello world"),
        ]),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let mut contents = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        contents.extend_from_slice(&chunk);
    }
    assert_eq!(bytes::Bytes::from_owner(contents), "hello world");
    Ok(())
}

#[tokio::test]
async fn start_permanent_error() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(401).body("uh-oh"),
        ]),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let err = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await
        .expect_err("test generates permanent error");
    assert_eq!(err.http_status_code(), Some(401), "{err:?}");
    Ok(())
}

#[tokio::test]
async fn start_too_many_transients() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .times(3)
        .respond_with(cycle![status_code(429).body("try-again"),]),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let err = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .with_retry_policy(crate::retry_policy::RetryableErrors.with_attempt_limit(3))
        .send()
        .await
        .expect_err("test generates permanent error");
    assert_eq!(err.http_status_code(), Some(429), "{err:?}");
    Ok(())
}

// Verify the retry options are used and that exhausted policies result in
// errors.
#[tokio::test]
async fn start_uses_request_retry_options() -> Result {
    let server = Server::run();
    let matching = || {
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/bucket/o/object"),
            request::query(url_decoded(contains(("alt", "media")))),
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
        .returning(|_, e| RetryResult::Continue(e));

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
        .read_object("projects/_/buckets/bucket", "object")
        .with_retry_policy(retry.with_attempt_limit(3))
        .with_backoff_policy(backoff)
        .with_retry_throttler(throttler)
        .send()
        .await
        .expect_err("request should fail after 3 retry attempts");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");

    Ok(())
}

// Verify the client retry options are used and that exhausted policies
// result in errors.
#[tokio::test]
async fn start_uses_client_retry_options() -> Result {
    use google_cloud_gax::retry_policy::RetryPolicyExt;
    let server = Server::run();
    let matching = || {
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/bucket/o/object"),
            request::query(url_decoded(contains(("alt", "media")))),
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
        .returning(|_, e| RetryResult::Continue(e));

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
        .build()
        .await?;
    let err = client
        .read_object("projects/_/buckets/bucket", "object")
        .send()
        .await
        .expect_err("request should fail after 3 retry attempts");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");

    Ok(())
}

fn test_line(i: usize) -> String {
    let contents = String::from_iter(('a'..='z').cycle().skip(i).take(32));
    format!("{i:06} {contents}\n")
}

fn test_body(range: std::ops::Range<usize>) -> String {
    range.map(test_line).fold(String::new(), |s, l| s + &l)
}

fn return_fragments(server: &Server, count: usize, expect: usize) {
    let fragments = (0..count)
        .map(|i| test_body(i..(i + 1)))
        .collect::<Vec<_>>();
    let length = fragments.iter().fold(0_usize, |s, b| s + b.len());
    let mut acc = 0_usize;

    let responses = fragments
        .into_iter()
        .map(move |fragment| {
            let start = acc;
            acc += fragment.len();
            let responder: Box<dyn Responder> = Box::new(
                status_code(206)
                    .append_header(
                        "content-range",
                        format!("bytes {start}-{}/{length}", length - 1),
                    )
                    .append_header("x-goog-generation", 123456)
                    .body(fragment),
            );
            responder
        })
        .collect::<Vec<_>>();

    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .times(expect)
        .respond_with(cycle(responses)),
    );
}

#[tokio::test]
async fn long_read_error() -> Result {
    let server = Server::run();
    let fragment0 = test_body(0..8);
    let fragment1 = test_body(8..10);
    let len0 = fragment0.len();

    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
            request::query(url_decoded(not(contains(("generation", any()))))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(206)
                .append_header("content-range", format!("bytes 0-{}/{len0}", len0 - 1))
                .append_header("x-goog-generation", 123456)
                .body(fragment0 + &fragment1),
        ]),
    );

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let mut err = None;
    while let Some(r) = reader.next().await {
        if let Err(e) = r {
            err = Some(e);
            break;
        }
    }
    let err = err.expect("too many bytes returned should result in error");
    assert!(err.is_deserialization(), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn resume_after_start() -> Result {
    let server = Server::run();
    let fragment0 = test_body(0..8);
    let fragment1 = test_body(8..10);
    let len0 = fragment0.len();
    let len1 = fragment1.len();
    let length = len0 + len1;

    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
            request::query(url_decoded(not(contains(("generation", any()))))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(206)
                .append_header("content-range", format!("bytes 0-{}/{length}", length - 1))
                .append_header("x-goog-generation", 123456)
                .body(fragment0),
        ]),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(all_of![
                contains(("alt", "media")),
                contains(("generation", "123456"))
            ])),
            request::headers(contains(("range", format!("bytes={len0}-{}", length - 1)))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(206)
                .append_header("x-goog-generation", 123)
                .append_header(
                    "content-range",
                    format!("bytes {}-{}/{length}", len0, length - 1),
                )
                .body(fragment1),
        ]),
    );
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let mut got = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        got.extend_from_slice(&chunk);
    }
    assert_eq!(bytes::Bytes::from_owner(got), test_body(0..10));

    Ok(())
}

#[tokio::test]
async fn resume_after_start_range() -> Result {
    let server = Server::run();
    let fragment0 = test_body(0..6);
    let fragment1 = test_body(6..10);
    let len0 = fragment0.len();
    let len1 = fragment1.len();
    const OFFSET: i32 = 1_000_000;
    let length = OFFSET as usize + len0 + len1;

    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
            request::query(url_decoded(not(contains(("generation", any()))))),
            request::headers(contains(("range", format!("bytes={OFFSET}-")))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(206)
                .append_header(
                    "content-range",
                    format!("bytes {OFFSET}-{}/{length}", length - 1)
                )
                .append_header("x-goog-generation", 123456)
                .body(fragment0),
        ]),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(all_of![
                contains(("alt", "media")),
                contains(("generation", "123456"))
            ])),
            request::headers(contains((
                "range",
                format!("bytes={}-{}", OFFSET as usize + len0, length - 1)
            ))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(206)
                .append_header("x-goog-generation", 123)
                .append_header(
                    "content-range",
                    format!("bytes {}-{}/{length}", OFFSET as usize + len0, length - 1),
                )
                .body(fragment1),
        ]),
    );
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .set_read_range(ReadRange::offset(OFFSET as u64))
        .send()
        .await?;
    let mut got = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        got.extend_from_slice(&chunk);
    }
    assert_eq!(bytes::Bytes::from_owner(got), test_body(0..10));

    Ok(())
}

#[tokio::test]
async fn resume_after_start_permanent() -> Result {
    let server = Server::run();
    let fragment0 = test_body(0..8);
    let fragment1 = test_body(8..10);
    let len0 = fragment0.len();
    let len1 = fragment1.len();
    let length = len0 + len1;

    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
            request::query(url_decoded(not(contains(("generation", any()))))),
        ])
        .times(2)
        .respond_with(cycle![
            status_code(429).body("try-again"),
            status_code(206)
                .append_header("content-range", format!("bytes 0-{}/{length}", length - 1))
                .append_header("x-goog-generation", 123456)
                .body(fragment0),
        ]),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(all_of![
                contains(("alt", "media")),
                contains(("generation", "123456"))
            ])),
            request::headers(contains(("range", format!("bytes={len0}-{}", length - 1)))),
        ])
        .times(1)
        .respond_with(cycle![status_code(404).body("NOT FOUND"),]),
    );
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let mut partial = Vec::new();
    let mut err = None;
    while let Some(r) = reader.next().await {
        match r {
            Ok(b) => partial.extend_from_slice(&b),
            Err(e) => err = Some(e),
        };
    }
    assert_eq!(bytes::Bytes::from_owner(partial), test_body(0..8));
    let err = err.expect("the read should have failed");
    assert_eq!(err.http_status_code(), Some(404), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn request_after_start_too_many_transients() -> Result {
    let server = Server::run();
    return_fragments(&server, 10, 5);
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .with_read_resume_policy(Recommended.with_attempt_limit(5))
        .send()
        .await?;
    let mut partial = Vec::new();
    let mut err = None;
    while let Some(r) = reader.next().await {
        match r {
            Ok(b) => partial.extend_from_slice(&b),
            Err(e) => err = Some(e),
        };
    }
    assert_eq!(bytes::Bytes::from_owner(partial), test_body(0..5));
    let err = err.expect("the read should have failed");
    assert!(err.is_io(), "{err:?}");
    Ok(())
}

// Verify the retry options are used and that exhausted policies result in
// errors.
#[tokio::test]
async fn resume_uses_request_retry_options() -> Result {
    let fragment0 = test_body(0..8);
    let fragment1 = test_body(8..10);
    let len0 = fragment0.len();
    let len1 = fragment1.len();
    let length = len0 + len1;

    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/bucket/o/object"),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .times(4)
        .respond_with(cycle![
            status_code(206)
                .append_header("content-range", format!("bytes 0-{}/{length}", length - 1))
                .append_header("x-goog-generation", 123456)
                .body(fragment0),
            status_code(429).body("try-again"),
            status_code(429).body("try-again"),
            status_code(503).body("try-again"),
        ]),
    );

    let mut retry = MockRetryPolicy::new();
    retry
        .expect_on_error()
        .times(1..)
        .returning(|_, e| RetryResult::Continue(e));

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
    throttler.expect_on_success().times(1).return_const(());

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let mut read = client
        .read_object("projects/_/buckets/bucket", "object")
        .with_retry_policy(retry.with_attempt_limit(3))
        .with_backoff_policy(backoff)
        .with_retry_throttler(throttler)
        .send()
        .await?;

    let mut partial = Vec::new();
    let mut err = None;
    while let Some(r) = read.next().await {
        match r {
            Ok(b) => partial.extend_from_slice(&b),
            Err(e) => err = Some(e),
        };
    }
    assert_eq!(bytes::Bytes::from_owner(partial), test_body(0..8));
    let err = err.expect("read should fail after 3 retry attempts");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");

    Ok(())
}

// Verify the client retry options are used and that exhausted policies
// result in errors.
#[tokio::test]
async fn resume_uses_client_retry_options() -> Result {
    use google_cloud_gax::retry_policy::RetryPolicyExt;

    let fragment0 = test_body(0..8);
    let fragment1 = test_body(8..10);
    let len0 = fragment0.len();
    let len1 = fragment1.len();
    let length = len0 + len1;

    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/bucket/o/object"),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .times(4)
        .respond_with(cycle![
            status_code(206)
                .append_header("content-range", format!("bytes 0-{}/{length}", length - 1))
                .append_header("x-goog-generation", 123456)
                .body(fragment0),
            status_code(429).body("try-again"),
            status_code(429).body("try-again"),
            status_code(503).body("try-again"),
        ]),
    );

    let mut retry = MockRetryPolicy::new();
    retry
        .expect_on_error()
        .times(1..)
        .returning(|_, e| RetryResult::Continue(e));

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
    throttler.expect_on_success().times(1).return_const(());

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_retry_policy(retry.with_attempt_limit(3))
        .with_backoff_policy(backoff)
        .with_retry_throttler(throttler)
        .build()
        .await?;

    let mut read = client
        .read_object("projects/_/buckets/bucket", "object")
        .send()
        .await?;

    let mut partial = Vec::new();
    let mut err = None;
    while let Some(r) = read.next().await {
        match r {
            Ok(b) => partial.extend_from_slice(&b),
            Err(e) => err = Some(e),
        };
    }
    assert_eq!(bytes::Bytes::from_owner(partial), test_body(0..8));
    let err = err.expect("the read should have failed");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn request_resume_options() -> Result {
    let mut sequence = mockall::Sequence::new();

    let mut resume = MockReadResumePolicy::new();
    for i in 1..10 {
        resume
            .expect_on_error()
            .once()
            .in_sequence(&mut sequence)
            .withf(move |got, _| got.attempt_count == i)
            .returning(|_, e| RetryResult::Continue(e));
    }

    let server = Server::run();
    return_fragments(&server, 10, 10);
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .with_read_resume_policy(resume)
        .send()
        .await?;
    let mut got = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        got.extend_from_slice(&chunk);
    }
    assert_eq!(bytes::Bytes::from(got), test_body(0..10));
    Ok(())
}

#[tokio::test]
async fn client_resume_options() -> Result {
    let mut sequence = mockall::Sequence::new();

    let mut resume = MockReadResumePolicy::new();
    for i in 1..10 {
        resume
            .expect_on_error()
            .once()
            .in_sequence(&mut sequence)
            .withf(move |got, _| got.attempt_count == i)
            .returning(|_, e| RetryResult::Continue(e));
    }

    let server = Server::run();
    return_fragments(&server, 10, 10);
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(Anonymous::new().build())
        .with_read_resume_policy(resume)
        .build()
        .await?;
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let mut got = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        got.extend_from_slice(&chunk);
    }
    assert_eq!(bytes::Bytes::from_owner(got), test_body(0..10));
    Ok(())
}

async fn read_http_head(socket: &mut tokio::net::TcpStream) -> String {
    let mut req = Vec::new();
    let mut buf = [0_u8; 1024];
    loop {
        let n = socket.read(&mut buf).await.expect("read request head");
        assert!(n > 0, "unexpected EOF before end of HTTP headers");
        req.extend_from_slice(&buf[..n]);
        if req.windows(4).any(|w| w == b"\r\n\r\n") {
            return String::from_utf8_lossy(&req).into_owned();
        }
    }
}

#[tokio::test]
async fn stalled_body_read_timeout_resumes() -> Result {
    // Arrange
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        // Attempt 1: send 5 of 10 bytes, then stall past read_timeout.
        let (mut s1, _) = listener.accept().await.expect("accept 1 succeeds");
        let _ = read_http_head(&mut s1).await;
        let resp1 = concat!(
            "HTTP/1.1 200 OK\r\n",
            "x-goog-generation: 123456\r\n",
            "content-length: 10\r\n",
            "\r\n",
            "hello",
        );
        s1.write_all(resp1.as_bytes())
            .await
            .expect("write 1 succeeds");

        // Attempt 2: client resumes from byte 5 pinned to generation 123456.
        let (mut s2, _) = listener.accept().await.expect("accept 2 succeeds");
        let req_head = read_http_head(&mut s2).await.to_ascii_lowercase();
        assert!(req_head.contains("range: bytes=5-9\r\n"), "{req_head}");
        assert!(req_head.contains("generation=123456"), "{req_head}");

        let resp2 = concat!(
            "HTTP/1.1 206 Partial Content\r\n",
            "x-goog-generation: 123456\r\n",
            "content-range: bytes 5-9/10\r\n",
            "content-length: 5\r\n",
            "\r\n",
            "world",
        );
        s2.write_all(resp2.as_bytes())
            .await
            .expect("write 2 succeeds");

        // Keep s1 alive until the client finishes so Attempt 1 fails from read_timeout, not EOF.
        let _ = done_rx.await;
        drop(s1);
    });

    let client = test_builder()
        .with_endpoint(format!("http://{addr}"))
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(crate::retry_policy::RetryableErrors.with_attempt_limit(3))
        .with_read_timeout(Duration::from_millis(100))
        .build()
        .await?;

    // Act
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let mut got = Vec::new();
    while let Some(chunk) = reader.next().await.transpose()? {
        got.extend_from_slice(&chunk);
    }

    // Assert
    assert_eq!(bytes::Bytes::from_owner(got), "helloworld");
    let _ = done_tx.send(());
    server.await?;
    Ok(())
}

#[tokio::test]
async fn stalled_body_read_timeout_exhausted_preserves_error_kind() -> Result {
    // Arrange
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        // Attempt 1: send 5 of 10 bytes, then stall past read_timeout.
        let (mut s1, _) = listener.accept().await.expect("accept 1 succeeds");
        let _ = read_http_head(&mut s1).await;
        let resp1 = concat!(
            "HTTP/1.1 200 OK\r\n",
            "x-goog-generation: 123456\r\n",
            "content-length: 10\r\n",
            "\r\n",
            "hello",
        );
        s1.write_all(resp1.as_bytes())
            .await
            .expect("write 1 succeeds");

        // Attempt 2 (resume 1 of 2): send headers, then stall past read_timeout again.
        let (mut s2, _) = listener.accept().await.expect("accept 2 succeeds");
        let req_head = read_http_head(&mut s2).await.to_ascii_lowercase();
        assert!(req_head.contains("range: bytes=5-9\r\n"), "{req_head}");
        assert!(req_head.contains("generation=123456"), "{req_head}");
        let resp2 = concat!(
            "HTTP/1.1 206 Partial Content\r\n",
            "x-goog-generation: 123456\r\n",
            "content-range: bytes 5-9/10\r\n",
            "content-length: 5\r\n",
            "\r\n",
        );
        s2.write_all(resp2.as_bytes())
            .await
            .expect("write 2 succeeds");

        let _ = done_rx.await;
        drop((s1, s2));
    });

    let client = test_builder()
        .with_endpoint(format!("http://{addr}"))
        .with_credentials(Anonymous::new().build())
        .with_retry_policy(crate::retry_policy::RetryableErrors.with_attempt_limit(3))
        .with_read_timeout(Duration::from_millis(100))
        .build()
        .await?;

    // Act
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .with_read_resume_policy(Recommended.with_attempt_limit(2))
        .send()
        .await?;
    let first = reader.next().await.transpose()?;
    let second = reader.next().await;
    let after_err = reader.next().await;

    // Assert
    assert_eq!(first, Some(bytes::Bytes::from_static(b"hello")));
    let err = second
        .expect("stream should yield timeout error once resumes are exhausted")
        .expect_err("second read should time out");
    assert!(err.is_timeout(), "{err:?}");
    assert!(after_err.is_none(), "{after_err:?}");

    let _ = done_tx.send(());
    server.await?;
    Ok(())
}
