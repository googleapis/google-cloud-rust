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

//! Verify read_object() retries downloads.
//!
//! Download requests may be rejected by the service. Even after they start
//! successfully, they may be interrupted. The client library should
//! automatically retry downloads that fail to start, and automatically resume
//! downloads that are interrupted.
//!
//! This module contains tests to verify the library performs these functions.
//!
//! To simulate transient download errors we use 206 - PARTIAL_CONTENT
//! responses that return less data than promised.
//!
//! We have not found a way to create HTTP transfer errors with `httptest`, and
//! it is impossible to create a mismatched content length value for 200 - OK
//! responses.
//!
//! In addition to the common retry tests [^1] we need to test scenarios
//! specific to resuming interrupted downloads. Specifically:
//!
//! - An interrupted download of the full object is resumed starting with the
//!   last received byte and the received generation.
//! - An interrupted download of a range is resumed starting with the last
//!   received byte and the received generation.
//! - Permanent errors (such as NOT_FOUND) detected while resuming cause the
//!   download to stop.
//! - Resuming a download uses the retry policies configured in the request.
//! - If there are no retry policies in the request, resuming a download uses
//!   the retry policies configured in the client.
//!
//! [^1]: verify that (1) a transient error followed by a successful request
//! works, (2) a transient error followed by a permanent error fails, (3) too
//! many transients results in an error, (4) the policies set in the
//! request options are used for the retry loop, and (5) if there are no
//! policies in the request, the policies set in the client are used for the
//! retry loop.

use crate::{
    download_resume_policy::{DownloadResumePolicyExt, Recommended},
    storage::client::tests::{
        MockBackoffPolicy, MockDownloadResumePolicy, MockRetryPolicy, MockRetryThrottler,
        test_builder,
    },
};
use gax::retry_policy::RetryPolicyExt;
use gax::retry_result::RetryResult;
use httptest::{Expectation, Server, matchers::*, responders::*};
use std::time::Duration;

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
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let got = reader.all_bytes().await?;
    assert_eq!(got, "hello world");

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
        .with_credentials(auth::credentials::testing::test_credentials())
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
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let err = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .with_retry_policy(crate::retry_policy::RecommendedPolicy.with_attempt_limit(3))
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
    use gax::retry_policy::RetryPolicyExt;
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
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let err = reader
        .all_bytes()
        .await
        .expect_err("too many bytes returned should result in error");
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
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let got = reader.all_bytes().await?;
    assert_eq!(got, test_body(0..10));

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
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .with_read_offset(OFFSET as i64)
        .send()
        .await?;
    let got = reader.all_bytes().await?;
    assert_eq!(got, test_body(0..10));

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
        .with_credentials(auth::credentials::testing::test_credentials())
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
    let err = err.expect("the download should have failed");
    assert_eq!(err.http_status_code(), Some(404), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn request_after_start_too_many_transients() -> Result {
    let server = Server::run();
    return_fragments(&server, 10, 5);
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let mut reader = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .with_download_resume_policy(Recommended.with_attempt_limit(5))
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
    let err = err.expect("the download should have failed");
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
    throttler.expect_on_success().times(1).return_const(());

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_resumable_upload_threshold(0_usize)
        .build()
        .await?;
    let read = client
        .read_object("projects/_/buckets/bucket", "object")
        .with_retry_policy(retry.with_attempt_limit(3))
        .with_backoff_policy(backoff)
        .with_retry_throttler(throttler)
        .send()
        .await?;

    let err = read
        .all_bytes()
        .await
        .expect_err("download should fail after 3 retry attempts");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");

    Ok(())
}

// Verify the client retry options are used and that exhausted policies
// result in errors.
#[tokio::test]
async fn resume_uses_client_retry_options() -> Result {
    use gax::retry_policy::RetryPolicyExt;

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
    throttler.expect_on_success().times(1).return_const(());

    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_retry_policy(retry.with_attempt_limit(3))
        .with_backoff_policy(backoff)
        .with_retry_throttler(throttler)
        .build()
        .await?;

    let read = client
        .read_object("projects/_/buckets/bucket", "object")
        .send()
        .await?;

    let err = read
        .all_bytes()
        .await
        .expect_err("download should fail after 3 retry attempts");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn request_resume_options() -> Result {
    let mut sequence = mockall::Sequence::new();

    let mut resume = MockDownloadResumePolicy::new();
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
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let got = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .with_download_resume_policy(resume)
        .send()
        .await?
        .all_bytes()
        .await?;
    assert_eq!(got, test_body(0..10));
    Ok(())
}

#[tokio::test]
async fn client_resume_options() -> Result {
    let mut sequence = mockall::Sequence::new();

    let mut resume = MockDownloadResumePolicy::new();
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
        .with_credentials(auth::credentials::testing::test_credentials())
        .with_download_resume_policy(resume)
        .build()
        .await?;
    let got = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?
        .all_bytes()
        .await?;
    assert_eq!(got, test_body(0..10));
    Ok(())
}
