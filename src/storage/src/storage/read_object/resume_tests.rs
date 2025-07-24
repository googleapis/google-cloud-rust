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
//! TODO(#2048) - also test "resume", that is, continuing a download after an
//!   interrupted stream.

use crate::storage::client::tests::{
    MockBackoffPolicy, MockRetryPolicy, MockRetryThrottler, test_builder,
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
            status_code(200).body("hello world"),
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
async fn request_retry_options() -> Result {
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
async fn client_retry_options() -> Result {
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
