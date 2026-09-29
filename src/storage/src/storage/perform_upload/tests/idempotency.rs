// Copyright 2026 Google LLC
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

//! Verify the client library sends `x-goog-gcs-idempotency-token` correctly on
//! uploads.

use super::*;
use crate::idempotency::IDEMPOTENCY_TOKEN_HEADER;
use crate::storage::streaming_source::BytesSource;
use httptest::{Expectation, Server, matchers::*, responders::*};
use std::sync::Mutex;

// The token is stamped once, outside the retry loop, so every attempt sends the
// same token.
#[tokio::test]
async fn unbuffered_single_shot_reuses_token() -> Result {
    let (server, tokens) = single_shot_server(2);
    start_single_shot(&server)
        .await?
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
    let tokens = tokens.lock().unwrap();
    let [Some(first), Some(second)] = tokens.as_slice() else {
        panic!("expected 2 captured tokens, got {tokens:?}");
    };
    assert_eq!(first, second);
    Ok(())
}

#[tokio::test]
async fn unbuffered_single_shot_unconditioned_omits_token() -> Result {
    let (server, tokens) = single_shot_server(1);
    let err = start_single_shot(&server)
        .await?
        .send_unbuffered()
        .await
        .expect_err("unconditioned uploads are not retried");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");
    assert_eq!(*tokens.lock().unwrap(), vec![None]);
    Ok(())
}

#[tokio::test]
async fn unbuffered_single_shot_idempotency_false_omits_token() -> Result {
    let (server, tokens) = single_shot_server(1);
    let err = start_single_shot(&server)
        .await?
        .set_if_generation_match(0)
        .with_idempotency(false)
        .send_unbuffered()
        .await
        .expect_err("with_idempotency(false) disables retries");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");
    assert_eq!(*tokens.lock().unwrap(), vec![None]);
    Ok(())
}

#[tokio::test]
async fn buffered_resumable_reuses_token() -> Result {
    let (server, tokens) = resumable_server();
    start_resumable(&server)
        .await?
        .set_if_generation_match(0)
        .send_buffered()
        .await?;
    let tokens = tokens.lock().unwrap();
    let [Some(first), Some(second)] = tokens.as_slice() else {
        panic!("expected 2 captured tokens, got {tokens:?}");
    };
    assert_eq!(first, second);
    Ok(())
}

#[tokio::test]
async fn unbuffered_resumable_reuses_token() -> Result {
    let (server, tokens) = resumable_server();
    start_resumable(&server)
        .await?
        .set_if_generation_match(0)
        .send_unbuffered()
        .await?;
    let tokens = tokens.lock().unwrap();
    let [Some(first), Some(second)] = tokens.as_slice() else {
        panic!("expected 2 captured tokens, got {tokens:?}");
    };
    assert_eq!(first, second);
    Ok(())
}

// `with_idempotency(false)` suppresses the token. Session creation is still
// retried, because resumable uploads are always idempotent.
#[tokio::test]
async fn buffered_resumable_idempotency_false_omits_token() -> Result {
    let (server, tokens) = resumable_server();
    start_resumable(&server)
        .await?
        .set_if_generation_match(0)
        .with_idempotency(false)
        .send_buffered()
        .await?;
    assert_eq!(*tokens.lock().unwrap(), vec![None, None]);
    Ok(())
}

async fn start_single_shot(server: &Server) -> anyhow::Result<WriteObject<BytesSource>> {
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .build()
        .await?;
    Ok(client
        .write_object("projects/_/buckets/test-bucket", "test-object", "hello")
        .with_resumable_upload_threshold(1024 * 1024_usize))
}

async fn start_resumable(server: &Server) -> anyhow::Result<WriteObject<BytesSource>> {
    let client = test_builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .build()
        .await?;
    Ok(client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .with_resumable_upload_threshold(0_usize))
}

/// Returns a server expecting `attempts` single-shot uploads, where the first
/// one fails with `503`, and the tokens sent on each attempt.
fn single_shot_server(attempts: usize) -> (Server, CapturedTokens) {
    let server = Server::run();
    let tokens = CapturedTokens::default();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
        .times(attempts)
        .respond_with(TokenCapture::json_body(
            tokens.clone(),
            response_body().to_string().into(),
        )),
    );
    (server, tokens)
}

/// Returns a server where creating the session fails once with `503`, and the
/// tokens sent on each attempt to create the session.
///
/// The data `PUT` must not carry a token.
fn resumable_server() -> (Server, CapturedTokens) {
    let server = Server::run();
    let session = server.url("/upload/session/test-only-001");
    let tokens = CapturedTokens::default();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .times(2)
        .respond_with(TokenCapture::resumable_session(
            tokens.clone(),
            session.to_string(),
        )),
    );
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", session.path().to_string()),
            request::headers(contains(("content-range", "bytes */0"))),
            not(request::headers(contains(key(IDEMPOTENCY_TOKEN_HEADER)))),
        ])
        .respond_with(
            status_code(200)
                .append_header("content-type", "application/json")
                .body(response_body().to_string()),
        ),
    );
    (server, tokens)
}

/// The tokens observed by a [TokenCapture], in request order.
///
/// An entry is `None` when the request carried no idempotency token.
type CapturedTokens = Arc<Mutex<Vec<Option<String>>>>;

enum Success {
    Session(String),
    Json(bytes::Bytes),
}

/// Fails the first request with `503` and succeeds afterwards, recording the
/// idempotency token seen on every attempt.
///
/// The stock `httptest` responders cannot inspect request headers.
struct TokenCapture {
    tokens: CapturedTokens,
    call_count: usize,
    success: Success,
}

impl TokenCapture {
    /// Responds like the "create resumable upload session" endpoint, returning
    /// `session_url` in the `location` header once the transient failure is
    /// past.
    fn resumable_session(tokens: CapturedTokens, session_url: String) -> Self {
        Self {
            tokens,
            call_count: 0,
            success: Success::Session(session_url),
        }
    }

    /// Responds with a JSON object payload once the transient failure is past.
    fn json_body(tokens: CapturedTokens, body: bytes::Bytes) -> Self {
        Self {
            tokens,
            call_count: 0,
            success: Success::Json(body),
        }
    }
}

impl httptest::responders::Responder for TokenCapture {
    fn respond<'a>(
        &mut self,
        req: &'a http::Request<bytes::Bytes>,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = http::Response<bytes::Bytes>> + std::marker::Send + 'a>,
    > {
        let token = req
            .headers()
            .get(IDEMPOTENCY_TOKEN_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        self.tokens.lock().unwrap().push(token);
        let count = self.call_count;
        self.call_count += 1;
        let res = if count == 0 {
            http::Response::builder()
                .status(503)
                .body(bytes::Bytes::from("try-again"))
                .unwrap()
        } else {
            match &self.success {
                Success::Session(url) => http::Response::builder()
                    .status(200)
                    .header("location", url)
                    .body(bytes::Bytes::new())
                    .unwrap(),
                Success::Json(body) => http::Response::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(body.clone())
                    .unwrap(),
            }
        };
        Box::pin(async move { res })
    }
}
