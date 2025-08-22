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

//! Unit tests for single shot uploads and unbuffered uploads.

use super::v1;
use crate::builder::storage::WriteObject;
use crate::model::Object;
use crate::model_ext::{KeyAes256, tests::create_key_helper};
use crate::storage::client::{
    Storage,
    tests::{test_builder, test_inner_client},
};
use crate::streaming_source::IterSource;
use crate::streaming_source::SizeHint;
use gax::retry_policy::RetryPolicyExt;
use http_body_util::BodyExt;
use httptest::{Expectation, Server, matchers::*, responders::*};
use serde_json::{Value, json};

type Result = anyhow::Result<()>;

#[tokio::test]
async fn http_error() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
        .respond_with(status_code(404).body("NOT FOUND")),
    );

    let client = Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let err = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .send_unbuffered()
        .await
        .expect_err("expected a not found error");
    assert_eq!(err.http_status_code(), Some(404));

    Ok(())
}

#[tokio::test]
async fn http_308_error() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
        .respond_with(status_code(308).body("never happens")),
    );

    let client = Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let err = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .send_unbuffered()
        .await
        .expect_err("expected a not found error");
    assert_eq!(err.http_status_code(), Some(308));

    Ok(())
}

#[tokio::test]
async fn deserialization() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
        .respond_with(status_code(200).body("bad format")),
    );

    let client = Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let err = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
        .send_unbuffered()
        .await
        .expect_err("expected a deserialization error");
    assert!(err.is_deserialization(), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn source_error() -> Result {
    let server = Server::run();

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
        .once()
        .returning(|| Ok(SizeHint::with_exact(1024)));
    let err = client
        .write_object("projects/_/buckets/test-bucket", "test-object", source)
        .send_unbuffered()
        .await
        .expect_err("expected a serialization error");
    assert!(err.is_serialization(), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn seek_error() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
        .times(0)
        .respond_with(status_code(200).body("bad format")),
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
        .returning(|| Ok(SizeHint::with_exact(1024_u64)));
    let err = client
        .write_object("projects/_/buckets/test-bucket", "test-object", source)
        .send_unbuffered()
        .await
        .expect_err("expected a serialization error");
    assert!(err.is_serialization(), "{err:?}");

    Ok(())
}

async fn parse_multipart_body(
    mut request: reqwest::Request,
) -> anyhow::Result<(bytes::Bytes, bytes::Bytes)> {
    let boundary = request
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("multipart/related; boundary="))
        .expect("request should include content-type")
        .to_string();
    let stream = request
        .body_mut()
        .take()
        .expect("request should have a body")
        .into_data_stream();
    let mut multipart = multer::Multipart::new(stream, boundary);
    let Some(m) = multipart.next_field().await? else {
        return Err(anyhow::Error::msg("missing metadata field"));
    };
    let metadata = m.bytes().await?;

    let Some(p) = multipart.next_field().await? else {
        return Err(anyhow::Error::msg("missing payload field"));
    };
    let payload = p.bytes().await?;

    assert!(
        multipart.next_field().await?.is_none(),
        "unexpected extra fields"
    );

    Ok((metadata, payload))
}

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
async fn upload_object_bytes() -> Result {
    const PAYLOAD: &str = "hello";
    let inner = test_inner_client(test_builder());
    let request = WriteObject::new(inner, "projects/_/buckets/bucket", "object", PAYLOAD)
        .build()
        .single_shot_builder(SizeHint::with_exact(PAYLOAD.len() as u64))
        .await?
        .build()?;

    assert_eq!(request.method(), reqwest::Method::POST);
    assert_eq!(
        request.url().as_str(),
        "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=multipart&name=object"
    );
    let (_metadata, contents) = parse_multipart_body(request).await?;
    assert_eq!(contents, "hello");
    Ok(())
}

#[tokio::test]
async fn upload_object_metadata() -> Result {
    let inner = test_inner_client(test_builder());
    let request = WriteObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
        .set_metadata([("k0", "v0"), ("k1", "v1")])
        .build()
        .single_shot_builder(SizeHint::new())
        .await?
        .build()?;

    assert_eq!(request.method(), reqwest::Method::POST);
    assert_eq!(
        request.url().as_str(),
        "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=multipart&name=object"
    );
    let (metadata, contents) = parse_multipart_body(request).await?;
    assert_eq!(contents, "hello");
    let object = serde_json::from_slice::<Value>(&metadata)?;
    assert_eq!(object, json!({"metadata": {"k0": "v0", "k1": "v1"}}));
    Ok(())
}

#[tokio::test]
async fn upload_object_stream() -> Result {
    let stream = IterSource::new(
        [
            "the ", "quick ", "brown ", "fox ", "jumps ", "over ", "the ", "lazy ", "dog",
        ]
        .map(|x| bytes::Bytes::from_static(x.as_bytes())),
    );
    let inner = test_inner_client(test_builder());
    let request = WriteObject::new(inner, "projects/_/buckets/bucket", "object", stream)
        .build()
        .single_shot_builder(SizeHint::new())
        .await?
        .build()?;

    assert_eq!(request.method(), reqwest::Method::POST);
    assert_eq!(
        request.url().as_str(),
        "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=multipart&name=object"
    );
    let (_metadata, contents) = parse_multipart_body(request).await?;
    assert_eq!(contents, "the quick brown fox jumps over the lazy dog");
    Ok(())
}

#[tokio::test]
async fn upload_object_error_credentials() -> Result {
    let inner = test_inner_client(
        test_builder().with_credentials(auth::credentials::testing::error_credentials(false)),
    );
    let _ = WriteObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
        .build()
        .single_shot_builder(SizeHint::new())
        .await
        .inspect_err(|e| assert!(e.is_authentication()))
        .expect_err("invalid credentials should err");
    Ok(())
}

#[tokio::test]
async fn upload_object_bad_bucket() -> Result {
    let inner = test_inner_client(test_builder());
    WriteObject::new(inner, "malformed", "object", "hello")
        .build()
        .single_shot_builder(SizeHint::new())
        .await
        .expect_err("malformed bucket string should error");
    Ok(())
}

#[tokio::test]
async fn upload_object_headers() -> Result {
    // Make a 32-byte key.
    let (key, key_base64, _, key_sha256_base64) = create_key_helper();

    let inner = test_inner_client(test_builder());
    let request = WriteObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
        .set_key(KeyAes256::new(&key)?)
        .build()
        .single_shot_builder(SizeHint::new())
        .await?
        .build()?;

    assert_eq!(request.method(), reqwest::Method::POST);
    assert_eq!(
        request.url().as_str(),
        "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=multipart&name=object"
    );

    let want = vec![
        ("x-goog-encryption-algorithm", "AES256".to_string()),
        ("x-goog-encryption-key", key_base64),
        ("x-goog-encryption-key-sha256", key_sha256_base64),
    ];

    for (name, value) in want {
        assert_eq!(
            request.headers().get(name).unwrap().as_bytes(),
            bytes::Bytes::from(value)
        );
    }
    Ok(())
}

#[tokio::test]
async fn send_unbuffered() -> Result {
    let payload = response_body().to_string();
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
        .times(1)
        .respond_with(
            status_code(200)
                .append_header("content-type", "application/json")
                .body(payload),
        ),
    );

    let client = Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::testing::test_credentials())
        .build()
        .await?;
    let response = client
        .write_object("projects/_/buckets/test-bucket", "test-object", "")
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
async fn retry_transient_not_idempotent() -> Result {
    let server = Server::run();
    let matching = || {
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/bucket/o"),
            request::query(url_decoded(contains(("name", "object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
    };
    server.expect(
        matching()
            .times(1)
            .respond_with(cycle![status_code(503).body("try-again"),]),
    );

    let inner =
        test_inner_client(test_builder().with_endpoint(format!("http://{}", server.addr())));
    let err = WriteObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
        .send_unbuffered()
        .await
        .expect_err("expected error as request is not idempotent");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn retry_transient_override_idempotency() -> Result {
    let server = Server::run();
    let matching = || {
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/bucket/o"),
            request::query(url_decoded(contains(("name", "object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
    };
    server.expect(matching().times(3).respond_with(cycle![
        status_code(429).body("try-again"),
        status_code(429).body("try-again"),
        json_encoded(response_body()).append_header("content-type", "application/json"),
    ]));

    let inner =
        test_inner_client(test_builder().with_endpoint(format!("http://{}", server.addr())));
    let got = WriteObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
        .with_idempotency(true)
        .send_unbuffered()
        .await?;
    let want = Object::from(serde_json::from_value::<v1::Object>(response_body())?);
    assert_eq!(got, want);

    Ok(())
}

#[tokio::test]
async fn retry_transient_failures_then_success() -> Result {
    let server = Server::run();
    let matching = || {
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
    };
    server.expect(matching().times(3).respond_with(cycle![
        status_code(503).body("try-again"),
        status_code(503).body("try-again"),
        json_encoded(response_body()).append_header("content-type", "application/json"),
    ]));

    let inner =
        test_inner_client(test_builder().with_endpoint(format!("http://{}", server.addr())));
    let got = WriteObject::new(
        inner,
        "projects/_/buckets/test-bucket",
        "test-object",
        "hello",
    )
    .set_if_generation_match(0)
    .send_unbuffered()
    .await?;
    let want = Object::from(serde_json::from_value::<v1::Object>(response_body())?);
    assert_eq!(got, want);

    Ok(())
}

#[tokio::test]
async fn retry_transient_failures_then_permanent() -> Result {
    let server = Server::run();
    let matching = || {
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
    };
    server.expect(matching().times(3).respond_with(cycle![
        status_code(503).body("try-again"),
        status_code(503).body("try-again"),
        status_code(403).body("uh-oh"),
    ]));

    let inner =
        test_inner_client(test_builder().with_endpoint(format!("http://{}", server.addr())));
    let err = WriteObject::new(
        inner,
        "projects/_/buckets/test-bucket",
        "test-object",
        "hello",
    )
    .set_if_generation_match(0)
    .send_unbuffered()
    .await
    .expect_err("expected permanent error");
    assert_eq!(err.http_status_code(), Some(403), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn retry_transient_failures_exhausted() -> Result {
    let server = Server::run();
    let matching = || {
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
        ])
    };
    server.expect(
        matching()
            .times(3)
            .respond_with(status_code(503).body("try-again")),
    );

    let inner =
        test_inner_client(test_builder().with_endpoint(format!("http://{}", server.addr())));
    let err = WriteObject::new(
        inner,
        "projects/_/buckets/test-bucket",
        "test-object",
        "hello",
    )
    .set_if_generation_match(0)
    .with_retry_policy(crate::retry_policy::RetryableErrors.with_attempt_limit(3))
    .send_unbuffered()
    .await
    .expect_err("expected permanent error");
    assert_eq!(err.http_status_code(), Some(503), "{err:?}");

    Ok(())
}
