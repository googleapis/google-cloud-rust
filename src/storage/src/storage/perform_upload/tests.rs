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

use super::*;
use crate::builder::storage::WriteObject;
use crate::model_ext::{KeyAes256, tests::create_key_helper};
use crate::storage::client::tests::{test_builder, test_inner_client};
use crate::streaming_source::Payload;
use google_cloud_auth::credentials::testing::error_credentials;
use google_cloud_gax::options::RequestOptions;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use test_case::test_case;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Result = anyhow::Result<()>;

mod checksums;
mod idempotency;
mod preconditions;

fn response_body() -> Value {
    json!({
        "name": "test-object",
        "bucket": "test-bucket",
        "metadata": {
            "is-test-object": "true",
        }
    })
}

pub(crate) fn perform_upload<T>(
    inner: Arc<StorageInner>,
    builder: WriteObject<T>,
) -> PerformUpload<Payload<T>> {
    PerformUpload::new(
        builder.payload,
        inner,
        builder.request.spec,
        builder.request.params,
        builder.options,
    )
}

#[tokio::test]
async fn start_resumable_upload() -> Result {
    let inner = test_inner_client(test_builder()).await;
    let options = inner.options.clone();
    let stub = crate::storage::transport::Storage::new_test(inner.clone());
    let builder = WriteObject::new(
        stub,
        "projects/_/buckets/bucket",
        "object",
        "hello",
        options,
    );
    let mut request = perform_upload(inner, builder)
        .start_resumable_upload_request()
        .await?
        .build_for_tests()
        .await?;

    assert_eq!(request.method(), Method::POST);
    assert_eq!(
        request.url().as_str(),
        "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=resumable&name=object"
    );
    let body = request.body_mut().take().unwrap();
    let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
    let json = serde_json::from_slice::<Value>(&contents)?;
    assert_eq!(json, json!({}));
    Ok(())
}

#[tokio::test]
async fn start_resumable_upload_headers() -> Result {
    // Make a 32-byte key.
    let (key, key_base64, _, key_sha256_base64) = create_key_helper();

    let inner = test_inner_client(test_builder()).await;
    let options = inner.options.clone();
    let stub = crate::storage::transport::Storage::new_test(inner.clone());
    let builder = WriteObject::new(
        stub,
        "projects/_/buckets/bucket",
        "object",
        "hello",
        options,
    )
    .set_key(KeyAes256::new(&key)?);
    let request = perform_upload(inner, builder)
        .start_resumable_upload_request()
        .await?
        .build_for_tests()
        .await?;

    assert_eq!(request.method(), Method::POST);
    assert_eq!(
        request.url().as_str(),
        "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=resumable&name=object"
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
async fn start_resumable_upload_bad_bucket() -> Result {
    let inner = test_inner_client(test_builder()).await;
    let options = inner.options.clone();
    let stub = crate::storage::transport::Storage::new_test(inner.clone());
    let builder = WriteObject::new(stub, "malformed", "object", "hello", options);
    let _ = perform_upload(inner, builder)
        .start_resumable_upload_request()
        .await
        .expect_err("malformed bucket string should error");
    Ok(())
}

#[tokio::test]
async fn start_resumable_upload_metadata_in_request() -> Result {
    use crate::model::ObjectAccessControl;
    let inner = test_inner_client(test_builder()).await;
    let options = inner.options.clone();
    let stub = crate::storage::transport::Storage::new_test(inner.clone());
    let builder = WriteObject::new(stub, "projects/_/buckets/bucket", "object", "", options)
        .set_if_generation_match(10)
        .set_if_generation_not_match(20)
        .set_if_metageneration_match(30)
        .set_if_metageneration_not_match(40)
        .set_predefined_acl("private")
        .set_acl([ObjectAccessControl::new()
            .set_entity("allAuthenticatedUsers")
            .set_role("READER")])
        .set_cache_control("public; max-age=7200")
        .set_content_disposition("inline")
        .set_content_encoding("gzip")
        .set_content_language("en")
        .set_content_type("text/plain")
        .set_custom_time(wkt::Timestamp::try_from("2025-07-07T18:11:00Z")?)
        .set_event_based_hold(true)
        .set_metadata([("k0", "v0"), ("k1", "v1")])
        .set_retention(
            crate::model::object::Retention::new()
                .set_mode(crate::model::object::retention::Mode::Locked)
                .set_retain_until_time(wkt::Timestamp::try_from("2035-07-07T18:14:00Z")?),
        )
        .set_storage_class("ARCHIVE")
        .set_temporary_hold(true)
        .set_kms_key("test-key")
        .with_known_crc32c(crc32c::crc32c(b""))
        .with_known_md5_hash(md5::compute(b"").0);
    let mut request = perform_upload(inner, builder)
        .start_resumable_upload_request()
        .await?
        .build_for_tests()
        .await?;

    assert_eq!(request.method(), Method::POST);
    let want_pairs: BTreeMap<String, String> = [
        ("uploadType", "resumable"),
        ("name", "object"),
        ("ifGenerationMatch", "10"),
        ("ifGenerationNotMatch", "20"),
        ("ifMetagenerationMatch", "30"),
        ("ifMetagenerationNotMatch", "40"),
        ("kmsKeyName", "test-key"),
        ("predefinedAcl", "private"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let query_pairs: BTreeMap<String, String> = request
        .url()
        .query_pairs()
        .map(|param| (param.0.to_string(), param.1.to_string()))
        .collect();
    assert_eq!(query_pairs, want_pairs);

    let body = request.body_mut().take().unwrap();
    let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
    let json = serde_json::from_slice::<Value>(&contents)?;
    assert_eq!(
        json,
        json!({
            "acl": [{"entity": "allAuthenticatedUsers", "role": "READER"}],
            "cacheControl": "public; max-age=7200",
            "contentDisposition": "inline",
            "contentEncoding": "gzip",
            "contentLanguage": "en",
            "contentType": "text/plain",
            "crc32c": "AAAAAA==",
            "customTime": "2025-07-07T18:11:00Z",
            "eventBasedHold": true,
            "md5Hash": "1B2M2Y8AsgTpgAmY7PhCfg==",
            "metadata": {"k0": "v0", "k1": "v1"},
            "retention": {"mode": "LOCKED", "retainUntilTime": "2035-07-07T18:14:00Z"},
            "storageClass": "ARCHIVE",
            "temporaryHold": true,
        })
    );
    Ok(())
}

#[tokio::test]
async fn start_resumable_upload_credentials() -> Result {
    let inner = test_inner_client(test_builder().with_credentials(error_credentials(false))).await;
    let options = inner.options.clone();
    let stub = crate::storage::transport::Storage::new_test(inner.clone());
    let builder = WriteObject::new(
        stub,
        "projects/_/buckets/bucket",
        "object",
        "hello",
        options,
    );
    let _ = perform_upload(inner, builder)
        .start_resumable_upload_request()
        .await?
        .build_for_tests()
        .await
        .inspect_err(|e| assert!(e.is_authentication()))
        .expect_err("invalid credentials should err");
    Ok(())
}

#[tokio::test]
async fn resumable_upload_attempts_preserve_auth_error() -> Result {
    // Arrange
    let inner = test_inner_client(test_builder().with_credentials(error_credentials(false))).await;
    let options = inner.options.clone();
    let stub = crate::storage::transport::Storage::new_test(inner.clone());
    let builder = WriteObject::new(
        stub,
        "projects/_/buckets/bucket",
        "object",
        "hello",
        options,
    );
    let upload = perform_upload(inner, builder);

    // Act & Assert
    let err = upload
        .start_resumable_upload_attempt(0, &RequestOptions::default())
        .await
        .expect_err("invalid credentials should fail start_resumable_upload_attempt");
    assert!(err.is_authentication(), "{err:?}");

    let err = upload
        .query_resumable_upload_attempt("http://private.googleapis.com/upload/session", 0)
        .await
        .expect_err("invalid credentials should fail query_resumable_upload_attempt");
    assert!(err.is_authentication(), "{err:?}");
    Ok(())
}

#[tokio::test]
async fn resumable_upload_attempts_preserve_timeout_error() -> Result {
    // Arrange
    // Accept both connections and hold them open before sending headers.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let _server = tokio::spawn(async move {
        let mut sockets = Vec::new();
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.expect("accept succeeds");
            let mut req = [0_u8; 1024];
            let _ = socket.read(&mut req).await.expect("read succeeds");
            sockets.push(socket);
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let inner = test_inner_client(
        test_builder()
            .with_endpoint(&endpoint)
            .with_read_timeout(Duration::from_millis(100)),
    )
    .await;
    let options = inner.options.clone();
    let stub = crate::storage::transport::Storage::new_test(inner.clone());
    let builder = WriteObject::new(
        stub,
        "projects/_/buckets/bucket",
        "object",
        "hello",
        options,
    );
    let upload = perform_upload(inner, builder);

    // Act & Assert
    let err = upload
        .start_resumable_upload_attempt(0, &RequestOptions::default())
        .await
        .expect_err("stalled session start should time out");
    assert!(err.is_timeout(), "{err:?}");

    let err = upload
        .query_resumable_upload_attempt(&format!("{endpoint}/upload/session"), 0)
        .await
        .expect_err("stalled session query should time out");
    assert!(err.is_timeout(), "{err:?}");
    Ok(())
}

#[tokio::test]
async fn handle_start_resumable_upload_response() -> Result {
    let response = http::Response::builder()
        .header(
            "Location",
            "http://private.googleapis.com/test-only/session-123",
        )
        .body(Vec::new())?;
    let response = Response::from(response);
    let url = super::handle_start_resumable_upload_response(response).await?;
    assert_eq!(url, "http://private.googleapis.com/test-only/session-123");
    Ok(())
}

#[test_case(None, Some(0))]
#[test_case(Some("bytes=0-12345"), Some(12345))]
#[test_case(Some("bytes=0-1"), Some(1))]
#[test_case(Some("bytes=0-0"), Some(0))]
#[test_case(Some("bytes=1-12345"), None)]
#[test_case(Some(""), None)]
fn range_end(input: Option<&str>, want: Option<u64>) {
    use gaxi::http::reqwest::{HeaderMap, HeaderName, HeaderValue};
    let headers = HeaderMap::from_iter(input.into_iter().map(|s| {
        (
            HeaderName::from_static("range"),
            HeaderValue::from_str(s).unwrap(),
        )
    }));
    assert_eq!(super::parse_range_end(&headers), want, "{headers:?}");
}

#[test]
fn validate_status_code() {
    assert_eq!(RESUME_INCOMPLETE, 308);
}

#[tokio::test]
async fn query_resumable_upload_partial() -> Result {
    let response = http::Response::builder()
        .header("range", "bytes=0-99")
        .status(RESUME_INCOMPLETE)
        .body(Vec::new())?;
    let response = Response::from(response);
    let status = super::query_resumable_upload_handle_response(response).await?;
    assert_eq!(status, ResumableUploadStatus::Partial(100_u64));
    Ok(())
}

#[tokio::test]
async fn query_resumable_upload_finalized() -> Result {
    let response = http::Response::builder()
        .status(200)
        .body(response_body().to_string())?;
    let response = Response::from(response);
    let status = super::query_resumable_upload_handle_response(response).await?;
    assert!(
        matches!(status, ResumableUploadStatus::Finalized(_)),
        "{status:?}"
    );
    Ok(())
}

#[tokio::test]
async fn query_resumable_upload_http_error() -> Result {
    let response = http::Response::builder().status(429).body(Vec::new())?;
    let response = Response::from(response);
    let err = super::query_resumable_upload_handle_response(response)
        .await
        .expect_err("HTTP error should return error");
    assert_eq!(err.http_status_code(), Some(429), "{err:?}");
    Ok(())
}

#[tokio::test]
async fn query_resumable_upload_finalized_deser() -> Result {
    let response = http::Response::builder()
        .status(200)
        .body("a string is not a valid object".to_string())?;
    let response = Response::from(response);
    let err = super::query_resumable_upload_handle_response(response)
        .await
        .expect_err("bad response should return an error");
    assert!(err.is_deserialization(), "{err:?}");
    Ok(())
}

#[tokio::test]
async fn handle_object_response_read_timeout() -> Result {
    // Arrange
    // Send 200 OK headers and an incomplete JSON body, then hold the socket open.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let _server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept succeeds");
        let mut req = [0_u8; 1024];
        let _ = socket.read(&mut req).await.expect("read succeeds");
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n{",
            )
            .await
            .expect("write succeeds");
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let inner = test_inner_client(
        test_builder()
            .with_endpoint(endpoint)
            .with_read_timeout(Duration::from_millis(100)),
    )
    .await;

    // Act
    let response = inner
        .client
        .http_builder(Method::GET, "/")
        .send(RequestOptions::default(), AttemptInfo::new(0))
        .await?;
    let err = super::handle_object_response(response)
        .await
        .expect_err("stalled JSON body should time out");

    // Assert
    assert!(err.is_timeout(), "{err:?}");
    Ok(())
}

#[tokio::test]
async fn handle_object_response_incomplete_body() -> Result {
    // Arrange
    // Close the socket after 1 byte of a 100-byte body.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let _server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept succeeds");
        // Read the whole request first so closing the socket sends a clean EOF
        // instead of a connection reset.
        let mut req = Vec::new();
        let mut buf = [0_u8; 256];
        while !req.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = socket.read(&mut buf).await.expect("read succeeds");
            if n == 0 {
                break;
            }
            req.extend_from_slice(&buf[..n]);
        }
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n{",
            )
            .await
            .expect("write succeeds");
        socket.shutdown().await.expect("shutdown succeeds");
    });

    let inner = test_inner_client(test_builder().with_endpoint(endpoint)).await;

    // Act
    let response = inner
        .client
        .http_builder(Method::GET, "/")
        .send(RequestOptions::default(), AttemptInfo::new(0))
        .await?;
    let err = super::handle_object_response(response)
        .await
        .expect_err("truncated HTTP body should error");

    // Assert
    assert!(err.is_io(), "{err:?}");
    Ok(())
}

#[tokio::test]
async fn parse_range() -> Result {
    let response = http::Response::builder()
        .header("range", "bytes=0-99")
        .status(RESUME_INCOMPLETE)
        .body(Vec::new())?;
    let response = Response::from(response);
    let range = super::parse_range(response).await?;
    assert_eq!(range, ResumableUploadStatus::Partial(100_u64));
    Ok(())
}

#[tokio::test]
async fn parse_range_missing() -> Result {
    let response = http::Response::builder()
        .status(RESUME_INCOMPLETE)
        .body(Vec::new())?;
    let response = Response::from(response);
    let range = super::parse_range(response).await?;
    assert_eq!(range, ResumableUploadStatus::Partial(0));
    Ok(())
}

#[tokio::test]
async fn parse_range_invalid_range() -> Result {
    let response = http::Response::builder()
        .header("range", "bytes=100-999")
        .status(RESUME_INCOMPLETE)
        .body(Vec::new())?;
    let response = Response::from(response);
    let err = super::parse_range(response)
        .await
        .expect_err("invalid range should create an error");
    assert_eq!(err.http_status_code(), Some(308), "{err:?}");
    Ok(())
}
