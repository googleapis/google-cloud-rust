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

use auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
use futures::TryStreamExt;
use google_cloud_storage::client::Storage;
use google_cloud_storage::streaming_source::{Seek, SizeHint, StreamingSource};
use http::{Extensions, HeaderMap};
use httptest::{Expectation, Server, matchers::*, responders::*};

type Result = anyhow::Result<()>;

#[derive(Debug)]
struct TestCredentialsProvider {
    token: String,
}

impl CredentialsProvider for TestCredentialsProvider {
    fn headers(
        &self,
        _extensions: Extensions,
    ) -> impl std::future::Future<
        Output = std::result::Result<CacheableResource<HeaderMap>, auth::errors::CredentialsError>,
    > + Send {
        let token = self.token.clone();
        async move {
            let mut headers = HeaderMap::new();
            headers.insert(
                http::header::AUTHORIZATION,
                format!("Bearer {}", token)
                    .parse()
                    .map_err(|e| auth::errors::CredentialsError::from_source(false, e))?,
            );
            Ok(CacheableResource::New {
                entity_tag: EntityTag::new(),
                data: headers,
            })
        }
    }
    fn universe_domain(&self) -> impl std::future::Future<Output = Option<String>> + Send {
        async { None }
    }
}

struct TestStreamingSource {
    data: Vec<u8>,
    pos: usize,
}

impl TestStreamingSource {
    fn new(data: Vec<u8>) -> Self {
        Self { data, pos: 0 }
    }
}

impl StreamingSource for TestStreamingSource {
    type Error = std::io::Error;
    fn next(
        &mut self,
    ) -> impl std::future::Future<Output = Option<std::result::Result<bytes::Bytes, std::io::Error>>>
    + Send {
        async move {
            if self.pos >= self.data.len() {
                None
            } else {
                let chunk = self.data[self.pos..].to_vec();
                self.pos += chunk.len();
                Some(Ok(bytes::Bytes::from(chunk)))
            }
        }
    }
    fn size_hint(
        &self,
    ) -> impl std::future::Future<Output = std::result::Result<SizeHint, std::io::Error>> + Send
    {
        async move { Ok(SizeHint::with_exact(self.data.len() as u64)) }
    }
}

impl Seek for TestStreamingSource {
    type Error = std::io::Error;
    fn seek(
        &mut self,
        offset: u64,
    ) -> impl std::future::Future<Output = std::result::Result<(), std::io::Error>> + Send {
        async move {
            self.pos = offset as usize;
            Ok(())
        }
    }
}

async fn create_test_client(server: &Server) -> anyhow::Result<Storage> {
    Ok(Storage::builder()
        .with_endpoint(format!("http://{}", server.addr()))
        .with_credentials(auth::credentials::Credentials::from(
            TestCredentialsProvider {
                token: "test-token".to_string(),
            },
        ))
        .build()
        .await?)
}

#[tokio::test]
async fn test_headers_and_user_agent() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::headers(contains(("authorization", "Bearer test-token"))),
            request::headers(contains(("host", server.addr().to_string()))),
            |req: &http::Request<_>| {
                req.headers()
                    .get("x-goog-api-client")
                    .map(|v| v.as_bytes().starts_with(b"gl-rust/"))
                    .unwrap_or(false)
            },
        ])
        .respond_with(
            status_code(200)
                .body("data")
                .append_header("x-goog-generation", "1"),
        ),
    );

    let client = create_test_client(&server).await?;

    let _ = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_simple_upload() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::headers(contains(("authorization", "Bearer test-token"))),
            |req: &http::Request<_>| {
                req.headers()
                    .get("content-type")
                    .map(|v| v.as_bytes().starts_with(b"multipart/related"))
                    .unwrap_or(false)
            },
            |req: &http::Request<bytes::Bytes>| {
                let s = String::from_utf8_lossy(req.body());
                s.contains("hello world")
            },
        ])
        .respond_with(
            status_code(200)
                .body(r#"{"name": "test-object", "bucket": "test-bucket"}"#)
                .append_header("x-goog-generation", "1"),
        ),
    );

    let client = create_test_client(&server).await?;
    let _ = client
        .write_object(
            "projects/_/buckets/test-bucket",
            "test-object",
            "hello world",
        )
        .send_buffered()
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_streaming_upload() -> Result {
    let server = Server::run();
    let body_size = 150 * 1024;
    let body_content = "a".repeat(body_size);
    let body_content_clone = body_content.clone();

    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::query(url_decoded(contains(("uploadType", "multipart")))),
            request::query(url_decoded(contains(("name", "test-object")))),
            request::headers(contains(("authorization", "Bearer test-token"))),
            request::headers(contains(("transfer-encoding", "chunked"))),
            |req: &http::Request<bytes::Bytes>| {
                req.headers()
                    .get("content-type")
                    .map(|v| v.as_bytes().starts_with(b"multipart/related"))
                    .unwrap_or(false)
            },
            move |req: &http::Request<bytes::Bytes>| {
                let s = String::from_utf8_lossy(req.body());
                s.contains(&body_content_clone)
            },
        ])
        .respond_with(
            status_code(200)
                .body(r#"{"name": "test-object", "bucket": "test-bucket"}"#)
                .append_header("x-goog-generation", "1"),
        ),
    );

    let client = create_test_client(&server).await?;

    let source = TestStreamingSource::new(body_content.into_bytes());

    let _ = client
        .write_object("projects/_/buckets/test-bucket", "test-object", source)
        .send_unbuffered()
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_simple_download() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::query(url_decoded(contains(("alt", "media")))),
            request::headers(contains(("authorization", "Bearer test-token"))),
            request::headers(contains(("accept-encoding", "gzip"))),
        ])
        .respond_with(
            status_code(200)
                .body("hello world")
                .append_header("x-goog-generation", "1"),
        ),
    );

    let client = create_test_client(&server).await?;
    let response = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let content = response.into_stream().try_collect::<Vec<_>>().await?;
    assert_eq!(content.concat(), b"hello world");

    Ok(())
}

#[tokio::test]
async fn test_streaming_download() -> Result {
    let server = Server::run();
    let body_size = 150 * 1024;
    let body_content = "a".repeat(body_size);

    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::headers(contains(("authorization", "Bearer test-token"))),
            request::query(url_decoded(contains(("alt", "media")))),
        ])
        .respond_with(
            status_code(200)
                .body(body_content.clone())
                .append_header("x-goog-generation", "1"),
        ),
    );

    let client = create_test_client(&server).await?;
    let mut response = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;

    let mut content = Vec::new();
    let mut chunks_count = 0;
    while let Some(chunk) = response.next().await.transpose()? {
        content.extend_from_slice(&chunk);
        chunks_count += 1;
    }
    assert_eq!(content.len(), body_size);
    assert!(
        chunks_count > 1,
        "Expected multiple chunks, got {}",
        chunks_count
    );

    Ok(())
}

#[tokio::test]
async fn test_resumable_upload() -> Result {
    let server = Server::run();
    let addr = server.addr();
    let location = format!("http://{}/upload/session-123", addr);

    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::headers(contains(("authorization", "Bearer test-token"))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .respond_with(
            status_code(200)
                .append_header("Location", &location)
                .append_header("x-goog-generation", "1"),
        ),
    );

    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", "/upload/session-123"),
            request::body("hello world"),
        ])
        .respond_with(
            status_code(200)
                .body(r#"{"name": "test-object", "bucket": "test-bucket"}"#)
                .append_header("x-goog-generation", "1"),
        ),
    );

    let client = create_test_client(&server).await?;
    let _ = client
        .write_object(
            "projects/_/buckets/test-bucket",
            "test-object",
            "hello world",
        )
        .with_resumable_upload_threshold(1_usize)
        .send_buffered()
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_resumable_upload_interrupted() -> Result {
    let server = Server::run();
    let addr = server.addr();
    let location = format!("http://{}/upload/session-123", addr);

    // 1. Start session
    server.expect(
        Expectation::matching(all_of![
            request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
            request::headers(contains(("authorization", "Bearer test-token"))),
            request::query(url_decoded(contains(("uploadType", "resumable")))),
        ])
        .respond_with(
            status_code(200)
                .append_header("Location", &location)
                .append_header("x-goog-generation", "1"),
        ),
    );

    // 2. First upload attempt fails with 500
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", "/upload/session-123"),
            request::body("hello world"),
        ])
        .times(1)
        .respond_with(status_code(500)),
    );

    // 3. Client queries status
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", "/upload/session-123"),
            request::headers(contains(("content-range", "bytes */*"))),
        ])
        .times(1)
        .respond_with(
            status_code(308).append_header("Range", "bytes=0-4"), // Persisted "hello" (5 bytes)
        ),
    );

    // 4. Client resumes sending remaining bytes " world"
    server.expect(
        Expectation::matching(all_of![
            request::method_path("PUT", "/upload/session-123"),
            request::body(" world"), // Remaining part
            request::headers(contains(("content-range", "bytes 5-10/11"))),
        ])
        .times(1)
        .respond_with(
            status_code(200)
                .body(r#"{"name": "test-object", "bucket": "test-bucket"}"#)
                .append_header("x-goog-generation", "1"),
        ),
    );

    let client = create_test_client(&server).await?;

    let source = TestStreamingSource::new(b"hello world".to_vec());

    let _ = client
        .write_object("projects/_/buckets/test-bucket", "test-object", source)
        .with_resumable_upload_threshold(1_usize)
        .send_unbuffered()
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_error_handling_404() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::headers(contains(("authorization", "Bearer test-token"))),
        ])
        .respond_with(
            status_code(404)
                .body("Not Found")
                .append_header("x-goog-generation", "1"),
        ),
    );

    let client = create_test_client(&server).await?;
    let err: google_cloud_storage::Error = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await
        .expect_err("expected 404 error");

    Ok(())
}

#[tokio::test]
async fn test_default_compression_behavior() -> Result {
    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            // Expect accept-encoding: gzip because automatic_decompression is false by default
            request::headers(contains(("authorization", "Bearer test-token"))),
            request::headers(contains(("accept-encoding", "gzip"))),
        ])
        .respond_with(
            status_code(200)
                .body("raw data")
                .append_header("x-goog-generation", "1"),
        ),
    );

    let client = create_test_client(&server).await?;

    let _ = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_no_automatic_decompression() -> Result {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    let server = Server::run();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(b"hello world")?;
    let compressed_data = encoder.finish()?;

    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
            request::headers(contains(("authorization", "Bearer test-token"))),
        ])
        .respond_with(
            status_code(200)
                .append_header("content-encoding", "gzip")
                .body(compressed_data.clone())
                .append_header("x-goog-generation", "1"),
        ),
    );

    let client = create_test_client(&server).await?;

    let response = client
        .read_object("projects/_/buckets/test-bucket", "test-object")
        .send()
        .await?;
    let content = response.into_stream().try_collect::<Vec<_>>().await?;

    // We expect the raw compressed data, NOT "hello world"
    assert_eq!(content.concat(), compressed_data);

    Ok(())
}
