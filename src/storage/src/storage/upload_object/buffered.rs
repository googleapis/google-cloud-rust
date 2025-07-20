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

impl<T> UploadObject<T>
where
    T: StreamingSource + Send + Sync + 'static,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    /// Upload an object from a streaming source without rewinds.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub async fn send(self) -> crate::Result<Object> {
        let hint = self
            .payload
            .lock()
            .await
            .size_hint()
            .await
            .map_err(Error::io)?
            .1;
        if hint.is_none_or(|max| max >= self.options.resumable_upload_threshold as u64) {
            let upload_url = self.start_resumable_upload().await?;
            // The buffer size must be a multiple of the upload quantum. The
            // upload is finalized on the first PUT request with a size that is
            // not such.
            let target_size = self
                .options
                .resumable_upload_buffer_size
                .div_ceil(RESUMABLE_UPLOAD_QUANTUM)
                * RESUMABLE_UPLOAD_QUANTUM;
            let target_size = target_size.max(RESUMABLE_UPLOAD_QUANTUM);
            return self.upload_by_chunks(&upload_url, target_size).await;
        }
        self.send_buffered_single_shot().await
    }

    async fn upload_by_chunks(&self, upload_url: &str, target_size: usize) -> Result<Object> {
        let mut remainder = None;
        let mut offset = 0_usize;
        loop {
            let NextChunk {
                chunk,
                size: chunk_size,
                remainder: r,
            } = self::next_chunk(&mut *self.payload.lock().await, remainder, target_size).await?;
            let full_size = if chunk_size < target_size {
                Some(offset + chunk_size)
            } else {
                None
            };
            let (builder, chunk_size) = self
                .partial_upload_request(upload_url, offset, chunk, chunk_size, full_size)
                .await?;
            let response = builder.send().await.map_err(Error::io)?;
            match self::partial_upload_handle_response(response, offset + chunk_size).await? {
                PartialUpload::Finalized(o) => {
                    return Ok(*o);
                }
                PartialUpload::Partial {
                    persisted_size,
                    chunk_remainder,
                } => {
                    offset = persisted_size;
                    // TODO(#2043) - handle partial uploads
                    assert_eq!(chunk_remainder, 0);
                    remainder = r;
                }
            }
        }
    }

    async fn partial_upload_request(
        &self,
        upload_url: &str,
        offset: usize,
        chunk: VecDeque<bytes::Bytes>,
        chunk_size: usize,
        full_size: Option<usize>,
    ) -> Result<(reqwest::RequestBuilder, usize)> {
        let range = match (chunk_size, full_size) {
            (0, Some(s)) => format!("bytes */{s}"),
            (0, None) => format!("bytes */{offset}"),
            (n, Some(s)) => format!("bytes {offset}-{}/{s}", offset + n - 1),
            (n, None) => format!("bytes {offset}-{}/*", offset + n - 1),
        };
        let builder = self
            .inner
            .client
            .request(reqwest::Method::PUT, upload_url)
            .header("content-type", "application/octet-stream")
            .header("Content-Range", range)
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );

        let builder = apply_customer_supplied_encryption_headers(builder, &self.params);
        let builder = self.inner.apply_auth_headers(builder).await?;
        let stream = unfold(Some(chunk), move |state| async move {
            if let Some(mut payload) = state {
                if let Some(next) = payload.pop_front() {
                    return Some((Ok::<bytes::Bytes, Error>(next), Some(payload)));
                }
            }
            None
        });
        Ok((builder.body(reqwest::Body::wrap_stream(stream)), chunk_size))
    }

    async fn send_buffered_single_shot(self) -> Result<Object> {
        let mut stream = self.payload.lock().await;
        let mut collected = Vec::new();
        while let Some(b) = stream.next().await.transpose().map_err(Error::io)? {
            collected.push(b);
        }
        let upload = UploadObject {
            payload: Arc::new(Mutex::new(InsertPayload::from(collected))),
            inner: self.inner,
            spec: self.spec,
            params: self.params,
            options: self.options,
        };
        upload.send_unbuffered().await
    }
}

async fn next_chunk<T>(
    payload: &mut InsertPayload<T>,
    remainder: Option<bytes::Bytes>,
    target_size: usize,
) -> Result<NextChunk>
where
    T: StreamingSource + Send + Sync,
{
    let mut partial = VecDeque::new();
    let mut size = 0;
    let mut process_buffer = |mut b: bytes::Bytes| match b.len() {
        n if size + n > target_size => {
            let remainder = b.split_off(target_size - size);
            size = target_size;
            partial.push_back(b);
            Some(Some(remainder))
        }
        n if size + n == target_size => {
            size = target_size;
            partial.push_back(b);
            Some(None)
        }
        n => {
            size += n;
            partial.push_back(b);
            None
        }
    };

    if let Some(b) = remainder {
        if let Some(p) = process_buffer(b) {
            return Ok(NextChunk {
                chunk: partial,
                size,
                remainder: p,
            });
        }
    }

    while let Some(b) = payload.next().await.transpose().map_err(Error::io)? {
        if let Some(p) = process_buffer(b) {
            return Ok(NextChunk {
                chunk: partial,
                size,
                remainder: p,
            });
        }
    }
    Ok(NextChunk {
        chunk: partial,
        size,
        remainder: None,
    })
}

async fn partial_upload_handle_response(
    response: reqwest::Response,
    expected_offset: usize,
) -> Result<PartialUpload> {
    if response.status() == self::RESUME_INCOMPLETE {
        return self::parse_range(response, expected_offset).await;
    }
    if !response.status().is_success() {
        return gaxi::http::to_http_error(response).await;
    }
    let response = response.json::<v1::Object>().await.map_err(Error::io)?;
    Ok(PartialUpload::Finalized(Box::new(Object::from(response))))
}

async fn parse_range(response: reqwest::Response, expected_offset: usize) -> Result<PartialUpload> {
    let Some(end) = self::parse_range_end(response.headers()) else {
        return gaxi::http::to_http_error(response).await;
    };
    // The `Range` header returns an inclusive range, i.e. bytes=0-999 means "1000 bytes".
    let (persisted_size, chunk_remainder) = match (expected_offset, end as usize) {
        (o, 0) => (0, o),
        (o, e) if o < e + 1 => panic!("more data persistent than sent {response:?}"),
        (o, e) => (e + 1, o - e - 1),
    };
    Ok(PartialUpload::Partial {
        persisted_size,
        chunk_remainder,
    })
}

#[derive(Debug, PartialEq)]
enum PartialUpload {
    Finalized(Box<Object>),
    Partial {
        persisted_size: usize,
        chunk_remainder: usize,
    },
}

/// The result of breaking the source data into a fixed sized chunk.
#[derive(Debug, PartialEq)]
struct NextChunk {
    /// The data for this chunk.
    chunk: VecDeque<bytes::Bytes>,
    /// The total number of bytes in `chunk`.
    size: usize,
    // Any data received from the source that did not fit in the chunk.
    remainder: Option<bytes::Bytes>,
}

// Resumable uploads chunks (except for the last chunk) *must* be sized to a
// multiple of 256 KiB.
const RESUMABLE_UPLOAD_QUANTUM: usize = 256 * 1024;

#[cfg(test)]
mod tests {
    use super::super::client::tests::{test_builder, test_inner_client};
    use super::upload_source::{BytesSource, IterSource, tests::UnknownSize};
    use super::*;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use serde_json::json;
    use test_case::test_case;

    type Result = anyhow::Result<()>;

    // We rely on the tests from `unbuffered.rs` for coverage of other
    // single-shot upload features. Here we just want to verify the right upload
    // type is selected depending on the with_resumable_upload_threshold()
    // option.
    #[tokio::test]
    async fn upload_object_buffered_single_shot() -> Result {
        let payload = serde_json::json!({
            "name": "test-object",
            "bucket": "test-bucket",
            "metadata": {
                "is-test-object": "true",
            }
        })
        .to_string();
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "multipart")))),
            ])
            .respond_with(
                status_code(200)
                    .append_header("content-type", "application/json")
                    .body(payload),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .with_resumable_upload_threshold(4 * RESUMABLE_UPLOAD_QUANTUM)
            .build()
            .await?;
        let response = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", "")
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
    async fn upload_object_buffered_resumable() -> Result {
        let payload = serde_json::json!({
            "name": "test-object",
            "bucket": "test-bucket",
            "metadata": {
                "is-test-object": "true",
            }
        })
        .to_string();
        let server = Server::run();
        let session = server.url("/upload/session/test-only-001");
        let path = session.path().to_string();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
            ])
            .respond_with(
                status_code(200)
                    .append_header("location", session.to_string())
                    .body(""),
            ),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path("PUT", path),
                request::headers(contains(("content-range", "bytes */0")))
            ])
            .respond_with(
                status_code(200)
                    .append_header("content-type", "application/json")
                    .body(payload),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .with_resumable_upload_threshold(0_usize)
            .build()
            .await?;
        let response = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", "")
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
    async fn upload_object_buffered_resumable_unknown_size() -> Result {
        let payload = serde_json::json!({
            "name": "test-object",
            "bucket": "test-bucket",
            "metadata": {
                "is-test-object": "true",
            }
        })
        .to_string();
        let server = Server::run();
        let session = server.url("/upload/session/test-only-001");
        let path = session.path().to_string();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
            ])
            .respond_with(
                status_code(200)
                    .append_header("location", session.to_string())
                    .body(""),
            ),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path("PUT", path),
                request::headers(contains(("content-range", "bytes */0")))
            ])
            .respond_with(
                status_code(200)
                    .append_header("content-type", "application/json")
                    .body(payload),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .with_resumable_upload_threshold(4 * RESUMABLE_UPLOAD_QUANTUM)
            .build()
            .await?;
        let source = UnknownSize::new(BytesSource::new(bytes::Bytes::from_static(b"")));
        let response = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", source)
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
    async fn upload_object_buffered_not_found() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
            ])
            .respond_with(status_code(404).body("NOT FOUND")),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let err = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", "")
            .with_resumable_upload_threshold(0_usize)
            .send()
            .await
            .expect_err("expected a not found error");
        assert_eq!(err.http_status_code(), Some(404), "{err:?}");

        Ok(())
    }

    #[test_case(0, RESUMABLE_UPLOAD_QUANTUM)]
    #[test_case(RESUMABLE_UPLOAD_QUANTUM / 2, RESUMABLE_UPLOAD_QUANTUM)]
    #[test_case(RESUMABLE_UPLOAD_QUANTUM, RESUMABLE_UPLOAD_QUANTUM)]
    #[test_case(3 * RESUMABLE_UPLOAD_QUANTUM + 1, 4 * RESUMABLE_UPLOAD_QUANTUM)]
    #[tokio::test]
    async fn upload_object_buffered_resumable_multiple_chunks(
        configured_buffer_size: usize,
        actual_buffer_size: usize,
    ) -> Result {
        assert!(
            actual_buffer_size % RESUMABLE_UPLOAD_QUANTUM == 0,
            "{actual_buffer_size}"
        );
        assert!(
            configured_buffer_size <= actual_buffer_size,
            "{configured_buffer_size} should be <= {actual_buffer_size}"
        );
        let payload = serde_json::json!({
            "name": "test-object",
            "bucket": "test-bucket",
            "metadata": {
                "is-test-object": "true",
            }
        })
        .to_string();
        let server = Server::run();
        let session = server.url("/upload/session/test-only-001");
        let path = session.path().to_string();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
            ])
            .respond_with(
                status_code(200)
                    .append_header("location", session.to_string())
                    .body(""),
            ),
        );

        let start0 = 0;
        let end0 = actual_buffer_size - 1;
        let start1 = actual_buffer_size;
        let end1 = 2 * actual_buffer_size - 1;
        let end2 = 2 * actual_buffer_size;
        server.expect(
            Expectation::matching(all_of![
                request::method_path("PUT", path.clone()),
                request::headers(contains((
                    "content-range",
                    format!("bytes {start0}-{end0}/*")
                )))
            ])
            .respond_with(status_code(308).append_header("range", format!("bytes=0-{end0}"))),
        );
        server.expect(
            Expectation::matching(all_of![
                request::method_path("PUT", path.clone()),
                request::headers(contains((
                    "content-range",
                    format!("bytes {start1}-{end1}/*")
                )))
            ])
            .respond_with(status_code(308).append_header("range", format!("bytes=0-{end1}"))),
        );
        server.expect(
            Expectation::matching(all_of![
                request::method_path("PUT", path.clone()),
                request::headers(contains(("content-range", format!("bytes */{end2}"))))
            ])
            .respond_with(
                status_code(200)
                    .append_header("content-type", "application/json")
                    .body(payload),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(auth::credentials::testing::test_credentials())
            .with_resumable_upload_threshold(0_usize)
            .with_resumable_upload_buffer_size(configured_buffer_size)
            .build()
            .await?;
        let contents = (0..4)
            .map(|i| vec![i as u8; actual_buffer_size / 2])
            .map(bytes::Bytes::from_owner)
            .collect::<Vec<_>>();
        let response = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", contents)
            .send()
            .await;
        let response = response?;
        assert_eq!(response.name, "test-object");
        assert_eq!(response.bucket, "projects/_/buckets/test-bucket");
        assert_eq!(
            response.metadata.get("is-test-object").map(String::as_str),
            Some("true")
        );

        Ok(())
    }

    #[test_case("projects/p", "projects%2Fp")]
    #[test_case("kebab-case", "kebab-case")]
    #[test_case("dot.name", "dot.name")]
    #[test_case("under_score", "under_score")]
    #[test_case("tilde~123", "tilde~123")]
    #[test_case("exclamation!point!", "exclamation%21point%21")]
    #[test_case("spaces   spaces", "spaces%20%20%20spaces")]
    #[test_case("preserve%percent%21", "preserve%percent%21")]
    #[test_case(
        "testall !#$&'()*+,/:;=?@[]",
        "testall%20%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D"
    )]
    #[tokio::test]
    async fn test_percent_encoding_object_name(name: &str, want: &str) -> Result {
        let inner = test_inner_client(test_builder());
        let request = UploadObject::new(inner, "projects/_/buckets/bucket", name, "hello")
            .start_resumable_upload_request()
            .await?
            .build()?;

        let got = request
            .url()
            .query_pairs()
            .find_map(|(key, val)| match key.to_string().as_str() {
                "name" => Some(val.to_string()),
                _ => None,
            })
            .unwrap();
        assert_eq!(got, want);
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
        let response = reqwest::Response::from(response);
        let url = super::handle_start_resumable_upload_response(response).await?;
        assert_eq!(url, "http://private.googleapis.com/test-only/session-123");
        Ok(())
    }

    fn new_line_string(i: i32, len: usize) -> String {
        format!("{i:022} {:width$}\n", "", width = len - 22 - 2)
    }

    fn new_line(i: i32, len: usize) -> bytes::Bytes {
        bytes::Bytes::from_owner(new_line_string(i, len))
    }

    #[tokio::test]
    async fn upload_by_chunks() -> Result {
        const LEN: usize = 32;

        let payload = serde_json::json!({
            "name": "test-object",
            "bucket": "test-bucket",
            "metadata": {
                "is-test-object": "true",
            }
        })
        .to_string();

        let chunk0 = new_line_string(0, LEN) + &new_line_string(1, LEN);
        let chunk1 = new_line_string(2, LEN) + &new_line_string(3, LEN);
        let chunk2 = new_line_string(4, LEN);

        let server = Server::run();
        let session = server.url("/upload/session/test-only-001");
        let path = session.path().to_string();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("PUT", path.clone()),
                request::headers(contains(("content-range", "bytes 0-63/*"))),
                request::body(chunk0.clone()),
            ])
            .respond_with(status_code(308).append_header("range", "bytes=0-63")),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path("PUT", path.clone()),
                request::headers(contains(("content-range", "bytes 64-127/*"))),
                request::body(chunk1.clone()),
            ])
            .respond_with(status_code(308).append_header("range", "bytes=0-127")),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path("PUT", path.clone()),
                request::headers(contains(("content-range", "bytes 128-159/160"))),
                request::body(chunk2.clone()),
            ])
            .respond_with(status_code(200).body(payload.clone())),
        );

        let stream = IterSource::new((0..5).map(|i| new_line(i, LEN)));
        let inner = test_inner_client(test_builder());
        let upload = UploadObject::new(inner, "projects/_/buckets/bucket", "object", stream);
        let response = upload
            .upload_by_chunks(session.to_string().as_str(), 2 * LEN)
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
    async fn partial_upload_request_empty() -> Result {
        use reqwest::header::HeaderValue;
        let inner = test_inner_client(test_builder());
        let upload = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "");

        let chunk = VecDeque::new();
        let (builder, size) = upload
            .partial_upload_request("http://localhost/chunk-0", 0_usize, chunk, 0_usize, None)
            .await?;
        assert_eq!(size, 0);
        let mut request = builder.build()?;

        assert_eq!(
            request.headers().get("content-type"),
            Some(&HeaderValue::from_static("application/octet-stream"))
        );
        assert_eq!(
            request.headers().get("content-range"),
            Some(&HeaderValue::from_static("bytes */0"))
        );
        assert!(
            request.headers().get("x-goog-api-client").is_some(),
            "{request:?}"
        );
        let body = request.body_mut().take().unwrap();
        let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
        assert!(&contents.is_empty(), "{contents:?}");
        Ok(())
    }

    #[tokio::test]
    async fn partial_upload_request_chunk0() -> Result {
        use reqwest::header::HeaderValue;
        const LEN: usize = 32;
        let inner = test_inner_client(test_builder());
        let upload = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "");

        let chunk = VecDeque::from_iter([new_line(0, LEN), new_line(1, LEN)]);
        let expected = chunk.iter().fold(Vec::new(), |mut a, b| {
            a.extend_from_slice(b);
            a
        });
        let (builder, size) = upload
            .partial_upload_request("http://localhost/chunk-0", 0_usize, chunk, 2 * LEN, None)
            .await?;
        assert_eq!(size, 2 * LEN);
        let mut request = builder.build()?;

        assert_eq!(
            request.headers().get("content-type"),
            Some(&HeaderValue::from_static("application/octet-stream"))
        );
        assert_eq!(
            request.headers().get("content-range"),
            Some(&HeaderValue::from_static("bytes 0-63/*"))
        );
        assert!(
            request.headers().get("x-goog-api-client").is_some(),
            "{request:?}"
        );
        let body = request.body_mut().take().unwrap();
        let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
        assert_eq!(&contents, &expected);
        Ok(())
    }

    #[tokio::test]
    async fn partial_upload_request_chunk1() -> Result {
        use reqwest::header::HeaderValue;
        const LEN: usize = 32;
        let inner = test_inner_client(test_builder());
        let upload = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "");

        let chunk = VecDeque::from_iter([new_line(2, LEN), new_line(3, LEN)]);
        let expected = chunk.iter().fold(Vec::new(), |mut a, b| {
            a.extend_from_slice(b);
            a
        });
        let (builder, size) = upload
            .partial_upload_request("http://localhost/chunk-1", 2 * LEN, chunk, 2 * LEN, None)
            .await?;
        assert_eq!(size, 2 * LEN);
        let mut request = builder.build()?;

        assert_eq!(
            request.headers().get("content-type"),
            Some(&HeaderValue::from_static("application/octet-stream"))
        );
        assert_eq!(
            request.headers().get("content-range"),
            Some(&HeaderValue::from_static("bytes 64-127/*"))
        );
        assert!(
            request.headers().get("x-goog-api-client").is_some(),
            "{request:?}"
        );
        let body = request.body_mut().take().unwrap();
        let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
        assert_eq!(&contents, &expected);
        Ok(())
    }

    #[tokio::test]
    async fn partial_upload_request_chunk_finalize() -> Result {
        use reqwest::header::HeaderValue;
        const LEN: usize = 32;
        let inner = test_inner_client(test_builder());
        let upload = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "");

        let chunk = VecDeque::from_iter([new_line(2, LEN)]);
        let expected = chunk.iter().fold(Vec::new(), |mut a, b| {
            a.extend_from_slice(b);
            a
        });
        let (builder, size) = upload
            .partial_upload_request(
                "http://localhost/chunk-finalize",
                4 * LEN,
                chunk,
                LEN,
                Some(5 * LEN),
            )
            .await?;
        assert_eq!(size, LEN);
        let mut request = builder.build()?;

        assert_eq!(
            request.headers().get("content-type"),
            Some(&HeaderValue::from_static("application/octet-stream"))
        );
        assert_eq!(
            request.headers().get("content-range"),
            Some(&HeaderValue::from_static("bytes 128-159/160"))
        );
        assert!(
            request.headers().get("x-goog-api-client").is_some(),
            "{request:?}"
        );
        let body = request.body_mut().take().unwrap();
        let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
        assert_eq!(&contents, &expected);
        Ok(())
    }

    #[tokio::test]
    async fn next_chunk_success() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..5).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, None, LEN * 2).await?;
        assert!(remainder.is_none(), "{remainder:?}");
        assert_eq!(chunk, vec![new_line(0, LEN), new_line(1, LEN)]);
        assert_eq!(size, 2 * LEN);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, remainder, LEN * 2).await?;
        assert!(remainder.is_none(), "{remainder:?}");
        assert_eq!(chunk, vec![new_line(2, LEN), new_line(3, LEN)]);
        assert_eq!(size, 2 * LEN);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, remainder, LEN * 2).await?;
        assert!(remainder.is_none(), "{remainder:?}");
        assert_eq!(chunk, vec![new_line(4, LEN)]);
        assert_eq!(size, LEN);

        Ok(())
    }

    #[tokio::test]
    async fn next_chunk_split() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..5).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, None, LEN * 2 + LEN / 2).await?;
        assert_eq!(remainder, Some(new_line(2, LEN).split_off(LEN / 2)));
        assert_eq!(
            chunk,
            vec![
                new_line(0, LEN),
                new_line(1, LEN),
                new_line(2, LEN).split_to(LEN / 2)
            ]
        );
        assert_eq!(size, 2 * LEN + LEN / 2);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, remainder, LEN * 2 + LEN / 2).await?;
        assert!(remainder.is_none());
        assert_eq!(
            chunk,
            vec![
                new_line(2, LEN).split_off(LEN / 2),
                new_line(3, LEN),
                new_line(4, LEN)
            ]
        );
        assert_eq!(size, 2 * LEN + LEN / 2);

        Ok(())
    }

    #[tokio::test]
    async fn next_chunk_split_large_remainder() -> Result {
        const LEN: usize = 32;
        let buffer = (0..3)
            .map(|i| new_line_string(i, LEN))
            .collect::<Vec<_>>()
            .join("");
        let stream = IterSource::new(vec![bytes::Bytes::from_owner(buffer), new_line(3, LEN)]);
        let mut payload = InsertPayload::from(stream);

        let remainder = None;
        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, remainder, LEN).await?;
        assert_eq!(chunk, vec![new_line(0, LEN)]);
        assert_eq!(size, LEN);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, remainder, LEN).await?;
        assert!(remainder.is_some());
        assert_eq!(chunk, vec![new_line(1, LEN)]);
        assert_eq!(size, LEN);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, remainder, LEN).await?;
        assert!(remainder.is_none());
        assert_eq!(chunk, vec![new_line(2, LEN)]);
        assert_eq!(size, LEN);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, remainder, LEN).await?;
        assert!(remainder.is_none());
        assert_eq!(chunk, vec![new_line(3, LEN)]);
        assert_eq!(size, LEN);

        Ok(())
    }

    #[tokio::test]
    async fn next_chunk_join_remainder() -> Result {
        const LEN: usize = 32;
        let buffer = (0..3)
            .map(|i| new_line_string(i, LEN))
            .collect::<Vec<_>>()
            .join("");
        let stream = IterSource::new(vec![
            bytes::Bytes::from_owner(buffer.clone()),
            new_line(3, LEN),
        ]);
        let mut payload = InsertPayload::from(stream);

        let remainder = None;
        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, remainder, 2 * LEN).await?;
        assert!(remainder.is_some());
        assert_eq!(
            chunk,
            vec![bytes::Bytes::from_owner(buffer.clone()).slice(0..(2 * LEN))]
        );
        assert_eq!(size, 2 * LEN);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, remainder, 2 * LEN).await?;
        assert!(remainder.is_none());
        assert_eq!(
            chunk,
            vec![
                bytes::Bytes::from_owner(buffer.clone()).slice((2 * LEN)..),
                new_line(3, LEN)
            ]
        );
        assert_eq!(size, 2 * LEN);

        Ok(())
    }

    #[tokio::test]
    async fn next_chunk_done() -> Result {
        const LEN: usize = 32;
        let stream = IterSource::new((0..2).map(|i| new_line(i, LEN)));
        let mut payload = InsertPayload::from(stream);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, None, LEN * 4).await?;
        assert!(remainder.is_none(), "{remainder:?}");
        assert_eq!(chunk, vec![new_line(0, LEN), new_line(1, LEN)]);
        assert_eq!(size, 2 * LEN);

        let NextChunk {
            chunk,
            size,
            remainder,
        } = super::next_chunk(&mut payload, remainder, LEN * 4).await?;
        assert!(remainder.is_none(), "{remainder:?}");
        assert!(chunk.is_empty(), "{chunk:?}");
        assert_eq!(size, 0);

        Ok(())
    }

    #[tokio::test]
    async fn partial_handle_response_incomplete() -> Result {
        let response = http::Response::builder()
            .header("range", "bytes=0-999")
            .status(RESUME_INCOMPLETE)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let partial = super::partial_upload_handle_response(response, 1000).await?;
        assert_eq!(
            partial,
            PartialUpload::Partial {
                persisted_size: 1000,
                chunk_remainder: 0
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn partial_handle_response_err() -> Result {
        let response = http::Response::builder()
            .status(reqwest::StatusCode::NOT_FOUND)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err = super::partial_upload_handle_response(response, 1000)
            .await
            .expect_err("NOT_FOUND should fail");
        assert_eq!(err.http_status_code(), Some(404), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    async fn partial_handle_response_finalized() -> Result {
        let response = http::Response::builder()
            .status(reqwest::StatusCode::OK)
            .body(
                json!({"bucket": "test-bucket", "name": "test-object", "size": "1000"}).to_string(),
            )?;
        let response = reqwest::Response::from(response);
        let partial = super::partial_upload_handle_response(response, 1000).await?;
        assert_eq!(
            partial,
            PartialUpload::Finalized(Box::new(
                Object::new()
                    .set_name("test-object")
                    .set_bucket("projects/_/buckets/test-bucket")
                    .set_finalize_time(wkt::Timestamp::default())
                    .set_create_time(wkt::Timestamp::default())
                    .set_update_time(wkt::Timestamp::default())
                    .set_update_storage_class_time(wkt::Timestamp::default())
                    .set_size(1000_i64)
            ))
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_range_success() -> Result {
        let response = http::Response::builder()
            .header("range", "bytes=0-999")
            .status(RESUME_INCOMPLETE)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let partial = super::parse_range(response, 1000).await?;
        assert_eq!(
            partial,
            PartialUpload::Partial {
                persisted_size: 1000,
                chunk_remainder: 0
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_range_partial() -> Result {
        let response = http::Response::builder()
            .header("range", "bytes=0-999")
            .status(RESUME_INCOMPLETE)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let partial = super::parse_range(response, 1234).await?;
        assert_eq!(
            partial,
            PartialUpload::Partial {
                persisted_size: 1000,
                chunk_remainder: 234
            }
        );
        Ok(())
    }

    #[tokio::test]
    #[should_panic]
    async fn parse_range_bad_end() {
        let response = http::Response::builder()
            .header("range", "bytes=0-999")
            .status(RESUME_INCOMPLETE)
            .body(Vec::new())
            .unwrap();
        let response = reqwest::Response::from(response);
        let _ = super::parse_range(response, 500).await;
    }

    #[tokio::test]
    async fn parse_range_missing_range() -> Result {
        let response = http::Response::builder()
            .status(RESUME_INCOMPLETE)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let partial = super::parse_range(response, 1234).await?;
        assert_eq!(
            partial,
            PartialUpload::Partial {
                persisted_size: 0,
                chunk_remainder: 1234
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn parse_range_invalid_range() -> Result {
        let response = http::Response::builder()
            .header("range", "bytes=100-999")
            .status(RESUME_INCOMPLETE)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err = super::parse_range(response, 1234)
            .await
            .expect_err("invalid range should create an error");
        assert!(err.http_status_code().is_some(), "{err:?}");
        Ok(())
    }
}
