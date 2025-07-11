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

use serde_with::DeserializeAs;

use super::*;

/// The request builder for [Storage::read_object][crate::client::Storage::read_object] calls.
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::client::Storage;
/// use google_cloud_storage::client::ReadObject;
/// # let client = Storage::builder()
/// #   .with_endpoint("https://storage.googleapis.com")
/// #    .build().await?;
/// let builder: ReadObject = client.read_object("projects/_/buckets/my-bucket", "my-object");
/// let contents = builder.send().await?.all_bytes().await?;
/// println!("object contents={contents:?}");
/// # Ok::<(), anyhow::Error>(()) });
/// ```
pub struct ReadObject {
    inner: std::sync::Arc<StorageInner>,
    request: crate::model::ReadObjectRequest,
}

impl ReadObject {
    pub(crate) fn new<B, O>(inner: std::sync::Arc<StorageInner>, bucket: B, object: O) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        ReadObject {
            inner,
            request: crate::model::ReadObjectRequest::new()
                .set_bucket(bucket)
                .set_object(object),
        }
    }

    /// If present, selects a specific revision of this object (as
    /// opposed to the latest version, the default).
    pub fn with_generation<T: Into<i64>>(mut self, v: T) -> Self {
        self.request.generation = v.into();
        self
    }

    /// Makes the operation conditional on whether the object's current generation
    /// matches the given value. Setting to 0 makes the operation succeed only if
    /// there are no live versions of the object.
    pub fn with_if_generation_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_generation_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's live generation
    /// does not match the given value. If no live object exists, the precondition
    /// fails. Setting to 0 makes the operation succeed only if there is a live
    /// version of the object.
    pub fn with_if_generation_not_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_generation_not_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn with_if_metageneration_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_metageneration_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    pub fn with_if_metageneration_not_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_metageneration_not_match = Some(v.into());
        self
    }

    /// The offset for the first byte to return in the read, relative to
    /// the start of the object.
    ///
    /// A negative `read_offset` value will be interpreted as the number of bytes
    /// back from the end of the object to be returned.
    ///
    /// # Examples
    ///
    /// Read starting at 100 bytes to end of file.
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_offset(100)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// Read last 100 bytes of file:
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_offset(-100)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// Read bytes 1000 to 1099.
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_offset(1000)
    ///     .with_read_limit(100)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub fn with_read_offset<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.read_offset = v.into();
        self
    }

    /// The maximum number of `data` bytes the server is allowed to
    /// return.
    ///
    /// A `read_limit` of zero indicates that there is no limit,
    /// and a negative `read_limit` will cause an error.
    ///
    /// # Examples:
    ///
    /// Read first 100 bytes.
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_limit(100)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// Read bytes 1000 to 1099.
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_offset(1000)
    ///     .with_read_limit(100)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub fn with_read_limit<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.read_limit = v.into();
        self
    }

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    ///
    /// Example:
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::client::KeyAes256;
    /// # let client = Storage::builder().build().await?;
    /// let key: &[u8] = &[97; 32];
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_key(KeyAes256::new(key)?)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub fn with_key(mut self, v: KeyAes256) -> Self {
        self.request.common_object_request_params = Some(v.into());
        self
    }

    /// Sends the request.
    pub async fn send(self) -> Result<ReadObjectResponse> {
        let full_content_requested = self.request.read_offset == 0 && self.request.read_limit == 0;

        let builder = self.http_request_builder().await?;

        tracing::info!("builder={builder:?}");

        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        Ok(ReadObjectResponse {
            inner: response,
            full_content_requested,
            crc32c: 0,
        })
    }

    async fn http_request_builder(self) -> Result<reqwest::RequestBuilder> {
        // Collect the required bucket and object parameters.
        let bucket: String = self.request.bucket;
        let bucket_id = bucket
            .as_str()
            .strip_prefix("projects/_/buckets/")
            .ok_or_else(|| {
                Error::binding(format!(
                    "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
                ))
            })?;
        let object: String = self.request.object;

        // Build the request.
        let builder = self
            .inner
            .client
            .request(
                reqwest::Method::GET,
                format!(
                    "{}/storage/v1/b/{bucket_id}/o/{}",
                    &self.inner.endpoint,
                    enc(&object)
                ),
            )
            .query(&[("alt", "media")])
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );

        // Add the optional query parameters.
        let builder = if self.request.generation != 0 {
            builder.query(&[("generation", self.request.generation)])
        } else {
            builder
        };
        let builder = self
            .request
            .if_generation_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifGenerationMatch", v)]));
        let builder = self
            .request
            .if_generation_not_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifGenerationNotMatch", v)]));
        let builder = self
            .request
            .if_metageneration_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifMetagenerationMatch", v)]));
        let builder = self
            .request
            .if_metageneration_not_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifMetagenerationNotMatch", v)]));

        let builder = apply_customer_supplied_encryption_headers(
            builder,
            self.request.common_object_request_params,
        );

        // Apply "range" header for read limits and offsets.
        let builder = match (self.request.read_offset, self.request.read_limit) {
            // read_limit can't be negative.
            (_, l) if l < 0 => Err(RangeError::NegativeLimit),
            // negative offset can't also have a read_limit.
            (o, l) if o < 0 && l > 0 => Err(RangeError::NegativeOffsetWithLimit),
            // If both are zero, we use default implementation (no range header).
            (0, 0) => Ok(builder),
            // read_limit is zero, means no limit. Read from offset to end of file.
            // This handles cases like (5, 0) -> "bytes=5-"
            (o, 0) => Ok(builder.header("range", format!("bytes={o}-"))),
            // General case: non-negative offset and positive limit.
            // This covers cases like (0, 100) -> "bytes=0-99", (5, 100) -> "bytes=5-104"
            (o, l) => Ok(builder.header("range", format!("bytes={o}-{}", o + l - 1))),
        }
        .map_err(Error::ser)?;

        self.inner.apply_auth_headers(builder).await
    }
}

fn headers_to_crc32c(headers: &http::HeaderMap) -> Option<u32> {
    headers
        .get("x-goog-hash")
        .and_then(|hash| hash.to_str().ok())
        .and_then(|hash| hash.split(",").find(|v| v.starts_with("crc32c")))
        .and_then(|hash| {
            let hash = hash.trim_start_matches("crc32c=");
            v1::Crc32c::deserialize_as(serde_json::json!(hash)).ok()
        })
}

/// A response to a [Storage::read_object] request.
#[derive(Debug)]
pub struct ReadObjectResponse {
    inner: reqwest::Response,
    // Fields for tracking the crc checksum checks.
    full_content_requested: bool,
    crc32c: u32,
}

impl ReadObjectResponse {
    // Get the full object as bytes.
    //
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let contents = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?
    ///     .all_bytes()
    ///     .await?;
    /// println!("object contents={contents:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub async fn all_bytes(self) -> Result<bytes::Bytes> {
        let response_crc32c = self.check_crc32c();

        let bytes = self.inner.bytes().await.map_err(Error::io)?;
        let crc32c = crc32c::crc32c(&bytes);
        check_crc32c_match(crc32c, response_crc32c)?;
        Ok(bytes)
    }

    /// Stream the next bytes of the object.
    ///
    /// When the response has been exhausted, this will return None.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let mut resp = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?;
    ///
    /// while let Some(next) = resp.next().await {
    ///     println!("next={:?}", next?);
    /// }
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub async fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        let res = self.inner.chunk().await.map_err(Error::io);
        match res {
            Ok(Some(chunk)) => {
                self.crc32c = crc32c::crc32c_append(self.crc32c, &chunk);
                Some(Ok(chunk))
            }
            Ok(None) => {
                let res = check_crc32c_match(self.crc32c, self.check_crc32c());
                match res {
                    Err(e) => Some(Err(e)),
                    Ok(()) => None,
                }
            }
            Err(e) => Some(Err(e)),
        }
    }

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Convert the response to a [futures::Stream].
    pub fn into_stream(self) -> impl futures::Stream<Item = Result<bytes::Bytes>> {
        // TODO(#2049): implement checksum checks for streams.
        use futures::TryStreamExt;
        self.inner.bytes_stream().map_err(Error::io)
    }

    fn check_crc32c(&self) -> Option<u32> {
        // Check the CRC iff all of the following hold:
        // 1. We requested the full content (request.read_limit = 0, request.read_offset = 0).
        // 2. We got all the content (status != PartialContent).
        // 3. The server sent a CRC header.
        // 4. The http stack did not uncompress the file.
        // 5. We were not served compressed data that was uncompressed on download.
        //
        // For 4, we turn off automatic decompression in reqwest::Client when we create it,
        // so it will not be turned on.
        check_crc32c_helper(
            self.full_content_requested,
            self.inner.status(),
            self.inner.headers(),
        )
    }
}

/// Represents an error that can occur when reading response data.
#[derive(thiserror::Error, Debug, PartialEq)]
#[non_exhaustive]
enum ReadError {
    /// The calculated crc32c did not match server provided crc32c.
    #[error("bad CRC on read: got {got}, want {want}")]
    BadCrc { got: u32, want: u32 },
}

// helper to make testing easier.
fn check_crc32c_helper(
    full_content_requested: bool,
    status: http::StatusCode,
    headers: &http::HeaderMap,
) -> Option<u32> {
    if !full_content_requested || status == http::StatusCode::PARTIAL_CONTENT {
        return None;
    }
    let stored_encoding = headers
        .get("x-goog-stored-content-encoding")
        .and_then(|e| e.to_str().ok())
        .map_or("", |e| e);
    let content_encoding = headers
        .get("content-encoding")
        .and_then(|e| e.to_str().ok())
        .map_or("", |e| e);
    if stored_encoding == "gzip" && content_encoding != "gzip" {
        return None;
    }
    headers_to_crc32c(headers)
}

fn check_crc32c_match(crc32c: u32, response: Option<u32>) -> Result<()> {
    if let Some(response) = response {
        if crc32c != response {
            return Err(Error::deser(ReadError::BadCrc {
                got: crc32c,
                want: response,
            }));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_key_helper;
    use super::super::tests::test_inner_client;
    use super::*;
    use futures::TryStreamExt;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use std::collections::HashMap;
    use std::error::Error;
    use test_case::test_case;

    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn read_object_normal() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "//storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(status_code(200).body("hello world")),
        );

        let endpoint = server.url("");
        let client = Storage::builder()
            .with_endpoint(endpoint.to_string())
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
    async fn read_object_stream() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "//storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(status_code(200).body("hello world")),
        );

        let endpoint = server.url("");
        let client = Storage::builder()
            .with_endpoint(endpoint.to_string())
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let response = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        let result: Vec<_> = response.into_stream().try_collect().await?;
        assert_eq!(result, vec![bytes::Bytes::from_static(b"hello world")]);

        Ok(())
    }

    #[tokio::test]
    async fn read_object_not_found() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "//storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(status_code(404).body("NOT FOUND")),
        );

        let endpoint = server.url("");
        let client = Storage::builder()
            .with_endpoint(endpoint.to_string())
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let err = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await
            .expect_err("expected a not found error");
        assert_eq!(err.http_status_code(), Some(404));

        Ok(())
    }

    #[tokio::test]
    async fn read_object_incorrect_crc32c_check() -> Result {
        // Calculate and serialize the crc32c checksum
        let u = crc32c::crc32c("goodbye world".as_bytes());
        let bytes = [
            (u >> 24 & 0xFF) as u8,
            (u >> 16 & 0xFF) as u8,
            (u >> 8 & 0xFF) as u8,
            (u & 0xFF) as u8,
        ];
        let value = base64::prelude::BASE64_STANDARD.encode(bytes);

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "//storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .times(2)
            .respond_with(
                status_code(200)
                    .body("hello world")
                    .append_header("x-goog-hash", format!("crc32c={value}")),
            ),
        );

        let endpoint = server.url("");
        let client = Storage::builder()
            .with_endpoint(endpoint.to_string())
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let response = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        response
            .all_bytes()
            .await
            .expect_err("expect error on incorrect crc32c");

        let mut response = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        let res: crate::Result<()> = async {
            {
                while (response.next().await.transpose()?).is_some() {}
                Ok(())
            }
        }
        .await;
        assert!(res.is_err(), "expect error on incorrect crc32c");
        Ok(())
    }

    #[tokio::test]
    async fn read_object() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = ReadObject::new(inner, "projects/_/buckets/bucket", "object")
            .http_request_builder()
            .await?
            .build()?;

        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_object_error_credentials() -> Result {
        let config = gaxi::options::ClientConfig {
            cred: Some(auth::credentials::testing::error_credentials(false)),
            ..Default::default()
        };
        let inner = test_inner_client(config);
        let _ = ReadObject::new(inner, "projects/_/buckets/bucket", "object")
            .http_request_builder()
            .await
            .inspect_err(|e| assert!(e.is_authentication()))
            .expect_err("invalid credentials should err");
        Ok(())
    }

    #[tokio::test]
    async fn read_object_bad_bucket() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        ReadObject::new(inner, "malformed", "object")
            .http_request_builder()
            .await
            .expect_err("malformed bucket string should error");
        Ok(())
    }

    #[tokio::test]
    async fn read_object_query_params() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = ReadObject::new(inner, "projects/_/buckets/bucket", "object")
            .with_generation(5)
            .with_if_generation_match(10)
            .with_if_generation_not_match(20)
            .with_if_metageneration_match(30)
            .with_if_metageneration_not_match(40)
            .http_request_builder()
            .await?
            .build()?;

        assert_eq!(request.method(), reqwest::Method::GET);
        let want_pairs: HashMap<String, String> = [
            ("alt", "media"),
            ("generation", "5"),
            ("ifGenerationMatch", "10"),
            ("ifGenerationNotMatch", "20"),
            ("ifMetagenerationMatch", "30"),
            ("ifMetagenerationNotMatch", "40"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let query_pairs: HashMap<String, String> = request
            .url()
            .query_pairs()
            .map(|param| (param.0.to_string(), param.1.to_string()))
            .collect();
        assert_eq!(query_pairs.len(), want_pairs.len());
        assert_eq!(query_pairs, want_pairs);
        Ok(())
    }

    #[tokio::test]
    async fn read_object_headers() -> Result {
        // Make a 32-byte key.
        let (key, key_base64, _, key_sha256_base64) = create_key_helper();

        // The API takes the unencoded byte array.
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = ReadObject::new(inner, "projects/_/buckets/bucket", "object")
            .with_key(KeyAes256::new(&key)?)
            .http_request_builder()
            .await?
            .build()?;

        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
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

    #[test_case(0, 0, None; "no headers needed")]
    #[test_case(10, 0, Some(&http::HeaderValue::from_static("bytes=10-")); "offset only")]
    #[test_case(-2000, 0, Some(&http::HeaderValue::from_static("bytes=-2000-")); "negative offset")]
    #[test_case(0, 100, Some(&http::HeaderValue::from_static("bytes=0-99")); "limit only")]
    #[test_case(1000, 100, Some(&http::HeaderValue::from_static("bytes=1000-1099")); "offset and limit")]
    #[tokio::test]
    async fn range_header(offset: i64, limit: i64, want: Option<&http::HeaderValue>) -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = ReadObject::new(inner, "projects/_/buckets/bucket", "object")
            .with_read_offset(offset)
            .with_read_limit(limit)
            .http_request_builder()
            .await?
            .build()?;

        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
        );

        assert_eq!(request.headers().get("range"), want);
        Ok(())
    }

    #[test_case(0, -100, RangeError::NegativeLimit; "negative limit")]
    #[test_case(-100, 100, RangeError::NegativeOffsetWithLimit; "negative offset with positive limit")]
    #[tokio::test]
    async fn test_range_header_error(offset: i64, limit: i64, want_err: RangeError) -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let err = ReadObject::new(inner, "projects/_/buckets/bucket", "object")
            .with_read_offset(offset)
            .with_read_limit(limit)
            .http_request_builder()
            .await
            .unwrap_err();

        assert_eq!(
            err.source().unwrap().downcast_ref::<RangeError>().unwrap(),
            &want_err
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
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = ReadObject::new(inner, "projects/_/buckets/bucket", name)
            .http_request_builder()
            .await?
            .build()?;
        let got = request.url().path_segments().unwrap().next_back().unwrap();
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(10, Some(10), false; "Match values")]
    #[test_case(10, None, false; "None response")]
    #[test_case(10, Some(20), true; "Different values")]
    fn test_check_crc(crc: u32, resp_crc: Option<u32>, want_err: bool) {
        let res = check_crc32c_match(crc, resp_crc);
        if want_err {
            assert_eq!(
                res.unwrap_err()
                    .source()
                    .unwrap()
                    .downcast_ref::<ReadError>()
                    .unwrap(),
                &ReadError::BadCrc {
                    got: crc,
                    want: resp_crc.unwrap(),
                }
            );
        } else {
            res.unwrap();
        }
    }

    #[test_case("", None; "no header")]
    #[test_case("crc32c=hello", None; "invalid value")]
    #[test_case("crc32c=AAAAAA==", Some(0); "zero value")]
    #[test_case("crc32c=SZYC0g==", Some(1234567890_u32); "value")]
    #[test_case("crc32c=SZYC0g==,md5=something", Some(1234567890_u32); "md5 after crc32c")]
    #[test_case("md5=something,crc32c=SZYC0g==", Some(1234567890_u32); "md5 before crc32c")]
    fn test_headers_to_crc(val: &str, want: Option<u32>) -> Result {
        let mut headers = http::HeaderMap::new();
        if !val.is_empty() {
            headers.insert("x-goog-hash", http::HeaderValue::from_str(val)?);
        }
        let got = headers_to_crc32c(&headers);
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case(false, vec![("x-goog-hash", "crc32c=SZYC0g==")], http::StatusCode::OK, None; "full content not requested")]
    #[test_case(true, vec![], http::StatusCode::PARTIAL_CONTENT, None; "No x-goog-hash")]
    #[test_case(true, vec![("x-goog-hash", "crc32c=SZYC0g=="), ("x-goog-stored-content-encoding", "gzip"), ("content-encoding", "json")], http::StatusCode::OK, None; "server uncompressed")]
    #[test_case(true, vec![("x-goog-hash", "crc32c=SZYC0g=="), ("x-goog-stored-content-encoding", "gzip"), ("content-encoding", "gzip")], http::StatusCode::OK, Some(1234567890_u32); "both gzip")]
    #[test_case(true, vec![("x-goog-hash", "crc32c=SZYC0g==")], http::StatusCode::OK, Some(1234567890_u32); "all ok")]
    fn test_check_crc_enabled(
        full_content_requested: bool,
        headers: Vec<(&str, &str)>,
        status: http::StatusCode,
        want: Option<u32>,
    ) -> Result {
        let mut header_map = http::HeaderMap::new();
        for (key, value) in headers {
            header_map.insert(
                http::HeaderName::from_bytes(key.as_bytes())?,
                http::HeaderValue::from_bytes(value.as_bytes())?,
            );
        }

        let got = check_crc32c_helper(full_content_requested, status, &header_map);
        assert_eq!(got, want);
        Ok(())
    }
}
