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
    request: control::model::ReadObjectRequest,
}

impl ReadObject {
    pub(crate) fn new<B, O>(inner: std::sync::Arc<StorageInner>, bucket: B, object: O) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        ReadObject {
            inner,
            request: control::model::ReadObjectRequest::new()
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
        let builder = self.http_request_builder().await?;

        tracing::info!("builder={builder:?}");

        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        Ok(ReadObjectResponse { inner: response })
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

/// A response to a [Storage::read_object] request.
#[derive(Debug)]
pub struct ReadObjectResponse {
    inner: reqwest::Response,
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
        self.inner.bytes().await.map_err(Error::io)
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
        self.inner.chunk().await.map_err(Error::io).transpose()
    }

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Convert the response to a [futures::Stream].
    pub fn into_stream(self) -> impl futures::Stream<Item = Result<bytes::Bytes>> {
        use futures::TryStreamExt;
        self.inner.bytes_stream().map_err(Error::io)
    }
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
}
