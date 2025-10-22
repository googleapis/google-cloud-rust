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

mod non_resumable;
mod parse_http_response;
mod resumable;

use super::client::*;
use super::*;
use crate::model_ext::KeyAes256;
use crate::read_object::ReadObjectResponse;
use crate::read_resume_policy::ReadResumePolicy;
use crate::storage::checksum::details::Md5;
use crate::storage::request_options::RequestOptions;

/// The request builder for [Storage::read_object][crate::client::Storage::read_object] calls.
///
/// # Example: accumulate the contents of an object into a vector
/// ```
/// use google_cloud_storage::{client::Storage, builder::storage::ReadObject};
/// async fn sample(client: &Storage) -> anyhow::Result<()> {
///     let builder: ReadObject = client.read_object("projects/_/buckets/my-bucket", "my-object");
///     let mut reader = builder.send().await?;
///     let mut contents = Vec::new();
///     while let Some(chunk) = reader.next().await.transpose()? {
///         contents.extend_from_slice(&chunk);
///     }
///     println!("object contents={:?}", contents);
///     Ok(())
/// }
/// ```
///
/// # Example: read part of an object
/// ```
/// use google_cloud_storage::{client::Storage, builder::storage::ReadObject};
/// use google_cloud_storage::model_ext::ReadRange;
/// async fn sample(client: &Storage) -> anyhow::Result<()> {
///     const MIB: u64 = 1024 * 1024;
///     let mut contents = Vec::new();
///     let mut reader = client
///         .read_object("projects/_/buckets/my-bucket", "my-object")
///         .set_read_range(ReadRange::segment(4 * MIB, 2 * MIB))
///         .send()
///         .await?;
///     while let Some(chunk) = reader.next().await.transpose()? {
///         contents.extend_from_slice(&chunk);
///     }
///     println!("range contents={:?}", contents);
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ReadObject<S = crate::storage::transport::Storage>
where
    S: crate::storage::stub::Storage + 'static,
{
    stub: std::sync::Arc<S>,
    request: crate::model::ReadObjectRequest,
    options: RequestOptions,
}

impl<S> ReadObject<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    pub(crate) fn new<B, O>(
        stub: std::sync::Arc<S>,
        bucket: B,
        object: O,
        options: RequestOptions,
    ) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        ReadObject {
            stub,
            request: crate::model::ReadObjectRequest::new()
                .set_bucket(bucket)
                .set_object(object),
            options,
        }
    }

    /// Enables computation of MD5 hashes.
    ///
    /// Crc32c hashes are checked by default.
    ///
    /// Checksum validation is supported iff:
    /// 1. The full content is requested.
    /// 2. All of the content is returned (status != PartialContent).
    /// 3. The server sent a checksum header.
    /// 4. The http stack did not uncompress the file.
    /// 5. The server did not uncompress data on read.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let builder =  client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .compute_md5();
    /// let mut reader = builder
    ///     .send()
    ///     .await?;
    /// let mut contents = Vec::new();
    /// while let Some(chunk) = reader.next().await.transpose()? {
    ///     contents.extend_from_slice(&chunk);
    /// }
    /// println!("object contents={:?}", contents);
    /// # Ok(()) }
    /// ```
    pub fn compute_md5(self) -> Self {
        let mut this = self;
        this.options.checksum.md5_hash = Some(Md5::default());
        this
    }

    /// If present, selects a specific revision of this object (as
    /// opposed to the latest version, the default).
    pub fn set_generation<T: Into<i64>>(mut self, v: T) -> Self {
        self.request.generation = v.into();
        self
    }

    /// Makes the operation conditional on whether the object's current generation
    /// matches the given value. Setting to 0 makes the operation succeed only if
    /// there are no live versions of the object.
    pub fn set_if_generation_match<T>(mut self, v: T) -> Self
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
    pub fn set_if_generation_not_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_generation_not_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn set_if_metageneration_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_metageneration_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    pub fn set_if_metageneration_not_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_metageneration_not_match = Some(v.into());
        self
    }

    /// The range of bytes to return in the read.
    ///
    /// This can be all the bytes starting at a given offset
    /// (`ReadRange::offset()`), all the bytes in an explicit range
    /// (`Range::segment`), or the last N bytes of the object
    /// (`ReadRange::tail`).
    ///
    /// # Examples
    ///
    /// Read starting at 100 bytes to end of file.
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::model_ext::ReadRange;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_read_range(ReadRange::offset(100))
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// Read last 100 bytes of file:
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::model_ext::ReadRange;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_read_range(ReadRange::tail(100))
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// Read bytes 1000 to 1099.
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::model_ext::ReadRange;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_read_range(ReadRange::segment(1000, 100))
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub fn set_read_range(mut self, range: crate::model_ext::ReadRange) -> Self {
        self.request.with_range(range);
        self
    }

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    ///
    /// Example:
    /// ```
    /// # use google_cloud_storage::{model_ext::KeyAes256, client::Storage};
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let key: &[u8] = &[97; 32];
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_key(KeyAes256::new(key)?)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub fn set_key(mut self, v: KeyAes256) -> Self {
        self.request.common_object_request_params = Some(v.into());
        self
    }

    /// The retry policy used for this request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::retry_policy::RetryableErrors;
    /// use std::time::Duration;
    /// use gax::retry_policy::RetryPolicyExt;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_retry_policy(
    ///         RetryableErrors
    ///             .with_attempt_limit(5)
    ///             .with_time_limit(Duration::from_secs(10)),
    ///     )
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub fn with_retry_policy<V: Into<gax::retry_policy::RetryPolicyArg>>(mut self, v: V) -> Self {
        self.options.retry_policy = v.into().into();
        self
    }

    /// The backoff policy used for this request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use std::time::Duration;
    /// use gax::exponential_backoff::ExponentialBackoff;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_backoff_policy(ExponentialBackoff::default())
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub fn with_backoff_policy<V: Into<gax::backoff_policy::BackoffPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.backoff_policy = v.into().into();
        self
    }

    /// The retry throttler used for this request.
    ///
    /// Most of the time you want to use the same throttler for all the requests
    /// in a client, and even the same throttler for many clients. Rarely it
    /// may be necessary to use an custom throttler for some subset of the
    /// requests.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_retry_throttler(adhoc_throttler())
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// fn adhoc_throttler() -> gax::retry_throttler::SharedRetryThrottler {
    ///     # panic!();
    /// }
    /// # Ok(()) }
    /// ```
    pub fn with_retry_throttler<V: Into<gax::retry_throttler::RetryThrottlerArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_throttler = v.into().into();
        self
    }

    /// Configure the resume policy for read requests.
    ///
    /// The Cloud Storage client library can automatically resume a read that is
    /// interrupted by a transient error. Applications may want to limit the
    /// number of read attempts, or may wish to expand the type of errors
    /// treated as retryable.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::read_resume_policy::{AlwaysResume, ReadResumePolicyExt};
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_resume_policy(AlwaysResume.with_attempt_limit(3))
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_read_resume_policy<V>(mut self, v: V) -> Self
    where
        V: ReadResumePolicy + 'static,
    {
        self.options.read_resume_policy = std::sync::Arc::new(v);
        self
    }

    /// Enables automatic decompression.
    ///
    /// The Cloud Storage service [automatically decompresses] objects
    /// with `content_encoding == "gzip"` during reads. The client library
    /// disables this behavior by default, as it is not possible to
    /// perform ranged reads or to resume interrupted downloads if automatic
    /// decompression is enabled.
    ///
    /// Use this option to enable automatic decompression.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_automatic_decompression(true)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub fn with_automatic_decompression(mut self, v: bool) -> Self {
        self.options.automatic_decompression = v;
        self
    }

    /// Sends the request.
    pub async fn send(self) -> Result<ReadObjectResponse> {
        self.stub.read_object(self.request, self.options).await
    }
}

// A convenience struct that saves the request conditions and performs the read.
#[derive(Clone, Debug)]
pub(crate) struct Reader {
    pub inner: std::sync::Arc<StorageInner>,
    pub request: crate::model::ReadObjectRequest,
    pub options: RequestOptions,
}

impl Reader {
    async fn read(self) -> Result<reqwest::Response> {
        let throttler = self.options.retry_throttler.clone();
        let retry = self.options.retry_policy.clone();
        let backoff = self.options.backoff_policy.clone();

        gax::retry_loop_internal::retry_loop(
            async move |_| self.read_attempt().await,
            async |duration| tokio::time::sleep(duration).await,
            true,
            throttler,
            retry,
            backoff,
        )
        .await
    }

    async fn read_attempt(&self) -> Result<reqwest::Response> {
        let builder = self.http_request_builder().await?;
        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        Ok(response)
    }

    async fn http_request_builder(&self) -> Result<reqwest::RequestBuilder> {
        // Collect the required bucket and object parameters.
        let bucket = &self.request.bucket;
        let bucket_id = bucket
            .as_str()
            .strip_prefix("projects/_/buckets/")
            .ok_or_else(|| {
                Error::binding(format!(
                    "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
                ))
            })?;
        let object = &self.request.object;

        // Build the request.
        let builder = self
            .inner
            .client
            .request(
                reqwest::Method::GET,
                format!(
                    "{}/storage/v1/b/{bucket_id}/o/{}",
                    &self.inner.endpoint,
                    enc(object)
                ),
            )
            .query(&[("alt", "media")])
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );

        let builder = if self.options.automatic_decompression {
            builder
        } else {
            // Disable decompressive transcoding: https://cloud.google.com/storage/docs/transcoding
            //
            // The default is to decompress objects that have `contentEncoding == "gzip"`. This header
            // tells Cloud Storage to disable automatic decompression. It has no effect on objects
            // with a different `contentEncoding` value.
            builder.header(
                "accept-encoding",
                reqwest::header::HeaderValue::from_static("gzip"),
            )
        };

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
            &self.request.common_object_request_params,
        );

        // Apply "range" header for read limits and offsets.
        let builder = match (self.request.read_offset, self.request.read_limit) {
            // read_limit can't be negative.
            (_, l) if l < 0 => {
                unreachable!("ReadObject build never sets a negative read_limit value")
            }
            // negative offset can't also have a read_limit.
            (o, l) if o < 0 && l > 0 => unreachable!(
                "ReadObject builder never sets a positive read_offset value with a negative read_limit value"
            ),
            // If both are zero, we use default implementation (no range header).
            (0, 0) => builder,
            // negative offset with no limit means the last N bytes.
            (o, 0) if o < 0 => builder.header("range", format!("bytes={o}")),
            // read_limit is zero, means no limit. Read from offset to end of file.
            // This handles cases like (5, 0) -> "bytes=5-"
            (o, 0) => builder.header("range", format!("bytes={o}-")),
            // General case: non-negative offset and positive limit.
            // This covers cases like (0, 100) -> "bytes=0-99", (5, 100) -> "bytes=5-104"
            (o, l) => builder.header("range", format!("bytes={o}-{}", o + l - 1)),
        };

        self.inner.apply_auth_headers(builder).await
    }

    fn is_gunzipped(response: &reqwest::Response) -> bool {
        // Cloud Storage automatically [decompresses gzip-compressed][transcoding]
        // objects. Reading such objects comes with a number of restrictions:
        // - Ranged reads do not work.
        // - The size of the decompressed data is not known.
        // - Checksums do not work because the object checksums correspond to the
        //   compressed data and the client library receives the decompressed data.
        //
        // Because ranged reads do not work, resuming a read does not work. Consequently,
        // the implementation of `ReadObjectResponse` is substantially different for
        // objects that are gunzipped.
        //
        // [transcoding]: https://cloud.google.com/storage/docs/transcoding
        const TRANSFORMATION: &str = "x-guploader-response-body-transformations";
        use http::header::WARNING;
        if response
            .headers()
            .get(TRANSFORMATION)
            .is_some_and(|h| h.as_bytes() == "gunzipped".as_bytes())
        {
            return true;
        }
        response
            .headers()
            .get(WARNING)
            .is_some_and(|h| h.as_bytes() == "214 UploadServer gunzipped".as_bytes())
    }

    pub(crate) async fn response(self) -> Result<ReadObjectResponse> {
        let response = self.clone().read().await?;
        if Self::is_gunzipped(&response) {
            return Ok(ReadObjectResponse::new(Box::new(
                non_resumable::NonResumableResponse::new(response)?,
            )));
        }
        Ok(ReadObjectResponse::new(Box::new(
            resumable::ResumableResponse::new(self, response)?,
        )))
    }
}

#[cfg(test)]
mod resume_tests;

#[cfg(test)]
mod tests {
    use super::client::tests::{test_builder, test_inner_client};
    use super::*;
    use crate::error::{ChecksumMismatch, ReadError};
    use crate::model_ext::{KeyAes256, ReadRange, tests::create_key_helper};
    use auth::credentials::anonymous::Builder as Anonymous;
    use base64::Engine;
    use futures::TryStreamExt;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use test_case::test_case;

    type Result = anyhow::Result<()>;

    async fn http_request_builder(
        inner: Arc<StorageInner>,
        builder: ReadObject,
    ) -> crate::Result<reqwest::RequestBuilder> {
        let reader = Reader {
            inner,
            request: builder.request,
            options: builder.options,
        };
        reader.http_request_builder().await
    }

    // Verify `read_object()` meets normal Send, Sync, requirements.
    #[tokio::test]
    async fn test_read_is_send_and_static() -> Result {
        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        fn need_send<T: Send>(_val: &T) {}
        fn need_sync<T: Sync>(_val: &T) {}
        fn need_static<T: 'static>(_val: &T) {}

        let read = client.read_object("projects/_/buckets/test-bucket", "test-object");
        need_send(&read);
        need_sync(&read);
        need_static(&read);

        let read = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send();
        need_send(&read);
        need_static(&read);

        Ok(())
    }

    #[tokio::test]
    async fn read_object_normal() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
                request::headers(contains(("accept-encoding", "gzip"))),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(
                status_code(200)
                    .body("hello world")
                    .append_header("x-goog-generation", 123456),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let mut reader = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        let mut got = Vec::new();
        while let Some(b) = reader.next().await.transpose()? {
            got.extend_from_slice(&b);
        }
        assert_eq!(bytes::Bytes::from_owner(got), "hello world");

        Ok(())
    }

    #[tokio::test]
    async fn read_object_stream() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(
                status_code(200)
                    .append_header("x-goog-generation", 123456)
                    .body("hello world"),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
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
    async fn read_object_next_then_consume_response() -> Result {
        // Create a large enough file that will require multiple chunks to read.
        const BLOCK_SIZE: usize = 500;
        let mut contents = Vec::new();
        for i in 0..50 {
            contents.extend_from_slice(&[i as u8; BLOCK_SIZE]);
        }

        // Calculate and serialize the crc32c checksum
        let u = crc32c::crc32c(&contents);
        let value = base64::prelude::BASE64_STANDARD.encode(u.to_be_bytes());

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .times(1)
            .respond_with(
                status_code(200)
                    .body(contents.clone())
                    .append_header("x-goog-hash", format!("crc32c={value}"))
                    .append_header("x-goog-generation", 123456),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        // Read some bytes, then remainder with stream.
        let mut response = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;

        let mut all_bytes = bytes::BytesMut::new();
        let chunk = response.next().await.transpose()?.unwrap();
        assert!(!chunk.is_empty());
        all_bytes.extend(chunk);
        use futures::StreamExt;
        let mut stream = response.into_stream();
        while let Some(chunk) = stream.next().await.transpose()? {
            all_bytes.extend(chunk);
        }
        assert_eq!(all_bytes, contents);

        Ok(())
    }

    #[tokio::test]
    async fn read_object_not_found() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(status_code(404).body("NOT FOUND")),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
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
        let value = base64::prelude::BASE64_STANDARD.encode(u.to_be_bytes());

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .times(3)
            .respond_with(
                status_code(200)
                    .body("hello world")
                    .append_header("x-goog-hash", format!("crc32c={value}"))
                    .append_header("x-goog-generation", 123456),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let mut response = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        let mut partial = Vec::new();
        let mut err = None;
        while let Some(r) = response.next().await {
            match r {
                Ok(b) => partial.extend_from_slice(&b),
                Err(e) => err = Some(e),
            };
        }
        assert_eq!(bytes::Bytes::from_owner(partial), "hello world");
        let err = err.expect("expect error on incorrect crc32c");
        let source = err.source().and_then(|e| e.downcast_ref::<ReadError>());
        assert!(
            matches!(
                source,
                Some(&ReadError::ChecksumMismatch(
                    ChecksumMismatch::Crc32c { .. }
                ))
            ),
            "err={err:?}"
        );

        let mut response = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        let err: crate::Error = async {
            {
                while (response.next().await.transpose()?).is_some() {}
                Ok(())
            }
        }
        .await
        .expect_err("expect error on incorrect crc32c");
        let source = err.source().and_then(|e| e.downcast_ref::<ReadError>());
        assert!(
            matches!(
                source,
                Some(&ReadError::ChecksumMismatch(
                    ChecksumMismatch::Crc32c { .. }
                ))
            ),
            "err={err:?}"
        );

        use futures::TryStreamExt;
        let err = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?
            .into_stream()
            .try_collect::<Vec<bytes::Bytes>>()
            .await
            .expect_err("expect error on incorrect crc32c");
        let source = err.source().and_then(|e| e.downcast_ref::<ReadError>());
        assert!(
            matches!(
                source,
                Some(&ReadError::ChecksumMismatch(
                    ChecksumMismatch::Crc32c { .. }
                ))
            ),
            "err={err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_object_incorrect_md5_check() -> Result {
        // Calculate and serialize the md5 checksum
        let digest = md5::compute("goodbye world".as_bytes());
        let value = base64::prelude::BASE64_STANDARD.encode(digest.as_ref());

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .times(1)
            .respond_with(
                status_code(200)
                    .body("hello world")
                    .append_header("x-goog-hash", format!("md5={value}"))
                    .append_header("x-goog-generation", 123456),
            ),
        );

        let client = Storage::builder()
            .with_endpoint(format!("http://{}", server.addr()))
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let mut response = client
            .read_object("projects/_/buckets/test-bucket", "test-object")
            .compute_md5()
            .send()
            .await?;
        let mut partial = Vec::new();
        let mut err = None;
        while let Some(r) = response.next().await {
            match r {
                Ok(b) => partial.extend_from_slice(&b),
                Err(e) => err = Some(e),
            };
        }
        assert_eq!(bytes::Bytes::from_owner(partial), "hello world");
        let err = err.expect("expect error on incorrect md5");
        let source = err.source().and_then(|e| e.downcast_ref::<ReadError>());
        assert!(
            matches!(
                source,
                Some(&ReadError::ChecksumMismatch(ChecksumMismatch::Md5 { .. }))
            ),
            "err={err:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_object() -> Result {
        let inner = test_inner_client(test_builder());
        let stub = crate::storage::transport::Storage::new(inner.clone());
        let builder = ReadObject::new(
            stub,
            "projects/_/buckets/bucket",
            "object",
            inner.options.clone(),
        );
        let request = http_request_builder(inner, builder).await?.build()?;

        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_object_error_credentials() -> Result {
        let inner = test_inner_client(
            test_builder().with_credentials(auth::credentials::testing::error_credentials(false)),
        );
        let stub = crate::storage::transport::Storage::new(inner.clone());
        let builder = ReadObject::new(
            stub,
            "projects/_/buckets/bucket",
            "object",
            inner.options.clone(),
        );
        let _ = http_request_builder(inner, builder)
            .await
            .inspect_err(|e| assert!(e.is_authentication()))
            .expect_err("invalid credentials should err");
        Ok(())
    }

    #[tokio::test]
    async fn read_object_bad_bucket() -> Result {
        let inner = test_inner_client(test_builder());
        let stub = crate::storage::transport::Storage::new(inner.clone());
        let builder = ReadObject::new(stub, "malformed", "object", inner.options.clone());
        let _ = http_request_builder(inner, builder)
            .await
            .expect_err("malformed bucket string should error");
        Ok(())
    }

    #[tokio::test]
    async fn read_object_query_params() -> Result {
        let inner = test_inner_client(test_builder());
        let stub = crate::storage::transport::Storage::new(inner.clone());
        let builder = ReadObject::new(
            stub,
            "projects/_/buckets/bucket",
            "object",
            inner.options.clone(),
        )
        .set_generation(5)
        .set_if_generation_match(10)
        .set_if_generation_not_match(20)
        .set_if_metageneration_match(30)
        .set_if_metageneration_not_match(40);
        let request = http_request_builder(inner, builder).await?.build()?;

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
    async fn read_object_default_headers() -> Result {
        // The API takes the unencoded byte array.
        let inner = test_inner_client(test_builder());
        let stub = crate::storage::transport::Storage::new(inner.clone());
        let builder = ReadObject::new(
            stub,
            "projects/_/buckets/bucket",
            "object",
            inner.options.clone(),
        );
        let request = http_request_builder(inner, builder).await?.build()?;

        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
        );

        let want = [("accept-encoding", "gzip")];
        let headers = request.headers();
        for (name, value) in want {
            assert_eq!(
                headers.get(name).and_then(|h| h.to_str().ok()),
                Some(value),
                "{request:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn read_object_automatic_decompression_headers() -> Result {
        // The API takes the unencoded byte array.
        let inner = test_inner_client(test_builder());
        let stub = crate::storage::transport::Storage::new(inner.clone());
        let builder = ReadObject::new(
            stub,
            "projects/_/buckets/bucket",
            "object",
            inner.options.clone(),
        )
        .with_automatic_decompression(true);
        let request = http_request_builder(inner, builder).await?.build()?;

        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
        );

        let headers = request.headers();
        assert!(headers.get("accept-encoding").is_none(), "{request:?}");
        Ok(())
    }

    #[tokio::test]
    async fn read_object_encryption_headers() -> Result {
        // Make a 32-byte key.
        let (key, key_base64, _, key_sha256_base64) = create_key_helper();

        // The API takes the unencoded byte array.
        let inner = test_inner_client(test_builder());
        let stub = crate::storage::transport::Storage::new(inner.clone());
        let builder = ReadObject::new(
            stub,
            "projects/_/buckets/bucket",
            "object",
            inner.options.clone(),
        )
        .set_key(KeyAes256::new(&key)?);
        let request = http_request_builder(inner, builder).await?.build()?;

        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
        );

        let want = [
            ("x-goog-encryption-algorithm", "AES256".to_string()),
            ("x-goog-encryption-key", key_base64),
            ("x-goog-encryption-key-sha256", key_sha256_base64),
        ];

        let headers = request.headers();
        for (name, value) in want {
            assert_eq!(
                headers.get(name).and_then(|h| h.to_str().ok()),
                Some(value.as_str())
            );
        }
        Ok(())
    }

    #[test_case(ReadRange::all(), None; "no headers needed")]
    #[test_case(ReadRange::offset(10), Some(&http::HeaderValue::from_static("bytes=10-")); "offset only")]
    #[test_case(ReadRange::tail(2000), Some(&http::HeaderValue::from_static("bytes=-2000")); "negative offset")]
    #[test_case(ReadRange::segment(0, 100), Some(&http::HeaderValue::from_static("bytes=0-99")); "limit only")]
    #[test_case(ReadRange::segment(1000, 100), Some(&http::HeaderValue::from_static("bytes=1000-1099")); "offset and limit")]
    #[tokio::test]
    async fn range_header(input: ReadRange, want: Option<&http::HeaderValue>) -> Result {
        let inner = test_inner_client(test_builder());
        let stub = crate::storage::transport::Storage::new(inner.clone());
        let builder = ReadObject::new(
            stub,
            "projects/_/buckets/bucket",
            "object",
            inner.options.clone(),
        )
        .set_read_range(input.clone());
        let request = http_request_builder(inner, builder).await?.build()?;

        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request.url().as_str(),
            "http://private.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
        );

        assert_eq!(request.headers().get("range"), want);
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
        let stub = crate::storage::transport::Storage::new(inner.clone());
        let builder = ReadObject::new(
            stub,
            "projects/_/buckets/bucket",
            name,
            inner.options.clone(),
        );
        let request = http_request_builder(inner, builder).await?.build()?;
        let got = request.url().path_segments().unwrap().next_back().unwrap();
        assert_eq!(got, want);
        Ok(())
    }

    #[test_case("x-guploader-response-body-transformations", "gunzipped", true)]
    #[test_case("x-guploader-response-body-transformations", "no match", false)]
    #[test_case("warning", "214 UploadServer gunzipped", true)]
    #[test_case("warning", "no match", false)]
    #[test_case("unused", "unused", false)]
    fn test_is_gunzipped(name: &'static str, value: &'static str, want: bool) -> Result {
        let response = http::Response::builder()
            .status(200)
            .header(name, value)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let got = Reader::is_gunzipped(&response);
        assert_eq!(got, want, "{response:?}");
        Ok(())
    }
}
