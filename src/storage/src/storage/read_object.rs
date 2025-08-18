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

use super::client::*;
use super::*;
use crate::error::ReadError;
use crate::model::ObjectChecksums;
use crate::model_ext::KeyAes256;
use crate::model_ext::ObjectHighlights;
use crate::read_resume_policy::ReadResumePolicy;
use crate::storage::checksum::{
    ChecksumEngine,
    details::{Crc32c, Md5, validate},
};
use base64::Engine;
#[cfg(feature = "unstable-stream")]
use futures::Stream;
use serde_with::DeserializeAs;

/// The request builder for [Storage::read_object][crate::client::Storage::read_object] calls.
///
/// # Example: accumulate the contents of an object into a vector
/// ```
/// use google_cloud_storage::{client::Storage, builder::storage::ReadObject, ReadObjectResponse};
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
/// use google_cloud_storage::{client::Storage, builder::storage::ReadObject, ReadObjectResponse};
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
pub struct ReadObject<C = Crc32c> {
    inner: std::sync::Arc<StorageInner>,
    request: crate::model::ReadObjectRequest,
    options: super::request_options::RequestOptions,
    checksum: C,
}

impl ReadObject<Crc32c> {
    pub(crate) fn new<B, O>(inner: std::sync::Arc<StorageInner>, bucket: B, object: O) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        let options = inner.options.clone();
        ReadObject {
            inner,
            request: crate::model::ReadObjectRequest::new()
                .set_bucket(bucket)
                .set_object(object),
            options,
            checksum: Crc32c::default(),
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
    /// use google_cloud_storage::ReadObjectResponse;
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
    pub fn compute_md5(self) -> ReadObject<Md5<Crc32c>> {
        self.switch_checksum(Md5::from_inner)
    }
}

impl<C> ReadObject<C>
where
    C: Clone + ChecksumEngine + Send + Sync + 'static,
{
    fn switch_checksum<F, U>(self, new: F) -> ReadObject<U>
    where
        F: FnOnce(C) -> U,
    {
        ReadObject {
            inner: self.inner,
            request: self.request,
            options: self.options,
            checksum: new(self.checksum),
        }
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
    /// This can be all the bytes starting at a give offset (`ReadRange::offset()`),
    /// all the bytes in a explicit range (`Range::segment`), the last N bytes
    /// of the object (`ReadRange::tail`)
    /// A negative `read_offset` value will be interpreted as the number of bytes
    /// back from the end of the object to be returned.
    ///
    /// # Examples
    ///
    /// Read starting at 100 bytes to end of file.
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::model_ext::ReadRange;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::ReadObjectResponse;
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
    /// use google_cloud_storage::ReadObjectResponse;
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
    /// use google_cloud_storage::ReadObjectResponse;
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
    /// use google_cloud_storage::ReadObjectResponse;
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
    /// use google_cloud_storage::ReadObjectResponse;
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
    /// use google_cloud_storage::ReadObjectResponse;
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
    /// use google_cloud_storage::ReadObjectResponse;
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
    /// use google_cloud_storage::ReadObjectResponse;
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

    /// Sends the request.
    pub async fn send(self) -> Result<impl ReadObjectResponse> {
        let read = self.clone().read().await?;
        ReadObjectResponseImpl::new(self, read)
    }

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
            // read_limit is zero, means no limit. Read from offset to end of file.
            // This handles cases like (5, 0) -> "bytes=5-"
            (o, 0) => builder.header("range", format!("bytes={o}-")),
            // General case: non-negative offset and positive limit.
            // This covers cases like (0, 100) -> "bytes=0-99", (5, 100) -> "bytes=5-104"
            (o, l) => builder.header("range", format!("bytes={o}-{}", o + l - 1)),
        };

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

fn headers_to_md5_hash(headers: &http::HeaderMap) -> Vec<u8> {
    headers
        .get("x-goog-hash")
        .and_then(|hash| hash.to_str().ok())
        .and_then(|hash| hash.split(",").find(|v| v.starts_with("md5")))
        .and_then(|hash| {
            let hash = hash.trim_start_matches("md5=");
            base64::prelude::BASE64_STANDARD.decode(hash).ok()
        })
        .unwrap_or_default()
}

/// A response to a [Storage::read_object] request.
#[derive(Debug)]
struct ReadObjectResponseImpl<C> {
    inner: Option<reqwest::Response>,
    highlights: ObjectHighlights,
    // Fields for tracking the crc checksum checks.
    response_checksums: ObjectChecksums,
    // Fields for resuming a read request.
    range: ReadRange,
    generation: i64,
    builder: ReadObject<C>,
    resume_count: u32,
}

impl<C> ReadObjectResponseImpl<C>
where
    C: ChecksumEngine + Clone + Send,
{
    fn new(builder: ReadObject<C>, inner: reqwest::Response) -> Result<Self> {
        let full = builder.request.read_offset == 0 && builder.request.read_limit == 0;
        let response_checksums = checksums_from_response(full, inner.status(), inner.headers());
        let range = response_range(&inner).map_err(Error::deser)?;
        let generation = response_generation(&inner).map_err(Error::deser)?;

        let headers = inner.headers();
        let get_as_i64 = |header_name: &str| -> i64 {
            headers
                .get(header_name)
                .and_then(|s| s.to_str().ok())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or_default()
        };
        let get_as_string = |header_name: &str| -> String {
            headers
                .get(header_name)
                .and_then(|sc| sc.to_str().ok())
                .map(|sc| sc.to_string())
                .unwrap_or_default()
        };
        let highlights = ObjectHighlights {
            generation,
            metageneration: get_as_i64("x-goog-metageneration"),
            size: get_as_i64("x-goog-stored-content-length"),
            content_encoding: get_as_string("x-goog-stored-content-encoding"),
            storage_class: get_as_string("x-goog-storage-class"),
            content_type: get_as_string("content-type"),
            content_language: get_as_string("content-language"),
            content_disposition: get_as_string("content-disposition"),
            etag: get_as_string("etag"),
            checksums: headers.get("x-goog-hash").map(|_| {
                crate::model::ObjectChecksums::new()
                    .set_or_clear_crc32c(headers_to_crc32c(headers))
                    .set_md5_hash(headers_to_md5_hash(headers))
            }),
        };

        Ok(Self {
            inner: Some(inner),
            highlights,
            // Fields for computing checksums.
            response_checksums,
            // Fields for resuming a read request.
            range,
            generation,
            builder,
            resume_count: 0,
        })
    }
}

impl<C> ReadObjectResponse for ReadObjectResponseImpl<C>
where
    C: ChecksumEngine + Clone + Send + Sync + 'static,
{
    fn object(&self) -> ObjectHighlights {
        self.highlights.clone()
    }

    // A type-checking cycle is detected with `async fn` when its return type
    // depends on an opaque type that is defined within the function body.
    // Writing out `impl Future` breaks this cycle, allowing the compiler to
    // resolve the return type and proceed.
    #[allow(clippy::manual_async_fn)]
    fn next(&mut self) -> impl Future<Output = Option<Result<bytes::Bytes>>> + Send {
        async move {
            match self.next_attempt().await {
                None => None,
                Some(Ok(b)) => Some(Ok(b)),
                // Recursive async requires pin:
                //     https://rust-lang.github.io/async-book/07_workarounds/04_recursion.html
                Some(Err(e)) => Box::pin(self.resume(e)).await,
            }
        }
    }

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    fn into_stream(self) -> impl Stream<Item = Result<bytes::Bytes>> + Unpin {
        use futures::stream::unfold;
        Box::pin(unfold(Some(self), move |state| async move {
            if let Some(mut this) = state {
                if let Some(chunk) = this.next().await {
                    return Some((chunk, Some(this)));
                }
            };
            None
        }))
    }
}

impl<C> ReadObjectResponseImpl<C>
where
    C: ChecksumEngine + Clone + Send + Sync + 'static,
{
    async fn next_attempt(&mut self) -> Option<Result<bytes::Bytes>> {
        let inner = self.inner.as_mut()?;
        let res = inner.chunk().await.map_err(Error::io);
        match res {
            Ok(Some(chunk)) => {
                self.builder.checksum.update(self.range.start, &chunk);
                let len = chunk.len() as u64;
                if self.range.limit < len {
                    return Some(Err(Error::deser(ReadError::LongRead {
                        expected: self.range.limit,
                        got: len,
                    })));
                }
                self.range.limit -= len;
                self.range.start += len;
                Some(Ok(chunk))
            }
            Ok(None) => {
                if self.range.limit != 0 {
                    return Some(Err(Error::io(ReadError::ShortRead(self.range.limit))));
                }
                let computed = self.builder.checksum.finalize();
                let res = validate(&self.response_checksums, &Some(computed));
                match res {
                    Err(e) => Some(Err(Error::deser(ReadError::ChecksumMismatch(e)))),
                    Ok(()) => None,
                }
            }
            Err(e) => Some(Err(e)),
        }
    }

    async fn resume(&mut self, error: Error) -> Option<Result<bytes::Bytes>> {
        use crate::read_resume_policy::{ResumeQuery, ResumeResult};

        // The existing read is no longer valid.
        self.inner = None;
        self.resume_count += 1;
        let query = ResumeQuery::new(self.resume_count);
        match self
            .builder
            .options
            .read_resume_policy
            .on_error(&query, error)
        {
            ResumeResult::Continue(_) => {}
            ResumeResult::Permanent(e) => return Some(Err(e)),
            ResumeResult::Exhausted(e) => return Some(Err(e)),
        };
        self.builder.request.read_offset = self.range.start as i64;
        self.builder.request.read_limit = self.range.limit as i64;
        self.builder.request.generation = self.generation;
        self.inner = match self.builder.clone().read().await {
            Ok(r) => Some(r),
            Err(e) => return Some(Err(e)),
        };
        self.next().await
    }
}

mod sealed {
    pub trait ReadObjectResponse {}
}

impl<T> sealed::ReadObjectResponse for T where T: ReadObjectResponse {}

pub trait ReadObjectResponse: sealed::ReadObjectResponse + std::fmt::Debug {
    /// Get the highlights of the object metadata included in the
    /// response.
    ///
    /// To get full metadata about this object, use [crate::client::StorageControl::get_object].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::ReadObjectResponse;
    /// let object = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?
    ///     .object();
    /// println!("object generation={}", object.generation);
    /// println!("object metageneration={}", object.metageneration);
    /// println!("object size={}", object.size);
    /// println!("object content encoding={}", object.content_encoding);
    /// # Ok(()) }
    /// ```
    fn object(&self) -> ObjectHighlights;

    /// Stream the next bytes of the object.
    ///
    /// When the response has been exhausted, this will return None.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::ReadObjectResponse;
    /// let mut resp = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?;
    /// while let Some(next) = resp.next().await {
    ///     println!("next={:?}", next?);
    /// }
    /// # Ok(()) }
    /// ```
    fn next(&mut self) -> impl Future<Output = Option<Result<bytes::Bytes>>> + Send;

    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    /// Convert the response to a [Stream].
    fn into_stream(self) -> impl Stream<Item = Result<bytes::Bytes>> + Unpin;
}

/// Returns the object checksums to validate against.
///
/// For some responses, the checksums are not expected to match the data.
/// The function returns an empty `ObjectChecksums` in such a case.
///
/// Checksum validation is supported iff:
/// 1. We requested the full content.
/// 2. We got all the content (status != PartialContent).
/// 3. The server sent a CRC header.
/// 4. The http stack did not uncompress the file.
/// 5. We were not served compressed data that was uncompressed on read.
///
/// For 4, we turn off automatic decompression in reqwest::Client when we
/// create it,
fn checksums_from_response(
    full_content_requested: bool,
    status: http::StatusCode,
    headers: &http::HeaderMap,
) -> ObjectChecksums {
    let checksums = ObjectChecksums::new();
    if !full_content_requested || status == http::StatusCode::PARTIAL_CONTENT {
        return checksums;
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
        return checksums;
    }
    checksums
        .set_or_clear_crc32c(headers_to_crc32c(headers))
        .set_md5_hash(headers_to_md5_hash(headers))
}

fn response_range(response: &reqwest::Response) -> std::result::Result<ReadRange, ReadError> {
    match response.status() {
        reqwest::StatusCode::OK => {
            let header = required_header(response, "content-length")?;
            let limit = header
                .parse::<u64>()
                .map_err(|e| ReadError::BadHeaderFormat("content-length", e.into()))?;
            Ok(ReadRange { start: 0, limit })
        }
        reqwest::StatusCode::PARTIAL_CONTENT => {
            let header = required_header(response, "content-range")?;
            let header = header.strip_prefix("bytes ").ok_or_else(|| {
                ReadError::BadHeaderFormat("content-range", "missing bytes prefix".into())
            })?;
            let (range, _) = header.split_once('/').ok_or_else(|| {
                ReadError::BadHeaderFormat("content-range", "missing / separator".into())
            })?;
            let (start, end) = range.split_once('-').ok_or_else(|| {
                ReadError::BadHeaderFormat("content-range", "missing - separator".into())
            })?;
            let start = start
                .parse::<u64>()
                .map_err(|e| ReadError::BadHeaderFormat("content-range", e.into()))?;
            let end = end
                .parse::<u64>()
                .map_err(|e| ReadError::BadHeaderFormat("content-range", e.into()))?;
            // HTTP ranges are inclusive, we need to compute the number of bytes
            // in the range:
            let end = end + 1;
            let limit = end
                .checked_sub(start)
                .ok_or_else(|| ReadError::BadHeaderFormat("content-range", format!("range start ({start}) should be less than or equal to the range end ({end})").into()))?;
            Ok(ReadRange { start, limit })
        }
        s => Err(ReadError::UnexpectedSuccessCode(s.as_u16())),
    }
}

fn response_generation(response: &reqwest::Response) -> std::result::Result<i64, ReadError> {
    let header = required_header(response, "x-goog-generation")?;
    header
        .parse::<i64>()
        .map_err(|e| ReadError::BadHeaderFormat("x-goog-generation", e.into()))
}

fn required_header<'a>(
    response: &'a reqwest::Response,
    name: &'static str,
) -> std::result::Result<&'a str, ReadError> {
    let header = response
        .headers()
        .get(name)
        .ok_or_else(|| ReadError::MissingHeader(name))?;
    header
        .to_str()
        .map_err(|e| ReadError::BadHeaderFormat(name, e.into()))
}

#[derive(Debug, PartialEq)]
struct ReadRange {
    start: u64,
    limit: u64,
}

#[cfg(test)]
mod resume_tests;

#[cfg(test)]
mod tests {
    use super::client::tests::{test_builder, test_inner_client};
    use super::*;
    use crate::error::ChecksumMismatch;
    use crate::model_ext::{KeyAes256, ReadRange, tests::create_key_helper};
    use futures::TryStreamExt;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use std::collections::HashMap;
    use std::error::Error;
    use test_case::test_case;

    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    // Verify `read_object()` meets normal Send, Sync, requirements.
    #[tokio::test]
    async fn test_read_is_send_and_static() -> Result {
        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
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
            .with_credentials(auth::credentials::testing::test_credentials())
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
    async fn read_object_metadata() -> Result {
        const CONTENTS: &str = "the quick brown fox jumps over the lazy dog";
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "//storage/v1/b/test-bucket/o/test-object"),
                request::query(url_decoded(contains(("alt", "media")))),
            ])
            .respond_with(
                status_code(200)
                    .body(CONTENTS)
                    .append_header(
                        "x-goog-hash",
                        "crc32c=PBj01g==,md5=d63R1fQSI9VYL8pzalyzNQ==",
                    )
                    .append_header("x-goog-generation", 500)
                    .append_header("x-goog-metageneration", "1")
                    .append_header("x-goog-stored-content-length", 30)
                    .append_header("x-goog-stored-content-encoding", "identity")
                    .append_header("x-goog-storage-class", "STANDARD")
                    .append_header("content-language", "en")
                    .append_header("content-type", "text/plain")
                    .append_header("content-disposition", "inline")
                    .append_header("etag", "etagval"),
            ),
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
        let object = reader.object();
        assert_eq!(object.generation, 500);
        assert_eq!(object.metageneration, 1);
        assert_eq!(object.size, 30);
        assert_eq!(object.content_encoding, "identity");
        assert_eq!(
            object.checksums.as_ref().unwrap().crc32c.unwrap(),
            crc32c::crc32c(CONTENTS.as_bytes())
        );
        assert_eq!(
            object.checksums.as_ref().unwrap().md5_hash,
            base64::prelude::BASE64_STANDARD.decode("d63R1fQSI9VYL8pzalyzNQ==")?
        );

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
            .with_credentials(auth::credentials::testing::test_credentials())
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
            .with_credentials(auth::credentials::testing::test_credentials())
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
            .with_credentials(auth::credentials::testing::test_credentials())
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
        let inner = test_inner_client(
            test_builder().with_credentials(auth::credentials::testing::error_credentials(false)),
        );
        let _ = ReadObject::new(inner, "projects/_/buckets/bucket", "object")
            .http_request_builder()
            .await
            .inspect_err(|e| assert!(e.is_authentication()))
            .expect_err("invalid credentials should err");
        Ok(())
    }

    #[tokio::test]
    async fn read_object_bad_bucket() -> Result {
        let inner = test_inner_client(test_builder());
        ReadObject::new(inner, "malformed", "object")
            .http_request_builder()
            .await
            .expect_err("malformed bucket string should error");
        Ok(())
    }

    #[tokio::test]
    async fn read_object_query_params() -> Result {
        let inner = test_inner_client(test_builder());
        let request = ReadObject::new(inner, "projects/_/buckets/bucket", "object")
            .set_generation(5)
            .set_if_generation_match(10)
            .set_if_generation_not_match(20)
            .set_if_metageneration_match(30)
            .set_if_metageneration_not_match(40)
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
        let inner = test_inner_client(test_builder());
        let request = ReadObject::new(inner, "projects/_/buckets/bucket", "object")
            .set_key(KeyAes256::new(&key)?)
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

    #[test_case(ReadRange::all(), None; "no headers needed")]
    #[test_case(ReadRange::offset(10), Some(&http::HeaderValue::from_static("bytes=10-")); "offset only")]
    #[test_case(ReadRange::tail(2000), Some(&http::HeaderValue::from_static("bytes=-2000-")); "negative offset")]
    #[test_case(ReadRange::segment(0, 100), Some(&http::HeaderValue::from_static("bytes=0-99")); "limit only")]
    #[test_case(ReadRange::segment(1000, 100), Some(&http::HeaderValue::from_static("bytes=1000-1099")); "offset and limit")]
    #[tokio::test]
    async fn range_header(input: ReadRange, want: Option<&http::HeaderValue>) -> Result {
        let inner = test_inner_client(test_builder());
        let request = ReadObject::new(inner, "projects/_/buckets/bucket", "object")
            .set_read_range(input.clone())
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
        let request = ReadObject::new(inner, "projects/_/buckets/bucket", name)
            .http_request_builder()
            .await?
            .build()?;
        let got = request.url().path_segments().unwrap().next_back().unwrap();
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn document_crc32c_values() {
        let bytes = (1234567890_u32).to_be_bytes();
        let base64 = base64::prelude::BASE64_STANDARD.encode(bytes);
        assert_eq!(base64, "SZYC0g==", "{bytes:?}");
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

    #[test_case("", None; "no header")]
    #[test_case("md5=invalid", None; "invalid value")]
    #[test_case("md5=AAAAAAAAAAAAAAAAAA==",Some("AAAAAAAAAAAAAAAAAA=="); "zero value")]
    #[test_case("md5=d63R1fQSI9VYL8pzalyzNQ==", Some("d63R1fQSI9VYL8pzalyzNQ=="); "value")]
    #[test_case("crc32c=something,md5=d63R1fQSI9VYL8pzalyzNQ==", Some("d63R1fQSI9VYL8pzalyzNQ=="); "md5 after crc32c")]
    #[test_case("md5=d63R1fQSI9VYL8pzalyzNQ==,crc32c=something", Some("d63R1fQSI9VYL8pzalyzNQ=="); "md5 before crc32c")]
    fn test_headers_to_md5(val: &str, want: Option<&str>) -> Result {
        let mut headers = http::HeaderMap::new();
        if !val.is_empty() {
            headers.insert("x-goog-hash", http::HeaderValue::from_str(val)?);
        }
        let got = headers_to_md5_hash(&headers);
        match want {
            Some(w) => assert_eq!(got, base64::prelude::BASE64_STANDARD.decode(w)?),
            None => assert!(got.is_empty()),
        }
        Ok(())
    }

    #[test_case(false, vec![("x-goog-hash", "crc32c=SZYC0g==,md5=d63R1fQSI9VYL8pzalyzNQ==")], http::StatusCode::OK, None, ""; "full content not requested")]
    #[test_case(true, vec![], http::StatusCode::PARTIAL_CONTENT, None, ""; "No x-goog-hash")]
    #[test_case(true, vec![("x-goog-hash", "crc32c=SZYC0g==,md5=d63R1fQSI9VYL8pzalyzNQ=="), ("x-goog-stored-content-encoding", "gzip"), ("content-encoding", "json")], http::StatusCode::OK, None, ""; "server uncompressed")]
    #[test_case(true, vec![("x-goog-hash", "crc32c=SZYC0g==,md5=d63R1fQSI9VYL8pzalyzNQ=="), ("x-goog-stored-content-encoding", "gzip"), ("content-encoding", "gzip")], http::StatusCode::OK, Some(1234567890_u32), "d63R1fQSI9VYL8pzalyzNQ=="; "both gzip")]
    #[test_case(true, vec![("x-goog-hash", "crc32c=SZYC0g==,md5=d63R1fQSI9VYL8pzalyzNQ==")], http::StatusCode::OK, Some(1234567890_u32), "d63R1fQSI9VYL8pzalyzNQ=="; "all ok")]
    fn test_checksums_validation_enabled(
        full_content_requested: bool,
        headers: Vec<(&str, &str)>,
        status: http::StatusCode,
        want_crc32c: Option<u32>,
        want_md5: &str,
    ) -> Result {
        let mut header_map = http::HeaderMap::new();
        for (key, value) in headers {
            header_map.insert(
                http::HeaderName::from_bytes(key.as_bytes())?,
                http::HeaderValue::from_bytes(value.as_bytes())?,
            );
        }

        let got = checksums_from_response(full_content_requested, status, &header_map);
        assert_eq!(got.crc32c, want_crc32c);
        assert_eq!(
            got.md5_hash,
            base64::prelude::BASE64_STANDARD.decode(want_md5)?
        );
        Ok(())
    }

    #[test_case(0)]
    #[test_case(1024)]
    fn response_range_success(limit: u64) -> Result {
        let response = http::Response::builder()
            .status(200)
            .header("content-length", limit)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let range = response_range(&response)?;
        assert_eq!(range, super::ReadRange { start: 0, limit });
        Ok(())
    }

    #[test]
    fn response_range_missing() -> Result {
        let response = http::Response::builder().status(200).body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err = response_range(&response).expect_err("missing header should result in an error");
        assert!(
            matches!(err, ReadError::MissingHeader(h) if h == "content-length"),
            "{err:?}"
        );
        Ok(())
    }

    #[test_case("")]
    #[test_case("abc")]
    #[test_case("-123")]
    fn response_range_format(value: &'static str) -> Result {
        let response = http::Response::builder()
            .status(200)
            .header("content-length", value)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err = response_range(&response).expect_err("header value should result in an error");
        assert!(
            matches!(err, ReadError::BadHeaderFormat(h, _) if h == "content-length"),
            "{err:?}"
        );
        assert!(err.source().is_some(), "{err:?}");
        Ok(())
    }

    #[test_case(0, 123)]
    #[test_case(123, 456)]
    fn response_range_partial_success(start: u64, end: u64) -> Result {
        let response = http::Response::builder()
            .status(206)
            .header(
                "content-range",
                format!("bytes {}-{}/{}", start, end, end + 1),
            )
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let range = response_range(&response)?;
        assert_eq!(
            range,
            super::ReadRange {
                start,
                limit: (end + 1 - start)
            }
        );
        Ok(())
    }

    #[test]
    fn response_range_partial_missing() -> Result {
        let response = http::Response::builder().status(206).body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err = response_range(&response).expect_err("missing header should result in an error");
        assert!(
            matches!(err, ReadError::MissingHeader(h) if h == "content-range"),
            "{err:?}"
        );
        Ok(())
    }

    #[test_case("")]
    #[test_case("123-456/457"; "bad prefix")]
    #[test_case("bytes 123-456 457"; "bad separator")]
    #[test_case("bytes 123+456/457"; "bad separator [2]")]
    #[test_case("bytes abc-456/457"; "start is not numbers")]
    #[test_case("bytes 123-cde/457"; "end is not numbers")]
    #[test_case("bytes 123-0/457"; "invalid range")]
    fn response_range_partial_format(value: &'static str) -> Result {
        let response = http::Response::builder()
            .status(206)
            .header("content-range", value)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err = response_range(&response).expect_err("header value should result in an error");
        assert!(
            matches!(err, ReadError::BadHeaderFormat(h, _) if h == "content-range"),
            "{err:?}"
        );
        assert!(err.source().is_some(), "{err:?}");
        Ok(())
    }

    #[test]
    fn response_range_bad_response() -> Result {
        let code = reqwest::StatusCode::CREATED;
        let response = http::Response::builder().status(code).body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err = response_range(&response).expect_err("unexpected status creates error");
        assert!(
            matches!(err, ReadError::UnexpectedSuccessCode(c) if c == code),
            "{err:?}"
        );
        Ok(())
    }

    #[test_case(0)]
    #[test_case(1024)]
    fn response_generation_success(value: i64) -> Result {
        let response = http::Response::builder()
            .status(200)
            .header("x-goog-generation", value)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let got = response_generation(&response)?;
        assert_eq!(got, value);
        Ok(())
    }

    #[test]
    fn response_generation_missing() -> Result {
        let response = http::Response::builder().status(200).body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err =
            response_generation(&response).expect_err("missing header should result in an error");
        assert!(
            matches!(err, ReadError::MissingHeader(h) if h == "x-goog-generation"),
            "{err:?}"
        );
        Ok(())
    }

    #[test_case("")]
    #[test_case("abc")]
    fn response_generation_format(value: &'static str) -> Result {
        let response = http::Response::builder()
            .status(200)
            .header("x-goog-generation", value)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err =
            response_generation(&response).expect_err("header value should result in an error");
        assert!(
            matches!(err, ReadError::BadHeaderFormat(h, _) if h == "x-goog-generation"),
            "{err:?}"
        );
        assert!(err.source().is_some(), "{err:?}");
        Ok(())
    }

    #[test]
    fn required_header_not_str() -> Result {
        let name = "x-goog-test";
        let response = http::Response::builder()
            .status(200)
            .header(name, http::HeaderValue::from_bytes(b"invalid\xfa")?)
            .body(Vec::new())?;
        let response = reqwest::Response::from(response);
        let err =
            required_header(&response, name).expect_err("header value should result in an error");
        assert!(
            matches!(err, ReadError::BadHeaderFormat(h, _) if h == name),
            "{err:?}"
        );
        assert!(err.source().is_some(), "{err:?}");
        Ok(())
    }
}
