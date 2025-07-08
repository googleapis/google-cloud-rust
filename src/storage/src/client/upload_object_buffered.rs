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
use futures::stream::unfold;
use std::collections::VecDeque;

/// A request builder for uploads without rewind.
pub struct UploadObjectBuffered<T> {
    inner: std::sync::Arc<StorageInner>,
    resource: control::model::Object,
    spec: control::model::WriteObjectSpec,
    params: Option<control::model::CommonObjectRequestParams>,
    payload: InsertPayload<T>,
}

impl<T> UploadObjectBuffered<T> {
    /// Set a [request precondition] on the object generation to match.
    ///
    /// With this precondition the request fails if the current object
    /// generation matches the provided value. A common value is `0`, which
    /// prevents uploads from succeeding if the object already exists.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_if_generation_match(0)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn with_if_generation_match<V>(mut self, v: V) -> Self
    where
        V: Into<i64>,
    {
        self.spec.if_generation_match = Some(v.into());
        self
    }

    /// Set a [request precondition] on the object generation to match.
    ///
    /// With this precondition the request fails if the current object
    /// generation does not match the provided value.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_if_generation_not_match(0)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn with_if_generation_not_match<V>(mut self, v: V) -> Self
    where
        V: Into<i64>,
    {
        self.spec.if_generation_not_match = Some(v.into());
        self
    }

    /// Set a [request precondition] on the object meta generation.
    ///
    /// With this precondition the request fails if the current object metadata
    /// generation does not match the provided value. This may be useful to
    /// prevent changes when the metageneration is known.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_if_metageneration_match(1234)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn with_if_metageneration_match<V>(mut self, v: V) -> Self
    where
        V: Into<i64>,
    {
        self.spec.if_metageneration_match = Some(v.into());
        self
    }

    /// Set a [request precondition] on the object meta-generation.
    ///
    /// With this precondition the request fails if the current object metadata
    /// generation matches the provided value. This is rarely useful in uploads,
    /// it is more commonly used on downloads to prevent downloads if the value
    /// is already cached.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_if_metageneration_not_match(1234)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn with_if_metageneration_not_match<V>(mut self, v: V) -> Self
    where
        V: Into<i64>,
    {
        self.spec.if_metageneration_not_match = Some(v.into());
        self
    }

    /// Sets the ACL for the new object.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # use control::model::ObjectAccessControl;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_acl([ObjectAccessControl::new().set_entity("allAuthenticatedUsers").set_role("READER")])
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub fn with_acl<I, V>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<control::model::ObjectAccessControl>,
    {
        self.resource.acl = v.into_iter().map(|a| a.into()).collect();
        self
    }

    /// Sets the [cache control] for the new object.
    ///
    /// This can be used to control caching in [public objects].
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_cache_control("public; max-age=7200")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [public objects]: https://cloud.google.com/storage/docs/access-control/making-data-public
    /// [cache control]: https://datatracker.ietf.org/doc/html/rfc7234#section-5.2
    pub fn with_cache_control<V: Into<String>>(mut self, v: V) -> Self {
        self.resource.cache_control = v.into();
        self
    }

    /// Sets the [content disposition] for the new object.
    ///
    /// Google Cloud Storage can serve content directly to web browsers. This
    /// attribute sets the `Content-Disposition` header, which may change how
    /// the browser displays the contents.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_content_disposition("inline")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [content disposition]: https://datatracker.ietf.org/doc/html/rfc6266
    pub fn with_content_disposition<V: Into<String>>(mut self, v: V) -> Self {
        self.resource.content_disposition = v.into();
        self
    }

    /// Sets the [content encoding] for the object data.
    ///
    /// This can be used to upload compressed data and enable [transcoding] of
    /// the data during downloads.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// use flate2::write::GzEncoder;
    /// use std::io::Write;
    /// let mut e = GzEncoder::new(Vec::new(), flate2::Compression::default());
    /// e.write_all(b"hello world");
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", bytes::Bytes::from_owner(e.finish()?))
    ///     .with_content_encoding("gzip")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [transcoding]: https://cloud.google.com/storage/docs/transcoding
    /// [content encoding]: https://datatracker.ietf.org/doc/html/rfc7231#section-3.1.2.2
    pub fn with_content_encoding<V: Into<String>>(mut self, v: V) -> Self {
        self.resource.content_encoding = v.into();
        self
    }

    /// Sets the [content language] for the new object.
    ///
    /// Google Cloud Storage can serve content directly to web browsers. This
    /// attribute sets the `Content-Language` header, which may change how the
    /// browser displays the contents.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_content_language("en")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [content language]: https://cloud.google.com/storage/docs/metadata#content-language
    pub fn with_content_language<V: Into<String>>(mut self, v: V) -> Self {
        self.resource.content_language = v.into();
        self
    }

    /// Sets the [content type] for the new object.
    ///
    /// Google Cloud Storage can serve content directly to web browsers. This
    /// attribute sets the `Content-Type` header, which may change how the
    /// browser interprets the contents.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_content_type("text/plain")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [content type]: https://datatracker.ietf.org/doc/html/rfc7231#section-3.1.1.5
    pub fn with_content_type<V: Into<String>>(mut self, v: V) -> Self {
        self.resource.content_type = v.into();
        self
    }

    /// Sets the [custom time] for the new object.
    ///
    /// This field is typically set in order to use the [DaysSinceCustomTime]
    /// condition in Object Lifecycle Management.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let time = wkt::Timestamp::try_from("2025-07-07T18:30:00Z")?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_custom_time(time)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [DaysSinceCustomTime]: https://cloud.google.com/storage/docs/lifecycle#dayssincecustomtime
    /// [custom time]: https://cloud.google.com/storage/docs/metadata#custom-time
    pub fn with_custom_time<V: Into<wkt::Timestamp>>(mut self, v: V) -> Self {
        self.resource.custom_time = Some(v.into());
        self
    }

    /// Sets the [event based hold] flag for the new object.
    ///
    /// This field is typically set in order to prevent objects from being
    /// deleted or modified.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_event_based_hold(true)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [event based hold]: https://cloud.google.com/storage/docs/object-holds
    pub fn with_event_based_hold<V: Into<bool>>(mut self, v: V) -> Self {
        self.resource.event_based_hold = Some(v.into());
        self
    }

    /// Sets the [custom metadata] for the new object.
    ///
    /// This field is typically set to annotate the object with
    /// application-specific metadata.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let time = wkt::Timestamp::try_from("2025-07-07T18:30:00Z")?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_metadata([("test-only", "true"), ("environment", "qa")])
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [custom metadata]: https://cloud.google.com/storage/docs/metadata#custom-metadata
    pub fn with_metadata<I, K, V>(mut self, i: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.resource.metadata = i.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        self
    }

    /// Sets the [retention configuration] for the new object.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use control::model::object::{Retention, retention};
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_retention(
    ///         Retention::new()
    ///             .set_mode(retention::Mode::Locked)
    ///             .set_retain_until_time(wkt::Timestamp::try_from("2035-01-01T00:00:00Z")?))
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [retention configuration]: https://cloud.google.com/storage/docs/metadata#retention-config
    pub fn with_retention<V>(mut self, v: V) -> Self
    where
        V: Into<control::model::object::Retention>,
    {
        self.resource.retention = Some(v.into());
        self
    }

    /// Sets the [storage class] for the new object.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_storage_class("ARCHIVE")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [storage class]: https://cloud.google.com/storage/docs/storage-classes
    pub fn with_storage_class<V>(mut self, v: V) -> Self
    where
        V: Into<String>,
    {
        self.resource.storage_class = v.into();
        self
    }

    /// Sets the [temporary hold] flag for the new object.
    ///
    /// This field is typically set in order to prevent objects from being
    /// deleted or modified.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let time = wkt::Timestamp::try_from("2025-07-07T18:30:00Z")?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_temporary_hold(true)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [temporary hold]: https://cloud.google.com/storage/docs/object-holds
    pub fn with_temporary_hold<V: Into<bool>>(mut self, v: V) -> Self {
        self.resource.temporary_hold = v.into();
        self
    }

    /// Sets the resource name of the [Customer-managed encryption key] for this
    /// object.
    ///
    /// The service imposes a number of restrictions on the keys used to encrypt
    /// Google Cloud Storage objects. Read the documentation in full before
    /// trying to use customer-managed encryption keys. In particular, verify
    /// the service has the necessary permissions, and the key is in a
    /// compatible location.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_kms_key("projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [Customer-managed encryption key]: https://cloud.google.com/storage/docs/encryption/customer-managed-keys
    pub fn with_kms_key<V>(mut self, v: V) -> Self
    where
        V: Into<String>,
    {
        self.resource.kms_key = v.into();
        self
    }

    /// Configure this object to use one of the [predefined ACLs].
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_predefined_acl("private")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// [predefined ACLs]: https://cloud.google.com/storage/docs/access-control/lists#predefined-acl
    pub fn with_predefined_acl<V>(mut self, v: V) -> Self
    where
        V: Into<String>,
    {
        self.spec.predefined_acl = v.into();
        self
    }

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::client::KeyAes256;
    /// # let client = Storage::builder().build().await?;
    /// let key: &[u8] = &[97; 32];
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_key(KeyAes256::new(key)?)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub fn with_key(mut self, v: KeyAes256) -> Self {
        self.params = Some(v.into());
        self
    }

    // TODO(#2050) - this should be automatically computed?
    #[allow(dead_code)]
    fn with_crc32c<V>(mut self, v: V) -> Self
    where
        V: Into<u32>,
    {
        let mut checksum = self.resource.checksums.take().unwrap_or_default();
        checksum.crc32c = Some(v.into());
        self.resource.checksums = Some(checksum);
        self
    }

    // TODO(#2050) - this should be automatically computed?
    #[allow(dead_code)]
    fn with_md5_hash<I, V>(mut self, i: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<u8>,
    {
        let mut checksum = self.resource.checksums.take().unwrap_or_default();
        checksum.md5_hash = i.into_iter().map(|v| v.into()).collect();
        // TODO(#2050) - should we return an error (or panic?) if the size is wrong?
        self.resource.checksums = Some(checksum);
        self
    }

    pub(crate) fn new<B, O, P>(
        inner: std::sync::Arc<StorageInner>,
        bucket: B,
        object: O,
        payload: P,
    ) -> Self
    where
        B: Into<String>,
        O: Into<String>,
        P: Into<InsertPayload<T>>,
    {
        UploadObjectBuffered {
            inner,
            resource: control::model::Object::new()
                .set_bucket(bucket)
                .set_name(object),
            spec: control::model::WriteObjectSpec::new(),
            params: None,
            payload: payload.into(),
        }
    }
}

impl<T> UploadObjectBuffered<T>
where
    T: StreamingSource + Send + Sync + 'static,
    T::Error: std::error::Error + Send + Sync + 'static,
{
    /// Upload an object from a streaming source without rewinds.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .upload_object_buffered("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub async fn send(self) -> crate::Result<Object> {
        let upload_url = self.start_resumable_upload().await?;
        // TODO(#2043) - make this configurable
        if self.payload.size_hint().0 > RESUMABLE_UPLOAD_QUANTUM as u64 {
            return self
                .upload_by_chunks(&upload_url, RESUMABLE_UPLOAD_QUANTUM)
                .await;
        }
        let builder = self.upload_request(upload_url).await?;
        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        let response = response.json::<v1::Object>().await.map_err(Error::io)?;

        Ok(Object::from(response))
    }

    async fn start_resumable_upload(&self) -> Result<String> {
        let builder = self.start_resumable_upload_request().await?;
        let response = builder.send().await.map_err(Error::io)?;
        self::handle_start_resumable_upload_response(response).await
    }

    async fn start_resumable_upload_request(&self) -> Result<reqwest::RequestBuilder> {
        let bucket = &self.resource.bucket;
        let bucket_id = bucket.strip_prefix("projects/_/buckets/").ok_or_else(|| {
            Error::binding(format!(
                "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
            ))
        })?;
        let object = &self.resource.name;
        let builder = self
            .inner
            .client
            .request(
                reqwest::Method::POST,
                format!("{}/upload/storage/v1/b/{bucket_id}/o", &self.inner.endpoint),
            )
            .query(&[("uploadType", "resumable")])
            .query(&[("name", enc(object))])
            .header("content-type", "application/json")
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );

        let builder = self.apply_preconditions(builder);
        let builder = apply_customer_supplied_encryption_headers(builder, self.params.clone());
        let builder = self.inner.apply_auth_headers(builder).await?;
        let builder = builder.json(&v1::insert_body(&self.resource));
        Ok(builder)
    }

    async fn upload_by_chunks(mut self, upload_url: &str, target_size: usize) -> Result<Object> {
        let mut remainder = None;
        let mut offset = 0_usize;
        loop {
            let (chunk, r) = self::next_chunk(&mut self.payload, remainder, target_size).await?;
            let (builder, chunk_size) = self
                .partial_upload_request(upload_url, offset, chunk, target_size)
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
        target_size: usize,
    ) -> Result<(reqwest::RequestBuilder, usize)> {
        let chunk_size = chunk.iter().fold(0, |s, b| s + b.len());
        let range = match chunk_size {
            0 => format!("bytes */{offset}"),
            n if n == target_size => format!("bytes {}-{}/*", offset, offset + n - 1),
            n => format!("bytes {}-{}/{}", offset, offset + n - 1, offset + n),
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

        let builder = apply_customer_supplied_encryption_headers(builder, self.params.clone());
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

    async fn upload_request(mut self, upload_url: String) -> Result<reqwest::RequestBuilder> {
        let (chunk, target_size) = {
            let mut chunk = VecDeque::new();
            let mut size = 0_usize;
            while let Some(b) = self.payload.next().await.transpose().map_err(Error::io)? {
                size += b.len();
                chunk.push_back(b);
            }
            (chunk, size)
        };
        let target_size = target_size.div_ceil(RESUMABLE_UPLOAD_QUANTUM);
        let (builder, _size) = self
            .partial_upload_request(upload_url.as_str(), 0, chunk, target_size)
            .await?;
        Ok(builder)
    }

    fn apply_preconditions(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let builder = self
            .spec
            .if_generation_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifGenerationMatch", v)]));
        let builder = self
            .spec
            .if_generation_not_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifGenerationNotMatch", v)]));
        let builder = self
            .spec
            .if_metageneration_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifMetagenerationMatch", v)]));
        let builder = self
            .spec
            .if_metageneration_not_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifMetagenerationNotMatch", v)]));

        [
            ("kmsKeyName", self.resource.kms_key.as_str()),
            ("predefinedAcl", self.spec.predefined_acl.as_str()),
        ]
        .into_iter()
        .fold(
            builder,
            |b, (k, v)| if v.is_empty() { b } else { b.query(&[(k, v)]) },
        )
    }
}

async fn handle_start_resumable_upload_response(response: reqwest::Response) -> Result<String> {
    if !response.status().is_success() {
        return gaxi::http::to_http_error(response).await;
    }
    let location = response
        .headers()
        .get("Location")
        .ok_or_else(|| Error::deser("missing Location header in start resumable upload"))?;
    location.to_str().map_err(Error::deser).map(str::to_string)
}

async fn next_chunk<T>(
    payload: &mut InsertPayload<T>,
    remainder: Option<bytes::Bytes>,
    target_size: usize,
) -> Result<(VecDeque<bytes::Bytes>, Option<bytes::Bytes>)>
where
    T: StreamingSource,
{
    let mut partial = VecDeque::new();
    let mut size = 0;
    if let Some(mut b) = remainder {
        match b.len() {
            n if n > target_size => {
                let remainder = b.split_off(target_size);
                partial.push_back(b);
                return Ok((partial, Some(remainder)));
            }
            n if n == target_size => {
                partial.push_back(b);
                return Ok((partial, None));
            }
            _ => {
                size += b.len();
                partial.push_back(b);
            }
        }
    }

    while let Some(mut b) = payload.next().await.transpose().map_err(Error::io)? {
        match b.len() {
            n if size + n > target_size => {
                let remainder = b.split_off(target_size - size);
                partial.push_back(b);
                return Ok((partial, Some(remainder)));
            }
            n if size + n == target_size => {
                partial.push_back(b);
                return Ok((partial, None));
            }
            _ => {
                size += b.len();
                partial.push_back(b);
            }
        };
    }
    Ok((partial, None))
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
    let (persisted_size, chunk_remainder) = match (expected_offset, end) {
        (o, 0) => (0, o),
        (o, e) if o < e + 1 => panic!("more data persistent than sent {response:?}"),
        (o, e) => (e + 1, o - e - 1),
    };
    Ok(PartialUpload::Partial {
        persisted_size,
        chunk_remainder,
    })
}

fn parse_range_end(headers: &reqwest::header::HeaderMap) -> Option<usize> {
    let Some(range) = headers.get("range") else {
        // A missing `Range:` header indicates that no bytes are persisted.
        return Some(0_usize);
    };
    let end = std::str::from_utf8(range.as_bytes().strip_prefix(b"bytes=0-")?).ok()?;
    end.parse::<usize>().ok()
}

#[derive(Debug, PartialEq)]
enum PartialUpload {
    Finalized(Box<Object>),
    Partial {
        persisted_size: usize,
        chunk_remainder: usize,
    },
}

const RESUME_INCOMPLETE: reqwest::StatusCode = reqwest::StatusCode::PERMANENT_REDIRECT;
// Resumable uploads chunks (except for the last chunk) *must* be sized to a
// multiple of 256 KiB.
const RESUMABLE_UPLOAD_QUANTUM: usize = 256 * 1024;

#[cfg(test)]
mod tests {
    use super::super::tests::create_key_helper;
    use super::super::tests::test_inner_client;
    use super::*;
    use crate::upload_source::test::VecStream;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use test_case::test_case;

    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    const SESSION: &str = "https://private.googleapis.com/test-only-session-123";

    #[tokio::test]
    async fn upload_object_buffered_normal() -> Result {
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
                request::method_path("POST", "//upload/storage/v1/b/test-bucket/o"),
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

        let endpoint = server.url("");
        let client = Storage::builder()
            .with_endpoint(endpoint.to_string())
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let response = client
            .upload_object_buffered("projects/_/buckets/test-bucket", "test-object", "")
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
                request::method_path("POST", "//upload/storage/v1/b/test-bucket/o"),
                request::query(url_decoded(contains(("name", "test-object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
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
            .upload_object_buffered("projects/_/buckets/test-bucket", "test-object", "")
            .send()
            .await
            .expect_err("expected a not found error");
        assert_eq!(err.http_status_code(), Some(404), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn start_resumable_upload() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let mut request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "hello")
                .start_resumable_upload_request()
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::POST);
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

        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "hello")
                .with_key(KeyAes256::new(&key)?)
                .start_resumable_upload_request()
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::POST);
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
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        UploadObjectBuffered::new(inner, "malformed", "object", "hello")
            .start_resumable_upload_request()
            .await
            .expect_err("malformed bucket string should error");
        Ok(())
    }

    #[tokio::test]
    async fn upload_object_unbuffered_metadata() -> Result {
        use control::model::ObjectAccessControl;
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let mut request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "")
                .with_if_generation_match(10)
                .with_if_generation_not_match(20)
                .with_if_metageneration_match(30)
                .with_if_metageneration_not_match(40)
                .with_predefined_acl("private")
                .with_acl([ObjectAccessControl::new()
                    .set_entity("allAuthenticatedUsers")
                    .set_role("READER")])
                .with_cache_control("public; max-age=7200")
                .with_content_disposition("inline")
                .with_content_encoding("gzip")
                .with_content_language("en")
                .with_content_type("text/plain")
                .with_crc32c(crc32c::crc32c(b""))
                .with_custom_time(wkt::Timestamp::try_from("2025-07-07T18:11:00Z")?)
                .with_event_based_hold(true)
                .with_md5_hash(md5::compute(b"").0)
                .with_metadata([("k0", "v0"), ("k1", "v1")])
                .with_retention(
                    control::model::object::Retention::new()
                        .set_mode(control::model::object::retention::Mode::Locked)
                        .set_retain_until_time(wkt::Timestamp::try_from("2035-07-07T18:14:00Z")?),
                )
                .with_storage_class("ARCHIVE")
                .with_temporary_hold(true)
                .with_kms_key("test-key")
                .start_resumable_upload_request()
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::POST);
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
        let config = gaxi::options::ClientConfig {
            cred: Some(auth::credentials::testing::error_credentials(false)),
            ..Default::default()
        };
        let inner = test_inner_client(config);
        let _ = UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .start_resumable_upload_request()
            .await
            .inspect_err(|e| assert!(e.is_authentication()))
            .expect_err("invalid credentials should err");
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
        let request = UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", name, "hello")
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

    #[tokio::test]
    async fn upload_request() -> Result {
        use reqwest::header::HeaderValue;

        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let mut request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "hello")
                .upload_request(SESSION.to_string())
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::PUT);
        assert_eq!(request.url().as_str(), SESSION);
        assert_eq!(
            request.headers().get("content-range"),
            Some(&HeaderValue::from_static("bytes 0-4/5"))
        );
        let body = request.body_mut().take().unwrap();
        let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
        assert_eq!(contents, "hello");
        Ok(())
    }

    #[tokio::test]
    async fn upload_object_buffered_stream() -> Result {
        let stream = VecStream::new(
            [
                "the ", "quick ", "brown ", "fox ", "jumps ", "over ", "the ", "lazy ", "dog",
            ]
            .map(|x| bytes::Bytes::from_static(x.as_bytes()))
            .to_vec(),
        );
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let mut request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", stream)
                .upload_request(SESSION.to_string())
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::PUT);
        assert_eq!(request.url().as_str(), SESSION);
        let body = request.body_mut().take().unwrap();
        let contents = http_body_util::BodyExt::collect(body).await?.to_bytes();
        assert_eq!(contents, "the quick brown fox jumps over the lazy dog");
        Ok(())
    }

    #[tokio::test]
    async fn upload_request_headers() -> Result {
        // Make a 32-byte key.
        let (key, key_base64, _, key_sha256_base64) = create_key_helper();

        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "hello")
                .with_key(KeyAes256::new(&key)?)
                .upload_request(SESSION.to_string())
                .await?
                .build()?;

        assert_eq!(request.method(), reqwest::Method::PUT);
        assert_eq!(request.url().as_str(), SESSION);

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

        let stream = VecStream::new((0..5).map(|i| new_line(i, LEN)).collect::<Vec<_>>());

        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let upload =
            UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", stream);
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
        const LEN: usize = 32;
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let upload = UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "");

        let chunk = VecDeque::new();
        let (builder, size) = upload
            .partial_upload_request("http://localhost/chunk-0", 0_usize, chunk, 2 * LEN)
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
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let upload = UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "");

        let chunk = VecDeque::from_iter([new_line(0, LEN), new_line(1, LEN)]);
        let expected = chunk.iter().fold(Vec::new(), |mut a, b| {
            a.extend_from_slice(b);
            a
        });
        let (builder, size) = upload
            .partial_upload_request("http://localhost/chunk-0", 0_usize, chunk, 2 * LEN)
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
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let upload = UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "");

        let chunk = VecDeque::from_iter([new_line(2, LEN), new_line(3, LEN)]);
        let expected = chunk.iter().fold(Vec::new(), |mut a, b| {
            a.extend_from_slice(b);
            a
        });
        let (builder, size) = upload
            .partial_upload_request("http://localhost/chunk-1", 2 * LEN, chunk, 2 * LEN)
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
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let upload = UploadObjectBuffered::new(inner, "projects/_/buckets/bucket", "object", "");

        let chunk = VecDeque::from_iter([new_line(2, LEN)]);
        let expected = chunk.iter().fold(Vec::new(), |mut a, b| {
            a.extend_from_slice(b);
            a
        });
        let (builder, size) = upload
            .partial_upload_request("http://localhost/chunk-finalize", 4 * LEN, chunk, 2 * LEN)
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
        let stream = VecStream::new((0..5).map(|i| new_line(i, LEN)).collect::<Vec<_>>());
        let mut payload = InsertPayload::from(stream);

        let (vec, remainder) = super::next_chunk(&mut payload, None, LEN * 2).await?;
        assert!(remainder.is_none(), "{remainder:?}");
        assert_eq!(vec, vec![new_line(0, LEN), new_line(1, LEN)]);

        let (vec, remainder) = super::next_chunk(&mut payload, remainder, LEN * 2).await?;
        assert!(remainder.is_none(), "{remainder:?}");
        assert_eq!(vec, vec![new_line(2, LEN), new_line(3, LEN)]);

        let (vec, remainder) = super::next_chunk(&mut payload, remainder, LEN * 2).await?;
        assert!(remainder.is_none(), "{remainder:?}");
        assert_eq!(vec, vec![new_line(4, LEN)]);

        Ok(())
    }

    #[tokio::test]
    async fn next_chunk_split() -> Result {
        const LEN: usize = 32;
        let stream = VecStream::new((0..5).map(|i| new_line(i, LEN)).collect::<Vec<_>>());
        let mut payload = InsertPayload::from(stream);

        let (vec, remainder) = super::next_chunk(&mut payload, None, LEN * 2 + LEN / 2).await?;
        assert_eq!(remainder, Some(new_line(2, LEN).split_off(LEN / 2)));
        assert_eq!(
            vec,
            vec![
                new_line(0, LEN),
                new_line(1, LEN),
                new_line(2, LEN).split_to(LEN / 2)
            ]
        );

        let (vec, remainder) =
            super::next_chunk(&mut payload, remainder, LEN * 2 + LEN / 2).await?;
        assert!(remainder.is_none());
        assert_eq!(
            vec,
            vec![
                new_line(2, LEN).split_off(LEN / 2),
                new_line(3, LEN),
                new_line(4, LEN)
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn next_chunk_split_large_remainder() -> Result {
        const LEN: usize = 32;
        let buffer = (0..3)
            .map(|i| new_line_string(i, LEN))
            .collect::<Vec<_>>()
            .join("");
        let stream = VecStream::new(vec![bytes::Bytes::from_owner(buffer), new_line(3, LEN)]);
        let mut payload = InsertPayload::from(stream);

        let remainder = None;
        let (vec, remainder) = super::next_chunk(&mut payload, remainder, LEN).await?;
        assert!(remainder.is_some());
        assert_eq!(vec, vec![new_line(0, LEN)]);

        let (vec, remainder) = super::next_chunk(&mut payload, remainder, LEN).await?;
        assert!(remainder.is_some());
        assert_eq!(vec, vec![new_line(1, LEN)]);

        let (vec, remainder) = super::next_chunk(&mut payload, remainder, LEN).await?;
        assert!(remainder.is_none());
        assert_eq!(vec, vec![new_line(2, LEN)]);

        let (vec, remainder) = super::next_chunk(&mut payload, remainder, LEN).await?;
        assert!(remainder.is_none());
        assert_eq!(vec, vec![new_line(3, LEN)]);

        Ok(())
    }

    #[tokio::test]
    async fn next_chunk_join_remainder() -> Result {
        const LEN: usize = 32;
        let buffer = (0..3)
            .map(|i| new_line_string(i, LEN))
            .collect::<Vec<_>>()
            .join("");
        let stream = VecStream::new(vec![bytes::Bytes::from_owner(buffer.clone()), new_line(3, LEN)]);
        let mut payload = InsertPayload::from(stream);

        let remainder = None;
        let (vec, remainder) = super::next_chunk(&mut payload, remainder, 2 * LEN).await?;
        assert!(remainder.is_some());
        assert_eq!(vec, vec![bytes::Bytes::from_owner(buffer.clone()).slice(0..(2*LEN))]);

        let (vec, remainder) = super::next_chunk(&mut payload, remainder, 2 * LEN).await?;
        assert!(remainder.is_none());
        assert_eq!(vec, vec![bytes::Bytes::from_owner(buffer.clone()).slice((2*LEN)..), new_line(3, LEN)]);

        Ok(())
    }

    #[tokio::test]
    async fn next_chunk_done() -> Result {
        const LEN: usize = 32;
        let stream = VecStream::new((0..2).map(|i| new_line(i, LEN)).collect::<Vec<_>>());
        let mut payload = InsertPayload::from(stream);

        let (vec, remainder) = super::next_chunk(&mut payload, None, LEN * 4).await?;
        assert!(remainder.is_none(), "{remainder:?}");
        assert_eq!(vec, vec![new_line(0, LEN), new_line(1, LEN)]);

        let (vec, remainder) = super::next_chunk(&mut payload, remainder, LEN * 4).await?;
        assert!(remainder.is_none(), "{remainder:?}");
        assert!(vec.is_empty(), "{vec:?}");

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

    #[test_case(None, Some(0))]
    #[test_case(Some("bytes=0-12345"), Some(12345))]
    #[test_case(Some("bytes=0-1"), Some(1))]
    #[test_case(Some("bytes=0-0"), Some(0))]
    #[test_case(Some("bytes=1-12345"), None)]
    #[test_case(Some(""), None)]
    fn range_end(input: Option<&str>, want: Option<usize>) {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
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
}
