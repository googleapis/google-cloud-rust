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

//! Contains the request builder for [upload_object()] and related types.
//!
//! [upload_object()]: crate::storage::client::Storage::upload_object()

use super::client::*;
use super::perform_upload::PerformUpload;
use super::upload_source::{Seek, StreamingSource};
use super::*;
use crate::storage::checksum::{
    ChecksumEngine,
    details::{Crc32c, Known, KnownCrc32c, KnownMd5, Md5, update as checksum_update},
};

/// A request builder for object uploads.
///
/// # Example: hello world
/// ```
/// use google_cloud_storage::client::Storage;
/// async fn sample(client: &Storage) -> anyhow::Result<()> {
///     let response = client
///         .upload_object("projects/_/buckets/my-bucket", "hello", "Hello World!")
///         .send_unbuffered()
///         .await?;
///     println!("response details={response:?}");
///     Ok(())
/// }
/// ```
///
/// # Example: upload a file
/// ```
/// use google_cloud_storage::client::Storage;
/// async fn sample(client: &Storage) -> anyhow::Result<()> {
///     let payload = tokio::fs::File::open("my-data").await?;
///     let response = client
///         .upload_object("projects/_/buckets/my-bucket", "my-object", payload)
///         .send_unbuffered()
///         .await?;
///     println!("response details={response:?}");
///     Ok(())
/// }
/// ```
///
/// # Example: upload a custom data source
/// ```
/// use google_cloud_storage::{client::Storage, upload_source::StreamingSource};
/// struct DataSource;
/// impl StreamingSource for DataSource {
///     type Error = std::io::Error;
///     async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
///         # panic!();
///     }
/// }
///
/// async fn sample(client: &Storage) -> anyhow::Result<()> {
///     let response = client
///         .upload_object("projects/_/buckets/my-bucket", "my-object", DataSource)
///         .send_buffered()
///         .await?;
///     println!("response details={response:?}");
///     Ok(())
/// }
/// ```
pub struct UploadObject<T, C = Crc32c> {
    inner: std::sync::Arc<StorageInner>,
    spec: crate::model::WriteObjectSpec,
    params: Option<crate::model::CommonObjectRequestParams>,
    payload: Payload<T>,
    options: super::request_options::RequestOptions,
    checksum: C,
}

impl<T, C> UploadObject<T, C> {
    /// Set a [request precondition] on the object generation to match.
    ///
    /// With this precondition the request fails if the current object
    /// generation matches the provided value. A common value is `0`, which
    /// prevents uploads from succeeding if the object already exists.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_if_generation_match(0)
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_if_generation_not_match(0)
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_if_metageneration_match(1234)
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_if_metageneration_not_match(1234)
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// # use google_cloud_storage::model::ObjectAccessControl;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_acl([ObjectAccessControl::new().set_entity("allAuthenticatedUsers").set_role("READER")])
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub fn with_acl<I, V>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<crate::model::ObjectAccessControl>,
    {
        self.mut_resource().acl = v.into_iter().map(|a| a.into()).collect();
        self
    }

    /// Sets the [cache control] for the new object.
    ///
    /// This can be used to control caching in [public objects].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_cache_control("public; max-age=7200")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [public objects]: https://cloud.google.com/storage/docs/access-control/making-data-public
    /// [cache control]: https://datatracker.ietf.org/doc/html/rfc7234#section-5.2
    pub fn with_cache_control<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().cache_control = v.into();
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_content_disposition("inline")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [content disposition]: https://datatracker.ietf.org/doc/html/rfc6266
    pub fn with_content_disposition<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_disposition = v.into();
        self
    }

    /// Sets the [content encoding] for the object data.
    ///
    /// This can be used to upload compressed data and enable [transcoding] of
    /// the data during downloads.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use flate2::write::GzEncoder;
    /// use std::io::Write;
    /// let mut e = GzEncoder::new(Vec::new(), flate2::Compression::default());
    /// e.write_all(b"hello world");
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", bytes::Bytes::from_owner(e.finish()?))
    ///     .with_content_encoding("gzip")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [transcoding]: https://cloud.google.com/storage/docs/transcoding
    /// [content encoding]: https://datatracker.ietf.org/doc/html/rfc7231#section-3.1.2.2
    pub fn with_content_encoding<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_encoding = v.into();
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_content_language("en")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [content language]: https://cloud.google.com/storage/docs/metadata#content-language
    pub fn with_content_language<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_language = v.into();
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_content_type("text/plain")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [content type]: https://datatracker.ietf.org/doc/html/rfc7231#section-3.1.1.5
    pub fn with_content_type<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_type = v.into();
        self
    }

    /// Sets the [custom time] for the new object.
    ///
    /// This field is typically set in order to use the [DaysSinceCustomTime]
    /// condition in Object Lifecycle Management.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let time = wkt::Timestamp::try_from("2025-07-07T18:30:00Z")?;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_custom_time(time)
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [DaysSinceCustomTime]: https://cloud.google.com/storage/docs/lifecycle#dayssincecustomtime
    /// [custom time]: https://cloud.google.com/storage/docs/metadata#custom-time
    pub fn with_custom_time<V: Into<wkt::Timestamp>>(mut self, v: V) -> Self {
        self.mut_resource().custom_time = Some(v.into());
        self
    }

    /// Sets the [event based hold] flag for the new object.
    ///
    /// This field is typically set in order to prevent objects from being
    /// deleted or modified.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_event_based_hold(true)
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [event based hold]: https://cloud.google.com/storage/docs/object-holds
    pub fn with_event_based_hold<V: Into<bool>>(mut self, v: V) -> Self {
        self.mut_resource().event_based_hold = Some(v.into());
        self
    }

    /// Sets the [custom metadata] for the new object.
    ///
    /// This field is typically set to annotate the object with
    /// application-specific metadata.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let time = wkt::Timestamp::try_from("2025-07-07T18:30:00Z")?;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_metadata([("test-only", "true"), ("environment", "qa")])
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [custom metadata]: https://cloud.google.com/storage/docs/metadata#custom-metadata
    pub fn with_metadata<I, K, V>(mut self, i: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.mut_resource().metadata = i.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        self
    }

    /// Sets the [retention configuration] for the new object.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// # use google_cloud_storage::model::object::{Retention, retention};
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_retention(
    ///         Retention::new()
    ///             .set_mode(retention::Mode::Locked)
    ///             .set_retain_until_time(wkt::Timestamp::try_from("2035-01-01T00:00:00Z")?))
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [retention configuration]: https://cloud.google.com/storage/docs/metadata#retention-config
    pub fn with_retention<V>(mut self, v: V) -> Self
    where
        V: Into<crate::model::object::Retention>,
    {
        self.mut_resource().retention = Some(v.into());
        self
    }

    /// Sets the [storage class] for the new object.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_storage_class("ARCHIVE")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [storage class]: https://cloud.google.com/storage/docs/storage-classes
    pub fn with_storage_class<V>(mut self, v: V) -> Self
    where
        V: Into<String>,
    {
        self.mut_resource().storage_class = v.into();
        self
    }

    /// Sets the [temporary hold] flag for the new object.
    ///
    /// This field is typically set in order to prevent objects from being
    /// deleted or modified.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let time = wkt::Timestamp::try_from("2025-07-07T18:30:00Z")?;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_temporary_hold(true)
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [temporary hold]: https://cloud.google.com/storage/docs/object-holds
    pub fn with_temporary_hold<V: Into<bool>>(mut self, v: V) -> Self {
        self.mut_resource().temporary_hold = v.into();
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_kms_key("projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [Customer-managed encryption key]: https://cloud.google.com/storage/docs/encryption/customer-managed-keys
    pub fn with_kms_key<V>(mut self, v: V) -> Self
    where
        V: Into<String>,
    {
        self.mut_resource().kms_key = v.into();
        self
    }

    /// Configure this object to use one of the [predefined ACLs].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_predefined_acl("private")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// # use google_cloud_storage::builder::storage::KeyAes256;
    /// let key: &[u8] = &[97; 32];
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_key(KeyAes256::new(key)?)
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub fn with_key(mut self, v: KeyAes256) -> Self {
        self.params = Some(v.into());
        self
    }

    /// Configure the idempotency for this upload.
    ///
    /// By default, the client library treats single-shot uploads without
    /// preconditions, as non-idempotent. If the destination bucket is
    /// configured with [object versioning] then the operation may succeed
    /// multiple times with observable side-effects. With object versioning and
    /// a [lifecycle] policy limiting the number of versions, uploading the same
    /// data multiple times may result in data loss.
    ///
    /// The client library cannot efficiently determine if these conditions
    /// apply to your upload. If they do, or your application can tolerate
    /// multiple versions of the same data for other reasons, consider using
    /// `with_idempotency(true)`.
    ///
    /// The client library treats resumable uploads as idempotent, regardless of
    /// the value in this option. Such uploads can succeed at most once.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::retry_policy::RecommendedPolicy;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use std::time::Duration;
    /// use gax::retry_policy::RetryPolicyExt;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_idempotency(true)
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// [lifecycle]: https://cloud.google.com/storage/docs/lifecycle
    /// [object versioning]: https://cloud.google.com/storage/docs/object-versioning
    pub fn with_idempotency(mut self, v: bool) -> Self {
        self.options.idempotency = Some(v);
        self
    }

    /// The retry policy used for this request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::retry_policy::RecommendedPolicy;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use std::time::Duration;
    /// use gax::retry_policy::RetryPolicyExt;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_retry_policy(RecommendedPolicy
    ///         .with_attempt_limit(5)
    ///         .with_time_limit(Duration::from_secs(10)),
    ///     )
    ///     .send_buffered()
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
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_backoff_policy(ExponentialBackoff::default())
    ///     .send_buffered()
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
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_retry_throttler(adhoc_throttler())
    ///     .send_buffered()
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

    /// Sets the payload size threshold to switch from single-shot to resumable uploads.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_resumable_upload_threshold(0_usize) // Forces a resumable upload.
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// The client library can perform uploads using [single-shot] or
    /// [resumable] uploads. For small objects, single-shot uploads offer better
    /// performance, as they require a single HTTP transfer. For larger objects,
    /// the additional request latency is not significant, and resumable uploads
    /// offer better recovery on errors.
    ///
    /// The library automatically selects resumable uploads when the payload is
    /// equal to or larger than this option. For smaller uploads the client
    /// library uses single-shot uploads.
    ///
    /// The exact threshold depends on where the application is deployed and
    /// destination bucket location with respect to where the application is
    /// running. The library defaults should work well in most cases, but some
    /// applications may benefit from fine-tuning.
    ///
    /// [single-shot]: https://cloud.google.com/storage/docs/uploading-objects
    /// [resumable]: https://cloud.google.com/storage/docs/resumable-uploads
    pub fn with_resumable_upload_threshold<V: Into<usize>>(mut self, v: V) -> Self {
        self.options.resumable_upload_threshold = v.into();
        self
    }

    /// Changes the buffer size for some resumable uploads.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_resumable_upload_buffer_size(32 * 1024 * 1024_usize)
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// When performing [resumable uploads] from sources without [Seek] the
    /// client library needs to buffer data in memory until it is persisted by
    /// the service. Otherwise the data would be lost if the upload fails.
    /// Applications may want to tune this buffer size:
    ///
    /// - Use smaller buffer sizes to support more concurrent uploads in the
    ///   same application.
    /// - Use larger buffer sizes for better throughput. Sending many small
    ///   buffers stalls the upload until the client receives a successful
    ///   response from the service.
    ///
    /// Keep in mind that there are diminishing returns on using larger buffers.
    ///
    /// [resumable uploads]: https://cloud.google.com/storage/docs/resumable-uploads
    /// [Seek]: crate::upload_source::Seek
    pub fn with_resumable_upload_buffer_size<V: Into<usize>>(mut self, v: V) -> Self {
        self.options.resumable_upload_buffer_size = v.into();
        self
    }

    fn mut_resource(&mut self) -> &mut crate::model::Object {
        self.spec
            .resource
            .as_mut()
            .expect("resource field initialized in `new()`")
    }

    pub(crate) fn build(self) -> PerformUpload<C, Payload<T>> {
        PerformUpload::new(
            self.checksum,
            self.payload,
            self.inner,
            self.spec,
            self.params,
            self.options,
        )
    }

    fn switch_checksum<F, U>(self, new: F) -> UploadObject<T, U>
    where
        F: FnOnce(C) -> U,
    {
        UploadObject {
            payload: self.payload,
            inner: self.inner,
            spec: self.spec,
            params: self.params,
            options: self.options,
            checksum: new(self.checksum),
        }
    }

    fn set_crc32c<V: Into<u32>>(mut self, v: V) -> Self {
        let checksum = self.mut_resource().checksums.get_or_insert_default();
        checksum.crc32c = Some(v.into());
        self
    }

    pub fn set_md5_hash<I, V>(mut self, i: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<u8>,
    {
        let checksum = self.mut_resource().checksums.get_or_insert_default();
        checksum.md5_hash = i.into_iter().map(|v| v.into()).collect();
        self
    }
}

impl<T> UploadObject<T, Crc32c> {
    /// Provide a precomputed value for the CRC32C checksum.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use crc32c::crc32c;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_known_crc32c(crc32c(b"hello world"))
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// In some applications, the payload's CRC32C checksum is already known.
    /// For example, the application may be downloading the data from another
    /// blob storage system.
    ///
    /// In such cases, it is safer to pass the known CRC32C of the payload to
    /// [Cloud Storage], and more efficient to skip the computation in the
    /// client library.
    ///
    /// Note that once you provide a CRC32C value to this builder you cannot
    /// use [compute_md5()] to also have the library compute the checksums.
    ///
    /// [compute_md5()]: UploadObject::compute_md5
    pub fn with_known_crc32c<V: Into<u32>>(self, v: V) -> UploadObject<T, KnownCrc32c> {
        let this = self.switch_checksum(|_| KnownCrc32c);
        this.set_crc32c(v)
    }

    /// Provide a precomputed value for the MD5 hash.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use md5::compute;
    /// let hash = md5::compute(b"hello world");
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_known_md5_hash(bytes::Bytes::from_owner(hash.0))
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// In some applications, the payload's MD5 hash is already known. For
    /// example, the application may be downloading the data from another blob
    /// storage system.
    ///
    /// In such cases, it is safer to pass the known MD5 of the payload to
    /// [Cloud Storage], and more efficient to skip the computation in the
    /// client library.
    ///
    /// Note that once you provide a MD5 value to this builder you cannot
    /// use [compute_md5()] to also have the library compute the checksums.
    ///
    /// [compute_md5()]: UploadObject::compute_md5
    pub fn with_known_md5_hash<I, V>(self, i: I) -> UploadObject<T, Crc32c<KnownMd5>>
    where
        I: IntoIterator<Item = V>,
        V: Into<u8>,
    {
        let this = self.switch_checksum(|_| Crc32c::from_inner(KnownMd5));
        this.set_md5_hash(i)
    }

    /// Enables computation of MD5 hashes.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let payload = tokio::fs::File::open("my-data").await?;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", payload)
    ///     .compute_md5()
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// See [precompute_checksums][UploadObject::precompute_checksums] for more
    /// details on how checksums are used by the client library and their
    /// limitations.
    pub fn compute_md5(self) -> UploadObject<T, Md5<Crc32c>> {
        self.switch_checksum(Md5::from_inner)
    }
}

impl<T> UploadObject<T, Crc32c<KnownMd5>> {
    /// See [UploadObject<T, Crc32c>::with_known_crc32c].
    pub fn with_known_crc32c<V: Into<u32>>(self, v: V) -> UploadObject<T, Known> {
        let this = self.switch_checksum(|_| Known);
        this.set_crc32c(v)
    }
}

impl<T> UploadObject<T, Md5<Crc32c>> {
    /// See [UploadObject<T, Crc32c>::with_known_crc32c].
    pub fn with_known_crc32c<V: Into<u32>>(self, v: V) -> UploadObject<T, Md5<KnownCrc32c>> {
        let this = self.switch_checksum(|_| Md5::from_inner(KnownCrc32c));
        this.set_crc32c(v)
    }

    /// See [UploadObject<T, Crc32c>::with_known_md5_hash].
    pub fn with_known_md5_hash<I, V>(self, i: I) -> UploadObject<T, Crc32c<KnownMd5>>
    where
        I: IntoIterator<Item = V>,
        V: Into<u8>,
    {
        let this = self.switch_checksum(|_| Crc32c::from_inner(KnownMd5));
        this.set_md5_hash(i)
    }
}

impl<T> UploadObject<T, Md5<KnownCrc32c>> {
    /// See [UploadObject<T, Crc32c>::with_known_md5_hash].
    pub fn with_known_md5_hash<I, V>(self, i: I) -> UploadObject<T, Known>
    where
        I: IntoIterator<Item = V>,
        V: Into<u8>,
    {
        let this = self.switch_checksum(|_| Known);
        this.set_md5_hash(i)
    }
}

impl<T> UploadObject<T, KnownCrc32c> {
    /// See [UploadObject<T, Crc32c>::with_known_md5_hash].
    pub fn with_known_md5_hash<I, V>(self, i: I) -> UploadObject<T, Known>
    where
        I: IntoIterator<Item = V>,
        V: Into<u8>,
    {
        let this = self.switch_checksum(|_| Known);
        this.set_md5_hash(i)
    }

    /// See [UploadObject<T, Crc32c>::compute_md5()].
    pub fn compute_md5(self) -> UploadObject<T, Md5<KnownCrc32c>> {
        self.switch_checksum(Md5::from_inner)
    }
}

impl<T> UploadObject<T> {
    pub(crate) fn new<B, O, P>(
        inner: std::sync::Arc<StorageInner>,
        bucket: B,
        object: O,
        payload: P,
    ) -> Self
    where
        B: Into<String>,
        O: Into<String>,
        P: Into<Payload<T>>,
    {
        let options = inner.options.clone();
        let resource = crate::model::Object::new()
            .set_bucket(bucket)
            .set_name(object);
        UploadObject {
            inner,
            spec: crate::model::WriteObjectSpec::new().set_resource(resource),
            params: None,
            payload: payload.into(),
            options,
            checksum: Crc32c::default(),
        }
    }
}

impl<T, C> UploadObject<T, C>
where
    C: ChecksumEngine + Send + Sync + 'static,
    T: StreamingSource + Seek + Send + Sync + 'static,
    <T as StreamingSource>::Error: std::error::Error + Send + Sync + 'static,
    <T as Seek>::Error: std::error::Error + Send + Sync + 'static,
{
    /// A simple upload from a buffer.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .send_unbuffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub async fn send_unbuffered(self) -> Result<Object> {
        self.build().send_unbuffered().await
    }

    /// Precompute the payload checksums before uploading the data.
    ///
    /// If the checksums are known when the upload starts, the client library
    /// can include the checksums with the upload request, and the service can
    /// reject the upload if the payload and the checksums do not match.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let payload = tokio::fs::File::open("my-data").await?;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", payload)
    ///     .precompute_checksums()
    ///     .await?
    ///     .send_unbuffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// Precomputing the checksums can be expensive if the data source is slow
    /// to read. Therefore, the client library does not precompute the checksums
    /// by default. The client library compares the checksums computed by the
    /// service against its own checksums. If they do not match, the client
    /// library returns an error. However, the service has already created the
    /// object with the (likely incorrect) data.
    ///
    /// The client library currently uses the [JSON API], it is not possible to
    /// send the checksums at the end of the upload with this API.
    ///
    /// [JSON API]: https://cloud.google.com/storage/docs/json_api
    pub async fn precompute_checksums(mut self) -> Result<UploadObject<T, Known>>
    where
        C: ChecksumEngine + Send + Sync + 'static,
    {
        let mut offset = 0_u64;
        self.payload.seek(offset).await.map_err(Error::ser)?;
        while let Some(n) = self.payload.next().await.transpose().map_err(Error::ser)? {
            self.checksum.update(offset, &n);
            offset += n.len() as u64;
        }
        self.payload.seek(0_u64).await.map_err(Error::ser)?;
        let computed = self.checksum.finalize();
        let current = self.mut_resource().checksums.get_or_insert_default();
        checksum_update(current, computed);
        Ok(self.switch_checksum(|_| Known))
    }
}

impl<T, C> UploadObject<T, C>
where
    C: ChecksumEngine + Send + Sync + 'static,
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
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub async fn send_buffered(self) -> crate::Result<Object> {
        self.build().send().await
    }
}

// We need `Debug` to use `expect_err()` in `Result<UploadObject, ...>`.
impl<T, C> std::fmt::Debug for UploadObject<T, C>
where
    C: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadObject")
            .field("inner", &self.inner)
            .field("spec", &self.spec)
            .field("params", &self.params)
            // skip payload, as it is not `Debug`
            .field("options", &self.options)
            .field("checksum", &self.checksum)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::client::tests::{test_builder, test_inner_client};
    use super::*;
    use crate::model::{ObjectChecksums, WriteObjectSpec};
    use crate::upload_source::tests::MockSeekSource;
    use std::error::Error as _;
    use std::io::{Error as IoError, ErrorKind};

    type Result = anyhow::Result<()>;

    // Verify `upload_object()` can be used with a source that implements `StreamingSource` **and** `Seek`
    #[tokio::test]
    async fn test_upload_streaming_source_and_seek() -> Result {
        struct Source;
        impl crate::upload_source::StreamingSource for Source {
            type Error = std::io::Error;
            async fn next(&mut self) -> Option<std::result::Result<bytes::Bytes, Self::Error>> {
                None
            }
        }
        impl crate::upload_source::Seek for Source {
            type Error = std::io::Error;
            async fn seek(&mut self, _offset: u64) -> std::result::Result<(), Self::Error> {
                Ok(())
            }
        }

        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let _ = client.upload_object("projects/_/buckets/test-bucket", "test-object", Source);
        Ok(())
    }

    // Verify `upload_object()` can be used with a source that **only** implements `StreamingSource`.
    #[tokio::test]
    async fn test_upload_only_streaming_source() -> Result {
        struct Source;
        impl crate::upload_source::StreamingSource for Source {
            type Error = std::io::Error;
            async fn next(&mut self) -> Option<std::result::Result<bytes::Bytes, Self::Error>> {
                None
            }
        }

        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let _ = client.upload_object("projects/_/buckets/test-bucket", "test-object", Source);
        Ok(())
    }

    // Verify `upload_object()` meets normal Send, Sync, requirements.
    #[tokio::test]
    async fn test_upload_is_send_and_static() -> Result {
        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        fn need_send<T: Send>(_val: &T) {}
        fn need_sync<T: Sync>(_val: &T) {}
        fn need_static<T: 'static>(_val: &T) {}

        let upload = client.upload_object("projects/_/buckets/test-bucket", "test-object", "");
        need_send(&upload);
        need_sync(&upload);
        need_static(&upload);

        let upload = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", "")
            .send_unbuffered();
        need_send(&upload);
        need_static(&upload);

        let upload = client
            .upload_object("projects/_/buckets/test-bucket", "test-object", "")
            .send_buffered();
        need_send(&upload);
        need_static(&upload);

        Ok(())
    }

    #[test]
    fn upload_object_unbuffered_metadata() -> Result {
        use crate::model::ObjectAccessControl;
        let inner = test_inner_client(test_builder());
        let mut request = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "")
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
            .with_known_crc32c(crc32c::crc32c(b""))
            .with_custom_time(wkt::Timestamp::try_from("2025-07-07T18:11:00Z")?)
            .with_event_based_hold(true)
            .with_known_md5_hash(md5::compute(b"").0)
            .with_metadata([("k0", "v0"), ("k1", "v1")])
            .with_retention(
                crate::model::object::Retention::new()
                    .set_mode(crate::model::object::retention::Mode::Locked)
                    .set_retain_until_time(wkt::Timestamp::try_from("2035-07-07T18:14:00Z")?),
            )
            .with_storage_class("ARCHIVE")
            .with_temporary_hold(true)
            .with_kms_key("test-key");

        let resource = request.spec.resource.take().unwrap();
        let request = request;
        assert_eq!(
            &request.spec,
            &WriteObjectSpec::new()
                .set_if_generation_match(10)
                .set_if_generation_not_match(20)
                .set_if_metageneration_match(30)
                .set_if_metageneration_not_match(40)
                .set_predefined_acl("private")
        );

        assert_eq!(
            resource,
            Object::new()
                .set_name("object")
                .set_bucket("projects/_/buckets/bucket")
                .set_acl([ObjectAccessControl::new()
                    .set_entity("allAuthenticatedUsers")
                    .set_role("READER")])
                .set_cache_control("public; max-age=7200")
                .set_content_disposition("inline")
                .set_content_encoding("gzip")
                .set_content_language("en")
                .set_content_type("text/plain")
                .set_checksums(
                    crate::model::ObjectChecksums::new()
                        .set_crc32c(crc32c::crc32c(b""))
                        .set_md5_hash(bytes::Bytes::from_iter(md5::compute(b"").0))
                )
                .set_custom_time(wkt::Timestamp::try_from("2025-07-07T18:11:00Z")?)
                .set_event_based_hold(true)
                .set_metadata([("k0", "v0"), ("k1", "v1")])
                .set_retention(
                    crate::model::object::Retention::new()
                        .set_mode("LOCKED")
                        .set_retain_until_time(wkt::Timestamp::try_from("2035-07-07T18:14:00Z")?)
                )
                .set_storage_class("ARCHIVE")
                .set_temporary_hold(true)
                .set_kms_key("test-key")
        );

        Ok(())
    }

    #[test]
    fn upload_object_options() {
        let inner = test_inner_client(
            test_builder()
                .with_resumable_upload_threshold(123_usize)
                .with_resumable_upload_buffer_size(234_usize),
        );
        let request = UploadObject::new(inner.clone(), "projects/_/buckets/bucket", "object", "");
        assert_eq!(request.options.resumable_upload_threshold, 123);
        assert_eq!(request.options.resumable_upload_buffer_size, 234);

        let request = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "")
            .with_resumable_upload_threshold(345_usize)
            .with_resumable_upload_buffer_size(456_usize);
        assert_eq!(request.options.resumable_upload_threshold, 345);
        assert_eq!(request.options.resumable_upload_buffer_size, 456);
    }

    const QUICK: &str = "the quick brown fox jumps over the lazy dog";
    const VEXING: &str = "how vexingly quick daft zebras jump";

    fn quick_checksum<E: ChecksumEngine>(mut engine: E) -> ObjectChecksums {
        engine.update(0, &bytes::Bytes::from_static(QUICK.as_bytes()));
        engine.finalize()
    }

    async fn collect<S: StreamingSource>(mut stream: S) -> anyhow::Result<Vec<u8>> {
        let mut collected = Vec::new();
        while let Some(b) = stream.next().await.transpose()? {
            collected.extend_from_slice(&b);
        }
        Ok(collected)
    }

    #[tokio::test]
    async fn checksum_default() -> Result {
        let client = test_builder().build().await?;
        let upload = client
            .upload_object("my-bucket", "my-object", QUICK)
            .precompute_checksums()
            .await?;
        let want = quick_checksum(Crc32c::default());
        assert_eq!(upload.spec.resource.and_then(|r| r.checksums), Some(want));
        let collected = collect(upload.payload).await?;
        assert_eq!(collected, QUICK.as_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn checksum_md5_and_crc32c() -> Result {
        let client = test_builder().build().await?;
        let upload = client
            .upload_object("my-bucket", "my-object", QUICK)
            .compute_md5()
            .precompute_checksums()
            .await?;
        let want = quick_checksum(Crc32c::from_inner(Md5::default()));
        assert_eq!(upload.spec.resource.and_then(|r| r.checksums), Some(want));
        Ok(())
    }

    #[tokio::test]
    async fn checksum_precomputed() -> Result {
        let mut engine = Crc32c::from_inner(Md5::default());
        engine.update(0, &bytes::Bytes::from_static(VEXING.as_bytes()));
        let ck = engine.finalize();

        let client = test_builder().build().await?;
        let upload = client
            .upload_object("my-bucket", "my-object", QUICK)
            .with_known_crc32c(ck.crc32c.unwrap())
            .with_known_md5_hash(ck.md5_hash.clone())
            .precompute_checksums()
            .await?;
        // Note that the checksums do not match the data. This is intentional,
        // we are trying to verify that whatever is provided in with_crc32c()
        // and with_md5() is respected.
        assert_eq!(upload.spec.resource.and_then(|r| r.checksums), Some(ck));

        Ok(())
    }

    #[tokio::test]
    async fn checksum_crc32c_known_md5_computed() -> Result {
        let mut engine = Crc32c::from_inner(Md5::default());
        engine.update(0, &bytes::Bytes::from_static(VEXING.as_bytes()));
        let ck = engine.finalize();

        let client = test_builder().build().await?;
        let upload = client
            .upload_object("my-bucket", "my-object", QUICK)
            .compute_md5()
            .with_known_crc32c(ck.crc32c.unwrap())
            .precompute_checksums()
            .await?;
        // Note that the checksums do not match the data. This is intentional,
        // we are trying to verify that whatever is provided in with_known*()
        // is respected.
        let want = quick_checksum(Md5::default()).set_crc32c(ck.crc32c.unwrap());
        assert_eq!(upload.spec.resource.and_then(|r| r.checksums), Some(want));

        Ok(())
    }

    #[tokio::test]
    async fn checksum_mixed_then_precomputed() -> Result {
        let mut engine = Crc32c::from_inner(Md5::default());
        engine.update(0, &bytes::Bytes::from_static(VEXING.as_bytes()));
        let ck = engine.finalize();

        let client = test_builder().build().await?;
        let upload = client
            .upload_object("my-bucket", "my-object", QUICK)
            .with_known_md5_hash(ck.md5_hash.clone())
            .with_known_crc32c(ck.crc32c.unwrap())
            .precompute_checksums()
            .await?;
        // Note that the checksums do not match the data. This is intentional,
        // we are trying to verify that whatever is provided in with_known*()
        // is respected.
        let want = ck.clone();
        assert_eq!(upload.spec.resource.and_then(|r| r.checksums), Some(want));

        Ok(())
    }

    #[tokio::test]
    async fn checksum_full_computed_then_md5_precomputed() -> Result {
        let mut engine = Crc32c::from_inner(Md5::default());
        engine.update(0, &bytes::Bytes::from_static(VEXING.as_bytes()));
        let ck = engine.finalize();

        let client = test_builder().build().await?;
        let upload = client
            .upload_object("my-bucket", "my-object", QUICK)
            .compute_md5()
            .with_known_md5_hash(ck.md5_hash.clone())
            .precompute_checksums()
            .await?;
        // Note that the checksums do not match the data. This is intentional,
        // we are trying to verify that whatever is provided in with_known*()
        // is respected.
        let want = quick_checksum(Crc32c::default()).set_md5_hash(ck.md5_hash.clone());
        assert_eq!(upload.spec.resource.and_then(|r| r.checksums), Some(want));

        Ok(())
    }

    #[tokio::test]
    async fn checksum_known_crc32_then_computed_md5() -> Result {
        let mut engine = Crc32c::from_inner(Md5::default());
        engine.update(0, &bytes::Bytes::from_static(VEXING.as_bytes()));
        let ck = engine.finalize();

        let client = test_builder().build().await?;
        let upload = client
            .upload_object("my-bucket", "my-object", QUICK)
            .with_known_crc32c(ck.crc32c.unwrap())
            .compute_md5()
            .with_known_md5_hash(ck.md5_hash.clone())
            .precompute_checksums()
            .await?;
        // Note that the checksums do not match the data. This is intentional,
        // we are trying to verify that whatever is provided in with_known*()
        // is respected.
        let want = ck.clone();
        assert_eq!(upload.spec.resource.and_then(|r| r.checksums), Some(want));

        Ok(())
    }

    #[tokio::test]
    async fn checksum_known_crc32_then_known_md5() -> Result {
        let mut engine = Crc32c::from_inner(Md5::default());
        engine.update(0, &bytes::Bytes::from_static(VEXING.as_bytes()));
        let ck = engine.finalize();

        let client = test_builder().build().await?;
        let upload = client
            .upload_object("my-bucket", "my-object", QUICK)
            .with_known_crc32c(ck.crc32c.unwrap())
            .with_known_md5_hash(ck.md5_hash.clone())
            .precompute_checksums()
            .await?;
        // Note that the checksums do not match the data. This is intentional,
        // we are trying to verify that whatever is provided in with_known*()
        // is respected.
        let want = ck.clone();
        assert_eq!(upload.spec.resource.and_then(|r| r.checksums), Some(want));

        Ok(())
    }

    #[tokio::test]
    async fn precompute_checksums_seek_error() -> Result {
        let mut source = MockSeekSource::new();
        source
            .expect_seek()
            .once()
            .returning(|_| Err(IoError::new(ErrorKind::Deadlock, "test-only")));

        let client = test_builder().build().await?;
        let err = client
            .upload_object("my-bucket", "my-object", source)
            .precompute_checksums()
            .await
            .expect_err("seek() returns an error");
        assert!(err.is_serialization(), "{err:?}");
        assert!(
            err.source()
                .and_then(|e| e.downcast_ref::<IoError>())
                .is_some(),
            "{err:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn precompute_checksums_next_error() -> Result {
        let mut source = MockSeekSource::new();
        source.expect_seek().returning(|_| Ok(()));
        let mut seq = mockall::Sequence::new();
        source
            .expect_next()
            .times(3)
            .in_sequence(&mut seq)
            .returning(|| Some(Ok(bytes::Bytes::new())));
        source
            .expect_next()
            .once()
            .in_sequence(&mut seq)
            .returning(|| Some(Err(IoError::new(ErrorKind::BrokenPipe, "test-only"))));

        let client = test_builder().build().await?;
        let err = client
            .upload_object("my-bucket", "my-object", source)
            .precompute_checksums()
            .await
            .expect_err("seek() returns an error");
        assert!(err.is_serialization(), "{err:?}");
        assert!(
            err.source()
                .and_then(|e| e.downcast_ref::<IoError>())
                .is_some(),
            "{err:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn debug() -> Result {
        let client = test_builder().build().await?;
        let upload = client
            .upload_object("my-bucket", "my-object", "")
            .precompute_checksums()
            .await;

        let fmt = format!("{upload:?}");
        ["UploadObject", "inner", "spec", "options", "checksum"]
            .into_iter()
            .for_each(|text| {
                assert!(fmt.contains(text), "expected {text} in {fmt}");
            });
        Ok(())
    }
}
