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

use super::writer::AppendableObjectWriter;
use crate::model::{Object, WriteObjectSpec};
use crate::model_ext::{KeyAes256, OpenAppendableObjectRequest};
use crate::storage::request_options::RequestOptions;
use std::sync::Arc;

/// A request builder for opening a new object for exclusive appends.
///
/// # Example
/// ```ignore
/// use google_cloud_storage::client::Storage;
/// # #[cfg(google_cloud_unstable_storage_bidi)]
/// # async fn sample(client: &Storage) -> anyhow::Result<()> {
/// # use google_cloud_storage::storage::append_object::builder::OpenAppendableObject;
/// let builder: OpenAppendableObject = client
///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object");
/// let mut writer = builder
///     .set_kms_key("my-kms-key")
///     .send()
///     .await?;
/// // Use `writer` to append data to `my-object`.
/// # Ok(()) }
/// ```
/// 
/// TODO(#5716): This is a work in progress. Logic will be implemented soon.
#[cfg(google_cloud_unstable_storage_bidi)]
#[derive(Clone, Debug)]
pub struct OpenAppendableObject<S = crate::storage::transport::Storage> {
    stub: Arc<S>,
    request: OpenAppendableObjectRequest,
    options: RequestOptions,
}

#[cfg(google_cloud_unstable_storage_bidi)]
impl<S> OpenAppendableObject<S>
where
    S: crate::storage::stub::Storage
        + crate::storage::append_object::stub::AppendableStorage
        + 'static,
{
    /// Sends the request, returning a new [AppendableObjectWriter].
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?;
    /// writer.append("some bytes").await?;
    /// # Ok(()) }
    /// ```
    pub async fn send(mut self) -> crate::Result<AppendableObjectWriter> {
        // Skeleton implementation: return a dummy writer
        let resource = self.request.spec.resource.take().unwrap_or_default();
        Ok(AppendableObjectWriter {
            stub: self.stub,
            bucket: resource.bucket,
            object: resource.name,
            params: self.request.params,
            if_metageneration_match: self.request.spec.if_metageneration_match,
            if_metageneration_not_match: self.request.spec.if_metageneration_not_match,
            options: self.options,
        })
    }
}

#[cfg(google_cloud_unstable_storage_bidi)]
impl<S> OpenAppendableObject<S> {
    pub(crate) fn new<B, O>(stub: Arc<S>, bucket: B, object: O, options: RequestOptions) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        let mut spec = WriteObjectSpec::new();
        spec.appendable = Some(true);
        let mut resource = Object::new();
        resource.bucket = bucket.into();
        resource.name = object.into();
        spec.resource = Some(resource);

        Self {
            stub,
            request: OpenAppendableObjectRequest { spec, params: None },
            options,
        }
    }

    fn mut_resource(&mut self) -> &mut Object {
        self.request
            .spec
            .resource
            .as_mut()
            .expect("resource field initialized in `new()`")
    }

    // Create-only setters

    /// Configure this object to use one of the [predefined ACLs].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_predefined_acl("private")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [predefined ACLs]: https://cloud.google.com/storage/docs/access-control/lists#predefined-acl
    pub fn set_predefined_acl<V: Into<String>>(mut self, v: V) -> Self {
        self.request.spec.predefined_acl = v.into();
        self
    }

    /// Sets the ACL for the new object.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// # use google_cloud_storage::model::ObjectAccessControl;
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_acl([ObjectAccessControl::new().set_entity("allAuthenticatedUsers").set_role("READER")])
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_acl<I, V>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<crate::model::ObjectAccessControl>,
    {
        self.mut_resource().acl = v.into_iter().map(|x| x.into()).collect();
        self
    }

    /// Sets the [cache control] for the new object.
    ///
    /// This can be used to control caching in [public objects].
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_cache_control("public; max-age=7200")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [public objects]: https://cloud.google.com/storage/docs/access-control/making-data-public
    /// [cache control]: https://datatracker.ietf.org/doc/html/rfc7234#section-5.2
    pub fn set_cache_control<V: Into<String>>(mut self, v: V) -> Self {
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
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_content_disposition("inline")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [content disposition]: https://datatracker.ietf.org/doc/html/rfc6266
    pub fn set_content_disposition<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_disposition = v.into();
        self
    }

    /// Sets the [content encoding] for the object data.
    ///
    /// This can be used to upload compressed data and enable [transcoding] of
    /// the data during reads.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_content_encoding("gzip")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [transcoding]: https://cloud.google.com/storage/docs/transcoding
    /// [content encoding]: https://datatracker.ietf.org/doc/html/rfc7231#section-3.1.2.2
    pub fn set_content_encoding<V: Into<String>>(mut self, v: V) -> Self {
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
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_content_language("en")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [content language]: https://cloud.google.com/storage/docs/metadata#content-language
    pub fn set_content_language<V: Into<String>>(mut self, v: V) -> Self {
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
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_content_type("text/plain")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [content type]: https://datatracker.ietf.org/doc/html/rfc7231#section-3.1.1.5
    pub fn set_content_type<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_type = v.into();
        self
    }

    /// Sets the [custom time] for the new object.
    ///
    /// This field is typically set in order to use the [DaysSinceCustomTime]
    /// condition in Object Lifecycle Management.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let time = wkt::Timestamp::try_from("2025-07-07T18:30:00Z")?;
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_custom_time(time)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [DaysSinceCustomTime]: https://cloud.google.com/storage/docs/lifecycle#dayssincecustomtime
    /// [custom time]: https://cloud.google.com/storage/docs/metadata#custom-time
    pub fn set_custom_time<V: Into<wkt::Timestamp>>(mut self, v: V) -> Self {
        self.mut_resource().custom_time = Some(v.into());
        self
    }

    /// Sets the [event based hold] flag for the new object.
    ///
    /// This field is typically set in order to prevent objects from being
    /// deleted or modified.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_event_based_hold(true)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [event based hold]: https://cloud.google.com/storage/docs/object-holds
    pub fn set_event_based_hold<V: Into<bool>>(mut self, v: V) -> Self {
        self.mut_resource().event_based_hold = Some(v.into());
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
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_kms_key("projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [Customer-managed encryption key]: https://cloud.google.com/storage/docs/encryption/customer-managed-keys
    pub fn set_kms_key<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().kms_key = v.into();
        self
    }

    /// Sets the [custom metadata] for the new object.
    ///
    /// This field is typically set to annotate the object with
    /// application-specific metadata.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_metadata([("test-only", "true"), ("environment", "qa")])
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [custom metadata]: https://cloud.google.com/storage/docs/metadata#custom-metadata
    pub fn set_metadata<I, K, V>(mut self, i: I) -> Self
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
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// # use google_cloud_storage::model::object::{Retention, retention};
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_retention(
    ///         Retention::new()
    ///             .set_mode(retention::Mode::Locked)
    ///             .set_retain_until_time(wkt::Timestamp::try_from("2035-01-01T00:00:00Z")?))
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [retention configuration]: https://cloud.google.com/storage/docs/metadata#retention-config
    pub fn set_retention<V: Into<crate::model::object::Retention>>(mut self, v: V) -> Self {
        self.mut_resource().retention = Some(v.into());
        self
    }

    /// Sets the [storage class] for the new object.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_storage_class("ARCHIVE")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [storage class]: https://cloud.google.com/storage/docs/storage-classes
    pub fn set_storage_class<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().storage_class = v.into();
        self
    }

    /// Sets the [temporary hold] flag for the new object.
    ///
    /// This field is typically set in order to prevent objects from being
    /// deleted or modified.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_temporary_hold(true)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [temporary hold]: https://cloud.google.com/storage/docs/object-holds
    pub fn set_temporary_hold<V: Into<bool>>(mut self, v: V) -> Self {
        self.mut_resource().temporary_hold = v.into();
        self
    }

    /// Sets the MD5 hash for the new object.
    pub fn set_md5_hash<I, V>(mut self, i: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<u8>,
    {
        let checksum = self.mut_resource().checksums.get_or_insert_default();
        checksum.md5_hash = i.into_iter().map(|x| x.into()).collect();
        self
    }

    /// Sets the object custom contexts.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// # use google_cloud_storage::model::{ObjectContexts, ObjectCustomContextPayload};
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_contexts(
    ///         ObjectContexts::new().set_custom([
    ///             ("example", ObjectCustomContextPayload::new().set_value("true")),
    ///         ])
    ///     )
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_contexts<V: Into<crate::model::ObjectContexts>>(mut self, v: V) -> Self {
        self.mut_resource().contexts = Some(v.into());
        self
    }

    /// Provide a precomputed value for the CRC32C checksum.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use crc32c::crc32c;
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_known_crc32c(crc32c(b"hello world"))
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// In some applications, the payload's CRC32C checksum is already known.
    /// For example, the application may be reading the data from another blob
    /// storage system.
    ///
    /// In such cases, it is safer to pass the known CRC32C of the payload to
    /// [Cloud Storage], and more efficient to skip the computation in the
    /// client library.
    ///
    /// [Cloud Storage]: https://cloud.google.com/storage/docs
    pub fn with_known_crc32c<V: Into<u32>>(mut self, v: V) -> Self {
        let checksum = self.mut_resource().checksums.get_or_insert_default();
        checksum.crc32c = Some(v.into());
        self
    }

    /// Provide a precomputed value for the MD5 hash.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use md5::compute;
    /// let hash = md5::compute(b"hello world");
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_known_md5_hash(bytes::Bytes::from_owner(hash.0))
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// In some applications, the payload's MD5 hash is already known. For
    /// example, the application may be reading the data from another blob
    /// storage system.
    ///
    /// In such cases, it is safer to pass the known MD5 of the payload to
    /// [Cloud Storage], and more efficient to skip the computation in the
    /// client library.
    ///
    /// [Cloud Storage]: https://cloud.google.com/storage/docs
    pub fn with_known_md5_hash<I, V>(self, i: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<u8>,
    {
        self.set_md5_hash(i)
    }

    // Preconditions

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::{model_ext::KeyAes256, client::Storage};
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let key: &[u8] = &[97; 32];
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_key(KeyAes256::new(key)?)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_key(mut self, v: KeyAes256) -> Self {
        self.request.params = Some(v.into());
        self
    }

    /// Set a [request precondition] on the object generation to match.
    ///
    /// With this precondition the request fails if the current object
    /// generation does not match the provided value. A common value is `0`, which
    /// prevents writes from succeeding if the object already exists.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_generation_match(0)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_generation_match<V: Into<i64>>(mut self, v: V) -> Self {
        self.request.spec.if_generation_match = Some(v.into());
        self
    }

    /// Set a [request precondition] on the object meta generation.
    ///
    /// With this precondition the request fails if the current object metadata
    /// generation does not match the provided value. This may be useful to
    /// prevent changes when the metageneration is known.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_metageneration_match(1234)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_metageneration_match<V: Into<i64>>(mut self, v: V) -> Self {
        self.request.spec.if_metageneration_match = Some(v.into());
        self
    }

    /// Set a [request precondition] on the object meta-generation.
    ///
    /// With this precondition the request fails if the current object metadata
    /// generation matches the provided value. This is rarely useful in uploads,
    /// it is more commonly used on reads to prevent a large response if the
    /// data is already cached.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_metageneration_not_match(1234)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_metageneration_not_match<V: Into<i64>>(mut self, v: V) -> Self {
        self.request.spec.if_metageneration_not_match = Some(v.into());
        self
    }

    // Options-layer

    /// The retry policy used for this request.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use std::time::Duration;
    /// use google_cloud_gax::retry_policy::RetryPolicyExt;
    /// use google_cloud_storage::retry_policy::RetryableErrors;
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_retry_policy(
    ///         RetryableErrors
    ///             .with_attempt_limit(5)
    ///             .with_time_limit(Duration::from_secs(90)),
    ///     )
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_policy(
        mut self,
        v: impl Into<google_cloud_gax::retry_policy::RetryPolicyArg>,
    ) -> Self {
        self.options.retry_policy = v.into().into();
        self
    }

    /// The backoff policy used for this request.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_backoff_policy(ExponentialBackoff::default())
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_backoff_policy(
        mut self,
        v: impl Into<google_cloud_gax::backoff_policy::BackoffPolicyArg>,
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
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_gax::retry_throttler::CircuitBreaker;
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_retry_throttler(CircuitBreaker::default())
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_throttler(
        mut self,
        v: impl Into<google_cloud_gax::retry_throttler::RetryThrottlerArg>,
    ) -> Self {
        self.options.retry_throttler = v.into().into();
        self
    }

    /// Configure the idempotency for this operation.
    ///
    /// By default, the client library treats this operation as non-idempotent
    /// unless request preconditions are set.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_idempotency(true)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_idempotency(mut self, v: bool) -> Self {
        self.options.idempotency = Some(v);
        self
    }

    /// Sets the `User-Agent` header for this request.
    ///
    /// # Example
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # #[cfg(google_cloud_unstable_storage_bidi)]
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_user_agent("my-app/1.0.0")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_user_agent(mut self, v: impl Into<String>) -> Self {
        self.options.user_agent = Some(v.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::CommonObjectRequestParams;
    use crate::model_ext::tests::create_key_helper;
    use std::sync::Arc;

    #[derive(Debug)]
    struct DummyStorage;

    impl crate::storage::stub::Storage for DummyStorage {}

    #[async_trait::async_trait]
    impl crate::storage::append_object::stub::AppendableStorage for DummyStorage {}

    // Verify `OpenAppendableObject` meets normal Send, Sync, requirements.
    #[tokio::test]
    async fn traits() -> anyhow::Result<()> {
        static_assertions::assert_impl_all!(
            OpenAppendableObject: Clone,
            std::fmt::Debug,
            Send,
            Sync
        );

        let stub = Arc::new(DummyStorage);
        let open = OpenAppendableObject::new(
            stub.clone(),
            "test-bucket".to_string(),
            "test-object".to_string(),
            RequestOptions::new(),
        );

        fn need_send<T: Send>(_val: &T) {}
        fn need_static<T: 'static>(_val: &T) {}

        need_static(&open);

        let fut = open.send();
        need_send(&fut);
        need_static(&fut);
        Ok(())
    }

    #[tokio::test]
    async fn attributes() -> anyhow::Result<()> {
        let stub = Arc::new(DummyStorage);
        let builder = OpenAppendableObject::new(
            stub,
            "test-bucket".to_string(),
            "test-object".to_string(),
            RequestOptions::new(),
        )
        .set_kms_key("test-kms-key")
        .set_metadata([("key1".to_string(), "val1".to_string())])
        .set_if_generation_match(123)
        .set_if_metageneration_match(42)
        .set_if_metageneration_not_match(43);

        let mut spec = WriteObjectSpec::new();
        spec.appendable = Some(true);
        let mut resource = Object::new();
        resource.bucket = "test-bucket".to_string();
        resource.name = "test-object".to_string();
        resource.kms_key = "test-kms-key".to_string();
        resource.metadata = [("key1".to_string(), "val1".to_string())]
            .into_iter()
            .collect();
        spec.resource = Some(resource);
        spec.if_generation_match = Some(123);
        spec.if_metageneration_match = Some(42);
        spec.if_metageneration_not_match = Some(43);

        let want = OpenAppendableObjectRequest { spec, params: None };
        assert_eq!(builder.request, want);
        Ok(())
    }

    #[tokio::test]
    async fn csek() -> anyhow::Result<()> {
        let stub = Arc::new(DummyStorage);
        let builder = OpenAppendableObject::new(
            stub,
            "test-bucket".to_string(),
            "test-object".to_string(),
            RequestOptions::new(),
        );

        let (raw_key, _, _, _) = create_key_helper();
        let key = KeyAes256::new(&raw_key)?;
        let builder = builder.set_key(key.clone());

        let mut spec = WriteObjectSpec::new();
        spec.appendable = Some(true);
        let mut resource = Object::new();
        resource.bucket = "test-bucket".to_string();
        resource.name = "test-object".to_string();
        spec.resource = Some(resource);

        let want = OpenAppendableObjectRequest {
            spec,
            params: Some(CommonObjectRequestParams::from(key)),
        };
        assert_eq!(builder.request, want);
        Ok(())
    }

    #[tokio::test]
    async fn request_options() -> anyhow::Result<()> {
        use google_cloud_gax::exponential_backoff::ExponentialBackoffBuilder;
        use google_cloud_gax::retry_policy::Aip194Strict;
        use google_cloud_gax::retry_throttler::CircuitBreaker;

        let stub = Arc::new(DummyStorage);
        let builder = OpenAppendableObject::new(
            stub,
            "test-bucket".to_string(),
            "test-object".to_string(),
            RequestOptions::new(),
        )
        .with_backoff_policy(
            ExponentialBackoffBuilder::default()
                .with_scaling(4.0)
                .build()
                .expect("exponential backoff builds"),
        )
        .with_retry_policy(Aip194Strict)
        .with_retry_throttler(CircuitBreaker::default())
        .with_idempotency(true)
        .with_user_agent("test-agent");

        let got = builder.options;
        assert!(
            format!("{:?}", got.backoff_policy).contains("ExponentialBackoff"),
            "{got:?}"
        );
        assert!(
            format!("{:?}", got.retry_policy).contains("Aip194Strict"),
            "{got:?}"
        );
        assert!(
            format!("{:?}", got.retry_throttler).contains("CircuitBreaker"),
            "{got:?}"
        );
        assert_eq!(got.idempotency, Some(true), "{got:?}");
        assert_eq!(got.user_agent.as_deref(), Some("test-agent"), "{got:?}");

        Ok(())
    }

    #[tokio::test]
    async fn send() -> anyhow::Result<()> {
        let stub = Arc::new(DummyStorage);
        let builder = OpenAppendableObject::new(
            stub,
            "test-bucket".to_string(),
            "test-object".to_string(),
            RequestOptions::new(),
        )
        .set_if_metageneration_match(42)
        .set_if_metageneration_not_match(43)
        .with_idempotency(true)
        .with_user_agent("test-agent");

        let writer = builder.send().await.unwrap();
        assert_eq!(writer.bucket, "test-bucket");
        assert_eq!(writer.object, "test-object");
        assert_eq!(writer.if_metageneration_match, Some(42));
        assert_eq!(writer.if_metageneration_not_match, Some(43));
        assert_eq!(writer.options.idempotency, Some(true));
        assert_eq!(writer.options.user_agent.as_deref(), Some("test-agent"));
        Ok(())
    }
}
