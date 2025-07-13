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
use futures::stream::unfold;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

mod buffered;
mod unbuffered;

/// A request builder for uploads without rewind.
pub struct UploadObject<T> {
    inner: std::sync::Arc<StorageInner>,
    resource: crate::model::Object,
    spec: crate::model::WriteObjectSpec,
    params: Option<crate::model::CommonObjectRequestParams>,
    // We need `Arc<Mutex<>>` because this is re-used in retryable uploads.
    payload: Arc<Mutex<InsertPayload<T>>>,
}

impl<T> UploadObject<T> {
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
    ///     .send()
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
    ///     .send()
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
    ///     .send()
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
    ///     .send()
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
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub fn with_acl<I, V>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<crate::model::ObjectAccessControl>,
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_cache_control("public; max-age=7200")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_content_disposition("inline")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use flate2::write::GzEncoder;
    /// use std::io::Write;
    /// let mut e = GzEncoder::new(Vec::new(), flate2::Compression::default());
    /// e.write_all(b"hello world");
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", bytes::Bytes::from_owner(e.finish()?))
    ///     .with_content_encoding("gzip")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_content_language("en")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_content_type("text/plain")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let time = wkt::Timestamp::try_from("2025-07-07T18:30:00Z")?;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_custom_time(time)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_event_based_hold(true)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let time = wkt::Timestamp::try_from("2025-07-07T18:30:00Z")?;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_metadata([("test-only", "true"), ("environment", "qa")])
    ///     .send()
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
        self.resource.metadata = i.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
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
    ///     .send()
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
        self.resource.retention = Some(v.into());
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
    ///     .send()
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let time = wkt::Timestamp::try_from("2025-07-07T18:30:00Z")?;
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_temporary_hold(true)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_kms_key("projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key")
    ///     .send()
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
        self.resource.kms_key = v.into();
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
    ///     .send()
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
    /// # use google_cloud_storage::client::KeyAes256;
    /// let key: &[u8] = &[97; 32];
    /// let response = client
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .with_key(KeyAes256::new(key)?)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
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
        UploadObject {
            inner,
            resource: crate::model::Object::new()
                .set_bucket(bucket)
                .set_name(object),
            spec: crate::model::WriteObjectSpec::new(),
            params: None,
            payload: Arc::new(Mutex::new(payload.into())),
        }
    }

    async fn start_resumable_upload(&self) -> Result<String>
    where
        T: Send + Sync + 'static,
    {
        let id = gax::retry_loop_internal::retry_loop(
            // TODO(#2044) - we need to apply any timeouts here.
            async |_| self.start_resumable_upload_attempt().await,
            async |duration| tokio::time::sleep(duration).await,
            // Creating a resumable upload is always idempotent. There are no
            // **observable** side-effects if executed multiple times. Any extra
            // sessions created in the retry loop are simply lost and eventually
            // garbage collected.
            true,
            self.inner.retry_throttler.clone(),
            self.inner.retry_policy.clone(),
            self.inner.backoff_policy.clone(),
        )
        .await?;
        Ok(id)
    }

    async fn start_resumable_upload_attempt(&self) -> Result<String> {
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
        let builder = apply_customer_supplied_encryption_headers(builder, &self.params);
        let builder = self.inner.apply_auth_headers(builder).await?;
        let builder = builder.json(&v1::insert_body(&self.resource));
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

#[cfg(test)]
mod tests {
    use super::client::tests::{create_key_helper, test_inner_client};
    use super::*;
    use crate::model::WriteObjectSpec;
    use gax::retry_policy::RetryPolicyExt;
    use httptest::{Expectation, Server, matchers::*, responders::status_code};
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    type Result = anyhow::Result<()>;

    #[test]
    fn upload_object_unbuffered_metadata() -> Result {
        use crate::model::ObjectAccessControl;
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let request = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "")
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
                crate::model::object::Retention::new()
                    .set_mode(crate::model::object::retention::Mode::Locked)
                    .set_retain_until_time(wkt::Timestamp::try_from("2035-07-07T18:14:00Z")?),
            )
            .with_storage_class("ARCHIVE")
            .with_temporary_hold(true)
            .with_kms_key("test-key");

        assert_eq!(
            request.spec,
            WriteObjectSpec::new()
                .set_if_generation_match(10)
                .set_if_generation_not_match(20)
                .set_if_metageneration_match(30)
                .set_if_metageneration_not_match(40)
                .set_predefined_acl("private")
        );

        assert_eq!(
            request.resource,
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

    #[tokio::test]
    async fn start_resumable_upload() -> Result {
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
        let mut request = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
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
        let request = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
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
        UploadObject::new(inner, "malformed", "object", "hello")
            .start_resumable_upload_request()
            .await
            .expect_err("malformed bucket string should error");
        Ok(())
    }

    #[tokio::test]
    async fn start_resumable_upload_metadata_in_request() -> Result {
        use crate::model::ObjectAccessControl;
        let inner = test_inner_client(gaxi::options::ClientConfig::default());
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
            .with_crc32c(crc32c::crc32c(b""))
            .with_custom_time(wkt::Timestamp::try_from("2025-07-07T18:11:00Z")?)
            .with_event_based_hold(true)
            .with_md5_hash(md5::compute(b"").0)
            .with_metadata([("k0", "v0"), ("k1", "v1")])
            .with_retention(
                crate::model::object::Retention::new()
                    .set_mode(crate::model::object::retention::Mode::Locked)
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
        let _ = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .start_resumable_upload_request()
            .await
            .inspect_err(|e| assert!(e.is_authentication()))
            .expect_err("invalid credentials should err");
        Ok(())
    }

    #[tokio::test]
    async fn start_resumable_upload_immediate_success() -> Result {
        let server = Server::run();
        let session = server.url("/upload/session/test-only-001");
        let want = session.to_string();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/bucket/o"),
                request::query(url_decoded(contains(("name", "object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
            ])
            .respond_with(
                status_code(200)
                    .append_header("location", session.to_string())
                    .body(""),
            ),
        );

        let inner = test_inner_client(gaxi::options::ClientConfig {
            endpoint: Some(format!("http://{}", server.addr())),
            ..Default::default()
        });
        let got = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .start_resumable_upload()
            .await?;
        assert_eq!(got, want);

        Ok(())
    }

    #[tokio::test]
    async fn start_resumable_upload_immediate_error() -> Result {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/bucket/o"),
                request::query(url_decoded(contains(("name", "object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
            ])
            .respond_with(status_code(403).body("uh-oh")),
        );

        let inner = test_inner_client(gaxi::options::ClientConfig {
            endpoint: Some(format!("http://{}", server.addr())),
            ..Default::default()
        });
        let err = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .start_resumable_upload()
            .await
            .expect_err("request should fail");
        assert_eq!(err.http_status_code(), Some(403), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn start_resumable_upload_retry_transient_failures_then_success() -> Result {
        use httptest::responders::cycle;
        let server = Server::run();
        let session = server.url("/upload/session/test-only-001");
        let want = session.to_string();
        let matching = || {
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/bucket/o"),
                request::query(url_decoded(contains(("name", "object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
            ])
        };
        server.expect(matching().times(3).respond_with(cycle![
            status_code(503).body("try-again"),
            status_code(503).body("try-again"),
            status_code(200).append_header("location", session.to_string()),
        ]));

        let inner = test_inner_client(gaxi::options::ClientConfig {
            endpoint: Some(format!("http://{}", server.addr())),
            ..Default::default()
        });
        let got = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .start_resumable_upload()
            .await?;
        assert_eq!(got, want);

        Ok(())
    }

    #[tokio::test]
    async fn start_resumable_upload_retry_too_many_transients() -> Result {
        let server = Server::run();
        let matching = || {
            Expectation::matching(all_of![
                request::method_path("POST", "/upload/storage/v1/b/bucket/o"),
                request::query(url_decoded(contains(("name", "object")))),
                request::query(url_decoded(contains(("uploadType", "resumable")))),
            ])
        };
        server.expect(
            matching()
                .times(3)
                .respond_with(status_code(503).body("try-again")),
        );

        let inner = test_inner_client(gaxi::options::ClientConfig {
            endpoint: Some(format!("http://{}", server.addr())),
            retry_policy: Some(Arc::new(
                crate::retry_policy::RecommendedPolicy.with_attempt_limit(3),
            )),
            ..Default::default()
        });
        let err = UploadObject::new(inner, "projects/_/buckets/bucket", "object", "hello")
            .start_resumable_upload()
            .await
            .expect_err("request should fail after 3 retry attempts");
        assert_eq!(err.http_status_code(), Some(503), "{err:?}");

        Ok(())
    }
}
