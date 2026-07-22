// Copyright 2026 Google LLC
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

use crate::Result;
use crate::appendable_object_writer::AppendableObjectWriter;
use crate::model::Object;
use crate::model_ext::OpenAppendableObjectRequest;
use crate::request_options::RequestOptions;
use std::sync::Arc;
use std::time::Duration;

#[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
/// A request builder for [Storage::open_appendable_object][crate::client::Storage::open_appendable_object].
///
/// # Example
/// ```
/// use google_cloud_storage::client::Storage;
/// # use google_cloud_storage::builder::storage::OpenAppendableObject;
/// async fn sample(client: &Storage) -> anyhow::Result<()> {
///     let builder: OpenAppendableObject = client
///         .open_appendable_object("projects/_/buckets/my-bucket", "my-object");
///     let mut writer = builder
///         .send()
///         .await?;
///     writer.append(bytes::Bytes::from("hello")).await?;
///     writer.finalize().await?;
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct OpenAppendableObject<S = crate::storage::transport::Storage> {
    stub: Arc<S>,
    request: OpenAppendableObjectRequest,
    options: RequestOptions,
}

impl<S> OpenAppendableObject<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    /// Sends the request, returning an appendable object writer.
    ///
    /// Example:
    /// ```ignore
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?;
    /// writer.append(bytes::Bytes::from("world")).await?;
    /// writer.finalize().await?;
    /// # Ok(()) }
    /// ```
    pub async fn send(self) -> Result<AppendableObjectWriter> {
        self.stub
            .open_appendable_object(self.request, self.options)
            .await
    }
}

impl<S> OpenAppendableObject<S> {
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
        let request = OpenAppendableObjectRequest {
            spec: crate::model::WriteObjectSpec::default()
                .set_appendable(true)
                .set_resource(
                    crate::model::Object::new()
                        .set_bucket(bucket.into())
                        .set_name(object.into()),
                ),
            params: None,
        };
        Self {
            request,
            options,
            stub,
        }
    }

    /// Set a [request precondition] making the operation conditional on whether
    /// the object's current generation matches the given value. Setting to 0
    /// makes the operation succeed only if there are no live versions of the
    /// object.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_generation_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_generation_match<V>(mut self, v: V) -> Self
    where
        V: Into<i64>,
    {
        self.request.spec.if_generation_match = Some(v.into());
        self
    }

    /// Set a [request precondition] making the operation conditional on whether
    /// the object's live generation does not match the given value. If no live
    /// object exists, the precondition fails. Setting to 0 makes the operation
    /// succeed only if there is a live version of the object.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_generation_not_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_generation_not_match<V>(mut self, v: V) -> Self
    where
        V: Into<i64>,
    {
        self.request.spec.if_generation_not_match = Some(v.into());
        self
    }

    /// Set a [request precondition] making the operation conditional on whether
    /// the object's current metageneration matches the given value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_metageneration_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_metageneration_match<V>(mut self, v: V) -> Self
    where
        V: Into<i64>,
    {
        self.request.spec.if_metageneration_match = Some(v.into());
        self
    }

    /// Set a [request precondition] making the operation conditional on whether
    /// the object's current metageneration does not match the given value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_metageneration_not_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_metageneration_not_match<V>(mut self, v: V) -> Self
    where
        V: Into<i64>,
    {
        self.request.spec.if_metageneration_not_match = Some(v.into());
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
    /// use google_cloud_gax::retry_policy::RetryPolicyExt;
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_retry_policy(
    ///         RetryableErrors
    ///             .with_attempt_limit(5)
    ///             .with_time_limit(Duration::from_secs(10)),
    ///     )
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_policy<V: Into<google_cloud_gax::retry_policy::RetryPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
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
    /// use google_cloud_gax::exponential_backoff::ExponentialBackoff;
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_backoff_policy(ExponentialBackoff::default())
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_backoff_policy<V: Into<google_cloud_gax::backoff_policy::BackoffPolicyArg>>(
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
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_retry_throttler(adhoc_throttler())
    ///     .send()
    ///     .await?;
    /// fn adhoc_throttler() -> google_cloud_gax::retry_throttler::SharedRetryThrottler {
    ///     # panic!();
    /// }
    /// # Ok(()) }
    /// ```
    pub fn with_retry_throttler<V: Into<google_cloud_gax::retry_throttler::RetryThrottlerArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_throttler = v.into().into();
        self
    }

    /// Configure per-attempt timeout.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use std::time::Duration;
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_attempt_timeout(Duration::from_secs(120))
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// The Cloud Storage client library times out `open_appendable_object()` attempts by
    /// default (with a 60s timeout). Applications may want to set a different
    /// value depending on how they are deployed.
    ///
    /// Note that the per-attempt timeout is subject to the overall retry loop
    /// time limits (if any). The effective timeout for each attempt is the
    /// smallest of (a) the per-attempt timeout, and (b) the remaining time in
    /// the retry loop.
    pub fn with_attempt_timeout(mut self, v: Duration) -> Self {
        self.options.set_bidi_attempt_timeout(v);
        self
    }

    /// Sets the `User-Agent` header for this request.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_user_agent("my-app/1.0.0")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.options.user_agent = Some(user_agent.into());
        self
    }

    /// Sets the project that will be billed for this request.
    ///
    /// Required for [Requester Pays] buckets. The value overrides any
    /// `quota_project_id` configured on the credentials; the credential-level
    /// header is suppressed for this RPC.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_quota_project("my-billing-project")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [Requester Pays]: https://cloud.google.com/storage/docs/requester-pays
    pub fn with_quota_project(mut self, project: impl Into<String>) -> Self {
        self.options.set_quota_project(project);
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
    /// ```
    /// # use google_cloud_storage::client::Storage;
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
    /// ```
    /// # use google_cloud_storage::client::Storage;
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
    /// ```
    /// # use google_cloud_storage::client::Storage;
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
    /// ```
    /// # use google_cloud_storage::client::Storage;
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
    /// ```
    /// # use google_cloud_storage::client::Storage;
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

    /// Sets the [custom metadata] for the new object.
    ///
    /// This field is typically set to annotate the object with
    /// application-specific metadata.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
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
    /// let mut writer = client
    ///     .open_appendable_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_kms_key("projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [Customer-managed encryption key]: https://cloud.google.com/storage/docs/encryption/customer-managed-keys
    pub fn set_kms_key<V>(mut self, v: V) -> Self
    where
        V: Into<String>,
    {
        self.mut_resource().kms_key = v.into();
        self
    }

    /// Sets the object custom contexts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
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
    pub fn set_contexts<V>(mut self, v: V) -> Self
    where
        V: Into<crate::model::ObjectContexts>,
    {
        self.mut_resource().contexts = Some(v.into());
        self
    }

    fn mut_resource(&mut self) -> &mut Object {
        self.request.spec.resource.get_or_insert_with(Object::new)
    }
}

#[cfg(test)]
#[cfg(google_cloud_unstable_storage_bidi)]
mod tests {
    use super::*;
    use crate::client::Storage;
    use crate::request_options::RequestOptions;
    use anyhow::Result;
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use static_assertions::assert_impl_all;
    use std::sync::Arc;

    #[derive(Debug)]
    struct StorageStub;
    impl crate::stub::Storage for StorageStub {}

    const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";
    const OBJECT_NAME: &str = "test-object";

    #[tokio::test]
    async fn traits() -> Result<()> {
        assert_impl_all!(OpenAppendableObject: Clone, std::fmt::Debug, Send, Sync);
        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        fn need_send<T: Send>(_val: &T) {}
        fn need_static<T: 'static>(_val: &T) {}

        let open = client.open_appendable_object(BUCKET_NAME, OBJECT_NAME);
        need_static(&open);
        let fut = client
            .open_appendable_object(BUCKET_NAME, OBJECT_NAME)
            .send();
        need_send(&fut);
        need_static(&fut);
        Ok(())
    }

    #[tokio::test]
    async fn attributes() -> Result<()> {
        use crate::model::{ObjectContexts, ObjectCustomContextPayload};
        let options = RequestOptions::new();
        let time = wkt::Timestamp::default();
        let builder = OpenAppendableObject::new(
            Arc::new(StorageStub),
            BUCKET_NAME.to_string(),
            OBJECT_NAME.to_string(),
            options,
        )
        .set_if_generation_match(234)
        .set_if_generation_not_match(345)
        .set_if_metageneration_match(456)
        .set_if_metageneration_not_match(567)
        .set_cache_control("public; max-age=7200")
        .set_content_disposition("attachment; filename=test.txt")
        .set_content_encoding("gzip")
        .set_content_language("en")
        .set_content_type("text/plain")
        .set_custom_time(time)
        .set_kms_key(
            "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key",
        )
        .set_metadata([("test-only", "true".to_string())])
        .set_contexts(ObjectContexts::new().set_custom([(
            "context-key",
            ObjectCustomContextPayload::new().set_value("context-value"),
        )]));

        assert_eq!(builder.request.spec.appendable, Some(true));
        assert_eq!(builder.request.spec.if_generation_match, Some(234));
        assert_eq!(builder.request.spec.if_generation_not_match, Some(345));
        assert_eq!(builder.request.spec.if_metageneration_match, Some(456));
        assert_eq!(builder.request.spec.if_metageneration_not_match, Some(567));

        let r = builder.request.spec.resource.as_ref().unwrap();
        assert_eq!(r.bucket, BUCKET_NAME);
        assert_eq!(r.name, OBJECT_NAME);
        assert_eq!(r.cache_control, "public; max-age=7200");
        assert_eq!(r.content_disposition, "attachment; filename=test.txt");
        assert_eq!(r.content_encoding, "gzip");
        assert_eq!(r.content_language, "en");
        assert_eq!(r.content_type, "text/plain");
        assert_eq!(r.custom_time, Some(time));
        assert_eq!(
            r.kms_key,
            "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key"
        );
        assert_eq!(
            r.metadata.get("test-only").map(|s: &String| s.as_str()),
            Some("true")
        );
        assert_eq!(
            r.contexts
                .as_ref()
                .unwrap()
                .custom
                .get("context-key")
                .unwrap(),
            &ObjectCustomContextPayload::new().set_value("context-value")
        );
        Ok(())
    }

    #[tokio::test]
    async fn options() -> Result<()> {
        use crate::retry_policy::RetryableErrors;
        use google_cloud_gax::exponential_backoff::ExponentialBackoff;
        let builder = crate::client::Storage::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?
            .open_appendable_object(BUCKET_NAME, OBJECT_NAME)
            .with_attempt_timeout(std::time::Duration::from_secs(12))
            .with_user_agent("test-agent")
            .with_quota_project("test-project")
            .with_retry_policy(RetryableErrors)
            .with_backoff_policy(ExponentialBackoff::default());

        assert_eq!(
            builder.options.bidi_attempt_timeout,
            std::time::Duration::from_secs(12)
        );
        assert_eq!(builder.options.user_agent.as_deref(), Some("test-agent"));
        assert_eq!(
            builder.options.quota_project.as_deref(),
            Some("test-project")
        );
        Ok(())
    }
}
