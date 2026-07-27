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
use crate::model_ext::ReopenAppendableObjectRequest;
use crate::request_options::RequestOptions;
use std::sync::Arc;
use std::time::Duration;

#[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
/// A request builder for [Storage::reopen_appendable_object][crate::client::Storage::reopen_appendable_object].
///
/// # Example
/// ```
/// # use google_cloud_storage::client::Storage;
/// # async fn sample(client: &Storage) -> anyhow::Result<()> {
/// let mut writer = client
///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
///     .send()
///     .await?;
/// writer.append(bytes::Bytes::from("hello")).await?;
/// writer.finalize().await?;
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct ReopenAppendableObject<S = crate::storage::transport::Storage> {
    stub: Arc<S>,
    request: ReopenAppendableObjectRequest,
    options: RequestOptions,
}

impl<S> ReopenAppendableObject<S>
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
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
    ///     .send()
    ///     .await?;
    /// writer.append(bytes::Bytes::from("world")).await?;
    /// writer.finalize().await?;
    /// # Ok(()) }
    /// ```
    pub async fn send(self) -> Result<AppendableObjectWriter> {
        self.stub
            .reopen_appendable_object(self.request, self.options)
            .await
    }
}

impl<S> ReopenAppendableObject<S> {
    pub(crate) fn new<B, O>(
        stub: std::sync::Arc<S>,
        bucket: B,
        object: O,
        generation: i64,
        options: RequestOptions,
    ) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        let request = ReopenAppendableObjectRequest {
            bucket: bucket.into(),
            object: object.into(),
            generation,
            if_metageneration_match: None,
            if_metageneration_not_match: None,
            routing_token: None,
            write_handle: None,
            params: None,
        };
        Self {
            request,
            options,
            stub,
        }
    }

    /// Set a [request precondition] making the operation conditional on whether
    /// the object's current metageneration matches the given value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
    ///     .set_if_metageneration_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Note that this precondition is ignored if a `write_handle` is also set.
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_metageneration_match<V>(mut self, v: V) -> Self
    where
        V: Into<i64>,
    {
        self.request.if_metageneration_match = Some(v.into());
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
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
    ///     .set_if_metageneration_not_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Note that this precondition is ignored if a `write_handle` is also set.
    ///
    /// [request precondition]: https://cloud.google.com/storage/docs/request-preconditions
    pub fn set_if_metageneration_not_match<V>(mut self, v: V) -> Self
    where
        V: Into<i64>,
    {
        self.request.if_metageneration_not_match = Some(v.into());
        self
    }

    /// Explicitly provide the routing token from the previous operation.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
    ///     .set_routing_token("my-token")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_routing_token<V>(mut self, token: V) -> Self
    where
        V: Into<String>,
    {
        self.request.routing_token = Some(token.into());
        self
    }

    /// Explicitly provide the write handle from the previous operation.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut writer = client
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
    ///     .set_write_handle(bytes::Bytes::from("my-handle"))
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// Note that setting a `write_handle` will cause metageneration preconditions
    /// (`if_metageneration_match` and `if_metageneration_not_match`) to be ignored.
    pub fn set_write_handle<V>(mut self, handle: V) -> Self
    where
        V: Into<bytes::Bytes>,
    {
        self.request.write_handle = Some(handle.into());
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
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
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
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
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
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
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
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
    ///     .with_attempt_timeout(Duration::from_secs(120))
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// The Cloud Storage client library times out `reopen_appendable_object()` attempts by
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
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
    ///     .with_user_agent("my-app/1.0.0")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_user_agent<V>(mut self, user_agent: V) -> Self
    where
        V: Into<String>,
    {
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
    ///     .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456)
    ///     .with_quota_project("my-billing-project")
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [Requester Pays]: https://cloud.google.com/storage/docs/requester-pays
    pub fn with_quota_project<V>(mut self, project: V) -> Self
    where
        V: Into<String>,
    {
        self.options.set_quota_project(project);
        self
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
        assert_impl_all!(ReopenAppendableObject: Clone, std::fmt::Debug, Send, Sync);
        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        fn need_send<T: Send>(_val: &T) {}
        fn need_static<T: 'static>(_val: &T) {}

        let open = client.reopen_appendable_object(BUCKET_NAME, OBJECT_NAME, 123456);
        need_static(&open);
        let fut = client
            .reopen_appendable_object(BUCKET_NAME, OBJECT_NAME, 123456)
            .send();
        need_send(&fut);
        need_static(&fut);
        Ok(())
    }

    #[test]
    fn attributes() {
        let options = RequestOptions::new();
        let builder = ReopenAppendableObject::new(
            Arc::new(StorageStub),
            BUCKET_NAME.to_string(),
            OBJECT_NAME.to_string(),
            123456,
            options,
        )
        .set_if_metageneration_match(456)
        .set_if_metageneration_not_match(567)
        .set_routing_token("token")
        .set_write_handle(bytes::Bytes::from("handle"));

        assert_eq!(builder.request.if_metageneration_match, Some(456));
        assert_eq!(builder.request.if_metageneration_not_match, Some(567));
        assert_eq!(builder.request.routing_token.as_deref(), Some("token"));
        assert_eq!(
            builder.request.write_handle,
            Some(bytes::Bytes::from("handle"))
        );
    }

    #[tokio::test]
    async fn options() -> Result<()> {
        use crate::retry_policy::RetryableErrors;
        use google_cloud_gax::exponential_backoff::ExponentialBackoff;
        let client = crate::client::Storage::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let builder = client
            .reopen_appendable_object(BUCKET_NAME, OBJECT_NAME, 123456)
            .with_attempt_timeout(std::time::Duration::from_secs(12))
            .with_user_agent("test-agent")
            .with_quota_project("test-project")
            .with_retry_policy(RetryableErrors)
            .with_backoff_policy(ExponentialBackoff::default())
            .with_retry_throttler(google_cloud_gax::retry_throttler::CircuitBreaker::default());

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
