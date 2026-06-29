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
use crate::model_ext::{KeyAes256, ReopenAppendableObjectRequest};
use crate::request_options::RequestOptions;
use std::sync::Arc;
use std::time::Duration;

#[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
/// A request builder for [Storage::reopen_appendable_object][crate::client::Storage::reopen_appendable_object].
///
/// # Example
/// ```
/// use google_cloud_storage::client::Storage;
/// # use google_cloud_storage::builder::storage::ReopenAppendableObject;
/// async fn sample(client: &Storage) -> anyhow::Result<()> {
///     let builder: ReopenAppendableObject = client
///         .reopen_appendable_object("projects/_/buckets/my-bucket", "my-object", 123456);
///     let mut writer = builder
///         .send()
///         .await?;
///     writer.append(bytes::Bytes::from("hello")).await?;
///     writer.finalize().await?;
///     Ok(())
/// }
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
    /// # use google_cloud_storage::{model_ext::KeyAes256, client::Storage};
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

    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn set_if_metageneration_match(mut self, v: i64) -> Self {
        self.request.if_metageneration_match = Some(v);
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    pub fn set_if_metageneration_not_match(mut self, v: i64) -> Self {
        self.request.if_metageneration_not_match = Some(v);
        self
    }

    /// Explicitly provide the routing token from the previous operation.
    pub fn set_routing_token(mut self, token: impl Into<String>) -> Self {
        self.request.routing_token = Some(token.into());
        self
    }

    /// Explicitly provide the write handle from the previous operation.
    pub fn set_write_handle(mut self, handle: bytes::Bytes) -> Self {
        self.request.write_handle = Some(handle);
        self
    }

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    pub fn set_key(mut self, v: KeyAes256) -> Self {
        self.request.params = Some(crate::model::CommonObjectRequestParams::from(v));
        self
    }

    /// The retry policy used for this request.
    pub fn with_retry_policy<V: Into<google_cloud_gax::retry_policy::RetryPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_policy = v.into().into();
        self
    }

    /// The backoff policy used for this request.
    pub fn with_backoff_policy<V: Into<google_cloud_gax::backoff_policy::BackoffPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.backoff_policy = v.into().into();
        self
    }

    /// The retry throttler used for this request.
    pub fn with_retry_throttler<V: Into<google_cloud_gax::retry_throttler::RetryThrottlerArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_throttler = v.into().into();
        self
    }

    /// Configure per-attempt timeout.
    pub fn with_attempt_timeout(mut self, v: Duration) -> Self {
        self.options.set_bidi_attempt_timeout(v);
        self
    }

    /// Sets the `User-Agent` header for this request.
    pub fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.options.user_agent = Some(user_agent.into());
        self
    }

    /// Sets the project that will be billed for this request.
    pub fn with_quota_project(mut self, project: impl Into<String>) -> Self {
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

    #[tokio::test]
    async fn attributes() -> Result<()> {
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
        Ok(())
    }
}
