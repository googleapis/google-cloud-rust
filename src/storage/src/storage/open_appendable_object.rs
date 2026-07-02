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
use crate::model_ext::{KeyAes256, OpenAppendableObjectRequest};
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
    /// # use google_cloud_storage::{model_ext::KeyAes256, client::Storage};
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

    /// Makes the operation conditional on whether the object's live generation
    /// matches the given value.
    pub fn set_if_generation_match(mut self, v: i64) -> Self {
        self.request.spec.if_generation_match = Some(v);
        self
    }

    /// Makes the operation conditional on whether the object's live generation
    /// does not match the given value. If no live object exists, the precondition
    /// fails.
    pub fn set_if_generation_not_match(mut self, v: i64) -> Self {
        self.request.spec.if_generation_not_match = Some(v);
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn set_if_metageneration_match(mut self, v: i64) -> Self {
        self.request.spec.if_metageneration_match = Some(v);
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    pub fn set_if_metageneration_not_match(mut self, v: i64) -> Self {
        self.request.spec.if_metageneration_not_match = Some(v);
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

    fn mut_resource(&mut self) -> &mut crate::model::Object {
        if self.request.spec.resource.is_none() {
            self.request.spec.resource = Some(crate::model::Object::new());
        }
        self.request.spec.resource.as_mut().unwrap()
    }

    /// Sets the ACL for the new object.
    pub fn set_acl<I, V>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<crate::model::ObjectAccessControl>,
    {
        self.mut_resource().acl = v.into_iter().map(|a| a.into()).collect();
        self
    }

    /// Sets the [cache control] for the new object.
    pub fn set_cache_control<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().cache_control = v.into();
        self
    }

    /// Sets the [content disposition] for the new object.
    pub fn set_content_disposition<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_disposition = v.into();
        self
    }

    /// Sets the [content encoding] for the object data.
    pub fn set_content_encoding<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_encoding = v.into();
        self
    }

    /// Sets the [content language] for the new object.
    pub fn set_content_language<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_language = v.into();
        self
    }

    /// Sets the [content type] for the new object.
    pub fn set_content_type<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_type = v.into();
        self
    }

    /// Sets the [custom time] for the new object.
    pub fn set_custom_time<V: Into<wkt::Timestamp>>(mut self, v: V) -> Self {
        self.mut_resource().custom_time = Some(v.into());
        self
    }

    /// Sets the [event based hold] flag for the new object.
    pub fn set_event_based_hold<V: Into<bool>>(mut self, v: V) -> Self {
        self.mut_resource().event_based_hold = Some(v.into());
        self
    }

    /// Sets the [custom metadata] for the new object.
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
    pub fn set_retention<V>(mut self, v: V) -> Self
    where
        V: Into<crate::model::object::Retention>,
    {
        self.mut_resource().retention = Some(v.into());
        self
    }

    /// Sets the [storage class] for the new object.
    pub fn set_storage_class<V>(mut self, v: V) -> Self
    where
        V: Into<String>,
    {
        self.mut_resource().storage_class = v.into();
        self
    }

    /// Sets the [temporary hold] flag for the new object.
    pub fn set_temporary_hold<V: Into<bool>>(mut self, v: V) -> Self {
        self.mut_resource().temporary_hold = v.into();
        self
    }

    /// Sets the resource name of the [Customer-managed encryption key] for this
    /// object.
    pub fn set_kms_key<V>(mut self, v: V) -> Self
    where
        V: Into<String>,
    {
        self.mut_resource().kms_key = v.into();
        self
    }

    /// Configure this object to use one of the [predefined ACLs].
    pub fn set_predefined_acl<V>(mut self, v: V) -> Self
    where
        V: Into<String>,
    {
        self.request.spec.predefined_acl = v.into();
        self
    }

    /// Sets the object custom contexts.
    pub fn set_contexts<V>(mut self, v: V) -> Self
    where
        V: Into<crate::model::ObjectContexts>,
    {
        self.mut_resource().contexts = Some(v.into());
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
        let options = RequestOptions::new();
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
        .set_content_type("text/plain");

        assert_eq!(builder.request.spec.if_generation_match, Some(234));
        assert_eq!(builder.request.spec.if_generation_not_match, Some(345));
        assert_eq!(builder.request.spec.if_metageneration_match, Some(456));
        assert_eq!(builder.request.spec.if_metageneration_not_match, Some(567));
        assert_eq!(
            builder.request.spec.resource.as_ref().unwrap().content_type,
            "text/plain"
        );
        Ok(())
    }
}
