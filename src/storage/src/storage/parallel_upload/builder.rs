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
use crate::model::{Object, ObjectAccessControl, ObjectContexts, object::Retention};
use crate::model_ext::{CreateObjectWriteSessionRequest, KeyAes256};
use crate::parallel_upload_session::ParallelUploadSession;
use crate::request_options::RequestOptions;
use std::sync::Arc;

/// A request builder for [Storage::parallel_upload][crate::client::Storage::parallel_upload].
#[cfg_attr(docsrs, doc(cfg(google_cloud_unstable_storage_parallel_upload)))]
#[derive(Clone, Debug)]
pub struct ParallelUpload<S = crate::storage::transport::Storage> {
    pub(crate) stub: Arc<S>,
    pub(crate) request: CreateObjectWriteSessionRequest,
    pub(crate) max_concurrency: Option<usize>,
    pub(crate) enable_hedging: bool,
    pub(crate) options: RequestOptions,
}

impl<S> ParallelUpload<S> {
    pub(crate) fn new<B, O>(stub: Arc<S>, bucket: B, object: O, options: RequestOptions) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        let request = CreateObjectWriteSessionRequest {
            resource: Some(
                Object::new()
                    .set_bucket(bucket.into())
                    .set_name(object.into()),
            ),
            ..CreateObjectWriteSessionRequest::default()
        };
        Self {
            stub,
            request,
            max_concurrency: None,
            enable_hedging: false,
            options,
        }
    }

    /// Sets the access control list for the new object.
    pub fn set_acl<I, V>(mut self, v: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<ObjectAccessControl>,
    {
        self.mut_resource().acl = v.into_iter().map(|a| a.into()).collect();
        self
    }

    /// Sets the `Cache-Control` directive for the new object.
    pub fn set_cache_control<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().cache_control = v.into();
        self
    }

    /// Sets the `Content-Disposition` header for the new object.
    pub fn set_content_disposition<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_disposition = v.into();
        self
    }

    /// Sets the `Content-Encoding` header for the object data.
    pub fn set_content_encoding<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_encoding = v.into();
        self
    }

    /// Sets the `Content-Language` header for the new object.
    pub fn set_content_language<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_language = v.into();
        self
    }

    /// Sets the `Content-Type` header for the new object.
    pub fn set_content_type<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().content_type = v.into();
        self
    }

    /// Sets the object custom contexts.
    pub fn set_contexts<V: Into<ObjectContexts>>(mut self, v: V) -> Self {
        self.mut_resource().contexts = Some(v.into());
        self
    }

    /// Sets the custom time for the new object.
    pub fn set_custom_time<V: Into<wkt::Timestamp>>(mut self, v: V) -> Self {
        self.mut_resource().custom_time = Some(v.into());
        self
    }

    /// Sets the event-based hold flag for the new object.
    pub fn set_event_based_hold<V: Into<bool>>(mut self, v: V) -> Self {
        self.mut_resource().event_based_hold = Some(v.into());
        self
    }

    /// Sets the customer-supplied AES-256 encryption key for this object.
    pub fn set_key(mut self, v: KeyAes256) -> Self {
        self.request.params = Some(v.into());
        self
    }

    /// Sets the resource name of the Cloud KMS key used to encrypt this object.
    pub fn set_kms_key<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().kms_key = v.into();
        self
    }

    /// Sets the user-provided custom metadata for the new object.
    pub fn set_metadata<I, K, V>(mut self, i: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.mut_resource().metadata = i.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        self
    }

    /// Configures this object to use one of the predefined ACLs.
    pub fn set_predefined_acl<V: Into<String>>(mut self, v: V) -> Self {
        self.request.predefined_acl = v.into();
        self
    }

    /// Sets the preferred segment size hint in bytes.
    pub fn set_preferred_segment_size_bytes<V: Into<i64>>(mut self, v: V) -> Self {
        self.request.preferred_segment_size_bytes = v.into();
        self
    }

    /// Sets the retention configuration for the new object.
    pub fn set_retention<V: Into<Retention>>(mut self, v: V) -> Self {
        self.mut_resource().retention = Some(v.into());
        self
    }

    /// Sets the storage class for the new object.
    pub fn set_storage_class<V: Into<String>>(mut self, v: V) -> Self {
        self.mut_resource().storage_class = v.into();
        self
    }

    /// Sets the temporary hold flag for the new object.
    pub fn set_temporary_hold<V: Into<bool>>(mut self, v: V) -> Self {
        self.mut_resource().temporary_hold = v.into();
        self
    }

    /// Sets the backoff policy used for this upload.
    pub fn with_backoff_policy<V: Into<google_cloud_gax::backoff_policy::BackoffPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.backoff_policy = v.into().into();
        self
    }

    /// Enables or disables hedging for segment uploads.
    pub fn with_enable_hedging(mut self, enable_hedging: bool) -> Self {
        self.enable_hedging = enable_hedging;
        self
    }

    /// Sets the maximum number of concurrent segment uploads.
    pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = Some(max_concurrency);
        self
    }

    /// Sets the project that will be billed for this upload.
    pub fn with_quota_project<V: Into<String>>(mut self, project: V) -> Self {
        self.options.set_quota_project(project);
        self
    }

    /// Sets the retry policy used for this upload.
    pub fn with_retry_policy<V: Into<google_cloud_gax::retry_policy::RetryPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_policy = v.into().into();
        self
    }

    /// Sets the retry throttler used for this upload.
    pub fn with_retry_throttler<V: Into<google_cloud_gax::retry_throttler::RetryThrottlerArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.options.retry_throttler = v.into().into();
        self
    }

    /// Sets the `User-Agent` header for this upload.
    pub fn with_user_agent<V: Into<String>>(mut self, user_agent: V) -> Self {
        self.options.user_agent = Some(user_agent.into());
        self
    }

    fn mut_resource(&mut self) -> &mut Object {
        self.request.resource.get_or_insert_with(Object::new)
    }
}

impl<S> ParallelUpload<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    /// Creates a new parallel upload session without uploading payload bytes yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the session creation RPC fails.
    ///
    /// # Panics
    ///
    /// Panics with the default transport until session creation is implemented.
    pub async fn create_session(self) -> Result<ParallelUploadSession> {
        // `max_concurrency` only applies to whole-object uploads;
        // TODO: plumb `enable_hedging` via `RequestOptions` once hedging is implemented.
        let _ = (self.max_concurrency, self.enable_hedging);
        self.stub
            .create_object_write_session(self.request, self.options)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Storage;
    use crate::model::{CommonObjectRequestParams, ObjectCustomContextPayload, object::retention};
    use crate::storage::client::tests::{MockBackoffPolicy, MockRetryPolicy, MockRetryThrottler};
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use static_assertions::assert_impl_all;

    const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";
    const OBJECT_NAME: &str = "test-object";

    #[derive(Debug)]
    struct StorageStub;
    impl crate::stub::Storage for StorageStub {}

    #[tokio::test]
    async fn traits_and_defaults() -> anyhow::Result<()> {
        fn assert_send<T: Send>(_: &T) {}
        fn assert_static<T: 'static>(_: &T) {}

        assert_impl_all!(ParallelUpload: Clone, std::fmt::Debug, Send, Sync);

        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        let builder = client.parallel_upload(BUCKET_NAME, OBJECT_NAME);
        assert_static(&builder);

        let resource = builder.request.resource.as_ref().expect("resource set");
        assert_eq!(resource.bucket, BUCKET_NAME);
        assert_eq!(resource.name, OBJECT_NAME);
        assert!(builder.request.predefined_acl.is_empty());
        assert_eq!(builder.request.params, None);
        assert_eq!(builder.request.preferred_segment_size_bytes, 0);
        assert_eq!(builder.max_concurrency, None);
        assert!(!builder.enable_hedging);

        let create_fut = builder.create_session();
        assert_send(&create_fut);
        assert_static(&create_fut);
        drop(create_fut);

        Ok(())
    }

    #[test]
    fn attributes() -> anyhow::Result<()> {
        let key = KeyAes256::new(&[0x42; 32])?;
        let custom_time = wkt::Timestamp::try_from("2025-07-07T18:11:00Z")?;
        let retain_until = wkt::Timestamp::try_from("2035-07-07T18:14:00Z")?;

        let builder = ParallelUpload::new(
            Arc::new(StorageStub),
            BUCKET_NAME,
            OBJECT_NAME,
            RequestOptions::new(),
        )
        .set_acl([ObjectAccessControl::new()
            .set_entity("allAuthenticatedUsers")
            .set_role("READER")])
        .set_cache_control("public; max-age=7200")
        .set_content_disposition("inline")
        .set_content_encoding("gzip")
        .set_content_language("en")
        .set_content_type("text/plain")
        .set_contexts(ObjectContexts::new().set_custom([(
            "context-key",
            ObjectCustomContextPayload::new().set_value("context-value"),
        )]))
        .set_custom_time(custom_time)
        .set_event_based_hold(true)
        .set_key(key.clone())
        .set_kms_key(
            "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key",
        )
        .set_metadata([("k0", "v0"), ("k1", "v1")])
        .set_predefined_acl("private")
        .set_preferred_segment_size_bytes(64 * 1024 * 1024)
        .set_retention(
            Retention::new()
                .set_mode(retention::Mode::Locked)
                .set_retain_until_time(retain_until),
        )
        .set_storage_class("ARCHIVE")
        .set_temporary_hold(true);

        assert_eq!(builder.request.predefined_acl, "private");
        assert_eq!(
            builder.request.params,
            Some(CommonObjectRequestParams::from(key))
        );
        assert_eq!(
            builder.request.preferred_segment_size_bytes,
            64 * 1024 * 1024
        );

        let want_resource = Object::new()
            .set_bucket(BUCKET_NAME)
            .set_name(OBJECT_NAME)
            .set_acl([ObjectAccessControl::new()
                .set_entity("allAuthenticatedUsers")
                .set_role("READER")])
            .set_cache_control("public; max-age=7200")
            .set_content_disposition("inline")
            .set_content_encoding("gzip")
            .set_content_language("en")
            .set_content_type("text/plain")
            .set_contexts(ObjectContexts::new().set_custom([(
                "context-key",
                ObjectCustomContextPayload::new().set_value("context-value"),
            )]))
            .set_custom_time(custom_time)
            .set_event_based_hold(true)
            .set_kms_key(
                "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key",
            )
            .set_metadata([("k0", "v0"), ("k1", "v1")])
            .set_retention(
                Retention::new()
                    .set_mode(retention::Mode::Locked)
                    .set_retain_until_time(retain_until),
            )
            .set_storage_class("ARCHIVE")
            .set_temporary_hold(true);

        assert_eq!(builder.request.resource, Some(want_resource));
        Ok(())
    }

    #[tokio::test]
    async fn options() -> anyhow::Result<()> {
        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .with_retry_policy(google_cloud_gax::retry_policy::NeverRetry)
            .with_backoff_policy(
                google_cloud_gax::exponential_backoff::ExponentialBackoff::default(),
            )
            .with_retry_throttler(google_cloud_gax::retry_throttler::CircuitBreaker::new(
                1000, 250, 10,
            )?)
            .build()
            .await?;

        let default_builder = client.parallel_upload(BUCKET_NAME, OBJECT_NAME);
        let default_gax_debug = format!("{:?}", default_builder.options.gax());
        assert!(
            default_gax_debug.contains("NeverRetry"),
            "{default_gax_debug}"
        );
        assert!(
            default_gax_debug.contains("ExponentialBackoff"),
            "{default_gax_debug}"
        );
        assert!(
            default_gax_debug.contains("CircuitBreaker"),
            "{default_gax_debug}"
        );

        let builder = default_builder
            .with_max_concurrency(8)
            .with_enable_hedging(true)
            .with_user_agent("test-agent")
            .with_quota_project("override-project")
            .with_retry_policy(MockRetryPolicy::new())
            .with_backoff_policy(MockBackoffPolicy::new())
            .with_retry_throttler(MockRetryThrottler::new());

        assert_eq!(builder.max_concurrency, Some(8));
        assert!(builder.enable_hedging);
        assert_eq!(builder.options.user_agent.as_deref(), Some("test-agent"));
        assert_eq!(
            builder.options.quota_project.as_deref(),
            Some("override-project")
        );

        let gax_debug = format!("{:?}", builder.options.gax());
        assert!(gax_debug.contains("MockRetryPolicy"), "{gax_debug}");
        assert!(gax_debug.contains("MockBackoffPolicy"), "{gax_debug}");
        assert!(gax_debug.contains("MockRetryThrottler"), "{gax_debug}");

        Ok(())
    }

    #[tokio::test]
    async fn create_session_forwards_request_and_options() -> anyhow::Result<()> {
        mockall::mock! {
            #[derive(Debug)]
            StorageStub {}
            impl crate::stub::Storage for StorageStub {
                async fn create_object_write_session(
                    &self,
                    request: CreateObjectWriteSessionRequest,
                    options: RequestOptions,
                ) -> Result<ParallelUploadSession>;
            }
        }

        #[derive(Debug)]
        struct DummySession;
        impl crate::stub::ParallelUploadSession for DummySession {
            fn name(&self) -> &str {
                "projects/_/buckets/test-bucket/objectWriteSessions/s-1"
            }
        }

        let key = KeyAes256::new(&[0x42; 32])?;
        let custom_time = wkt::Timestamp::try_from("2025-07-07T18:11:00Z")?;
        let retain_until = wkt::Timestamp::try_from("2035-07-07T18:14:00Z")?;

        let want_request = CreateObjectWriteSessionRequest::new()
            .set_predefined_acl("private")
            .set_params(CommonObjectRequestParams::from(key.clone()))
            .set_preferred_segment_size_bytes(64 * 1024 * 1024)
            .set_resource(
                Object::new()
                    .set_bucket(BUCKET_NAME)
                    .set_name(OBJECT_NAME)
                    .set_acl([ObjectAccessControl::new()
                        .set_entity("allAuthenticatedUsers")
                        .set_role("READER")])
                    .set_cache_control("public; max-age=7200")
                    .set_content_disposition("inline")
                    .set_content_encoding("gzip")
                    .set_content_language("en")
                    .set_content_type("text/plain")
                    .set_contexts(ObjectContexts::new().set_custom([(
                        "context-key",
                        ObjectCustomContextPayload::new().set_value("context-value"),
                    )]))
                    .set_custom_time(custom_time)
                    .set_event_based_hold(true)
                    .set_kms_key(
                        "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key",
                    )
                    .set_metadata([("k0", "v0"), ("k1", "v1")])
                    .set_retention(
                        Retention::new()
                            .set_mode(retention::Mode::Locked)
                            .set_retain_until_time(retain_until),
                    )
                    .set_storage_class("ARCHIVE")
                    .set_temporary_hold(true),
            );

        let mut mock = MockStorageStub::new();
        mock.expect_create_object_write_session()
            .times(1)
            .returning(move |got_request, got_options| {
                assert_eq!(got_request, want_request);
                assert_eq!(got_options.user_agent.as_deref(), Some("test-agent"));
                assert_eq!(
                    got_options.quota_project.as_deref(),
                    Some("override-project")
                );
                let gax_debug = format!("{:?}", got_options.gax());
                assert!(gax_debug.contains("MockRetryPolicy"), "{gax_debug}");
                assert!(gax_debug.contains("MockBackoffPolicy"), "{gax_debug}");
                assert!(gax_debug.contains("MockRetryThrottler"), "{gax_debug}");
                Ok(ParallelUploadSession::new(DummySession))
            });

        let client = Storage::from_stub(mock);
        let session = client
            .parallel_upload(BUCKET_NAME, OBJECT_NAME)
            .set_acl([ObjectAccessControl::new()
                .set_entity("allAuthenticatedUsers")
                .set_role("READER")])
            .set_cache_control("public; max-age=7200")
            .set_content_disposition("inline")
            .set_content_encoding("gzip")
            .set_content_language("en")
            .set_content_type("text/plain")
            .set_contexts(ObjectContexts::new().set_custom([(
                "context-key",
                ObjectCustomContextPayload::new().set_value("context-value"),
            )]))
            .set_custom_time(custom_time)
            .set_event_based_hold(true)
            .set_key(key)
            .set_kms_key(
                "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key",
            )
            .set_metadata([("k0", "v0"), ("k1", "v1")])
            .set_predefined_acl("private")
            .set_preferred_segment_size_bytes(64 * 1024 * 1024)
            .set_retention(
                Retention::new()
                    .set_mode(retention::Mode::Locked)
                    .set_retain_until_time(retain_until),
            )
            .set_storage_class("ARCHIVE")
            .set_temporary_hold(true)
            .with_max_concurrency(8)
            .with_enable_hedging(true)
            .with_user_agent("test-agent")
            .with_quota_project("override-project")
            .with_retry_policy(MockRetryPolicy::new())
            .with_backoff_policy(MockBackoffPolicy::new())
            .with_retry_throttler(MockRetryThrottler::new())
            .create_session()
            .await?;

        assert_eq!(
            session.name(),
            "projects/_/buckets/test-bucket/objectWriteSessions/s-1"
        );
        Ok(())
    }
}
