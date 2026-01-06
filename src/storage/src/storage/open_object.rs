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

use crate::Result;
use crate::model_ext::{KeyAes256, OpenObjectRequest};
use crate::object_descriptor::ObjectDescriptor;
use crate::read_resume_policy::ReadResumePolicy;
use crate::request_options::RequestOptions;
use std::sync::Arc;
use std::time::Duration;

/// A request builder for [Storage::open_object][crate::client::Storage::open_object].
///
/// # Example
/// ```
/// use google_cloud_storage::client::Storage;
/// # use google_cloud_storage::builder::storage::OpenObject;
/// async fn sample(client: &Storage) -> anyhow::Result<()> {
///     let builder: OpenObject = client
///         .open_object("projects/_/buckets/my-bucket", "my-object");
///     let descriptor = builder
///         .set_generation(123)
///         .send()
///         .await?;
///     println!("object metadata={:?}", descriptor.object());
///     // Use `descriptor` to read data from `my-object`.
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct OpenObject<S = crate::storage::transport::Storage> {
    stub: Arc<S>,
    request: OpenObjectRequest,
    options: RequestOptions,
}

impl<S> OpenObject<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    /// Sends the request, returning a new object descriptor.
    ///
    /// Example:
    /// ```ignore
    /// # use google_cloud_storage::{model_ext::KeyAes256, client::Storage};
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let open = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?;
    /// println!("object metadata={:?}", open.object());
    /// # Ok(()) }
    /// ```
    pub async fn send(self) -> Result<ObjectDescriptor> {
        let (descriptor, _) = self.stub.open_object(self.request, self.options).await?;
        Ok(descriptor)
    }
}

impl<S> OpenObject<S> {
    pub(crate) fn new(
        bucket: String,
        object: String,
        stub: Arc<S>,
        options: RequestOptions,
    ) -> Self {
        let request = OpenObjectRequest::default()
            .set_bucket(bucket)
            .set_object(object);
        Self {
            request,
            options,
            stub,
        }
    }

    /// If present, selects a specific revision of this object (as
    /// opposed to the latest version, the default).
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::read_resume_policy::{AlwaysResume, ReadResumePolicyExt};
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_generation(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_generation<T: Into<i64>>(mut self, v: T) -> Self {
        self.request = self.request.set_generation(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current generation
    /// matches the given value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::read_resume_policy::{AlwaysResume, ReadResumePolicyExt};
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_generation_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_if_generation_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request = self.request.set_if_generation_match(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's live generation
    /// does not match the given value. If no live object exists, the precondition
    /// fails.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::read_resume_policy::{AlwaysResume, ReadResumePolicyExt};
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_generation_not_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_if_generation_not_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request = self.request.set_if_generation_not_match(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::read_resume_policy::{AlwaysResume, ReadResumePolicyExt};
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_metageneration_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_if_metageneration_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request = self.request.set_if_metageneration_match(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use google_cloud_storage::read_resume_policy::{AlwaysResume, ReadResumePolicyExt};
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_if_metageneration_not_match(123456)
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn set_if_metageneration_not_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request = self.request.set_if_metageneration_not_match(v.into());
        self
    }

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    ///
    /// Example:
    /// ```
    /// # use google_cloud_storage::{model_ext::KeyAes256, client::Storage};
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let key: &[u8] = &[97; 32];
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
    ///     .set_key(KeyAes256::new(key)?)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    pub fn set_key(mut self, v: KeyAes256) -> Self {
        self.request = self
            .request
            .set_common_object_request_params(crate::model::CommonObjectRequestParams::from(v));
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
    /// use gax::retry_policy::RetryPolicyExt;
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
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
    /// use std::time::Duration;
    /// use gax::exponential_backoff::ExponentialBackoff;
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
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
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
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
    /// use google_cloud_storage::read_resume_policy::{AlwaysResume, ReadResumePolicyExt};
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_resume_policy(AlwaysResume.with_attempt_limit(3))
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_read_resume_policy<V>(mut self, v: V) -> Self
    where
        V: ReadResumePolicy + 'static,
    {
        self.options.set_read_resume_policy(std::sync::Arc::new(v));
        self
    }

    /// Configure per-attempt timeout.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// use std::time::Duration;
    /// let response = client
    ///     .open_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_attempt_timeout(Duration::from_secs(120))
    ///     .send()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// The Cloud Storage client library times out `open_object()` attempts by
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Storage;
    use crate::model::{CommonObjectRequestParams, Object};
    use crate::model_ext::tests::create_key_helper;
    use anyhow::Result;
    use auth::credentials::anonymous::Builder as Anonymous;
    use gax::retry_policy::NeverRetry;
    use http::HeaderValue;
    use static_assertions::assert_impl_all;
    use storage_grpc_mock::google::storage::v2::{BidiReadObjectResponse, Object as ProtoObject};
    use storage_grpc_mock::{MockStorage, start};

    // Verify `open_object()` meets normal Send, Sync, requirements.
    #[tokio::test]
    async fn traits() -> Result<()> {
        assert_impl_all!(OpenObject: Clone, std::fmt::Debug);
        assert_impl_all!(OpenObject: Send, Sync);

        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;

        fn need_send<T: Send>(_val: &T) {}
        fn need_static<T: 'static>(_val: &T) {}

        let open = client.open_object("projects/_/buckets/test-bucket", "test-object");
        need_static(&open);

        let fut = client
            .open_object("projects/_/buckets/test-bucket", "test-object")
            .send();
        need_send(&fut);
        need_static(&fut);
        Ok(())
    }

    #[tokio::test]
    async fn open_object_normal() -> Result<()> {
        const BUCKET_NAME: &str = "projects/_/buckets/test-bucket";

        let (tx, rx) = tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(1);
        let initial = BidiReadObjectResponse {
            metadata: Some(ProtoObject {
                bucket: BUCKET_NAME.to_string(),
                name: "test-object".to_string(),
                generation: 123456,
                size: 42,
                ..ProtoObject::default()
            }),
            ..BidiReadObjectResponse::default()
        };
        tx.send(Ok(initial.clone())).await?;

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object()
            .return_once(|_| Ok(tonic::Response::from(rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;

        let client = Storage::builder()
            .with_endpoint(endpoint)
            .with_credentials(Anonymous::new().build())
            .build()
            .await?;
        let descriptor = client
            .open_object(BUCKET_NAME, "test-object")
            .send()
            .await?;

        let got = descriptor.object();
        let want = Object::new()
            .set_bucket(BUCKET_NAME)
            .set_name("test-object")
            .set_generation(123456)
            .set_size(42);
        assert_eq!(got, want);

        Ok(())
    }

    #[tokio::test]
    async fn attributes() -> Result<()> {
        let options = RequestOptions::new();
        let builder = OpenObject::new(
            "bucket".to_string(),
            "object".to_string(),
            Arc::new(StorageStub),
            options,
        )
        .set_generation(123)
        .set_if_generation_match(234)
        .set_if_generation_not_match(345)
        .set_if_metageneration_match(456)
        .set_if_metageneration_not_match(567);
        let want = OpenObjectRequest::default()
            .set_bucket("bucket")
            .set_object("object")
            .set_generation(123)
            .set_if_generation_match(234)
            .set_if_generation_not_match(345)
            .set_if_metageneration_match(456)
            .set_if_metageneration_not_match(567);
        assert_eq!(builder.request, want);
        Ok(())
    }

    #[tokio::test]
    async fn csek() -> Result<()> {
        let options = RequestOptions::new();
        let builder = OpenObject::new(
            "bucket".to_string(),
            "object".to_string(),
            Arc::new(StorageStub),
            options,
        );

        let (raw_key, _, _, _) = create_key_helper();
        let key = KeyAes256::new(&raw_key)?;
        let builder = builder.set_key(key.clone());
        let want = OpenObjectRequest::default()
            .set_bucket("bucket")
            .set_object("object")
            .set_common_object_request_params(CommonObjectRequestParams::from(key));
        assert_eq!(builder.request, want);
        Ok(())
    }

    #[tokio::test]
    async fn request_options() -> Result<()> {
        use crate::read_resume_policy::NeverResume;
        use gax::exponential_backoff::ExponentialBackoffBuilder;
        use gax::retry_policy::Aip194Strict;
        use gax::retry_throttler::CircuitBreaker;

        let options = RequestOptions::new();
        let builder = OpenObject::new(
            "bucket".to_string(),
            "object".to_string(),
            Arc::new(StorageStub),
            options.clone(),
        )
        .with_backoff_policy(
            ExponentialBackoffBuilder::default()
                .with_scaling(4.0)
                .build()
                .expect("expontial backoff builds"),
        )
        .with_retry_policy(Aip194Strict)
        .with_retry_throttler(CircuitBreaker::default())
        .with_read_resume_policy(NeverResume)
        .with_attempt_timeout(Duration::from_secs(120));

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
        assert!(
            format!("{:?}", got.read_resume_policy()).contains("NeverResume"),
            "{got:?}"
        );
        assert_eq!(
            got.bidi_attempt_timeout,
            Duration::from_secs(120),
            "{got:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn send() -> anyhow::Result<()> {
        use storage_grpc_mock::google::storage::v2::{
            BidiReadObjectResponse, Object as ProtoObject,
        };
        use storage_grpc_mock::{MockStorage, start};

        let (tx, rx) = tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(1);
        let initial = BidiReadObjectResponse {
            metadata: Some(ProtoObject {
                bucket: "projects/_/buckets/test-bucket".to_string(),
                name: "test-object".to_string(),
                generation: 123456,
                ..ProtoObject::default()
            }),
            ..BidiReadObjectResponse::default()
        };
        tx.send(Ok(initial.clone())).await?;

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object()
            .return_once(|_| Ok(tonic::Response::from(rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;

        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .with_endpoint(endpoint.clone())
            .build()
            .await?;

        let descriptor = client
            .open_object("projects/_/buckets/test-bucket", "test-object")
            .send()
            .await?;
        let want = Object::new()
            .set_bucket("projects/_/buckets/test-bucket")
            .set_name("test-object")
            .set_generation(123456);
        assert_eq!(descriptor.object(), want, "{descriptor:?}");
        assert_eq!(
            descriptor.headers().get("content-type"),
            Some(&HeaderValue::from_static("application/grpc")),
            "headers={:?}",
            descriptor.headers()
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn timeout() -> anyhow::Result<()> {
        use storage_grpc_mock::google::storage::v2::BidiReadObjectResponse;
        use storage_grpc_mock::{MockStorage, start};

        let (_tx, rx) = tokio::sync::mpsc::channel::<tonic::Result<BidiReadObjectResponse>>(1);

        let mut mock = MockStorage::new();
        mock.expect_bidi_read_object()
            .return_once(|_| Ok(tonic::Response::from(rx)));
        let (endpoint, _server) = start("0.0.0.0:0", mock).await?;

        let client = Storage::builder()
            .with_credentials(Anonymous::new().build())
            .with_endpoint(endpoint.clone())
            .with_retry_policy(NeverRetry)
            .build()
            .await?;

        // This will timeout because we never send the initial message over `_tx`.
        let target = Duration::from_secs(120);
        let start = tokio::time::Instant::now();
        let err = client
            .open_object("projects/_/buckets/test-bucket", "test-object")
            .with_attempt_timeout(target)
            .send()
            .await
            .unwrap_err();
        assert!(err.is_timeout(), "{err:?}");
        let elapsed = start.elapsed();
        assert!(
            elapsed >= target && elapsed < target + Duration::from_secs(5),
            "elapsed={elapsed:?}"
        );

        Ok(())
    }

    #[derive(Debug)]
    struct StorageStub;
    impl crate::stub::Storage for StorageStub {}
}
