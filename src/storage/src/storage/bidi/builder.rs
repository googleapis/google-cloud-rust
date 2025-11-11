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

use super::connector::Connector;
use super::object_descriptor::ObjectDescriptor;
use super::transport::ObjectDescriptorTransport;
use crate::Result;
use crate::google::storage::v2::BidiReadObjectSpec;
use crate::model_ext::KeyAes256;
use crate::read_resume_policy::ReadResumePolicy;
use crate::request_options::RequestOptions;
use gaxi::grpc::Client as GrpcClient;
use gaxi::prost::ToProto;

/// A request builder for `open_object()`.
///
/// # Example
/// ```compile_fail
/// use google_cloud_storage::client::Storage;
/// use google_cloud_storage::retry_policy::RetryableErrors;
/// async fn sample(client: &Storage) -> anyhow::Result<()> {
///     # use gax::retry_policy::RetryPolicyExt;
///     let builder: OpenObject = client
///         .open_object("projects/_/buckets/my-bucket", "my-object");
///     let open = builder
///         .set_generation(123)
///         .with_retry_policy(
///             RetryableErrors
///                 .with_attempt_limit(5)
///                 .with_time_limit(Duration::from_secs(90)),
///         )
///         .send()
///         .await?;
///     println!("response details={response:?}");
///     // Use `open` to read data from `my-object`.
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct OpenObject {
    spec: BidiReadObjectSpec,
    options: RequestOptions,
    client: GrpcClient,
    reconnect_attempts: u32,
}

impl OpenObject {
    pub(crate) fn new(
        bucket: String,
        object: String,
        client: GrpcClient,
        options: RequestOptions,
    ) -> Self {
        let spec = BidiReadObjectSpec {
            bucket,
            object,
            ..BidiReadObjectSpec::default()
        };
        Self {
            spec,
            options,
            client,
            reconnect_attempts: 0_u32,
        }
    }

    /// Sends the request, returning a new object descriptor.
    ///
    /// Example:
    /// ```ignore
    /// # use google_cloud_storage::{model_ext::KeyAes256, client::Storage};
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let open = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?;
    /// println!("object metadata={:?}", open.object());
    /// # Ok(()) }
    /// ```
    pub async fn send(self) -> Result<ObjectDescriptor> {
        let connector = Connector::new(self.spec, self.options, self.client);
        let transport = ObjectDescriptorTransport::new(connector).await?;

        Ok(ObjectDescriptor::new(transport))
    }

    /// If present, selects a specific revision of this object (as
    /// opposed to the latest version, the default).
    ///
    /// # Example
    /// ```compile_fail
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
        self.spec.generation = v.into();
        self
    }

    /// Makes the operation conditional on whether the object's current generation
    /// matches the given value.
    ///
    /// # Example
    /// ```compile_fail
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
        self.spec.if_generation_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's live generation
    /// does not match the given value. If no live object exists, the precondition
    /// fails.
    ///
    /// # Example
    /// ```compile_fail
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
        self.spec.if_generation_not_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    ///
    /// # Example
    /// ```compile_fail
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
        self.spec.if_metageneration_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    ///
    /// # Example
    /// ```compile_fail
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
        self.spec.if_metageneration_not_match = Some(v.into());
        self
    }

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    ///
    /// Example:
    /// ```compile_fail
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
        let proto = crate::model::CommonObjectRequestParams::from(v)
            .to_proto()
            .expect("conversion from AesKey256 never fails");
        self.spec.common_object_request_params = Some(proto);
        self
    }

    /// The retry policy used for this request.
    ///
    /// # Example
    /// ```compile_fail
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
    /// ```compile_fail
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
    /// ```compile_fail
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
    /// ```compile_fail
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::storage::v2::CommonObjectRequestParams;
    use crate::model_ext::tests::create_key_helper;
    use anyhow::Result;

    #[tokio::test]
    async fn attributes() -> Result<()> {
        let client = test_grpc_client().await?;
        let options = RequestOptions::new();
        let builder = OpenObject::new("bucket".to_string(), "object".to_string(), client, options)
            .set_generation(123)
            .set_if_generation_match(234)
            .set_if_generation_not_match(345)
            .set_if_metageneration_match(456)
            .set_if_metageneration_not_match(567);
        let want = BidiReadObjectSpec {
            bucket: "bucket".into(),
            object: "object".into(),
            generation: 123,
            if_generation_match: Some(234),
            if_generation_not_match: Some(345),
            if_metageneration_match: Some(456),
            if_metageneration_not_match: Some(567),
            ..BidiReadObjectSpec::default()
        };
        assert_eq!(builder.spec, want);
        Ok(())
    }

    #[tokio::test]
    async fn csek() -> Result<()> {
        let client = test_grpc_client().await?;
        let options = RequestOptions::new();
        let builder = OpenObject::new("bucket".to_string(), "object".to_string(), client, options);

        let (key, _, key_sha256, _) = create_key_helper();
        let builder = builder.set_key(KeyAes256::new(&key)?);
        let params = CommonObjectRequestParams {
            encryption_algorithm: "AES256".into(),
            encryption_key_bytes: bytes::Bytes::from_owner(key),
            encryption_key_sha256_bytes: bytes::Bytes::from_owner(key_sha256),
        };
        let want = BidiReadObjectSpec {
            bucket: "bucket".into(),
            object: "object".into(),
            common_object_request_params: Some(params),
            ..BidiReadObjectSpec::default()
        };
        assert_eq!(builder.spec, want);
        Ok(())
    }

    #[tokio::test]
    async fn request_options() -> Result<()> {
        use crate::read_resume_policy::NeverResume;
        use gax::exponential_backoff::ExponentialBackoffBuilder;
        use gax::retry_policy::Aip194Strict;
        use gax::retry_throttler::CircuitBreaker;

        let client = test_grpc_client().await?;
        let options = RequestOptions::new();
        let builder = OpenObject::new(
            "bucket".to_string(),
            "object".to_string(),
            client,
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
        .with_read_resume_policy(NeverResume);

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

        Ok(())
    }

    async fn test_grpc_client() -> Result<gaxi::grpc::Client> {
        let mut config = gaxi::options::ClientConfig::default();
        config.cred = Some(auth::credentials::anonymous::Builder::new().build());
        let client = gaxi::grpc::Client::new(config, "http://storage.googleapis.com").await?;
        Ok(client)
    }
}
