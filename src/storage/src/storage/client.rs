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

use super::request_options::RequestOptions;
use crate::Error;
use crate::builder::storage::ReadObject;
use crate::builder::storage::WriteObject;
use crate::read_resume_policy::ReadResumePolicy;
use crate::streaming_source::Payload;
use auth::credentials::CacheableResource;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use http::Extensions;
use std::sync::Arc;

/// Implements a client for the Cloud Storage API.
///
/// # Example
/// ```
/// # async fn sample() -> anyhow::Result<()> {
/// # use google_cloud_storage::client::Storage;
/// let client = Storage::builder().build().await?;
/// // use `client` to make requests to Cloud Storage.
/// # Ok(()) }
/// ```
///
/// # Configuration
///
/// To configure `Storage` use the `with_*` methods in the type returned
/// by [builder()][Storage::builder]. The default configuration should
/// work for most applications. Common configuration changes include
///
/// * [with_endpoint()]: by default this client uses the global default endpoint
///   (`https://storage.googleapis.com`). Applications using regional
///   endpoints or running in restricted networks (e.g. a network configured
///   with [Private Google Access with VPC Service Controls]) may want to
///   override this default.
/// * [with_credentials()]: by default this client uses
///   [Application Default Credentials]. Applications using custom
///   authentication may need to override this default.
///
/// # Pooling and Cloning
///
/// `Storage` holds a connection pool internally, it is advised to
/// create one and then reuse it.  You do not need to wrap `Storage` in
/// an [Rc](std::rc::Rc) or [Arc] to reuse it, because it already uses an `Arc`
/// internally.
///
/// # Service Description
///
/// The Cloud Storage API allows applications to read and write data through
/// the abstractions of buckets and objects. For a description of these
/// abstractions please see <https://cloud.google.com/storage/docs>.
///
/// Resources are named as follows:
///
/// - Projects are referred to as they are defined by the Resource Manager API,
///   using strings like `projects/123456` or `projects/my-string-id`.
///
/// - Buckets are named using string names of the form:
///   `projects/{project}/buckets/{bucket}`
///   For globally unique buckets, `_` may be substituted for the project.
///
/// - Objects are uniquely identified by their name along with the name of the
///   bucket they belong to, as separate strings in this API. For example:
///   ```no_rust
///   bucket = "projects/_/buckets/my-bucket"
///   object = "my-object/with/a/folder-like/name"
///   ```
///   Note that object names can contain `/` characters, which are treated as
///   any other character (no special directory semantics).
///
/// [with_endpoint()]: ClientBuilder::with_endpoint
/// [with_credentials()]: ClientBuilder::with_credentials
/// [Private Google Access with VPC Service Controls]: https://cloud.google.com/vpc-service-controls/docs/private-connectivity
/// [Application Default Credentials]: https://cloud.google.com/docs/authentication#adc
#[derive(Clone, Debug)]
pub struct Storage<S = crate::storage::transport::Storage>
where
    S: crate::storage::stub::Storage + 'static,
{
    stub: std::sync::Arc<S>,
    options: RequestOptions,
}

#[derive(Clone, Debug)]
pub(crate) struct StorageInner {
    pub client: reqwest::Client,
    pub cred: auth::credentials::Credentials,
    pub endpoint: String,
    pub options: RequestOptions,
}

impl Storage {
    /// Returns a builder for [Storage].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Storage::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }
}

impl<S> Storage<S>
where
    S: crate::storage::stub::Storage + 'static,
{
    /// Write an object using a local buffer.
    ///
    /// If the data source does **not** implement [Seek] the client library must
    /// buffer data sent to the service until the service confirms it has
    /// persisted the data. This requires more memory in the client, and when
    /// the buffer grows too large, may require stalling the writer until the
    /// service can persist the data.
    ///
    /// Use this function for data sources representing computations where
    /// it is expensive or impossible to restart said computation. This function
    /// is also useful when it is hard or impossible to predict the number of
    /// bytes emitted by a stream, even if restarting the stream is not too
    /// expensive.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .write_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let response = client
    ///     .write_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .send_unbuffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// # Parameters
    /// * `bucket` - the bucket name containing the object. In
    ///   `projects/_/buckets/{bucket_id}` format.
    /// * `object` - the object name.
    /// * `payload` - the object data.
    ///
    /// [Seek]: crate::streaming_source::Seek
    pub fn write_object<B, O, T, P>(&self, bucket: B, object: O, payload: T) -> WriteObject<P, S>
    where
        B: Into<String>,
        O: Into<String>,
        T: Into<Payload<P>>,
    {
        WriteObject::new(
            self.stub.clone(),
            bucket,
            object,
            payload,
            self.options.clone(),
        )
    }

    /// Reads the contents of an object.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let mut resp = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?;
    /// let mut contents = Vec::new();
    /// while let Some(chunk) = resp.next().await.transpose()? {
    ///   contents.extend_from_slice(&chunk);
    /// }
    /// println!("object contents={:?}", bytes::Bytes::from_owner(contents));
    /// # Ok(()) }
    /// ```
    ///
    /// # Parameters
    /// * `bucket` - the bucket name containing the object. In
    ///   `projects/_/buckets/{bucket_id}` format.
    /// * `object` - the object name.
    pub fn read_object<B, O>(&self, bucket: B, object: O) -> ReadObject<S>
    where
        B: Into<String>,
        O: Into<String>,
    {
        ReadObject::new(self.stub.clone(), bucket, object, self.options.clone())
    }
}

impl Storage {
    pub(crate) fn new(builder: ClientBuilder) -> gax::client_builder::Result<Self> {
        use gax::client_builder::Error;
        let client = reqwest::Client::builder()
            // Disable all automatic decompression. These could be enabled by users by enabling
            // the corresponding features flags, but we will not be able to tell whether this
            // has happened.
            .no_brotli()
            .no_deflate()
            .no_gzip()
            .no_zstd()
            .build()
            .map_err(Error::transport)?;
        let mut builder = builder;
        let cred = if let Some(c) = builder.credentials {
            c
        } else {
            auth::credentials::Builder::default()
                .build()
                .map_err(Error::cred)?
        };
        let endpoint = builder
            .endpoint
            .unwrap_or_else(|| self::DEFAULT_HOST.to_string());
        builder.credentials = Some(cred);
        builder.endpoint = Some(endpoint);
        let inner = Arc::new(StorageInner::new(client, builder));
        let options = inner.options.clone();
        let stub = crate::storage::transport::Storage::new(inner);
        Ok(Self { stub, options })
    }
}

impl StorageInner {
    /// Builds a client assuming `config.cred` and `config.endpoint` are initialized, panics otherwise.
    pub(self) fn new(client: reqwest::Client, builder: ClientBuilder) -> Self {
        Self {
            client,
            cred: builder
                .credentials
                .expect("StorageInner assumes the credentials are initialized"),
            endpoint: builder
                .endpoint
                .expect("StorageInner assumes the endpoint is initialized"),
            options: builder.default_options,
        }
    }

    // Helper method to apply authentication headers to the request builder.
    pub async fn apply_auth_headers(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> crate::Result<reqwest::RequestBuilder> {
        let cached_auth_headers = self
            .cred
            .headers(Extensions::new())
            .await
            .map_err(Error::authentication)?;

        let auth_headers = match cached_auth_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => {
                unreachable!("headers are not cached");
            }
        };

        let builder = builder.headers(auth_headers);
        Ok(builder)
    }
}

/// A builder for [Storage].
///
/// ```
/// # use google_cloud_storage::client::Storage;
/// # async fn sample() -> anyhow::Result<()> {
/// let builder = Storage::builder();
/// let client = builder
///     .with_endpoint("https://storage.googleapis.com")
///     .build()
///     .await?;
/// # Ok(()) }
/// ```
pub struct ClientBuilder {
    pub(crate) endpoint: Option<String>,
    pub(crate) credentials: Option<auth::credentials::Credentials>,
    // Default options for requests.
    pub(crate) default_options: RequestOptions,
}

impl ClientBuilder {
    pub(crate) fn new() -> Self {
        Self {
            endpoint: None,
            credentials: None,
            default_options: RequestOptions::new(),
        }
    }

    /// Creates a new client.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Storage::builder().build().await?;
    /// # Ok(()) }
    /// ```
    pub async fn build(self) -> gax::client_builder::Result<Storage> {
        Storage::new(self)
    }

    /// Sets the endpoint.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Storage::builder()
    ///     .with_endpoint("https://private.googleapis.com")
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_endpoint<V: Into<String>>(mut self, v: V) -> Self {
        self.endpoint = Some(v.into());
        self
    }

    /// Configures the authentication credentials.
    ///
    /// Google Cloud Storage requires authentication for most buckets. Use this
    /// method to change the credentials used by the client. More information
    /// about valid credentials types can be found in the [google-cloud-auth]
    /// crate documentation.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use auth::credentials::mds;
    /// let client = Storage::builder()
    ///     .with_credentials(
    ///         mds::Builder::default()
    ///             .with_scopes(["https://www.googleapis.com/auth/cloud-platform.read-only"])
    ///             .build()?)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [google-cloud-auth]: https://docs.rs/google-cloud-auth
    pub fn with_credentials<V: Into<auth::credentials::Credentials>>(mut self, v: V) -> Self {
        self.credentials = Some(v.into());
        self
    }

    /// Configure the retry policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// retry policy controls what errors are considered retryable, sets limits
    /// on the number of attempts or the time trying to make attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use gax::retry_policy::{AlwaysRetry, RetryPolicyExt};
    /// let client = Storage::builder()
    ///     .with_retry_policy(AlwaysRetry.with_attempt_limit(3))
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_policy<V: Into<gax::retry_policy::RetryPolicyArg>>(mut self, v: V) -> Self {
        self.default_options.retry_policy = v.into().into();
        self
    }

    /// Configure the retry backoff policy.
    ///
    /// The client libraries can automatically retry operations that fail. The
    /// backoff policy controls how long to wait in between retry attempts.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use gax::exponential_backoff::ExponentialBackoff;
    /// use std::time::Duration;
    /// let policy = ExponentialBackoff::default();
    /// let client = Storage::builder()
    ///     .with_backoff_policy(policy)
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_backoff_policy<V: Into<gax::backoff_policy::BackoffPolicyArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.default_options.backoff_policy = v.into().into();
        self
    }

    /// Configure the retry throttler.
    ///
    /// Advanced applications may want to configure a retry throttler to
    /// [Address Cascading Failures] and when [Handling Overload] conditions.
    /// The client libraries throttle their retry loop, using a policy to
    /// control the throttling algorithm. Use this method to fine tune or
    /// customize the default retry throtler.
    ///
    /// [Handling Overload]: https://sre.google/sre-book/handling-overload/
    /// [Addressing Cascading Failures]: https://sre.google/sre-book/addressing-cascading-failures/
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use gax::retry_throttler::AdaptiveThrottler;
    /// let client = Storage::builder()
    ///     .with_retry_throttler(AdaptiveThrottler::default())
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_retry_throttler<V: Into<gax::retry_throttler::RetryThrottlerArg>>(
        mut self,
        v: V,
    ) -> Self {
        self.default_options.retry_throttler = v.into().into();
        self
    }

    /// Sets the payload size threshold to switch from single-shot to resumable uploads.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Storage::builder()
    ///     .with_resumable_upload_threshold(0_usize) // Forces a resumable upload.
    ///     .build()
    ///     .await?;
    /// let response = client
    ///     .write_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// The client library can write objects using [single-shot] or [resumable]
    /// uploads. For small objects, single-shot uploads offer better
    /// performance, as they require a single HTTP transfer. For larger objects,
    /// the additional request latency is not significant, and resumable uploads
    /// offer better recovery on errors.
    ///
    /// The library automatically selects resumable uploads when the payload is
    /// equal to or larger than this option. For smaller writes the client
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
        self.default_options.resumable_upload_threshold = v.into();
        self
    }

    /// Changes the buffer size for some resumable uploads.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample() -> anyhow::Result<()> {
    /// let client = Storage::builder()
    ///     .with_resumable_upload_buffer_size(32 * 1024 * 1024_usize)
    ///     .build()
    ///     .await?;
    /// let response = client
    ///     .write_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .send_buffered()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok(()) }
    /// ```
    ///
    /// When performing [resumable uploads] from sources without [Seek] the
    /// client library needs to buffer data in memory until it is persisted by
    /// the service. Otherwise the data would be lost if the upload is
    /// interrupted. Applications may want to tune this buffer size:
    ///
    /// - Use smaller buffer sizes to support more concurrent writes in the
    ///   same application.
    /// - Use larger buffer sizes for better throughput. Sending many small
    ///   buffers stalls the writer until the client receives a successful
    ///   response from the service.
    ///
    /// Keep in mind that there are diminishing returns on using larger buffers.
    ///
    /// [resumable uploads]: https://cloud.google.com/storage/docs/resumable-uploads
    /// [Seek]: crate::streaming_source::Seek
    pub fn with_resumable_upload_buffer_size<V: Into<usize>>(mut self, v: V) -> Self {
        self.default_options.resumable_upload_buffer_size = v.into();
        self
    }

    /// Configure the resume policy for object reads.
    ///
    /// The Cloud Storage client library can automatically resume a read request
    /// that is interrupted by a transient error. Applications may want to
    /// limit the number of read attempts, or may wish to expand the type
    /// of errors treated as retryable.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample() -> anyhow::Result<()> {
    /// use google_cloud_storage::read_resume_policy::{AlwaysResume, ReadResumePolicyExt};
    /// let client = Storage::builder()
    ///     .with_read_resume_policy(AlwaysResume.with_attempt_limit(3))
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn with_read_resume_policy<V>(mut self, v: V) -> Self
    where
        V: ReadResumePolicy + 'static,
    {
        self.default_options.read_resume_policy = Arc::new(v);
        self
    }
}

/// The default host used by the service.
const DEFAULT_HOST: &str = "https://storage.googleapis.com";

pub(crate) mod info {
    const NAME: &str = env!("CARGO_PKG_NAME");
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    lazy_static::lazy_static! {
        pub(crate) static ref X_GOOG_API_CLIENT_HEADER: String = {
            let ac = gaxi::api_header::XGoogApiClient{
                name:          NAME,
                version:       VERSION,
                library_type:  gaxi::api_header::GCCL,
            };
            ac.grpc_header_value()
        };
    }
}

/// The set of characters that are percent encoded.
///
/// This set is defined at https://cloud.google.com/storage/docs/request-endpoints#encoding:
///
/// Encode the following characters when they appear in either the object name
/// or query string of a request URL:
///     !, #, $, &, ', (, ), *, +, ,, /, :, ;, =, ?, @, [, ], and space characters.
const ENCODED_CHARS: percent_encoding::AsciiSet = percent_encoding::CONTROLS
    .add(b'!')
    .add(b'#')
    .add(b'$')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'=')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b']')
    .add(b' ');

/// Percent encode a string.
///
/// To ensure compatibility certain characters need to be encoded when they appear
/// in either the object name or query string of a request URL.
pub(crate) fn enc(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, &ENCODED_CHARS).to_string()
}

pub(crate) fn apply_customer_supplied_encryption_headers(
    builder: reqwest::RequestBuilder,
    common_object_request_params: &Option<crate::model::CommonObjectRequestParams>,
) -> reqwest::RequestBuilder {
    common_object_request_params.iter().fold(builder, |b, v| {
        b.header(
            "x-goog-encryption-algorithm",
            v.encryption_algorithm.clone(),
        )
        .header(
            "x-goog-encryption-key",
            BASE64_STANDARD.encode(v.encryption_key_bytes.clone()),
        )
        .header(
            "x-goog-encryption-key-sha256",
            BASE64_STANDARD.encode(v.encryption_key_sha256_bytes.clone()),
        )
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use gax::retry_result::RetryResult;
    use gax::retry_state::RetryState;
    use std::{sync::Arc, time::Duration};

    pub(crate) fn test_builder() -> ClientBuilder {
        ClientBuilder::new()
            .with_credentials(auth::credentials::testing::test_credentials())
            .with_endpoint("http://private.googleapis.com")
            .with_backoff_policy(
                gax::exponential_backoff::ExponentialBackoffBuilder::new()
                    .with_initial_delay(Duration::from_millis(1))
                    .with_maximum_delay(Duration::from_millis(2))
                    .build()
                    .expect("hard coded policy should build correctly"),
            )
    }

    /// This is used by the request builder tests.
    pub(crate) fn test_inner_client(builder: ClientBuilder) -> Arc<StorageInner> {
        let client = reqwest::Client::new();
        Arc::new(StorageInner::new(client, builder))
    }

    mockall::mock! {
        #[derive(Debug)]
        pub RetryThrottler {}

        impl gax::retry_throttler::RetryThrottler for RetryThrottler {
            fn throttle_retry_attempt(&self) -> bool;
            fn on_retry_failure(&mut self, flow: &RetryResult);
            fn on_success(&mut self);
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub RetryPolicy {}

        impl gax::retry_policy::RetryPolicy for RetryPolicy {
            fn on_error(&self, state: &RetryState, error: gax::error::Error) -> RetryResult;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub BackoffPolicy {}

        impl gax::backoff_policy::BackoffPolicy for BackoffPolicy {
            fn on_failure(&self, loop_start: std::time::Instant, attempt_count: u32) -> std::time::Duration;
        }
    }

    mockall::mock! {
        #[derive(Debug)]
        pub ReadResumePolicy {}

        impl crate::read_resume_policy::ReadResumePolicy for ReadResumePolicy {
            fn on_error(&self, query: &crate::read_resume_policy::ResumeQuery, error: gax::error::Error) -> crate::read_resume_policy::ResumeResult;
        }
    }
}
