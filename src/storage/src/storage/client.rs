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

use crate::Error;

use crate::builder::storage::ReadObject;
use crate::builder::storage::UploadObject;

use crate::upload_source::{InsertPayload, Seek, StreamingSource};
use auth::credentials::CacheableResource;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use gax::{
    backoff_policy::BackoffPolicy, retry_policy::RetryPolicy, retry_throttler::SharedRetryThrottler,
};
use http::Extensions;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Implements a client for the Cloud Storage API.
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::client::Storage;
/// let client = Storage::builder().build().await?;
/// // use `client` to make requests to Cloud Storage.
/// # gax::client_builder::Result::<()>::Ok(()) });
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
pub struct Storage {
    inner: std::sync::Arc<StorageInner>,
}

#[derive(Clone, Debug)]
pub(crate) struct StorageInner {
    pub client: reqwest::Client,
    pub cred: auth::credentials::Credentials,
    pub endpoint: String,
    #[allow(dead_code)]
    pub retry_policy: Arc<dyn RetryPolicy>,
    #[allow(dead_code)]
    pub backoff_policy: Arc<dyn BackoffPolicy>,
    #[allow(dead_code)]
    pub retry_throttler: SharedRetryThrottler,
}

impl Storage {
    /// Returns a builder for [Storage].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// let client = Storage::builder().build().await?;
    /// # gax::client_builder::Result::<()>::Ok(()) });
    /// ```
    pub fn builder() -> ClientBuilder {
        gax::client_builder::internal::new_builder(client_builder::Factory)
    }

    /// Upload an object using a local buffer.
    ///
    /// If the data source does **not** implement [Seek] the client library must
    /// buffer uploaded data until this data is persisted in the service. This
    /// requires more memory in the client, and when the buffer grows too large,
    /// may require stalling the upload until the service can persist the data.
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
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
    ///     .send()
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
    ///     .upload_object("projects/_/buckets/my-bucket", "my-object", "hello world")
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
    pub fn upload_object<B, O, T, P>(&self, bucket: B, object: O, payload: T) -> UploadObject<P>
    where
        B: Into<String>,
        O: Into<String>,
        T: Into<InsertPayload<P>>,
        InsertPayload<P>: StreamingSource + Seek,
    {
        UploadObject::new(self.inner.clone(), bucket, object, payload)
    }

    /// A simple download into a buffer.
    ///
    /// # Parameters
    /// * `bucket` - the bucket name containing the object. In
    ///   `projects/_/buckets/{bucket_id}` format.
    /// * `object` - the object name.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// # async fn sample(client: &Storage) -> anyhow::Result<()> {
    /// let contents = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?
    ///     .all_bytes()
    ///     .await?;
    /// println!("object contents={contents:?}");
    /// # Ok(()) }
    /// ```
    pub fn read_object<B, O>(&self, bucket: B, object: O) -> ReadObject
    where
        B: Into<String>,
        O: Into<String>,
    {
        ReadObject::new(self.inner.clone(), bucket, object)
    }

    pub(crate) async fn new(
        config: gaxi::options::ClientConfig,
    ) -> gax::client_builder::Result<Self> {
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
        let mut config = config;
        let cred = if let Some(c) = config.cred {
            c
        } else {
            auth::credentials::Builder::default()
                .build()
                .map_err(Error::cred)?
        };
        let endpoint = config
            .endpoint
            .unwrap_or_else(|| self::DEFAULT_HOST.to_string());
        config.cred = Some(cred);
        config.endpoint = Some(endpoint);
        let inner = Arc::new(StorageInner::new(client, config));
        Ok(Self { inner })
    }
}

impl StorageInner {
    /// Builds a client assuming `config.cred` and `config.endpoint` are initialized, panics otherwise.
    pub(self) fn new(client: reqwest::Client, config: gaxi::options::ClientConfig) -> Self {
        let retry_policy = config
            .retry_policy
            .unwrap_or_else(|| Arc::new(crate::retry_policy::default()));
        let backoff_policy = config
            .backoff_policy
            .unwrap_or_else(|| Arc::new(crate::backoff_policy::default()));
        let retry_throttler = config.retry_throttler;
        Self {
            client,
            cred: config
                .cred
                .expect("StorageInner assumes the credentials are initialized"),
            endpoint: config
                .endpoint
                .expect("StorageInner assumes the endpoint is initialized"),
            retry_policy,
            backoff_policy,
            retry_throttler,
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
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::*;
/// # use builder::storage::ClientBuilder;
/// # use client::Storage;
/// let builder : ClientBuilder = Storage::builder();
/// let client = builder
///     .with_endpoint("https://storage.googleapis.com")
///     .build().await?;
/// # gax::client_builder::Result::<()>::Ok(()) });
/// ```
pub type ClientBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::Storage;
    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = Storage;
        type Credentials = gaxi::options::Credentials;
        async fn build(
            self,
            config: gaxi::options::ClientConfig,
        ) -> gax::client_builder::Result<Self::Client> {
            Self::Client::new(config).await
        }
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

/// Represents an error that can occur when invalid range is specified.
#[derive(thiserror::Error, Debug, PartialEq)]
#[non_exhaustive]
pub(crate) enum RangeError {
    /// The provided read limit was negative.
    #[error("read limit was negative, expected non-negative value.")]
    NegativeLimit,
    /// A negative offset was provided with a read limit.
    #[error("negative read offsets cannot be used with read limits.")]
    NegativeOffsetWithLimit,
}

#[derive(Debug)]
/// KeyAes256 represents an AES-256 encryption key used with the
/// Customer-Supplied Encryption Keys (CSEK) feature.
///
/// This key must be exactly 32 bytes in length and should be provided in its
/// raw (unencoded) byte format.
///
/// # Examples
///
/// Creating a `KeyAes256` instance from a valid byte slice:
/// ```
/// # use google_cloud_storage::client::{KeyAes256, KeyAes256Error};
/// let raw_key_bytes: [u8; 32] = [0x42; 32]; // Example 32-byte key
/// let key_aes_256 = KeyAes256::new(&raw_key_bytes)?;
/// # Ok::<(), KeyAes256Error>(())
/// ```
///
/// Handling an error for an invalid key length:
/// ```
/// # use google_cloud_storage::client::{KeyAes256, KeyAes256Error};
/// let invalid_key_bytes: &[u8] = b"too_short_key"; // Less than 32 bytes
/// let result = KeyAes256::new(invalid_key_bytes);
///
/// assert!(matches!(result, Err(KeyAes256Error::InvalidLength)));
/// ```
pub struct KeyAes256 {
    key: [u8; 32],
}

/// Represents errors that can occur when converting to [`KeyAes256`] instances.
///
/// # Example:
/// ```
/// # use google_cloud_storage::client::{KeyAes256, KeyAes256Error};
/// let invalid_key_bytes: &[u8] = b"too_short_key"; // Less than 32 bytes
/// let result = KeyAes256::new(invalid_key_bytes);
///
/// assert!(matches!(result, Err(KeyAes256Error::InvalidLength)));
/// ```
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum KeyAes256Error {
    /// The provided key's length was not exactly 32 bytes.
    #[error("Key has an invalid length: expected 32 bytes.")]
    InvalidLength,
}

impl KeyAes256 {
    /// Attempts to create a new [KeyAes256].
    ///
    /// This conversion will succeed only if the input slice is exactly 32 bytes long.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::{KeyAes256, KeyAes256Error};
    /// let raw_key_bytes: [u8; 32] = [0x42; 32]; // Example 32-byte key
    /// let key_aes_256 = KeyAes256::new(&raw_key_bytes)?;
    /// # Ok::<(), KeyAes256Error>(())
    /// ```
    pub fn new(key: &[u8]) -> std::result::Result<Self, KeyAes256Error> {
        match key.len() {
            32 => Ok(Self {
                key: key[..32].try_into().unwrap(),
            }),
            _ => Err(KeyAes256Error::InvalidLength),
        }
    }
}

impl std::convert::From<KeyAes256> for crate::model::CommonObjectRequestParams {
    fn from(value: KeyAes256) -> Self {
        crate::model::CommonObjectRequestParams::new()
            .set_encryption_algorithm("AES256")
            .set_encryption_key_bytes(value.key.to_vec())
            .set_encryption_key_sha256_bytes(Sha256::digest(value.key).as_slice().to_owned())
    }
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
    use std::{sync::Arc, time::Duration};
    use test_case::test_case;

    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    /// This is used by the request builder tests.
    pub(crate) fn test_inner_client(config: gaxi::options::ClientConfig) -> Arc<StorageInner> {
        let client = reqwest::Client::new();
        let mut config = config;
        config.cred = config
            .cred
            .or_else(|| Some(auth::credentials::testing::test_credentials()));
        config.endpoint = config
            .endpoint
            .or_else(|| Some("http://private.googleapis.com".into()));
        // For unit tests we want really fast backoffs
        config.backoff_policy = Some(Arc::new(
            gax::exponential_backoff::ExponentialBackoffBuilder::new()
                .with_initial_delay(Duration::from_millis(1))
                .with_maximum_delay(Duration::from_millis(2))
                .clamp(),
        ));
        Arc::new(StorageInner::new(client, config))
    }

    /// This is used by the request builder tests.
    pub(crate) fn create_key_helper() -> (Vec<u8>, String, Vec<u8>, String) {
        // Make a 32-byte key.
        let key = vec![b'a'; 32];
        let key_base64 = BASE64_STANDARD.encode(key.clone());

        let key_sha256 = Sha256::digest(key.clone());
        let key_sha256_base64 = BASE64_STANDARD.encode(key_sha256);
        (key, key_base64, key_sha256.to_vec(), key_sha256_base64)
    }

    #[test]
    // This tests converting to KeyAes256 from some different types
    // that can get converted to &[u8].
    fn test_key_aes_256() -> Result {
        let v_slice: &[u8] = &[b'c'; 32];
        KeyAes256::new(v_slice)?;

        let v_vec: Vec<u8> = vec![b'a'; 32];
        KeyAes256::new(&v_vec)?;

        let v_array: [u8; 32] = [b'a'; 32];
        KeyAes256::new(&v_array)?;

        let v_bytes: bytes::Bytes = bytes::Bytes::copy_from_slice(&v_array);
        KeyAes256::new(&v_bytes)?;

        Ok(())
    }

    #[test_case(&[b'a'; 0]; "no bytes")]
    #[test_case(&[b'a'; 1]; "not enough bytes")]
    #[test_case(&[b'a'; 33]; "too many bytes")]
    fn test_key_aes_256_err(input: &[u8]) {
        KeyAes256::new(input).unwrap_err();
    }

    #[test]
    fn test_key_aes_256_to_control_model_object() -> Result {
        let (key, _, key_sha256, _) = create_key_helper();
        let key_aes_256 = KeyAes256::new(&key)?;
        let params = crate::model::CommonObjectRequestParams::from(key_aes_256);
        assert_eq!(params.encryption_algorithm, "AES256");
        assert_eq!(params.encryption_key_bytes, key);
        assert_eq!(params.encryption_key_sha256_bytes, key_sha256);
        Ok(())
    }
}
