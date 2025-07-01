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

pub use crate::Error;
pub use crate::Result;
use auth::credentials::CacheableResource;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
pub use control::model::Object;
use http::Extensions;
use sha2::{Digest, Sha256};

mod v1;

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
/// an [Rc](std::rc::Rc) or [Arc](std::sync::Arc) to reuse it, because it
/// already uses an `Arc` internally.
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
struct StorageInner {
    client: reqwest::Client,
    cred: auth::credentials::Credentials,
    endpoint: String,
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

    /// A simple upload from a buffer.
    ///
    /// # Parameters
    /// * `bucket` - the bucket name containing the object. In
    ///   `projects/_/buckets/{bucket_id}` format.
    /// * `object` - the object name.
    /// * `payload` - the object data.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .insert_object("projects/_/buckets/my-bucket", "my-object", "the quick brown fox jumped over the lazy dog")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub fn insert_object<B, O, P>(&self, bucket: B, object: O, payload: P) -> InsertObject
    where
        B: Into<String>,
        O: Into<String>,
        P: Into<bytes::Bytes>,
    {
        InsertObject::new(self.inner.clone(), bucket, object, payload)
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
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let contents = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?
    ///     .all_bytes()
    ///     .await?;
    /// println!("object contents={contents:?}");
    /// # Ok::<(), anyhow::Error>(()) });
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
        let client = reqwest::Client::new();
        let cred = if let Some(c) = config.cred.clone() {
            c
        } else {
            auth::credentials::Builder::default()
                .build()
                .map_err(Error::cred)?
        };
        let endpoint = config
            .endpoint
            .unwrap_or_else(|| self::DEFAULT_HOST.to_string());
        Ok(Self {
            inner: std::sync::Arc::new(StorageInner {
                client,
                cred,
                endpoint,
            }),
        })
    }
}

impl StorageInner {
    // Helper method to apply authentication headers to the request builder.
    async fn apply_auth_headers(
        &self,
        builder: reqwest::RequestBuilder,
    ) -> crate::Result<reqwest::RequestBuilder> {
        let cached_auth_headers = self
            .cred
            .headers(Extensions::new())
            .await
            .map_err(Error::authentication)?;

        let auth_headers = match cached_auth_headers {
            CacheableResource::New { data, .. } => Ok(data),
            CacheableResource::NotModified => {
                unreachable!("headers are not cached");
            }
        };

        let auth_headers = auth_headers?;
        let builder = auth_headers
            .iter()
            .fold(builder, |b, (k, v)| b.header(k, v));

        Ok(builder)
    }
}

/// A builder for [Storage].
///
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::*;
/// # use client::ClientBuilder;
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

pub struct InsertObject {
    inner: std::sync::Arc<StorageInner>,
    request: control::model::WriteObjectRequest,
}

impl InsertObject {
    fn new<B, O, P>(inner: std::sync::Arc<StorageInner>, bucket: B, object: O, payload: P) -> Self
    where
        B: Into<String>,
        O: Into<String>,
        P: Into<bytes::Bytes>,
    {
        InsertObject {
            inner,
            request: control::model::WriteObjectRequest::new()
                .set_write_object_spec(
                    control::model::WriteObjectSpec::new().set_resource(
                        control::model::Object::new()
                            .set_bucket(bucket)
                            .set_name(object),
                    ),
                )
                .set_checksummed_data(control::model::ChecksummedData::new().set_content(payload)),
        }
    }

    /// A simple upload from a buffer.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .insert_object("projects/_/buckets/my-bucket", "my-object", "the quick brown fox jumped over the lazy dog")
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub async fn send(self) -> crate::Result<Object> {
        let builder = self.http_request_builder().await?;

        tracing::info!("builder={builder:?}");

        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        let response = response.json::<v1::Object>().await.map_err(Error::io)?;

        Ok(Object::from(response))
    }

    async fn http_request_builder(self) -> Result<reqwest::RequestBuilder> {
        use control::model::write_object_request::*;

        let resource = match self.request.first_message {
            Some(FirstMessage::WriteObjectSpec(spec)) => spec.resource.unwrap(),
            _ => unreachable!("write object spec set in constructor"),
        };
        let bucket = &resource.bucket;
        let bucket_id = bucket.strip_prefix("projects/_/buckets/").ok_or_else(|| {
            Error::binding(format!(
                "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
            ))
        })?;
        let object = &resource.name;
        let builder = self
            .inner
            .client
            .request(
                reqwest::Method::POST,
                format!("{}/upload/storage/v1/b/{bucket_id}/o", &self.inner.endpoint),
            )
            .query(&[("uploadType", "media")])
            .query(&[("name", enc(object))])
            .header("content-type", "application/octet-stream")
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );

        let builder = apply_customer_supplied_encryption_headers(
            builder,
            self.request.common_object_request_params,
        );

        let builder = self.inner.apply_auth_headers(builder).await?;
        let content = match self.request.data {
            Some(Data::ChecksummedData(data)) => data.content,
            _ => unreachable!("content for the checksummed data is set in the constructor"),
        };
        let builder = builder.body(content);
        Ok(builder)
    }

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    ///
    /// Example:
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::client::KeyAes256;
    /// # let client = Storage::builder().build().await?;
    /// let key: &[u8] = &[97; 32];
    /// let response = client
    ///     .insert_object("projects/_/buckets/my-bucket", "my-object", "the quick brown fox jumped over the lazy dog")
    ///     .with_key(KeyAes256::new(key)?)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub fn with_key(mut self, v: KeyAes256) -> Self {
        self.request.common_object_request_params = Some(v.into());
        self
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
fn enc(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, &ENCODED_CHARS).to_string()
}

/// The request builder for [Storage::read_object][crate::client::Storage::read_object] calls.
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::client::Storage;
/// use google_cloud_storage::client::ReadObject;
/// # let client = Storage::builder()
/// #   .with_endpoint("https://storage.googleapis.com")
/// #    .build().await?;
/// let builder: ReadObject = client.read_object("projects/_/buckets/my-bucket", "my-object");
/// let contents = builder.send().await?.all_bytes().await?;
/// println!("object contents={contents:?}");
/// # Ok::<(), anyhow::Error>(()) });
/// ```
pub struct ReadObject {
    inner: std::sync::Arc<StorageInner>,
    request: control::model::ReadObjectRequest,
}

impl ReadObject {
    fn new<B, O>(inner: std::sync::Arc<StorageInner>, bucket: B, object: O) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        ReadObject {
            inner,
            request: control::model::ReadObjectRequest::new()
                .set_bucket(bucket)
                .set_object(object),
        }
    }

    /// If present, selects a specific revision of this object (as
    /// opposed to the latest version, the default).
    pub fn with_generation<T: Into<i64>>(mut self, v: T) -> Self {
        self.request.generation = v.into();
        self
    }

    /// Makes the operation conditional on whether the object's current generation
    /// matches the given value. Setting to 0 makes the operation succeed only if
    /// there are no live versions of the object.
    pub fn with_if_generation_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_generation_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's live generation
    /// does not match the given value. If no live object exists, the precondition
    /// fails. Setting to 0 makes the operation succeed only if there is a live
    /// version of the object.
    pub fn with_if_generation_not_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_generation_not_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration matches the given value.
    pub fn with_if_metageneration_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_metageneration_match = Some(v.into());
        self
    }

    /// Makes the operation conditional on whether the object's current
    /// metageneration does not match the given value.
    pub fn with_if_metageneration_not_match<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.if_metageneration_not_match = Some(v.into());
        self
    }

    /// The offset for the first byte to return in the read, relative to
    /// the start of the object.
    ///
    /// A negative `read_offset` value will be interpreted as the number of bytes
    /// back from the end of the object to be returned.
    ///
    /// # Examples
    ///
    /// Read starting at 100 bytes to end of file.
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_offset(100)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// Read last 100 bytes of file:
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_offset(-100)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// Read bytes 1000 to 1099.
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_offset(1000)
    ///     .with_read_limit(100)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub fn with_read_offset<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.read_offset = v.into();
        self
    }

    /// The maximum number of `data` bytes the server is allowed to
    /// return.
    ///
    /// A `read_limit` of zero indicates that there is no limit,
    /// and a negative `read_limit` will cause an error.
    ///
    /// # Examples:
    ///
    /// Read first 100 bytes.
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_limit(100)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    ///
    /// Read bytes 1000 to 1099.
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_read_offset(1000)
    ///     .with_read_limit(100)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub fn with_read_limit<T>(mut self, v: T) -> Self
    where
        T: Into<i64>,
    {
        self.request.read_limit = v.into();
        self
    }

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    ///
    /// Example:
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # use google_cloud_storage::client::KeyAes256;
    /// # let client = Storage::builder().build().await?;
    /// let key: &[u8] = &[97; 32];
    /// let response = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .with_key(KeyAes256::new(key)?)
    ///     .send()
    ///     .await?;
    /// println!("response details={response:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub fn with_key(mut self, v: KeyAes256) -> Self {
        self.request.common_object_request_params = Some(v.into());
        self
    }

    /// Sends the request.
    pub async fn send(self) -> Result<ReadObjectResponse> {
        let builder = self.http_request_builder().await?;

        tracing::info!("builder={builder:?}");

        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        Ok(ReadObjectResponse { inner: response })
    }

    async fn http_request_builder(self) -> Result<reqwest::RequestBuilder> {
        // Collect the required bucket and object parameters.
        let bucket: String = self.request.bucket;
        let bucket_id = bucket
            .as_str()
            .strip_prefix("projects/_/buckets/")
            .ok_or_else(|| {
                Error::binding(format!(
                    "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
                ))
            })?;
        let object: String = self.request.object;

        // Build the request.
        let builder = self
            .inner
            .client
            .request(
                reqwest::Method::GET,
                format!(
                    "{}/storage/v1/b/{bucket_id}/o/{}",
                    &self.inner.endpoint,
                    enc(&object)
                ),
            )
            .query(&[("alt", "media")])
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );

        // Add the optional query parameters.
        let builder = if self.request.generation != 0 {
            builder.query(&[("generation", self.request.generation)])
        } else {
            builder
        };
        let builder = self
            .request
            .if_generation_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifGenerationMatch", v)]));
        let builder = self
            .request
            .if_generation_not_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifGenerationNotMatch", v)]));
        let builder = self
            .request
            .if_metageneration_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifMetagenerationMatch", v)]));
        let builder = self
            .request
            .if_metageneration_not_match
            .iter()
            .fold(builder, |b, v| b.query(&[("ifMetagenerationNotMatch", v)]));

        let builder = apply_customer_supplied_encryption_headers(
            builder,
            self.request.common_object_request_params,
        );

        // Apply "range" header for read limits and offsets.
        let builder = match (self.request.read_offset, self.request.read_limit) {
            // read_limit can't be negative.
            (_, l) if l < 0 => Err(RangeError::NegativeLimit),
            // negative offset can't also have a read_limit.
            (o, l) if o < 0 && l > 0 => Err(RangeError::NegativeOffsetWithLimit),
            // If both are zero, we use default implementation (no range header).
            (0, 0) => Ok(builder),
            // read_limit is zero, means no limit. Read from offset to end of file.
            // This handles cases like (5, 0) -> "bytes=5-"
            (o, 0) => Ok(builder.header("range", format!("bytes={o}-"))),
            // General case: non-negative offset and positive limit.
            // This covers cases like (0, 100) -> "bytes=0-99", (5, 100) -> "bytes=5-104"
            (o, l) => Ok(builder.header("range", format!("bytes={o}-{}", o + l - 1))),
        }
        .map_err(Error::ser)?;

        let builder = self.inner.apply_auth_headers(builder).await?;
        Ok(builder)
    }
}

/// A response to a [Storage::read_object] request.
#[derive(Debug)]
pub struct ReadObjectResponse {
    inner: reqwest::Response,
}

impl ReadObjectResponse {
    // Get the full object as bytes.
    //
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let contents = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?
    ///     .all_bytes()
    ///     .await?;
    /// println!("object contents={contents:?}");
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub async fn all_bytes(self) -> Result<bytes::Bytes> {
        self.inner.bytes().await.map_err(Error::io)
    }

    /// Stream the next bytes of the object.
    ///
    /// When the response has been exhausted, this will return None.
    ///
    /// # Example
    /// ```
    /// # tokio_test::block_on(async {
    /// # use google_cloud_storage::client::Storage;
    /// # let client = Storage::builder().build().await?;
    /// let mut resp = client
    ///     .read_object("projects/_/buckets/my-bucket", "my-object")
    ///     .send()
    ///     .await?;
    ///
    /// while let Some(next) = resp.next().await {
    ///     println!("next={:?}", next?);
    /// }
    /// # Ok::<(), anyhow::Error>(()) });
    /// ```
    pub async fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        self.inner.chunk().await.map_err(Error::io).transpose()
    }
}

/// Represents an error that can occur when invalid range is specified.
#[derive(thiserror::Error, Debug, PartialEq)]
#[non_exhaustive]
enum RangeError {
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

impl std::convert::From<KeyAes256> for control::model::CommonObjectRequestParams {
    fn from(value: KeyAes256) -> Self {
        control::model::CommonObjectRequestParams::new()
            .set_encryption_algorithm("AES256")
            .set_encryption_key_bytes(value.key.to_vec())
            .set_encryption_key_sha256_bytes(Sha256::digest(value.key).as_slice().to_owned())
    }
}

fn apply_customer_supplied_encryption_headers(
    builder: reqwest::RequestBuilder,
    common_object_request_params: Option<control::model::CommonObjectRequestParams>,
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
mod tests {
    use super::*;
    use std::{collections::HashMap, error::Error};
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;
    use test_case::test_case;

    #[tokio::test]
    async fn test_insert_object() -> Result {
        let client = Storage::builder()
            .with_endpoint("http://private.googleapis.com")
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        let insert_object_builder = client
            .insert_object("projects/_/buckets/bucket", "object", "hello")
            .http_request_builder()
            .await?
            .build()?;

        assert_eq!(insert_object_builder.method(), reqwest::Method::POST);
        assert_eq!(
            insert_object_builder.url().as_str(),
            "http://private.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=media&name=object"
        );
        assert_eq!(
            b"hello",
            insert_object_builder.body().unwrap().as_bytes().unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_object_error_credentials() -> Result {
        let client = Storage::builder()
            .with_endpoint("http://private.googleapis.com")
            .with_credentials(auth::credentials::testing::error_credentials(false))
            .build()
            .await?;

        client
            .insert_object("projects/_/buckets/bucket", "object", "hello")
            .http_request_builder()
            .await
            .inspect_err(|e| assert!(e.is_authentication()))
            .expect_err("invalid credentials should err");
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_object_bad_bucket() -> Result {
        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        client
            .insert_object("malformed", "object", "hello")
            .http_request_builder()
            .await
            .expect_err("malformed bucket string should error");
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_object_headers() -> Result {
        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        // Make a 32-byte key.
        let (key, key_base64, _, key_sha256_base64) = create_key_helper();

        // The API takes the unencoded byte array.
        let insert_object_builder = client
            .insert_object("projects/_/buckets/bucket", "object", "hello")
            .with_key(KeyAes256::new(&key)?)
            .http_request_builder()
            .await?
            .build()?;

        assert_eq!(insert_object_builder.method(), reqwest::Method::POST);
        assert_eq!(
            insert_object_builder.url().as_str(),
            "https://storage.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=media&name=object"
        );

        let want = vec![
            ("x-goog-encryption-algorithm", "AES256".to_string()),
            ("x-goog-encryption-key", key_base64),
            ("x-goog-encryption-key-sha256", key_sha256_base64),
        ];

        for (name, value) in want {
            assert_eq!(
                insert_object_builder
                    .headers()
                    .get(name)
                    .unwrap()
                    .as_bytes(),
                bytes::Bytes::from(value)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_read_object() -> Result {
        let client = Storage::builder()
            .with_endpoint("http://private.googleapis.com")
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        let read_object_builder = client
            .read_object("projects/_/buckets/bucket", "object")
            .http_request_builder()
            .await?
            .build()?;

        assert_eq!(read_object_builder.method(), reqwest::Method::GET);
        assert_eq!(
            read_object_builder.url().as_str(),
            "http://private.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_read_object_error_credentials() -> Result {
        let client = Storage::builder()
            .with_endpoint("http://private.googleapis.com")
            .with_credentials(auth::credentials::testing::error_credentials(false))
            .build()
            .await?;

        client
            .read_object("projects/_/buckets/bucket", "object")
            .http_request_builder()
            .await
            .inspect_err(|e| assert!(e.is_authentication()))
            .expect_err("invalid credentials should err");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_object_bad_bucket() -> Result {
        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        client
            .read_object("malformed", "object")
            .http_request_builder()
            .await
            .expect_err("malformed bucket string should error");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_object_query_params() -> Result {
        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        let read_object_builder = client
            .read_object("projects/_/buckets/bucket", "object")
            .with_generation(5)
            .with_if_generation_match(10)
            .with_if_generation_not_match(20)
            .with_if_metageneration_match(30)
            .with_if_metageneration_not_match(40)
            .http_request_builder()
            .await?
            .build()?;

        assert_eq!(read_object_builder.method(), reqwest::Method::GET);
        let want_pairs: HashMap<String, String> = [
            ("alt", "media"),
            ("generation", "5"),
            ("ifGenerationMatch", "10"),
            ("ifGenerationNotMatch", "20"),
            ("ifMetagenerationMatch", "30"),
            ("ifMetagenerationNotMatch", "40"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let query_pairs: HashMap<String, String> = read_object_builder
            .url()
            .query_pairs()
            .map(|param| (param.0.to_string(), param.1.to_string()))
            .collect();
        assert_eq!(query_pairs.len(), want_pairs.len());
        assert_eq!(query_pairs, want_pairs);
        Ok(())
    }

    #[tokio::test]
    async fn test_read_object_headers() -> Result {
        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        // Make a 32-byte key.
        let (key, key_base64, _, key_sha256_base64) = create_key_helper();

        // The API takes the unencoded byte array.
        let read_object_builder = client
            .read_object("projects/_/buckets/bucket", "object")
            .with_key(KeyAes256::new(&key)?)
            .http_request_builder()
            .await?
            .build()?;

        assert_eq!(read_object_builder.method(), reqwest::Method::GET);
        assert_eq!(
            read_object_builder.url().as_str(),
            "https://storage.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
        );

        let want = vec![
            ("x-goog-encryption-algorithm", "AES256".to_string()),
            ("x-goog-encryption-key", key_base64),
            ("x-goog-encryption-key-sha256", key_sha256_base64),
        ];

        for (name, value) in want {
            assert_eq!(
                read_object_builder.headers().get(name).unwrap().as_bytes(),
                bytes::Bytes::from(value)
            );
        }
        Ok(())
    }

    #[test_case("projects/p", "projects%2Fp")]
    #[test_case("kebab-case", "kebab-case")]
    #[test_case("dot.name", "dot.name")]
    #[test_case("under_score", "under_score")]
    #[test_case("tilde~123", "tilde~123")]
    #[test_case("exclamation!point!", "exclamation%21point%21")]
    #[test_case("spaces   spaces", "spaces%20%20%20spaces")]
    #[test_case("preserve%percent%21", "preserve%percent%21")]
    #[test_case(
        "testall !#$&'()*+,/:;=?@[]",
        "testall%20%21%23%24%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D"
    )]
    #[tokio::test]
    async fn test_percent_encoding_object_name(name: &str, want: &str) -> Result {
        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        let insert_object_builder = client
            .insert_object("projects/_/buckets/bucket", name, "hello")
            .http_request_builder()
            .await?
            .build()?;

        let got = insert_object_builder
            .url()
            .query_pairs()
            .find_map(|(key, val)| match key.to_string().as_str() {
                "name" => Some(val.to_string()),
                _ => None,
            })
            .unwrap();
        assert_eq!(got, want);

        let read_object_request_builder = client
            .read_object("projects/_/buckets/bucket", name)
            .http_request_builder()
            .await?
            .build()?;

        let got = read_object_request_builder
            .url()
            .path_segments()
            .unwrap()
            .next_back()
            .unwrap();
        assert_eq!(got, want);

        Ok(())
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
        let params = control::model::CommonObjectRequestParams::from(key_aes_256);
        assert_eq!(params.encryption_algorithm, "AES256");
        assert_eq!(params.encryption_key_bytes, key);
        assert_eq!(params.encryption_key_sha256_bytes, key_sha256);
        Ok(())
    }

    fn create_key_helper() -> (Vec<u8>, String, Vec<u8>, String) {
        // Make a 32-byte key.
        let key = vec![b'a'; 32];
        let key_base64 = BASE64_STANDARD.encode(key.clone());

        let key_sha256 = Sha256::digest(key.clone());
        let key_sha256_base64 = BASE64_STANDARD.encode(key_sha256);
        (key, key_base64, key_sha256.to_vec(), key_sha256_base64)
    }

    #[test_case(0, 0, None; "no headers needed")]
    #[test_case(10, 0, Some(&http::HeaderValue::from_static("bytes=10-")); "offset only")]
    #[test_case(-2000, 0, Some(&http::HeaderValue::from_static("bytes=-2000-")); "negative offset")]
    #[test_case(0, 100, Some(&http::HeaderValue::from_static("bytes=0-99")); "limit only")]
    #[test_case(1000, 100, Some(&http::HeaderValue::from_static("bytes=1000-1099")); "offset and limit")]
    #[tokio::test]
    async fn test_range_header(
        offset: i64,
        limit: i64,
        want: Option<&http::HeaderValue>,
    ) -> Result {
        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;

        let read_object_builder = client
            .read_object("projects/_/buckets/bucket", "object")
            .with_read_offset(offset)
            .with_read_limit(limit)
            .http_request_builder()
            .await?
            .build()?;

        assert_eq!(read_object_builder.method(), reqwest::Method::GET);
        assert_eq!(
            read_object_builder.url().as_str(),
            "https://storage.googleapis.com/storage/v1/b/bucket/o/object?alt=media"
        );

        assert_eq!(read_object_builder.headers().get("range"), want);
        Ok(())
    }

    #[test_case(0, -100, RangeError::NegativeLimit; "negative limit")]
    #[test_case(-100, 100, RangeError::NegativeOffsetWithLimit; "negative offset with positive limit")]
    #[tokio::test]
    async fn test_range_header_error(offset: i64, limit: i64, want_err: RangeError) -> Result {
        let client = Storage::builder()
            .with_credentials(auth::credentials::testing::test_credentials())
            .build()
            .await?;
        let err = client
            .read_object("projects/_/buckets/bucket", "object")
            .with_read_offset(offset)
            .with_read_limit(limit)
            .http_request_builder()
            .await
            .unwrap_err();

        assert_eq!(
            err.source().unwrap().downcast_ref::<RangeError>().unwrap(),
            &want_err
        );
        Ok(())
    }
}
