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
    /// # use google_cloud_storage::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let response = client
    ///         .insert_object("projects/_/buckets/my-bucket", "my-object", "the quick brown fox jumped over the lazy dog")
    ///         .send()
    ///         .await?;
    ///     println!("response details={response:?}");
    ///     Ok(())
    /// }
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
    /// # use google_cloud_storage::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let contents = client
    ///         .read_object("projects/_/buckets/my-bucket", "my-object")
    ///         .send()
    ///         .await?;
    ///     println!("object contents={contents:?}");
    ///     Ok(())
    /// }
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
    /// # use google_cloud_storage::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let response = client
    ///         .insert_object("projects/_/buckets/my-bucket", "my-object", "the quick brown fox jumped over the lazy dog")
    ///         .send()
    ///         .await?;
    ///     println!("response details={response:?}");
    ///     Ok(())
    /// }
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
        let resource = match self.request.first_message {
            Some(control::model::write_object_request::FirstMessage::WriteObjectSpec(spec)) => {
                match spec.resource {
                    Some(resource) => resource,
                    _ => unreachable!("write object spec set in constructor"),
                }
            }
            _ => unreachable!("write object spec set in constructor"),
        };
        let bucket: String = resource.bucket;
        let bucket_id = bucket
            .as_str()
            .strip_prefix("projects/_/buckets/")
            .ok_or_else(|| {
                Error::other(format!(
                    "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
                ))
            })?;
        let object: String = resource.name;
        let builder = self
            .inner
            .client
            .request(
                reqwest::Method::POST,
                format!("{}/upload/storage/v1/b/{bucket_id}/o", &self.inner.endpoint),
            )
            .query(&[("uploadType", "media")])
            .query(&[("name", &object)])
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
        let builder = builder.body(match self.request.data {
            Some(control::model::write_object_request::Data::ChecksummedData(data)) => data.content,
            _ => unreachable!("content for the checksummed data is set in the constructor"),
        });
        Ok(builder)
    }

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    pub fn with_key<T>(mut self, v: T) -> Self
    where
        T: Into<bytes::Bytes>,
    {
        self.request.common_object_request_params =
            Some(key_to_common_object_request_params(v.into()));
        self
    }
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
/// let contents = builder.send().await?;
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

    /// The encryption key used with the Customer-Supplied Encryption Keys
    /// feature. In raw bytes format (not base64-encoded).
    pub fn with_key<T>(mut self, v: T) -> Self
    where
        T: Into<bytes::Bytes>,
    {
        self.request.common_object_request_params =
            Some(key_to_common_object_request_params(v.into()));
        self
    }

    /// Sends the request.
    pub async fn send(self) -> crate::Result<bytes::Bytes> {
        let builder = self.http_request_builder().await?;

        tracing::info!("builder={builder:?}");

        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        let response = response.bytes().await.map_err(Error::io)?;

        Ok(response)
    }

    async fn http_request_builder(self) -> Result<reqwest::RequestBuilder> {
        // Collect the required bucket and object parameters.
        let bucket: String = self.request.bucket;
        let bucket_id = bucket
            .as_str()
            .strip_prefix("projects/_/buckets/")
            .ok_or_else(|| {
                Error::other(format!(
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
                    "{}/storage/v1/b/{bucket_id}/o/{object}",
                    &self.inner.endpoint
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

        let builder = self.inner.apply_auth_headers(builder).await?;
        Ok(builder)
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

fn key_to_common_object_request_params(
    v: bytes::Bytes,
) -> control::model::CommonObjectRequestParams {
    control::model::CommonObjectRequestParams::new()
        .set_encryption_algorithm("AES256")
        .set_encryption_key_bytes(v.clone())
        .set_encryption_key_sha256_bytes(Sha256::digest(v.clone()).as_slice().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

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
        let key = vec![b'a'; 32];
        let key_base64 = BASE64_STANDARD.encode(key.clone());

        let key_sha256 = Sha256::digest(key.clone());
        let key_sha256_base64 = BASE64_STANDARD.encode(key_sha256);

        // The API takes the unencoded byte array.
        let insert_object_builder = client
            .insert_object("projects/_/buckets/bucket", "object", "hello")
            .with_key(key)
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
        let key = vec![b'a'; 32];
        assert_eq!(key.len(), 32);
        let key_base64 = BASE64_STANDARD.encode(key.clone());

        let key_sha256 = Sha256::digest(key.clone());
        let key_sha256_base64 = BASE64_STANDARD.encode(key_sha256);

        // The API takes the unencoded byte array.
        let read_object_builder = client
            .read_object("projects/_/buckets/bucket", "object")
            .with_key(key)
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
}

mod v1 {
    use base64::Engine as _;

    #[serde_with::serde_as]
    #[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
    #[serde(default, rename_all = "camelCase")]
    // See http://cloud.google.com/storage/docs/json_api/v1/objects#resource for API reference.
    pub struct Object {
        id: String,
        name: String,
        bucket: String,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        generation: i64,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        metageneration: i64,
        content_type: String,
        storage_class: String,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        size: u64,
        component_count: i32,
        kms_key_name: String,
        etag: String,
        restore_token: Option<String>,
        content_encoding: String,
        content_disposition: String,
        content_language: String,
        cache_control: String,
        temporary_hold: bool,
        event_based_hold: Option<bool>,
        soft_delete_time: Option<wkt::Timestamp>,
        hard_delete_time: Option<wkt::Timestamp>,
        retention_expiration_time: Option<wkt::Timestamp>,
        time_created: wkt::Timestamp,
        time_finalized: wkt::Timestamp,
        time_deleted: Option<wkt::Timestamp>,
        time_storage_class_updated: wkt::Timestamp,
        updated: wkt::Timestamp,
        custom_time: Option<wkt::Timestamp>,
        acl: Vec<ObjectAccessControl>,
        owner: Option<Owner>,
        customer_encryption: Option<CustomerEncryption>,
        metadata: std::collections::HashMap<String, String>,
        #[serde_as(as = "Option<Crc32c>")]
        crc32c: Option<u32>,
        #[serde_as(as = "serde_with::base64::Base64")]
        md5_hash: bytes::Bytes,
        // The following are excluded from the protos, so we don't really need to parse them.
        media_link: String,
        self_link: String,
        // ObjectRetention cannot be configured or reported through the gRPC API.
        retention: Retention,
    }

    #[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
    #[serde(default, rename_all = "camelCase")]
    struct Retention {
        retain_until_time: wkt::Timestamp,
        mode: String,
    }

    // CRC32c checksum is a unsigned 32-bit int encoded using base64 in big-endian byte order.
    struct Crc32c;

    impl<'de> serde_with::DeserializeAs<'de, u32> for Crc32c {
        fn deserialize_as<D>(deserializer: D) -> Result<u32, D::Error>
        where
            D: serde::de::Deserializer<'de>,
        {
            struct Crc32cVisitor;

            impl serde::de::Visitor<'_> for Crc32cVisitor {
                type Value = u32;

                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("a base64 encoded string")
                }

                fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    let bytes = base64::prelude::BASE64_STANDARD
                        .decode(value)
                        .map_err(serde::de::Error::custom)?;

                    let length = bytes.len();
                    if bytes.len() != 4 {
                        return Err(serde::de::Error::invalid_length(
                            length,
                            &"a Byte Vector of length 4.",
                        ));
                    }
                    Ok(((bytes[0] as u32) << 24)
                        + ((bytes[1] as u32) << 16)
                        + ((bytes[2] as u32) << 8)
                        + (bytes[3] as u32))
                }
            }

            deserializer.deserialize_str(Crc32cVisitor)
        }
    }

    fn new_object_checksums(
        crc32c: Option<u32>,
        md5_hash: bytes::Bytes,
    ) -> Option<control::model::ObjectChecksums> {
        if crc32c.is_none() && md5_hash.is_empty() {
            return None;
        }
        Some(
            control::model::ObjectChecksums::new()
                .set_or_clear_crc32c(crc32c)
                .set_md5_hash(md5_hash),
        )
    }

    #[serde_with::serde_as]
    #[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
    #[serde(default, rename_all = "camelCase")]
    struct ObjectAccessControl {
        id: String,
        entity: String,
        role: String,
        email: String,
        domain: String,
        entity_id: String,
        etag: String,
        project_team: Option<ProjectTeam>,
    }

    #[serde_with::serde_as]
    #[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
    #[serde(default, rename_all = "camelCase")]
    struct ProjectTeam {
        project_number: String,
        team: String,
    }

    impl From<ObjectAccessControl> for control::model::ObjectAccessControl {
        fn from(value: ObjectAccessControl) -> Self {
            Self::new()
                .set_id(value.id)
                .set_entity(value.entity)
                .set_role(value.role)
                .set_email(value.email)
                .set_domain(value.domain)
                .set_entity_id(value.entity_id)
                .set_etag(value.etag)
                .set_or_clear_project_team::<control::model::ProjectTeam>(
                    value.project_team.map(|x| x.into()),
                )
        }
    }

    impl From<ProjectTeam> for control::model::ProjectTeam {
        fn from(p: ProjectTeam) -> Self {
            control::model::ProjectTeam::new()
                .set_project_number(p.project_number)
                .set_team(p.team)
        }
    }

    #[serde_with::serde_as]
    #[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
    #[serde(default, rename_all = "camelCase")]
    struct Owner {
        entity: String,
        entity_id: String,
    }

    impl From<Owner> for control::model::Owner {
        fn from(value: Owner) -> Self {
            Self::new()
                .set_entity(value.entity)
                .set_entity_id(value.entity_id)
        }
    }

    #[serde_with::serde_as]
    #[derive(Debug, Default, serde::Deserialize, PartialEq, Clone)]
    #[serde(default, rename_all = "camelCase")]
    struct CustomerEncryption {
        encryption_algorithm: String,
        #[serde_as(as = "serde_with::base64::Base64")]
        key_sha256: bytes::Bytes,
    }

    impl From<CustomerEncryption> for control::model::CustomerEncryption {
        fn from(value: CustomerEncryption) -> Self {
            Self::new()
                .set_encryption_algorithm(value.encryption_algorithm)
                .set_key_sha256_bytes(value.key_sha256)
        }
    }

    impl From<Object> for control::model::Object {
        fn from(value: Object) -> Self {
            Self::new()
                .set_name(value.name)
                .set_bucket(format!("projects/_/buckets/{}", value.bucket))
                .set_generation(value.generation)
                .set_metageneration(value.metageneration)
                .set_content_type(value.content_type)
                .set_storage_class(value.storage_class)
                .set_size(value.size as i64)
                .set_kms_key(value.kms_key_name)
                .set_etag(value.etag)
                .set_or_clear_restore_token(value.restore_token)
                .set_content_encoding(value.content_encoding)
                .set_content_disposition(value.content_disposition)
                .set_content_language(value.content_language)
                .set_cache_control(value.cache_control)
                .set_temporary_hold(value.temporary_hold)
                .set_or_clear_event_based_hold(value.event_based_hold)
                .set_component_count(value.component_count)
                .set_or_clear_soft_delete_time(value.soft_delete_time)
                .set_or_clear_hard_delete_time(value.hard_delete_time)
                .set_or_clear_retention_expire_time(value.retention_expiration_time)
                .set_create_time(value.time_created)
                .set_finalize_time(value.time_finalized)
                .set_or_clear_delete_time(value.time_deleted)
                .set_update_storage_class_time(value.time_storage_class_updated)
                .set_or_clear_custom_time(value.custom_time)
                .set_update_time(value.updated)
                .set_acl(value.acl)
                .set_or_clear_owner(value.owner)
                .set_metadata(value.metadata)
                .set_or_clear_customer_encryption(value.customer_encryption)
                .set_or_clear_checksums(new_object_checksums(value.crc32c, value.md5_hash))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use serde_with::DeserializeAs;
        use test_case::test_case;

        #[test]
        fn test_deserialize_object() {
            let json = serde_json::json!({
                // string fields:
                "id": "obj1",
                "name": "test-object.txt",
                "bucket": "my-bucket",
                "contentType": "text/plain",
                "storageClass": "STANDARD",
                // i64 and u64 fields:
                "generation": "123",
                "metageneration": "456",
                "size": "789",
                // boolean fields:
                "temporaryHold": true,
                // number fields:
                "componentCount": 5,
                // datetime fields:
                "timeCreated": "2025-05-13T10:30:00Z",
                // list fields:
                "acl": [{"id": "acl-id","unknownField": 5, "projectTeam": {"projectNumber": "123456", "team": "myteam"}}],
                // map fields:
                "metadata": {"key1": "val1", "key2": "val2", "key3": "val3"},
                // base64 fields:
                "customerEncryption": {"encryptionAlgorithm": "algorithm", "keySha256": "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw"},
                // checksum fields:
                // $ echo 'The quick brown fox jumps over the lazy dog' > quick.txt
                //
                // $ gcloud storage hash quick.txt
                // ---
                // crc32c_hash: /ieOcg==
                // digest_format: base64
                // md5_hash: N8S4ft/8XRmP9aGFzufuCQ==
                // url: quick.txt
                "md5Hash": "N8S4ft/8XRmP9aGFzufuCQ==",
                // base64 encoded uint32 in BigEndian order field:
                "crc32c": "/ieOcg==",
                // unused fields:
                "mediaLink": "my-link",
                "retention": { "mode": "my-mode", "retainUntilTime": "2026-05-13T10:30:00Z"}
            });
            let object: Object = serde_json::from_value(json)
                .expect("json value in object test should be deserializable");

            let want = Object {
                // string fields:
                id: "obj1".to_string(),
                name: "test-object.txt".to_string(),
                bucket: "my-bucket".to_string(),
                content_type: "text/plain".to_string(),
                storage_class: "STANDARD".to_string(),
                // i64 and u64 fields:
                generation: 123,
                metageneration: 456,
                size: 789,
                // boolean fields:
                temporary_hold: true,
                // number fields:
                component_count: 5,
                // datetime fields:
                time_created: wkt::Timestamp::clamp(1747132200, 0),
                // list fields:
                acl: vec![ObjectAccessControl {
                    id: "acl-id".to_string(),
                    project_team: Some(ProjectTeam {
                        project_number: "123456".to_string(),
                        team: "myteam".to_string(),
                    }),
                    ..Default::default()
                }],
                // map fields:
                metadata: std::collections::HashMap::from([
                    ("key1".to_string(), "val1".to_string()),
                    ("key2".to_string(), "val2".to_string()),
                    ("key3".to_string(), "val3".to_string()),
                ]),
                // base64 encoded fields:
                customer_encryption: Some(CustomerEncryption {
                    encryption_algorithm: "algorithm".to_string(),
                    key_sha256: bytes::Bytes::from("the quick brown fox jumps over the lazy dog"),
                }),
                md5_hash: vec![
                    55, 196, 184, 126, 223, 252, 93, 25, 143, 245, 161, 133, 206, 231, 238, 9,
                ]
                .into(),
                // base64 encoded uint32 in BigEndian order field:
                crc32c: Some(4264005234),
                // unused in control::model::Object:
                media_link: "my-link".to_string(),
                retention: Retention {
                    retain_until_time: wkt::Timestamp::clamp(1778668200, 0),
                    mode: "my-mode".to_string(),
                },
                ..Default::default()
            };

            assert_eq!(object, want);
        }

        #[test_case(Object::default(); "default fields")]
        #[test_case(Object {
            // string fields:
            id: "obj1".to_string(),
            name: "test-object.txt".to_string(),
            bucket: "my-bucket".to_string(),
            content_type: "text/plain".to_string(),
            storage_class: "STANDARD".to_string(),
            // i64 and u64 fields:
            generation: 123,
            metageneration: 456,
            size: 789,
            // boolean fields:
            temporary_hold: true,
            // number fields:
            component_count: 5,
            // datetime fields:
            time_created: wkt::Timestamp::clamp(1747132200, 0),
            // list fields:
            acl: vec![
                ObjectAccessControl {
                    id: "acl1".to_string(),
                    ..Default::default()
                },
                ObjectAccessControl {
                    id: "acl2".to_string(),
                    ..Default::default()
                },
            ],
            // map fields:
            metadata: std::collections::HashMap::from([
                ("key1".to_string(), "val1".to_string()),
                ("key2".to_string(), "val2".to_string()),
                ("key3".to_string(), "val3".to_string()),
            ]),
            // unused in control::model
            media_link: "my-media-link".to_string(),
            ..Default::default()
        }; "some fields set")]
        #[test_case(Object {
            id: "obj1".to_string(),
            name: "test-object.txt".to_string(),
            bucket: "my-bucket".to_string(),
            generation: 123,
            metageneration: 456,
            content_type: "text/plain".to_string(),
            storage_class: "STANDARD".to_string(),
            size: 789,
            component_count: 101112,
            kms_key_name: "my-kms-key".to_string(),
            etag: "etag1".to_string(),
            restore_token: Some("restore-token1".to_string()),
            content_encoding: "content-encoding".to_string(),
            content_disposition: "content-disposition1".to_string(),
            content_language: "content-language1".to_string(),
            cache_control: "cache-control1".to_string(),
            temporary_hold: true,
            event_based_hold: Some(false),
            soft_delete_time: Some(wkt::Timestamp::clamp(1747132200, 1)),
            hard_delete_time: Some(wkt::Timestamp::clamp(1747132200, 2)),
            retention_expiration_time: Some(wkt::Timestamp::clamp(1747132200, 3)),
            time_created: wkt::Timestamp::clamp(1747132200, 4),
            time_finalized: wkt::Timestamp::clamp(1747132200, 5),
            time_deleted: Some(wkt::Timestamp::clamp(1747132200, 6)),
            time_storage_class_updated: wkt::Timestamp::clamp(1747132200, 7),
            updated: wkt::Timestamp::clamp(1747132200, 8),
            custom_time: Some(wkt::Timestamp::clamp(1747132200, 9)),
            acl: vec![
                ObjectAccessControl {
                    id: "acl1".to_string(),
                    ..Default::default()
                },
                ObjectAccessControl {
                    id: "acl2".to_string(),
                    ..Default::default()
                },
            ],
            owner: Some(Owner{
                entity: "user-emailAddress".to_string(),
                entity_id: "entity-id".to_string(),
            }),
            metadata: std::collections::HashMap::from([
                ("key1".to_string(), "val1".to_string()),
                ("key2".to_string(), "val2".to_string()),
                ("key3".to_string(), "val3".to_string()),
            ]),
            customer_encryption: Some(CustomerEncryption{
                encryption_algorithm: "my-encryption-alg".to_string(),
                key_sha256: "hash-of-encryption-key".into(),
            }),
            md5_hash: "md5Hash".into(),
            crc32c: Some(4321),
            // unused in control::model
            media_link: "my-media-link".to_string(),
            self_link: "my-self-link".to_string(),
            retention: Retention { retain_until_time: wkt::Timestamp::clamp(1747132200, 10), mode: "mode".to_string() }
        }; "all fields set")]
        // Tests for acl values.
        #[test_case(Object { acl: Vec::new(), ..Default::default()}; "empty acl")]
        #[test_case(Object {acl: vec![ObjectAccessControl::default(), object_acl_with_some_fields(), object_acl_with_all_fields()], ..Default::default()}; "acls with different fields")]
        fn test_convert_object_to_control_model(object: Object) {
            let got = control::model::Object::from(object.clone());
            assert_eq_object(object, got);
        }

        fn assert_eq_object(object: Object, got: control::model::Object) {
            assert_eq!(got.name, object.name);
            assert_eq!(got.bucket, format!("projects/_/buckets/{}", object.bucket));
            assert_eq!(got.etag, object.etag);
            assert_eq!(got.generation, object.generation);
            assert_eq!(got.restore_token, object.restore_token);
            assert_eq!(got.metageneration, object.metageneration);
            assert_eq!(got.storage_class, object.storage_class);
            assert_eq!(got.size, object.size as i64);
            assert_eq!(got.content_encoding, object.content_encoding);
            assert_eq!(got.content_disposition, object.content_disposition);
            assert_eq!(got.cache_control, object.cache_control);
            got.acl
                .iter()
                .zip(object.acl)
                .for_each(|a| assert_eq_object_access_control(a.0, &a.1));
            assert_eq!(got.content_language, object.content_language);
            assert_eq!(got.delete_time, object.time_deleted);
            assert_eq!(
                got.finalize_time.expect("finalize time is set"),
                object.time_finalized
            );
            assert_eq!(got.content_type, object.content_type);
            assert_eq!(
                got.create_time.expect("create time is set"),
                object.time_created
            );
            assert_eq!(got.component_count, object.component_count);
            assert_eq!(got.update_time.expect("update time is set"), object.updated);
            assert_eq!(got.kms_key, object.kms_key_name);
            assert_eq!(
                got.update_storage_class_time
                    .expect("update storage class time is set"),
                object.time_storage_class_updated
            );
            assert_eq!(got.temporary_hold, object.temporary_hold);
            assert_eq!(got.retention_expire_time, object.retention_expiration_time);
            assert_eq!(got.event_based_hold, object.event_based_hold);
            assert_eq!(got.custom_time, object.custom_time);
            assert_eq!(got.soft_delete_time, object.soft_delete_time);
            assert_eq!(got.hard_delete_time, object.hard_delete_time);
            match (&object.owner, &got.owner) {
                (None, None) => {}
                (Some(from), None) => panic!("expected a value in the owner, {from:?}"),
                (None, Some(got)) => panic!("unexpected value in the owner, {got:?}"),
                (Some(from), Some(got)) => {
                    assert_eq!(got.entity, from.entity);
                    assert_eq!(got.entity_id, from.entity_id);
                }
            }
            assert_eq!(got.metadata, object.metadata);
            match (&object.customer_encryption, &got.customer_encryption) {
                (None, None) => {}
                (Some(from), None) => {
                    panic!("expected a value in the customer_encryption, {from:?}")
                }
                (None, Some(got)) => panic!("unexpected value in the customer_encryption, {got:?}"),
                (Some(from), Some(got)) => {
                    assert_eq!(got.encryption_algorithm, from.encryption_algorithm);
                    assert_eq!(got.key_sha256_bytes, from.key_sha256);
                }
            }
            match got.checksums {
                Some(checksums) => {
                    assert_eq!(object.md5_hash, checksums.md5_hash);
                    assert_eq!(object.crc32c, checksums.crc32c)
                }
                None => {
                    assert!(object.md5_hash.is_empty());
                    assert!(object.crc32c.is_none());
                }
            }
        }

        fn object_acl_with_all_fields() -> ObjectAccessControl {
            ObjectAccessControl {
                id: "acl1".to_string(),
                entity: "entity1".to_string(),
                role: "role1".to_string(),
                email: "email1".to_string(),
                domain: "domain1".to_string(),
                entity_id: "entity1".to_string(),
                etag: "etag1".to_string(),
                project_team: Some(ProjectTeam {
                    project_number: "123456".to_string(),
                    team: "team1".to_string(),
                }),
            }
        }

        fn object_acl_with_some_fields() -> ObjectAccessControl {
            ObjectAccessControl {
                id: "acl1".to_string(),
                entity: "entity1".to_string(),
                role: "role1".to_string(),
                project_team: Some(ProjectTeam {
                    project_number: "123456".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }
        }

        #[test_case(ObjectAccessControl::default(); "default fields")]
        #[test_case(object_acl_with_all_fields(); "all fields have values")]
        #[test_case(object_acl_with_some_fields(); "some fields have values" )]
        fn test_object_access_control(from: ObjectAccessControl) {
            let got = control::model::ObjectAccessControl::from(from.clone());
            assert_eq_object_access_control(&got, &from);
        }

        fn assert_eq_object_access_control(
            got: &control::model::ObjectAccessControl,
            from: &ObjectAccessControl,
        ) {
            assert_eq!(got.id, from.id);
            assert_eq!(got.entity, from.entity);
            assert_eq!(got.role, from.role);
            assert_eq!(got.email, from.email);
            assert_eq!(got.domain, from.domain);
            assert_eq!(got.entity_id, from.entity_id);
            assert_eq!(got.etag, from.etag);
            match (&from.project_team, &got.project_team) {
                (None, None) => {}
                (Some(from), None) => {
                    panic!("expected a value in the project team, {from:?}")
                }
                (None, Some(got)) => panic!("unexpected value in the project team, {got:?}"),
                (Some(from), Some(got)) => {
                    assert_eq!(got.project_number, from.project_number);
                    assert_eq!(got.team, from.team);
                }
            }
        }

        #[test_case(None, bytes::Bytes::new(), None; "unset")]
        #[test_case(Some(5), bytes::Bytes::new(), Some(control::model::ObjectChecksums::new().set_crc32c(5_u32)); "crc32c set")]
        #[test_case(None, "hello".into(), Some(control::model::ObjectChecksums::new().set_md5_hash("hello")); "md5_hash set")]
        #[test_case(Some(5), "hello".into(), Some(control::model::ObjectChecksums::new().set_crc32c(5_u32).set_md5_hash("hello")); "both set")]
        fn test_new_object_checksums(
            crc32c: Option<u32>,
            md5_hash: bytes::Bytes,
            want: Option<control::model::ObjectChecksums>,
        ) {
            assert_eq!(new_object_checksums(crc32c, md5_hash), want)
        }

        #[test_case("AAAAAA==", 0_u32; "zero")]
        #[test_case("SZYC0g==", 1234567890_u32; "number")]
        #[test_case("/////w==", u32::MAX; "max u32")]
        fn test_deserialize_crc32c(s: &str, want: u32) {
            let got = Crc32c::deserialize_as(serde_json::json!(s))
                .expect("deserialization should not error");
            assert_eq!(got, want);
        }

        #[test_case(""; "empty")]
        #[test_case("invalid"; "invalid")]
        #[test_case("AAA="; "too small")]
        #[test_case("AAAAAAAAAAA="; "too large")]
        fn test_deserialize_crc32c_err(input: &str) {
            Crc32c::deserialize_as(serde_json::json!(input))
                .expect_err("expected error deserializing string");
        }

        #[test]
        fn test_deserialize_crc32c_not_string_err() {
            Crc32c::deserialize_as(serde_json::json!(5))
                .expect_err("expected error deserializing int");
        }
    }
}
