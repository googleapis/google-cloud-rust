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
pub use control::model::Object;
use http::Extensions;

/// Implements a client for the Cloud Storage API.
///
/// # Example
/// ```
/// # tokio_test::block_on(async {
/// # use google_cloud_storage::client::Storage;
/// let client = Storage::builder().build().await?;
/// // use `client` to make requests to Cloud Storage.
/// # gax::Result::<()>::Ok(()) });
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
    inner: reqwest::Client,
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
    /// # gax::Result::<()>::Ok(()) });
    /// ```
    pub fn builder() -> ClientBuilder {
        gax::client_builder::internal::new_builder(client_builder::Factory)
    }

    /// A simple upload from a buffer.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let response = client
    ///         .insert_object(
    ///             "projects/_/buckets/my-bucket",
    ///             "my-object",
    ///             "the quick brown fox jumped over the lazy dog",
    ///         )
    ///         .await?;
    ///     println!("response details={response:?}");
    ///     Ok(())
    /// }
    /// ```
    pub async fn insert_object<B, O, P>(
        &self,
        bucket: B,
        object: O,
        payload: P,
    ) -> crate::Result<Object>
    where
        B: Into<String>,
        O: Into<String>,
        P: Into<bytes::Bytes>,
    {
        let bucket: String = bucket.into();
        let bucket_id = bucket
            .as_str()
            .strip_prefix("projects/_/buckets/")
            .ok_or_else(|| {
                Error::other(format!(
                    "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
                ))
            })?;
        let object: String = object.into();
        let builder = self
            .inner
            .request(
                reqwest::Method::POST,
                format!("{}upload/storage/v1/b/{bucket_id}/o", &self.endpoint),
            )
            .query(&[("uploadType", "media")])
            .query(&[("name", &object)])
            .header("content-type", "application/octet-stream")
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );
        let cached_auth_headers = self
            .cred
            .headers(Extensions::new())
            .await
            .map_err(Error::authentication)?;

        let auth_headers = match cached_auth_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => reqwest::header::HeaderMap::new(),
        };

        let builder = auth_headers
            .iter()
            .fold(builder, |b, (k, v)| b.header(k, v));
        let builder = builder.body(payload.into());

        tracing::info!("builder={builder:?}");
        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        let response = response.json::<v1::Object>().await.map_err(Error::io)?;

        Ok(Object::from(response))
    }

    /// A simple download into a buffer.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::client::Storage;
    /// async fn example(client: &Storage) -> gax::Result<()> {
    ///     let contents = client
    ///         .read_object(
    ///             "projects/_/buckets/my-bucket",
    ///             "my-object",
    ///         )
    ///         .await?;
    ///     println!("object contents={contents:?}");
    ///     Ok(())
    /// }
    /// ```
    pub async fn read_object<B, O>(&self, bucket: B, object: O) -> crate::Result<bytes::Bytes>
    where
        B: Into<String>,
        O: Into<String>,
    {
        let bucket: String = bucket.into();
        let bucket_id = bucket
            .as_str()
            .strip_prefix("projects/_/buckets/")
            .ok_or_else(|| {
                Error::other(format!(
                    "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
                ))
            })?;
        let object: String = object.into();
        let builder = self
            .inner
            .request(
                reqwest::Method::GET,
                format!("{}storage/v1/b/{bucket_id}/o/{object}", &self.endpoint),
            )
            .query(&[("alt", "media")])
            .header(
                "x-goog-api-client",
                reqwest::header::HeaderValue::from_static(&self::info::X_GOOG_API_CLIENT_HEADER),
            );
        let cached_auth_headers = self
            .cred
            .headers(Extensions::new())
            .await
            .map_err(Error::authentication)?;
        let auth_headers = match cached_auth_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => reqwest::header::HeaderMap::new(),
        };

        let builder = auth_headers
            .iter()
            .fold(builder, |b, (k, v)| b.header(k, v));
        tracing::info!("builder={builder:?}");

        let response = builder.send().await.map_err(Error::io)?;
        if !response.status().is_success() {
            return gaxi::http::to_http_error(response).await;
        }
        let response = response.bytes().await.map_err(Error::io)?;

        Ok(response)
    }

    pub(crate) async fn new(config: gaxi::options::ClientConfig) -> crate::Result<Self> {
        let inner = reqwest::Client::new();
        let cred = if let Some(c) = config.cred.clone() {
            c
        } else {
            auth::credentials::Builder::default()
                .build()
                .map_err(Error::authentication)?
        };
        let endpoint = config
            .endpoint
            .unwrap_or_else(|| self::DEFAULT_HOST.to_string());
        Ok(Self {
            inner,
            cred,
            endpoint,
        })
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
/// # gax::Result::<()>::Ok(()) });
/// ```
pub type ClientBuilder =
    gax::client_builder::ClientBuilder<client_builder::Factory, gaxi::options::Credentials>;

pub(crate) mod client_builder {
    use super::Storage;
    pub struct Factory;
    impl gax::client_builder::internal::ClientFactory for Factory {
        type Client = Storage;
        type Credentials = gaxi::options::Credentials;
        async fn build(self, config: gaxi::options::ClientConfig) -> gax::Result<Self::Client> {
            Self::Client::new(config).await
        }
    }
}

/// The default host used by the service.
const DEFAULT_HOST: &str = "https://storage.googleapis.com/";

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
            ac.header_value()
        };
    }
}

mod v1 {
    #[serde_with::serde_as]
    #[derive(Debug, Default, serde::Deserialize)]
    #[serde(default, rename_all = "camelCase")]
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
        // TODO(#2039) - add all the other fields.
    }

    impl std::convert::From<Object> for control::model::Object {
        fn from(value: Object) -> Self {
            Self::new()
                .set_name(value.name)
                .set_bucket(format!("projects/_/buckets/{}", value.bucket))
                .set_generation(value.generation)
                .set_metageneration(value.metageneration)
                .set_content_type(value.content_type)
                .set_storage_class(value.storage_class)
                .set_size(value.size as i64)
        }
    }
}
