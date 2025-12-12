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

use crate::{error::SigningError, signed_url::UrlStyle, storage::client::ENCODED_CHARS};
use auth::signer::Signer;
use percent_encoding::{AsciiSet, utf8_percent_encode};
use std::collections::BTreeMap;

/// Same encoding set as used in https://cloud.google.com/storage/docs/request-endpoints#encoding
/// but for signed URLs, we do not encode '/'.
const PATH_ENCODE_SET: AsciiSet = ENCODED_CHARS.remove(b'/');

/// Creates [Signed URLs].
///
/// This builder allows you to generate signed URLs for Google Cloud Storage objects and buckets.
/// [Signed URLs] provide a way to give time-limited read or write access to specific resources
/// without sharing your credentials.
///
/// # Example
///
/// ```no_run
/// use google_cloud_storage::builder::storage::SignedUrlBuilder;
/// use std::time::Duration;
/// # use auth::signer::Signer;
///
/// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
/// let url = SignedUrlBuilder::for_object("my-bucket", "my-object.txt")
///     .with_method(http::Method::GET)
///     .with_expiration(Duration::from_secs(3600)) // 1 hour
///     .sign_with(signer)
///     .await?;
///
/// println!("Signed URL: {}", url);
/// # Ok(())
/// # }
/// ```
/// [signed urls]: https://docs.cloud.google.com/storage/docs/access-control/signed-urls
#[derive(Debug)]
#[allow(dead_code)]
pub struct SignedUrlBuilder {
    scope: SigningScope,
    method: http::Method,
    expiration: std::time::Duration,
    headers: BTreeMap<String, String>,
    query_parameters: BTreeMap<String, String>,
    endpoint: Option<String>,
    universe_domain: String,
    client_email: Option<String>,
    url_style: UrlStyle,
}

#[derive(Debug)]
#[allow(dead_code)]
enum SigningScope {
    Bucket(String),
    Object(String, String),
}

impl SigningScope {
    fn bucket_name(&self) -> String {
        let bucket = match self {
            SigningScope::Bucket(bucket) => bucket,
            SigningScope::Object(bucket, _) => bucket,
        };

        bucket.trim_start_matches("projects/_/buckets/").to_string()
    }

    fn bucket_endpoint(&self, scheme: &str, host: &str, url_style: UrlStyle) -> String {
        let bucket_name = self.bucket_name();
        match url_style {
            UrlStyle::PathStyle => {
                format!("{scheme}://{host}")
            }
            UrlStyle::BucketBoundHostname => {
                format!("{scheme}://{host}")
            }
            UrlStyle::VirtualHostedStyle => {
                format!("{scheme}://{bucket_name}.{host}")
            }
        }
    }

    fn canonical_uri(&self, url_style: UrlStyle) -> String {
        let bucket_name = self.bucket_name();
        match self {
            SigningScope::Object(_, object) => {
                let encoded_object = utf8_percent_encode(object, &PATH_ENCODE_SET);
                match url_style {
                    UrlStyle::PathStyle => {
                        format!("/{bucket_name}/{encoded_object}")
                    }
                    UrlStyle::BucketBoundHostname => {
                        format!("/{encoded_object}")
                    }
                    UrlStyle::VirtualHostedStyle => {
                        format!("/{encoded_object}")
                    }
                }
            }
            SigningScope::Bucket(_) => match url_style {
                UrlStyle::PathStyle => {
                    format!("/{bucket_name}")
                }
                UrlStyle::BucketBoundHostname => "".to_string(),
                UrlStyle::VirtualHostedStyle => "".to_string(),
            },
        }
    }

    fn canonical_url(&self, scheme: &str, host: &str, url_style: UrlStyle) -> String {
        let bucket_endpoint = self.bucket_endpoint(scheme, host, url_style.clone());
        let uri = self.canonical_uri(url_style.clone());
        format!("{bucket_endpoint}{uri}")
    }
}

impl SignedUrlBuilder {
    fn new(scope: SigningScope) -> Self {
        Self {
            scope,
            method: http::Method::GET,
            expiration: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            headers: BTreeMap::new(),
            query_parameters: BTreeMap::new(),
            endpoint: None,
            universe_domain: "googleapis.com".to_string(),
            client_email: None,
            url_style: UrlStyle::PathStyle,
        }
    }

    /// Creates a new `SignedUrlBuilder` for a specific object.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use auth::signer::Signer;
    ///
    /// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// let url = SignedUrlBuilder::for_object("my-bucket", "my-object.txt")
    ///     .sign_with(signer)
    ///     .await?;
    /// # Ok(())
    /// # }
    ///```
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket containing the object.
    /// * `object` - The name of the object.    
    pub fn for_object<B, O>(bucket: B, object: O) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        Self::new(SigningScope::Object(bucket.into(), object.into()))
    }

    /// Creates a new `SignedUrlBuilder` for a specific bucket.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use auth::signer::Signer;
    ///
    /// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// let url = SignedUrlBuilder::for_bucket("my-bucket")
    ///     .sign_with(signer)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `bucket` - The name of the bucket.
    pub fn for_bucket<B>(bucket: B) -> Self
    where
        B: Into<String>,
    {
        Self::new(SigningScope::Bucket(bucket.into()))
    }

    /// Sets the HTTP method for the signed URL. The default is "GET".
    ///
    /// # Example
    ///
    /// ```no_run
    /// use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use auth::signer::Signer;
    ///
    /// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// let url = SignedUrlBuilder::for_object("my-bucket", "my-object.txt")
    ///     .with_method(http::Method::PUT)
    ///     .sign_with(signer)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_method(mut self, method: http::Method) -> Self {
        self.method = method;
        self
    }

    /// Sets the expiration time for the signed URL. The default is 7 days.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// use std::time::Duration;
    /// # use auth::signer::Signer;
    ///
    /// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// let url = SignedUrlBuilder::for_object("my-bucket", "my-object.txt")
    ///     .with_expiration(Duration::from_secs(3600))
    ///     .sign_with(signer)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_expiration(mut self, expiration: std::time::Duration) -> Self {
        self.expiration = expiration;
        self
    }

    /// Sets the URL style for the signed URL. The default is `UrlStyle::PathStyle`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// use google_cloud_storage::signed_url::UrlStyle;
    /// # use auth::signer::Signer;
    ///
    /// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// let url = SignedUrlBuilder::for_object("my-bucket", "my-object.txt")
    ///     .with_url_style(UrlStyle::VirtualHostedStyle)
    ///     .sign_with(signer)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_url_style(mut self, url_style: UrlStyle) -> Self {
        self.url_style = url_style;
        self
    }

    /// Adds a header to the signed URL.
    ///
    /// Note: These headers must be present in the request when using the signed URL.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use auth::signer::Signer;
    ///
    /// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// let url = SignedUrlBuilder::for_object("my-bucket", "my-object.txt")
    ///     .with_header("content-type", "text/plain")
    ///     .sign_with(signer)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_header<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Adds a query parameter to the signed URL.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use auth::signer::Signer;
    ///
    /// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// let url = SignedUrlBuilder::for_object("my-bucket", "my-object.txt")
    ///     .with_query_param("generation", "1234567890")
    ///     .sign_with(signer)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_query_param<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.query_parameters.insert(key.into(), value.into());
        self
    }

    /// Sets the endpoint for the signed URL. The default is "https://storage.googleapis.com".
    ///
    /// # Example
    ///
    /// ```no_run
    /// use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use auth::signer::Signer;
    ///
    /// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// let url = SignedUrlBuilder::for_object("my-bucket", "my-object.txt")
    ///     .with_endpoint("https://my-custom-endpoint.com")
    ///     .sign_with(signer)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets the universe domain for the signed URL. The default is "googleapis.com".
    ///
    /// # Example
    ///
    /// ```no_run
    /// use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use auth::signer::Signer;
    ///
    /// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// let url = SignedUrlBuilder::for_object("my-bucket", "my-object.txt")
    ///     .with_universe_domain("my-universe.com")
    ///     .sign_with(signer)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_universe_domain<S: Into<String>>(mut self, universe_domain: S) -> Self {
        self.universe_domain = universe_domain.into();
        self
    }

    /// Sets the client email for the signed URL.
    ///
    /// If not set, the email will be fetched from the signer.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use auth::signer::Signer;
    ///
    /// # async fn run(signer: &Signer) -> Result<(), Box<dyn std::error::Error>> {
    /// let url = SignedUrlBuilder::for_object("my-bucket", "my-object.txt")
    ///     .with_client_email("my-service-account@my-project.iam.gserviceaccount.com")
    ///     .sign_with(signer)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_client_email<S: Into<String>>(mut self, client_email: S) -> Self {
        self.client_email = Some(client_email.into());
        self
    }

    fn resolve_endpoint_url(&self) -> Result<(url::Url, String), SigningError> {
        let endpoint = self.resolve_endpoint();
        let url = url::Url::parse(&endpoint)
            .map_err(|e| SigningError::invalid_parameter("endpoint", e))?;

        let endpoint_url = url.clone();
        let host = url
            .host_str()
            .ok_or_else(|| SigningError::invalid_parameter("endpoint", "invalid endpoint host"))?;
        Ok((endpoint_url, host.to_string()))
    }

    fn resolve_endpoint(&self) -> String {
        match self.endpoint.as_ref() {
            Some(e) if e.starts_with("http://") => e.clone(),
            Some(e) if e.starts_with("https://") => e.clone(),
            Some(e) => format!("https://{}", e),
            None => format!("https://storage.{}", self.universe_domain),
        }
    }

    /// Generates the signed URL using the provided signer.
    ///
    /// # Arguments
    ///
    /// * `signer` - The signer to use for signing the URL.
    ///
    /// # Returns
    ///
    /// A `Result` containing the signed URL as a `String` or a `SigningError`.
    pub async fn sign_with(self, _signer: &Signer) -> std::result::Result<String, SigningError> {
        let (endpoint_url, host) = self.resolve_endpoint_url()?;
        let scheme = endpoint_url.scheme();
        let _canonical_url = self
            .scope
            .canonical_url(scheme, &host, self.url_style.clone());

        // TODO(#3645): implement gcs logic for signed url generation.
        Err(SigningError::signing("not implemented".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::signer::{Signer, SigningProvider};
    use tokio::time::Duration;

    type TestResult = anyhow::Result<()>;

    mockall::mock! {
        #[derive(Debug)]
        Signer {}

        impl SigningProvider for Signer {
            async fn client_email(&self) -> auth::signer::Result<String>;
            async fn sign(&self, content: &[u8]) -> auth::signer::Result<bytes::Bytes>;
        }
    }

    #[tokio::test]
    async fn test_signed_url_builder() -> TestResult {
        let mut mock = MockSigner::new();
        mock.expect_client_email()
            .return_once(|| Ok("test@example.com".to_string()));
        mock.expect_sign()
            .return_once(|_content| Ok(bytes::Bytes::from("test-signature")));

        let signer = Signer::from(mock);
        let err = SignedUrlBuilder::for_object("test-bucket", "test-object")
            .with_method(http::Method::PUT)
            .with_expiration(Duration::from_secs(3600))
            .with_header("x-goog-meta-test", "value")
            .with_query_param("test", "value")
            .with_endpoint("https://storage.googleapis.com")
            .with_universe_domain("googleapis.com")
            .with_client_email("test@example.com")
            .with_url_style(UrlStyle::PathStyle)
            .sign_with(&signer)
            .await
            .unwrap_err();

        assert!(err.is_signing());

        Ok(())
    }

    #[tokio::test]
    async fn test_signed_url_error_endpoint() -> TestResult {
        let mut mock = MockSigner::new();
        mock.expect_client_email()
            .return_once(|| Ok("test@example.com".to_string()));
        mock.expect_sign()
            .return_once(|_content| Ok(bytes::Bytes::from("test-signature")));

        let signer = Signer::from(mock);
        let err = SignedUrlBuilder::for_object("b", "o")
            .with_endpoint("invalid url")
            .sign_with(&signer)
            .await
            .unwrap_err();

        assert!(err.is_invalid_parameter());

        Ok(())
    }

    #[test_case::test_case(
        Some("path/with/slashes/under_score/amper&sand/file.ext"),
        None,
        UrlStyle::PathStyle,
        "https://storage.googleapis.com/test-bucket/path/with/slashes/under_score/amper%26sand/file.ext"
    ; "escape object name")]
    #[test_case::test_case(
        Some("folder/test object.txt"),
        None,
        UrlStyle::PathStyle,
        "https://storage.googleapis.com/test-bucket/folder/test%20object.txt"
    ; "escape object name with spaces")]
    #[test_case::test_case(
        Some("test-object"),
        None,
        UrlStyle::VirtualHostedStyle,
        "https://test-bucket.storage.googleapis.com/test-object"
    ; "virtual hosted style")]
    #[test_case::test_case(
        Some("test-object"),
        Some("http://mydomain.tld"),
        UrlStyle::BucketBoundHostname,
        "http://mydomain.tld/test-object"
    ; "bucket bound style")]
    #[test_case::test_case(
        None,
        None,
        UrlStyle::PathStyle,
        "https://storage.googleapis.com/test-bucket"
    ; "list objects")]
    fn test_signed_url_canonical_url(
        object: Option<&str>,
        endpoint: Option<&str>,
        url_style: UrlStyle,
        expected_url: &str,
    ) -> TestResult {
        let builder = if let Some(object) = object {
            SignedUrlBuilder::for_object("test-bucket", object)
        } else {
            SignedUrlBuilder::for_bucket("test-bucket")
        };
        let builder = builder.with_url_style(url_style);
        let builder = endpoint.iter().fold(builder, |builder, endpoint| {
            builder.with_endpoint(*endpoint)
        });

        let (endpoint_url, host) = builder.resolve_endpoint_url()?;
        let url = builder
            .scope
            .canonical_url(endpoint_url.scheme(), &host, builder.url_style);
        assert_eq!(url, expected_url);

        Ok(())
    }
}
