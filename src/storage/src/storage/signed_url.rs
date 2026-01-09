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
use chrono::{DateTime, Utc};
use google_cloud_auth::signer::Signer;
use percent_encoding::{AsciiSet, utf8_percent_encode};
use sha2::{Digest, Sha256};
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
/// This implementation uses the [V4 signing process].
///
/// # Example: Generating a Signed URL
///
/// ## Generating a Signed URL for Downloading an Object (GET)
///
/// ```
/// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
/// use std::time::Duration;
/// use google_cloud_auth::signer::Signer;
/// # async fn run(signer: &Signer) -> anyhow::Result<()> {
/// let url = SignedUrlBuilder::for_object("projects/_/buckets/my-bucket", "my-object.txt")
///     .with_method(http::Method::GET)
///     .with_expiration(Duration::from_secs(3600)) // 1 hour
///     .sign_with(signer)
///     .await?;
///
/// println!("Signed URL: {}", url);
/// # Ok(())
/// # }
/// ```
///
/// ## Generating a Signed URL for Uploading an Object (PUT)
///
/// ```
/// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
/// use std::time::Duration;
/// # use google_cloud_auth::signer::Signer;
///
/// # async fn run(signer: &Signer) -> anyhow::Result<()> {
/// let url = SignedUrlBuilder::for_object("projects/_/buckets/my-bucket", "my-object.txt")
///     .with_method(http::Method::PUT)
///     .with_expiration(Duration::from_secs(3600)) // 1 hour
///     .with_header("content-type", "application/json") // Optional: Enforce content type
///     .sign_with(signer)
///     .await?;
///
/// println!("Upload URL: {}", url);
/// # Ok(())
/// # }
/// ```
///
/// # Example: Creating a Signer
///
/// You can use `google-cloud-auth` to create a `Signer`.
///
/// ## Using [Application Default Credentials] (ADC)
///
/// This is the recommended way for most applications. It automatically finds credentials
/// from the environment. See how [Application Default Credentials] works.
///
/// ```
/// use google_cloud_auth::credentials::Builder;
/// use google_cloud_auth::signer::Signer;
///
/// # fn build_signer() -> anyhow::Result<()> {
/// let signer = Builder::default().build_signer()?;
/// # Ok(())
/// # }
/// ```
///
/// ## Using a Service Account Key File
///
/// This is useful when you have a specific service account key file (JSON) and want to use it directly.
/// Service account based signers work by local signing and do not make network requests, which can be
/// useful in environments where network access is restricted and performance is critical.
///
/// <div class="warning">
///     <strong>Caution:</strong> Service account keys are a security risk if not managed correctly.
///     See <a href="https://docs.cloud.google.com/iam/docs/best-practices-for-managing-service-account-keys">
///     Best practices for managing service account keys</a> for more information.
/// </div>
///
/// ```
/// use google_cloud_auth::credentials::service_account::Builder;
/// use google_cloud_auth::signer::Signer;
///
/// # async fn build_signer() -> anyhow::Result<()> {
/// let service_account_key = serde_json::json!({ /* add details here */ });
///
/// let signer = Builder::new(service_account_key).build_signer()?;
/// # Ok(())
/// # }
/// ```
///
/// [Application Default Credentials]: https://docs.cloud.google.com/docs/authentication/application-default-credentials
/// [signed urls]: https://docs.cloud.google.com/storage/docs/access-control/signed-urls
/// [V4 signing process]: https://docs.cloud.google.com/storage/docs/access-control/signed-urls
#[derive(Debug)]
pub struct SignedUrlBuilder {
    scope: SigningScope,
    method: http::Method,
    expiration: std::time::Duration,
    headers: BTreeMap<String, String>,
    query_parameters: BTreeMap<String, String>,
    endpoint: Option<String>,
    client_email: Option<String>,
    timestamp: DateTime<Utc>,
    url_style: UrlStyle,
}

#[derive(Debug)]
enum SigningScope {
    Bucket(String),
    Object(String, String),
}

impl SigningScope {
    fn check_bucket_name(&self) -> Result<(), SigningError> {
        let bucket = match self {
            SigningScope::Bucket(bucket) => bucket,
            SigningScope::Object(bucket, _) => bucket,
        };

        bucket.strip_prefix("projects/_/buckets/").ok_or_else(|| {
            SigningError::invalid_parameter(
                "bucket",
                format!(
                    "malformed bucket name, it must start with `projects/_/buckets/`: {bucket}"
                ),
            )
        })?;

        Ok(())
    }

    fn bucket_name(&self) -> String {
        let bucket = match self {
            SigningScope::Bucket(bucket) => bucket,
            SigningScope::Object(bucket, _) => bucket,
        };

        bucket.trim_start_matches("projects/_/buckets/").to_string()
    }

    fn bucket_host(&self, host: &str, url_style: UrlStyle) -> String {
        match url_style {
            UrlStyle::PathStyle => host.to_string(),
            UrlStyle::BucketBoundHostname => host.to_string(),
            UrlStyle::VirtualHostedStyle => format!("{}.{host}", self.bucket_name()),
        }
    }

    fn bucket_endpoint(&self, scheme: &str, host: &str, url_style: UrlStyle) -> String {
        let bucket_host = self.bucket_host(host, url_style);
        format!("{scheme}://{bucket_host}")
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
        let bucket_endpoint = self.bucket_endpoint(scheme, host, url_style);
        let uri = self.canonical_uri(url_style);
        format!("{bucket_endpoint}{uri}")
    }
}

// Used to check conformance test expectations.
struct SigningComponents {
    #[cfg(test)]
    canonical_request: String,
    #[cfg(test)]
    string_to_sign: String,
    signed_url: String,
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
            client_email: None,
            timestamp: Utc::now(),
            url_style: UrlStyle::PathStyle,
        }
    }

    /// Creates a new `SignedUrlBuilder` for a specific object.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use google_cloud_auth::signer::Signer;
    /// async fn run(signer: &Signer) -> anyhow::Result<()> {
    ///     let url = SignedUrlBuilder::for_object("projects/_/buckets/my-bucket", "my-object.txt")
    ///         .sign_with(signer)
    ///         .await?;
    /// # Ok(())
    /// }
    ///```
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
    /// ```
    /// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use google_cloud_auth::signer::Signer;
    /// async fn run(signer: &Signer) -> anyhow::Result<()> {
    ///     let url = SignedUrlBuilder::for_bucket("projects/_/buckets/my-bucket")
    ///         .sign_with(signer)
    ///         .await?;
    /// # Ok(())
    /// }
    /// ```
    pub fn for_bucket<B>(bucket: B) -> Self
    where
        B: Into<String>,
    {
        Self::new(SigningScope::Bucket(bucket.into()))
    }

    #[cfg(test)]
    /// Sets the timestamp for the signed URL. Only used in tests.
    fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Sets the HTTP method for the signed URL. The default is [GET][crate::http::Method::GET].
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use google_cloud_auth::signer::Signer;
    /// use google_cloud_storage::signed_url::http;
    ///
    /// async fn run(signer: &Signer) -> anyhow::Result<()> {
    ///     let url = SignedUrlBuilder::for_object("projects/_/buckets/my-bucket", "my-object.txt")
    ///         .with_method(http::Method::PUT)
    ///         .sign_with(signer)
    ///         .await?;
    /// # Ok(())
    /// }
    /// ```
    pub fn with_method(mut self, method: http::Method) -> Self {
        self.method = method;
        self
    }

    /// Sets the expiration time for the signed URL. The default is 7 days.
    ///
    /// The maximum expiration time for V4 signed URLs is 7 days.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// use std::time::Duration;
    /// # use google_cloud_auth::signer::Signer;
    ///
    /// async fn run(signer: &Signer) -> anyhow::Result<()> {
    ///     let url = SignedUrlBuilder::for_object("projects/_/buckets/my-bucket", "my-object.txt")
    ///         .with_expiration(Duration::from_secs(3600))
    ///         .sign_with(signer)
    ///         .await?;
    /// # Ok(())
    /// }
    /// ```
    pub fn with_expiration(mut self, expiration: std::time::Duration) -> Self {
        self.expiration = expiration;
        self
    }

    /// Sets the URL style for the signed URL. The default is `UrlStyle::PathStyle`.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// use google_cloud_storage::signed_url::UrlStyle;
    /// # use google_cloud_auth::signer::Signer;
    ///
    /// async fn run(signer: &Signer) -> anyhow::Result<()> {
    ///     let url = SignedUrlBuilder::for_object("projects/_/buckets/my-bucket", "my-object.txt")
    ///         .with_url_style(UrlStyle::VirtualHostedStyle)
    ///         .sign_with(signer)
    ///         .await?;
    /// # Ok(())
    /// }
    /// ```
    pub fn with_url_style(mut self, url_style: UrlStyle) -> Self {
        self.url_style = url_style;
        self
    }

    /// Adds a header to the signed URL.
    ///
    /// Subsequent calls to this method with the same key will override the previous value.
    ///
    /// Note: These headers must be present in the request when using the signed URL.
    ///
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use google_cloud_auth::signer::Signer;
    /// async fn run(signer: &Signer) -> anyhow::Result<()> {
    ///     let url = SignedUrlBuilder::for_object("projects/_/buckets/my-bucket", "my-object.txt")
    ///         .with_header("content-type", "text/plain")
    ///         .sign_with(signer)
    ///         .await?;
    /// # Ok(())
    /// }
    /// ```
    pub fn with_header<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.headers.insert(key.into().to_lowercase(), value.into());
        self
    }

    /// Adds a query parameter to the signed URL.
    ///
    /// Subsequent calls to this method with the same key will override the previous value.
    ///
    /// # Example
    /// ```
    /// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use google_cloud_auth::signer::Signer;
    /// async fn run(signer: &Signer) -> anyhow::Result<()> {
    ///     let url = SignedUrlBuilder::for_object("projects/_/buckets/my-bucket", "my-object.txt")
    ///         .with_query_param("generation", "1234567890")
    ///         .sign_with(signer)
    ///         .await?;
    /// # Ok(())
    /// }
    /// ```
    pub fn with_query_param<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.query_parameters.insert(key.into(), value.into());
        self
    }

    /// Sets the endpoint for the signed URL. The default is `"https://storage.googleapis.com"`.
    ///
    /// This is useful when using a custom domain, or when testing with some Cloud Storage emulators.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use google_cloud_auth::signer::Signer;
    /// async fn run(signer: &Signer) -> anyhow::Result<()> {
    ///     let url = SignedUrlBuilder::for_object("projects/_/buckets/my-bucket", "my-object.txt")
    ///         .with_endpoint("https://private.googleapis.com")
    ///         .sign_with(signer)
    ///         .await?;
    /// # Ok(())
    /// }
    /// ```
    pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets the client email for the signed URL.
    ///
    /// If not set, the email will be fetched from the signer.
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_storage::builder::storage::SignedUrlBuilder;
    /// # use google_cloud_auth::signer::Signer;
    /// async fn run(signer: &Signer) -> anyhow::Result<()> {
    ///     let url = SignedUrlBuilder::for_object("projects/_/buckets/my-bucket", "my-object.txt")
    ///         .with_client_email("my-service-account@my-project.iam.gserviceaccount.com")
    ///         .sign_with(signer)
    ///         .await?;
    /// # Ok(())
    /// }
    /// ```
    pub fn with_client_email<S: Into<String>>(mut self, client_email: S) -> Self {
        self.client_email = Some(client_email.into());
        self
    }

    fn resolve_endpoint_url(&self) -> Result<SignedUrlEndpoint, SigningError> {
        let endpoint = self.resolve_endpoint();
        let url = url::Url::parse(&endpoint)
            .map_err(|e| SigningError::invalid_parameter("endpoint", e))?;
        let host = url.host_str().ok_or_else(|| {
            SigningError::invalid_parameter("endpoint", "Invalid endpoint, missing host.")
        })?;

        // Extract host and port exactly as they appear in the endpoint.
        // We do this because the url crate omits default ports (80/443),
        // but GCS requires them to be maintained if explicitly provided.
        let path = url.path();
        let scheme = format!("{}://", url.scheme());
        let host_with_port = endpoint.trim_start_matches(&scheme).trim_end_matches(path);

        Ok(SignedUrlEndpoint {
            scheme: url.scheme().to_string(),
            host_with_port: host_with_port.to_string(),
            host: host.to_string(),
        })
    }

    fn resolve_endpoint(&self) -> String {
        match self.endpoint.as_ref() {
            Some(e) if e.starts_with("http://") => e.clone(),
            Some(e) if e.starts_with("https://") => e.clone(),
            Some(e) => format!("https://{}", e),
            None => "https://storage.googleapis.com".to_string(),
        }
    }

    fn canonicalize_header_value(value: &str) -> String {
        let clean_value = value.replace("\t", " ").trim().to_string();
        clean_value.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    /// Generates the signed URL using the provided signer.
    /// Returns the signed URL, the string to sign, and the canonical request.
    /// Used to check conformance test expectations.
    async fn sign_internal(
        self,
        signer: &Signer,
    ) -> std::result::Result<SigningComponents, SigningError> {
        // Validate the bucket name.
        self.scope.check_bucket_name()?;

        let now = self.timestamp;
        let request_timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let datestamp = now.format("%Y%m%d");
        let credential_scope = format!("{datestamp}/auto/storage/goog4_request");
        let client_email = if let Some(email) = self.client_email.clone() {
            email
        } else {
            signer.client_email().await.map_err(SigningError::signing)?
        };
        let credential = format!("{client_email}/{credential_scope}");

        let endpoint = self.resolve_endpoint_url()?;
        let canonical_url = endpoint.canonical_url(&self.scope, self.url_style);
        let canonical_host = endpoint.canonical_host(&self.scope, self.url_style);

        let mut headers = self.headers;
        headers.insert("host".to_string(), canonical_host);

        let header_keys = headers.keys().cloned().collect::<Vec<_>>();
        let signed_headers = header_keys.join(";");

        let mut query_parameters = self.query_parameters;
        query_parameters.insert(
            "X-Goog-Algorithm".to_string(),
            "GOOG4-RSA-SHA256".to_string(),
        );
        query_parameters.insert("X-Goog-Credential".to_string(), credential);
        query_parameters.insert("X-Goog-Date".to_string(), request_timestamp.clone());
        query_parameters.insert(
            "X-Goog-Expires".to_string(),
            self.expiration.as_secs().to_string(),
        );
        query_parameters.insert("X-Goog-SignedHeaders".to_string(), signed_headers.clone());

        let mut canonical_query = url::form_urlencoded::Serializer::new("".to_string());
        for (k, v) in &query_parameters {
            canonical_query.append_pair(k, v);
        }

        let canonical_query = canonical_query.finish();
        let canonical_query = canonical_query
            .replace("%7E", "~") // rollback to ~
            .replace("+", "%20"); // missing %20 in +

        let canonical_headers = headers.iter().fold("".to_string(), |acc, (k, v)| {
            let header_value = Self::canonicalize_header_value(v);
            format!("{acc}{}:{}\n", k, header_value)
        });

        // If the user provides a value for X-Goog-Content-SHA256, we must use
        // that value in the request string. If not, we use UNSIGNED-PAYLOAD.
        let signature = headers
            .get("x-goog-content-sha256")
            .cloned()
            .unwrap_or_else(|| "UNSIGNED-PAYLOAD".to_string());

        let canonical_uri = self.scope.canonical_uri(self.url_style);
        let canonical_request = [
            self.method.to_string(),
            canonical_uri.clone(),
            canonical_query.clone(),
            canonical_headers,
            signed_headers,
            signature,
        ]
        .join("\n");

        let canonical_request_hash = Sha256::digest(canonical_request.as_bytes());
        let canonical_request_hash = hex::encode(canonical_request_hash);

        let string_to_sign = [
            "GOOG4-RSA-SHA256".to_string(),
            request_timestamp,
            credential_scope,
            canonical_request_hash,
        ]
        .join("\n");

        let signature = signer
            .sign(string_to_sign.as_str())
            .await
            .map_err(SigningError::signing)?;

        let signature = hex::encode(signature);

        let signed_url = format!(
            "{}?{}&X-Goog-Signature={}",
            canonical_url, canonical_query, signature
        );

        Ok(SigningComponents {
            #[cfg(test)]
            canonical_request,
            #[cfg(test)]
            string_to_sign,
            signed_url,
        })
    }

    /// Generates the signed URL using the provided signer.
    ///
    /// # Returns
    ///
    /// A `Result` containing the signed URL as a `String` or a `SigningError`.
    pub async fn sign_with(self, signer: &Signer) -> std::result::Result<String, SigningError> {
        let components = self.sign_internal(signer).await?;
        Ok(components.signed_url)
    }
}

/// The resolved endpoint for a signed URL.
struct SignedUrlEndpoint {
    scheme: String,
    host: String,
    host_with_port: String,
}

impl SignedUrlEndpoint {
    fn canonical_url(&self, scope: &SigningScope, url_style: UrlStyle) -> String {
        scope.canonical_url(&self.scheme, &self.host_with_port, url_style)
    }

    fn canonical_host(&self, scope: &SigningScope, url_style: UrlStyle) -> String {
        scope.bucket_host(&self.host, url_style)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use google_cloud_auth::credentials::service_account::Builder as ServiceAccount;
    use google_cloud_auth::signer::{Result as SignResult, Signer, SigningError, SigningProvider};
    use serde::Deserialize;
    use std::collections::HashMap;
    use tokio::time::Duration;

    type TestResult = anyhow::Result<()>;

    mockall::mock! {
        #[derive(Debug)]
        Signer {}

        impl SigningProvider for Signer {
            async fn client_email(&self) -> SignResult<String>;
            async fn sign(&self, content: &[u8]) -> SignResult<bytes::Bytes>;
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
        let _ = SignedUrlBuilder::for_object("projects/_/buckets/test-bucket", "test-object")
            .with_method(http::Method::PUT)
            .with_expiration(Duration::from_secs(3600))
            .with_header("x-goog-meta-test", "value")
            .with_query_param("test", "value")
            .with_endpoint("https://storage.googleapis.com")
            .with_client_email("test@example.com")
            .with_url_style(UrlStyle::PathStyle)
            .sign_with(&signer)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_signed_url_error_signing() -> TestResult {
        let mut mock = MockSigner::new();
        mock.expect_client_email()
            .return_once(|| Ok("test@example.com".to_string()));
        mock.expect_sign()
            .return_once(|_content| Err(SigningError::from_msg("test".to_string())));

        let signer = Signer::from(mock);
        let err = SignedUrlBuilder::for_object("projects/_/buckets/b", "o")
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
        let err = SignedUrlBuilder::for_object("projects/_/buckets/b", "o")
            .with_endpoint("invalid url")
            .sign_with(&signer)
            .await
            .unwrap_err();

        assert!(err.is_invalid_parameter());
        assert!(err.to_string().contains("invalid `endpoint` parameter"));

        Ok(())
    }

    #[tokio::test]
    async fn test_signed_url_error_bucket() -> TestResult {
        let mut mock = MockSigner::new();
        mock.expect_client_email()
            .return_once(|| Ok("test@example.com".to_string()));
        mock.expect_sign()
            .return_once(|_content| Ok(bytes::Bytes::from("test-signature")));

        let signer = Signer::from(mock);
        let err = SignedUrlBuilder::for_object("invalid-bucket-name", "o")
            .sign_with(&signer)
            .await
            .unwrap_err();

        assert!(err.is_invalid_parameter());
        assert!(err.to_string().contains("malformed bucket name"));

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
            SignedUrlBuilder::for_object("projects/_/buckets/test-bucket", object)
        } else {
            SignedUrlBuilder::for_bucket("projects/_/buckets/test-bucket")
        };
        let builder = builder.with_url_style(url_style);
        let builder = endpoint.iter().fold(builder, |builder, endpoint| {
            builder.with_endpoint(*endpoint)
        });

        let endpoint = builder.resolve_endpoint_url()?;
        let url = endpoint.canonical_url(&builder.scope, builder.url_style);
        assert_eq!(url, expected_url);

        Ok(())
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SignedUrlTestSuite {
        signing_v4_tests: Vec<SignedUrlTest>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SignedUrlTest {
        description: String,
        bucket: String,
        object: Option<String>,
        method: String,
        expiration: u64,
        timestamp: String,
        expected_url: String,
        headers: Option<HashMap<String, String>>,
        query_parameters: Option<HashMap<String, String>>,
        scheme: Option<String>,
        url_style: Option<String>,
        bucket_bound_hostname: Option<String>,
        expected_canonical_request: String,
        expected_string_to_sign: String,
        hostname: Option<String>,
        client_endpoint: Option<String>,
        emulator_hostname: Option<String>,
        universe_domain: Option<String>,
    }

    #[tokio::test]
    async fn signed_url_conformance() -> anyhow::Result<()> {
        let service_account_key = serde_json::from_slice(include_bytes!(
            "conformance/test_service_account.not-a-test.json"
        ))?;

        let signer = ServiceAccount::new(service_account_key)
            .build_signer()
            .expect("failed to build signer");

        let suite: SignedUrlTestSuite =
            serde_json::from_slice(include_bytes!("conformance/v4_signatures.json"))?;

        let mut failed_tests = Vec::new();
        let mut skipped_tests = Vec::new();
        let mut passed_tests = Vec::new();
        let total_tests = suite.signing_v4_tests.len();
        for test in suite.signing_v4_tests {
            let timestamp =
                DateTime::parse_from_rfc3339(&test.timestamp).expect("invalid timestamp");
            let method = http::Method::from_bytes(test.method.as_bytes()).expect("invalid method");
            let scheme = test.scheme.unwrap_or("https".to_string());
            let url_style = match test.url_style {
                Some(url_style) => match url_style.as_str() {
                    "VIRTUAL_HOSTED_STYLE" => UrlStyle::VirtualHostedStyle,
                    "BUCKET_BOUND_HOSTNAME" => UrlStyle::BucketBoundHostname,
                    _ => UrlStyle::PathStyle,
                },
                None => UrlStyle::PathStyle,
            };

            let bucket = format!("projects/_/buckets/{}", test.bucket);
            let builder = match test.object {
                Some(object) => SignedUrlBuilder::for_object(bucket, object),
                None => SignedUrlBuilder::for_bucket(bucket),
            };

            if test.emulator_hostname.is_some() {
                skipped_tests.push(test.description);
                continue;
            }

            let builder = builder
                .with_method(method)
                .with_url_style(url_style)
                .with_expiration(Duration::from_secs(test.expiration))
                .with_timestamp(timestamp.into());

            let builder = test
                .universe_domain
                .iter()
                .fold(builder, |builder, universe_domain| {
                    builder.with_endpoint(format!("https://storage.{}", universe_domain))
                });
            let builder = test
                .client_endpoint
                .iter()
                .fold(builder, |builder, client_endpoint| {
                    builder.with_endpoint(client_endpoint)
                });
            let builder = test
                .bucket_bound_hostname
                .iter()
                .fold(builder, |builder, hostname| {
                    builder.with_endpoint(format!("{}://{}", scheme, hostname))
                });
            let builder = test.hostname.iter().fold(builder, |builder, hostname| {
                builder.with_endpoint(format!("{}://{}", scheme, hostname))
            });
            let builder = test.headers.iter().fold(builder, |builder, headers| {
                headers.iter().fold(builder, |builder, (k, v)| {
                    builder.with_header(k.clone(), v.clone())
                })
            });
            let builder = test
                .query_parameters
                .iter()
                .fold(builder, |builder, query_params| {
                    query_params.iter().fold(builder, |builder, (k, v)| {
                        builder.with_query_param(k.clone(), v.clone())
                    })
                });

            let components = builder.sign_internal(&signer).await;
            let components = match components {
                Ok(components) => components,
                Err(e) => {
                    println!("❌ Failed test: {}", test.description);
                    println!("Error: {}", e);
                    failed_tests.push(test.description);
                    continue;
                }
            };

            let canonical_request = components.canonical_request;
            let string_to_sign = components.string_to_sign;
            let signed_url = components.signed_url;

            if canonical_request != test.expected_canonical_request
                || string_to_sign != test.expected_string_to_sign
                || signed_url != test.expected_url
            {
                println!("❌ Failed test: {}", test.description);
                let diff = pretty_assertions::StrComparison::new(
                    &canonical_request,
                    &test.expected_canonical_request,
                );
                println!("Canonical request diff: {}", diff);
                let diff = pretty_assertions::StrComparison::new(
                    &string_to_sign,
                    &test.expected_string_to_sign,
                );
                println!("String to sign diff: {}", diff);
                let diff = pretty_assertions::StrComparison::new(&signed_url, &test.expected_url);
                println!("Signed URL diff: {}", diff);
                failed_tests.push(test.description);
                continue;
            }
            passed_tests.push(test.description);
        }

        let failed = !failed_tests.is_empty();
        let total_passed = passed_tests.len();
        for test in passed_tests {
            println!("✅ Passed test: {}", test);
        }
        for test in skipped_tests {
            println!("🟡 Skipped test: {}", test);
        }
        for test in failed_tests {
            println!("❌ Failed test: {}", test);
        }
        println!("{}/{} tests passed", total_passed, total_tests);

        if failed {
            anyhow::bail!("Some tests failed")
        }
        Ok(())
    }
}
