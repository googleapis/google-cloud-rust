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

use crate::error::SigningError;
use auth::signer::Signer;
use chrono::{DateTime, Utc};
use percent_encoding::{AsciiSet, utf8_percent_encode};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// https://cloud.google.com/storage/docs/request-endpoints#encoding
/// !, #, $, &, ', (, ), *, +, ,, /, :, ;, =, ?, @, [, ]
const PATH_ENCODE_SET: AsciiSet = AsciiSet::EMPTY
    .add(b' ')
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
    .add(b':')
    .add(b';')
    .add(b'=')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b']');

/// A builder for creating signed URLs.
#[derive(Debug)]
pub struct SignedUrlBuilder {
    scope: SigningScope<String, String>,
    method: http::Method,
    expiration: std::time::Duration,
    headers: BTreeMap<String, String>,
    query_parameters: BTreeMap<String, String>,
    endpoint: Option<String>,
    universe_domain: String,
    client_email: Option<String>,
    timestamp: DateTime<Utc>,
    url_style: UrlStyle,
}

#[derive(Debug, Clone)]
pub enum UrlStyle {
    PathStyle,
    BucketBoundHostname,
    VirtualHostedStyle,
}

impl Default for UrlStyle {
    fn default() -> Self {
        UrlStyle::PathStyle
    }
}

#[derive(Debug)]
pub enum SigningScope<B, O>
where
    B: Into<String>,
    O: Into<String>,
{
    Bucket(B),
    Object(B, O),
}

impl SigningScope<String, String> {
    fn bucket_name(&self) -> String {
        let bucket = match self {
            SigningScope::Bucket(bucket) => bucket,
            SigningScope::Object(bucket, _) => bucket,
        };

        bucket.trim_start_matches("projects/_/buckets/").to_string()
    }

    fn bucket_endpoint(&self, endpoint: &str, url_style: UrlStyle) -> String {
        let bucket_name = self.bucket_name();
        let scheme = if endpoint.starts_with("http://") {
            "http"
        } else {
            "https"
        };
        let endpoint = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        match url_style {
            UrlStyle::PathStyle => {
                format!("{scheme}://{endpoint}")
            }
            UrlStyle::BucketBoundHostname => {
                format!("{scheme}://{endpoint}")
            }
            UrlStyle::VirtualHostedStyle => {
                format!("{scheme}://{bucket_name}.{endpoint}")
            }
        }
    }

    fn canonical_uri(&self, url_style: UrlStyle) -> String {
        let bucket_name = self.bucket_name();
        match self {
            SigningScope::Object(_, object) => {
                let encoded_object = utf8_percent_encode(&object, &PATH_ENCODE_SET).to_string();
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

    fn canonical_url(&self, endpoint: &str, url_style: UrlStyle) -> String {
        let bucket_endpoint = self.bucket_endpoint(endpoint, url_style.clone());
        let uri = self.canonical_uri(url_style.clone());
        format!("{bucket_endpoint}{uri}")
    }
}

impl SignedUrlBuilder {
    fn new<B, O>(scope: SigningScope<B, O>) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        Self {
            scope: match scope {
                SigningScope::Bucket(bucket) => SigningScope::Bucket(bucket.into()),
                SigningScope::Object(bucket, object) => {
                    SigningScope::Object(bucket.into(), object.into())
                }
            },
            method: http::Method::GET,
            expiration: std::time::Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            headers: BTreeMap::new(),
            query_parameters: BTreeMap::new(),
            endpoint: None,
            universe_domain: "googleapis.com".to_string(),
            client_email: None,
            timestamp: Utc::now(),
            url_style: UrlStyle::PathStyle,
        }
    }

    pub fn for_object<B, O>(bucket: B, object: O) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        Self::new(SigningScope::Object(bucket, object))
    }

    pub fn for_bucket<B>(bucket: B) -> Self
    where
        B: Into<String>,
    {
        Self::new(SigningScope::Bucket(bucket))
    }

    #[cfg(test)]
    /// Sets the timestamp for the signed URL. Only used in tests.
    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Sets the HTTP method for the signed URL. Default is "GET".
    pub fn with_method(mut self, method: http::Method) -> Self {
        self.method = method;
        self
    }

    /// Sets the expiration time for the signed URL. Default is 7 days.
    pub fn with_expiration(mut self, expiration: std::time::Duration) -> Self {
        self.expiration = expiration;
        self
    }

    /// Sets the URL style for the signed URL. Default is `UrlStyle::PathStyle`.
    pub fn with_url_style(mut self, url_style: UrlStyle) -> Self {
        self.url_style = url_style;
        self
    }

    /// Adds a header to the signed URL.
    /// Note: These headers must be present in the request when using the signed URL.
    pub fn with_header<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Adds a query parameter to the signed URL.
    pub fn with_query_param<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.query_parameters.insert(key.into(), value.into());
        self
    }

    /// Sets the endpoint for the signed URL. Default is "https://storage.googleapis.com".
    pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets the universe domain for the signed URL. Default is "googleapis.com".
    pub fn with_universe_domain<S: Into<String>>(mut self, universe_domain: S) -> Self {
        self.universe_domain = universe_domain.into();
        self
    }

    /// Sets the client email for the signed URL.
    /// If not set, the email will be fetched from the signer.
    pub fn with_client_email<S: Into<String>>(mut self, client_email: S) -> Self {
        self.client_email = Some(client_email.into());
        self
    }

    fn resolve_endpoint(&self) -> String {
        if let Some(endpoint) = self.endpoint.clone() {
            if !endpoint.starts_with("http") {
                return format!("https://{}", endpoint);
            }
            return endpoint;
        }

        let emulator_host = std::env::var("STORAGE_EMULATOR_HOST");
        if let Ok(host) = emulator_host
            && !host.is_empty()
        {
            if host.starts_with("http") {
                return host;
            }
            return format!("http://{host}");
        }

        format!("https://storage.{}", self.universe_domain.clone())
    }

    /// Generates the signed URL using the provided signer.
    pub async fn sign_with(self, signer: &Signer) -> std::result::Result<String, SigningError> {
        let (url, _, _) = self.sign_internal(signer).await?;
        Ok(url)
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
    ) -> std::result::Result<(String, String, String), SigningError> {
        let now = self.timestamp;
        let request_timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let datestamp = now.format("%Y%m%d");
        let credential_scope = format!("{datestamp}/auto/storage/goog4_request");
        let client_email = if let Some(email) = self.client_email.clone() {
            email
        } else {
            signer.client_email().await.map_err(SigningError::Signing)?
        };
        let credential = format!("{client_email}/{credential_scope}");

        let endpoint = self.resolve_endpoint();
        let canonical_url = self.scope.canonical_url(&endpoint, self.url_style.clone());
        let endpoint_url =
            url::Url::parse(&canonical_url).map_err(|e| SigningError::InvalidEndpoint(e.into()))?;
        let endpoint_host = endpoint_url
            .host_str()
            .ok_or_else(|| SigningError::InvalidEndpoint("invalid endpoint host".into()))?;

        let mut headers = self.headers;
        headers.insert("host".to_string(), endpoint_host.to_string());

        let mut sorted_headers = headers.keys().collect::<Vec<_>>();
        sorted_headers.sort_by_key(|k| k.to_lowercase());

        let signed_headers = sorted_headers.iter().fold("".to_string(), |acc, k| {
            format!("{acc}{};", k.to_lowercase())
        });
        let signed_headers = signed_headers.trim_end_matches(';').to_string();

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
        let mut sorted_query_parameters_keys = query_parameters.keys().collect::<Vec<_>>();
        sorted_query_parameters_keys.sort_by_key(|k| k.to_string());
        sorted_query_parameters_keys.iter().for_each(|k| {
            let value = query_parameters.get(k.as_str());
            if value.is_none() {
                return;
            }
            let value = value.unwrap();
            canonical_query.append_pair(k, value);
        });
        let canonical_query = canonical_query.finish();
        let canonical_query = canonical_query
            .replace("%7E", "~") // rollback to ~
            .replace("+", "%20"); // missing %20 in +

        let canonical_headers = sorted_headers.iter().fold("".to_string(), |acc, k| {
            let header_value = headers.get(k.as_str());
            if header_value.is_none() {
                return acc;
            }
            let header_value = Self::canonicalize_header_value(&header_value.unwrap());
            format!("{acc}{}:{}\n", k.to_lowercase(), header_value)
        });

        // If the user provides a value for X-Goog-Content-SHA256, we must use
        // that value in the request string. If not, we use UNSIGNED-PAYLOAD.
        let signature = "UNSIGNED-PAYLOAD".to_string();
        let signature = headers.iter().fold(signature, |acc, (k, v)| {
            if k.to_lowercase().eq("x-goog-content-sha256") {
                return v.clone();
            }
            acc
        });

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
            .map_err(SigningError::Signing)?;

        let signed_url = format!(
            "{}?{}&X-Goog-Signature={}",
            canonical_url, canonical_query, signature
        );

        Ok((signed_url, string_to_sign, canonical_request))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use auth::credentials::service_account::Builder as ServiceAccount;
    use auth::signer::{Signer, SigningProvider};
    use chrono::DateTime;
    use scoped_env::ScopedEnv;
    use serde::Deserialize;
    use std::collections::HashMap;
    use tokio::time::Duration;

    type TestResult = anyhow::Result<()>;

    #[derive(Debug)]
    struct MockSigner;

    #[async_trait::async_trait]
    impl SigningProvider for MockSigner {
        async fn client_email(&self) -> auth::signer::Result<String> {
            Ok("test@example.com".to_string())
        }

        async fn sign(&self, _content: &[u8]) -> auth::signer::Result<String> {
            Ok("test-signature".to_string())
        }
    }

    #[tokio::test]
    async fn test_signed_url_generation() -> TestResult {
        let signer = Signer::from(MockSigner);
        let url = SignedUrlBuilder::new(SigningScope::Object("test-bucket", "test-object"))
            .with_method(http::Method::PUT)
            .with_expiration(std::time::Duration::from_secs(3600))
            .with_header("x-goog-meta-test", "value")
            .sign_with(&signer)
            .await
            .unwrap();

        assert!(url.starts_with("https://storage.googleapis.com/test-bucket/test-object"));
        assert!(url.contains("X-Goog-Signature=test-signature"));
        assert!(url.contains("X-Goog-Algorithm=GOOG4-RSA-SHA256"));
        assert!(url.contains("X-Goog-Credential=test%40example.com"));

        Ok(())
    }

    #[tokio::test]
    async fn test_signed_url_generation_escaping() -> TestResult {
        let signer: Signer = Signer::from(MockSigner);
        let url = SignedUrlBuilder::new(SigningScope::Object(
            "test-bucket",
            "folder/test object.txt",
        ))
        .with_method(http::Method::PUT)
        .with_header("content-type", "text/plain")
        .sign_with(&signer)
        .await
        .unwrap();

        assert!(
            url.starts_with("https://storage.googleapis.com/test-bucket/folder/test%20object.txt?")
        );
        assert!(url.contains("X-Goog-Signature="));

        Ok(())
    }

    #[tokio::test]
    async fn test_signed_url_error_signing() -> TestResult {
        #[derive(Debug)]
        struct FailSigner;
        #[async_trait::async_trait]
        impl SigningProvider for FailSigner {
            async fn client_email(&self) -> auth::signer::Result<String> {
                Ok("test@example.com".to_string())
            }
            async fn sign(&self, _content: &[u8]) -> auth::signer::Result<String> {
                Err(auth::signer::SigningError::from_msg("test".to_string()))
            }
        }
        let signer = Signer::from(FailSigner);
        let err = SignedUrlBuilder::new(SigningScope::Object("b", "o"))
            .sign_with(&signer)
            .await
            .unwrap_err();

        match err {
            SigningError::Signing(e) => assert!(e.is_sign()),
            _ => panic!("unexpected error type: {:?}", err),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_signed_url_error_endpoint() -> TestResult {
        let signer: Signer = Signer::from(MockSigner);
        let err = SignedUrlBuilder::new(SigningScope::Object("b", "o"))
            .with_endpoint("invalid url")
            .sign_with(&signer)
            .await
            .unwrap_err();

        match err {
            SigningError::InvalidEndpoint(_) => {}
            _ => panic!("unexpected error type: {:?}", err),
        }

        Ok(())
    }

    #[derive(Deserialize)]
    struct SignedUrlTestSuite {
        #[serde(rename = "signingV4Tests")]
        signing_v4_tests: Vec<SignedUrlTest>,
    }

    #[derive(Deserialize)]
    struct SignedUrlTest {
        description: String,
        bucket: String,
        object: Option<String>,
        method: String,
        expiration: u64,
        timestamp: String,
        #[serde(rename = "expectedUrl")]
        expected_url: String,
        headers: Option<HashMap<String, String>>,
        #[serde(rename = "queryParameters")]
        query_parameters: Option<HashMap<String, String>>,
        scheme: Option<String>,
        #[serde(rename = "urlStyle")]
        url_style: Option<String>,
        #[serde(rename = "bucketBoundHostname")]
        bucket_bound_hostname: Option<String>,
        #[serde(rename = "expectedCanonicalRequest")]
        expected_canonical_request: String,
        #[serde(rename = "expectedStringToSign")]
        expected_string_to_sign: String,
        hostname: Option<String>,
        #[serde(rename = "clientEndpoint")]
        client_endpoint: Option<String>,
        #[serde(rename = "emulatorHostname")]
        emulator_hostname: Option<String>,
        #[serde(rename = "universeDomain")]
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
            let builder = match test.object {
                Some(object) => SignedUrlBuilder::for_object(test.bucket, object),
                None => SignedUrlBuilder::for_bucket(test.bucket),
            };

            let emulator_hostname = test.emulator_hostname.unwrap_or_default();
            let _e = ScopedEnv::set("STORAGE_EMULATOR_HOST", emulator_hostname.as_str());

            let builder = builder
                .with_method(method)
                .with_url_style(url_style)
                .with_expiration(Duration::from_secs(test.expiration))
                .with_timestamp(timestamp.into());

            let builder = test
                .universe_domain
                .iter()
                .fold(builder, |builder, universe_domain| {
                    builder.with_universe_domain(universe_domain)
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

            let (signed_url, string_to_sign, canonical_request) =
                builder.sign_internal(&signer).await?;

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
        for test in failed_tests {
            println!("❌ Failed test: {}", test);
        }
        println!("{}/{} tests passed", total_passed, total_tests);

        if failed {
            Err(anyhow::anyhow!("Some tests failed"))
        } else {
            Ok(())
        }
    }
}
