// Copyright 2026 Google LLC
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
use crate::signed_url::UrlStyle;
use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::{DateTime, SecondsFormat, Utc};
use google_cloud_auth::signer::Signer;
use serde::Serialize;
use std::collections::BTreeMap;

/// Describes the URL and form fields for a signed V4 POST policy.
///
/// Applications should use [url][Self::url] as the HTML form action and include
/// every entry in [fields][Self::fields] as a multipart form field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostPolicyV4 {
    /// The URL that receives the multipart upload.
    pub url: String,
    /// The multipart form fields that must accompany the file upload.
    pub fields: BTreeMap<String, String>,
}

/// Optional form fields for a signed V4 POST policy.
///
/// Each non-empty field is added to the generated policy and to the returned
/// form fields. Metadata keys must begin with `x-goog-meta-`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PolicyV4Fields {
    /// Access control permissions for the uploaded object.
    pub acl: String,
    /// Cache control directives for the uploaded object.
    pub cache_control: String,
    /// Content disposition for the uploaded object.
    pub content_disposition: String,
    /// Content encoding for the uploaded object.
    pub content_encoding: String,
    /// Content type for the uploaded object.
    pub content_type: String,
    /// Custom metadata for the uploaded object. Keys must begin with `x-goog-meta-`.
    pub metadata: BTreeMap<String, String>,
    /// Status code Cloud Storage returns after a successful upload.
    pub status_code_on_success: Option<u16>,
    /// Redirect URL Cloud Storage returns after a successful upload.
    pub redirect_to_url_on_success: String,
}

impl PolicyV4Fields {
    /// Creates empty policy fields.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the access control permissions for the uploaded object.
    pub fn with_acl<S: Into<String>>(mut self, acl: S) -> Self {
        self.acl = acl.into();
        self
    }

    /// Sets the cache control directives for the uploaded object.
    pub fn with_cache_control<S: Into<String>>(mut self, cache_control: S) -> Self {
        self.cache_control = cache_control.into();
        self
    }

    /// Sets the content disposition for the uploaded object.
    pub fn with_content_disposition<S: Into<String>>(mut self, content_disposition: S) -> Self {
        self.content_disposition = content_disposition.into();
        self
    }

    /// Sets the content encoding for the uploaded object.
    pub fn with_content_encoding<S: Into<String>>(mut self, content_encoding: S) -> Self {
        self.content_encoding = content_encoding.into();
        self
    }

    /// Sets the content type for the uploaded object.
    pub fn with_content_type<S: Into<String>>(mut self, content_type: S) -> Self {
        self.content_type = content_type.into();
        self
    }

    /// Adds a custom metadata field for the uploaded object.
    ///
    /// The key must begin with `x-goog-meta-`.
    pub fn with_metadata<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Sets the status code Cloud Storage returns after a successful upload.
    pub fn with_status_code_on_success(mut self, status_code: u16) -> Self {
        self.status_code_on_success = (status_code > 0).then_some(status_code);
        self
    }

    /// Sets the redirect URL Cloud Storage returns after a successful upload.
    pub fn with_redirect_to_url_on_success<S: Into<String>>(mut self, redirect_url: S) -> Self {
        self.redirect_to_url_on_success = redirect_url.into();
        self
    }
}

/// A constraint that the uploaded multipart form must satisfy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PostPolicyV4Condition {
    /// Requires a multipart form field to start with a prefix.
    StartsWith {
        /// The multipart form field name, for example `$key` or `$acl`.
        field: String,
        /// The required field prefix.
        value: String,
    },
    /// Requires the uploaded file size to fall within a byte range.
    ContentLengthRange {
        /// The inclusive minimum byte length.
        start: u64,
        /// The inclusive maximum byte length.
        end: u64,
    },
}

impl PostPolicyV4Condition {
    /// Creates a `starts-with` condition.
    ///
    /// Empty values are ignored, matching the Go Storage SDK behavior.
    pub fn starts_with<K, V>(field: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        Self::StartsWith {
            field: field.into(),
            value: value.into(),
        }
    }

    /// Creates a `content-length-range` condition.
    ///
    /// A range of `0..=0` is ignored, matching the Go Storage SDK behavior.
    pub fn content_length_range(start: u64, end: u64) -> Self {
        Self::ContentLengthRange { start, end }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::StartsWith { value, .. } => value.is_empty(),
            Self::ContentLengthRange { start, end } => *start == 0 && *end == 0,
        }
    }

    fn to_json_condition(&self) -> JsonCondition {
        match self {
            Self::StartsWith { field, value } => JsonCondition::StringArray(vec![
                "starts-with".to_string(),
                field.clone(),
                value.clone(),
            ]),
            Self::ContentLengthRange { start, end } => {
                JsonCondition::ContentLengthRange(*start, *end)
            }
        }
    }
}

#[derive(Debug)]
enum JsonCondition {
    Object(BTreeMap<String, String>),
    StringArray(Vec<String>),
    ContentLengthRange(u64, u64),
}

impl Serialize for JsonCondition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Object(value) => value.serialize(serializer),
            Self::StringArray(value) => value.serialize(serializer),
            Self::ContentLengthRange(start, end) => {
                ("content-length-range", start, end).serialize(serializer)
            }
        }
    }
}

#[derive(Serialize)]
struct JsonPolicy {
    conditions: Vec<JsonCondition>,
    expiration: String,
}

/// Creates signed V4 POST policies for Cloud Storage uploads.
///
/// The generated policy lets an unauthenticated caller upload one object with a
/// multipart HTML form while satisfying the returned policy fields and any
/// additional conditions.
#[derive(Debug)]
pub struct PostPolicyV4Builder {
    bucket: String,
    object: String,
    expiration: Expiration,
    fields: PolicyV4Fields,
    conditions: Vec<PostPolicyV4Condition>,
    endpoint: Option<String>,
    client_email: Option<String>,
    timestamp: DateTime<Utc>,
    url_style: UrlStyle,
}

#[derive(Debug)]
enum Expiration {
    Duration(std::time::Duration),
    At(DateTime<Utc>),
}

impl PostPolicyV4Builder {
    /// Creates a builder for an object signed POST policy.
    ///
    /// `bucket` must use the resource form `projects/_/buckets/{bucket}`.
    pub fn for_object<B, O>(bucket: B, object: O) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            expiration: Expiration::Duration(std::time::Duration::from_secs(7 * 24 * 60 * 60)),
            fields: PolicyV4Fields::default(),
            conditions: Vec::new(),
            endpoint: None,
            client_email: None,
            timestamp: Utc::now(),
            url_style: UrlStyle::PathStyle,
        }
    }

    /// Sets how long the policy remains valid.
    ///
    /// The default is 7 days.
    pub fn with_expiration(mut self, expiration: std::time::Duration) -> Self {
        self.expiration = Expiration::Duration(expiration);
        self
    }

    /// Sets the absolute expiration time for the policy.
    pub fn with_expires_at(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expiration = Expiration::At(expires_at);
        self
    }

    /// Sets the URL style for the returned upload URL.
    pub fn with_url_style(mut self, url_style: UrlStyle) -> Self {
        self.url_style = url_style;
        self
    }

    /// Sets optional form fields for the generated policy.
    pub fn with_fields(mut self, fields: PolicyV4Fields) -> Self {
        self.fields = fields;
        self
    }

    /// Adds a condition that the multipart upload must satisfy.
    pub fn with_condition(mut self, condition: PostPolicyV4Condition) -> Self {
        self.conditions.push(condition);
        self
    }

    /// Sets the endpoint for the returned upload URL.
    ///
    /// This is useful for bucket-bound hostnames and for testing against custom
    /// Cloud Storage-compatible endpoints. Endpoints without a scheme use
    /// `https`.
    pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets the client email used in the signing credential.
    ///
    /// If not set, the email is fetched from the provided signer.
    pub fn with_client_email<S: Into<String>>(mut self, client_email: S) -> Self {
        self.client_email = Some(client_email.into());
        self
    }

    fn bucket_name(&self) -> Result<String, SigningError> {
        self.bucket
            .strip_prefix("projects/_/buckets/")
            .map(str::to_string)
            .ok_or_else(|| {
                SigningError::invalid_parameter(
                    "bucket",
                    format!(
                        "malformed bucket name, it must start with `projects/_/buckets/`: {}",
                        self.bucket
                    ),
                )
            })
            .and_then(|bucket| {
                if bucket.is_empty() {
                    Err(SigningError::invalid_parameter(
                        "bucket",
                        "bucket must be non-empty",
                    ))
                } else {
                    Ok(bucket)
                }
            })
    }

    fn expiration_time(&self) -> DateTime<Utc> {
        match self.expiration {
            Expiration::Duration(duration) => self.timestamp + duration,
            Expiration::At(expires_at) => expires_at,
        }
    }

    fn resolve_endpoint_url(&self) -> Result<PostPolicyEndpoint, SigningError> {
        let endpoint = self.resolve_endpoint();
        let url = url::Url::parse(&endpoint)
            .map_err(|e| SigningError::invalid_parameter("endpoint", e))?;
        url.host_str().ok_or_else(|| {
            SigningError::invalid_parameter("endpoint", "Invalid endpoint, missing host.")
        })?;

        let path = url.path();
        let scheme = format!("{}://", url.scheme());
        let host_with_port = endpoint.trim_start_matches(&scheme).trim_end_matches(path);

        Ok(PostPolicyEndpoint {
            scheme: url.scheme().to_string(),
            host_with_port: host_with_port.to_string(),
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

    fn validate_metadata(&self) -> Result<(), SigningError> {
        let invalid = self
            .fields
            .metadata
            .keys()
            .filter(|key| !key.starts_with("x-goog-meta-"))
            .cloned()
            .collect::<Vec<_>>();
        if invalid.is_empty() {
            Ok(())
        } else {
            Err(SigningError::invalid_parameter(
                "fields.metadata",
                format!(
                    "expected metadata keys to begin with `x-goog-meta-`, got {}",
                    invalid.join(", ")
                ),
            ))
        }
    }

    fn push_single_value_condition(
        conditions: &mut Vec<JsonCondition>,
        name: &str,
        value: impl Into<String>,
    ) {
        let value = value.into();
        if value.is_empty() {
            return;
        }
        conditions.push(single_value_condition(name, value));
    }

    fn policy_conditions(
        &self,
        bucket: &str,
        request_timestamp: &str,
        credential: &str,
    ) -> Vec<JsonCondition> {
        let mut conditions = self
            .conditions
            .iter()
            .filter(|condition| !condition.is_empty())
            .map(PostPolicyV4Condition::to_json_condition)
            .collect::<Vec<_>>();

        // These are ordered lexicographically to match Go and the
        // cross-language conformance fixture.
        Self::push_single_value_condition(&mut conditions, "acl", &self.fields.acl);
        Self::push_single_value_condition(
            &mut conditions,
            "cache-control",
            &self.fields.cache_control,
        );
        Self::push_single_value_condition(
            &mut conditions,
            "content-disposition",
            &self.fields.content_disposition,
        );
        Self::push_single_value_condition(
            &mut conditions,
            "content-encoding",
            &self.fields.content_encoding,
        );
        Self::push_single_value_condition(
            &mut conditions,
            "content-type",
            &self.fields.content_type,
        );
        Self::push_single_value_condition(
            &mut conditions,
            "success_action_redirect",
            &self.fields.redirect_to_url_on_success,
        );
        if let Some(status) = self.fields.status_code_on_success {
            Self::push_single_value_condition(
                &mut conditions,
                "success_action_status",
                status.to_string(),
            );
        }
        for (key, value) in &self.fields.metadata {
            Self::push_single_value_condition(&mut conditions, key, value);
        }

        Self::push_single_value_condition(&mut conditions, "bucket", bucket);
        Self::push_single_value_condition(&mut conditions, "key", &self.object);
        Self::push_single_value_condition(&mut conditions, "x-goog-date", request_timestamp);
        Self::push_single_value_condition(&mut conditions, "x-goog-credential", credential);
        Self::push_single_value_condition(&mut conditions, "x-goog-algorithm", "GOOG4-RSA-SHA256");

        conditions
    }

    fn policy_fields(
        &self,
        request_timestamp: String,
        credential: String,
    ) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::from([
            ("key".to_string(), self.object.clone()),
            ("x-goog-date".to_string(), request_timestamp),
            ("x-goog-credential".to_string(), credential),
            (
                "x-goog-algorithm".to_string(),
                "GOOG4-RSA-SHA256".to_string(),
            ),
            ("acl".to_string(), self.fields.acl.clone()),
            (
                "cache-control".to_string(),
                self.fields.cache_control.clone(),
            ),
            (
                "content-disposition".to_string(),
                self.fields.content_disposition.clone(),
            ),
            (
                "content-encoding".to_string(),
                self.fields.content_encoding.clone(),
            ),
            ("content-type".to_string(), self.fields.content_type.clone()),
            (
                "success_action_redirect".to_string(),
                self.fields.redirect_to_url_on_success.clone(),
            ),
        ]);

        for (key, value) in &self.fields.metadata {
            fields.insert(key.clone(), value.clone());
        }
        if let Some(status) = self.fields.status_code_on_success {
            fields.insert("success_action_status".to_string(), status.to_string());
        }
        fields.retain(|_, value| !value.is_empty());
        fields
    }

    /// Generates the signed V4 POST policy using the provided signer.
    pub async fn sign_with(
        self,
        signer: &Signer,
    ) -> std::result::Result<PostPolicyV4, SigningError> {
        let bucket = self.bucket_name()?;
        self.validate_metadata()?;

        let expires_at = self.expiration_time();
        if expires_at < self.timestamp {
            return Err(SigningError::invalid_parameter(
                "expires_at",
                "expiration must not be in the past",
            ));
        }

        let request_timestamp = self.timestamp.format("%Y%m%dT%H%M%SZ").to_string();
        let datestamp = self.timestamp.format("%Y%m%d").to_string();
        let client_email = match self.client_email.clone() {
            Some(email) => email,
            None => signer.client_email().await.map_err(SigningError::signing)?,
        };
        let credential = format!("{client_email}/{datestamp}/auto/storage/goog4_request");

        let policy = JsonPolicy {
            conditions: self.policy_conditions(&bucket, &request_timestamp, &credential),
            expiration: expires_at.to_rfc3339_opts(SecondsFormat::Secs, true),
        };
        let policy = serde_json::to_string(&policy)
            .map_err(|e| SigningError::invalid_parameter("policy", e))?;
        let policy = escape_like_go_json(&policy);
        let encoded_policy = BASE64_STANDARD.encode(policy);
        let signature = signer
            .sign(encoded_policy.as_bytes())
            .await
            .map_err(SigningError::signing)?;
        let signature = hex::encode(signature);

        let mut fields = self.policy_fields(request_timestamp, credential);
        fields.insert("policy".to_string(), encoded_policy);
        fields.insert("x-goog-signature".to_string(), signature);

        let endpoint = self.resolve_endpoint_url()?;
        Ok(PostPolicyV4 {
            url: endpoint.url(&bucket, self.url_style),
            fields,
        })
    }
}

struct PostPolicyEndpoint {
    scheme: String,
    host_with_port: String,
}

impl PostPolicyEndpoint {
    fn url(&self, bucket: &str, url_style: UrlStyle) -> String {
        let host = match url_style {
            UrlStyle::PathStyle => self.host_with_port.clone(),
            UrlStyle::BucketBoundHostname => self.host_with_port.clone(),
            UrlStyle::VirtualHostedStyle => format!("{}.{}", bucket, self.host_with_port),
        };
        let path = match url_style {
            UrlStyle::PathStyle => format!("/{bucket}/"),
            UrlStyle::BucketBoundHostname | UrlStyle::VirtualHostedStyle => "/".to_string(),
        };

        format!("{}://{host}{path}", self.scheme)
    }
}

fn single_value_condition(name: &str, value: impl Into<String>) -> JsonCondition {
    JsonCondition::Object(BTreeMap::from([(name.to_string(), value.into())]))
}

fn escape_like_go_json(json: &str) -> String {
    let mut escaped = String::with_capacity(json.len());
    for ch in json.chars() {
        match ch {
            '<' => escaped.push_str("\\u003c"),
            '>' => escaped.push_str("\\u003e"),
            '&' => escaped.push_str("\\u0026"),
            '\u{2028}' => escaped.push_str("\\u2028"),
            '\u{2029}' => escaped.push_str("\\u2029"),
            ch if ch.is_ascii() => escaped.push(ch),
            ch => push_json_unicode_escape(&mut escaped, ch),
        }
    }
    escaped
}

fn push_json_unicode_escape(output: &mut String, ch: char) {
    let code = ch as u32;
    if code <= 0xffff {
        output.push_str(&format!("\\u{code:04x}"));
        return;
    }

    let value = code - 0x1_0000;
    let high = 0xd800 + ((value >> 10) & 0x3ff);
    let low = 0xdc00 + (value & 0x3ff);
    output.push_str(&format!("\\u{high:04x}\\u{low:04x}"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_auth::credentials::service_account::Builder as ServiceAccount;
    use google_cloud_auth::signer::{Result as SignResult, SigningError, SigningProvider};
    use serde::Deserialize;
    use std::time::Duration;

    type TestResult = anyhow::Result<()>;

    mockall::mock! {
        #[derive(Debug)]
        Signer {}

        impl SigningProvider for Signer {
            async fn client_email(&self) -> SignResult<String>;
            async fn sign(&self, content: &[u8]) -> SignResult<bytes::Bytes>;
        }
    }

    impl PostPolicyV4Builder {
        fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
            self.timestamp = timestamp;
            self
        }
    }

    #[tokio::test]
    async fn post_policy_builder_generates_expected_fields() -> TestResult {
        let timestamp = DateTime::parse_from_rfc3339("2020-01-23T04:35:30Z")?.into();
        let mut mock = MockSigner::new();
        mock.expect_sign()
            .return_once(|content| Ok(bytes::Bytes::copy_from_slice(content)));

        let signer = Signer::from(mock);
        let policy =
            PostPolicyV4Builder::for_object("projects/_/buckets/test-bucket", "test-object")
                .with_timestamp(timestamp)
                .with_expiration(Duration::from_secs(10))
                .with_client_email("test@example.com")
                .with_fields(
                    PolicyV4Fields::new()
                        .with_acl("public-read")
                        .with_cache_control("public,max-age=60")
                        .with_status_code_on_success(201),
                )
                .sign_with(&signer)
                .await?;

        assert_eq!(policy.url, "https://storage.googleapis.com/test-bucket/");
        assert_eq!(policy.fields["key"], "test-object");
        assert_eq!(policy.fields["acl"], "public-read");
        assert_eq!(policy.fields["cache-control"], "public,max-age=60");
        assert_eq!(policy.fields["success_action_status"], "201");
        assert_eq!(policy.fields["x-goog-algorithm"], "GOOG4-RSA-SHA256");
        assert_eq!(
            policy.fields["x-goog-credential"],
            "test@example.com/20200123/auto/storage/goog4_request"
        );

        Ok(())
    }

    #[tokio::test]
    async fn rejects_invalid_metadata_key() -> TestResult {
        let mut mock = MockSigner::new();
        mock.expect_sign().never();

        let signer = Signer::from(mock);
        let err = PostPolicyV4Builder::for_object("projects/_/buckets/b", "o")
            .with_fields(PolicyV4Fields::new().with_metadata("bad-meta", "value"))
            .with_client_email("test@example.com")
            .sign_with(&signer)
            .await
            .unwrap_err();

        assert!(err.is_invalid_parameter(), "{err:?}");
        assert!(err.to_string().contains("fields.metadata"), "{err}");

        Ok(())
    }

    #[tokio::test]
    async fn rejects_expired_policy() -> TestResult {
        let timestamp = DateTime::parse_from_rfc3339("2020-01-23T04:35:30Z")?.into();
        let mut mock = MockSigner::new();
        mock.expect_sign().never();

        let signer = Signer::from(mock);
        let err = PostPolicyV4Builder::for_object("projects/_/buckets/b", "o")
            .with_timestamp(timestamp)
            .with_expires_at(DateTime::parse_from_rfc3339("2020-01-23T04:35:29Z")?.into())
            .with_client_email("test@example.com")
            .sign_with(&signer)
            .await
            .unwrap_err();

        assert!(err.is_invalid_parameter(), "{err:?}");
        assert!(err.to_string().contains("expires_at"), "{err}");

        Ok(())
    }

    #[tokio::test]
    async fn rejects_malformed_bucket_name() -> TestResult {
        let mut mock = MockSigner::new();
        mock.expect_sign().never();

        let signer = Signer::from(mock);
        let err = PostPolicyV4Builder::for_object("bucket", "o")
            .with_client_email("test@example.com")
            .sign_with(&signer)
            .await
            .unwrap_err();

        assert!(err.is_invalid_parameter(), "{err:?}");
        assert!(err.to_string().contains("malformed bucket name"), "{err}");

        Ok(())
    }

    #[tokio::test]
    async fn surfaces_signer_identity_error() -> TestResult {
        let mut mock = MockSigner::new();
        mock.expect_client_email()
            .return_once(|| Err(SigningError::from_msg("missing email")));
        mock.expect_sign().never();

        let signer = Signer::from(mock);
        let err = PostPolicyV4Builder::for_object("projects/_/buckets/b", "o")
            .sign_with(&signer)
            .await
            .unwrap_err();

        assert!(err.is_signing(), "{err:?}");

        Ok(())
    }

    #[tokio::test]
    async fn sign_with_is_send() -> TestResult {
        fn assert_send<T: Send>(_t: &T) {}

        let mut mock = MockSigner::new();
        mock.expect_sign()
            .return_once(|_| Err(SigningError::from_msg("signing failed")));

        let signer = Signer::from(mock);
        let fut = PostPolicyV4Builder::for_object("projects/_/buckets/b", "o")
            .with_client_email("test@example.com")
            .sign_with(&signer);

        assert_send(&fut);

        Ok(())
    }

    #[test]
    fn empty_conditions_are_ignored() -> TestResult {
        let timestamp = DateTime::parse_from_rfc3339("2020-01-23T04:35:30Z")?.into();
        let builder = PostPolicyV4Builder::for_object("projects/_/buckets/b", "o")
            .with_timestamp(timestamp)
            .with_condition(PostPolicyV4Condition::starts_with("$acl", ""))
            .with_condition(PostPolicyV4Condition::content_length_range(0, 0));

        let conditions = builder.policy_conditions(
            "b",
            "20200123T043530Z",
            "test@example.com/20200123/auto/storage/goog4_request",
        );
        let encoded = serde_json::to_string(&conditions)?;
        assert!(!encoded.contains("starts-with"), "{encoded}");
        assert!(!encoded.contains("content-length-range"), "{encoded}");

        Ok(())
    }

    #[test]
    fn zero_success_status_is_ignored() -> TestResult {
        let timestamp = DateTime::parse_from_rfc3339("2020-01-23T04:35:30Z")?.into();
        let builder = PostPolicyV4Builder::for_object("projects/_/buckets/b", "o")
            .with_timestamp(timestamp)
            .with_fields(PolicyV4Fields::new().with_status_code_on_success(0));

        let conditions = builder.policy_conditions(
            "b",
            "20200123T043530Z",
            "test@example.com/20200123/auto/storage/goog4_request",
        );
        let encoded = serde_json::to_string(&conditions)?;
        assert!(!encoded.contains("success_action_status"), "{encoded}");

        let fields = builder.policy_fields(
            "20200123T043530Z".to_string(),
            "test@example.com/20200123/auto/storage/goog4_request".to_string(),
        );
        assert!(!fields.contains_key("success_action_status"), "{fields:?}");

        Ok(())
    }

    #[test_case::test_case(
        UrlStyle::PathStyle,
        None,
        "https://storage.googleapis.com/test-bucket/"
    ; "path style")]
    #[test_case::test_case(
        UrlStyle::VirtualHostedStyle,
        None,
        "https://test-bucket.storage.googleapis.com/"
    ; "virtual hosted style")]
    #[test_case::test_case(
        UrlStyle::BucketBoundHostname,
        Some("http://mydomain.tld"),
        "http://mydomain.tld/"
    ; "bucket bound hostname")]
    fn post_policy_url_styles(
        url_style: UrlStyle,
        endpoint: Option<&str>,
        want: &str,
    ) -> TestResult {
        let builder =
            PostPolicyV4Builder::for_object("projects/_/buckets/test-bucket", "test-object")
                .with_url_style(url_style);
        let builder = endpoint.iter().fold(builder, |builder, endpoint| {
            builder.with_endpoint(*endpoint)
        });
        let endpoint = builder.resolve_endpoint_url()?;
        assert_eq!(endpoint.url("test-bucket", builder.url_style), want);

        Ok(())
    }

    #[test]
    fn json_escaping_matches_go_policy_bytes() {
        assert_eq!(
            escape_like_go_json("{\"key\":\"é<&>\"}"),
            "{\"key\":\"\\u00e9\\u003c\\u0026\\u003e\"}"
        );
        assert_eq!(
            escape_like_go_json("{\"key\":\"😀\"}"),
            "{\"key\":\"\\ud83d\\ude00\"}"
        );
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SignedUrlTestSuite {
        post_policy_v4_tests: Vec<PostPolicyV4Test>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PostPolicyV4Test {
        description: String,
        policy_input: PolicyInput,
        policy_output: PolicyOutput,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PolicyInput {
        scheme: String,
        bucket: String,
        object: String,
        expiration: u64,
        timestamp: String,
        url_style: Option<String>,
        bucket_bound_hostname: Option<String>,
        fields: Option<BTreeMap<String, String>>,
        conditions: Option<PolicyInputConditions>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PolicyInputConditions {
        starts_with: Option<Vec<String>>,
        content_length_range: Option<Vec<u64>>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PolicyOutput {
        url: String,
        fields: BTreeMap<String, String>,
        expected_decoded_policy: String,
    }

    #[tokio::test]
    async fn post_policy_conformance() -> TestResult {
        let service_account_key = serde_json::from_slice(include_bytes!(
            "conformance/test_service_account.not-a-test.json"
        ))?;

        let signer = ServiceAccount::new(service_account_key)
            .build_signer()
            .expect("failed to build signer");

        let suite: SignedUrlTestSuite =
            serde_json::from_slice(include_bytes!("conformance/v4_signatures.json"))?;

        let mut failed = Vec::new();
        for test in suite.post_policy_v4_tests {
            let timestamp = DateTime::parse_from_rfc3339(&test.policy_input.timestamp)?.to_utc();
            let bucket = format!("projects/_/buckets/{}", test.policy_input.bucket);
            let mut builder = PostPolicyV4Builder::for_object(bucket, test.policy_input.object)
                .with_timestamp(timestamp)
                .with_expiration(Duration::from_secs(test.policy_input.expiration));

            if let Some(style) = &test.policy_input.url_style {
                builder = builder.with_url_style(match style.as_str() {
                    "VIRTUAL_HOSTED_STYLE" => UrlStyle::VirtualHostedStyle,
                    "BUCKET_BOUND_HOSTNAME" => UrlStyle::BucketBoundHostname,
                    _ => UrlStyle::PathStyle,
                });
            }
            if let Some(hostname) = &test.policy_input.bucket_bound_hostname {
                builder =
                    builder.with_endpoint(format!("{}://{}", test.policy_input.scheme, hostname));
            }
            if let Some(conditions) = &test.policy_input.conditions {
                if let Some(starts_with) = &conditions.starts_with {
                    builder = builder.with_condition(PostPolicyV4Condition::starts_with(
                        starts_with[0].clone(),
                        starts_with[1].clone(),
                    ));
                }
                if let Some(range) = &conditions.content_length_range {
                    builder = builder.with_condition(PostPolicyV4Condition::content_length_range(
                        range[0], range[1],
                    ));
                }
            }
            if let Some(fields) = &test.policy_input.fields {
                builder = builder.with_fields(fields_from_fixture(fields));
            }

            let got = builder.sign_with(&signer).await?;
            let decoded_policy = String::from_utf8(BASE64_STANDARD.decode(&got.fields["policy"])?)?;
            let decoded_policy_json: serde_json::Value = serde_json::from_str(&decoded_policy)?;
            let expected_policy_json: serde_json::Value =
                serde_json::from_str(&test.policy_output.expected_decoded_policy)?;
            if got.url != test.policy_output.url
                || decoded_policy_json != expected_policy_json
                || !expected_fields_match(&got.fields, &test.policy_output.fields)
            {
                println!("failed post policy conformance test: {}", test.description);
                println!("got url: {}", got.url);
                println!("want url: {}", test.policy_output.url);
                println!(
                    "policy diff: {}",
                    pretty_assertions::StrComparison::new(
                        &decoded_policy,
                        &test.policy_output.expected_decoded_policy
                    )
                );
                failed.push(test.description);
            }
        }

        assert!(failed.is_empty(), "failed conformance tests: {failed:?}");
        Ok(())
    }

    fn fields_from_fixture(fields: &BTreeMap<String, String>) -> PolicyV4Fields {
        let mut policy_fields = PolicyV4Fields::new();
        for (key, value) in fields {
            match key.as_str() {
                "acl" => policy_fields = policy_fields.with_acl(value),
                "cache-control" => policy_fields = policy_fields.with_cache_control(value),
                "content-disposition" => {
                    policy_fields = policy_fields.with_content_disposition(value)
                }
                "content-encoding" => policy_fields = policy_fields.with_content_encoding(value),
                "content-type" => policy_fields = policy_fields.with_content_type(value),
                "success_action_redirect" => {
                    policy_fields = policy_fields.with_redirect_to_url_on_success(value)
                }
                "success_action_status" => {
                    policy_fields =
                        policy_fields.with_status_code_on_success(value.parse().unwrap())
                }
                key if key.starts_with("x-goog-meta") => {
                    policy_fields = policy_fields.with_metadata(key, value)
                }
                _ => {}
            }
        }
        policy_fields
    }

    fn expected_fields_match(
        got: &BTreeMap<String, String>,
        want: &BTreeMap<String, String>,
    ) -> bool {
        want.iter().all(|(key, value)| got.get(key) == Some(value))
            && got
                .keys()
                .all(|key| want.contains_key(key) || key == "x-goog-signature")
    }
}
