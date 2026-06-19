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

use crate::{error::SigningError, signed_url::UrlStyle};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::{DateTime, Utc};
use google_cloud_auth::signer::Signer;
use std::collections::BTreeMap;
use std::time::Duration;

/// Builder for constructing GCS V4 Signed Policy Documents (POST Object Forms).
#[derive(Debug, Clone)]
pub struct PostPolicyV4Builder {
    bucket: String,
    object: String,
    expiration: Duration,
    timestamp: Option<DateTime<Utc>>,
    url_style: UrlStyle,
    bucket_bound_hostname: Option<String>,
    starts_with_conditions: Vec<(String, String)>,
    content_length_range: Option<(u64, u64)>,
    fields: BTreeMap<String, String>,
    client_email: Option<String>,
    universe_domain: Option<String>,
    endpoint: Option<String>,
}

/// The result of signing a V4 POST Policy Document.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PostPolicyV4Result {
    /// The destination URL for the POST request.
    pub url: String,
    /// The form fields (hidden inputs) that must be included in the multipart POST request.
    pub fields: BTreeMap<String, String>,
}

/// Private internal structure for serializing the GCS policy JSON document.
#[derive(Debug, serde::Serialize)]
struct PostPolicyV4Document {
    conditions: Vec<serde_json::Value>,
    expiration: String,
}

impl PostPolicyV4Builder {
    /// Creates a new builder for the specified bucket and object.
    pub fn for_object<B, O>(bucket: B, object: O) -> Self
    where
        B: Into<String>,
        O: Into<String>,
    {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            expiration: Duration::from_secs(604800), // Default to max: 7 days
            timestamp: None,
            url_style: UrlStyle::PathStyle,
            bucket_bound_hostname: None,
            starts_with_conditions: Vec::new(),
            content_length_range: None,
            fields: BTreeMap::new(),
            client_email: None,
            universe_domain: None,
            endpoint: None,
        }
    }

    /// Sets the policy expiration duration. Maximum is 7 days (604,800 seconds).
    pub fn with_expiration(mut self, expiration: Duration) -> Self {
        self.expiration = expiration;
        self
    }

    /// Sets the URL formatting style.
    pub fn with_url_style(mut self, url_style: UrlStyle) -> Self {
        self.url_style = url_style;
        self
    }

    /// Sets a CNAME alias/bucket bound hostname.
    pub fn with_bucket_bound_hostname<S: Into<String>>(mut self, hostname: S) -> Self {
        self.bucket_bound_hostname = Some(hostname.into());
        self
    }

    /// Sets the authorizer client email. If not set, it falls back to the signer's email.
    pub fn with_client_email<S: Into<String>>(mut self, client_email: S) -> Self {
        self.client_email = Some(client_email.into());
        self
    }

    /// Sets the GCS universe domain (defaults to `googleapis.com`).
    pub fn with_universe_domain<S: Into<String>>(mut self, universe_domain: S) -> Self {
        self.universe_domain = Some(universe_domain.into());
        self
    }

    /// Sets a custom endpoint.
    pub fn with_endpoint<S: Into<String>>(mut self, endpoint: S) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Adds a form field/exact condition match (e.g. "acl" = "public-read").
    pub fn with_field<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    /// Adds a starts-with condition constraint (e.g. "$key", "uploads/").
    pub fn with_starts_with<F: Into<String>, P: Into<String>>(
        mut self,
        field: F,
        prefix: P,
    ) -> Self {
        let mut f: String = field.into();
        if !f.starts_with('$') {
            f.insert(0, '$');
        }
        self.starts_with_conditions.push((f, prefix.into()));
        self
    }

    /// Adds a content-length-range constraint (minimum and maximum file size in bytes).
    pub fn with_content_length_range(mut self, min: u64, max: u64) -> Self {
        self.content_length_range = Some((min, max));
        self
    }

    fn bucket_name(&self) -> &str {
        self.bucket
            .strip_prefix("projects/_/buckets/")
            .unwrap_or(&self.bucket)
    }

    fn resolve_endpoint(&self) -> String {
        match self.endpoint.as_ref() {
            Some(e) if e.starts_with("http://") => e.clone(),
            Some(e) if e.starts_with("https://") => e.clone(),
            Some(e) => format!("https://{}", e),
            None => {
                let universe_domain = self.universe_domain.as_deref().unwrap_or("googleapis.com");
                format!("https://storage.{universe_domain}")
            }
        }
    }

    fn resolve_url(&self) -> Result<String, SigningError> {
        let bucket_name = self.bucket_name();

        let endpoint_url = self.resolve_endpoint();
        let url = url::Url::parse(&endpoint_url)
            .map_err(|err| SigningError::invalid_parameter("endpoint", err))?;

        let scheme = url.scheme();
        let mut host = url
            .host_str()
            .ok_or_else(|| SigningError::invalid_parameter("endpoint", "Missing host"))?
            .to_string();

        if let Some(port) = url.port() {
            host.push_str(&format!(":{port}"));
        }

        let url = match self.url_style {
            UrlStyle::PathStyle => {
                format!("{}://{}/{}/", scheme, host, bucket_name)
            }
            UrlStyle::VirtualHostedStyle => {
                format!("{}://{}.{}/", scheme, bucket_name, host)
            }
            UrlStyle::BucketBoundHostname => {
                let hostname = self.bucket_bound_hostname.as_deref().ok_or_else(|| {
                    SigningError::invalid_parameter(
                        "url_style",
                        "bucket_bound_hostname must be set for BucketBoundHostname style",
                    )
                })?;
                let clean_hostname = hostname
                    .strip_prefix("http://")
                    .unwrap_or(hostname)
                    .strip_prefix("https://")
                    .unwrap_or(hostname);
                format!("{}://{}/", scheme, clean_hostname)
            }
        };

        Ok(url)
    }

    /// Sign the policy document.
    pub async fn sign_with(mut self, signer: &Signer) -> Result<PostPolicyV4Result, SigningError> {
        if self.expiration > Duration::from_secs(604800) {
            return Err(SigningError::invalid_parameter(
                "expiration",
                "Expiration cannot exceed 7 days (604,800 seconds)",
            ));
        }

        if let Some((min, max)) = self.content_length_range
            && min > max
        {
            return Err(SigningError::invalid_parameter(
                "content_length_range",
                "min must be less than or equal to max",
            ));
        }

        let now = self.timestamp.unwrap_or_else(Utc::now);
        let request_timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let datestamp = now.format("%Y%m%d");
        let credential_scope = format!("{datestamp}/auto/storage/goog4_request");

        let client_email = if let Some(email) = self.client_email.take() {
            email
        } else {
            signer.client_email().await.map_err(SigningError::signing)?
        };
        let credential = format!("{client_email}/{credential_scope}");

        let mut conditions = Vec::new();

        // 1. Add custom headers/metadata (except x-ignore- fields and system fields)
        let system_keys = [
            "bucket",
            "key",
            "x-goog-date",
            "x-goog-credential",
            "x-goog-algorithm",
            "x-goog-signature",
            "policy",
        ];
        for (key, value) in &self.fields {
            if !key.starts_with("x-ignore-") && !system_keys.contains(&key.as_str()) {
                conditions.push(serde_json::json!({ key: value }));
            }
        }

        // 2. Add starts-with conditions
        for (field, prefix) in &self.starts_with_conditions {
            conditions.push(serde_json::json!(["starts-with", field, prefix]));
        }

        // 3. Add content-length-range condition
        if let Some((min, max)) = self.content_length_range {
            conditions.push(serde_json::json!(["content-length-range", min, max]));
        }

        // 4. Add required conditions
        conditions.push(serde_json::json!({ "bucket": self.bucket_name() }));
        conditions.push(serde_json::json!({ "key": self.object }));
        conditions.push(serde_json::json!({ "x-goog-date": request_timestamp }));
        conditions.push(serde_json::json!({ "x-goog-credential": credential }));
        conditions.push(serde_json::json!({ "x-goog-algorithm": "GOOG4-RSA-SHA256" }));

        // Expiration
        let expiration_time = now
            + chrono::Duration::from_std(self.expiration)
                .map_err(|e| SigningError::signing(format!("Invalid expiration duration: {e}")))?;
        let expiration_str = expiration_time.format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let doc = PostPolicyV4Document {
            conditions,
            expiration: expiration_str,
        };

        // Serialize to minified JSON string (retaining "conditions" first then "expiration")
        let serialized = serde_json::to_string(&doc)
            .map_err(|e| SigningError::signing(format!("JSON serialization failed: {e}")))?;

        let escaped_json = escape_non_ascii(&serialized);

        // Base64 encode
        let encoded_policy = BASE64_STANDARD.encode(escaped_json.as_bytes());

        // Sign the base64 string
        let signature_bytes = signer
            .sign(encoded_policy.as_bytes())
            .await
            .map_err(SigningError::signing)?;

        let signature_hex = hex::encode(signature_bytes);

        // Build target URL
        let url = self.resolve_url()?;

        // Build output form fields
        let mut fields = BTreeMap::new();

        // Add user-supplied fields (including custom metadata or x-ignore- fields)
        for (key, value) in &self.fields {
            fields.insert(key.clone(), value.clone());
        }

        // Add required system fields
        fields.insert("key".to_string(), self.object.clone());
        fields.insert(
            "x-goog-algorithm".to_string(),
            "GOOG4-RSA-SHA256".to_string(),
        );
        fields.insert("x-goog-credential".to_string(), credential);
        fields.insert("x-goog-date".to_string(), request_timestamp);
        fields.insert("x-goog-signature".to_string(), signature_hex);
        fields.insert("policy".to_string(), encoded_policy);

        Ok(PostPolicyV4Result { url, fields })
    }
}

fn escape_non_ascii(s: &str) -> String {
    use std::fmt::Write;
    let mut escaped = String::with_capacity(s.len());
    let mut buf = [0; 2];
    for c in s.chars() {
        if c.is_ascii() {
            escaped.push(c);
        } else {
            for &mut unit in c.encode_utf16(&mut buf) {
                let _ = write!(escaped, "\\u{:04x}", unit);
            }
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signed_url::UrlStyle;
    use google_cloud_auth::credentials::service_account::Builder as ServiceAccount;
    use serde::Deserialize;
    use std::collections::HashMap;

    impl PostPolicyV4Builder {
        /// Sets the creation timestamp for the policy signature. Only used in tests.
        pub(crate) fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
            self.timestamp = Some(timestamp);
            self
        }
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PostPolicyV4TestSuite {
        post_policy_v4_tests: Vec<PostPolicyV4Test>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PostPolicyV4Test {
        description: String,
        policy_input: PostPolicyV4TestInput,
        policy_output: PostPolicyV4TestOutput,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PostPolicyV4TestInput {
        scheme: String,
        bucket: String,
        object: String,
        expiration: u64,
        timestamp: String,
        url_style: Option<String>,
        bucket_bound_hostname: Option<String>,
        client_endpoint: Option<String>,
        universe_domain: Option<String>,
        fields: Option<HashMap<String, String>>,
        conditions: Option<PostPolicyV4TestConditions>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PostPolicyV4TestConditions {
        starts_with: Option<Vec<String>>,
        content_length_range: Option<Vec<u64>>,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct PostPolicyV4TestOutput {
        url: String,
        fields: HashMap<String, String>,
        _expected_decoded_policy: String,
    }

    #[tokio::test]
    async fn post_policy_v4_conformance() -> anyhow::Result<()> {
        let service_account_key = serde_json::from_slice(include_bytes!(
            "conformance/test_service_account.not-a-test.json"
        ))?;

        let signer = ServiceAccount::new(service_account_key)
            .build_signer()
            .expect("failed to build signer");

        let suite: PostPolicyV4TestSuite =
            serde_json::from_slice(include_bytes!("conformance/v4_signatures.json"))?;

        let mut failed_tests = Vec::new();
        let mut passed_tests = Vec::new();
        let total_tests = suite.post_policy_v4_tests.len();

        for test in suite.post_policy_v4_tests {
            let timestamp = DateTime::parse_from_rfc3339(&test.policy_input.timestamp)
                .expect("invalid timestamp");
            let scheme = test.policy_input.scheme.clone();

            let url_style = match test.policy_input.url_style.as_deref() {
                Some("VIRTUAL_HOSTED_STYLE") => UrlStyle::VirtualHostedStyle,
                Some("BUCKET_BOUND_HOSTNAME") => UrlStyle::BucketBoundHostname,
                _ => UrlStyle::PathStyle,
            };

            let mut builder = PostPolicyV4Builder::for_object(
                format!("projects/_/buckets/{}", test.policy_input.bucket),
                test.policy_input.object.clone(),
            )
            .with_url_style(url_style)
            .with_timestamp(timestamp.into())
            .with_expiration(Duration::from_secs(test.policy_input.expiration));

            if let Some(hostname) = &test.policy_input.bucket_bound_hostname {
                builder = builder.with_bucket_bound_hostname(hostname.clone());
                builder = builder.with_endpoint(format!("{}://{}", scheme, hostname));
            }

            if let Some(endpoint) = &test.policy_input.client_endpoint {
                builder = builder.with_endpoint(endpoint.clone());
            }

            if let Some(domain) = &test.policy_input.universe_domain {
                builder = builder.with_universe_domain(domain.clone());
            }

            if let Some(fields) = &test.policy_input.fields {
                for (k, v) in fields {
                    builder = builder.with_field(k.clone(), v.clone());
                }
            }

            if let Some(conds) = &test.policy_input.conditions {
                if let Some(starts_with) = &conds.starts_with
                    && starts_with.len() == 2
                {
                    builder =
                        builder.with_starts_with(starts_with[0].clone(), starts_with[1].clone());
                }
                if let Some(range) = &conds.content_length_range
                    && range.len() == 2
                {
                    builder = builder.with_content_length_range(range[0], range[1]);
                }
            }

            let result = builder.sign_with(&signer).await;
            let result = match result {
                Ok(res) => res,
                Err(e) => {
                    println!("❌ Failed test: {}", test.description);
                    println!("Error: {}", e);
                    failed_tests.push(test.description);
                    continue;
                }
            };

            let expected_fields = &test.policy_output.fields;
            let mut mismatch = false;

            // Verify URL
            if result.url != test.policy_output.url {
                println!("❌ Failed test: {}", test.description);
                let diff =
                    pretty_assertions::StrComparison::new(&result.url, &test.policy_output.url);
                println!("URL diff: {}", diff);
                mismatch = true;
            }

            // Verify Fields
            for (k, v) in expected_fields {
                let actual_val = result.fields.get(k);
                match actual_val {
                    Some(actual) if actual == v => {}
                    Some(actual) => {
                        println!("❌ Failed test: {} (field: {})", test.description, k);
                        let diff = pretty_assertions::StrComparison::new(actual, v);
                        println!("Field '{}' diff: {}", k, diff);
                        mismatch = true;
                    }
                    None => {
                        println!(
                            "❌ Failed test: {} (missing field: {})",
                            test.description, k
                        );
                        mismatch = true;
                    }
                }
            }

            // Verify No Extra Fields in actual
            for k in result.fields.keys() {
                if !expected_fields.contains_key(k) {
                    println!(
                        "❌ Failed test: {} (extra actual field: {})",
                        test.description, k
                    );
                    mismatch = true;
                }
            }

            if mismatch {
                failed_tests.push(test.description);
            } else {
                passed_tests.push(test.description);
            }
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
            anyhow::bail!("Some conformance tests failed")
        }
        Ok(())
    }

    #[tokio::test]
    async fn post_policy_v4_edge_cases() {
        let builder = PostPolicyV4Builder::for_object("bucket", "object")
            .with_client_email("test@example.com")
            .with_universe_domain("custom.domain")
            .with_endpoint("https://custom.endpoint:8080")
            .with_starts_with("no_dollar_sign", "prefix");

        // Test if !f.starts_with('$')
        assert_eq!(
            builder.starts_with_conditions[0],
            ("$no_dollar_sign".to_string(), "prefix".to_string()),
        );

        // Test with_client_email, with_universe_domain, with_endpoint properties
        assert_eq!(builder.client_email.as_deref(), Some("test@example.com"));
        assert_eq!(builder.universe_domain.as_deref(), Some("custom.domain"));
        assert_eq!(
            builder.endpoint.as_deref(),
            Some("https://custom.endpoint:8080")
        );

        // Test the mapping error in resolve_url
        let bad_endpoint_builder =
            PostPolicyV4Builder::for_object("bucket", "object").with_endpoint("");
        assert!(bad_endpoint_builder.resolve_url().is_err());

        // Test SigningError::invalid_parameter of url_style (BucketBoundHostname without hostname)
        let bad_url_style_builder = PostPolicyV4Builder::for_object("bucket", "object")
            .with_url_style(UrlStyle::BucketBoundHostname);
        assert!(bad_url_style_builder.resolve_url().is_err());

        let service_account_key = serde_json::from_slice(include_bytes!(
            "conformance/test_service_account.not-a-test.json",
        ))
        .unwrap();
        let signer = ServiceAccount::new(service_account_key)
            .build_signer()
            .expect("failed to build signer");

        // Test SigningError::invalid_parameter of expiration (> 7 days)
        let bad_expiration_builder = PostPolicyV4Builder::for_object("bucket", "object")
            .with_expiration(Duration::from_secs(604801)); // > 7 days
        assert!(bad_expiration_builder.sign_with(&signer).await.is_err());

        // Test SigningError::invalid_parameter of content_length_range (min > max)
        let bad_content_length_builder =
            PostPolicyV4Builder::for_object("bucket", "object").with_content_length_range(10, 5); // min > max
        assert!(bad_content_length_builder.sign_with(&signer).await.is_err());
    }

    #[tokio::test]
    async fn post_policy_v4_custom_fields() {
        let assert_not_contains = |conditions: &[serde_json::Value], item: serde_json::Value| {
            assert!(
                !conditions.contains(&item),
                "Expected conditions to NOT contain: {:?}",
                item
            );
        };

        let service_account_key = serde_json::from_slice(include_bytes!(
            "conformance/test_service_account.not-a-test.json",
        ))
        .unwrap();
        let signer = ServiceAccount::new(service_account_key)
            .build_signer()
            .expect("failed to build signer");

        // Test custom fields:
        // 1. Valid custom fields (e.g., x-goog-meta-*, acl) should be preserved.
        // 2. Conflicting system keys should be silently overwritten in output, but both sent to backend.
        let builder = PostPolicyV4Builder::for_object("bucket", "object")
            .with_field("x-goog-meta-custom", "custom_value")
            .with_field("acl", "public-read")
            .with_field("key", "malicious_key")
            .with_field("x-goog-algorithm", "malicious_algo")
            .with_field("x-goog-credential", "malicious_credential")
            .with_field("x-goog-date", "malicious_date")
            .with_field("x-goog-signature", "malicious_signature")
            .with_field("policy", "malicious_policy")
            .with_field("x-ignore-test-field", "ignored_value");

        let result = builder.sign_with(&signer).await.unwrap();

        // 1. Output Fields Check: Valid custom fields should be preserved
        assert_eq!(
            result.fields.get("x-goog-meta-custom").unwrap(),
            "custom_value"
        );
        assert_eq!(result.fields.get("acl").unwrap(), "public-read");
        assert_eq!(
            result.fields.get("x-ignore-test-field").unwrap(),
            "ignored_value"
        );

        // 2. Output Fields Check: System keys silently overwrote the malicious ones
        assert_eq!(result.fields.get("key").unwrap(), "object");
        assert_eq!(
            result.fields.get("x-goog-algorithm").unwrap(),
            "GOOG4-RSA-SHA256"
        );
        assert_ne!(
            result.fields.get("x-goog-credential").unwrap(),
            "malicious_credential"
        );
        assert_ne!(result.fields.get("x-goog-date").unwrap(), "malicious_date");
        assert_ne!(
            result.fields.get("x-goog-signature").unwrap(),
            "malicious_signature"
        );
        assert_ne!(result.fields.get("policy").unwrap(), "malicious_policy");

        // 3. Conditions Input: Verify the conditions array inside the generated policy does NOT
        // contain the user's conflicting keys, only the system keys.
        let decoded_policy = BASE64_STANDARD
            .decode(result.fields.get("policy").unwrap())
            .unwrap();
        let policy_json: serde_json::Value = serde_json::from_slice(&decoded_policy).unwrap();
        let conditions = policy_json.get("conditions").unwrap().as_array().unwrap();

        // Check valid custom fields
        assert!(conditions.contains(&serde_json::json!({"x-goog-meta-custom": "custom_value"})));
        assert!(conditions.contains(&serde_json::json!({"acl": "public-read"})));
        // Check that x-ignore- field is NOT in conditions of the signed policy
        assert_not_contains(
            conditions,
            serde_json::json!({"x-ignore-test-field": "ignored_value"}),
        );

        // Check conflicting user keys vs system keys: system keys must override user keys in conditions
        assert_not_contains(conditions, serde_json::json!({"key": "malicious_key"}));
        assert_not_contains(
            conditions,
            serde_json::json!({"x-goog-algorithm": "malicious_algo"}),
        );
        assert_not_contains(
            conditions,
            serde_json::json!({"x-goog-credential": "malicious_credential"}),
        );
        assert_not_contains(
            conditions,
            serde_json::json!({"x-goog-date": "malicious_date"}),
        );
        assert_not_contains(
            conditions,
            serde_json::json!({"x-goog-signature": "malicious_signature"}),
        );
        assert_not_contains(
            conditions,
            serde_json::json!({"policy": "malicious_policy"}),
        );

        assert!(conditions.contains(&serde_json::json!({"key": "object"})));
    }
}
