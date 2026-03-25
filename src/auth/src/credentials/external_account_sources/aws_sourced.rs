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

use crate::{
    Result,
    credentials::subject_token::{
        Builder as SubjectTokenBuilder, SubjectToken, SubjectTokenProvider,
    },
    io::{HttpRequest, SharedEnvProvider, SharedHttpClientProvider},
};
use chrono::Utc;
use google_cloud_gax::error::CredentialsError;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const AWS_REGION: &str = "AWS_REGION";
const AWS_DEFAULT_REGION: &str = "AWS_DEFAULT_REGION";
const AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const AWS_SESSION_TOKEN: &str = "AWS_SESSION_TOKEN";

const IMDSV2_TOKEN_TTL_HEADER: &str = "x-aws-ec2-metadata-token-ttl-seconds";
const IMDSV2_TOKEN_HEADER: &str = "x-aws-ec2-metadata-token";
const IMDSV2_DEFAULT_TOKEN_TTL_SECONDS: &str = "21600";

const X_AMZ_DATE: &str = "x-amz-date";
const X_AMZ_SECURITY_TOKEN: &str = "x-amz-security-token";
const X_GOOG_CLOUD_TARGET_RESOURCE: &str = "x-goog-cloud-target-resource";

const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
const AWS4_REQUEST: &str = "aws4_request";
const AWS_STS_SERVICE: &str = "sts";

const DEFAULT_REGIONAL_CRED_VERIFICATION_URL: &str =
    "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15";

const MSG: &str = "failed to fetch AWS credentials for subject token";

/// Credential source for AWS workloads using Workload Identity Federation.
///
/// This provider fetches a subject token by making a signed AWS STS `GetCallerIdentity`
/// request, following the specifications in [AIP-4117].
///
/// [AIP-4117]: https://google.aip.dev/auth/4117
#[derive(Debug, Clone)]
pub(crate) struct AwsSourcedCredentials {
    /// The URL to fetch the AWS region from IMDS.
    pub region_url: Option<String>,
    /// The URL to fetch the AWS IAM role credentials from IMDS.
    pub role_url: Option<String>,
    /// The regional AWS STS endpoint used for verification.
    pub regional_cred_verification_url: Option<String>,
    /// The URL to fetch an IMDSv2 session token.
    pub imdsv2_session_token_url: Option<String>,
    /// The audience for the x-goog-cloud-target-resource header.
    pub audience: String,
    /// The environment variable provider.
    pub env: SharedEnvProvider,
    /// The HTTP client provider.
    pub http: SharedHttpClientProvider,
}

impl AwsSourcedCredentials {
    pub(crate) fn new(
        region_url: Option<String>,
        role_url: Option<String>,
        regional_cred_verification_url: Option<String>,
        imdsv2_session_token_url: Option<String>,
        audience: String,
        env: SharedEnvProvider,
        http: SharedHttpClientProvider,
    ) -> Self {
        Self {
            region_url,
            role_url,
            regional_cred_verification_url,
            imdsv2_session_token_url,
            audience,
            env,
            http,
        }
    }
}

#[derive(Debug, Deserialize)]
struct AwsSecurityCredentials {
    #[serde(rename = "AccessKeyId")]
    access_key_id: String,
    #[serde(rename = "SecretAccessKey")]
    secret_access_key: String,
    #[serde(rename = "Token")]
    token: Option<String>,
}

#[derive(Serialize)]
struct AwsStsRequest {
    url: String,
    method: String,
    headers: Vec<AwsHeader>,
    body: String,
}

#[derive(Serialize)]
struct AwsHeader {
    key: String,
    value: String,
}

impl SubjectTokenProvider for AwsSourcedCredentials {
    type Error = CredentialsError;

    async fn subject_token(&self) -> Result<SubjectToken> {
        let imdsv2_token = self.resolve_imdsv2_token().await?;

        let region = self.resolve_region(imdsv2_token.as_deref()).await?;
        let creds = self.resolve_credentials(imdsv2_token.as_deref()).await?;

        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let url = resolve_sts_url(self.regional_cred_verification_url.as_deref(), &region)?;
        let host = url.host_str().unwrap(); // unwrap is safe because resolve_sts_url checks for a host
        let sts_url = url.to_string();

        let method = "POST";
        let body = "";
        let canonical_uri = "/";

        let query_params: BTreeMap<_, _> = url.query_pairs().collect();
        let canonical_query = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(query_params)
            .finish();

        let mut headers = BTreeMap::new();
        headers.insert("host".to_string(), host.to_string());
        headers.insert(X_AMZ_DATE.to_string(), amz_date.clone());
        if let Some(token) = &creds.token {
            headers.insert(X_AMZ_SECURITY_TOKEN.to_string(), token.clone());
        }
        headers.insert(
            X_GOOG_CLOUD_TARGET_RESOURCE.to_string(),
            self.audience.clone(),
        );

        let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
        let canonical_headers = headers.iter().fold(String::new(), |mut acc, (k, v)| {
            acc.push_str(&format!("{}:{}\n", k, v.trim()));
            acc
        });

        let payload_hash = hash_sha256(body);

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method, canonical_uri, canonical_query, canonical_headers, signed_headers, payload_hash
        );

        let credential_scope = format!(
            "{}/{}/{}/{}",
            date_stamp, region, AWS_STS_SERVICE, AWS4_REQUEST
        );
        let string_to_sign = format!(
            "{}\n{}\n{}\n{}",
            AWS4_HMAC_SHA256,
            amz_date,
            credential_scope,
            hash_sha256(&canonical_request)
        );

        let signing_key = get_signing_key(
            &creds.secret_access_key,
            &date_stamp,
            &region,
            AWS_STS_SERVICE,
        )?;
        let signature = hex::encode(hmac_sha256(&signing_key, &string_to_sign)?);

        let authorization_header = format!(
            "{} Credential={}/{}, SignedHeaders={}, Signature={}",
            AWS4_HMAC_SHA256, creds.access_key_id, credential_scope, signed_headers, signature
        );

        let final_headers: Vec<_> = headers
            .into_iter()
            .map(|(key, value)| AwsHeader { key, value })
            .chain(std::iter::once(AwsHeader {
                key: "Authorization".to_string(),
                value: authorization_header,
            }))
            .collect();

        let aws_sts_request = AwsStsRequest {
            url: sts_url,
            method: method.to_string(),
            headers: final_headers,
            body: body.to_string(),
        };

        let json_token = serde_json::to_string(&aws_sts_request)
            .map_err(|e| CredentialsError::from_source(false, e))?;

        let subject_token: String =
            url::form_urlencoded::byte_serialize(json_token.as_bytes()).collect();

        Ok(SubjectTokenBuilder::new(subject_token).build())
    }
}

fn resolve_sts_url(template: Option<&str>, region: &str) -> Result<url::Url> {
    let sts_url = template
        .unwrap_or(DEFAULT_REGIONAL_CRED_VERIFICATION_URL)
        .replace("{region}", region);
    let sts_url = if sts_url.starts_with("http") {
        sts_url
    } else {
        format!("https://{sts_url}")
    };
    let url = url::Url::parse(&sts_url)
        .map_err(|e| CredentialsError::from_msg(false, format!("invalid AWS STS URL: {e}")))?;

    if url.host_str().is_none() {
        return Err(CredentialsError::from_msg(
            false,
            "invalid AWS STS URL: missing host",
        ));
    }
    Ok(url)
}

fn hash_sha256(data: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn hmac_sha256(key: &[u8], data: &str) -> Result<Vec<u8>> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|e| {
        CredentialsError::from_msg(
            false,
            format!("failed to initialize HMAC from secret key: {e}"),
        )
    })?;
    mac.update(data.as_bytes());
    Ok(mac.finalize().into_bytes().to_vec())
}

fn get_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Result<Vec<u8>> {
    let secret_key = format!("AWS4{}", secret);
    let k_date = hmac_sha256(secret_key.as_bytes(), date)?;
    let k_region = hmac_sha256(&k_date, region)?;
    let k_service = hmac_sha256(&k_region, service)?;
    hmac_sha256(&k_service, AWS4_REQUEST)
}

// Zone name "us-east-1d" -> Region "us-east-1"
fn parse_region_from_zone(zone: &str) -> Option<&str> {
    let zone = zone.trim();
    if zone.is_empty() {
        return None;
    }
    if let Some(last_char) = zone.chars().last() {
        if last_char.is_ascii_alphabetic() && zone.len() > 1 {
            let potential_region = &zone[..zone.len() - 1];
            if potential_region
                .chars()
                .last()
                .is_some_and(|c| c.is_ascii_digit())
            {
                return Some(potential_region);
            }
        }
    }
    Some(zone)
}

impl AwsSourcedCredentials {
    async fn resolve_imdsv2_token(&self) -> Result<Option<String>> {
        if let Some(url) = &self.imdsv2_session_token_url {
            let request = HttpRequest::put(url)
                .header(IMDSV2_TOKEN_TTL_HEADER, IMDSV2_DEFAULT_TOKEN_TTL_SECONDS);

            let response = self
                .http
                .execute(request)
                .await
                .map_err(|e| crate::errors::from_http_error(e, MSG))?;

            if !response.is_success() {
                return Err(crate::errors::from_http_response(
                    &response,
                    "failed to resolve IMDSv2 token",
                ));
            }

            let token = response
                .text()
                .map_err(|e| CredentialsError::from_source(false, e))?;

            return Ok(Some(token));
        }
        Ok(None)
    }

    async fn get_with_imdsv2_token(
        &self,
        url: &str,
        imdsv2_token: Option<&str>,
        error_msg: &str,
    ) -> Result<Vec<u8>> {
        let mut request = HttpRequest::get(url);
        if let Some(token) = imdsv2_token {
            request = request.header(IMDSV2_TOKEN_HEADER, token);
        }

        let response = self
            .http
            .execute(request)
            .await
            .map_err(|e| crate::errors::from_http_error(e, MSG))?;

        if !response.is_success() {
            return Err(crate::errors::from_http_response(&response, error_msg));
        }
        Ok(response.body)
    }

    async fn resolve_region(&self, imdsv2_token: Option<&str>) -> Result<String> {
        if let Some(region) = self.env.var(AWS_REGION) {
            return Ok(region);
        }
        if let Some(region) = self.env.var(AWS_DEFAULT_REGION) {
            return Ok(region);
        }

        if let Some(url) = &self.region_url {
            let body = self
                .get_with_imdsv2_token(url, imdsv2_token, "could not resolve AWS region")
                .await?;

            let zone =
                String::from_utf8(body).map_err(|e| CredentialsError::from_source(false, e))?;
            if let Some(region) = parse_region_from_zone(&zone) {
                return Ok(region.to_string());
            }
        }
        Err(CredentialsError::from_msg(
            false,
            "could not resolve AWS region",
        ))
    }

    async fn resolve_role_name(&self, imdsv2_token: Option<&str>) -> Result<String> {
        if let Some(url) = &self.role_url {
            let body = self
                .get_with_imdsv2_token(url, imdsv2_token, "could not resolve AWS role name")
                .await?;

            let role_name =
                String::from_utf8(body).map_err(|e| CredentialsError::from_source(false, e))?;

            return Ok(role_name.trim().to_string());
        }
        Err(CredentialsError::from_msg(
            false,
            "unable to determine the AWS metadata server security credentials endpoint",
        ))
    }

    async fn resolve_role_credentials(
        &self,
        role_name: &str,
        imdsv2_token: Option<&str>,
    ) -> Result<AwsSecurityCredentials> {
        if let Some(url) = &self.role_url {
            let role_url = format!("{}/{}", url.trim_end_matches('/'), role_name.trim());
            let body = self
                .get_with_imdsv2_token(&role_url, imdsv2_token, "could not resolve AWS credentials")
                .await?;

            let creds: AwsSecurityCredentials = serde_json::from_slice(&body)
                .map_err(|e| CredentialsError::from_source(e.is_io(), e))?;
            return Ok(creds);
        }
        Err(CredentialsError::from_msg(
            false,
            "unable to determine the AWS metadata server security credentials endpoint",
        ))
    }

    async fn resolve_credentials(
        &self,
        imdsv2_token: Option<&str>,
    ) -> Result<AwsSecurityCredentials> {
        if let (Some(ak), Some(sk)) = (
            self.env.var(AWS_ACCESS_KEY_ID),
            self.env.var(AWS_SECRET_ACCESS_KEY),
        ) {
            return Ok(AwsSecurityCredentials {
                access_key_id: ak,
                secret_access_key: sk,
                token: self.env.var(AWS_SESSION_TOKEN),
            });
        }

        let role_name = self.resolve_role_name(imdsv2_token).await?;
        let role_credentials = self
            .resolve_role_credentials(&role_name, imdsv2_token)
            .await?;

        Ok(role_credentials)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use scoped_env::ScopedEnv;
    use serde_json::json;
    use serial_test::{parallel, serial};
    use test_case::test_case;

    type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

    #[test_case("us-east-1a", Some("us-east-1"); "zone_to_region")]
    #[test_case("us-east-1", Some("us-east-1"); "already_region")]
    #[test_case("us-gov-west-1a", Some("us-gov-west-1"); "gov_zone_to_region")]
    #[test_case("us-gov-west-1", Some("us-gov-west-1"); "gov_already_region")]
    #[test_case("  us-east-1a  ", Some("us-east-1"); "trimmed_zone")]
    #[test_case("", None; "empty")]
    #[test_case("   ", None; "whitespace")]
    #[test_case("a", Some("a"); "short_zone")]
    fn test_parse_region_from_zone(zone: &str, expected: Option<&str>) {
        assert_eq!(parse_region_from_zone(zone), expected);
    }

    #[test_case(None, "us-east-1", "https://sts.us-east-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15"; "default_template")]
    #[test_case(Some("http://custom.sts.url/{region}"), "us-west-2", "http://custom.sts.url/us-west-2"; "custom_template_with_region")]
    #[test_case(Some("sts.amazonaws.com"), "us-east-1", "https://sts.amazonaws.com/"; "no_scheme")]
    #[test_case(Some("https://sts.amazonaws.com"), "us-east-1", "https://sts.amazonaws.com/"; "with_scheme")]
    fn test_resolve_sts_url(template: Option<&str>, region: &str, expected: &str) {
        let url = resolve_sts_url(template, region).expect("should resolve");
        assert_eq!(url.as_str(), expected);
    }

    #[test]
    fn test_resolve_sts_url_invalid() {
        let result = resolve_sts_url(Some("not a url"), "region");
        assert!(result.is_err());
    }

    #[tokio::test]
    #[serial]
    async fn test_resolve_region_env() -> TestResult {
        let _e = ScopedEnv::set(AWS_REGION, "us-west-2");
        let _e2 = ScopedEnv::remove(AWS_DEFAULT_REGION);
        let creds = AwsSourcedCredentials::new(
            None,
            None,
            Some("sts.{region}.amazonaws.com".into()),
            None,
            "aud".into(),
            SharedEnvProvider::default(),
            SharedHttpClientProvider::default(),
        );
        assert_eq!(creds.resolve_region(None).await?, "us-west-2", "{creds:?}");
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_resolve_region_imds() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/zone"))
                .respond_with(status_code(200).body("us-east-1d")),
        );

        let creds = AwsSourcedCredentials::new(
            Some(server.url("/zone").to_string()),
            None,
            Some("sts.{region}.amazonaws.com".into()),
            None,
            "aud".into(),
            SharedEnvProvider::default(),
            SharedHttpClientProvider::default(),
        );
        assert_eq!(creds.resolve_region(None).await?, "us-east-1", "{creds:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_resolve_credentials_env() -> TestResult {
        let _e1 = ScopedEnv::set(AWS_ACCESS_KEY_ID, "ACCESS_KEY_ID");
        let _e2 = ScopedEnv::set(AWS_SECRET_ACCESS_KEY, "SECRET");
        let _e3 = ScopedEnv::remove(AWS_SESSION_TOKEN);
        let creds = AwsSourcedCredentials::new(
            None,
            None,
            Some("sts.{region}.amazonaws.com".into()),
            None,
            "aud".into(),
            SharedEnvProvider::default(),
            SharedHttpClientProvider::default(),
        );
        let resolved = creds.resolve_credentials(None).await?;
        assert_eq!(resolved.access_key_id, "ACCESS_KEY_ID", "{resolved:?}");
        assert_eq!(resolved.secret_access_key, "SECRET", "{resolved:?}");
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_resolve_credentials_imds() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(request::method_path("GET", "/role"))
                .respond_with(status_code(200).body("my-role")),
        );
        server.expect(
            Expectation::matching(request::method_path("GET", "/role/my-role")).respond_with(
                status_code(200).body(
                    json!({
                        "AccessKeyId": "ACCESS_KEY_ID_IMDS",
                        "SecretAccessKey": "SECRET_IMDS",
                        "Token": "TOKEN_IMDS"
                    })
                    .to_string(),
                ),
            ),
        );

        let creds = AwsSourcedCredentials::new(
            None,
            Some(server.url("/role").to_string()),
            Some("sts.{region}.amazonaws.com".into()),
            None,
            "aud".into(),
            SharedEnvProvider::default(),
            SharedHttpClientProvider::default(),
        );
        let resolved = creds.resolve_credentials(None).await?;
        assert_eq!(resolved.access_key_id, "ACCESS_KEY_ID_IMDS", "{resolved:?}");
        assert_eq!(resolved.secret_access_key, "SECRET_IMDS", "{resolved:?}");
        assert_eq!(
            resolved.token,
            Some("TOKEN_IMDS".to_string()),
            "{resolved:?}"
        );
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_resolve_imdsv2_token() -> TestResult {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method("PUT"),
                request::path("/token"),
                request::headers(contains((
                    IMDSV2_TOKEN_TTL_HEADER,
                    IMDSV2_DEFAULT_TOKEN_TTL_SECONDS
                )))
            ])
            .respond_with(status_code(200).body("test-token")),
        );

        let creds = AwsSourcedCredentials::new(
            None,
            None,
            Some("sts.{region}.amazonaws.com".into()),
            Some(server.url("/token").to_string()),
            "aud".into(),
            SharedEnvProvider::default(),
            SharedHttpClientProvider::default(),
        );
        let token = creds.resolve_imdsv2_token().await?;
        assert_eq!(token, Some("test-token".to_string()), "{token:?}");
        Ok(())
    }

    #[tokio::test]
    #[parallel]
    async fn test_subject_token_imdsv2_success() -> TestResult {
        let server = Server::run();
        // IMDSv2 Token
        server.expect(
            Expectation::matching(all_of![request::method("PUT"), request::path("/token")])
                .respond_with(status_code(200).body("test-token")),
        );
        // Region
        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/zone"),
                request::headers(contains((IMDSV2_TOKEN_HEADER, "test-token")))
            ])
            .respond_with(status_code(200).body("us-east-1d")),
        );
        // Role Name
        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/role"),
                request::headers(contains((IMDSV2_TOKEN_HEADER, "test-token")))
            ])
            .respond_with(status_code(200).body("my-role")),
        );
        // Role Credentials
        server.expect(
            Expectation::matching(all_of![
                request::method("GET"),
                request::path("/role/my-role"),
                request::headers(contains((IMDSV2_TOKEN_HEADER, "test-token")))
            ])
            .respond_with(
                status_code(200)
                    .insert_header("Content-Type", "application/json")
                    .body(
                        json!({
                            "AccessKeyId": "ACCESS_KEY_ID_IMDS",
                            "SecretAccessKey": "SECRET_IMDS",
                            "Token": "TOKEN_IMDS"
                        })
                        .to_string(),
                    ),
            ),
        );

        let creds = AwsSourcedCredentials::new(
            Some(server.url("/zone").to_string()),
            Some(server.url("/role").to_string()),
            Some("sts.{region}.amazonaws.com".into()),
            Some(server.url("/token").to_string()),
            "another_audience".into(),
            SharedEnvProvider::default(),
            SharedHttpClientProvider::default(),
        );

        let subject_token = creds.subject_token().await?;
        let token_str = subject_token.token;

        // Subject token is URL encoded, so we need to decode it once before parsing JSON.
        let decoded_json: String = url::form_urlencoded::parse(token_str.as_bytes())
            .map(|(k, _)| k)
            .collect();

        let val: serde_json::Value = serde_json::from_str(&decoded_json)?;

        assert_eq!(val["method"], "POST", "{val:?}");
        assert_eq!(
            val["url"], "https://sts.us-east-1.amazonaws.com/",
            "{val:?}"
        );

        let headers = val["headers"]
            .as_array()
            .ok_or("headers should be an array")?;

        // Find x-goog-cloud-target-resource
        let target_resource = headers
            .iter()
            .find(|h| h["key"] == X_GOOG_CLOUD_TARGET_RESOURCE)
            .ok_or("missing target resource header")?;
        assert_eq!(target_resource["value"], "another_audience", "{val:?}");

        // Find Authorization
        let auth = headers
            .iter()
            .find(|h| h["key"] == "Authorization")
            .ok_or("missing auth header")?;
        assert!(
            auth["value"].as_str().unwrap().contains("AWS4-HMAC-SHA256"),
            "{val:?}"
        );
        assert!(
            auth["value"]
                .as_str()
                .unwrap()
                .contains("ACCESS_KEY_ID_IMDS"),
            "{val:?}"
        );

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_subject_token_env_success() -> TestResult {
        let _e1 = ScopedEnv::set(AWS_REGION, "us-west-2");
        let _e2 = ScopedEnv::set(AWS_ACCESS_KEY_ID, "AN_ACCESS_KEY_ID");
        let _e3 = ScopedEnv::set(AWS_SECRET_ACCESS_KEY, "SECRET_ENV");
        let _e4 = ScopedEnv::remove(AWS_SESSION_TOKEN);

        let creds = AwsSourcedCredentials::new(
            None,
            None,
            Some("sts.{region}.amazonaws.com".into()),
            None,
            "some_audience".into(),
            SharedEnvProvider::default(),
            SharedHttpClientProvider::default(),
        );

        let subject_token = creds.subject_token().await?;
        let token_str = subject_token.token;
        let decoded_json: String = url::form_urlencoded::parse(token_str.as_bytes())
            .map(|(k, _)| k)
            .collect();
        let val: serde_json::Value = serde_json::from_str(&decoded_json)?;

        assert_eq!(
            val["url"], "https://sts.us-west-2.amazonaws.com/",
            "{val:?}"
        );

        let headers = val["headers"]
            .as_array()
            .ok_or("headers should be an array")?;
        let auth = headers
            .iter()
            .find(|h| h["key"] == "Authorization")
            .ok_or("missing auth header")?;
        assert!(
            auth["value"].as_str().unwrap().contains("AN_ACCESS_KEY_ID"),
            "{auth:?}"
        );

        Ok(())
    }
}
