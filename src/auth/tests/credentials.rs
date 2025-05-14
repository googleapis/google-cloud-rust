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

use google_cloud_auth::credentials::mds::Builder as MdsBuilder;
use google_cloud_auth::credentials::service_account::Builder as ServiceAccountBuilder;
use google_cloud_auth::credentials::testing::test_credentials;
use google_cloud_auth::credentials::user_account::Builder as UserAccountCredentialBuilder;
use google_cloud_auth::credentials::{
    Builder as AccessTokenCredentialBuilder, Credentials, CredentialsProvider,
    api_key_credentials::Builder as ApiKeyCredentialsBuilder,
};
use google_cloud_auth::errors::CredentialsError;
use serde_json::json;

type Result<T> = std::result::Result<T, CredentialsError>;

#[cfg(test)]
mod test {
    use super::*;
    use http::header::{HeaderName, HeaderValue};
    use http::{Extensions, HeaderMap};
    use scoped_env::ScopedEnv;
    use std::error::Error;

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credentials_fallback_to_mds() {
        let _e1 = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _e2 = ScopedEnv::remove("HOME"); // For posix
        let _e3 = ScopedEnv::remove("APPDATA"); // For windows

        let mds = AccessTokenCredentialBuilder::default().build().unwrap();
        let fmt = format!("{:?}", mds);
        assert!(fmt.contains("MDSCredentials"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credentials_errors_if_adc_env_is_not_a_file() {
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", "file-does-not-exist.json");
        let err = AccessTokenCredentialBuilder::default()
            .build()
            .err()
            .unwrap();
        let msg = err.source().unwrap().to_string();
        assert!(msg.contains("Failed to load Application Default Credentials"));
        assert!(msg.contains("file-does-not-exist.json"));
        assert!(msg.contains("GOOGLE_APPLICATION_CREDENTIALS"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credentials_malformed_adc_is_error() {
        for contents in ["{}", r#"{"type": 42}"#] {
            let file = tempfile::NamedTempFile::new().unwrap();
            let path = file.into_temp_path();
            std::fs::write(&path, contents).expect("Unable to write to temporary file.");
            let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());

            let err = AccessTokenCredentialBuilder::default()
                .build()
                .err()
                .unwrap();
            let msg = err.source().unwrap().to_string();
            assert!(msg.contains("Failed to parse"));
            assert!(msg.contains("`type` field"));
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credentials_adc_unimplemented_credential_type() {
        let contents = r#"{
            "type": "some_unknown_credential_type"
        }"#;

        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, contents).expect("Unable to write to temporary file.");
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());

        let err = AccessTokenCredentialBuilder::default()
            .build()
            .err()
            .unwrap();
        let msg = err.source().unwrap().to_string();
        assert!(msg.contains("Invalid or unsupported"));
        assert!(msg.contains("some_unknown_credential_type"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credentials_adc_user_credentials() {
        let contents = r#"{
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user"
        }"#;

        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, contents).expect("Unable to write to temporary file.");
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());

        let uc = AccessTokenCredentialBuilder::default().build().unwrap();
        let fmt = format!("{:?}", uc);
        assert!(fmt.contains("UserCredentials"));
    }

    #[tokio::test]
    async fn create_access_token_credentials_json_user_credentials() {
        let contents = r#"{
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user"
        }"#;

        let quota_project = "test-quota-project";

        let uc = AccessTokenCredentialBuilder::new(serde_json::from_str(contents).unwrap())
            .with_quota_project_id(quota_project)
            .build()
            .unwrap();

        let fmt = format!("{:?}", uc);
        assert!(fmt.contains("UserCredentials"));
        assert!(fmt.contains(quota_project));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credentials_adc_service_account_credentials() {
        let contents = r#"{
            "type": "service_account",
            "project_id": "test-project-id",
            "private_key_id": "test-private-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nBLAHBLAHBLAH\n-----END PRIVATE KEY-----\n",
            "client_email": "test-client-email",
            "universe_domain": "test-universe-domain"
        }"#;

        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, contents).expect("Unable to write to temporary file.");
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());

        let sac = AccessTokenCredentialBuilder::default().build().unwrap();
        let fmt = format!("{:?}", sac);
        assert!(fmt.contains("ServiceAccountCredentials"));
    }

    #[tokio::test]
    async fn create_access_token_credentials_json_service_account_credentials() {
        let contents = r#"{
            "type": "service_account",
            "project_id": "test-project-id",
            "private_key_id": "test-private-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nBLAHBLAHBLAH\n-----END PRIVATE KEY-----\n",
            "client_email": "test-client-email",
            "universe_domain": "test-universe-domain"
        }"#;

        let quota_project = "test-quota-project";

        let sac = AccessTokenCredentialBuilder::new(serde_json::from_str(contents).unwrap())
            .with_quota_project_id(quota_project)
            .build()
            .unwrap();
        let fmt = format!("{:?}", sac);
        assert!(fmt.contains("ServiceAccountCredentials"));
        assert!(fmt.contains(quota_project));
    }

    #[tokio::test]
    async fn create_api_key_credentials_success() {
        let creds = ApiKeyCredentialsBuilder::new("test-api-key").build();
        let fmt = format!("{:?}", creds);
        assert!(fmt.contains("ApiKeyCredentials"), "{fmt:?}");
        assert!(!fmt.contains("test-api-key"), "{fmt:?}");
    }

    mockall::mock! {
        #[derive(Debug)]
        Credentials {}

        impl CredentialsProvider for Credentials {
            async fn headers(&self, extensions: Extensions) -> Result<HeaderMap>;
            async fn universe_domain(&self) -> Option<String>;
        }
    }

    #[tokio::test]
    async fn mocking_with_default_values() -> Result<()> {
        let mut mock = MockCredentials::new();
        mock.expect_headers()
            .return_once(|_extensions| Ok(HeaderMap::default()));
        mock.expect_universe_domain().return_once(|| None);

        let creds = Credentials::from(mock);
        assert!(creds.headers(Extensions::new()).await?.is_empty());
        assert_eq!(creds.universe_domain().await, None);

        Ok(())
    }

    #[tokio::test]
    async fn mocking_with_custom_header() -> Result<()> {
        let mut mock = MockCredentials::new();
        let headers = HeaderMap::from_iter([(
            HeaderName::from_static("test-header"),
            HeaderValue::from_static("test-value"),
        )]);
        let headers_clone = headers.clone();
        mock.expect_headers()
            .return_once(|_extensions| Ok(headers_clone));
        mock.expect_universe_domain().return_once(|| None);

        let creds = Credentials::from(mock);
        assert_eq!(creds.headers(Extensions::new()).await?, headers);
        assert_eq!(creds.universe_domain().await, None);

        Ok(())
    }

    #[tokio::test]
    async fn mocking_with_custom_universe_domain() -> Result<()> {
        let mut mock = MockCredentials::new();

        let universe_domain = "test-universe-domain";
        let universe_domain_clone = universe_domain.to_string();
        mock.expect_headers()
            .return_once(|_extensions| Ok(HeaderMap::default()));
        mock.expect_universe_domain()
            .return_once(|| Some(universe_domain_clone));

        let creds = Credentials::from(mock);
        assert!(creds.headers(Extensions::new()).await?.is_empty());
        assert_eq!(creds.universe_domain().await.unwrap(), universe_domain);

        Ok(())
    }

    #[tokio::test]
    async fn testing_credentials() -> Result<()> {
        let creds = test_credentials();
        assert!(creds.headers(Extensions::new()).await?.is_empty());
        assert_eq!(creds.universe_domain().await, None);
        Ok(())
    }

    #[tokio::test]
    async fn get_mds_credentials_from_builder() -> Result<()> {
        let test_quota_project = "test-quota-project";
        let test_universe_domain = "test-universe-domain";
        let mdcs = MdsBuilder::default()
            .with_quota_project_id(test_quota_project)
            .with_universe_domain(test_universe_domain)
            .build()?;
        let fmt = format!("{:?}", mdcs);
        assert!(fmt.contains("MDSCredentials"));
        assert!(fmt.contains(test_quota_project));
        assert!(fmt.contains(test_universe_domain));
        Ok(())
    }

    #[tokio::test]
    async fn get_service_account_credentials_from_builder() -> Result<()> {
        let test_quota_project = "test-quota-project";
        let service_account_info_json = json!({
            "client_email": "test-client-email",
            "private_key_id": "test-private-key-id",
            "private_key": "",
            "project_id": "test-project-id",
            "universe_domain": "test-universe-domain",
        });
        let service_account = ServiceAccountBuilder::new(service_account_info_json)
            .with_quota_project_id(test_quota_project)
            .build()?;
        let fmt = format!("{:?}", service_account);
        assert!(fmt.contains("ServiceAccountCredentials"));
        assert!(fmt.contains(test_quota_project));
        Ok(())
    }

    #[tokio::test]
    async fn get_user_account_credentials_from_builder() -> Result<()> {
        let test_quota_project = "test-quota-project";
        let authorized_user = serde_json::json!({
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "refresh_token": "test-refresh-token",
            "type": "authorized_user",
        });
        let user_account = UserAccountCredentialBuilder::new(authorized_user)
            .with_quota_project_id(test_quota_project)
            .build()?;
        let fmt = format!("{:?}", user_account);
        assert!(fmt.contains("UserCredentials"), "{fmt:?}");
        assert!(fmt.contains(test_quota_project));
        Ok(())
    }
}
