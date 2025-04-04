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
use google_cloud_auth::credentials::{
    ApiKeyOptions, Credential, CredentialsTrait, create_access_token_credential,
    create_api_key_credentials,
};
use google_cloud_auth::errors::CredentialError;
use google_cloud_auth::token::Token;
use serde_json::json;

type Result<T> = std::result::Result<T, CredentialError>;

#[cfg(test)]
mod test {
    use super::*;
    use http::header::{HeaderName, HeaderValue};
    use scoped_env::ScopedEnv;
    use std::error::Error;

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credential_fallback_to_mds() {
        let _e1 = ScopedEnv::remove("GOOGLE_APPLICATION_CREDENTIALS");
        let _e2 = ScopedEnv::remove("HOME"); // For posix
        let _e3 = ScopedEnv::remove("APPDATA"); // For windows

        let mds = create_access_token_credential().await.unwrap();
        let fmt = format!("{:?}", mds);
        assert!(fmt.contains("MDSCredential"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credential_errors_if_adc_env_is_not_a_file() {
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", "file-does-not-exist.json");
        let err = create_access_token_credential().await.err().unwrap();
        let msg = err.source().unwrap().to_string();
        assert!(msg.contains("Failed to load Application Default Credentials"));
        assert!(msg.contains("file-does-not-exist.json"));
        assert!(msg.contains("GOOGLE_APPLICATION_CREDENTIALS"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credential_malformed_adc_is_error() {
        for contents in ["{}", r#"{"type": 42}"#] {
            let file = tempfile::NamedTempFile::new().unwrap();
            let path = file.into_temp_path();
            std::fs::write(&path, contents).expect("Unable to write to temporary file.");
            let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());

            let err = create_access_token_credential().await.err().unwrap();
            let msg = err.source().unwrap().to_string();
            assert!(msg.contains("Failed to parse"));
            assert!(msg.contains("`type` field"));
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credential_adc_unimplemented_credential_type() {
        let contents = r#"{
            "type": "some_unknown_credential_type"
        }"#;

        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, contents).expect("Unable to write to temporary file.");
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());

        let err = create_access_token_credential().await.err().unwrap();
        let msg = err.source().unwrap().to_string();
        assert!(msg.contains("Unimplemented"));
        assert!(msg.contains("some_unknown_credential_type"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credential_adc_user_credentials() {
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

        let uc = create_access_token_credential().await.unwrap();
        let fmt = format!("{:?}", uc);
        assert!(fmt.contains("UserCredential"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_access_token_credential_adc_service_account_credentials() {
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

        let sac = create_access_token_credential().await.unwrap();
        let fmt = format!("{:?}", sac);
        assert!(fmt.contains("ServiceAccountCredential"));
    }

    #[tokio::test]
    async fn create_api_key_credentials_success() {
        let creds = create_api_key_credentials("test-api-key", ApiKeyOptions::default())
            .await
            .unwrap();
        let fmt = format!("{:?}", creds);
        assert!(fmt.contains("ApiKeyCredential"), "{fmt:?}");
    }

    mockall::mock! {
        #[derive(Debug)]
        Credential {}

        impl CredentialsTrait for Credential {
            async fn get_token(&self) -> Result<Token>;
            async fn get_headers(&self) -> Result<Vec<(HeaderName, HeaderValue)>>;
            async fn get_universe_domain(&self) -> Option<String>;
        }
    }

    #[tokio::test]
    async fn mocking() -> Result<()> {
        let mut mock = MockCredential::new();
        mock.expect_get_token().return_once(|| {
            Ok(Token {
                token: "test-token".to_string(),
                token_type: "Bearer".to_string(),
                expires_at: None,
                metadata: None,
            })
        });
        mock.expect_get_headers().return_once(|| Ok(Vec::new()));
        mock.expect_get_universe_domain().return_once(|| None);

        let creds = Credential::from(mock);
        assert_eq!(creds.get_token().await?.token, "test-token");
        assert!(creds.get_headers().await?.is_empty());
        assert_eq!(creds.get_universe_domain().await, None);

        Ok(())
    }

    #[tokio::test]
    async fn testing_credentials() -> Result<()> {
        let creds = test_credentials();
        assert_eq!(creds.get_token().await?.token, "test-only-token");
        assert!(creds.get_headers().await?.is_empty());
        assert_eq!(creds.get_universe_domain().await, None);
        Ok(())
    }

    #[tokio::test]
    async fn get_mds_credentials_from_builder() -> Result<()> {
        let test_quota_project = "test-quota-project";
        let test_universe_domain = "test-universe-domain";
        let mdcs = MdsBuilder::default()
            .quota_project_id(test_quota_project)
            .universe_domain(test_universe_domain)
            .build();
        let fmt = format!("{:?}", mdcs);
        assert!(fmt.contains("MDSCredential"));
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
        assert!(fmt.contains("ServiceAccountCredential"));
        assert!(fmt.contains(test_quota_project));
        Ok(())
    }
}
