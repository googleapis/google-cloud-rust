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

#[cfg(test)]
mod test {
    use std::error::Error;
    use std::fmt;

    use google_cloud_auth::credentials::external_account::ProgrammaticBuilder;
    use google_cloud_auth::credentials::subject_token::{
        Builder as SubjectTokenBuilder, SubjectToken, SubjectTokenProvider,
    };
    use google_cloud_auth::errors::SubjectTokenProviderError;

    use google_cloud_auth::credentials::EntityTag;
    use google_cloud_auth::credentials::mds::Builder as MdsBuilder;
    use google_cloud_auth::credentials::service_account::Builder as ServiceAccountBuilder;
    use google_cloud_auth::credentials::testing::test_credentials;
    use google_cloud_auth::credentials::user_account::Builder as UserAccountCredentialBuilder;
    use google_cloud_auth::credentials::{
        Builder as AccessTokenCredentialBuilder, CacheableResource, Credentials,
        CredentialsProvider, api_key_credentials::Builder as ApiKeyCredentialsBuilder,
    };
    use google_cloud_auth::errors::CredentialsError;
    use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
    use http::{Extensions, HeaderMap};
    use httptest::{Expectation, Server, matchers::*, responders::*};
    use scoped_env::ScopedEnv;
    use serde_json::json;
    use test_case::test_case;

    type Result<T> = anyhow::Result<T>;
    type TestResult = anyhow::Result<(), Box<dyn std::error::Error>>;

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
        let err = AccessTokenCredentialBuilder::default().build().unwrap_err();
        assert!(err.is_loading(), "{err:?}");
        let msg = err.to_string();
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

            let err = AccessTokenCredentialBuilder::default().build().unwrap_err();
            assert!(err.is_parsing(), "{err:?}");
            assert!(err.to_string().contains("`type` field"), "{err}");
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

        let err = AccessTokenCredentialBuilder::default().build().unwrap_err();
        assert!(err.is_unknown_type(), "{err:?}");
        assert!(
            err.to_string().contains("some_unknown_credential_type"),
            "{err}"
        );
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
    async fn create_access_token_credentials_adc_impersonated_service_account() {
        let contents = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            "source_credentials": {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token"
            }
        }).to_string();

        let file = tempfile::NamedTempFile::new().unwrap();
        let path = file.into_temp_path();
        std::fs::write(&path, contents).expect("Unable to write to temporary file.");
        let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());

        let ic = AccessTokenCredentialBuilder::default().build().unwrap();
        let fmt = format!("{:?}", ic);
        assert!(fmt.contains("ImpersonatedServiceAccount"));
    }

    #[tokio::test]
    async fn create_access_token_credentials_json_impersonated_service_account() {
        let contents = json!({
            "type": "impersonated_service_account",
            "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/test-principal:generateAccessToken",
            "source_credentials": {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token"
            }
        });

        let quota_project = "test-quota-project";

        let ic = AccessTokenCredentialBuilder::new(contents)
            .with_quota_project_id(quota_project)
            .build()
            .unwrap();

        let fmt = format!("{:?}", ic);
        assert!(fmt.contains("ImpersonatedServiceAccount"));
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

    #[tokio::test]
    async fn create_external_account_access_token() -> TestResult {
        let source_token_response_body = json!({
            "access_token":"an_example_token",
        })
        .to_string();

        let token_response_body = json!({
            "access_token":"an_exchanged_token",
            "issued_token_type":"urn:ietf:params:oauth:token-type:access_token",
            "token_type":"Bearer",
            "expires_in":3600,
            "scope":"https://www.googleapis.com/auth/cloud-platform"
        })
        .to_string();

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/source_token"),
                request::headers(contains(("metadata", "True",))),
            ])
            .respond_with(status_code(200).body(source_token_response_body)),
        );

        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/token"),
                request::body(url_decoded(contains(("subject_token", "an_example_token")))),
                request::body(url_decoded(contains((
                    "subject_token_type",
                    "urn:ietf:params:oauth:token-type:jwt"
                )))),
                request::body(url_decoded(contains(("audience", "some-audience")))),
                request::headers(contains((
                    "content-type",
                    "application/x-www-form-urlencoded"
                ))),
            ])
            .respond_with(status_code(200).body(token_response_body)),
        );

        let contents = json!({
          "type": "external_account",
          "audience": "some-audience",
          "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
          "token_url": server.url("/token").to_string(),
          "credential_source": {
            "url": server.url("/source_token").to_string(),
            "headers": {
              "Metadata": "True"
            },
            "format": {
              "type": "json",
              "subject_token_field_name": "access_token"
            }
          }
        })
        .to_string();

        let creds =
            AccessTokenCredentialBuilder::new(serde_json::from_str(contents.as_str()).unwrap())
                .build()
                .unwrap();

        // Use the debug output to verify the right kind of credentials are created.
        let fmt = format!("{:?}", creds);
        print!("{:?}", creds);
        assert!(fmt.contains("ExternalAccountCredentials"));

        let cached_headers = creds.headers(Extensions::new()).await?;
        match cached_headers {
            CacheableResource::New { data, .. } => {
                let token = data
                    .get(AUTHORIZATION)
                    .and_then(|token_value| token_value.to_str().ok())
                    .map(|s| s.to_string())
                    .unwrap();
                assert!(token.contains("Bearer an_exchanged_token"));
            }
            CacheableResource::NotModified => {
                unreachable!("Expecting a header to be present");
            }
        };

        Ok(())
    }

    #[tokio::test]
    async fn create_external_account_access_token_fail() -> TestResult {
        let source_token_response_body = json!({
            "error":"invalid_token",
        })
        .to_string();

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("GET", "/source_token"),
                request::headers(contains(("metadata", "True",))),
            ])
            .respond_with(status_code(429).body(source_token_response_body)),
        );

        let contents = json!({
          "type": "external_account",
          "audience": "some-audience",
          "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
          "token_url": server.url("/token").to_string(),
          "credential_source": {
            "url": server.url("/source_token").to_string(),
            "headers": {
              "Metadata": "True"
            },
            "format": {
              "type": "json",
              "subject_token_field_name": "access_token"
            }
          }
        })
        .to_string();

        let creds =
            AccessTokenCredentialBuilder::new(serde_json::from_str(contents.as_str()).unwrap())
                .build()
                .unwrap();

        let error = creds.headers(Extensions::new()).await.unwrap_err();
        let original_error = error
            .source()
            .expect("should have a source")
            .downcast_ref::<CredentialsError>()
            .expect("source should be a CredentialsError");
        assert!(original_error.to_string().contains("invalid_token"));
        assert!(error.is_transient());

        Ok(())
    }

    mockall::mock! {
        #[derive(Debug)]
        Credentials {}

        impl CredentialsProvider for Credentials {
            async fn headers(&self, extensions: Extensions) -> std::result::Result<CacheableResource<HeaderMap>, CredentialsError>;
            async fn universe_domain(&self) -> Option<String>;
        }
    }

    #[tokio::test]
    async fn mocking_with_default_values() -> Result<()> {
        let mut mock = MockCredentials::new();
        mock.expect_headers().return_once(|_extensions| {
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: HeaderMap::new(),
            })
        });
        mock.expect_universe_domain().return_once(|| None);

        let creds = Credentials::from(mock);
        let cached_headers = creds.headers(Extensions::new()).await?;
        match cached_headers {
            CacheableResource::New { entity_tag, data } => {
                assert_eq!(entity_tag, EntityTag::default());
                assert!(data.is_empty());
            }
            CacheableResource::NotModified => {
                unreachable!("Expecting a header to be present");
            }
        };
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
        let cached_headers = CacheableResource::New {
            entity_tag: EntityTag::new(),
            data: headers,
        };
        let cached_headers_clone = cached_headers.clone();
        mock.expect_headers()
            .return_once(|_extensions| Ok(cached_headers));
        mock.expect_universe_domain().return_once(|| None);

        let creds = Credentials::from(mock);
        assert_eq!(
            creds.headers(Extensions::new()).await?,
            cached_headers_clone
        );
        assert_eq!(creds.universe_domain().await, None);

        Ok(())
    }

    #[tokio::test]
    async fn mocking_with_custom_universe_domain() -> Result<()> {
        let mut mock = MockCredentials::new();

        let universe_domain = "test-universe-domain";
        let universe_domain_clone = universe_domain.to_string();
        mock.expect_headers().return_once(|_extensions| {
            Ok(CacheableResource::New {
                entity_tag: EntityTag::default(),
                data: HeaderMap::new(),
            })
        });
        mock.expect_universe_domain()
            .return_once(|| Some(universe_domain_clone));

        let creds = Credentials::from(mock);
        match creds.headers(Extensions::new()).await? {
            CacheableResource::New { entity_tag, data } => {
                assert_eq!(entity_tag, EntityTag::default());
                assert!(data.is_empty());
            }
            CacheableResource::NotModified => {
                unreachable!("Expecting a header to be present");
            }
        };
        assert_eq!(creds.universe_domain().await.unwrap(), universe_domain);

        Ok(())
    }

    #[tokio::test]
    async fn testing_credentials() -> Result<()> {
        let creds = test_credentials();
        let cached_headers = creds.headers(Extensions::new()).await?;
        match cached_headers {
            CacheableResource::New { entity_tag, data } => {
                assert_eq!(entity_tag, EntityTag::default());
                assert!(data.is_empty());
            }
            CacheableResource::NotModified => {
                unreachable!("Expecting a header to be present");
            }
        };
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

    #[derive(Debug)]
    struct TestProviderError;

    impl fmt::Display for TestProviderError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "TestProviderError")
        }
    }

    impl Error for TestProviderError {}

    impl SubjectTokenProviderError for TestProviderError {
        fn is_transient(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct TestSubjectTokenProvider;

    impl SubjectTokenProvider for TestSubjectTokenProvider {
        type Error = TestProviderError;

        async fn subject_token(&self) -> std::result::Result<SubjectToken, Self::Error> {
            Ok(SubjectTokenBuilder::new("test-subject-token".to_string()).build())
        }
    }

    #[test_case(Some(vec!["scope1".to_string(), "scope2".to_string()]), vec!["scope1", "scope2"]; "with custom scopes")]
    #[test_case(None, vec!["https://www.googleapis.com/auth/cloud-platform"]; "with default scopes")]
    #[tokio::test]
    async fn create_programmatic_external_account_access_token(
        scopes: Option<Vec<String>>,
        expected_scopes: Vec<&str>,
    ) -> TestResult {
        let token_response_body = json!({
            "access_token":"an_exchanged_token",
            "issued_token_type":"urn:ietf:params:oauth:token-type:access_token",
            "token_type":"Bearer",
            "expires_in":3600,
            "scope": expected_scopes.join(" "),
        })
        .to_string();

        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", "/token"),
                request::body(url_decoded(contains((
                    "subject_token",
                    "test-subject-token"
                )))),
                request::body(url_decoded(contains(("scope", expected_scopes.join(" ")))))
            ])
            .respond_with(status_code(200).body(token_response_body)),
        );

        let provider = TestSubjectTokenProvider;
        let mut builder = ProgrammaticBuilder::new(std::sync::Arc::new(provider))
            .with_audience("some-audience".to_string())
            .with_subject_token_type("urn:ietf:params:oauth:token-type:jwt".to_string())
            .with_token_url(server.url("/token").to_string());

        if let Some(scopes) = scopes {
            builder = builder.with_scopes(scopes);
        }

        let creds = builder.build().unwrap();

        let cached_headers = creds.headers(Extensions::new()).await?;
        match cached_headers {
            CacheableResource::New { data, .. } => {
                let token = data
                    .get(AUTHORIZATION)
                    .and_then(|token_value| token_value.to_str().ok())
                    .map(|s| s.to_string())
                    .unwrap();
                assert_eq!(token, "Bearer an_exchanged_token");
            }
            CacheableResource::NotModified => {
                unreachable!("Expecting a header to be present");
            }
        };

        Ok(())
    }
}