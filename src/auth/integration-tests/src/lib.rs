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

use auth::credentials::{
    Builder as AccessTokenCredentialBuilder,
    api_key_credentials::Builder as ApiKeyCredentialsBuilder,
    external_account::{
        Builder as ExternalAccountCredentialsBuilder,
        ProgrammaticBuilder as ExternalAccountProgrammaticBuilder,
    },
    impersonated::Builder as ImpersonatedCredentialsBuilder,
    service_account::Builder as ServiceAccountCredentialsBuilder,
    subject_token::{Builder as SubjectTokenBuilder, SubjectToken, SubjectTokenProvider},
};
use auth::errors::SubjectTokenProviderError;
use bigquery::client::DatasetService;
use gax::error::rpc::Code;
use httptest::{Expectation, Server, matchers::*, responders::*};
use iamcredentials::client::IAMCredentials;
use language::client::LanguageService;
use language::model::Document;
use scoped_env::ScopedEnv;
use secretmanager::client::SecretManagerService;
use std::sync::Arc;

pub async fn service_account() -> anyhow::Result<()> {
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");

    // Create a SecretManager client. When running on GCB, this loads MDS
    // credentials for our `integration-test-runner` service account.
    let client = SecretManagerService::builder().build().await?;

    // Load the ADC json for the principal under test, in this case, a
    // service account.
    let response = client
        .access_secret_version()
        .set_name(format!(
            "projects/{project}/secrets/test-sa-creds-json/versions/latest"
        ))
        .send()
        .await?;
    let adc_json = response
        .payload
        .expect("missing payload in test-sa-creds-json response")
        .data;

    // Write the ADC to a temporary file
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.into_temp_path();
    std::fs::write(&path, adc_json).expect("Unable to write to temporary file.");

    // Create credentials for the principal under test.
    let _e = ScopedEnv::set("GOOGLE_APPLICATION_CREDENTIALS", path.to_str().unwrap());
    let creds = AccessTokenCredentialBuilder::default().build()?;

    // Construct a new SecretManager client using the credentials.
    let client = SecretManagerService::builder()
        .with_credentials(creds)
        .build()
        .await?;

    // Access a secret, which only this principal has permissions to do.
    let response = client
        .access_secret_version()
        .set_name(format!(
            "projects/{project}/secrets/test-sa-creds-secret/versions/latest"
        ))
        .send()
        .await?;
    let secret = response
        .payload
        .expect("missing payload in test-sa-creds-secret response")
        .data;
    assert_eq!(secret, "service_account");

    Ok(())
}

pub async fn service_account_with_audience() -> anyhow::Result<()> {
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");

    // Create a SecretManager client. When running on GCB, this loads MDS
    // credentials for our `integration-test-runner` service account.
    let client = SecretManagerService::builder().build().await?;

    // Load the ADC json for the principal under test, in this case, a
    // service account.
    let response = client
        .access_secret_version()
        .set_name(format!(
            "projects/{project}/secrets/test-sa-creds-json/versions/latest"
        ))
        .send()
        .await?;
    let sa_json = response
        .payload
        .expect("missing payload in test-sa-creds-json response")
        .data;

    let sa_json: serde_json::Value = serde_json::from_slice(&sa_json)?;

    // Create credentials for the principal under test, but with an audience.
    let creds = ServiceAccountCredentialsBuilder::new(sa_json)
        .with_access_specifier(
            auth::credentials::service_account::AccessSpecifier::from_audience(
                "https://secretmanager.googleapis.com/",
            ),
        )
        .build()?;

    // Construct a new SecretManager client using the credentials.
    let client = SecretManagerService::builder()
        .with_credentials(creds)
        .build()
        .await?;

    // Access a secret, which only this principal has permissions to do.
    let response = client
        .access_secret_version()
        .set_name(format!(
            "projects/{project}/secrets/test-sa-creds-secret/versions/latest"
        ))
        .send()
        .await?;
    let secret = response
        .payload
        .expect("missing payload in test-sa-creds-secret response")
        .data;
    assert_eq!(secret, "service_account");

    Ok(())
}

pub async fn impersonated() -> anyhow::Result<()> {
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");

    // Create a SecretManager client. When running on GCB, this loads MDS
    // credentials for our `integration-test-runner` service account.
    let client = SecretManagerService::builder().build().await?;

    // Load the service account json that will be the source credential
    let response = client
        .access_secret_version()
        .set_name(format!(
            "projects/{project}/secrets/test-sa-creds-json/versions/latest"
        ))
        .send()
        .await?;
    let source_sa_json = response
        .payload
        .expect("missing payload in test-sa-creds-json response")
        .data;

    let source_sa_json: serde_json::Value = serde_json::from_slice(&source_sa_json)?;

    let source_sa_creds = ServiceAccountCredentialsBuilder::new(source_sa_json).build()?;

    let impersonated_creds =
        ImpersonatedCredentialsBuilder::from_source_credentials(source_sa_creds.clone())
            .with_target_principal(format!(
                "impersonation-target@{project}.iam.gserviceaccount.com"
            ))
            .build()?;

    let client = SecretManagerService::builder()
        .with_credentials(impersonated_creds)
        .build()
        .await?;

    // Access a secret, which only this principal has permissions to do.
    let response = client
        .access_secret_version()
        .set_name(format!(
            "projects/{project}/secrets/impersonation-target-secret/versions/latest"
        ))
        .send()
        .await?;
    let secret = response
        .payload
        .expect("missing payload in impersonation-target-secret response")
        .data;
    assert_eq!(secret, "impersonated_secret_value");

    // Verify that using the source credential directly does not work
    let client_with_source_creds = SecretManagerService::builder()
        .with_credentials(source_sa_creds)
        .build()
        .await?;
    let result = client_with_source_creds
        .access_secret_version()
        .set_name(format!(
            "projects/{project}/secrets/impersonation-target-secret/versions/latest"
        ))
        .send()
        .await;

    match result {
        Ok(_) => panic!(
            "source credentials should not have access to the secret, but the call succeeded"
        ),
        Err(e) => {
            // The error `e` from a client call is of type `google_cloud_gax::error::Error`.
            // We can inspect it to see if it's the error we expect.
            // In this case, we expect a `PermissionDenied` error from the service.
            if let Some(status) = e.status() {
                assert_eq!(
                    status.code,
                    Code::PermissionDenied,
                    "Expected PermissionDenied, but got a different status: {status:?}"
                );
            } else {
                panic!("Expected a service error, but got a different kind of error: {e}");
            }
        }
    }

    Ok(())
}

pub async fn api_key() -> anyhow::Result<()> {
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");

    // Create a SecretManager client. When running on GCB, this loads MDS
    // credentials for our `integration-test-runner` service account.
    let client = SecretManagerService::builder().build().await?;

    // Load the API key under test.
    let response = client
        .access_secret_version()
        .set_name(format!(
            "projects/{project}/secrets/test-api-key/versions/latest",
        ))
        .send()
        .await?;
    let api_key = response
        .payload
        .expect("missing payload in test-api-key response")
        .data;
    let api_key = std::str::from_utf8(&api_key).unwrap();

    // Create credentials using the API key.
    let creds = ApiKeyCredentialsBuilder::new(api_key).build();

    // Construct a Natural Language client using the credentials.
    let client = LanguageService::builder()
        .with_credentials(creds)
        .build()
        .await?;

    // Make a request using the API key.
    let d = Document::new()
        .set_content("Hello, world!")
        .set_type(language::model::document::Type::PlainText);
    client.analyze_sentiment().set_document(d).send().await?;

    Ok(())
}

pub async fn workload_identity_provider_url_sourced(
    with_impersonation: bool,
) -> anyhow::Result<()> {
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");
    let audience = get_oidc_audience();
    let target_principal_email = get_external_account_service_account_email();

    let id_token = generate_id_token(audience.clone(), target_principal_email).await?;

    let source_token_response_body = serde_json::json!({
        "id_token": id_token,
    });

    let server = Server::run();
    server.expect(
        Expectation::matching(all_of![
            request::method_path("GET", "/source_token"),
            request::headers(contains(("metadata", "True",))),
        ])
        .respond_with(json_encoded(source_token_response_body)),
    );

    let mut contents = serde_json::json!({
      "type": "external_account",
      "audience": audience,
      "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
      "token_url": "https://sts.googleapis.com/v1/token",
      "credential_source": {
        "url": server.url("/source_token").to_string(),
        "headers": {
          "Metadata": "True"
        },
        "format": {
          "type": "json",
          "subject_token_field_name": "id_token"
        }
      }
    });

    if with_impersonation {
        let impersonated_email = format!("impersonation-target@{project}.iam.gserviceaccount.com");
        let impersonation_url = format!(
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{impersonated_email}:generateAccessToken"
        );
        contents["service_account_impersonation_url"] =
            serde_json::Value::String(impersonation_url);
    }

    // Create external account with Url sourced creds
    let creds = ExternalAccountCredentialsBuilder::new(contents).build()?;

    // Construct a BigQuery client using the credentials.
    // Using BigQuery as it doesn't require a billing account.
    let client = DatasetService::builder()
        .with_credentials(creds)
        .build()
        .await?;

    // Make a request using the external account credentials
    client
        .list_datasets()
        .set_project_id(project)
        .send()
        .await?;

    Ok(())
}

pub async fn workload_identity_provider_executable_sourced(
    with_impersonation: bool,
) -> anyhow::Result<()> {
    // allow command execution
    let _e = ScopedEnv::set("GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES", "1");
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");
    let audience = get_oidc_audience();
    let target_principal_email = get_external_account_service_account_email();

    let id_token = generate_id_token(audience.clone(), target_principal_email).await?;

    let source_token_output_file = serde_json::json!({
        "success": true,
        "version": 1,
        "token_type": "urn:ietf:params:oauth:token-type:jwt",
        "id_token": id_token,
    })
    .to_string();

    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.into_temp_path();
    std::fs::write(&path, source_token_output_file)
        .expect("Unable to write to temp file with id token");

    let path = path.to_str().unwrap();
    let mut contents = serde_json::json!({
      "type": "external_account",
      "audience": audience,
      "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
      "token_url": "https://sts.googleapis.com/v1/token",
      "credential_source": {
        "executable": {
            "command": format!("cat {path}"),
        },
      }
    });

    if with_impersonation {
        let impersonated_email = format!("impersonation-target@{project}.iam.gserviceaccount.com");
        let impersonation_url = format!(
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{impersonated_email}:generateAccessToken"
        );
        contents["service_account_impersonation_url"] =
            serde_json::Value::String(impersonation_url);
    }

    // Create external account with Url sourced creds
    let creds = ExternalAccountCredentialsBuilder::new(contents).build()?;

    // Construct a BigQuery client using the credentials.
    // Using BigQuery as it doesn't require a billing account.
    let client = DatasetService::builder()
        .with_credentials(creds)
        .build()
        .await?;

    // Make a request using the external account credentials
    client
        .list_datasets()
        .set_project_id(project)
        .send()
        .await?;

    Ok(())
}

pub async fn workload_identity_provider_file_sourced(
    with_impersonation: bool,
) -> anyhow::Result<()> {
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");
    let audience = get_oidc_audience();
    let target_principal_email = get_external_account_service_account_email();

    let id_token = generate_id_token(audience.clone(), target_principal_email).await?;

    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.into_temp_path();
    std::fs::write(&path, id_token).expect("Unable to write to temp file with id token");

    let path = path.to_str().unwrap();
    let mut contents = serde_json::json!({
      "type": "external_account",
      "audience": audience,
      "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
      "token_url": "https://sts.googleapis.com/v1/token",
      "credential_source": {
        "file": path,
      }
    });

    if with_impersonation {
        let impersonated_email = format!("impersonation-target@{project}.iam.gserviceaccount.com");
        let impersonation_url = format!(
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{impersonated_email}:generateAccessToken"
        );
        contents["service_account_impersonation_url"] =
            serde_json::Value::String(impersonation_url);
    }

    // Create external account with File sourced creds
    let creds = ExternalAccountCredentialsBuilder::new(contents).build()?;

    // Construct a BigQuery client using the credentials.
    // Using BigQuery as it doesn't require a billing account.
    let client = DatasetService::builder()
        .with_credentials(creds)
        .build()
        .await?;

    // Make a request using the external account credentials
    client
        .list_datasets()
        .set_project_id(project)
        .send()
        .await?;

    Ok(())
}

pub async fn workload_identity_provider_programmatic_sourced() -> anyhow::Result<()> {
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");
    let audience = get_oidc_audience();
    let target_principal_email = get_external_account_service_account_email();

    let id_token = generate_id_token(audience.clone(), target_principal_email).await?;

    let subject_token_provider = Arc::new(TestSubjectTokenProvider {
        subject_token: id_token,
    });

    let builder = ExternalAccountProgrammaticBuilder::new(subject_token_provider)
        .with_audience(audience)
        .with_subject_token_type("urn:ietf:params:oauth:token-type:jwt");

    // Create external account with programmatic sourced creds
    let creds = builder.build()?;

    // Construct a BigQuery client using the credentials.
    // Using BigQuery as it doesn't require a billing account.
    let client = DatasetService::builder()
        .with_credentials(creds)
        .build()
        .await?;

    // Make a request using the external account credentials
    client
        .list_datasets()
        .set_project_id(project)
        .send()
        .await?;

    Ok(())
}

/// Generates a Google ID token using the iamcredentials generateIdToken API.
/// https://cloud.google.com/iam/docs/creating-short-lived-service-account-credentials#sa-credentials-oidc
async fn generate_id_token(
    audience: String,
    target_principal_email: String,
) -> anyhow::Result<String> {
    let creds = AccessTokenCredentialBuilder::default()
        .build()
        .expect("failed to get default credentials for IAM");

    let client = IAMCredentials::builder()
        .with_credentials(creds)
        .build()
        .await
        .expect("failed to setup IAM client");

    let res = client
        .generate_id_token()
        .set_audience(audience)
        .set_include_email(true)
        .set_name(format!(
            "projects/-/serviceAccounts/{target_principal_email}"
        ))
        .set_delegates(vec![format!(
            "projects/-/serviceAccounts/{target_principal_email}"
        )])
        .send()
        .await
        .expect("failed to generate id token");

    Ok(res.token)
}

fn get_oidc_audience() -> String {
    std::env::var("GOOGLE_WORKLOAD_IDENTITY_OIDC_AUDIENCE")
        .expect("GOOGLE_WORKLOAD_IDENTITY_OIDC_AUDIENCE not set")
}

fn get_external_account_service_account_email() -> String {
    std::env::var("EXTERNAL_ACCOUNT_SERVICE_ACCOUNT_EMAIL")
        .expect("EXTERNAL_ACCOUNT_SERVICE_ACCOUNT_EMAIL not set")
}

#[derive(Debug)]
struct TestSubjectTokenProvider {
    subject_token: String,
}

#[derive(Debug)]
struct TestProviderError;
impl std::fmt::Display for TestProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TestProviderError")
    }
}
impl std::error::Error for TestProviderError {}
impl SubjectTokenProviderError for TestProviderError {
    fn is_transient(&self) -> bool {
        false
    }
}

impl SubjectTokenProvider for TestSubjectTokenProvider {
    type Error = TestProviderError;
    async fn subject_token(&self) -> std::result::Result<SubjectToken, Self::Error> {
        Ok(SubjectTokenBuilder::new(self.subject_token.clone()).build())
    }
}
