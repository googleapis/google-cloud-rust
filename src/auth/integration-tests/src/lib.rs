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
    external_account::Builder as ExternalAccountCredentialsBuilder,
    impersonated::Builder as ImpersonatedCredentialsBuilder,
    service_account::Builder as ServiceAccountCredentialsBuilder,
};
use bigquery::client::DatasetService;
use gax::error::rpc::Code;
use httptest::{Expectation, Server, matchers::*, responders::*};
use iamcredentials::client::IAMCredentials;
use language::client::LanguageService;
use language::model::Document;
use scoped_env::ScopedEnv;
use secretmanager::client::SecretManagerService;

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

pub async fn workload_identity_provider_url_sourced() -> anyhow::Result<()> {
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");
    let audience = get_oidc_audience();
    let (service_account, client_email) = get_byoid_service_account_and_email();

    let id_token = generate_id_token(audience.clone(), client_email, service_account).await?;

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

    let contents = serde_json::json!({
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

pub async fn workload_identity_provider_executable_sourced() -> anyhow::Result<()> {
    // allow command execution
    let _e = ScopedEnv::set("GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES", "1");
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");
    let audience = get_oidc_audience();
    let (service_account, client_email) = get_byoid_service_account_and_email();

    let id_token = generate_id_token(audience.clone(), client_email, service_account).await?;

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
    let contents = serde_json::json!({
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

/// Generates a Google ID token using the iamcredentials generateIdToken API.
/// https://cloud.google.com/iam/docs/creating-short-lived-service-account-credentials#sa-credentials-oidc
async fn generate_id_token(
    audience: String,
    client_email: String,
    service_account: serde_json::Value,
) -> anyhow::Result<String> {
    let creds = AccessTokenCredentialBuilder::new(service_account.clone())
        .build()
        .expect("failed to setup service account credentials for IAM");

    let client = IAMCredentials::builder()
        .with_credentials(creds)
        .build()
        .await
        .expect("failed to setup IAM client");

    let res = client
        .generate_id_token()
        .set_audience(audience)
        .set_include_email(true)
        .set_name(format!("projects/-/serviceAccounts/{client_email}"))
        .send()
        .await?;

    Ok(res.token)
}

fn get_oidc_audience() -> String {
    std::env::var("GOOGLE_WORKLOAD_IDENTITY_OIDC_AUDIENCE")
        .expect("GOOGLE_WORKLOAD_IDENTITY_OIDC_AUDIENCE not set")
}

fn get_byoid_service_account_and_email() -> (serde_json::Value, String) {
    let service_account = get_byoid_service_account();
    let client_email = match service_account.get("client_email") {
        Some(serde_json::Value::String(v)) => v.clone(),
        None | Some(_) => {
            panic!("missing `client_email` string in service account: {service_account:?}")
        }
    };

    (service_account, client_email)
}

fn get_byoid_service_account() -> serde_json::Value {
    let path = std::env::var("GOOGLE_WORKLOAD_IDENTITY_CREDENTIALS")
        .expect("GOOGLE_WORKLOAD_IDENTITY_CREDENTIALS not set");

    let service_account_content =
        std::fs::read_to_string(path).expect("unable to read service account");
    let service_account: serde_json::Value = serde_json::from_str(service_account_content.as_str())
        .expect("unable to parse service account");

    service_account
}
