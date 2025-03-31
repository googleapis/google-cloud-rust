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

use auth::credentials::{ApiKeyOptions, create_access_token_credential, create_api_key_credential};
use gax::error::Error;
use language::client::LanguageService;
use language::model::Document;
use scoped_env::ScopedEnv;
use secretmanager::client::SecretManagerService;

pub type Result<T> = std::result::Result<T, gax::error::Error>;

pub async fn service_account() -> Result<()> {
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");

    // Create a SecretManager client. When running on GCB, this loads MDS
    // credentials for our `integration-test-runner` service account.
    let client = SecretManagerService::builder().build().await?;

    // Load the ADC json for the principal under test, in this case, a
    // service account.
    let response = client
        .access_secret_version(format!(
            "projects/{}/secrets/test-sa-creds-json/versions/latest",
            project
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
    let creds = create_access_token_credential()
        .await
        .map_err(Error::authentication)?;

    // Construct a new SecretManager client using the credentials.
    let client = SecretManagerService::builder()
        .with_credentials(creds)
        .build()
        .await?;

    // Access a secret, which only this principal has permissions to do.
    let response = client
        .access_secret_version(format!(
            "projects/{}/secrets/test-sa-creds-secret/versions/latest",
            project
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

pub async fn api_key() -> Result<()> {
    let project = std::env::var("GOOGLE_CLOUD_PROJECT").expect("GOOGLE_CLOUD_PROJECT not set");

    // Create a SecretManager client. When running on GCB, this loads MDS
    // credentials for our `integration-test-runner` service account.
    let client = SecretManagerService::builder().build().await?;

    // Load the API key under test.
    let response = client
        .access_secret_version(format!(
            "projects/{}/secrets/test-api-key/versions/latest",
            project
        ))
        .send()
        .await?;
    let api_key = response
        .payload
        .expect("missing payload in test-api-key response")
        .data;
    let api_key = std::str::from_utf8(&api_key).unwrap();

    // Create credentials using the API key.
    let creds = create_api_key_credential(api_key, ApiKeyOptions::default())
        .await
        .map_err(Error::authentication)?;

    // Construct a Natural Language client using the credentials.
    let client = LanguageService::builder()
        .with_credentials(creds)
        .build()
        .await?;

    // Make a request using the API key.
    let d = Document::new()
        .set_content("Hello, world!")
        .set_type(language::model::document::Type::PLAIN_TEXT);
    client.analyze_sentiment().set_document(d).send().await?;

    Ok(())
}
