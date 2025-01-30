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

#[cfg(all(test, feature = "run-integration-tests"))]
mod driver {
    use auth::credentials::create_access_token_credential;
    use gax::error::Error;
    use gax::options::ClientConfig as Config;
    use scoped_env::ScopedEnv;
    use secretmanager::client::SecretManagerService;

    type Result<T> = std::result::Result<T, gax::error::Error>;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    #[serial_test::serial]
    async fn service_account() -> Result<()> {
        // Create a SecretManager client. When running on GCB, this loads MDS
        // credentials for our `integration-test-runner` service account.
        let client = SecretManagerService::new().await?;

        // Load the ADC json for the principal under test, in this case, a
        // service account.
        let response = client
            .access_secret_version(
                "projects/rust-auth-testing/secrets/test-sa-creds-json/versions/latest",
            )
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
        let config = Config::new().set_credential(creds);
        let client = SecretManagerService::new_with_config(config).await?;

        // Access a secret, which only this principal has permissions to do.
        let response = client
            .access_secret_version(
                "projects/rust-auth-testing/secrets/test-sa-creds-secret/versions/latest",
            )
            .send()
            .await?;
        let secret = response
            .payload
            .expect("missing payload in test-sa-creds-secret response")
            .data;
        assert_eq!(secret, "service_account");

        Ok(())
    }
}
