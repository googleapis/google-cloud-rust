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
mod tests {
    use google_cloud_storage::client::Storage;
    use scoped_env::ScopedEnv;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn load_default_credentials_missing() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let missing = tmp.path().join("--does-not-exist--");
        let _e = ScopedEnv::set(
            "GOOGLE_APPLICATION_CREDENTIALS",
            missing.to_str().expect("tmp is a UTF-8 string"),
        );

        // This should fail because the file does not exist.
        let err = Storage::builder().build().await.unwrap_err();
        assert!(err.is_default_credentials(), "{err:?}");
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn load_default_credentials_success() -> anyhow::Result<()> {
        let tmp = tempfile::tempdir()?;
        let destination = tmp.path().join("sa.json");
        let contents = serde_json::json!({
            "type": "service_account",
            "project_id": "test-project-id",
            "private_key_id": "test-private-key-id",
            "private_key": "-----BEGIN PRIVATE KEY-----\nBLAHBLAHBLAH\n-----END PRIVATE KEY-----\n",
            "client_email": "test-client-email",
            "universe_domain": "test-universe-domain"
        });
        std::fs::write(destination.clone(), contents.to_string())?;

        let _e = ScopedEnv::set(
            "GOOGLE_APPLICATION_CREDENTIALS",
            destination.to_str().expect("tmp is a UTF-8 string"),
        );

        // This should fail because the file does not exist.
        let result = Storage::builder().build().await;
        assert!(result.is_ok(), "{result:?}");
        Ok(())
    }
}
