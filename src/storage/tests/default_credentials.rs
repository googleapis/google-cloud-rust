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
    async fn load_default_credentials() -> anyhow::Result<()> {
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
}
