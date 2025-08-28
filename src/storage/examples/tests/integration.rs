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
mod tests {
    use google_cloud_storage::client::StorageControl;
    use storage_samples::*;

    #[cfg(feature = "skipped-integration-tests")]
    #[tokio::test]
    async fn anywhere_cache_examples() -> anyhow::Result<()> {
        let client = StorageControl::builder().build().await?;

        let mut buckets = Vec::new();
        let result = run_anywhere_cache_examples(&mut buckets).await;
        // Ignore cleanup errors.
        for id in buckets.into_iter() {
            let _ = cleanup_bucket(client.clone(), format!("projects/_/buckets/{id}")).await;
        }
        result
    }

    #[tokio::test]
    async fn bucket_examples() -> anyhow::Result<()> {
        let client = StorageControl::builder().build().await?;

        let mut buckets = Vec::new();
        let result = run_bucket_examples(&mut buckets).await;
        // Ignore cleanup errors.
        for id in buckets.into_iter() {
            let _ = cleanup_bucket(client.clone(), format!("projects/_/buckets/{id}")).await;
        }
        result
    }

    #[tokio::test]
    async fn managed_folder_examples() -> anyhow::Result<()> {
        let client = StorageControl::builder().build().await?;

        let mut buckets = Vec::new();
        let result = run_managed_folder_examples(&mut buckets).await;
        // Ignore cleanup errors.
        for id in buckets.into_iter() {
            let _ = cleanup_bucket(client.clone(), format!("projects/_/buckets/{id}")).await;
        }
        result
    }

    #[tokio::test]
    async fn object_examples() -> anyhow::Result<()> {
        let client = StorageControl::builder().build().await?;

        let mut buckets = Vec::new();
        let result = run_object_examples(&mut buckets).await;
        // Ignore cleanup errors.
        for id in buckets.into_iter() {
            let _ = cleanup_bucket(client.clone(), format!("projects/_/buckets/{id}")).await;
        }
        result
    }
}
