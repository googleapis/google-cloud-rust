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
    use storage_samples::{random_bucket_id, run_bucket_examples};

    #[tokio::test]
    async fn run_all_examples() -> anyhow::Result<()> {
        let client = StorageControl::builder().build().await?;

        let bucket_id = random_bucket_id();
        let result = run_bucket_examples(&bucket_id).await;
        // Ignore cleanup errors.
        let _ = cleanup_bucket(client, format!("projects/_/buckets/{bucket_id}")).await;
        result
    }
}
