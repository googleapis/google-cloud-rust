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

pub mod storage {
    pub mod queue;
    pub mod quickstart;

    pub use integration_tests::random_bucket_id;

    #[cfg(all(test, feature = "run-integration-tests"))]
    mod driver {
        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        async fn quickstart() -> anyhow::Result<()> {
            let project_id = std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
            let bucket_id = random_bucket_id();
            let response = super::quickstart::quickstart(&project_id, &bucket_id).await;
            // Ignore cleanup errors.
            let _ = super::cleanup_bucket(&bucket_id).await;
            response
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn queue() -> anyhow::Result<()> {
            let (control, bucket) = integration_tests::storage::create_test_bucket().await?;
            let response = super::queue::queue(&bucket.name, "test-only").await;
            // Ignore cleanup errors.
            let _ = integration_tests::storage::cleanup_bucket(control, bucket.name).await;
            response
        }
    }

    pub async fn cleanup_bucket(bucket_id: &str) -> anyhow::Result<()> {
        let control = google_cloud_storage::client::StorageControl::builder()
            .build()
            .await?;
        integration_tests::storage::cleanup_bucket(control, format!("projects/_/{bucket_id}")).await
    }
}
