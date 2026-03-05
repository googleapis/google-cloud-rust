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
    use google_cloud_test_utils::errors::anydump;
    use google_cloud_test_utils::resource_names::random_bucket_id;
    use google_cloud_test_utils::runtime_config::project_id;

    #[tokio::test]
    async fn images_samples() -> anyhow::Result<()> {
        user_guide_samples::compute::drive_image_samples()
            .await
            .inspect_err(anydump)
    }

    #[ignore = "TODO(#3691) - disabled because it was flaky"]
    #[tokio::test]
    async fn instance_samples() -> anyhow::Result<()> {
        user_guide_samples::compute::drive_instance_samples()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test]
    async fn usage_report_samples() -> anyhow::Result<()> {
        let project_id = project_id()?;
        let control = StorageControl::builder().build().await?;
        let bucket_id = random_bucket_id();
        user_guide_samples::compute::create_reports_bucket(&control, &project_id, &bucket_id)
            .await?;
        let result =
            user_guide_samples::compute::drive_usage_report_samples(&project_id, &bucket_id).await;
        let _ = storage_samples::cleanup_bucket(control, format!("projects/_/buckets/{bucket_id}"))
            .await
            .inspect_err(|e| eprintln!("error cleaning up bucket {bucket_id}: {e:?}"));
        result.inspect_err(anydump)
    }
}
