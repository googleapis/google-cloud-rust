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
    use google_cloud_compute_v1::client::{Images, Instances, Projects};
    use google_cloud_lro::Poller;
    use integration_tests::random_vm_id;
    use user_guide_samples::compute::{compute_usage_report_set, *};

    #[tokio::test]
    async fn images_samples() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;

        let client = Images::builder().build().await?;
        compute_images_list::sample(&client, &project_id).await?;
        compute_images_list_page::sample(&client, &project_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn instance_samples() -> anyhow::Result<()> {
        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;

        let client = Instances::builder().build().await?;

        let _cleanup = tokio::spawn({
            let client = client.clone();
            let project_id = project_id.clone();
            async move {
                if let Err(err) = cleanup_stale_instances(&client, &project_id).await {
                    eprintln!("Error cleaning up stale instances: {err:?}");
                }
            }
        });

        let name = random_vm_id();
        compute_instances_create::sample(&client, &project_id, &name).await?;
        compute_instances_list_all::sample(&client, &project_id).await?;
        quickstart::quickstart(&project_id).await?;
        compute_instances_delete::sample(&client, &project_id, &name).await?;

        let name = random_vm_id();
        compute_instances_operation_check::sample(&client, &project_id, &name).await?;
        compute_instances_delete::sample(&client, &project_id, &name).await?;

        Ok(())
    }

    #[tokio::test]
    async fn usage_report_samples() -> anyhow::Result<()> {
        use google_cloud_storage::client::StorageControl;

        let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")?;

        let control = StorageControl::builder().build().await?;
        let bucket_id = storage_samples::random_bucket_id();
        create_reports_bucket(&control, &project_id, &bucket_id).await?;

        let result = usage_report_samples_impl(&project_id, &bucket_id).await;
        if let Err(err) =
            storage_samples::cleanup_bucket(control, format!("projects/_/buckets/{bucket_id}"))
                .await
        {
            eprintln!("Error cleaning up reports bucket {bucket_id}: {err:?}");
        };
        result
    }

    async fn usage_report_samples_impl(project_id: &str, bucket_id: &str) -> anyhow::Result<()> {
        use google_cloud_compute_v1::model::UsageExportLocation;
        let client = Projects::builder().build().await?;
        compute_usage_report_set::sample(&client, project_id, bucket_id).await?;
        compute_usage_report_get::sample(&client, project_id).await?;
        // Disable the reports.
        let _operation = client
            .set_usage_export_bucket()
            .set_project(project_id)
            .set_body(UsageExportLocation::new())
            .poller()
            .until_done()
            .await?;
        Ok(())
    }
}
