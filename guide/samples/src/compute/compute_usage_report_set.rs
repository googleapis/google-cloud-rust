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

// [START compute_usage_report_set]
use google_cloud_compute_v1::client::Projects;
use google_cloud_compute_v1::model::UsageExportLocation;
use google_cloud_lro::Poller;

pub async fn sample(client: &Projects, project_id: &str, bucket_name: &str) -> anyhow::Result<()> {
    let operation = client
        .set_usage_export_bucket()
        .set_project(project_id)
        .set_body(UsageExportLocation::new().set_bucket_name(bucket_name))
        .poller()
        .until_done()
        .await?;
    println!(
        "Setting the usage export bucket with the default prefix (`usage_gce`) completed successfully: {operation:?}"
    );

    let operation = client
        .set_usage_export_bucket()
        .set_project(project_id)
        .set_body(
            UsageExportLocation::new()
                .set_bucket_name(bucket_name)
                .set_report_name_prefix("report-prefix"),
        )
        .poller()
        .until_done()
        .await?;
    println!("Setting the usage export bucket completed successfully: {operation:?}");

    Ok(())
}
// [END compute_usage_report_set]
