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

// [START compute_usage_report_get]
use google_cloud_compute_v1::client::Projects;

pub async fn sample(client: &Projects, project_id: &str) -> anyhow::Result<()> {
    let response = client.get().set_project(project_id).send().await?;
    let Some(bucket) = response
        .usage_export_location
        .as_ref()
        .and_then(|l| l.bucket_name.as_ref())
    else {
        println!("usage reports are disabled for {project_id}");
        return Ok(());
    };
    let prefix = response
        .usage_export_location
        .as_ref()
        .and_then(|l| l.report_name_prefix.as_deref())
        .unwrap_or("usage_gce");
    println!("usage reports are enabled and go to bucket {bucket} with prefix {prefix}");

    Ok(())
}
// [END compute_usage_report_get]
