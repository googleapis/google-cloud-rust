// Copyright 2026 Google LLC
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

// [START storage_get_service_account]
use google_cloud_storage::client::Storage;

pub async fn sample(project_id: &str) -> anyhow::Result<()> {
    let client = Storage::builder().build().await?;

    // Fetches the email address of the GCS service agent for the given project.
    // If the service agent does not exist yet, the GCS control plane creates it under the hood.
    let email = client
        .get_service_account(project_id)
        .send()
        .await?;

    println!("Service account email for project {project_id}: {email}");

    Ok(())
}
// [END storage_get_service_account]
