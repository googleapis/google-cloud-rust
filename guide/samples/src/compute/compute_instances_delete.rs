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

// [START compute_instances_delete]
use google_cloud_compute_v1::client::Instances;
use google_cloud_lro::Poller;

pub async fn sample(client: &Instances, project_id: &str, name: &str) -> anyhow::Result<()> {
    const ZONE: &str = "us-central1-a";

    let operation = client
        .delete()
        .set_project(project_id)
        .set_zone(ZONE)
        .set_instance(name)
        .poller()
        .until_done()
        .await?
        .to_result()?;
    println!("Instance successfully deleted: {operation:?}");

    Ok(())
}
// [END compute_instances_delete]
