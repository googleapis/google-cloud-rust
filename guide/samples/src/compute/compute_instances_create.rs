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

// [START compute_instances_create]
use google_cloud_compute_v1::client::Instances;
use google_cloud_compute_v1::model::{
    AttachedDisk, AttachedDiskInitializeParams, Instance, NetworkInterface,
};
use google_cloud_lro::Poller;

pub async fn sample(client: &Instances, project_id: &str, name: &str) -> anyhow::Result<()> {
    const ZONE: &str = "us-central1-a";

    let instance = Instance::new()
        .set_machine_type(format!("zones/{ZONE}/machineTypes/f1-micro"))
        .set_name(name)
        .set_description("A test VM created by the Rust client library.")
        .set_labels([("source", "compute_instances_create")])
        .set_disks([AttachedDisk::new()
            .set_initialize_params(
                AttachedDiskInitializeParams::new()
                    .set_source_image("projects/cos-cloud/global/images/family/cos-stable"),
            )
            .set_boot(true)
            .set_auto_delete(true)])
        .set_network_interfaces([NetworkInterface::new().set_network("global/networks/default")]);

    let operation = client
        .insert()
        .set_project(project_id)
        .set_zone(ZONE)
        .set_body(instance)
        .poller()
        .until_done()
        .await?;
    println!("Instance successfully created: {operation:?}");

    Ok(())
}
// [END compute_instances_create]
