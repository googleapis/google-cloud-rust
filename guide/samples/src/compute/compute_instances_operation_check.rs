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

// [START compute_instances_operation_check]
use google_cloud_compute_v1::client::{Instances, ZoneOperations};
use google_cloud_compute_v1::model::{
    AttachedDisk, AttachedDiskInitializeParams, Instance, NetworkInterface,
};
use google_cloud_compute_v1::model::{Operation, operation::Status};

pub async fn sample(client: &Instances, project_id: &str, name: &str) -> anyhow::Result<()> {
    const ZONE: &str = "us-central1-a";
    let operations_client = ZoneOperations::builder().build().await?;

    // Start an operation, in this example we use a VM creation.
    let mut op = start_instance_insert(client, project_id, ZONE, name).await?;

    // Manually wait for the operation.
    let operation = loop {
        if op.status.as_ref().is_some_and(|s| s == &Status::Done) {
            break op;
        }
        let Some(name) = op.name.clone() else {
            return Err(anyhow::Error::msg(format!(
                "the operation name should be set, operation={op:?}"
            )));
        };
        println!("polling operation {op:?}");
        op = operations_client
            .wait()
            .set_project(project_id)
            .set_zone(ZONE)
            .set_operation(name)
            .send()
            .await?;
    };

    println!("Instance creation finished: {operation:?}");
    // Check if there was an error.
    if let Err(error) = operation.to_result() {
        println!("Instance creation failed: {error:?}");
        return Err(anyhow::Error::msg(format!(
            "instance creation failed with: {error:?}"
        )));
    }

    Ok(())
}

async fn start_instance_insert(
    client: &Instances,
    project_id: &str,
    zone: &str,
    name: &str,
) -> anyhow::Result<Operation> {
    let instance = Instance::new()
        .set_machine_type(format!("zones/{zone}/machineTypes/f1-micro"))
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

    // Start the operation without waiting for it to complete. This will require
    // manually waiting for the operation. In most cases we recommend you use
    // `.poller().until_done()` instead of `.send()`:
    let op = client
        .insert()
        .set_project(project_id)
        .set_zone(zone)
        .set_body(instance)
        .send()
        .await?;
    Ok(op)
}

// [END compute_instances_operation_check]
