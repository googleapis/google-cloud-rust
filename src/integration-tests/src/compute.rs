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

use crate::Result;
use compute::client::{Images, Instances, MachineTypes, ZoneOperations, Zones};
use compute::model::{
    AttachedDisk, AttachedDiskInitializeParams, Duration as ComputeDuration, Instance,
    NetworkInterface, Scheduling, ServiceAccount, operation::Status,
    scheduling::InstanceTerminationAction, scheduling::ProvisioningModel,
};
use gax::paginator::ItemPaginator as _;

pub async fn zones() -> Result<()> {
    let client = Zones::builder().with_tracing().build().await?;
    let project_id = crate::project_id()?;
    let zone_id = crate::zone_id();

    tracing::info!("Testing Zones::list()");
    let mut items = client.list().set_project(&project_id).by_item();
    while let Some(item) = items.next().await.transpose()? {
        tracing::info!("item = {item:?}");
    }
    tracing::info!("DONE with Zones::list()");

    tracing::info!("Testing Zones::get()");
    // us-central1-a is well-known, and if it goes away fixing this test is the
    // least of our problems.
    let response = client
        .get()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .send()
        .await?;
    assert_eq!(
        response.status,
        Some(compute::model::zone::Status::Up),
        "response={response:?}"
    );
    tracing::info!("Zones::get() = {response:?}");

    Ok(())
}

pub async fn machine_types() -> Result<()> {
    let client = MachineTypes::builder().with_tracing().build().await?;
    let project_id = crate::project_id()?;
    let zone_id = crate::zone_id();

    tracing::info!("Testing MachineTypes::list()");
    let mut items = client
        .list()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .by_item();
    while let Some(item) = items.next().await.transpose()? {
        tracing::info!("item = {item:?}");
    }
    tracing::info!("DONE with MachineTypes::list()");

    tracing::info!("Testing MachineTypes::aggregated_list()");
    let mut aggregates = client
            .aggregated_list()
            .set_project(&project_id)
            .by_item();
    let mut count = 0;
    while let Some((zone, scoped_list)) = aggregates.next().await.transpose()? {
        if count > 10 {
            // This can be a very slow test because it returns many pages.
            break;
        }
        if scoped_list.machine_types.is_empty() {
            // The service returns many uninteresting, empty items.
            continue;
        }
        if let Some(warning) = scoped_list.warning {
            tracing::info!("missing response for {zone}: {warning:?}");
            count += 1;
            continue;
        }
        tracing::warn!("zone {zone} has {} machine types", scoped_list.machine_types.len());
        count += 1;
    }
    tracing::info!("DONE with MachineTypes::aggregated_list()");

    tracing::info!("Testing MachineTypes::get()");
    // us-central1-a is well-known, and if it goes away fixing this test is the
    // least of our problems.
    let response = client
        .get()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .set_machine_type("f1-micro")
        .send()
        .await?;
    assert_eq!(response.is_shared_cpu, Some(true), "response={response:?}");
    tracing::info!("MachineTypes::get() = {response:?}");

    Ok(())
}

async fn latest_image(client: &Images, project_id: &str, prefix: &str) -> Result<String> {
    use compute::model::image::Architecture;

    let mut latest = None;
    let mut items = client.list().set_project(project_id).by_item();
    while let Some(item) = items.next().await.transpose()? {
        tracing::info!("item = {item:?}");
        if item.architecture != Some(Architecture::X8664)
            || item
                .family
                .as_deref()
                .is_some_and(|v| v.strip_prefix(prefix).is_none())
        {
            continue;
        }
        latest = match &latest {
            None => Some(item),
            Some(i)
                if item
                    .family
                    .as_deref()
                    .is_some_and(|f| f > i.family.as_deref().unwrap_or_default()) =>
            {
                Some(item)
            }
            Some(i)
                if item.family == i.family && item.creation_timestamp > i.creation_timestamp =>
            {
                Some(item)
            }
            _ => latest,
        };
    }
    latest
        .and_then(|i| i.name)
        .ok_or(anyhow::Error::msg(format!(
            "cannot find an image in project {project_id} starting with {prefix}"
        )))
}

pub async fn images() -> Result<()> {
    tracing::info!("Testing Images::list()");
    let client = Images::builder().with_tracing().build().await?;
    let name = latest_image(&client, "debian-cloud", "debian-").await?;
    tracing::info!("DONE with Images::list()");
    tracing::info!("LATEST cos-cloud image is {name}");
    Ok(())
}

pub async fn instances() -> Result<()> {
    let images = Images::builder().build().await?;
    let client = Instances::builder().with_tracing().build().await?;
    let operations = ZoneOperations::builder().with_tracing().build().await?;
    let project_id = crate::project_id()?;
    let service_account = crate::test_service_account()?;
    let zone_id = crate::zone_id();

    // We need a recent image in an allowed image family.
    let image = latest_image(&images, "cos-cloud", "cos-").await?;

    let id = crate::random_vm_id();
    let body = Instance::new()
        .set_machine_type(format!("zones/{zone_id}/machineTypes/f1-micro"))
        .set_name(&id)
        .set_description("A test VM created by the Rust client library.")
        .set_disks([AttachedDisk::new()
            .set_initialize_params(
                AttachedDiskInitializeParams::new()
                    .set_source_image(format!("projects/cos-cloud/global/images/{image}")),
            )
            .set_boot(true)
            .set_auto_delete(true)])
        .set_network_interfaces([NetworkInterface::new().set_network("global/networks/default")])
        .set_service_accounts([ServiceAccount::new()
            .set_email(&service_account)
            .set_scopes(["https://www.googleapis.com/auth/cloud-platform.read-only"])]);
    // Automatically shutdown and delete the instance after 15m.
    let body = body.set_scheduling(
        Scheduling::new()
            .set_provisioning_model(ProvisioningModel::Spot)
            .set_instance_termination_action(InstanceTerminationAction::Delete)
            .set_max_run_duration(ComputeDuration::new().set_seconds(15 * 60)),
    );

    tracing::info!("Starting new instance.");
    let mut operation = client
        .insert()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .set_body(body)
        .send()
        .await?;

    while operation.status.as_ref().is_none_or(|s| *s != Status::Done) {
        if let Some(err) = operation.error {
            return Err(anyhow::Error::msg(format!("{err:?}")));
        }
        tracing::info!("Waiting for new instance operation: {operation:?}");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        operation = operations
            .get()
            .set_project(&project_id)
            .set_zone(&zone_id)
            .set_operation(operation.name.unwrap_or_default())
            .send()
            .await?;
    }
    tracing::info!("Operation completed with = {operation:?}");

    if let Some(err) = operation.error {
        tracing::error!("Operation failed: {err:?}");
        return Err(anyhow::Error::msg(format!(
            "instance creation failed - {err:?}"
        )));
    }

    tracing::info!("Getting instance details.");
    let instance = client
        .get()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .set_instance(&id)
        .send()
        .await?;
    tracing::info!("instance = {instance:?}");

    tracing::info!("Testing Instances::list()");
    let mut items = client
        .list()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .by_item();
    while let Some(instance) = items.next().await.transpose()? {
        tracing::info!("instance = {instance:?}");
    }
    tracing::info!("DONE Instances::list()");

    Ok(())
}
