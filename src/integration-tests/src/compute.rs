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
use compute::client::{Images, Instances, MachineTypes, Zones};
use compute::model::{
    AttachedDisk, AttachedDiskInitializeParams, Duration as ComputeDuration, Instance,
    NetworkInterface, Scheduling, ServiceAccount, scheduling::InstanceTerminationAction,
    scheduling::ProvisioningModel,
};
use gax::paginator::ItemPaginator as _;
use lro::Poller;

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

pub async fn errors() -> Result<()> {
    use gax::error::rpc::Code;
    use gax::error::rpc::StatusDetails;

    let project_id = crate::project_id()?;
    let zone_id = crate::zone_id();

    let credentials = auth::credentials::anonymous::Builder::new().build();
    let client = Zones::builder()
        .with_credentials(credentials)
        .build()
        .await?;

    let err = client
        .get()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .send()
        .await
        .expect_err("request should fail with anonymous credentials");

    assert_eq!(
        err.status().map(|s| s.code),
        Some(Code::Unauthenticated),
        "{err:?}"
    );
    assert!(
        err.status()
            .map(|s| &s.details)
            .is_some_and(|d| !d.is_empty()),
        "{err:?}"
    );

    let client = Zones::builder().build().await?;
    let err = client
        .get()
        .set_project("google-not-a-valid-project-name-starts-with-google--")
        .set_zone(&zone_id)
        .send()
        .await
        .expect_err("request should fail with bad project id");
    assert_eq!(
        err.status().map(|s| s.code),
        Some(Code::NotFound),
        "{err:?}"
    );

    let err = client
        .get()
        .set_project("undefined")
        .set_zone(&zone_id)
        .send()
        .await
        .expect_err("request should fail with bad project id");
    assert_eq!(
        err.status().map(|s| s.code),
        Some(Code::PermissionDenied),
        "{err:?}"
    );

    let error_info = err.status().and_then(|s| {
        s.details
            .iter()
            .find(|d| matches!(d, StatusDetails::ErrorInfo(_)))
    });
    assert!(
        matches!(error_info, Some(StatusDetails::ErrorInfo(_))),
        "{err:?}"
    );
    let msg = err.status().and_then(|s| {
        s.details
            .iter()
            .find(|d| matches!(d, StatusDetails::LocalizedMessage(_)))
    });
    assert!(
        matches!(msg, Some(StatusDetails::LocalizedMessage(_))),
        "{err:?}"
    );

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
    let mut aggregates = client.aggregated_list().set_project(&project_id).by_item();
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
        tracing::warn!(
            "zone {zone} has {} machine types",
            scoped_list.machine_types.len()
        );
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

pub async fn images() -> Result<()> {
    tracing::info!("Testing Images::list()");
    let client = Images::builder().with_tracing().build().await?;
    // Debian 13 is supported until 2030. When it is dropped, the test will not
    // be as entertaining, but will still be useful.
    let mut items = client
        .list()
        .set_project("debian-cloud")
        .set_filter("family=debian-13 AND architecture=X86_64")
        .by_item();
    while let Some(item) = items.next().await.transpose()? {
        tracing::info!("item = {item:?}");
    }
    tracing::info!("DONE with Images::list()");
    Ok(())
}

pub async fn instances() -> Result<()> {
    let client = Instances::builder().with_tracing().build().await?;
    let project_id = crate::project_id()?;
    let service_account = crate::test_service_account()?;
    let zone_id = crate::zone_id();

    let id = crate::random_vm_id();
    let body = Instance::new()
        .set_machine_type(format!("zones/{zone_id}/machineTypes/f1-micro"))
        .set_name(&id)
        .set_description("A test VM created by the Rust client library.")
        .set_disks([AttachedDisk::new()
            .set_initialize_params(
                AttachedDiskInitializeParams::new()
                    .set_source_image("projects/cos-cloud/global/images/family/cos-stable"),
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
    let operation = client
        .insert()
        .set_project(&project_id)
        .set_zone(&zone_id)
        .set_body(body)
        .poller()
        .until_done()
        .await?;
    tracing::info!("Operation completed with = {operation:?}");

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

pub async fn region_instances() -> Result<()> {
    use compute::client::RegionInstances;
    use compute::model::{BulkInsertInstanceResource, InstanceProperties};

    let client = RegionInstances::builder().with_tracing().build().await?;
    let project_id = crate::project_id()?;
    let service_account = crate::test_service_account()?;
    let region_id = crate::region_id();

    let instance_properties = InstanceProperties::new()
        .set_description("A test VM created by the Rust client library.")
        .set_machine_type("f1-micro")
        .set_disks([AttachedDisk::new()
            .set_initialize_params(
                AttachedDiskInitializeParams::new()
                    .set_source_image("projects/cos-cloud/global/images/family/cos-stable"),
            )
            .set_boot(true)
            .set_auto_delete(true)])
        .set_network_interfaces([NetworkInterface::new().set_network("global/networks/default")])
        .set_service_accounts([ServiceAccount::new()
            .set_email(&service_account)
            .set_scopes(["https://www.googleapis.com/auth/cloud-platform.read-only"])]);
    // Automatically shutdown and delete the instance after 15m.
    let instance_properties = instance_properties.set_scheduling(
        Scheduling::new()
            .set_provisioning_model(ProvisioningModel::Spot)
            .set_instance_termination_action(InstanceTerminationAction::Delete)
            .set_max_run_duration(ComputeDuration::new().set_seconds(15 * 60)),
    );

    let id = crate::random_vm_prefix(16);
    let body = BulkInsertInstanceResource::new()
        .set_count(1)
        .set_name_pattern(format!("{id}-####"))
        .set_instance_properties(instance_properties);

    tracing::info!("Starting new instance.");
    let operation = client
        .bulk_insert()
        .set_project(&project_id)
        .set_region(&region_id)
        .set_body(body)
        .poller()
        .until_done()
        .await?;
    tracing::info!("Operation completed with = {operation:?}");

    Ok(())
}
